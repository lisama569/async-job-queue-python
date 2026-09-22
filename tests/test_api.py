"""HTTP 端到端：入队/幂等/投递/确认/位点/状态守恒与错误码可区分。"""

import logging

import pytest
from fastapi.testclient import TestClient

from taskqueue.api import create_app
from taskqueue.queue import TaskQueue
from taskqueue.storage import Storage

logger = logging.getLogger(__name__)


@pytest.fixture
def client_pair(tmp_path):
    from taskqueue.config import QueueConfig
    cfg = QueueConfig(db_path=str(tmp_path / "api.db"), lease_timeout_ms=30_000, max_attempts=3)
    q = TaskQueue(cfg, Storage(cfg.db_path))
    with TestClient(create_app(cfg, q)) as c:
        yield c, q
    q.close()


@pytest.fixture
def client(client_pair):
    return client_pair[0]


def test_health_and_empty_status(client):
    assert client.get("/health").json() == {"status": "ok"}
    body = client.get("/status").json()
    assert body["total"] == 0
    assert set(body["counts"]) == {"pending", "inflight", "succeeded", "failed", "rejected"}


def test_enqueue_is_idempotent_over_http(client):
    logger.info("输入: POST /tasks 同 dedup_key 两次；判定: 201 与 200(deduplicated=true)")
    r1 = client.post("/tasks", json={"task_type": "echo", "payload": {"v": 1}, "dedup_key": "k"})
    r2 = client.post("/tasks", json={"task_type": "echo", "payload": {"v": 2}, "dedup_key": "k"})
    assert r1.status_code == 201 and r1.json()["deduplicated"] is False
    assert r2.status_code == 200 and r2.json()["deduplicated"] is True
    assert r1.json()["id"] == r2.json()["id"]
    assert client.get("/status").json()["total"] == 1


def test_poll_ack_fail_flow_with_error_codes(client):
    logger.info("输入: 入队 2 个任务，走 poll->ack / poll->fail；判定: 状态与错误码")
    client.post("/tasks", json={"task_type": "echo", "payload": {}})
    client.post("/tasks", json={"task_type": "echo", "payload": {}})

    msgs = client.post("/tasks/poll", json={"group": "g", "consumer": "c1"}).json()["messages"]
    assert len(msgs) == 2
    id1, ep1 = msgs[0]["task"]["id"], msgs[0]["epoch"]
    id2, ep2 = msgs[1]["task"]["id"], msgs[1]["epoch"]

    # 重复 poll 无任务
    assert client.post("/tasks/poll", json={"group": "g", "consumer": "c2"}).json()["count"] == 0

    # 错误 epoch
    r = client.post(f"/tasks/{id1}/ack", json={"epoch": ep1 + 100})
    assert r.status_code == 409 and r.json()["error"] == "LEASE_EXPIRED"

    # 成功 + 幂等
    assert client.post(f"/tasks/{id1}/ack", json={"epoch": ep1}).json()["status"] == "succeeded"
    assert client.post(f"/tasks/{id1}/ack", json={"epoch": ep1}).status_code == 200

    # 失败需要原因
    r = client.post(f"/tasks/{id2}/fail", json={"epoch": ep2, "reason": "BOOM", "retry": False})
    body = r.json()
    assert body["status"] == "failed" and body["terminal"] is True
    assert body["terminal_reason"] == "BOOM"

    # 对终态任务再操作 -> NOT_INFLIGHT
    r = client.post(f"/tasks/{id2}/ack", json={"epoch": ep2})
    assert r.status_code == 409 and r.json()["error"] == "NOT_INFLIGHT"

    # 查询
    assert client.get(f"/tasks/{id2}").json()["terminal_reason"] == "BOOM"

    counts = client.get("/status").json()["counts"]
    assert counts["succeeded"] == 1 and counts["failed"] == 1


def test_offset_endpoints_reject_invalid_moves(client):
    for i in range(2):
        client.post("/tasks", json={"task_type": "echo", "payload": {"i": i}})
    msgs = client.post("/tasks/poll", json={"group": "g", "consumer": "c"}).json()["messages"]
    for m in msgs:
        client.post(f"/tasks/{m['task']['id']}/ack", json={"epoch": m["epoch"]})

    client.put("/groups/g")
    assert client.post("/groups/g/commit", json={"offset": 1}).json()["committed_offset"] == 1
    for payload, error in [
        ({"offset": 1}, "OFFSET_ROLLBACK"),
        ({"offset": 0}, "OFFSET_ROLLBACK"),
        ({"offset": 99}, "OFFSET_OVERRUN"),
    ]:
        r = client.post("/groups/g/commit", json=payload)
        assert r.status_code == 409 and r.json()["error"] == error, payload
    # 未知组 404
    r = client.post("/groups/nope/commit", json={"offset": 1})
    assert r.status_code == 404 and r.json()["error"] == "UNKNOWN_GROUP"


def test_unknown_task_404(client):
    r = client.get("/tasks/123456")
    assert r.status_code == 404 and r.json()["error"] == "UNKNOWN_TASK"


def test_reclaim_endpoint(client_pair):
    client, q = client_pair
    # 直接入库一个已过期 inflight 任务，验证 /reclaim 回收计数
    tid = q.storage.insert_task(
        dedup_key=None, task_type="echo", payload="{}",
        status=__import__("taskqueue.models", fromlist=["TaskStatus"]).TaskStatus.INFLIGHT,
        terminal_reason=None, max_attempts=3, now_ms=1000,
    )
    q.storage._conn.execute(
        "UPDATE tasks SET lease_owner='g/c', lease_expires_at_ms=? WHERE id=?",
        (1000, tid),
    )
    q.storage._conn.commit()
    # /reclaim 用真实时钟（远大于 1000），必然回收
    r = client.post("/reclaim")
    assert r.status_code == 200 and r.json()["reclaimed"] >= 1
