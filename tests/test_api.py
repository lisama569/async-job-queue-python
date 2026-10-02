"""通过 FastAPI 测试客户端端到端验证 HTTP 语义。"""

from __future__ import annotations

import logging

import pytest
from fastapi.testclient import TestClient

from jobqueue.api import create_app
from jobqueue.config import Settings

logger = logging.getLogger("tests.test_api")


@pytest.fixture
def client(settings) -> TestClient:
    app = create_app(settings)
    with TestClient(app) as c:
        yield c
    app.state.queue.close()


def test_health(client: TestClient) -> None:
    assert client.get("/health").json() == {"status": "ok"}


def test_enqueue_duplicate_returns_409(client: TestClient) -> None:
    r1 = client.post("/jobs", json={"payload": {"a": 1}, "dedupe_key": "k"})
    r2 = client.post("/jobs", json={"payload": {"a": 2}, "dedupe_key": "k"})
    logger.info("[输入] HTTP 重复入队 dedupe_key=k [判定] 201 后再 409 duplicate_job")
    assert r1.status_code == 201
    assert r2.status_code == 409
    assert r2.json()["error"]["code"] == "duplicate_job"


def test_full_lifecycle_over_http(client: TestClient) -> None:
    enq = client.post("/jobs", json={"payload": {"task": "work"}}).json()
    assert enq["state"] == "pending"

    client.post("/groups", json={"group": "g"})
    empty = client.post("/claim", json={"group": "g", "consumer": "c1"})
    logger.info("[输入] 任务已入队，claim [判定] 200 返回投递；空队列时为 204 empty_queue")
    assert empty.status_code == 200

    claimed = empty.json()
    delivery_id = claimed["delivery"]["id"]
    assert claimed["job"]["id"] == enq["id"]

    # 错误消费者确认 -> 409
    r = client.post(f"/deliveries/{delivery_id}/ack", json={"consumer": "c2"})
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "delivery_owner_mismatch"

    # 正确消费者确认 -> succeeded
    r = client.post(f"/deliveries/{delivery_id}/ack", json={"consumer": "c1"})
    assert r.status_code == 200 and r.json()["state"] == "succeeded"

    # 重复确认 -> 409 delivery_terminal
    r = client.post(f"/deliveries/{delivery_id}/ack", json={"consumer": "c1"})
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "delivery_terminal"

    detail = client.get(f"/jobs/{enq['id']}").json()
    assert detail["state"] == "succeeded"
    assert [d["state"] for d in detail["deliveries"]] == ["acked"]


def test_nack_and_reject_distinction(client: TestClient) -> None:
    jid = client.post("/jobs", json={"payload": {"x": 1}}).json()["id"]
    d = client.post("/claim", json={"group": "g", "consumer": "c"}).json()["delivery"]
    # 缺 reason -> 422 由 pydantic 拦截；显式 reject -> rejected
    r = client.post(
        f"/deliveries/{d['id']}/nack",
        json={"consumer": "c", "reason": "unsupported", "reject": True},
    )
    assert r.status_code == 200
    body = r.json()
    logger.info("[输入] HTTP nack reject=true job=%s [判定] state=rejected：%s", jid, body["last_error"])
    assert body["state"] == "rejected"
    assert "不可处理" in body["last_error"]


def test_offset_rules_over_http(client: TestClient) -> None:
    client.post("/jobs", json={"payload": {}})
    client.post("/groups", json={"group": "g"})
    # 先 claim + ack 让第 1 条任务定论，位点才允许推进到 1
    claimed = client.post("/claim", json={"group": "g", "consumer": "c"}).json()
    did = claimed["delivery"]["id"]
    assert client.post(f"/deliveries/{did}/ack", json={"consumer": "c"}).status_code == 200
    assert client.post("/groups/g/offset", json={"offset": 1}).status_code == 200
    r = client.post("/groups/g/offset", json={"offset": 1})
    assert r.status_code == 409 and r.json()["error"]["code"] == "offset_rollback"
    r = client.post("/groups/g/offset", json={"offset": 99})
    assert r.status_code == 409 and r.json()["error"]["code"] == "offset_out_of_range"
    r = client.post("/groups/ghost/offset", json={"offset": 1})
    assert r.status_code == 404 and r.json()["error"]["code"] == "group_not_found"
    logger.info("[输入] HTTP 位点原地/越界/未知组 [判定] 三种情况分别 409/409/404，位点保持 1")
    assert client.get("/groups/g/offset").json()["offset"] == 1


def test_job_not_found(client: TestClient) -> None:
    r = client.get("/jobs/nope")
    assert r.status_code == 404 and r.json()["error"]["code"] == "job_not_found"


def _enqueue_claim_ack(client: TestClient, group: str = "g", consumer: str = "c") -> str:
    client.post("/jobs", json={"payload": {}})
    client.post("/groups", json={"group": group})
    claimed = client.post("/claim", json={"group": group, "consumer": consumer}).json()
    did = claimed["delivery"]["id"]
    jid = claimed["job"]["id"]
    client.post(f"/deliveries/{did}/ack", json={"consumer": consumer})
    return jid


def test_progress_endpoint(client: TestClient) -> None:
    jid = _enqueue_claim_ack(client)
    client.post("/jobs", json={"payload": {}})  # 第 2 条未投递
    client.post("/groups/g/offset", json={"offset": 1})
    r = client.get("/groups/g/progress")
    logger.info("[输入] GET progress（1 定论 1 未投递）[判定] safe_offset=1，桶计数正确")
    assert r.status_code == 200
    body = r.json()
    assert body["safe_offset"] == 1
    assert body["total"] == 2
    assert body["counts"]["succeeded"] == 1
    assert body["counts"]["pending"] == 1
    assert body["jobs"]["succeeded"] == [jid]


def test_offset_gap_over_http(client: TestClient) -> None:
    client.post("/jobs", json={"payload": {}})
    client.post("/jobs", json={"payload": {}})
    client.post("/groups", json={"group": "g"})
    claimed = client.post("/claim", json={"group": "g", "consumer": "c"}).json()
    client.post(f"/deliveries/{claimed['delivery']['id']}/ack", json={"consumer": "c"})
    r = client.post("/groups/g/offset", json={"offset": 2})
    logger.info("[输入] 第 2 条未定论却提交 offset=2 [判定] 409 offset_gap")
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "offset_gap"


def test_group_jobs_and_group_views_endpoints(client: TestClient) -> None:
    jid = _enqueue_claim_ack(client)
    r = client.get("/groups/g/jobs")
    assert r.status_code == 200
    assert r.json()["jobs"][0]["status"] == "succeeded"
    job = client.get(f"/jobs/{jid}").json()
    logger.info("[输入] GET job 详情 [判定] 含 group_views，g 组 succeeded")
    assert any(v["group"] == "g" and v["status"] == "succeeded"
               for v in job["group_views"])


def test_checkpoint_export_restore_over_http(client: TestClient) -> None:
    jid = _enqueue_claim_ack(client)
    snap = client.get("/groups/g/checkpoint")
    assert snap.status_code == 200
    snapshot = snap.json()
    assert snapshot["jobs"][0]["status"] == "succeeded"
    # 用同一份快照恢复到另一个组（路径组必须与快照 group 一致）
    r = client.post("/groups/h/checkpoint", json=snapshot)
    logger.info("[输入] 把 g 组快照 POST 到 h 组路径 [判定] 400 checkpoint_invalid")
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "checkpoint_invalid"
    # 恢复到同名组：已定论不再投递
    r = client.post("/groups/g/checkpoint", json=snapshot)
    assert r.status_code == 200
    assert r.json()["jobs"]["succeeded"] == [jid]
    claim = client.post("/claim", json={"group": "g", "consumer": "c2"})
    assert claim.status_code == 204
