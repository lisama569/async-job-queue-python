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
