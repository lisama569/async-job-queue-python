"""FastAPI app exposing the queue over HTTP.

All rejections return HTTP 409 (or 400 for malformed input / 404 for unknown
resources) with a body ``{"error": "<REASON_CODE>", "detail": ...}`` so callers
can distinguish *why* something was refused.
"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .config import QueueConfig
from .exceptions import (
    AcknowledgeRejected,
    DeliveryRejected,
    EnqueueRejected,
    OffsetRejected,
    QueueError,
)
from .models import RejectReason, TaskStatus
from .queue import TaskQueue

_BAD_REQUEST = {
    RejectReason.MALFORMED_PAYLOAD.value,
}
_NOT_FOUND = {
    RejectReason.UNKNOWN_TASK.value,
    RejectReason.UNKNOWN_GROUP.value,
}


class EnqueueRequest(BaseModel):
    task_type: str = Field(min_length=1)
    payload: dict[str, Any]
    dedup_key: str | None = None


class PollRequest(BaseModel):
    group: str = Field(min_length=1)
    consumer: str = Field(min_length=1)
    max_messages: int | None = Field(default=None, ge=1, le=100)


class AckRequest(BaseModel):
    epoch: int = Field(ge=0)


class FailRequest(BaseModel):
    epoch: int = Field(ge=0)
    reason: str = Field(min_length=1)
    retry: bool = True


class CommitRequest(BaseModel):
    offset: int = Field(ge=0)


def _task_dict(task) -> dict[str, Any]:
    return {
        "id": task.id,
        "dedup_key": task.dedup_key,
        "task_type": task.task_type,
        "payload": task.payload,
        "status": task.status.value,
        "attempts": task.attempts,
        "max_attempts": task.max_attempts,
        "epoch": task.epoch,
        "lease_owner": task.lease_owner,
        "available_at_ms": task.available_at_ms,
        "terminal_reason": task.terminal_reason,
        "last_error": task.last_error,
    }


def create_app(config: QueueConfig | None = None, queue: TaskQueue | None = None) -> FastAPI:
    cfg = config or QueueConfig.from_env()
    task_queue = queue or TaskQueue(cfg)
    app = FastAPI(title="Local Async Task Queue", version="0.1.0")
    app.state.queue = task_queue

    @app.exception_handler(QueueError)
    async def _on_queue_error(_request, exc: QueueError) -> JSONResponse:
        code = exc.reason.value
        status = 400 if code in _BAD_REQUEST else 404 if code in _NOT_FOUND else 409
        return JSONResponse(
            status_code=status,
            content={"error": code, "detail": exc.detail},
        )

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/tasks", status_code=201)
    def enqueue(req: EnqueueRequest) -> JSONResponse:
        # Idempotent submit: a repeated dedup_key returns 200 with the stored
        # task; a fresh submit returns 201. The `deduplicated` flag (and the
        # status code) make the two outcomes distinguishable.
        existing = (
            task_queue.storage.find_task_by_dedup_key(req.dedup_key)
            if req.dedup_key
            else None
        )
        task = task_queue.enqueue(req.task_type, req.payload, req.dedup_key)
        body = _task_dict(task)
        body["deduplicated"] = existing is not None
        return JSONResponse(status_code=200 if existing is not None else 201, content=body)

    @app.post("/tasks/poll")
    def poll(req: PollRequest) -> dict[str, Any]:
        deliveries = task_queue.poll(req.group, req.consumer, req.max_messages)
        return {
            "messages": [
                {
                    "task": _task_dict(d.task),
                    "group": d.group_name,
                    "epoch": d.epoch,
                    "lease_expires_at_ms": d.lease_expires_at_ms,
                }
                for d in deliveries
            ],
            "count": len(deliveries),
        }

    @app.post("/tasks/{task_id}/ack")
    def acknowledge(task_id: int, req: AckRequest) -> dict[str, Any]:
        return _task_dict(task_queue.acknowledge(task_id, req.epoch))

    @app.post("/tasks/{task_id}/fail")
    def fail(task_id: int, req: FailRequest) -> dict[str, Any]:
        task = task_queue.fail(task_id, req.epoch, req.reason, retry=req.retry)
        body = _task_dict(task)
        body["terminal"] = task.is_terminal()
        return body

    @app.post("/reclaim")
    def reclaim() -> dict[str, int]:
        return {"reclaimed": task_queue.reclaim_expired_leases()}

    @app.get("/tasks/{task_id}")
    def get_task(task_id: int) -> dict[str, Any]:
        return _task_dict(task_queue.get_task(task_id))

    @app.get("/status")
    def status() -> dict[str, Any]:
        counts = task_queue.status_counts()
        return {
            "counts": {s.value: counts.get(s.value, 0) for s in TaskStatus},
            "total": sum(counts.values()),
        }

    @app.put("/groups/{name}")
    def create_group(name: str) -> dict[str, Any]:
        g = task_queue.create_group(name)
        return {"group": g.group_name, "committed_offset": g.committed_offset}

    @app.get("/groups/{name}")
    def get_group(name: str) -> dict[str, Any]:
        g = task_queue.storage.get_group(name)
        if g is None:
            raise DeliveryRejected(RejectReason.UNKNOWN_GROUP, f"group {name!r} not found")
        return {"group": g.group_name, "committed_offset": g.committed_offset}

    @app.post("/groups/{name}/commit")
    def commit_offset(name: str, req: CommitRequest) -> dict[str, Any]:
        g = task_queue.commit_offset(name, req.offset)
        return {"group": g.group_name, "committed_offset": g.committed_offset}

    return app


app = create_app()
