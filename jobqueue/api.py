"""FastAPI 路由与异常映射。

端点一览：
- POST   /jobs                     入队（dedupe_key 去重）
- GET    /jobs                     查询任务列表（可按 state 过滤）
- GET    /jobs/{job_id}            查询单个任务（含失败原因、投递历史）
- POST   /groups                   注册消费者组
- GET    /groups                   列出消费者组
- GET    /groups/{group}/offset    查询位点
- POST   /groups/{group}/offset    提交位点（严格前进、不越界、不跳过未定论任务）
- GET    /groups/{group}/progress  导出该组进度对账（位点 + 五类任务桶 + 安全位点）
- GET    /groups/{group}/jobs      列出该组每个任务的组内定论
- GET    /groups/{group}/checkpoint 导出该组检查点快照（JSON）
- POST   /groups/{group}/checkpoint 依据快照恢复该组进度（换进程续消费）
- POST   /claim                    占用一条任务
- POST   /deliveries/{id}/ack      确认成功
- POST   /deliveries/{id}/nack     否定确认（reason 必填；reject=true 立即拒绝）
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from jobqueue.config import Settings
from jobqueue.errors import ErrorCode, QueueError
from jobqueue.models import Delivery, GroupOffset, GroupProgress, Job
from jobqueue.queue import JobQueue

router = APIRouter()

# 业务错误码 -> HTTP 状态码
_STATUS: dict[ErrorCode, int] = {
    ErrorCode.INVALID_PAYLOAD: 400,
    ErrorCode.DUPLICATE_JOB: 409,
    ErrorCode.EMPTY_QUEUE: 404,
    ErrorCode.DELIVERY_NOT_FOUND: 404,
    ErrorCode.JOB_NOT_FOUND: 404,
    ErrorCode.GROUP_NOT_FOUND: 404,
    ErrorCode.DELIVERY_TERMINAL: 409,
    ErrorCode.DELIVERY_EXPIRED: 409,
    ErrorCode.DELIVERY_OWNER_MISMATCH: 409,
    ErrorCode.RETRIES_EXHAUSTED: 409,
    ErrorCode.UNPROCESSABLE_JOB: 422,
    ErrorCode.OFFSET_ROLLBACK: 409,
    ErrorCode.OFFSET_OUT_OF_RANGE: 409,
    ErrorCode.OFFSET_GAP: 409,
    ErrorCode.CHECKPOINT_INVALID: 400,
}


class EnqueueBody(BaseModel):
    payload: dict[str, Any]
    dedupe_key: str | None = None


class GroupBody(BaseModel):
    group: str = Field(min_length=1)


class OffsetBody(BaseModel):
    offset: int = Field(ge=0)


class ClaimBody(BaseModel):
    group: str = Field(min_length=1)
    consumer: str = Field(min_length=1)
    visibility_timeout: float | None = Field(default=None, gt=0)


class AckBody(BaseModel):
    consumer: str = Field(min_length=1)


class NackBody(BaseModel):
    consumer: str = Field(min_length=1)
    reason: str = Field(min_length=1)
    reject: bool = False


def _job_dict(job: Job) -> dict[str, Any]:
    return {
        "id": job.id,
        "dedupe_key": job.dedupe_key,
        "payload": job.payload,
        "state": job.state.value,
        "attempts": job.attempts,
        "max_retries": job.max_retries,
        "available_at": job.available_at,
        "last_error": job.last_error,
        "terminal": job.terminal,
        "created_at": job.created_at,
        "updated_at": job.updated_at,
    }


def _delivery_dict(d: Delivery) -> dict[str, Any]:
    return {
        "id": d.id,
        "job_id": d.job_id,
        "group": d.group,
        "consumer": d.consumer,
        "attempt": d.attempt,
        "delivered_at": d.delivered_at,
        "visible_after": d.visible_after,
        "state": d.state,
    }


def _group_job_view_dict(v: Any) -> dict[str, Any]:
    return {
        "job_id": v.job_id,
        "group": v.group,
        "status": v.status.value,
        "attempts": v.attempts,
        "delivery_id": v.delivery_id,
        "terminal": v.status.value in ("succeeded", "failed", "rejected"),
    }


def _progress_dict(p: GroupProgress) -> dict[str, Any]:
    return {
        "group": p.group,
        "offset": p.offset,
        "safe_offset": p.safe_offset,
        "total": p.total,
        "counts": {
            "succeeded": len(p.succeeded),
            "failed": len(p.failed),
            "rejected": len(p.rejected),
            "inflight": len(p.inflight),
            "retrying": len(p.retrying),
            "pending": len(p.pending),
        },
        "jobs": {
            "succeeded": p.succeeded,
            "failed": p.failed,
            "rejected": p.rejected,
            "inflight": p.inflight,
            "retrying": p.retrying,
            "pending": p.pending,
        },
    }


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    app = FastAPI(title="local-async-job-queue", version="0.1.0")
    app.state.settings = settings
    app.state.queue = JobQueue(settings)

    @app.exception_handler(QueueError)
    async def _on_queue_error(_: Request, exc: QueueError) -> JSONResponse:
        return JSONResponse(
            status_code=_STATUS.get(exc.code, 400),
            content={"error": {"code": exc.code.value, "message": exc.message}},
        )

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/jobs", status_code=201)
    def enqueue(body: EnqueueBody) -> dict[str, Any]:
        job = app.state.queue.enqueue(body.payload, body.dedupe_key)
        return _job_dict(job)

    @app.get("/jobs")
    def list_jobs(state: str | None = None) -> dict[str, Any]:
        jobs = app.state.queue.list_jobs(state)
        return {"jobs": [_job_dict(j) for j in jobs]}

    @app.get("/jobs/{job_id}")
    def get_job(job_id: str) -> dict[str, Any]:
        job = app.state.queue.get_job(job_id)
        deliveries = app.state.queue.list_deliveries(job_id)
        group_views = app.state.queue.get_job_group_views(job_id)
        return {
            **_job_dict(job),
            "deliveries": [_delivery_dict(d) for d in deliveries],
            "group_views": [_group_job_view_dict(v) for v in group_views],
        }

    @app.post("/groups", status_code=201)
    def create_group(body: GroupBody) -> dict[str, str]:
        app.state.queue.create_group(body.group)
        return {"group": body.group}

    @app.get("/groups")
    def list_groups() -> dict[str, Any]:
        return {"groups": app.state.queue.list_groups()}

    @app.get("/groups/{group}/offset")
    def get_offset(group: str) -> dict[str, Any]:
        offset: GroupOffset | None = app.state.queue.get_offset(group)
        if offset is None:
            raise QueueError(
                ErrorCode.GROUP_NOT_FOUND, f"消费者组 {group!r} 不存在"
            )
        return {"group": offset.group, "offset": offset.offset}

    @app.post("/groups/{group}/offset")
    def commit_offset(group: str, body: OffsetBody) -> dict[str, Any]:
        offset = app.state.queue.commit_offset(group, body.offset)
        return {"group": offset.group, "offset": offset.offset}

    @app.get("/groups/{group}/progress")
    def group_progress(group: str) -> dict[str, Any]:
        return _progress_dict(app.state.queue.group_progress(group))

    @app.get("/groups/{group}/jobs")
    def list_group_jobs(group: str) -> dict[str, Any]:
        views = app.state.queue.list_group_jobs(group)
        return {"group": group, "jobs": [_group_job_view_dict(v) for v in views]}

    @app.get("/groups/{group}/checkpoint")
    def export_checkpoint(group: str) -> dict[str, Any]:
        checkpoint = app.state.queue.export_checkpoint(group)
        return app.state.queue.checkpoint_to_dict(checkpoint)

    @app.post("/groups/{group}/checkpoint")
    def restore_checkpoint(group: str, body: dict[str, Any]) -> dict[str, Any]:
        # 路径 group 必须与快照内 group 一致，避免把 A 组结论恢复进 B 组
        if body.get("group") != group:
            raise QueueError(
                ErrorCode.CHECKPOINT_INVALID,
                f"路径组 {group!r} 与检查点所属组 {body.get('group')!r} 不一致",
            )
        progress = app.state.queue.restore_checkpoint_dict(body)
        return _progress_dict(progress)

    @app.post("/claim")
    def claim(body: ClaimBody) -> JSONResponse:
        delivery = app.state.queue.claim(
            body.group, body.consumer, body.visibility_timeout
        )
        if delivery is None:
            return JSONResponse(
                status_code=204,
                content={"error": {
                    "code": ErrorCode.EMPTY_QUEUE.value,
                    "message": f"组 {body.group!r} 当前没有可投递的任务",
                }},
            )
        job = app.state.queue.find_job(delivery.job_id)
        assert job is not None
        return JSONResponse(
            status_code=200,
            content={"delivery": _delivery_dict(delivery), "job": _job_dict(job)},
        )

    @app.post("/deliveries/{delivery_id}/ack")
    def ack(delivery_id: int, body: AckBody) -> dict[str, Any]:
        job = app.state.queue.ack(delivery_id, body.consumer)
        return _job_dict(job)

    @app.post("/deliveries/{delivery_id}/nack")
    def nack(delivery_id: int, body: NackBody) -> dict[str, Any]:
        job = app.state.queue.nack(
            delivery_id, body.consumer, body.reason, unprocessable=body.reject
        )
        return _job_dict(job)

    app.include_router(router)
    return app
