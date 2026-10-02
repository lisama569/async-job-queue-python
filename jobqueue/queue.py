"""对外服务门面：封装存储与策略（退避、校验等）。

职责：
- 构造/持有 JobStore（SQLite 即本地消息代理）；
- 入队去重：相同 dedupe_key 返回 DUPLICATE_JOB，调用方可据此取回既有任务；
- 投递 / 确认 / 否定确认（失败原因必填、不可处理任务显式拒绝）；
- 消费者组与位点的只读/校验式提交。
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from jobqueue.checkpoint import (
    load_checkpoint_file,
    save_checkpoint_file,
)
from jobqueue.config import Settings
from jobqueue.errors import ErrorCode, QueueError
from jobqueue.models import (
    Checkpoint,
    Delivery,
    GroupOffset,
    GroupProgress,
    Job,
)
from jobqueue.store import JobStore


class JobQueue:
    def __init__(
        self,
        settings: Settings,
        store: JobStore | None = None,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self.settings = settings
        self.store = store if store is not None else JobStore(settings, clock=clock)
        self._owns_store = store is None

    def enqueue(self, payload: dict, dedupe_key: str | None = None) -> Job:
        return self.store.enqueue(payload, dedupe_key)

    def get_existing_by_dedupe_key(self, dedupe_key: str) -> Job | None:
        return self.store.get_job_by_dedupe_key(dedupe_key)

    def claim(
        self, group: str, consumer: str, visibility_timeout: float | None = None
    ) -> Delivery | None:
        return self.store.claim(group, consumer, visibility_timeout)

    def ack(self, delivery_id: int, consumer: str) -> Job:
        return self.store.ack(delivery_id, consumer)

    def nack(
        self,
        delivery_id: int,
        consumer: str,
        reason: str,
        *,
        unprocessable: bool = False,
    ) -> Job:
        return self.store.nack(
            delivery_id, consumer, reason, unprocessable=unprocessable
        )

    def get_job(self, job_id: str) -> Job:
        job = self.store.get_job(job_id)
        if job is None:
            raise QueueError(
                ErrorCode.JOB_NOT_FOUND, f"任务 id={job_id} 不存在"
            )
        return job

    def find_job(self, job_id: str) -> Job | None:
        return self.store.get_job(job_id)

    def list_jobs(self, state: str | None = None) -> list[Job]:
        return self.store.list_jobs(state)

    def list_deliveries(self, job_id: str) -> list[Delivery]:
        return self.store.list_deliveries(job_id)

    def create_group(self, group: str) -> None:
        self.store.create_group(group)

    def list_groups(self) -> list[str]:
        return self.store.list_groups()

    def commit_offset(self, group: str, new_offset: int) -> GroupOffset:
        return self.store.commit_offset(group, new_offset)

    def get_offset(self, group: str) -> GroupOffset | None:
        return self.store.get_offset(group)

    # ---------- 进度对账 ----------

    def group_progress(self, group: str) -> GroupProgress:
        """导出某组的进度对账结果（位点 + 各状态任务清单）。"""
        return self.store.group_progress(group)

    def job_group_statuses(self, job_id: str) -> dict[str, str]:
        """一个任务在各消费者组内的处理结论。"""
        return self.store.job_group_statuses(job_id)

    # ---------- 检查点 ----------

    def export_checkpoint(
        self, group: str, path: str | Path | None = None
    ) -> Checkpoint:
        """导出某组的进度快照；给定 path 时同时持久化为 JSON 文件。"""
        cp = self.store.export_checkpoint(group)
        if path is not None:
            save_checkpoint_file(cp, path)
        return cp

    def restore_checkpoint(
        self, source: str | Path | Checkpoint, group: str | None = None
    ) -> GroupOffset:
        """从快照（文件路径或 Checkpoint 对象）恢复某组进度。"""
        cp = (
            load_checkpoint_file(source)
            if isinstance(source, (str, Path))
            else source
        )
        return self.store.restore_checkpoint(cp, group)

    def close(self) -> None:
        if self._owns_store:
            self.store.close()
