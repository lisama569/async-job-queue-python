"""任务与投递的领域模型及状态机。"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class JobState(str, Enum):
    # 已入队、等待投递
    PENDING = "pending"
    # 已被某消费者占用、尚未确认
    INFLIGHT = "inflight"
    # 消费失败、等待退避后重新投递
    RETRYING = "retrying"
    # 终态：成功确认
    SUCCEEDED = "succeeded"
    # 终态：重试超过上限 / 占用超时耗尽
    FAILED = "failed"
    # 终态：入队即被判定为无法处理
    REJECTED = "rejected"


TERMINAL_STATES: frozenset[JobState] = frozenset(
    {JobState.SUCCEEDED, JobState.FAILED, JobState.REJECTED}
)

# 合法状态流转表
TRANSITIONS: dict[JobState, frozenset[JobState]] = {
    JobState.PENDING: frozenset(
        {JobState.INFLIGHT, JobState.REJECTED}
    ),
    JobState.INFLIGHT: frozenset(
        {JobState.SUCCEEDED, JobState.RETRYING, JobState.FAILED}
    ),
    JobState.RETRYING: frozenset({JobState.INFLIGHT, JobState.FAILED}),
    JobState.SUCCEEDED: frozenset(),
    JobState.FAILED: frozenset(),
    JobState.REJECTED: frozenset(),
}


def can_transition(src: JobState, dst: JobState) -> bool:
    return dst in TRANSITIONS[src]


@dataclass
class Job:
    id: str
    dedupe_key: str | None
    payload: dict[str, Any] = field(default_factory=dict)
    state: JobState = JobState.PENDING
    attempts: int = 0
    max_retries: int = 0
    available_at: float = 0.0
    last_error: str | None = None
    created_at: float = 0.0
    updated_at: float = 0.0

    @property
    def terminal(self) -> bool:
        return self.state in TERMINAL_STATES


class DeliveryState(str, Enum):
    # 占用中
    ACTIVE = "active"
    # 已成功确认
    ACKED = "acked"
    # 显式否定确认
    NACKED = "nacked"
    # 占用超时、被队列回收
    TIMED_OUT = "timed_out"


@dataclass
class Delivery:
    id: int
    job_id: str
    group: str
    consumer: str
    attempt: int
    delivered_at: float
    visible_after: float
    state: str


@dataclass
class GroupOffset:
    group: str
    offset: int


# 组内分发状态（group_jobs.status 的取值）
class GroupJobStatus(str, Enum):
    # 尚未投递给该组
    PENDING = "pending"
    # 该组有消费者占用中
    INFLIGHT = "inflight"
    # 该组内失败、退避等待重投
    RETRYING = "retrying"
    # 终态：该组已成功确认
    SUCCEEDED = "succeeded"
    # 终态：该组内重试耗尽
    FAILED = "failed"
    # 终态：该组判定不可处理
    REJECTED = "rejected"


TERMINAL_GROUP_STATUSES: frozenset[str] = frozenset(
    {
        GroupJobStatus.SUCCEEDED.value,
        GroupJobStatus.FAILED.value,
        GroupJobStatus.REJECTED.value,
    }
)


@dataclass
class GroupProgress:
    """单个消费者组的进度对账结果。"""

    group: str
    offset: int
    total_jobs: int
    # 各状态 -> 该状态下的任务 id 列表（按入队顺序）
    buckets: dict[str, list[str]] = field(default_factory=dict)

    def count(self, status: str) -> int:
        return len(self.buckets.get(status, []))

    @property
    def counts(self) -> dict[str, int]:
        return {status: len(ids) for status, ids in self.buckets.items()}


@dataclass
class CheckpointJob:
    """快照中收录的一条任务（让快照自足，可恢复进空队列数据）。"""

    id: str
    payload: dict[str, Any] = field(default_factory=dict)
    dedupe_key: str | None = None
    # 入队顺序由 seq 严格给出（对应导出库 jobs.rowid，从 1 开始）
    seq: int = 0
    created_at: float = 0.0
    max_retries: int = 0


@dataclass
class Checkpoint:
    """一个组的可持久化进度快照（可落盘为 JSON，供换进程恢复）。

    快照自足：除组级位点与终态结论外，还携带导出时队列中的全部任务
    （id / payload / 去重键 / 入队顺序）。因此即使恢复进一份完全空白的
    队列数据，该组也能照着快照接着消费。
    """

    group: str
    offset: int
    created_at: float
    # job_id -> 该组内的终态结论（仅收录终态任务）
    conclusions: dict[str, str] = field(default_factory=dict)
    # 导出时队列中的全部任务，按入队顺序排列
    jobs: list[CheckpointJob] = field(default_factory=list)
