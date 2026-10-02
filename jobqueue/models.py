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


class GroupStatus(str, Enum):
    """某个任务在某个消费者组内的分发结论（group_jobs.status）。"""

    # 尚未投递（含从未投递）
    PENDING = "pending"
    # 已投递、被消费者占用，尚无最终结论
    INFLIGHT = "inflight"
    # 投递失败、退避等待重投
    RETRYING = "retrying"
    # 终态：成功
    SUCCEEDED = "succeeded"
    # 终态：失败（重试耗尽 / 超时耗尽）
    FAILED = "failed"
    # 终态：显式拒绝（不可处理）
    REJECTED = "rejected"


GROUP_TERMINAL_STATUSES: frozenset[GroupStatus] = frozenset(
    {GroupStatus.SUCCEEDED, GroupStatus.FAILED, GroupStatus.REJECTED}
)


@dataclass
class GroupJobView:
    """单个任务在单个消费者组内的处理视图。"""

    job_id: str
    group: str
    status: GroupStatus
    attempts: int
    delivery_id: int | None


@dataclass
class GroupProgress:
    """一个消费者组的进度对账结果。

    ``offset`` 为已提交位点；``safe_offset`` 为按入队顺序计算出的
    「连续定论前缀」长度，也就是当前允许提交到的最大位点。
    五类任务桶按组内账本划分：retrying 单独成桶（运维对账用），
    不计入 succeeded/failed/rejected/inflight/pending 的任何一类。
    """

    group: str
    offset: int
    safe_offset: int
    total: int
    succeeded: list[str]
    failed: list[str]
    rejected: list[str]
    inflight: list[str]
    retrying: list[str]
    pending: list[str]


@dataclass
class CheckpointJob:
    """检查点中单个任务的定论快照。"""

    job_id: str
    dedupe_key: str | None
    payload: dict[str, Any]
    # 该任务在本组的定论：succeeded/failed/rejected；
    # 非终态任务也会带上（inflight 会被规范化为 pending 以便重投）
    status: GroupStatus
    attempts: int
    max_retries: int
    # retrying 任务退避到期时间；其它情况为 0
    available_at: float
    created_at: float


@dataclass
class GroupCheckpoint:
    """一个消费者组的可持久化检查点。"""

    version: int
    group: str
    offset: int
    created_at: float
    jobs: list[CheckpointJob]

