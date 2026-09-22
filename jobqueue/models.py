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
