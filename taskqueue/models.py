"""Domain types: task status, delivery attempt, offsets."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from .config import QueueConfig


class TaskStatus(str, Enum):
    PENDING = "pending"        # enqueued, never delivered
    INFLIGHT = "inflight"      # leased by exactly one consumer
    SUCCEEDED = "succeeded"    # terminal
    FAILED = "failed"          # terminal: retries exhausted (business failure)
    REJECTED = "rejected"      # terminal: rejected at enqueue (poison/malformed)


# Machine-checkable reason codes so failures are distinguishable, never silent.
class RejectReason(str, Enum):
    MALFORMED_PAYLOAD = "MALFORMED_PAYLOAD"          # 400 at enqueue
    EMPTY_QUEUE = "EMPTY_QUEUE"                      # poll found nothing
    NOT_INFLIGHT = "NOT_INFLIGHT"                    # ack/nack on non-inflight task
    LEASE_EXPIRED = "LEASE_EXPIRED"                  # stale epoch / lost lease
    UNKNOWN_TASK = "UNKNOWN_TASK"
    UNKNOWN_GROUP = "UNKNOWN_GROUP"
    OFFSET_ROLLBACK = "OFFSET_ROLLBACK"              # new_offset < committed
    OFFSET_SKIP = "OFFSET_SKIP"                      # gap: must advance exactly +1
    OFFSET_OVERRUN = "OFFSET_OVERRUN"                # beyond the log frontier
    OFFSET_NOT_TERMINAL = "OFFSET_NOT_TERMINAL"      # cannot commit past unfinished task


@dataclass
class Task:
    id: int
    dedup_key: str | None
    task_type: str
    payload: str
    status: TaskStatus
    attempts: int
    max_attempts: int
    epoch: int
    lease_owner: str | None
    lease_expires_at_ms: int | None
    available_at_ms: int
    created_at_ms: int
    updated_at_ms: int
    terminal_reason: str | None = None
    last_error: str | None = None

    def is_terminal(self) -> bool:
        return self.status in (TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.REJECTED)


@dataclass
class GroupOffset:
    group_name: str
    committed_offset: int          # offset of the last committed log entry (0 = nothing)
    created_at_ms: int
    updated_at_ms: int


@dataclass
class Delivery:
    """A leased task handed to a consumer; `epoch` is the fencing token."""
    task: Task
    group_name: str
    epoch: int
    lease_expires_at_ms: int


def next_available_time(now_ms: int, attempt: int, cfg: QueueConfig) -> int:
    """Exponential backoff with a cap.

    `attempt` is the 1-based ordinal of the attempt that just failed, so after
    the first failure the delay is ``backoff_base_ms``, then base*factor, ...
    """
    if attempt < 1:
        attempt = 1
    raw = cfg.backoff_base_ms * (cfg.backoff_factor ** (attempt - 1))
    return now_ms + int(min(cfg.backoff_max_ms, raw))
