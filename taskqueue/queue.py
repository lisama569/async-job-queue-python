"""TaskQueue: the queue facade — enqueue/deliver/ack/nack/reclaim/offset commit.

It owns the business rules; the Storage owns atomic SQLite operations. Outcomes
that cannot be accepted are turned into typed exceptions with a distinct
``RejectReason`` — nothing is dropped silently.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable

from .config import QueueConfig
from .exceptions import (
    AcknowledgeRejected,
    DeliveryRejected,
    EnqueueRejected,
    OffsetRejected,
)
from .models import (
    Delivery,
    GroupOffset,
    RejectReason,
    Task,
    TaskStatus,
    next_available_time,
)
from .offsets import validate_advance
from .storage import (
    ACKED,
    ALREADY_SUCCEEDED,
    NACKED,
    NOT_INFLIGHT,
    STALE_EPOCH,
    UNKNOWN,
    Storage,
)


class TaskQueue:
    def __init__(
        self,
        config: QueueConfig | None = None,
        storage: Storage | None = None,
        *,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self.config = config or QueueConfig.from_env()
        self.storage = storage or Storage(self.config.db_path)
        self._clock = clock or time.time

    def close(self) -> None:
        self.storage.close()

    def _now_ms(self) -> int:
        return int(self._clock() * 1000)

    # ---------------------------------------------------------- enqueue
    @staticmethod
    def _encode_payload(payload: dict | str) -> tuple[str, str | None]:
        """Return (canonical json, reject_reason). Only valid JSON objects are accepted."""
        if isinstance(payload, dict):
            return json.dumps(payload, ensure_ascii=False, sort_keys=True), None
        if isinstance(payload, str):
            text = payload.strip()
            if not text:
                return "", RejectReason.MALFORMED_PAYLOAD.value
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                return "", RejectReason.MALFORMED_PAYLOAD.value
            if not isinstance(parsed, dict):
                return "", RejectReason.MALFORMED_PAYLOAD.value
            return json.dumps(parsed, ensure_ascii=False, sort_keys=True), None
        return "", RejectReason.MALFORMED_PAYLOAD.value

    def enqueue(
        self, task_type: str, payload: dict | str, dedup_key: str | None = None
    ) -> Task:
        if not isinstance(task_type, str) or not task_type.strip():
            raise EnqueueRejected(
                RejectReason.MALFORMED_PAYLOAD, "task_type must be a non-empty string"
            )
        encoded, reject = self._encode_payload(payload)
        if reject is not None:
            raise EnqueueRejected(
                RejectReason.MALFORMED_PAYLOAD,
                f"payload must be a JSON object: {payload!r}",
            )
        if dedup_key is not None and (not isinstance(dedup_key, str) or not dedup_key.strip()):
            raise EnqueueRejected(
                RejectReason.MALFORMED_PAYLOAD, "dedup_key must be a non-empty string"
            )

        # Idempotent submit: a repeated dedup_key returns the stored task as-is,
        # so a producer retry never causes double execution.
        if dedup_key is not None:
            existing = self.storage.find_task_by_dedup_key(dedup_key)
            if existing is not None:
                return existing

        now = self._now_ms()
        tid = self.storage.insert_task(
            dedup_key=dedup_key,
            task_type=task_type.strip(),
            payload=encoded,
            status=TaskStatus.PENDING,
            terminal_reason=None,
            max_attempts=self.config.max_attempts,
            now_ms=now,
        )
        return self.storage.get_task(tid)  # type: ignore[return-value]

    # ---------------------------------------------------------- delivery
    def poll(
        self, group_name: str, consumer: str, max_messages: int | None = None
    ) -> list[Delivery]:
        if not group_name or not consumer:
            raise DeliveryRejected(
                RejectReason.MALFORMED_PAYLOAD, "group_name and consumer are required"
            )
        self.storage.ensure_group(group_name, self._now_ms())
        limit = max_messages or self.config.poll_batch_size
        now = self._now_ms()
        # Opportunistically return timed-out leases to the pool before leasing.
        self.storage.reclaim_expired(now, self.config.lease_timeout_ms)
        rows = self.storage.lease_available(
            limit=limit,
            now_ms=now,
            lease_timeout_ms=self.config.lease_timeout_ms,
            owner=f"{group_name}/{consumer}",
        )
        return [
            Delivery(
                task=t,
                group_name=group_name,
                epoch=t.epoch,
                lease_expires_at_ms=t.lease_expires_at_ms or 0,
            )
            for t in rows
        ]

    # ---------------------------------------------------------- ack / nack
    def acknowledge(self, task_id: int, epoch: int) -> Task:
        task, code = self.storage.ack_task(task_id, epoch, self._now_ms())
        if code == ACKED or code == ALREADY_SUCCEEDED:
            return task  # type: ignore[return-value]
        if code == UNKNOWN or task is None:
            raise AcknowledgeRejected(
                RejectReason.UNKNOWN_TASK, f"task {task_id} does not exist"
            )
        if code == NOT_INFLIGHT:
            raise AcknowledgeRejected(
                RejectReason.NOT_INFLIGHT,
                f"task {task_id} is {task.status.value}, only inflight tasks can be acked",
            )
        if code == STALE_EPOCH:
            raise AcknowledgeRejected(
                RejectReason.LEASE_EXPIRED,
                f"epoch {epoch} is stale (current={task.epoch}, owner={task.lease_owner}); "
                f"the lease was lost and the task redelivered",
            )
        raise AcknowledgeRejected(RejectReason.NOT_INFLIGHT, f"unexpected outcome {code}")

    def fail(self, task_id: int, epoch: int, reason: str, retry: bool = True) -> Task:
        if not reason or not reason.strip():
            raise AcknowledgeRejected(
                RejectReason.MALFORMED_PAYLOAD,
                "a failure reason is mandatory so failures stay distinguishable",
            )
        task = self.storage.get_task(task_id)
        if task is None:
            raise AcknowledgeRejected(
                RejectReason.UNKNOWN_TASK, f"task {task_id} does not exist"
            )
        # Attempt budget: the attempt currently holding the lease is `task.attempts`.
        budget_exhausted = task.attempts >= task.max_attempts
        if not retry or budget_exhausted:
            updated, code = self.storage.nack_task(
                task_id, epoch, reason, None, self._now_ms(), failed=True
            )
        else:
            retry_at = next_available_time(
                self._now_ms(), task.attempts, self.config
            )
            updated, code = self.storage.nack_task(
                task_id, epoch, reason, retry_at, self._now_ms(), failed=False
            )
        if code == NACKED:
            return updated  # type: ignore[return-value]
        if code == UNKNOWN or updated is None:
            raise AcknowledgeRejected(
                RejectReason.UNKNOWN_TASK, f"task {task_id} does not exist"
            )
        if code == NOT_INFLIGHT:
            raise AcknowledgeRejected(
                RejectReason.NOT_INFLIGHT,
                f"task {task_id} is {updated.status.value}, cannot fail a non-inflight task",
            )
        if code == STALE_EPOCH:
            raise AcknowledgeRejected(
                RejectReason.LEASE_EXPIRED,
                f"epoch {epoch} is stale (current={updated.epoch}); the lease was lost",
            )
        raise AcknowledgeRejected(RejectReason.NOT_INFLIGHT, f"unexpected outcome {code}")

    def reclaim_expired_leases(self) -> int:
        return self.storage.reclaim_expired(
            self._now_ms(), self.config.lease_timeout_ms
        )

    # ---------------------------------------------------------- queries
    def get_task(self, task_id: int) -> Task:
        task = self.storage.get_task(task_id)
        if task is None:
            raise AcknowledgeRejected(
                RejectReason.UNKNOWN_TASK, f"task {task_id} does not exist"
            )
        return task

    def status_counts(self) -> dict[str, int]:
        return self.storage.count_tasks_by_status()

    # ---------------------------------------------------------- groups
    def create_group(self, name: str) -> GroupOffset:
        if not name or not name.strip():
            raise EnqueueRejected(
                RejectReason.MALFORMED_PAYLOAD, "group name must be a non-empty string"
            )
        return self.storage.ensure_group(name.strip(), self._now_ms())

    def commit_offset(self, group_name: str, offset: int) -> GroupOffset:
        if not isinstance(offset, int) or offset < 0:
            raise OffsetRejected(
                RejectReason.MALFORMED_PAYLOAD, f"offset must be a non-negative int"
            )
        group = self.storage.get_group(group_name)
        if group is None:
            raise OffsetRejected(
                RejectReason.UNKNOWN_GROUP, f"consumer group {group_name!r} does not exist"
            )
        frontier = self.storage.max_task_id()
        task = self.storage.get_task(offset) if offset > 0 else None
        validate_advance(
            current=group.committed_offset,
            requested=offset,
            frontier=frontier,
            tasks_by_id={offset: task} if task is not None else {},
        )
        now = self._now_ms()
        committed = self.storage.advance_offset(group_name, offset, now)
        if not committed:
            # Concurrent committer won; re-read and report the true state.
            fresh = self.storage.get_group(group_name)
            raise OffsetRejected(
                RejectReason.OFFSET_ROLLBACK,
                f"concurrent commit moved the offset to {fresh.committed_offset}; "
                f"requested {offset} is not ahead of it",
            )
        return self.storage.get_group(group_name)  # type: ignore[return-value]
