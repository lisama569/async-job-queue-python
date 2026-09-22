"""Local consumer loop: polls a group, runs registered handlers, acks/nacks.

Failures are classified into distinguishable reason codes (never silent):

* ``NO_HANDLER:<type>``  – no handler registered for this task type
* ``DECODE_ERROR``       – stored payload could not be decoded (poison message)
* ``HANDLER_REJECTED``   – handler raised :class:`PermanentTaskError` (no retry)
* ``HANDLER_ERROR:<exc>``– any other handler exception (retry with backoff)
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from dataclasses import dataclass

from .exceptions import AcknowledgeRejected
from .models import Delivery, Task
from .queue import TaskQueue

logger = logging.getLogger("taskqueue.consumer")

# A handler takes the decoded payload dict and returns an arbitrary result.
Handler = Callable[[dict], str]


class PermanentTaskError(Exception):
    """Raised by a handler to declare a non-retryable business failure."""


@dataclass
class ConsumeResult:
    processed: int = 0
    succeeded: list[int] = None  # type: ignore[assignment]
    failed: list[tuple[int, str]] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.succeeded is None:
            self.succeeded = []
        if self.failed is None:
            self.failed = []


class Consumer:
    def __init__(
        self,
        queue: TaskQueue,
        group_name: str,
        consumer_name: str,
        handlers: dict[str, Handler] | None = None,
    ) -> None:
        self.queue = queue
        self.group_name = group_name
        self.consumer_name = consumer_name
        self.handlers: dict[str, Handler] = dict(handlers or {})

    def register(self, task_type: str, handler: Handler) -> None:
        self.handlers[task_type] = handler

    def process_once(self) -> ConsumeResult:
        """Poll one batch and handle synchronously; returns a ConsumeResult."""
        deliveries = self.queue.poll(self.group_name, self.consumer_name)
        result = ConsumeResult(processed=len(deliveries))
        if not deliveries:
            logger.info(
        'input group=%s consumer=%s deliveries=0 decision=NOOP reason=EMPTY_QUEUE',
                self.group_name, self.consumer_name,
            )
            return result

        for delivery in deliveries:
            task = delivery.task
            logger.info(
                'input group=%s consumer=%s task=%d type=%s epoch=%d payload=%s',
                self.group_name, self.consumer_name, task.id, task.task_type,
                delivery.epoch, task.payload,
            )
            self._handle_one(delivery, result)
        return result

    # ------------------------------------------------------------------
    def _handle_one(self, delivery: Delivery, result: ConsumeResult) -> None:
        task = delivery.task
        try:
            reason, retry, output = self._dispatch(task)
        except Exception as exc:  # defensive: dispatch itself must never crash loop
            reason, retry, output = f"DISPATCH_ERROR:{type(exc).__name__}", True, ""

        if reason is None:
            try:
                done = self.queue.acknowledge(task.id, delivery.epoch)
                result.succeeded.append(task.id)
                logger.info(
                    'decision=ACK task=%d epoch=%d status=%s output=%r',
                    task.id, delivery.epoch, done.status.value, output,
                )
            except AcknowledgeRejected as exc:
                # Lease lost / redelivered to someone else: do not execute side
                # effects twice; record and move on.
                result.failed.append((task.id, exc.reason.value))
                logger.warning(
                    'decision=ACK_REJECTED task=%d epoch=%d reason=%s detail=%s',
                    task.id, delivery.epoch, exc.reason.value, exc.detail,
                )
            return

        try:
            updated = self.queue.fail(task.id, delivery.epoch, reason, retry=retry)
            terminal = "TERMINAL" if updated.is_terminal() else "RETRY_SCHEDULED"
            result.failed.append((task.id, reason))
            logger.warning(
                'decision=NACK task=%d epoch=%d reason=%s retry=%s -> %s attempts=%d/%d',
                task.id, delivery.epoch, reason, retry, terminal,
                updated.attempts, updated.max_attempts,
            )
        except AcknowledgeRejected as exc:
            result.failed.append((task.id, exc.reason.value))
            logger.warning(
                'decision=NACK_REJECTED task=%d epoch=%d reason=%s detail=%s',
                task.id, delivery.epoch, exc.reason.value, exc.detail,
            )

    def _dispatch(self, task: Task) -> tuple[str | None, bool, str]:
        """Return (failure_reason or None, retry, output)."""
        handler = self.handlers.get(task.task_type)
        if handler is None:
            # Unknown task types are non-retryable configuration errors.
            return f"NO_HANDLER:{task.task_type}", False, ""
        try:
            payload = json.loads(task.payload)
        except json.JSONDecodeError as exc:
            return f"DECODE_ERROR:{exc.msg}", False, ""
        if not isinstance(payload, dict):
            return "DECODE_ERROR:payload is not a JSON object", False, ""
        try:
            output = handler(payload)
            return None, False, str(output)
        except PermanentTaskError as exc:
            return f"HANDLER_REJECTED:{exc}", False, ""
        except Exception as exc:  # transient: backoff + retry
            return f"HANDLER_ERROR:{type(exc).__name__}:{exc}", True, ""
