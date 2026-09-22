"""Distinguishable error types. Nothing is dropped silently."""

from __future__ import annotations

from .models import RejectReason


class QueueError(Exception):
    reason: RejectReason

    def __init__(self, reason: RejectReason, detail: str = ""):
        self.reason = reason
        self.detail = detail
        super().__init__(f"{reason.value}: {detail}" if detail else reason.value)


class EnqueueRejected(QueueError):
    """The submit was invalid (e.g. malformed payload). Task is not accepted."""


class DeliveryRejected(QueueError):
    """Poll cannot deliver (empty queue is reported distinctly)."""


class AcknowledgeRejected(QueueError):
    """ack/nack refused: not inflight, unknown, or stale lease epoch."""


class OffsetRejected(QueueError):
    """Offset rollback / skip / overrun / committing past non-terminal task."""
