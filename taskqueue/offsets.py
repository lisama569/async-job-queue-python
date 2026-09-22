"""Pure offset rules. Offsets only advance, exactly one slot at a time,
and never past a task that has not reached a terminal state."""

from __future__ import annotations

from .exceptions import OffsetRejected
from .models import RejectReason, Task


def validate_advance(
    current: int,
    requested: int,
    frontier: int,
    tasks_by_id: dict[int, Task],
) -> None:
    """Raise OffsetRejected unless `requested` is exactly current+1, within the
    log frontier, and the task at that offset is terminal.

    Offsets start at 0 ("nothing committed"); task ids (log sequence numbers)
    start at 1. Therefore the first legal advance is 0 -> 1.
    """
    if requested < current:
        raise OffsetRejected(
            RejectReason.OFFSET_ROLLBACK,
            f"committed={current} requested={requested}: offsets never move backwards",
        )
    if requested == current:
        # Re-committing the same position is also a (harmless) form of rollback;
        # refuse it explicitly so callers cannot pretend progress happened.
        raise OffsetRejected(
            RejectReason.OFFSET_ROLLBACK,
            f"requested={requested} already committed",
        )
    if requested > frontier:
        raise OffsetRejected(
            RejectReason.OFFSET_OVERRUN,
            f"requested={requested} beyond log frontier={frontier}",
        )
    if requested > current + 1:
        raise OffsetRejected(
            RejectReason.OFFSET_SKIP,
            f"committed={current} requested={requested}: must advance exactly one slot "
            f"to {current + 1}, skipping is not allowed",
        )
    task = tasks_by_id.get(requested)
    if task is None or not task.is_terminal():
        state = "missing" if task is None else task.status.value
        raise OffsetRejected(
            RejectReason.OFFSET_NOT_TERMINAL,
            f"task at offset {requested} is {state}: cannot commit past it",
        )
