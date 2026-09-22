"""Shared fixtures: isolated DB per test, controllable clock, caplog setup."""

from __future__ import annotations

import logging
import os
import tempfile

# Importing taskqueue.api builds a module-level default app; point its DB at a
# throwaway path *before* any test module imports it, so the test run never
# leaves a data/ directory in the repository.
os.environ.setdefault(
    "TASKQUEUE_DB_PATH", os.path.join(tempfile.gettempdir(), "taskqueue-pytest-default.db")
)

import pytest  # noqa: E402

from taskqueue.config import QueueConfig  # noqa: E402
from taskqueue.queue import TaskQueue  # noqa: E402
from taskqueue.storage import Storage  # noqa: E402


class FakeClock:
    """Deterministic millisecond-resolution clock for backoff/lease tests."""

    def __init__(self, start_ms: int = 1_000_000) -> None:
        self.now_ms = start_ms

    def __call__(self) -> float:
        return self.now_ms / 1000.0

    def bump(self, ms: int) -> None:
        self.now_ms += ms


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def queue(tmp_path, clock):
    cfg = QueueConfig(
        db_path=str(tmp_path / "queue.db"),
        lease_timeout_ms=30_000,
        max_attempts=4,
        backoff_base_ms=100,
        backoff_factor=2.0,
        backoff_max_ms=10_000,
        poll_batch_size=10,
    )
    q = TaskQueue(cfg, Storage(cfg.db_path), clock=clock)
    yield q
    q.close()


@pytest.fixture(autouse=True)
def _capture_decision_logs(caplog):
    """Ensure every test captures input/decision logs at INFO level."""
    caplog.set_level(logging.INFO, logger="taskqueue.consumer")
    yield
