"""pytest 夹具：每个测试使用独立的临时数据库，测试间完全隔离。"""

from __future__ import annotations

import logging

import pytest

from jobqueue.config import Settings
from jobqueue.queue import JobQueue


@pytest.fixture
def settings(tmp_path) -> Settings:
    return Settings(
        db_path=str(tmp_path / "queue.db"),
        visibility_timeout=0.2,
        max_retries=3,
        base_backoff=0.01,
        backoff_multiplier=2.0,
        backoff_cap=1.0,
        jitter=0.0,
        poll_interval=0.01,
    )


@pytest.fixture
def queue(settings: Settings) -> JobQueue:
    q = JobQueue(settings)
    yield q
    q.close()


@pytest.fixture(autouse=True)
def _caplog_level(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO, logger="jobqueue.worker")
