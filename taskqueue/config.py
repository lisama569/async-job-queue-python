"""Queue configuration (env-overridable). All settings are local, no external services."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class QueueConfig:
    db_path: str = "data/queue.db"
    lease_timeout_ms: int = 30_000          # visibility timeout / lease lock
    max_attempts: int = 5                   # attempts incl. the first one
    backoff_base_ms: int = 100              # exponential backoff base
    backoff_factor: float = 2.0
    backoff_max_ms: int = 30_000
    poll_batch_size: int = 10
    reclaim_interval_ms: int = 5_000        # how often the reaper scans expired leases

    @staticmethod
    def from_env() -> QueueConfig:
        def _int(name: str, default: int) -> int:
            return int(os.environ.get(name, default))

        def _float(name: str, default: float) -> float:
            return float(os.environ.get(name, default))

        return QueueConfig(
            db_path=os.environ.get("TASKQUEUE_DB_PATH", QueueConfig.db_path),
            lease_timeout_ms=_int("TASKQUEUE_LEASE_TIMEOUT_MS", QueueConfig.lease_timeout_ms),
            max_attempts=_int("TASKQUEUE_MAX_ATTEMPTS", QueueConfig.max_attempts),
            backoff_base_ms=_int("TASKQUEUE_BACKOFF_BASE_MS", QueueConfig.backoff_base_ms),
            backoff_factor=_float("TASKQUEUE_BACKOFF_FACTOR", QueueConfig.backoff_factor),
            backoff_max_ms=_int("TASKQUEUE_BACKOFF_MAX_MS", QueueConfig.backoff_max_ms),
            poll_batch_size=_int("TASKQUEUE_POLL_BATCH_SIZE", QueueConfig.poll_batch_size),
            reclaim_interval_ms=_int("TASKQUEUE_RECLAIM_INTERVAL_MS", QueueConfig.reclaim_interval_ms),
        )
