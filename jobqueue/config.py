"""队列运行配置（可通过环境变量覆盖）。"""

from __future__ import annotations

import os
import random
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    # SQLite 数据库路径，:memory: 仅用于测试
    db_path: str = "queue.db"
    # 占用（可见性）超时：投递后多久未确认即视为占用超时，可被重新投递
    visibility_timeout: float = 30.0
    # 最大重试次数（超过后任务进入 failed 终态）
    max_retries: int = 3
    # 指数退避：base * multiplier**(attempt-1)，封顶 backoff_cap
    base_backoff: float = 1.0
    backoff_multiplier: float = 2.0
    backoff_cap: float = 60.0
    # 随机抖动上限（秒），0 表示关闭抖动
    jitter: float = 0.0
    # 消费轮询间隔（worker 使用）
    poll_interval: float = 0.1

    @staticmethod
    def from_env() -> "Settings":
        def _f(name: str, default: float) -> float:
            raw = os.environ.get(name)
            return float(raw) if raw is not None else default

        def _i(name: str, default: int) -> int:
            raw = os.environ.get(name)
            return int(raw) if raw is not None else default

        return Settings(
            db_path=os.environ.get("JOBQUEUE_DB_PATH", "queue.db"),
            visibility_timeout=_f("JOBQUEUE_VISIBILITY_TIMEOUT", 30.0),
            max_retries=_i("JOBQUEUE_MAX_RETRIES", 3),
            base_backoff=_f("JOBQUEUE_BASE_BACKOFF", 1.0),
            backoff_multiplier=_f("JOBQUEUE_BACKOFF_MULTIPLIER", 2.0),
            backoff_cap=_f("JOBQUEUE_BACKOFF_CAP", 60.0),
            jitter=_f("JOBQUEUE_JITTER", 0.0),
            poll_interval=_f("JOBQUEUE_POLL_INTERVAL", 0.1),
        )

    def backoff_delay(self, attempt: int) -> float:
        """第 attempt 次重试（attempt 从 1 开始）前的等待秒数。"""
        if attempt <= 0:
            return 0.0
        delay = min(
            self.backoff_cap,
            self.base_backoff * self.backoff_multiplier ** (attempt - 1),
        )
        if self.jitter > 0:
            delay += random.uniform(0, self.jitter)
        return delay
