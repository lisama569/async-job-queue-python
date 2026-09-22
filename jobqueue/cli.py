"""命令行入口：本地起服务或跑端到端演示。

用法：
    uv run python -m jobqueue.cli serve [--host 127.0.0.1] [--port 8000]
    uv run python -m jobqueue.cli demo
"""

from __future__ import annotations

import argparse
import logging
import os
import tempfile
import time

from jobqueue.config import Settings
from jobqueue.errors import QueueError
from jobqueue.queue import JobQueue
from jobqueue.worker import RejectJob, Worker

logger = logging.getLogger("jobqueue.cli")


def _demo_handler(payload: dict) -> None:
    action = payload.get("action")
    if action == "ok":
        return
    if action == "reject":
        raise RejectJob("任务类型不支持，无法处理")
    if action == "fail":
        raise RuntimeError("模拟瞬时故障")
    raise RejectJob(f"未知 action={action!r}")


def run_demo(settings: Settings) -> None:
    settings = Settings(
        db_path=os.path.join(tempfile.mkdtemp(prefix="jobqueue-demo-"), "demo.db"),
        visibility_timeout=2.0,
        max_retries=2,
        base_backoff=0.05,
        backoff_multiplier=2.0,
        backoff_cap=1.0,
    )
    queue = JobQueue(settings)
    queue.create_group("demo")
    jobs = [
        queue.enqueue({"action": "ok"}, "demo-ok"),
        queue.enqueue({"action": "fail"}, "demo-fail"),
        queue.enqueue({"action": "reject"}, "demo-reject"),
    ]
    worker = Worker(queue, "demo", "consumer-1", _demo_handler, poll_interval=0.02)
    deadline = time.time() + 10
    while time.time() < deadline and any(
        not queue.get_job(j.id).terminal for j in jobs
    ):
        worker.run_once()
        time.sleep(0.02)
    print("=== 演示结果 ===")
    for j in jobs:
        fresh = queue.get_job(j.id)
        print(f"- {j.dedupe_key}: state={fresh.state.value} attempts={fresh.attempts} "
              f"last_error={fresh.last_error}")
    queue.close()


def main() -> None:
    parser = argparse.ArgumentParser(prog="jobqueue")
    sub = parser.add_subparsers(dest="command", required=True)

    serve = sub.add_parser("serve", help="启动 FastAPI 服务")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8000)

    sub.add_parser("demo", help="运行本地端到端演示")

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if args.command == "demo":
        run_demo(Settings.from_env())
    elif args.command == "serve":
        import uvicorn

        uvicorn.run(
            "main:app",
            host=args.host,
            port=args.port,
            reload=False,
        )


if __name__ == "__main__":
    main()
