#!/usr/bin/env python3
"""End-to-end local demo of the task queue (no external services).

Run:
    uv run python scripts/demo.py

It walks through: idempotent enqueue, exclusive poll, stale-epoch fence,
retry with backoff, terminal failure, lease expiry/redelivery, and offsets.
Prints the input and the decision basis for every step.
"""

from __future__ import annotations

import shutil
import sys
import tempfile
import textwrap
from pathlib import Path

# Allow running directly: `uv run python scripts/demo.py`
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from taskqueue.config import QueueConfig
from taskqueue.consumer import Consumer, PermanentTaskError
from taskqueue.exceptions import QueueError
from taskqueue.queue import TaskQueue
from taskqueue.storage import Storage


def banner(title: str) -> None:
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


def main() -> None:
    db_dir = tempfile.mkdtemp(prefix="taskqueue-demo-")
    cfg = QueueConfig(
        db_path=f"{db_dir}/demo.db",
        lease_timeout_ms=30_000,
        max_attempts=3,
        backoff_base_ms=100,
        backoff_factor=2.0,
        backoff_max_ms=1_000,
    )

    class Clock:
        def __init__(self):
            self.t = 1_000_000

        def __call__(self):
            return self.t / 1000.0

        def sleep(self, ms):
            self.t += ms

    clock = Clock()
    q = TaskQueue(cfg, Storage(cfg.db_path), clock=clock)

    banner("1) 入队：正常 + 重复提交（dedup_key 幂等）+ 非法提交")
    t1 = q.enqueue("echo", {"msg": "hello"}, dedup_key="order-1")
    t1_dup = q.enqueue("echo", {"msg": "IGNORED"}, dedup_key="order-1")
    print(f"首次入队 id={t1.id}；重复提交返回 id={t1_dup.id}（同一任务，payload 不被覆盖）")
    try:
        q.enqueue("echo", "not-a-json")
    except QueueError as exc:
        print(f"非法提交被拒绝: {exc.reason.value} | {exc.detail}")

    banner("2) 投递：同一时刻只有一个消费者占用")
    d1 = q.poll("demo-group", "consumer-A")
    d2 = q.poll("demo-group", "consumer-B")
    print(f"A 拿到: {[d.task.id for d in d1]}（epoch={d1[0].epoch}）；"
          f"B 拿到: {[d.task.id for d in d2]}")
    print("判定依据: 条件 UPDATE ... status='inflight' 已在一个 BEGIN IMMEDIATE 事务里完成")

    banner("3) 确认：fence token + 幂等")
    try:
        q.acknowledge(t1.id, epoch=d1[0].epoch + 1)
    except QueueError as exc:
        print(f"错误 epoch ack 被拒: {exc.reason.value}")
    q.acknowledge(t1.id, epoch=d1[0].epoch)
    q.acknowledge(t1.id, epoch=d1[0].epoch)
    print("正确 epoch ack 成功；用同一 epoch 重复 ack 幂等返回 succeeded")

    banner("4) 重试：指数退避，超过上限进入 failed 终态")
    t2 = q.enqueue("flaky", {})
    attempts = 0

    def flaky(_):
        nonlocal attempts
        attempts += 1
        raise RuntimeError("upstream timeout")

    consumer = Consumer(q, "demo-group", "consumer-A", {"flaky": flaky})
    while not q.get_task(t2.id).is_terminal():
        r = consumer.process_once()
        task = q.get_task(t2.id)
        print(f"第 {attempts} 次处理 -> status={task.status.value} "
              f"attempts={task.attempts}/{task.max_attempts} reason={task.last_error}")
        if not task.is_terminal():
            print("    ^ 退避中，时钟前推到可投递时刻")
            clock.sleep(cfg.backoff_max_ms + 1)
    print(f"终态: {q.get_task(t2.id).status.value}，可查询原因: "
          f"{q.get_task(t2.id).terminal_reason}")

    banner("5) 租约超时：崩溃后任务可重投，迟到 ack 被拒")
    t3 = q.enqueue("echo", {})
    first = q.poll("demo-group", "consumer-A")[0]
    clock.sleep(cfg.lease_timeout_ms + 1)
    reclaimed = q.reclaim_expired_leases()
    second = q.poll("demo-group", "consumer-B")[0]
    print(f"回收超时租约 {reclaimed} 条；重投 epoch {first.epoch} -> {second.epoch}，"
          f"owner={second.task.lease_owner}")
    try:
        q.acknowledge(t3.id, first.epoch)
    except QueueError as exc:
        print(f"旧持有者迟到 ack 被拒: {exc.reason.value}")
    q.acknowledge(t3.id, second.epoch)
    print("新持有者用新 epoch ack 成功，任务只完成一次")

    banner("6) 位点：只能顺序 +1，回退/跳号/越界/越过未完成均拒绝")
    q.create_group("audit")
    q.commit_offset("audit", 1)
    q.commit_offset("audit", 2)
    print("顺序提交位点 0->1->2 成功")
    # 再造一条已完成任务把日志前沿推到 4，但位点仍停在 2
    t4 = q.enqueue("echo", {})
    d4 = q.poll("demo-group", "consumer-A")[0]
    q.acknowledge(t4.id, d4.epoch)
    for requested, expect in [(2, "OFFSET_ROLLBACK"), (4, "OFFSET_SKIP"),
                              (99, "OFFSET_OVERRUN")]:
        try:
            q.commit_offset("audit", requested)
        except QueueError as exc:
            print(f"提交位点 {requested}: {exc.reason.value}（预期 {expect}）")

    banner("7) 状态守恒")
    counts = q.status_counts()
    print(f"最终计数: {counts}；合计={sum(counts.values())}（入队 3 条，守恒）")
    q.close()
    shutil.rmtree(db_dir, ignore_errors=True)
    print(textwrap.dedent("""
        演示结束。也可启动 HTTP 服务手工验证：
            uv run uvicorn main:app --reload
            curl -X POST localhost:8000/tasks -H 'content-type: application/json' \\
                 -d '{"task_type":"echo","payload":{"x":1},"dedup_key":"k1"}'
    """))


if __name__ == "__main__":
    main()
