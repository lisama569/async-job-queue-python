"""跨场景状态守恒不变量：任务不丢失、不重复完成，失败原因始终可区分。"""

import logging
import threading

from taskqueue.consumer import Consumer, PermanentTaskError
from taskqueue.models import TaskStatus

logger = logging.getLogger(__name__)


def test_total_task_count_is_conserved_under_mixed_workload(queue, clock):
    logger.info("输入: 30 个混合任务（成功/瞬时失败/永久失败/无处理器），多消费者并发；"
                "判定: 最终每个任务恰好处于一个终态，succeeded+failed==30")
    consumer_a = Consumer(queue, "grp", "a")
    consumer_b = Consumer(queue, "grp", "b")

    flaky_calls = {"n": 0}

    def flaky(_):
        flaky_calls["n"] += 1
        raise RuntimeError("transient")

    def boom(_):
        raise PermanentTaskError("400 bad business input")

    consumer_a.register("ok", lambda p: "done")
    consumer_a.register("flaky", flaky)
    consumer_b.register("ok", lambda p: "done")
    consumer_b.register("boom", boom)

    n = 30
    for i in range(n):
        kind = ("ok", "flaky", "boom", "ghost")[i % 4]
        queue.enqueue(kind, {"i": i})

    def loop(consumer):
        for _ in range(100):
            r = consumer.process_once()
            if r.processed == 0:
                break
            clock.bump(queue.config.backoff_max_ms + 1)

    t1 = threading.Thread(target=loop, args=(consumer_a,))
    t2 = threading.Thread(target=loop, args=(consumer_b,))
    t1.start(); t2.start(); t1.join(); t2.join()
    # flaky 可能仍有退避中的任务，推进时钟后收尾
    for _ in range(10):
        clock.bump(queue.config.backoff_max_ms + 1)
        a = consumer_a.process_once()
        b = consumer_b.process_once()
        if a.processed == 0 and b.processed == 0:
            break

    counts = queue.status_counts()
    terminal = counts.get("succeeded", 0) + counts.get("failed", 0) + counts.get("rejected", 0)
    inflight = counts.get("inflight", 0)
    logger.info("结果: 计数=%s 终态合计=%d inflight=%d", counts, terminal, inflight)
    assert sum(counts.values()) == n
    assert terminal == n
    assert inflight == 0
    assert counts.get("pending", 0) == 0

    # 每个 failed 任务都带有可区分原因
    for tid in range(1, n + 1):
        task = queue.get_task(tid)
        if task.status is TaskStatus.FAILED:
            assert task.terminal_reason
            assert task.terminal_reason.split(":")[0] in {
                "HANDLER_ERROR", "HANDLER_REJECTED", "NO_HANDLER",
                "DECODE_ERROR", "LEASE_EXPIRED",
            }


def test_succeeded_tasks_are_never_counted_twice(queue):
    logger.info("输入: 成功后用相同/不同 epoch 反复 ack；判定: succeeded 计数恒为 1")
    queue.enqueue("ok", {})
    d = queue.poll("grp", "c")[0]
    queue.acknowledge(d.task.id, d.epoch)
    queue.acknowledge(d.task.id, d.epoch)  # 幂等
    try:
        queue.acknowledge(d.task.id, d.epoch + 1)  # 过期 epoch
    except Exception:
        pass
    counts = queue.status_counts()
    assert counts == {"succeeded": 1}
