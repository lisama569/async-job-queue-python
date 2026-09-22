"""本地消费者：处理器分发、失败原因可区分、日志含输入与判定、状态守恒。"""

import logging

from taskqueue.consumer import Consumer, PermanentTaskError
from taskqueue.models import TaskStatus

logger = logging.getLogger(__name__)


def test_consumer_classifies_failures_and_logs_decisions(queue, caplog):
    logger.info("输入: 4 类任务 echo/flaky(瞬时)/poison(永久)/unknown(无处理器)")
    attempts = {"n": 0}

    def flaky(_):
        attempts["n"] += 1
        raise RuntimeError("network down")

    def poison(_):
        raise PermanentTaskError("value must be positive")

    def echo(payload):
        return f"handled:{payload['x']}"

    consumer = Consumer(queue, "grp", "c1", {"echo": echo, "flaky": flaky, "poison": poison})
    queue.enqueue("echo", {"x": 7})
    queue.enqueue("flaky", {})
    queue.enqueue("poison", {})
    queue.enqueue("unknown", {})

    with caplog.at_level(logging.INFO):
        result = consumer.process_once()

    assert result.processed == 4
    assert result.succeeded == [1]
    reasons = dict(result.failed)
    assert reasons[2].startswith("HANDLER_ERROR:RuntimeError:network down")
    assert reasons[3].startswith("HANDLER_REJECTED:value must be positive")
    assert reasons[4] == "NO_HANDLER:unknown"

    # 日志必须打印“输入”与“判定依据”
    log_text = caplog.text
    assert "input group=grp consumer=c1 task=1" in log_text
    assert "decision=ACK task=1" in log_text
    assert "decision=NACK task=2" in log_text and "retry=True" in log_text
    assert "NO_HANDLER:unknown" in log_text
    logger.info("判定日志已覆盖 input/decision/reason，失败原因彼此可区分")

    # 终态：3/4 立即失败，2 号排程重试
    assert queue.get_task(3).status is TaskStatus.FAILED
    assert queue.get_task(4).status is TaskStatus.FAILED
    assert queue.get_task(2).status is TaskStatus.PENDING
    assert queue.get_task(1).status is TaskStatus.SUCCEEDED


def test_flaky_task_ends_terminal_after_budget_and_state_conserved(queue, clock, caplog):
    logger.info("输入: 一个持续瞬时失败的任务，max_attempts=4；判定: 重试 3 次后终态，"
                "总数始终=1，终态原因是 HANDLER_ERROR")
    consumer = Consumer(queue, "grp", "c1", {"flaky": lambda p: (_ for _ in ()).throw(RuntimeError("x"))})
    queue.enqueue("flaky", {})
    rounds = 0
    seen_states = []
    while True:
        result = consumer.process_once()
        if result.processed == 0:
            break
        rounds += 1
        seen_states.append(queue.get_task(1).status.value)
        task = queue.get_task(1)
        if not task.is_terminal():
            clock.bump(queue.config.backoff_max_ms + 1)
    final = queue.get_task(1)
    logger.info("结果: 处理轮次=%d 历次状态=%s 最终=%s 原因=%s",
                rounds, seen_states, final.status.value, final.terminal_reason)
    assert rounds == 4
    assert final.status is TaskStatus.FAILED
    assert final.terminal_reason.startswith("HANDLER_ERROR")
    counts = queue.status_counts()
    assert sum(counts.values()) == 1  # 任务守恒：没有被静默丢弃或复制


def test_successful_handler_is_acked_once(queue):
    consumer = Consumer(queue, "grp", "c1", {"echo": lambda p: "ok"})
    queue.enqueue("echo", {})
    queue.enqueue("echo", {})
    r = consumer.process_once()
    assert r.succeeded == [1, 2]
    # 再次轮询为空，不会重复执行
    assert consumer.process_once().processed == 0
    assert queue.status_counts().get("succeeded", 0) == 2


def test_no_handler_on_empty_queue_logs_noop(queue, caplog):
    consumer = Consumer(queue, "grp", "c1")
    with caplog.at_level(logging.INFO):
        r = consumer.process_once()
    assert r.processed == 0
    assert "decision=NOOP reason=EMPTY_QUEUE" in caplog.text
