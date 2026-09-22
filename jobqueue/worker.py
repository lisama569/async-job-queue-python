"""本地消费者进程：轮询、占用、执行、确认与失败重试。

Handler 约定：
- 正常返回 -> ack，任务成功；
- 抛出 :class:`RejectJob` -> nack(reject=True)，任务立即进入 rejected 终态，
  表示任务内容本身无法处理（区别于可重试的瞬时失败）；
- 抛出其它异常 -> nack(reason=异常文本)，按指数退避等待重试，超限进入 failed。
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from typing import Any

from jobqueue.errors import QueueError
from jobqueue.queue import JobQueue

logger = logging.getLogger("jobqueue.worker")

JobHandler = Callable[[dict[str, Any]], None]


class RejectJob(Exception):
    """由 handler 抛出：任务不可处理，应立即进入 rejected 终态。"""


class Worker:
    def __init__(
        self,
        queue: JobQueue,
        group: str,
        consumer: str,
        handler: JobHandler,
        poll_interval: float | None = None,
        visibility_timeout: float | None = None,
    ) -> None:
        self.queue = queue
        self.group = group
        self.consumer = consumer
        self.handler = handler
        self.poll_interval = (
            poll_interval
            if poll_interval is not None
            else queue.settings.poll_interval
        )
        self.visibility_timeout = visibility_timeout
        self._stop = threading.Event()

    def run_once(self) -> bool:
        """处理一条任务；有任务被处理返回 True，队列空返回 False。"""
        delivery = self.queue.claim(
            self.group, self.consumer, self.visibility_timeout
        )
        if delivery is None:
            return False
        job = self.queue.find_job(delivery.job_id)
        logger.info(
            "[输入] consumer=%s group=%s delivery=%s attempt=%s job=%s payload=%r",
            self.consumer,
            self.group,
            delivery.id,
            delivery.attempt,
            delivery.job_id,
            job.payload if job else None,
        )
        try:
            self.handler(dict(job.payload) if job else {})
        except RejectJob as exc:
            reason = str(exc) or "handler 判定任务不可处理"
            logger.info(
                "[判定] delivery=%s -> reject，依据=RejectJob: %s", delivery.id, reason
            )
            self._safe_nack(delivery.id, reason, unprocessable=True)
        except Exception as exc:  # noqa: BLE001 - 任何业务异常都要落盘失败原因
            reason = f"{type(exc).__name__}: {exc}"
            latest = self.queue.find_job(delivery.job_id)
            exhausted = latest is not None and latest.attempts >= latest.max_retries + 1
            logger.info(
                "[判定] delivery=%s -> %s，依据=handler 异常 %s",
                delivery.id,
                "failed(终态)" if exhausted else "retry(退避后重投)",
                reason,
            )
            self._safe_nack(delivery.id, reason, unprocessable=False)
        else:
            logger.info("[判定] delivery=%s -> ack 成功", delivery.id)
            self._safe_ack(delivery.id)
        return True

    def _safe_ack(self, delivery_id: int) -> None:
        try:
            self.queue.ack(delivery_id, self.consumer)
        except QueueError as exc:
            # 典型场景：占用超时已被回收，旧消费者的确认必须被拒绝
            logger.warning(
                "[忽略确认] delivery=%s 被拒绝：%s（%s）",
                delivery_id,
                exc.code.value,
                exc.message,
            )

    def _safe_nack(self, delivery_id: int, reason: str, *, unprocessable: bool) -> None:
        try:
            self.queue.nack(
                delivery_id, self.consumer, reason, unprocessable=unprocessable
            )
        except QueueError as exc:
            logger.warning(
                "[忽略否定确认] delivery=%s 被拒绝：%s（%s）",
                delivery_id,
                exc.code.value,
                exc.message,
            )

    def run(self, max_iterations: int | None = None) -> None:
        iterations = 0
        while not self._stop.is_set():
            processed = self.run_once()
            iterations += 1
            if max_iterations is not None and iterations >= max_iterations:
                return
            if not processed:
                self._stop.wait(self.poll_interval)

    def stop(self) -> None:
        self._stop.set()
