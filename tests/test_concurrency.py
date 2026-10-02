"""并发消费：同一任务同一时刻只能被一个消费者占用；任务守恒。"""

from __future__ import annotations

import logging
import threading
from dataclasses import replace

from jobqueue.models import JobState
from jobqueue.queue import JobQueue
from jobqueue.worker import Worker

logger = logging.getLogger("tests.test_concurrency")


def test_concurrent_claim_each_job_claimed_once(settings, queue) -> None:
    settings = replace(settings, base_backoff=0.0)
    queue.close()
    queue = JobQueue(settings)
    n = 30
    for i in range(n):
        queue.enqueue({"i": i})

    results: dict[str, int] = {}
    lock = threading.Lock()
    barrier = threading.Barrier(8)

    def consume(consumer: str) -> None:
        barrier.wait()
        while True:
            d = queue.claim("g", consumer)
            if d is None:
                if all(
                    qj.state in {JobState.SUCCEEDED, JobState.FAILED, JobState.REJECTED}
                    for qj in queue.list_jobs()
                ):
                    return
                continue
            with lock:
                results.setdefault(d.job_id, 0)
                results[d.job_id] += 1
            queue.ack(d.id, consumer)

    threads = [threading.Thread(target=consume, args=(f"c{i}",)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    logger.info(
        "[输入] 30 条任务、8 个消费者并发 claim+ack [判定] 每个任务恰好占用一次，"
        "占用分布=%s，成功任务数=%s",
        set(results.values()),
        len(queue.list_jobs(JobState.SUCCEEDED.value)),
    )
    assert len(results) == n
    assert all(count == 1 for count in results.values())
    assert len(queue.list_jobs(JobState.SUCCEEDED.value)) == n
    # 每个任务仅一条 acked 投递
    for job in queue.list_jobs():
        acked = [d for d in queue.list_deliveries(job.id) if d.state == "acked"]
        assert len(acked) == 1


def test_concurrent_workers_with_failures_reach_terminal(settings, tmp_path) -> None:
    settings = replace(
        settings, db_path=str(tmp_path / "c.db"), base_backoff=0.0, max_retries=2
    )
    queue = JobQueue(settings)
    n = 20
    for i in range(n):
        queue.enqueue({"i": i})

    def handler(payload: dict) -> None:
        # i 为 3 的倍数的任务始终失败，其余成功
        if payload["i"] % 3 == 0:
            raise RuntimeError(f"job {payload['i']} always fails")

    workers = [
        Worker(queue, "g", f"worker-{i}", handler, poll_interval=0.002)
        for i in range(4)
    ]
    threads = [
        threading.Thread(target=w.run, kwargs={"max_iterations": 400}) for w in workers
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=15)

    jobs = queue.list_jobs()
    succeeded = [j for j in jobs if j.state is JobState.SUCCEEDED]
    failed = [j for j in jobs if j.state is JobState.FAILED]
    logger.info(
        "[输入] 20 条任务（每 3 条 1 条永久失败）4 worker 并发 [判定] 成功=%s 终态失败=%s，"
        "成功任务仅执行 1 次，失败任务 attempts=3",
        len(succeeded),
        len(failed),
    )
    assert len(jobs) == n
    assert len(failed) == len([i for i in range(n) if i % 3 == 0])
    assert len(succeeded) == n - len(failed)
    assert all(j.attempts == 1 for j in succeeded)
    assert all(j.attempts == 3 and "重试已耗尽" in (j.last_error or "") for j in failed)
    for j in jobs:
        acked = [d for d in queue.list_deliveries(j.id) if d.state == "acked"]
        assert len(acked) <= 1
    queue.close()


def test_concurrent_offset_commits_never_regress(settings, queue) -> None:
    queue.close()
    queue = JobQueue(settings)
    queue.create_group("g")
    for i in range(20):
        queue.enqueue({"i": i})
    # 位点推进以组内终态为前提：先把 20 条全部消费到 succeeded
    for _ in range(20):
        d = queue.claim("g", "setup")
        queue.ack(d.id, "setup")

    errors: list[str] = []
    lock = threading.Lock()

    def commit(target: int) -> None:
        try:
            queue.commit_offset("g", target)
        except Exception as exc:  # noqa: BLE001
            with lock:
                errors.append(str(exc))

    # 8 个线程各重复多轮、乱序提交 1..20；最终位点必须是 20，且回退必被拒绝
    barrier = threading.Barrier(8)

    def commit_loop() -> None:
        barrier.wait()
        for _ in range(20):
            for target in range(1, 21):
                try:
                    queue.commit_offset("g", target)
                except Exception as exc:  # noqa: BLE001
                    with lock:
                        errors.append(str(exc))

    threads = [threading.Thread(target=commit_loop) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    logger.info(
        "[输入] 8 线程并发重复提交位点 1..20 [判定] 最终位点=20，回退提交被拒绝（共拒绝 %s 次）",
        len(errors),
    )
    assert queue.get_offset("g").offset == 20
    assert errors  # 并发竞争中必然有回退提交被 offset_rollback 拒绝
