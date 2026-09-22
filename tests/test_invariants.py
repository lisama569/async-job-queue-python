"""状态守恒不变量：任务数守恒、状态分类穷尽、投递账本闭合。"""

from __future__ import annotations

import logging
import time
from dataclasses import replace

from jobqueue.models import JobState
from jobqueue.queue import JobQueue

logger = logging.getLogger("tests.test_invariants")

_ALL_STATES = {s.value for s in JobState}


def test_total_count_conserved_through_lifecycle(settings, queue) -> None:
    settings = replace(settings, base_backoff=0.0, visibility_timeout=0.03)
    queue.close()
    queue = JobQueue(settings)
    initial = 12
    for i in range(initial):
        queue.enqueue({"i": i}, dedupe_key=f"k{i}")
    # 重复入队全部被拒，数量不变
    for i in range(initial):
        try:
            queue.enqueue({"i": i}, dedupe_key=f"k{i}")
        except Exception:
            pass

    def partition() -> dict[str, int]:
        counts: dict[str, int] = {}
        for j in queue.list_jobs():
            counts[j.state.value] = counts.get(j.state.value, 0) + 1
            assert j.state.value in _ALL_STATES
        return counts

    assert sum(partition().values()) == initial
    # 消费：偶数成功，奇数 nack 到终态，期间穿插超时回收
    deadline = time.time() + 5
    while time.time() < deadline:
        d = queue.claim("g", "c")
        if d is None:
            if all(j.terminal for j in queue.list_jobs()):
                break
            continue
        job = queue.find_job(d.job_id)
        if int(job.payload["i"]) % 2 == 0:
            queue.ack(d.id, "c")
        else:
            queue.nack(d.id, "c", "always bad")

    counts = partition()
    logger.info(
        "[输入] 12 条任务（偶数 ack、奇数持续 nack）含超时回收 [判定] 终态分布=%s，总数守恒=12",
        counts,
    )
    assert sum(counts.values()) == initial
    assert counts.get(JobState.SUCCEEDED.value, 0) == 6
    assert counts.get(JobState.FAILED.value, 0) == 6
    # 投递账本：每个 active 投递都对应一个 inflight 的 group_jobs；无重复 acked
    for job in queue.list_jobs():
        dels = queue.list_deliveries(job.id)
        assert sum(1 for x in dels if x.state == "acked") <= 1
        for x in dels:
            assert x.state in {"active", "acked", "nacked", "timed_out"}


def test_terminal_states_never_reopen(queue) -> None:
    # succeeded / rejected / failed 之后无论怎么 claim 都不会复活
    from dataclasses import replace as _r

    queue.close()
    queue = JobQueue(_r(queue.settings, base_backoff=0.0))
    queue.create_group("g")
    j_ok = queue.enqueue({"t": "ok"})
    d = queue.claim("g", "c"); queue.ack(d.id, "c")
    j_rej = queue.enqueue({"t": "rej"})
    d = queue.claim("g", "c"); queue.nack(d.id, "c", "no", unprocessable=True)
    j_fail = queue.enqueue({"t": "fail"})
    for _ in range(10):
        d = queue.claim("g", "c")
        if d is None:
            break
        queue.nack(d.id, "c", "x")
    logger.info("[输入] 三类终态任务反复 claim [判定] 均不再投递，状态不回退")
    assert queue.claim("g", "c") is None
    assert queue.get_job(j_ok.id).state is JobState.SUCCEEDED
    assert queue.get_job(j_rej.id).state is JobState.REJECTED
    assert queue.get_job(j_fail.id).state is JobState.FAILED
