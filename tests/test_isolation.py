"""组间隔离：终态不被其它组改回、退避互不牵连、按组查看任务结论。"""

from __future__ import annotations

import logging
import time
from dataclasses import replace

from jobqueue.models import JobState
from jobqueue.queue import JobQueue

logger = logging.getLogger("tests.test_isolation")


def test_terminal_conclusion_not_reverted_by_other_groups(queue) -> None:
    job = queue.enqueue({"n": 1})
    da = queue.claim("A", "c1")
    queue.ack(da.id, "c1")  # A 组终态：succeeded
    logger.info(
        "[输入] 任务在 A 组已 succeeded 后，B 组对它 claim -> nack -> 再 claim -> ack "
        "[判定] A 组结论始终为 succeeded，不被改回在途/待处理"
    )
    db = queue.claim("B", "c2")
    assert db is not None and db.job_id == job.id
    queue.nack(db.id, "c2", "B 组瞬时故障")  # B 组 retrying
    assert queue.job_group_statuses(job.id)["A"] == "succeeded"
    db2 = queue.claim("B", "c2")
    # B 组退避未到期时应取不到；等到期后取到并 ack
    if db2 is None:
        time.sleep(0.02)
        db2 = queue.claim("B", "c2")
    assert db2 is not None
    queue.ack(db2.id, "c2")
    statuses = queue.job_group_statuses(job.id)
    assert statuses == {"A": "succeeded", "B": "succeeded"}
    # A 组对账中该任务仍在 succeeded 桶
    p = queue.group_progress("A")
    assert p.buckets["succeeded"] == [job.id]
    assert p.count("inflight") == 0 and p.count("pending") == 0


def test_failed_conclusion_not_reverted_by_other_groups(settings) -> None:
    queue = JobQueue(replace(settings, base_backoff=0.0))
    job = queue.enqueue({"n": 1})
    # A 组重试耗尽 -> failed 终态
    for _ in range(4):
        d = queue.claim("A", "c1")
        queue.nack(d.id, "c1", "故障")
    assert queue.job_group_statuses(job.id)["A"] == "failed"
    logger.info(
        "[输入] 任务在 A 组 failed 终态后，B 组正常消费成功 "
        "[判定] A 组仍是 failed；全局状态按汇总规则呈现"
    )
    db = queue.claim("B", "c2")
    queue.ack(db.id, "c2")
    statuses = queue.job_group_statuses(job.id)
    assert statuses["A"] == "failed" and statuses["B"] == "succeeded"
    assert queue.get_job(job.id).state is JobState.SUCCEEDED  # 汇总：任一组合格即合格
    queue.close()


def test_backoff_does_not_block_other_groups(settings) -> None:
    queue = JobQueue(
        replace(settings, base_backoff=60.0, backoff_cap=60.0, jitter=0.0)
    )
    j1 = queue.enqueue({"n": 1})
    j2 = queue.enqueue({"n": 2})
    da = queue.claim("A", "c1")
    queue.nack(da.id, "c1", "A 组故障")  # A 组进入 60s 退避
    logger.info(
        "[输入] A 组对 j1 失败进入 60s 退避 [判定] B 组立即取到 j1 并 ack，"
        "且能继续推进 j2；A 组退避期间取不到件"
    )
    db = queue.claim("B", "c2")
    assert db is not None and db.job_id == j1.id
    queue.ack(db.id, "c2")
    db2 = queue.claim("B", "c2")
    assert db2 is not None and db2.job_id == j2.id
    # A 组退避只挡住 j1：能取到 j2，但取不到退避中的 j1
    da2 = queue.claim("A", "c1")
    assert da2 is not None and da2.job_id == j2.id
    assert queue.claim("A", "c1") is None  # j1 仍在退避，j2 已被占用
    p_a = queue.group_progress("A")
    p_b = queue.group_progress("B")
    assert p_a.buckets["retrying"] == [j1.id]
    assert p_b.buckets["succeeded"] == [j1.id]
    queue.close()


def test_job_query_shows_per_group_conclusions(queue) -> None:
    job = queue.enqueue({"n": 1})
    da = queue.claim("A", "c1")
    queue.ack(da.id, "c1")
    db = queue.claim("B", "c2")
    queue.nack(db.id, "c2", "B 不可处理", unprocessable=True)
    queue.claim("C", "c3")  # C 组在途
    logger.info(
        "[输入] 同一任务：A ack、B reject、C 在途、D 未注册 "
        "[判定] job_group_statuses 分别给出各组结论"
    )
    statuses = queue.job_group_statuses(job.id)
    assert statuses == {"A": "succeeded", "B": "rejected", "C": "inflight"}
