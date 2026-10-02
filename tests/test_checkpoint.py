"""检查点：组进度快照的导出、持久化与恢复（重启/换进程接着消费）。"""

from __future__ import annotations

import logging

import pytest

from jobqueue.errors import ErrorCode, QueueError
from jobqueue.queue import JobQueue

logger = logging.getLogger("tests.test_checkpoint")


def _prepare(queue: JobQueue, group: str = "g"):
    """三条任务：j1 ack（终态）、j2 在途未确认、j3 未投递；位点推进到 1。"""
    queue.create_group(group)
    jobs = [queue.enqueue({"n": i}) for i in range(3)]
    d = queue.claim(group, "c")
    queue.ack(d.id, "c")  # j1 -> succeeded
    queue.commit_offset(group, 1)
    queue.claim(group, "c2")  # j2 -> inflight（不确认）
    return jobs


def test_checkpoint_export_content(queue, tmp_path) -> None:
    jobs = _prepare(queue)
    path = tmp_path / "cp.json"
    cp = queue.export_checkpoint("g", path)
    logger.info(
        "[输入] j1 succeeded、j2 inflight、j3 pending、位点=1 时导出快照 "
        "[判定] 快照只收录终态结论（j1），位点=1，文件已落盘"
    )
    assert cp.group == "g" and cp.offset == 1
    assert cp.conclusions == {jobs[0].id: "succeeded"}
    assert path.exists()
    # 文件可被独立解析回同一份快照
    from jobqueue.checkpoint import load_checkpoint_file

    loaded = load_checkpoint_file(path)
    assert loaded == cp


def test_restore_after_restart_resumes_without_redo(settings, tmp_path) -> None:
    path = tmp_path / "cp.json"
    q1 = JobQueue(settings)
    jobs = _prepare(q1)
    q1.export_checkpoint("g", path)
    q1.close()
    logger.info(
        "[输入] 导出快照后关闭进程，重开 JobQueue 并从快照恢复 "
        "[判定] 位点=1 恢复；j1 不再投递；j2（在途超时后）与 j3 仍会投递"
    )
    q2 = JobQueue(settings)
    offset = q2.restore_checkpoint(path)
    assert offset.group == "g" and offset.offset == 1
    # 已定论的 j1 不会再被投递；接下来投出的应是 j3（j2 占用未超时）
    d = q2.claim("g", "c3")
    assert d is not None and d.job_id == jobs[2].id
    q2.ack(d.id, "c3")
    p = q2.group_progress("g")
    assert p.buckets["succeeded"] == [jobs[0].id, jobs[2].id]
    assert p.buckets["inflight"] == [jobs[1].id]  # j2 未被跳过，仍在途等回收
    q2.close()


def test_restore_to_new_group_for_another_process(settings, tmp_path) -> None:
    """换消费进程（新组名）凭快照接着消费，且不影响原组。"""
    path = tmp_path / "cp.json"
    q1 = JobQueue(settings)
    jobs = _prepare(q1, group="old")
    q1.export_checkpoint("old", path)
    logger.info(
        "[输入] 把 old 组的快照恢复到新组 new "
        "[判定] new 组继承终态结论（j1 不重投），j2/j3 照常投递；old 组进度不变"
    )
    offset = q1.restore_checkpoint(path, group="new")
    assert offset.group == "new" and offset.offset == 1
    assert q1.job_group_statuses(jobs[0].id)["new"] == "succeeded"
    # new 组不会再收到 j1，先收到 j2（在 old 组在途不影响 new 组）
    d = q1.claim("new", "c9")
    assert d is not None and d.job_id == jobs[1].id
    # old 组对账不受恢复动作影响
    p_old = q1.group_progress("old")
    assert p_old.offset == 1 and p_old.buckets["succeeded"] == [jobs[0].id]
    assert p_old.buckets["inflight"] == [jobs[1].id]
    q1.close()


def test_restore_conflict_rejected(queue, tmp_path) -> None:
    jobs = _prepare(queue)
    path = tmp_path / "cp.json"
    queue.export_checkpoint("g", path)
    # 另一组 B 已对 j1 得出不同终态结论（rejected）
    db = queue.claim("B", "cb")
    queue.nack(db.id, "cb", "B 判定不可处理", unprocessable=True)
    logger.info(
        "[输入] 快照中 j1=succeeded，恢复到已判定 j1=rejected 的 B 组 "
        "[判定] checkpoint_conflict，B 组结论与位点均不被修改"
    )
    with pytest.raises(QueueError) as exc:
        queue.restore_checkpoint(path, group="B")
    assert exc.value.code is ErrorCode.CHECKPOINT_CONFLICT
    assert queue.job_group_statuses(jobs[0].id)["B"] == "rejected"
    assert queue.get_offset("B").offset == 0


def test_restore_unknown_or_malformed_checkpoint(queue, tmp_path) -> None:
    bad = tmp_path / "bad.json"
    bad.write_text('{"format": "other", "version": 9}', encoding="utf-8")
    logger.info("[输入] 非本格式快照文件 [判定] checkpoint_invalid")
    with pytest.raises(QueueError) as exc:
        queue.restore_checkpoint(bad)
    assert exc.value.code is ErrorCode.CHECKPOINT_INVALID

    not_json = tmp_path / "not.json"
    not_json.write_text("{{{", encoding="utf-8")
    with pytest.raises(QueueError) as exc:
        queue.restore_checkpoint(not_json)
    assert exc.value.code is ErrorCode.CHECKPOINT_INVALID

    missing = tmp_path / "missing.json"
    with pytest.raises(QueueError) as exc:
        queue.restore_checkpoint(missing)
    assert exc.value.code is ErrorCode.CHECKPOINT_INVALID


def test_restore_offset_never_rewinds(queue, tmp_path) -> None:
    _prepare(queue)
    path = tmp_path / "cp.json"
    queue.export_checkpoint("g", path)  # 快照位点=1
    # 之后组内继续推进：j2 超时回收前直接由持有者确认，位点前进到 2
    d = queue.list_deliveries(queue.list_jobs()[1].id)[-1]
    queue.ack(d.id, "c2")
    queue.commit_offset("g", 2)
    logger.info(
        "[输入] 组位点已推进到 2 后恢复位点=1 的旧快照 "
        "[判定] 位点保持 2（只前进不回退），快照结论与现有一致不冲突"
    )
    offset = queue.restore_checkpoint(path)
    assert offset.offset == 2


def test_restore_idempotent(queue, tmp_path) -> None:
    _prepare(queue)
    path = tmp_path / "cp.json"
    queue.export_checkpoint("g", path)
    logger.info("[输入] 同一快照连续恢复两次 [判定] 幂等，结果一致")
    first = queue.restore_checkpoint(path, group="new")
    second = queue.restore_checkpoint(path, group="new")
    assert first == second
