"""检查点：组进度快照的导出、持久化与恢复（重启/换进程接着消费）。"""

from __future__ import annotations

import json
import logging
from dataclasses import replace

import pytest

from jobqueue.errors import ErrorCode, QueueError
from jobqueue.models import Checkpoint, CheckpointJob
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


# ---------- v2 自足快照：跨进程 / 空队列数据恢复 ----------


def _cp_jobs(jobs) -> list[CheckpointJob]:
    return [
        CheckpointJob(
            id=j.id,
            dedupe_key=j.dedupe_key,
            payload=j.payload,
            max_retries=j.max_retries,
            created_at=j.created_at,
        )
        for j in jobs
    ]


def test_checkpoint_v2_is_self_contained(queue, tmp_path) -> None:
    jobs = _prepare(queue)
    path = tmp_path / "cp.json"
    cp = queue.export_checkpoint("g", path)
    logger.info(
        "[输入] 导出 v2 快照 [判定] 快照收录全部 3 条任务（按入队顺序），"
        "结论仅 j1=succeeded，文件为 version=2"
    )
    assert len(cp.jobs) == 3
    assert [j.id for j in cp.jobs] == [j.id for j in jobs]
    doc = json.loads(path.read_text(encoding="utf-8"))
    assert doc["version"] == 2
    assert [j["id"] for j in doc["jobs"]] == [j.id for j in jobs]
    assert doc["jobs"][1]["payload"] == {"n": 1}


def test_restore_into_empty_db_then_consume(settings, tmp_path) -> None:
    """快照恢复进一份完全空的队列数据：任务照单重建，定论不重做、未做完不跳过。"""
    path = tmp_path / "cp.json"
    q1 = JobQueue(settings)
    jobs = _prepare(q1)  # j1 succeeded、j2 inflight、j3 pending，位点 1
    q1.export_checkpoint("g", path)
    q1.close()

    empty_db = tmp_path / "fresh.db"
    q2 = JobQueue(replace(settings, db_path=str(empty_db)))
    assert q2.list_jobs() == [] and q2.list_groups() == []
    offset = q2.restore_checkpoint(path)
    logger.info(
        "[输入] 把含 3 条任务、j1 已定论、j2 在途的快照恢复进空库 "
        "[判定] 3 条任务按序重建；位点=1；j1 不再投递；j2/j3 依次被消费不丢失"
    )
    assert offset.group == "g" and offset.offset == 1
    restored = q2.list_jobs()
    assert [j.id for j in restored] == [j.id for j in jobs]
    assert restored[0].state.value == "succeeded"
    assert q2.job_group_statuses(jobs[0].id)["g"] == "succeeded"
    # j2 原在途投递不属于本进程：恢复后被接管，新进程可以立即重新领取 j2
    d = q2.claim("g", "new-c")
    assert d is not None and d.job_id == jobs[1].id
    q2.ack(d.id, "new-c")
    d = q2.claim("g", "new-c")
    assert d is not None and d.job_id == jobs[2].id
    q2.ack(d.id, "new-c")
    assert q2.claim("g", "new-c") is None  # 全部消费完
    assert [j.state.value for j in q2.list_jobs()] == ["succeeded"] * 3
    q2.commit_offset("g", 3)
    assert q2.get_offset("g").offset == 3
    q2.close()


def test_restore_into_other_process_db_preserves_other_groups(
    settings, tmp_path
) -> None:
    """快照恢复进另一个已有自己任务/组的进程：只补任务与目标组，其它组不动。"""
    path = tmp_path / "cp.json"
    q1 = JobQueue(settings)
    jobs = _prepare(q1, group="old")
    q1.export_checkpoint("old", path)
    q1.close()

    other_db = tmp_path / "other.db"
    q2 = JobQueue(replace(settings, db_path=str(other_db)))
    q2.create_group("local")
    local_job = q2.enqueue({"n": 999})
    dl = q2.claim("local", "lc")
    q2.ack(dl.id, "lc")
    q2.commit_offset("local", 1)
    offset = q2.restore_checkpoint(path, group="taken-over")
    logger.info(
        "[输入] 另一进程库中已有 local 组及 1 条本地任务，把 old 组快照以新组名恢复 "
        "[判定] 快照 3 条任务补入；接管组位点=1、j1 不重投；local 组进度不受影响"
    )
    assert offset.group == "taken-over" and offset.offset == 1
    # 本地任务与 local 组原样保留（local 组结论不受恢复影响；
    # 接管组按"新组可见历史任务"的惰性规则也能看到本地任务，属正常）
    assert q2.get_job(local_job.id).state.value == "succeeded"
    assert q2.get_offset("local").offset == 1
    assert q2.job_group_statuses(local_job.id)["local"] == "succeeded"
    # 接管组：j1 定论，随后依次取到 j2、j3
    d = q2.claim("taken-over", "tc")
    assert d is not None and d.job_id == jobs[1].id
    q2.ack(d.id, "tc")
    d = q2.claim("taken-over", "tc")
    assert d is not None and d.job_id == jobs[2].id
    q2.ack(d.id, "tc")
    # 接管组也能看到该库原有的本地任务（组级视角独立），消费之不影响 local 组
    d = q2.claim("taken-over", "tc")
    assert d is not None and d.job_id == local_job.id
    q2.ack(d.id, "tc")
    assert q2.claim("taken-over", "tc") is None
    assert q2.job_group_statuses(local_job.id)["local"] == "succeeded"
    assert q2.job_group_statuses(local_job.id)["taken-over"] == "succeeded"
    q2.close()


def test_restore_rebuilds_unconcluded_jobs_as_pending(queue, tmp_path) -> None:
    """快照恢复后未定论任务（含在途的）在目标组都是 pending，等待正常投递。"""
    path = tmp_path / "cp.json"
    q1 = JobQueue(queue.settings)
    jobs = _prepare(q1)
    q1.export_checkpoint("g", path)
    q1.close()
    queue.restore_checkpoint(path, group="fresh")
    logger.info(
        "[输入] 快照（j1 定论、j2 在途、j3 未投递）恢复到同库新组 fresh "
        "[判定] fresh 组：j1 succeeded，j2/j3 都是 pending（在途结论不跨组继承）"
    )
    statuses_g = queue.job_group_statuses(jobs[1].id)
    assert statuses_g["g"] == "inflight"  # 原组在途保持不变
    assert statuses_g["fresh"] == "pending"
    assert queue.job_group_statuses(jobs[2].id)["fresh"] == "pending"
    p = queue.group_progress("fresh")
    assert p.buckets["succeeded"] == [jobs[0].id]
    assert p.buckets["pending"] == [jobs[1].id, jobs[2].id]


# ---------- 恢复位点与真实进度对齐 ----------


def test_restore_inflated_offset_is_corrected(settings) -> None:
    """位点虚高（记 3、实际只定论 1）：恢复时纠正到真实可支撑的 1。"""
    q = JobQueue(replace(settings, base_backoff=0.0))
    q.create_group("g")
    jobs = [q.enqueue({"n": i}) for i in range(3)]
    d = q.claim("g", "c")
    q.ack(d.id, "c")  # 仅 j1 定论
    fake = Checkpoint(
        group="g",
        offset=3,
        created_at=0.0,
        conclusions={jobs[0].id: "succeeded"},
        jobs=_cp_jobs(jobs),
    )
    offset = q.restore_checkpoint(fake)
    logger.info(
        "[输入] 快照位点=3，但前 3 条中只有 j1 有终态结论 "
        "[判定] 恢复位点纠正为 1，j2/j3 仍在 pending 未被跳过"
    )
    assert offset.offset == 1
    p = q.group_progress("g")
    assert p.buckets["succeeded"] == [jobs[0].id]
    assert set(p.buckets["pending"]) == {jobs[1].id, jobs[2].id}
    q.close()


def test_restore_out_of_range_offset_is_corrected(settings) -> None:
    """位点越界（超过队列任务总数）：恢复时夹到真实终态前缀，不越界。"""
    q = JobQueue(replace(settings, base_backoff=0.0))
    q.create_group("g")
    jobs = [q.enqueue({"n": i}) for i in range(3)]
    d = q.claim("g", "c")
    q.ack(d.id, "c")
    fake = Checkpoint(
        group="g2",
        offset=99,
        created_at=0.0,
        conclusions={jobs[0].id: "succeeded"},
        jobs=_cp_jobs(jobs),
    )
    offset = q.restore_checkpoint(fake)
    logger.info(
        "[输入] 快照位点=99，队列仅 3 条且只有 j1 定论 "
        "[判定] 恢复位点=1（≤ 总数、= 终态前缀长度），不留超前位点"
    )
    assert offset.offset == 1
    assert q.get_offset("g2").offset == 1
    q.close()


def test_restore_zero_prefix_offset_corrected(settings) -> None:
    """位点虚高到连第 1 条都没定论：恢复位点纠正为 0。"""
    q = JobQueue(settings)
    q.create_group("g")
    jobs = [q.enqueue({"n": i}) for i in range(2)]
    fake = Checkpoint(
        group="g",
        offset=2,
        created_at=0.0,
        conclusions={},
        jobs=_cp_jobs(jobs),
    )
    offset = q.restore_checkpoint(fake)
    logger.info(
        "[输入] 快照位点=2 但无任何终态结论 [判定] 恢复位点纠正为 0"
    )
    assert offset.offset == 0
    q.close()


def test_restore_honest_offset_unchanged(settings) -> None:
    """位点如实（=终态前缀长度）时恢复结果就是快照位点。"""
    q = JobQueue(replace(settings, base_backoff=0.0))
    q.create_group("g")
    jobs = [q.enqueue({"n": i}) for i in range(3)]
    d = q.claim("g", "c")
    q.ack(d.id, "c")
    d = q.claim("g", "c")
    q.ack(d.id, "c")
    honest = Checkpoint(
        group="g",
        offset=2,
        created_at=0.0,
        conclusions={
            jobs[0].id: "succeeded",
            jobs[1].id: "succeeded",
        },
        jobs=_cp_jobs(jobs),
    )
    offset = q.restore_checkpoint(honest)
    logger.info(
        "[输入] 快照位点=2，前 2 条均已定论 [判定] 恢复位点保持 2，不被压低"
    )
    assert offset.offset == 2
    q.close()


def test_restore_non_self_contained_checkpoint_rejected(queue) -> None:
    """v1 风格、缺少 jobs 却引用了库内不存在的任务：明确拒绝、不留半成品。"""
    q = JobQueue(queue.settings)
    jobs = [q.enqueue({"n": 0})]
    ghost = Checkpoint(
        group="g",
        offset=1,
        created_at=0.0,
        conclusions={"does-not-exist": "succeeded"},
        jobs=_cp_jobs(jobs),
    )
    logger.info(
        "[输入] 快照结论引用了 jobs 未收录的任务 id "
        "[判定] checkpoint_invalid，组不创建结论、位点不变"
    )
    with pytest.raises(QueueError) as exc:
        q.restore_checkpoint(ghost)
    assert exc.value.code is ErrorCode.CHECKPOINT_INVALID
    q.close()


def test_v1_checkpoint_still_restorable_when_jobs_present(settings) -> None:
    """旧版 v1 快照（不含 jobs）恢复进已有所引任务的库：仍然可用。"""
    from jobqueue.checkpoint import checkpoint_from_dict

    q = JobQueue(settings)
    q.create_group("g")
    j = q.enqueue({"n": 1})
    v1 = checkpoint_from_dict(
        {
            "format": "jobqueue-checkpoint",
            "version": 1,
            "group": "g",
            "offset": 1,
            "created_at": 1.0,
            "conclusions": {j.id: "succeeded"},
        }
    )
    assert v1.jobs == []
    offset = q.restore_checkpoint(v1)
    logger.info(
        "[输入] v1 旧快照（无 jobs 字段）恢复，且所引任务已在库中 "
        "[判定] 向后兼容恢复成功，位点=1，结论生效"
    )
    assert offset.offset == 1
    assert q.job_group_statuses(j.id)["g"] == "succeeded"
    q.close()


def test_restore_takes_over_inflight_delivery(queue) -> None:
    """恢复结论时接管目标组未确认的在途投递：旧消费者的迟到确认不能翻回状态。"""
    q = JobQueue(queue.settings)
    q.create_group("h")
    jobs = [q.enqueue({"m": i}) for i in range(2)]
    stale = q.claim("h", "old-c")  # 第一条任务在 h 组在途
    cp = Checkpoint(
        group="h",
        offset=1,
        created_at=0.0,
        conclusions={stale.job_id: "succeeded"},
        jobs=_cp_jobs(jobs),
    )
    q.restore_checkpoint(cp)
    logger.info(
        "[输入] 快照把 h 组在途任务定为 succeeded 后，原消费者 old-c 迟到 ack "
        "[判定] 旧投递已被接管为 timed_out，迟到 ack 返回 delivery_terminal，"
        "h 组结论保持 succeeded"
    )
    assert q.job_group_statuses(stale.job_id)["h"] == "succeeded"
    with pytest.raises(QueueError) as exc:
        q.ack(stale.id, "old-c")
    assert exc.value.code is ErrorCode.DELIVERY_TERMINAL
    assert q.job_group_statuses(stale.job_id)["h"] == "succeeded"
    assert q.list_deliveries(stale.job_id)[-1].state == "timed_out"
    q.close()


def test_restore_dedupe_key_clash_rejected(settings) -> None:
    """快照任务的 dedupe_key 被库里另一个任务占用：身份冲突，整体拒绝。"""
    q = JobQueue(settings)
    clash_job = q.enqueue({"x": 1}, dedupe_key="k-1")
    snap_jobs = [
        CheckpointJob(
            id="snap-job-1",
            dedupe_key="k-1",
            payload={"x": 2},
            max_retries=3,
            created_at=1.0,
        )
    ]
    cp = Checkpoint(
        group="g",
        offset=1,
        created_at=0.0,
        conclusions={"snap-job-1": "succeeded"},
        jobs=snap_jobs,
    )
    logger.info(
        "[输入] 快照任务 id=snap-job-1 使用的 dedupe_key 已被库内 %s 占用 "
        "[判定] checkpoint_invalid，库内任务不被改动",
        clash_job.id,
    )
    with pytest.raises(QueueError) as exc:
        q.restore_checkpoint(cp)
    assert exc.value.code is ErrorCode.CHECKPOINT_INVALID
    assert q.find_job("snap-job-1") is None
    assert q.find_job(clash_job.id) is not None
    q.close()
