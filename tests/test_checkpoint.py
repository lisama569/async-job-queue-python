"""检查点：组进度快照的导出、持久化与恢复（重启/换进程接着消费）。"""

from __future__ import annotations

import logging

import pytest

from jobqueue.errors import ErrorCode, QueueError
from jobqueue.config import Settings
from jobqueue.models import Checkpoint, CheckpointJob, JobState
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


def test_checkpoint_is_self_contained_carries_all_jobs(queue, tmp_path) -> None:
    jobs = _prepare(queue)
    cp = queue.export_checkpoint("g", tmp_path / "cp.json")
    logger.info(
        "[输入] 3 条任务（j1 定论、j2 在途、j3 未投递）导出快照 "
        "[判定] 快照按入队顺序携带全部 3 条任务及其 payload / 去重信息，结论只含 j1"
    )
    assert [j.id for j in cp.jobs] == [j.id for j in jobs]
    assert cp.jobs[0].payload == {"n": 0}
    assert [j.seq for j in cp.jobs] == [1, 2, 3]
    assert set(cp.conclusions) == {jobs[0].id}


def test_restore_onto_empty_database_resumes_consumption(settings, tmp_path) -> None:
    """快照恢复进一份完全空白的队列数据，也能照着它接着消费。"""
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    dst_dir = tmp_path / "dst"
    dst_dir.mkdir()
    q1 = JobQueue(
        Settings(
            db_path=str(src_dir / "q.db"),
            visibility_timeout=settings.visibility_timeout,
            max_retries=settings.max_retries,
            base_backoff=0.0,
        )
    )
    q1.create_group("g")
    jobs = [
        q1.enqueue({"n": i}, dedupe_key=f"k{i}") for i in range(4)
    ]
    d = q1.claim("g", "c")
    q1.ack(d.id, "c")  # j1 succeeded
    d = q1.claim("g", "c")
    q1.nack(d.id, "c", "不可处理", unprocessable=True)  # j2 rejected
    q1.claim("g", "c")  # j3 inflight（未确认）——j4 从未投递
    q1.commit_offset("g", 2)
    cp_path = tmp_path / "cp.json"
    q1.export_checkpoint("g", cp_path)
    q1.close()
    logger.info(
        "[输入] 把含 4 条任务（succeeded/rejected/inflight/pending、位点=2）的快照"
        "恢复进全新空库 [判定] 任务原样补齐；j1/j2 不重做；j3/j4 按序投递不丢失；"
        "payload 与去重键保留；位点=2"
    )

    q2 = JobQueue(
        Settings(
            db_path=str(dst_dir / "empty.db"),
            visibility_timeout=settings.visibility_timeout,
            max_retries=settings.max_retries,
            base_backoff=0.0,
        )
    )
    offset = q2.restore_checkpoint(cp_path)
    assert offset.group == "g" and offset.offset == 2
    restored = q2.list_jobs()
    assert [j.id for j in restored] == [j.id for j in jobs]
    assert q2.get_job(jobs[0].id).payload == {"n": 0}
    assert q2.get_job(jobs[2].id).dedupe_key == "k2"
    assert q2.get_job(jobs[0].id).state is JobState.SUCCEEDED
    # 已定论的 j1/j2 不再投出；先拿到未完成的 j3，再拿 j4
    consumed: list[str] = []
    for _ in range(2):
        d = q2.claim("g", "c2")
        assert d is not None
        consumed.append(d.job_id)
        q2.ack(d.id, "c2")
    assert consumed == [jobs[2].id, jobs[3].id]
    assert q2.claim("g", "c2") is None
    p = q2.group_progress("g")
    assert p.offset == 2
    assert set(p.buckets["succeeded"]) == {jobs[0].id, jobs[2].id, jobs[3].id}
    assert p.buckets["rejected"] == [jobs[1].id]
    # 恢复后新入队任务可正常投递（rowid 不冲突）
    j5 = q2.enqueue({"n": 4})
    d = q2.claim("g", "c2")
    assert d is not None and d.job_id == j5.id
    # 空库上的其它组不携带任何结论，从头消费
    d_other = q2.claim("other", "x")
    assert d_other is not None and d_other.job_id == jobs[0].id
    q2.close()


def test_restore_into_new_process_with_new_group_name(settings, tmp_path) -> None:
    """快照搬到另一个消费进程并用新组名接管：不影响快照原组。"""
    dir1 = tmp_path / "p1"
    dir1.mkdir()
    q1 = JobQueue(
        Settings(db_path=str(dir1 / "q.db"), base_backoff=0.0)
    )
    jobs = _prepare(q1, group="old")
    cp_path = tmp_path / "cp.json"
    q1.export_checkpoint("old", cp_path)
    q1.close()
    logger.info(
        "[输入] 另一个进程（全新空库）用新组名 resume 恢复 old 组快照 "
        "[判定] resume 组继承 j1 结论、j2/j3 继续消费；任务与 payload 齐全"
    )
    dir2 = tmp_path / "p2"
    dir2.mkdir()
    q2 = JobQueue(Settings(db_path=str(dir2 / "q.db"), base_backoff=0.0))
    offset = q2.restore_checkpoint(cp_path, group="resume")
    assert offset.group == "resume" and offset.offset == 1
    assert q2.job_group_statuses(jobs[0].id)["resume"] == "succeeded"
    d = q2.claim("resume", "w")
    assert d is not None and d.job_id == jobs[1].id
    q2.ack(d.id, "w")
    d = q2.claim("resume", "w")
    assert d is not None and d.job_id == jobs[2].id
    q2.ack(d.id, "w")
    assert q2.claim("resume", "w") is None
    q2.close()


def test_restore_inflated_offset_is_corrected_to_real_progress(
    queue, tmp_path
) -> None:
    """快照位点虚高（只对账到 1 条却记成 3）：恢复时纠正到真实进度 1。"""
    import time

    jobs = _prepare(queue)  # 仅 j1 定论
    bogus = Checkpoint(
        group="g",
        offset=3,
        created_at=time.time(),
        conclusions={jobs[0].id: "succeeded"},
        jobs=[
            CheckpointJob(id=j.id, payload=j.payload, seq=i + 1)
            for i, j in enumerate(jobs)
        ],
    )
    logger.info(
        "[输入] 伪造位点=3 但只有 j1 有结论的快照，恢复到新组 h "
        "[判定] 位点被纠正为 1（最长连续定论前缀），j2/j3 保持 pending 不丢"
    )
    offset = queue.restore_checkpoint(bogus, group="h")
    assert offset.offset == 1
    p = queue.group_progress("h")
    assert p.offset == 1 and p.buckets["succeeded"] == [jobs[0].id]
    assert set(p.buckets["pending"]) == {jobs[1].id, jobs[2].id}
    d = queue.claim("h", "c")
    assert d is not None and d.job_id == jobs[1].id


def test_restore_offset_corrected_even_without_gap_when_undecided_after(
    queue, tmp_path
) -> None:
    """位点=2 但第 2 条在途：纠正为 1；全部定论时位点维持。"""
    import time

    jobs = _prepare(queue)
    bogus = Checkpoint(
        group="g",
        offset=2,
        created_at=time.time(),
        conclusions={jobs[0].id: "succeeded"},
        jobs=[
            CheckpointJob(id=j.id, payload=j.payload, seq=i + 1)
            for i, j in enumerate(jobs)
        ],
    )
    offset = queue.restore_checkpoint(bogus, group="h")
    logger.info(
        "[输入] 快照位点=2 而第 2 条任务无结论 [判定] 恢复位点=1，不留前虚位点"
    )
    assert offset.offset == 1


def test_restore_offset_beyond_total_jobs_rejected(settings, tmp_path) -> None:
    """快照位点超过恢复后任务总数：明确拒绝，不落任何修改。"""
    import time

    d = tmp_path / "x"
    d.mkdir()
    q = JobQueue(Settings(db_path=str(d / "q.db"), base_backoff=0.0))
    bogus = Checkpoint(
        group="g",
        offset=99,
        created_at=time.time(),
        conclusions={"j1": "succeeded"},
        jobs=[CheckpointJob(id="j1", payload={"n": 1}, seq=1)],
    )
    logger.info(
        "[输入] 空库恢复位点=99 而快照仅 1 条任务 [判定] checkpoint_invalid，"
        "整笔回滚：组与任务均未被创建"
    )
    with pytest.raises(QueueError) as exc:
        q.restore_checkpoint(bogus)
    assert exc.value.code is ErrorCode.CHECKPOINT_INVALID
    assert "越界" in exc.value.message
    assert q.list_groups() == []
    assert q.list_jobs() == []
    q.close()


def test_legacy_checkpoint_without_jobs_still_works_on_matching_db(
    queue, settings, tmp_path
) -> None:
    """旧版快照（无 jobs 字段）：同库可恢复，空库明确拒绝。"""
    import time

    queue.create_group("g")
    job = queue.enqueue({"n": 1})
    legacy = Checkpoint(
        group="g",
        offset=1,
        created_at=time.time(),
        conclusions={job.id: "succeeded"},
        jobs=[],
    )
    logger.info(
        "[输入] 手工构造无 jobs 的旧版快照恢复到含该任务的库 "
        "[判定] 恢复成功、位点=1；恢复进空库则 checkpoint_invalid"
    )
    offset = queue.restore_checkpoint(legacy, group="h")
    assert offset.offset == 1
    assert queue.job_group_statuses(job.id)["h"] == "succeeded"

    q_empty = JobQueue(Settings(
        db_path=str(tmp_path / "empty.db"), base_backoff=0.0
    ))
    with pytest.raises(QueueError) as exc:
        q_empty.restore_checkpoint(legacy)
    assert exc.value.code is ErrorCode.CHECKPOINT_INVALID
    q_empty.close()


def test_restore_malformed_jobs_section_rejected(queue, tmp_path) -> None:
    import json

    base = {
        "format": "jobqueue-checkpoint",
        "version": 1,
        "group": "g",
        "offset": 0,
        "created_at": 1.0,
        "conclusions": {},
    }
    bad_cases = {
        "jobs 不是数组": {**base, "jobs": {}},
        "任务缺 id": {**base, "jobs": [{"payload": {}, "seq": 1}]},
        "payload 非对象": {**base, "jobs": [{"id": "j1", "payload": [], "seq": 1}]},
        "seq 非正整数": {**base, "jobs": [{"id": "j1", "payload": {}, "seq": 0}]},
        "id 重复": {
            **base,
            "jobs": [
                {"id": "j1", "payload": {}, "seq": 1},
                {"id": "j1", "payload": {}, "seq": 2},
            ],
        },
        "结论引用了 jobs 外的任务": {
            **base,
            "conclusions": {"ghost": "succeeded"},
            "jobs": [{"id": "j1", "payload": {}, "seq": 1}],
        },
    }
    for desc, doc in bad_cases.items():
        path = tmp_path / "bad.json"
        path.write_text(json.dumps(doc), encoding="utf-8")
        logger.info("[输入] %s 的快照 [判定] checkpoint_invalid", desc)
        with pytest.raises(QueueError) as exc:
            queue.restore_checkpoint(path)
        assert exc.value.code is ErrorCode.CHECKPOINT_INVALID


def test_restore_dedupe_key_collision_rejected(queue, tmp_path) -> None:
    import time

    existing = queue.enqueue({"n": 1}, dedupe_key="k1")
    bogus = Checkpoint(
        group="g",
        offset=0,
        created_at=time.time(),
        conclusions={},
        jobs=[
            CheckpointJob(
                id="other-id", payload={"n": 2}, dedupe_key="k1", seq=2
            )
        ],
    )
    logger.info(
        "[输入] 快照携带的任务使用了库中已存在的 dedupe_key=k1（属主不同） "
        "[判定] checkpoint_invalid，库内任务不被改动"
    )
    with pytest.raises(QueueError) as exc:
        queue.restore_checkpoint(bogus)
    assert exc.value.code is ErrorCode.CHECKPOINT_INVALID
    assert queue.get_job(existing.id).dedupe_key == "k1"
    assert queue.find_job("other-id") is None


def test_restore_offset_never_rewinds_after_correction(queue, tmp_path) -> None:
    """当前组位点已真实推进到 2 时，虚高旧快照不会把位点拉回。"""
    import time

    jobs = _prepare(queue)  # g: j1 定论、j2 在途、j3 pending，位点 1
    d = queue.list_deliveries(jobs[1].id)[-1]
    queue.ack(d.id, "c2")  # j2 定论
    queue.commit_offset("g", 2)
    stale = Checkpoint(
        group="g",
        offset=1,
        created_at=time.time(),
        conclusions={jobs[0].id: "succeeded"},
        jobs=[
            CheckpointJob(id=j.id, payload=j.payload, seq=i + 1)
            for i, j in enumerate(jobs)
        ],
    )
    logger.info(
        "[输入] 组位点已真实推进到 2，恢复只支撑到 1 的旧快照 "
        "[判定] 纠正值 1 不回退既有位点，恢复后仍为 2"
    )
    offset = queue.restore_checkpoint(stale)
    assert offset.offset == 2

