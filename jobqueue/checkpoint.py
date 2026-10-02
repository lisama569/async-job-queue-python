"""消费者组检查点的 JSON 序列化 / 反序列化与文件落盘。

快照为一份自描述 JSON（含版本号、组名、位点、每个任务的组内定论），
可在同库重启或换一个消费进程时用于恢复，恢复语义见
``JobStore.restore_checkpoint``。
"""

from __future__ import annotations

import json
import os
import tempfile
from typing import Any

from jobqueue.errors import ErrorCode, QueueError
from jobqueue.models import (
    CheckpointJob,
    GroupCheckpoint,
    GroupStatus,
)
from jobqueue.store import CHECKPOINT_VERSION

# status 白名单：只接受合法的组账本状态字符串
_VALID_STATUS = {s.value for s in GroupStatus}


def checkpoint_to_dict(checkpoint: GroupCheckpoint) -> dict[str, Any]:
    return {
        "version": checkpoint.version,
        "group": checkpoint.group,
        "offset": checkpoint.offset,
        "created_at": checkpoint.created_at,
        "jobs": [
            {
                "job_id": j.job_id,
                "dedupe_key": j.dedupe_key,
                "payload": j.payload,
                "status": j.status.value,
                "attempts": j.attempts,
                "max_retries": j.max_retries,
                "available_at": j.available_at,
                "created_at": j.created_at,
            }
            for j in checkpoint.jobs
        ],
    }


def _require(cond: bool, message: str) -> None:
    if not cond:
        raise QueueError(ErrorCode.CHECKPOINT_INVALID, message)


def checkpoint_from_dict(data: Any) -> GroupCheckpoint:
    _require(isinstance(data, dict), "检查点文件不是 JSON 对象")
    _require(
        data.get("version") == CHECKPOINT_VERSION,
        f"检查点版本 {data.get('version')!r} 不受支持，"
        f"当前仅支持版本 {CHECKPOINT_VERSION}",
    )
    group = data.get("group")
    _require(isinstance(group, str) and group != "", "检查点缺少消费者组名 group")
    offset = data.get("offset")
    _require(isinstance(offset, int) and offset >= 0, "检查点位点 offset 非法")
    created_at = data.get("created_at", 0.0)
    _require(isinstance(created_at, (int, float)), "检查点 created_at 非法")
    raw_jobs = data.get("jobs", [])
    _require(isinstance(raw_jobs, list), "检查点 jobs 必须是数组")

    jobs: list[CheckpointJob] = []
    seen: set[str] = set()
    for i, item in enumerate(raw_jobs):
        _require(isinstance(item, dict), f"检查点第 {i} 个任务不是对象")
        job_id = item.get("job_id")
        _require(
            isinstance(job_id, str) and job_id != "" and job_id not in seen,
            f"检查点第 {i} 个任务 id 非法或重复：{job_id!r}",
        )
        seen.add(job_id)
        status = item.get("status")
        _require(
            status in _VALID_STATUS,
            f"检查点任务 {job_id} 的状态非法：{status!r}",
        )
        payload = item.get("payload")
        _require(isinstance(payload, dict), f"检查点任务 {job_id} payload 非法")
        dedupe_key = item.get("dedupe_key")
        _require(
            dedupe_key is None or isinstance(dedupe_key, str),
            f"检查点任务 {job_id} dedupe_key 非法",
        )
        attempts = item.get("attempts", 0)
        max_retries = item.get("max_retries", 0)
        available_at = item.get("available_at", 0.0)
        created = item.get("created_at", 0.0)
        _require(isinstance(attempts, int) and attempts >= 0,
                 f"检查点任务 {job_id} attempts 非法")
        _require(isinstance(max_retries, int) and max_retries >= 0,
                 f"检查点任务 {job_id} max_retries 非法")
        _require(isinstance(available_at, (int, float)) and available_at >= 0,
                 f"检查点任务 {job_id} available_at 非法")
        _require(isinstance(created, (int, float)),
                 f"检查点任务 {job_id} created_at 非法")
        jobs.append(
            CheckpointJob(
                job_id=job_id,
                dedupe_key=dedupe_key,
                payload=payload,
                status=GroupStatus(status),
                attempts=attempts,
                max_retries=max_retries,
                available_at=float(available_at),
                created_at=float(created),
            )
        )
    return GroupCheckpoint(
        version=CHECKPOINT_VERSION,
        group=group,
        offset=offset,
        created_at=float(created_at),
        jobs=jobs,
    )


def save_checkpoint(checkpoint: GroupCheckpoint, path: str) -> None:
    """原子写盘：先写临时文件再 os.replace，避免进程崩溃留下半截快照。"""
    directory = os.path.dirname(os.path.abspath(path)) or "."
    fd, tmp_path = tempfile.mkstemp(prefix=".ckpt-", suffix=".tmp", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(
                checkpoint_to_dict(checkpoint),
                fh,
                ensure_ascii=False,
                sort_keys=True,
                indent=2,
            )
            fh.write("\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, path)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def load_checkpoint(path: str) -> GroupCheckpoint:
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
    except FileNotFoundError:
        raise QueueError(
            ErrorCode.CHECKPOINT_INVALID, f"检查点文件不存在：{path}"
        ) from None
    except (OSError, json.JSONDecodeError) as exc:
        raise QueueError(
            ErrorCode.CHECKPOINT_INVALID, f"检查点文件无法解析：{exc}"
        ) from None
    return checkpoint_from_dict(data)
