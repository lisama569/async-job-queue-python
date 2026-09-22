"""错误码与可区分的队列异常。"""

from __future__ import annotations

from enum import Enum


class ErrorCode(str, Enum):
    INVALID_PAYLOAD = "invalid_payload"
    DUPLICATE_JOB = "duplicate_job"
    EMPTY_QUEUE = "empty_queue"
    DELIVERY_NOT_FOUND = "delivery_not_found"
    DELIVERY_TERMINAL = "delivery_terminal"
    DELIVERY_EXPIRED = "delivery_expired"
    DELIVERY_OWNER_MISMATCH = "delivery_owner_mismatch"
    RETRIES_EXHAUSTED = "retries_exhausted"
    UNPROCESSABLE_JOB = "unprocessable_job"
    OFFSET_ROLLBACK = "offset_rollback"
    OFFSET_OUT_OF_RANGE = "offset_out_of_range"
    GROUP_NOT_FOUND = "group_not_found"
    JOB_NOT_FOUND = "job_not_found"


class QueueError(Exception):
    def __init__(self, code: ErrorCode, message: str):
        self.code = code
        self.message = message
        super().__init__(message)
