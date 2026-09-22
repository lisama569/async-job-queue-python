# 本地异步任务队列（Python / FastAPI / SQLite）

在现有 FastAPI 服务上实现的一套**本地持久化异步任务队列**：用 SQLite（WAL 模式）
模拟消息代理，无需任何外部服务、真实凭据或付费资源，仓库内一条命令即可运行与验证。

- 任务入队（`dedupe_key` 幂等去重）、投递（claim）、确认（ack / nack）
- 消费者组：组间相互独立、组内竞争消费；每组建组后可看到历史任务
- 位点（offset）管理：只能严格前进，回退 / 原地提交 / 越界一律明确拒绝
- at-least-once 投递 + 占用超时自动回收重投 + 可控指数退避重试
- 同一任务在同一组内同一时刻只能被一个消费者占用
- 超过重试上限 → `failed` 终态；内容不可处理 → `rejected` 终态；均可查询原因
- 进程重启后任务、投递与位点全部从 SQLite 恢复，任务不丢失、不重复完成

## 快速开始

```bash
uv sync                                   # 安装依赖（含 pytest / httpx）
uv run python -m jobqueue.cli demo        # 端到端演示：成功 / 重试耗尽 / 拒绝
uv run python -m jobqueue.cli serve       # 启动 HTTP 服务（默认 127.0.0.1:8000）
uv run pytest -q                          # 运行全部测试（36 个，含并发/重启边界）
```

## HTTP API

| 方法   | 路径                          | 说明                                     |
|--------|-------------------------------|------------------------------------------|
| POST   | `/jobs`                       | 入队，body：`{"payload": {...}, "dedupe_key": "可选"}` |
| GET    | `/jobs?state=failed`          | 列出任务，可按状态过滤                   |
| GET    | `/jobs/{id}`                  | 查询任务（含 `last_error` 与投递历史）   |
| POST   | `/groups`                     | 注册消费者组 `{"group": "g1"}`（claim 时也会自动注册）|
| GET    | `/groups`                     | 列出消费者组                             |
| GET    | `/groups/{g}/offset`          | 查询位点                                 |
| POST   | `/groups/{g}/offset`          | 提交位点 `{"offset": n}`                 |
| POST   | `/claim`                      | 占用一条 `{"group","consumer","visibility_timeout"?}` |
| POST   | `/deliveries/{id}/ack`        | 确认成功 `{"consumer"}`                  |
| POST   | `/deliveries/{id}/nack`       | 否定确认 `{"consumer","reason","reject"?}`|
| GET    | `/health`                     | 健康检查                                 |

### 一分钟上手（curl）

```bash
curl -s -XPOST localhost:8000/jobs -H 'content-type: application/json' \
  -d '{"payload":{"action":"send_email"},"dedupe_key":"email-1"}'
curl -s -XPOST localhost:8000/claim -H 'content-type: application/json' \
  -d '{"group":"mailers","consumer":"worker-1"}'
# -> {"delivery":{"id":1,...},"job":{...}}
curl -s -XPOST localhost:8000/deliveries/1/ack -H 'content-type: application/json' \
  -d '{"consumer":"worker-1"}'
```

## 投递语义

- **至少一次（at-least-once）**：成功处理以 ack 为准；未确认的投递会在占用超时后
  被回收并重新投递，因此任务可能被投递多次。
- **不重复完成**：每次投递生成全局唯一 `delivery_id`，只有处于 `active` 且**未超时**
  的投递、且由**占用它的消费者**才能确认。重复 ack、越权 ack、超时后 ack 分别返回
  `delivery_terminal` / `delivery_owner_mismatch` / `delivery_expired`，
  任务状态不会被二次翻转。
- **入队幂等**：相同 `dedupe_key` 第二次入队返回 `409 duplicate_job`（错误信息带回
  既有任务 id），不会产生第二条任务；不带 `dedupe_key` 时即使 payload 相同也视为两条任务。
- **不可静默丢弃**：nack 必须携带非空 `reason`；`reject=true` 表示任务内容本身
  无法处理，立即进入 `rejected` 终态；其它异常进入重试，超限后进入 `failed` 终态。
  `last_error` 始终落盘，可通过 `GET /jobs/{id}` 区分失败原因。
- **组间独立**：每个组对每个任务各有一份投递账本（`group_jobs`），A 组占用 / 失败
  不影响 B 组消费；`jobs.state` 是所有组分发状态的汇总视图
  （有 inflight → inflight；有 retrying → retrying；全终态时 succeeded 优先于
  rejected 优先于 failed）。
- **组内唯一占用**：`group_jobs.delivery_id` 非空即表示被占用，claim 在单个
  `BEGIN IMMEDIATE` 事务内完成「回收超时 → 选取 → 建投递 → 占用」，多线程并发下
  同一任务也只会被一个消费者拿到。

## 状态流转

```
                 入队校验失败（存储层直接拒绝，不落库）
                          │ invalid_payload（400）
                          ▼
 enqueue ──► pending ──claim──► inflight ──ack──────────────► succeeded（终态）
                ▲                │  │
                │                │  ├─nack(reject=true)──────► rejected（终态）
                │                │  ├─nack（仍有重试额度）────► retrying
                │                │  └─占用超时（仍有额度）────► retrying
                └────退避到期────┘
                                 ├─nack / 占用超时（额度耗尽）► failed（终态）
```

- 终态（`succeeded` / `failed` / `rejected`）不可逆，不会被再次投递。
- 投递（delivery）另有独立账本状态：`active → acked | nacked | timed_out`，
  任务的完整投递历史可在 `GET /jobs/{id}` 中看到。
- 重试额度 = `max_retries`：首次投递 + 最多 `max_retries` 次重投，
  即总投递次数达到 `max_retries + 1` 仍未成功则 `failed`。

## 重试与退避

- 第 `attempt` 次投递失败后的等待时间：
  `delay = min(backoff_cap, base_backoff * backoff_multiplier ** (attempt - 1))`
  ，可选加 `jitter` 范围内的随机抖动。
- 退避窗口内任务状态为 `retrying`、`available_at > now`，claim 不会提前取出。
- 占用超时（visibility timeout）：消费者宕机 / 忘记 ack 时，下一次 claim 会把
  `visible_after` 已到期的 active 投递标记为 `timed_out` 并按同一退避规则重投；
  旧消费者之后迟到的 ack/nack 会被 `delivery_expired` 拒绝，避免"僵尸确认"。

## 消费者组与位点规则

- 位点是**组级游标**，表示"已处理到第几条"（从 0 开始），与投递确认相互独立：
  位点是游标记账，ack 是投递语义，二者互不自动推进。
- 提交位点必须满足：`new_offset > current_offset`（严格前进；原地提交与回退返回
  `409 offset_rollback`），且 `new_offset <= 任务总数`（越界返回
  `409 offset_out_of_range`）；对不存在的组提交返回 `404 group_not_found`。
- 位点提交在 `BEGIN IMMEDIATE` 事务内做比较与更新，并发提交不会使位点回退。
- 组是惰性可见的：新注册（或首次 claim）的组通过 `group_jobs` 懒补齐看到建组之前
  已存在的任务。

## 错误码速查

| 错误码                       | HTTP | 含义                                     |
|------------------------------|------|------------------------------------------|
| `invalid_payload`            | 400  | payload 非法 / 参数为空 / visibility_timeout 非正 / nack 无 reason |
| `duplicate_job`              | 409  | dedupe_key 冲突，任务不会重复创建        |
| `delivery_terminal`          | 409  | 投递已 ack/nack/timed_out，禁止重复确认  |
| `delivery_owner_mismatch`    | 409  | 非占用消费者确认，拒绝                   |
| `delivery_expired`           | 409  | 占用已超时，投递已被回收                 |
| `offset_rollback`            | 409  | 位点回退或原地提交                       |
| `offset_out_of_range`        | 409  | 位点越过任务总数                         |
| `unprocessable_job`          | 422  | 任务内容不可处理（worker 层 RejectJob）  |
| `job_not_found` / `delivery_not_found` / `group_not_found` | 404 | 资源不存在 |
| `empty_queue`                | 204  | claim 时组内暂无可投递任务               |
| `retries_exhausted`          | 409  | 预留给客户端的重试耗尽判定（终态可查询） |

## 配置项（环境变量）

| 环境变量                              | 默认值    | 说明                                    |
|---------------------------------------|-----------|-----------------------------------------|
| `JOBQUEUE_DB_PATH`                    | `queue.db`| SQLite 文件路径                         |
| `JOBQUEUE_VISIBILITY_TIMEOUT`         | `30.0`    | 占用超时秒数（claim 可逐次覆盖）        |
| `JOBQUEUE_MAX_RETRIES`                | `3`       | 最大重试次数（总投递 = max_retries + 1）|
| `JOBQUEUE_BASE_BACKOFF`               | `1.0`     | 退避基数（秒）                          |
| `JOBQUEUE_BACKOFF_MULTIPLIER`         | `2.0`     | 退避倍数                                |
| `JOBQUEUE_BACKOFF_CAP`                | `60.0`    | 退避上限（秒）                          |
| `JOBQUEUE_JITTER`                     | `0.0`     | 随机抖动上限（秒），0 关闭              |
| `JOBQUEUE_POLL_INTERVAL`              | `0.1`     | worker 空闲轮询间隔（秒）               |

存储层固定开启 `PRAGMA journal_mode=WAL`、`busy_timeout=5000`，所有写操作在进程内
互斥锁 + 立即事务中执行，保证崩溃时 WAL 检查点恢复与并发写入不撕裂。

## 在代码中作为库使用

```python
from jobqueue.config import Settings
from jobqueue.queue import JobQueue
from jobqueue.worker import RejectJob, Worker

queue = JobQueue(Settings(db_path="queue.db", visibility_timeout=30))
queue.create_group("emails")
job = queue.enqueue({"to": "a@example.com"}, dedupe_key="email-1")

def send_email(payload):
    if "to" not in payload:
        raise RejectJob("缺少收件人")      # -> rejected 终态
    ...                                  # 正常返回 -> ack；抛其它异常 -> 退避重试

Worker(queue, "emails", "worker-1", send_email).run()
```

## 测试覆盖

`uv run pytest -q` 覆盖：

- 入队幂等 / 重复提交 / 非法 payload（`tests/test_enqueue.py`）
- 占用、重复 ack、越权 ack、超时 ack 与重投（`tests/test_delivery.py`）
- 退避等待、重试上限、failed/rejected 原因可区分（`tests/test_retry.py`）
- 组间独立、新组看历史、位点回退 / 越界 / 未知组拒绝（`tests/test_groups.py`）
- 进程重启恢复、崩溃后超时重投、终态不复活（`tests/test_restart.py`）
- 8 消费者并发 claim 每任务仅一次、4 worker 并发含永久失败、位点并发竞争
  （`tests/test_concurrency.py`）
- 数量守恒、投递账本闭合、终态不可逆（`tests/test_invariants.py`）
- 全部 HTTP 状态码与错误码（`tests/test_api.py`）

测试与 worker 日志均打印 `[输入]` 与 `[判定] 依据`，可用
`uv run pytest --log-cli-level=INFO` 查看判定过程。
