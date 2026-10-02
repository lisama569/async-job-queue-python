# 本地异步任务队列（Python / FastAPI / SQLite）

在现有 FastAPI 服务上实现的一套**本地持久化异步任务队列**：用 SQLite（WAL 模式）
模拟消息代理，无需任何外部服务、真实凭据或付费资源，仓库内一条命令即可运行与验证。

- 任务入队（`dedupe_key` 幂等去重）、投递（claim）、确认（ack / nack）
- 消费者组：组间相互独立、组内竞争消费；每组建组后可看到历史任务
- 位点（offset）管理：只能严格前进，回退 / 原地提交 / 越界一律明确拒绝
- **位点与处理进度强对齐**：位点不能跳过本组仍在途 / 未投递（未定论）的任务，
  提交到「连续定论前缀」之后会被明确拒绝（`offset_gap`）
- **进度对账**：随时导出某组的位点、安全位点（safe_offset）与成功 / 失败 / 拒绝 /
  在途 / 退避中 / 待处理各桶任务清单，供运维对账
- **组间互不干扰**：某组一旦对任务有最终结论即「终态粘滞」，其它组再投递 / 确认
  也无法把它改回在途或待处理；退避按**组**独立计时，A 组退避不拖住 B 组取件
- **检查点快照**：把某组对账结果导出为持久化 JSON，重启或换一个消费进程后据此接着
  消费——已定论不重放、在途按至少一次重投、未完成不跳过，且只影响该组
- at-least-once 投递 + 占用超时自动回收重投 + 可控指数退避重试
- 同一任务在同一组内同一时刻只能被一个消费者占用
- 超过重试上限 → `failed` 终态；内容不可处理 → `rejected` 终态；均可查询原因
- 进程重启后任务、投递与位点全部从 SQLite 恢复，任务不丢失、不重复完成

## 快速开始

```bash
uv sync                                   # 安装依赖（含 pytest / httpx）
uv run python -m jobqueue.cli demo        # 端到端演示：成功 / 重试耗尽 / 拒绝
uv run python -m jobqueue.cli serve       # 启动 HTTP 服务（默认 127.0.0.1:8000）
uv run pytest -q                          # 运行全部测试（60+ 个，含并发/重启/对账/检查点）
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
| GET    | `/groups/{g}/progress`        | 进度对账：位点/安全位点/五类任务桶清单   |
| GET    | `/groups/{g}/jobs`            | 列出该组每个任务的组内定论               |
| GET    | `/groups/{g}/checkpoint`      | 导出该组检查点快照（JSON）               |
| POST   | `/groups/{g}/checkpoint`      | 按快照恢复该组进度（body 为快照 JSON）   |
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
- **组间独立 / 终态粘滞**：每个组对每个任务各有一份投递账本（`group_jobs`），
  A 组占用 / 失败不影响 B 组消费。任务一旦在**任一组**有了最终结论，其全局状态即
  定格为终态（多组都终态时 succeeded 优先于 rejected 优先于 failed），其它组之后的
  投递 / 退避都不会把它改回 inflight / retrying / pending；每个组仍保留各自独立的
  组内结论，可通过 `GET /jobs/{id}` 的 `group_views` 或库接口
  `get_job_group_views` 分别查看。
- **退避按组独立**：退避到期时间记录在每组账本（`group_jobs.available_at`），
  A 组某任务失败退避时，B 组对同一任务照常 claim；同组内退避任务也不会挡住该组
  对后续任务的取件。
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

## 消费者组、进度对账与位点规则

- 位点是**组级游标**，表示"该组已处理到第几条"（从 0 开始），与投递确认相互独立：
  位点是游标记账，ack 是投递语义，二者互不自动推进。
- 位点必须与该组的**实际处理进度对得上**。系统按入队顺序维护一个「连续定论前缀」，
  其长度即对账结果里的 `safe_offset`：只有 `succeeded / failed / rejected`
  才算定论，`inflight / retrying / pending` 都算未定论。
- 提交位点会被逐条校验，四种情况分别识别、明确拒绝：
  - `new_offset <= current`（原地提交或回退）→ `409 offset_rollback`；
  - `new_offset > 队列任务总数`（越过队尾）→ `409 offset_out_of_range`；
  - `new_offset > safe_offset`（跳过本组仍在途 / 退避中 / 未投递的任务，
    出现"位点到了后面、前面还有没结果的任务"）→ `409 offset_gap`，
    错误信息会给出第一个缺口任务及其状态；
  - 对不存在的组提交 → `404 group_not_found`。
- `GET /groups/{g}/progress` 返回该组的完整对账结果：当前位点 `offset`、
  安全位点 `safe_offset`、任务总数 `total`，以及六个任务桶的**数量与具体任务 id**：
  `succeeded / failed / rejected / inflight / retrying / pending`
  （退避中的 `retrying` 单独成桶，便于运维区分"在处理"与"等待重投"）。
- `GET /groups/{g}/jobs` 按入队顺序列出该组每个任务的组内状态、投递次数与占用 id；
  `GET /jobs/{id}` 的 `group_views` 字段给出同一任务在**各个组**的结论。
- 位点提交在 `BEGIN IMMEDIATE` 事务内做比较与更新，并发提交不会使位点回退或越过缺口。
- 组是惰性可见的：新注册（或首次 claim）的组通过 `group_jobs` 懒补齐看到建组之前
  已存在的任务。

## 检查点（快照续消费）

- `GET /groups/{g}/checkpoint` 导出该组当前对账结果的自描述 JSON 快照
  （`version / group / offset / created_at / jobs[]`），每个任务带其组内定论、
  投递次数、退避到期时间与 payload；库接口可用 `queue.save_checkpoint(g, path)`
  原子落盘（临时文件 + `os.replace`）。
- 恢复（`POST /groups/{g}/checkpoint` 或 `queue.load_and_restore_checkpoint(path)`）
  语义：
  - **已定论不重放**：快照中 `succeeded/failed/rejected` 的任务补写为该组终态，
    claim 不会再取到；
  - **未完成不跳过**：`pending` 保持待处理，`retrying` 保留退避到期时间；
    导出时仍 `inflight` 的任务（原消费者可能已失联）规范化为 `pending` 重新投递，
    保持至少一次语义；
  - **终态不回退**：目标组已经是终态的条目不会被任何快照改回非终态；
  - **只影响快照所属组**：其它组的账本与位点一行不动；路径组与快照 `group`
    不一致会被 `400 checkpoint_invalid` 拒绝；
  - **位点自动钳制**：恢复后位点取「库内当前位点」与「恢复后连续定论前缀」的合法
    值，绝不会出现位点在前、定论在后留有缺口；快照可恢复到同库（重启）或一个全新库
    （换消费进程；缺失任务按快照 payload 补建，`dedupe_key` 冲突会拒绝）。
  - 损坏 JSON / 缺字段 / 重复任务 id / 不支持的版本号一律返回
    `400 checkpoint_invalid`，当前快照版本为 `version=1`。

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
| `offset_gap`                 | 409  | 位点跳过本组仍在途/退避/未投递的任务      |
| `checkpoint_invalid`         | 400  | 检查点损坏/版本不符/组不一致/字段非法     |
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

进度对账与检查点续消费：

```python
# 运维对账：查看某组位点与各类任务
progress = queue.group_progress("emails")
print(progress.offset, progress.safe_offset, progress.total)
print(progress.succeeded, progress.failed, progress.rejected,
      progress.inflight, progress.retrying, progress.pending)
# 位点只能推进到连续定论前缀；任务都定论后可提交到 safe_offset
queue.commit_offset("emails", progress.safe_offset)

# 某任务在各组的结论
for view in queue.get_job_group_views(job.id):
    print(view.group, view.status, view.attempts)

# 导出检查点 -> 落盘 -> 换进程（或换新库）恢复接着消费
queue.save_checkpoint("emails", "emails.ckpt.json")
# ... 另一个进程 / 另一份 SQLite ...
progress = queue.load_and_restore_checkpoint("emails.ckpt.json")
```

### 兼容范围

- 数据库文件向后兼容：旧版库（`group_jobs` 无 `status / attempts / available_at`
  列）首次打开时自动幂等迁移；旧的全局 `jobs.available_at` 退避时间会回填到每组
  账本，无需人工干预（见 `tests/test_migration.py`）。
- 检查点快照仅承诺当前主版本（`version=1`）可读；未来不兼容格式会提升版本号，
  旧版本恢复时明确报 `checkpoint_invalid`，不会静默错恢复。
- 位点提交语义较上一版更严格：过去位点可在不处理任务时任意前进，现在必须对齐连续
  定论前缀；调用方应在任务定论（ack / nack 到终态）后，按 `safe_offset` 提交。
- at-least-once 投递、占用超时重投、组内唯一占用、投递账本等既有语义保持不变。

## 测试覆盖

`uv run pytest -q` 覆盖：

- 入队幂等 / 重复提交 / 非法 payload（`tests/test_enqueue.py`）
- 占用、重复 ack、越权 ack、超时 ack 与重投（`tests/test_delivery.py`）
- 退避等待、重试上限、failed/rejected 原因可区分（`tests/test_retry.py`）
- 组间独立、新组看历史、位点回退 / 越界 / 未知组拒绝（`tests/test_groups.py`）
- 进度对账五类任务桶与 safe_offset、位点跳过在途 / 未投递 / retrying 的
  `offset_gap` 拒绝、failed/rejected 算定论（`tests/test_progress.py`）
- 组间隔离：终态粘滞不被改回、退避按组互不牵连、各组按自己节奏推进、
  单任务跨组结论可分别查询（`tests/test_group_isolation.py`）
- 检查点：快照形状、换新库续消费、已定论不重放、在途重投、retrying 保留退避、
  只影响本组、位点钳制、终态不降级、损坏 / 版本不符拒绝（`tests/test_checkpoint.py`）
- 旧库迁移：补 status/attempts 列、退避时间回填到每组账本（`tests/test_migration.py`）
- 进程重启恢复、崩溃后超时重投、终态不复活（`tests/test_restart.py`）
- 8 消费者并发 claim 每任务仅一次、4 worker 并发含永久失败、位点并发竞争
  （`tests/test_concurrency.py`）
- 数量守恒、投递账本闭合、终态不可逆（`tests/test_invariants.py`）
- 全部 HTTP 状态码与错误码（`tests/test_api.py`）

测试与 worker 日志均打印 `[输入]` 与 `[判定] 依据`，可用
`uv run pytest --log-cli-level=INFO` 查看判定过程。
