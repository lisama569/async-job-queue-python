# 本地异步任务队列（Python / FastAPI / SQLite）

在现有 FastAPI 服务上实现的一套**本地持久化异步任务队列**：用 SQLite（WAL 模式）
模拟消息代理，无需任何外部服务、真实凭据或付费资源，仓库内一条命令即可运行与验证。

- 任务入队（`dedupe_key` 幂等去重）、投递（claim）、确认（ack / nack）
- 消费者组：组间相互独立、组内竞争消费；每组建组后可看到历史任务
- 位点（offset）管理：只能严格前进，回退 / 原地提交 / 越界 / 跳过未定论任务
  一律明确拒绝，位点始终与组内实际处理进度对齐
- 进度对账：按组导出位点 + 各状态（成功/失败/拒绝/在途/待处理/退避中）任务清单
- 检查点：组进度导出为**自足** JSON 快照（v2，含全量任务），重启、换消费进程
  甚至恢复进空队列数据都能据此接着消费，已定论任务不重做、未做完任务不跳过、
  其它组不受影响；恢复位点自动与该组真实进度对齐（虚高/越界会被纠正）
- at-least-once 投递 + 占用超时自动回收重投 + 可控指数退避重试（退避按组隔离）
- 同一任务在同一组内同一时刻只能被一个消费者占用
- 超过重试上限 → `failed` 终态；内容不可处理 → `rejected` 终态；均可查询原因
- 进程重启后任务、投递与位点全部从 SQLite 恢复，任务不丢失、不重复完成

## 快速开始

```bash
uv sync                                   # 安装依赖（含 pytest / httpx）
uv run python -m jobqueue.cli demo        # 端到端演示：成功 / 重试耗尽 / 拒绝
uv run python -m jobqueue.cli serve       # 启动 HTTP 服务（默认 127.0.0.1:8000）
uv run pytest -q                          # 运行全部测试（72 个，含并发/重启/检查点边界）
```

## HTTP API

| 方法   | 路径                          | 说明                                     |
|--------|-------------------------------|------------------------------------------|
| POST   | `/jobs`                       | 入队，body：`{"payload": {...}, "dedupe_key": "可选"}` |
| GET    | `/jobs?state=failed`          | 列出任务，可按状态过滤                   |
| GET    | `/jobs/{id}`                  | 查询任务（含 `last_error`、投递历史、各组结论 `groups`）|
| POST   | `/groups`                     | 注册消费者组 `{"group": "g1"}`（claim 时也会自动注册）|
| GET    | `/groups`                     | 列出消费者组                             |
| GET    | `/groups/{g}/offset`          | 查询位点                                 |
| POST   | `/groups/{g}/offset`          | 提交位点 `{"offset": n}`                 |
| GET    | `/groups/{g}/progress`        | 进度对账：位点 + 各状态任务计数与清单    |
| POST   | `/groups/{g}/checkpoint`      | 导出检查点 `{"path": "可选落盘路径"}`    |
| POST   | `/checkpoints/restore`        | 恢复检查点 `{"path", "group": "可选改名"}`|
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
  不影响 B 组消费；任务对外的全局 `jobs.state` 是所有组分发状态的汇总视图。
- **终态不被改回**：某组对任务一旦得出终态结论（succeeded / failed / rejected），
  全局状态即**锁定在终态**——其它组随后的领取（claim）、失败重试（nack/超时回收）
  或确认都只写自己的账本，不会再把全局状态改回 inflight / retrying / pending。
  存在多个终态结论时按 succeeded 优先于 rejected 优先于 failed 汇总（后续更优
  结论可以提升全局状态，但不会回落到非终态）；各组自己的结论始终可通过
  `GET /jobs/{id}` 的 `groups` 字段分别查询。
- **退避按组隔离**：失败后的退避等待时间记录在 `group_jobs.available_at`（组级），
  A 组退避只挡住 A 组自己对该任务的投递，B 组照常取件、按自己的节奏推进。
- **按组查询结论**：`GET /jobs/{id}` 的 `groups` 字段（或库接口
  `queue.job_group_statuses(job_id)`）给出该任务在每个组内的处理结论。
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
- 提交位点必须满足以下全部条件，任一不满足即明确拒绝、位点不变：
  1. `new_offset > current_offset`——严格前进，原地提交与回退返回
     `409 offset_rollback`；
  2. `new_offset <= 任务总数`——越界返回 `409 offset_out_of_range`；
  3. 按入队顺序的前 `new_offset` 条任务在**该组内**必须全部已有终态结论
     （succeeded / failed / rejected）——跳过仍在途、退避中或尚未投递的任务
     返回 `409 offset_gap`，错误信息会指认未定论的任务 id。
  对不存在的组提交返回 `404 group_not_found`。
- 位点提交在 `BEGIN IMMEDIATE` 事务内做比较与更新，并发提交不会使位点回退。
- 组是惰性可见的：新注册（或首次 claim）的组通过 `group_jobs` 懒补齐看到建组之前
  已存在的任务。

## 进度对账与检查点

### 进度对账（reconciliation）

`GET /groups/{g}/progress`（库接口 `queue.group_progress(g)`）导出某组当前的对账结果：

```json
{
  "group": "g", "offset": 2, "total_jobs": 4,
  "counts": {"pending": 1, "inflight": 1, "retrying": 0,
             "succeeded": 1, "failed": 0, "rejected": 1},
  "jobs":  {"succeeded": ["<id1>"], "rejected": ["<id2>"], ...}
}
```

- 每个状态桶给出**任务 id 清单**（按入队顺序），运维可逐条对账；
- 由于位点提交受 `offset_gap` 约束，位点 ≤ 已定论任务数恒成立，
  不会出现"位点显示处理到后面、前面其实还有没结果的任务"。

### 检查点（checkpoint）

- **导出**：`POST /groups/{g}/checkpoint`（库接口
  `queue.export_checkpoint(g, path)`）把该组当前对账结果固化为一份**自足的**
  JSON 快照（v2：位点 + 该组所有终态结论 + 导出时队列内的**全部任务**，
  按入队顺序收录，含 `id / dedupe_key / payload / max_retries / created_at`），
  给定 `path` 时原子落盘（先写临时文件再改名）。
- **恢复**：`POST /checkpoints/restore`（库接口
  `queue.restore_checkpoint(path_or_cp, group=None)`）把快照应用到目标组
  （默认快照原组名，可改名，用于换消费进程接管）。**快照可以跨进程、
  甚至恢复进一份完全空的队列数据**——恢复时会把目标库缺失的快照任务照单
  物料化（保持入队顺序与原 id / dedupe_key）：
  - 快照中的终态结论写入目标组账本——已定论任务**不会再被投递执行**；
    目标组此前若有未确认的在途投递，恢复时统一接管为 `timed_out`，
    旧消费者之后的迟到 ack/nack 会被投递账本拒绝，无法把快照结论翻回在途；
  - 快照未定论的任务（包括导出时在途 / 退避 / 未投递的）在目标组统一保持
    pending——**不会被跳过也不会丢失**，恢复后照常按序 claim；
  - 只写目标组的账本，**其它组的进度不受影响**（目标库中原有的任务和组
    也原样保留；接管组按"新组可见历史任务"的惰性规则同样能看到它们）；
  - **恢复位点与该组真实进度严格对齐**：恢复位点 = 夹取后的
    `min(max(当前位点, 快照位点), 恢复后按入队顺序的终态前缀长度)`，
    因此它不回退、不超前、也不可能超过当前队列任务总数。快照位点虚高
    （比如实际只对账到 1 条却记成 3）时会被**自动纠正**到真实能支撑的
    位置，并在服务日志中留下 `[检查点纠正]` 记录，绝不留下
    "位点显示处理到后面、前面其实还没结果"的状态；
  - 目标组已有**不同**终态结论时整体拒绝（`409 checkpoint_conflict`，不落
    任何修改）；快照结构非法、结论引用了 jobs 与目标库中都不存在的任务、
    或快照任务与库内任务身份冲突（同 id 不同 dedupe_key，或 dedupe_key
    被别的任务占用）时返回 `400 checkpoint_invalid`；
  - 恢复是幂等的：同一快照重复恢复结果一致；
  - 旧版 v1 快照（只有结论、不含 jobs）仍可读取与恢复，但只能恢复进
    已包含其所引任务的队列数据；新导出的快照一律为自足的 v2。
- 恢复后原有的至少一次投递、占用超时重投语义不变：快照只标记"已定论"，
  在途 / 待处理任务仍走正常的 claim → ack/nack → 超时回收流程。

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
| `offset_gap`                 | 409  | 位点跳过该组尚未定论（在途/退避/未投递）的任务 |
| `checkpoint_invalid`         | 400  | 检查点文件损坏、格式不符或引用未知任务   |
| `checkpoint_conflict`        | 409  | 检查点与目标组现有终态结论冲突           |
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

## 兼容范围

- **旧库自动迁移**：早期版本创建的 SQLite 文件打开时幂等补列
  （`group_jobs.status / attempts / available_at`），并按旧字段回填，
  任务、投递与位点不丢失。
- **行为收紧（需知晓）**：位点提交新增 `offset_gap` 校验——目标位点覆盖的任务
  在该组内必须全部已有终态结论。此前允许"先记账后处理"的调用方需要改为
  先确认处理结论、再提交位点。
- **退避语义修正**：失败退避由全局（`jobs.available_at`）改为组级
  （`group_jobs.available_at`），某组退避不再拖住其它组；`jobs.available_at`
  仍维护为各组未终结账本的最小值，仅作汇总展示。
- 检查点快照为自描述 JSON（`format=jobqueue-checkpoint`）。当前导出 **version=2**：
  除位点与终态结论外还收录导出时的全部任务，**可跨进程、跨机器搬运，也能恢复进
  一份空的队列数据**（任务照单重建后续跑）；version=1 的旧快照仍可读取，
  但只能恢复进已包含其所引任务的队列数据。

## 本地验证

```bash
uv sync                                   # 安装依赖（含 pytest / httpx）
uv run pytest -q                          # 全部单测（72 个）
uv run pytest --log-cli-level=INFO -q     # 查看每条用例的 [输入] / [判定] 依据
uv run pytest tests/test_isolation.py tests/test_checkpoint.py -q   # 仅本轮修复相关
uv run python -m jobqueue.cli demo        # 端到端冒烟演示
```

跨进程 / 空库续跑可手动验证：

```python
from jobqueue.config import Settings
from jobqueue.queue import JobQueue

q1 = JobQueue(Settings(db_path="a.db"))   # 进程 A：消费到一半后导出
q1.export_checkpoint("g", "snapshot.json")
q2 = JobQueue(Settings(db_path="fresh.db"))  # 进程 B、空队列数据
q2.restore_checkpoint("snapshot.json")    # 任务照单重建，定论不重做、未完成不跳过
# q2.claim("g", ...) 接着消费即可；恢复位点已与该组真实进度对齐
```

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
- 进度对账分桶、位点跳过在途/退避/未投递任务的 `offset_gap` 拒绝
  （`tests/test_reconcile.py`）
- 组间隔离：终态不被其它组改回（含全局状态锁定）、退避互不牵连、
  按组查看任务结论（`tests/test_isolation.py`）
- 检查点导出 / 落盘 / 恢复：重启接着消费、换组接管、冲突与非法快照拒绝、
  位点只前进、幂等恢复（`tests/test_checkpoint.py`）
- 自足快照（v2）：恢复进空队列数据后任务照单重建、定论不重做、未完成不丢失，
  恢复进另一进程的已有库不影响其本地组；在途旧投递被接管后迟到确认无效；
  v1 旧快照向后兼容；dedupe_key 身份冲突拒绝（`tests/test_checkpoint.py`）
- 恢复位点对齐：快照位点虚高（只定论 1 条记 3）纠正为真实终态前缀、
  位点越界（超过任务总数）夹回、零定论纠正到 0、如实位点不被压低
  （`tests/test_checkpoint.py`）
- 进程重启恢复、崩溃后超时重投、终态不复活（`tests/test_restart.py`）
- 8 消费者并发 claim 每任务仅一次、4 worker 并发含永久失败、位点并发竞争
  （`tests/test_concurrency.py`）
- 数量守恒、投递账本闭合、终态不可逆（`tests/test_invariants.py`）
- 全部 HTTP 状态码与错误码（`tests/test_api.py`）

测试与 worker 日志均打印 `[输入]` 与 `[判定] 依据`，可用
`uv run pytest --log-cli-level=INFO` 查看判定过程。
