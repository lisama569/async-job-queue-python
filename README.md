# 本地异步任务队列（SQLite 模拟消息代理）

一个完全运行在本仓库内的异步任务队列：用单个 SQLite 文件（WAL 模式）模拟消息代理，
提供**入队幂等、独占投递、确认 fence、指数退避重试、租约超时重投、消费者组位点管理**。
不依赖任何外部服务、真实凭据或付费资源。

## 快速开始

```bash
uv sync                         # 安装依赖
uv run pytest                   # 运行全部测试（52 条）
uv run python scripts/demo.py   # 端到端演示（打印每步输入与判定依据）

# HTTP 服务
uv run uvicorn main:app --port 8000
```

## 投递语义（Delivery Semantics）

整体语义是 **at-least-once 投递 + exactly-once 完成**：

- 投递可能重复（消费者崩溃、租约超时后重投），但任务**同一时刻只被一个消费者占用**，
  且**成功终态只会出现一次**。
- 每次投递携带单调递增的 `epoch`（fencing token）。消费者 ack/nack 时必须回传它拿到的
  epoch；若租约已过期、任务已被重投，旧 epoch 的确认会以 `LEASE_EXPIRED` 被**明确拒绝**，
  因此“僵尸消费者迟到的确认”不可能让任务被重复完成。
- **ack 幂等**：用同一个完成任务的 epoch 重复 ack 返回同一个 `succeeded` 任务，不报错、不重复执行。
- 不静默丢弃：所有无法接受的操作都抛出带 `RejectReason` 错误码的类型化异常
  （HTTP 下为 `400/404/409` + `{"error": "<CODE>"}`）。

### 拒绝/失败原因码（可区分，非静默）

| 原因码 | 含义 | 典型场景 |
| --- | --- | --- |
| `MALFORMED_PAYLOAD` | 提交非法 | payload 不是 JSON 对象、task_type/dedup_key 为空、失败原因为空 |
| `EMPTY_QUEUE` | 暂无可投递任务 | poll 返回空批（消费者日志记为 `NOOP`） |
| `UNKNOWN_TASK` / `UNKNOWN_GROUP` | 资源不存在 | ack 不存在的任务、向未建组提交位点 |
| `NOT_INFLIGHT` | 任务不在投递中 | 对已 succeeded/failed 的任务再 ack/nack |
| `LEASE_EXPIRED` | epoch 过期 | 租约超时被重投后，旧持有者迟到确认（fence 拒绝） |
| `OFFSET_ROLLBACK` / `OFFSET_SKIP` / `OFFSET_OVERRUN` / `OFFSET_NOT_TERMINAL` | 位点非法推进 | 见“位点规则” |

消费者侧的业务失败也带可区分前缀：`NO_HANDLER:<type>`（无处理器，不重试）、
`DECODE_ERROR`（载荷损坏，不重试）、`HANDLER_REJECTED:<原因>`（处理器抛
`PermanentTaskError`，不重试）、`HANDLER_ERROR:<异常>`（瞬时错误，按退避重试）。

## 状态流转

```
                 入队（合法 JSON 对象）
                       │
                       ▼
                   ┌────────┐  poll（条件 UPDATE，epoch+1, attempts+1）
                   │ PENDING│──────────────────────────────┐
                   └────────┘                               ▼
                       ▲                                ┌──────────┐
        nack 且重试预算未用完                             │ INFLIGHT │
        （置 available_at = now+退避）                    └──────────┘
                       │                              ┌────┬─────┬─────┐
                       │                       ack(epoch) │     │     │ nack(retry=False)
        租约超时（≤ lease_timeout）                     │     │     │ 或 attempts>=max
        reclaim_expired / 下次 poll 前自动回收          │     │     ▼
                       └──────────────────────────────┘     │ ┌────────┐
                                                             │ │ FAILED │ 终态
                                                             ▼ └────────┘
                                                       ┌───────────┐
                                                       │ SUCCEEDED │ 终态
                                                       └───────────┘
```

- 终态三选一：`succeeded`、`failed`（重试耗尽或显式不重试，带 `terminal_reason`）、
  `rejected`（预留给入队即拒绝且需留痕的场景；当前非法入队直接拒绝、不落库）。
- 终态任务可通过 `GET /tasks/{id}` 查询状态、尝试次数与失败原因；不会再被投递。
- **状态守恒**：任务数自入队起不增不减，最终每个任务恰好落在一个终态（测试中有不变量校验）。

## 重试与租约规则

- **可见超时（租约）**：poll 成功即写入 `lease_owner` 与 `lease_expires_at_ms`。
  超过 `TASKQUEUE_LEASE_TIMEOUT_MS` 未确认，任务可被回收重投：
  回收把行重置为 `pending`，下一次授予时 `epoch += 1`，使旧确认失效。
  `poll` 前会顺带执行一次回收；也可手动 `POST /reclaim`。
- **退避**：瞬时失败（`fail(retry=True)`）后按指数退避重新可见：

  `available_at = now + min(backoff_max_ms, backoff_base_ms * backoff_factor ** (attempt-1))`

  退避时间未到之前 poll 不可见。
- **尝试上限**：第 `max_attempts` 次投递仍失败即进入 `failed` 终态（无论是否要求重试），
  原因写入 `terminal_reason` / `last_error`，之后不再投递。
- 处理器可用 `PermanentTaskError` 显式声明业务性永久失败，立即终态、不消耗后续重试预算。

## 消费者组与位点规则

- 组在首次 poll 或 `PUT /groups/{name}` 时自动创建，位点持久化在 `consumer_groups` 表，
  **进程重启后仍然存在**。位点从 0 开始，任务自增 id 即日志位置（1, 2, 3, …）。
- 提交位点只接受**从当前值恰好 +1**，且严格拒绝以下推进（而不是“从中间继续”）：
  - **回退** `requested <= current` → `OFFSET_ROLLBACK`（重复提交同一位置也按回退拒绝）
  - **跳号** `requested > current+1` 且未越过日志尾 → `OFFSET_SKIP`
  - **越界** `requested > max(task_id)` → `OFFSET_OVERRUN`
  - **越过未终态任务**：目标位置的任务还不是 succeeded/failed → `OFFSET_NOT_TERMINAL`
  - `failed` 也是终态，**允许**位点越过失败任务，避免失败消息永久阻塞队列。
- 数据库层还有一道 `committed_offset < new_offset` 的条件更新兜底并发提交。

> 说明：投递队列是全局的（租约 owner 记录为 `组名/消费者名`），位点是每组独立的
> 审计/恢复游标；位点非法推进不会静默改动数据。

## 并发与持久化如何保证

- SQLite 开启 `journal_mode=WAL`、`busy_timeout=10000`；租约授予在单个
  `BEGIN IMMEDIATE` 事务内用 `UPDATE ... WHERE status='pending' AND available_at<=?`
  的条件更新完成，线程/进程间由写锁串行化，保证一条任务不会同时租给两个消费者。
- 所有状态与位点都在同一个 DB 文件内，进程退出/重启后原样恢复；测试
  `tests/test_concurrency_restart.py` 用 spawn 多进程与“重开同一 DB”验证了
  不重复投递与崩溃恢复。

## HTTP 接口

| 方法与路径 | 说明 |
| --- | --- |
| `GET /health` | 健康检查 |
| `POST /tasks` | 入队；body：`task_type`、`payload`(JSON 对象)、可选 `dedup_key`。首次 `201`，重复 dedup_key 返回 `200` 且 `deduplicated=true` |
| `POST /tasks/poll` | 投递；body：`group`、`consumer`、可选 `max_messages`。返回 0..N 条带 `epoch` 的消息 |
| `POST /tasks/{id}/ack` | 确认；body：`{"epoch": n}`。同 epoch 重复调用幂等 |
| `POST /tasks/{id}/fail` | 否定确认；body：`epoch`、`reason`(必填)、`retry`(默认 true) |
| `POST /reclaim` | 立即回收所有超时租约，返回回收数 |
| `GET /tasks/{id}` | 查询任务状态/尝试次数/终态原因 |
| `GET /status` | 各状态任务计数（守恒核对） |
| `PUT /groups/{name}` | 创建（幂等）消费者组 |
| `GET /groups/{name}` | 查询组位点 |
| `POST /groups/{name}/commit` | 提交位点 `{"offset": n}`，非法推进返回对应错误码 |

快速体验：

```bash
curl -s -X POST localhost:8000/tasks -H 'content-type: application/json' \
  -d '{"task_type":"echo","payload":{"x":1},"dedup_key":"k1"}'
curl -s -X POST localhost:8000/tasks/poll -H 'content-type: application/json' \
  -d '{"group":"g","consumer":"c1"}'
# 用上一条响应里的 id 与 epoch 确认
curl -s -X POST localhost:8000/tasks/1/ack -H 'content-type: application/json' \
  -d '{"epoch":1}'
```

## 配置项（均可通过环境变量覆盖）

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `TASKQUEUE_DB_PATH` | `data/queue.db` | SQLite 文件路径（自动建目录与表） |
| `TASKQUEUE_LEASE_TIMEOUT_MS` | `30000` | 租约/可见超时（毫秒） |
| `TASKQUEUE_MAX_ATTEMPTS` | `5` | 最大投递尝试次数（含首次），超过即 `failed` |
| `TASKQUEUE_BACKOFF_BASE_MS` | `100` | 退避基数（首次失败后的延迟） |
| `TASKQUEUE_BACKOFF_FACTOR` | `2.0` | 退避指数因子 |
| `TASKQUEUE_BACKOFF_MAX_MS` | `30000` | 单次退避上限 |
| `TASKQUEUE_POLL_BATCH_SIZE` | `10` | poll 默认批量大小 |
| `TASKQUEUE_RECLAIM_INTERVAL_MS` | `5000` | 后台回收扫描间隔（供外部调度器参考） |

## 代码结构

```
taskqueue/
  config.py    # 配置（环境变量覆盖）
  models.py    # Task/Delivery/GroupOffset、状态与原因枚举、退避计算
  exceptions.py# 类型化错误（QueueError + RejectReason）
  storage.py   # SQLite 持久化：条件 UPDATE、fence、WAL
  offsets.py   # 位点推进纯规则
  queue.py     # 队列门面：入队/poll/ack/fail/reclaim/位点
  consumer.py  # 本地消费者：处理器注册、失败分类、判定日志
  api.py       # FastAPI 路由与错误码映射
tests/         # 边界场景与不变量测试（含多线程/多进程/重启）
scripts/demo.py
```
