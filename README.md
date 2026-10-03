# notification-fanout：面向用户的通知分发与调度服务

把上游业务事件统一编排后投递到多个通知渠道，在重复触发、并发发送、失败与
进程重启条件下保持结果可解释。投递目标使用**进程内可注入通道**或**本地回环
HTTP 通道**模拟，不依赖任何外部服务与真实凭据。

## 架构

```
事件接入 ──► 路由(按用户订阅偏好) ──► 每渠道一条通知(幂等键)
                                          │
                    ┌──────────────────────┼──────────────────────┐
                    ▼                      ▼                      ▼
              定时/延迟调度            静默时段/频次策略        重试/死信
                    │                      │                      │
                    └──────────────────────┼──────────────────────┘
                                           ▼
                    调度器(单线程扫描) ──► 有界线程池(按 user+channel 串行/限流)
                                           │
                                           ▼
                                    Channel.send(...)
```

- `notify/models.py`：领域类型（通知、状态、事件、错误类型）。
- `notify/channels.py`：通道抽象。`InMemoryChannel`（可配置失败次数/不可重试
  失败/延迟，记录所有投递）与 `LoopbackHttpChannel`（标准库 urllib 打本地回环）。
- `notify/repository.py`：SQLite 持久化。所有状态迁移都是**条件 UPDATE**，
  终态行永不被再次更新；`status_history` 记录每次迁移的原因。
- `notify/policy.py`：静默时段与频次上限策略（在每次投递时评估，配置变更只
  影响后续发送）。
- `notify/ratelimit.py`：按 `(user_id, channel)` 维度的非阻塞互斥锁 + 令牌桶。
- `notify/scheduler.py`：后台调度线程，负责到期扫描、策略评估、租约恢复、
  退避重试与死信；慢渠道只重新排队，不阻塞其他渠道。
- `notify/service.py`：事件接入、偏好、查询等应用服务。
- `notify/api.py`：FastAPI HTTP 接口。

## 路由与订阅语义

- 同一事件按用户订阅偏好**扇出到多个渠道**，每个渠道生成一条独立通知。
- 用户关闭的渠道**绝不投递**，终态为 `skipped` 并注明原因；某渠道失败不影响
  其余渠道。
- 接入时可显式指定渠道列表，但仍会与用户偏好取交集（关闭的渠道一律跳过）。
- 未配置偏好的用户默认所有渠道开启。

## 幂等

- 幂等键为 `event_id::channel`：同一事件在同一渠道重复触发只产生一次投递；
  不同渠道的同一事件**不会**被合并或去重。
- 重复接入返回既有通知行（ID 相同），进程重启后依然有效。

## 定时/延迟投递与重启恢复

- 接入时可指定 `scheduled_at`（最早投递时刻），未到时刻绝不发出。
- 所有通知持久化在 SQLite（WAL）中，重启后：
  - 未到期通知保持 `pending`，到期后正常投递，不丢不重；
  - 到期未发的通知立即被扫描投递；
  - 崩溃后遗留的 `sending` 租约（超过 `lease_timeout`）会被重置为 `pending`
    并重新投递，条件更新保证不会重复发送。

## 静默时段

- 非紧急通知（`urgent=false`）在静默窗口内**顺延到窗口结束时刻**（默认
  22:00–次日 08:00，支持跨午夜），原因记录在 `reason` 中。
- 紧急通知（`urgent=true`）直接发送，不受静默时段限制。
- 静默窗口可按用户偏好单独配置（设为相同小时可对该用户关闭静默）。

## 频次上限

- 按 `(user_id, channel)` 统计滑动窗口内**已成功投递**数，超过 `freq_cap` 时：
  - `defer`（默认）：顺延到下一个允许的时刻（窗口内最老一条投递 + 窗口长度）；
  - `drop`：丢弃，终态 `dropped` 并注明原因。
- 频次策略在投递前评估，配置变更（`PUT /admin/config`）只影响后续发送。

## 失败重试与死信

- 通道错误分两类：
  - `RetryableError`（超时、5xx、429、连接错误）：按指数退避重试
    （`retry_base_delay * 2^attempts`，带抖动，不超过 `retry_max_delay`）；
  - `NonRetryableError`（4xx 等永久失败）：直接进入死信，**绝不重放**。
- 重试次数超过 `max_attempts` 的通知进入死信（`dead_letters` 表 + 终态
  `dead_letter`），保留最后一次失败原因，可通过 `GET /dead-letters` 查询。

## 状态机

```
pending ──► sending ──► sent(终态)
  ▲           │
  │           └──► pending(退避重试 / 租约恢复)
  ├──► skipped(终态，接入时渠道关闭/未知)
  ├──► dropped(终态，频次策略丢弃)
  └──► dead_letter(终态)
```

终态（`sent`/`skipped`/`dropped`/`dead_letter`）全局唯一：所有迁移均为
带条件的原子 UPDATE，并发触发与状态更新下只有一方能获胜，历史中可审计。

## 限流与并发

- 同一用户同一渠道：**串行**（非阻塞互斥锁，忙则短暂重新排队）+
  **令牌桶限流**（`rate_per_second` / `rate_burst`），避免打垮下游。
- 不同用户/渠道：在线程池中并行投递；慢渠道只占用自己的键，不会阻塞
  快渠道，也不会拖垮调度线程。
- 限流器实时读取配置，调整速率只影响后续发送。

## 配置项

| 配置 | 默认值 | 说明 |
| --- | --- | --- |
| `db_path` | `notifications.db` | SQLite 路径（`:memory:` 为内存库） |
| `tick_interval` | `0.2` | 调度扫描周期（秒） |
| `dispatch_workers` | `16` | 投递线程池大小 |
| `batch_size` | `100` | 每轮最多领取的到期通知数 |
| `default_max_attempts` | `5` | 默认最大尝试次数 |
| `retry_base_delay` | `0.5` | 退避初始延迟（秒） |
| `retry_max_delay` | `30.0` | 退避上限（秒） |
| `lease_timeout` | `30.0` | 租约超时（秒），超时重置 |
| `rate_per_second` | `2.0` | 每 user+channel 令牌桶补充速率 |
| `rate_burst` | `1` | 每 user+channel 突发容量 |
| `freq_cap` | `20` | 滑动窗口内每 user+channel 最大投递数 |
| `freq_window_seconds` | `3600.0` | 频次窗口长度（秒） |
| `freq_strategy` | `defer` | 超上限策略：`defer` 顺延 / `drop` 丢弃 |
| `quiet_start_hour` / `quiet_end_hour` | `22` / `8` | 静默窗口起止本地小时（`start==end` 关闭） |

可通过 `PUT /admin/config` 热更（仅影响后续发送）。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/events` | 接入事件（event_id、user_id、payload、channels、scheduled_at、urgent、max_attempts） |
| GET | `/events/{event_id}` | 查询事件下各渠道通知状态 |
| GET | `/notifications/{id}` | 查询单条通知状态 |
| GET | `/notifications/{id}/history` | 查询状态迁移历史（结果可解释） |
| GET/PUT | `/users/{user_id}/preferences` | 查询/更新渠道开关与静默时段 |
| GET | `/dead-letters` | 死信列表（含失败原因） |
| GET/PUT | `/admin/config` | 查看/热更配置 |
| GET | `/health` | 健康检查 |

## 测试

```bash
uv sync
uv run pytest -q
```

覆盖：扇出与渠道隔离、幂等与并发重复触发、定时与重启恢复/租约恢复、
静默时段与频次策略（顺延/丢弃/按用户覆盖）、重试退避与死信（含不可重试
失败不重放）、并发领取互斥与终态唯一、慢渠道不阻塞快渠道、本地回环 HTTP
渠道（200/500/400）以及全部 HTTP 接口。
