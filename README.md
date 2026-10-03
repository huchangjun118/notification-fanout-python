# notification-fanout-python

面向用户的通知**分发与调度**能力：把上游业务事件按用户订阅偏好编排后，扇出
（fan-out）投递到多个通知渠道，并在**重复触发、并发发送、失败、进程重启**等
条件下保证结果确定、可解释。

投递目标只使用**进程内可注入通道**或**本地回环 HTTP（127.0.0.1）**，不依赖
任何外部服务，也不需要真实凭据。仅使用 Python 标准库 + FastAPI（HTTP 门面/
测试 sink）+ pytest。

## 目录结构

```
notification_fanout/
  models.py       # 领域类型：Event / Notification / 状态机 / 策略枚举
  clock.py        # 可注入时钟：SystemClock（真实）/ FakeClock（虚拟时间）
  store.py        # SQLite 持久化：UNIQUE(event_id,channel)、状态 CAS、尝试记录
  channels.py     # InProcessChannel（可注入）/ LoopbackHttpChannel（回环）
  idempotency.py  # (event_id, channel) 幂等键与进程内短路缓存
  policy.py       # 静默时段 QuietHours / 频次上限 FrequencyCap / 退避 RetryPolicy
  router.py       # 订阅偏好路由，把一个事件展开成多个逐渠道通知
  scheduler.py    # 定时堆、恢复、串行/限流/并发闸、重试、死信、终态 CAS
  config.py       # DispatchConfig、原子快照热更、令牌桶
  service.py      # 高层门面：trigger / 查询 / 订阅 / 配置 / 生命周期
  api.py          # FastAPI：/events、/notifications、/dead-letters、/config ...
tests/            # 39 个自动化测试（重复/定时/重启/静默/频次/重试/并发/HTTP）
main.py           # 本地启动入口（uvicorn，默认监听 127.0.0.1）
```

## 快速开始

```bash
uv sync
uv run pytest                       # 全部测试
uv run python main.py --db ./nf.db # 启动 HTTP 服务（回环/进程内通道）
```

编程式用法：

```python
from datetime import timedelta
from notification_fanout import (
    NotificationService, NotificationStore, ChannelRegistry,
    InProcessChannel, Channel, Subscription, Event,
    RetryPolicy, QuietHours, FrequencyCap, ExceedPolicy,
)
from datetime import time

registry = ChannelRegistry()
email = InProcessChannel(Channel.EMAIL)   # 默认成功；可注入 handler / 脚本化失败
registry.register(email)

svc = NotificationService(
    store=NotificationStore("notifications.db"),  # 文件 DB 即支持重启恢复
    channels=registry,
    retry_policy=RetryPolicy(max_attempts=5, base_delay=timedelta(seconds=1),
                             multiplier=2, max_delay=timedelta(minutes=5)),
    quiet_hours=QuietHours(start=time(22), end=time(8), tzname="UTC"),
    frequency_caps={
        Channel.EMAIL: FrequencyCap(max_sends=10, window=timedelta(hours=1),
                                    exceed_policy=ExceedPolicy.DEFER),
    },
)
svc.set_subscription(Subscription("user-1", frozenset({Channel.EMAIL})))

result = svc.trigger(Event(
    event_type="login_alert", user_id="user-1", title="新设备登录",
    event_id="biz-correlation-id-123",   # 业务幂等键
    # not_before=...,                    # 定时/延迟投递
))
print(svc.explain(result.notifications[0].id))  # 全程状态轨迹
```

## 路由语义

* 一个 `Event` 按用户订阅展开为**每个已开启渠道一个** `Notification`；
  通知是调度、投递、重试、查询的最小单位，事件上只携带幂等关联 id。
* **用户已关闭的渠道不投递**（结果里的 `skipped_disabled` 可解释）。
* 各渠道通知彼此独立：**任一渠道失败不影响其他渠道**；调度器为每个渠道
  维护**独立线程池**，慢渠道不可能占用快渠道的线程（详见"并发语义"）。

## 幂等语义（第 2 条）

幂等作用域是 **(event_id, channel)**：

* 同一事件在**同一渠道**被触发任意次，只产生一条通知、一次成功投递。
  存储层 `notifications` 表对 `(event_id, channel)` 有 UNIQUE 约束，
  `INSERT` 冲突即抑制重复；即使多线程并发触发或跨进程重启后再次触发，
  约束仍然生效（进程内缓存只是优化，不承担正确性）。
* 同一事件在**不同渠道**是不同通知，**永不互相合并或去重**。
* 投递语义为 at-least-once：若进程死在渠道调用中途（`DELIVERING`），
  重启后该通知会被重试；"重复触发不重复投递"由上述唯一键保证。

## 定时 / 延迟与重启恢复（第 3 条）

* `Event.not_before` 指定**最早投递时刻**，未到时刻绝不提前发出。
  调度器用最小堆 + 单个守护线程按时刻弹出。
* 所有非终态通知持久化在 SQLite。服务启动时 `recover()`：
  * `PENDING/RETRYING` 且到点的立即重新调度，未到点的按原时刻排队——**不丢失**；
  * 上次进程在渠道调用中断留下的 `DELIVERING` 行，转为 `RETRYING` 重新投递
    （确认结果未知，宁可重发一次）；
  * `DELIVERED/DEAD_LETTER/DROPPED` 终态行**不会被重放**。

## 静默时段（第 4 条）

`QuietHours(start, end, tzname)` 为按天重复的静默窗口（支持跨午夜，如
22:00-08:00，使用 IANA 时区名）：

* 命中窗口的**非紧急**通知顺延到窗口结束时刻；历史轨迹记录顺延原因。
* **紧急通知（`Urgency.URGENT`）绕过静默**，立即投递。
* 退避后的重试若落在静默窗口，同样顺延到窗口结束。

## 频次上限（第 4 条）

`FrequencyCap(max_sends, window, exceed_policy)` 按 **(用户, 渠道)** 在滚动
窗口内计数；**只有真正调用了下游渠道（无论成败）才消耗额度**，顺延/未发送的
通知不占额度：

* `ExceedPolicy.DEFER`：顺延到"窗口内最早一次尝试 + 窗口长度"形成的第一个
  空位；若该空位又落入静默时段则继续顺延。
* `ExceedPolicy.DROP`：直接进入终态 `DROPPED`，历史中保留
  `frequency cap ... policy=drop` 原因；`GET /notifications/{id}/explain`
  可查看完整决策链。
* 紧急通知绕过频次上限。

## 失败重试与死信（第 5 条）

渠道返回 `DeliveryResult(success=False, error_kind=...)`：

* `RETRYABLE`：指数退避
  `min(max_delay, base_delay * multiplier ** (attempt-1))`，在
  `not_before = now + delay` 后重试；超过 `max_attempts` 进入 `DEAD_LETTER`，
  死信保留最后失败原因与全部尝试历史（`delivery_attempts` 表可查）。
* `FATAL`（不可重试，如 4xx、地址无效、handler 抛错）：**一次调用即死信，
  之后永不重放**。
* 回环 HTTP 渠道把连接失败/超时/429/5xx 判为可重试，其他 4xx 判为致命。

## 状态最终一致查询（第 6 条）

状态机：

```
PENDING ──► DELIVERING ──► DELIVERED            （终态，唯一成功）
   ▲           │
   │           ├──► RETRYING ──► (退避后再次进入 PENDING/DELIVERING)
   │           └──► DEAD_LETTER                 （终态，保留原因）
   └──────────────► DROPPED                     （终态，频次策略丢弃）
```

* 每次状态推进都是**比较与交换**：`UPDATE ... WHERE id=? AND status IN
  (允许的前置状态)`。两个并发工作者不可能把同一条通知写成两个互相矛盾的
  终态；终态一旦写入即不可变。
* 每次状态变化以 append-only 的 `StateChange`（时刻、状态、尝试号、原因）
  落库，`Notification.explain()` / `GET .../explain` 给出完整可解释轨迹。
* `GET /events/{event_id}` 返回该事件所有渠道通知；当全部到达相同终态时给
  出 `aggregate_status`，否则以每条通知的逐渠道状态为准。

## 并发、速率与配置（第 7 条）

* **同一用户同一渠道串行**：每个 (user_id, channel) 有独立 FIFO 队列，
  队列只有一个 pump，投递严格按顺序、互不重叠；不同用户/不同渠道并行。
* **慢渠道隔离**：每个渠道一个独立线程池（上限 64），一个渠道被拖慢只影响
  它自己的池。
* **全局并发度** `max_concurrency`：跨渠道在飞投递总数的可热更信号量；
  调大立即放行等待者，调小只约束后续获取，不取消在飞发送。
* **渠道发送速率** `rate_per_second` + `burst`：渠道级令牌桶（默认
  0 = 不限）。所有配置以**不可变快照 + 原子替换**发布，正在进行的发送不受
  影响，**配置变更只影响后续发送**。

### 配置项（`DispatchConfig` / `GET,PATCH /config`）

| 配置项 | 含义 | 默认 |
| --- | --- | --- |
| `max_concurrency` | 全局跨渠道在飞投递上限 | `8` |
| `default_rate_per_second` | 未单独配置渠道的默认每秒发送速率，`0` 不限 | `0` |
| `default_burst` | 默认令牌桶突发容量 | `1` |
| `channels[ch].rate_per_second` | 指定渠道速率覆盖 | 无覆盖 |
| `channels[ch].burst` | 指定渠道突发容量 | `1` |

策略类配置（构造时注入）：`RetryPolicy(max_attempts, base_delay, multiplier,
max_delay)`、`QuietHours(start, end, tzname)`、
`FrequencyCap(max_sends, window, exceed_policy)`。

## HTTP 接口

| 方法 路径 | 说明 |
| --- | --- |
| `POST /events` | 触发事件（可带 `event_id`、`not_before`、`urgency`），返回逐渠道通知与跳过原因 |
| `GET /notifications/{id}` | 查询单条通知状态与历史 |
| `GET /notifications/{id}/explain` | 人类可读的完整决策/投递轨迹 |
| `GET /events/{event_id}` | 查询事件下所有渠道通知与聚合终态 |
| `GET /dead-letters?limit=` | 死信列表（含失败原因） |
| `PUT /subscriptions/{user_id}` | 设置用户已开启渠道 |
| `GET /config` · `PATCH /config` | 查看/热更速率与并发配置 |

## 测试

```bash
uv run pytest                       # 39 个用例
```

覆盖：重复触发幂等（含并发 8 线程）、跨渠道不合并、失败渠道隔离、定时不
提前发送、重启后 PENDING/DELIVERING 恢复且终态不重放、静默顺延与紧急绕过、
频次顺延/丢弃（按用户+渠道）、可重试退避、重试耗尽死信、致命错误不重放、
同用户同渠道串行、慢渠道不阻塞快渠道、全局并发度、配置热更只影响后续、
令牌桶限速，以及通过真实本地回环 HTTP 服务（stdlib `http.server` +
`LoopbackHttpChannel`）验证 socket 投递、连接拒绝重试/死信、4xx 致命。

时间相关测试注入 `FakeClock`（`sleep` 即时推进虚拟时间），确定性、无真实
等待；并发测试使用真实 `SystemClock` 与脚本化的阻塞/计数 handler。
