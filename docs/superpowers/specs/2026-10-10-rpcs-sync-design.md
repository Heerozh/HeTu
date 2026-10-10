# 等推送的 RPC（`rpcs` + 通用栅栏）— 设计稿

- 日期：2026-10-10
- 状态：已实施（§11、§12 是已定事项；Linux 负载下的实测待补，§8.2）
- 分支：`feat/rpcs-sync`（基于 dev `41186147`）
- 影响范围：`hetu/server/receiver.py`（新命令 `rpcs`）、`hetu/data/sub.py`（hub 的通用栅栏、门面的 sync 帧）、
  `hetu/server/websocket.py`（发送循环发 sync 帧）、`hetu/data/backend/base.py` 与 `hetu/data/backend/sqlite/mq.py`
  （栅栏延迟）、C# SDK（`ClientBase.cs`、`JsonbLayer.cs`、`UnityClient.cs`、`SessionClient.cs` /
  `SessionClientBase.cs`、`HeadlessHeTuClient.cs`）、测试、文档。
- **不改**：`rpc` 命令及其回复语义；commit、Lua、keyspace 配置、PUBLISH；订阅的读、比对、交付；MQ 队列算法；
  不新增任何 master 读写。

## 1. 背景

UI 上常见的写法：点按钮时置灰、发请求，请求完成后按订阅数据恢复按钮（钱还够不够、库存剩几个）。今天请求的
回包比订阅推送早约 100ms，按钮会按旧数据恢复成错误的状态。

时序（dev `41186147`）：

- **RPC**：`receiver.rpc()` 在 endpoint 跑完（含 commit）后立刻把 `rsp` 放进 push_queue，发送循环按顺序发出。
  回复不带请求 id，SDK 按顺序对应。
- **推送**：commit → 通知经副本到达本 worker 的通知接收器 → 进 hub 的 MQ 队列（按到达时刻排队，同频道合并）→
  满一个 interval（`1/UPDATE_FREQUENCY` = 100ms，同时是复制延迟的预算）才弹出 → tick 里读副本、比对，tick 末尾
  交到各连接的待发区 → 发送循环取走发出。推送比 commit 晚 δ + interval + 合批窗口（≤5ms）+ tick 耗时，
  其中 δ 是"commit 返回 → 通知进本 worker 的队列"，要经副本多绕一跳。
- **SDK** 按到达顺序处理：`updt` 在 `OnReceived` 里同步 `UpdateRows`（`OnUpdate` 等事件当场触发），`rsp`
  完成请求回调（`ClientBase.cs` 的 `OnReceived`）。所以只要"完成信号"排在推送后面到达，await 返回时订阅
  对象就已是新值。

## 2. 目标与非目标

目标：

- 客户端可以按次选择：这次调用要等它引起的订阅推送都发出之后才算完成。
- 服务端只加一个通用栅栏：不追踪写入、不判断哪些订阅受影响、不给 System 打标。
- 不读 master，不新增 PUBLISH，不新增副本读。
- 可以不精确，但任何情况下都不比今天差：最坏退化为今天的行为（完成时推送还没到）。

非目标：

- 强一致的"读己之写"（带版本号的推送等，§10）。
- 改 `rpc` 的语义或默认行为。
- JS SDK（还没有）。

## 3. 协议

客户端 → 服务端：

```
["rpcs", sync_id, endpoint, *args]
```

- 与 `["rpc", endpoint, *args]` 相同，只多一个 `sync_id`：非负整数（0..2^31-1），SDK 在每个连接上递增生成、
  重连归零。服务端只校验类型和范围，原样带回，不记账。
- 能调的 endpoint 与 `rpc` 相同（System 自动生成的、`define_endpoint` 定义的）。

服务端 → 客户端：

- 照旧按请求顺序回 `["rsp", ...]` / `["rej", ...]` / `["err", ...]`（debug 模式），立即发，与 `rpc` 完全一样。
- 只在回了 `rsp` 时，之后再发一帧 `["sync", sync_id]`。它不占回复顺序（SDK 按 id 认），随推送一起从待发区
  发出，排在它之前交来的推送后面。
- `rej` / `err` 没有提交，不发 sync。release 模式下调用失败照旧断开连接。
- `sync_id` 缺失、不是整数（bool 不算）、超出范围：同其他格式错误的消息，断开连接。

兼容性：`rpc` 不变，老 SDK 不受影响。新 SDK 连老服务器时 `rpcs` 是未知消息类型、会被断开；SDK 与服务器按
版本配套发布（`package.json` 与服务器 tag 对应），不做探测。

## 4. 服务端设计

```
客户端                                  服务端（本连接所在 worker）
["rpcs", 7, "buy", ...] ───────────►  按 rpc 执行、commit，rsp 照常入队
        ◄─────────────────────────── rsp（立即发，和今天一样）
                                     g 之后往 hub 的 MQ 队列放栅栏
                                     commit 的通知经副本到达，排在栅栏前面
                                     约 1 个 interval 后同一个 tick 把它们一起弹出：
                                     读副本，推送放进待发区，然后栅栏回调把
                                     sync 7 放到待发区旁边
        ◄─────────────────────────── updt ...
        ◄─────────────────────────── ["sync", 7]（和推送同一批发出，排在推送后面）
await 返回，订阅对象已是新值
```

### 4.1 栅栏的语义

hub 的 MQ 队列按到达时刻排序（`_enqueue` 只往队尾追加，时刻取 `time.monotonic()`），`get_message` 每次从队头
弹出所有已满一个 interval 的项。所以在时刻 t 入队的栅栏键 F：

- 排在 t 之前到达的所有项（真实通知、定向补读、断线重订的补发）后面；
- 它满期弹出时，排在它前面的项都已满期，在同一批或更早的批里弹出；
- 它在弹出它的那个 tick 交付（`_deliver`）之后才触发。

于是：**F 触发时，本 worker 在 t 之前收到的通知都已处理完，推送都已交到各成员的待发区。** 推送仍走原路径
（每条通知离读至少一个 interval），新鲜度与今天的订阅相同。

`rpcs` 在 endpoint 跑完（commit 已返回）之后隔 g（`FENCE_DELAY`，§4.6）把栅栏入队。commit 的通知只要在 g 内
进到本 worker 的队列，就排在栅栏前面，sync 帧就在它们的推送之后。g 是本设计唯一新增的假设。

### 4.2 hub：`SubscriptionHub.fence_(callback)`

- `fence_(callback)`：登记一个栅栏 `fid`（hub 内递增），`call_later(FENCE_DELAY, ...)` 到时把虚拟键
  `"\0\0{fid}"` 用 `mq.request_reread` 放进同一个 MQ 队列。定向补读的键是 `"\0{token}\0{频道}"`（token ≥ 1），
  真实频道不以 NUL 开头，都不会撞。
- `_tick(batch)`：先把栅栏键从这批里拣出来，剩下的照旧 `_collect` / `_repair`（`_collect` 把 `\0` 开头的键按
  定向补读解析，栅栏键不能进去）；在最外层的 `finally` 里、现有的 `_settle_channels` / `_deliver` 之后，逐个触发
  这批的栅栏。没有 work 提前 return、处理出错时也会走到。手动模式（`step_`）同样经过 `_tick`。
- 保险定时器：每个栅栏另挂 `FENCE_TIMEOUT_INTERVALS`（默认 20，即 2 秒）个 interval 的定时器，到时还没触发就
  直接触发。覆盖栅栏键被 `DROP_AFTER` 丢掉、处理循环因 bug 重启等情况，保证连接活着时 sync 一定会来，SDK 不用
  自己计时。
- `close()`：触发所有未触发的栅栏，取消它们的定时器。
- 触发 = 取消两个定时器、从登记表移除、同步调用 callback。callback 不能 await；抛出的异常记日志后吞掉（同
  `deliver_` 叫醒发送循环出错的处理），不影响同批其他栅栏和后续 tick。
- 只有栅栏的一批（hub 本来空闲）跑一个空 tick：`_collect` 为空、提前 return、触发栅栏，不计入 `_ticks`。

### 4.3 门面：`SubscriptionBroker`

- `sync_(sync_id)`：门面已关闭则忽略；否则 `hub.fence_(回调)`。
- 回调：门面已关闭则忽略；否则把 `sync_id` 追加到 `_synced`，按 `deliver_` 的规则叫醒发送循环（它空闲等着、
  且没有叫醒在途时才叫）。
- `take_synced_()`：取走并清空 `_synced`；`has_synced_()`：是否非空。
- `has_updates_` / `take_updates_` / `get_updates` / `stalled_` 不变：
  - `get_updates` 在手动模式下把 `has_updates_` 交给 `step_` 当"已就绪"判断。算上 sync 的话，待发区为空、只有
    sync 时 `step_` 每次都当场返回，`get_updates` 会在不让出事件循环的情况下空转。
  - `stalled_` 管的是订阅推送的背压，排着的 sync 不算卡住。
- `close()`：清空 `_synced`。

### 4.4 receiver：`rpcs`

- `client_handler` 加 `case "rpcs"`：`check_length("rpcs", data, 3, 101)`，校验 `sync_id`，然后剥掉 id 走 `rpc()`。
- `rpc()` 加一个可选回调参数，在 `rsp` 入队之后调用；`rpcs` 传入 `broker.sync_(sync_id)`。`rej` / `err` / 断开的
  分支不调。返回值与失败处理（被顶号带 close 码断开、release 模式断开）和 `rpc` 共用。
- g 从这里开始算：endpoint 里所有 System 的提交（包括 `ctx.session_commit()` 提前提交）都已返回，`rsp` 已入队。
- 计入接收频率上限的方式与 `rpc` 相同（一条消息）。
- 不判断 endpoint 有没有写库：没写也照发 sync（约 g + interval 后）。

### 4.5 发送循环

`send_loop` 取待发区时（push_queue 空，或推送被回复压住超过 `PUSH_MAX_HOLD` 且没有订阅回复占位排着）：

1. 在同一个同步段里 `updates = broker.take_updates_()`、`synced = broker.take_synced_()`；
2. 先发 `updt` 帧，再发 `["sync", id]` 帧。

判断"有没有要取的"、推送被压住的计时，都看 `has_updates_() or has_synced_()`：只有 sync 时也要能叫醒、能取走。

顺序保证：栅栏回调在它那个 tick 的 `_deliver` 之后才追加 sync，所以取到某个 sync 时，它前面应发的推送要么已在
更早的一次里发出，要么就在这一次里、排在它前面。订阅回复占位的规则不变：sync 与推送同进同出，不会抢到排着的
订阅回复前面，也不会把应排在它前面的推送留下。

通常 `rsp` 比 sync 早约 g + interval 到达。只有发送拥塞、`PUSH_MAX_HOLD` 让推送插到排着的回复前面时，sync 才
可能先于它的 `rsp`。SDK 两种顺序都要处理（§5）。

### 4.6 `FENCE_DELAY`（g）

`MQClient.FENCE_DELAY`（秒）：给"commit 返回 → 通知进本 worker 的队列"（δ）留的余量，按后端覆盖。

- Redis（基类默认）：0.02。δ = 副本应用 + 副本经 pubsub 推到 worker + 读协程调度，空闲实测 p99 约 0.2ms
  （§8.1）；留 20ms 应对负载尖峰，负载下的分布待测（§8.2）。
- SQLite（`SQLiteMQClient`）：通知表每 interval/2（50ms）轮询一次，Windows 上 asyncio 的睡眠还会多睡一个定时器
  周期（约 15.6ms），实测 δ 最长约 63ms，取 `0.5 * interval + 0.03`（80ms）。只用于开发，延迟不敏感。

g 越大越稳、完成越晚。以后可以做成配置项。

## 5. SDK 设计（C#：Unity 与 headless）

共享的 `HeTuClientBase`（`ClientBase.cs`）：

- 常量 `CommandRpcSync = "rpcs"`、`MessageSync = "sync"`。`JsonbLayer` 的标准解码加 `sync` 分支（`[cmd, 整数]`），
  不走异常回退。
- `CallSystemSync(systemName, args, onResponse, awaitPush = false)`（内部）：`awaitPush` 时取 `++_syncSeq` 作 id，
  发 `["rpcs", id, systemName, *args]`，登记 `_pendingPushCalls[id]`；FIFO 回调照旧登记。按顺序的回复到达时：
  - 取消 → 撤掉 pending，`Canceled`；
  - `rej` / `err` → 撤掉，立即 `Rejected` / `Failed`；
  - `rsp` → 存下 payload，sync 已到就完成。
- `OnReceived` 加 `case MessageSync`：id 按整数解析（MessagePack 可能解成 byte / int / long）。找到 pending 就打上
  标记，`rsp` 已存则用它完成（`Completed`）；找不到（已因 rej / err 结束，或重连前的旧 id）就忽略。
- 断线（`HandleClosed`）、主动 `Close()`、`Dispose()`、重连前的清理（`ConnectSync`）：在触发 `OnClosed`、取消等待者
  之前，先把已收到 `rsp` 的 pending 按成功完成——提交确定发生了，重连后订阅会恢复；没收到 `rsp` 的照旧由回复
  队列取消（Session 层照旧报 `CallOutcomeUnknownException`）。然后清空 `_pendingPushCalls`、`_syncSeq` 归零。必须排在
  `OnClosed` 之前：Session 的 transport 在 `OnClosed` 里把在途调用判成结果未知。
- 不需要计时器：连接活着时服务端保证会发 sync（§4.2 的保险定时器）。

公开 API：

- `HeTuClient.CallSystemAwaitPush(string systemName, params object[] args)`（Awaitable / UniTask）。不能给
  `CallSystem(string, params object[])` 加 bool 参数：`CallSystem("x", true)` 会被绑到新重载上。
- `HeTuSessionClient.CallSystemAwaitPush(...)`：`PendingCall` 带上标记，`IHeTuSessionTransport.CallSystem` 加
  `awaitPush` 参数。在途语义不变：发出后断线、没收到 `rsp` 的报结果未知。
- `HeadlessHeTuClient.CallSystemAwaitPush(...)`（Task）。
- `SystemLocalCallbacks` 照旧在发出时执行；Inspector 记一条 callsystem，调用完成时结束。

`hetu build` 生成的代码不涉及 `CallSystem`，不改。

## 6. 正确性与限制

| 场景 | 结果 |
|---|---|
| commit 的通知在 g 内进本 worker 的队列（常态） | 排在栅栏前，sync 在推送之后 ✓ |
| 写入没改到本连接可见的内容（指纹相同、RLS 不可见、不在范围、没订这张表） | 没有推送，sync 约 g + interval 后照常到达 ✓ |
| 栅栏入队前别人的写入 | 一并推完。sync 的含义是"栅栏入队前本 worker 收到的通知都推完了"，不区分来源 |
| 同一连接连发几个 `rpcs`，或 `rpcs` 后紧跟别的消息 | 各自一个栅栏，按入队顺序触发；`rsp` 的顺序、订阅回复与推送的先后规则都不变 ✓ |
| 订阅回复占位排在 sync 前面 | sync 与推送一起被压住，不会抢到订阅回复前面 ✓ |
| 通知晚于 g 才到（副本延迟尖峰、worker 事件循环卡住超过 g） | sync 可能先于推送：退化为今天 |
| 合并进队头的通知（同频道已有一条在排） | 首次读离这次 commit 可能不足 interval；读到落后的副本时要等尾随重读再晚一个 interval，sync 可能先于补推：退化为今天 |
| 复制延迟超过 interval | 推送本身可能是旧值，同今天订阅的预算 |
| 本连接推送卡住（网络拥塞，订阅被 park） | 攒着的通知在取走待发区后才重读，sync 可能先于它们：退化为今天 |
| 频道补订中（`_repair`）、断线重订的 RESYNC、`DROP_AFTER` 丢弃 | 同上，退化为今天 |
| tick 处理出错 | 栅栏照常触发，推送可能不全：退化为今天 |
| 栅栏键丢失、处理循环卡死 | 保险定时器到时发 sync：退化为今天 |
| 断线 | 已收到 `rsp`：按成功完成；没收到：取消 / 结果未知，同今天 |

保证只针对本连接的订阅：栅栏在本连接所在 worker 的 hub 里，sync 帧只发给发起的连接；本连接的订阅都在这个 hub
上（门面只用默认 backend 的 hub）。

## 7. 成本

- master：零新增读写，零 PUBLISH。副本：零新增读（推送走原路径）。
- worker：每个 `rpcs` 两个定时器、一个 MQ 项、一帧几字节的 sync；hub 空闲时可能多跑一个空 tick。
- 完成延迟：约 RTT + g + interval + ≤5ms + tick 耗时（Redis 默认约 RTT + 125ms），比推送本身晚约 g。`rsp` 照旧
  约一个 RTT 到达。
- 不阻塞同一连接的其他回复和推送。

## 8. 实测

### 8.1 Windows 本机、空闲（2026-10-10）

Docker 起的 Redis 主 + 1 副本，以及 SQLite。hub 订一行，每次改这行之后记"commit 返回 → 这行的通知进 hub 队列"
的间隔 δ（临时用例，没提交），各 300 次：

| 后端 | min | p50 | p90 | p99 | max |
|---|---|---|---|---|---|
| Redis 主从 | -0.06ms | 0.06ms | 0.08ms | 0.20ms | 0.31ms |
| SQLite | 13.7ms | 14.8ms | 15.9ms | 62.1ms | 62.8ms |

负值是 pubsub 消息比 commit 的回复先被事件循环处理。SQLite 的长尾来自 50ms 的轮询睡眠在 Windows 上多睡一个
定时器周期：原定的 60ms 余量不够，改为 80ms。

按 `rpcs` 的时序（写完立刻放栅栏）各跑 300 次，看栅栏触发时这次写入的推送是否已在待发区：Redis（g = 20ms）
漏 0/300；SQLite 在 g = 60ms 与 80ms 下都漏 0/300（60ms 时没漏，是因为栅栏自己的定时器在 Windows 上也晚到，
不能指望）。

### 8.2 待测（Linux）

用 `benchmark/sub_scenarios_ws.py`（加 `rpcs` 模式）在 Redis 主 + 副本上测：

- 聊天 / 背包压测负载下 δ 的分布，据此复核 Redis 的 20ms；
- `rpcs` 完成时刻与推送到达时刻的差（p50 / p99），以及 sync 先于推送的比例；
- 每秒大量 `rpcs` 时 hub 的额外开销（预期可忽略）。

## 9. 测试计划（先写 red）

服务端：

- `tests/test_backend_sub_hub.py`（hub）：
  - 栅栏在处理"它之前入队的通知"的那个 tick 交付之后才触发（回调里看得到门面待发区已有更新）；
  - 只有栅栏时，不早于 g + interval 触发；
  - 它之后才入队的通知不要求（可以在它之后才处理），写进用例说明语义；
  - 没有 work、处理出错（打桩让 `get_updated` 抛）、hub 关闭、栅栏键丢失（保险定时器）时都会触发；
  - 栅栏键不进 `_collect`，与定向补读、真实频道混在同一批时互不影响；
  - 回调抛异常不影响同批其他栅栏和下一个 tick。
- 门面（`tests/test_backend_sub.py` 或 hub 用例）：`sync_` 之后 `take_synced_` 拿到 id；关闭后不再追加；
  `has_updates_` / `get_updates` 不受影响（手动模式不空转）。
- `tests/test_websocket.py`：
  - 发送循环（`_FakeOutbox` 加 `take_synced_` / `has_synced_`）：同一次取待发区先发 `updt` 再发 `sync`；只有 sync
    时也能叫醒、发出；排着订阅回复占位时 sync 不抢到前面；`PUSH_MAX_HOLD` 插队时 sync 跟推送一起；
  - 端到端：登录、订阅，`["rpcs", 7, "add_rls_comp_value", 1]` → 依次收到 `rsp`、带新值的 `updt`、`["sync", 7]`；
  - 写入不影响本连接订阅的 `rpcs` → `rsp`、`["sync", 7]`，中间没有 `updt`；
  - 守卫拒绝（`rej`）、debug 模式失败（`err`）不发 sync；
  - `sync_id` 缺失 / 字符串 / bool / 负数 / 超范围 → 断开；
  - `rpc` 行为回归不变。
- `tests/test_arch_master_reads.py`：`rpcs` 路径不新增 master 读（有必要就加一条）。

SDK（`ClientSDK/csharp/HeTu.Client.Tests`，Unity `Tests/Editor` 镜像；仿 `CallRejectTests` 用 `Receive` 注入帧）：

- 发出的帧是 `["rpcs", id, name, ...]`，id 递增、重连归零；
- `rsp` 后 `sync` → 用 `rsp` 的 payload 完成；`sync` 先于 `rsp` → 同样完成；
- `rej` / `err` → 立即结束，之后来的 `sync` 被忽略；
- 断线时已收到 `rsp` → 成功；没收到 → 取消；
- 不认识的 sync id 忽略；
- Session 层（`SessionClientBaseTest` 的 FakeTransport）：`CallSystemAwaitPush` 的排队、派发、断线后的结果未知语义。

## 10. 备选方案与否决理由

1. **回包带上受影响的组件，客户端等推送到了才算完成**（最初的想法）：写入后客户端可见内容没变时（指纹相同、
   RLS、不在范围、没订）根本没有推送，只能靠超时；推送不带版本、会合并，认不出到的是不是这次写入的；服务端回包
   时也不知道会不会有推送。
2. **回包排在推送之后，服务端追踪写入、找受影响的订阅、定向补读、再加栅栏**：不依赖 g，但服务端要加 commit
   钩子、按行匹配订阅、给 System 打标；用户倾向把复杂度放到 SDK。
3. **客户端分两条消息发 `rpc` 和 `sync`**：两个包之间卡住或断线时，错误处理要分别对待（用户否决）。
4. **`rpcs` 的 `rsp` 本身等栅栏再发**：SDK 最省（只改命令名），但同一连接排在后面的回复和推送都要跟着等
   g + interval，断线时"结果未知"的窗口变长，还要改发送循环里等回复的逻辑。
5. **纯客户端按时间猜**（`rsp` 后等 interval + 余量）：零服务端改动，但每次都要等满，负载一高就抢跑。
6. **推送带 `_version`、回包带写入行的版本，客户端比版本**：客户端要预测范围订阅的进出（limit 截断、RLS），
   可见内容没变的写入不推导致等不到，改指纹又会增加推送量。
7. **等副本追上**（`WAIT`、读复制偏移）：要在 master 上执行，违背不碰 master 的约束；代理部署下不可用。

## 11. 已定事项（2026-10-10，用户）

- 方向：改 SDK 和协议（用户不多，可以接受协议变动），服务端只加通用栅栏。
- 客户端只发一个 `rpcs` 包；服务端照常回 `rsp`，提交后自己起栅栏，再发 `["sync", id]`。
- 可以不精确，但不能比今天差；不读 master。

## 12. 原待定项（2026-10-10，用户：按本稿的推荐）

1. g：Redis 20ms、SQLite 60ms（实测后改为 80ms，§8.1），先用类常量，不做配置项；SQLite 不改成"栅栏入队前先
   poll 一次通知表"。
2. 保险超时：20 个 interval（2 秒）。
3. `sync_id` 的范围 0..2^31-1，帧名 `sync`。
4. SDK 公开 API 叫 `CallSystemAwaitPush`。
5. 断线时已收到 `rsp` 的按成功完成。
6. 暂不加独立的客户端 `["sync", id]` 命令，也不做 sync 带 sub_id、服务端定向补读的精确模式（不依赖 g，相当于
   §10 第 2 条的轻量版）；以后需要时服务端走同一个入口。

## 13. 提交计划

1. 本设计稿。
2. red：hub 栅栏、门面 sync、发送循环、websocket `rpcs` 的用例。
3. hub `fence_` 与 `FENCE_DELAY`（基类、SQLite）。
4. 门面 `sync_` / `take_synced_` 与发送循环。
5. receiver `rpcs`。
6. SDK：`ClientBase` / `JsonbLayer` / Unity / Session / headless 与 C# 用例。
7. 文档（中英同一个提交）：`docs/zh|en/unity-client.md`（调用 System 一节）、`docs/zh|en/concepts.md`（订阅一致性
   一节）、SDK 的 `README.md` 与 `ClientSDK/unity/CLAUDE.md`（协议说明）、`hetu/skills/hetu-csharp-client/SKILL.md`、
   `hetu/llms.txt` 的 SDK 摘要、`AGENTS.md`（receiver 路由的命令）。
8. §8 实测，按结果调整 g。
