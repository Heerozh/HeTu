# Worker 级订阅器（同一 worker 内共享订阅）— 设计稿

- 日期：2026-09-28
- 状态：草稿，待审（§13 是已定事项）
- 分支：`perf/worker-subscriptions`（基于 dev `b0f13060`）
- 取代：PR #166（`perf/sub-shared-reads`，不合并）
- 分两个 PR：本 PR 是第一步，把订阅处理搬到 worker 级、行为等价（§4–§9）；第二步让同一查询在 worker 内
  共享一个订阅对象，另开 PR（§10，届时细化）。
- 影响范围（第一步）：`hetu/data/sub.py`（新增 `SubscriptionHub`；`SubscriptionBroker` 改为每连接的门面）、
  `hetu/data/backend/base.py`（hub 用的 MQClient 不做单连接频道数告警）、
  `hetu/data/backend/__init__.py`（`Backend` 持有并关闭 hub）、测试、`benchmark/`、`AGENTS.md` 与
  `base.py` 头部的结构图。
- **不改**：Lua、commit、客户端协议、keyspace 配置、通知接收器（`PubSubHub` / `SQLiteNotifyHub`）、三种
  订阅的比对逻辑、订阅保证的措辞（尾随重读设计稿 §3.6）；不新增 master 读写，不新增 PUBLISH。

## 1. 背景

### 1.1 问题：同一查询被很多连接订阅时，处理按连接数放大

订阅推送的流程：commit → 通知 → 每个 worker 一个通知接收器（`PubSubHub` / `SQLiteNotifyHub`）把通知分发进
worker 内订了该频道的**每个连接**的本地队列 → 各连接按自己的 tick 弹出、各自去副本读、各自比对 → 推送。

全服聊天（几千人订同一个"最近 N 条"）里，每条消息、每个连接都要做一遍：hub 分发、队列唤醒、range 读和
集合差、新行读、新行补读、行频道增删。PR #166 把读合并之后，N=1024 时每个连接每条消息仍要 234µs（原来
742µs），大头是集合差、MQ 唤醒、tick 记账、行频道增删，都是"每连接一份"的处理本身（#166 设计稿 §10）。

### 1.2 为什么不走共享读（PR #166）

共享读只共享读结果，每个连接仍有自己的 MQ 队列，也就是各自一条时间线。连接 B 要用连接 A 发出的读，就得
证明这次读对 B 的通知也够新（发出时刻 r ≥ 覆盖时刻 c + T）。为此引入了 hub 统一时间戳、覆盖时刻、
`known_at` / `may_skip`、`read_at` 只进不退、搭车 future 与失败回退、`HORIZON` 清扫……审查出的 15 条问题，
多数出在这些跨时间线的证明上。

### 1.3 思路

每个 worker 的每个 backend 只留一个订阅器、一个队列、一条时间线。今天一个连接内部已经是"多个订阅共用一个
队列、共用 `_prefetch_rows` 的预读"，正确性靠的正是这一点。worker 级订阅器等于把"一个连接"放大成"整个
worker"，尾随重读和补读的论证原样成立，不需要新的新鲜度证明。

在此之上，第二步让同一查询的订阅在 worker 内只有一个对象。共享的是订阅状态机（已推送状态 + 比对），不是
读结果；不写穿，不给事务用，也不是已放弃的行缓存（PR #144）。

## 2. 目标与非目标

第一步（本 PR）：
- 订阅的处理从连接级搬到 worker 级：每个 worker × backend 一个 `SubscriptionHub`（一个 MQ 队列 + 一个
  处理循环）。连接只剩门面（权限、sub_id、订阅数上限）和待发区。
- 行为等价：键一律带连接，每个订阅对象仍只属于一个连接；三种订阅的比对逻辑不动；全后端全量测试通过。
  与今天的差别列在 §4.7，其中有意改变的只有 tick 里的错误处理（§6，用户已定）。
- 私有订阅不变慢：用 cProfile 按函数与 dev 对比（§12）。顺带的收益：同一条通知在 worker 内只分发一次；
  同一 tick 里，worker 内各连接用到的同一行只读一次，每张表一次批量读。

第二步（另一个 PR，§10）：非 RLS 订阅按查询在 worker 内共享一个对象，后来的连接用快照加入，不读库。

非目标：
- 跨 worker 共享；
- RLS 订阅共享（第二步仍按连接私有，§10.7）；
- 改造通知接收器或 MQ 队列算法；
- 多个连接共用推送编码（msgpack 只做一次），以后再议。

## 3. 现状（dev `b0f13060`）

每个 ws 连接有：
- 一个 `HubMQClient`：本地队列，负责合批、尾随重读、`request_reread`、`DROP_AFTER`，挂在 worker 共享的
  通知接收器上；
- 一个 `SubscriptionBroker`：`_subs`（sub_id → 订阅）、`_channel_subs`（频道 → sub_id）。`get_updates`
  弹出一批后调 `_apply_notifications`：先 `_prefetch_rows`（本批行频道按表各一次 `get_many`，填进
  `RowSubscription` 的每 tick 缓存），再逐频道逐订阅 `get_updated` 并记账，tick 末尾统一订阅新增频道、给
  新订上的频道补读、退订没人要的频道；
- 一个 `subscription_handler` 协程：循环 `get_updates`，把 `["updt", sub_id, rows]` 放进 `push_queue`。

hub 分发一条通知，就是对订了它的每个连接各调一次 `push_pulled_`（`MQHub._dispatch`）。

## 4. 设计（第一步）

### 4.1 结构

```
每个 worker × backend：SubscriptionHub                      （新）
  ├─ 一个 MQClient：队列、合批、尾随重读，逻辑不变
  ├─ 一个处理循环：弹出一批 → 预读 → 各订阅 get_updated → 记账 → 扇出
  └─ 频道 → {订阅对象}；订阅对象记着它的成员（连接 → 该连接的 sub_id）

每个连接：SubscriptionBroker（门面，对外 API 不变）
  ├─ 权限检查、算 sub_id、同连接重复订阅、订阅数上限
  ├─ sub_id → 订阅对象
  ├─ 待发区 outbox {sub_id: {row_id: 行 | None}}，get_updates 取走
  └─ watch_channel 用的 MQClient（只做服务端内部关注，懒建）
```

`subscription_handler`、推送循环和 `receiver.sub_call` 不用改。

### 4.2 `SubscriptionHub`

- **取得**：`SubscriptionHub.of(backend)`，挂在 backend 上（`Backend.sub_hub_`）。第一次建
  `SubscriptionBroker` 时，在当前事件循环里懒建。`SubscriptionBroker(backend, hub=...)` 可以传入别的
  实例，供测试使用。
- **MQClient**：建 hub 时调一次 `backend.get_mq_client()`。今天是每个连接随机挑一个 servant 的通知接收器；
  现在一个 worker 的订阅通知都走同一个 servant 的 pubsub，跨 worker 仍随机分散。worker 内也不会再因为连接
  落在不同 servant 上而把同一条通知收几份。
- **关闭**：`Backend.close()` 先关 hub（取消处理循环、关 MQClient），再关 master / servant 连接。
- **处理循环**：按 tick 捕获异常（§6），自己不会结束。万一因 bug 结束，记错误日志，下一次 attach 时重新拉起。
- **手动模式（测试用）**：`SubscriptionHub(backend, autostart=False)` 不起后台循环，由门面的 `get_updates`
  自己弹出一批、跑一个 tick，节奏与今天每连接相同；几个门面并发驱动同一个手动 hub 时，一次只跑一个 tick。
  现有订阅用例依赖"不调 `get_updates`，通知就留在队列里"（`tick_with` 倒拨时刻、手工 `push_pulled_`、数
  本连接订了几个频道），这些用例给每个 broker 一个独立的手动 hub。生产、websocket 测试和新增的 hub 测试
  用后台循环。
- **告警**：hub 的 MQClient 订的是整个 worker 的频道，不做 `MAX_SUBSCRIBED` 单连接频道数告警，改由门面
  负责（§4.5）。
- **已生效的频道**：hub 另记一份 `_effective`，存 SUBSCRIBE 已经回来的频道。`MQClient.subscribed` 在
  SUBSCRIBE 发出时就记上，不能用来判断"是否已生效"（§4.4 的 fresh 判定要用）。

订阅对象上加几项 hub 的簿记（三种订阅的比对逻辑不动）：
- `members: dict[SubscriptionBroker, str]`：成员连接 → 该连接的 sub_id。第一步恒为一个。
- `active`：频道订阅生效、门面登记完成时才置真，之前 tick 不处理它（§4.6）。
- `closed`：最后一个成员离开时置真，处理到一半的 tick 据此丢掉结果。
- `token`：hub 内唯一的整数，定向补读用（§4.4）。

### 4.3 一个 tick

基本照搬今天的 `_apply_notifications`，差别都在"一个连接"变成了"整个 worker"：

1. **分组**：弹出的每一项，如果是真实频道，交给订了它的所有 `active` 订阅；如果是定向补读项（§4.4），只交给
   它指定的那个订阅，前提是该订阅还没 `closed`、还订着那个频道。结果是"订阅 → 它这批要处理的 (频道,
   payload)"，每个订阅内保持弹出顺序。本批涉及的频道若 hub 没订着（之前订阅失败），先按 §6 补订。
2. **预读**：各订阅要处理的行频道按表分组，每张表一次 `get_many`，填进每 tick 行缓存（沿用
   `RowSubscription` 的 ContextVar，处理循环的子任务继承同一个缓存）。同一行在 worker 内只读一次。
3. **按订阅并发处理**：每个订阅按顺序处理自己的频道，订阅之间并发。今天每个连接一个协程，天然并发；改成
   一个循环后如果逐个 await，各订阅读库的往返时间会累加起来。
   - 用 eager task（`asyncio.Task(..., eager_start=True)`；不能用 3.14 的 `create_task(..., eager_start=True)`，
     生产在 Linux / macOS 上跑的 uvloop 的 `create_task` 不收这个参数）：从缓存命中、不需要 I/O 的订阅当场跑完，
     不进调度；要读库的（range、整表订阅的 `get_many`）并发执行。每跑完一定数量就让出一次事件循环，
     不能长时间饿死接收 / 发送协程。
   - 处理某个频道前，先确认订阅还订着它。同一 tick 里，订阅自己的处理可能刚把这行放出范围；今天是逐频道
     现查 `_channel_subs`，现在分组在前，要补这一步。
   - 每次 await 回来先看 `closed`：处理期间被退订就丢掉结果，同今天的 `self._subs.get(sub_id) is not sub`。
   - 新增 / 移除的频道记到 hub 级的 added / released，并记下是哪个订阅新增的（补读要定向，见 §4.4）。
   - 更新按成员暂存：`staged[连接][sub_id] = (订阅, 合并后的更新)`。
4. **tick 末尾**：先订阅新增频道，订上后其中 fresh 的（§4.4）给**新增它的订阅**各发一次定向补读；等 SUBSCRIBE
   回来之后再定退订名单，退订放到后台、不等（交付不依赖它，跑起来时还会按频道表复查）。SUBSCRIBE 最多等
   `SUBSCRIBE_WAIT_INTERVALS` 个 interval（默认 1）：ack 迟迟不来（某个节点的 pubsub 连接半开，要靠 TCP
   keepalive 才发现）时照常交付，不能冻住整个 worker；晚回来的补读 / 失败后的补订由订阅任务自己做。
5. **提交暂存**：暂存的更新并进各成员的 outbox，并唤醒该成员。如果该连接的 sub_id 已不再指向这个订阅对象
   （退订了，或退订后用同一 id 重订），就丢弃。提交放在 tick 末尾有两个好处：推给客户端的新行一般在它的行
   频道订阅生效之后（同今天；SUBSCRIBE 等超时的除外，靠生效后的补读追上）；`get_updates` 拿到的总是完整的 tick。

代价是 **tick 屏障**：一个 tick 里所有订阅都处理完才提交、才弹下一批，最慢的订阅决定整个 worker 这一批
何时送达；今天慢订阅只拖自己的连接。正常负载下，tick 时长约等于本批总 CPU 加上最慢一次读的往返。事件
循环本来就是单线程，所以差别只在"最慢一次往返"这一项。会慢在读上的情形有：整表订阅的 `RESYNC`（只在
pubsub 断线恢复后出现）、limit 很大的 range、副本变慢。第一步先接受，§12 实测；如果有问题，再考虑 §11 的
"每订阅串行、无屏障"。

### 4.4 定向补读

今天的补读都进本连接自己的队列，只重读本连接的订阅。worker 只剩一个队列之后，如果还按频道补读，一个新
订阅的补读会让 worker 里所有订着这些频道的订阅都重跑一遍。拿聊天来说：第一步的键带连接，每个连接都有
自己的聊天订阅，于是每进来一个连接，就要重跑 1024 个行频道 × worker 内所有聊天订阅。

所以订阅层发出的补读一律**定向到单个订阅**：在同一个 MQ 队列里用虚拟键 `"\0{token}\0{频道}"` 入队（走
`request_reread`，表级频道的 row_id 照样放在 payload 里）。它和真实通知享有同样的 T 延迟、合批、尾随重读
和 `DROP_AFTER`，`MQClient` 一行不用改。弹出时只交给 token 对应的订阅处理那个频道。真实通知和断线重订的
`RESYNC` 仍按频道交给所有订阅。

调用点：
- `subscribe_get` 读完之后的补读；
- `subscribe_range` 生效之后的补读（索引频道 + 各行频道）；
- 整表订阅初始化期间攒下的 `pending`；
- tick 末尾新订上的频道（fresh）。

**fresh 的判定**：看新增的频道在本 tick 订阅之前是否已在 `_effective` 里。已生效的频道，此后的一切写入都会
有通知进 hub 的队列；订阅在本 tick 读了这行，tick 结束前就已登记，之后的通知都会交给它处理；而本 tick
弹出的通知离这次读已至少 T。所以只有尚未生效的频道需要补读。有两种做法不行：
- 按"本连接有没有订着"判断：今天这样做，是因为今天每个连接一个队列。
- 按 `MQClient.subscribed` 判断：别的连接正在 attach、SUBSCRIBE 还没回来的频道也会被当成已订着，于是漏掉
  补读。今天在同一连接内部也有这个窄窗口，worker 级之后会放大。

### 4.5 门面 `SubscriptionBroker`

对外 API 不变：`subscribe_get` / `subscribe_range` / `subscribe_table` / `begin_subscribe_table`、
`unsubscribe`、`get_updates(timeout)`、`count`、`close`、`watch_channel`、`make_query_id_`。

- **订阅流程**与今天同序，只是订频道、登记、补读改走 hub：
  - `subscribe_get`：attach（§4.6）→ 在门面登记 → 读 → `pushed` / `UNKNOWN` 照旧 → 定向补读。读的期间，
    tick 仍可能替它算好推送、并先于回复送达，由 `UNKNOWN` 处理，与今天相同。
  - `subscribe_range`：读 → 建订阅 → attach → 在门面登记 → 定向补读 → 返回。attach 之后到回复入队之间没有
    await，tick 不会先于回复推送它，与今天相同。
  - 整表订阅：attach（订阅处于"初始化中"，`get_updated` 只攒 `pending`）→ 登记 → 后半段在后台等 T、全量读、
    `finish_init_`、定向补读 `pending`，同今天。
- **`unsubscribe`**：先同步撤掉门面登记，以及 outbox 里该 sub_id 未取走的更新，再调 hub.detach：成员撤空时
  置 `closed`、撤登记，并退订没人要的频道。
- **`close`**：撤掉本连接全部订阅，关掉 watch 用的 MQClient；后端出错也不抛，同今天。
- **`get_updates(timeout)`**：outbox 空就等唤醒，非空就整个取走返回；`timeout` 按总时长计算，到时返回 `{}`，
  与今天相同。消费慢时（推送阻塞在 `push_queue` 上），几个 tick 的更新在 outbox 里按 sub_id / row_id 合并，
  后到的覆盖先到的。效果与今天本地队列按频道去重一样，内存上限就是该连接订阅的数据本身。SDK 删除不存在
  的行是空操作，所以"先进入后离开"合并成一个 None 没问题。
- **`watch_channel`**：第一次调用时为本连接建一个 MQClient，只做内部关注（顶号检测），随 `close` 一起关掉。
  回调语义不变，也不经过 hub 的队列。
- **`MAX_SUBSCRIBED` 告警**：按本连接订阅时登记的频道数估算（tick 里行进出范围不计），超过就告警。与今天
  一样只是告警。
- `count()` 和订阅数上限（`receiver.sub_call` 里按 `max_row_sub` 等检查）照旧按连接计算。

### 4.6 attach：先占位再订阅

hub 的 MQClient 由整个 worker 共用。有几个竞态今天只在单个连接内部存在，搬到 worker 级之后会被放大：

1. **退订与订阅交错**：订阅 A 正在等 SUBSCRIBE 回来，频道列表里有 X。此时 tick 末尾发现 X 没人要了（另一个
   订阅刚把 X 放出范围），于是退订 X。A 回来后登记 X，却再也收不到 X 的通知。今天这只会发生在同一个连接的
   两个订阅之间；worker 级之后，任意两个连接之间都会发生。
   做法：attach 先把订阅以 `active=False` 登记进频道表（占位），再 await 订阅。tick 和其他 detach 定退订
   名单时看得到占位，就不会退订这些频道。tick 分组时跳过未生效的订阅；它生效之前的通知由它自己的补读覆盖，
   同今天。
   `active` 由门面在订阅回来之后、与门面登记同一个同步段里置真，不能由订阅任务自己置：否则 tick 可能在门面
   登记之前就处理它，推送因 sub_id 还没登记被丢掉，指纹却已经更新，之后的补读读回同样内容也不会再推，客户端
   就一直停在初始行上。
2. **一个连接的取消撤掉别人的订阅**：`HubMQClient` / `PubSubHub` 按 MQClient 计数登记。共用一个 MQClient
   之后，连接 A 在等 SUBSCRIBE 时被取消（连接断开），会把这个 MQClient 对 X 的登记连同搭车的连接 B 一起
   撤掉。
   做法：对 MQClient 的 `subscribe` 放进 hub 自己的任务里跑（跑完把频道记进 `_effective`），调用方只旁观地
   等它。门面被取消时，订阅照常完成，再由门面的清理 detach 掉自己的占位，没人要就退订。
3. **重叠的订阅一次失败撤掉另一次的频道**：同样因为按 MQClient 登记，两次 subscribe 在同一频道上重叠时，
   一次失败（比如 [C, D] 里 D 在连不上的节点上）回滚"本次新登记的频道"，会把另一次搭车订上、已经返回
   成功的 C 一并撤掉，那个订阅 active 却再也收不到通知。
   做法：hub 里同一频道同时只发一次 subscribe（`_inflight`）：已生效的跳过，正在订的等那一次的结果（它
   失败，等它的都失败），其余合成一次发出。退订时把在途的那次作废，它回来也不算生效，之后再要这个频道的
   另发一次。

### 4.7 与今天行为上的差别

1. tick 里出错不再断开连接（§6，用户已定）。
2. 同一 tick 里的行读在 worker 内共用（论证见 §5）。
3. fresh 按 hub 已生效的订阅判定，比今天更准（§4.4）。
4. 消费慢时 outbox 合并多个 tick；今天是队列按频道去重，效果相当。
5. 一个 worker 的订阅通知都走同一个 servant 的 pubsub（§4.2）。
6. tick 屏障（§4.3）。

## 5. 正确性

核心论证不变（尾随重读设计稿 §3、§4）：对每个订阅频道，在它的最后一条通知之后、以及订阅生效之后，都一定
有一次读，其发出时刻晚至少 T。

| 场景 | 今天 | 第一步 |
|---|---|---|
| 单条通知、合并、尾随重读 | 连接队列里按频道处理 | hub 队列里按频道处理，同一份代码 |
| 同一 tick 里几个订阅读同一行 | 连接内共用预读 | worker 内共用预读。这些读都在本 tick 弹出之后发出，离各自要覆盖的通知都已至少 T |
| 订阅生效前的写（行 / 索引订阅） | 本连接队列里补读 | 定向补读，时刻与今天相同 |
| 整表订阅初始化 | 先订阅，等 T 再全量读，`pending` 重读 | 同，`pending` 走定向补读 |
| 行新进入范围（fresh） | 本连接没订着就补读 | hub 尚未生效地订着就定向补读；已生效的见 §4.4 |
| pubsub 断线期间的写 | 重订后 `RESYNC` 分发到各连接队列 | 分发到 hub 队列，处理所有订阅 |
| SQLite | 同上 | 同上 |

退订、重订与 tick 中途交错：`test_backend_sub_race.py` 今天覆盖的三种情形，在 hub 里逻辑相同——退订名单
等 SUBSCRIBE 回来再定；快照里已被退订的订阅跳过；处理期间被退订的丢掉结果。另外加上 §4.6 的两种。

## 6. 错误处理（用户已定：重读，不断开）

今天 tick 里读或订阅出错，异常从 `get_updates` 抛出，`subscription_handler` 断开这个连接。搬到 worker 级
之后，一个订阅出错不该牵连别人，改为：

- **订阅处理出错**（`get_updated` 里读失败等）：记日志，给这个订阅定向补读那个频道（带原 payload），T 之后
  重试；本 tick 其余照常。
- **预读出错**：只是出错的那张表不填缓存，它的订阅在 `get_updated` 里各自单行读，读不出的按上一条处理。
  一行坏数据（如 `direct_set` 不做类型检查写进去的值）只卡住订了它的订阅，不牵连同批别的行、别的表；
  以前整批原样重新入队，同批的行跟着一次次重试，永远推不出去。
- **tick 末尾订阅新增频道出错**：频道仍留在频道表里，按真实频道重新入队。之后弹出时，如果 hub 还没订着它，
  这些项本 tick 不处理，交给后台补订（不等它回来，别卡住本批别的交付）：成功则再按真实频道补读一次（T 之后
  读，不在当下读），失败则再次入队。
- **退订出错**：通知接收器本来就会吞掉并记日志，这里不另外处理。
- **重试要能算出同样的结果**：`IndexSubscription._rerange` 原来在读新进入的行之前就改了
  `last_range_result`，读失败后重试会算不出这次的进出（新行不推、离开的行留在 `row_subs` 里）。以前出错
  就断连接、状态跟着丢，没暴露；改为读完才改状态。其余 `get_updated` 都是先读后改。
- **日志限流**：首次带栈，之后按间隔汇总条数，免得 Redis 挂掉时每个 tick 都刷屏（同 SQLite 轮询的做法）。

行为变化：Redis 出错时，连接不再因订阅路径被断开；恢复后读自然继续，断线重订的 `RESYNC` 照旧补齐。RPC
路径的错误处理不变。`subscription_handler` 里按 Redis 错误断开的分支从此不会走到，保留无害。如果是代码 bug
导致持续异常，会每 T 重试一次、限流记日志，订阅停在旧数据上，但不会拖垮处理循环。

## 7. 成本（预期，实测见 §12）

- **master**：零新增读写；PUBLISH、Lua、复制流不变。
- **通知分发**：每条通知在 worker 内只 `push_pulled_` 一次；今天是订了它的每个连接各一次。
- **副本读**：行预读在 worker 内按 tick 合批去重；今天是每个连接每 tick 各读一次。range 和整表订阅的读，第一步
  仍按订阅各读各的，第二步才合并同一查询。
- **每个有更新的成员**：每 tick 一次暂存合并加一次唤醒，替代今天每个连接一个队列的弹出与 tick 记账。
- **频道增删**：hub 级按 tick 合批一次；今天是每个连接各自调 hub 的 add / remove。
- **内存**：每连接的本地队列没了；每个 tick 多一份暂存，按有更新的成员计。
- **任务**：每 tick 每个受影响的订阅一个 eager task，缓存命中的当场跑完、不进调度。今天是每个连接的协程被
  唤醒一次，量级相当，§12 实测确认。
- **延迟**：tick 屏障，见 §4.3。

## 8. 测试计划（先写 red）

新增：按后端参数化的放在 `tests/test_backend_sub_hub.py`；竞态用假 pubsub，放在
`tests/test_backend_sub_race.py`。数读次数的用例各自建独立的 hub，免得同模块其他用例留下的订阅干扰计数。

- 同一 backend 上多个门面共用一个 hub；同一条通知在 hub 队列里只入一次（数 `push_pulled_`）。
- 多个连接订同一行：一次写入，一个 tick 里只读一次（数 `get_many` / `get`），各连接都收到推送。
- 定向补读：新订阅的补读不会让别的连接订着同一频道的订阅重跑（数 `get_updated` 调用次数）；整表订阅的
  `pending` 补读不会给别的连接重推。
- fresh：hub 正在等某频道的 SUBSCRIBE 时，tick 里另一个订阅新增了同一频道，仍会给它定向补读。
- `get_updates` 拿到的是完整的 tick；消费慢时几个 tick 按 sub_id / row_id 合并；退订后 outbox 里该 sub_id
  未取走的更新被丢掉；tick 中途退订再用同一 sub_id 重订，拿不到旧订阅那一 tick 的更新。
- §4.6 的两种竞态：
  - 占位挡住退订。red：先订阅后登记时会丢频道。
  - 一个连接在等 SUBSCRIBE 时被取消，不影响搭车的连接。
  - 订阅回来到门面登记之间的 tick 不处理它。red：由订阅任务置 `active` 时，这期间的推送丢了、指纹却已更新，
    之后的补读不再推。
- tick 并发：一个订阅的读卡住时，其他订阅的读照样已经发出，不是串行。
- 错误处理：
  - `get_updated` 读失败时不断开，T 之后重试并推到；
  - 预读失败时改为逐行读，一行读不出来不卡住同批别的行；
  - tick 末尾订阅失败后，之后能补订并补读；
  - 日志限流。
- hub 生命周期：`Backend.close()` 关掉处理循环；循环意外结束后，下一次 attach 会重新拉起。
- 手动模式：不调 `get_updates` 时通知留在队列里；两个门面并发驱动同一个手动 hub，不会同时跑两个 tick。

改写：
- `tests/test_backend_sub.py` 的 `broker` fixture 及直接建 broker 的几处，改为每个 broker 一个独立的手动
  hub；`broker._mq_client` 改为 `broker._hub.mq`（`tick_with`、`count_notifications`、`test_mq_backlog`、
  数频道的断言）。其余用例只走公开 API，照旧。
- `tests/test_backend_sub_race.py` 的三个用例改在 hub 上跑。订阅处理并发之后，原来"判断先处理谁"的写法改成
  "两者都在处理中"。
- `tests/test_backend_pubsub_hub.py` 里看 `broker._mq_client` 的断言，改看 watch 用的 MQClient。
- `test_arch_master_reads`、`test_arch_publish`、`test_master_read_budget`、`test_websocket`、
  `test_endpoint_connection` 全部通过。

## 9. 提交计划

每步先提交 red，再提交实现；每个实现提交之后全部测试通过。

1. `docs(spec)`：本设计稿。
2. `test(mq)` → `feat(mq)`：hub 用的 MQClient 可以关掉 `MAX_SUBSCRIBED` 告警。
3. `test(sub)` → `refactor(sub)`：`SubscriptionHub`（含手动模式）和门面、定向补读、fresh 判定、tick 暂存与
   提交、§4.6 的两种竞态，以及测试 fixture、辅助函数与竞态用例的改写。
4. `test(sub)` → `fix(sub)`：错误处理（§6）。
5. `bench`：压测脚本（从 `perf/sub-shared-reads` 拿 `benchmark/sub_fanout_chat.py`，去掉共享读的开关），
   对比 dev 与本分支，结果补进 §12。
6. `docs`：`AGENTS.md`、`base.py` 头部的结构图、相关 docstring（中英双语）。

## 10. 第二步：同一查询在 worker 内共享（另一个 PR，届时细化）

**10.1 键**：`(table_ref, 查询, 可见性)`。
- 查询：`subscribe_get` 为 row_id；`subscribe_range` 为 `(index_name, repr(left), repr(right), limit, desc)`；
  整表订阅为 `"table"`。
- 不用 sub_id 字符串做键：`right=None` 与字符串 `"None"` 会拼出同一个 sub_id。今天只会在同一连接内撞；共享
  之后会让别人拿到另一个查询的结果。`repr` 在 None / bool / int / float / str 之间不会撞；`1` 与 `1.0` 分成
  两个键，只是少共享一些。
- sub_id 仍按 `make_query_id_` 给客户端，记在各成员上。SDK 的 `MakeSubId` 在预测它，格式不能动。
- 可见性：组件不是 RLS、或 ctx 是 admin 时为 None（共享）；否则为该连接（私有）。

**10.2 已推送状态**：订阅保存成员手里的完整行，而不只是指纹，供后来者做快照。
- 行订阅：一行。
- 范围订阅：按 id 存可见行；快照按索引顺序输出，同 range 回复。
- 整表订阅：按 id 存可见行，取代 `known_ids`。

每 worker 每个键一份。整表订阅最多 `MAX_TABLE_SUBSCRIPTION_ROWS` 行；今天则是每连接一份 `known_ids`。

**10.3 原子加入**：等订阅就绪后，取快照、登记为成员、回复入队，三步在同一个同步段里完成。扇出按提交那一刻
的成员表进行，中途加入的连接不会收到快照里已经包含的更新。为此，`get_updated` 在修改已推送状态和返回之间
不能有 await：`_rerange` 先读完 range 和新行再一次性提交，第一步为了重试已经这样改了（§6）。

**10.4 初始化**：
- 第一个成员建订阅，初始读放在 hub 自己的任务里跑，不挂在第一个连接上，它断开不会连累别人。
- 后来者 `shield` 着等订阅就绪。
- 初始化期间订阅已占位，但不处理通知；就绪时对全部频道定向补读一次，补读时刻晚于这期间的所有通知。这样
  `subscribe_get` 的 `UNKNOWN` 处理和加入者的补读都不再需要。
- 等待的人全部离开时，取消初始化、撤掉占位。

**10.5 离开**：最后一个成员离开就撤掉订阅，不设保留期。

**10.6 同一连接的重复订阅**：仍按 sub_id 认出来，回复用快照，不再读库。

**10.7 RLS**：可见性取决于 ctx，而 `rls_check` 每次都读 ctx 当时的属性（ctx 会变），所以按连接私有，行为同
今天。以后如果"用 RLS 做隐私的公会聊天"这类同查询扇出成了问题，可以改成共享原始状态、按成员做 RLS 过滤并
各自记可见集；本稿不做。

**10.8 `subscribe_get` 按非 id 索引查**：每个加入者自己查一次 row_id（range limit 1），再按 row_id 加入。

**10.9 测试**：
- N 个连接订同一查询时，每条消息在 worker 内只有一次 range 读、一次新行读、一次新行补读；
- 快照等于成员持有的状态；
- tick 中途加入；
- 最后一个成员离开时撤掉订阅；
- 初始化中的等待与取消；
- RLS 订阅仍私有。

压测与第一步对比。

**10.10 文档**：`benchmark/sub_budget_result.md` 的成本模型，以及 concepts / operations 的相关段落。

## 11. 备选方案与否决理由

- **共享读（PR #166）**：见 §1.2。
- **每个共享订阅自己一个队列和循环**：私有订阅就变成每个订阅一个协程，比今天每连接一个还多；worker 内的预读
  合批也没了。
- **补读按频道、不定向**：一个新订阅的补读会让 worker 里所有订着同一批频道的订阅都重跑（§4.4）。聊天里每进
  一个连接，就是 1024 × 订阅数次比对。
- **每订阅串行、没有 tick 屏障（每个订阅一个邮箱）**：能消掉 §4.3 的屏障，但要拆掉 tick 的合批和"一个 tick
  一批更新"的语义。第一步不做；§12 实测屏障有问题再议。
- **扇出直接放进 `push_queue`**：处理循环不能 await 任何连接，而 `put_nowait` 遇到队列满只能丢弃或断开；
  outbox 合并有上限，也不丢。
- **读失败断开成员**：用户定为重读、不断开（§6）。
- **hub 队列按"频道 + 目标集合"入队（改 `MQClient`）**：效果与虚拟键相同，但要改队列、尾随重读和
  `DROP_AFTER` 的数据结构；虚拟键不用改一行队列代码。

## 12. 实测（第一步）

环境：Windows 11 + Docker Redis（主 + 1 副本，读走副本），单进程 = 1 个 worker，不含 ws 推送编码。
同一个驱动脚本分别用两份代码的环境跑：dev（`b0f13060`，主仓库环境
`uv run --project C:/xsoft/HeTu --no-sync python -P`）与本分支。本机多次运行的 CPU 在 ±15% 内波动。

**聊天扇出**（`benchmark/sub_fanout_chat.py`：200 连接都订"最近 N 条"，每秒 2 条消息共 10 条。
第一步的键带连接，聊天还没有共享）：

| 每条消息 | N=1024 dev | 本分支 | N=50 dev | 本分支 |
|---|---|---|---|---|
| worker CPU（每连接） | 656–750µs | 609–656µs | 289µs | 195µs |
| 副本 CPU | 31.3–31.8ms | 25.3–25.7ms | 17.9ms | 12.7ms |
| 副本读命令 | 200 ZRANGE + 400 HGETALL | 200 + 201 | 200 + 400 | 200 + 201 |
| 订阅 + 补读消化完的 CPU（每连接） | 39.4ms | 35.8ms | 2.2ms | 1.9ms |
| 常驻内存（每连接） | 864KB | 510KB | 50KB | 34KB |

- HGETALL 少掉的一半是新行的补读：新行频道只在 hub 第一次订上时补读一次，读在 worker 内只发一次。
- 订阅的墙钟耗时本分支更长（N=1024：39ms 对 22ms），是因为后台循环在订阅期间就把补读消化了，dev 要等
  消费协程起来才处理；按"订阅 + 补读消化完"算 CPU，本分支更省。
- cProfile（计时窗口，按函数对比）：两边总 tottime 持平。每连接的 `get_message`、`get_updates` 弹出、
  `_prefetch_rows`、`reset_cache_` 换成 hub 的 `_process`；`hgetall_many_` 批次减半。剩下的大头是每个
  订阅各自的 `_rerange`（1024 个 id 的解码与集合差），第二步共享后消掉。

**AOI**（`benchmark/sub_budget.py`：200 连接、每连接订自己 zone 的 50 行、每秒 300 次写）：

| 每次交付 | K=10（20 个 zone）dev | 本分支 | K=1（200 个 zone，纯私有）dev | 本分支 |
|---|---|---|---|---|
| worker CPU | 106µs | 27–31µs | 254µs | 164µs |
| 副本 CPU | 27µs | 8.5–8.7µs | 101µs | 75µs |
| 副本命令 | 1.18 | 0.26 | 2.53 | 2.53 |
| 写→交付延迟 p99 | 139ms | 109ms | 108ms | 109ms |
| 事件循环卡顿 p99 | 9.5ms | 8.7ms | 7.8ms | 8.8ms |
| 常驻内存（每连接） | 62KB | 41KB | 86KB | 81KB |

- K=10：同一行的通知在 worker 内只分发一次，同一 tick 的行预读 worker 内只读一次。
- K=1：命令数一样（没有可共享的），但各连接的行读在一个 tick 里并成一批，往返少了，每连接的队列唤醒和
  tick 记账也没了。
- `sub_budget.py` 的采样协程原来读每连接的 `broker._mq_client.pulled_deque`，改为两份代码都能取
  （worker 级之后读 hub 那一个队列）。

## 13. 已定事项（2026-09-28）

1. 所有订阅都走 worker 级，只有一条代码路径。
2. RLS 订阅按连接私有（第二步也是）。
3. tick 里读失败：记日志、重新入队重读，不断开成员。
4. PR #166 不合并。
5. 分两个 PR：第一步等价搬迁（本稿 §4–§9），第二步开启共享（§10）；一份设计稿覆盖两步。
