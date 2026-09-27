# 订阅读合并（同一 worker 内共享通知触发的读）— 设计稿

- 日期：2026-09-28
- 状态：实施中（§11 已定）
- 分支：`perf/sub-shared-reads`（基于 dev `b0f13060`）
- 影响范围：`hetu/data/backend/base.py`（`MQHub` 统一通知时刻、记频道时刻；`MQClient` 队列记覆盖
  时刻、`request_reread` 取 hub 给的覆盖时刻）、`hetu/data/backend/redis/pubsub.py` + `redis/mq.py`
  （订阅生效时刻）、`hetu/data/backend/sqlite/mq.py`（同）、新模块 `hetu/data/shared_reads.py`
  （共享读层）、`hetu/data/sub.py`（读路径改走共享层、补读带数据读的时刻）、`benchmark/`（聊天扇出
  压测）、测试。
- **不改**：Lua、客户端协议、keyspace 配置、订阅语义与保证措辞（尾随重读设计稿 §3.6）；不新增任何
  master 读写、不新增 PUBLISH；System / 事务的读不受影响。

## 1. 背景

### 1.1 问题：同一查询被很多连接订阅时，读按连接数放大

订阅推送的流程：commit → Redis 通知 → 每个 worker 一个 hub（`PubSubHub` / `SQLiteNotifyHub`）收到后
分发进本 worker 内订了该频道的各连接的本地队列 → 各连接按自己的 tick 弹出、**各自**去副本重读 → 推送。

同一条通知在一个 worker 内扇出到 K 个连接，就是 K 次一样的读。全服聊天（几千人订同一个"最近 N 条"）
是最典型的情况。以 `examples/chat` 客户端的写法
`WatchRange<ChatMessage>("id", 0, long.MaxValue, 1024, desc: true)` 为例，每条消息、每个连接要做：

1. 索引频道通知 → `_rerange`：`ZRANGE` 取 1024 个 id（约 35KB 回复），Python 端解析后做两次集合差；
2. `_rerange` 里 `get_many` 读新进入的那一行；
3. 下一个 tick：新订上的行频道补读一次（`_apply_notifications` 的 `fresh`，bb21a440）。它只在有行
   新进入结果时触发，改行、行离开不触发；聊天每条消息都是新行进入。

### 1.2 实测

单进程（= 1 个 worker）200 连接、每秒 2 条消息，Windows 11 + Docker Redis 8.10，不含 ws 推送编码
（脚本随本分支进 `benchmark/`，见 §8）。"合并上限"是给 servant 的读包一层"同参数 50ms 内合并"
的粗估，语义不对（见 §9），只用来看收益上限。

| 每条消息 × 每连接 | limit 1024 现状 | 合并上限 | limit 50 现状 | 合并上限 |
|---|---|---|---|---|
| worker CPU | 602–688µs | 242µs | 273–297µs | 70µs |
| Redis CPU | 148µs | ≈0 | 83µs | ≈0 |
| Redis 出网 | 34.5KB | ≈0 | 2.2KB | ≈0 |
| 其中新行补读（关掉它对比） | 110µs，Redis 23µs | | 85µs，Redis 20µs | |

- 现状每条消息每连接 1 次 ZRANGE + 2 次 HGETALL；合并后每个 worker 每条消息约 2 次 ZRANGE、2 次
  HGETALL，Redis 每条约 1.8ms（含 commit）。
- 换算 3000 人、limit 1024：每条消息约 2 核·秒 worker CPU、0.44 核·秒 Redis、约 100MB 出网。读占
  worker CPU 的 65–76%，不只是副本读和 RTT。
- 附带：limit 1024 每连接常驻约 866KB（limit 50 为 51KB），订阅时读 1024 行约 23ms。

### 1.3 为什么不是已放弃的行缓存

行缓存（PR #144，2026-09-24 放弃）卡在两个问题上：

1. 这条 keyspace 通知是不是本进程自己那次写的？——决定写穿进缓存的行能不能留。
2. 这次副本读回来的行够不够新？——决定能不能放进缓存。

本设计只共享**订阅因通知而发的读**，不写穿、不给事务用，问题 1 不存在。问题 2 已有现成答案：尾随
重读设计稿（`2026-09-23-sub-trailing-reread-design.md` §3、§7）的时间下限——通知之后至少隔 T 才读，
副本复制延迟在 T 内就读得到。共享只是让"满足这个下限的一次读"被同一 worker 里需要它的连接共用，
不引入新的概率假设，不需要 PUBLISH、版本号、master 读或数通知条数。

## 2. 目标与非目标

目标：
- 同一 worker 内，同一条通知引起的同一份读（同一行、同一个 range 查询）只发一次，结果给所有需要它的
  连接用；
- 订阅生效后的补读、新行进入后的补读：能证明不需要时不读，需要时同样按 worker 合并；
- 保持尾随重读设计稿 §3 的核心不变量与 §3.6 的保证措辞。

非目标：
- 进程级通用行缓存、写穿、事务读走缓存；
- 合并每连接的其余工作（范围结果的集合差、推送编码、MQ 唤醒），见 §9；
- 订阅时的初始读（`subscribe_get` / `subscribe_range` / `subscribe_table` 回复里的数据）走共享，见 §9；
- 跨 worker 共享。

## 3. 术语与判据

- **T**：`1 / MQClient.UPDATE_FREQUENCY`，队列的 interval，也是复制延迟预算。调用时读取（测试会改它）。
- **通知时刻**：hub 收到这条通知的时刻（`time.monotonic()`）。每条通知只取一次，分发给所有连接的是同
  一个值（§4.1）。
- **覆盖时刻 c**：一个连接这次弹出某频道时，要保证读到的最晚那条通知（或订阅生效时刻）的时刻（§4.2、
  §4.5）。
- **发出时刻 r**：一次读发给后端之前取的 `time.monotonic()`。

**共享判据**：某个 key 的一次读，发出时刻 r ≥ c + T，就可以给覆盖时刻为 c 的连接用。

理由：尾随重读的保证就是"通知之后至少 T 发出的读读得到它"。连接自己去读，也不过是在 c + T 之后发
一次；任何满足 r ≥ c + T 的读给出同样的保证。r 取在发送之前，只会比真实发送早，偏保守。

## 4. 设计

### 4.1 hub 每条通知只取一次时间戳

`MQHub._dispatch` 取一次 `now`，传给每个连接的 `push_pulled_(channel, ids, stamp)`；`_enqueue` 用它
作入队时刻，不再各自取 `time.monotonic()`。同时记 `_last_notified[channel] = now`（§4.5 用），频道
从 hub 撤掉时在 `MQHub._release` 里一并清掉（不靠子类的 `_on_channel_gone`，SQLite 覆盖了它）。
`resync_` 走 `_dispatch`，同样处理。`push_pulled_` 的 `stamp` 缺省为现在，给直接调用它的测试。

为什么要统一：现在同一条消息在分发循环里给 N 个连接各记一个递增的时刻（实测每连接 1.2–1.6µs，1000
连接首尾差约 1.5ms）。连接只知道自己的时刻，分不清别人早一点的那个是不是同一条消息，覆盖时刻只能取
自己的。模拟（真实 `push_pulled_` / `get_message`，每连接处理 100µs，读 0.5ms），每条通知实际发出的
读次数：

| 每 worker 连接数 | 50 | 200 | 500 | 1000 |
|---|---|---|---|---|
| 逐连接时刻，Windows 默认计时器 | 1 | 1 | — | 1 |
| 逐连接时刻，1ms 计时器（接近 Linux epoll） | 1 | 1 | 1（最多 3） | 中位 2（最多 3） |
| hub 统一时刻 | 1 | 1 | 1 | 1 |

事件循环按批唤醒，首读受的影响不大；统一后变成确定的 1 次，也好测。真正受影响的是按各连接自己的处理
时刻计时的补读，见 §4.2 的尾随重读和 §4.5。

hub 时刻是真正收到的时刻，不晚于原来各连接的入队时刻；按它睡 T，仍满足尾随重读的不变量。

### 4.2 `MQClient`：队列里记覆盖时刻

队列（`pulled_deque`）里的时刻仍是"什么时候醒"：排序、合批、`DROP_AFTER` 都按它。另加
`_cover[channel]` 记"这次弹出要覆盖到的时刻"。两者多数时候相同，尾随重读和补读时不同。

- 新入队：`_cover[ch] = 覆盖时刻`（通知为 hub 时刻；补读见 §4.5）。
- 合并进已在队列里的项：`_late[ch] = max(_late[ch], 覆盖时刻)`。以前记的是合并那一刻的 `now`，统一
  时间戳后通知就是 hub 时刻；补读合并进来时用它自己的覆盖时刻（可能早于现在，§4.5）。
- 弹出（`cutoff = now - T`）：
  - 没有合并进来的，或 `_late[ch] <= cutoff`：本次覆盖时刻取 `max(_cover[ch], _late[ch])`，不尾随。
  - `_late[ch] > cutoff`：本次覆盖时刻取 `_cover[ch]`；尾随项的**醒来时刻**为现在（保持队列按时间
    有序，同今天），**覆盖时刻**为 `_late[ch]`。合并进来的那些由尾随重读覆盖，不变量不变。
- `DROP_AFTER` 清理时一并清 `_cover`。
- `get_message()` 返回形状不变（`{频道: payload}`）；新加 `get_batch()` 返回
  `{频道: (payload, 覆盖时刻)}`，`SubscriptionBroker` 改用它。

效果：同一条通知在同一 worker 所有连接里的覆盖时刻相同；尾随项的覆盖时刻是迟到那条的 hub 时刻，也
相同。今天尾随重读按各连接自己的弹出时刻入队，这些时刻散布在整波处理时间里，基本共享不上。

取舍：`_late[ch] > cutoff` 时，本次弹出不保证读到 cutoff 之前合并进来的通知（今天现读会读到），由
尾随重读在 T 之后补上。热频道（每 T 不止一条通知）的中间推送可能比今天旧一点，最终停留的值不变。反过来
若这时覆盖时刻取 cutoff，就只能用本连接弹出之后才发的读，同一批里后弹出的连接都用不上前面的读，热频道
等于不合并。

### 4.3 共享读层 `SharedReads`

新模块 `hetu/data/shared_reads.py`。每个 `Backend` 一个实例（按 backend 登记，`SharedReads.of(backend)`）；
`SubscriptionBroker` 默认取它，构造参数可以传入别的实例或关掉共享（测试、压测对比用）。

- key：
  - 行：`(table_ref, row_id)` → 原始行（`TYPED_DICT`，含 `_version`）或 `None`；
  - 范围：`(table_ref, index_name, left, right, limit, desc)` → id 列表（`ID_LIST`）。
- 每个 key 只留最近一次读：`(发出时刻 r, 结果)`。更晚的读满足的覆盖时刻只多不少，旧的直接替换掉。
- 取数：
  - `range_ids(table_ref, query, cover)`、`rows(table_ref, [(row_id, cover), ...])`；
  - 有条目且 `r >= cover + T`：用它（在途的就等它），不再发；
  - 否则取 `r = time.monotonic()`，向 `backend.servant`（随机副本，同今天 `_prefetch_rows`）发一次读，
    登记后等它；
  - 多行：命中的行直接用，没命中的并成一次 `get_many`，每行各自登记（共享同一个批次）；
  - 返回结果时一并返回所用读的发出时刻（§4.4、§4.5 要用）。
- 读在独立 task 里跑，等待方只旁观（`asyncio.wait`）：发起它的连接断开被取消，不影响别的等待方。
- 读失败：发起方照常抛出；搭车的等待方各自改为自己读一次（再失败才抛）。否则一次偶发的读错误会让整个
  worker 里共享它的连接一起断开（今天只断发起的那一个）。失败的条目不留。
- 共享出去的结果只读。`TableSubscription.get_updated` / `_resync` 里就地 `del row["_version"]` 改为
  拷贝；`RowSubscription.decode_row_` 已经拷贝；`_rerange` 的 `set(row_ids)` 本来就是新集合。
- 保留期：条目只要满足判据就一直可用，与年龄无关。为控内存，发出超过 `HORIZON`（暂定 10T，即 1s）的
  条目惰性清掉，清掉只是多一次读。
- 运行中的事件循环换了（测试按模块换 loop）就清空全部条目：future 不跨 loop。
- 计数：命中、发出、回退，给测试和压测用。

### 4.4 订阅读路径改走共享层

`_apply_notifications` 拿到 `{频道: (payload, 覆盖时刻)}`：

- `_prefetch_rows`：本批行频道按表分组，逐行带各自的覆盖时刻走 `SharedReads.rows`，结果照旧填进本
  tick 的 `RowSubscription` 缓存。
- `get_updated(channel, payload, cover)` 多一个参数；缺省 `None` 表示不走共享、直接读，保持直接调用
  它的测试语义。
  - `RowSubscription.read_`：本 tick 缓存没有时，按 cover 走共享单行读；
  - `IndexSubscription`：索引频道 → `_rerange(cover)`；行频道 → `read_(channel, cover)`，值频道离开
    时 `_rerange(cover)`；
  - `_rerange(cover)`：范围 id 走 `range_ids`，进入的行走 `rows`；
  - `TableSubscription`：payload 里的 id 走 `rows`；`RESYNC` 的整表重读不共享（只在断线恢复时发生）。
- 读哪个 servant 由共享层随机选，与今天 `_prefetch_rows` 一样（今天 `_rerange` / `read_` 用订阅时选定
  的 servant；频道名与 servant 无关）。

**每个订阅用到的读只进不退**：订阅记住它用过的最新一次读的发出时刻（`RowSubscription.read_at`、
`IndexSubscription.range_read_at`、`TableSubscription.read_at`），取数时覆盖时刻取
`max(弹出给的覆盖时刻, read_at - T)`，用到的共享读就不会比它已经用过的更早发出。`_prefetch_rows` 对
一个频道取该频道上各订阅的最大值。没有这条的话，一个频道积压的旧通知（覆盖时刻早）可能拿到比本连接
刚在 `_rerange` 里读过的更早的读，客户端短暂倒退：之后的通知会纠正，但今天不会有这种倒退。

### 4.5 补读：覆盖时刻由 hub 给出，能证明不需要时跳过

今天几处补读的覆盖时刻都是"调 `request_reread` 的那一刻"，各连接不同：订阅生效后的补读
（`subscribe_get`、`subscribe_range`）、新行进入后的补读（`_apply_notifications` 的 `fresh`）。它们
盖的是"数据读与本连接订阅生效之间"的空档，空档里可能漏掉两类写：

- hub 自己订上之前的写：要读到它们，读须在 hub 订阅生效 + T 之后发出；
- hub 已经收到、但在本连接登记之前分发掉的通知：读须在该通知 + T 之后发出。

hub 知道这两样：

- **订阅生效时刻** `eff[ch]`：Redis 取 `SUBSCRIBE` ack 到达的时刻（`AsyncKeyspacePubSub` 监听协程
  收到 ack 时记；退订、节点失效时清掉；重订成功后是新的 ack 时刻）；SQLite 取水位记下的时刻。ack 到达
  不早于真正生效，偏保守。
- **最后一条通知时刻** `_last_notified[ch]`（§4.1）。

`request_reread(*channels, payload=None, known_at=None, may_skip=True)`，`known_at` 是本连接手里这份
数据那次读的发出时刻。对每个频道：

```
hub 对该频道没有生效的订阅（还在订、节点失效中）→ 覆盖时刻 = 现在（同今天）
否则 h = max(eff[ch], _last_notified[ch])
  known_at 为 None                 → 覆盖时刻 = 现在（同今天）
  may_skip 且 h <= known_at - T    → 不补读
  否则                              → 入队：醒来时刻 = 现在，覆盖时刻 = max(h, known_at - T)
```

- 覆盖时刻 ≥ h，满足判据的读读得到空档里那两类写。
- 跳过时 `known_at - T >= h`，数据读本身就满足判据。
- 覆盖时刻 ≥ `known_at - T`：用到的共享读不早于这份数据的读（同 §4.4 的只进不退）。
- 醒来时刻为现在 ≥ 覆盖时刻，弹出时现读也满足判据。
- 基类 `MQClient`（不挂 hub）一律覆盖时刻 = 现在。

调用点：

| 调用点 | known_at | 不许跳过的情形 |
|---|---|---|
| `_apply_notifications` 新行进入（`fresh`） | `_rerange` 读这些行的发出时刻（取最早） | — |
| `subscribe_get` | 初始读的发出时刻 | 读期间被 tick 碰过（`pushed` 保持 `UNKNOWN`）：这次补读要无条件推一次，是必须的 |
| `subscribe_range` 行频道 | 初始 range 读的发出时刻 | — |
| `subscribe_range` 索引频道 | 同上 | RLS 订阅：初始结果按 RLS 过滤过，靠这次重跑比对把范围内不可见的行订上（尾随重读设计稿 §3.2） |
| 整表订阅初始化期间攒下的 `pending` | 不给（同今天） | — |

聊天的效果：新行进入后的补读，同一 worker 所有连接的覆盖时刻都是该行频道的 `eff`（第一个连接订上它
的 ack 时刻），只读一次。模拟（每连接 tick 250µs），每条消息的补读次数：

| 每 worker 连接数 | 50 | 200 | 1000 |
|---|---|---|---|
| 覆盖时刻 = 各连接登记时刻，Windows 默认计时器 | 2 | 11 | 18 |
| 同上，1ms 计时器 | 22 | 118 | 118 |
| 覆盖时刻 = hub 生效时刻 | 1 | 1 | 1 |

新连接订阅时，数据读之后 hub 早就订着、期间又没有通知的频道，补读直接省掉。例如聊天窗口里的 1024 个
行频道早被别的连接订上，消息写入后不改，新连接的这 1024 行不再补读。

## 5. 正确性

今天的保证：连接弹出的每一项，都有一次在覆盖时刻 + T 之后发出的读（它自己弹出时现读）。本设计里这次
读可能是别的连接发的，但满足同一个不等式；补读的覆盖时刻由 §4.5 给出，醒来时刻不早于覆盖时刻。对照
尾随重读设计稿 §4 的表：

| 场景 | 今天 | 本设计 |
|---|---|---|
| 单条通知 | 通知 + T 后现读 | 覆盖时刻 = hub 时刻，用 r ≥ 它 + T 的读 |
| 合并，最晚一条 ≤ cutoff | 弹出时现读 | 覆盖时刻取最晚一条 |
| 合并，最晚一条 > cutoff | 尾随重读 | 尾随项覆盖时刻 = 最晚一条的 hub 时刻 |
| 订阅生效前的写 | 生效 + T 后补读 | 覆盖时刻 ≥ eff；或数据读已盖住而跳过 |
| 新行进入后空档里的写 | 登记 + T 后补读 | 同上 |
| pubsub 断线期间的写 | 重订后 RESYNC，各自补读 | RESYNC 走 `_dispatch`，覆盖时刻为 hub 时刻；断线期间 eff 未知，补读不跳过 |
| SQLite | 同 Redis 逻辑 | 同；无副本，判据平凡成立 |

其他：

- 不倒退：满足预算时，后一次读不旧于前一次读对应的通知（同今天的表述）；§4.4 保证同一订阅用到的读
  只进不退。
- 共享读可能比"自己现读"早一点发出，推给客户端的数据可能比今天旧一点，但一定覆盖到它要覆盖的通知；
  之后的通知照常触发新的读。热频道的中间推送见 §4.2 的取舍。
- 超预算（副本延迟 > T）：今天各连接各读各的，有的可能读到新值；共享后同一 worker 的连接一起拿到同一
  份旧值。保证措辞不变（"可能残留旧数据，直到该行下次变更"），只是残留在同一 worker 内一致。
- RLS：共享的是原始行与 id 列表，RLS 仍按各订阅自己的 ctx 判定。
- 多 servant：共享读落在随机一个副本上，与今天一样依赖 T 预算。

## 6. 成本

- master：0 新增读写；PUBLISH、Lua、复制流不变。
- 副本读：同一 worker 内同一 key 按通知合并，扇出 K 的读从 K 次降到约 1 次；补读能跳过的不读。
- worker CPU：每连接省掉读的往返与解码；新增每条通知一次 `time.monotonic()` 和一次 dict 写，每次取数
  几次 dict 查找。
- 内存：条目最多保留 `HORIZON`，按活跃 key 数计；每个 hub 多两张按频道的时刻表。
- 延迟：醒来时刻按 hub 时刻，最多早分发循环那一点；搭车等在途读的连接，往往比自己去读更快拿到结果。

## 7. 测试计划（先写 red）

MQ（`tests/test_backend_mq_client.py`、`tests/test_backend_pubsub_hub.py`，假时钟）：

- `_dispatch` 给所有连接同一个时刻；`_last_notified` 记最后一条，频道撤掉后清。
- 覆盖时刻：单条 = hub 时刻；合并且 ≤ cutoff 取最晚；> cutoff 本次取队头、尾随项取最晚；`DROP_AFTER`
  连 `_cover` 一起清；`get_message()` 形状不变。
- `request_reread`：hub 未生效 → 覆盖时刻 = 现在；已生效 → `max(h, known_at - T)`；`known_at` 足够新
  且可跳过 → 不入队；`may_skip=False` 不跳过。
- Redis：ack 记 `eff`；退订、节点失效清掉，重订后是新时刻。SQLite：水位时刻。

共享读（新 `tests/test_shared_reads.py`，假读方法 + 控制时间）：

- `r >= c + T` 复用，`r < c + T` 新发；在途复用；多行部分命中只读缺的、一次批量；
- 发起方取消不影响等待方；读失败时发起方抛、等待方各自重读；
- 超过 `HORIZON` 清掉；换 loop 清空；结果只读（改了不影响别人）。

订阅（按后端参数化，`tests/test_backend_sub.py`、`tests/test_backend_sub_race.py`）：

- 同一 backend 上 N 个 broker 订同一个 range：一次插入 → range 读 1 次、行读 1 次、新行补读 1 次，
  N 个都推到；
- 判据：覆盖时刻 + T 之前发出的读不被复用（滞后副本模拟：一个连接读到旧行之后的通知必须触发新读）；
- 只进不退：积压的旧通知不会让订阅用上比它已用过的更早的读；
- 尾随重读跨连接合并；
- 补读跳过：hub 早就订着且期间无通知 → 不补读；期间有通知 → 补读且合并；`subscribe_get` 的 `UNKNOWN`
  情形与 RLS 区间订阅的索引频道不跳过（`test_query_subscribe_rls_gain_without_index` 保持绿）；
- 整表订阅两个连接共享同一批行：各自推送都不带 `_version`、内容正确（就地删字段的回归）。
- 现有 `test_backend_sub*`、race、recovery、`test_arch_master_reads`、`test_arch_publish` 全绿。
  断言具体读调用次数的用例（如 `test_subscription_rejects_foreign_channel`）按需给 broker 独立的
  `SharedReads` 或关掉共享。

## 8. 提交计划

每步先提交 red，再提交实现，每个实现提交之后全部测试通过。

1. `docs(spec)`：本设计稿。
2. `test(mq)` → `feat(mq)`：hub 统一时间戳、`_last_notified`、覆盖时刻、`get_batch`（§4.1、§4.2）。
3. `test(sub)` → `feat(sub)`：`SharedReads`、读路径改造、结果只读、读只进不退（§4.3、§4.4）。
4. `test(sub)` → `feat(sub)`：订阅生效时刻、补读的覆盖时刻与跳过（§4.5）。
5. `bench`：聊天扇出压测进 `benchmark/`（`sub_fanout_chat.py`，可开关共享对比）并补 §10；
   `benchmark/sub_budget_result.md` 的成本模型加"同查询扇出"一节。

## 9. 备选方案与否决理由（留给后来人，别再重复提）

- **进程级行缓存 + 写穿**（PR #144）：见 §1.3；本设计不碰事务读。
- **按固定 TTL 合并同参数的读**（§1.2 粗估的做法）：不看通知时刻，TTL 内、通知之前发出的读会被当成
  新的，违反不变量。
- **各连接保留自己的时刻，靠通知序号认"同一条消息"**：效果等于 hub 统一时间戳，还多一套序号。
- **worker 统一 tick（所有连接同一时刻弹出）**：要改"每连接一个推送循环"的结构；统一时间戳后各连接
  本来就同一时刻到期。
- **同查询的订阅在 worker 内合成一个、读完算好差再扇出**：能再省每连接的集合差和 MQ 唤醒，但要重做
  sub_id 归属、回复与推送的顺序、RLS、中途加入的连接。先做读合并，看剩下的成本再议。
- **范围结果的差按对象身份复用**：limit 1024 合并后剩下的大头是每连接对 1024 个 id 建集合、求差；要先
  去掉 `last_range_result` 的就地修改，留作后续。
- **初始读走共享**：`subscribe_range` 先读后订，初始读时 hub 可能还没订这些频道，判据要另想；初始读还
  牵涉回复顺序（尾随重读设计稿 §9）。留作后续；§4.5 已经省掉了新订阅的大部分补读。
- **补读推迟到最多 1s、顺带在下个 tick 读**：只减少 tick 数，读照样每连接一次；§4.5 直接让它合并或
  省掉。

## 10. 实测（实施后补）

## 11. 已定事项（2026-09-28）

1. `HORIZON` 取 10T（1s）：越长，积压连接的读越可能命中，内存越多。
2. 不加配置开关，只保留 `SubscriptionBroker` 的构造参数给测试和压测用。
3. §4.5（补读）与前面放同一个 PR。
4. 聊天示例和教程的窗口 1024 改小，另开提交，不在本分支。
