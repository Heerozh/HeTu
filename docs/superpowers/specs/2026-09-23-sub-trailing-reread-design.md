# 订阅推送的尾随重读（trailing re-read）— 设计稿

- 日期：2026-09-23
- 状态：已实施（全后端全量测试通过）
- 分支：`fix/sub-trailing-reread`（基于 dev `136f0c0`）
- 取代：`perf/row-cache` 分支的"行频道改由 commit 主动 PUBLISH 并携带 `_version`"（8c47e074）。
  row-cache 分支已决定废弃，行缓存日后基于本 spec 重写（约束见 §7）。
- 影响范围：`hetu/data/backend/base.py`（`MQClient` 本地队列）、`hetu/data/sub.py`（三种订阅、
  `get_updates`）、`hetu/data/backend/redis/pubsub.py` + `redis/mq.py`（重订生效回调）、文档与
  docstring、C# SDK 订阅方法的 XML 注释。
  **不改 Lua、不改客户端协议、不改 keyspace 配置（仍是 `Kghz`）、不给 master 增加任何读写。**

## 1. 背景

### 1.1 问题：通知后的数据残余

行 / 索引频道用的是 keyspace 通知，消息里只有事件名，不带任何内容。订阅者收到通知后去读这行，
读的是"某个副本"——推荐的生产拓扑是 Redis Proxy 做分片 + 读写分离（`operations.md`），客户端
指定不了读落在哪个节点。发通知的节点 X 与读的节点 Y 不是同一个时，Y 可能还没应用这次写入，
推给客户端的就是旧值，而且**一直旧到这行下次变更**：这次写入的通知已经用掉了，不会再来。

### 1.2 为什么不用带版本号的 PUBLISH

row-cache 分支的做法是 commit 在 Lua 里对每个改动行 `PUBLISH 行频道 新_version`，订阅者据此判断
副本读是否滞后。实测（Redis 8.10、不带模块、固定单核，perf 数用户态指令）它的代价落在
最不该落的地方：

| 每次操作（master 上） | 无副本 | 1 个副本 |
|---|---:|---:|
| `redis.call("PUBLISH")` | 2,903 | 4,382 |
| keyspace 通知（实例上有订阅者） | 2,221 | 2,221（不复制） |
| keyspace 通知（实例上没有任何订阅者） | 12 | 12 |

端到端（8 worker、1 副本）：master 指令 +10～13%、master 周期 +6～9%、副本周期 +13～16%、
复制流字节 +35%。原因有二：没人订阅的行也照发（压测里一个订阅都没有，全是白付）；PUBLISH 进
复制流，每个副本都要再执行一遍——读写分离本意是卸 master、靠加副本扩展订阅，它却让开销随
副本数增长。keyspace 通知则由各副本在应用写入时各自本地产生，master 上没有订阅者时几乎免费。

### 1.3 不带版本号能做到什么程度

代理把节点藏起来之后，订阅者手里只有三样东西：不带内容的通知、副本读回来的行（带 `_version`，
但不知道来自哪个副本）、master 上的脚本读。要判断一次副本读"够不够新"必须知道目标版本，
而目标版本只能来自通知 payload（即 §1.2）、master、或本进程自己的 commit。不带 payload、又
不增加 master 读，就只剩**时间**：假设副本相对发通知节点的复制延迟有上界 T，只要每次通知之后
至少隔 T 再读一次，读到的就一定不旧于那次通知。

这是一个**概率性**的保证：正常负载下复制延迟通常远低于 1ms，T 取 100ms 留了两个数量级的余量；
但 Redis 压力过大（副本 CPU 打满、全量同步、网络抖动）时延迟可以超过 T，旧值仍可能残留到该行
下次变更。本 spec 接受这一点，并在文档、docstring、C# 注释里明确写出来（§3.6）。

## 2. 现状：残余从哪来

单条通知其实已经有余量：`MQClient.get_message`（`base.py:928`）只弹出**首次**到达已超过
`1/UPDATE_FREQUENCY`（默认 100ms）的频道，通知到读之间本来就隔了约 100ms。真正会停在旧值上的
是下面几个窗口：

1. **合并窗口**。频道已在队列里时，`push_pulled_`（`base.py:882`）只保留最早那条的时间戳
   （index 更新的 remove/add 两条靠它合并）。批内后来的写可能紧贴着这次读：比如首条通知
   t=0，第二条 t=99ms，t=100ms 弹出就读，第二次写只有 1ms 的复制余量，而它的通知已经被合并
   掉了。每 50～100ms 写一次的热行（位置、血量）几乎每批都会碰上。
2. **订阅窗口**。
   - `subscribe_get`（`sub.py:356`）已是先订后读，但 SUBSCRIBE 生效前 X 上已应用、Y 上还没应用
     的写，读不到，也不会再有通知。
   - `subscribe_range`（`sub.py:443`）、`subscribe_table`（`sub.py:568`）是先读后订：读与
     SUBSCRIBE 生效之间的写入既不在读回的行里，也没有通知——这个窗口连单机都有。
3. **重连窗口**。pubsub 节点失效到 `resubscribe_all`（`pubsub.py:375`）全部重订生效之间的
   写入没有通知，订阅在重连后就一直停在旧值。

另外，不同副本之间来回读还可能让客户端短暂倒退（先读到新版本、下一批又从更慢的副本读到旧版本），
在满足延迟预算时不会发生（§4），这里不单独处理。

## 3. 设计

核心不变量：

```
对每个订阅频道：它的最后一条通知之后，以及订阅生效、pubsub 重订生效这两个时刻之后，
都一定有一次读，其发出时刻比该通知 / 该时刻晚至少 T（T = 1/UPDATE_FREQUENCY，默认 100ms）。
```

只要副本相对发通知节点的复制延迟 < T，客户端最终停留的一定是最新值；中间可能短暂看到旧值，
但不会停在旧值上。

### 3.1 `MQClient`：合并进来的通知补一次尾随重读

- `push_pulled_`：频道已在队列里（被合并）时，记下 `_late[ch] = now`；带 payload 的（表级频道），
  这些迟到消息的 row_id 另记进 `_late_payload[ch]`。
- `get_message` 弹出频道时，若 `_late[ch] > cutoff`（`cutoff = now - interval`，即最后一条合并进来
  的通知离这次读不足 interval），就以当前时刻把它重新入队，payload 为 `_late_payload[ch]`——
  interval 后再读一次。
- 重新入队用弹出时刻而不是迟到那条的时刻：队列保持按时间有序（`get_message` 依赖队头最老），
  尾随那次读最多比必要的晚一个 interval。
- 持续写入的热行：尾随那条恰好顶替了下一批的队头，读的次数不变；只有一波写停下来时多读 1 次。
  单条通知（没有合并）不产生尾随重读。
- `DROP_AFTER` 丢弃旧通知时一并清掉对应的 `_late` / `_late_payload`。
- 全部是同步 O(1) 操作，不引入新的 await。

### 3.2 订阅生效后的补读

`MQClient` 加一个公开方法 `request_reread(*channels, payload=None)`：把这些频道当作"刚收到一条
通知"放进本连接的本地队列（不走 `watch` 回调），interval 后照常弹出、重读。

- `subscribe_get`：读完初始行、确认可见后，`request_reread(row_channel)`。
- `subscribe_range`：订阅生效并登记后，`request_reread(index_channel, *row_channels)`——重跑一次
  range 比对（补上读与订之间进出范围的行），并重读各行（补上内容变化）。
  副作用（有益）：初始行按 RLS 过滤过，重跑比对时范围内不可见的行会被当作"新进入"订上行频道
  （只订不推），之后它不经索引变化重新获得 RLS 也能从行频道推出来。以前要等该索引下一次变动
  才订上它们，`test_query_subscribe_rls_gain_without_index` 因此从已知缺陷（xfail）转正。
- `subscribe_table`：整表重读太贵，不做补读，改成**先订阅、隔 T 再全量读**：
  1. 先查重复订阅（sub_id 固定为 `{comp}.table`），再 SUBSCRIBE、立即登记订阅，订阅进入
     "初始化中"：这期间 `get_updated` 不读库、不推送，只把通知带来的 row_id 攒进 `pending`
     （初始读可能很久，期间弹出的通知不能丢，也不能推给还没拿到 sub_id 的客户端）；
  2. `await asyncio.sleep(T)` 后全量读（与今天相同的 `range(id, -inf, inf)`），超限则撤销登记与
     订阅、返回 None；
  3. 结束初始化，`known_ids` 取初始读结果；`pending` 非空就 `request_reread(table_channel,
     payload=pending)`，这些行 interval 后按增量规则重读推送。
  代价是整表订阅的返回多等约 100ms，期间该连接的后续消息排队（`sub_call` 在接收循环里内联
  执行）。整表订阅本就是"很少变的表、每连接很少几个"，可以接受。（审查后改成后台等待，
  后续消息不再排队，见 §9。）

### 3.3 pubsub 重订生效后的补读

- `AsyncKeyspacePubSub.__init__` 加可选回调 `on_resubscribed: Callable[[list[str]], None]`；
  `resubscribe_all` 在确认 `targets` 全部重订生效、清空之前，同步调用它并传入这批频道。
  回调抛异常只记日志，不影响恢复流程。
- `PubSubHub` 实现该回调：对仍在 `_subs` 里的频道 `_dispatch(channel, [MQClient.RESYNC])`。
  `RESYNC = "*"` 是表级频道 payload 里的特殊 row_id，意思是"这段时间的变更不可知，整表重同步"。
  - 行订阅、索引订阅本来就忽略 payload：重读行 / 重跑 range 比对。
  - 整表订阅看到 `RESYNC`：重新全量读（上限 `max_table_rows`），推送所有可见行；`known_ids`
    里读不到或已不可见的行推 None。
  - 服务端内部 `watch` 的频道（如 `Connection` owner 值频道）回调照常触发一次：重连期间丢的
    顶号通知由此补查一次，是期望的行为。
- 走 `_dispatch` 即各连接各自入队，interval 后弹出，满足不变量。
- SQL 后端（`SQLNotifyHub`）按水位轮询通知表，失败退避重试、不丢通知，不需要此回调。

### 3.4 按行内容指纹去重

补读与尾随重读多数时候读回来的是客户端已有的数据，不去重的话推送流量会翻倍。

- 去重键是**行内容指纹**：`hash(repr(原始行))`，原始行含 `_version`；行不存在为 None。
  不能只比 `_version`：同 id 删除后重插时版本从 1 重来，而刚插入、没改过的行都是 1，
  "版本相同"并不代表内容相同。字段顺序由 dtype 固定，子数组字段 `item()` 出来是 list、
  不能直接 hash，所以取 `repr`。每个订阅只多存一个整数。
- `RowSubscription` 记 `pushed`：客户端当前持有内容的指纹（订阅登记后、初始行读回前为
  "未知"，任何读都推）。`get_updated` 读回的指纹与它相等就不推，否则推并更新。
  - 初始值：`subscribe_get` 读完初始行后写入——初始读之前登记的订阅，期间 tick 推的会被
    还没拿到 sub_id 的客户端丢弃，所以以初始读回的内容为准；`subscribe_range` 的初始行、
    `IndexSubscription` 新进入范围的行都带着读回内容的指纹创建。
  - 相等才去重，内容不同（包括读到更旧的副本）照推。满足延迟预算时读不会倒退（§4）。
- 整表订阅：`known_ids` 已是每连接一份，不再按行常驻指纹；只记**上一批**读过的行的指纹，
  下一批（尾随重读就是紧接着的那一批）读回相同的不推。内存随批大小，不随表大小。

### 3.5 `get_updates` 等到真有更新才返回

去重之后，一批通知可能一条更新都没有。`get_updates` 此时继续等下一批，而不是返回空 dict；
传了 `timeout` 的按总时长计。推送循环（`receiver.subscription_handler`）不会被空批唤醒，
"一次写入 → 一次 `get_updates` 拿到它"的调用语义也保持不变。

### 3.6 保证的措辞

订阅推送是**最终一致、尽力而为**的，统一说法（中文，英文版同义）：

> 订阅推送是最终一致的：正常负载下约 99% 的变更会在约 `1/UPDATE_FREQUENCY`（默认 100ms）内
> 推到最新值；Redis 压力过大（副本复制延迟超过约 100ms）时，客户端可能残留旧数据，直到该行
> 下次变更。需要强一致的判断请放在 System 里读（写事务有乐观锁兜底）。

写进：
- `SubscriptionBroker` 类 docstring 与 `subscribe_get` / `subscribe_range` / `subscribe_table`
  的 docstring（中英双语）；
- `docs/zh/concepts.md`、`docs/en/concepts.md` 的"订阅"一节（顺带更正"延迟主要由 Redis 往返
  决定"——推送按 1/UPDATE_FREQUENCY 合批）；`docs/zh/operations.md`、`docs/en/operations.md`
  的副本一节加"复制延迟预算"；`hetu/llms.txt` 一句；
- C# SDK：`HeTuClientBase.WatchRowSync` / `WatchRangeSync` / `WatchTableSync`、
  `HeTuClient.WatchRow` / `WatchRange` / `WatchTable`、`HeTuSessionClient` 的 Watch 系列、
  `BaseSubscription` 类注释（XML 注释中英混写，与现有风格一致）。

## 4. 正确性

设 W 为某行的最后一次写，X 为发出其通知的节点，Y 为某次读落到的副本，δ 为 Y 相对 X 的复制
延迟，假设 δ < T。

| 场景 | 保证它的读 | 该读为何不旧于 W |
|---|---|---|
| W 的通知单独到达（未合并） | 首次到达 + T 之后弹出的读 | 读发出时 Y 已应用 W（δ < T） |
| W 的通知被合并进更早的队头 | 弹出时发现迟到 → 尾随重读，晚于弹出 T | 尾随读晚于 W 的通知至少 T |
| W 在 X 上应用于 SUBSCRIBE 生效之前（行 / 索引订阅） | `request_reread` 的补读，晚于生效 T | W 在 master 上更早应用，Y 在生效 + T 前已应用 |
| 同上（整表订阅） | 生效后 sleep T 才发出的初始全量读 | 同上 |
| W 在 X 上应用于 SUBSCRIBE 生效之后、初始读完成之前 | W 自己的通知（按上两行处理；整表订阅攒进 `pending` 后重读） | 同第 1、2 行 |
| W 发生在 pubsub 断线期间 | 重订生效后 `RESYNC` 补读 | W 在重订生效前已在 master 上应用 |
| 连续多批读 | 每批读都晚于其对应通知 T | 满足预算时后一次读不旧于前一次读对应的通知，不会倒退 |

去重的正确性：只在"读回内容 == 客户端持有内容"时不推（指纹相等；64 位 hash 的碰撞可忽略）。
满足预算时，最后那次读回的就是 W 之后的状态；若它与客户端持有的内容相同，客户端已经是最新。

已知残留（不处理，记录在此）：
- 复制延迟超过 T：旧值可能残留到该行下次变更——这正是 §3.6 写明的那 ~1%。
- `MQClient` 本地队列积压超过 `DROP_AFTER`（120s）被丢弃的通知：连接已严重跟不上，与本 spec 无关。

## 5. 成本

- master：0 新增读写；复制流：0 新增；Lua、keyspace 配置、客户端协议：不变。
- 副本多出的读：
  - 合并窗口：每波写结束时每连接最多 1 次尾随重读；持续写入时不增加；
  - 订阅补读：每个行 / 索引订阅 1 次（按表 `get_many` 批量）；整表订阅 0 次（改为延后初始读）；
  - 重连：每连接对全部订阅频道各 1 次；整表订阅为一次全量重读（只在节点失效恢复时发生）。
- 推送流量：补读、尾随重读读回内容相同的不推，不增加客户端流量（整表订阅的 `RESYNC` 全量
  重同步除外，只在节点失效恢复时发生）。每次重读多一次 `repr` + `hash`（微秒级）。
- 延迟：常规推送不变；整表订阅的初始返回多约 100ms。

## 6. 测试计划（先写 red）

纯单元（假 pubsub / 不连后端，`tests/test_backend_mq_client.py`、`tests/test_backend_pubsub_recovery.py`）：
- 合并进队头的通知：首次弹出后，interval 后同一频道再弹出一次（尾随重读）。
- 单条通知：弹出后不再出现。
- 迟到通知离弹出已超过 interval：不补排。
- 表级频道的尾随重读只带迟到消息的 row_id。
- `request_reread`：interval 后弹出，不触发 `watch` 回调。
- `DROP_AFTER` 清理连同 `_late` 一起清。
- pubsub 重订全部生效后回调 `on_resubscribed`，hub 给仍有人订的频道各分发一条 `RESYNC`。

订阅（按后端参数化，`tests/test_backend_sub.py`；副本滞后用 patch 读方法返回旧行模拟）：
- 两次写被合并，弹出时的读返回旧行 → 之后的 `get_updates` 推出最新值（现状：永远拿不到）。
- `subscribe_get` 初始读返回旧行 → 补读推出最新值；初始读已是最新 → 补读不推任何东西。
- `subscribe_range` 初始读漏了读与订之间插入的行 / 返回旧内容 → 补读推出。
- `subscribe_table`：初始全量读晚于 SUBSCRIBE 生效至少 T；初始化期间到达的通知在返回后被重读推送。
- 整表订阅收到 `RESYNC`：推所有可见行，已知但读不到的行推 None。
- 去重：内容没变的补读 / 尾随重读不推（行、范围、整表订阅）；同 id 删除后重插、版本从 1 重来
  的行照推；`get_updates(timeout)` 在只有空批时等满超时返回 `{}`。
- 现有用例回归：`test_subscribe_mq_merge_message` 的语义随 §3.1 更新（合并后多一次尾随重读，
  内容未变不推）。

## 7. 对日后重写行缓存的约束

- 版本下限换成时间下限：一行的缓存只能由"在该行最后一条通知之后至少 T 才发出的读"填充；
  通知到达即逐出。满足延迟预算时缓存不会存进旧值；超预算时的陈旧与订阅推送同一量级，文档同一
  措辞。
- keyspace 通知分不出是不是本进程自己的写：commit 写穿进缓存的行会被自己那条通知逐出，写穿只能
  省掉"提交后到通知到达之间"的读。要长期保住写穿，需要另外的版本来源（例如按事件计数，每次
  commit 对每行恰好一条 HSET / DEL），另立 spec。

## 8. 提交计划

每步先提交 red 测试，再提交实现，保证每个实现提交之后全部测试通过。

1. `docs(spec)`：本设计稿。
2. `test(mq)` red → `fix(mq)`：合并通知的尾随重读、`request_reread`（§3.1、§3.2 的 MQ 部分），
   连同 `get_updates` 跳过空批（§3.5）——尾随重读会把一次写入的推送拆成几批、并产生读下来
   没有变化的批次，现有用例改用 `settled_updates`（收集到安静为止）断言，这一步必须一起做
   才能保持全绿。
3. `test(sub)` red → `fix(sub)`：行内容指纹去重（§3.4）。
4. `test(sub)` red → `fix(sub)`：三种订阅生效后的补读 / 整表订阅先订阅后延迟读（§3.2）。
5. `test(redis)` red → `fix(redis)`：重订生效回调、hub 分发 `RESYNC`、整表订阅全量重同步（§3.3）。
6. `docs`：保证措辞写进文档、docstring、`llms.txt`、C# XML 注释（§3.6）。

## 9. 审查后的修正（2026-09-24）

合入后做了一轮审查（分支 `fix/sub-review-148`），改了这些：

- **`subscribe_get` 的回复被在途推送盖掉**：先登记后读，读的期间 tick 可能已拿更早预读的旧行
  替新订阅算好推送，却在 sub 回复之后才送达；`pushed` 又被初始读覆盖，之后的补读被去重挡掉，
  客户端一直停在旧行。改为：读完时若 `pushed` 已不是 `UNKNOWN`（读期间被 tick 碰过），保持
  `UNKNOWN`，让 §3.2 的补读无条件推一次。
- **`RESYNC` 只发给表级频道**（修 §3.3）：行 / 索引（含值）频道按约定 payload 为 None，
  收到就重读，不需要标记。
- **合并进来的 `RESYNC` 只做一次整表重同步**：表级频道已在队列里时，`RESYNC` 合并进去，
  弹出时整表重读一次、尾随重读又一次。改为离弹出不足 interval 的 `RESYNC` 只留给尾随重读。
- **`request_reread` 清掉积压通知时也打警告**，与收到通知那条路径一致。
- **整表订阅的等待移出接收协程**（修 §3.2 的代价）：`subscribe_table` 拆成
  `begin_subscribe_table`（检查、订阅、登记，在接收协程里做完）和后半段（等 T、全量读），
  后半段交给后台任务。回复没有请求 id、SDK 按顺序对应，所以先在 `push_queue` 里放一个
  Future 占位，发送循环按顺序等它填好再发。建一个 task 约 3µs、挂起时约 1.6KB。收益只在
  客户端同时有多个请求在飞时：几张整表订阅一起等、后面的 RPC 立即执行（回复仍排在订阅回复
  之后），所以文档建议客户端把几个 `WatchTable` 先都发出去再 await。

判定不改：

- `RESYNC` 触发每个已登录连接的 `watch` 回调、各从 master 读一次 Connection 行；以及
  `RESYNC` 让所有频道一起重读、整表订阅全量重推——都只在 pubsub 断线重订后发生，是一次性
  尖峰，量级不超过全服重连后重新订阅一次。
- "没有副本时补读、尾随重读、等 T 都落在 master 上"：推荐部署走 Redis 代理层，`servants`
  留空，河图看不到副本拓扑；生产环境又要求至少一个副本，前提不成立。按"有没有副本"开关这些
  机制，反而会在代理部署里关掉复制延迟保护。
- 指纹用 `hash(repr(row))`：组件不允许子数组字段，行里都是标量，repr 精确，每行不到 1µs。
