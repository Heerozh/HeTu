# 通知按声明发送（`table_sub` / `point_sub`）— 设计稿

- 日期：2026-09-24
- 状态：已实施（实测见 §10）
- 分支：`perf/publish-opt-in`（基于 dev `95f690a`）
- 依据：`benchmark/redis_publish_cost_result.md`（PUBLISH / payload / keyspace 通知的成本与使用规则）
- 影响范围：`hetu/data/component.py`（两个声明）、`hetu/data/backend/redis/client.py` +
  `commit_v2.lua`（发什么、payload 形状）、`hetu/data/backend/sql/client.py`（通知行同语义）、
  `hetu/data/sub.py`（整表订阅、点查询选频道、值频道订阅发现"离开"）、`hetu/data/backend/redis/mq.py`
  （空消息不解包）、`hetu/endpoint/connection.py`（`Connection.owner` 声明）、文档 / docstring /
  C# 注释、benchmark 组件与回放变体、新增守门测试 `tests/test_arch_publish.py`。
- **不改**：keyspace 配置（仍是 `Kghz`）、行频道与整索引频道、区间订阅的语义、客户端协议。
- 目前没有现网部署：两个声明直接进组件 schema，不做旧 meta 兼容以外的迁移处理。

## 1. 背景

### 1.1 commit 里的 PUBLISH 挂着两个功能

`commit_v2.lua` 的 Phase 3 对 payload 里的每一条通知执行一次 `redis.call("PUBLISH", ...)`，
通知有两类：

- **表频道**（PR #136，整表订阅）：每个事务对每张被改动的表发一条，消息是 msgpack 的 row_id 列表。
- **索引值频道**（5975362，点查询只被"自己那个值"叫醒；2177814 的顶号检测也用它）：
  insert / delete 给每个非 id 索引字段各发一条，update 给每个变更了的索引字段的旧值、新值各发一条。

而且**不管有没有人订阅，全部照发**。以 Item 为例（3 个非 id 索引）：插入一行发 4 条（3 个值频道 +
1 个表频道）、转手发 3 条（表 + 旧值 + 新值）、删除发 4 条、只改普通字段也发 1 条（表频道）。

### 1.2 成本

报告（Linux、perf 数用户态指令）的结论：

- 每条 PUBLISH 在 master 上约 0.9~1.3 万条指令：`redis.call` 调度约 3k（和 `EXISTS` 一样）、强制写进
  复制流约 1.5k（副本越多越贵）、把频道名和消息经 msgpack payload 带进 Lua 约 6k。单行 commit 本身
  只有 6.7~8.5 万条，所以每多一条 PUBLISH 就是 +14%~19%。
- PUBLISH 进复制流，每个副本还要再执行一遍，挤占副本的读能力。
- SPUBLISH 在非 cluster 模式下同样被强制复制，成本一样，不是解药。

本设计补充的拆解（Docker/WSL2，master 挂 1 个副本，主线程 µs，两轮一致，只看相对值）：

| 每条通知 | payload 搬运 | PUBLISH 调用 | 合计 |
|---|---:|---:|---:|
| 现状 `[频道, msgpack(ids)]` | 0.48 | 0.29~0.37 | 0.77~0.85 |
| 扁平：只传频道名，消息为 `""` | 0.24 | 0.27~0.31 | 0.52~0.55 |

- 只发一条时（基础 commit 约 4.2µs）：现状格式多 1.0µs，扁平格式多 0.39~0.46µs。
- 搬运比调用本身还贵，和报告推算的 6k : 4.3k 一致。
- Lua 里纯字符串拼接约 0.09µs/次，是报告里"拼一个 key 约 4.5k 指令"（`prefix .. 数字`，含数字转
  字符串）的一半；但在 Lua 里从已有的 push 拼出频道名，还要截取、还要额外告诉 Lua 哪些索引是声明的，
  不比直接传频道名省。

## 2. 目标与非目标

目标：

1. **没人会订的通知不发**：表频道只给声明了 `table_sub` 的组件发；值频道只给声明了 `point_sub` 的
   索引发。
2. **值频道只发"进入"**：insert、或字段改成该值时发；"离开"（delete、字段从该值改走）不发，由订阅者
   本来就订着的行频道发现。
3. **值频道的消息内容没人用**：payload 扁平化，只传频道名，消息为空串。
4. **守门**：加测试限制 PUBLISH 的使用——新增调用点、往消息里塞内容、没声明也发，都会让测试失败。

非目标：

- 区间订阅不变：仍订整个索引的 keyspace 频道，该索引任何写入都会叫醒它（设计如此）。
- 不改 keyspace 配置、行频道、整索引频道、客户端协议。
- 不彻底去掉 PUBLISH（备选方案与否决理由见 §9）。

## 3. 设计

### 3.1 声明

- `@define_component(..., table_sub=False)`：该组件允许整表订阅。只有这类组件的 commit 发表频道。
  预期是个位数的冷表（行多、行小、很少变，如所有玩家的名字）。
- `property_field(..., point_sub=False)`：该索引支持高效点订阅——`range(Comp, field=v)` 的订阅
  只在"有行进入 / 离开 v"时被叫醒。
  - `point_sub=True` 强制打开 `index`（同 `unique` 的处理：显式 `index=False` 时打警告并修正）。
  - `id` 是内置主键，不能声明（用户也定义不了 `id`）。
- 两个声明都写进组件 schema：`make_json` 输出 `table_sub` 与每个属性的 `point_sub`，`load_json`
  还原为 `BaseComponent.table_sub_: bool`、`point_subs_: frozenset[str]`，`Property` 增加
  `point_sub` 字段。于是 `duplicate()`、headless 按组件名从 meta 还原的组件都自动带上声明——
  headless 写入走同一条 `Session.commit()`，声明对不上会漏发通知。
  - 改声明就是 schema 变更（`json_` 的 md5 变了），要跑一次 `hetu upgrade`。
  - `load_json` 读不到这两个键时按 `False` 处理，只为了迁移路径读旧 meta。
  - headless 传本地类时，这两个声明与服务器 meta 不一致也报 `SchemaMismatch`：声明少了，
    headless 的写入就会少发通知，服务器上的订阅会漏更新。
- core 组件 `Connection.owner` 声明 `point_sub=True`（顶号检测用，见 §3.5）。

### 3.2 commit 发什么（Redis）

| 操作 | 表频道（只给 `table_sub` 组件） | 值频道（只给 `point_sub` 索引） |
|---|---|---|
| insert | 每个事务每张表 1 条 | 该行每个 `point_sub` 字段的值各 1 条 |
| update | 同上 | 改了的 `point_sub` 字段的**新值**各 1 条 |
| delete | 同上 | 不发 |

- 同一事务内同一 `(索引, 值)` 合并为 1 条（现状已是）。
- 表频道消息：msgpack 的 row_id 列表，整表订阅要用，不变。
- 值频道消息：空串。

payload 形状：

```
[checks, pushes, deleted, table_pubs, value_chans]
table_pubs  = [[表频道, msgpack(row_ids)], ...]
value_chans = [值频道, ...]
```

Lua Phase 3 只保留两个固定的调用点：

```lua
for _, pub in ipairs(table_pubs) do redis_call("PUBLISH", pub[1], pub[2]) end
for _, ch in ipairs(value_chans) do redis_call("PUBLISH", ch, "") end
```

Lua 与 Python 两处都写明：PUBLISH 很贵（见报告），不要加调用点、不要往消息里塞内容，
`tests/test_arch_publish.py` 守门。

Item（3 个非 id 索引，只给 `owner` 声明 `point_sub`，不声明 `table_sub`）每次 commit 的 PUBLISH 条数：

| 操作 | 现在 | 本设计 |
|---|---:|---:|
| 插入 | 4 | 1 |
| 转手（改 owner） | 3 | 1 |
| 删除 | 4 | 0 |
| 改普通字段 | 1 | 0 |

### 3.3 SQL 后端

通知行按同样的语义收窄：表频道行只给 `table_sub` 组件插；值频道行只给 `point_sub` 索引、只插新值，
payload 为 NULL。行频道行与整索引频道行不变——它们是 keyspace 通知的等价物，点查询发现"离开"、
区间订阅都靠它们。

### 3.4 订阅侧

1. **`subscribe_table`**：组件没有声明 `table_sub` 时打警告、返回 `(None, [])`，不订阅（和无权限时
   的处理一样）。
2. **`subscribe_range` 的点查询**（`point_query_value_` 不为 None）：
   - 索引声明了 `point_sub`：订值频道，同现状。
   - 没声明（且不是 `id`）：退化为订整个索引的 keyspace 频道，即区间订阅的行为——该索引任何写入都会
     叫醒它重跑比对，结果仍然正确。每个组件类的每个索引只警告一次，提示加 `point_sub=True`。
   - `id` 的点查询维持现状（订整个 id 索引频道），不警告。
3. **值频道上的 `IndexSubscription` 靠行频道发现"离开"**：
   - 它本来就订着结果里每一行的行频道（包括 RLS 不可见的行）。行频道的通知到达时，先看读回的原始行
     （本 tick 的预读缓存，不增加 IO）：行不存在，或该字段已经不等于订阅的值，就按"索引变化"处理，
     重跑一次 range 比对——和值频道通知走同一段逻辑：推 None、退订该行频道、把被 limit 截掉的行补进来。
     否则照常作为行更新。
   - 字段比较用 `dtype.type(row[field]) != point_value`，与 `point_query_value_` 的规范化一致。
   - 区间订阅、退化的点查询不做这个检查：整索引频道本来就会因为"离开"而触发。
4. **`index_value_channel()`**：索引没有声明 `point_sub` 时抛 `ValueError`，免得订一个永远不会有人
   发的频道（比如服务端 watch 顶号这类用法）。

### 3.5 顶号

- 连接登录后 watch 的是 `Connection.owner == user_id` 的值频道。顶号在 `elevate()` 的同一个事务里
  完成：旧连接那行的 owner 改成 0（离开，不发），新连接那行的 owner 改成 user_id（进入，发 user_id
  的值频道）。旧连接照样收到通知。
- 连接断开时删除 Connection 行，只涉及本连接自己，不需要通知。
- 更新 `hetu/endpoint/connection.py`、`hetu/server/websocket.py` 里描述这个机制的注释。

### 3.6 hub

`PubSubHub._on_message`：非 keyspace 频道的消息为空时直接当作无 payload，不走 msgpack 解包
（现在空消息会先抛一次异常再被兜住）。

### 3.7 守门测试 `tests/test_arch_publish.py`

写法同 `tests/test_arch_master_reads.py`：文件头写清楚为什么要限制、挂了应该怎么办
（先读 `benchmark/redis_publish_cost_result.md` 的规则和本 spec）。

**静态扫描：**

- `hetu/` 下所有 `.lua` 文件里的 `PUBLISH` / `SPUBLISH` 调用点，只允许 `commit_v2.lua` 里的这两处
  （按规范化后的整行比对）：`redis_call("PUBLISH", pub[1], pub[2])`、`redis_call("PUBLISH", ch, "")`。
  其他任何调用点（包括任何 SPUBLISH）都会让测试失败。
- `hetu/` 下 `.py` 文件不允许直接调用 publish（`.publish(`、`.spublish(`、
  `execute_command("PUBLISH"` 等）。
- 同时检查允许清单里的调用点都还在，清单不能过期。

**运行时**（Redis 系后端，读 master 上 `INFO commandstats` 的 `cmdstat_publish` 计数差值）：

- 未声明的组件 / 索引上的 insert、update（包括改索引字段）、delete：0 条；
- `point_sub` 索引：insert 每个字段 1 条；改字段只发 1 条（新值）；delete 0 条；一个事务里多行同值只发 1 条；
- `table_sub` 组件：每个事务每张表 1 条，与改了几行无关；
- 另开连接订阅这些频道：值频道消息必须是空串，表频道消息必须恰好是 msgpack 的 row_id 字符串列表，
  不能多带字段。

失败信息指向报告和本 spec。

### 3.8 文档与其他

- `docs/zh/concepts.md`、`docs/en/concepts.md` 的订阅一节：两个声明、不声明的后果（整表订阅被拒；
  点查询退化为整索引唤醒并警告）、怎么选。
- docstring（中英）：`define_component`、`property_field`、`subscribe_table`、`subscribe_range`、
  `BackendClient.table_channel` / `index_value_channel`。
- C# SDK `WatchTable` 系列的 XML 注释：组件要声明 `table_sub`。
- `hetu/llms.txt` 里有订阅的段落就补一句。
- benchmark 里依赖点查询 / 整表订阅的组件补上声明，免得压测悄悄退化。
- 按报告规则 5，`benchmark/redis_commit_cost.py replay` 加本设计的 payload 变体，与现状对比并记录。

## 4. 正确性

值频道点查询 `range(Comp, f=v)` 在各种变更下怎么发现：

| 事件 | 需要知道的订阅者 | 如何发现 |
|---|---|---|
| 插入一行，`f=v` | v 的订阅者 | v 的值频道 → 重跑比对 |
| 某行 `f` 从 w 改成 v | v 的订阅者 | v 的值频道 → 重跑比对 |
| 同上 | w 的订阅者（持有该行） | 该行的行频道 → 读回 `f≠w` → 重跑比对 |
| 删除一行 | 持有该行的订阅者 | 该行的行频道 → 读回 None → 重跑比对 |
| 改普通字段 | 持有该行的订阅者 | 该行的行频道 → 行更新 |
| 被 limit 截在结果外的行离开 | 无 | 不影响结果窗口 |
| 结果内的行离开，窗口要补位 | 持有该行的订阅者 | 重跑比对时补进下一行 |

- 与 #148 的时间预算兼容：行频道通知同样满足"通知后至少隔 T 再读"。预算内，读回的行和重跑的
  range 都不旧于那次写入；超出预算时的残留与现状同一措辞。
- 订阅生效后的补读（`request_reread`）不变。pubsub 重订后的 `RESYNC` 同样会让行频道重读、触发
  成员检查，结果无害。
- 发送方的声明 ⊇ 订阅方的期望，就一定正确；反过来会漏通知。
- 混合版本：新写入方不再发旧值 / 未声明索引的值频道 / 未声明表的表频道，老版本订阅方会漏通知，
  所以订阅方（worker）要先升级或一起升级。目前没有部署，记录在此即可。

## 5. 成本

- master：未声明的通知一条不发；声明了的值频道每次"进入"一条，扁平格式；表频道只给少数冷表。
- 副本：少执行同样数量的 PUBLISH。
- worker：值频道订阅收到行频道通知时多一次字段比较（内存操作）；"离开"时重跑一次 range，相当于
  现在旧值频道触发的那一次。
- 未声明索引上的点查询退化为整索引唤醒，成本见 `benchmark/sub_budget_result.md` 的区间查询一栏——
  这正是警告要提醒的。

## 6. 测试计划（先写 red）

按后端参数化（`mod_auto_backend`）。TDD 时 `HETU_TEST_BACKENDS=redis`，收尾跑全后端。

- **组件**：`point_sub` 强制打开 index；`table_sub_` / `point_subs_` 进 `json_`，由 `load_json`、
  `duplicate()` 还原；`load_json` 读旧 JSON（没有这两个键）按 False。
- **订阅**（`tests/test_backend_sub.py`）：
  - 未声明 `table_sub` → `subscribe_table` 返回 `(None, [])`，不留订阅；
  - 未声明 `point_sub` 的点查询 → 订的是整索引频道，同一组件索引只警告一次，进入 / 离开 / 修改仍都推得到；
  - `index_value_channel` 对未声明索引抛 ValueError；
  - 声明了 `point_sub`：改走、删除靠行频道推 None；有 limit 截断时离开后补位；RLS 不可见的行离开不推；
  - 现有点查询 / 整表订阅用例改用声明了的组件（测试夹具 `Item`、`RLSTest` 补声明），全部回归。
- **commit**（`tests/test_backend_client.py` / `test_backend_sql.py`）：§3.2 表格的每一行。Redis 数
  PUBLISH，SQL 数通知行。
- **顶号**：`test_endpoint_connection`、`test_websocket` 回归。
- **守门**：`tests/test_arch_publish.py`（§3.7）。

## 7. 提交计划

每一步先提交 red 测试，再提交实现，保证每个实现提交之后测试全绿。

1. `docs(spec)`：本设计稿。
2. `test(component)` red → `feat(component)`：`table_sub` / `point_sub` 声明并进 schema。
3. `test(sub)` red → `feat(sub)`：整表订阅拒绝未声明的组件；点查询按声明选频道，未声明退化并警告；
   `index_value_channel` 校验声明；测试夹具、`Connection.owner` 补声明。
4. `test(arch)` red（守门测试与 commit 计数，现状"全都发"时失败）→ `perf(commit)`：Redis 与 SQL 的
   commit 按声明发、值频道只发新值、payload 扁平、Lua 调用点固定；值频道订阅靠行频道发现离开
   （和 commit 改动必须同一个提交，否则现有"离开"用例会红）；hub 空消息不解包。
5. `docs`：文档、docstring、C# 注释；`bench`：benchmark 组件声明、commit 回放加变体并记录结果。

## 8. 已知限制

- 改声明需要 `hetu upgrade`（schema 版本变了）。
- 未声明索引上的点查询功能正确，但性能退化为区间订阅，只有警告提醒，不会报错。
- 声明了 `point_sub`，写入方每次"进入"仍要付一条 PUBLISH；要连这条也去掉，只能改索引存储（§9 D）。

## 9. 备选方案与否决理由（留给后来人，别再重复提）

- **A. 副本上用 `PSUBSCRIBE __keyspace@0__:{prefix}:id:*` 代替表频道。** 在副本上验证可行，master
  零成本；但只是把成本挪到了副本——每条 keyspace 事件要和所有模式各比一次（约 24ns/个），被整表订阅的
  表一多，副本反而比 PUBLISH 贵。阿里云代理怎么路由 PSUBSCRIBE 也没法确定。值频道也推不出来：
  zset 的 keyspace 事件不带 member。
- **B. `CLIENT TRACKING ... BCAST PREFIX`。** 在副本上可用，还能合批；但前缀只能加、不能单独删，
  RESP2 下要另开控制连接 REDIRECT，重连要重建，代理基本不支持。
- **C. 单独一个"通知 key"，commit 写它来触发 keyspace 事件。** 写这个 key 本身就是一条复制到所有
  副本、在 master 和每个副本上都要执行的命令，成本 ≥ PUBLISH，还要管 key 的生命周期。keyspace 通知
  只有搭在"本来就要做的写"上才是白捡的。
- **D. 声明了的索引按值拆成多个 key 存储**（每个值一个 zset，commit 本来就要做的 ZADD / ZREM 落在
  值自己的 key 上，它的 keyspace 事件就是值频道）。零额外命令、完全没有 PUBLISH；但这类索引做不了
  区间查询，要改存储、迁移和 unique 检查。本设计的开销不够时再考虑。
- **E. worker 收整表行事件、自己按值分拣。** master 零成本，但每个有点查询的 worker 都要收、都要读
  这张表的每一次写入，而点查询恰恰用在热表上；Connection 的心跳（direct_set）也会被带回来。

## 10. 实测（实施后）

Docker/WSL2，master 挂 1 个副本，master 主线程 µs/commit，只看相对值；Linux 上请用
`benchmark/redis_commit_cost.py replay` 复测后更新 `benchmark/redis_publish_cost_result.md`。

`redis_commit_cost.py replay`（新增 `optin` / `optin_val` 两个变体，两轮平均）：

| rows | old | main（现状） | optin（未声明） | optin_val（每行 1 条扁平值频道） |
|---:|---:|---:|---:|---:|
| 1 | 4.29 | 5.34 | 4.36（−18%） | 5.27 |
| 2 | 6.74 | 8.36 | 6.69（−20%） | 8.01 |

- 没声明 `table_sub` / `point_sub` 的组件（绝大多数写入）回到 old 的水平。
- `optin_val` 与 `main` 不是严格的格式对照：`main` 的表频道名每次相同，`optin_val` 的值频道名
  每行不同。

严格对照（同一批 key、同一种频道命名，交错运行，新旧 Lua 各一份）：单条通知在 commit 上的增量，
扁平格式（只传频道名、消息为空串）+0.46~0.49µs，现状格式 `[频道, msgpack(ids)]` +1.02µs。

回归：Redis 后端全量测试通过（464 passed / 2 skipped）。
