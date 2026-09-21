# 整表订阅（TableSubscription）与大结果集订阅提速设计

## 背景与问题

典型需求：客户端需要"所有玩家的名字"这类**行数多、单行小、极少变化**的整表数据，并且希望
某一行变了能收到推送。目前只能用 `subscribe_range` 做，会遇到两层问题：

1. **订阅粒度被通知源锁死在行级。** HeTu 的变更通知来自 Redis keyspace notification
   （`client.py` 的 `row_channel` / `index_channel`，`post_configure` 打开 `Kghz`）。
   keyspace 通知天然是"每个 key 一条、没有 payload"，所以 `subscribe_range` 只能为区间内
   每一行各订一个 channel（`sub.py` 的 `IndexSubscription.add_row_subscriber`）。
   这不是 `sub.py` 的选择，是通知源的约束。
2. **一万行 = 两万次串行往返。** `RedisBackendClient.range()` 拿到 id 列表后逐行
   `await aio.hgetall()`；`subscribe_range` 再对每行 `await mq.subscribe()`，而
   `AsyncKeyspacePubSub.subscribe()` 每次都等 Redis 回 subscribe ack 才返回。
   1ms RTT 下一万行要 20 秒以上，这才是"几千次 rpc 卡死双方"的真实来源，
   与客户端 RPC 次数无关。
3. **Redis 侧的硬墙。** 每个连接每个订阅 channel 在 Redis 的 `pubsub_channels`
   里占一个节点。一万在线 × 一万行 = 一亿条。`MAX_SUBSCRIBED=5000` 只是告警，不是保护。

"订阅一个版本号 + RPC 拉全量 JSON"的绕路方案，本质上是把"表级变更通知 + 全量快照"
手工拼出来。本设计把它内建到引擎里。

## 目标

- **第一步**：不改架构，消除 `range()` 与批量订阅的串行往返。对现有 `subscribe_range`
  也直接受益。
- **第二步**：新增独立语义 `subscribe_table`：一个 channel 订阅整张表，客户端一次拿到
  全量快照，之后按行收增量。与 `subscribe_range` **语义独立、代码独立**，不做自动切换。

## 非目标

- 不做 Redis Stream 变更日志 / 断线续传（`since=`）。断线重连仍是重新 `subscribe_table`
  拉全量。
- 不改 `subscribe_range` / `subscribe_get` 的语义与 channel 粒度。
- 不做 SPUBLISH / SSUBSCRIBE（见"已知限制与后续"）。
- 不做 `logic_query`。

---

## 第一步：消除串行往返

### 1.1 `range()` 行读取走 pipeline（Redis 后端）

`hetu/data/backend/redis/client.py` 的 `range()`：

```python
# 现状
for _id in row_ids:
    if row := await aio.hgetall(key_prefix + str(_id)):
        rows.append(...)
```

改为分块 pipeline（`transaction=False`），块大小常量 `RANGE_PIPELINE_CHUNK = 1000`：

```python
for chunk in itertools.batched(row_ids, RANGE_PIPELINE_CHUNK):
    pipe = aio.pipeline(transaction=False)
    for _id in chunk:
        pipe.hgetall(key_prefix + str(_id))
    for row in await pipe.execute():
        if row:
            rows.append(self.row_decode_(comp_cls, row, row_format))
```

要点：

- 同一张表的所有行 key 都带同一个 `{CLU<id>}` hash tag，cluster 模式下同 slot，
  redis-py 的 async `ClusterPipeline` 可直接用，不会跨节点。
- 与 `batch.py` 里被否决的 `RedisBatchedClient` 不是一回事：那是**跨请求**的自动合批，
  会让不相关的请求互相等待抬高 RTT；这里是**同一个逻辑操作内部**的 N 次读取合并成
  ⌈N/1000⌉ 次往返，不引入任何等待。
- 抽出内部方法 `_hgetall_many(key_prefix, row_ids) -> list[dict]`，第二步的 `get_many`
  与 `TableSubscription` 复用。
- SQL 后端的 `range()` 已经是单条 `SELECT ... WHERE ... IN`，不需要改。

### 1.2 新增 `BackendClient.get_many()`

```python
async def get_many(
    self, table_ref, row_ids: Iterable[int], row_format=RowFormat.STRUCT
) -> list[dict | np.record | None]:
    """按 row_ids 顺序批量读行，不存在的位置为 None。"""
```

- Redis：复用 `_hgetall_many`。
- SQL：`SELECT * FROM t WHERE id IN (...)`，按入参顺序回填，分块尊重
  `MAX_CHANNELS_IN_FILTER` 同量级的参数上限。
- `IndexSubscription.get_updated()` 的 inserts 循环可顺手改用 `get_many`，非必需。

### 1.3 批量 SUBSCRIBE / UNSUBSCRIBE

`MQClient` 基类签名改为可变参数，语义为"全部订阅成功后返回"：

```python
async def subscribe(self, *channel_names: str) -> None
async def unsubscribe(self, *channel_names: str) -> None
```

`AsyncKeyspacePubSub`（`pubsub.py`）：

- `subscribe(*channels)`：cluster 模式先按 slot 分组到 node，每个 node 一次
  `ps.subscribe(*group)`（redis-py 单条命令支持多 channel）；全部加入
  `_pending_subscribe`，然后用现有 `_subscribe_notify` Condition 等到
  `_pending_subscribe.isdisjoint(channels)`。
- `unsubscribe(*channels)`：同样按 node 分组，一次 `ps.unsubscribe(*group)`。
- 单 channel 调用是多 channel 的特例，不再单独维护。

`RedisMQClient` / `SQLMQClient`：透传，`subscribed` 集合批量更新，`MAX_SUBSCRIBED` 告警
逻辑不变。

`SubscriptionBroker`：

- `subscribe_range()` 收集全部 `row_channel` 后调用一次 `mq.subscribe(index_channel, *row_channels)`。
- `unsubscribe()` 收集本 sub 独占的 channel 后调用一次 `mq.unsubscribe(*chans)`。
- `get_updates()` 内 `new_chans` / `rem_chans` 同理各调用一次。

### 1.4 第一步验收

- 现有 `tests/test_backend_sub.py`、`tests/test_backend_redis_cluster_pubsub.py` 全绿。
- 新增 `test_subscribe_range_large`：插入 3000 行，`subscribe_range(limit=3000)`，
  断言返回 3000 行、`mq.subscribed_channels` 为 3001 个；不断言耗时（CI 波动），
  但本地记录改前/改后耗时写进 PR 描述。
- 新增 `test_get_many`：混合存在/不存在的 id，断言顺序与 None 位置。

---

## 第二步：整表订阅 `subscribe_table`

### 2.1 语义

- 订阅 **整张 Component 表**：初始返回 caller 可见的全部行，之后任何行的
  insert / update / delete 推送该行最新值（删除或失去 RLS 权限推 `None`）。
- 权限：表权限沿用 `_has_table_permission`；RLS 组件逐行过滤，与 `subscribe_range` 相同：
  失去权限**会**收到删除通知，获得权限**会**收到添加通知（这一点比 `subscribe_range`
  更强，因为表级通知覆盖所有行，见 2.5）。
- 每个 `(连接, Component)` 只允许一个整表订阅，重复订阅返回同一个 `sub_id` 并 warning，
  与现有行为一致。
- 与同一张表上的 `subscribe_range` / `subscribe_get` 可以共存，互不影响：它们仍走
  keyspace channel。

### 2.2 后端：表级变更 channel

`BackendClient` 基类新增抽象方法：

```python
def table_channel(self, table_ref: TableReference) -> str:
    """返回表级变更频道名。表内任何行变动，会向该频道发送变动的 row_id 列表。"""
```

**Redis**（`client.py`）：

```python
def table_channel(self, table_ref):
    return f"{self.cluster_prefix(table_ref)}:table"
```

- 这是普通 PUBLISH channel，不带 `__keyspace@` 前缀，与 keyspace 通知互不干扰。
- 名字里带 `{CLU<id>}`，`AsyncKeyspacePubSub.subscribe` 按 slot 路由订阅到对应 node；
  cluster 模式下 PUBLISH 会经 cluster bus 广播到全部 node，订阅在任一 node 都能收到。
- 消息 payload：msgpack 打包的 `list[str]`（row_id 字符串列表，与 Lua 里 key 的 id 段一致）。

**Lua**（`commit_v2.lua`）：payload 增加第 4 段 `publishes`，Python 侧已按表聚合好：

```lua
local publishes = payload[4]
-- ...Phase 2 pushes 之后
if publishes then
    for _, pub in ipairs(publishes) do
        -- pub 格式: [channel, packed_row_ids]
        redis_call("PUBLISH", pub[1], pub[2])
    end
end
```

**Python `commit()`**：在 dirties 循环里按 `ref` 收集 `touched_ids: dict[ref, list[str]]`
（insert / update / delete 的 `row_id` 全部收进去），循环结束后：

```python
publishes = [
    [self.table_channel(ref), msg_packer.pack(ids)]
    for ref, ids in touched_ids.items()
]
payload_json = msg_packer.pack([checks, pushes, deleted, publishes])
```

一个事务一张表只 PUBLISH 一条。纯读行（clean rows）不发。没有订阅者时 PUBLISH 是 O(1)，
不做条件判断。

**SQL**（`sql/client.py`）：

- `_Hetu_Notify` 表新增 `payload` 列：`sa.LargeBinary`，nullable。schema 变更走
  `hetu upgrade` 已有的建表逻辑（`notify_table()` 在 `create_all` 时自动带上新列；
  已有部署的旧表需要在 `maint.py` 的升级路径里 `ALTER TABLE ADD COLUMN`）。
- `table_channel()` 返回 `f"{self.table_prefix(table_ref)}:table"`。
- `commit()` 里现有的 `channels` set 之外，追加一条
  `{"channel": table_channel, "payload": msgpack(ids), "created_at": now}`。

### 2.3 MQClient：携带 payload

`MQClient.get_message()` 返回类型从 `set[str]` 改为 `dict[str, set[str] | None]`：
key 是 channel 名，value 是该 tick 内合并后的 row_id 集合；keyspace channel 的 value 为
`None`。

`RedisMQClient`：

- 新增 `self.pulled_payload: dict[str, set[str]]`。
- `pull()`：`msg["data"]` 非空且能 msgpack 解出 list 时（即表级 channel），
  `pulled_payload.setdefault(channel, set()).update(ids)`；channel 名进 `pulled_deque` /
  `pulled_set` 的去重逻辑不变。keyspace 消息的 `data` 是事件名（`hset` 等），忽略。
- 2 分钟丢弃逻辑：丢弃 channel 时同步 `pulled_payload.pop(channel)`。
- `get_message()`：pop 出的 channel 一并 `pulled_payload.pop(channel, None)` 组成返回 dict。

`SQLMQClient`：`pull()` 的 `SELECT` 增加 `payload` 列，非空时同样合并到 `pulled_payload`；
其余同上。

`BaseSubscription.get_updated()` 签名增加 `payload: set[str] | None = None`。
`RowSubscription` / `IndexSubscription` 忽略该参数。

### 2.4 `TableSubscription`

新增 `hetu/data/sub.py`：

```python
class TableSubscription(BaseSubscription):
    def __init__(self, table_ref, servant, ctx, table_channel, known_ids: set[int]):
        ...
        self.known_ids = known_ids   # 当前已推送给客户端、且客户端仍持有的行 id

    async def get_updated(self, channel, payload=None):
        assert channel == self.table_channel
        if not payload:
            return set(), set(), {}
        ids = [int(i) for i in payload]
        rows = await self.servant.get_many(self.table_ref, ids, RowFormat.TYPED_DICT)
        rtn = {}
        for row_id, row in zip(ids, rows):
            visible = row is not None and (
                self.rls_ctx is None or self.rls_ctx.rls_check(comp_cls, row)
            )
            if visible:
                del row["_version"]
                rtn[row_id] = row
                self.known_ids.add(row_id)
            elif row_id in self.known_ids:
                rtn[row_id] = None
                self.known_ids.discard(row_id)
            # 既不可见、客户端也从未持有：不推
        return set(), set(), rtn

    @property
    def channels(self):
        return {self.table_channel}
```

不需要 `RowSubscription.__cache`：表级 sub 每个连接每张表只有一个，不存在交叉重复读。

### 2.5 `SubscriptionBroker.subscribe_table()`

```python
async def subscribe_table(
    self, table_ref: TableReference, ctx: Context
) -> tuple[str | None, list[dict]]:
```

流程：

1. `_has_table_permission` 不通过 → warning，返回 `(None, [])`。
2. `sub_id = f"{table_ref.comp_name}.table"`；已存在 → warning，返回旧 `sub_id` 与
   当前全量行（重新查一次，保持"重复订阅返回数据"的既有约定）。
3. 全量读取：`servant.range(table_ref, "id", -inf, +inf, limit=max_rows + 1, row_format=TYPED_DICT)`。
   `id` 是每个 Component 的隐式 unique index（`component.py` 的 `properties["id"]`），
   第一步的 pipeline 让这一步是 ⌈N/1000⌉ 次往返。
   - 结果超过 `max_rows` → warning "整表订阅行数超过 MAX_TABLE_SUBSCRIPTION_ROWS"，
     返回 `(None, [])`，不订阅。
4. RLS 组件逐行 `_has_row_permission` 过滤；`del row["_version"]`。
5. `await mq.subscribe(servant.table_channel(table_ref))`，注册 `TableSubscription`
   到 `_subs` / `_channel_subs`。
6. 返回 `(sub_id, rows)`。

`unsubscribe()`、`get_updates()` 对 `TableSubscription` 无特殊处理：`channels` 属性只有
一个 channel，`get_updated` 不会返回 new/rem chans。`get_updates()` 循环改为
`for channel, payload in updated.items()` 并把 `payload` 传给 `sub.get_updated`。

`count()` 返回三元组 `(row, index, table)`；`_index_sub_count` 的 `list(map(type, ...))`
统计改成订阅/取消时增减计数器，顺手去掉 O(n)。

### 2.6 限额与配置

`Context` 新增 `max_table_sub: int`，`configure()` 增加同名参数；`connection.py` 的
`elevate()` 同样 `*= 50`。

`CONFIG_TEMPLATE.yml` 新增：

```yaml
# 每个连接允许的整表订阅数（subscribe_table）
MAX_TABLE_SUBSCRIPTION: 3
# 单次整表订阅允许的最大行数，超过则拒绝订阅。整表订阅的目标是"行多但每行小、很少变"的表
MAX_TABLE_SUBSCRIPTION_ROWS: 100000
```

`websocket.py` 读取并传入 `context.configure(max_table_sub=...)`；`SubscriptionBroker`
构造时接收 `max_table_rows`（由 `websocket.py` 从 config 取，默认 100000）。

`receiver.py` 的 `sub_call()`：

```python
case "table":
    check_length("table", data, 3, 3)
    sub_id, sub_data = await broker.subscribe_table(table, ctx)
```

限额检查扩展为三项：`num_table_sub > ctx.max_table_sub` 同样视为非法操作断开。

### 2.7 协议

- 请求：`["sub", "<ComponentName>", "table"]`
- 回复：`["sub", sub_id, list[row]]`（与 `range` 回复格式相同）
- 更新：`["updt", sub_id, {row_id: row | null}]`（与 `range` 更新格式相同）

格式与 `range` 完全一致是刻意的：客户端可以**直接复用** `IndexSubscription<T>` 类，
`OnInsert` / `OnUpdate` / `OnDelete` / `ObserveAdd` / `ObserveRow` 全部原样可用。

### 2.8 客户端 SDK

Unity（`ClientSDK/unity/cn.hetudb.clientsdk/HeTu/`）：

- `ClientBase.cs` 新增 `internal const string QueryTable = "table"`。
- `SessionClientBase.cs` 新增 `WatchTable<T>(string componentName = null)`，实现与
  `WatchRangeSync` 同构，发送 `["sub", comp, "table"]`，回包构造 `IndexSubscription<T>`。
- `IndexSubscription<T>` 的 `Restore`：目前按 `RestoreIndex / RestoreLeft / RestoreRight`
  重发 `range`。需要一个标记（如 `RestoreIndex == null` 或独立的 `RestoreKind` 字段）
  让整表订阅断线重连时重发 `table` 而非 `range`。这是本步骤客户端侧唯一的非增量改动，
  需要在 `Tests/Editor/SubscriptionTest.cs` 覆盖。
- Headless C#（`ClientSDK/csharp/HeTu.Client/HeadlessHeTuClient.cs`）加同名
  `WatchTable<T>` 包装。

`hetu build` 的 C# sourcegen（`hetu/sourcegen/csharp.py`）目前不生成 Watch 相关代码，
不需要改。

### 2.9 测试

`tests/test_backend_sub.py` 新增（`mod_auto_backend` 夹具，redis / sql 全后端跑）：

- `test_subscribe_table`：订阅后 insert / update / delete 各一行，`get_updates` 依次收到
  `{id: row}` / `{id: row}` / `{id: None}`；`mq.subscribed_channels` 始终只有 1 个。
- `test_subscribe_table_merge`：一个 tick 内两个事务各改不同行，一次 `get_updates`
  收到两行；同一行改两次只收到最终值一次。
- `test_subscribe_table_coexist_range`：同表同时 `subscribe_range` 与 `subscribe_table`，
  一次修改两个 sub 都收到，且 keyspace 与 table channel 各自独立。
- `test_subscribe_table_rls_lost` / `_gain`：RLS 组件行的 owner 改走 / 改来，
  分别收到 `None` / 行数据；从未可见的行删除不推送。
- `test_subscribe_table_permission_denied`：ADMIN 组件普通用户返回 `(None, [])`。
- `test_subscribe_table_row_cap`：`max_table_rows=10`，插 11 行，返回 `(None, [])` 且
  没有订阅任何 channel。
- `test_subscribe_table_duplicate` / `test_unsubscribe_table`：重复返回同 id；取消后
  channel 被释放、再改行不再收到。
- `test_subscribe_table_large`：3000 行整表订阅，断言行数与 channel 数 == 1。

`tests/test_backend_redis_cluster_pubsub.py` 新增：cluster 模式下 table channel 的
PUBLISH 能被按 slot 订阅的 node 收到。

`tests/test_websocket.py` 新增：通过 WebSocket 发 `["sub", comp, "table"]`，收到 `sub`
回包与后续 `updt`；超过 `MAX_TABLE_SUBSCRIPTION` 被断开。

Unity `SubscriptionTest.cs`：`WatchTable` 的初始行、更新、断线 Restore 重发 `table`。

### 2.10 文档

- `docs/zh/concepts.md` "订阅"一节：加一段"整表订阅"，说明适用场景（行多、行小、少变）、
  与 `range` 的选择标准、行数上限。
- `docs/zh/advanced.md` "每连接状态"：补 `ctx.max_table_sub`。
- `docs/zh/unity-client.md`：`WatchTable<T>` 用法示例（PlayerNames 场景）。
- `docs/en/` 对应文件同步。
- `hetu/CONFIG_TEMPLATE.yml` 注释见 2.6。

---

## 已知限制与后续

- **消息丢失无法补偿。** `pull()` 的 2 分钟丢弃逻辑丢掉表级消息时，其中的 row_id 一并丢失，
  `TableSubscription` 会漏掉那些行。与现有行级订阅的行为一致；补偿需要变更日志 + 续传，
  本设计明确不做。
- **表级通知是全表广播。** 每个整表订阅者收到该表**所有**写入，包括它因 RLS 看不见的行
  （服务端过滤后不推给客户端，但服务端要读一次）。所以整表订阅只适合冷表；热表继续用
  `subscribe_range`。文档要写清楚这个选择标准，`MAX_TABLE_SUBSCRIPTION_ROWS` 是兜底不是指导。
- **cluster 模式 PUBLISH 走 cluster bus 广播。** 每个提交多一条小消息的集群内广播。
  后续可改 SPUBLISH / SSUBSCRIBE（Redis ≥ 7.0，`post_configure` 已要求 7.0，channel 已带
  `{CLU}` tag，`AsyncKeyspacePubSub` 已按 slot 路由，改动集中在 `pubsub.py`）。
  本设计先用 PUBLISH 把语义跑通。
- **客户端全量快照走单条 `sub` 回包。** 十万行会是一条几 MB 的消息，经 zlib 层压缩后
  可接受；更大的表应该先问"这真的适合整表订阅吗"。分页快照不在本设计内。

## 实施顺序

1. 1.1 + 1.2（`range` pipeline、`get_many`）——独立 PR，现有测试即回归。
2. 1.3（批量 subscribe/unsubscribe）——独立 PR。
3. 2.2 + 2.3（Lua PUBLISH、SQL payload 列、MQClient payload）——此时还没有消费者，
   现有测试应全绿，用于确认对现有订阅零影响。
4. 2.4 + 2.5 + 2.6 + 2.7（`TableSubscription`、`subscribe_table`、receiver、配置）+ 2.9 服务端测试。
5. 2.8 客户端 SDK + Unity 测试。
6. 2.10 文档。
