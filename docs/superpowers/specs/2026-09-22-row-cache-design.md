# 订阅驱动的 worker 行缓存（RowCache）— 设计稿

- 日期：2026-09-22
- 状态：已实施（第 3 版：行通知改为带版本号的 PUBLISH，以版本下限 + 权威读取代副本亲和；
  订阅推送与事务共用同一读路径，`RowSubscription` 的 per-tick 缓存并入进程缓存）
- 取代：`hetu/data/backend/redis/batch.py` 里被否决的两个想法（短 TTL 缓存、跨请求合批），
  该文件与 `tests/test_backend_redis_batch.py` 随本 spec 删除，结论留档在新模块 docstring。
- 影响范围：新增 `hetu/data/backend/rowcache.py`、`hetu/data/backend/redis/get_rows.lua`；
  `hetu/data/backend/__init__.py`（`Backend` 持有缓存）、`base.py`（`BackendClient.row_cache`
  属性、权威读接口）、`redis/client.py`（**行通知改 PUBLISH**、commit 写穿 / 逐出、
  `direct_set` 明确不通知、权威读）、`redis/mq.py`（hub 钩子）、`redis/pubsub.py`（节点失效 /
  恢复回调）、`repo.py`（`_fetch_by_id` / `range` 走共用
  读路径）、`sub.py`（订阅刷新走共用读路径、删 per-tick ContextVar 缓存、`subscribe_get`
  先订后读）、`CONFIG_TEMPLATE.yml`（开关）。
  **不涉及 schema、不涉及客户端协议、不改 `BackendClient.get / get_many` 签名。**
  SQL 后端不参与（hub 从不激活，缓存对它恒为 miss）。
- 升级注意：行频道从 keyspace 通知改为 commit 主动 PUBLISH，**所有写入方必须一起升级**
  （老写入方不发行频道，新订阅方收不到它的更新），与 #142 值频道的要求同类。

## 1. 背景与目标

"减少 Redis 往返"系列前两步（#142）把写事务压到了 `insert` 1 RTT、`get(id) + update` 2 RTT。
剩下的 1 次是事务里的读：每个 System 调用都要把它碰的行从 Redis 拉一遍，哪怕这行几毫秒前
刚被同一连接读过、而且本 worker 的订阅系统正把它的每次变更实时推给客户端。

`batch.py` 早先想用短 TTL 缓存解决，否决理由是"客户端收到订阅更新时读到的还是老数据，
把缓存失效和订阅通知绑定又太不灵活"。#142 之后这个绑定变得很自然：

- 每 worker 一个 `PubSubHub`（`redis/mq.py`），`_on_message` 同步看到本进程订阅的**每一条**
  通知，分发之前就能先动手；
- `hub._subs` 是"本进程哪些频道有人订"的引用计数表，天然划定了"哪些行的变更我们一定收得到
  通知"；
- commit 对所有干净读行做 VER 检查（`redis/client.py:964`），写事务读到陈旧数据只会
  `RaceCondition` 重试，不会写错。

同时要修一个既有问题：**代理模式下的副本滞后**。推荐的生产拓扑是 Redis Proxy 做分片 +
读写分离（`operations.md`），客户端指定不了读落在哪个副本，而通知（keyspace 事件）由某一个
节点发出——订阅收到通知后从随机副本重读，读到的可能是还没应用这次写入的旧值，并且**一直陈旧
到该行下次变更**。今天的订阅推送已经如此（`get_updated` 读随机副本），只是没人注意；缓存会把
这个旧值锁进进程内存，所以必须一并解决，而不能靠"读订阅所在节点"（代理模式下做不到）。

**目标**：把"本 worker 有客户端订阅着的行"在进程内存里保留一份，事务读命中即 0 往返；
commit 成功把新行写穿进缓存，同一连接反复读写同一行时只剩 commit 那 1 次往返；订阅推送
从同一份缓存取行——本 worker 自己写的行推送 0 次读，别人写的行每 worker 只读 1 次而不是
每个订阅连接各读 1 次；一致性由
**带版本号的通知 + 版本下限 + 权威读 + VER 检查**保证，不用 TTL，与拓扑无关。实测口径
（临时 Redis 挂 `redis-py` 计数器）：

| 操作（行已被本 worker 订阅） | 现在 RTT | 命令序列 | 本 spec 后 |
|---|---:|---|---:|
| `get(id)` 只读 System | 1 | HGETALL | **0** |
| `get(id)` + update + commit，首次 | 2 | HGETALL + EVALSHA | 2（首次填充走权威读） |
| `get(id)` + update + commit，之后每次 | 2 | HGETALL + EVALSHA | **1**（commit 写穿，下次命中） |
| `get(owner=)`（非主键）+ update + commit | 3 | ZRANGE + HGETALL + EVALSHA | 2 |
| `range(owner=)` 10 行只读 | 2 | ZRANGE + PIPELINE(10) | **1** |
| `range` 10 行，其中 3 行未订阅 | 2 | ZRANGE + PIPELINE(10) | 2（PIPELINE 只读 3 行） |
| `insert` + commit | 1 | EVALSHA | 1（不变；写穿后再读命中） |
| 未被订阅的行，任何操作 | — | — | 不变（miss 即今天的路径） |
| 订阅推送：本 worker commit 改的行 | 每连接 1 | HGETALL / PIPELINE | **0**（写穿） |
| 订阅推送：别的 worker 改的行，本 worker 有 N 个连接订着 | N | N 次 HGETALL / PIPELINE | **1**（首个填充，其余命中） |

索引读（ZRANGE）不缓存，所以非主键 `get` / `range` 仍各付 1 次；这是有意的（§7）。

### 已确认的设计决策

- **只缓存本 worker 有订阅的行**。`batch.py` 注释里"没人订阅短期旧数据也无所谓"不成立：没人订
  的 key 没有任何失效路径，不是"短期旧"而是永远旧。没人订 → 不缓存 → 与今天完全一致。
- **行通知带版本号**：行频道从 keyspace 事件改为 commit 在 Lua Phase 3 主动 PUBLISH，payload
  是该行的新 `_version`（删除为 0）。keyspace 事件不带任何内容，没有它缓存无法判断一次副本读
  是否"至少和通知一样新"。索引频道仍用 keyspace（`z`）。
- **版本下限（floor）**：缓存对每个激活的行记"已知至少到达的版本"= 收到的通知版本、本进程
  commit 得到的版本、RACE 返回的当前版本三者之最大。副本读回的行 `_version < floor` 即判定
  滞后，不入缓存，改走权威读。
- **权威读 = 在 master 上执行的 Lua `HGETALL`**。单机 / servants / 原生集群下 `master.get`
  本来就是权威的（集群模式的 master 客户端不从副本读，`redis/client.py:237`）；只有代理拓扑
  不是——`master:` 填的是代理地址，读写分离代理按命令类型分流，`HGETALL` 无论从哪个客户端
  对象发出都会被送到副本，协议上没有"这条读命令请走主节点"的写法，唯一一定被送到主节点的
  是脚本（代理无法判断脚本是否写）。客户端识别不了代理，而 EVALSHA 与 HGETALL 在真 master
  上成本相同，所以不分拓扑一律用脚本。首次填充（floor 未知）与检测到滞后时使用；一行的订阅
  生命周期内通常只发生一次。
- **commit 写穿**：commit 成功后把本事务 insert / update 的新行（版本 +1）直接放进缓存（仅当
  该行在提交前已激活、且提交往返期间没失活过），floor 同步抬到新版本；自己那条通知到达时
  版本不高于缓存，不逐出。
- **易失（`volatile=True`）组件不缓存，`direct_set` 明确不发通知**：`direct_set` 只允许用于易失
  组件且不动 `_version`，别的事务的 VER 检查感知不到它，也就不会因它冲突；文档早已写明它
  "不保证通知一致"（`advanced.md:553`），SQL 后端从来不发。把它定成规则：易失组件的行不进
  缓存（hub 不激活、repo 不查），`direct_set` 就不需要任何通知，缓存也没有"版本没动但内容
  变了"的中间态。内置的 `Connection` / `WorkerLease` / `SystemLock` 都是 ADMIN 权限，客户端
  订阅不到，本就不会被缓存。
- **事务读与订阅刷新共用一条读路径**（`CachedRowReader`，§3.5），`BackendClient.get`
  本身不变：`ConnectionAliveChecker`、`wait_for_synced` 等直读路径不受影响，`only_master`
  事务直接绕过。`RowSubscription` 自己那个 per-tick ContextVar 缓存删除：它解决的"同 tick
  交叉订阅不重复读、按表批量预读"由进程缓存 + 批量读路径覆盖，而且进程缓存跨连接、跨 tick，
  客户端收到的行与服务端事务读到的是同一份。
- **写事务的正确性不依赖缓存新鲜度**（VER 兜底）；**只读事务读到的行不早于本进程最近处理的
  该行通知**——与客户端订阅推送同等新鲜，陈旧窗口 = 通知延迟（Redis 毫秒级）。需要强一致读的
  场景用 `only_master`（headless 默认即 True，`headless.py:331`）。

## 2. 已确认的事实（均已核对）

- `PubSubHub.add`（`redis/mq.py:43`）先登记 `_subs[channel]` 再发 SUBSCRIBE，**返回时所有
  频道都已 ack**（搭车者也等到 ack）；`_release`（`base.py:1009`）同步删 `_subs` 并调
  `_on_channel_gone`（`base.py:1023`，子类钩子，PubSubHub 尚未覆盖），UNSUBSCRIBE 之后才发
  （可能在后台）。`_on_message`（`redis/mq.py:116`）在监听协程里同步执行：非 keyspace 频道
  尝试 msgpack 解 payload，不是 list 就当 None，然后 `_dispatch(channel, ids)`。
- `AsyncKeyspacePubSub`：`_subscribed` 只在收到 ack 且频道仍在 `_channel_node` 时加入
  （`pubsub.py:394`）；节点失效走 `_on_node_listener_done`（`:418`）→ `resubscribe_all`
  （`:355`）：**清空全部** `_subscribed` / `_channel_node` 后对所有频道重发 SUBSCRIBE，退避
  重试直到成功。失效到恢复之间的通知全部丢失。频道名只要带 `{hash tag}` 就能按 slot 路由。
- commit Lua Phase 3（`commit_v2.lua:89-99`）逐条 `PUBLISH pub[1] pub[2]`，`publishes` 列表
  完全由 Python 组装（`redis/client.py:956-960`：表级频道一条 + 每个 (索引, 值) 一条）——
  **加行频道通知不用改 Lua**。VER 失败返回
  `"RACE: Version mismatch <key> exp:<v> got:<current>"`（`commit_v2.lua:33`），带 key 与
  master 上的当前版本。`_hset_key` 里算出的 `_ver` 就是新版本。
- `get_dirty_rows`（`idmap.py:359`）的 update `new_rows` **只含变更字段**；完整的新行在
  idmap 的类型化缓存里（`_row_cache[ref]` + 状态 INSERT / UPDATE）。
- keyspace 通知由 `configure_servant` 配置（`redis/client.py:329`，目标 `Kghz`，只增不减；
  master 有 servants 时不配，`test_redis_notify_configuration` 断言 master 为空、副本含
  `Kghz` 全部四个标志）。`K` 是 keyspace 频道前缀，`z` 供索引 zset 的 ZADD / ZREM 通知
  （范围订阅订 `index_channel`），`h` 供行 hash 的 HSET、`g` 供行 key 的 DEL——后两者只服务
  行频道。zset 最后一个成员被 ZREM 时 Redis 除 `zrem` 外还会发 `del`，范围订阅有 `zrem` 就够。
  `direct_set`（`redis/client.py:1023`）是裸 HSET，不动 `_version`，只允许易失组件
  （`base.py:512`），`advanced.md:553` 已写明"绕过事务，不保证通知一致"，SQL 后端不发通知；
  `maint.upsert_row` / `delete_row`（`redis/maint.py:97-109`）是裸 DEL / HSET，维护工具只在
  停服时使用。`direct_set` 在 Redis 上今天靠 keyspace 事件通知，唯一观察到行频道心跳通知的是
  `tests/test_endpoint_connection.py:273`（顺带断言，alive 检查本身订的是 owner 索引值频道）。
- SQL 后端的 `row_channel` 已经就是 `row_key`（`sql/client.py:334`），通知表带 payload 列
  （`sql/mq.py:190`），行通知 payload 为空。
- `tests/test_backend_client.py:802` 断言行频道通知到 MQClient 时 payload 为 None——本 spec
  保持这一点：版本号只进缓存，不进 MQClient 队列。
- 原生集群模式下 master 客户端 `load_balancing_strategy=None`（`redis/client.py:237`），读只
  打主节点；servant 客户端才 `ROUND_ROBIN_REPLICAS`。所以 `master.get` 在单机 / servants /
  集群三种拓扑下都是权威读，只有代理拓扑不是。
- 脚本的路由：读写分离代理按命令类型分流，`EVAL` / `EVALSHA` 一律送主节点（无法判断脚本是否
  写）；redis-py cluster 的 `READ_COMMANDS` 含 `HGETALL` 不含 `EVALSHA`（已核对），EVALSHA
  固定发主节点。`commit_v2.lua` 没有 `#!lua` shebang（兼容模式）：Redis 7 对带 shebang 而未声明
  `no-writes` 的脚本在**启动前**就做写检查（副本 READONLY、`MISCONF`、`NOREPLICAS`），兼容
  模式只在脚本内真的执行写命令时才检查——只读脚本因此不会被 `min-replicas-to-write` 之类拒掉。
  `lua_commit` 已用 `register_script`（`redis/client.py:106`）在 master 上注册并处理
  NoScriptError。
- `Backend.servant` / `get_mq_client` 每次随机选 servant（`__init__.py:122` / `:139`）→ N 个
  servant 就有 N 个 hub；`master_or_servant` 按权重随机（`session.py:79`）。没配 servants
  时 `_servants` 是一个连到 master 地址、`is_servant=True` 的独立客户端。
- `SessionRepository._fetch_by_id`（`repo.py:177`）：`master_or_servant.get(STRUCT)` → 非空
  则 `idmap.add_clean`；`range`（`:365-391`）：ZRANGE 拿 id → idmap 命中的直接用 → 未命中
  的一次 `get_many` → `add_clean`。返回给用户的 `np.record` 就是客户端解码出的那个对象，
  用户会就地改它再 `update(row)`；`add_clean` 用 `np.append` 自己拷了一份。
  `np.record.copy()` 得到独立副本（已验证）。
- commit 失败分支 `redis/client.py:985` 还留着 `# self._batched_aio.invalidate_cache(
  idmap.get_clean_row_keys())`；`idmap.get_clean_row_keys`（`idmap.py:322`）除这行注释外
  无人使用。
- `RowSubscription` 已有一个 per-tick 的 ContextVar 行缓存（`sub.py:50`）；`subscribe_get`
  **先读后订**（`sub.py:378-393` 读，`:412` 订阅）；`get_updated`（`:97`）与 broker 的
  `_prefetch_rows` 都读订阅创建时随机选定的 `servant`——即 §1 说的既有陈旧推送问题。
- `batch.py` / `RedisBatchedClient` 只被 `tests/test_backend_redis_batch.py` 使用，
  `redis/client.py` 里 `:33 :260 :397 :517 :739 :985` 六处是注释引用。

## 3. 设计

### 3.1 不变量

```
缓存里的每一行的 _version ≥ 本进程对该行已知的版本下限 floor；
floor ≥ 本进程已处理的该行最新通知的版本；
该行之后的每一次 commit 都会向本进程发带版本号的通知 → 版本更高则逐出并抬 floor。
```

四条规则共同维持它：

1. **激活**（允许缓存）：仅当某个 hub 对该行频道的 SUBSCRIBE 已 ack、且频道仍在它的
   `_subs` 里。激活时 floor = 未知。
2. **通知**：`(channel, version)` → 若 `version > 缓存行版本` 则逐出；`floor = max(floor,
   version)`；删除（version 0）→ 逐出，floor = 已删除，并把该频道标成**只信权威读**直到本次
   激活结束（同 id 重插后 `_version` 从 1 重来，滞后副本上删除前的旧行版本反而更高，floor 挡不住）。
3. **填充**：副本读只在 `floor` 已知、频道未标只信权威读、且 `row._version ≥ floor` 时入缓存；floor 未知 / 已删除 /
   读回版本低于 floor 时改走权威读，权威读回的行无条件入缓存并把 floor 设为它的版本。
   填充还要求取 lease 时的激活代次仍当前（退订又重订的中间态不填）。
4. **逐出**：通知（规则 2）、本进程 commit 的 RACE（逐出本事务全部行，冲突行的 floor 抬到
   Lua 回显的当前版本）、最后一个本地订阅者退订、pubsub 节点失效。commit 成功不逐出而是
   写穿（§3.5）。

写事务另有 VER 检查兜底，即便规则被破坏也只是多一次重试。

### 3.2 行通知改为带版本号的 PUBLISH

- `RedisBackendClient.row_channel(ref, id)` 改为返回 `row_key(ref, id)`（与 SQL 后端一致，
  带 `{CLU}` hash tag，`AsyncKeyspacePubSub` 按 slot 路由）。
- `commit()` 组 `publishes` 时，每个 insert / update 行追加 `[row_channel, msgpack(new_version)]`，
  每个 delete 行追加 `[row_channel, msgpack(0)]`。Lua 不改。payload 是 msgpack 的**整数**，
  与表级 / 值频道的 list payload 可区分。
- `direct_set`：**不发通知**，docstring 与 `advanced.md` 从"不保证通知一致"改为"不通知"。
  它不动 `_version`，别的事务不会因它 RACE；易失组件不进缓存，所以缓存也不需要知道它。
- `maint.upsert_row` / `delete_row`：不补通知——维护工具只在停服时使用，没有在线订阅者和缓存。
- `PubSubHub._on_message`：payload 解出 int 且频道是行频道（含 `":id:"` 段）→
  `row_cache.notify(channel, version)`，然后照旧 `_dispatch(channel, None)`；list →
  今天的路径。keyspace 前缀分支只剩索引频道。
- `configure_servant` 的目标从 `Kghz` 改为 **`Kz`**：`h`、`g` 只服务行频道，改 PUBLISH 后没有
  订阅者，留着只是让每次 HSET / DEL 白拼一次频道名、查一次字典。函数本身仍只增不减（不动
  同一实例上别的应用设的标志），已经配过 `Kghz` 的旧实例保持原样、无害，新开的实例只得到
  `Kz`。无权限时的告警文案随 `target_keyspace` 一起变。
- 集群模式下 PUBLISH 是全集群广播（Lua 注释已接受，表级 / 值频道今天就是），行通知让广播量
  从"每事务每表一条"变成"每事务每行一条"；原生集群本就不推荐，记录即可。

### 3.3 `RowCache`（`hetu/data/backend/rowcache.py`，后端无关）

```python
UNKNOWN = ...   # floor 哨兵：激活后尚无任何版本信息（含节点恢复后重新激活）
DELETED = ...   # floor 哨兵：最近一次通知是删除

@dataclass(frozen=True, slots=True)
class Lease:
    channel: str
    epoch: int            # 取 lease 时该频道的激活代次；失活再激活会推进

class RowCache:
    # 读路径
    def get(self, channel) -> np.record | None                # 命中返回 .copy()
    def lease(self, channel) -> Lease | None                  # 未激活返回 None
    def floor(self, channel) -> int | UNKNOWN | DELETED       # 原始 floor
    def replica_floor(self, channel) -> int | None             # 副本读可信才给 floor，否则权威读
    def fill(self, lease, row, *, authoritative: bool) -> bool
    # 写路径（commit）
    def put_committed(self, lease, row) -> bool               # 写穿：凭据提交前取；floor = row._version
    def notify(self, channel, version: int) -> None           # 通知 / RACE 回显：见 §3.1 规则 2
    # hub
    def activate(self, channel, owner) / deactivate(self, channel, owner) / clear(self)
    stats: hits / misses / authoritative_reads / size
```

- 内部：`_rows: dict[channel, np.record]`、`_active: dict[channel, set[owner]]`、
  `_epoch: dict[channel, int]`、`_floor: dict[channel, int | UNKNOWN | DELETED]`；后三者随
  激活建、随最后一个 owner 失活删。
- `fill` 逻辑：频道未激活或 `lease.epoch` 过期 → False；`authoritative` → 存行，
  `floor = row._version`；否则 floor 必须是整数且 `row._version >= floor`，通过则存行、
  `floor = row._version`。
- `notify(version)`：`0` → 逐出，`floor = DELETED`；`v` → 若缓存行版本 `< v` 则逐出；
  `floor = v if floor in (UNKNOWN, DELETED) else max(floor, v)`。
  多 hub 重复送达幂等；跨 hub 乱序最坏让 floor 暂时偏高（下次填充多走一次权威读）。
- 键用**行频道名**（= `row_key`），hub 收到的通知就是它，零转换。只对行频道激活，且易失
  组件的行频道不激活（hub 从频道名解析不出组件，由 `RedisBackendClient` 提供
  `is_cacheable_channel(channel)`：行频道且组件非易失；repo 侧用 `comp_cls.volatile_` 直接跳过）。
- 值存 `np.record.copy()`，命中再 `.copy()` 返回：调用方就地改行不会污染缓存。不存 None。
- 容量：订阅刷新也填充后，常驻集 = 本进程激活且被读过的行（上限 = 各连接订阅行数之和，登录
  用户默认 `MAX_ROW_SUBSCRIPTION × 50` = 500 行 / 连接），每行约 0.3～0.5 KB。`_rows` 用
  `OrderedDict` 做 LRU，`row_cache_max_rows`（§3.7）封顶，超限只丢行不丢激活 / floor 状态，
  被丢的行下次 miss 再填。不做 TTL。

### 3.4 hub 钩子（`PubSubHub`）

`PubSubHub(client, row_cache)`；`get_mq_client` 里建 hub 时传入 `self.row_cache`
（`redis/client.py:1046`）。`self._active: set[str]` 记本 hub 已激活的行频道。

- `add()`：`wait_acks` 之后，对 `registered` 里仍在 `_subs`、可缓存（非易失组件的行频道）、且不在 `_active`
  的频道：`_active.add`；`row_cache.activate(channel, self)`。失败 / 取消分支不激活。
- `_on_channel_gone(channel)`：`_active.discard`；`row_cache.deactivate(channel, self)`。
  它在 `_release` 里同步调用，先于 UNSUBSCRIBE 发出。
- `_on_message(msg)`：解 payload → 行频道 `row_cache.notify(...)`（在 `_subs` 判断之前，
  刚退订的频道也无害）→ `_dispatch`。
- `close()`：对 `_active` 全部 `deactivate`。
- 节点失效 / 恢复：`AsyncKeyspacePubSub` 新增 `on_reset` / `on_restored` 两个无参回调
  （分别在 `_on_node_listener_done` 发起 `resubscribe_all` 前、`resubscribe_all` 成功后调用）。
  hub 的 `on_reset` 把 `_active` 全部失活；`on_restored` 对 `_subs` 里的行频道重新激活
  （floor 回到未知 → 下次权威读）。恢复期间没有激活 → 没有填充，丢失的通知不会留下陈旧行。

### 3.5 共用读路径 `CachedRowReader`、Session 接入与 commit

事务（`repo.py`）与订阅刷新（`sub.py`）读行都经过同一个对象（`rowcache.py`）：

```python
class CachedRowReader:
    """缓存命中直接返回；miss 按 floor 决定读副本还是权威读，读回的行填进缓存。
    fallback 是本次 miss 时读副本用的客户端（事务传 session.master_or_servant，
    订阅传 backend.servant）；权威读固定走 backend.master.get_authoritative。"""
    def __init__(self, backend: Backend): ...
    async def get(self, ref, row_id, fallback: BackendClient) -> np.record | None
    async def get_many(self, ref, row_ids, fallback: BackendClient) -> list[np.record | None]
```

`get(ref, row_id, fallback)`：

```python
cache = backend.row_cache
if cache is None or ref.comp_cls.volatile_:
    return await fallback.get(ref, row_id, RowFormat.STRUCT)          # 今天的路径
channel = backend.master.row_channel(ref, row_id)
if (row := cache.get(channel)) is not None:
    return row
lease = cache.lease(channel)
if lease is None:
    return await fallback.get(ref, row_id, RowFormat.STRUCT)          # 没人订：不缓存
floor = cache.floor(channel)
authoritative = floor is UNKNOWN or floor is DELETED
if not authoritative:
    row = await fallback.get(ref, row_id, RowFormat.STRUCT)
    authoritative = row is not None and row._version < floor            # 副本滞后
if authoritative:
    row = await backend.master.get_authoritative(ref, row_id)
if row is not None:
    cache.fill(lease, row, authoritative=authoritative)
return row
```

`get_many`：命中的直接取；其余分两组——无 lease 或 floor 已知的一次 `fallback.get_many`，
floor 未知 / 已删除的一次 `master.get_many_authoritative`（两组并发）；副本组里版本低于 floor
的再补一次权威批读；回来的行逐个 `fill`；返回与 `row_ids` 顺序一致、不存在为 None。稳态下
（floor 全部已知、副本不滞后）仍是 1 次 PIPELINE。返回的都是缓存外的独立 record，调用方可
随意改。

**Session 接入**：`Session` 持有 `backend.row_reader`（`Backend` 上一个共享实例；
`only_master` 为 True 时 `_fetch_by_id` / `range` 直接用 `session.master.get / get_many`，
不经缓存）。`_fetch_by_id` = `reader.get(ref, id, session.master_or_servant)` + `add_clean`；
`range` 现有"idmap 命中直接用、未命中收集 `miss_ids`"不变，`miss_ids` 交给
`reader.get_many(ref, miss_ids, session.master_or_servant)`，占位 / 顺序 / `add_clean` 批量
逻辑照旧。

**权威读**：`BackendClient.get_authoritative(ref, id)` / `get_many_authoritative(ref, ids)`，
基类默认退化为 `get` / `get_many`（SQL 无副本）。Redis 实现：`get_rows.lua`
（`for i, key in ipairs(KEYS) do res[i] = redis.call('HGETALL', key) end`），**不写
`#!lua` shebang、不声明 `no-writes`**——路由到主节点靠的是命令名（代理 / redis-py cluster 都
按 EVALSHA 路由），与脚本标志无关；不带 shebang 走兼容模式，只读脚本不会被主节点的写检查
（`NOREPLICAS` / `MISCONF`）拒掉，与 `commit_v2.lua` 风格一致。在 master 客户端上
`register_script`，与 `lua_commit` 同样在 `post_configure` 加载；同一张表的行同 slot，一次
EVALSHA 多 key 即可，按 `RANGE_PIPELINE_CHUNK` 分块。

**commit**（`redis/client.py`）：

- 成功：对本事务 insert / update 的行，从 idmap 取类型化整行、`_version` 置为 `_hset_key`
  算出的新版本，`row_cache.put_committed(lease, row)`；delete 行 `notify(channel, 0)`。
  写穿的凭据（`lease`）在提交**之前**取：只对提交前已激活的行生效，且提交往返期间该频道
  失活过（退订又重订，期间别的进程的写入没通知到本进程）就丢弃，留给下次的权威读；
  易失组件的行从不激活。
- `RACE`：`evict` 逐出 `dirties` ∪ `get_clean_rows()` 全部行（保留 floor）；若消息是
  `Version mismatch`，解析出
  key 与 `got:` 版本，对该行 `notify(channel, got)`（`got` 为 nil 即已删除 → 0）——重试时
  该行必走权威读，不再吃到同一份旧数据，也不会靠副本滞后再 RACE 一轮。
- `UNIQUE` / 其他错误不动缓存。删除 `idmap.get_clean_row_keys` 与 `:985` 注释。

### 3.6 拓扑

| 拓扑 | 通知来源 | 副本读 | 权威读 | 保证 |
|---|---|---|---|---|
| 单机 / master 兼 servant | master | 同一节点 | 同一节点 | 完整 |
| 显式 `servants` | 各副本（应用复制流时 PUBLISH 同样被复制） | 随机副本 | master | 完整：滞后副本由 floor 识破 |
| 原生集群（副本轮询） | 订阅所在节点 | 轮询副本 | slot 主节点（EVALSHA） | 完整 |
| 代理（分片 + 读写分离） | 代理路由到的节点 | 代理选的副本 | 代理把脚本送到主节点 | 完整 |

唯一的前提是代理把 `EVALSHA` 路由到主节点——所有做读写分离的代理都必须如此（脚本可能写）。
`operations.md` 写明这一前提。

### 3.7 配置与开关

`BACKENDS.<name>.row_cache: true`（`CONFIG_TEMPLATE.yml`，默认 true）。`Backend.__init__`
读取并从 `extra_config` 排除，`row_cache=False` 时 `Backend.row_cache = None`：hub 不激活、
repo 不查、commit 不写穿——但**行通知照发**（订阅方需要它，且开关不应改变 wire 行为）。
`Backend` 建好客户端后 `client.row_cache = self.row_cache`（基类属性默认 None）。
`row_cache_max_rows`（默认 200_000，约 60～100 MB / worker 上限）：缓存行数 LRU 上限，只丢行
不丢激活 / floor 状态。

### 3.8 订阅侧统一到同一读路径（`sub.py`）

今天 `RowSubscription` 有一个 per-tick 的 ContextVar 缓存（`sub.py:50`）：tick 开始清空，
`_prefetch_rows` 按表一次 `get_many` 预读本 tick 所有变更行填进去，交叉订阅共用。它只活一个
tick、只在一个连接内。进程缓存把这两个作用都覆盖了，而且更强：

- **删除 `RowSubscription.__cache` / `reset_cache_` / `prefill_cache_`**。`_prefetch_rows`
  改为按表调 `row_reader.get_many(ref, ids, backend.servant)`，结果放进一个 tick 局部的
  `dict[channel, np.record | None]`（就是预读的输出，随 `get_updates` 这一轮结束丢弃），
  传给各 `get_updated`；不在里面的（tick 中途新建的行订阅）走 `row_reader.get`。
  同 tick 交叉订阅仍只读一次（预读按频道去重 + 进程缓存命中），SQL 后端（从不激活）退化为
  今天的"按表一次 get_many"。
- `IndexSubscription.get_updated` 里进入范围的行（`inserts`）与 `TableSubscription` 的
  变更行同样走 `row_reader.get_many`（表订阅的行没有行频道、不激活，等于今天的 `get_many`）。
- `decode_row_` 改收 `np.record`：`struct_to_dict` → RLS 判定 → 去 `_version`。缓存里的是
  record，推送前才转 dict，和今天 `RowFormat.TYPED_DICT` 的解码成本相同。
- **效果**：本 worker commit 写穿的行，推送时缓存命中，0 次读（玩家自己的改动推回自己是最常见
  的推送）；别的 worker 改的行，本 worker 上 N 个连接订着同一行 → 第一个刷新的填充，其余命中，
  每 worker 每次变更 1 次读而不是 N 次（通知风暴场景的读放大就此消失）；推送的行不早于最近
  通知（floor + 权威读），修掉 §1 说的随机副本陈旧推送。客户端看到的行与服务端事务读到的
  永远是同一份。
- `subscribe_get` 改为**先订后读**：`id` 点查先订阅（激活，floor 未知）再 `row_reader.get`
  （首次权威读，同时把缓存填好）；非 `id` 先 ZRANGE 拿 id 再订阅再 `row_reader.get`。
  每次订阅至多多 1 次往返，换来初始推送也是权威的，并修掉"先读后订"在读与订之间漏写的
  既有窗口。权限不过 / 行不存在则退订。`subscribe_range` 的初始行照旧（先 ZRANGE 取行再订
  行频道，读在激活前，不填充；下次通知时填）。
- `row_cache=False` 时 `row_reader` 全部退化为 `fallback` 读，订阅侧行为与今天相同。

## 4. 正确性与并发分析

记 W 为某行的一次写（新版本 u），`tn` 为本进程处理完 W 通知的时刻。

| 场景 | 结果 |
|---|---|
| 副本读回 `_version ≥ floor` | 已应用到不早于最近通知的状态，入缓存；若另有更新的写 W' 未通知，W' 通知到达时版本更高 → 逐出 |
| 副本读回 `_version < floor`（滞后） | 不入缓存，改权威读；事务用权威值 |
| 读发出后、填充前处理了 W 通知 | floor 抬到 u；填充时 `row._version < u` → 作废（副本读）；权威读回的版本 ≥ u 则照常 |
| floor 未知（刚激活 / 节点恢复后） | 首次走权威读；此前发生的写全部包含在内，之后的写必有通知 |
| 本进程 commit 成功 | 写穿新行，floor = 新版本；自己的通知版本相等 → 不逐出；同一连接下次读命中 |
| 别的 worker commit | 通知 → 逐出 + floor；下次读副本，滞后则识破 |
| commit 因该行 VER 失败 | 逐出本事务全部行，冲突行 floor = master 当前版本 → 重试必权威读 |
| 同事务重复读同一行 | idmap 命中，缓存不参与（现状） |
| 最后一个本地订阅者退订 | `_release` 同步失活 + 逐出，早于 UNSUBSCRIBE；此后的读不填充 |
| 退订未 ack 时又有人订（搭车） | 新登记要等 ack 后才在 `add` 末尾激活（新代次）；旧 lease 作废 |
| pubsub 节点失效 | `on_reset` 全部失活清空；`on_restored` 重新激活，floor 未知 → 权威读 |
| `direct_set`（不动 `_version`，只能用于易失组件） | 不通知；易失组件不缓存，别的事务 VER 不受影响，无冲突 |
| 行被删除 | 通知 0 → 逐出 + floor 已删除；副本仍读到旧行 → 权威读 → None → 事务看到不存在 |
| 删除后同 id 重插 | 新一代版本从 1 重来，滞后副本上删除前的旧行版本更高、floor 挡不住 → 见过删除通知的频道本次激活周期内只走权威读，退订再订才重新信副本 |
| `only_master` 事务 | 全程绕过 |
| 用户就地改返回的行 | 返回的是副本，缓存不受影响 |
| 通知乱序 / 多 hub 重复 | `max` 与逐出幂等；最坏 floor 暂时偏高，多一次权威读 |

- **不会无限重试**：RACE 后冲突行 floor 已是 master 当前版本，重试直接权威读，不再受副本
  滞后影响；与今天相比少掉"副本滞后 → 再 RACE"的循环。
- **只读事务的陈旧上界**：通知延迟。比今天订阅推送（随机副本、可能一直旧到下次写）更强。
- **master 负载**：权威读只在激活后首次、删除 / 节点恢复之后、以及副本确实滞后时
  发生；稳态命中与副本读之间不碰 master。写穿把"读-改-写"型 RPC 的读彻底省掉，副本读也随之
  下降。
- **订阅推送的新鲜度**：推送的行来自缓存命中（不早于最近处理的通知，含本 worker 写穿）或
  floor 校验过的读；今天"随机副本读到旧值并一直旧到下次写"的情况不再出现。
- **内存**：≤ min(激活行数, `row_cache_max_rows`) × 行大小；失活即释放。

## 5. 用户可见变化与文档

- 默认开启。语义变化：只读 System 可能读到"通知尚未到达"的旧值（≤ 毫秒级），写 System 不受
  影响（VER）。`docs/zh/_index.md` "事务"节补一段"事务读的新鲜度"：命中条件（本 worker 有
  订阅的行）、兜底（VER）、强一致读用 `only_master`。
- `docs/zh/operations.md`：`row_cache` 开关；行通知改为 commit 主动发布、升级需所有写入方
  一起；代理必须把 `EVALSHA` 路由到主节点；`notify-keyspace-events` 只剩索引需要 `z`
  （HeTu 自动 `CONFIG SET` 的目标改为 `Kz`；已配成 `Kghz` 的旧实例不用动）；节点失效会清空
  缓存。`docs/zh/advanced.md:394/553` 与 `base.py` 的 `direct_set` docstring：易失组件不进
  缓存，`direct_set` 不发通知、不影响别的事务。
- `docs/zh/concepts.md` "读写分离"段一句：worker 缓存订阅中的行，事务读命中免往返。
- `hetu/llms.txt` 同步；`docs/api/*` 重新生成（`Backend.row_cache`、`Session.row_cache`、
  `BackendClient.get_authoritative`）。en 文档由 `scripts/translate_new_content.py` 同步。

## 6. 测试计划

`tests/test_backend_rowcache.py`（新，纯单元，无后端）

- `fill` 的三种判定（floor 未知 / 已知 / 已删除 × 权威 / 副本）；版本低于 floor 拒绝；
  `notify` 的逐出与 floor 单调、0 哨兵、乱序幂等；`put_committed` 只对凭据代次仍当前的行生效且
  自己的通知不逐出；失活再激活后旧 lease 作废；同频道两个 owner 一个失活不影响另一个；`get`
  返回副本；`clear`；stats。

`tests/test_backend_client.py`

- `test_redis_commit_payload`：`publishes` 含每个 insert / update 行 `[row_key, pack(ver)]`、
  delete 行 `[row_key, pack(0)]`；现有 `:775-818` 行频道订阅用例改为断言 commit 后收到通知、
  MQClient 侧 payload 仍为 None。`direct_set` 后**没有**通知（各后端一致）。
- `get_authoritative` / `get_many_authoritative`：读到的行与 `get` 一致，cluster 下走主节点。

`tests/test_backend_sub.py::test_redis_notify_configuration`：期望标志改为 `Kz`（测试容器是
新起的，不会带旧的 `gh`）。

`tests/test_backend_pubsub_hub.py`（`@use_redis_family_backend_only`，含 cluster）

- `subscribe_get` 后 hub 已激活该行频道，floor 为未知；索引 / 表级频道不激活。
- 另一 session 改该行 → `notify` 后缓存逐出、floor = 新版本。
- `unsubscribe` 最后一个订阅者 → 失活；两个 broker 订同一行，退一个不失活。
- 模拟节点失效（直接调 `_on_node_listener_done`）→ 清空、`resubscribe_all` 后重新激活。

`tests/test_backend_session_basic.py`（各后端参数化；SQL 后端断言"恒 miss、行为不变"）

- `test_row_cache_hit_after_subscribe`：订阅行 → 事务 A `get(id)`（miss：权威读 1 次，
  `patch.object` 计数 `get_authoritative` 与各客户端 `get`）→ 事务 B `get(id)` 0 次远程读；
  `only_master=True` 的事务不命中也不填充。
- `test_row_cache_write_through`：订阅行 → 事务 A `get + update + commit` → 事务 B `get`
  0 次远程读且读到新值；再来一轮 A 的 commit 后 B 仍命中（自己的通知不逐出）。
- `test_row_cache_range_partial_hit`：订阅 10 行中的 7 行 → `range` 只对 3 行 `get_many`，
  顺序与索引一致；本事务修改 / 删除可见性回归。
- `test_row_cache_stale_replica_goes_authoritative`：用 `fill` 造一个 floor 已知的状态，
  `patch` servant `get` 返回低版本行 → 读路径改调 `get_authoritative`，事务拿到新值、缓存
  存的是权威行。
- `test_row_cache_race_bumps_floor`：并发改行后 commit RACE → 冲突行 floor = master 版本、
  缓存逐出；`Session.retry` 重试时走权威读并成功。
- `test_row_cache_delete`：删除通知后副本旧行不入缓存、事务读到 None。
- `test_row_cache_skips_volatile`：订阅一个易失组件的行 → hub 不激活、事务读每次远程。
- `test_row_cache_returns_copy`、`test_row_cache_disabled`（`row_cache: False` 时无缓存、
  但行通知仍发）。

`tests/test_backend_sub.py`（各后端参数化）

- `test_sub_push_uses_row_cache`：两个 broker 订同一行 → 另一 session 改行 → 两个
  `get_updates` 合计只 1 次远程读（计数各客户端 `get` / `get_many` / `get_authoritative`），
  推送内容一致且 `_version` 为新值；本进程 session 改行（写穿）→ 推送 0 次读。
- `test_sub_push_stale_replica`：`patch` servant 读返回低版本行 → 推送走权威读、内容为新值。
- `test_subscribe_get_subscribes_before_read`：`subscribe_get` 后缓存已有该行（首次权威读），
  权限不过 / 行不存在时 hub 无残留订阅。
- 现有交叉订阅 / IndexSubscription 进出范围 / TableSubscription 用例回归；SQL 后端行为不变。

`tests/test_endpoint_connection.py` / `test_websocket.py`：回归——alive 检查不走 repo；
`test_endpoint_connection.py:273` 那段"Redis 心跳打到行频道"的顺带断言删掉（`direct_set`
不再通知，各后端一致），其余行为不变。`test_backend_sub.py`：行频道名变化不影响断言。

RTT 复测：`rtt_probe.py` 加"先订阅再事务"与"同一连接连续两次 get+update"两组，核对 §1 表；
benchmark `ya` 脚本跑一次 CPS 对照（订阅 + RPC 混合负载）。

## 7. 取舍与边界（YAGNI）

- **不做副本亲和（读订阅所在节点）**：第 1 版方案，代理模式下做不到，且要为集群写按节点的
  reader；版本下限 + 权威读一套机制覆盖所有拓扑，弃用。
- **索引不缓存**：ZRANGE 结果的失效需要解析索引成员变化，收益不如行缓存。非主键 `get` /
  `range` 仍付 1 次索引往返。todo 第 10 条（只读 Lua 合并索引查询与行读）与本 spec 正交。
- **不缓存"不存在"（tombstone）**：negative cache 留在 idmap（事务内）；删除后的订阅行只在
  再次通知时才会被读，同 tick 交叉订阅对同一删除行的重复权威读由 tick 局部的预读结果去重。
- **`subscribe_range` 的初始行不填充**：范围订阅先 ZRANGE 取行再订各行频道，初始行读在激活
  前；改成"订完再读一遍"每次范围订阅多 1 次往返，收益只是首个通知前的命中，不做。
- **权威读一律走 EVALSHA**，不按拓扑区分：只有代理拓扑需要它（§1），但客户端识别不了代理，
  加一个 `proxy: true` 配置又是配错就悄悄失去新鲜度的那种开关，而 EVALSHA 与 HGETALL 在真
  master 上成本相同。`only_master` 事务与 `ConnectionAliveChecker.kicked` 今天在代理下同样
  不权威，可后续改用 `get_authoritative`，本 spec 不动。
- **通知 payload 只带版本不带行内容**：带内容能省掉别的 worker 的重读，但把每次写的行体广播
  到所有订阅 worker，pubsub 带宽随订阅方数放大；版本号 1 个整数足以判定新鲜度。
- **不给 System 暴露"绕过缓存"开关**：用 `only_master`。
- **跨请求合批（`batch.py` 另一半）彻底放弃**：事务内合批已由 `_hgetall_many` / `range` /
  `get_many` 完成；跨请求合批以延迟换单节点吞吐，读写分离下副本并行更划算；本缓存把最热的
  读直接消掉。

## 8. 主要改动文件清单

- 新增 `hetu/data/backend/rowcache.py`（`Lease`、`RowCache`、`CachedRowReader`、哨兵；
  docstring 留档 `batch.py` 结论）、`hetu/data/backend/redis/get_rows.lua`。
- `hetu/data/backend/__init__.py`：`Backend.row_cache` / `row_reader`（开关、上限、attach）。
- `hetu/data/backend/base.py`：`BackendClient.row_cache`、`get_authoritative` /
  `get_many_authoritative`（默认退化）。
- `hetu/data/backend/session.py`：`row_reader`（`only_master` 绕过）。
- `hetu/data/backend/repo.py`：`_fetch_by_id` / `range` 改走 `row_reader`。
- `hetu/data/sub.py`：删 per-tick ContextVar 缓存；`_prefetch_rows` / `get_updated` /
  `TableSubscription` 走 `row_reader`；`decode_row_` 收 record；`subscribe_get` 先订后读。
- `hetu/data/backend/redis/client.py`：`row_channel` 改名；`configure_servant` 目标 `Kz`；
  commit `publishes` 加行通知、成功
  写穿、RACE 逐出与 floor；`direct_set` docstring 改"不通知"；`is_cacheable_channel`；
  权威读脚本加载与实现；`get_mq_client`
  传 `row_cache`；删六处 `_batched_aio` 注释。
- `hetu/data/backend/redis/mq.py`：`PubSubHub` 钩子、`_active`、payload 分流。
- `hetu/data/backend/redis/pubsub.py`：`on_reset` / `on_restored`。
- `hetu/data/backend/idmap.py`：删 `get_clean_row_keys`；加"取 INSERT / UPDATE 行的类型化
  整行"访问器供写穿用。
- 删除 `hetu/data/backend/redis/batch.py`、`tests/test_backend_redis_batch.py`。
- `hetu/CONFIG_TEMPLATE.yml`：`row_cache`。
- 测试：§6。文档：§5。
