# SQLite 开发后端（取代通用 SQL 后端）— 设计稿

- 日期：2026-09-26
- 状态：设计已对齐（§3），待审阅；落地前另写实施计划
- 分支：`feat/sqlite-backend`（从 dev `30cb1ca9` 拉，尚未创建）。文中行号均基于 `30cb1ca9`
- 影响范围：删除 `hetu/data/backend/sql/`；新增 `hetu/data/backend/sqlite/` 与共享模块
  `hetu/data/backend/redis_model.py`（名字可再议）；`redis/client.py`、`redis/maint.py`、`redis/mq.py`
  （纯搬迁）；`base.py`（工厂、`__init_subclass__`、direct_set 契约）；`backend/__init__.py`；
  `worker_keeper.py`；CLI（`start` / `init`）；`hetu/testing`；`CONFIG_TEMPLATE.yml`；`pyproject.toml`；
  测试夹具与用例；文档。
- **不改**：客户端协议、System / Session / Repo 的公开 API、订阅语义、Redis 后端的行为与性能。

## 1. 背景与目标

### 1.1 问题

`hetu/data/backend/sql/` 用 SQLAlchemy Core 同时支持 PostgreSQL / MariaDB / SQLite，比较规则交给各
数据库，和 Redis 后端有一批行为偏差，目前用 strict xfail 标着（§2.2）。偏差归结为两类：

1. **索引比较**：用数据库原生的列类型与 collation 比较，而不是 Redis 的"可排序字节"：float32 按
   double 比，整数边界向 0 截断，±inf 钳成闭区间的极值，同一个值内按 id 数值排序……
2. **提交不原子**：校验（纯读行的版本、区间重跑、unique）是不加锁的 SELECT；SQLite 的驱动更是在第一
   条写语句前才发 BEGIN，校验根本不在写事务里（`sql/client.py:1273`），交错的提交都能通过。

PG / MariaDB 另有 collation、死锁、通知表 id 不按提交序等各自的问题。通用 SQL 后端本来只推荐开发用
（`CONFIG_TEMPLATE.yml:142`），实际用到的几乎只有 SQLite（`hetu init` 默认、`hetu.testing.Sandbox`、
教程），却要为三种数据库付兼容成本；而那些偏差恰恰让开发期暴露不了生产（Redis）上的问题。

### 1.2 目标

- 删除通用 SQL 后端，新写一个只支持 SQLite 的后端 `hetu/data/backend/sqlite/`，专门给开发和随手测试用。
- **行为尽可能和 Redis 一致**；原生难以一致的地方，用低性能的办法模拟 Redis。验收标准：同一套后端
  参数化测试在 redis 与 sqlite 上结果相同；§2.2 里 SQLite 的 9 条 xfail 全部转绿、去掉标记。
- 不考虑性能：每个客户端的 SQLite 操作单线程串行执行；仍支持多个进程共用一个库文件（§2.4）。
- 测试矩阵只剩 `redis` / `valkey` / `redis_cluster` / `sqlite`，删除 PG / MariaDB 的夹具、docker 服务、
  专属用例与 xfail。
- 移出 SQLAlchemy 与全部 SQL 驱动依赖，只用标准库 `sqlite3`（§3.3）。

### 1.3 非目标

- 生产用的 SQL 后端。将来可能另做 PostgreSQL 专用后端，本设计不为它预留抽象，只在 direct_set 的通知
  契约上替它考虑（§3.2）。
- 性能。
- 模拟副本复制延迟（生产上读可能落后于 master，SQLite 读总是最新的，见 §8）。
- 旧 SQL 后端库文件的数据迁移（开发数据，删掉重建）。
- `:memory:` 库（跨客户端、跨进程看不到同一份数据，Sandbox 已经要求用文件）。

## 2. 已确认的事实（均已核对）

### 2.1 Redis 后端：要对齐的目标

- **key 布局**（`redis/client.py:132-201`）：行 `{实例}:{组件}:{CLU<簇>}:id:<id>`，索引
  `{实例}:{组件}:{CLU<簇>}:index:<字段>`，meta `{实例}:{组件}:meta`。
- **行 = hash**：每个字段存 `str(值)`（`row.item()` 之后再 `str`，float32 存的是展宽成 double 的文本；
  bytes 字段存原字节，`idmap.py:55-61`），`_version` 也是字符串（`redis/client.py:1000-1007`）。读回由
  `rows_decode_` 按 dtype 解析（`:497-520`），RAW 一律 utf-8 容错解码成 str。所以 NaN、±inf、uint64
  全范围、含 `\x00` 的字符串都能原样往返。
- **索引 = zset**：score 恒 0，member = `to_sortable_bytes(值) + b"\x00" + ascii(id)`（`:1009-1034`），按
  `ZRANGE BYLEX` 查，边界由 `range_normalize_` 计算（`:647-705`，整数区间走 `normalize_int_bounds_`）。
  字符串按 UTF-8 字节序；同一个值内按 id 的十进制字符串排序。
- **提交 = 一次 `commit_v2.lua`**（全文 118 行）：checks（`VER` / `NX` / `EX` / `UNIQ` / `CNT`，竞态类在前、
  确定性类在后，首个失败即返回 `RACE: …` / `UNIQUE: …`）→ pushes（`HSET` / `ZADD` / `ZREM` / `DEL`）→
  `PUBLISH` 表频道与值频道，整段原子执行。Python 侧的 payload 组装在 `commit`（`redis/client.py:947-1161`），
  range 观察变成 `CNT` 在 `_range_checks`（`:904-944`）。
- **通知**：行频道、整个索引的频道是 keyspace 通知 `__keyspace@{db}__:{key}`，由副本应用写入时自己
  产生（servant 的 `notify-keyspace-events` 含 `Kghz`，`:357-407`）。HSET 必发；ZADD / ZREM 只在 zset
  真变了时发；DEL 只在 key 存在时发。表频道（`table_sub`）与值频道（`point_sub` 的"进入"）由 commit 里的
  PUBLISH 发。订阅 ack 之后发出的都收得到；pubsub 断线期间丢的，由 `PubSubHub._on_resubscribed` 补发
  （表频道带 `RESYNC`，`redis/mq.py`）。
- **range 读分两步**：先查索引拿 id，再另一次调用取行（`repo.py:390-475`），两步之间行可能被删改，
  这是 `InconsistentRangeRead` 的来源之一（另一个是索引残留）。
- **同一个客户端只能在一个事件循环里用**（`aio` 的断言，`redis/client.py:118-129`）。
- **direct_set = HSET**（`:1179-1208`）：不改 `_version`；缺行时建出缺 id 的残缺行（记录在案、待定的
  bug）；行频道就是行 key 的 keyspace 通知，所以它会顺带触发行频道，但不触发表频道。
- **维护**（`redis/maint.py`）：`create_table` 只写 meta，行和索引的 key 写入时才出现，所以不建表也能
  直接提交；`delete_row` 只删行 key、不动索引，留下的残留由 `rebuild_index` 清；`rebuild_index` 按行数据
  重算 member，先建在临时 key 上，unique 冲突就报错，成功才 RENAME 覆盖（`:235-288`）；锁是 redis-py
  的 `Lock`（5 分钟过期）。

### 2.2 要转绿的 SQLite xfail（9 条）

| 用例 | 位置 | 现在 SQLite 为什么挂 |
|---|---|---|
| `test_float32_point_query` | `test_backend_index_semantics.py:107` | 存的是展宽成 double 的 float32，查询边界没先转成 float32 |
| `test_same_value_rows_ordered_by_id_string` | `:233` | 同值内按 id 数值排序（`sql/client.py:1103-1110`） |
| `test_int_index_bounds_beyond_sql_column_range` | `:285` | 边界超出 int64，驱动 OverflowError |
| `test_int_index_fractional_bounds` | `:307` | 小数边界向 0 截断（`sql/client.py:722-723`） |
| `test_int_index_infinite_bounds` | `:329` | ±inf 钳成 dtype 极值且是闭区间（`:749-758`） |
| `test_range_phantom_interleaved_commits` | `test_backend_session_race.py:754` | 提交不原子（§1.1） |
| `test_write_skew_interleaved_commits` | `:792` | 同上 |
| `test_unique_blind_insert_interleaved_commits` | `:834` | 同上；约束错误还被当成竞态 |
| `test_float_special_values[nan]` | `test_backend_dtypes.py:300` | NaN 绑定成 NULL，撞上 NOT NULL 约束被当成竞态 |

随 PG / MariaDB 一起消失的 xfail：索引语义里的大小写、尾随空格、字节序（collation）几条及其余各条的
PG / MariaDB 参数，`test_opposite_order_updates_interleaved_commits`（死锁），`test_float_special_values`
（MariaDB），`test_str_with_nul_roundtrip`（PG），`test_maintenance_range_infinite_bounds`（MariaDB），
`test_backend_sql.py` 里通知表 id 不按提交序的 2 条。

### 2.3 没有 xfail，但也和 Redis 不一样

- uint64 超过 `2**63-1` 写入被拒（`sql/client.py:1209-1234`），Redis 能存。
- NaN 存不了，`test_untouched_nan_row_stays_clean_read` 因此只给 Redis 跑。
- 约束错误一律按竞态重试（`:1350-1357`、`:1386-1393`），非 unique 的约束错误会一直重试。
- 行频道名是裸 key（Redis 带 `__keyspace@0__:` 前缀）；insert / delete 给全部索引的频道发通知，不管
  zset 语义上变没变。
- direct_set 不发通知（`test_endpoint_connection.py:272-274` 为此按后端分支）。
- 缺表时建表、整个提交重试一次（`:1442-1453`）；索引由数据库维护，`delete_row` 不会留下索引残留，
  `rebuild_index` 只查 unique，Redis 专属的重建索引用例因此跑不了 SQL。
- 可以跨事件循环使用。

### 2.4 进程、线程与 SQLite 本身

- 模板默认 `WORKER_NUM: 4`（`CONFIG_TEMPLATE.yml:46`），每个 worker 一个进程、共用一个库文件；headless
  （`hetu/headless.py`）、`hetu upgrade` 也会另起进程直连。所以通知必须能跨进程送达。
- 旧后端经 aiosqlite 每条连接一个线程，事务跨 await：loop 关闭时遗弃的写事务会一直占着写锁
  （`server/websocket.py:314` 的注释就是那次排查的结论）。
- `Backend` 在 servants 为空时把 master 地址也当 servant（`backend/__init__.py:66-68`），所以一个进程里
  对同一个文件有 master、servant 两个客户端。
- Python 3.14 自带 SQLite 3.53。BLOB 比较是 memcmp 再比长度，与 Redis zset 的 lex 比较一致（已实测
  `b"a" < b"a\x00" < b"a\x00\xff"`）。SQLite 的标识符（表名、列名）不区分大小写，Redis 的 key 区分。
- `hetu init` 只替换 type 和 master（`cli/init.py:297-313`），生成的 SQLite 配置里也带着 Redis 专用的
  `raw_clustering` / `max_connections` / `pool_timeout`。

## 3. 已定事项（2026-09-26 与用户对齐）

| # | 事项 | 决定 |
|---|---|---|
| 1 | 行数据落盘形态 | 每个（实例, 组件, 簇）一张行表，每字段一列，值存 Redis 同款文本，用 DB 工具能直接看；索引存模拟 zset 的 BLOB 表（§4.3） |
| 2 | 代码组织 | Redis 客户端里与 redis-py 无关的纯逻辑抽成共享模块，两个后端共用；SQLite 只实现存储原语与 `commit_v2.lua` 的 Python 版（§4.2） |
| 3 | direct_set 通知契约 | "不保证触发订阅通知"，SQLite 不发（§3.2） |
| 4 | 旧配置 | 直接改名：只认 `type: SQLite`，`type: SQL` 报错提示改名；旧后端建的库文件报错，提示删掉重建（§4.10） |
| 5 | SQLAlchemy | 移出，只用标准库 `sqlite3`（§3.3） |
| 6 | Worker ID | 继续用 `FixedWorkerKeeper`（§4.9） |

### 3.1 总体思路：在 SQLite 上模拟 Redis 的数据模型

不再"用 SQL 表达 HeTu 的语义"，而是把 HeTu 用到的那一小撮 Redis 语义原样搬到 SQLite 上：hash（行）、
lex zset（索引）、带过期的字符串（锁）、commit 脚本、keyspace 通知与 PUBLISH。索引 member、区间边界、
提交 payload、返回的错误串都与 Redis 字节相同，偏差按构造消失，而不是逐条去修。Redis 客户端里与
redis-py 无关的逻辑两边共用，以后 Redis 那边改了语义，SQLite 自动跟上。

### 3.2 direct_set 的通知契约

现状：

- Redis：见 §2.1，顺带触发行频道，不触发表频道。旧 SQL 后端：都不触发。
- 订阅侧按内容指纹决定推不推，不看 `_version`（`sub.py:82-91`）：没收到通知时，direct_set 的改动会在
  该行下一次被通知时一起推给客户端。
- 生产代码里只有两处调用：连接心跳写 `last_active`（`endpoint/connection.py:246-248`）、雪花水位
  （`snowflake_timestamp.py:176`），都不需要通知。踢线检测订的是 `owner` 的值频道
  （`server/websocket.py:180`），direct_set 改不了索引字段，碰不到它。
- 文档已经写着"不保证数据一致性"（`docs/api/system.md:773`），headless 页还写着"`direct_set` 不保证
  通知一致，请勿使用"（`docs/api/headless.md:72`）。

各后端实现"发"与"不发"的代价：

| 后端 | 发通知 | 不发通知 |
|---|---|---|
| Redis（现状） | 免费：keyspace 天然触发 | 做不到：要么把 direct_set 的字段挪到另一个 key（热路径上每次读行多一次 HGETALL），要么行通知改 PUBLISH（违反"不用 PUBLISH"的架构规则） |
| 将来的 PG：从备库逻辑解码（CDC，和 Redis keyspace 同构，不占 master） | 免费 | 加一个"`_version` 没变就跳过"的过滤，便宜 |
| 将来的 PG：提交时写通知表 / NOTIFY | 每次心跳都在主库多写一次；NOTIFY 提交时还要拿全局锁 | 免费 |
| SQLite（本设计） | 心跳不停往通知表写行 | 免费 |

结论：唯一在所有实现上都零成本的契约是 **"direct_set 不保证触发订阅通知"**。

- 契约写进 `BackendClient.direct_set` 的 docstring 与 API 文档：direct_set 是维护类写入，不改
  `_version`、不参与乐观锁、不保证触发任何订阅通知。行订阅可能立刻收到，也可能等该行下一次事务写入时
  一起推；整表订阅收不到。需要订阅方及时看到的数据请走事务。
- Redis：代码不改，现在的行频道通知算附带效果。
- SQLite：不发。这是契约最严的一端，依赖它的代码在开发期就会暴露；心跳也不往通知表写。
- 将来的 PG：两种实现都满足契约。
- 缺行时的行为：Redis 的 HSET 会建出缺 id 的残缺行（修法是 Lua 里先 EXISTS 再 HSET，待定）。SQLite 先
  照 Redis 现状写出只有这几列的行；那个 bug 定了之后两边一起改。

### 3.3 移出 SQLAlchemy

新后端只剩几类固定的语句（行表 upsert 与查询、zset 区间、通知表、meta），用不着方言抽象；而事务必须
由我们自己控制（`BEGIN IMMEDIATE`、事务不跨调用）。pysqlite 默认"第一条写语句前才 BEGIN"的行为正是
§1.1 提交不原子的原因之一。所以直接用标准库 `sqlite3`，把 SQLAlchemy（连同 `[asyncio]` 带进来的
greenlet）、aiosqlite、asyncpg、aiomysql、pymysql、psycopg、psycopg-binary 从依赖里去掉。将来的 PG
后端用不用 SQLAlchemy 届时再定（生产后端多半直接用 asyncpg）。

## 4. 设计

### 4.1 结构

```
BackendClient（base.py）
 └─ RedisModelClient（新，redis_model.py：Redis 数据模型的纯逻辑，不 import redis-py）
     ├─ RedisBackendClient（redis/client.py：redis-py I/O、Lua、PubSubHub）
     └─ SQLiteBackendClient（sqlite/client.py：SQLite I/O、commit 执行器、SQLiteNotifyHub）
```

`hetu/data/backend/sqlite/`：

| 文件 | 内容 |
|---|---|
| `client.py` | `SQLiteBackendClient(RedisModelClient, alias="sqlite")`：配置解析、专用线程与连接、读路径、commit、direct_set |
| `store.py` | 存储原语：行表（hash）、zset 表、带过期的 kv、meta、通知表；建库与格式校验。只在客户端的专用线程里调用 |
| `commit.py` | `commit_v2.lua` 的 Python 版（§4.6） |
| `maint.py` | `SQLiteTableMaintenance`，逐项对应 `RedisTableMaintenance`（§4.8） |
| `mq.py` | `SQLiteNotifyHub` + `SQLiteMQClient`（§4.7） |

删除 `hetu/data/backend/sql/` 整个包。

### 4.2 共享模块 `redis_model.py`

从 `redis/client.py` 搬过去、两个后端共用的，都是不做 I/O 的纯逻辑：

- key 与频道命名：`table_prefix` / `cluster_prefix` / `row_key` / `index_key` / `value_channel_`，
  `index_channel` / `index_value_channel` / `row_channel` / `table_channel`（keyspace 前缀里的 db 号：
  Redis 取连接的 db，SQLite 固定 0）；
- 区间：`range_normalize_`、空区间判定，以及 `range_read_` 里由 member 生成 `RangeObservation`（含截断
  时收窄边界）的部分；
- 行解码：`row_decode_` / `rows_decode_`（入参是 redis-py 形状的 `dict[bytes, bytes]`）；
- 提交：payload 组装拆成 `build_commit_payload(idmap)`，加上 `_range_checks` 和"返回串 → 异常"的映射
  （`RACE` → `RaceCondition`，`UNIQUE` → `UniqueViolation`）；
- `direct_set` 的参数校验、索引 dtype 的 schema 检查（现在的 `_schema_checking_for_redis`）、
  `_get_referred_components`；
- 重建索引时"行字段值 → member"的计算：两边的 `do_rebuild_index_` 与 commit 用同一个函数。

约束：

- Redis 侧是纯搬迁：行为、Lua、payload 字节不变。带 I/O 的读方法（`get` / `get_many` / `range` 等）
  留在各自的客户端里，Redis 的读路径代码不动，也不多一层 await。
- `RedisBackendClient.index_key(...)` 这类类属性访问照旧可用（继承而来），测试不用跟着改。
- `BackendClient.__init_subclass__` 改成有 `alias` 才注册（`base.py:389-392`），中间类不注册。
- 两个客户端都提供 `commit_script_(keys, args) -> bytes`，参数与返回和 `lua_commit` 一致（Redis 的实现
  就是转调 `lua_commit`）。测试的碰头点、抓 payload 统一 patch 它。
- 搬迁单独一个提交，redis / valkey / redis_cluster 全绿之后再动 SQLite。

### 4.3 存储布局

库文件用 `PRAGMA application_id`（HeTu 的固定魔数）加 `PRAGMA user_version`（格式版本）标识（§4.10）。
内部表一律用 `__hetu_` 前缀：SQLite 表名不分大小写，单下划线会和旧后端的 `_Hetu_Notify` 撞名。

**行表（hash）**：每个 `cluster_prefix` 一张，表名就是它，例如 `"pytest:Item:{CLU1}"`：

```sql
CREATE TABLE "pytest:Item:{CLU1}" (
    "_hetu_key" INTEGER PRIMARY KEY,  -- 行 key 里的 id（…:id:<id>）
    "id", "owner", "name", "qty", "_version"  -- 不声明类型，照 Redis 存每个字段的字节
);
```

- 值是合法 UTF-8 就存 TEXT（DB 工具里直接可读），否则存 BLOB；读回一律转成 bytes，和 Redis 一样按
  字节还原，存储类型只影响显示。列不声明类型（无亲和性），SQLite 不会改写存进去的值。
- HGETALL = 该行非 NULL 的列（去掉 `_hetu_key`）。NULL 就是"hash 里没有这个字段"：direct_set 建出的
  残缺行、schema 改了但没迁移的旧行，读出来都和 Redis 一样缺字段（按 STRUCT 解码报 KeyError）。
- HSET = upsert（`INSERT … ON CONFLICT("_hetu_key") DO UPDATE`，只写给出的列），列不存在就
  `ALTER TABLE ADD COLUMN`（Redis 的 hash 字段不受限）。DEL = 删行；EXISTS = 行在不在。
- 表在第一次写入时建：commit 在同一个事务里先按 `TableReference` 把缺的行表建好（列序按组件定义），
  direct_set、`upsert_row` 同理；`create_table` 也会提前建空表，开服后在工具里就能看到。读不存在的表
  当空，和 Redis 读不存在的 key 一样，所以和 Redis 一样不需要先 `create_table` 就能提交。
- 大小写：建表时发现已有只差大小写的表名，或同一组件里有只差大小写的字段名，直接报错（§8）。

**索引（zset）**：一张全局表

```sql
CREATE TABLE "__hetu_zset" (
    "key" TEXT NOT NULL,     -- Redis 的 index key
    "member" BLOB NOT NULL,  -- 与 Redis 相同的 member 字节
    PRIMARY KEY ("key", "member")
) WITHOUT ROWID;
```

`ZRANGE key min max BYLEX [REV] LIMIT 0 n` 翻译成 `member` 上的区间查询：`[` 闭、`(` 开、`-` / `+`
无界，照 Redis 解析；`REV` 就 `ORDER BY member DESC`。`ZLEXCOUNT` 同理数行数。ZADD / ZREM 用
`INSERT OR IGNORE` / `DELETE` 实现，按影响的行数判断 zset 是否真的变了（决定发不发通知，§4.7）。

**其他内部表**：

- `__hetu_meta(instance, comp, json, version, cluster_id)`：对应 Redis 的 `{实例}:{组件}:meta`，可读。
- `__hetu_kv(key, value, expire_at)`：带过期的字符串，目前只有维护锁用；过期按访问时惰性判断。
- `__hetu_notify(id INTEGER PRIMARY KEY AUTOINCREMENT, channel, payload, created_at)`：通知（§4.7）。
  AUTOINCREMENT 保证清理之后 id 也不回退。

PRAGMA：`journal_mode=WAL`、`synchronous=NORMAL`、`busy_timeout`（沿用 5 秒）。

### 4.4 执行模型：每个客户端一条专用线程

- 每个 `SQLiteBackendClient` 一个 `ThreadPoolExecutor(max_workers=1)`，外加一条在这个线程里打开的
  `sqlite3` 连接（自动提交模式，事务由我们显式开）。
- 客户端的每个方法是交给这条线程的一个 job：async 方法用 `await loop.run_in_executor(...)`，同步的
  维护接口用 `submit(...).result()`。同一个客户端的所有操作严格串行。
- **事务不跨 job，更不跨 await**：commit、维护里的批量改写各自是一个 `BEGIN IMMEDIATE … COMMIT` 的
  job，其余是自动提交的单条语句。不会再出现"loop 关了、写事务还开着占着写锁"。
- 每次后端调用都是一次真正的 await，协程的交错点和 Redis 一样；`range` 的查索引和取行是两次调用，
  中间能插进别的提交，`InconsistentRangeRead` 的路径照样走得到。
- 照 Redis 的 `aio` 断言：同一个客户端只能在一个事件循环里用，开发期就能暴露跨 loop 使用。
- master / servant：同一个文件的两个客户端（两条线程、两条连接），读总是最新的；`is_synced` 恒为
  `(True, checkpoint)`。
- 跨进程：写事务用 `BEGIN IMMEDIATE`，一开始就拿写锁，其他进程的写者按 busy_timeout 等；WAL 下读不
  被写挡住。
- `close()`：停通知轮询 → 在线程里关连接 → 关线程池；幂等，之后再调用抛
  `ConnectionError("连接已关闭，已调用过close")`（同 Redis）。

### 4.5 读路径

- `get`：一个 job 读行（HGETALL 语义），整理成 `dict[bytes, bytes]` 交给共享解码。STRUCT / RAW /
  TYPED_DICT 的结果与 Redis 逐字节相同。
- `get_many` / `get_many_array_`：一个 job 按 id 分块 `IN` 查，读不到的给 None / 记进 missing。
- `range` / `range_read_`：共享的 `range_normalize_` 算出 BYLEX 两端，查 zset 表拿 member，用
  `rsplit(b"\x00", 1)` 取 id。`range_read_` 生成的 `RangeObservation`（bounds、members、截断时收窄的
  边界）与 Redis 完全相同，commit 时由共享的 `_range_checks` 变成 `CNT`。取行是另一次调用。

### 4.6 提交

1. 共享的 `build_commit_payload(idmap)` 得到 `[checks, pushes, deleted, table_pubs, value_chans]`，与
   Redis 完全相同（`_range_checks` 先在本地核对读取一致性，对不上直接抛 `InconsistentRangeRead`）。
2. `commit_script_(keys, [msgpack(payload)])` 交给线程执行 `commit.py`：
   - `BEGIN IMMEDIATE`，先把 payload 涉及的 `TableReference` 里缺的行表建好；
   - checks 逐条照 Lua 执行。payload 按 `raw=True` 解开，和 Lua 里一样全是字节串：
     - `VER` 比较 `_version` 的字节，缺行时返回串里写 `got:false`（Lua 的 `tostring(false)`）；
     - `NX` / `EX` 看行在不在；
     - `UNIQ` 先按 (索引, 值) 去重，再取 zset 里该值的第一个 member，其 id 在 `deleted` 里就不算冲突；
     - `CNT` 数观察区间里的 member。
     首个失败就 `ROLLBACK`，返回与 Lua 字节相同的串；
   - pushes：HSET / ZADD / ZREM / DEL；
   - 按 §4.7 记通知；
   - `COMMIT`，返回 `b"committed"`。
   payload 末尾缺元素时按空处理（同 Lua 的 nil）。
3. 共享的映射把返回串变成 `RaceCondition` / `UniqueViolation`。

`commit.py` 文件头写明"逐段对应 `commit_v2.lua`，改一边必须改另一边"，并由两个后端共跑的检查码用例
守住（§7.4）。库里没有 UNIQUE / NOT NULL 约束，也就没有"约束错误当竞态"的路径：unique 完全由 `UNIQ`
判定，和 Redis 一样。写锁等不到（另一个进程长时间占着）时抛 `sqlite3.OperationalError`，不转成
`RaceCondition`，因为重试解决不了卡住的写者。

### 4.7 通知

**频道名**与 Redis 一字不差：行 `__keyspace@0__:{row_key}`，整个索引 `__keyspace@0__:{index_key}`，
值频道 `{index_key}:{token}`，表频道 `{cluster_prefix}:table`。

**只有 commit 发**，按 Redis 产生通知的规则：

- 行频道：commit 里 HSET 过的行、DEL 掉的已有行；
- 整个索引的频道：commit 里 ZADD 真加进了新 member、或 ZREM 真删掉了 member 的索引；
- 表频道（payload 为 msgpack 的 row_id 列表）与值频道：照 Lua 的 PUBLISH；
- 一个提交里同一频道只记一条（Redis 可能发多条，MQ 按频道合并，订阅方分不出来）；
- direct_set（§3.2）和维护接口不发。维护在停服时跑，Redis 上它们触发的 keyspace 通知同样没人收。

通知行与数据在同一个写事务里提交；SQLite 只有一个写者，所以 id 顺序就是提交顺序。

**送达**：每个 servant 客户端一个 `SQLiteNotifyHub`（`Backend.get_mq_client` 走 servant），结构沿用现在
的 `SQLNotifyHub`：

- 订阅生效 = 先登记，再取当前 `max(id)` 作为该频道的水位，之后提交的都会送到（对应 Redis 的"SUBSCRIBE
  ack 之后 PUBLISH 的都收得到"）；
- 轮询 `id > 游标`，订阅的频道少时加 `channel IN (…)` 过滤，没人订阅时不轮询，间隔沿用 `interval / 2`；
- 分发走基类的 `push_pulled_`：表频道解 payload，其余为 None（同 `PubSubHub._on_message`）；
- 保留现在 hub 的并发加固：先登记再取水位、取水位不随调用方取消、轮询失败时指数退避并限流日志。

**清理与补发**：通知保留一段时间（沿用 1 小时，commit 时顺手按时间清，带抖动）。hub 如果发现游标之后
的通知已经被清掉（`min(id) > 游标 + 1`，或表已清空而序号超过了游标，比如进程卡住太久），就按 Redis
pubsub 断线重订的语义，给仍在订阅的频道补发一次（表频道带 `RESYNC`）。这段逻辑与
`PubSubHub._on_resubscribed` 相同，上移到基类两边共用。

### 4.8 维护接口 `SQLiteTableMaintenance`

逐项对应 `RedisTableMaintenance`，同步接口经客户端线程执行：

| 方法 | 行为（与 Redis 相同） |
|---|---|
| `get` / `range` / `get_all_row_id` | 读行表 / zset；表不存在当空 |
| `delete_row` | 只删行，不动索引（残留由 `rebuild_index` 清） |
| `upsert_row` | 先删再整行写（DEL + HSET） |
| `read_meta` / `do_update_meta_` | `__hetu_meta` |
| `get_lock` | `__hetu_kv` 上带过期的锁：5 分钟过期，阻塞轮询获取，只释放自己拿到的（同 redis-py `Lock`） |
| `do_create_table_` | 写 meta（已存在就断言失败）+ 建空行表 |
| `do_rename_table_` | `ALTER TABLE RENAME` + 改 zset 里该前缀的 key + 重写 meta |
| `do_drop_table_` | 删该组件所有簇的行表、zset 里该前缀的 key 和 meta；返回删掉的键数（行数 + 索引 key 数，对应 Redis 删掉的 key 数） |
| `do_rebuild_index_` | 用共享的 member 计算按行数据重算，unique 重复就报错；在一个事务里整体替换，失败时旧索引原样保留；表里没有行时清空索引 |

维护写入不发通知（§4.7）。

### 4.9 Worker ID

继续用 `FixedWorkerKeeper`（本机进程序号，`worker_keeper.py:47`），`create_worker_keeper` /
`live_worker_ids` 的判断不变（不是 Redis 就用 Fixed / 返回 `[]`）。

不模拟 Redis 租约的原因：租约的发号围栏在续约停顿 45 秒后拒绝发号，60 秒后续约失败让 worker 退出。
开发时断点一停就会触发，只会添乱；而它防的是多机部署下撞号，开发场景是单机。代价见 §8。

### 4.10 配置、CLI 与库文件

- `type: SQLite`（alias `sqlite`，大小写不敏感），`master: sqlite:///<路径>`：`sqlite:///./hetu.db` 相对
  当前目录，`sqlite:////abs/hetu.db` 是绝对路径，Windows 可写 `sqlite:///C:/…`。文件不存在就建。
- `type: SQL` 报错："SQL 后端已移除：SQLite 请把 type 改成 SQLite（地址不变），PostgreSQL / MariaDB
  不再支持"（在 `BackendClientFactory.create` 里特判）。
- `servants` 非空报错（SQLite 没有只读副本）：`Backend.__init__` 建客户端前调用客户端类的配置检查钩子
  （默认不做事），SQLite 在钩子里拒绝。Redis 专用的 `raw_clustering` / `max_connections` /
  `pool_timeout` 忽略，模板生成的配置里都带着；`master_weight` 没有意义，但不报错。
- 打开库文件时校验：
  - `application_id` 是 HeTu 的、`user_version` 也对 → 正常；
  - 空库（一张表都没有）→ 初始化，写入这两个标识；
  - 有旧后端的 `_HeTu_Component_Meta` 表 → 报错："`<路径>` 是旧 SQL 后端建的库文件，新的 SQLite 后端
    不兼容，开发数据请删掉重建或换个文件名"；
  - 其他情况（别的程序的库、格式版本不对）→ 报错，不往里写。
- `BackendClientFactory._BUILTIN_MODULES`：`sqlite` → `hetu.data.backend.sqlite`；`import hetu` 仍然不
  加载任何后端模块。
- `hetu start --db`：`infer_backend_type_from_db_url`（`cli/start.py:43`）遇到 `sqlite` 返回 `sqlite`，
  postgres / mysql / mariadb 报错说明已移除。
- `hetu init` 生成 `type: SQLite`（`cli/init.py:297-313`）。
- `hetu.testing.Sandbox` 改用 `{"type": "sqlite", …}`（`testing/__init__.py:211-213`）。
- `examples/chat/server/config.yml` 改成 `type: SQLite`。
- `CONFIG_TEMPLATE.yml`：BACKENDS 的注释改成 SQLite，删掉 aiosqlite 的日志条目（`:191-195`）。

## 5. 正确性与并发分析

- **提交原子**：commit 在一个 `BEGIN IMMEDIATE` 事务里完成全部校验与写入。同一客户端的 job 串行，跨
  客户端、跨进程由 SQLite 的写锁串行，等价于 Lua 在 Redis 单线程里原子执行：后一个提交的 checks 一定
  看得到前一个的写入。§2.2 的 3 条交错提交用例因此成立；`test_opposite_order_updates_interleaved_commits`
  在 SQLite 上是一个 `VER` 失败（没有行锁，也就没有死锁）。
- **WAL 快照**：`BEGIN IMMEDIATE` 先拿写锁再读，不会出现"用旧快照读、升级写锁时报 SQLITE_BUSY_SNAPSHOT"；
  事务里读到的就是最新的提交。
- **读的一致性**：读是自动提交的单条语句，和 Redis 的单条命令一样各自原子、彼此不保证一致。commit 的
  正确性不依赖读的一致性（有版本与区间校验兜底），同 Redis。
- **索引语义**：member 编码、BYLEX 边界、比较规则都和 Redis 相同，§2.2 的 5 条索引语义用例按构造成立；
  uint64 按 8 字节编码，不受 SQLite INTEGER 范围的限制。
- **NaN / inf / uint64**：值存 `str()` 文本，不经过 SQLite 的数值绑定，NaN 绑成 NULL 的问题不复存在。
- **通知不漏**：通知与数据同事务、单写者，所以 id 就是提交序；水位取订阅那一刻的 `max(id)`，订阅之后
  提交的都在水位之后；轮询按 id 顺序消费。唯一的丢失来源是清理，按 `RESYNC` 补发。
- **大小写**：只差大小写的表名 / 字段名在建表时报错，而不是静默共用一张表 / 一列（§8）。

## 6. 与 Redis 的对齐一览

| 行为 | Redis | 旧 SQL 后端 | 新 SQLite |
|---|---|---|---|
| 字符串索引比较 | UTF-8 字节序 | 各库 collation | 同 Redis |
| 同一个值内的顺序 | id 十进制字符串 | id 数值 | 同 Redis |
| float32 点查 | 边界先转 float32 | 按 double 比 | 同 Redis |
| 整数的小数 / ±inf / 越界边界 | 按数学含义收成闭区间 | 截断 / 钳位 / 驱动报错 | 同 Redis |
| uint64 > 2**63-1 | 能存能查 | 写入被拒 | 同 Redis |
| NaN | 原样往返 | 变 NULL，误判竞态 | 同 Redis |
| 提交 | Lua 原子 | 校验不在写事务里 | `BEGIN IMMEDIATE` 原子 |
| unique 判定 | `UNIQ` 检查 | SELECT + 约束兜底 | 同 Redis（无约束兜底） |
| 提交返回的错误串 | Lua 拼 | 自拼 | 与 Lua 字节相同 |
| 行 / 索引频道 | keyspace 通知 | 通知表，名字不同 | 模拟 keyspace，名字相同 |
| 索引频道何时发 | zset 真变了才发 | 按涉及的索引 | 同 Redis |
| 通知送达 | ack 之后都到；断线补 RESYNC | PG / MariaDB 可能漏 | 不漏；游标过旧时补 RESYNC |
| direct_set 的通知 | 顺带触发行频道 | 不发 | 不发（契约：不保证） |
| direct_set 缺行 | 建残缺行（待定 bug） | 静默不写 | 同 Redis |
| 不建表直接提交 | 可以 | 缺表时建表重试 | 可以 |
| `delete_row` 之后的索引 | 留下残留 | 库自动维护 | 同 Redis |
| `rebuild_index` | 按行重建、原子替换 | 只查 unique | 同 Redis |
| 跨事件循环使用 | 断言失败 | 允许 | 同 Redis |
| 副本延迟 | 有 | 无 | 无（非目标） |
| Worker ID | 租约 | 本机进程序号 | 本机进程序号 |

## 7. 测试计划

### 7.1 夹具与矩阵

- `tests/fixtures/backends.py`：删掉 `mod_postgres_backend`、`mod_mariadb_backend` 及各处分支；
  `SQL_BACKENDS` 改为 `SQLITE_BACKENDS = ["sqlite"]`；`mod_sqlite_backend` 的配置改成 `type: sqlite`；
  `xfail_on_backends` 在 xfail 删完后没人用了，一并删掉。
- `tests/fixtures/sql_service.py` 改名为 `sqlite_service.py`，只留 `ses_sqlite_service`；`conftest.py`
  跟着改 import。
- `HETU_TEST_BACKENDS` 取值改为 `redis` / `valkey` / `redis_cluster` / `sqlite`，`AGENTS.md` 同步，并删掉
  Postgres / MariaDB 容器的说明。

### 7.2 删除

- `tests/test_backend_sql.py` 整个文件（SQLAlchemy、通知表结构、aiomysql 垫片、PG / MariaDB 的通知顺序
  xfail）。其中 hub 的并发单测与维护锁用例按新实现改写，迁到 `test_backend_sqlite.py`（§7.5）。
- `test_backend_session_basic.py::test_unique_ci_collation_multi_candidate_is_deterministic` 和
  `test_backend_session_race.py::test_get_hit_ci_collation_commits`（用 NOCASE 模拟 MariaDB collation）。
- `test_backend_dtypes.py::test_sql_rejects_uint64_above_bigint` 和 `::test_sql_rows_decode_roundtrip`。
- `test_safelogging.py::test_config_template_quiets_aiosqlite_logger`。
- 各文件里 PG / MariaDB 专属的 xfail 随用例参数一起消失（§2.2 末段）。

### 7.3 去掉 xfail 标记、必须转绿

§2.2 的 9 条。同时把 `test_backend_session_race.py` 的 `_meet_between_check_and_write` /
`_meet_before_second_update` 改成两个后端都 patch `commit_script_`（碰头点在原子提交之前，不再 patch
`AsyncConnection.execute`），去掉 `import sqlalchemy`。

### 7.4 从 Redis 专属扩到两个后端

| 用例 | 需要的改动 |
|---|---|
| `test_backend_client.py::test_redis_lua_check_codes`、`::test_redis_lua_range_count_check` | 改调 `commit_script_`，名字去掉 redis；这两条就是 `commit.py` 与 Lua 一致的守门用例 |
| `test_backend_client.py::test_redis_range_check_payload` | spy 改成 `commit_script_` |
| `test_backend_dtypes.py::test_uint64_above_int64_max` | 无 |
| `test_backend_dtypes.py::test_rebuild_index_matches_commit` | 用测试 helper 读原始索引 member（Redis：`io.zrange`；SQLite：查 zset 表） |
| `test_backend_session_basic.py::test_untouched_nan_row_stays_clean_read` | 无 |
| `test_backend_session_basic.py::test_redis_empty_index` | 同上的 helper |
| `test_backend_session_race.py::test_orphan_index_member_raises_inconsistent_range_read` | 无 |
| `test_migration.py` 的 `test_rebuild_index_removes_orphans` / `_failure_keeps_old_index` / `_without_snowflake` / `test_manager_rebuild_index_all` | helper 读 member、直接改行字段 |
| `test_common.py::test_snowflake_timestamp_keeper_legacy_partial_row` | 无（SQLite 的 direct_set 缺行时同样建残缺行） |

仍然只给 Redis 跑的：PUBLISH 预算（`test_arch_publish`）、pubsub / 原生集群 / 连接池、keyspace 配置、
租约（`test_common` 里 keeper 的几条、`test_live_worker_ids_sees_unexpired_leases`）、master 读预算
（要有副本）、`test_row_subscription_catches_up_after_pubsub_resubscribe`（SQLite 对应的是 §7.5 的补发
用例）。

### 7.5 新增 `tests/test_backend_sqlite.py`

- 配置：DSN 的相对 / 绝对 / Windows 路径；`type: SQL` 与 postgres / mysql 地址的报错文案；servants 非空
  报错；Redis 专用配置项被忽略。
- 库文件：新库写入 `application_id` / `user_version`；遇到旧 SQL 后端的库、别的程序的库时报错，并且
  不写入。
- 行表可读：提交后物理表每字段一列、值是可读文本；bytes 字段是非法 UTF-8 时存 BLOB，往返逐字节一致；
  只差大小写的表名 / 字段名报错。
- zset 语义：`[` / `(` / `-` / `+`、REV、LIMIT、ZLEXCOUNT 的结果符合 Redis 的规则（成员按 memcmp 排序）。
  可选：同一组操作在 Redis 与 SQLite 上各跑一遍比对结果。
- 通知：频道名与 Redis 相同；commit 按 §4.7 的规则发（ZADD 已存在的 member 不发）；direct_set 与维护
  接口不发；hub 的并发单测（水位、取消、退避与日志限流，从 `test_backend_sql.py` 改写）；清理越过游标
  时补发 `RESYNC`。
- 执行模型：跨事件循环使用会断言失败；`close` 幂等、之后调用抛 `ConnectionError`；维护锁的阻塞与过期。
- 跨进程：两个进程对同一个文件交错提交同一个 unique 值，恰好一个成功（`BEGIN IMMEDIATE` 的烟雾测试）。

### 7.6 需要跟着改的

- `test_common.py::test_worker_keeper_factory_picks_by_backend`（类型名），以及
  `test_snowflake_timestamp_keeper` 里讲 SQL 语义的注释。
- `test_endpoint_connection.py::test_owner_value_channel_ignores_own_heartbeat`：Redis 分支的行频道断言
  保留（它验证值频道不会被行频道的附带通知误触发），注释改成"附带效果，契约不保证"。
- `test_headless_process.py`：懒加载检查从 `sqlalchemy` 改为 `hetu.data.backend.sqlite`。
- `test_safelogging.py`：线程用例里 `aiosqlite` 这个名字换成中性的。
- `test_cli_init.py` 断言 `type: SQLite`；`test_migration.py:775` 的配置改为 `type: sqlite`。

### 7.7 验收

- 全后端跑 `uv run pytest -n 8 tests/` 全绿；`HETU_TEST_BACKENDS=sqlite` 单跑全绿；仓库里（历史设计稿
  除外）不再有 SQL 后端的 xfail。
- `hetu/`、`tests/`、`pyproject.toml` 里搜不到 sqlalchemy / aiosqlite / asyncpg / aiomysql / pymysql /
  psycopg。
- 共享模块搬迁之后，Redis 的 range / commit 基准（`benchmark/`）不回退。
- 改动过的文件 ruff / basedpyright 通过；跑 `scripts/gen_api_docs.py` 后 `docs/api/` 没有漂移。

## 8. 已知限制

- 同一实例里只差大小写的组件名、同一组件里只差大小写的字段名，SQLite 上报错，Redis 上可以。
- `hetu upgrade` 看不出 SQLite 上有没有服务器在跑（`FixedWorkerKeeper` 没有租约，旧后端也是这样）；
  同一台机器对同一个库文件起两个 `hetu start` 会撞 worker id。
- 读总是最新的，暴露不了生产上因副本滞后才出现的问题（事务侧有版本与区间校验兜底，订阅侧有尾随
  重读）。以后需要的话，可以加一个"模拟滞后的副本"。
- 库文件放在网络文件系统上（NFS、WSL 经 9p 访问 Windows 盘）时 WAL 可能用不了。
- 写锁等不到直接抛错，不重试。

## 9. 备选方案与否决理由（留给后来人，别再重复提）

- **保留通用 SQL 后端、逐条修偏差**：每条偏差要在三种数据库上分别修，而且 Redis 那边的语义一改就又
  漂移；已决定不再做通用 SQL 后端。
- **行数据存成通用 hash 表（key, field, value）**：实现更简单、和 Redis 一一对应，但数据在 DB 工具里是
  EAV，没法看（§3 决定 1）。
- **SQLite 客户端独立复制一份 Redis 逻辑、不动 Redis**：以后改 commit 逻辑要改两处，只能靠测试防漂移
  （§3 决定 2）。
- **用 lupa 在 SQLite 上跑真正的 `commit_v2.lua`**：零漂移，但多一个原生依赖；118 行的脚本用 Python
  移植、再由两个后端共跑检查码用例就够了。
- **继续用 aiosqlite**：事务跨 await 正是那次写锁泄漏的根源；每个操作一个 job 更简单。
- **不开线程、直接同步调用 sqlite3**：后端调用不再让出事件循环，协程的交错点和 Redis 不同，会藏住
  竞态；等写锁时还会卡住整个事件循环。
- **direct_set "保证发通知" / "Redis 也不发"**：见 §3.2 的代价表。
- **模拟 Redis 租约的 WorkerKeeper**：见 §4.9。
- **给 `type: SQL` 留兼容别名**：已决定直接改名（§3 决定 4）。

## 10. 实施顺序

每一步测试全绿；细化的实施计划另写到 `docs/superpowers/plans/`。

1. 抽共享模块：Redis 纯搬迁，加 `commit_script_`，改 `__init_subclass__`；redis / valkey / redis_cluster
   全绿、基准不回退。
2. 新的 SQLite 后端：`store` / `commit` / `client` / `maint` / `mq`；夹具切到新后端，§2.2 的 xfail 转绿，
   补上 §7.4、§7.5 的用例。
3. 删除 `sql/` 包、PG / MariaDB 的夹具与用例、依赖（`uv remove` 之后 `uv sync --all-packages`）。
4. direct_set 契约：docstring、API 文档、`test_endpoint_connection` 的注释。
5. CLI、配置模板、Sandbox、示例、文档（zh 与 en 放同一个提交）、`AGENTS.md`。

## 11. 主要改动文件清单

- 新增：`hetu/data/backend/redis_model.py`；`hetu/data/backend/sqlite/`（`__init__`、`client`、`store`、
  `commit`、`maint`、`mq`）；`tests/test_backend_sqlite.py`；`tests/fixtures/sqlite_service.py`。
- 删除：`hetu/data/backend/sql/`；`tests/test_backend_sql.py`；`tests/fixtures/sql_service.py`。
- 修改：
  - 后端：`redis/client.py`、`redis/maint.py`、`redis/mq.py`（补发逻辑上移）、`base.py`、`__init__.py`、
    `worker_keeper.py`，`repo.py` / `idmap.py`（docstring 里关于 SQL 的说明）；
  - 其他代码：`hetu/cli/start.py`、`hetu/cli/init.py`、`hetu/testing/__init__.py`、
    `hetu/CONFIG_TEMPLATE.yml`、`hetu/safelogging/filter.py`（注释）；
  - 依赖：`pyproject.toml`、`uv.lock`；
  - 示例与说明：`examples/chat/server/config.yml`、`AGENTS.md`；
  - 文档：`docs/zh` 与 `docs/en` 的 `_index` / `concepts` / `advanced` / `operations` / `getting-started` /
    `tutorial/chat-room`，`docs/api/`（重新生成）；
  - 测试：见 §7。
