# 无服务器进程的表直读写客户端（`hetu.headless`）— 设计稿

- 日期：2026-09-20
- 状态：需求疑问（Q1–Q10）已与用户对齐，本稿待评审；评审通过后写实现计划
- 影响范围：新增 `hetu/headless.py`；核心层小改（`Backend.post_configure` 增参、
  `TableMaintenance.read_meta` 接受名字、`Session` 增 `explicit_ids_only`、
  `Table.servant_get_many`、`BackendClientFactory` 懒注册）；`hetu.testing.Sandbox`
  改为基于 headless client；文档（`docs/zh|en/advanced.md`、`docs/api/`）；测试。
  **不涉及客户端 SDK、不涉及 schema 迁移、不改 `commit_v2.lua`。**

## 1. 背景与目标

### 1.1 场景

游戏的战斗层是独立进程（一进程跑多个地图级战斗），**不是 HeTu 应用**：不跑 Sanic、不定义
System、不收客户端连接。它要和游戏服务器（HeTu 应用）共用同一个后端做三件事：

| 用途                                   | 表              | 方向                     | 频率           |
|----------------------------------------|-----------------|--------------------------|----------------|
| 读命令队列（spawn / despawn / order）  | `BattleCommand` | 游戏服务器写，战斗进程读 | ~1 条/秒/玩家  |
| 写回报队列（死亡 / 战利品 / 心跳）     | `BattleReport`  | 战斗进程写，游戏服务器读 | 低频，突发几百 |
| 读写租约与状态（谁跑哪个区域、epoch）  | `BattleSim`     | 双向                     | 每几秒一次     |

硬要求只有一条：**战斗进程写进去的行，必须和 System 写的行一模一样地触发客户端订阅推送**
（`BattleSim` 的宿主地址被客户端 `WatchRow` 到；`BattleReport` 被游戏服务器的整表 / 索引
订阅或 FutureCall 消费到）。

### 1.2 关键现状发现（均已核对，决定了实现路线）

- **提交路径只有一条**。System、`Sandbox`、以及本设计的 headless 最终都只走
  `Session.commit()` → `master.commit(idmap)`（`hetu/data/backend/session.py:87`）；
  Redis 侧即 `commit_v2.lua`，SQL 侧即那一个事务函数。表级 PUBLISH、keyspace 通知、
  乐观锁版本检查、unique 校验都在这一层。**只要 headless 用 `Session`，"逐字节一致"
  天然成立，不需要也不应该另造提交路径。**
- **数据层和 `SystemClusters` 只有一个耦合点**：`_get_referred_components()`
  （`hetu/data/backend/redis/client.py:52`、`sql/client.py:98`），被 `post_configure()`
  调用做索引 dtype 检查。`Sandbox`（`hetu/testing/__init__.py:201`）和
  `tests/fixtures/backends.py` 都在 monkeypatch 它。
- **表 meta 已存 `cluster_id`**：`RedisTableMaintenance.read_meta` /
  `SQLTableMaintenance.read_meta` 只需 `instance + 组件名` 即返回
  `cluster_id / version / json`，Redis key 为 `{instance}:{name}:meta`，SQL 在
  `_HeTu_Component_Meta` 表。`version = md5(comp_cls.json_)`（`base.py:515`），而 `json_`
  **含 `permission / rls_compare / volatile / readonly / backend / namespace`**，不只是列。
- **`Session` 没有跨表簇检查**：`SessionRepository` 的 ref 直接拿 `session.cluster_id`
  构造（`repo.py:41`）。同一 session 里 `using()` 一个属于别的簇的组件，会静默写进错误的
  `{CLU}` 前缀，服务器永远看不到。headless 必须自己校验。
- **不发号的现状**：`SnowflakeID` 未初始化时 `new_row()` 会 `AssertionError`
  （`component.py:232`），但 `python -O` 下 assert 被剥掉会产生 worker_id=-1 的垃圾 id。
  `UpsertContext.__aenter__` 未命中时也会调 `new_row()`（`repo.py:481`），且锚定 `id` 时
  先发一个雪花号再用锚定值覆盖（白白消耗一个号）。
- **`range` 的 `right=None` 是精确匹配**，不是 `>=`（`redis/client.py:526`、
  `sql/client.py:634`）。原需求第 4/5 节的轮询示例按此 API 是错的，文档示例按 HeTu 习惯写。
- **资源**：本机实测 `import hetu` 不含 sanic，+65 MB / 0.54 s，其中 numpy 14、redis 17、
  sqlalchemy 25（含 `ext.asyncio`）；`hetu.server.main` 再 +15 MB。重依赖全部来自
  `hetu/data/backend/__init__.py` 对 `redis` / `sql` 子包的 eager import（仅为触发
  `__init_subclass__` 注册 alias）。
- **`Backend(config)` 会原地修改传入 dict**（`servants` 为空时 append master，
  `backend/__init__.py:59-61`）。

### 1.3 已确认的设计决策（对应需求讨论 Q1–Q10）

- **Q1 示例按 HeTu 接口习惯写**：轮询用 `servant_range("created_at", left, float("inf"))`
  之类的显式区间；不迁就原需求里的错误示例。
- **Q2 schema 守卫只比"数据布局"，两种组件传法都支持**：
  传**类** → 本地类与 meta 比对 `namespace` 与 `properties`（列名 / dtype / unique /
  index），不一致报错；`permission / rls_compare / volatile / readonly / backend` 差异忽略，
  `default` 差异只 warning。传**名字** → 直接用 `BaseComponent.load_json(meta.json)`
  从服务器 meta 生成类，战斗进程零共享代码。
- **Q3 不发号规则**：`insert` 要求 `id != 0`；**锚定 `id=<非零显式值>` 的 `upsert` 允许新建
  （它不发号）**，锚定其他 unique 字段未命中才报错。不要求 id 为负，但文档推荐负数区
  （与 `ensure_future_call` 约定一致）。
- **Q4 簇迁移**：运维规则"迁簇必须重启战斗进程"保持；**再加 `client.check_schema()`**
  供调用方周期自检，发现簇 / schema 变更即抛异常，由调用方重连或退出。
- **Q5 同簇前提**：三张表在游戏服务器侧定义为 `core` namespace 组件（无需 System 引用即
  加载，见 `docs/zh/advanced.md` "core 命名空间"），不属于本设计改动。headless 只校验。
- **Q6 批量 insert 的 unique 预检往返**（每行至少 1 次）：本期不动，另案优化。
- **Q7 "不建表"边界**：SQL 后端仅开发用，接受"支持表可建、组件表不建"；Redis 的
  `notify-keyspace-events` CONFIG SET 与 SQL 的支持表创建保持现状。
- **Q8 `namespace` 参数**：删除（不是"可选"）。meta 由 `instance + 组件名` 唯一确定，组件类
  自带 `namespace_`；原本"从 `ComponentDefines` 按 `ns:Name` 解析"的用法被"传名字 → 用
  meta 生成类"取代，且 `ns:Name` 与 `duplicate()` 的 `Name:suffix` 命名有歧义。
- **Q9 测试隔离**：`sanic not in sys.modules`、懒加载、`connect` 耗时 / RSS 用 subprocess
  跑；订阅一致性复用 `test_websocket.py` 的 `worker_main` + sanic_testing 模式。
- **Q10 重依赖懒加载**：一起做。`BackendClientFactory` 按 alias 懒 import 内置后端模块，
  `hetu/data/backend/__init__.py` 去掉 eager import。

## 2. 设计

### 2.1 模块与 API 形状

新增单文件模块 `hetu/headless.py`，并在 `hetu/__init__.py` 里 `from . import headless`
（它只依赖已加载的 `hetu.data.*`，无新依赖）。

```python
import hetu.headless as headless

client = await headless.connect(
    backend_config,                                 # config.yml 里 BACKENDS[x] 的 dict，Redis / SQL 都支持
    instance="xxx-region",
    components=[BattleCommand, "BattleReport", BattleSim],  # 类 或 名字，可混用
)
```

```python
async def connect(
    backend_config: dict,
    instance: str,
    components: Sequence[type[BaseComponent] | str],
) -> HeadlessClient: ...


class HeadlessClient:
    backend: Backend
    instance: str

    @classmethod
    async def from_backend(
        cls, backend: Backend, instance: str,
        components: Sequence[type[BaseComponent] | str],
    ) -> HeadlessClient:
        """复用一个已有的 Backend（不接管其生命周期；测试 / 嵌入用）。"""

    def __init__(
        self, backend: Backend, instance: str, tables: Iterable[Table], *,
        explicit_ids_only: bool = True, owns_backend: bool = True,
    ) -> None:
        """底层构造：tables 已经解析好。Sandbox 用它。"""

    @property
    def tables(self) -> Mapping[str, Table]: ...          # 按组件名
    def table(self, comp: type[BaseComponent] | str) -> Table: ...
    def session(self, *comps: type[BaseComponent] | str,
                only_master: bool = True) -> HeadlessSession: ...
    async def check_schema(self) -> None: ...
    async def close(self) -> None: ...
    # async with await connect(...) as client: ...


class HeadlessSession(Session):
    def __getitem__(self, comp: type[BaseComponent] | str) -> SessionRepository: ...
    # retry() / commit() / discard() 等全部继承自 Session


class HeadlessError(Exception): ...
class TableNotFound(HeadlessError): ...        # meta 不存在：建表 / 迁移权归服务器
class SchemaMismatch(HeadlessError): ...       # .comp_name, .diff: list[str]
class ClusterChanged(HeadlessError): ...       # .comp_name, .old_id, .new_id（check_schema 用）
```

- `client.table(...)` 返回的就是现有 `hetu.data.backend.Table`（frozen dataclass，含
  backend + instance + cluster_id），非事务读走它已有的 `servant_get` / `servant_range`，
  新增 `servant_get_many`。
- `s[Comp]` 返回的就是现有 `SessionRepository`（insert / update / upsert / delete / get /
  range），**不另造一套写 API**。
- 传名字时，本地拿类的方式是 `client.table("BattleReport").comp_cls`（启动时取一次即可，
  之后 `Comp.new_row(id_=...)` 照常用）。

### 2.2 `connect` / `from_backend` 流程

`connect` = `Backend(copy.deepcopy(backend_config))` + `from_backend(...)`，并标记
`owns_backend=True`；任一步失败先 `await backend.close()` 再抛。`from_backend`：

1. `components` 非空、组件名无重复，否则 `ValueError`。
2. `maint = backend.get_table_maintenance()`；对每个组件
   `meta = maint.read_meta(instance, name)`（`read_meta` 改为接受类或名字，见 §2.7）。
   - `meta is None` → `TableNotFound`。**不建表、不迁移。**
   - 传的是类 → `_schema_diff(cls, meta.json)`，非空 → `SchemaMismatch(name, diff)`。
     `default` 差异只 `logger.warning`。比对通过后**用本地类**做 `Table.comp_cls`（布局相同，
     且调用方能按类身份查表、用它的 `new_row`）。
   - 传的是名字 → `comp_cls = BaseComponent.load_json(meta.json)`（生成新类，**不**注册进
     `ComponentDefines`，不污染进程全局）。
3. `backend.post_configure(components=[所有 comp_cls])`：Redis 加载 `commit_v2.lua`
   （commit 必需）+ 索引 dtype 检查；SQL 确保支持表。**不再 monkeypatch
   `_get_referred_components`。**
4. 构造 `Table(comp_cls, instance, meta.cluster_id, backend)`，每表打一行 INFO 日志
   （名字 → cluster_id），便于运维核对。

`connect()` 内的 ping / 读 meta / 加载 Lua 都是同步 IO，直接在调用方 loop 上阻塞执行
（一次性，LAN 上远低于 1 s）。保持 `async` 签名是为了将来改成异步 IO 时不动 API。

`_schema_diff(cls, meta_json) -> list[str]` 的规则：

| 字段                                                     | 处理                                  |
|----------------------------------------------------------|---------------------------------------|
| `namespace`                                              | 必须一致（SQL 表名含 namespace）      |
| `properties` 列增删；列的 `dtype` / `unique` / `index`   | 必须一致 → `SchemaMismatch`，diff 逐条 |
| `properties.*.default`                                   | 不一致 → `logger.warning`             |
| `permission / rls_compare / volatile / readonly / backend` | 忽略（headless 不做权限，不 flush）  |

diff 行形如 `+ col foo (<i8)`、`- col bar`、`~ col baz.dtype: <i4 -> <i8`、
`~ col x.unique: False -> True`、`namespace: a -> b`，直接进异常消息。

### 2.3 事务写：`HeadlessSession`

```python
async with client.session(BattleReport, BattleSim) as s:
    await s[BattleReport].insert(BattleReport.new_row(id_=-report_key(r)).fill(r))
    async with s[BattleSim].upsert(system_id=7) as sim:
        sim.epoch += 1
# 退出即 commit；RaceCondition 抛出

async for attempt in client.session(BattleSim).retry(5):   # 或自动重试
    async with attempt as s:
        ...
```

- `client.session(*comps)`：把每个 comp 解析成 `Table`，**要求 `cluster_id` 全部相同**，否则
  `ValueError`（消息列出 `组件 → cluster_id`），绝不静默拆成两个事务。至少一个 comp。
- `HeadlessSession(backend, instance, cluster_id, comps)` 直接构造（不经 `Table.session()`，
  因为要带 comps 白名单）；`__getitem__` 只接受声明过的组件（类或名字），未声明 → `KeyError`；
  repo 按组件缓存一份，跨 `retry` 复用（`Session.clean()` 只换 idmap，repo 持有的是
  session 引用，与 `SystemCaller` 的用法一致）。
- `only_master` 默认 **True**：事务内的 `get / range / unique 预检`一律读 master。理由：
  战斗进程写量小，读 replica 省不了什么，却会把复制延迟变成 `RaceCondition` 重试
  （尤其确定性 id 的 insert：replica 说"不存在"，commit NX 说"存在"，反复空转）。
  轮询用的 `servant_*` 不受此开关影响，仍走 replica。
- `explicit_ids_only` 从 client 继承（`connect` 恒 True；Sandbox False）。
- commit 一次往返（Redis 一次 Lua 调用；SQL 一个事务）。每行 `insert` 前的 unique 远程预检
  仍是每行 1 次往返（Q6，另案）。

### 2.4 非事务读：`Table.servant_*`

- `tbl.servant_get(row_id)`、`tbl.servant_range(index, left, right, limit, desc, row_format)`：
  现成。
- 新增 `tbl.servant_get_many(row_ids, row_format=RowFormat.STRUCT)`：
  `bind_first_arg_with_typehint(self.backend.servant.get_many, self)`，与 `servant_get`
  同款包装（`hetu/data/backend/table.py`）。
- 轮询读命令队列的正确写法（`created_at` 为带 index 的 float 列，由写入方填）：

  ```python
  rows = await cmd_tbl.servant_range("created_at", watermark - 2.0, float("inf"), limit=4096)
  ```

  `right` 不能省（省略 = 精确等于 `left`）。MySQL / MariaDB 不接受 `inf` 绑定参数，用
  有限上界（如 `time.time() + 3600`）；Redis / SQLite / PostgreSQL 均可用 `inf`。

### 2.5 不发号：`Session.explicit_ids_only`

`connect()` **不**初始化 `SnowflakeID`、不申请 worker id、不起 keep_alive。写侧规则在
`Session` 上加一个开关 `explicit_ids_only: bool = False`（服务器与 `Sandbox` 为 False，
行为不变），`SessionRepository` 两处各加一个 if：

| 操作                                   | `explicit_ids_only=True`（headless）            | False（服务器 / Sandbox，现状） |
|----------------------------------------|-------------------------------------------------|---------------------------------|
| `insert(row)`，`row.id == 0`           | `ValueError`：必须 `Comp.new_row(id_=...)` 显式给 id | 允许（0 也是个 id）           |
| `upsert(id=<非零>)` 未命中             | 允许新建：`new_row(id_=query_value)`，不发号     | 同左（改动：不再先发一个雪花号再覆盖） |
| `upsert(<其他 unique>=...)` 未命中     | `LookupError`：新行由服务器 System 预建         | `new_row()` 发雪花号（现状）    |
| `update / delete / get / range`        | 不变                                            | 不变                            |

`upsert(id=...)` 允许新建的意义：回报表用确定性 id 时，战斗进程崩溃重启后**重发同一批
回报**可以用 `upsert(id=-key)` 幂等落地（命中则 update，字段没变则连写都不写），而不是
`insert` 撞 `UniqueViolation` 让整批失败。

### 2.6 `check_schema()` 与簇迁移

`await client.check_schema()`：对每张表重读 meta，依次判定
`TableNotFound` → `ClusterChanged(name, old, new)` → `SchemaMismatch`（同 §2.2 规则，只比
数据布局）。**只报错，不自动换 cluster_id**：`Table` 是 frozen 且已被调用方持有
（`cmd_tbl = client.table(...)`），静默换 id 会让在飞的 session 和调用方缓存的 Table 不一致。
调用方的自然写法是在租约循环里调它，抛异常就退出进程交给 supervisor 重启（或 `close()` 后
重新 `connect()`）。

服务器 `hetu upgrade` 迁簇会把旧前缀下的**全部** key 一起改名（含 headless 之前写的），所以
只有"迁移之后、重启之前"这段窗口内的 headless 写入会成为孤儿；运维规则"迁簇必须重启战斗
进程"（Q4a）+ `check_schema`（Q4b）把窗口关掉。

### 2.7 核心层改动清单（都是小改，服务器行为不变）

1. **`Backend.post_configure(components=None)`** →
   `BackendClient.post_configure(components=None)`；Redis 的 `configure_master` /
   `_schema_checking_for_redis`、SQL 的 `_schema_checking_for_sql` 改为
   `comps = components if components is not None else self._get_referred_components()`。
   `configure_servant()` 不变。
2. **`TableMaintenance.read_meta(instance_name, comp: type[BaseComponent] | str)`**：两个实现
   本来就只用 `comp_cls.name_`，改成 `name = comp if isinstance(comp, str) else comp.name_`。
3. **`Session.explicit_ids_only`** + `SessionRepository.insert` 的 id==0 守卫 +
   `UpsertContext.__aenter__` 的未命中分支（§2.5）。
4. **`Table.servant_get_many`** property。
5. **`BackendClientFactory` 懒注册**：`hetu/data/backend/__init__.py` 删除
   `from . import redis / sql`；factory 加静态表
   `_BUILTIN = {"redis": "hetu.data.backend.redis", "sql": "hetu.data.backend.sql"}`，
   `create()` 遇到未注册 alias 且在表内时 `importlib.import_module` 后再查注册表。
   已核对：仓库内其它地方都是显式 `import` 或函数内懒 import（`idmap.py:285`、
   `worker_keeper.py:124`、`server/*`），无人依赖 `hetu.data.backend.redis` 的属性访问。
   效果：`import hetu` 不再加载 `redis` / `sqlalchemy`；Redis-only 的 headless 进程省下
   sqlalchemy 的 ~25 MB。
6. `hetu/__init__.py` 导出 `headless`；`scripts/api_extras.py` + `gen_api_docs.py` 增加
   `headless` topic（`connect / HeadlessClient / HeadlessSession / 三个异常`），重新生成
   `docs/api/`。
7. 所有新增用户可见字符串走 `_()`。

### 2.8 `Sandbox` 基于 headless client（S1）

`Sandbox` = headless client + 簇构建 + 建表 + `SystemCaller`。改动：

- `create()`：去掉 `_get_referred_components` monkeypatch，改
  `backend.post_configure(ComponentDefines().get_all())`；建簇、建表、`SnowflakeID.init`
  不变（这些是 Sandbox 的职责，headless 明确不做）。
- `__init__`：`self.client = HeadlessClient(backend, instance_name,
  [tbl for _, tbl in tbl_mgr.items()], explicit_ids_only=False)`；新增公开属性
  `Sandbox.client`，测试里可直接 `sb.client.session(A, B)` 拿多表事务。
- `get / must_get / range / insert / upsert / _resolve_table` 改为委托
  `self.client.table(comp)` / `self.client.session(comp)`；对外签名与错误消息不变。
- `aclose()` → `await self.client.close()`（client 拥有 backend）。
- `tests/test_testing_sandbox.py` 必须原样通过。

### 2.9 线程 / event loop 约束

- headless 可在**非主线程的 event loop** 中创建与使用；不依赖 Sanic `app.ctx`。
- Redis 异步连接绑定创建时的 loop（`redis/client.py:109` 有断言），所以
  **`connect` / 使用 / `close` 必须在同一个 loop 上**，一个 loop 一个 client。战斗进程的
  "HeTu 跑在独立线程的 loop 上，sim 线程只碰内存队列"正好满足。`from_backend` 复用别的 loop
  创建的 Backend 会触发该断言——测试里跨线程场景必须走 `connect(config)`。
- `SnowflakeID` / `ComponentDefines` 是进程单例，headless 不写它们。

### 2.10 用法示意（按 HeTu 接口习惯，替换原需求第 5 节）

```python
# 战斗宿主进程：HeTu 跑在独立线程的 event loop 上
client = await hetu.headless.connect(cfg, instance, components=[BattleCommand, BattleReport, BattleSim])
cmd_tbl = client.table(BattleCommand)


async def inbound():  # 命令：每秒一次索引 range，按每系 seq 去重
    while True:
        rows = await cmd_tbl.servant_range("created_at", watermark - 2.0, float("inf"), limit=4096)
        for row in rows:
            if row.seq > cursor[row.system_id]:
                cmd_tail[row.system_id].append(row)
        await asyncio.sleep(1.0)


async def outbound():  # 回报：单写者，确定性负数 id，攒批一次事务；重发幂等
    while batch := await report_q.get_batch():
        async for attempt in client.session(BattleReport).retry(5):
            async with attempt as s:
                for r in batch:
                    async with s[BattleReport].upsert(id=-report_key(r)) as row:
                        fill(row, r)


async def lease_loop():  # 租约与宿主地址，被客户端 WatchRow；顺带自检簇 / schema
    while True:
        await client.check_schema()  # ClusterChanged / SchemaMismatch → 让进程退出，由 supervisor 重启
        async with client.session(BattleSim) as s:
            async with s[BattleSim].upsert(system_id=sid) as row:  # 行由服务器 System 预建
                row.owner_host, row.lease_until, row.epoch = me, now + 30, epoch
        await asyncio.sleep(10)
```

## 3. 正确性与并发分析

- **订阅一致性**：写路径与 System 完全相同（同一个 `IdentityMap` → 同一个 `commit()`），
  Redis 的行 / 索引 keyspace 通知、表级 PUBLISH，SQL 的 `_Hetu_Notify` 行，都由后端 client
  产生，与谁开的 session 无关。
- **跨簇**：`client.session(A, B)` 在构造时用 meta 的 cluster_id 校验；`s[C]` 只放行声明过的
  组件。两道门之后，`IdentityMap.is_same_txn_group` 的 assert 永远不会因 headless 触发。
- **幂等重发**：`upsert(id=-key)` 未命中 → `get` 登记 negative observation → 退出时
  `insert`；若并发被别人抢先插入同 id，commit 的 NX / UNIQ 检查返回 RACE →
  `RaceCondition` → `retry` 后 `get` 命中 → 走 update（无变化则不写）。与
  `ensure_future_call` 的机制相同。
- **陈旧读**：`only_master=True` 让事务内读取不受复制延迟影响；`servant_*` 轮询读本就允许
  落后（业务按 seq 去重，水位线回看 2 s）。
- **确定性 id 的取值范围**：雪花 id 恒正且随时间单调，任何小于当前水位
  `(now - EPOCH) << 22` 的正数都不可能再被发出；但为免解释成本，文档统一推荐负数区。
- **迁移窗口**：见 §2.6。
- **schema 守卫的宽严**：忽略权限类字段是安全的，因为 headless 不做权限、不 flush；
  `namespace` 必须一致是因为 SQL 表名含它，否则 SQL commit 的"缺表自动建表"fallback
  （`sql/client.py:1033`）会静默造出一张野表。
- **懒注册不改变任何已有路径**：`Backend(config)` 仍经 factory；显式 import 子包的调用方
  照旧触发 `__init_subclass__` 注册。

## 4. 测试计划

测试组件：在 `tests/app.py` 增加 `HeadlessCommand`（`pytest` namespace；`system_id`
int64 index、`seq` int64、`created_at` float64 index、`payload` `<U32`）与 `HeadlessSim`
（**`core` namespace、`force=True`**，镜像生产的 Q5 用法，顺带验证 core 组件不被任何 app
System 引用也有簇和表；`system_id` int64 unique、`epoch` int64、`owner_host` `<U64`），
以及一个引用两者的 System `push_headless_command(system_id, seq)`（服务器侧写命令，也让
两表同簇）。两张都放 `core` 会被 pin 进每个 namespace 的簇构建、拖慢所有起服测试，故只放
一张。跨簇用例用 `HeadlessCommand` × `PublicNames`（不同簇）。

`tests/fixtures/backends.py` 增加 `mod_backend_config`（按 `backend_name` 参数化，从各
service fixture 拼出 `BACKENDS[x]` 形状的 dict），供 `connect(config)` 与跨线程用例使用。

- **`tests/test_headless.py`**（`mod_auto_backend` 全后端参数化；服务器侧表由
  `mod_tbl_mgr` 建好）：
  1. `from_backend` 得到的 `cluster_id` 与 `ComponentTableManager` 一致；传类 / 传名字两种
     模式；传名字得到的类 `properties_` 与本地类一致，且未注册进 `ComponentDefines`。
  2. **反向读取**：System 插入 → `servant_range` / `servant_get_many` 读到；
     `servant_get_many` 顺序与 `row_ids` 一致、缺失位为 None。
  3. **不发号**：`insert` id==0 → `ValueError`；`upsert(system_id=...)` 未命中 →
     `LookupError`；`upsert(id=-5)` 未命中 → 成功新建；重发同 id → 命中、无写入；
     用例内把 `SnowflakeID()` 单例 `worker_id` 置回 -1 并断言 connect 前后不变（其它测试
     已初始化过它，单进程内只能这样隔离）。
  4. **事务语义**：headless 与 `SystemCaller` 并发 update 同一行 → 一方 `RaceCondition`，
     `retry()` 后终态正确；`client.session(HeadlessCommand, PublicNames)` → `ValueError`；
     `s[未声明组件]` → `KeyError`；`only_master` 默认 True（`session.only_master`）。
  5. **schema 守卫**：用 `BaseComponent.load_json(改过 properties 的 json)` 造同名类
     （多一列 / 改 dtype / 改 unique）→ `SchemaMismatch` 且 `diff` 含该列；只改
     `permission` → 连接成功；只改 `default` → 成功且有 warning；未建表的组件名 →
     `TableNotFound`，且之后 meta 仍不存在（Redis 无 meta key；SQL 无表）。
  6. **`check_schema`**：正常返回；手工改 meta 的 `cluster_id` → `ClusterChanged`；
     删 meta → `TableNotFound`。
  7. **轮询读**（验收 8）：后台任务经 `SystemCaller` 每 20 ms 插一条命令（`created_at`
     取 `ctx.timestamp`），共 N 条；headless 每 200 ms 一次
     `servant_range("created_at", wm - 2.0, inf)` + 按 `(system_id, seq)` 去重 →
     无漏无重。
- **`tests/test_headless_ws.py`**（验收 1 + 7）：复用 `test_websocket.py` 的
  `test_server` / `mimic` 模式，`BACKENDS` 参数化 Redis 与 SQLite。ws 客户端先
  `sub` 行 / 索引 / 整表；headless 在**另一个 `threading.Thread` + 新 event loop**里
  `connect(config)` 后 insert / update / delete；断言三种订阅各收到对应 `updt` 帧。
  若 sanic_testing 下 SQLite 起服不稳，退化为 `test_backend_sub.py` 的
  `SubscriptionBroker` 级验证（全后端参数化），ws 只跑 Redis。
- **`tests/test_headless_process.py`**（验收 6，subprocess）：
  `import hetu, hetu.headless` 后 `"sanic" not in sys.modules` 且
  `"sqlalchemy" not in sys.modules`、`"redis" not in sys.modules`（懒加载）；
  `Backend({"type": "redis", ...})` 在未显式 import 子包时可用；`connect` 耗时 < 1 s；
  Redis 后端 `connect` 前后 RSS 增量 < 10 MB（`pytest.importorskip("psutil")`）。
- **回归**：`tests/test_testing_sandbox.py`、`tests/test_backend_*`、`tests/test_migration.py`
  原样通过；fixtures 里的 `_get_referred_components` monkeypatch 仍有效（可顺手改成
  `post_configure(ComponentDefines().get_all())`，非必需）。

## 5. 取舍与边界（YAGNI）

- **不做**：RPC 调 System（无 `SystemCaller`）、权限 / RLS / 限流、FutureCalls、变更通知 /
  `MQClient`（读侧轮询）、雪花 id 与 worker id 租约、WebSocket 协议。
- **`direct_set` 不暴露也不文档化**：它是 `Table` 上现成的属性，但 Redis 侧只有 keyspace
  通知没有表级 PUBLISH、SQL 侧完全不写通知行，**不满足** S2 的"必须同样 PUBLISH"；且只支持
  volatile 组件。headless 文档明确写"不要用"。
- **不做批量 insert 的预检 pipeline 化**（Q6，另案）。
- **`check_schema` 只报错不自动刷新**（§2.6）。
- **`connect()` 内同步阻塞 IO**：一次性、< 1 s；将来需要时可 `asyncio.to_thread`，API 不变。
- **`namespace` 参数删除**（Q8）。
- **命名**：`hetu.headless` 与 C# 侧的 "HeTu.Client（headless .NET 客户端）" 同用 headless 一词，
  但一个是表直读写、一个是 WebSocket SDK；文档里本模块统一称"无服务器进程的表直读写客户端"。
- **schema 守卫不比 `default`**（只 warning）：default 只影响 headless 本地 `new_row()`
  的填充值，不影响已存行的读写；要严格可自行比对。
- **哈希 / 负数 id 的碰撞**与 `ensure_future_call` 同边界，由调用方的 `report_key` 设计负责。

## 6. 主要改动文件清单

- 新增 `hetu/headless.py`：`connect`、`HeadlessClient`、`HeadlessSession`、三个异常、
  `_schema_diff`。
- `hetu/__init__.py`：导出 `headless`。
- `hetu/data/backend/__init__.py`：`Backend.post_configure(components=None)`；删除对
  `redis` / `sql` 子包的 eager import。
- `hetu/data/backend/base.py`：`BackendClient.post_configure(components=None)`；
  `TableMaintenance.read_meta` 接受名字；`BackendClientFactory` 懒注册。
- `hetu/data/backend/redis/client.py`、`sql/client.py`：`post_configure` /
  `_schema_checking_*` 接受 `components`；`redis/maint.py`、`sql/maint.py`：`read_meta`。
- `hetu/data/backend/session.py`：`explicit_ids_only`。
- `hetu/data/backend/repo.py`：`insert` id==0 守卫；`UpsertContext.__aenter__` 未命中分支。
- `hetu/data/backend/table.py`：`servant_get_many`。
- `hetu/testing/__init__.py`：Sandbox 基于 `HeadlessClient`（§2.8）。
- `scripts/api_extras.py`、`scripts/gen_api_docs.py`：`headless` topic；重新生成 `docs/api/`。
- `docs/zh/advanced.md`（新节"非服务器进程读写表（`hetu.headless`）"：只给可信内部进程用、
  不做权限 / RLS、显式 id 规则、轮询写法、`check_schema` + 重启、同 loop 约束、不要用
  `direct_set`）；`docs/zh/operations.md` 集群重排一节加一句"迁簇后重启 headless 进程"；
  `docs/en/*` 同步翻译；`hetu/llms.txt` 加 API 链接；
  `hetu/skills/building-on-hetu/SKILL.md` 补一段（可选）。
- `tests/app.py`、`tests/fixtures/backends.py`、`tests/test_headless.py`、
  `tests/test_headless_ws.py`、`tests/test_headless_process.py`。

## 7. 待确认的新决定

评审时请确认以下几处本稿新引入、之前未讨论的选择：

1. `client.session()` 的 `only_master` **默认 True**（§2.3）。
2. `connect()` 之外增加 `HeadlessClient.from_backend()` 与底层构造函数（`owns_backend`
   语义），主要为测试与 Sandbox 服务（§2.1 / §2.8）。
3. `check_schema()` 只抛异常、不自动刷新 cluster_id（§2.6）。
4. `Sandbox` 新增公开属性 `client`（§2.8）。
5. 传名字生成的类**不注册**进 `ComponentDefines`（§2.2）。
