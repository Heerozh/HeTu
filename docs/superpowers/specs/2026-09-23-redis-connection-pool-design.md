# redis-py 8 下的异步连接池与单行读取 — 设计稿

- 日期：2026-09-23
- 状态：已实施
- 起因：生产压测发现 `423b05d9`（standalone 连接池换成 `BlockingConnectionPool`）与
  `552f3bd1`（`SessionRepository.range` 的行读取改走 `get_many`，单行也建 pipeline）
  合计让每次 System 调用的 HeTu CPU 多 8%~11%，压测吞吐掉约 30%。
- 影响范围：`hetu/data/backend/redis/pool.py`（新增 `HeTuConnectionPool`）、
  `hetu/data/backend/redis/client.py`（standalone 连接池、`_hgetall_many`）。
  **不涉及 schema、客户端协议、配置项；原生集群模式的连接池不变。**

## 1. 背景与目标

`423b05d9` 之前用的是 redis-py 普通 `ConnectionPool`。redis-py 8.0 把它的默认上限从 2³¹
降到 100，满了立即抛 `MaxConnectionsError`（`ConnectionError` 的子类，HeTu 的接收协程遇到
它会断开 ws 连接，即"池满就踢人"）。当时换成 `BlockingConnectionPool` 解决了这个问题，
但它每次取、还连接都有固定开销。

生产压测（4 worker，HeTu 进程每次调用的 CPU µs，3 轮中位数）：

| 版本 | get | get_then_update | get2_update2 |
|---|---:|---:|---:|
| old（普通池 + 单行直接 HGETALL，redis-py 7.4） | 119.5 | 433.9 | 765.9 |
| main（阻塞池 + 单行也走 pipeline） | 130.1 | 482.9 | 799.8 |
| main 把这两处改回（普通池 + 大上限、单行直接 HGETALL） | 118.7 | 429.9 | 708.7 |

同一写法下 redis-py 8.1 与 7.4 只差 1%~2%，所以问题在这两处写法，不在包版本本身。

**目标**：保留"有上限、满了排队"（不让并发突发变成连接突发，也不因池满踢人），但平时
取/还连接不付排队的代价；单行读取不建 pipeline。

## 2. 已确认的事实（redis-py 8.1 源码与本机实测）

### 2.1 连接池的每次取/还

- 普通 `ConnectionPool`（`redis/asyncio/connection.py`）
  - `get_connection`：外面套一层 `deprecated_args` 包装；`async with self._lock`；OTel 连接
    计数 `record_connection_count` await 2 次（新建连接时 1 次）；再 `ensure_connection`。
  - `release`：`async with self._lock`；`_event_dispatcher.dispatch_async(
    AsyncAfterConnectionReleasedEvent)`；`record_connection_count` await 2 次。
  - OTel 没开时 `record_*` 也要走一遍协程创建和 `get_observability_instance()` 查询。
- `BlockingConnectionPool` 在此之上：`get_connection` 多 `async with self._condition` +
  `async_timeout(self.timeout)`（**每次挂一个 asyncio 定时器**）+ `_maybe_pool_lock()`
  （`asynccontextmanager`）+ `record_connection_wait_time`；`release` 多一次
  `async with self._condition` + `notify()`。
- `ensure_connection` 里的 `can_read()` 在 8.1 是纯内存检查（hiredis reader 的
  `has_data` / `at_eof` / `_buffer`），不额外进事件循环。
- 维护通知（maint notifications）：RESP3 下默认 `enabled="auto"`，MOVING 等处理器持
  `_get_pool_lock()`（就是 `self._lock`）**跨 await** 改池状态。
- 流式凭证（`StreamingCredentialProvider`）的重新认证挂在释放事件上
  （`AsyncReAuthConnectionListener`）。
- 下文快路径用到的内部属性 `_available_connections` / `_in_use_connections` / `_lock` /
  `make_connection` / `ensure_connection`、连接的 `should_reconnect()`、
  `MaxConnectionsError`，redis-py 7.4 与 8.1 都有。

### 2.2 单条命令与 pipeline

- `socket_timeout` 默认值 8.0 起从 None 改成 5 秒：`send_packed_command` 走
  `asyncio.wait_for`，`read_response` 走 `async_timeout`，每条命令收、发各挂一个定时器；
  pipeline 里每条回复各一个。
- `Pipeline.execute`：建 `Pipeline` 对象；每次都跑 HIMPORT 的 `pipeline_prepares` /
  `drain_pipeline_prepares`；`reset()` 里 `asyncio.shield(pool.release(conn))` 为还连接
  多建一个 Task；再加 `record_operation_duration`。所以 1 条命令的 pipeline 比直接发这条
  命令贵一截（本机 +9µs，生产 Linux 实测 +24µs）；条数多了被摊薄，10 条的 pipeline 只有
  逐条执行 CPU 的约 1/4。

### 2.3 本机微基准（redis-py 层）

Windows asyncio，redis-py 8.1 + hiredis，32 并发，单位是每次操作客户端 CPU µs，3 轮中位数；
最后一列是 128 并发（超过上限 64）时实际开的连接数。

| 连接池 | HGETALL | 1 条的 pipeline | 10 条的 pipeline | 128 并发连接数 |
|---|---:|---:|---:|---:|
| `BlockingConnectionPool(64)` | 54.3 | 65.6 | 123.0 | 64 |
| 普通池，上限 2³¹（7.x 行为） | 48.0 | 58.2 | 119.1 | 128 |
| 候选①：普通池快路径，满了才排队 | 48.8 | 57.8 | 117.2 | 64 |
| 候选②：①再精简取/还（本设计） | 44.1 | 53.1 | 111.3 | 64 |
| ② + `socket_timeout=None` | 38.7 | 46.5 | 91.8 | 64 |

256 并发时普通池预热一下子要开上千条连接，把 Docker Desktop 的端口代理打挂了——无上限
池的风险就是并发突发会变成连接突发。

## 3. 设计

### 3.1 `HeTuConnectionPool(redis.asyncio.ConnectionPool)`

- **快路径**：`_available_connections.pop()`，没有就在未满时 `make_connection()`，加进
  `_in_use_connections`，然后 `ensure_connection`；还连接时从 `_in_use_connections` 移到
  `_available_connections`。pop/add、remove/append 之间没有 await，asyncio 单线程下不需要锁。
  跳过的是：池锁、OTel 连接计数、释放事件分发、`deprecated_args` 包装。
- **慢路径**：池满（快路径返回 None，或原路径抛 `MaxConnectionsError`）才进 FIFO 等待队列
  （每个排队者一个 future，`asyncio.timeout_at` 统一截止时间）；`release` 之后叫醒队头，
  被叫醒者重新走一次取连接。等过 `timeout` 抛 `ConnectionError("No connection available.")`，
  与 `BlockingConnectionPool` 一致；`timeout=None` 一直等。
- **退回 redis-py 原逻辑**（`super().get_connection()` / `super().release()`，带锁、计数、
  事件）的条件：
  - 池构造时判定、之后不变：配置了 `StreamingCredentialProvider`、传了自定义
    `event_dispatcher`、开启了 redis-py 的 OTel（`enabled_telemetry`）；
  - 每次判定：`self._lock.locked()`（维护通知处理器正持锁跨 await 改池）；还连接时
    `connection.should_reconnect()`。
- 对外接口不变：`RedisBackendClient` 仍用 `BACKENDS.<name>.max_connections`（默认 64）和
  `pool_timeout`（默认 5 秒）构造，`from_url` + `Redis.from_pool` 接管关闭。

### 3.2 `_hgetall_many` 单行直读

入参先转成 list；只有 1 个 id 时直接 `aio.hgetall`，2 个及以上仍按
`RANGE_PIPELINE_CHUNK` 分块走 pipeline。`get_many`、`RedisBackendClient.range`、
`SessionRepository.range` 以及经由它们的 `get(unique=)` / `upsert(unique=)` 都走这一个函数，
改一处全覆盖；集群模式同样适用（`RedisCluster.hgetall`）。

## 4. 正确性与并发分析

- **簿记只有一份**：快路径和原路径操作的是同一组 `_available_connections` /
  `_in_use_connections`，混用不会失衡；上限判定与原逻辑相同，连接总数 ≤ `max_connections`。
- **与维护通知处理器互斥**：快路径的临界区不 await，只在锁空闲时进入；处理器持锁期间所有
  取/还都走原路径排在锁上。处理器在快路径取到连接之后、`ensure_connection` 期间开始工作，
  和原路径一样（原路径在 `ensure_connection` 前也已放锁）。
- **不丢唤醒**：
  - 被叫醒后还没用就被取消或恰好超时：future 已有结果但没被消费，在异常分支里把名额
    传给下一个排队者；
  - 被叫醒后取连接时连不上：`ensure_connection` 失败会 `release` 这条连接，`release` 的
    `finally` 叫醒下一个；
  - 被叫醒后名额被新来的请求抢走（快路径不看队列）：排回**队头**继续等，不丢位置。
    这种插队让持续饱和时不是严格公平，但有 `timeout` 兜底，且饱和时插队反而少一次调度。
- **`reset()`**：清空后叫醒所有排队者去重新取（此时可以直接建新连接）。
- **代价**：快路径下 redis-py 的 OTel 连接池计数不准（HeTu 不用；开了 OTel 会整体退回
  原路径）；依赖 redis-py 内部属性，见 §5 的钉住用例。

## 5. 测试

`tests/test_backend_redis_pool.py`（standalone 的 redis / valkey 各一遍，单行直读另跑集群）：

- backend 的 master / servant 用的就是本池，且走快路径；快路径依赖的 redis-py 内部属性
  存在且语义符合预期（redis-py 升级改名会先挂在这里）；
- 自定义事件分发器时不走快路径；
- 并发 50、上限 2：只建 2 条连接，全部完成，结束后无残留排队者；
- 池满超时抛 `ConnectionError`，还回来后立即可用；
- 排队者按先来后到拿到连接；
- 被叫醒后又被取消的排队者把名额让给下一个（删掉这段逻辑时该用例失败，验证过）；
- 取连接时连不上：连接还回池里，名额不漏；
- 池锁被占用时走原路径、等锁；
- `get_many` / `range` 读 1 行不建 pipeline，2 行以上照旧批量；迭代器入参、空列表、
  不存在的 id 都正确。

## 6. 结果

HeTu 层前后对比（本机，同进程结构分别跑两个版本，32 并发，每次操作 CPU µs，两次运行平均）：

| 路径 | main（阻塞池 + 单行 pipeline） | 本设计 | 变化 |
|---|---:|---:|---:|
| `get(id=)`（1 条 HGETALL） | 65.1 | 52.5 | -19% |
| `get_many` 读 1 行 | 74.9 | 52.5 | -30% |
| 事务 get + update + commit | 229.1 | 201.9 | -12% |
| `upsert(unique=)` | 354.0 | 302.8 | -14% |

`benchmark/redis_pool_bench.py` 复测（同上单位，"CPU µs / 连接数"）：

| | 阻塞池 | 普通池（7.x 行为） | `HeTuConnectionPool` |
|---|---:|---:|---:|
| 32 并发 HGETALL | 52.3 / 32 | 46.9 / 32 | 41.8 / 32 |
| 128 并发（超过上限 64） | 55.6 / 64 | 50.9 / 128 | 44.6 / 64 |

每条命令比普通池还少约 5µs，所以预期不只回到 old 的水平。以上是 Windows asyncio 的数字，
生产 Linux（uvloop）以压测为准。

## 7. 经验

1. **redis-py 大版本升级先看默认值**。8.0 同时改了两个会影响 HeTu 的默认值：async 连接池
   上限 2³¹ → 100、`socket_timeout` None → 5 秒；以及默认 RESP3（代理层不支持 `HELLO` 时要
   `?protocol=2`）。升级后先跑 `benchmark/redis_pool_bench.py` 和
   `tests/test_backend_redis_pool.py`。
2. **不要用 `BlockingConnectionPool`**。要上限就用 `HeTuConnectionPool`：上限只在池满时
   付代价。也不要退回无上限的普通池：Redis 一卡，每个 worker 的连接数就跟着在途请求数涨。
3. **单条命令不要包 pipeline**。redis-py 8 的 pipeline 有固定开销，至少 2 条才值得批；
   批量读的接口要给 1 条的情况留快路径。
4. **量每条命令的客户端 CPU**：用 `time.process_time()` 在 32 左右的并发下量、取多轮中位数。
   单并发时数字被事件循环空转主导（本机同一轮里 HGETALL 单并发 139µs、32 并发 50µs），
   不能拿来比。
5. **cProfile 看协程要小心**：协程每次从挂起恢复都算一次调用，`ncalls` 常常是实际次数的
   2 倍，看 `tottime` 与源码对照，别按 `ncalls` 判断"执行了两次"。
6. **Windows 上的 asyncio 结论要回 Linux 复测**：本机定时器是纯 Python 堆、IO 走 proactor，
   uvloop 下这两项便宜得多；相对排序可信，绝对值和收益幅度以生产压测为准。
7. **依赖第三方内部属性就写钉住用例**：快路径直接动 redis-py 池的内部集合，钉住用例让
   redis-py 改名时测试先挂，而不是线上静默出错。

## 8. 取舍与边界（未做）

- **`socket_timeout` 配置项**：关掉能让每条命令再省约 5µs（本机），pipeline 按条数省；代价是
  Redis 宕机或网络分区、对端不回 RST 时，请求会挂到 TCP 超时而不是 5 秒报错。等生产压测
  再决定是否做成 `BACKENDS` 配置项，默认值保持 redis-py 的 5 秒。
- **精简 pipeline**（绕开 redis-py `Pipeline`，直接在连接上 `send_packed_command` +
  `read_response`）：要自己处理重试、断线和 RESP3 回调，收益只在多行读取上，暂不做。
- **原生集群模式**：redis-py 的 `RedisCluster` 每节点池只能设上限、满了直接抛，
  `HeTuConnectionPool` 接不进去；HeTu 本就不推荐原生集群，维持现状。

## 9. 主要改动文件清单

| 文件 | 改动 |
|---|---|
| `hetu/data/backend/redis/pool.py` | 新增 `HeTuConnectionPool` |
| `hetu/data/backend/redis/client.py` | standalone 连接池换成 `HeTuConnectionPool`；`_hgetall_many` 单行直读 |
| `tests/test_backend_redis_pool.py` | 新增，见 §5 |
| `benchmark/redis_pool_bench.py` | 新增，三种连接池的每命令 CPU / 连接数对比 |
