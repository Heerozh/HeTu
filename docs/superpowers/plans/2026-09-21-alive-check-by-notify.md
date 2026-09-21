# 连接存活检查改为"变更通知触发 + 兜底间隔"实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 登录连接每次 RPC 固定的那次 `Connection` 行读取（"我是否被顶号"）去掉：ws 连接订阅自己那行 Connection 的行频道，收到变更通知才重查、并主动断开被顶号的连接；通知丢失时靠 `CONNECTION_ALIVE_RECHECK_INTERVAL`（默认 5 秒）兜底。`get + update` 的 System 从 3 RTT → 2，纯 insert → 1。

**Architecture:** `MQClient.push_pulled_()` 加 watcher 表（命中只回调、不进推送队列），`SubscriptionBroker.watch_channel()` 封装登记 + 订阅；`ConnectionAliveChecker` 增加通知模式（脏标记 + 兜底间隔），**只做非事务的单 key 读写，绝不开 Session**；`websocket.py` 在 broker 建好后接线，回调置脏并起短任务从 master 读一次、被顶号就 `fail_connection()`。裸 executor / Sandbox / future call 不接通知，行为不变。

**Tech Stack:** Python 3.14、Sanic、Redis keyspace 通知 / SQL notify 表、pytest（Redis 需 Docker，sqlite 免）。

## 全局约定

- TDD 用 Redis：`HETU_TEST_BACKENDS=redis uv run --frozen pytest ...`；SQL 用 `sqlite`；收尾
  `redis,redis_cluster,valkey,postgres,sqlite` 全跑一次（mariadb 本机 aiomysql 驱动在 HEAD 就坏，跳过）。
  Windows 下加 `PYTHONUTF8=1`；用 `uv run --frozen` 免得 uv 顺手改 `uv.lock`。
- Lint/类型检查只查自己改的文件，与 HEAD 比告警数不增；含反斜杠的脚本用 Write 写到 scratchpad 再跑。
- 提交前缀：`perf(endpoint):` / `feat(sub):` / `test:` / `docs:`。
- 开工先把本计划存一份到 `docs/superpowers/plans/2026-09-21-alive-check-by-notify.md`（格式对齐
  `2026-09-21-unique-check-at-commit.md`），随实施勾选。

---

## Task 1: MQClient 内部关注频道（watcher）+ `SubscriptionBroker.watch_channel`

**Files:** `hetu/data/backend/base.py`（`MQClient.__init__` ~846、`push_pulled_` ~860）；
`hetu/data/sub.py`（`SubscriptionBroker` ~289）；`tests/test_backend_pubsub_hub.py`（新增用例）。

- [ ] **先写测试 `test_watch_channel_callback_bypasses_client_queue(filled_item_ref, mod_auto_backend)`**
  （各后端参数化，仿 `test_hub_shared_subscription`）：`broker = SubscriptionBroker(backend)`；
  `channel = backend.servant.row_channel(ref, row_id)`（`row_id` 取 time=110 那行）；
  `hits = []`；`await broker.watch_channel(channel, lambda: hits.append(1))`；用 `_update_qty`
  改该行 → `async with asyncio.timeout(3): while not hits: await asyncio.sleep(0.02)`；断言
  `hits == [1]`（或 ≥1，Redis 行 HSET 一条通知）；再断言该频道**没有**进推送队列：
  `broker._mq_client.pulled_set` 不含 channel，且 `broker.get_updates(timeout=0.3)` 返回 `{}`；
  `await broker.close()` 后 hub 分发表里没有这个频道（`_hub(backend)._subs.get(channel)` 为空）；
  回调抛异常不影响后续通知（第二次 update 仍能到）。
- [ ] **实现 `MQClient`**：

```python
# __init__
self._watchers: dict[str, Callable[[], None]] = {}

def watch_(self, channel_name: str, callback: Callable[[], None]) -> None:
    """服务端内部关注频道：收到通知只同步回调，不进推送队列。调用方自行 subscribe。"""
    self._watchers[channel_name] = callback

def unwatch_(self, channel_name: str) -> None:
    self._watchers.pop(channel_name, None)

# push_pulled_ 开头：
cb = self._watchers.get(channel_name)
if cb is not None:
    try:
        cb()
    except Exception:  # noqa: BLE001 别让一个回调拖垮 hub 的监听协程
        logger.exception(_("⚠️ [MQ] 频道 {channel} 的内部回调异常").format(channel=channel_name))
    return 0
```

  回调在 hub 的监听协程里同步执行，必须非阻塞（置标记 / `create_task`），docstring 写明。
- [ ] **实现 `SubscriptionBroker.watch_channel`**：

```python
async def watch_channel(self, channel: str, callback: Callable[[], None]) -> None:
    """
    服务端内部关注一个频道（如本连接自己的 Connection 行）：收到通知只调 callback，
    不推给客户端、不计入订阅数、不做权限检查。连接关闭时随 mq_client.close() 一起退订。
    先登记再订阅，避免订阅生效到登记之间的消息落进客户端推送队列。
    """
    self._mq_client.watch_(channel, callback)
    await self._mq_client.subscribe(channel)
```

- [ ] 验证：`HETU_TEST_BACKENDS=redis,sqlite ... pytest tests/test_backend_pubsub_hub.py tests/test_backend_sub.py -q`；
  lint；commit `feat(sub): MQClient 内部关注频道，通知只回调不进推送队列；broker.watch_channel`。

---

## Task 2: `ConnectionAliveChecker` 通知模式 + 兜底间隔 + 配置

**Files:** `hetu/endpoint/connection.py`（模块级占位常量 ~29-30、`ConnectionAliveChecker` ~155-190）；
`hetu/server/main.py`（配置传递 ~364-370）；`hetu/CONFIG_TEMPLATE.yml`（`ENDPOINT_CALL_IDLE_TIMEOUT` 旁）；
`tests/test_endpoint_connection.py`（新增用例）。

- [ ] **先写测试**（裸 executor，`mod_test_app, tbl_mgr, new_ctx`，用 `unittest.mock.patch.object`
  计数 `conn_tbl.backend.servant.get` / `.master.get`——注意 `Table.servant_get` 是 property 绑定，
  patch 的目标是 `backend.servant` 实例上的 `get`；`only_master` 不适用，`servant` 可能是随机，
  测试里 `backend = tbl.backend`，若 `backend._servants` 多个则全部 patch）：
  1. `test_alive_checker_default_checks_every_call`：不开通知模式，`login` 后连调 3 次
     `add_rls_comp_value` → servant.get 被调 3 次（今天的行为，回归护栏）。
  2. `test_alive_checker_notify_mode`：`connection.CONNECTION_ALIVE_RECHECK_INTERVAL = 3600`；
     `on_change = executor.alive_checker.enable_notify_mode()`；`login`；连调 3 次 → servant.get
     只 1 次（首次置脏）；`on_change()` 后再调 1 次 → 2 次；另起 executor `login` 同一用户把它
     顶掉 → `on_change()` → `execute` 返回 `not ok`（被踢检测仍有效）。
  3. `test_alive_checker_fallback_interval`：通知模式、间隔设 0 → 每次都查；间隔设 3600、
     用 `monkeypatch` 把 `time.time` 前推 > 间隔 → 下次调用重查 1 次。
  4. `test_alive_checker_kicked_by_master_read`：`await executor.alive_checker.kicked(ctx)`
     顶号前 False、顶号后 True，且它不写 last_active、不开 Session（patch `backend.session`
     断言未被调用）。
- [ ] **实现**：

```python
CONNECTION_ALIVE_RECHECK_INTERVAL = 0  # 占位符，实际由Config里修改；通知模式下的兜底重查间隔（秒）

class ConnectionAliveChecker:
    """
    连接合规性检查（是否被顶号）。**只做非事务的单 key 读写**（servant_get / master.get /
    direct_set），绝不开 Session——它在 System 事务之外的 Endpoint 入口执行，开事务会嵌套。

    两种模式：
    - 默认：每次调用都读一次 Connection 行（裸 executor / Sandbox / future call）。
    - 通知模式（websocket 层 `enable_notify_mode()` 后）：只在收到本连接 Connection 行的变更
      通知（脏标记）或距上次检查超过 CONNECTION_ALIVE_RECHECK_INTERVAL 时才读。
    """

    def __init__(self, tbl_mgr):
        ...原有...
        self._notify_mode = False
        self._dirty = True        # 通知模式下：收到通知置位，下次 is_illegal 必查；初值 True 让登录后首个调用核一次
        self._last_check = 0.0

    def enable_notify_mode(self) -> Callable[[], None]:
        """切到通知模式，返回给 broker.watch_channel 的回调（只置脏标记，可在 hub 监听协程里同步调用）"""
        self._notify_mode = True
        def on_change() -> None:
            self._dirty = True
        return on_change

    def _need_check(self, now: float) -> bool:
        if not self._notify_mode:
            return True
        return self._dirty or now - self._last_check >= CONNECTION_ALIVE_RECHECK_INTERVAL

    async def kicked(self, ctx) -> bool:
        """从 master 读一次本连接的 Connection 行判断是否被顶号（非事务）。给通知回调用，不写 last_active"""
        if not ctx.caller:
            return False
        conn = await self.conn_tbl.backend.master.get(self.conn_tbl, ctx.connection_id, RowFormat.STRUCT)
        return conn is None or conn.owner != ctx.caller
```

  `is_illegal`：`if caller and self._need_check(now):` → 先 `self._dirty = False; self._last_check = now`
  再 `servant_get`；其余（日志、`last_active` 节流写）不动。`now = time.time()` 提到最前复用。
- [ ] **配置**：`CONFIG_TEMPLATE.yml` 在 `ENDPOINT_CALL_IDLE_TIMEOUT` 后加

```yaml
# 登录连接"是否被顶号"检查的兜底重查间隔（秒）。正常靠本连接 Connection 行的变更通知触发，
# 此间隔只在通知丢失（pubsub 缓冲溢出、节点切换、未开 notify-keyspace-events）时兜底；0 表示每次调用都查
CONNECTION_ALIVE_RECHECK_INTERVAL: 5
```

  `main.py` 传递：`connection.CONNECTION_ALIVE_RECHECK_INTERVAL = config.get("CONNECTION_ALIVE_RECHECK_INTERVAL", 5)`。
- [ ] 验证：`HETU_TEST_BACKENDS=redis,sqlite ... pytest tests/test_endpoint_connection.py tests/test_system_executor.py tests/test_testing_sandbox.py -q`；
  lint；commit `perf(endpoint): ConnectionAliveChecker 通知模式——收到变更通知或超兜底间隔才读 Connection 行`。

---

## Task 3: websocket 接线 + 收到通知主动断连 + ws 用例调整

**Files:** `hetu/server/websocket.py`（broker 创建之后 ~117-122）；`tests/test_websocket.py`
（`test_websocket_kick_connect` ~238-270）。

- [ ] **接线**（broker 创建后、起 task 前）：

```python
# 订阅本连接自己的 Connection 行：被顶号（owner 被改）时收到通知才重查，RPC 路径上不再每次读库；
# 收到通知还主动核一次，被顶号就立刻断连，不用等它下次调用
conn_tbl = tbl_mgr.get_table(connection.Connection)
if conn_tbl is not None and conn_tbl.backend is request.app.ctx.default_backend:
    alive_checker = endpoint_executor.alive_checker
    mark_dirty = alive_checker.enable_notify_mode()

    async def recheck_alive():
        try:
            if await alive_checker.kicked(context):
                logger.info(_("⛓️ [📡WSConnect] 连接已被顶号，主动断开：{ctx}").format(ctx=context))
                ws.fail_connection()
        except Exception as e:  # noqa: BLE001 读库失败不致命，兜底间隔和下次调用会再查
            logger.warning(...)

    def on_conn_row_changed():
        mark_dirty()
        request.app.add_task(recheck_alive())

    await broker.watch_channel(
        conn_tbl.backend.servant.row_channel(conn_tbl, context.connection_id),
        on_conn_row_changed,
    )
```

  说明：`kicked()` 里 `ctx.caller == 0` 直接 False，所以本连接自己 `elevate` 触发的通知不会误判
  （`ctx.caller` 在 commit 后才赋值，两种先后都安全）；`fail_connection` 对已关闭的 ws 无害；
  `add_task` 的短任务由 finally 里的 `purge_tasks()` 清理。频道名与 hub 必须同一后端，故有
  `backend is default_backend` 守卫，不满足就保持每次都查。
- [ ] **调整 `test_websocket_kick_connect`**：被顶号后服务器会主动断连，原来"client1 需要调一次
  system 才发现被踢"的前提不再成立。改为：client2 登录并调一次 system 后，`await asyncio.sleep(0.5)`
  （SQL hub 轮询 0.1 s，留余量），然后 `with pytest.raises(ConnectionClosedError): await client1.send([... 4])`；
  删掉中间那次 `send([... 3])`；注释改为"被顶号后服务器收到 Connection 行变更通知主动断开"。
  末尾 `client_sent[-1] == [..., 4]` 断言保留。
- [ ] 验证：`HETU_TEST_BACKENDS=redis ... pytest tests/test_websocket.py tests/test_headless_ws.py -q`
  （起服类测试，注意 `test-order-sensitivity` 记忆：单跑通过后收尾全量再跑一次）；lint；
  commit `perf(server): ws 连接订阅自己的 Connection 行，被顶号时通知触发重查并主动断连`。

---

## Task 4: 收尾

- [ ] `docs/zh/operations.md` "Redis 拓扑"或配置段补一句：连接存活检查依赖 keyspace 通知，未开
  `notify-keyspace-events` 时退化为 `CONNECTION_ALIVE_RECHECK_INTERVAL` 兜底；en 同步一句。
- [ ] `todo.md` 第 6 条 `[x]`，写清最终方案与效果；建议顺序改为 `10/11 视需要`。
- [ ] 全量回归：`HETU_TEST_BACKENDS=redis,redis_cluster,valkey,postgres,sqlite uv run --frozen pytest tests -q`。
- [ ] commit `docs: 连接存活检查改为通知触发的说明；todo 更新`。

---

## 各步会变红的现有测试

| 步骤 | 测试 | 原因 | 处理 |
|---|---|---|---|
| Task 3 | `test_websocket.py::test_websocket_kick_connect` | 被顶号后服务器主动断连，原用例里"被踢后第一次 send 仍成功"的假设失效 | 同任务改用例 |
| 不变 | `test_endpoint_connection.py::test_connect_kick*`、`test_system_executor.py`、`test_testing_sandbox.py` | 裸 executor 不开通知模式，行为不变 | 回归 |

## 风险与注意

- 回调在 hub 监听协程里同步跑：只置标记 + `add_task`，绝不 await、绝不开 Session。
- 主动重查读 **master**（权威）；每次调用的 `is_illegal` 仍读 servant（今天的行为）。副本滞后
  只影响 servant 读，通知本身来自副本应用写入之后（keyspace 事件由各节点执行命令时发出），顺序上安全。
- 通知模式首个调用（`_dirty` 初值 True）仍读一次，顺带核实登录后副本已同步。
- `direct_set(last_active)` 在 Redis 上会触发自己的通知 → 每 `IDLE/5` 秒多一次 master 读，可忽略；
  SQL 上没有。
- 多 servant 时 `row_channel` 里的 `__keyspace@{dbi}__` 取自随机 servant，与现有行订阅同一做法。

## 效果与验证

- 单元：`test_alive_checker_notify_mode` 直接断言"3 次 RPC 只读 1 次"。
- 端到端：登录用户 `get + update` 的 System 从 3 RTT → 2，纯 insert 的 System → 1（探针不覆盖
  Endpoint 层，靠上面的调用计数用例；如需可在 benchmark 用 ya 脚本复测 CPS）。
- 被顶号的连接在通知到达后（Redis 毫秒级，SQL ≤0.1 s 轮询）被服务器主动关闭。
