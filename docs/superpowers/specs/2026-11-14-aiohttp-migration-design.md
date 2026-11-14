# 用 aiohttp 替换 Sanic — 设计稿

- 日期：2026-11-14
- 状态：设计中，已做一轮独立审查（§15）。§3.4 的现存问题已由 PR #225 修复；第 0 步在 `test/server-harness`
  分支上做。已定事项见 §13，待定问题见 §14。
- 基线：dev `71bde8c5`
- 影响范围：`hetu/server/`（main、websocket、receiver、web、threaded_accept；新增 runtime、config、conn、
  adapters、supervisor）、`hetu/cli/start.py`、`hetu/webext.py`、`hetu/system/future.py`、
  `hetu/endpoint/context.py`、`hetu/safelogging/`、`hetu/data/backend/worker_keeper.py`、
  `hetu/CONFIG_TEMPLATE.yml`、`pyproject.toml`、测试、文档。
- **不改**：线协议（握手、消息格式、close 码、`motd`）；pipeline；Session / commit；订阅（hub、broker）；
  后端与读写分布（不新增任何 master 读写）；客户端 SDK。

## 1. 背景

### 1.1 Sanic 停滞

- 25.3（2025-03）之后只出了一个功能版本 25.12 LTS（2025-12），2026 年只有补丁 25.12.1（2026-05-31）。
  此前是每季度一版。
- 25.12.1 在 Python 3.14 上已经报 PEP 765 的 SyntaxWarning（`sanic/server/websockets/impl.py:437`、`:463`，
  finally 里 return），还调用 `asyncio.set_event_loop_policy`（`sanic/server/loop.py:47`），这个 API 预定在
  Python 3.16 删除。不出新版的话，Sanic 跑不到 3.16。

### 1.2 ws 层的 CPU

2026-11-14 本机实测。服务端绑一个 P 核（约 4.7GHz），Python 3.14.10 + uvloop 0.23，64 个连接，128B 二进制帧，
一问一答，压测端是 4 个 picows 进程。表中是服务端每帧的 CPU，跑了 2 轮，相差不到 2%。

| 方案 | 回显 µs/帧（≈RPC） | 连发 32 帧 µs/帧（≈推送） |
|---|---:|---:|
| Sanic 25.12.1（HeTu 的 `recv_streaming` 收法） | 10.82 | 2.55 |
| aiohttp 3.14.5 | 3.12 | 1.47 |
| FastAPI + uvicorn 0.54 | 6.56 | 2.30 |
| picows 2.3.1（websockets 兼容 API） | 2.36 | 1.00 |
| Granian 2.8.8 RSGI | 8.84 | 3.91 |

Sanic 慢在收包，和 `benchmark/hello_world_optimization_report.md` 里 `recv_streaming` 占收发栈 23% 的观察一致。

粗估：hello_world 现在每次 RPC 22.09µs，其中 Sanic 那段约 10.8µs。换成 aiohttp 约省 7.7µs，单核 QPS 约 +50%。
推送每帧只省 1~1.5µs，不到现在 13~18µs 的一成，推送的大头是每个连接各做一遍的 zlib 和加密。以上是估算，
以第 4c 步的实测为准（§5.6）。

### 1.3 为什么是 aiohttp

- 由 aio-libs 维护，发版勤（3.14.5，2026-10），有 cp314 和 Windows 的 wheel。
- 自带生产级 HTTP 服务器（HTTP 解析用 llhttp），不需要 uvicorn。ws 的帧解析和掩码处理是编译扩展
  （`aiohttp/_websocket/reader_c`、`mask`）。
- 有完整的 HTTP 路由，`define_route` 能整体搬过去。
- 缺多进程管理，由 HeTu 自己写（§5.7）。`main.py` 里围绕 Sanic WorkerManager 的逻辑（ack 阈值、提前中止、
  丢租约重启、worker 序号、Windows accept）本来就是 HeTu 自己的。

## 2. 目标与非目标

目标：

- 删除 `sanic` 和 `sanic-testing` 依赖。
- 每一步都能单独合并，测试全绿。
- 第 6 步之前，Sanic 栈只有两类有意的行为变化：迁移前单独修掉的现存问题（§3.4），第 2 步删掉的配置项。
- 线协议和客户端 SDK 不变。close 码的细微差别见 §6。
- Windows 开发环境（包括多 worker）继续可用。
- 第 4c 步实测 RPC 和推送的成本（§5.6）。

非目标：

- HeTu 自己终止 TLS：删掉 `CERT_CHAIN` 和 `--cert`，交给 Caddy（§13）。
- 访问日志：删掉 `ACCESS_LOG`，交给 Caddy。
- `INSPECTOR`：删掉，以后由 CLI 提供 `reload` 命令（另立 spec）。
- HTTP/2、静态文件性能、webext 功能扩展。
- 换掉 uvloop。

## 3. 现状（dev `71bde8c5`）

### 3.1 Sanic 触点

| 位置 | 用到的 Sanic 东西 | 第几步处理 |
|---|---|---|
| `hetu/server/main.py` | `Sanic` app；`before_server_start(worker_start)` / `after_server_stop(worker_close)`；`add_task` 三个（FutureCalls、租约续约、雪花水位，`:340-344`）；`app.ctx`（各 backend、`default_backend`、`table_managers`、`snowflake_lease`）；`app.config`；`app.stop()`（`:115`）；`app.m.restart()`；`WorkerManager` / `WorkerProcess.THRESHOLD`；`logging.getLogger("sanic")` 重定向；`app.blueprint`、`webext.apply_to`、`web_root` | 1、2、4a、5 |
| `hetu/cli/start.py` | `sanic.config.Config`；`AppLoader`；`app.prepare(debug, access_log, motd, auto_tls, auto_reload, ssl, workers, single_process)`；`Sanic.serve` / `serve_single`；`SANIC_IGNORE_PRODUCTION_WARNING`；`--cert`；CLI 路径总会传 `CERT_CHAIN: ""`、`ACCESS_LOG: False`（`:199-201`） | 2、4a、5 |
| `hetu/server/websocket.py` | `Request`、`Websocket`、`WebsocketClosed`；`@HETU_BLUEPRINT.websocket("/hetu/<db_name>")`（导入即注册）；`ws.recv(timeout=10)`、`ws.send`、`ws.ws_proto.parser_exc`；`request.app.ctx/config/add_task/cancel_task/purge_tasks`；`request.client_ip`、`request.id`（`:157`）；`SystemContext(request=request)` | 1、3 |
| `hetu/server/receiver.py` | `SanicException`、`WebsocketClosed`；`ws.fail_connection`；`ws.recv_streaming`；`ws.send(str)`（motd） | 3 |
| `hetu/server/web.py` | `Blueprint`、`Request`、`text` | 6 |
| `hetu/webext.py` | 公开 API：`define_route(uri, **sanic_kwargs)`（Sanic 的 handler 签名）、`on_server_setup(app: Sanic)`；`apply_to` 里用 `Blueprint` | 6（破坏性变更） |
| `hetu/endpoint/context.py:49` | `ctx.request: sanic.Request`（文档标注 unsafe） | 3（类型）、6（对外） |
| `hetu/system/future.py:586` | `future_call_task(app)` 用到 `app.ctx.table_managers`、`app.config["NAMESPACE"]` | 1 |
| `hetu/safelogging/formatter.py` | 基类 `sanic.logging.formatter.AutoFormatter` | 2 |
| `hetu/safelogging/default.py` | `DEFAULT_LOGGING_CONFIG` 直接引用 `sanic.logging.formatter.AutoFormatter` | 2 |
| `hetu/safelogging/filter.py:23`、`hetu/data/backend/worker_keeper.py:86` | `SANIC_WORKER_IDENTIFIER`（日志 ident、SQLite 开发模式下的 worker id） | 2 |
| `hetu/server/threaded_accept.py` | 在 `before_server_start` 里安装；依赖 Windows 上的 selector 事件循环 | 4a、5 |
| `hetu/CONFIG_TEMPLATE.yml` | `INSPECTOR`、`PROXIES_COUNT`、`CERT_CHAIN`、`ACCESS_LOG`、`WEBSOCKET_MAX_SIZE` 由 Sanic 读取 | 2 |
| `pyproject.toml` | `sanic~=25.3`；dev 组的 `sanic-testing`；aiohttp 只在 dev 组；uvloop 只是 sanic 的传递依赖（非 Windows） | 4a、6 |

只在注释或文档里提到 Sanic 的地方：`snowflake_lease.py:115/134`、`main.py:137`（这条注释与事实不符，见 §3.4）、
`common/helper.py:112-120`、`safelogging/handlers.py:46`、`system/future.py:663-666`、
`docs/{zh,en}/{advanced,getting-started,operations}.md`、`hetu/llms.txt`。

### 3.2 Sanic 隐式提供、HeTu 依赖的行为

以下都已对照 Sanic 25.12.1 源码核实，关服顺序还做过实测（§15）。迁移时最容易在不知不觉中丢掉的就是这些，
每一条都要落实到某一步：

1. **断线时取消连接协程**。传输层一关，Sanic 就取消 ws handler 协程。而且对端关闭后，`recv_streaming` 不会
   返回（`sanic/server/websockets/frame.py:196-200`；`receiver.py:303-304` 的注释也这么写），所以在 Sanic 上
   拆连接只能靠取消 handler。`websocket.py:303` 在 finally 里用 shield 包住清理任务、`receiver.py:430` 在
   `QueueFull` 时的兜底，都依赖这一点。aiohttp 默认 `handler_cancellation=False`，不会取消。
2. **两种任务**：
   - loop 启动前 `add_task` 的任务（`main.py:340-344` 的 FutureCalls、续约、水位）走 delayed-task 路径：
     由 `after_server_start` 派发（`sanic/app.py` 的 `dispatch_delayed_tasks`），**不进任务表**，也不允许起名
     （`app.py:1766-1775`）。Sanic 从不取消它们，loop 关闭时直接销毁。
   - 运行中带名字的 `add_task`（各连接的 `client_handler:{request.id}`）会进任务表；不带名字的
     （`recheck_alive`）不进。
3. **ack 时机**。`before_server_start` 跑完就 ack（`sanic/server/runners.py:296-297`），此时还没开始监听。
4. **关服顺序**（实测 uvloop：SIGTERM 后约 2ms 客户端就收到 1001，早于 on_disconnect；单 worker 约 0.35 秒
   退出，2 个 worker 约 0.5 秒）：
   1. `app.stop()` 先调 `shutdown_tasks(timeout=0)`（`sanic/mixins/startup.py:620`），取消任务表里的任务，
      也就是各连接的接收协程；
   2. `before_server_stop`：Sanic 自带的 `_cancel_websocket_tasks`（`app.py:845`）取消各 ws handler 任务；
   3. `_cleanup`：停止监听；`close_if_idle` 对还开着的 ws 发 1001（`websocket_protocol.py:91-100`）；剩下的连接
      最多再等 `GRACEFUL_SHUTDOWN_TIMEOUT`（只有不回 close 帧的客户端会拖到这里），之后强制
      `fail_connection(1001)`；
   4. `after_server_stop`：`worker_close` → `wait_connections_closed` → 释放租约、写精确水位 → 关后端。此时
      FutureCalls、续约、水位三个任务**还在跑**（见第 2 条和 §3.4）。

   Windows 开发环境（asyncio 事件循环）上，`Server.wait_closed()` 会等所有连接都断开，客户端要等清理完成后
   才收到 1000。
5. **任务表持有强引用**：只对进了任务表的有名任务成立。delayed-task 里的异常会打出
   "Task exception was never retrieved"。HeTu 的各个后台循环都自己兜住了异常。
6. **ws 默认值**：
   - ping 间隔 / 超时为 20 / 20 秒，ping 超时时发 1011（`impl.py:246`）；
   - `WEBSOCKET_MAX_SIZE`：Sanic 默认 1MB，模板里设的是 0.5MB；超限的判断是 `>`（websockets 的
     `frames.py:313`）；
   - 不协商 permessage-deflate；
   - `recv(timeout)` 超时返回 None；
   - `fail_connection` 同步、立即生效、可以重复调用；关闭握手超过 10 秒就 `transport.abort()`
     （`impl.py:279-280`、465）。
7. **client_ip**（`sanic/headers.py:406-441`、472-487，`sanic/request/types.py` 的 `client_ip`）：
   - `REAL_IP_HEADER` 优先。否则在设了 `PROXIES_COUNT=N` 时，读 `FORWARDED_FOR_HEADER`（默认
     `X-Forwarded-For`）的所有头，合并后按逗号切分、去掉空白，取倒数第 N 个。`FORWARDED_SECRET` 用于启用
     RFC 7239 的 `Forwarded` 头。
   - 地址会被规范化：丢掉 "unknown"、IPv6 加方括号、转小写。HeTu 依赖带方括号的 IPv6
     （`hetu/endpoint/connection.py:60`、86）。
   - 都取不到时，退回直连对端的地址。
8. **事件循环**：
   - Linux / macOS 上用 uvloop；
   - Windows 上设 `WindowsSelectorEventLoopPolicy`（`sanic/server/loop.py` 的 `try_windows_loop`），
     `threaded_accept` 依赖它；
   - Windows 上还有 `ctrlc_workaround_for_windows`（`sanic/compat.py:159-180`）：每 0.1 秒唤醒一次，让 Ctrl+C
     能生效；第一次 Ctrl+C 优雅停机，第二次强制退出。
9. **日志**：
   - 由 `Sanic(app_name, log_config=...)` 在 `worker_main` 里执行 `dictConfig(LOGGING)`，
     `test_headless_ws.py` 依赖这个时机；
   - AutoFormatter 的输出依赖 `setup_logging` 设置的状态：LOG_EXTRA（等于 debug），以及 `SANIC_LOG_EXTRA` /
     `SANIC_NO_COLOR` 环境变量（`mixins/startup.py:658`）。
10. **worker 标识**：worker 进程里有 `SANIC_WORKER_IDENTIFIER`（"Srv 0" 等），启动 worker 后 Sanic 会把它从
    master 的环境里删掉（`process.py:81-82`）。管理进程和单进程模式下没有这个变量，日志显示 "Main "，序号为 0。
11. **多进程**：
    - 管理进程也会跑一遍 `worker_main`（`AppLoader.load`），因此会加载 app 文件、执行 `on_server_setup`；
    - worker 用 spawn 方式启动，监听 socket 共享给各 worker；
    - worker 里 `app_loader.load()` 也在 try 里，任何启动异常都会发 `__TERMINATE_EARLY__`，整个服务以非 0 退出
      （`worker/serve.py:52-56`、140-145）；
    - Sanic **不会**自动重启崩溃的 worker，manager 只把它标成 FAILED；
    - `DEBUG` 开启且 workers>1 时自动重载，监视的是 `sys.modules` 里所有文件（`reloader.py:79-108`）；
    - `app.m.restart()` 只重启本 worker；单进程模式下没有 `app.m`（`worker/serve.py:100-106`）；
    - Windows 上停 worker 是 TerminateProcess 硬杀，`helper.py` 的 `lease_owner_exited` 就是为此去认领残留的租约。
12. **HTTP**：
    - `REQUEST_MAX_SIZE` 为 100MB；aiohttp 的 `client_max_size` 默认 1MB；
    - `REQUEST_TIMEOUT` 为 60 秒，也管握手前空闲的连接；aiohttp 这里只有 `keepalive_timeout`，3.14.5 的
      默认值是 3630 秒（`web_protocol.py:182`）；
    - 默认能匹配带尾斜杠的路径，`/hetu/x/` 实测返回 200；aiohttp 的 `{db_name}` 匹配不上，会 404。
    - 以上只影响 `define_route` 和 ws 握手。
13. **`request.id`** 取自客户端的 `X-Request-ID` 头（`sanic/request/types.py:499`），见 §3.4。

### 3.3 测试的耦合

- `test_websocket.py`：34 处 `test_server.test_client.websocket(path, mimic=routine)`、2 处 `.get()`，都走
  sanic_testing（必须保留的语义见 §5.0）。此外：
  - 直接用 `test_server.config.WEBSOCKET_MAX_SIZE`、`test_server.ctx.table_managers`；
  - 直接读 `websocket_server._live_connections` / `_cleanup_tasks`（716）；
  - monkeypatch 了 `DISCONNECT_SYSTEM`（694）、`SubscriptionBroker`（1849）、`_cleanup_connection`（1880-1886）。
    这几个名字必须在 `websocket.py` 模块级保留、能被替换，第 1、3 步挪代码时受这条约束。
  - PR #225 之后又多了 2 处 `websocket`（其中一处给客户端传 `extra_headers`）、1 处 `.get()`，并用到
    `test_server.ctx` 上的 `snowflake_lease`、`exit_code`。第 0 步按合并后的代码改。
- `test_headless_ws.py`：`worker_main` + `test_client.websocket`。
- `test_webext.py`：不需要数据库（`BACKENDS: {}`，只建 app、不起服务），直接检查 Sanic 的路由对象。它的 app
  文本里写的是 `from sanic import ...`（`:38`、165）。
- `test_system_startup.py`：测 `resolve_ack_threshold` 的 0.1 秒单位、`StartupAborted` 的语义，用了假的
  `app.ctx`。`test_system_future.py`、`test_common.py:14,429-478`：用假的 app 调用 `future_call_task`、
  `start_backends` / `close_backends`。
- `test_safelogging.py`、`test_common.py`：测 AutoFormatter 的行为和 `SANIC_WORKER_IDENTIFIER` 的解析。
- `test_cli_debug.py`、`test_headless_process.py`：守门检查"不加载 sanic"。
- **多进程（WorkerManager）目前没有任何自动化测试**。
- CI 只在 Linux 上跑（`ci.yml`，超时 20 分钟）。xdist 按（文件，backend_name）分组（`conftest.py:59-65`），
  `test_websocket.py` 只有一个组。Windows 没有 CI。

### 3.4 迁移中发现的现存问题（已由 PR #225 修复）

1. **关服时后台任务和释放租约并发**。FutureCalls、续约、水位不受 Sanic 管理（§3.2 第 2 条）。`worker_close`
   释放租约、写精确水位、关后端的时候它们还在跑：FutureCalls 可能在释放之后还在发号；续约可能对已释放的 id
   触发 `on_lost` → `app.m.restart()`。`main.py:137` 注释里"sanic 已取消后台任务"这一前提不成立。
2. **启动慢时发不出号**。拿到租约 45 秒后围栏生效（`WORKER_ID_EXPIRE_SEC - FENCE_MARGIN_SEC`，
   `redis/worker_keeper.py:154`），续约却要等开始监听后才启动；而 `STARTUP_TIMEOUT` 默认 60 秒，on_start 超过
   45 秒就发不出号。
3. **连接的任务名可以被客户端控制**。`client_handler:{request.id}`（`websocket.py:157`）里的 id 来自 `X-Request-ID`
   头。同名任务会覆盖登记，一个连接的清理会取消另一个连接的接收协程，被覆盖的那个则泄漏。
4. **单进程模式下丢租约会抛异常**。单进程没有 `app.m`，`app.m.restart()` 抛 AttributeError。

四条依次由 PR #225 的 `11b50349`、`f605e92a`、`820a0cc9`、`6b5e58da` 修复。修复之后：

- FutureCalls 改由 `after_server_start` 启动，`worker_close` 在释放租约之前停掉它；
- 续约、水位预留两个循环在 `start_backends` 拿到租约时就启动（`SnowflakeLease.start_loops`），释放租约前
  停掉（`stop_loops`）；
- 连接的任务名改用进程内序号；
- 单进程模式丢租约时，记下 `app.ctx.exit_code` 并停服，`hetu start` 以非 0 退出。

第 1 步把这些逻辑原样搬进 runtime（§5.1）。

## 4. 总体思路

### 4.1 分层

```
hetu/server/
  runtime.py        WorkerRuntime：状态、生命周期、任务表、连接表                （第 1 步）
  config.py         HeTuConfig                                                  （第 2 步）
  conn.py           WsConnection 接口、ConnectionClosed                          （第 3 步）
  websocket.py      serve_connection / send_loop，与框架无关
  receiver.py       client_handler，与框架无关
  adapters/
    sanic_app.py    Sanic 适配：建 app、接线、SanicWsConnection（第 1~3 步收拢进来，第 6 步删除）
    aiohttp_app.py  aiohttp 适配：建 app、AiohttpWsConnection、单进程运行           （第 4a 步）
  supervisor.py     多进程管理                                                   （第 5 步）
```

第 3 步做完后，`import sanic` 只剩这几处：`adapters/sanic_app.py`、`cli/start.py` 里的 Sanic 分支、`web.py`
和 `webext.apply_to`（这两处第 6 步随 webext 一起改），以及 harness 的 Sanic 实现。

### 4.2 顺序：先抽象，后替换

- §3.4 的现存问题已在第 0 步之前单独修掉（PR #225，§13 第 7 条）。
- 第 0~3 步都在 Sanic 上做。其中第 2 步删掉几个配置项，是有意的行为变化；其余都是纯重构，由现有测试
  （只改调用方式）验证。
- aiohttp 的代码从第 4a 步才出现，通过开关和 Sanic 并存，同一套测试在两边各跑一遍（§4.3）。
- 否决"先换蓝图"：蓝图挂在 Sanic 的监听 socket 上，换它就得同时换服务器、生命周期、任务、ctx 和多进程，
  中间没有测试能全绿的状态（§11）。

### 4.3 两套实现并存时的开关（第 4a 步到第 6 步之间）

- 运行时：用环境变量 `HETU_HTTP_SERVER=sanic|aiohttp`。第 6 步之前默认 `sanic`。它只在过渡期使用，
  不写进配置模板和文档。
- 测试：`HETU_TEST_HTTP_SERVERS` 取 `sanic,aiohttp` 的子集，不设置时两个都跑，写法和 `HETU_TEST_BACKENDS`
  一样（`tests/fixtures/backends.py:177`）。CI 两个都跑。
- xdist 的分组键要加上栈，否则 `test_websocket.py` 一个组要跑两遍，成了最长的那条链。还要复核 `ci.yml` 的
  20 分钟超时够不够。
- 第 6 步删除这个开关。

## 5. 分步设计

### 5.0 第 0 步：测试脱钩

新增 `tests/fixtures/server_harness.py`：

```python
class ServerHarness:
    """起停一次 HeTu 服务、跑客户端 routine。第 0 步内部用 sanic_testing，第 4a 步加 aiohttp 实现。"""

    config: MutableMapping[str, Any]  # 起服前可以改（如 WEBSOCKET_MAX_SIZE）
    runtime: Any  # 服务端运行时状态：第 0 步是 Sanic 的 app.ctx，第 1 步换成 WorkerRuntime

    def websocket(self, path: str, mimic, **connect_kwargs) -> WsResult: ...  # .client_sent / .client_received
    def get(self, path: str) -> HttpResult: ...  # .status / .text
    def stop(self) -> None: ...
```

`connect_kwargs` 原样传给客户端的 `connect()`，比如 `extra_headers`。

**harness 的语义**，写在 `tests/fixtures/server_harness.py` 的模块说明里，两个实现都必须满足：

1. 每次调用 `websocket()` / `get()` 都完整起停一次服务（包括启动和关服钩子），每次一个新的事件循环。
   每次都用新的随机端口：sanic_testing 会记住第一次随机到的端口，同一个测试里第二次调用会 bind 失败，
   harness 每次把它清掉。
2. routine 跑在服务器的同一个事件循环里，服务开始监听、后台任务起来之后才开始（sanic_testing 里它是
   `after_server_start` 监听器，`testing.py:69`）。测试依赖这一点：routine 和服务端的 monkeypatch 共享
   `asyncio.Event`（`test_websocket.py:1374` 等），而且会在 routine 里读服务端状态（716、1925）。
3. routine 抛出的 `ConnectionClosedOK` 被吞掉；其他异常等服务停下之后**原样**抛给测试。sanic_testing
   原本会包成 ValueError（`testing.py:243`），harness 改成原样抛出：失败信息更清楚，aiohttp 实现也不用
   模仿这层包装。
4. routine 返回后，先关掉它开的所有客户端连接，再停服务。
5. Linux / macOS 上跑 uvloop。

第 1、3 条由新增的 `test_harness_*` 守住；第 2、4 条由依赖它们的现有用例守住
（`test_lease_lost_in_single_process_stops_with_failure_code`、`test_shutdown_waits_for_connection_cleanup` 等）。

第 4a 步的 aiohttp 实现：每次调用都新建 loop、runtime 和 `web.Application`。一个 Application 只能绑定一个
loop（`web_app.py:264-270`）。

改动：

- `setup_websocket_proxy` 里客户端 pipeline 的逻辑（握手、编解码、分片发送）搬进 harness，两个实现共用。
- `test_server` 和 headless 的 fixture 改为返回 harness。测试里需要改的写法：
  - `test_server.test_client.websocket(` 改成 `test_server.websocket(`，返回值不再是 `(request, response)`
    元组，直接是结果；
  - `response.body.decode()` 改成 `response.text`；对框架 request 对象的断言删掉；
  - `test_server.config.X = ...` 改成 `test_server.config["X"] = ...`；
  - `test_server.ctx.*` 改成 `test_server.runtime.*`。第 0 步它就是 Sanic 的 `app.ctx`，第 1 步换成
    WorkerRuntime，测试不用再改。
- `test_webext.py` **留到第 6 步再改**：它现在不需要数据库，app 文本里写的是 `from sanic import ...`，
  第 6 步之前改不掉。

验收：全部测试照旧通过；`test_websocket.py`、`test_headless_ws.py` 不再 import sanic / sanic_testing，
也不再用 `test_client`（docstring 里讲服务端行为时提到 Sanic 的不算）。

### 5.1 第 1 步：WorkerRuntime

新增 `hetu/server/runtime.py`：

```python
class TaskRegistry:
    """HeTu 自己的任务表，两个栈共用。"""

    def spawn(self, coro, *, name: str | None = None) -> asyncio.Task: ...
    async def cancel(self, name: str) -> None: ...  # 按名取消并等它结束；不存在就什么也不做
    async def cancel_all(self, timeout: float) -> None: ...

class WorkerRuntime:
    """一个服务实例一个，持有 HeTu 在一个 worker 里的全部运行时状态。"""

    config: Mapping[str, Any]
    backends: dict[str, Backend]
    default_backend: Backend
    table_managers: dict[str, ComponentTableManager]
    snowflake_lease: SnowflakeLease
    tasks: TaskRegistry  # 连接相关的任务（接收协程、recheck_alive）
    background: TaskRegistry  # FutureCalls、续约、水位
    on_lease_lost: Callable[[], None]  # 由适配层注入

    async def start(self) -> None: ...
    def start_background(self) -> None: ...  # 开始监听之后：FutureCalls、雪花水位
    async def wait_connections_closed(self, timeout: float) -> bool: ...
    async def stop(self) -> None: ...
```

**`start()`**：内容是现在的 `worker_start`，有两处改动：

- **拿到租约后立即启动续约**，不再等开始监听（修 §3.4 第 2 条）。
- `start_backends` 里的 `app.stop()`（`main.py:115`）不搬进 runtime，改成直接抛 `StartupAborted`。

**任务表**：用 HeTu 自己的实现，两个栈共用，不再交给 Sanic。

- 持有强引用，任务结束后自动移除，未处理的异常打 `logger.exception`。
- 名字在一个 runtime 内唯一：重名直接报错。
- 连接任务改用 runtime 内的计数器命名，例如 `client_handler:{seq}`，不再用 `request.id`（修 §3.4 第 3 条）。
- Sanic 的 `shutdown_tasks` 从此看不到 HeTu 的任务。接收协程改由连接清理来取消（`runtime.tasks.cancel(...)`），
  时间点在 `_cancel_websocket_tasks` 取消 handler 之后。和现在一样，都发生在关服一开始；现有测试会守住这一点。

**`stop()` 固定顺序**（修 §3.4 第 1 条）：

1. `wait_connections_closed`；
2. 取消并 await 后台任务（FutureCalls、续约、水位）；
3. 释放租约，写精确水位；
4. 关后端。

**连接表**：

- `websocket.py` 模块级的 `_live_connections`、`_cleanup_tasks`、`wait_connections_closed` 搬进 runtime，
  每个服务实例一份。之后不再需要"只算本 loop 的任务"那段过滤，测试里先后起的服务各用各的 runtime。
- `DISCONNECT_SYSTEM`、`SubscriptionBroker`、`_cleanup_connection` 仍保留为 `websocket.py` 模块级的名字（§3.3）。

**Sanic 适配**（`adapters/sanic_app.py`，由现在的 `worker_main` 拆出来）只负责接线：

- `before_server_start` 调 `runtime.start()`；
- `after_server_start` 调 `runtime.start_background()`，要注册在 `webext.apply_to` 之前，保持和用户钩子的
  先后顺序；
- `after_server_stop` 调 `runtime.stop()`；
- `app.ctx.runtime = runtime`；
- `on_lease_lost`：多进程时调 `app.m.restart()`；单进程时走优雅停机，再以非 0 退出（修 §3.4 第 4 条）。

**其他调用点**：

- `websocket.py` 里的 `request.app.ctx.*` 改走 runtime，`add_task` / `cancel_task` / `purge_tasks` 改走
  `runtime.tasks`；
- `future_call_task(app)` 改成 `future_call_task(runtime)`；
- `test_system_future.py`、`test_common.py`、`test_system_startup.py` 里的假 app 改成假 runtime。

这一步 `runtime.config` 仍是 Sanic 的 `app.config`（Mapping），第 2 步再换。

**新增测试**：

- 释放租约时，后台任务都已经结束；
- on_start 超过围栏时间后仍能发号（测试里缩短常数）；
- 两个连接带相同的 `X-Request-ID` 时互不影响；
- 单进程模式下丢租约，以非 0 退出。

§3.4 的四个修复已由 PR #225 先行合并（§13 第 7 条）。上面列的修复点和测试都已存在，这一步只把它们搬进
runtime。

### 5.2 第 2 步：配置、日志、worker 序号，删除 TLS / 访问日志 / Inspector

这一步不是纯重构：删配置项是有意的行为变化。

**配置**

- 新增 `hetu/server/config.py` 的 `HeTuConfig`：一个 dict，可以按属性读大写键（`config.NAMESPACE`），
  `update_config()` 只收大写键，和 Sanic 的语义相同。`start.py` 里的 `sanic.config.Config` 换成它。
- 原来由 Sanic 读取、迁移后还保留的键，由 HeTu 给出默认值，免得 Sanic 一走默认值就悄悄变掉：

  | 键 | 默认值 | 说明 |
  |---|---|---|
  | `GRACEFUL_SHUTDOWN_TIMEOUT` | 15.0 | 同 Sanic |
  | `WEBSOCKET_MAX_SIZE` | 2\*\*20 | 同 Sanic，模板里仍是 0.5MB |
  | `WEBSOCKET_PING_INTERVAL` | 20 | 同 Sanic |
  | `WEBSOCKET_PING_TIMEOUT` | 20 | 只对 Sanic 栈生效；aiohttp 上不可配，等 pong 的时间是 heartbeat/2（§6） |
  | `PROXIES_COUNT` / `REAL_IP_HEADER` / `FORWARDED_FOR_HEADER` | 0 / None / `X-Forwarded-For` | 同 Sanic（§3.2 第 7 条） |
  | `REQUEST_MAX_SIZE` | 100MB | 同 Sanic，第 6 步映射到 aiohttp 的 `client_max_size`（§14 第 5 条） |

- Sanic 适配层继续用 `app.update_config(...)` 把这些键交给 Sanic，Sanic 栈的行为不变。
- Sanic 的 `Config` 会自动读取 `SANIC_` 前缀的环境变量，换掉之后这个能力就没了。HeTu 的文档没写过它，
  写进发版说明即可。

**删掉的配置**（§13）

判断"配了"一律看值的真假：CLI 路径总会传 `CERT_CHAIN: ""`、`ACCESS_LOG: False`，模板里也有
`ACCESS_LOG: false`，这些都不能触发。

- `CERT_CHAIN` 有值，或者给了 `--cert`：**拒绝启动**，报错提示在 Caddy 处终止 TLS（§14 第 2 条）。`--cert`
  先保留为隐藏参数（`help=SUPPRESS`），只用来给出这条报错，下个版本再删。
- `ACCESS_LOG`、`INSPECTOR`、`INSPECTOR_PORT` 为真：打一条警告，然后忽略。
- Sanic 栈固定 `access_log=False`、`ssl=None`、`auto_tls=False`，`INSPECTOR` 不再传给 Sanic。
- 模板和文档（`operations.md` 中英文里 `--cert` 那一行）同步删除。

**日志**

- `hetu/safelogging/formatter.py` 的 `ContextAutoFormatter` 不再继承 Sanic 的类，自己实现带颜色的格式，
  输出和 Sanic 25.12 的 AutoFormatter 一致。可以移植它的实现（MIT 许可，注明出处）。类名和路径不变，因为
  用户的配置里写的就是 `hetu.safelogging.formatter.ContextAutoFormatter`。
- 加 golden test，把 LOG_EXTRA（等于 debug）、`SANIC_NO_COLOR` 这些影响输出的状态固定下来（§3.2 第 9 条）。
- `DEFAULT_LOGGING_CONFIG` 改用 HeTu 的类。
- **兼容处理**：
  - 在 `dictConfig` 之前，把 formatter 里的 `sanic.logging.formatter.AutoFormatter`（包括 `class` 和 `()` 两种
    写法）改写成 HeTu 的类，并打一次警告。2026-09-07（commit 85958ba2）之前的模板写的就是 Sanic 的类。
  - LOGGING 里引用的其他 `sanic.*` 类：这一步先打警告，第 6 步改为给出明确报错，不能等到 ImportError。
- `dictConfig` 改由 HeTu 自己调用，时机和现在一样：`worker_main` 里、建 app 之前。传给 Sanic 的是
  `configure_logging=False`。
- `sanic` logger 重定向到 `HeTu.root` 的逻辑留在 Sanic 适配层；第 4a 步 aiohttp 适配层对 `aiohttp` 的 logger
  做同样处理。

**worker 序号**

- 改用 HeTu 自己的环境变量 `HETU_WORKER_INDEX`（从 0 开始的整数），第 5 步由 supervisor 设置。读取方有两个：
  - `ContextFilter.IDENT`：设了就显示 "Srv {n}"，没设显示 "Main "；
  - `FixedWorkerKeeper`：取 worker 序号。
- 过渡期：没设 `HETU_WORKER_INDEX` 时，照旧解析 `SANIC_WORKER_IDENTIFIER`。这个回退在第 6 步删除。
  `test_common.py` 两种情况都要测。

### 5.3 第 3 步：ws 连接抽象（仍跑在 Sanic 上）

新增 `hetu/server/conn.py`：

```python
class ConnectionClosed(Exception):
    """对端已断开或本端已关闭。替代 sanic.exceptions.WebsocketClosed。"""

class WsConnection(Protocol):
    conn_id: str  # 本 worker 内唯一，由 runtime 的计数器生成（§5.1）
    client_ip: str  # 已经按 §3.2 第 7 条解析过
    request: Any  # 框架的原始请求，放进 ctx.request（unsafe）

    async def recv(self, timeout: float | None = None) -> str | bytes | None: ...
    async def send(self, data: str | bytes) -> None: ...
    def fail(self, code: int, reason: str) -> None: ...
    def protocol_error(self) -> BaseException | None: ...
```

接口语义：

- `recv`：返回一整条消息；超时返回 None。连接断开后的行为**两个栈不同**：
  - aiohttp：立即抛 `ConnectionClosed`；
  - Sanic：可能一直挂着（§3.2 第 1 条），靠取消 handler 兜底。
- `send`：连接已断开时抛 `ConnectionClosed`。
- `fail`：同步调用，尽力发出 close 帧后断开；可以重复调用；保证在有限时间内断开，不受对端配合与否影响
  （aiohttp 的实现见 §5.4.2）。
- `protocol_error`：只返回协议层错误，替代 `ws.ws_proto.parser_exc`。对端正常掉线不算。

改动：

- `websocket_connection` 的函数体改名为 `serve_connection(runtime, conn, instance)`，不再依赖框架。Sanic 的
  路由 handler 只负责包装：
  `await serve_connection(request.app.ctx.runtime, SanicWsConnection(request, ws, conn_id), db_name)`。ws 路由
  改由适配层显式注册，不再在 import 时注册。
- **显式管住收发两边**：
  - `serve_connection` 把 `send_loop` 也作为任务启动，用 `asyncio.wait({send_task, recv_task},
    return_when=FIRST_COMPLETED)` 等任意一边结束，再取消另一边。
  - `asyncio.wait` 自己被取消时，不会取消它在等的任务。所以 handler 被取消时（Sanic 上仍会发生），finally 里
    要显式 cancel 并 await 收发两个子任务，而且要在 `broker.close()` / `terminate()` 之前完成，保持现在
    "先等接收协程结束、再退订"的顺序。shield 包住的清理流程保持不变。
  - 动机：aiohttp 上真实存在的卡死。接收协程退出时，如果队列已满，PUSH_CLOSE 会被 `put_nowait` 丢掉
    （`receiver.py:427-430`）；与此同时发送循环卡在 send 的 drain 上（对端不读数据，见 §5.4.2）。两件事叠在
    一起，清理就永远不会执行，Connection 行会漏删。
  - 订阅回复的占位 future 不会造成卡死：deferred 任务一结束，`settle` 回调就会把它取消
    （`receiver.py:166-170`）。
- `close_connection(ws, code)` 改成 `conn.fail(code, reason)`；`WebsocketClosed` 改成 `ConnectionClosed`；
  `receiver.py` 里 `except (SanicException, BaseException)` 去掉 `SanicException`（`BaseException` 已经能接住）；
  motd 改成 `conn.send(str)`。
- `ctx.request` 的类型标注改成 `Any`（框架的原始请求）。在 Sanic 栈上仍是 Sanic 的 Request，对外不变。
- 新增单元测试：用一个内存里的假 `WsConnection`，不走网络，覆盖：
  - 握手超时、握手格式错误；
  - 收发频率超限、被顶号、初始化失败时各自的 close 码；
  - 收发途中断开；
  - 上面那种"队列满 + send 卡住"的场景：断开后清理必须执行完，Connection 行要删掉。

### 5.4 第 4a 步：aiohttp 单进程，与 Sanic 并存

#### 5.4.1 app、路由与客户端地址

`adapters/aiohttp_app.py` 的 `build_app(runtime) -> web.Application`：

- `GET /hetu/{db_name}`，同时注册带尾斜杠的 `/hetu/{db_name}/`，和 Sanic 的行为一致（§3.2 第 12 条）：
  1. 先 `ws = web.WebSocketResponse(...)`（参数见 §5.4.2），再 `await ws.prepare(request)`；
  2. 然后 `await serve_connection(runtime, AiohttpWsConnection(...), request.match_info["db_name"])`；
  3. 最后返回 ws。
- `GET /`：`web_root`。
- 用户的 webext 路由在这一步**不支持**：现在的声明是 Sanic 形状的（handler 签名、`<name:str>` 路径语法），
  同一个函数没法同时给两个框架用。`HETU_HTTP_SERVER=aiohttp` 时如果有 `define_route` / `on_server_setup` 注册，
  打警告并跳过。
- 客户端地址按 §3.2 第 7 条完整实现：`REAL_IP_HEADER`、`FORWARDED_FOR_HEADER`、`PROXIES_COUNT`，以及地址
  规范化（丢掉 "unknown"、IPv6 加方括号、转小写）。
  - `FORWARDED_SECRET` 不实现，aiohttp 栈上配了就拒绝启动，免得静默退回代理地址。不然的话：Caddy 在本机时，
    地址变成 127.0.0.1 被豁免，匿名连接数限制等于失效；Caddy 在别的机器时，所有匿名用户共用代理的 IP，会被
    `MAX_ANONYMOUS_CONNECTION_BY_IP` 一起挡掉。
  - 注意：HeTu 如果绕过 Caddy、被直接访问，`X-Forwarded-For` 是可以伪造的，可以借此绕过按 IP 限制的匿名连接
    数。这是现有行为，文档提示用防火墙挡住直连。

#### 5.4.2 ws 参数与 `fail()`

和 Sanic 不一样的默认值必须显式设置：

| aiohttp 参数 | 取值 | 原因 |
|---|---|---|
| `compress` | `False` | 默认 True。浏览器一定会请求 permessage-deflate，会和 HeTu 自己的 zlib 层重复压缩 |
| `max_msg_size` | `WEBSOCKET_MAX_SIZE + 1` | 默认 4MB。aiohttp 判断超限用的是 `>=`（`reader_py.py:533`），加 1 才能保持"刚好等于上限的消息能收"。超限时以 1009 关闭（`test_websocket_fragmented_message_size_limit`） |
| `heartbeat` | `WEBSOCKET_PING_INTERVAL` | 默认不发 ping，半开的连接会一直挂着。等 pong 的时间是 heartbeat/2，超时后直接关 transport，不发 close 帧（`web_ws.py:254-264`），见 §6 |
| `autoping` | `True` | 回应客户端发来的 ping |
| `timeout` | 短，如 2 秒 | 关闭时等对端 close 帧的时间，默认 10 秒 |

**`fail()` 的实现**：aiohttp 的 `close()` 不保证能断开。`_writer.close()` 和 `drain()` 不受任何超时约束
（`web_ws.py:542-548`），`timeout` 只管读对端 close 帧的那一段（572-578），而 `_set_closed()` 还会取消心跳
（266-272）。审查实测：对端停止读数据时，`close()` 一直不返回，transport 也不关；Sanic 在同样场景下 10 秒后会
abort。所以：

1. 同步设置 `_failed` 标志，此后 `send` / `recv` 立即抛 `ConnectionClosed`；
2. `transport.pause_reading()`；
3. 起一个关闭任务调用 `ws.close(code, message)`，并持有它的强引用；
4. `loop.call_later(close_timeout, transport.abort)` 兜底。

**其他**：

- `protocol_error()` 只返回 `WebSocketError`。对端直接断开时 `ws.exception()` 是 ConnectionResetError，心跳
  超时时是 TimeoutError，都不算协议错误，否则 `websocket.py:284-291` 每次掉线都会打一条堆栈。
- `AiohttpWsConnection.recv` 把 aiohttp 的 `CLOSE` / `CLOSING` / `CLOSED` / `ERROR` 消息转成
  `ConnectionClosed`。分片由 aiohttp 自己重组，`max_msg_size` 限制的是整条消息的长度。

#### 5.4.3 依赖、事件循环与单进程运行

**依赖**：这一步就把 `aiohttp~=3.14` 和 `uvloop; sys_platform != 'win32'` 加进运行时依赖。否则用
`pip install .` 构建的 Docker 镜像跑不了 aiohttp 栈，没法在线上做 A/B。CI 用 pip 安装，不走 lock。

**事件循环**：

- Linux / macOS 用 `asyncio.Runner(loop_factory=uvloop.new_event_loop)`。Python 3.14 已经弃用
  `set_event_loop_policy`，不要再用。
- Windows 用 `asyncio.SelectorEventLoop`，在开始监听之前安装 `threaded_accept`，并移植 Ctrl+C 的唤醒处理
  （§3.2 第 8 条）。
- `DEBUG > 1` 时调用 `loop.set_debug(True)`，对应 Sanic 的 debug 模式。

**AppRunner 参数**：`access_log=None`（默认是开着的，模板的 root logger 是 DEBUG，会打出访问日志）；
`shutdown_timeout` 设小；`keepalive_timeout` 显式设为 60 秒，对应 Sanic 的 `REQUEST_TIMEOUT`（§3.2 第 12 条）。

**单进程运行**：`HETU_HTTP_SERVER=aiohttp` 且 `WORKER_NUM=1` 时，`hetu start` 在主进程里依次执行：

1. `dictConfig`，启动 `AutoListener`；
2. bind 监听 socket。端口冲突时在这里就失败，不会先跑一遍 on_start；
3. `runtime.start()`。`StartupAborted` 照现在的处理，以 1 退出；
4. `runner.setup()`，在已经 bind 好的 socket 上 `web.SockSite(...).start()`；失败时调用 `runtime.stop()`
   释放租约；
5. `runtime.start_background()`；
6. 等待信号；
7. 走关服流程（§5.4.4）。

第 5 步之前，`WORKER_NUM>1` 配上 `HETU_HTTP_SERVER=aiohttp` 时报错退出（§14 第 6 条）。

#### 5.4.4 关服流程（这一步还不做排空）

由 HeTu 自己编排，不依赖 aiohttp `on_shutdown` 的顺序：

1. 停止监听；
2. 对所有连接调用 `fail(1001)`，在途的 RPC 随之取消，和 Sanic 栈一样；
3. `runtime.wait_connections_closed`（断线 System、删 Connection 行），最多等 `GRACEFUL_SHUTDOWN_TIMEOUT`；
4. `runtime.stop()`：取消后台任务 → 释放租约 → 关后端（§5.1）；
5. `runner.cleanup()`。

保证的性质和第 1 步修好之后的 Sanic 栈相同：拆连接先于关后端（现有测试覆盖，`test_websocket.py:1875` 附近）；
释放租约前后台任务都已结束。

#### 5.4.5 测试

- harness 加上 aiohttp 实现，fixture 按 `HETU_TEST_HTTP_SERVERS` 参数化，ws 测试在两个栈上各跑一遍。
- aiohttp 栈专属的新测试：
  - 客户端请求 deflate 时也不协商压缩（用 `websockets` 客户端带 `compression="deflate"` 连接，断言没有协商
    出扩展）；
  - 不回 pong 的客户端会在 heartbeat 内被断开；
  - **不读数据的客户端被 `fail()` 之后，N 秒内断开**；
  - 客户端地址解析（单元测试）：多个头、条目不够、没有这个头、IPv6、"unknown"、大小写；
  - 各个 close 码确实送到了客户端；
  - 带尾斜杠的路径能连上；不打访问日志；
  - 关服顺序：断线 System 的提交早于关后端，释放租约时后台任务都已结束。

### 5.5 第 4b 步：关服排空（单独的 PR，只在 aiohttp 栈上做）

目标：关服时，在途的 RPC 处理完再断开，而不是直接取消。Sanic 栈做不到，它的 `_cancel_websocket_tasks` 会在
`before_server_stop` 里直接取消 handler，所以 Sanic 栈保持原样，排空的测试只在 aiohttp 栈上跑。

设计：

- runtime 进入 draining 状态。接收协程在两条消息之间停下来：检查标志位，并取消正空闲等待的 `recv`。已经读到
  的消息照常处理完。没读的消息就没执行，客户端重连后重发不会造成重复执行。
- 排空期间的 PUSH_CLOSE 用 `await put` 放进队列，不能用 `put_nowait` 丢掉。
- 发送循环要等占位都完成、把回复发出去之后，才调用 `fail(1001)`。
- 预算不复用 `GRACEFUL_SHUTDOWN_TIMEOUT`，单独设一个短的 `SHUTDOWN_DRAIN_TIMEOUT`（如 5 秒）。关服总时长要放进
  编排器给的宽限期：docker 默认 10 秒，k8s 默认 30 秒。文档里提示设置 compose 的 `stop_grace_period`。

### 5.6 第 4c 步：性能对比

- 用 `benchmark/rpc_throughput.py` 测 hello_world，方法照 `hello_world_optimization_report.md`：服务端绑 P 核
  CPU 3，4 个客户端进程各 16 个连接绑 E 核，5 × 10 秒窗口。用 `HETU_HTTP_SERVER` 切换 sanic / aiohttp，其他
  条件不变。再跑 `sub_scenarios_ws.py` / `sub_budget_ws.py` 测推送每帧的成本。结果写进 §12。
- Sanic 撑不过 Python 3.16，这次迁移是被迫的，所以收益多少不决定做不做。预期约 14µs/RPC（QPS 约 +50%），
  **收益低于 +20% 就先查原因**。**进入第 5 步的条件**是：RPC 和推送的每帧成本、p99 延迟都没有比 Sanic 差
  （§14 第 8 条）。
- 人工端到端测一次：Unity / C# headless 客户端连接、调用、订阅，被顶号时显示 close 码，断线后能重连。

### 5.7 第 5 步：自己的多进程 supervisor（到这一步时再细化）

替代 Sanic 的 WorkerManager，只给 aiohttp 栈用（Sanic 栈继续用它自己的，直到第 6 步）。

**进程模型**

- master 不加载 app 文件，不再有 `AppLoader`（§14 第 3 条）。master 负责：
  - 解析配置，执行 `dictConfig(LOGGING)`（会创建进程安全队列），启动 `AutoListener`；
  - 生成 boot uuid；
  - bind 监听 socket：端口冲突时尽早失败，Windows 上照旧给出 10013 的提示。
- 用 `multiprocessing.get_context("spawn")` 启动 N 个 worker，传入配置、socket、序号和控制管道。
- `HETU_WORKER_INDEX` 要在 `Process.start()` 之前设进 master 的 `os.environ`（spawn 启动时会复制环境变量），
  启动完再从 master 的环境里删掉，Sanic 也是这么做的。因为 `ContextFilter.IDENT` 在 import 时就读取，worker
  里再设就晚了。

**启动**

- worker 在 `runtime.start()` 完成、并且开始监听之后，通过控制管道发 ready。
- **ready 之前的任何退出，都算整体启动失败**：app 导入出错、建簇出错、`PACKET_LAYERS` 配错、`StartupAborted`、
  ack 超时。master 停掉所有 worker，首个 traceback 只打一次，然后以 1 退出。docker / k8s 能识别为失败，也不会
  陷入重启死循环。
- ack 超时为 `STARTUP_TIMEOUT + ACK_TIMEOUT_MARGIN` 秒。`resolve_ack_threshold` 改成返回秒，
  `test_system_startup.py` 跟着改。超时时日志里写明是哪个 worker 卡住了。

**运行中**

- ready 之后 worker 崩溃：用同一个序号重启，带退避和次数上限，超过上限就整体退出。Sanic 不会自动重启崩溃的
  worker（§3.2 第 11 条），这是新行为，要有测试覆盖。
- 所有重启（崩溃、丢租约、重载）都**先停旧进程，再起新进程**。SQLite 的 `FixedWorkerKeeper` 直接拿序号当
  worker id，同一个序号的两个进程并存，就会发出重复的 ID。
- 丢租约：不能在续约任务里 `sys.exit`，SystemExit 会直接冒出 run，跳过关服流程。应该走正常的关服流程
  （§5.4.4），再以约定的退出码退出，由 master 重启。
- master 死掉（被 kill -9、OOM）时，daemon 标志不起作用，worker 会变成孤儿继续监听。所以 worker 读到控制管道
  EOF 时，自己优雅退出。

**停机与信号**

- Unix：
  - 在终端里按 Ctrl+C，SIGINT 会发给整个前台进程组，worker 会同时收到 SIGINT 和 master 转发的 SIGTERM。
    worker 对这两个信号都用 `add_signal_handler` 处理，而且要幂等。
  - Docker 里 PID 1 是 `python -m hetu`，必须显式安装 SIGTERM 处理。
  - master 等到超时后，杀掉还没退出的 worker。超时预算要放进编排器的宽限期（§5.5）。
- Windows：
  - Ctrl+C 会发给控制台里的所有进程，所以 worker 忽略 SIGINT，由 master 通过控制管道通知它们停机。这样
    Windows 上也能优雅停机，不再硬杀。`lease_owner_exited` 保留作兜底。
  - selector 事件循环的 `select()` 只能等 socket，所以等控制管道要用一个线程。
  - master 也需要 Ctrl+C 的唤醒处理（§3.2 第 8 条）。

**socket**

- Linux / macOS：master bind 好之后传给 worker（multiprocessing 支持把 socket 传给子进程）。
- Windows：同一机制走 `socket.share`，情况和 Sanic 相同，所以 `threaded_accept` 继续保留，worker 必须用
  selector 事件循环。
- 默认共享监听 socket；Linux 上的 SO_REUSEPORT 实测后作为可选项（§14 第 7 条）。

**DEBUG 自动重载**

- 和 Sanic 一样，只在 DEBUG 开启且 workers>1 时启用。用轮询 mtime 实现，监视范围同 Sanic：所有已加载模块的
  文件（§14 第 4 条）。有改动就逐个重启 worker，先停旧的再起新的。

**单进程**

- `WORKER_NUM=1` 时不经过 supervisor，在主进程里跑（同 §5.4.3）。

**测试**（全部是新增的，现在一个都没有）

- 在随机端口上起 2 个 worker，两个都能提供服务；
- `kill -9` 其中一个，它会以原序号被重启；连续崩溃达到上限时整体退出；
- SIGTERM 后 Connection 行被清掉、租约被释放；
- 在 ready 之前出错（app 导入错误、`StartupAborted`）：master 很快以非 0 退出，不会重启；
- 启动时卡住，ack 超时，以非 0 退出并给出提示；
- kill master 后，worker 自己退出；
- 各 worker 的 `HETU_WORKER_INDEX` 互不相同。
- Windows 没有 CI，另列一份人工检查清单：多 worker 下的 accept、Ctrl+C、租约释放、重载。

### 5.8 第 6 步：切换默认、删除 Sanic

- 默认值改为 `HETU_HTTP_SERVER=aiohttp`，然后删除：开关本身、`adapters/sanic_app.py`、`start.py` 里的
  Sanic 分支、`SANIC_WORKER_IDENTIFIER` 回退、harness 的 Sanic 实现，以及依赖 `sanic` 和 `sanic-testing`。
- `hetu/` 的运行时代码没有 import `websockets`，把它移到 dev 组。动手前再 grep 确认一遍。
- **webext 改成 aiohttp 的接口**（破坏性变更）：
  - `define_route(uri, *, methods=("GET",), **aiohttp_kwargs)`；
  - handler 签名为 `(request: web.Request) -> web.StreamResponse`，路径参数从 `request.match_info` 取，
    路径语法改为 `{name}`；
  - `on_server_setup(app: web.Application)` 只在 worker 里执行（§5.7）；
  - `REQUEST_MAX_SIZE` 映射到 `client_max_size`（§14 第 5 条）；
  - `web.py`、`webext.apply_to`、`tests/app.py` 里的路由一起改；
  - `test_webext.py` 改成行为测试，通过 harness 发真实请求，后端用 SQLite，不需要 Docker。要验证：路由能访问
    到；`methods` 限制生效（返回 405）；用户注册的 "/" 会替换掉默认欢迎页；占用 `/hetu` 前缀会被拒绝；app 文件
    被重复 exec 时不会重复注册。

  对照示例（写进发版说明和 `advanced.md`）：

  ```python
  # 之前（Sanic）
  from sanic import Request, response

  @hetu.define_route("/download/<name:str>")
  async def download(request: Request, name: str):
      return response.text(f"downloaded {name}")

  # 之后（aiohttp）
  from aiohttp import web

  @hetu.define_route("/download/{name}")
  async def download(request: web.Request):
      return web.Response(text=f"downloaded {request.match_info['name']}")
  ```

- `ctx.request` 变成 aiohttp 的 `web.Request`。
- LOGGING 里引用 `sanic.*` 类的，给出明确报错（§5.2）。
- 文档要改的地方：
  - `advanced.md`（中英文）的 webext 一节；
  - `getting-started.md` 里关于 Sanic 启动横幅的描述；
  - `operations.md` 里 Windows 硬杀那一段，并补充：TLS 和访问日志由 Caddy 负责；Caddy 要设
    `stream_close_delay`，否则每次重载配置都会断开所有 ws；编排器的停机宽限期；
  - `CONFIG_TEMPLATE.yml`、`hetu/llms.txt`、`hetu/skills/building-on-hetu/SKILL.md`。
- 守门测试：`test_headless_process.py`、`test_cli_debug.py` 里"不加载 sanic"的检查，改成"不加载 aiohttp"。
- 版本：下一个 minor 版本，发版说明里列出 §6 的全部变更。

## 6. 对外变更汇总（写进发版说明）

| 变更 | 第几步 | 兼容处理 |
|---|---|---|
| 删除 `CERT_CHAIN`、`--cert` | 2 | 有值就拒绝启动，提示改用 Caddy |
| 删除 `ACCESS_LOG`、`INSPECTOR`、`INSPECTOR_PORT` | 2 | 为真时打一条警告，然后忽略 |
| LOGGING 里的 `sanic.logging.formatter.AutoFormatter` | 2 | 自动改写成 HeTu 的 formatter，并打警告 |
| `SANIC_` 前缀的环境变量配置失效 | 2 | 无 |
| `FORWARDED_SECRET` 不再支持 | 4a（aiohttp 栈）、6 | 配了就拒绝启动 |
| `define_route` / `on_server_setup` 改为 aiohttp 接口 | 6 | 无，发版说明给出对照示例 |
| `ctx.request` 变成 aiohttp 的 `web.Request` | 6 | 无（文档本来就标注 unsafe） |
| `on_server_setup` 只在 worker 里执行，不再在管理进程执行 | 5（aiohttp 栈）、6 | 无 |
| 关服时在途的 RPC 先处理完再断开 | 4b（aiohttp 栈）、6 | 无 |
| ws 心跳超时：客户端看到 1006（aiohttp 直接断开 transport），以前是 1011；`WEBSOCKET_PING_TIMEOUT` 不再可配，等 pong 的时间是 heartbeat/2 | 4a（aiohttp 栈）、6 | 无 |
| Windows 开发环境关服时，客户端收到的 close 码从 1000（清理完才发）变成 1001。C# SDK 对 1000 会把 reason 置为 null（`WebSocketTransport.cs:62-65`），按 reason 判断"正常关闭"的代码会受影响 | 4a（aiohttp 栈）、6 | 无 |
| 崩溃的 worker 会被自动重启（Sanic 不会） | 5（aiohttp 栈）、6 | 无 |

## 7. 测试计划汇总

| 步 | 验证方式 |
|---|---|
| 0 | 全部现有测试照旧通过；新增 `test_harness_*` 守住 harness 的语义（§5.0） |
| 1 | 现有测试（含 §3.4 修复时加的测试）照旧通过；假 app 改成假 runtime |
| 2 | 新增：配置兼容改写、被删除的键（按真值判断，拒绝启动 / 警告）、`HETU_WORKER_INDEX` 和回退、formatter 的 golden test |
| 3 | 新增：用假 `WsConnection` 的单元测试，包括"队列满 + send 卡住"的场景；现有 ws 测试照旧通过 |
| 4a | 整套 ws 测试在两个栈上都通过；aiohttp 专属测试（§5.4.5） |
| 4b | 排空测试（只在 aiohttp 栈）：在途的 RPC 回复送达后才收到 1001；没读的消息不执行；总时长不超过预算 |
| 4c | 性能对比（§5.6），结果写进 §12 |
| 5 | 新增 supervisor 测试（§5.7）；Windows 人工检查清单 |
| 6 | 全部测试只跑 aiohttp；`test_webext.py` 改成行为测试；守门测试改成检查 aiohttp |

每一步都是一个 PR，CI 只在 PR 上跑。docs/api 不提交。新增的用户可见字符串用 `_()` 包起来。

## 8. 提交计划

0. `test/server-harness`：第 0 步
1. `refactor/worker-runtime`：第 1 步（§3.4 的修复在此之前单独合，§13 第 7 条）
2. `refactor/config-logging`：第 2 步，包括删除 `CERT_CHAIN` / `ACCESS_LOG` / `INSPECTOR`
3. `refactor/ws-connection`：第 3 步
4. `feat/aiohttp-server`：第 4a 步，同时把 aiohttp 和 uvloop 加进运行时依赖
5. `feat/aiohttp-drain`：第 4b 步
6. 第 4c 步的性能对比，结果写进 §12，不一定单独开 PR
7. `feat/supervisor`：第 5 步
8. `feat/drop-sanic`：第 6 步

## 9. 风险

- **Windows 没有 CI**：第 3~5 步要按人工清单在 Windows 上验证，包括 Ctrl+C 和多 worker 的 accept。
- **aiohttp 的 ws 语义差异发现得晚**：靠两套实现跑同一套测试，加上假连接的单元测试，在第 3、4a 步提前暴露。
- **关服时间超过编排器的宽限期**：docker 默认 10 秒、k8s 默认 30 秒后强杀，强杀会跳过清理。关服预算要放进
  宽限期，文档里写明怎么配。
- **supervisor 是全新代码，现在没有任何测试覆盖**：第 5 步新增一整套测试，重点覆盖失败路径（§5.7）。
- **两套实现都跑以后 CI 变慢**：调整 xdist 分组（§4.3），复核 20 分钟超时。
- **两套适配层要并存约 4 个 PR**：第 6 步一次性删除。

## 10. 后续工作（不在本次范围内）

- CLI 的 `hetu reload`，替代 Inspector 的 reload 功能，另立 spec。
- 推送的大头是每个连接各做一遍的 zlib 和加密（§1.2）。换框架后它在总成本里的占比会更高，值得单独优化。
- 如果 ws 层还要再省 CPU，可以在 `WsConnection` 抽象的基础上再评估 picows。

## 11. 备选方案与否决理由

- **先换蓝图（网络优先）**：蓝图挂在 Sanic 的监听 socket 上，等于一次性全换，中间没有能全绿的状态。
- **uvicorn + Starlette / FastAPI**：ws 回显成本是 aiohttp 的 2 倍（§1.2）。
- **picows**：最快，但只管 ws，webext 没处安放；只有一个维护者。
- **Granian**：每条消息都要跨一次 Rust↔Python 边界，小包 ws 比 Sanic 好不了多少，推送反而更慢。
- **继续用 Sanic**：最多撑到 Python 3.16（§1.1）。
- **过渡期在 Sanic worker 里另起一个 aiohttp 端口**：多一个端口；Windows 上多进程没法共用同一个端口。
- **用 gunicorn 管进程**：不支持 Windows，也没有 HeTu 需要的 ack 和提前中止语义。
- **每个 worker 用 SO_REUSEPORT 各自 bind**：Windows 不支持，而且 worker 挂掉时排在它队列里的连接会被重置。
  留作第 5 步在 Linux 上的可选项。

## 12. 实测

### 12.1 框架对比（2026-11-14）

见 §1.2。脚本是临时写的，没有提交；方法如下：

- 服务端用 taskset 绑 CPU 3，压测端是 4 个 picows 客户端进程，分别绑 CPU 4、6、8、10，每个进程 16 个连接；
- 先预热 3 秒，再统计 8 秒窗口，用 psutil 统计服务端（含子进程）的 user+sys 时间；
- 当时在 Claude 的沙箱里 perf 读不到计数器，所以用服务端饱和时的 CPU 时间；各组频率都在 4.6~4.8GHz。

### 12.2 第 4c 步性能对比（待测）

### 12.3 第 5 步多 worker（待测）

## 13. 已定事项（2026-11-14）

1. 换成 aiohttp，不用 uvicorn。
2. 先抽象非网络部分（A 方案），第 0 步先做测试脱钩。
3. 第 4 步两套实现并存，测试和压测都做 A/B 对比。
4. 删掉 `CERT_CHAIN`，TLS 交给 Caddy。
5. 删掉 `ACCESS_LOG`，访问日志交给 Caddy。
6. 删掉 `INSPECTOR`，以后由 CLI 提供 reload 命令（另立 spec）。
7. §3.4 的现存问题先单独修，不等迁移；迁移从第 0 步开始，排在修复之后。已由 PR #225 修复。
8. 不做原型，直接从第 0 步开始。

## 14. 待定问题

括号里是审查意见（§15）。

1. 关服排空（第 4b 步）只在 aiohttp 栈上做，单独设一个短预算 `SHUTDOWN_DRAIN_TIMEOUT`。（赞成。）
2. `CERT_CHAIN` 有值或给了 `--cert` 时拒绝启动，而不是打警告后跑明文；判断看真值。（赞成。）
3. 第 5 步 master 不加载 app 文件，`on_server_setup` 不再在 master 里执行；ready 之前任何退出都算整体失败。
   （赞成。）
4. DEBUG 自动重载用轮询 mtime 实现，不加依赖，监视范围同 Sanic。（赞成，Sanic 本身就是这么做的，Windows
   上也能用。）
5. webext 请求体上限：沿用 `REQUEST_MAX_SIZE` 这个键名，默认 100MB，映射到 `client_max_size`。（赞成，上传
   接口不会悄悄开始返回 413。）
6. 第 5 步之前，`WORKER_NUM>1` 配上 `HETU_HTTP_SERVER=aiohttp` 时报错退出。（赞成，降级为单进程会让 A/B
   数据失真。）
7. 默认共享监听 socket，SO_REUSEPORT 实测后作为可选项。（赞成，跨平台，和现在一致，worker 挂掉时不会重置
   排队中的连接。）
8. 性能：+20% 只用来触发排查；进入第 5 步的条件是每帧成本和 p99 都不退步。（审查提出的改法。）
9. ~~§3.4 的四个修复放在第 1 步一起做，还是先单独合？~~ 已定：迁移前单独修（§13 第 7 条）。
10. ~~要不要在第 0 步之前，先做一个不合并的原型，提前拿到 RPC 的收益数字？~~ 已定：不做（§13 第 8 条）。

## 15. 审查记录

### 15.1 第一轮（2026-11-14，独立审查）

主要问题和处理：

- **Sanic 的关服模型写错了，还掩盖了一个现存的发号竞态**：重写 §3.2 第 2、4、5 条，新增 §3.4；第 1 步改用
  HeTu 自己的任务表，固定 `stop()` 的顺序；删掉"关服时干等 15 秒"的说法。
- **aiohttp 的 `close()` 不保证断开**：§5.4.2 的 `fail()` 加上 abort 兜底，并加测试。
- **Sanic 上 recv 的契约做不到；占位 future 的卡死场景不成立**：§5.3 的契约按栈分别写明，改写动机，并补上
  handler 被取消时显式收掉子任务。
- **supervisor 的失败语义有缺口**：ready 之前的退出、重启退避、孤儿进程、丢租约、信号、Windows，并入 §5.7。
- **harness 的语义没写清；`test_webext` 不该在第 0 步改**：§5.0、§5.8。
- **客户端地址的规则不全**：§3.2 第 7 条、§5.4.1。
- **第 4 步太大**：拆成 4a / 4b / 4c；aiohttp 提前进运行时依赖。
- 次要问题都已并入各节：keepalive 的默认值、访问日志、`protocol_error`、心跳和消息上限的差异、尾斜杠、
  `X-Request-ID`、围栏时间、配置项按真值判断、formatter 依赖的状态、xdist 分组。

审查用的复现脚本（Sanic / aiohttp 探针、用 SQLite 跑的真实关服实测）放在会话临时目录
`/tmp/claude-1000/-home-heerozh-ssw-re-Game-HeTu/33e75902-4186-4572-a3e3-542e86f777b7/scratchpad/review/`，
没有入库。
