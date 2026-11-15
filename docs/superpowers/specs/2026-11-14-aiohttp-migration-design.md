# 用 aiohttp 替换 Sanic — 设计稿

- 日期：2026-11-14
- 状态：实施中，设计做过一轮独立审查（§15）。§3.4 的现存问题已由 PR #225 修复；第 0~4b 步已合并（PR #226~#231），
  第 4c 步的实测见 §12.2。已定事项见 §13，待定问题见 §14。
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
推送每帧只省 1~1.5µs，不到现在 13~18µs 的一成，推送的大头是每个连接各做一遍的 zlib 和加密。以上是估算。
第 4c 步实测（§12.2）：每次 RPC 21.9 → 11.5µs，QPS +86%，比估算好；推送每帧少 6~7k 个周期（约 1.4µs），
和估算一致。

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
14. **Windows 控制台的颜色**：在 Windows 上，`import sanic.app` 会调用 `enable_windows_color_support()`，打开
    控制台的 ANSI 转义支持（`sanic/app.py:102-103`）。HeTu 的彩色日志和启动 LOGO 都暗中依赖它；第 6 步删掉
    Sanic 后，要自己打开（比如 `ctypes` 调 `SetConsoleMode` 开 ENABLE_VIRTUAL_TERMINAL_PROCESSING），否则传统
    控制台会显示裸转义码。

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

### 5.1 第 1 步：WorkerRuntime（已实施，分支 `refactor/worker-runtime`）

新增 `hetu/server/runtime.py`、`hetu/server/adapters/sanic_app.py`，删掉 `hetu/server/main.py`：

```python
class TaskRegistry:
    """HeTu 自己的任务表，两个栈共用。"""

    def spawn(self, coro, *, name: str | None = None) -> asyncio.Task: ...  # 重名抛 ValueError
    async def cancel(self, target: asyncio.Task | str) -> None: ...  # 按句柄或名字取消并等它结束
    async def cancel_all(self, timeout: float | None) -> bool: ...

class WorkerRuntime:
    """一个服务实例一个：适配层每次开服新建一个。"""

    config: Mapping[str, Any]
    backends: dict[str, Backend]  # 按配置名登记，另有 "default" 别名指向第一个
    default_backend: Backend  # 属性：配置里的第一个后端（和原来一致）
    table_managers: dict[str, ComponentTableManager]
    snowflake_lease: SnowflakeLease | None
    tasks: TaskRegistry  # 连接相关：接收协程、顶号核查
    background: TaskRegistry  # FutureCalls
    live_connections: set[asyncio.Task]  # 连接表：Connection 行已落库的连接协程
    cleanup_tasks: set[asyncio.Task]  # 连接表：拆连接的清理任务
    on_lease_lost: Callable[[], None]  # 由适配层注入
    exit_code: int  # 单进程丢租约时由适配层置为非 0

    async def start(self) -> None: ...
    async def start_backends(self) -> None: ...
    def start_background(self) -> None: ...  # 开始监听之后：FutureCalls
    async def wait_connections_closed(self, timeout: float) -> bool: ...
    async def stop(self) -> None: ...
    async def close_backends(self) -> None: ...
```

另有两个进程级、与框架无关的准备函数：`prepare_worker(config)` 加载 app 文件、建簇、把配置传给各模块；
`setup_message_pipeline(config)` 初始化服务端消息流水线。

**每次开服新建一个 runtime**：Sanic 的测试客户端会对同一个 app 反复起停，每次起服各用各的 runtime，连接表
天然按服务实例隔离。原来 `wait_connections_closed` 里"只算本 loop 的任务"那段过滤删掉了。

**`start()`**：原 `worker_start` 原样搬过来，拿到租约就开始续约（#225）。不同之处：

- 迁移检查失败时不再调 `app.stop()`，直接抛错，由外层包成 `StartupAborted`；
- 任何异常（包括被取消）都会先停续约循环、释放租约再往外抛，原来只在 `StartupAborted` 时这样做。释放之后
  不再持有租约，之后的 `stop()` 不会再释放一次；
- 每个后端的 `post_configure` 只做一次。原来 "default" 别名让第一个后端做两次（版本探测、Lua 加载等同步调用）；
- 没配 `APP_FILE`（嵌入场景）时跳过"加载 xx 完成"那条日志，不再抛 TypeError。

**任务表**：用 HeTu 自己的实现，两个栈共用，不再交给 Sanic。

- 持有强引用，任务结束后自动移除，未处理的异常打 `logger.error`（带堆栈）。
- 有名字的任务名字唯一，重名抛 ValueError。名字按登记时的算，task 之后被改名也不会留下死条目。连接任务仍用
  #225 的进程内序号命名。
- `cancel()` 用 `asyncio.wait` 等任务结束，不直接 `await task`。直接 await 时调用方被取消，asyncio 会把这次
  取消转给 task；task 吞掉 CancelledError 正常返回的话（`client_handler` 就是这样），调用方的取消就丢了。
  另外，被取消任务的 CancelledError 也会冒充成调用方自己的取消。第 3 步在连接清理里取消收发两个子任务
  （§5.3），正好会踩到这两点。
- `cancel_all()` 会一直取消，直到表空或超时，等待期间新登记的任务也一起取消。
- 接收协程由连接清理按句柄取消（`runtime.tasks.cancel(recv_task)`），不按名字找。原来它进 Sanic 的任务表，
  会被 `app.stop()` 开头的 `shutdown_tasks(timeout=0)` 先取消；现在改为在 `_cancel_websocket_tasks` 取消
  handler、连接清理开始时取消，前后只差几毫秒。
- 顶号核查这类无名任务也登记进来，关服时一并取消。原来它们不受任何管理。

**续约和水位预留**：这两个循环仍由 `SnowflakeLease` 自己管（#225 的 `start_loops` / `stop_loops`，CLI 工具
进程也用它）。`background` 里只放 FutureCalls。

**`stop()` 的固定顺序**：

1. `wait_connections_closed`，等断线 System 跑完、Connection 行删掉；
2. 取消残留的连接任务，和第 1 步共用一个 `GRACEFUL_SHUTDOWN_TIMEOUT`（那边用完了也至少给 1 秒），停不下来
   打警告；
3. 停 FutureCalls，单独一个 `GRACEFUL_SHUTDOWN_TIMEOUT`：它执行的 System 会发号，必须在释放租约之前停掉；
4. 停续约和水位预留循环，写精确水位并释放 Worker ID；
5. 关后端。每个后端只关一次；原来 "default" 和第一个后端是同一个对象，会被关两次。

最坏情况是 2 × `GRACEFUL_SHUTDOWN_TIMEOUT` + 1 秒，和原来基本一样，另外还要加上 Sanic 自己 `_cleanup` 里等连接的
那一段。

**Sanic 适配**（`adapters/sanic_app.py`，即原来的 `worker_main`）只负责接线：

- `before_server_start`：新建 runtime，挂到 `app.ctx.runtime`，然后调 `runtime.start()`；
- `after_server_start`：调 `runtime.start_background()`，注册在 `webext.apply_to` 之前；
- `after_server_stop`：调 `runtime.stop()`；
- `on_lease_lost(app, runtime)`：多进程时只重启本 worker；单进程时把 `runtime.exit_code` 置为非 0 并停服。
  CLI 从 `app.ctx.runtime.exit_code` 读退出码，多进程时管理进程没有 runtime，按 0 处理。

**其他调用点**：

- `websocket.py` 改走 runtime：`table_managers`、`config`、`default_backend`、`tasks`、`live_connections`、
  `cleanup_tasks`。`_cleanup_connection` 的第一个参数从 request 换成 runtime；
- `future_call_task(app)` 改成 `future_call_task(runtime)`；
- 测试里的假 app 都换掉：`test_system_startup.py`、`test_common.py` 换成真的 `WorkerRuntime`，
  `test_system_future.py` 换成只有 `table_managers`、`config` 的 SimpleNamespace；
- `ServerHarness.runtime` 指向 `app.ctx.runtime`；
- `hetu.server` 改成按需加载 `worker_main`：只 import `runtime`、`pipeline` 时不加载 Sanic，也不注册 ws 路由。

这一步 `runtime.config` 仍是 Sanic 的 `app.config`。runtime 只用 `config[...]` / `config.get(...)` 读，第 2 步
换成 HeTuConfig 时不用再改。

**测试**：#225 加的测试照旧通过，改动有：

- 导入路径；
- monkeypatch 的目标，改为 `hetu.server.runtime.future_call_task`；
- `_wait_server_connections` 改为接收 runtime、读 `runtime.live_connections`；
- `on_lease_lost` 相关测试的签名。

新增的测试：

- `tests/test_server_runtime.py`：`TaskRegistry`，包括调用方被取消、不误抛目标的取消、重名、改名、
  `cancel_all` 超时和等待期间新登记的任务；
- `test_common.py`：每个后端只 post_configure、关闭一次；默认后端是第一个；迁移检查失败时中止；拿到租约后
  失败会释放租约，而且只释放一次；Sanic 的 `_runtime_start` 监听器原样抛出 `StartupAborted`；
- `test_websocket.py`：关服时残留的连接任务在释放租约之前已经取消；每次调用都换一个新 runtime。

### 5.2 第 2 步：配置、日志、worker 序号，删除 TLS / 访问日志 / Inspector（已实施，分支 `refactor/config-logging`）

这一步不是纯重构：删掉了几个配置项。按 §14 第 2 条的原则（还没有线上游戏，不需要向前兼容），所有兼容处理都
不做（§13 第 9 条）。

**配置**

- 新增 `hetu/server/config.py` 的 `HeTuConfig`：一个 dict，可以按属性读（`config.NAMESPACE`），读不到的属性
  抛 AttributeError。`update_config()` 只收大写键，和 Sanic 的语义相同。CLI 里的 `sanic.config.Config` 换成了
  它，`worker_main` 收到普通 dict 时（测试、嵌入场景）也先转成它。
- 原来由 Sanic 读取、迁移后还保留的键，由 HeTu 给出默认值，和 Sanic 25.12 的默认值相同，免得 Sanic 一走
  默认值就悄悄变掉：

  | 键 | 默认值 | 说明 |
  |---|---|---|
  | `GRACEFUL_SHUTDOWN_TIMEOUT` | 15.0 | 同 Sanic |
  | `WEBSOCKET_MAX_SIZE` | 2\*\*20 | 同 Sanic，模板里仍是 0.5MB |
  | `WEBSOCKET_PING_INTERVAL` | 20 | 同 Sanic |
  | `WEBSOCKET_PING_TIMEOUT` | 20 | 只对 Sanic 栈生效；aiohttp 上不可配，等 pong 的时间是 heartbeat/2（§6） |
  | `PROXIES_COUNT` / `REAL_IP_HEADER` / `FORWARDED_FOR_HEADER` | 0 / None / `X-Forwarded-For` | 同 Sanic（§3.2 第 7 条） |
  | `REQUEST_MAX_SIZE` | 100MB | 同 Sanic，第 6 步映射到 aiohttp 的 `client_max_size`（§14 第 5 条） |
  | `DEBUG` | False | 原来配置里没有 DEBUG 时 `worker_main` 会 KeyError |

- Sanic 适配层继续用 `app.update_config(...)` 把整份配置交给 Sanic。Sanic 栈上 `runtime.config` 仍是
  `app.config`（`HeTuConfig` 的键都已经 update 进去），测试通过 harness 改的配置（如 `WEBSOCKET_MAX_SIZE`）
  对 Sanic 和 runtime 都生效。aiohttp 栈直接用 `HeTuConfig`。
- Sanic 的 `Config` 会自动读取 `SANIC_` 前缀的环境变量，换掉之后这个能力就没了。建 Sanic app 时还传了
  `env_prefix=None`：不然 Sanic 栈上 `app.config` 仍会读到这些环境变量，而 CLI、`resolve_ack_threshold`
  读的 HeTuConfig 读不到，两份配置会不一致（比如只用环境变量调大 `STARTUP_TIMEOUT`，ack 阈值却还是旧的）。
  HeTu 的文档没写过这个能力，写进发版说明即可。
- `HeTuConfig` 按属性写（`config.X = v`）也写进 dict，属性读和 `config["X"]` 不会不一致。

**删掉的配置**

- `CERT_CHAIN`、`--cert`、`ACCESS_LOG`、`INSPECTOR`、`INSPECTOR_PORT` 直接删掉：不做检查，不打警告，也不留
  隐藏参数。配置里还留着的话，HeTu 不读。
- Sanic 栈：`app.prepare()` 不再传 `ssl` / `auto_tls`，固定 `access_log=False`；`worker_main` 在
  `update_config` 之后强制 `ACCESS_LOG = False`、`INSPECTOR = False`。配置里残留的键不会意外打开 Sanic 的
  访问日志或 Inspector，后者会在 6457 端口开放 reload / shutdown 接口。
- 启动日志里的监听地址固定显示 `http://`；TLS 由前面的反向代理终止。
- 模板（`CONFIG_TEMPLATE.yml`，`hetu init` 也用它）、文档（`operations.md` 中英文的 `--cert` 一行）、测试和
  压测脚本里的配置同步删除。

**日志**

- `hetu/safelogging/formatter.py` 的 `ContextAutoFormatter` 不再继承 Sanic 的类。格式移植自 Sanic 25.12 的
  AutoFormatter（MIT 许可，已注明出处），类名和路径不变：用户的配置里写的就是
  `hetu.safelogging.formatter.ContextAutoFormatter`。`DEFAULT_LOGGING_CONFIG` 也改用它。
- 和原来的差别：
  - **格式化不改 record**。原来 AutoFormatter 会把颜色码写进 `record.levelname`，QueueListener 把同一条
    record 交给排在后面的文件 handler 时，文件日志里就带上了颜色码。现在格式化时临时换上、格式化完还原，
    ident 也一样。
  - **extra 字段只在深度 Debug（DEBUG > 1）时附加**，由 `worker_main` 设置 `ContextAutoFormatter.LOG_EXTRA`。
    原来 Sanic 在 `app.prepare()` 里才把它设成 debug 的值，之前一直是 True，所以管理进程开服前打的那几行
    日志（LOGO、监听地址等）后面会多出一行 `ctx=...`。现在不会了。
  - **关颜色改用标准的 `NO_COLOR` 环境变量**（https://no-color.org），不再认 `SANIC_NO_COLOR`。
- golden test（`tests/test_safelogging.py`）：
  - 不上颜色时和 Sanic 的 AutoFormatter 逐字对照，带不带 extra 字段都比。第 6 步删 Sanic 时，把对照换成
    固定字符串；
  - 上颜色时比对固定字符串；
  - 格式化完 record 不变。
- 不做兼容改写：LOGGING 里引用了 `sanic.*` 类的老配置，第 6 步删掉 Sanic 后会在 dictConfig 时 ImportError。
- `dictConfig` 改由 HeTu 自己调用，时机和原来一样：在 `worker_main` 里、建 app 之前。传给 Sanic 的是
  `configure_logging=False`。设置 `LOG_EXTRA`、dictConfig、按 DEBUG 调 HeTu 日志级别这一段抽成了与框架
  无关的 `runtime.setup_logging(config)`，第 4a 步的 aiohttp 适配层、第 5 步的 master 都用它。
- YAML 里的 Sanic 配置键 `LOG_EXTRA`、`NO_COLOR` 原来会经 Sanic 的 `setup_logging` 生效，现在不认了。
- `sanic` logger 重定向到 `HeTu.root` 的逻辑留在 Sanic 适配层；第 4a 步 aiohttp 适配层对 `aiohttp` 的 logger
  做同样处理。

**worker 序号**

- 新增 `hetu.common.helper.worker_index()` / `worker_ident()`，读 HeTu 自己的环境变量 `HETU_WORKER_INDEX`
  （从 0 开始的整数），第 5 步由 supervisor 设置。读取方有两个：
  - `ContextFilter.IDENT` 和 formatter 的 `IDENT`：设了就显示 `f"Srv{n:2}"`（和 Sanic 的一样，"Srv 3"、
    "Srv12"），没设显示 "Main "；
  - `FixedWorkerKeeper`：取 worker 序号，没有就是 0。
- 过渡期：没设 `HETU_WORKER_INDEX` 时，照旧解析 `SANIC_WORKER_IDENTIFIER`；Reloader 这类标识里没有序号
  的进程，原样显示它的标识。这个回退在第 6 步删除。
- `HETU_WORKER_INDEX` 为空串时当作没设；不是非负整数时报错，不能静默得到 -1 这样的 worker id。
- `hetu start` 的管理进程启动时先清掉环境里残留的 `HETU_WORKER_INDEX`。它比 Sanic 给的序号优先，传下去的话
  所有 worker 会拿到同一个序号，SQLite 后端上 Worker ID 就重了。
- `test_common.py` 都测了。

**顺带修：第二个关服信号会打断关服流程**（冒烟测试发现，审查时纠正了原因）。Sanic 在 worker 里给 SIGINT /
SIGTERM 注册的处理是每次都调 `app.stop()`，而 worker 常常会收到第二个关服信号：

- 终端里 Ctrl+C 会把 SIGINT 发给整个进程组，多 worker 时管理进程还会再转发一次；
- systemd 默认的 KillMode=control-group 也会给每个进程都发 SIGTERM。

Linux 上用的是 uvloop，loop 停下之后 `remove_signal_handler` 不起作用（`uvloop/loop.pyx:2990`，直接返回
False），第二个信号照样调用 `loop.stop()`，正在跑的关服步骤的 `run_until_complete` 抛 RuntimeError
（标准 asyncio 下抛的是 KeyboardInterrupt）。后果分两种：

- **落在 before / after_server_stop 里**：被 `_run_shutdown_coro` 吞掉，整个重跑一遍，第一遍的 task 还挂着
  接着跑，于是两遍并发，租约释放、后端关闭都做两遍；
- **落在 `_cleanup()` 等连接拆完的那段里**（`runners.py:322`，只要还有连接就至少等 0.1 秒）：异常没人接，
  直接冲出 `_run_server_forever`，after_server_stop 一次都不跑。断线 System 不跑、Connection 行不删、租约
  不释放、后端不关。审查实测：挂着客户端时，7 次 Ctrl+C 里有 5 次出现这种情况。dev 上同样会出现。

修法：

- Sanic 适配层在 after_server_start 里（Windows 除外）用 `loop.add_signal_handler` 覆盖 Sanic 的处理：
  第一个关服信号一到，就把 SIGINT / SIGTERM 都设成忽略，再走 Sanic 原来的 `app.stop()`；
- before_server_stop 里再忽略一次，因为标准 asyncio 的 `remove_signal_handler` 会把它们恢复成默认处理。
  这一步只在确实收到过关服信号时才做，测试、嵌入场景里由代码调 `app.stop()` 时不动进程的信号处理；
- 代价：单进程模式下第二次 Ctrl+C 不能强退。关服的每一步都有 `GRACEFUL_SHUTDOWN_TIMEOUT` 兜底，不会一直卡住；
- 兜底：`WorkerRuntime.stop()` 可以重复调用、并发调用，只真正执行一次（锁加标记）；关到一半被打断后再调，
  已经释放的租约、已经关掉的后端不再来第二次。

测试：

- `tests/test_server_signals.py` 真起 `hetu start`，用一条请求头没发完的连接把服务器稳定地拖在 `_cleanup`
  里，再往进程组连发两次 SIGINT，断言关服流程跑完（修之前每次都失败）；
- `test_runtime_stop_runs_once_when_called_twice`、`test_runtime_stop_resumes_after_being_interrupted`；
- 冒烟测试：2 个 worker 挂 6 个客户端，往进程组发一次 SIGINT，5 次都正常关服。

管理进程这边：收到第二个关服信号时，Sanic 会直接 SIGKILL 整个进程组（`WorkerManager.shutdown_signal`）。GNU
`timeout` 会把信号发两次（发给子进程一次、发给进程组一次），用它停服务时偶尔会被直接杀掉。这一点留给第 5 步
自己的 supervisor（§5.7）。

### 5.3 第 3 步：ws 连接抽象（已实施，分支 `refactor/ws-connection`）

纯重构，仍跑在 Sanic 上。只有两处细小的行为变化，见下面"和原来行为的差别"。

**接口**（新增 `hetu/server/conn.py`）

```python
class ConnectionClosed(Exception):
    """对端已断开，或本端已经 fail 了连接。替代 sanic.exceptions.WebsocketClosed。"""

class WsConnection(Protocol):
    conn_id: str  # next_conn_id() 分配，本进程内唯一
    client_ip: str  # 已经按 §3.2 第 7 条解析过
    request: Any  # 框架的原始请求，放进 ctx.request（unsafe）

    async def recv(self, timeout: float | None = None) -> str | bytes | None: ...
    async def send(self, data: str | bytes) -> None: ...
    def fail(self, code: int, reason: str) -> None: ...
    def protocol_error(self) -> BaseException | None: ...

def next_conn_id() -> str: ...
```

接口语义：

- `recv`：返回一整条消息；超时返回 None。连接断开后的行为**两个栈不同**：
  - aiohttp：在有限时间内抛 `ConnectionClosed`。本端 fail 之后对端又不读数据的，已经在等的 recv 要到 fail 的
    abort 兜底时才醒（审查实测：close_timeout 设 2 秒，2.0 秒后醒）；
  - Sanic：可能一直挂着（§3.2 第 1 条），靠取消 handler 兜底；对端在 `recv(timeout)` 期间断开时，Sanic 会
    让它抛 CancelledError。
- `send`：连接已断开、或已经 fail 过时抛 `ConnectionClosed`。
- `fail`：同步调用，**立即生效**：返回时 close 码就已经定下，`serve_connection` 返回之后框架自己的关闭（Sanic
  的 `ws.close()`、aiohttp handler 返回时的 `write_eof()`）不能把它盖成 1000。握手阶段好几处都是 fail 之后直接
  返回。尽力发出 close 帧后断开；可以重复调用，只有第一次的 code 生效；保证在有限时间内断开，正卡在发送上的
  send 也要在有限时间内醒来，不受对端配合与否影响（aiohttp 的实现见 §5.4.2）。
- `protocol_error`：只返回协议层错误，替代 `ws.ws_proto.parser_exc`。对端正常掉线不算。

和原计划的出入：连接序号 `_conn_seq`（#225）从 websocket.py 挪到了 conn.py，由 `next_conn_id()` 分配。这样
适配层只依赖 conn.py 和 `serve_connection`。

**改动**

- `websocket_connection` 改成 `serve_connection(runtime, conn, instance)`，不再依赖框架。
- ws 路由由 Sanic 适配层显式注册，不再在 import 时注册：
  - `worker_main` 用 `app.add_websocket_route` 注册；路由 handler 只建
    `SanicWsConnection(request, ws, next_conn_id())`，再调 `serve_connection`；
  - `HETU_BLUEPRINT` 删掉了；
  - 和路由匹配有关的属性（路径、strict_slashes、方法、websocket 标记）和原来逐项对过，相同；生命周期钩子的
    注册顺序也不变。路由名从 `<app>.HeTuDB.websocket_connection` 变成了 `<app>._serve_websocket`；
    `error_format` 从 `'auto'` 变成 `''`，两者都不在 `MIME_BY_CONFIG` 里，实际效果相同。
- `SanicWsConnection` 也放在适配层：
  - `recv(None)` 走 `recv_streaming` 自己重组消息。这是原来 `receiver.py` 的 `receive_messages`：Sanic 的
    `recv()` 每条消息都要建一个 Task 再 `asyncio.wait`，慢。`recv(timeout)` 用 Sanic 的 `recv`；
  - `WebsocketClosed` 转成 `ConnectionClosed`；
  - `fail` 调 `fail_connection`；`protocol_error` 读 `ws_proto.parser_exc`，EOFError 不算。
- **收发两边各一个任务**：
  - `serve_connection` 把发送循环也起成任务（`ws_send:{conn_id}`），和接收协程（`client_handler:{conn_id}`）
    一样登记进 `runtime.tasks`，用 `asyncio.wait(..., return_when=FIRST_COMPLETED)` 等任意一边结束。
  - handler 被框架取消时（Sanic：传输层断开、关服），吞掉取消、照常拆连接，和原来一样（原来是在 send_loop
    外面 `except CancelledError` 吞掉）。
  - 吞掉取消时调 `current_task().uncancel()` 撤销这次取消请求，免得 `cancelling()` 一直留着计数。
  - `asyncio.wait` 被取消时不会取消它在等的任务，所以 `_cleanup_connection` 一开始先取消收发两个任务，并等
    它们都结束，然后才退订、跑断线 System、删行，保持"收发都停了再退订"的顺序。两个任务一起取消、一起等，
    不让发送循环多跑一阵（Sanic 上它可能正占着发送锁，拖住关服时发的 1001）。清理仍是 shield 包住的独立
    任务。
  - 收发两个任务和 `pack` / `flooded` 都在建连接的 try 里起，起任务出错（比如适配层给了重复的 conn_id）也走
    "连接初始化异常"那条路，fail(1011) 并拆连接。
  - 动机：接收协程退出时如果队列已满，PUSH_CLOSE 会被 `put_nowait` 丢掉；与此同时发送循环卡在 send 的 drain
    上（对端不读数据，见 §5.4.2）。只等发送循环的话，要等 fail 的 abort 兜底把卡住的 send 叫醒才能拆连接，
    这期间 Connection 行还在，还会多打一条发送异常。Sanic 上靠传输层关闭后取消 handler 兜底。现在接收协程
    一结束就拆。
  - PUSH_CLOSE 保留：接收协程结束时照旧塞，发送循环取到就自己返回；塞不进去也不影响拆连接。第 4b 步的排空
    没有用它，另加了 `PUSH_DRAINED`（§5.5）。
  - 订阅回复的占位 future 不会造成卡死：deferred 任务一结束，`settle` 回调就会把它取消
    （`receiver.py` 的 `defer_sub_reply_`）。
- 协议错误的日志改在等收发结束之后打，不管是 handler 被取消、还是收发某一边先结束。原来只在 handler 被取消的
  那条路径上打。
- 接收协程收到 `ConnectionClosed`（对端断开，或顶号核查、发送那边已经 fail 了连接）时安静地退出：不再 fail，
  "接收协程结束"降成 DEBUG，PUSH_CLOSE 照旧塞。换到 aiohttp 后每次正常掉线都会走到这里，拆连接时本来就会记
  一条"连接断开"。Sanic 上正常掉线走的是取消出口，这条路径很少走到。
- **修掉一个现存的 Connection 行泄漏**（审查发现，dev 上也有）：原来 `initialize()` 在有清理保证的 try 外面，
  而且行 id 是插入、等副本同步之后才记进 context。客户端握手完立刻断开时，Sanic 取消 handler，取消落在
  `initialize` 里就直接往外抛，这一行就永远留在库里（审查实测：不打补丁，60 次"握手完就断"漏 2 行）。现在：
  - `EndpointExecutor.initialize` 先分配 id、记进 context，再落库（`new_connection` 加了 `connection_id`
    参数）。落库途中被取消或出错，`terminate()` 也知道删哪一行；行没落库的话，删一行不存在的行无害；
  - 落库没走完时，`terminate()` 删行之前先等副本同步（`del_connection(..., wait_synced=True)`）：这一行可能
    刚提交、副本上还没有，而 `del_connection` 随机读主从，读到副本就删不掉。正常拆连接不多等；
  - `initialize` 挪进建连接的 try 里，`live_connections` 的登记也挪到它前面：初始化途中被取消也会走 finally
    拆连接，关服时也会等它；
  - 断线 System 只给 `initialize` 走完的连接跑（`EndpointExecutor.initialized`）。初始化途中断开、被拒的连接，
    游戏逻辑从没见过它，和原来一样不跑；
  - 同 IP 匿名连接数超限时，`new_connection` 抛 `TooManyAnonymousConnections`（`RuntimeError` 的子类），
    `initialize` 据此清掉预分配的 id。被拒的连接拆连接时不碰库：不等副本同步、不删行、不跑断线 System。
    这个限制是防刷连接的，被拒的连接不能让库（master）多干活。
- `close_connection(conn, code)` 改调 `conn.fail(code, CLOSE_REASONS[code])`；`WebsocketClosed` 改成
  `ConnectionClosed`；`receiver.py` 里 `except (SanicException, BaseException)` 去掉 `SanicException`；motd 改成
  `conn.send(str)`。
- `ctx.request` 的类型标注改成 `Any`（框架的原始请求）。在 Sanic 栈上仍是 Sanic 的 Request，对外不变。
- 握手：
  - 等握手消息的时间提成模块常量 `HANDSHAKE_TIMEOUT = 10`，超时单独记一条日志；
  - 握手之前、回复握手时对端就断开（`ConnectionClosed`），安静地结束。这是给 aiohttp 准备的：Sanic 上这时
    和原来一样以 CancelledError 结束（Sanic 自己抛的，或 connection_lost 取消了 handler），走不到这两个分支。
- 新连接、连接断开两条日志里的标识，从 Sanic handler 的任务名（`Task-123`）改成 conn_id，和
  `client_handler:` / `ws_send:` / `ws_cleanup:` 这几个任务名对得上。
- `push_queue` 的容量提成模块常量 `PUSH_QUEUE_SIZE = 1024`（测试要调小它）。
- websocket.py、receiver.py、conn.py、endpoint/context.py 都不再 import sanic，§4.1 的说法成立。

**和原来行为的差别**

1. 发送循环出了意外的异常时，以 1011（`CLOSE_INTERNAL_ERROR`）断开，和接收协程兜底时用的码一样。原来不
   fail，等拆完连接后由 Sanic 以 1000 关闭，客户端会当成服务端正常下线。
2. 协议错误在发送那边先被发现时（send 抛 `ConnectionClosed`）也会打日志，原来这种情况漏掉了。这条日志的
   标签从 `[📡WSSender]` 改成 `[📡WSConnect]`。
3. 落库 Connection 行（`initialize`）失败时，包括同 IP 匿名连接数超限，以 1011 断开。原来异常冒到 Sanic，由它
   记一条带堆栈的 "Exception occurred while handling uri"，并以 1000 关闭。现在匿名连接数超限只留
   `new_connection` 原有的那条警告，其他异常记"连接初始化异常"。断线 System 和原来一样不跑。
4. 客户端在初始化期间断开，Connection 行不再漏删（见上）。

**测试**

- 新增 `tests/test_ws_serve.py`：用内存里的假 `WsConnection`，不走网络，Redis、SQLite 各跑一遍。假连接按
  aiohttp 的语义实现：fail 之后、对端断开之后，新的收发立即抛 `ConnectionClosed`，不靠框架取消 handler；
  正卡在发送上的 send 要等 abort 兜底（3 秒）才醒，和契约一致。覆盖：
  - 握手超时、四种格式错误、实例名不存在、握手之前断开；
  - 收发频率超限（4002）、被顶号（4001）、初始化失败（1011）、发送异常（1011）各自的 close 码，Connection
    行都要删掉；
  - 收的途中、发的途中对端断开：清理跑完，断线 System 以用户身份跑；正常掉线、被顶号时不记"接收协程结束"；
  - "队列满 + send 卡住"：接收协程结束后马上拆连接，要在 abort 兜底之前拆完。改回只等发送循环（原来的
    结构）时，这个用例失败；
  - handler 被框架取消：取消不往外抛、取消请求已撤销，退订之前收发两个任务都已结束；
  - Connection 行已提交、`initialize` 还在等副本同步时被取消：这一行照样删掉，删之前又等了一次副本同步，
    不跑断线 System；初始化之后才出错（订阅管理器构造失败）的照常跑一次；
  - 同 IP 匿名连接数超限被拒：以 1011 断开，拆连接不等副本同步、不删行、不跑断线 System；
  - 协议错误打日志，对端正常掉线不打；motd 是文本帧。
- `test_websocket.py` 补上 Sanic 适配层的用例（审查做变异时，`SanicWsConnection` 的 5 个变异在原有测试上
  全部存活）：
  - `SanicWsConnection` 的单元测试（替身 ws）：`recv(timeout)` 交给 Sanic 的 recv、不限时走 recv_streaming
    重组；`WebsocketClosed` 转成 `ConnectionClosed`；fail；协议错误不算 EOFError；
  - 真实栈：超长消息恰好打一条协议错误；对端直接断开 TCP 不算协议错误、照常拆连接；不发握手消息的连接在
    `HANDSHAKE_TIMEOUT` 后收到 4002；握手完立刻断开（服务端还在 `initialize` 里）不漏删 Connection 行。
  - harness 的 `connect()` 加了 `handshake=False`，只建 ws 连接、不做 HeTu 的握手。
- 等 `serve_connection` 结束要用 `asyncio.wait`，不能用 `asyncio.wait_for`：后者超时会取消它，而它被取消时
  会照常拆完、正常返回，卡死就测不出来。
- `test_server_runtime.py`：在干净的子进程里 import websocket / receiver / conn / endpoint.context，不加载
  sanic。
- `test_websocket.py` 原有的用例只改了 send_loop 用的连接替身（`fail_connection` 改成 `fail`），其余原样通过。
- `tests/fixtures/testapp.py`：`mod_test_app` / `test_app` 先清注册表、再加载 `tests/app.py`，第一次导入时不再
  reload 一遍。原来同一进程里 `worker_main` 以别的模块名 exec 过这个文件之后（比如先跑了
  `test_websocket.py`），第一次 `import app` 会撞"Endpoint重复定义"。这是现存问题，全量跑时一般有别的模块
  先导入过 `app` 所以碰不到；新加的 `test_ws_serve.py` 排在 `test_websocket.py` 后面，xdist 下更容易撞上。

### 5.4 第 4a 步：aiohttp 单进程，与 Sanic 并存（已实施，分支 `feat/aiohttp-server`）

下面 5.4.1~5.4.5 是设计，和实现不一致的地方已经改过来；和原计划的出入、实施中发现的问题见 §5.4.6。

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
  - `FORWARDED_SECRET` 不实现，也不做检查：和 §14 第 2 条同一原则，还没有线上部署，不需要向前兼容。文档里写明
    只支持 `X-Forwarded-For` 这一套。如果误配成只靠 `Forwarded` 头，会退回代理的地址：Caddy 在本机时，地址
    变成 127.0.0.1 被豁免，匿名连接数限制等于失效；Caddy 在别的机器时，所有匿名用户共用代理的 IP，会被
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
| `timeout` | 2 秒 | 关闭时等对端回 close 帧的时间，默认 10 秒。等不到就 `transport.close()`，写缓冲照常发完；另有 10 秒的 abort 兜底，见下 |

**`fail()` 的实现**：aiohttp 的 `close()` 不保证能断开。`_writer.close()` 和 `drain()` 不受任何超时约束
（`web_ws.py:542-548`），`timeout` 只管读对端 close 帧的那一段（572-578），而 `_set_closed()` 还会取消心跳
（266-272）。审查实测：对端停止读数据时，`close()` 一直不返回，transport 也不关；Sanic 在同样场景下 10 秒后会
abort。所以：

1. 同步设置 `_failed` 标志，此后 `send` / `recv` 立即抛 `ConnectionClosed`；
2. ~~`transport.pause_reading()`~~ 不做：close() 要读对端回的 close 帧，暂停读取的话每次都要等满 `timeout` 才关
   TCP。fail 之后到的消息不再交出去（recv 收到后再看一次标记）；
3. 起一个关闭任务调用 `ws.close(code, message)`，并持有它的强引用。这个任务要 eager 启动（`eager_start=True`），
   让带码的 close 帧在 `fail()` 返回前就写出去；或者适配层等关闭任务结束后再返回 ws。否则 handler 一返回，
   aiohttp 的 `write_eof()` 就会抢在这个任务运行之前以 1000 关闭，盖掉 close 码（审查实测：不 eager 时被拒的
   握手客户端收到 1000，eager 时收到 4002，见 §5.3 的 fail 契约）；
4. `ABORT_TIMEOUT`（10 秒，同 Sanic 的 close_timeout）后 abort 兜底。abort 会丢掉写缓冲里还没发出的数据，
   太短的话链路慢、积压多的客户端收不到 close 帧（审查实测：最初的 2 秒下，8MB 积压、2MB/s 读的客户端被顶号
   时收不到 4001，Sanic 收得到）。abort 之后，卡在 drain 上的 send、已经在等的 recv 都会醒来，都要转成
   `ConnectionClosed`：drain 被 abort 叫醒时是正常返回的，send 要再看一次标记。
5. abort 兜底挂在 `HeTuWebSocketResponse.close()` 一进来的地方：aiohttp 自己发起的关闭（收到协议错误的帧、
   对端的 close 帧、handler 返回时的 `write_eof()`）也受它约束。aiohttp 的 close() 等 drain 不受超时约束，
   而 `_set_closed()` 已经停了心跳，对端先停止读数据、再发一个协议错误的帧时，连接会一直挂到关服（审查实测，
   Sanic 栈也一样，这是现存问题，Sanic 栈没修）。

**其他**：

- `protocol_error()` 只返回 `WebSocketError`。对端直接断开时 `ws.exception()` 是 ConnectionResetError，心跳
  超时时是 TimeoutError，都不算协议错误，否则 `serve_connection` 每次掉线都会打一条"WS协议异常"和堆栈。
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
2. 对所有连接调用 `fail(1001)`，再取消各连接的收发任务（`runtime.tasks.cancel_all`）：在途的 RPC 随之取消，
   和 Sanic 栈一样。fail 只发 close 帧、不取消任何任务，不取消的话 RPC 照样提交，回复却发不出去，客户端重连
   重试时就重复执行了（审查实测）；
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

#### 5.4.6 实施记录

**代码**：

- `hetu/server/forwarded.py`：客户端地址，和框架无关。测试直接拿 Sanic 的 `parse_xforwarded` 逐条对照。
- `hetu/server/adapters/aiohttp_app.py`：`AiohttpWsConnection`、`AiohttpServer`（一次开服到关服，`hetu start`
  和测试 harness 共用）、`run()`、`prepare()`（同 `worker_main` 里与框架无关的那几步）、`bind_socket()`、
  `new_event_loop()`。
- `hetu/cli/start.py`：按 `HETU_HTTP_SERVER` 分两条路，aiohttp 栈不加载 sanic。开服信息抽成
  `log_server_info()`，两个栈共用，多打一行用的是哪个 Web 服务器。
- `webext.registered()`：aiohttp 栈据此打警告，说明哪些声明被跳过。
- `common/helper.py` 的 `enable_windows_console_ansi()`：aiohttp 栈不 import sanic，Windows 控制台的 ANSI 支持
  要自己打开（§3.2 第 14 条）。

**和原计划的出入**：

- fail 不 pause_reading，见 §5.4.2。
- handler 结束时（不管连接怎么结束的）都补一次 `fail(1000)`：fail 过的不会再来一次；没 fail 过的（对端关的、心跳
  超时的）补上 abort 兜底。对端不读数据时 `transport.close()` 会一直等着把缓冲发完。
- recv 收到 ERROR 消息时自己记下里面的 `WebSocketError`：aiohttp 的 `receive()` 处理协议错误时不把它存进
  `ws.exception()`。
- 关服时置 `stopping` 标记：停止监听之前接进来、关服开始之后才握完手的连接，收不到关服时那一轮 `fail(1001)`，
  由 handler 看到标记自己马上断开。
- `bind_socket` 照 Sanic 的写：IPv6 地址用 AF_INET6（是否双栈按系统默认），主机名用 AF_INET；只是 Windows 上不设
  `SO_REUSEADDR`，那里设了它别的进程就能抢同一个端口。没用 `socket.create_server`，它对 IPv6 会强制只监听 v6。
- 关服信号：非 Windows 用 `loop.add_signal_handler`，每个信号都只置一个事件（幂等），之后的信号不打断关服，同
  Sanic 栈（§5.2 末尾）。Windows 上用 `signal.signal` 转回事件循环，再每 0.1 秒叫醒一次事件循环。Windows 的路径
  没有自动化测试，要按 §9 的人工清单验证。
- `HETU_HTTP_SERVER` 取值不对、aiohttp 栈配了多进程，都以 2 退出；端口被占以 1 退出，不会先开服。

**测试基础设施上发现的问题**：

- **两个栈共用 session 级的 Redis**：用例会在库里留下数据，同一个用例在两个栈上各跑一遍时，后跑的会看到先跑的
  留下的数据（`test_websocket_call_system` 因此一直等一条不会来的推送）。改成两个栈各用一个 Redis 库
  （`ws_redis_url`，sanic 0 号、aiohttp 1 号）。keyspace 频道名带库号；point_sub 的值频道（commit 里
  PUBLISH，顶号检测用它）不带，但两个栈的用例是先后跑的，不会同时用同一个 Redis，没有影响。直接读 Redis
  的用例改用同一个地址。
  headless 的用例每次开头都会把数据重置到同一个起点，不受影响。
- **pytest-timeout 打断不了 uvloop**：它靠 SIGALRM，而信号处理函数里抛的异常会被 uvloop 的回调处理吞掉，事件循环
  照转（实测）。Sanic 的测试同样跑在 uvloop 上，这是原来就有的隐患，只是以前没卡过。harness 改成在服务器的事件
  循环里按用例 timeout 标记的八成给 routine 限时（没标就 120 秒），卡住时以 TimeoutError 失败。
- **aiohttp 和测试用的事件循环不兼容**：conftest 让异步用例跑在 `UvloopSignatureLoop` 上（`create_task` 签名同
  uvloop 的标准循环），而 aiohttp 在 3.14 上看到标准循环就会给 `create_task` 传 `eager_start`。在 pytest 的事件
  循环里起 aiohttp 的用例（`test_aiohttp_adapter.py`）改成跑在生产用的事件循环上（Linux / macOS 是 uvloop）。
  harness 自己建事件循环，不受影响。
- xdist 的分组键加上栈，`test_websocket.py` 的两个栈分成两个组。CI 不设 `HETU_TEST_HTTP_SERVERS`，两个栈都跑。

**测试**：

- 整套 ws 测试（`test_websocket.py`、`test_headless_ws.py`）在两个栈上各跑一遍；webext 路由的用例在 aiohttp 栈上
  跳过。两个栈都要满足的新用例：带尾斜杠的路径能连上；不打访问日志（做过变异：打开 aiohttp 的访问日志，用例失败）。
- `test_forwarded.py`：客户端地址和 Sanic 逐条对照，多个头、条目不够、没有这个头、IPv6、unknown、大小写等 18 种
  情况；对端地址原样返回。
- `test_aiohttp_adapter.py`（不需要后端）：
  - fail 之后 handler 马上返回，客户端收到的仍是 fail 的码（不 eager 时会被盖成 1000）；
  - 不读数据的对端被 fail 之后约 2 秒断开，卡住的发送醒来；
  - 不回 pong 的对端被心跳断开；
  - 协议错误（消息超长，客户端收到 1009）记下来，对端正常关闭、直接断开不算；
  - 不协商压缩；收发和 fail 的语义；租约丢失时要求停服。
- `test_server_signals.py`：关服时第二个 SIGINT 不打断关服流程，两个栈各跑一遍；aiohttp 栈端口被占时在开服之前就
  以 1 退出。`test_cli_start.py`：多进程、取值不对时以 2 退出。
- 冒烟测试：`HETU_HTTP_SERVER=aiohttp hetu start`（SQLite），6 个客户端握手并各自登录后发 SIGINT，连跑 3 次：
  0.1 秒左右关完服，客户端都收到 1001，Connection 行为 0，断线 System 以各自的用户身份各跑一次。

**独立审查（PR #230）后的修改**：

- **关服时取消在途的 RPC**（见 §5.4.4 第 2 步）。原来 fail(1001) 之后 RPC 照样提交、回复发不出去，和 Sanic
  栈不一样，也和这一节原来写的不一样。
- **开服途中收到关服信号就中止开服**：开服起成任务，和关服信号一起等；先等到信号就取消开服（拿到的租约随之
  放弃），再把开到一半的关掉，以 0 退出。原来要等开服跑完（最长 `STARTUP_TIMEOUT`）才处理信号，开服跑完还会
  先打"已就绪"、起 FutureCalls。
- **abort 兜底**：从 2 秒改成 10 秒，和等对端 close 帧的 2 秒分开；挂在 `HeTuWebSocketResponse.close()` 一进来
  的地方，覆盖 aiohttp 自己发起的关闭（见 §5.4.2 第 4、5 步）。
- **关服信号**：
  - 非 Windows：第一个信号一到就把两种信号都忽略，卸载处理之后也继续忽略，同 Sanic 栈。原来卸载之后恢复成
    默认处理，关完服、进程退出前的收尾被迟到的 SIGTERM 直接杀掉（审查实测退出码 -15）。
  - Windows：第二次 Ctrl+C 强退，同 Sanic 栈的 ctrlc_workaround。
- **send**：卡在 drain 上时连接被 fail，abort 之后 drain 正常返回，这时 send 也抛 `ConnectionClosed`，那一帧
  多半没发出去。
- **`serve_connection` 抛异常时（不该发生）**：先以 1011 fail，客户端照样收到 close 帧。aiohttp 走它的出错
  处理时不会再调 close()。
- **`AiohttpServer.stop()`**：`runtime.stop()` 出错也照样收掉 aiohttp；关到一半被打断后再调，接着关。
- **`hetu start`**：
  - `run()` 抛 `StartupAborted` 以外的异常时，也停掉日志 Listener；
  - 多进程报错时提示命令行参数 `--workers`（命令行模式默认是 4）。
- `WEBSOCKET_MAX_SIZE: 0` 和 Sanic 一样只收空消息，原来是不限。
- **测试**：
  - 新增真起 `hetu start` 的用例（`test_server_signals.py`），两个栈各跑一遍：
    - 带着 3 个已登录的连接关服：都收到 1001、断线 System 各跑一次、Connection 行为 0、没有"没拆完"、以 0 退出；
    - 关服时在途的慢 RPC 被取消、没有提交，关服不等它；
    - 断线 System 要跑 2 秒时，连续来的 SIGINT / SIGTERM 不打断关服。原来"第二个 SIGINT"那个用例在 aiohttp
      栈上是空跑的：aiohttp 46 毫秒就关完了，第二个信号到的时候进程早已退出。现在那个用例只跑 Sanic 栈；
    - aiohttp 栈开服途中收到 SIGTERM，马上以 0 退出、不打"已就绪"。
  - harness 先关客户端再停服，带着活连接关服的路径原来没有任何自动化测试，只有这些用例测得到。
  - `test_aiohttp_adapter.py` 改成走真实的 `build_app` 和 ws handler（只把 `serve_connection` 换掉），handler
    的收尾、关服标记也在测试范围里。新增用例：
    - 消息刚好等于上限能收、超一个字节以 1009 关闭；
    - 不读数据的对端发协议错误的帧，照样限时断开；
    - handler 返回时 transport 还堵着，也限时断开；
    - 关服时才握完手的连接收到 1001；
    - `serve_connection` 抛异常时收到 1011；
    - `_serve` 在服务要求停下时关服、在开服途中要求停下时取消开服；
    - 第一个关服信号之后，信号一直被忽略。
  - 这个文件跑在 uvloop 上，pytest-timeout 改用 thread 方式兜底。
  - 变异检查：审查时存活的 6 个（第二个信号直接退出、删掉 fail(1001) 的循环、`runtime.stop()` 挪到最前、
    删掉关服标记的检查、`_serve` 不接停服请求，以及上面几处修复各自的变异）现在都会被抓到。

### 5.5 第 4b 步：关服排空（已实施，PR #231，分支 `feat/aiohttp-drain`，叠在第 4a 步的 PR #230 上）

目标：关服时，在途的 RPC 处理完再断开，而不是直接取消。Sanic 栈做不到，它的 `_cancel_websocket_tasks` 会在
`before_server_stop` 里直接取消 handler，所以 Sanic 栈保持原样，排空的测试只在 aiohttp 栈上跑。

**接口**：

- `conn.Draining(ConnectionClosed)`：适配层要求停止接收（关服排空）之后，`recv` 抛它，正在等的 recv 也要醒来
  抛它；没读的消息不再交出去。连接还开着，发送照常。不认识它的地方当成 `ConnectionClosed` 处理也安全：停下来、
  不再收。
- `CLOSE_GOING_AWAY = 1001` 进 `hetu/endpoint/connection.py`，原因留空，同 Sanic 关服时发的。

**aiohttp 适配层**：

- `AiohttpWsConnection.stop_receiving()`：置标记，之后的 recv 抛 `Draining`；正在等的 recv 用取消叫醒，转成
  `Draining`。
  - 叫醒用的取消要撤销（`uncancel`）；同时还有真的取消请求时照常抛 `CancelledError`，不能吞掉。比较的是进
    `recv` 时的取消计数（同 `asyncio.timeout`），不假定它是 0。
  - aiohttp 的读缓冲对取消是安全的（`WebSocketDataQueue.read` 被取消时不动缓冲）：叫醒时刚到的消息留在缓冲里，
    不交出去，也就不会执行。
- `AiohttpWsConnection.wait_closed()`：等到连接关了，也就是被 fail 了，或者发现对端已经断开：
  - recv 收到关闭；
  - send 发现连接断了，或者 aiohttp 心跳超时已经把它标成关了。

  对端已经断开的连接没有要发的东西了，排空不用等它跑完断线 System。
- `AiohttpServer.stop()`：
  1. 停止监听；
  2. `_drain()`：各连接 `stop_receiving()`，等它们都关了（`wait_closed`：发送循环发完回复后以 1001 断开，或者
     对端已经断开），最多 `SHUTDOWN_DRAIN_TIMEOUT`（新的配置键，默认 5 秒）。超时打一条警告。关服被打断后再调
     `stop()` 时，只等预算剩下的部分；
  3. 没在预算内排空的：`fail(1001)`，再取消各连接的收发任务，在途的 RPC 随之取消（第 4a 步的做法，见 §5.4.4）。
     取消落在提交之前的不提交；正好落在提交的往返里的可能已经提交了，回复却发不出去，同第 4a 步和 Sanic 栈；
  4. `runtime.stop()`、`runner.cleanup()`，同第 4a 步。
- 握手阶段遇到排空的连接（`recv` 抛 `Draining`，`serve_connection` 记一条"关服中"再返回），handler
  收尾时以 1001 断开。

**与框架无关的部分**（Sanic 栈上没人调 `stop_receiving`，这些分支走不到）：

- 接收协程（`client_handler`）收到 `Draining`：
  - 不再收新消息。它是在两条消息之间停下的，已经收到的那条照常执行完；
  - 不取消在后台跑的订阅后半段，等它们做完；
  - 用 `await put` 往队列塞 `PUSH_DRAINED`，满了也等着，不能丢；
  - 返回 True。

  在这期间被取消（排空超时，适配层兜底）时，取消还在等的订阅。
- 发送循环取到 `PUSH_DRAINED`：排在前面的回复（包括订阅回复的占位）都已发出。再把待发区里的推送、sync 发掉，
  然后以 1001 断开。
  - 这几帧不计发送频率上限：是服务端要关，超限断开的话客户端收到的是 4002（别重连）而不是 1001。
  - 待发区和 sync 同平时一样，在同一个同步段里取。发推送期间 hub 又交来的 tick，它的推送不发，它的 sync
    也不能发，否则客户端会以为那次调用引起的推送都到了。
  - 之后才交来的推送不等，客户端重连、重新订阅时会拿到最新的数据。
- `serve_connection`：接收协程因为排空返回 True 时，不马上拆连接，接着等发送协程。原来是收发任意一边结束就拆，
  发送协程会被取消，回复就发不出去了。

**和设计的出入**：

- 没有 runtime 级的 draining 状态：由 aiohttp 适配层逐个连接叫停。连接对象在 `AiohttpServer` 手里，runtime
  不持有。
- 发送循环在 `PUSH_DRAINED` 时顺带发掉待发区里的推送和 sync。
- 原设计是排空期间的 PUSH_CLOSE 改用 `await put`，实际另加了 `PUSH_DRAINED`。两者语义不同：
  - PUSH_CLOSE 的意思是"接收协程已经 fail 了连接"，发送循环取到就返回；
  - 排空时连接还开着，要发完再断开。
- 判断排空完没有，看的是"连接关了"（被 fail，或者对端已经断开），不只是"被 fail"。

**留到第 6 步**：

- 配置模板和文档里写 `SHUTDOWN_DRAIN_TIMEOUT`，提示设置 compose 的 `stop_grace_period`：这个键现在只在 aiohttp
  栈上生效，而 `HETU_HTTP_SERVER` 本身不进文档。
- 关服总时长：通常是排空的时间加断线 System 的时间。最坏情况下，排空（最多 `SHUTDOWN_DRAIN_TIMEOUT`）之后，
  下面三步各有一份 `GRACEFUL_SHUTDOWN_TIMEOUT` 的预算：
  - 取消收发任务；
  - 等连接拆完；
  - 停 FutureCalls。

  取消通常立即生效。默认配置本来就可能超过 docker 默认的 10 秒宽限期，第 6 步写 `stop_grace_period` 的提示时
  按这个估算。

**测试**：

- `test_ws_serve.py`（假连接，Redis / SQLite）：
  - 空闲的连接：马上以 1001 断开，照常拆完；
  - 正在执行的 RPC：执行完、回复送达之后才 1001，排在后面没读的消息不执行。假连接的发送带延迟，模拟慢链路：
    回复还在发的时候接收协程就已经停下了；
  - 在后台等着的订阅：做完、回复送达之后才 1001；
  - 排空时队列是满的（链路慢）：`PUSH_DRAINED` 等着塞进去，已经执行的调用的回复都送达；
  - 握手之前就排空：安静地结束；发送卡住时，适配层的兜底照样能收掉。
- `test_websocket.py`：
  - 发送循环取到 `PUSH_DRAINED` 时发掉推送和 sync 再 1001，不计发送频率。用例把 `PUSH_MAX_HOLD` 调大，推送
    不会在普通轮次里先发出去；
  - 发推送期间交来的 tick，它的 sync 不发。
- `test_aiohttp_adapter.py`：
  - `stop_receiving` 叫醒正在等的 recv，之后的 recv 也抛，发送照常；
  - 缓冲里没读的消息不交出去；
  - 叫醒之后取消计数归零；叫醒用的取消和真的取消碰在一起时照常取消；进 recv 之前吞过一次取消的任务，叫醒
    照样转成 `Draining`；
  - `wait_closed`：被 fail、recv 发现对端断开（正常关闭、直接断开 TCP）、send 发现连接断了、心跳超时之后
    send；
  - 握手中遇到排空收到 1001；
  - `_drain` 按预算等，都排空了就不等满；不等对端已经断开、还在拆的连接；被打断后再调只等剩下的预算，在等的
    任务都收掉。
- `test_server_signals.py`（真起 `hetu start`）：
  - aiohttp 栈：在途的 RPC 执行完、提交，回复送达后 1001，后面没读的不执行；超出排空预算的长 RPC 被取消、
    没有提交；
  - Sanic 栈仍是取消：原来"关服取消在途 RPC"的用例改成只跑 Sanic 栈；
  - 这几个用例等服务器输出里出现 `slow_mark` 开始执行的标记再发信号，不靠 sleep 估计 RPC 已经在跑了。
- 变异检查：下面 17 个都被抓到：
  - 排空时不等发送那一边；
  - 没有排空阶段；
  - `stop_receiving` 不叫醒；
  - 叫醒吞掉真的取消；
  - 排空时取消订阅；
  - `PUSH_DRAINED` 不发推送；
  - 发完推送才取 sync；
  - 握手中排空用 1000；
  - 排空不设预算；
  - 被打断后再调重新等满预算；
  - 被打断时不收掉在等的任务；
  - `PUSH_DRAINED` 用 `put_nowait`；
  - recv 发现对端断开不算关了；
  - send 发现连接断了不算关了（`ConnectionError`、`ws.closed` 两处）；
  - 叫醒时假定进 recv 时的取消计数是 0；
  - 叫醒不撤销取消计数。

**PR #231 审查后的修改**：

- 对端已经断开、还在跑断线 System 的连接也算排空完了。原来要等它拆完，到点还会误报没排空。
  - `wait_failed` 改成 `wait_closed`，recv 发现关闭、send 发现断开也触发；
  - 警告的文案改成"没在 N 秒内排空"。
- recv 叫醒时比较进来时的取消计数。
- 补测试：
  - 队列满时 `PUSH_DRAINED` 不能丢；
  - 叫醒后取消计数归零；
  - 对端断开、心跳超时之后的 send。
- 测试时序：
  - 发送循环的排空用例把 `PUSH_MAX_HOLD` 调大；
  - 命令行用例等服务器输出里的标记再发信号。
- 文档：
  - §5.3 关于 PUSH_CLOSE 的说法；
  - "和设计的出入"；
  - `serve_connection`、`AiohttpServer.connections` 的注释；
  - "不提交"的说法；
  - 关服总时长。

### 5.6 第 4c 步：性能对比（已测，结果见 §12.2）

- 用 `benchmark/rpc_throughput.py` 测 hello_world，方法照 `hello_world_optimization_report.md`：服务端绑 P 核
  CPU 3，4 个客户端进程各 16 个连接绑 E 核，5 × 10 秒窗口。用 `HETU_HTTP_SERVER` 切换 sanic / aiohttp，其他
  条件不变。再跑 `sub_scenarios_ws.py` / `sub_budget_ws.py` 测推送每帧的成本。结果写进 §12。
- 用 py-spy 按调用栈筛请求路径时（`get2_update2_optimization_report.md` 筛的是 `client_handler` /
  `websocket_connection`）：第 3 步之后发送那一侧单独一个任务，栈在 `_send_side` / `send_loop` 下面，handler
  改名为 `serve_connection`，筛选条件要跟着改，否则会漏掉发送的开销。
- Sanic 撑不过 Python 3.16，这次迁移是被迫的，所以收益多少不决定做不做。预期约 14µs/RPC（QPS 约 +50%），
  **收益低于 +20% 就先查原因**。**进入第 5 步的条件**是：RPC 和推送的每帧成本、p99 延迟都没有比 Sanic 差
  （§14 第 8 条）。
- 人工端到端测一次：Unity / C# headless 客户端连接、调用、订阅，被顶号时显示 close 码，断线后能重连。
- 实际做法和上面的出入（详见 §12.2）：
  - perf 这次能读计数器，开销按 user 态周期比，没有用 py-spy（它在 Python 3.14 上会把 C 层时间算到协程帧上）。
    收益远高于 +20%，没有按调用栈细查，只按 DSO 看了周期花在哪。
  - 端到端只测了 C# headless：测试工程的 NUnit 宿主要 ASP.NET Core 运行时，本机没装，就写了个只引用
    `HeTu.Client` 的控制台程序跑，没有入库。Unity 客户端没测。

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
    worker 必须只认第一个关服信号，第 2 步已经在 Sanic 栈上这么做了（`_guard_shutdown_signals`，见 §5.2 末尾）；
    也可以让 worker 不在终端的前台进程组里。master 自己也要幂等，不能像 Sanic 那样第二个信号就 SIGKILL 整个
    进程组。
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
| 删除 `CERT_CHAIN`、`--cert` | 2 | 无（不需要向前兼容，§14 第 2 条） |
| 删除 `ACCESS_LOG`、`INSPECTOR`、`INSPECTOR_PORT` | 2 | 无：直接不读，Sanic 栈强制关掉 |
| 关颜色改用标准的 `NO_COLOR` 环境变量，`SANIC_NO_COLOR` 不再认；YAML 里 Sanic 的 `LOG_EXTRA` / `NO_COLOR` 键不再生效 | 2 | 无 |
| worker 收到关服信号后忽略之后的关服信号：单进程模式下第二次 Ctrl+C 不再强退 | 2 | 无 |
| LOGGING 里的 `sanic.logging.formatter.AutoFormatter` 等 `sanic.*` 类 | 2、6 | 无：第 6 步删 Sanic 后 dictConfig 会 ImportError |
| `SANIC_` 前缀的环境变量配置失效 | 2 | 无 |
| `FORWARDED_SECRET` 不再支持 | 4a（aiohttp 栈）、6 | 无（同 §14 第 2 条） |
| `@hetu.define_route` / `@hetu.on_server_setup` 在 aiohttp 栈上被跳过（打警告），第 6 步改成 aiohttp 接口 | 4a（aiohttp 栈） | 无 |
| 开服途中收到关服信号：aiohttp 栈中止开服、以 0 退出；Sanic 栈 SIGTERM 直接杀掉进程（-15），SIGINT 抛 KeyboardInterrupt | 4a（aiohttp 栈）、6 | 无 |
| HTTP 层：ws 路径上的普通 GET 回 400（Sanic 回 426），请求头超长回 400（Sanic 回 413）；畸形请求（Content-Length 非法、请求头超过 8190 字节）aiohttp 打一条带堆栈的 ERROR，Sanic 不打；ws 路径上的普通 GET 反过来是 Sanic 打 ERROR | 4a（aiohttp 栈）、6 | 无。HeTu 前面有 Caddy 时这些请求一般到不了 |
| `define_route` / `on_server_setup` 改为 aiohttp 接口 | 6 | 无，发版说明给出对照示例 |
| `ctx.request` 变成 aiohttp 的 `web.Request` | 6 | 无（文档本来就标注 unsafe） |
| Sanic app 的 `app.ctx` 上不再有 `table_managers`、`default_backend` 和各后端名的属性，统一改为放在 `app.ctx.runtime` 上。这些属性没写进文档，但用户的 `on_server_setup` 钩子可能用过 | 1 | 无 |
| `on_server_setup` 只在 worker 里执行，不再在管理进程执行 | 5（aiohttp 栈）、6 | 无 |
| 关服时在途的 RPC 先处理完、回复发出去再断开，没读的消息不执行；新配置 `SHUTDOWN_DRAIN_TIMEOUT`（默认 5 秒），超时的取消 | 4b（aiohttp 栈）、6 | 无 |
| ws 心跳超时：客户端看到 1006（aiohttp 直接断开 transport），以前是 1011；`WEBSOCKET_PING_TIMEOUT` 不再可配，等 pong 的时间是 heartbeat/2 | 4a（aiohttp 栈）、6 | 无 |
| 发送数据时出了意外的异常：客户端收到的 close 码从 1000 变成 1011 | 3 | 无 |
| 落库 Connection 行失败时（包括同 IP 匿名连接数超限）：close 码从 1000 变成 1011；匿名连接数超限时不再打堆栈 | 3 | 无 |
| Windows 开发环境关服时，客户端收到的 close 码从 1000（清理完才发）变成 1001。C# SDK 对 1000 会把 reason 置为 null（`WebSocketTransport.cs:62-65`），按 reason 判断"正常关闭"的代码会受影响 | 4a（aiohttp 栈）、6 | 无 |
| 崩溃的 worker 会被自动重启（Sanic 不会） | 5（aiohttp 栈）、6 | 无 |

## 7. 测试计划汇总

| 步 | 验证方式 |
|---|---|
| 0 | 全部现有测试照旧通过；新增 `test_harness_*` 守住 harness 的语义（§5.0） |
| 1 | 现有测试（含 §3.4 修复时加的测试）照旧通过；假 app 改成假 runtime |
| 2 | 新增：`HeTuConfig`；`worker_main` 强制关掉残留的 `ACCESS_LOG` / `INSPECTOR`；`HETU_WORKER_INDEX` 和回退；formatter 的 golden test（和 Sanic 逐字对照、上颜色、record 不变） |
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
6. `bench/aiohttp-perf`：第 4c 步的性能对比，结果写进 §12.2
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

### 12.2 第 4c 步性能对比（2026-11-15）

同一份代码（dev `52feb264`，第 4b 步合并后），只用 `HETU_HTTP_SERVER` 切换两个栈。压测脚本的 `--http-server`
就是设这个变量；每次运行都核对过服务器日志里的"Web 服务器：sanic / aiohttp"。

**环境与方法**

- 同 §12.1 的笔记本，CachyOS（Linux 7.3），`platform_profile=performance`、EPP `performance`；Python 3.14.10 +
  uvloop 0.23.0；Sanic 25.12.1（websockets 17.4）、aiohttp 3.14.5；cryptography 50.0.2、msgspec 0.22.0；
  Redis 8.10.3（docker host 网络）。
- 这次 perf 能读计数器（§12.1 时读不到），开销按服务器的 user 态周期数比：P 核频率随负载变（推送那几档窗口的
  平均频率在 0.8~4.7GHz 之间），两个栈的负载不同，CPU 时间跨栈不可比。`perf_event_paranoid=2` 只能数 user 态。
  RPC 那组服务器是饱和的、两边频率一样，另给含内核态的 CPU 时间；按平均频率折算，内核态每次 RPC 约
  6.2k（Sanic）/ 5.4k（aiohttp）个周期，aiohttp 没有多出系统调用的开销。
- 两个栈交替跑：Sanic A → aiohttp A → Sanic B → aiohttp B。推送的尾延迟另补了几组窗口，表中注明。

**RPC（hello_world）**

方法同 `benchmark/hello_world_optimization_report.md`：`rpc_throughput.py --perf`，服务器绑 CPU 3，4 个客户端
进程各 16 个连接绑 CPU 4/6/8/10，预热 5 秒，每批 5 × 10 秒窗口，客户端源码固定在 `52feb264`。表中是两批共
10 个窗口的中位数。

| 栈 | QPS | CPU µs/RPC | user kcycles/RPC | p50 / p99 ms | 服务器核频率 |
|---|---:|---:|---:|---:|---:|
| Sanic | 43,837 | 21.93 | 96.7 | 1.453 / 1.604 | 4.69GHz |
| aiohttp | 81,631 | 11.51 | 48.2 | 0.780 / 0.858 | 4.65GHz |
| 变化 | **+86.2%** | −47.5% | −50.2% | −46% / −47% | |

- 两批一致：QPS Sanic 43,881 / 43,794，aiohttp 81,856 / 81,366；全部窗口 43,476~44,493 / 81,128~82,215。
- 服务器 CPU 利用率 Sanic 96%、aiohttp 94%。aiohttp 那组每个客户端进程用了约 74% 的核（Sanic 那组约 42%），
  服务器偶尔要等请求，QPS 偏保守；每次 RPC 的成本不受影响。
- 比 §1.2 的估算（约 14µs/RPC，QPS 约 +50%）好：每次 RPC 省 10.4µs，§1.2 帧级对比的差值是 7.7µs。
- 省在哪：另跑一次，`perf record -e cycles:u` 按 DSO 汇总，折成每次 RPC 的 user 态 kcycles：

  | DSO | Sanic | aiohttp |
  |---|---:|---:|
  | libpython（解释器） | 82.2 | 36.7 |
  | cryptography（`_rust`，加解密） | 4.7 | 4.3 |
  | libc + ld.so | 4.8 | 2.9 |
  | uvloop | 2.2 | 1.5 |
  | zlib | 1.1 | 0.9 |
  | aiohttp 的读帧 / 掩码（编译扩展） | — | 0.7 |

  省下的 48.5k 里 45.5k（94%）在解释器里：Sanic 这边的 ws 收发（websockets 的 sans-I/O 协议解析帧、Sanic 的
  帧组装和 `recv_streaming`）是 Python 代码，aiohttp 的读帧、掩码是编译扩展。加解密、压缩两边一样。比 §1.2
  多省的 2.7µs 也在解释器里，这次没有再细分到函数。

**推送**

- `sub_scenarios_ws.py`：2000 个连接，环境同 `benchmark/sub_scenarios_result.md`（worker 绑 CPU0；RPC 探针是一个
  不订阅的连接，每 20ms 发一次 ping，绑 CPU1；Redis 主 / 副本 CPU2 / 3；8 个客户端进程在 E 核；写进程在低功耗
  E 核），每档 5 秒预热 + 30 秒窗口。
- `sub_budget_ws.py`：1000 个连接，命令同 `benchmark/sub_budget_result.md`，20 秒窗口。
- 所有窗口核对一致性都是 0 差异，消息全部送达，服务器日志 0 条错误。Sanic 这一侧和 `sub_scenarios_result.md`
  的数一致（聊天 10 条/秒每条消息 137.8M 周期，那边是 137M）。

每帧成本（user 态 kcycles；A / B 两批，或注明的中位数）。聊天 10 条/秒时每个连接每 100ms 收一帧、每帧约 1 条，
每帧的成本就是推送一帧的成本；背包按每次写入算：

| 负载 | Sanic | aiohttp | 变化 |
|---|---|---|---|
| 聊天 10 条/秒（各 6 个窗口的中位数） | 68.8 | 61.5 | −10.5% |
| 聊天 100 条/秒（每帧约 10 条） | 114.5 / 113.8 | 107.2 / 107.8 | −5.8% |
| 聊天 200 条/秒（每帧约 20 条，各 6 个窗口的中位数） | 146.4 | 139.8 | −4.5% |
| sub_budget_ws：K=10，1000 次写/秒 | 91.2 / 91.1 | 85.1 / 84.5 | −7.0% |
| sub_budget_ws：K=10，2000 次写/秒 | 78.7 / 78.6 | 72.0 / 72.7 | −8.0% |
| sub_budget_ws：K=100，300 次写/秒（每帧约 3.6 行） | 81.0 / 79.9 | 74.0 / 73.9 | −8.1% |
| 背包增删 2000 次/秒（每次写入） | 725.5 / 724.7 | 708.9 / 707.2 | −2.4% |
| 背包增删 4000 次/秒（每次写入，各 6 个窗口的中位数） | 615.7 | 605.5 | −1.7% |

- 每帧都少 6.3~7.3k 个周期（4.7GHz 下约 1.4µs），和 §1.2 的估算（每帧省 1~1.5µs）一致。背包每次写入的大头是
  重读、比对，框架那段占得少。

尾延迟（ms）：

| 负载 | 栈 | 推送 p99 | 全员送达 p99 | RPC 探针 p50 / p99 |
|---|---|---|---|---|
| 聊天 10 条/秒，6 个窗口 | Sanic | 126.7~129.6 | 133.0~137.4 | 0.44~0.48 / 20.07~20.38 |
| | aiohttp | 122.6~125.8 | 130.5~131.9 | 0.39~0.53 / 20.20~20.53 |
| 聊天 100 条/秒，A / B | Sanic | 147.0 / 146.0 | 157.6 / 155.7 | 0.46 / 56.7，0.49 / 53.9 |
| | aiohttp | 143.5 / 143.3 | 153.6 / 153.6 | 0.39 / 54.9，0.40 / 55.1 |
| 聊天 200 条/秒，6 个窗口 | Sanic | 158.0~160.1 | 170.1~176.8 | 45~55 / 69.5~79.3 |
| | aiohttp | 155.6~156.9 | 166.9~172.4 | 0.7~48 / 71.4~79.0 |
| 背包增删 2000 次/秒，A / B | Sanic | 108.7 / 109.0 | | 0.28 / 1.62，0.29 / 1.63 |
| | aiohttp | 108.7 / 108.8 | | 0.26 / 1.32，0.26 / 1.38 |
| 背包增删 4000 次/秒，6 个窗口 | Sanic | 131.5~134.8；碰上全量 GC 的窗口 259.8 | | 0.31~0.38 / 2.56~5.08 |
| | aiohttp | 120.8~128.5；碰上全量 GC 的窗口 212.4 | | 0.27~0.28 / 2.22~4.02 |
| sub_budget_ws 三档（顺序同上表），A / B | Sanic | 112.5 / 112.1，111.6 / 111.6，133.6 / 133.3 | | |
| | aiohttp | 112.0 / 111.7，111.3 / 111.1，131.4 / 130.4 | | |

- 推送 p99 每档都持平或更低，全员送达 p99 也整体偏低（聊天 200 条/秒两边的范围有重叠，中位数低 3.8ms）。
- 背包 4000 次/秒 6 个窗口里两边各碰上一次 200~240ms 的全量 GC（`sub_scenarios_result.md` 记过，两版都有），其余
  窗口 aiohttp 的推送 p99 低 5~10ms。
- 聊天 200 条/秒时一轮 2000 帧的扇出约占事件循环的 65%。探针每 20ms 一次，p50 落在忙还是闲取决于两者的相位，
  所以 aiohttp 有的窗口 0.7ms、有的 44~48ms（Sanic 一直 45~55ms）；p99 两边的分布一样（中位数 75.8 / 75.7ms）。
- **聊天 10 条/秒时 RPC 探针的 p99 aiohttp 高 0.15ms**（中位数 20.46 / 20.31ms，+0.7%，6 个窗口的范围几乎不
  重叠），最大值高约 4.6ms（中位数 45.2 / 40.6ms）。其余负载下探针的最大值两边相当或 aiohttp 更低。关掉心跳
  （`WEBSOCKET_PING_INTERVAL: 0`）再测 4 个窗口一样（p99 20.41~20.78、最大 44.5~47.1ms），排除了心跳；两边的
  发送都不在每帧上让出事件循环，原因没查到。

**端到端**

C# headless 客户端（`HeadlessHeTuClient`）连 `tests/app.py`，两个栈各跑一遍，结果一样：

- 连接握手、登录、调用，`WatchRow` 订阅；等推送的调用返回时推送已到；
- 被顶号的连接收到 close 码 4001（kicked），顶号的一方照常在线；
- 关服时收到 1001，服务器重启后同一个客户端对象重连，照常调用。

**结论**

- RPC：QPS +86%，远高于 +20% 的排查线，也比 §1.2 的估算（+50%）好。
- 进入第 5 步的条件（§14 第 8 条）：
  - 每帧成本：RPC、推送都下降。
  - p99：RPC 的 p99 降一半；推送 p99 每档都持平或更低；RPC 探针的 p99 在聊天 100 / 200 条/秒和背包负载下相同或
    更低。
  - 唯一的例外是上面聊天 10 条/秒时探针的 p99（+0.15ms，+0.7%）。差别不到 1%，建议不拦第 5 步，第 5 步多
    worker 实测时再看。

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
9. 第 2 步的兼容处理（LOGGING 里 `sanic.*` 类的改写、删除键的警告）都不做，直接砍掉，理由同 §14 第 2 条。

## 14. 审查时的待定问题（2026-11-14 全部定下）

括号里是审查意见（§15），粗体是最终决定。编号保留，供正文引用。

1. 关服排空（第 4b 步）只在 aiohttp 栈上做，单独设一个短预算 `SHUTDOWN_DRAIN_TIMEOUT`。（赞成。）**同意。**
2. `CERT_CHAIN` 有值或给了 `--cert` 时拒绝启动，而不是打警告后跑明文；判断看真值。（赞成。）**不做检查：
   还没有线上游戏，不需要向前兼容，`CERT_CHAIN` 和 `--cert` 直接删。**
3. 第 5 步 master 不加载 app 文件，`on_server_setup` 不再在 master 里执行；ready 之前任何退出都算整体失败。
   （赞成。）**同意。**
4. DEBUG 自动重载用轮询 mtime 实现，不加依赖，监视范围同 Sanic。（赞成，Sanic 本身就是这么做的，Windows
   上也能用。）**同意。**
5. webext 请求体上限：沿用 `REQUEST_MAX_SIZE` 这个键名，默认 100MB，映射到 `client_max_size`。（赞成，上传
   接口不会悄悄开始返回 413。）**同意。**
6. 第 5 步之前，`WORKER_NUM>1` 配上 `HETU_HTTP_SERVER=aiohttp` 时报错退出。（赞成，降级为单进程会让 A/B
   数据失真。）**同意。**
7. 默认共享监听 socket，SO_REUSEPORT 实测后作为可选项。（赞成，跨平台，和现在一致，worker 挂掉时不会重置
   排队中的连接。）**同意。**
8. 性能：+20% 只用来触发排查；进入第 5 步的条件是每帧成本和 p99 都不退步。（审查提出的改法。）**同意。**
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
