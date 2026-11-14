"""
Sanic 适配：建 Sanic app，把它的生命周期接到 `WorkerRuntime` 上，把 ws 连接包成 `WsConnection` 交给
`serve_connection`。换成 aiohttp 后删除（设计稿
docs/superpowers/specs/2026-11-14-aiohttp-migration-design.md §5.1、§5.3、§5.8）。

@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2026, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com

Sanic adapter: builds the Sanic app and wires its lifecycle into `WorkerRuntime`.
"""

import asyncio
import logging
import signal
from collections.abc import Mapping
from typing import Any, cast

from sanic import Request, Sanic, Websocket
from sanic.exceptions import WebsocketClosed
from sanic.worker.manager import WorkerManager
from sanic.worker.process import WorkerProcess

from ... import webext
from ...i18n import _
from ..config import HeTuConfig
from ..conn import ConnectionClosed, next_conn_id
from ..runtime import (
    DEFAULT_STARTUP_TIMEOUT,
    LEASE_LOST_EXIT_CODE,
    STARTUP_TIMEOUT_CONFIG_KEY,
    WorkerRuntime,
    prepare_worker,
    setup_logging,
    setup_message_pipeline,
)
from ..threaded_accept import IS_WINDOWS, threaded_accept_listener
from ..web import web_root
from ..websocket import serve_connection

logger = logging.getLogger("HeTu.root")

# 管理进程等worker ack的超时 = STARTUP_TIMEOUT + 这个余量。
# 两级超时不是冗余：我们自己的 asyncio 超时挡不住阻塞式代码（启动System里写了
# time.sleep/requests 之类把事件循环卡死），那时只有管理进程（另一个进程在数数）能兜底。
# 余量只需盖住我们量不到的部分：进程spawn、import app文件、bind端口。
ACK_TIMEOUT_MARGIN = 10.0
# "不限制"给sanic的阈值。sanic的单位是0.1秒，此值约3年，等同永不超时。
# 注意不能给它0，0是"第一次轮询(0.1秒)就判定worker启动失败"，意思正好相反
UNLIMITED_ACK_THRESHOLD = 10**9


def resolve_ack_threshold(config: Mapping[str, Any]) -> int:
    """按 STARTUP_TIMEOUT 算出管理进程等worker ack的阈值（sanic单位为0.1秒）。

    ack超时必须晚于我们自己的启动超时，否则卡住时先被管理进程SIGKILL掉整个进程组，
    用户只能看到sanic那句没头没脑的"Not all workers acknowledged"，不知道卡在哪。
    所以它不单独配置，由 STARTUP_TIMEOUT 推算；STARTUP_TIMEOUT 为0(不限制)时它也不限制。

    Derive the manager's worker-ack threshold (in units of 0.1s) from STARTUP_TIMEOUT,
    so HeTu's own, more specific timeout always fires first.
    """
    startup_timeout = config.get(STARTUP_TIMEOUT_CONFIG_KEY, DEFAULT_STARTUP_TIMEOUT)
    if not startup_timeout:
        return UNLIMITED_ACK_THRESHOLD
    return max(1, int((float(startup_timeout) + ACK_TIMEOUT_MARGIN) * 10))


def on_lease_lost(app: Sanic, runtime: WorkerRuntime) -> None:
    """
    续约发现 worker id 已经不属于本进程：再发号就可能和接手的进程撞号（发号围栏会拦住，但
    本进程从此发不出号），必须换一个新进程重新租。

    多进程交给 Sanic 管理进程只重启本 worker。单进程没有管理进程（没有 app.multiplexer，
    app.m 会抛 AttributeError）：停服并把 runtime.exit_code 置为非 0，由 hetu start 退出时
    带出，交给 docker/systemd/k8s 重启。
    """
    if hasattr(app, "multiplexer"):
        app.multiplexer.restart()
        return
    logger.error(
        _(
            "❌ [📡WorkerKeeper] Worker ID 租约已丢失，单进程模式没法只重启 worker，停止服务器"
        )
    )
    runtime.exit_code = LEASE_LOST_EXIT_CODE
    if app.state.is_running:
        app.stop()
    else:
        # 还在开服：Sanic 用 run_until_complete 跑各个启动钩子，这时 app.stop() 停掉 loop 会让
        # 开服报错。进入正常服务再停；登记进 Sanic 的任务表，没等到就关服时由它取消
        app.add_task(_stop_when_serving(app), name="hetu:stop_on_lease_lost")


async def _stop_when_serving(app: Sanic) -> None:
    while not app.state.is_running:
        await asyncio.sleep(0.1)
    app.stop()


class SanicWsConnection:
    """
    把 Sanic 的 Websocket 包成 `WsConnection`。客户端地址用 Sanic 解析好的 `request.client_ip`
    （按 PROXIES_COUNT、REAL_IP_HEADER 等配置）。

    Sanic 上对端关闭之后 recv() 可能一直挂着（recv_streaming 不返回），要等 Sanic 取消连接的
    handler；对端在 recv(timeout) 期间断开时，Sanic 会让它抛 CancelledError。

    Adapts a Sanic Websocket to `WsConnection`.
    """

    __slots__ = ("_ws", "client_ip", "conn_id", "request")

    def __init__(self, request: Request, ws: Websocket, conn_id: str) -> None:
        self.conn_id = conn_id
        self.client_ip: str = request.client_ip
        self.request = request
        self._ws = ws

    async def recv(self, timeout: float | None = None) -> str | bytes | None:
        try:
            if timeout is not None:
                return await self._ws.recv(timeout)
            # 不限时就走 recv_streaming 自己重组：Sanic 的 recv() 每条消息都要建一个 Task 再
            # asyncio.wait，慢。帧迭代器必须取完，否则这个连接就不能再收了
            chunks = [chunk async for chunk in self._ws.recv_streaming()]
        except WebsocketClosed as e:
            raise ConnectionClosed from e
        if len(chunks) == 1:
            return chunks[0]
        if chunks and isinstance(chunks[0], str):
            return "".join(cast(list[str], chunks))
        return b"".join(cast(list[bytes], chunks))

    async def send(self, data: str | bytes) -> None:
        try:
            await self._ws.send(data)
        except WebsocketClosed as e:
            raise ConnectionClosed from e

    def fail(self, code: int, reason: str) -> None:
        # 同步、立即生效、可以重复调用；关闭握手超过 close_timeout 就 transport.abort()
        self._ws.fail_connection(code, reason)

    def protocol_error(self) -> BaseException | None:
        # parser_exc 是 EOFError 时是对端直接断开，不算协议错误
        exc = self._ws.ws_proto.parser_exc
        if exc is None or isinstance(exc, EOFError):
            return None
        return exc


async def _serve_websocket(request: Request, ws: Websocket, db_name: str) -> None:
    """ws 路由 /hetu/<db_name>：Sanic 做完 ws 握手后，把连接交给 serve_connection"""
    conn = SanicWsConnection(request, ws, next_conn_id())
    await serve_connection(request.app.ctx.runtime, conn, db_name)


_SHUTDOWN_SIGNALS = (signal.SIGINT, signal.SIGTERM)


def _ignore_shutdown_signals() -> None:
    for sig in _SHUTDOWN_SIGNALS:
        signal.signal(sig, signal.SIG_IGN)


async def _guard_shutdown_signals(app: Sanic) -> None:
    """
    after_server_start（Windows 除外）：关服信号只认第一个，之后的都忽略。

    Sanic 在 worker 里给 SIGINT / SIGTERM 注册的处理是每次都调 app.stop()。终端里 Ctrl+C 会把 SIGINT
    发给整个进程组，多 worker 时管理进程还会再转发一次；systemd 默认的 KillMode=control-group 也会给
    每个进程都发 SIGTERM。所以 worker 常常会收到第二个信号。Linux 上用的是 uvloop，loop 停下之后
    remove_signal_handler 不起作用，第二个信号照样 loop.stop()，正在跑的关服步骤的 run_until_complete
    抛 RuntimeError：

    - 落在 before / after_server_stop 里：Sanic 的 _run_shutdown_coro 把它整个重跑一遍，第一遍的 task
      还挂着接着跑（runtime.stop() 为此只执行一次）；
    - 落在 _cleanup 等连接拆完的那段里：异常没人接，after_server_stop 整个被跳过，断线 System 不跑、
      Connection 行不删、租约不释放、后端不关。

    所以第一个信号一到就把两种关服信号都忽略掉，再走 Sanic 原来的停服。代价是单进程模式下第二次
    Ctrl+C 不能强退；关服的每一步都有 GRACEFUL_SHUTDOWN_TIMEOUT 兜底，不会一直卡住。
    """
    loop = asyncio.get_running_loop()

    def on_shutdown_signal() -> None:
        app.ctx.shutdown_signal_received = True
        _ignore_shutdown_signals()
        app.stop(terminate=False)

    # 在 Sanic 注册之后（after_server_start）再注册，覆盖它的
    for sig in _SHUTDOWN_SIGNALS:
        loop.add_signal_handler(sig, on_shutdown_signal)


async def _keep_ignoring_shutdown_signals(app: Sanic) -> None:
    """
    before_server_stop：Sanic 停服时会卸掉信号处理。标准 asyncio 的 remove_signal_handler 会把 SIGINT
    恢复成抛 KeyboardInterrupt、把 SIGTERM 恢复成直接终止进程，这里重新忽略，保护接下来的关服步骤。
    只在确实是收到关服信号停服时才这么做：测试、嵌入场景里由代码调用 app.stop() 时不动进程的信号处理
    """
    if getattr(app.ctx, "shutdown_signal_received", False):
        _ignore_shutdown_signals()


async def _runtime_start(app: Sanic) -> None:
    """before_server_start：每次开服新建一个 runtime（Sanic 的测试客户端会对同一个 app 反复起停）"""
    runtime = WorkerRuntime(
        app.config, on_lease_lost=lambda: on_lease_lost(app, runtime)
    )
    app.ctx.runtime = runtime
    await runtime.start()


async def _runtime_start_background(app: Sanic) -> None:
    """after_server_start：开始监听之后再启动 FutureCalls"""
    app.ctx.runtime.start_background()


async def _runtime_stop(app: Sanic) -> None:
    """after_server_stop：Sanic 已停止监听、对各 ws 发了 1001，这时按 runtime 的顺序关服"""
    # Windows 多 worker 时不会执行：sanic 停 worker（Ctrl+C、DEBUG 自动重载）是
    # TerminateProcess 硬杀，留下的租约由 live_worker_ids 认出来（见 redis/worker_keeper.py）
    await app.ctx.runtime.stop()


def worker_main(app_name: str, config: Mapping[str, Any]) -> Sanic:
    """
    此函数会执行 workers+1 次。但如果是单worker，则只会执行1次。
    多worker时，第一次是Main函数的进程，负责管理workers，执行完不会启动任何注册的listener。
    后续Workers进程才会执行注册的listener。
    """
    # 测试、嵌入场景直接传 dict，补上默认值
    if not isinstance(config, HeTuConfig):
        config = HeTuConfig(config)

    # 加载玩家的app文件、建簇、把配置传给各模块
    prepare_worker(config)

    # 日志由 HeTu 自己配，不交给 Sanic
    setup_logging(config)

    # 加载web服务器。不让 Sanic 读 SANIC_ 前缀的环境变量：配置只有 HeTuConfig 这一份，CLI、runtime、
    # ack 阈值读到的都一样
    app = Sanic(app_name, configure_logging=False, env_prefix=None)
    app.update_config(config)
    # 访问日志交给前面的反向代理；Sanic 的 Inspector 不用（以后由 CLI 提供 reload）。配置里就算还
    # 留着这两个键也关掉
    app.config.ACCESS_LOG = False
    app.config.INSPECTOR = False

    # 设置管理进程等ack的超时。此函数在管理进程也会执行一次（AppLoader.load），
    # 所以在这里改类属性就能生效。注意WorkerManager.THRESHOLD是模块导入时从
    # WorkerProcess复制的，只改WorkerProcess的没用，两个都要设。
    ack_threshold = resolve_ack_threshold(config)
    WorkerManager.THRESHOLD = ack_threshold  # type: ignore
    WorkerProcess.THRESHOLD = ack_threshold  # type: ignore

    # 重定向logger，把sanic的重定向到hetu
    root_logger = logging.getLogger("sanic")
    root_logger.parent = logger
    if int(config["DEBUG"]) > 1:
        root_logger.setLevel(logging.DEBUG)

    # 加载协议, 初始化消息处理流水线
    setup_message_pipeline(config)

    # 服务器main进程setup/teardown回调
    # app.main_process_start()
    # app.main_process_stop()
    # 服务器work进程setup/teardown回调：开服、关服的顺序都由 runtime 管
    app.before_server_start(_runtime_start)
    app.after_server_stop(_runtime_stop)
    # Windows 多 worker 共享监听socket，accept 会把事件循环堵死，改到专用线程里接，
    # 见 threaded_accept.py
    if IS_WINDOWS:
        app.before_server_start(threaded_accept_listener)
    else:
        # 关服信号只认第一个，之后的不能打断关服流程，见 _guard_shutdown_signals
        app.after_server_start(_guard_shutdown_signals)
        app.before_server_stop(_keep_ignoring_shutdown_signals)

    # 开始监听之后启动 FutureCalls，要注册在用户的钩子（webext.apply_to）之前。续约、水位预留
    # 两个循环在 runtime.start() 拿到租约时就起了
    app.after_server_start(_runtime_start_background)

    # HeTu 的 ws 入口
    app.add_websocket_route(_serve_websocket, "/hetu/<db_name>")

    # 挂载用户自定义的HTTP端点（@hetu.define_route / @hetu.on_server_setup）
    webext.apply_to(app)

    # 最后补上HeTu的默认欢迎页，用户自己注册了"/"就让给用户，不去撞RouteExists
    if not any(route.path in ("", "/") for route in app.router.routes):
        app.route("/")(web_root)
    return app
