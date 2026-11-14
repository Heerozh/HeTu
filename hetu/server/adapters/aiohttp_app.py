"""
aiohttp 适配：单进程跑 HeTu 的服务（设计稿 docs/superpowers/specs/2026-11-14-aiohttp-migration-design.md
§5.4）。和 Sanic 并存的过渡期里由环境变量 `HETU_HTTP_SERVER=aiohttp` 选用（见 hetu/cli/start.py），
多进程要等第 5 步的 supervisor。

- `AiohttpWsConnection`：把 aiohttp 的 WebSocketResponse 包成 `WsConnection`；
- `AiohttpServer`：一次开服到关服，`hetu start` 和测试 harness 共用；
- `run()`：单进程运行，处理关服信号。

@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2026, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com

aiohttp adapter: serves HeTu from a single process on aiohttp.
"""

import asyncio
import ipaddress
import logging
import os
import signal
import socket
from collections.abc import Callable, Mapping
from typing import Any

from aiohttp import WebSocketError, WSCloseCode, WSMsgType, web

import hetu

from ... import webext
from ...endpoint import connection
from ...i18n import _
from ..config import HeTuConfig
from ..conn import ConnectionClosed, Draining, next_conn_id
from ..forwarded import client_ip
from ..receiver import close_connection
from ..runtime import (
    LEASE_LOST_EXIT_CODE,
    WorkerRuntime,
    prepare_worker,
    setup_logging,
    setup_message_pipeline,
)
from ..threaded_accept import IS_WINDOWS, install_threaded_accept
from ..websocket import serve_connection

logger = logging.getLogger("HeTu.root")

# 关闭握手时等对端回 close 帧的时间（秒）。aiohttp 默认等 10 秒。等不到就 transport.close()，写缓冲里
# 剩下的数据（包括 close 帧）照常发完
CLOSE_TIMEOUT = 2.0
# 开始关闭之后，最多再过多久直接 abort（秒），同 Sanic 的 close_timeout。abort 会丢掉写缓冲里还没发出的
# 数据，太短的话链路慢、积压多的客户端收不到 close 帧（比如顶号的 4001）
ABORT_TIMEOUT = 10.0
# HTTP keep-alive 的空闲超时（秒），对应 Sanic 的 REQUEST_TIMEOUT。aiohttp 默认 3630 秒
KEEPALIVE_TIMEOUT = 60.0
# runner.cleanup() 等还在处理的 HTTP 请求的时间（秒）。ws 连接在那之前已经由 HeTu 自己拆完
SHUTDOWN_TIMEOUT = 2.0
SHUTDOWN_SIGNALS = (signal.SIGINT, signal.SIGTERM)
# Windows 上每隔多久叫醒一次事件循环（秒）：selector 事件循环等 I/O 时 Python 收不到信号，Ctrl+C
# 要等它醒来才生效
WINDOWS_WAKEUP_INTERVAL = 0.1

# fail() 起的关闭任务：asyncio 只弱引用 task，连接对象没了之后由这里持有，直到关完
_closing_tasks: set[asyncio.Task] = set()


def _discard_closing_task(task: asyncio.Task) -> None:
    _closing_tasks.discard(task)
    if not task.cancelled():
        task.exception()  # close() 自己出的错不用管，别在 gc 时报 "never retrieved"


class HeTuWebSocketResponse(web.WebSocketResponse):
    """
    close() 一进来就挂上 abort 兜底（`arm_abort`）。aiohttp 自己发起的关闭（收到协议错误的帧、对端的
    close 帧、handler 返回时的 write_eof）也在 ABORT_TIMEOUT 内断开：aiohttp 的 close() 等 drain 不受
    任何超时约束，心跳也已经停了，对端不读数据时会一直卡着。

    A WebSocketResponse whose close() always arms an abort fallback.
    """

    _hetu_request: web.BaseRequest | None = None
    _hetu_abort: asyncio.TimerHandle | None = None

    async def prepare(self, request: web.BaseRequest) -> Any:
        self._hetu_request = request
        return await super().prepare(request)

    async def close(
        self, *, code: int = WSCloseCode.OK, message: bytes = b"", drain: bool = True
    ) -> bool:
        self.arm_abort()
        return await super().close(code=code, message=message, drain=drain)

    def arm_abort(self) -> None:
        """ABORT_TIMEOUT 秒后 abort 这条连接的 transport（已经断开的什么也不做）。重复调用只挂一次"""
        if self._hetu_abort is None:
            self._hetu_abort = asyncio.get_running_loop().call_later(
                ABORT_TIMEOUT, self._abort_transport
            )

    def _abort_transport(self) -> None:
        request = self._hetu_request
        transport = request.transport if request is not None else None
        if transport is not None:
            transport.abort()


class AiohttpWsConnection:
    """
    把 aiohttp 的 WebSocketResponse 包成 `WsConnection`（契约见 hetu/server/conn.py）。

    - `recv` 把 aiohttp 的 CLOSE / CLOSING / CLOSED / ERROR 消息都转成 `ConnectionClosed`。ERROR 里的
      `WebSocketError`（帧格式错、消息超长等）记成协议错误：aiohttp 不把它存进 `ws.exception()`。
    - `fail` 按设计稿 §5.4.2：
      - 同步置标记，之后的收发立即抛 `ConnectionClosed`；
      - 关闭任务 eager 启动，带码的 close 帧在 fail 返回前就写出去，handler 返回时 aiohttp 的
        `write_eof()` 不会把它盖成 1000；正在等的 recv 由 aiohttp 的 close() 塞一条 CLOSING 叫醒；
      - `ABORT_TIMEOUT` 秒后 abort 兜底（见 `HeTuWebSocketResponse`）。abort 之后，卡住的 send、在等的
        recv 都会醒来，都抛 `ConnectionClosed`。
    - fail 不 pause_reading（和设计稿最初写的不同）：close() 要读对端回的 close 帧，暂停读取的话每次都
      要等满 `CLOSE_TIMEOUT` 才关 TCP。fail 之后收到的消息不再交出去（recv 收到后再看一次标记）。
    - `stop_receiving`（关服排空）：之后的 recv 抛 `Draining`；正在等的 recv 用取消叫醒，转成 `Draining`。
      aiohttp 的读缓冲对取消是安全的：叫醒时刚到的消息留在缓冲里，不会交出去，也就不会执行。
    - `wait_closed` 等到连接关了：被 fail 了，或者发现对端已经断开（recv 收到关闭、send 发现连接断了）。
      关服排空按它判断排空完没有：对端已经断开的连接没有要发的东西了，不用等它跑完断线 System。

    Adapts an aiohttp WebSocketResponse to `WsConnection`.
    """

    __slots__ = (
        "_closed",
        "_closed_event",
        "_draining",
        "_failed",
        "_protocol_error",
        "_receiving",
        "_wakeup_pending",
        "_ws",
        "client_ip",
        "conn_id",
        "request",
    )

    def __init__(
        self,
        request: web.Request,
        ws: HeTuWebSocketResponse,
        conn_id: str,
        client_ip: str,
    ) -> None:
        self.conn_id = conn_id
        self.client_ip = client_ip
        self.request = request
        self._ws = ws
        self._failed = False
        # 连接关了：被 fail 了，或者发现对端已经断开。见 wait_closed
        self._closed = False
        self._closed_event: asyncio.Event | None = None
        self._protocol_error: BaseException | None = None
        # 关服排空：不再交出新消息
        self._draining = False
        # 正在 recv 里等着的 task，和 stop_receiving 为了叫醒它发的取消还没送到
        self._receiving: asyncio.Task | None = None
        self._wakeup_pending = False

    async def recv(self, timeout: float | None = None) -> str | bytes | None:
        if self._failed:
            raise ConnectionClosed
        if self._draining:
            raise Draining
        task = asyncio.current_task()
        # 进来时的取消计数：叫醒用的取消撤销之后还比它多，就是同时还有别的取消请求（同 asyncio.timeout）
        cancelling = task.cancelling() if task is not None else 0
        self._receiving = task
        try:
            msg = await self._ws.receive(timeout)
        except TimeoutError:
            return None
        except asyncio.CancelledError:
            if not self._wakeup_pending:
                raise
            # stop_receiving 为了叫醒这里发的取消：不是真的取消，撤销掉。同时还有别的取消请求时照常抛
            self._wakeup_pending = False
            if task is not None and task.uncancel() > cancelling:
                raise
            raise Draining from None
        finally:
            self._receiving = None
        if self._failed:
            # 等的时候连接被 fail 了：之后才轮到的消息不再交出去
            raise ConnectionClosed
        if msg.type is WSMsgType.BINARY or msg.type is WSMsgType.TEXT:
            return msg.data
        if msg.type is WSMsgType.ERROR and isinstance(msg.data, WebSocketError):
            self._protocol_error = msg.data
        # 对端关了、连接断了、协议出错（aiohttp 已经在关 transport）：之后收不到也发不出了
        self._mark_closed()
        raise ConnectionClosed

    async def send(self, data: str | bytes) -> None:
        if self._failed or self._ws.closed:
            self._mark_closed()
            raise ConnectionClosed
        try:
            if isinstance(data, str):
                await self._ws.send_str(data)
            else:
                await self._ws.send_bytes(data)
        except ConnectionError as e:  # 含 aiohttp 的 ClientConnectionResetError
            self._mark_closed()
            raise ConnectionClosed from e
        if self._failed:
            # 卡在 drain 上的时候连接被 fail 了：abort 之后 drain 正常返回，这一帧多半没发出去
            raise ConnectionClosed

    def stop_receiving(self) -> None:
        """
        关服排空：不再收新消息。之后的 recv 抛 `Draining`；正在等的 recv 马上醒来抛它。发送不受影响。

        Stop handing out messages; a pending recv raises Draining.
        """
        if self._draining:
            return
        self._draining = True
        if self._receiving is not None and not self._wakeup_pending:
            self._wakeup_pending = True
            self._receiving.cancel()

    async def wait_closed(self) -> None:
        """
        等到这条连接关了：被 fail 了，或者发现对端已经断开（已经关了就马上返回）。之后它不会再发出东西。

        Wait until the connection is failed or found closed by the peer.
        """
        if self._closed:
            return
        if self._closed_event is None:
            self._closed_event = asyncio.Event()
        await self._closed_event.wait()

    def _mark_closed(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._closed_event is not None:
            self._closed_event.set()

    def fail(self, code: int, reason: str) -> None:
        if self._failed:
            return
        self._failed = True
        self._mark_closed()
        # ws 已经关了（对端关的、心跳超时直接关了 transport）也要挂 abort 兜底：transport.close() 会一直
        # 等着把写缓冲发完
        self._ws.arm_abort()
        if not self._ws.closed:
            # 用 Task 构造而不是 create_task(..., eager_start=True)：uvloop 的 create_task 不收这个参数
            task = asyncio.Task(
                self._ws.close(code=code, message=reason.encode()),
                loop=asyncio.get_running_loop(),
                eager_start=True,
            )
            _closing_tasks.add(task)
            task.add_done_callback(_discard_closing_task)

    def protocol_error(self) -> BaseException | None:
        if self._protocol_error is not None:
            return self._protocol_error
        exc = self._ws.exception()
        return exc if isinstance(exc, WebSocketError) else None


def ws_response_options(config: Mapping[str, Any]) -> dict[str, Any]:
    """
    建 WebSocketResponse 的参数。和 Sanic 不一样的默认值都显式设置（设计稿 §5.4.2）：

    - 不协商 permessage-deflate：HeTu 自己有 zlib 层，浏览器又一定会请求压缩；
    - 消息上限是 `WEBSOCKET_MAX_SIZE`：aiohttp 判超限用的是 `>=`，所以加 1，刚好等于上限的消息能收；
      超限以 1009 关闭。None 不限；0 和 Sanic 一样，只收空消息；
    - 每 `WEBSOCKET_PING_INTERVAL` 秒发一次 ping，等 pong 的时间是它的一半，超时直接关 transport（不发
      close 帧）。没配就不发；
    - 关闭时等对端回 close 帧 `CLOSE_TIMEOUT` 秒。
    """
    max_size = config.get("WEBSOCKET_MAX_SIZE")
    ping_interval = config.get("WEBSOCKET_PING_INTERVAL")
    return {
        "compress": False,
        "max_msg_size": int(max_size) + 1 if max_size is not None else 0,
        "heartbeat": float(ping_interval) if ping_interval else None,
        "autoping": True,
        "timeout": CLOSE_TIMEOUT,
    }


class AiohttpServer:
    """
    aiohttp 栈上的一次开服到关服：建 `WorkerRuntime`，起 aiohttp，按 HeTu 自己的顺序关服（不依赖
    aiohttp `on_shutdown` 的顺序）。`hetu start`（单进程）和测试 harness 共用。

    on_stop_requested：服务要求停下时调用（目前只有雪花号租约丢失）。调用方负责随后调 `stop()`，
    `runtime.exit_code` 会被置为非 0。

    One server run on aiohttp, from startup to an orderly shutdown.
    """

    def __init__(
        self,
        config: HeTuConfig,
        *,
        on_stop_requested: Callable[[], None] | None = None,
    ) -> None:
        self.config = config
        self.runtime = WorkerRuntime(config, on_lease_lost=self._on_lease_lost)
        # 还开着的 ws 连接：关服时先排空（_drain），没排空的再 fail(1001)
        self.connections: set[AiohttpWsConnection] = set()
        # 开始关服了：之后才登记的连接（停止监听之前接进来、还在握手的）由 handler 自己马上 fail
        self.stopping = False
        self.ws_options = ws_response_options(config)
        self.drain_timeout = float(config["SHUTDOWN_DRAIN_TIMEOUT"])
        # 排空的截止时刻（事件循环时间），第一次排空时定下
        self._drain_deadline: float | None = None
        self._on_stop_requested = on_stop_requested
        self._runner: web.AppRunner | None = None
        self._site: web.SockSite | None = None
        self._stop_lock = asyncio.Lock()
        self._stopped = False

    def _on_lease_lost(self) -> None:
        """
        续约发现 worker id 已经不属于本进程：再发号就可能和接手的进程撞号，必须换一个新进程重新租。
        单进程没有管理进程，停服并把退出码置为非 0，交给 docker / systemd / k8s 重启
        """
        logger.error(
            _(
                "❌ [📡WorkerKeeper] Worker ID 租约已丢失，单进程模式没法只重启 worker，停止服务器"
            )
        )
        self.runtime.exit_code = LEASE_LOST_EXIT_CODE
        if self._on_stop_requested is not None:
            self._on_stop_requested()

    async def start(self, sock: socket.socket) -> None:
        """
        开服：`runtime.start()`（连后端、拿租约、跑启动 System），在已经 bind 好的 sock 上开始监听，再起
        后台任务（FutureCalls）。开服失败时 `StartupAborted` 原样抛出；开始监听这一步失败时先关服
        （释放租约、关后端）再抛出。
        """
        await self.runtime.start()
        try:
            self._runner = web.AppRunner(
                build_app(self),
                handle_signals=False,
                # 访问日志交给前面的反向代理（推荐 Caddy）。aiohttp 默认开着
                access_log=None,
                keepalive_timeout=KEEPALIVE_TIMEOUT,
                shutdown_timeout=SHUTDOWN_TIMEOUT,
            )
            await self._runner.setup()
            self._site = web.SockSite(self._runner, sock)
            await self._site.start()
        except BaseException:
            await self.stop()
            raise
        self.runtime.start_background()

    async def _drain(self) -> None:
        """
        关服排空：各连接停止接收，等它们都关了（发送循环发完回复后以 1001 断开，或者发现对端已经断开），
        最多 drain_timeout 秒。关服被打断后再调时，只等预算剩下的部分
        """
        loop = asyncio.get_running_loop()
        if self._drain_deadline is None:
            self._drain_deadline = loop.time() + self.drain_timeout
        conns = list(self.connections)
        if not conns:
            return
        for conn in conns:
            conn.stop_receiving()
        waiters = [asyncio.ensure_future(conn.wait_closed()) for conn in conns]
        try:
            _done, pending = await asyncio.wait(
                waiters, timeout=max(0.0, self._drain_deadline - loop.time())
            )
        finally:
            for waiter in waiters:
                waiter.cancel()
        if pending:
            logger.warning(
                _(
                    "⚠️ [📡Server] 关服排空：{count}个连接没在{timeout}秒内排空，直接断开，"
                    "在途的调用随之取消"
                ).format(count=len(pending), timeout=self.drain_timeout)
            )

    async def stop(self) -> None:
        """
        关服，可以重复调用、并发调用，只关一次；关到一半被打断后再调，接着关。顺序：

        1. 停止监听；
        2. 排空（`_drain`，最多 SHUTDOWN_DRAIN_TIMEOUT）：各连接不再收新消息，已经收到的处理完、回复发
           出去之后以 1001 断开。没读的消息不执行，客户端重连后重发不会重复执行；
        3. 没在预算内排空的：`fail(1001)`，再取消各连接的收发任务，在途的 RPC 随之取消。不取消的话 RPC
           照样提交，回复却发不出去，客户端重连重试时就重复执行了；
        4. `runtime.stop()`：等连接拆完（断线 System、删 Connection 行，最多 GRACEFUL_SHUTDOWN_TIMEOUT）、
           取消后台任务、释放租约、关后端；
        5. 收掉 aiohttp（这时 ws 的 handler 都已经返回）。`runtime.stop()` 出错也照样收。
        """
        async with self._stop_lock:
            if self._stopped:
                return
            self.stopping = True
            if self._site is not None:
                site, self._site = self._site, None
                await site.stop()
            await self._drain()
            for conn in list(self.connections):
                conn.fail(WSCloseCode.GOING_AWAY, "")
            await self.runtime.tasks.cancel_all(self.runtime.graceful_shutdown_timeout)
            try:
                await self.runtime.stop()
            finally:
                if self._runner is not None:
                    runner, self._runner = self._runner, None
                    await runner.cleanup()
            self._stopped = True


SERVER_KEY = web.AppKey("hetu_server", AiohttpServer)


async def _websocket_handler(request: web.Request) -> web.StreamResponse:
    """ws 路由 /hetu/{db_name}：做完 ws 握手，把连接交给 serve_connection"""
    server = request.app[SERVER_KEY]
    ws = HeTuWebSocketResponse(**server.ws_options)
    await ws.prepare(request)
    conn = AiohttpWsConnection(
        request,
        ws,
        next_conn_id(),
        client_ip(request.headers, request.remote or "", server.config),
    )
    server.connections.add(conn)
    if server.stopping:
        # 停止监听之前接进来、这时才握完手：赶上关服了，收不到关服时的 fail，自己断开
        conn.fail(WSCloseCode.GOING_AWAY, "")
    try:
        await serve_connection(server.runtime, conn, request.match_info["db_name"])
    except Exception:
        # serve_connection 不该抛异常。真抛了，aiohttp 走它的出错处理，不会再调 close()，客户端连
        # close 帧都收不到：先给一个服务端出错的码（客户端可以重连）
        close_connection(conn, connection.CLOSE_INTERNAL_ERROR)
        raise
    finally:
        server.connections.discard(conn)
        # 不管连接怎么结束的，都保证 TCP 在 ABORT_TIMEOUT 内断开：fail 过的不会再来一次；没 fail 过的
        # （对端关的、心跳超时的、关服排空时还没握完手的）这里补上 abort 兜底，对端不读数据时
        # transport.close() 会一直等着把缓冲发完
        conn.fail(WSCloseCode.GOING_AWAY if server.stopping else WSCloseCode.OK, "")
    return ws


async def _web_root(request: web.Request) -> web.Response:
    """HeTu 的默认欢迎页"""
    return web.Response(text=f"Powered by HeTu(v{hetu.__version__}) Database! ")


def build_app(server: AiohttpServer) -> web.Application:
    """
    建 aiohttp 的 app：ws 入口 /hetu/{db_name}（带尾斜杠的也注册，和 Sanic 一样能连上）、默认欢迎页 "/"。
    用户的 webext 路由这一步不支持（声明是 Sanic 形状的），见 `prepare`
    """
    app = web.Application(client_max_size=int(server.config["REQUEST_MAX_SIZE"]))
    app[SERVER_KEY] = server
    app.router.add_get("/hetu/{db_name}", _websocket_handler)
    app.router.add_get("/hetu/{db_name}/", _websocket_handler)
    app.router.add_get("/", _web_root)
    return app


def prepare(config: Mapping[str, Any]) -> HeTuConfig:
    """
    进程级的准备，同 Sanic 适配层 `worker_main` 里与框架无关的那几步：加载玩家的 app 文件、建簇、配
    日志、建消息流水线。aiohttp 的日志并到 HeTu.root 下面。返回 `HeTuConfig`

    Process-wide preparation shared with the Sanic adapter. Returns the HeTuConfig.
    """
    if not isinstance(config, HeTuConfig):
        config = HeTuConfig(config)
    prepare_worker(config)
    setup_logging(config)
    aiohttp_logger = logging.getLogger("aiohttp")
    aiohttp_logger.parent = logger
    if int(config["DEBUG"]) > 1:
        aiohttp_logger.setLevel(logging.DEBUG)
    setup_message_pipeline(config)
    if skipped := webext.registered():
        logger.warning(
            _(
                "⚠️ [📡Server] aiohttp 栈还不支持 @hetu.define_route / @hetu.on_server_setup，"
                "已跳过：{items}"
            ).format(items=", ".join(skipped))
        )
    return config


def bind_socket(listen: str, backlog: int = 100) -> socket.socket:
    """
    按 LISTEN（host:port）建好监听 socket，端口冲突等错误在这里就抛出，不会先跑一遍开服。和 Sanic 的
    bind_socket 一样：IPv6 地址用 AF_INET6（是否双栈按系统默认），主机名用 AF_INET；只是 Windows 上
    不设 SO_REUSEADDR，那里设了它别的进程就能抢同一个端口
    """
    host, port = listen.rsplit(":", 1)
    host = host.strip("[]")
    try:
        version = ipaddress.ip_address(host).version
    except ValueError:  # 主机名
        version = 4
    sock = socket.socket(socket.AF_INET6 if version == 6 else socket.AF_INET)
    try:
        if not IS_WINDOWS:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, int(port)))
        sock.listen(backlog)
    except BaseException:
        sock.close()
        raise
    return sock


def new_event_loop() -> asyncio.AbstractEventLoop:
    """aiohttp 栈用的事件循环：Linux / macOS 上用 uvloop，Windows 上用 selector 事件循环（threaded_accept 依赖它）"""
    if IS_WINDOWS:
        return asyncio.SelectorEventLoop()
    import uvloop

    return uvloop.new_event_loop()


def _ignore_shutdown_signals() -> None:
    for sig in SHUTDOWN_SIGNALS:
        signal.signal(sig, signal.SIG_IGN)


def _install_stop_signals(
    loop: asyncio.AbstractEventLoop, request_stop: Callable[[], None]
) -> Callable[[], None]:
    """
    装上关服信号（SIGINT / SIGTERM）的处理，返回卸载用的函数。

    非 Windows：关服信号只认第一个，同 Sanic 栈（设计稿 §5.2 末尾）。第一个一到就把两种信号都忽略掉，
    之后再来的不会打断关服；卸载之后也继续忽略，免得关完服、进程退出前的收尾（关事件循环、刷日志）
    被它直接杀掉。代价是第二次 Ctrl+C 不能强退。

    Windows：事件循环不支持 add_signal_handler，在 Python 的信号处理里转回事件循环，再定时叫醒事件循环，
    信号才能及时处理；第二次 Ctrl+C 强退。都同 Sanic 的 ctrlc_workaround_for_windows。
    """
    if not IS_WINDOWS:
        signalled = False

        def on_signal() -> None:
            nonlocal signalled
            signalled = True
            _ignore_shutdown_signals()
            request_stop()

        for sig in SHUTDOWN_SIGNALS:
            loop.add_signal_handler(sig, on_signal)

        def restore() -> None:
            # remove_signal_handler 会把信号恢复成默认处理
            for sig in SHUTDOWN_SIGNALS:
                loop.remove_signal_handler(sig)
            if signalled:
                _ignore_shutdown_signals()

        return restore

    pressed = False

    def on_ctrl_c(*_args: Any) -> None:
        nonlocal pressed
        if pressed:
            raise KeyboardInterrupt("Non-graceful Ctrl+C")
        pressed = True
        loop.call_soon_threadsafe(request_stop)

    previous = signal.signal(signal.SIGINT, on_ctrl_c)

    async def wake_periodically() -> None:
        while True:
            await asyncio.sleep(WINDOWS_WAKEUP_INTERVAL)

    waker = loop.create_task(wake_periodically())

    def restore_windows() -> None:
        waker.cancel()
        signal.signal(signal.SIGINT, previous)

    return restore_windows


async def _serve(config: HeTuConfig, sock: socket.socket) -> int:
    loop = asyncio.get_running_loop()
    # Windows 上 accept 挪到专用线程（见 threaded_accept.py），要在开始监听之前装
    if IS_WINDOWS and install_threaded_accept(loop):
        logger.info(
            _("🧵 进程[{pid}] 监听socket改由accept线程接连接").format(pid=os.getpid())
        )
    stop_requested = asyncio.Event()
    server = AiohttpServer(config, on_stop_requested=stop_requested.set)
    restore_signals = _install_stop_signals(loop, stop_requested.set)
    try:
        # 开服（连后端、拿租约、跑启动 System）可能要很久，期间收到关服信号就不等了：取消开服，已经拿到
        # 的租约随之放弃（见 WorkerRuntime.start），再把开到一半的关掉
        starting = asyncio.ensure_future(server.start(sock))
        signalled = asyncio.ensure_future(stop_requested.wait())
        await asyncio.wait((starting, signalled), return_when=asyncio.FIRST_COMPLETED)
        signalled.cancel()
        if not starting.done():
            logger.info(_("⌚ [📡Server] 开服途中收到关服信号，中止开服"))
            starting.cancel()
            await asyncio.wait((starting,))
            if not starting.cancelled() and starting.exception() is not None:
                starting.result()  # 被取消之前就已经失败了：照常抛出
            await server.stop()
            return server.runtime.exit_code
        starting.result()  # 开服失败：StartupAborted 等照常抛出
        logger.info(_("✅ [📡Server] 服务器已就绪，开始接受连接"))
        await stop_requested.wait()
        logger.info(_("⌚ [📡Server] 开始关服..."))
        await server.stop()
    finally:
        restore_signals()
    return server.runtime.exit_code


def run(config: HeTuConfig, sock: socket.socket) -> int:
    """
    单进程跑 aiohttp 栈：在已经 bind 好的 sock 上开服，阻塞到收到关服信号（或租约丢失）、关完服，
    返回退出码。开服失败时 `StartupAborted` 原样抛出。`DEBUG > 1` 时打开事件循环的 debug 模式。

    Serve on aiohttp in this process until a shutdown signal; returns the exit code.
    """
    debug = int(config["DEBUG"]) > 1
    with asyncio.Runner(debug=debug, loop_factory=new_event_loop) as runner:
        return runner.run(_serve(config, sock))
