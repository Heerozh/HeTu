"""
测试里起停 HeTu 服务、跑客户端 routine 的封装。

测试只经它和服务器打交道，不直接碰 Web 框架（设计稿
docs/superpowers/specs/2026-11-14-aiohttp-migration-design.md §5.0）。两个实现：`SanicHarness` 内部用
sanic_testing；`AiohttpHarness` 每次调用都新建事件循环和 `AiohttpServer`，走的就是 `hetu start` 单进程的
开服、关服代码。用 `make_harness` 按栈建。

两个实现都必须满足的语义，测试依赖它们：

1. 每次 `websocket()` / `get()` 都完整起停一次服务：启动钩子（连后端、跑启动 System、起后台
   任务）和关服钩子（拆连接、停后台任务、释放租约、关后端）都跑，每次一个新的事件循环。
2. routine 跑在服务器的同一个事件循环里，服务开始监听、后台任务起来之后才开始。所以 routine
   能和服务端的 monkeypatch 共享 asyncio 对象，也能直接读 `runtime` 上的状态。
3. routine 抛出的 `ConnectionClosedOK` 被吞掉；其他异常等服务停下之后原样抛给测试。routine 跑超过
   `routine_timeout` 秒就被取消，测试以 TimeoutError 失败（见 `routine_timeout_for`）。
4. routine 返回后，先关掉它开的所有客户端连接，再停服务。
5. Linux / macOS 上跑 uvloop。

Test harness: starts and stops a HeTu server per call and runs a client routine against
it, so tests never touch the web framework directly.
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable, Mapping, MutableMapping
from dataclasses import dataclass, field
from typing import Any, cast

import sanic_testing.testing
from nacl.public import PrivateKey
from sanic import Sanic
from websockets.exceptions import ConnectionClosedOK

from hetu.server import pipeline

logger = logging.getLogger("HeTu.root")

# routine 拿到的 connect()：新开一个已握手的客户端连接，send/recv 收发的是解码后的消息。
# connect(handshake=False) 只建 ws 连接、不做 HeTu 的握手，收发原始帧
Connect = Callable[..., Awaitable[Any]]
Routine = Callable[[Connect], Awaitable[Any]]

# routine 里抛出的这些不收集：取消、退出要照常往外传
_PASS_THROUGH = (asyncio.CancelledError, KeyboardInterrupt, SystemExit, GeneratorExit)
# 用例没标 timeout 时 routine 最多跑多久（秒）
DEFAULT_ROUTINE_TIMEOUT = 120.0


def routine_timeout_for(node: Any) -> float:
    """
    routine 的超时：用例 `@pytest.mark.timeout(N)` 的八成（留余量给起停服），没标就用
    DEFAULT_ROUTINE_TIMEOUT。harness 要在服务器的事件循环里自己限时：pytest-timeout 靠 SIGALRM，
    打断不了 uvloop（信号处理函数里抛的异常被 uvloop 的回调吞掉，事件循环照转），routine 卡住时
    整个测试进程就挂死了
    """
    marker = node.get_closest_marker("timeout")
    if marker is None or not marker.args:
        return DEFAULT_ROUTINE_TIMEOUT
    return max(1.0, float(marker.args[0]) * 0.8)


@dataclass
class WsResult:
    """一次 `websocket()` 调用里，所有客户端连接发出 / 收到的消息（解码后），按发生顺序"""

    client_sent: list[Any] = field(default_factory=list)
    client_received: list[Any] = field(default_factory=list)


@dataclass
class HttpResult:
    status: int
    text: str


class _ClientSession:
    """一次 `websocket()` 调用里 routine 开的客户端连接：握手、编解码、记录收发，结束时统一关掉"""

    def __init__(self, url: str, connect_kwargs: dict[str, Any]):
        self.url = url
        self.connect_kwargs = connect_kwargs
        self.result = WsResult()
        self.clients: list[Any] = []

    async def connect(self, *, handshake: bool = True) -> Any:
        from websockets.legacy.client import connect

        # 收发换成带编解码的版本，要往协议对象上挂属性，按 Any 处理
        ws: Any = await connect(self.url, **self.connect_kwargs)
        self.clients.append(ws)
        if not handshake:
            return ws
        result = self.result

        do_send = ws.send
        do_recv = ws.recv

        client_pipe = pipeline.MessagePipeline()
        client_pipe.add_layer(pipeline.JSONBinaryLayer())
        client_pipe.add_layer(pipeline.ZlibLayer())
        crypto_layer = pipeline.CryptoLayer()
        client_pipe.add_layer(crypto_layer)
        pipe_ctx = None
        logger.debug(f"客户端pipe {id(crypto_layer)}")

        async def do_handshake():
            nonlocal pipe_ctx
            # 生成密钥对
            private_key = PrivateKey.generate()
            public_key = private_key.public_key
            handshake_msg = [b""] * client_pipe.num_handshake_layers
            handshake_msg[-1] = public_key.encode()
            # 握手
            await do_send(client_pipe.encode(None, handshake_msg))
            data = cast(bytes, await do_recv())
            message = client_pipe.decode(None, data)
            assert type(message) is list
            ctx, _msg = client_pipe.handshake(message)
            ctx[-1] = crypto_layer.client_handshake(private_key.encode(), message[-1])
            pipe_ctx = ctx

        async def send(data, *, fragment_size=None):
            logger.debug(
                f"[{id(crypto_layer)}] > Sent: {data} [{len(repr(data))} bytes]"
            )
            result.client_sent.append(data)
            encoded = client_pipe.encode(pipe_ctx, data)
            if fragment_size is None:
                await do_send(encoded)
            else:

                async def fragments():
                    for offset in range(0, len(encoded), fragment_size):
                        yield encoded[offset : offset + fragment_size]
                        await asyncio.sleep(0)

                await do_send(fragments())

        async def recv():
            data = cast(bytes, await do_recv())
            message = client_pipe.decode(pipe_ctx, data)
            logger.debug(
                f"[{id(crypto_layer)}] < Received: {message} "
                f"[{len(repr(message))} bytes]"
            )
            result.client_received.append(message)
            return message

        def clear_recv():
            result.client_received.clear()

        await do_handshake()

        ws.send = send
        ws.recv = recv
        ws.clear_recv = clear_recv

        return ws

    async def close(self) -> None:
        for ws in self.clients:
            await ws.close()


class _SanicTestingResponse:
    """交给 sanic_testing 的返回值：它跑完 routine 后会调 `.ws.close()` 关客户端，再停服务"""

    def __init__(self, session: _ClientSession):
        self.ws = session


class SanicHarness:
    """
    Sanic 栈上一个 HeTu 服务的测试封装，语义见模块说明。

    `config` 是起服时用的配置，可以在调用 `websocket()` / `get()` 之前修改。`runtime` 是本次（或
    上一次）起服的 `WorkerRuntime`，每次起服新建一个。
    """

    def __init__(self, app: Sanic, routine_timeout: float = DEFAULT_ROUTINE_TIMEOUT):
        self._app = app
        self.routine_timeout = routine_timeout

    @property
    def config(self) -> MutableMapping[str, Any]:
        return self._app.config

    @property
    def runtime(self) -> Any:
        return self._app.ctx.runtime

    def _test_client(self) -> Any:
        # sanic_testing 第一次随机选端口后会记下来，下次照用，刚停的服务还占着它就 bind 失败。
        # 每次清掉，让它重新随机选
        client = self._app.test_client
        client.port = None
        return client

    def websocket(self, path: str, mimic: Routine, **connect_kwargs: Any) -> WsResult:
        """
        起服，用 `mimic(connect)` 跑客户端逻辑，再停服。`connect_kwargs` 原样传给客户端的
        `connect()`，比如 `extra_headers`。
        """
        sessions: list[_ClientSession] = []
        errors: list[BaseException] = []

        async def websocket_proxy(url: str, *, mimic: Routine, **kwargs: Any):
            session = _ClientSession(url, kwargs)
            sessions.append(session)
            try:
                async with asyncio.timeout(self.routine_timeout):
                    await mimic(session.connect)
            except ConnectionClosedOK:
                pass
            except _PASS_THROUGH:
                raise
            except BaseException as e:  # noqa: BLE001 留到服务停下之后原样抛给测试
                errors.append(e)
            return _SanicTestingResponse(session)

        # sanic_testing 每次请求都按这个模块级名字找 ws 客户端，换成自己的，用完换回去
        original = sanic_testing.testing.websocket_proxy
        sanic_testing.testing.websocket_proxy = websocket_proxy
        try:
            self._test_client().websocket(path, mimic=mimic, **connect_kwargs)
        finally:
            sanic_testing.testing.websocket_proxy = original
        if errors:
            raise errors[0]
        assert sessions, "服务没跑到 routine"
        return sessions[0].result

    def get(self, path: str) -> HttpResult:
        """起服，发一个 GET 请求，再停服"""
        _request, response = self._test_client().get(path)
        assert response is not None
        return HttpResult(status=response.status, text=response.text)

    def stop(self) -> None:
        self._app.stop()


class AiohttpHarness:
    """
    aiohttp 栈上一个 HeTu 服务的测试封装，语义见模块说明。

    每次调用都新建事件循环（Linux / macOS 上是 uvloop）和 `AiohttpServer`，runtime 和
    `web.Application` 跟着新建：一个 Application 只能绑定一个事件循环。
    """

    def __init__(
        self,
        config: Mapping[str, Any],
        routine_timeout: float = DEFAULT_ROUTINE_TIMEOUT,
    ):
        from hetu.server.adapters import aiohttp_app

        self._config = aiohttp_app.prepare(config)
        self._runtime: Any = None
        self.routine_timeout = routine_timeout

    @property
    def config(self) -> MutableMapping[str, Any]:
        return self._config

    @property
    def runtime(self) -> Any:
        return self._runtime

    def _run(self, body: Callable[[str], Awaitable[Any]]) -> Any:
        """新建事件循环，开服，跑 body("127.0.0.1:端口")，再关服。返回 body 的返回值"""
        from hetu.server.adapters import aiohttp_app

        async def main() -> Any:
            server = aiohttp_app.AiohttpServer(self._config)
            self._runtime = server.runtime
            sock = aiohttp_app.bind_socket("127.0.0.1:0")
            try:
                await server.start(sock)
                try:
                    return await body(f"127.0.0.1:{sock.getsockname()[1]}")
                finally:
                    await server.stop()
            finally:
                sock.close()

        with asyncio.Runner(loop_factory=aiohttp_app.new_event_loop) as runner:
            return runner.run(main())

    def websocket(self, path: str, mimic: Routine, **connect_kwargs: Any) -> WsResult:
        """
        起服，用 `mimic(connect)` 跑客户端逻辑，再停服。`connect_kwargs` 原样传给客户端的
        `connect()`，比如 `extra_headers`。
        """
        errors: list[BaseException] = []

        async def body(host: str) -> WsResult:
            session = _ClientSession(f"ws://{host}{path}", connect_kwargs)
            try:
                async with asyncio.timeout(self.routine_timeout):
                    await mimic(session.connect)
            except ConnectionClosedOK:
                pass
            except _PASS_THROUGH:
                raise
            except BaseException as e:  # noqa: BLE001 留到服务停下之后原样抛给测试
                errors.append(e)
            finally:
                await session.close()
            return session.result

        result = self._run(body)
        if errors:
            raise errors[0]
        return result

    def get(self, path: str) -> HttpResult:
        """起服，发一个 GET 请求，再停服"""
        import aiohttp

        async def body(host: str) -> HttpResult:
            async with (
                aiohttp.ClientSession() as http,
                http.get(f"http://{host}{path}") as response,
            ):
                return HttpResult(status=response.status, text=await response.text())

        return self._run(body)

    def stop(self) -> None:
        """每次调用都已经停服了，这里什么也不做"""


def make_harness(
    http_server: str,
    app_name: str,
    config: Mapping[str, Any],
    routine_timeout: float = DEFAULT_ROUTINE_TIMEOUT,
) -> SanicHarness | AiohttpHarness:
    """按 Web 服务器栈（sanic / aiohttp）建 harness。Sanic 栈用 app_name 建 Sanic app"""
    if http_server == "aiohttp":
        return AiohttpHarness(config, routine_timeout)
    from hetu.server import worker_main

    return SanicHarness(worker_main(app_name, config), routine_timeout)

