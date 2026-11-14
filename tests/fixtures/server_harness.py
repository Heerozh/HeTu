"""
测试里起停 HeTu 服务、跑客户端 routine 的封装。

测试只经它和服务器打交道，不直接碰 Web 框架：换 aiohttp 时只在这里加一个实现，测试本身不动
（设计稿 docs/superpowers/specs/2026-11-14-aiohttp-migration-design.md §5.0）。现在内部用
sanic_testing。

两个实现都必须满足的语义，测试依赖它们：

1. 每次 `websocket()` / `get()` 都完整起停一次服务：启动钩子（连后端、跑启动 System、起后台
   任务）和关服钩子（拆连接、停后台任务、释放租约、关后端）都跑，每次一个新的事件循环。
2. routine 跑在服务器的同一个事件循环里，服务开始监听、后台任务起来之后才开始。所以 routine
   能和服务端的 monkeypatch 共享 asyncio 对象，也能直接读 `runtime` 上的状态。
3. routine 抛出的 `ConnectionClosedOK` 被吞掉；其他异常等服务停下之后原样抛给测试。
4. routine 返回后，先关掉它开的所有客户端连接，再停服务。
5. Linux / macOS 上跑 uvloop。

Test harness: starts and stops a HeTu server per call and runs a client routine against
it, so tests never touch the web framework directly.
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable, MutableMapping
from dataclasses import dataclass, field
from typing import Any, cast

import sanic_testing.testing
from nacl.public import PrivateKey
from sanic import Sanic
from websockets.exceptions import ConnectionClosedOK

from hetu.server import pipeline

logger = logging.getLogger("HeTu.root")

# routine 拿到的 connect()：新开一个已握手的客户端连接，send/recv 收发的是解码后的消息
Connect = Callable[[], Awaitable[Any]]
Routine = Callable[[Connect], Awaitable[Any]]

# routine 里抛出的这些不收集：取消、退出要照常往外传
_PASS_THROUGH = (asyncio.CancelledError, KeyboardInterrupt, SystemExit, GeneratorExit)


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

    async def connect(self) -> Any:
        from websockets.legacy.client import connect

        # 收发换成带编解码的版本，要往协议对象上挂属性，按 Any 处理
        ws: Any = await connect(self.url, **self.connect_kwargs)
        self.clients.append(ws)
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

        async def handshake():
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

        await handshake()

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


class ServerHarness:
    """
    一个 HeTu 服务的测试封装，语义见模块说明。

    `config` 是起服时用的配置，可以在调用 `websocket()` / `get()` 之前修改。`runtime` 是本次（或
    上一次）起服的 `WorkerRuntime`，每次起服新建一个。
    """

    def __init__(self, app: Sanic):
        self._app = app

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
