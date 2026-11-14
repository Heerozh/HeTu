"""
aiohttp 适配层的单元测试，不需要后端：`AiohttpWsConnection` 的语义（在一个最小的 aiohttp app 上测）、
租约丢失时停服。客户端地址解析见 test_forwarded.py；整套 ws 测试在两个栈上各跑一遍，见 test_websocket.py。

这个文件的异步用例跑在生产用的事件循环上（Linux / macOS 是 uvloop），见 conftest 的
pytest_asyncio_loop_factories。
"""

import asyncio
import base64
import contextlib
import os
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import pytest
from aiohttp import WebSocketError, web
from websockets.exceptions import ConnectionClosedError
from websockets.legacy.client import connect

from hetu.server.adapters import aiohttp_app
from hetu.server.adapters.aiohttp_app import (
    CLOSE_TIMEOUT,
    AiohttpServer,
    AiohttpWsConnection,
    ws_response_options,
)
from hetu.server.config import HeTuConfig
from hetu.server.conn import ConnectionClosed, next_conn_id

# ==== AiohttpWsConnection ====


Body = Callable[[AiohttpWsConnection], Awaitable[Any]]


@contextlib.asynccontextmanager
async def _ws_app(body: Body, **config: Any) -> AsyncIterator[str]:
    """
    最小的 aiohttp 服务：/ws 上做完 ws 握手，把 AiohttpWsConnection 交给 body，返回前照真实 handler 的
    做法补一次 fail。ws 参数按 config 用 ws_response_options 算。yield 出 ws 地址。body 里的异常（包括
    断言失败）收下来，服务停下之后再抛
    """
    options = ws_response_options(HeTuConfig(config))
    failures: list[BaseException] = []

    async def handler(request: web.Request) -> web.StreamResponse:
        ws = web.WebSocketResponse(**options)
        await ws.prepare(request)
        conn = AiohttpWsConnection(request, ws, next_conn_id(), request.remote or "")
        try:
            await body(conn)
        except asyncio.CancelledError:
            raise
        except BaseException as e:  # noqa: BLE001 留到测试里抛
            failures.append(e)
        finally:
            conn.fail(1000, "")
        return ws

    app = web.Application()
    app.router.add_get("/ws", handler)
    runner = web.AppRunner(app, access_log=None, shutdown_timeout=1)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = runner.addresses[0][1]
    try:
        yield f"ws://127.0.0.1:{port}/ws"
    finally:
        await runner.cleanup()
    if failures:
        raise failures[0]


async def _raw_upgrade(url: str) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    """只做 HTTP upgrade 的客户端：不回 ping，读不读数据由调用方决定"""
    host, port = url.removeprefix("ws://").split("/", 1)[0].split(":")
    reader, writer = await asyncio.open_connection(host, int(port))
    key = base64.b64encode(os.urandom(16)).decode()
    writer.write(
        (
            f"GET /ws HTTP/1.1\r\nHost: {host}:{port}\r\nUpgrade: websocket\r\n"
            f"Connection: Upgrade\r\nSec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n\r\n"
        ).encode()
    )
    response = await reader.readuntil(b"\r\n\r\n")
    assert b" 101 " in response.split(b"\r\n", 1)[0]
    return reader, writer


async def test_ws_response_options_follow_config():
    """ws 参数按配置算：不压缩；消息上限 +1（aiohttp 判超限用 >=）；心跳间隔；关闭握手的超时"""
    options = ws_response_options(
        HeTuConfig({"WEBSOCKET_MAX_SIZE": 100, "WEBSOCKET_PING_INTERVAL": 7})
    )
    assert options == {
        "compress": False,
        "max_msg_size": 101,
        "heartbeat": 7.0,
        "autoping": True,
        "timeout": CLOSE_TIMEOUT,
    }
    unlimited = ws_response_options(
        HeTuConfig({"WEBSOCKET_MAX_SIZE": None, "WEBSOCKET_PING_INTERVAL": 0})
    )
    assert (unlimited["max_msg_size"], unlimited["heartbeat"]) == (0, None)


async def test_deflate_is_not_negotiated():
    """客户端请求 permessage-deflate 也不协商：HeTu 自己有 zlib 层"""

    async def body(conn):
        with contextlib.suppress(ConnectionClosed):
            await conn.recv()

    async with _ws_app(body) as url, connect(url, compression="deflate") as ws:
        assert ws.response_headers.get("Sec-WebSocket-Extensions") is None


async def test_close_code_survives_handler_returning_right_away():
    """fail 之后 handler 马上返回：客户端收到的是 fail 的码，不能被 aiohttp 的 write_eof 盖成 1000"""

    async def body(conn):
        conn.fail(4002, "rejected")

    async with _ws_app(body) as url, connect(url) as ws:
        with pytest.raises(ConnectionClosedError) as exc_info:
            await ws.recv()
    rcvd = exc_info.value.rcvd
    assert rcvd is not None and (rcvd.code, rcvd.reason) == (4002, "rejected")


async def test_recv_send_and_fail():
    """
    recv 返回整条消息（二进制是 bytes、文本是 str），超时返回 None；fail 叫醒正在等的 recv；fail
    之后收发都抛 ConnectionClosed，再 fail 什么也不做（第一次的码生效）
    """
    seen: list[Any] = []

    async def body(conn):
        seen.append(await conn.recv())
        seen.append(await conn.recv())
        seen.append(await conn.recv(timeout=0.05))
        await conn.send(b"pong")
        await conn.send("text")
        waiting = asyncio.ensure_future(conn.recv())
        await asyncio.sleep(0.05)
        conn.fail(4001, "kicked")
        conn.fail(4002, "rejected")
        with pytest.raises(ConnectionClosed):
            await asyncio.wait_for(waiting, 1)
        for op in (conn.recv(), conn.send(b"x")):
            with pytest.raises(ConnectionClosed):
                await op
        seen.append(conn.protocol_error())

    async with _ws_app(body) as url, connect(url) as ws:
        await ws.send(b"\x00binary")
        await ws.send("文本")
        assert await ws.recv() == b"pong"
        assert await ws.recv() == "text"
        with pytest.raises(ConnectionClosedError) as exc_info:
            await ws.recv()
    assert seen == [b"\x00binary", "文本", None, None]
    rcvd = exc_info.value.rcvd
    assert rcvd is not None and rcvd.code == 4001


async def test_protocol_error_is_recorded():
    """消息超长是协议错误：recv 抛 ConnectionClosed，protocol_error() 给出 WebSocketError，客户端收到 1009"""
    errors: list[BaseException | None] = []

    async def body(conn):
        with pytest.raises(ConnectionClosed):
            await conn.recv()
        errors.append(conn.protocol_error())

    async with _ws_app(body, WEBSOCKET_MAX_SIZE=10) as url, connect(url) as ws:
        await ws.send(b"x" * 100)
        with pytest.raises(ConnectionClosedError) as exc_info:
            await ws.recv()
    assert isinstance(errors[0], WebSocketError)
    rcvd = exc_info.value.rcvd
    assert rcvd is not None and rcvd.code == 1009


@pytest.mark.parametrize("how", ["close", "abort"])
async def test_peer_disconnect_is_not_protocol_error(how):
    """对端正常关闭、直接断开 TCP：recv 抛 ConnectionClosed，不算协议错误"""
    errors: list[BaseException | None] = []

    async def body(conn):
        with pytest.raises(ConnectionClosed):
            await conn.recv()
        errors.append(conn.protocol_error())

    async with _ws_app(body) as url:
        ws = await connect(url)
        if how == "close":
            await ws.close()
        else:
            ws.transport.abort()
        for _ in range(100):
            if errors:
                break
            await asyncio.sleep(0.02)
    assert errors == [None]


async def test_fail_disconnects_peer_that_stops_reading():
    """
    对端不读数据，发送卡在 drain 上：fail 之后 CLOSE_TIMEOUT 秒左右 abort 兜底，卡住的发送醒来，
    TCP 断开。aiohttp 自己的 close() 等 drain 不受超时约束，没有兜底会一直卡着
    """
    result: dict[str, Any] = {}

    async def flood(conn):
        chunk = b"x" * 65536
        try:
            while True:
                await conn.send(chunk)
        except ConnectionClosed:
            pass

    async def body(conn):
        sender = asyncio.ensure_future(flood(conn))
        await asyncio.sleep(0.5)
        result["stuck"] = not sender.done()  # 缓冲塞满之后卡在 drain 上
        start = time.monotonic()
        conn.fail(4002, "rejected")
        await asyncio.wait({sender}, timeout=CLOSE_TIMEOUT + 3)
        result["woke_after"] = time.monotonic() - start if sender.done() else None
        for _ in range(100):
            if conn.request.transport is None:
                break
            await asyncio.sleep(0.05)
        result["disconnected"] = conn.request.transport is None

    async with _ws_app(body) as url:
        _reader, writer = await _raw_upgrade(url)
        writer.transport.pause_reading()  # 再也不读
        for _ in range(200):
            if "disconnected" in result:
                break
            await asyncio.sleep(0.05)
        writer.close()
    assert result["stuck"] is True
    assert result["woke_after"] is not None
    assert result["woke_after"] < CLOSE_TIMEOUT + 1
    assert result["disconnected"] is True


async def test_heartbeat_disconnects_peer_that_does_not_answer_ping():
    """不回 pong 的对端在心跳间隔加一半之内被断开：recv 抛 ConnectionClosed，不算协议错误"""
    result: dict[str, Any] = {}

    async def body(conn):
        start = time.monotonic()
        with pytest.raises(ConnectionClosed):
            await conn.recv()
        result["after"] = time.monotonic() - start
        result["error"] = conn.protocol_error()

    async with _ws_app(body, WEBSOCKET_PING_INTERVAL=0.2) as url:
        _reader, writer = await _raw_upgrade(url)
        writer.transport.pause_reading()  # 收不到 ping，也就不回 pong
        for _ in range(100):
            if "after" in result:
                break
            await asyncio.sleep(0.05)
        writer.close()
    assert result["after"] < 2
    assert result["error"] is None


# ==== AiohttpServer ====


def test_lease_lost_requests_stop_with_failure_code():
    """
    雪花号租约丢了：单进程没法只重启 worker，要求停服（hetu start 随后关服），退出码非 0，交给
    docker / systemd / k8s 重启
    """
    from hetu.server.runtime import LEASE_LOST_EXIT_CODE

    requested: list[bool] = []
    server = AiohttpServer(
        HeTuConfig({"NAMESPACE": "pytest", "INSTANCES": [], "BACKENDS": {}}),
        on_stop_requested=lambda: requested.append(True),
    )
    server.runtime.on_lease_lost()
    assert requested == [True]
    assert server.runtime.exit_code == LEASE_LOST_EXIT_CODE


def test_new_event_loop_is_uvloop_off_windows():
    """aiohttp 栈在 Linux / macOS 上跑 uvloop，Windows 上跑 selector 事件循环"""
    loop = aiohttp_app.new_event_loop()
    try:
        if os.name == "nt":
            assert isinstance(loop, asyncio.SelectorEventLoop)
        else:
            import uvloop

            assert isinstance(loop, uvloop.Loop)
    finally:
        loop.close()
