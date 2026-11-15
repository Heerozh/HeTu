"""
aiohttp 适配层的单元测试，不需要后端：`AiohttpWsConnection` 的语义、handler 的收尾、`_serve` 的开关服编排、
租约丢失时停服。客户端地址解析见 test_forwarded.py；整套 ws 测试在两个栈上各跑一遍，见 test_websocket.py；
真起 `hetu start` 的见 test_server_signals.py。

这个文件的异步用例跑在生产用的事件循环上（Linux / macOS 是 uvloop），见 conftest 的
pytest_asyncio_loop_factories。pytest-timeout 的默认做法（SIGALRM）打断不了 uvloop，所以这里用 thread
方式兜底：卡住时打出各线程的栈、结束测试进程。
"""

import asyncio
import base64
import contextlib
import os
import struct
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from types import SimpleNamespace
from typing import Any, cast

import pytest
from aiohttp import WebSocketError, web
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK
from websockets.legacy.client import connect

from hetu.server.adapters import aiohttp_app
from hetu.server.adapters.aiohttp_app import (
    CLOSE_TIMEOUT,
    AiohttpServer,
    AiohttpWsConnection,
    ws_response_options,
)
from hetu.server.config import HeTuConfig
from hetu.server.conn import ConnectionClosed, Draining

pytestmark = pytest.mark.timeout(60, method="thread")

# 测试里把 abort 兜底调短（秒），免得每个用例都等满 ABORT_TIMEOUT
SHORT_ABORT = 1.0

Body = Callable[[AiohttpWsConnection], Awaitable[Any]]


@contextlib.asynccontextmanager
async def _hetu_app(
    monkeypatch, body: Body, **config: Any
) -> AsyncIterator[tuple[str, AiohttpServer]]:
    """
    用真实的 build_app / ws handler 起服务（runtime 不开服，不需要后端），只把 serve_connection 换成
    body：handler 收尾时补的 fail、关服标记这些逻辑也在测试范围里。ws 参数按 config 算。yield 出
    (ws 地址, server)。body 里的异常（包括断言失败）收下来，服务停下之后再抛
    """
    failures: list[BaseException] = []

    async def serve(runtime, conn, instance):
        try:
            await body(conn)
        except asyncio.CancelledError:
            raise
        except BaseException as e:  # noqa: BLE001 留到测试里抛
            failures.append(e)

    monkeypatch.setattr(aiohttp_app, "serve_connection", serve)
    server = AiohttpServer(HeTuConfig(config))
    runner = web.AppRunner(
        aiohttp_app.build_app(server), access_log=None, shutdown_timeout=1
    )
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = runner.addresses[0][1]
    try:
        yield f"ws://127.0.0.1:{port}/hetu/test", server
    finally:
        await runner.cleanup()
    if failures:
        raise failures[0]


async def _raw_upgrade(url: str) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    """只做 HTTP upgrade 的客户端：不回 ping，读不读数据由调用方决定"""
    host_port, path = url.removeprefix("ws://").split("/", 1)
    host, port = host_port.split(":")
    reader, writer = await asyncio.open_connection(host, int(port))
    key = base64.b64encode(os.urandom(16)).decode()
    writer.write(
        (
            f"GET /{path} HTTP/1.1\r\nHost: {host_port}\r\nUpgrade: websocket\r\n"
            f"Connection: Upgrade\r\nSec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n\r\n"
        ).encode()
    )
    response = await reader.readuntil(b"\r\n\r\n")
    assert b" 101 " in response.split(b"\r\n", 1)[0]
    return reader, writer


def _client_frame(opcode: int, payload: bytes) -> bytes:
    """客户端发的帧（要加掩码）"""
    mask = os.urandom(4)
    size = len(payload)
    if size < 126:
        header = struct.pack("!BB", 0x80 | opcode, 0x80 | size)
    elif size < 65536:
        header = struct.pack("!BBH", 0x80 | opcode, 0x80 | 126, size)
    else:
        header = struct.pack("!BBQ", 0x80 | opcode, 0x80 | 127, size)
    return header + mask + bytes(b ^ mask[i % 4] for i, b in enumerate(payload))


async def _flood(conn: AiohttpWsConnection, state: dict) -> None:
    """一直发，直到连接断开。记下 fail 之后还有几次 send 是正常返回的（应该一次都没有）"""
    chunk = b"x" * 65536
    state["returned_after_fail"] = 0
    try:
        while True:
            await conn.send(chunk)
            if "failed_at" in state:
                state["returned_after_fail"] += 1
    except ConnectionClosed:
        state["flood_ended"] = time.monotonic()


async def _wait_for(predicate: Callable[[], bool], timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.02)
    return predicate()


# ==== ws 参数 ====


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
    # None 不限；0 和 Sanic 一样只收空消息；心跳没配就不发
    unlimited = ws_response_options(
        HeTuConfig({"WEBSOCKET_MAX_SIZE": None, "WEBSOCKET_PING_INTERVAL": 0})
    )
    assert (unlimited["max_msg_size"], unlimited["heartbeat"]) == (0, None)
    zero = ws_response_options(HeTuConfig({"WEBSOCKET_MAX_SIZE": 0}))
    assert zero["max_msg_size"] == 1


async def test_deflate_is_not_negotiated(monkeypatch):
    """客户端请求 permessage-deflate 也不协商：HeTu 自己有 zlib 层"""

    async def body(conn):
        with contextlib.suppress(ConnectionClosed):
            await conn.recv()

    async with (
        _hetu_app(monkeypatch, body) as (url, _server),
        connect(url, compression="deflate") as ws,
    ):
        assert ws.response_headers.get("Sec-WebSocket-Extensions") is None


async def test_message_size_limit_boundary(monkeypatch):
    """
    刚好等于 WEBSOCKET_MAX_SIZE 的消息能收（同 Sanic，超限的判断是 >）；超过一个字节就以 1009 关闭，
    记成协议错误
    """
    seen: list[Any] = []

    async def body(conn):
        seen.append(await conn.recv())
        with pytest.raises(ConnectionClosed):
            await conn.recv()
        seen.append(conn.protocol_error())

    async with (
        _hetu_app(monkeypatch, body, WEBSOCKET_MAX_SIZE=100) as (url, _server),
        connect(url) as ws,
    ):
        await ws.send(b"x" * 100)
        await ws.send(b"x" * 101)
        with pytest.raises(ConnectionClosedError) as exc_info:
            await ws.recv()
    assert seen[0] == b"x" * 100
    assert isinstance(seen[1], WebSocketError)
    rcvd = exc_info.value.rcvd
    assert rcvd is not None and rcvd.code == 1009


# ==== 收发、fail ====


async def test_close_code_survives_handler_returning_right_away(monkeypatch):
    """fail 之后 handler 马上返回：客户端收到的是 fail 的码，不能被 aiohttp 的 write_eof 盖成 1000"""

    async def body(conn):
        conn.fail(4002, "rejected")

    async with (
        _hetu_app(monkeypatch, body) as (url, _server),
        connect(url) as ws,
    ):
        with pytest.raises(ConnectionClosedError) as exc_info:
            await ws.recv()
    rcvd = exc_info.value.rcvd
    assert rcvd is not None and (rcvd.code, rcvd.reason) == (4002, "rejected")


async def test_recv_send_and_fail(monkeypatch):
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
        with pytest.raises(ConnectionClosed):
            await conn.recv()
        with pytest.raises(ConnectionClosed):
            await conn.send(b"x")
        seen.append(conn.protocol_error())

    async with (
        _hetu_app(monkeypatch, body) as (url, _server),
        connect(url) as ws,
    ):
        await ws.send(b"\x00binary")
        await ws.send("文本")
        assert await ws.recv() == b"pong"
        assert await ws.recv() == "text"
        with pytest.raises(ConnectionClosedError) as exc_info:
            await ws.recv()
    assert seen == [b"\x00binary", "文本", None, None]
    rcvd = exc_info.value.rcvd
    assert rcvd is not None and rcvd.code == 4001


@pytest.mark.parametrize("how", ["close", "abort"])
async def test_peer_disconnect_is_not_protocol_error(monkeypatch, how):
    """对端正常关闭、直接断开 TCP：recv 抛 ConnectionClosed，不算协议错误"""
    errors: list[BaseException | None] = []

    async def body(conn):
        with pytest.raises(ConnectionClosed):
            await conn.recv()
        errors.append(conn.protocol_error())

    async with _hetu_app(monkeypatch, body) as (url, _server):
        ws = await connect(url)
        if how == "close":
            await ws.close()
        else:
            ws.transport.abort()
        assert await _wait_for(lambda: bool(errors), 2)
    assert errors == [None]


# ==== 有限时间内断开 ====


async def test_fail_disconnects_peer_that_stops_reading(monkeypatch):
    """
    对端不读数据，发送卡在 drain 上：fail 之后过 ABORT_TIMEOUT 秒 abort 兜底，卡住的发送醒来、抛
    ConnectionClosed（不能正常返回，那一帧多半没发出去），TCP 断开
    """
    monkeypatch.setattr(aiohttp_app, "ABORT_TIMEOUT", SHORT_ABORT)
    state: dict[str, Any] = {}

    async def body(conn):
        sender = asyncio.ensure_future(_flood(conn, state))
        await asyncio.sleep(0.5)
        state["stuck"] = not sender.done()  # 缓冲塞满之后卡在 drain 上
        state["failed_at"] = time.monotonic()
        conn.fail(4002, "rejected")
        await asyncio.wait({sender}, timeout=SHORT_ABORT + 3)
        state["disconnected"] = await _wait_for(
            lambda: conn.request.transport is None, 2
        )

    async with _hetu_app(monkeypatch, body) as (url, _server):
        _reader, writer = await _raw_upgrade(url)
        writer.transport.pause_reading()  # 再也不读
        await _wait_for(lambda: "disconnected" in state, SHORT_ABORT + 8)
        writer.close()
    assert state["stuck"] is True
    assert "flood_ended" in state
    assert state["flood_ended"] - state["failed_at"] < SHORT_ABORT + 1
    assert state["returned_after_fail"] == 0
    assert state["disconnected"] is True


async def test_protocol_error_from_peer_that_stops_reading(monkeypatch):
    """
    对端先停止读数据，再发一个超长的消息：aiohttp 自己发起的关闭（close(1009)）卡在 drain 上，心跳也
    停了。close() 一进来就挂上的 abort 兜底照样在 ABORT_TIMEOUT 内断开，recv 醒来
    """
    monkeypatch.setattr(aiohttp_app, "ABORT_TIMEOUT", SHORT_ABORT)
    state: dict[str, Any] = {}

    async def body(conn):
        sender = asyncio.ensure_future(_flood(conn, state))
        await asyncio.sleep(0.5)
        state["stuck"] = not sender.done()
        state["sent_bad_frame"] = asyncio.get_running_loop().create_future()
        await state["sent_bad_frame"]
        start = time.monotonic()
        with pytest.raises(ConnectionClosed):
            await conn.recv()
        state["recv_woke_after"] = time.monotonic() - start
        sender.cancel()

    async with _hetu_app(monkeypatch, body, WEBSOCKET_MAX_SIZE=100) as (url, _server):
        _reader, writer = await _raw_upgrade(url)
        writer.transport.pause_reading()
        await _wait_for(lambda: "sent_bad_frame" in state, 5)
        writer.write(_client_frame(0x2, b"x" * 1000))
        state["sent_bad_frame"].set_result(None)
        await _wait_for(lambda: "recv_woke_after" in state, SHORT_ABORT + 8)
        writer.close()
    assert state["stuck"] is True
    assert state["recv_woke_after"] < SHORT_ABORT + 1


async def test_handler_end_bounds_stuck_transport(monkeypatch):
    """
    handler 返回时写缓冲还堵着（对端不读数据），没人 fail 过：收尾时挂上的 abort 兜底照样在 ABORT_TIMEOUT
    内断开 TCP，不会一直等着把缓冲发完
    """
    monkeypatch.setattr(aiohttp_app, "ABORT_TIMEOUT", SHORT_ABORT)
    state: dict[str, Any] = {}

    async def body(conn):
        sender = asyncio.ensure_future(_flood(conn, state))
        await asyncio.sleep(0.5)
        state["stuck"] = not sender.done()
        state["conn"] = conn
        state["returned_at"] = time.monotonic()
        sender.cancel()

    async with _hetu_app(monkeypatch, body) as (url, _server):
        _reader, writer = await _raw_upgrade(url)
        writer.transport.pause_reading()
        await _wait_for(lambda: "conn" in state, 5)
        disconnected = await _wait_for(
            lambda: state["conn"].request.transport is None, SHORT_ABORT + 3
        )
        took = time.monotonic() - state["returned_at"]
        writer.close()
    assert state["stuck"] is True
    assert disconnected is True
    assert took < SHORT_ABORT + 1


async def test_heartbeat_disconnects_peer_that_does_not_answer_ping(monkeypatch):
    """不回 pong 的对端在心跳间隔加一半之内被断开：recv 抛 ConnectionClosed，不算协议错误"""
    result: dict[str, Any] = {}

    async def body(conn):
        start = time.monotonic()
        with pytest.raises(ConnectionClosed):
            await conn.recv()
        result["after"] = time.monotonic() - start
        result["error"] = conn.protocol_error()

    async with _hetu_app(monkeypatch, body, WEBSOCKET_PING_INTERVAL=0.2) as (
        url,
        _server,
    ):
        _reader, writer = await _raw_upgrade(url)
        writer.transport.pause_reading()  # 收不到 ping，也就不回 pong
        await _wait_for(lambda: "after" in result, 5)
        writer.close()
    assert result["after"] < 2
    assert result["error"] is None


async def test_close_code_when_serve_connection_raises(monkeypatch):
    """serve_connection 抛了异常（不该发生）：客户端照样收到 close 帧，码是服务端出错的 1011"""

    async def broken(runtime, conn, instance):
        raise RuntimeError("serve_connection blew up")

    async with _hetu_app(monkeypatch, lambda conn: asyncio.sleep(0)) as (url, _server):
        monkeypatch.setattr(aiohttp_app, "serve_connection", broken)
        async with connect(url) as ws:
            with pytest.raises(ConnectionClosedError) as exc_info:
                await asyncio.wait_for(ws.recv(), 3)
    rcvd = exc_info.value.rcvd
    assert rcvd is not None and rcvd.code == 1011


# ==== 关服排空（第 4b 步）====


async def test_stop_receiving_wakes_pending_recv(monkeypatch):
    """关服排空：正在等的 recv 马上醒来抛 Draining，之后的 recv 也抛；发送照常"""
    done: list[str] = []

    async def body(conn):
        waiting = asyncio.ensure_future(conn.recv())
        await asyncio.sleep(0.05)
        conn.stop_receiving()
        with pytest.raises(Draining):
            await asyncio.wait_for(waiting, 1)
        assert waiting.cancelling() == 0  # 叫醒用的取消撤销掉了
        with pytest.raises(Draining):
            await conn.recv()
        await conn.send(b"still sending")
        done.append("ok")

    async with (
        _hetu_app(monkeypatch, body) as (url, _server),
        connect(url) as ws,
    ):
        assert await asyncio.wait_for(ws.recv(), 3) == b"still sending"
    assert done == ["ok"]


async def test_stop_receiving_leaves_buffered_messages_unread(monkeypatch):
    """开始排空时已经到了、还没读的消息不交出去：没读的消息就不会执行，客户端重连后重发不会重复执行"""
    seen: list[Any] = []

    async def body(conn):
        seen.append(await conn.recv())
        await asyncio.sleep(0.2)  # 第二条这时已经到了，在 aiohttp 的读缓冲里
        conn.stop_receiving()
        with pytest.raises(Draining):
            await conn.recv()

    async with (
        _hetu_app(monkeypatch, body) as (url, _server),
        connect(url) as ws,
    ):
        await ws.send(b"first")
        await ws.send(b"second")
        with pytest.raises(ConnectionClosedOK):
            await asyncio.wait_for(ws.recv(), 3)
    assert seen == [b"first"]


async def test_stop_receiving_does_not_swallow_a_real_cancel(monkeypatch):
    """叫醒 recv 用的取消和真的取消碰在一起时，照常抛 CancelledError，不变成 Draining"""
    result: dict[str, Any] = {}

    async def body(conn):
        waiting = asyncio.ensure_future(conn.recv())
        await asyncio.sleep(0.05)
        conn.stop_receiving()
        waiting.cancel()
        await asyncio.wait({waiting}, timeout=1)
        result["cancelled"] = waiting.cancelled()

    async with (
        _hetu_app(monkeypatch, body) as (url, _server),
        connect(url) as _ws,
    ):
        assert await _wait_for(lambda: "cancelled" in result, 3)
    assert result["cancelled"] is True


async def test_stop_receiving_wakeup_ignores_earlier_swallowed_cancel(monkeypatch):
    """
    recv 所在的任务之前吞过一次取消、没撤销（cancelling() 留着 1）：叫醒照样转成 Draining，不当成同时
    还有别的取消请求
    """
    result: dict[str, Any] = {}

    async def body(conn):
        async def recv_after_swallowed_cancel():
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                pass  # 吞掉取消、不撤销
            return await conn.recv()

        waiting = asyncio.ensure_future(recv_after_swallowed_cancel())
        await asyncio.sleep(0.05)
        waiting.cancel()
        await asyncio.sleep(0.05)  # 这时在 recv 里等着
        conn.stop_receiving()
        await asyncio.wait({waiting}, timeout=1)
        result["exc"] = None if waiting.cancelled() else type(waiting.exception())

    async with (
        _hetu_app(monkeypatch, body) as (url, _server),
        connect(url) as _ws,
    ):
        assert await _wait_for(lambda: "exc" in result, 3)
    assert result["exc"] is Draining


async def test_wait_closed_after_fail(monkeypatch):
    """wait_closed 等到连接被 fail；已经关了就马上返回"""
    done: list[str] = []

    async def body(conn):
        waiting = asyncio.ensure_future(conn.wait_closed())
        await asyncio.sleep(0.05)
        assert not waiting.done()
        conn.fail(4002, "rejected")
        await asyncio.wait_for(waiting, 1)
        await asyncio.wait_for(conn.wait_closed(), 1)
        done.append("ok")

    async with (
        _hetu_app(monkeypatch, body) as (url, _server),
        connect(url) as ws,
    ):
        with pytest.raises(ConnectionClosedError):
            await asyncio.wait_for(ws.recv(), 3)
    assert done == ["ok"]


@pytest.mark.parametrize("how", ["close", "abort"])
async def test_wait_closed_when_recv_finds_peer_gone(monkeypatch, how):
    """recv 发现对端断开（正常关闭、直接断开 TCP）也算关了，不用等到 handler 收尾时 fail"""
    done: list[str] = []

    async def body(conn):
        with pytest.raises(ConnectionClosed):
            await conn.recv()
        await asyncio.wait_for(conn.wait_closed(), 1)
        done.append("ok")

    async with _hetu_app(monkeypatch, body) as (url, _server):
        ws = await connect(url)
        if how == "close":
            await ws.close()
        else:
            ws.transport.abort()
        assert await _wait_for(lambda: bool(done), 3)


async def test_wait_closed_when_send_finds_peer_gone(monkeypatch):
    """send 发现连接断了也算关了（关服排空时接收那一边已经停了，只有发送会发现对端断开）"""
    done: list[str] = []
    gone = asyncio.Event()

    async def body(conn):
        await gone.wait()
        with pytest.raises(ConnectionClosed):
            for _ in range(200):
                await conn.send(b"x")
                await asyncio.sleep(0.01)
        await asyncio.wait_for(conn.wait_closed(), 1)
        done.append("ok")

    async with _hetu_app(monkeypatch, body) as (url, _server):
        ws = await connect(url)
        ws.transport.abort()
        gone.set()
        assert await _wait_for(lambda: bool(done), 4)


async def test_drain_during_handshake_closes_with_going_away(monkeypatch):
    """开始排空时连接还在握手（等握手消息）：recv 抛 Draining，serve_connection 返回，handler 收尾时以 1001 断开"""
    registered = asyncio.Event()
    conns: list[AiohttpWsConnection] = []

    async def body(conn):
        conns.append(conn)
        registered.set()
        with pytest.raises(Draining):
            await conn.recv(timeout=5)

    async with (
        _hetu_app(monkeypatch, body) as (url, server),
        connect(url) as ws,
    ):
        await asyncio.wait_for(registered.wait(), 3)
        server.stopping = True
        conns[0].stop_receiving()
        with pytest.raises(ConnectionClosedOK) as exc_info:
            await asyncio.wait_for(ws.recv(), 3)
    rcvd = exc_info.value.rcvd
    assert rcvd is not None and rcvd.code == 1001


async def test_wait_closed_when_send_finds_heartbeat_timed_out(monkeypatch):
    """
    没人在 recv（关服排空时接收那一边已经停了）、对端不回 pong：aiohttp 心跳超时自己把 ws 标成关了，
    send 发现之后也算关了
    """
    done: list[str] = []

    async def body(conn):
        assert await _wait_for(lambda: conn._ws.closed, 5)
        with pytest.raises(ConnectionClosed):
            await conn.send(b"x")
        await asyncio.wait_for(conn.wait_closed(), 1)
        done.append("ok")

    async with _hetu_app(monkeypatch, body, WEBSOCKET_PING_INTERVAL=0.2) as (
        url,
        _server,
    ):
        _reader, writer = await _raw_upgrade(url)
        writer.transport.pause_reading()  # 收不到 ping，也就不回 pong
        assert await _wait_for(lambda: bool(done), 6)
        writer.close()


async def test_drain_does_not_wait_for_connections_closed_by_peer(monkeypatch):
    """对端已经断开、还在拆（跑断线 System）的连接没有要发的东西了：排空不等它拆完"""
    closed, release = asyncio.Event(), asyncio.Event()

    async def body(conn):
        with pytest.raises(ConnectionClosed):
            await conn.recv()
        closed.set()
        await release.wait()  # 还在拆连接：断线 System 还在跑

    async with _hetu_app(monkeypatch, body, SHUTDOWN_DRAIN_TIMEOUT=2) as (url, server):
        try:
            async with connect(url):
                pass  # 客户端关掉连接
            await asyncio.wait_for(closed.wait(), 3)
            assert len(server.connections) == 1
            start = time.monotonic()
            await asyncio.wait_for(server._drain(), 5)
            assert time.monotonic() - start < 1
        finally:
            release.set()


class _DrainingConn:
    """AiohttpServer._drain 用的连接替身：停止接收之后过 closes_after 秒关掉，None 就一直不关"""

    def __init__(self, closes_after: float | None):
        self.closes_after = closes_after
        self.stopped = False
        self._closed = asyncio.Event()
        # 在 wait_closed 里等着的任务
        self.waiting: list[asyncio.Task] = []

    def stop_receiving(self) -> None:
        self.stopped = True
        if self.closes_after is not None:
            asyncio.get_running_loop().call_later(self.closes_after, self._closed.set)

    async def wait_closed(self) -> None:
        task = asyncio.current_task()
        assert task is not None
        self.waiting.append(task)
        await self._closed.wait()


async def test_drain_waits_for_connections_up_to_the_budget():
    """_drain 让各连接停止接收，等它们都关了，最多 SHUTDOWN_DRAIN_TIMEOUT 秒，到点不再等"""
    server = AiohttpServer(HeTuConfig({"SHUTDOWN_DRAIN_TIMEOUT": 0.3}))
    quick, stuck = _DrainingConn(0.05), _DrainingConn(None)
    server.connections.update({cast(Any, quick), cast(Any, stuck)})
    start = time.monotonic()
    await asyncio.wait_for(server._drain(), 3)
    took = time.monotonic() - start
    assert quick.stopped and stuck.stopped
    assert 0.25 < took < 1
    await asyncio.sleep(0)
    # 到点还没关的：在等它的任务收掉
    assert stuck.waiting and all(task.done() for task in stuck.waiting)

    server = AiohttpServer(HeTuConfig({"SHUTDOWN_DRAIN_TIMEOUT": 5}))
    server.connections.update({cast(Any, _DrainingConn(0.05))})
    start = time.monotonic()
    await asyncio.wait_for(server._drain(), 3)
    assert time.monotonic() - start < 1  # 都排空了就不等满预算


async def test_drain_resumed_waits_only_the_rest_of_the_budget():
    """关服被打断后再排空：只等预算剩下的部分，不重新等满；被打断时在等的任务都收掉"""
    server = AiohttpServer(HeTuConfig({"SHUTDOWN_DRAIN_TIMEOUT": 1.0}))
    stuck = _DrainingConn(None)
    server.connections.add(cast(Any, stuck))
    first = asyncio.ensure_future(server._drain())
    await asyncio.sleep(0.6)
    first.cancel()
    await asyncio.wait({first}, timeout=1)
    await asyncio.sleep(0)
    assert stuck.waiting and all(task.done() for task in stuck.waiting)
    start = time.monotonic()
    await asyncio.wait_for(server._drain(), 3)
    assert time.monotonic() - start < 0.8  # 预算还剩 0.4 秒


# ==== 关服 ====


async def test_connection_during_shutdown_is_closed_with_going_away(monkeypatch):
    """关服开始之后才握完手的连接（停止监听之前接进来的）收不到关服时那一轮 fail，自己马上以 1001 断开"""
    errors: list[BaseException | None] = []

    async def body(conn):
        with pytest.raises(ConnectionClosed):
            await conn.recv()
        errors.append(conn.protocol_error())

    async with _hetu_app(monkeypatch, body) as (url, server):
        server.stopping = True
        async with connect(url) as ws:
            with pytest.raises(ConnectionClosedOK) as exc_info:
                await asyncio.wait_for(ws.recv(), 3)
    rcvd = exc_info.value.rcvd
    assert rcvd is not None and rcvd.code == 1001
    assert errors == [None]


class _FakeServer:
    """_serve 用的 AiohttpServer 替身：start 照 start_body 跑，记下调用顺序"""

    def __init__(self, start_body, events: list[str]):
        self.start_body = start_body
        self.events = events
        self.runtime = SimpleNamespace(exit_code=0)
        self.request_stop: Callable[[], None] = lambda: None
        self.restartable: bool | None = None

    def __call__(self, config, *, on_stop_requested, restartable=False):
        self.request_stop = on_stop_requested
        self.restartable = restartable
        return self

    async def start(self, sock):
        self.events.append("start")
        try:
            await self.start_body(self)
        except asyncio.CancelledError:
            self.events.append("start cancelled")
            raise

    async def stop(self):
        self.events.append("stop")


async def test_serve_stops_when_server_requests_it(monkeypatch):
    """服务要求停下时（比如租约丢了）_serve 关服、带出退出码，不等关服信号"""
    events: list[str] = []

    async def lease_lost(server):
        server.runtime.exit_code = 1
        server.request_stop()

    monkeypatch.setattr(aiohttp_app, "AiohttpServer", _FakeServer(lease_lost, events))
    sock = aiohttp_app.bind_socket("127.0.0.1:0")
    try:
        code = await asyncio.wait_for(aiohttp_app._serve(HeTuConfig(), sock), 5)
    finally:
        sock.close()
    assert events == ["start", "stop"]
    assert code == 1


async def test_serve_aborts_startup_when_stop_is_requested(monkeypatch):
    """开服途中（连后端、跑启动 System 可能要很久）要求停下：不等开服跑完，取消它，再关服"""
    events: list[str] = []
    fake = _FakeServer(lambda server: asyncio.Event().wait(), events)
    monkeypatch.setattr(aiohttp_app, "AiohttpServer", fake)
    sock = aiohttp_app.bind_socket("127.0.0.1:0")
    try:
        serving = asyncio.ensure_future(aiohttp_app._serve(HeTuConfig(), sock))
        assert await _wait_for(lambda: events == ["start"], 2)
        fake.request_stop()
        code = await asyncio.wait_for(serving, 5)
    finally:
        sock.close()
    assert events == ["start", "start cancelled", "stop"]
    assert code == 0


class _FakeControl:
    """_serve 的 worker 模式用的控制管道替身：记下发给管理进程的消息，留着停服回调"""

    def __init__(self) -> None:
        self.sent: list[str] = []
        self.on_stop: Callable[[], None] | None = None

    def listen(self, on_stop: Callable[[], None]) -> None:
        self.on_stop = on_stop

    def send_ready(self) -> None:
        self.sent.append("ready")

    def send_lease_lost(self) -> None:
        self.sent.append("lease_lost")


async def test_serve_worker_reports_ready_and_stops_on_control(monkeypatch):
    """
    多进程的 worker：开好服后报 ready；管理进程的停服通知（控制管道的线程里调 on_stop）到了就关服，
    以 0 退出。AiohttpServer 按可重启（worker）建
    """
    import threading

    events: list[str] = []
    fake = _FakeServer(lambda server: asyncio.sleep(0), events)
    monkeypatch.setattr(aiohttp_app, "AiohttpServer", fake)
    control = _FakeControl()
    sock = aiohttp_app.bind_socket("127.0.0.1:0")
    try:
        serving = asyncio.ensure_future(aiohttp_app._serve(HeTuConfig(), sock, control))
        assert await _wait_for(lambda: control.sent == ["ready"], 2)
        assert fake.restartable is True
        assert events == ["start"]
        on_stop = control.on_stop
        assert on_stop is not None
        threading.Thread(target=on_stop).start()
        code = await asyncio.wait_for(serving, 5)
    finally:
        sock.close()
    assert events == ["start", "stop"]
    assert code == 0


async def test_serve_worker_reports_lease_lost(monkeypatch):
    """多进程的 worker 丢了租约：告诉管理进程（由它重启），关服，以非 0 退出"""

    async def lease_lost(server):
        server.runtime.exit_code = 1
        server.request_stop()

    events: list[str] = []
    monkeypatch.setattr(aiohttp_app, "AiohttpServer", _FakeServer(lease_lost, events))
    control = _FakeControl()
    sock = aiohttp_app.bind_socket("127.0.0.1:0")
    try:
        code = await asyncio.wait_for(
            aiohttp_app._serve(HeTuConfig(), sock, control), 5
        )
    finally:
        sock.close()
    assert "lease_lost" in control.sent
    assert events == ["start", "stop"]
    assert code == 1


@pytest.mark.skipif(os.name == "nt", reason="POSIX 信号")
async def test_shutdown_signals_stay_ignored_after_the_first():
    """
    关服信号只认第一个：之后再来的被忽略，卸载处理之后也继续忽略，免得关完服、进程退出前的收尾被一个
    迟到的 SIGTERM 直接杀掉。这里真往测试进程发 SIGTERM，测完把信号处理恢复原样
    """
    import signal

    loop = asyncio.get_running_loop()
    stops: list[int] = []
    previous = signal.getsignal(signal.SIGINT), signal.getsignal(signal.SIGTERM)
    try:
        restore = aiohttp_app._install_stop_signals(loop, lambda: stops.append(1))
        os.kill(os.getpid(), signal.SIGTERM)
        assert await _wait_for(lambda: stops == [1], 2)
        os.kill(os.getpid(), signal.SIGTERM)  # 已经忽略了，进程还活着
        await asyncio.sleep(0.1)
        restore()
        assert signal.getsignal(signal.SIGTERM) == signal.SIG_IGN
        assert signal.getsignal(signal.SIGINT) == signal.SIG_IGN
        os.kill(os.getpid(), signal.SIGTERM)
        await asyncio.sleep(0.1)
        assert stops == [1]
    finally:
        signal.signal(signal.SIGINT, previous[0])
        signal.signal(signal.SIGTERM, previous[1])


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
