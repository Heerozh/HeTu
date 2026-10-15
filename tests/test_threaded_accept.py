"""accept 线程补丁的测试（根治 Windows 多 worker 的 accept 死锁，见 threaded_accept.py）

Windows 卡死现场：监听 socket 在 Python 层 gettimeout() 是 0.0，内核层却是阻塞模式。
只改内核层就能造出同一个现场（Linux 用 os.set_blocking，Windows 用 ioctlsocket），
所以这里在任何平台上都能验证"accept 堵住时事件循环还转不转"。
"""

import asyncio
import ctypes
import inspect
import os
import socket
import sys
import threading
import time
from asyncio import selector_events
from types import SimpleNamespace
from typing import Any

from hetu.server.threaded_accept import (
    install_threaded_accept,
    threaded_accept_listener,
)

BACKLOG = 1
FIONBIO = 0x8004667E


def _set_os_blocking(sock: socket.socket):
    """只把内核层改成阻塞，Python 层的 timeout 不动"""
    if sys.platform == "win32":
        # os.set_blocking 在 Windows 上不认 socket 句柄
        ws2_32 = ctypes.WinDLL("ws2_32")
        ws2_32.ioctlsocket.argtypes = [
            ctypes.c_size_t,
            ctypes.c_long,
            ctypes.POINTER(ctypes.c_ulong),
        ]
        nonblocking = ctypes.c_ulong(0)
        ret = ws2_32.ioctlsocket(
            sock.fileno(), ctypes.c_long(FIONBIO).value, ctypes.byref(nonblocking)
        )
        assert ret == 0, f"ioctlsocket 失败：{ws2_32.WSAGetLastError()}"
    else:
        os.set_blocking(sock.fileno(), True)


class _Proto(asyncio.Protocol):
    def __init__(self, state):
        self.state = state

    def connection_made(self, transport):
        self.state["accepted"] += 1
        self.state["peernames"].append(transport.get_extra_info("peername"))
        transport.close()


class _EchoProto(asyncio.Protocol):
    def connection_made(self, transport):
        assert isinstance(transport, asyncio.Transport)
        self.transport = transport

    def data_received(self, data):
        self.transport.write(data)


def _run_server(loop, state, ready, patched, os_blocking):
    """独立线程里跑一个 selector 事件循环 + 监听服务器 + 心跳"""

    async def setup():
        if patched:
            assert install_threaded_accept(loop)
        sock = socket.socket()
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("127.0.0.1", 0))
        server = await loop.create_server(
            lambda: _Proto(state), sock=sock, backlog=BACKLOG
        )
        if os_blocking:
            # 只改内核层，Python 层仍是 0.0——和 Windows 卡死现场一致
            _set_os_blocking(sock)
            assert sock.gettimeout() == 0.0
        state["port"] = sock.getsockname()[1]
        state["server"] = server

        async def heartbeat():
            while True:
                state["beats"] += 1
                await asyncio.sleep(0.02)

        state["hb"] = loop.create_task(heartbeat())
        ready.set()

    asyncio.set_event_loop(loop)
    loop.run_until_complete(setup())
    loop.run_forever()
    state["hb"].cancel()
    server = state["server"]
    server.close()
    loop.run_until_complete(asyncio.gather(state["hb"], return_exceptions=True))
    loop.close()


def _start(patched, os_blocking):
    loop = asyncio.SelectorEventLoop()
    state = {"beats": 0, "port": 0, "accepted": 0, "peernames": []}
    ready = threading.Event()
    thread = threading.Thread(
        target=_run_server,
        args=(loop, state, ready, patched, os_blocking),
        daemon=True,
    )
    thread.start()
    assert ready.wait(10), "测试服务器没起来"
    return loop, state, thread


def _beats_during(state, seconds):
    before = state["beats"]
    time.sleep(seconds)
    return state["beats"] - before


def _accept_threads():
    return [t for t in threading.enumerate() if t.name == "HeTuAcceptThread"]


def test_stock_loop_stalls_on_blocking_listen_socket():
    """复现前提：原版 selector 事件循环在内核层阻塞的监听 socket 上，接完一个连接就会
    堵死在下一次 accept() 里（Windows 多 worker 卡死的现场）。这条不成立，下面的测试
    就证明不了什么。"""
    loop, state, thread = _start(patched=False, os_blocking=True)
    try:
        assert _beats_during(state, 0.3) > 3, "卡之前心跳应该正常"
        socket.create_connection(("127.0.0.1", state["port"]), timeout=5).close()
        time.sleep(0.2)
        assert _beats_during(state, 0.5) == 0, "原版事件循环应该堵死在 accept 里"
    finally:
        # 喂满 _accept_connection 的 range(backlog + 1) 循环，把它捅醒才能干净退出
        for _i in range(BACKLOG + 2):
            try:
                socket.create_connection(
                    ("127.0.0.1", state["port"]), timeout=1
                ).close()
            except OSError:
                break
        loop.call_soon_threadsafe(loop.stop)
        thread.join(10)


def test_threaded_accept_keeps_loop_alive_on_blocking_listen_socket():
    """装了补丁，同样的现场下事件循环照转，连接照接"""
    loop, state, thread = _start(patched=True, os_blocking=True)
    try:
        for _i in range(3):
            socket.create_connection(("127.0.0.1", state["port"]), timeout=5).close()
        time.sleep(0.2)
        assert _beats_during(state, 0.5) > 5, "事件循环不该被 accept 堵住"
        assert state["accepted"] == 3
    finally:
        loop.call_soon_threadsafe(loop.stop)
        thread.join(10)
    assert not thread.is_alive()


async def test_threaded_accept_serves_and_stops():
    """正常（非阻塞）模式下：连接走原版 _accept_connection2 建 transport，
    peername、Server 的连接计数都和原版一致；Server.close() 后 accept 线程退出"""
    # 测试用的 loop 是 module 级共享的（Windows 上还是 Proactor），补丁装在一个新的
    # selector loop 上更干净，所以另起线程跑
    result = await asyncio.to_thread(_serve_echo_in_new_loop)
    assert result["echo"] == [b"hello-0", b"hello-1", b"hello-2"]
    assert all(p is not None and p[0] == "127.0.0.1" for p in result["peernames"])
    assert result["threads_after_close"] == 0


def _serve_echo_in_new_loop():
    loop = asyncio.SelectorEventLoop()
    result: dict[str, Any] = {"echo": [], "peernames": []}

    class Echo(_EchoProto):
        def connection_made(self, transport):
            super().connection_made(transport)
            result["peernames"].append(transport.get_extra_info("peername"))

    async def main():
        assert install_threaded_accept(loop)
        server = await loop.create_server(Echo, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        assert len(_accept_threads()) >= 1
        for i in range(3):
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.write(f"hello-{i}".encode())
            await writer.drain()
            result["echo"].append(await reader.read(100))
            writer.close()
            await writer.wait_closed()
        server.close()
        await server.wait_closed()

    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
    deadline = time.monotonic() + 5
    while _accept_threads() and time.monotonic() < deadline:
        time.sleep(0.05)
    result["threads_after_close"] = len(_accept_threads())
    return result


def test_install_threaded_accept_only_on_selector_loops():
    # 不是 selector 事件循环（Proactor、uvloop……）不装
    assert install_threaded_accept(SimpleNamespace()) is False  # pyright: ignore[reportArgumentType]

    loop = asyncio.SelectorEventLoop()
    try:
        assert install_threaded_accept(loop) is True
        patched = loop._start_serving  # type: ignore
        assert install_threaded_accept(loop) is True  # 重复安装是空操作
        assert loop._start_serving is patched  # type: ignore
        # 只改这个实例，别的 loop 不受影响
        other = asyncio.SelectorEventLoop()
        try:
            assert other._start_serving.__func__ is (  # type: ignore
                selector_events.BaseSelectorEventLoop._start_serving  # type: ignore
            )
        finally:
            other.close()
    finally:
        loop.close()


def test_listener_installs_on_running_loop():
    loop = asyncio.SelectorEventLoop()
    try:
        loop.run_until_complete(threaded_accept_listener(None))
        assert getattr(loop, "_hetu_threaded_accept", False)
    finally:
        loop.close()


def test_cpython_private_api_unchanged():
    """补丁依赖 CPython selector 事件循环的三个私有方法。CPython 改了它们的签名，
    这里先红，别等到 Windows 上线才发现补丁调错了参数"""
    cls = selector_events.BaseSelectorEventLoop

    def params(name):
        return list(inspect.signature(getattr(cls, name)).parameters)

    assert params("_start_serving") == [
        "self",
        "protocol_factory",
        "sock",
        "sslcontext",
        "server",
        "backlog",
        "ssl_handshake_timeout",
        "ssl_shutdown_timeout",
    ]
    assert params("_stop_serving") == ["self", "sock"]
    assert params("_accept_connection2") == [
        "self",
        "protocol_factory",
        "conn",
        "extra",
        "sslcontext",
        "server",
        "ssl_handshake_timeout",
        "ssl_shutdown_timeout",
    ]
