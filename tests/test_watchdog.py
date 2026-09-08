"""事件循环卡死看门狗的测试"""

import asyncio
import os
import socket
import threading
import time
from types import SimpleNamespace

import pytest

from hetu.server import watchdog
from hetu.server.watchdog import (
    ACCEPT_DEADLOCK_HEALED,
    WINDOWS_HANG_TIMEOUT,
    LoopWatchdog,
    _kick_address,
    hang_watchdog_task,
    resolve_hang_timeout,
)


@pytest.fixture
def dump_dir(tmp_path):
    return str(tmp_path / "logs")


async def test_watchdog_dumps_on_stall(dump_dir):
    # 心跳停了就该dump，且内容里要能看到卡住的那个函数名和协程
    watchdog = LoopWatchdog(0.2, dump_dir=dump_dir)

    async def stuck_coroutine():
        # 同步sleep，模拟把事件循环堵死的阻塞调用
        time.sleep(1.2)

    task = asyncio.create_task(stuck_coroutine(), name="StuckTask")
    watchdog.start()
    await task
    watchdog.stop()

    with open(watchdog.dump_file, encoding="utf-8") as f:
        dump = f.read()
    assert "stuck_coroutine" in dump  # faulthandler的线程栈
    assert "StuckTask" in dump  # 协程任务列表


async def test_watchdog_silent_when_healthy(dump_dir):
    # loop正常转动时不能产生任何dump文件
    watchdog = LoopWatchdog(0.2, dump_dir=dump_dir)
    watchdog.start()
    for _i in range(10):
        watchdog.beat()
        await asyncio.sleep(0.05)
    watchdog.stop()

    assert not os.path.exists(watchdog.dump_file)


async def test_watchdog_reports_recovery(dump_dir):
    # 卡住后恢复了，要留下恢复记录，用于区分永久死锁和长阻塞
    watchdog = LoopWatchdog(0.2, dump_dir=dump_dir)
    watchdog.start()
    time.sleep(0.6)
    for _i in range(8):
        watchdog.beat()
        await asyncio.sleep(0.05)
    watchdog.stop()

    with open(watchdog.dump_file, encoding="utf-8") as f:
        dump = f.read()
    assert "看门狗" in dump
    assert "恢复" in dump


@pytest.fixture
def not_windows(monkeypatch):
    monkeypatch.setattr(watchdog, "IS_WINDOWS", False)


@pytest.fixture
def on_windows(monkeypatch):
    monkeypatch.setattr(watchdog, "IS_WINDOWS", True)


def test_hang_timeout_defaults_off_elsewhere(not_windows):
    # 非Windows：默认关闭，配了才开
    assert resolve_hang_timeout({}) == 0
    assert resolve_hang_timeout({"HANG_WATCHDOG_TIMEOUT": 0}) == 0
    assert resolve_hang_timeout({"HANG_WATCHDOG_TIMEOUT": 30}) == 30


def test_hang_timeout_forced_on_windows(on_windows):
    # Windows：关不掉（那里的accept死锁没有看门狗既查不出也救不回来），但阈值可调
    assert resolve_hang_timeout({}) == WINDOWS_HANG_TIMEOUT
    assert resolve_hang_timeout({"HANG_WATCHDOG_TIMEOUT": 0}) == WINDOWS_HANG_TIMEOUT
    assert resolve_hang_timeout({"HANG_WATCHDOG_TIMEOUT": 30}) == 30


def test_autoheal_follows_platform(monkeypatch):
    # 自愈只在Windows自动开启，不占配置项
    monkeypatch.setattr(watchdog, "IS_WINDOWS", True)
    assert LoopWatchdog(1.0).autoheal is True
    monkeypatch.setattr(watchdog, "IS_WINDOWS", False)
    assert LoopWatchdog(1.0).autoheal is False
    assert LoopWatchdog(1.0, autoheal=True).autoheal is True  # 显式传参优先


async def test_watchdog_task_disabled_when_off(not_windows):
    # 解析出0时task应直接返回，不开线程
    fake_app = SimpleNamespace(config={"HANG_WATCHDOG_TIMEOUT": 0})
    await asyncio.wait_for(hang_watchdog_task(fake_app), timeout=1)


def test_kick_address_maps_wildcard():
    # 通配监听地址要换成回环地址，否则连不回自己
    assert _kick_address(_FakeSock(("0.0.0.0", 874))) == ("127.0.0.1", 874)
    assert _kick_address(_FakeSock(("::", 874))) == ("::1", 874)
    assert _kick_address(_FakeSock(("10.0.0.5", 874))) == ("10.0.0.5", 874)
    assert _kick_address(_FakeSock("/tmp/x.sock")) is None  # unix socket 捅不了


class _FakeSock:
    def __init__(self, name):
        self._name = name

    def getsockname(self):
        return self._name


def _run_deadlocked_server(loop, dump_dir, state, ready, autoheal=True):
    """在独立线程里跑一个必然会 accept 死锁的 selector 事件循环。

    复现手法：create_server 之后把监听socket设回阻塞模式。这样 select 报告可读、
    但队列已空时，_accept_connection 里的 accept() 会真的堵死——和 Windows 多worker
    共享监听socket时抢输的那个worker的现场完全一致。
    """

    async def setup():
        sock = socket.socket()
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("127.0.0.1", 0))
        sock.listen(100)

        class Proto(asyncio.Protocol):
            def connection_made(self, transport):
                transport.close()

        await loop.create_server(Proto, None, None, sock=sock, backlog=100)
        sock.setblocking(True)  # ← 病态状态
        state["port"] = sock.getsockname()[1]

        watchdog = LoopWatchdog(0.3, dump_dir=dump_dir, autoheal=autoheal)
        watchdog.start()
        state["watchdog"] = watchdog

        async def heartbeat():
            while True:
                watchdog.beat()
                state["beats"] += 1
                await asyncio.sleep(0.05)

        loop.create_task(heartbeat())
        ready.set()

    asyncio.set_event_loop(loop)
    loop.run_until_complete(setup())
    loop.run_forever()
    pending = asyncio.all_tasks(loop)  # 收尾，免得刷"Task was destroyed"
    for task in pending:
        task.cancel()
    if pending:
        loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
    loop.close()


def test_watchdog_heals_accept_deadlock(dump_dir):
    # 看门狗必须能把卡在 accept() 里的事件循环捅醒（Sanic on Windows 的共享socket BUG）
    loop = asyncio.SelectorEventLoop()
    state = {"beats": 0, "port": 0, "watchdog": None}
    ready = threading.Event()
    thread = threading.Thread(
        target=_run_deadlocked_server, args=(loop, dump_dir, state, ready), daemon=True
    )
    thread.start()
    assert ready.wait(10), "测试服务器没起来"

    try:
        # 来一个连接触发 _accept_connection，它接完这个后会堵死在下一次 accept 上
        socket.create_connection(("127.0.0.1", state["port"]), timeout=5).close()

        # 等看门狗把它救回来：心跳重新跳动即为恢复
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            before = state["beats"]
            time.sleep(0.5)
            if state["beats"] > before + 3:
                break
        else:
            pytest.fail("看门狗没能把 accept 死锁救回来")
    finally:
        loop.call_soon_threadsafe(loop.stop)
        thread.join(10)
        if state["watchdog"] is not None:
            state["watchdog"].stop()

    watchdog = state["watchdog"]
    assert watchdog is not None
    with open(watchdog.dump_file, encoding="utf-8") as f:
        dump = f.read()
    # 既证明它确实死锁过（不是测试跑空），也证明自愈跑到了成功分支
    assert "_accept_connection" in dump
    assert ACCEPT_DEADLOCK_HEALED in dump


def test_watchdog_autoheal_can_be_disabled(dump_dir):
    # autoheal=False 时不许主动建连，卡住就一直卡住
    loop = asyncio.SelectorEventLoop()
    state = {"beats": 0, "port": 0, "watchdog": None}
    ready = threading.Event()
    thread = threading.Thread(
        target=_run_deadlocked_server,
        args=(loop, dump_dir, state, ready, False),
        daemon=True,
    )
    try:
        thread.start()
        assert ready.wait(10), "测试服务器没起来"
        socket.create_connection(("127.0.0.1", state["port"]), timeout=5).close()

        time.sleep(3)  # 远超 0.3 秒阈值 + 自愈耗时
        frozen = state["beats"]
        time.sleep(1)
        assert state["beats"] == frozen, "关掉自愈后不该自己恢复"
    finally:
        if state["watchdog"] is not None:
            state["watchdog"].stop()
        # 把卡住的loop捅醒才能干净退出
        for _i in range(200):
            try:
                socket.create_connection(
                    ("127.0.0.1", state["port"]), timeout=1
                ).close()
            except OSError:
                break
        loop.call_soon_threadsafe(loop.stop)
        thread.join(10)
