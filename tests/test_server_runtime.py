"""
WorkerRuntime 的任务表 TaskRegistry：不依赖后端、不起服务器的单元测试。
"""

import asyncio
import inspect
import logging

import pytest

from hetu.server.runtime import TaskRegistry


async def _forever():
    await asyncio.Event().wait()


async def test_duplicate_name_rejected_and_coroutine_closed():
    reg = TaskRegistry()
    task = reg.spawn(_forever(), name="t")
    dup = _forever()
    with pytest.raises(ValueError, match="'t'"):
        reg.spawn(dup, name="t")
    # 被拒的协程要 close 掉，不留"coroutine was never awaited"
    assert inspect.getcoroutinestate(dup) == inspect.CORO_CLOSED
    await reg.cancel(task)
    assert len(reg) == 0


async def test_cancel_unknown_or_finished_is_noop():
    reg = TaskRegistry()
    await reg.cancel("never-spawned")

    async def quick():
        return 1

    task = reg.spawn(quick(), name="q")
    await asyncio.wait({task})
    await reg.cancel("q")
    await reg.cancel(task)
    assert len(reg) == 0


async def test_cancel_keeps_callers_own_cancellation():
    """
    调用方在等 cancel() 时自己被取消，要照常抛 CancelledError。用 `await task` 等的话，asyncio
    会把调用方的取消转给 task，task 吞掉 CancelledError 正常返回（client_handler 就是这样）时
    调用方的取消就丢了
    """
    reg = TaskRegistry()

    async def swallow():
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            return

    reg.spawn(swallow(), name="t")
    caller = asyncio.create_task(reg.cancel("t"))
    await asyncio.sleep(0)  # 让 caller 跑到等 task 结束那一步
    caller.cancel()
    with pytest.raises(asyncio.CancelledError):
        await caller


async def test_cancel_does_not_leak_target_cancellation():
    """
    被取消的 task 以 CancelledError 结束，不能当成调用方自己被取消往外抛。调用方之前被取消过、
    已经吞掉（cancelling() 仍是 1）时尤其容易误判：websocket_connection 在 send_loop 里就会这样
    """
    reg = TaskRegistry()

    async def caller_body():
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            pass  # 吞掉自己的取消
        reg.spawn(_forever(), name="t")
        await reg.cancel("t")
        return "done"

    caller = asyncio.create_task(caller_body())
    await asyncio.sleep(0)
    caller.cancel()
    assert await caller == "done"


async def test_failed_task_logged_once(caplog):
    reg = TaskRegistry()

    async def boom():
        raise RuntimeError("boom in task")

    task = reg.spawn(boom(), name="b")
    with caplog.at_level(logging.ERROR, logger="HeTu.root"):
        await asyncio.wait({task})
        await asyncio.sleep(0)  # 等 done 回调跑完
        await reg.cancel(task)
    records = [r for r in caplog.records if "boom in task" in r.getMessage()]
    assert len(records) == 1
    assert len(reg) == 0


async def test_cancel_all_reports_timeout():
    reg = TaskRegistry()
    release = asyncio.Event()

    async def stubborn():
        while not release.is_set():
            try:
                await release.wait()
            except asyncio.CancelledError:
                continue  # 不理取消

    task = reg.spawn(stubborn())
    await asyncio.sleep(0)  # 先跑到 await，没开始就取消的 task 根本不进函数体
    assert await reg.cancel_all(0.05) is False
    release.set()
    await asyncio.wait({task})
    assert len(reg) == 0


async def test_cancel_all_also_cancels_tasks_spawned_while_waiting():
    reg = TaskRegistry()
    late: list[asyncio.Task] = []

    async def spawn_on_cancel():
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            late.append(reg.spawn(_forever(), name="late"))

    reg.spawn(spawn_on_cancel())
    await asyncio.sleep(0)
    assert await reg.cancel_all(1) is True
    assert late and late[0].cancelled()
    assert len(reg) == 0


async def test_renamed_task_releases_its_name():
    """名字按登记时的算：task 被 set_name 改名后结束，原来的名字也要放出来"""
    reg = TaskRegistry()

    async def quick():
        return 1

    task = reg.spawn(quick(), name="x")
    task.set_name("renamed")
    await asyncio.wait({task})
    await asyncio.sleep(0)
    again = reg.spawn(_forever(), name="x")
    await reg.cancel(again)
