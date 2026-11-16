"""
不依赖后端、不起服务器的服务端单元测试：WorkerRuntime 的任务表 TaskRegistry、HeTuConfig 等。
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
    已经吞掉（cancelling() 仍是 1）时尤其容易误判：serve_connection 等收发协程时就会这样
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


def test_logging_config_referencing_sanic_is_rejected(monkeypatch):
    """
    LOGGING 里还引用着 Sanic 的类：setup_logging 先报 LoggingConfigError，写明是哪几项、要改成什么，
    不去 dictConfig（不然只报一句 "Unable to configure formatter"）。QueueHandler 的 queue、listener
    也查。文件名、logger 名里出现 sanic、名字以 sanic 开头的别的包都不算；配置模板、默认配置都能过
    """
    import logging.config
    from pathlib import Path

    import yaml

    import hetu
    from hetu.common import yamlloader
    from hetu.safelogging.default import DEFAULT_LOGGING_CONFIG
    from hetu.server.runtime import (
        LoggingConfigError,
        check_logging_config,
        setup_logging,
    )

    configured = []
    monkeypatch.setattr(logging.config, "dictConfig", configured.append)
    old_config = {
        "version": 1,
        "formatters": {"colour": {"class": "sanic.logging.formatter.AutoFormatter"}},
        "filters": {"verbosity": {"()": "sanic.logging.filter.VerbosityFilter"}},
        "handlers": {
            "console": {"class": "logging.StreamHandler"},
            "queued": {
                "class": "logging.handlers.QueueHandler",
                "queue": "sanic.compat.Queue",
                "listener": "sanic.logging.Listener",
            },
        },
    }
    with pytest.raises(LoggingConfigError) as exc_info:
        setup_logging({"LOGGING": old_config})
    message = str(exc_info.value)
    assert "formatters.colour.class: sanic.logging.formatter.AutoFormatter" in message
    assert "filters.verbosity.(): sanic.logging.filter.VerbosityFilter" in message
    assert "handlers.queued.queue: sanic.compat.Queue" in message
    assert "handlers.queued.listener: sanic.logging.Listener" in message
    assert "hetu.safelogging.formatter.ContextAutoFormatter" in message
    assert "logging.StreamHandler" not in message
    assert configured == []

    # 名字以 sanic 开头的别的包（sanic_tools 之类）也不算
    check_logging_config(
        {
            "version": 1,
            "formatters": {"plain": {"class": "sanic_tools.PlainFormatter"}},
            "handlers": {
                "file": {"class": "logging.FileHandler", "filename": "sanic.log"}
            },
            "loggers": {"sanic.root": {"level": "INFO"}},
        }
    )
    check_logging_config(DEFAULT_LOGGING_CONFIG)
    template = Path(hetu.__file__).parent / "CONFIG_TEMPLATE.yml"
    config = yaml.load(template.read_text(encoding="utf-8"), yamlloader.Loader)
    check_logging_config(config["LOGGING"])


def test_hetu_config_defaults_and_attribute_access():
    """HeTuConfig：只收大写键，可以按属性读，没给的键用默认值（和原来 Sanic 的默认值相同）"""
    import pickle

    from hetu.server.config import DEFAULTS, HeTuConfig

    config = HeTuConfig(
        {"NAMESPACE": "game", "lower_key": 1, "GRACEFUL_SHUTDOWN_TIMEOUT": 3}
    )
    assert config.NAMESPACE == config["NAMESPACE"] == "game"
    assert "lower_key" not in config
    assert config.GRACEFUL_SHUTDOWN_TIMEOUT == 3
    assert config.WEBSOCKET_MAX_SIZE == DEFAULTS["WEBSOCKET_MAX_SIZE"] == 2**20
    assert config.DEBUG is False
    with pytest.raises(AttributeError):
        _ = config.NOT_SET
    # 按属性写也写进 dict
    config.NEW_KEY = 1
    assert config["NEW_KEY"] == config.get("NEW_KEY") == 1
    # 多进程时要把配置 pickle 给 spawn 出来的 worker
    restored = pickle.loads(pickle.dumps(config))
    assert type(restored) is HeTuConfig
    assert restored == config


def test_connection_handling_does_not_load_web_server():
    """
    处理连接的代码（serve_connection、client_handler、WsConnection）与 Web 框架无关：在干净的子进程
    里 import 它们不加载 aiohttp。pytest 主进程早被别的测试 import 过 aiohttp，只能在子进程里看
    """
    import os
    import subprocess
    import sys

    code = (
        "import sys\n"
        "import hetu.endpoint.context, hetu.server.conn, hetu.server.receiver, "
        "hetu.server.websocket\n"
        "print('aiohttp' in sys.modules)\n"
    )
    proc = subprocess.run(
        [sys.executable, "-c", code],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=120,
        env={**os.environ, "PYTHONUTF8": "1"},
    )
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip().splitlines()[-1] == "False"
