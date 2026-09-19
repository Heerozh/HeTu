"""
验收 1 + 7：headless 进程在**另一个线程、另一个 event loop** 里写的行，经 WebSocket 连服务器的
客户端按行订阅（WatchRow）/ 索引订阅（WatchRange）/ 整表订阅都收到与 System 写入时一样的
推送。Redis 与 SQLite 各跑一遍（可用 HETU_TEST_BACKENDS 过滤）。
"""

import asyncio
import logging
import os
import threading
from contextlib import contextmanager
from typing import Any

import pytest
from fixtures.backends import ALL_BACKENDS, backend_config_by_name
from test_websocket import setup_websocket_proxy  # noqa: F401  ws 代理 fixture

from hetu import headless, webext
from hetu.endpoint.definer import EndpointDefines
from hetu.safelogging.default import DEFAULT_LOGGING_CONFIG
from hetu.server import worker_main
from hetu.system import SystemClusters

logger = logging.getLogger("HeTu.root")


@contextmanager
def _preserve_loggers():
    """worker_main 建 Sanic app 时会 dictConfig(LOGGING)，把 HeTu.replay 改成 ERROR 且
    不 propagate；本模块按字母序排在别的依赖 caplog 抓 replay 日志的测试之前，退出时把
    被改过的 logger 恢复原样，不把污染留给后面的测试。"""
    names = (None, "HeTu.root", "HeTu.replay")
    saved = {}
    for name in names:
        lg = logging.getLogger(name)
        saved[name] = (lg.level, lg.propagate, list(lg.handlers))
    try:
        yield
    finally:
        for name, (level, propagate, handlers) in saved.items():
            lg = logging.getLogger(name)
            lg.setLevel(level)
            lg.propagate = propagate
            lg.handlers[:] = handlers


@pytest.fixture(params=["redis", "sqlite"])
def hl_server(request, setup_websocket_proxy):  # noqa: F811
    backend_name = request.param
    if backend_name not in ALL_BACKENDS:
        pytest.skip(f"HETU_TEST_BACKENDS 未包含 {backend_name}")
    config = backend_config_by_name(backend_name, request)

    SystemClusters()._clear()
    EndpointDefines()._clear()
    # webext 注册表按 模块名.函数名 记路由：别的测试 `import app` 过之后再由 worker_main
    # 以 HeTuApp 之名 exec 同一个文件，同一 uri 就会注册两次撞 RouteExists，先清掉
    webext.clear()
    app_file = os.path.join(os.path.dirname(__file__), "app.py")
    with _preserve_loggers():
        server = worker_main(
            f"Hetu-headless-{backend_name}",
            {
                "APP_FILE": app_file,
                "NAMESPACE": "pytest",
                "INSTANCES": ["pytest_1"],
                "LISTEN": "0.0.0.0:874",
                "PACKET_LAYERS": [
                    {"type": "jsonb"},
                    {"type": "zlib"},
                    {"type": "crypto"},
                ],
                "BACKENDS": {"main": config},
                "CLIENT_SEND_LIMITS": [[10, 1], [27, 5], [100, 50], [300, 300]],
                "MAX_TABLE_SUBSCRIPTION": 1,
                "LOGGING": DEFAULT_LOGGING_CONFIG,
                "DEBUG": False,
                "WORKER_NUM": 1,
                "ACCESS_LOG": False,
            },
        )
        try:
            yield server, config
        finally:
            server.stop()
            webext.clear()


async def _drain(client, idle=0.8, total=6.0):
    """收 updt 帧直到 idle 秒内没有新帧；按 sub_id 合并（后到的覆盖）。"""
    merged: dict[str, dict] = {}
    loop = asyncio.get_running_loop()
    deadline = loop.time() + total
    while loop.time() < deadline:
        try:
            msg = await asyncio.wait_for(client.recv(), timeout=idle)
        except TimeoutError:
            break
        if msg[0] == "updt":
            merged.setdefault(msg[1], {}).update(msg[2])
    return merged


def _headless_writer(config: dict, steps: list[threading.Event], errors: list):
    """独立线程 + 独立 event loop 里的 headless 进程：按 steps 逐步写三次"""

    async def main():
        client = await headless.connect(config, "pytest_1", ["PublicNames"])
        Names = client.table("PublicNames").comp_cls  # 名字模式：本地零定义
        try:
            steps[0].wait(30)
            async with client.session("PublicNames") as s:
                row = await s[Names].get(owner=1)
                assert row is not None
                row.name = "Alice2"
                await s[Names].update(row)

            steps[1].wait(30)
            async with client.session("PublicNames") as s:
                row = Names.new_row(id_=-2)
                row.owner = 2
                row.name = "Bob"
                await s[Names].insert(row)

            steps[2].wait(30)
            async with client.session("PublicNames") as s:
                row = await s[Names].get(id=-2)
                assert row is not None
                s[Names].delete(int(row.id))
        finally:
            await client.close()

    try:
        asyncio.run(main())
    except BaseException as e:  # noqa: BLE001
        errors.append(repr(e))
        for ev in steps:
            ev.set()


def test_headless_writes_push_to_ws_subscribers(hl_server):
    server, config = hl_server
    steps = [threading.Event() for _ in range(3)]
    errors: list = []
    collected: dict[str, Any] = {}

    async def routine(connect):
        c = await connect()
        # 服务器 System 先造一行，作为 WatchRow 的目标
        await c.send(["rpc", "set_public_name", 1, "Alice"])
        await c.recv()
        await c.send(["sub", "PublicNames", "get", "owner", 1])
        sub_get = await c.recv()
        await c.send(["sub", "PublicNames", "range", "owner", 1, 999, 100, False])
        sub_range = await c.recv()
        await c.send(["sub", "PublicNames", "table"])
        sub_table = await c.recv()
        collected["ids"] = {
            "get": sub_get[1],
            "range": sub_range[1],
            "table": sub_table[1],
            "alice": int(sub_get[2]["id"]),
        }

        writer = threading.Thread(
            target=_headless_writer, args=(config, steps, errors), daemon=True
        )
        writer.start()
        for i in range(3):
            steps[i].set()
            collected[f"step{i}"] = await _drain(c)
        writer.join(30)
        collected["writer_alive"] = writer.is_alive()

    server.test_client.websocket("/hetu/pytest_1", mimic=routine)

    assert not errors, errors
    assert collected["writer_alive"] is False
    ids = collected["ids"]
    alice = ids["alice"]

    # step0：update → 行 / 索引 / 整表三种订阅都收到新名字
    step0 = collected["step0"]
    for kind in ("get", "range", "table"):
        assert step0[ids[kind]][alice]["name"] == "Alice2", (kind, step0)

    # step1：insert（显式负数 id）→ 索引 / 整表收到新行，行订阅不受影响
    step1 = collected["step1"]
    for kind in ("range", "table"):
        assert step1[ids[kind]][-2] == {"id": -2, "owner": 2, "name": "Bob"}, (
            kind,
            step1,
        )
    assert ids["get"] not in step1

    # step2：delete → 索引 / 整表收到删除
    step2 = collected["step2"]
    for kind in ("range", "table"):
        assert step2[ids[kind]][-2] is None, (kind, step2)
