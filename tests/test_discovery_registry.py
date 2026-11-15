"""
节点发现的注册表（hetu/discovery/registry.py，设计稿 docs/superpowers/specs/2026-11-15-node-discovery-design.md
§4）：登记的格式、hetu nodes 的汇总、节点这边的登记循环，以及真开服时什么时候登记、注销、删掉。

登记循环的用例用假的 Redis 客户端和假的租约；真开服的用例要 Docker（Redis 一主一从）。
"""

import asyncio
import json
import os
import subprocess
import sys
import time
from typing import Any

import pytest
import redis.asyncio
from websockets.legacy.client import connect

from hetu.discovery import registry
from hetu.discovery.netcheck import ADVERTISED_ADDRESS_CONFIG_KEY
from hetu.discovery.registry import (
    REGISTRY_KEY,
    STATE_DRAINING,
    STATE_READY,
    EntryError,
    NodeEntry,
    NodeRegistrar,
    decode_entry,
    decode_registry,
    encode_entry,
    summarize,
)
from hetu.endpoint.definer import EndpointDefines
from hetu.safelogging.default import DEFAULT_LOGGING_CONFIG
from hetu.server import runtime as server_runtime
from hetu.server.adapters import aiohttp_app
from hetu.server.pipeline import JSONBinaryLayer, MessagePipeline
from hetu.server.runtime import StartupAborted
from hetu.server.supervisor import worker_stop_timeout
from hetu.system import SystemClusters


def make_entry(**overrides: Any) -> NodeEntry:
    fields: dict[str, Any] = {
        "node": "m1:0123456789abcdef:w0:1",
        "ns": "game",
        "inst": ["s1"],
        "addr": "10.0.3.17",
        "port": 2466,
        "state": STATE_READY,
        "conns": 5,
        "ts": 1000.0,
        "up": 900.0,
        "ver": "1.4.0",
    }
    fields.update(overrides)
    return NodeEntry(**fields)


# ---------------------------------------------------------------- 格式


def test_entry_round_trip():
    entry = make_entry()
    raw = encode_entry(entry)
    assert decode_entry(raw) == entry
    assert json.loads(raw)["v"] == 1


@pytest.mark.parametrize(
    "raw,words",
    [
        (b'{"v": 2, "addr": "10.0.3.17"}', "格式版本 2"),
        (b"not json", None),
        (b'{"v": 1, "addr": "10.0.3.17"}', None),  # 少了字段
        (b'{"node": "x"}', None),  # 没有版本
        # 嵌套很深（哪怕在不认识的字段里）：msgspec 抛的是 RecursionError
        (b'{"v": 1, "x": ' + b"[" * 100_000 + b"]" * 100_000 + b"}", None),
    ],
)
def test_bad_entries_are_rejected(raw, words):
    with pytest.raises(EntryError, match=words):
        decode_entry(raw)


def test_decode_registry_keeps_the_good_entries():
    entry = make_entry()
    raw = {b"3": encode_entry(entry), b"x": encode_entry(entry), b"4": b"garbage"}
    entries, invalid = decode_registry(raw)
    assert entries == {3: entry}
    assert set(invalid) == {"x", "4"}


def test_summarize_groups_live_entries_by_address():
    now = 1000.0
    entries = {
        0: make_entry(conns=10),
        1: make_entry(conns=20, state=STATE_DRAINING, ts=now - 4, ver="1.5.0"),
        2: make_entry(addr="10.0.3.18", conns=7),
        3: make_entry(addr="10.0.3.19", ts=now - 60),  # 一分钟没写了
        4: make_entry(ns="other", addr="10.0.9.9"),
    }
    out = summarize(entries, {"x": "坏了"}, now)
    nodes = {(n["ns"], n["addr"]): n for n in out["nodes"]}
    assert set(nodes) == {
        ("game", "10.0.3.17"),
        ("game", "10.0.3.18"),
        ("other", "10.0.9.9"),
    }
    first = nodes["game", "10.0.3.17"]
    assert first["workers"] == 2
    assert first["states"] == {STATE_READY: 1, STATE_DRAINING: 1}
    assert first["conns"] == 30
    assert first["max_age"] == 4
    assert first["versions"] == ["1.4.0", "1.5.0"]
    assert first["worker_ids"] == [0, 1]
    assert [s["worker_id"] for s in out["stale"]] == [3]
    assert out["stale"][0]["age"] == 60
    assert out["invalid"] == [{"field": "x", "error": "坏了"}]
    assert out["namespaces"] == ["game", "other"]


def test_summarize_counts_any_state_without_clobbering_other_fields():
    """状态名是登记里写的：叫 versions、conns 之类的也只算进 states，不会把汇总弄崩、算错"""
    now = 1000.0
    entries = {0: make_entry(state="versions"), 1: make_entry(state="conns", conns=3)}
    (node,) = summarize(entries, {}, now)["nodes"]
    assert node["states"] == {"versions": 1, "conns": 1}
    assert node["conns"] == 8
    assert node["versions"] == ["1.4.0"]


def test_worker_stop_timeout_covers_the_deregister_wait():
    config = {"SHUTDOWN_DRAIN_TIMEOUT": 5, "GRACEFUL_SHUTDOWN_TIMEOUT": 15}
    assert worker_stop_timeout({**config, "DISCOVERY": True}) == (
        worker_stop_timeout(config) + registry.WRITE_TIMEOUT + registry.DEREGISTER_WAIT
    )


# ---------------------------------------------------------------- 登记循环


class FakeRedis:
    """记下 HSET / HDEL 落地的顺序；first_hset_gate 设了的话，第一条 HSET 要等它才落地"""

    def __init__(self) -> None:
        self.hash: dict[str, bytes] = {}
        self.landed: list[tuple[str, str]] = []
        self.calls = 0
        self.fail = False
        self.hang = False
        self.first_hset_gate: asyncio.Event | None = None

    async def hset(self, key: str, field: str, value: bytes) -> None:
        assert key == REGISTRY_KEY
        self.calls += 1
        if self.hang:
            await asyncio.Event().wait()
        if self.fail:
            raise ConnectionError("redis down")
        if self.calls == 1 and self.first_hset_gate is not None:
            await self.first_hset_gate.wait()
        self.hash[field] = value
        self.landed.append(("hset", decode_entry(value).state))

    async def hdel(self, key: str, field: str) -> None:
        assert key == REGISTRY_KEY
        self.hash.pop(field, None)
        self.landed.append(("hdel", field))


class FakeKeeper:
    def __init__(self) -> None:
        self.lease_deadline: float | None = time.monotonic() + 60

    def close_fence(self) -> None:
        self.lease_deadline = float("-inf")


def make_registrar(client: FakeRedis, keeper: FakeKeeper) -> NodeRegistrar:
    return NodeRegistrar(
        lambda: client,
        keeper,  # pyright: ignore[reportArgumentType]
        3,
        node="m1:0123456789abcdef:w0:1",
        namespace="game",
        instances=["s1"],
        addr="10.0.3.17",
        port=2466,
        version="1.4.0",
        conns=lambda: 7,
    )


async def until(condition, timeout: float = 5.0) -> None:
    async with asyncio.timeout(timeout):
        while not condition():
            await asyncio.sleep(0.01)


async def test_registrar_lifecycle():
    """开好服写 ready；关服第一步改成 draining；释放租约之前删掉，之后不再写"""
    client, keeper = FakeRedis(), FakeKeeper()
    registrar = make_registrar(client, keeper)
    registrar.start()
    await until(lambda: "3" in client.hash)
    entry = decode_entry(client.hash["3"])
    assert (entry.state, entry.addr, entry.port, entry.conns) == (
        STATE_READY,
        "10.0.3.17",
        2466,
        7,
    )
    assert (entry.ns, entry.inst, entry.ver) == ("game", ["s1"], "1.4.0")
    assert await registrar.mark_draining() is True
    assert decode_entry(client.hash["3"]).state == STATE_DRAINING
    await registrar.unregister()
    assert client.hash == {}
    assert client.landed[-1] == ("hdel", "3")
    await registrar.unregister()  # 可以重复调用
    assert client.landed.count(("hdel", "3")) == 1


async def test_registrar_does_nothing_once_the_fence_is_closed():
    """
    续约发现租约被接手（围栏关了）：这个 worker id 可能已经是别人的了，不写、不删，也不让调用方做注销等待
    """
    client, keeper = FakeRedis(), FakeKeeper()
    keeper.close_fence()
    registrar = make_registrar(client, keeper)
    registrar.start()
    await asyncio.sleep(0.05)
    assert client.calls == 0
    assert await registrar.mark_draining() is False
    await registrar.unregister()
    assert client.landed == []


async def test_registrar_stops_writing_after_the_safe_period():
    """续约一直失败、过了安全期：同样不写"""
    client, keeper = FakeRedis(), FakeKeeper()
    keeper.lease_deadline = time.monotonic() - 1
    assert await make_registrar(client, keeper).write() is False
    keeper.lease_deadline = None
    assert await make_registrar(client, keeper).write() is False
    assert client.calls == 0


async def test_draining_lands_after_an_in_flight_ready():
    """
    关服时循环刚好在写 ready：draining 要排在它后面落地，不能被这条晚到的 ready 盖掉（网关会以为它还能用）
    """
    client, keeper = FakeRedis(), FakeKeeper()
    client.first_hset_gate = asyncio.Event()
    registrar = make_registrar(client, keeper)
    registrar.start()
    await until(lambda: client.calls == 1)
    draining = asyncio.create_task(registrar.mark_draining())
    await asyncio.sleep(0.05)
    client.first_hset_gate.set()
    assert await draining is True
    assert client.landed == [("hset", STATE_READY), ("hset", STATE_DRAINING)]
    await registrar.unregister()


async def test_shutdown_steps_give_up_when_redis_hangs(monkeypatch):
    """Redis 卡住：改 draining、删登记各自限时，不挡关服；循环照样停掉"""
    monkeypatch.setattr(registry, "WRITE_TIMEOUT", 0.2)
    client, keeper = FakeRedis(), FakeKeeper()
    client.hang = True
    registrar = make_registrar(client, keeper)
    registrar.start()
    await until(lambda: client.calls == 1)
    started = time.monotonic()
    assert await registrar.mark_draining() is False
    await registrar.unregister()
    assert time.monotonic() - started < 2
    assert registrar._task is None


async def test_each_write_is_time_limited(monkeypatch):
    """
    登记循环的每次写都限时：看过围栏之后要尽快落地，围栏的安全期离别的进程能接手只差 5 秒。Redis 卡住时这一次
    放弃、下个周期再写，不会一直挂着
    """
    monkeypatch.setattr(registry, "WRITE_TIMEOUT", 0.05)
    monkeypatch.setattr(registry, "WRITE_INTERVAL", 0.02)
    client, keeper = FakeRedis(), FakeKeeper()
    client.hang = True
    registrar = make_registrar(client, keeper)
    registrar.start()
    try:
        await until(lambda: client.calls >= 3)
    finally:
        await registrar.stop()


async def test_write_failures_are_reported_once(monkeypatch, caplog):
    monkeypatch.setattr(registry, "WRITE_INTERVAL", 0.02)
    client, keeper = FakeRedis(), FakeKeeper()
    client.fail = True
    registrar = make_registrar(client, keeper)
    with caplog.at_level("INFO", logger="HeTu.root"):
        logger = registry.logger
        propagate, logger.propagate = logger.propagate, True
        try:
            registrar.start()
            await until(lambda: client.calls >= 3)
            client.fail = False
            await until(lambda: "3" in client.hash)
            await registrar.unregister()
        finally:
            logger.propagate = propagate
    messages = [r.getMessage() for r in caplog.records]
    assert sum("写注册表失败" in m for m in messages) == 1, messages
    assert any("写注册表恢复正常" in m for m in messages), messages


# ---------------------------------------------------------------- 真开服

NAMESPACE = "disc_test"
INSTANCE = "disc_1"
APP = """
import hetu
import numpy as np


@hetu.define_component(namespace="{ns}", force=True)
class Dummy(hetu.BaseComponent):
    owner: np.int64 = hetu.property_field(0, unique=True)


@hetu.define_system(namespace="{ns}", components=(Dummy,), force=True)
async def noop(ctx):
    pass
"""
# 测试里注销之后只等这么久（默认 4 秒）
TEST_DEREGISTER_WAIT = 2.0


@pytest.fixture
def clean_registry():
    """app 文件里的簇、Endpoint 定义是进程级的，用例前后都清掉，免得漏到别的测试文件"""
    SystemClusters()._clear()
    EndpointDefines()._clear()
    yield
    SystemClusters()._clear()
    EndpointDefines()._clear()


@pytest.fixture
def redis_urls(ses_redis_service):
    """这个模块独占 13 号库，用例前清空"""
    import redis as sync_redis

    master, replica = (url.rsplit("/", 1)[0] + "/13" for url in ses_redis_service)
    io = sync_redis.Redis.from_url(master)
    io.flushdb()
    io.close()
    return master, replica


def server_config(tmp_path, redis_urls: tuple[str, str], **overrides: Any) -> Any:
    app_file = tmp_path / "app.py"
    app_file.write_text(APP.format(ns=NAMESPACE), "utf-8")
    master, replica = redis_urls
    config = {
        "APP_FILE": str(app_file),
        "NAMESPACE": NAMESPACE,
        "INSTANCES": [INSTANCE],
        "LISTEN": "127.0.0.1:0",
        "PACKET_LAYERS": [{"type": "jsonb"}],
        "BACKENDS": {
            "main": {"type": "redis", "master": master, "servants": [replica]}
        },
        "LOGGING": DEFAULT_LOGGING_CONFIG,
        "DEBUG": False,
        "WORKER_NUM": 1,
        "DISCOVERY": True,
        # 开服检查（hetu start 的主进程做）算出来的广播地址，这里直接给
        ADVERTISED_ADDRESS_CONFIG_KEY: "10.9.8.7:2466",
    }
    config.update(overrides)
    return aiohttp_app.prepare(config)


def run_server(config: Any, scenario) -> None:
    """
    新建事件循环（同 hetu start），bind 好监听 socket，跑 scenario(server, sock, io)：开服、关服由它自己来，
    它返回或者出错之后再兜底关一次服
    """

    async def main() -> None:
        server = aiohttp_app.AiohttpServer(config)
        sock = aiohttp_app.bind_socket("127.0.0.1:0")
        io = redis.asyncio.Redis.from_url(config["BACKENDS"]["main"]["master"])
        try:
            await scenario(server, sock, io)
        finally:
            await server.stop()
            sock.close()
            await io.aclose()

    with asyncio.Runner(loop_factory=aiohttp_app.new_event_loop) as runner:
        runner.run(main())


async def entry_of(io: Any, worker_id: int) -> NodeEntry | None:
    raw = await io.hget(REGISTRY_KEY, str(worker_id))
    return None if raw is None else decode_entry(raw)


async def wait_entry(
    io: Any, worker_id: int, condition, *, refresh=None, timeout: float = 10.0
) -> NodeEntry:
    """等 worker_id 的登记满足 condition，返回它。给了 refresh 的话每次读之前先调它（让节点马上写一次）"""
    async with asyncio.timeout(timeout):
        while True:
            if refresh is not None:
                await refresh()
            entry = await entry_of(io, worker_id)
            if entry is not None and condition(entry):
                return entry
            await asyncio.sleep(0.02)


async def connect_and_handshake(port: int) -> Any:
    pipe = MessagePipeline()
    pipe.add_layer(JSONBinaryLayer())
    ws = await connect(f"ws://127.0.0.1:{port}/hetu/{INSTANCE}")
    await ws.send(pipe.encode(None, []))
    await ws.recv()
    return ws


@pytest.mark.timeout(60)
def test_server_registers_and_withdraws_before_it_stops_listening(
    tmp_path, redis_urls, clean_registry, monkeypatch
):
    """
    开好服登记 ready（地址、namespace、实例、持有者标识、在线连接数）；关服时先改成 draining、照常服务一会儿
    （这期间还在监听，新连接照常接），之后才停止监听；释放租约之前删掉登记
    """
    import aiohttp

    monkeypatch.setattr(server_runtime, "DEREGISTER_WAIT", TEST_DEREGISTER_WAIT)
    config = server_config(tmp_path, redis_urls)

    async def scenario(server, sock, io):
        port = sock.getsockname()[1]
        await server.start(sock)
        lease = server.runtime.snowflake_lease
        worker_id = lease.worker_id
        entry = await wait_entry(io, worker_id, lambda e: True)
        assert entry.state == STATE_READY
        assert (entry.addr, entry.port) == ("10.9.8.7", 2466)
        assert (entry.ns, entry.inst) == (NAMESPACE, [INSTANCE])
        assert entry.node == lease.keeper.identity
        assert entry.conns == 0

        ws = await connect_and_handshake(port)
        registrar = server.runtime.registrar
        await wait_entry(io, worker_id, lambda e: e.conns == 1, refresh=registrar.write)

        stopping = asyncio.create_task(server.stop())
        await wait_entry(io, worker_id, lambda e: e.state == STATE_DRAINING)
        seen_draining = time.monotonic()
        # 注销等待期间还在监听，新连接照常服务
        async with (
            aiohttp.ClientSession() as http,
            http.get(f"http://127.0.0.1:{port}/") as response,
        ):
            assert response.status == 200
        assert not stopping.done()
        await stopping
        assert time.monotonic() - seen_draining >= TEST_DEREGISTER_WAIT - 0.5
        assert await entry_of(io, worker_id) is None
        await ws.close()

    run_server(config, scenario)


@pytest.mark.timeout(60)
def test_lost_lease_leaves_the_registry_alone(
    tmp_path, redis_urls, clean_registry, monkeypatch
):
    """
    续约发现租约被接手（发号围栏关了）之后，这个 worker id 可能已经是别人的：不再写，关服时不改 draining、
    不等、不删（留下的登记 15 秒后被读的一方判为过期）
    """
    monkeypatch.setattr(server_runtime, "DEREGISTER_WAIT", 30.0)
    config = server_config(tmp_path, redis_urls)

    async def scenario(server, sock, io):
        await server.start(sock)
        lease = server.runtime.snowflake_lease
        worker_id = lease.worker_id
        await wait_entry(io, worker_id, lambda e: True)
        # 先停续约：不然下一次续约成功又会把围栏打开
        await lease.stop_loops()
        lease.keeper.close_fence()
        assert await server.runtime.registrar.write() is False
        started = time.monotonic()
        await server.stop()
        assert time.monotonic() - started < 10
        entry = await entry_of(io, worker_id)
        assert entry is not None
        assert entry.state == STATE_READY

    run_server(config, scenario)


@pytest.mark.timeout(60)
def test_embedded_start_runs_the_checks_itself(tmp_path, redis_urls, clean_registry):
    """不是经 hetu start 开的服（没有传进来的广播地址）：worker 自己做开服检查，不过就中止开服、不登记"""
    config = server_config(
        tmp_path, redis_urls, **{ADVERTISED_ADDRESS_CONFIG_KEY: None}
    )

    async def scenario(server, sock, io):
        # PROXIES_COUNT 默认是 0，过不了检查
        with pytest.raises(StartupAborted, match="PROXIES_COUNT"):
            await server.start(sock)
        assert await io.hlen(REGISTRY_KEY) == 0

    run_server(config, scenario)


# ---------------------------------------------------------------- hetu nodes


def run_nodes(*args: str, cwd) -> tuple[int, dict]:
    env = {k: v for k, v in os.environ.items() if k != "HETU_CONFIG"}
    proc = subprocess.run(
        [sys.executable, "-m", "hetu", "nodes", *args],
        cwd=cwd,
        capture_output=True,
        timeout=60,
        env=env,
        check=False,
    )
    lines = proc.stdout.decode("utf-8").splitlines()
    assert len(lines) == 1, (proc.stdout, proc.stderr.decode("utf-8", "replace"))
    return proc.returncode, json.loads(lines[0])


@pytest.mark.timeout(60)
def test_nodes_command_summarizes_the_registry(tmp_path, redis_urls):
    import redis as sync_redis

    master, _replica = redis_urls
    now = time.time()
    io = sync_redis.Redis.from_url(master)
    io.hset(
        REGISTRY_KEY,
        mapping={
            "0": encode_entry(make_entry(ts=now, conns=3)),
            "1": encode_entry(make_entry(ts=now, conns=4, state=STATE_DRAINING)),
            "2": encode_entry(make_entry(addr="10.0.3.18", ts=now - 100)),
            "x": b"garbage",
        },
    )
    io.close()
    code, out = run_nodes("--db", master, "--master", cwd=tmp_path)
    assert code == 0, out
    assert out["ok"] is True
    assert out["read_from"] == "master"
    (node,) = out["nodes"]
    assert (node["addr"], node["port"], node["workers"]) == ("10.0.3.17", 2466, 2)
    assert node["states"] == {STATE_READY: 1, STATE_DRAINING: 1}
    assert node["conns"] == 7
    assert [s["worker_id"] for s in out["stale"]] == [2]
    assert [i["field"] for i in out["invalid"]] == ["x"]
    assert out["namespaces"] == ["game"]


@pytest.mark.timeout(60)
def test_nodes_command_does_not_leak_the_password(tmp_path):
    """连不上 Redis 时以 3 退出，报错里的地址打码：排查命令的输出常被贴到工单、群里"""
    code, out = run_nodes(
        "--db", "redis://hetu:supersecret@127.0.0.1:1/0", cwd=tmp_path
    )
    assert code == 3, out
    assert "supersecret" not in json.dumps(out, ensure_ascii=False)
    assert "hetu:***@127.0.0.1:1" in out["error"]


def test_nodes_command_needs_redis(tmp_path):
    code, out = run_nodes("--db", f"sqlite:///{tmp_path / 'x.db'}", cwd=tmp_path)
    assert code == 2
    assert "Redis" in out["error"]
