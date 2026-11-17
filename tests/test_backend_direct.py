import numpy as np
import pytest
import redis.exceptions
from fixtures.backends import raw_hset, use_redis_family_backend_only
from fixtures.testdata import create_ref

from hetu.common.snowflake_id import SnowflakeID
from hetu.data.backend import Backend, RowFormat

SnowflakeID().init(1, 0)


async def test_table_direct_set(filled_rls_ref, mod_auto_backend):
    backend: Backend = mod_auto_backend()
    # 测试direct set
    async with backend.session(
        filled_rls_ref.instance_name, filled_rls_ref.cluster_id
    ) as session:
        repo = session.using(filled_rls_ref.comp_cls)
        row = await repo.get(owner=10)
        assert row

    assert row.friend == 11

    assert await backend.master.direct_set(filled_rls_ref, row.id, friend="9") is True

    async with backend.session(
        filled_rls_ref.instance_name, filled_rls_ref.cluster_id
    ) as session:
        repo = session.using(filled_rls_ref.comp_cls)
        row = await repo.get(owner=10)
        assert row

    assert row.friend == 9

    # 测试写入不存在的行
    with pytest.raises(ValueError, match="aaa"):
        await backend.master.direct_set(filled_rls_ref, row.id, aaa="11")


def _def_beat():
    from hetu.data import BaseComponent, define_component, property_field

    @define_component(namespace="pytest", volatile=True, force=True)
    class Beat(BaseComponent):
        last_active: np.int64 = property_field(0)
        note: np.int32 = property_field(0)

    return Beat


@pytest.fixture
async def beat_ref(new_component_env, mod_auto_backend):
    return create_ref(_def_beat(), mod_auto_backend())


async def _assert_only_existing_rows(backend: Backend, ref) -> None:
    """direct_set 只改已存在的行里已有的字段（同 HSETEX FXX），返回写没写"""
    comp = ref.comp_cls
    row = comp.new_row()
    async with backend.session("pytest", 1) as session:
        await session.using(comp).insert(row)
    row_id = int(row.id)

    # 已有的行：写入，返回 True
    assert await backend.master.direct_set(ref, row_id, last_active="5") is True
    got = await backend.master.get(ref, row_id)
    assert got is not None and got.last_active == 5

    # 行不存在：什么都不写，返回 False。连接行被删后晚到的心跳就是这样，以前会建出一个
    # 只有 last_active、缺 id 的残缺行
    missing = row_id + 1
    assert await backend.master.direct_set(ref, missing, last_active="6") is False
    assert await backend.master.get(ref, missing, RowFormat.RAW) is None

    # 行在、要写的字段不在（残缺行）：也不写
    partial = row_id + 2
    raw_hset(backend, ref, partial, last_active="1")
    assert await backend.master.direct_set(ref, partial, note="7") is False
    assert await backend.master.get(ref, partial, RowFormat.RAW) == {"last_active": "1"}
    assert await backend.master.direct_set(ref, partial, last_active="8") is True

    # 一个字段都不给是错误
    with pytest.raises(ValueError):
        await backend.master.direct_set(ref, row_id)


async def test_direct_set_only_existing_rows(beat_ref, mod_auto_backend):
    await _assert_only_existing_rows(mod_auto_backend(), beat_ref)


@use_redis_family_backend_only
async def test_direct_set_lua_fallback(beat_ref, mod_auto_backend, monkeypatch):
    """服务端不支持 HSETEX（Redis 7、Valkey 8、部分代理层）时回退到 Lua，语义相同；
    启动时的探测不留下任何 key"""
    from hetu.data.backend.redis import RedisBackendClient

    backend = mod_auto_backend()
    client = backend.master
    assert isinstance(client, RedisBackendClient)
    assert client.direct_set_script is None  # 测试用的 Redis 8 / Valkey 9 都支持 HSETEX
    assert not client.io.exists(RedisBackendClient.HSETEX_PROBE_KEY)

    monkeypatch.setattr(RedisBackendClient, "probe_hsetex_", lambda self: False)
    client.configure_master()
    try:
        assert client.direct_set_script is not None
        await _assert_only_existing_rows(backend, beat_ref)
    finally:
        client.direct_set_script = None


class _ProbeIO:
    """probe_hsetex_ 用的假同步连接：HSETEX 依次按 outcomes 抛错或成功，PING 按 ping_ok"""

    def __init__(self, *outcomes, ping_ok=True):
        self.outcomes = list(outcomes)
        self.ping_ok = ping_ok
        self.hsetex_calls = 0

    def execute_command(self, *args):
        assert args[0] == "HSETEX"
        self.hsetex_calls += 1
        outcome = self.outcomes.pop(0) if self.outcomes else None
        if isinstance(outcome, Exception):
            raise outcome
        return 0

    def ping(self):
        if not self.ping_ok:
            raise redis.exceptions.ConnectionError("Connection refused")
        return True


def _probe(io) -> bool:
    from hetu.data.backend.redis import RedisBackendClient

    client = object.__new__(RedisBackendClient)
    client._ios = [io]
    return client.probe_hsetex_()


def test_probe_hsetex_retries_transient_errors():
    """启动探测时网络抖一下不能让这个进程一直用 Lua：连接是好的（PING 通）就再试"""
    io = _ProbeIO(redis.exceptions.ConnectionError("Connection reset by peer"))
    assert _probe(io) is True
    io = _ProbeIO(redis.exceptions.TimeoutError("Timeout reading from socket"))
    assert _probe(io) is True


def test_probe_hsetex_falls_back_when_unsupported():
    """服务端回错（unknown command）、集群客户端的命令表里没有：回退 Lua。连接好好的、
    HSETEX 却每次都被断开（有的代理层遇到不认识的命令直接断连接）：也回退"""
    assert _probe(_ProbeIO(redis.exceptions.ResponseError("unknown command"))) is False
    missing = redis.exceptions.RedisError(
        "HSETEX command doesn't exist in Redis commands"
    )
    assert _probe(_ProbeIO(missing)) is False
    dropped = _ProbeIO(*[redis.exceptions.ConnectionError("closed")] * 10)
    assert _probe(dropped) is False
    assert dropped.hsetex_calls > 1


def test_probe_hsetex_raises_when_master_unreachable():
    """连 PING 都不通：照常抛出让启动失败，而不是悄悄降级到 Lua"""
    io = _ProbeIO(*[redis.exceptions.ConnectionError("refused")] * 10, ping_ok=False)
    with pytest.raises(redis.exceptions.ConnectionError):
        _probe(io)


@use_redis_family_backend_only
async def test_is_lag_free_with_replica(mod_backend_config):
    """测试夹具是一主一从（集群是多主）：经副本读、直连挂着副本的 master 都按可能落后
    处理（读会不会被代理层分到副本拿不准）。没有副本的单机 master 那一侧见下面的假连接
    用例"""
    from hetu.data.backend import Backend

    replicated = Backend(mod_backend_config)
    direct = Backend({**mod_backend_config, "servants": []})
    try:
        assert await replicated.servant.is_lag_free() is False
        assert await direct.servant.is_lag_free() is False
    finally:
        await replicated.close()
        await direct.close()


class _InfoAIO:
    """is_lag_free 用的假异步连接：INFO replication 回 info 或抛 error，记调用次数"""

    def __init__(self, info=None, error=None):
        self.reply = info
        self.error = error
        self.calls = 0

    async def info(self, section):
        assert section == "replication"
        self.calls += 1
        if self.error is not None:
            raise self.error
        return self.reply


def _lag_client(*aios, raw_clustering=False):
    from hetu.data.backend.redis import RedisBackendClient

    client = object.__new__(RedisBackendClient)
    client._async_ios = list(aios)
    client.raw_clustering = raw_clustering
    client.loop_id = 0
    return client


async def test_is_lag_free_only_for_replica_less_master():
    """只有 role:master 且 connected_slaves:0 才算不会落后，结论只查一次"""
    master = _InfoAIO({"role": "master", "connected_slaves": 0})
    client = _lag_client(master)
    assert await client.is_lag_free() is True
    assert await client.is_lag_free() is True
    assert master.calls == 1

    assert await _lag_client(_InfoAIO({"role": "slave"})).is_lag_free() is False
    with_replica = _InfoAIO({"role": "master", "connected_slaves": 1})
    assert await _lag_client(with_replica).is_lag_free() is False
    # 字段缺失（某些代理层的 INFO）：拿不准，按可能落后处理
    assert await _lag_client(_InfoAIO({"role": "master"})).is_lag_free() is False


async def test_is_lag_free_unknown_topology():
    """集群模式、一个客户端挂了几个地址：不查，按可能落后处理。代理层不认 INFO：记下
    False；网络抖动：这次回 False、下次再查"""
    master = _InfoAIO({"role": "master", "connected_slaves": 0})
    assert await _lag_client(master, raw_clustering=True).is_lag_free() is False
    assert await _lag_client(master, master).is_lag_free() is False
    assert master.calls == 0

    unsupported = _InfoAIO(error=redis.exceptions.ResponseError("unknown command"))
    client = _lag_client(unsupported)
    assert await client.is_lag_free() is False
    assert await client.is_lag_free() is False
    assert unsupported.calls == 1

    flaky = _InfoAIO(error=redis.exceptions.ConnectionError("reset by peer"))
    client = _lag_client(flaky)
    assert await client.is_lag_free() is False
    flaky.error = None
    flaky.reply = {"role": "master", "connected_slaves": 0}
    assert await client.is_lag_free() is True
    assert flaky.calls == 2
