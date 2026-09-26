import numpy as np
import pytest
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
