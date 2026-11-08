"""
提交序号列的库外消费方接口：`Table.servant_seq_head` / `servant_seq_head_many` 读计数器，
`Table.servant_seq_read` 增量读。设计稿见
docs/superpowers/specs/2026-11-09-commit-seq-third-party-design.md。
"""

import numpy as np
import pytest
from fixtures.backends import (
    raw_kv_set,
    raw_seq_counter_key,
    use_redis_family_backend_only,
)
from fixtures.testdata import create_ref

from hetu.common.snowflake_id import SnowflakeID
from hetu.data.backend import Backend, Table

SnowflakeID().init(1, 0)


def def_cmd():
    """命令表：seq 按 owner 分组，每个 owner 各自从 1 连续编号"""
    from hetu.data import BaseComponent, Permission, define_component, property_field

    @define_component(namespace="pytest", permission=Permission.USER, force=True)
    class Cmd(BaseComponent):
        owner: np.int64 = property_field(0, index=True)
        seq: np.int64 = property_field(0, commit_seq=True, groupby="owner")
        kind: np.int16 = property_field(0)

    return Cmd


def def_tick():
    """整张表一个序列"""
    from hetu.data import BaseComponent, define_component, property_field

    @define_component(namespace="pytest", force=True)
    class Tick(BaseComponent):
        seq: np.int64 = property_field(0, commit_seq=True)
        kind: np.int16 = property_field(0)

    return Tick


def make_table(backend: Backend, comp) -> Table:
    """建空表，返回能直接做非事务读的 `Table`"""
    create_ref(comp, backend)
    return Table(comp, "pytest", 1, backend)


@pytest.fixture
async def cmd(new_component_env, mod_auto_backend) -> Table:
    return make_table(mod_auto_backend(), def_cmd())


@pytest.fixture
async def tick(new_component_env, mod_auto_backend) -> Table:
    return make_table(mod_auto_backend(), def_tick())


async def insert(tbl: Table, *groups: dict, kind: int = 0) -> list[int]:
    """每个 group 插一行（一个事务），返回行 id。不分组的组件给空 dict"""
    comp = tbl.comp_cls
    rows = []
    async with tbl.backend.session("pytest", 1) as session:
        repo = session.using(comp)
        for group in groups:
            row = comp.new_row()
            for key, value in group.items():
                row[key] = value
            if "kind" in comp.dtype_map_:
                row.kind = kind
            await repo.insert(row)
            rows.append(row)
    await tbl.backend.wait_for_synced()
    return [int(row.id) for row in rows]


async def delete(tbl: Table, *row_ids: int) -> None:
    async with tbl.backend.session("pytest", 1) as session:
        repo = session.using(tbl.comp_cls)
        for row_id in row_ids:
            assert await repo.get(id=row_id) is not None
            repo.delete(row_id)
    await tbl.backend.wait_for_synced()


# ============ 读计数器 ============


async def test_seq_head_counts_issued_numbers(cmd: Table):
    """没发过号是 0；之后是这一组最近发出的号，各组互不影响，删了行也不回退"""
    assert await cmd.servant_seq_head(group={"owner": 7}) == 0
    ids = await insert(cmd, {"owner": 7}, {"owner": 7}, {"owner": 7})
    await insert(cmd, {"owner": 8}, {"owner": 8})
    await delete(cmd, ids[-1])
    assert await cmd.servant_seq_head(group={"owner": 7}) == 3
    assert await cmd.servant_seq_head(group={"owner": 8}) == 2
    heads = await cmd.servant_seq_head_many([{"owner": 8}, {"owner": 9}, {"owner": 7}])
    assert heads == [2, 0, 3]


async def test_seq_head_many_empty_skips_database(cmd: Table, monkeypatch):
    """空列表直接返回，不访问数据库"""

    async def boom(*_args):
        raise AssertionError("不该访问数据库")

    monkeypatch.setattr(type(cmd.backend.servant), "kv_get_many_", boom)
    assert await cmd.servant_seq_head_many([]) == []


async def test_seq_head_without_groupby(tick: Table):
    """不带 groupby 的组件：整张表一个计数器，不给 group"""
    assert await tick.servant_seq_head() == 0
    await insert(tick, {}, {})
    assert await tick.servant_seq_head() == 2
    assert await tick.servant_seq_head_many([None, None]) == [2, 2]


async def test_seq_head_rejects_bad_groups(cmd: Table, tick: Table, monkeypatch):
    """分组值少给、多给、给了不分组的组件：在访问数据库之前就抛 ValueError"""

    async def boom(*_args):
        raise AssertionError("不该访问数据库")

    monkeypatch.setattr(type(cmd.backend.servant), "kv_get_many_", boom)
    with pytest.raises(ValueError):
        await cmd.servant_seq_head()
    with pytest.raises(ValueError):
        await cmd.servant_seq_head(group={"owner": 7, "kind": 1})
    with pytest.raises(ValueError):
        await cmd.servant_seq_head_many([{"owner": 7}, None])
    with pytest.raises(ValueError):
        await tick.servant_seq_head(group={"kind": 1})


async def test_seq_head_requires_commit_seq(new_component_env, mod_auto_backend):
    """组件没有提交序号列：ValueError"""
    from hetu.data import BaseComponent, define_component, property_field

    @define_component(namespace="pytest", force=True)
    class Plain(BaseComponent):
        seq: np.int64 = property_field(0, index=True)

    tbl = make_table(mod_auto_backend(), Plain)
    with pytest.raises(ValueError, match="没有提交序号列"):
        await tbl.servant_seq_head()
    with pytest.raises(ValueError, match="没有提交序号列"):
        await tbl.servant_seq_head_many([None])


@pytest.mark.parametrize("raw", [b"abc", b"-1", b" 5", b"05", b"+5", b"5.0"])
async def test_seq_head_rejects_corrupted_counter(cmd: Table, raw: bytes):
    """计数器被写坏（不是 Redis 认的非负整数）：RuntimeError，报错里带着 key"""
    key = raw_seq_counter_key(cmd.backend, cmd, owner=7)
    raw_kv_set(cmd.backend, key, raw)
    with pytest.raises(RuntimeError, match="计数器") as err:
        await cmd.servant_seq_head(group={"owner": 7})
    assert key in str(err.value)


@use_redis_family_backend_only
async def test_seq_head_reads_wrong_type_as_zero(cmd: Table):
    """Redis 上计数器 key 被写成别的类型：MGET 对它返回 nil，读成 0（这时提交会报 WRONGTYPE）"""
    key = raw_seq_counter_key(cmd.backend, cmd, owner=7)
    io = cmd.backend.master.io  # type: ignore[attr-defined]
    io.delete(key)
    io.hset(key, "x", "1")
    assert await cmd.servant_seq_head(group={"owner": 7}) == 0


async def test_seq_head_many_in_chunks(cmd: Table, monkeypatch):
    """
    组多时分块：Redis 每块一条 MGET 放在一个 pipeline 里（redis_cluster 上 pipe.mget 被禁用，
    要用 execute_command），SQLite 按 IN 的参数上限分批
    """
    from hetu.data.backend.redis import RedisBackendClient
    from hetu.data.backend.sqlite import store

    monkeypatch.setattr(RedisBackendClient, "RANGE_PIPELINE_CHUNK", 2)
    monkeypatch.setattr(store, "IN_CHUNK", 2)
    for owner in range(1, 6):
        await insert(cmd, *[{"owner": owner}] * owner)
    groups = [{"owner": owner} for owner in range(1, 7)]
    assert await cmd.servant_seq_head_many(groups) == [1, 2, 3, 4, 5, 0]


async def test_seq_head_after_volatile_flush(new_component_env, mod_auto_backend):
    """易失表维护清空之后计数器为 0"""
    from hetu.data import BaseComponent, define_component, property_field

    @define_component(namespace="pytest", volatile=True, force=True)
    class Pulse(BaseComponent):
        seq: np.int64 = property_field(0, commit_seq=True)

    tbl = make_table(mod_auto_backend(), Pulse)
    await insert(tbl, {}, {})
    assert await tbl.servant_seq_head() == 2
    tbl.backend.get_table_maintenance().flush(tbl)
    assert await tbl.servant_seq_head() == 0


async def test_seq_head_string_groups(new_component_env, mod_auto_backend):
    """
    字符串和 bytes 分组列、组前缀超过 32 字节、值带结尾 \\x00：读计数器算出的 key 和提交时用的
    是同一个
    """
    from hetu.data import BaseComponent, define_component, property_field

    @define_component(namespace="pytest", force=True)
    class Chat(BaseComponent):
        room: str = property_field("", dtype="U40")
        tag: bytes = property_field(b"", dtype="S8")
        seq: np.int64 = property_field(0, commit_seq=True, groupby=("room", "tag"))
        kind: np.int16 = property_field(0)

    tbl = make_table(mod_auto_backend(), Chat)
    long_room = "房" * 40  # 组前缀远超 32 字节
    await insert(tbl, *[{"room": long_room, "tag": b"ab"}] * 3)
    await insert(tbl, {"room": "a", "tag": b"x\x00"}, {"room": "a", "tag": b"x\x00"})
    heads = await tbl.servant_seq_head_many(
        [
            {"room": long_room, "tag": b"ab"},
            {"room": "a", "tag": b"x\x00"},
            {"room": "a", "tag": b"x"},
            {"room": "b", "tag": b"ab"},
        ]
    )
    assert heads == [3, 2, 2, 0]
