"""
提交序号列的库外消费方接口：`Table.servant_seq_head` / `servant_seq_head_many` 读计数器，
`Table.servant_seq_read` 增量读。设计稿见
docs/superpowers/specs/2026-11-09-commit-seq-third-party-design.md。
"""

import pickle
from typing import Any, cast

import numpy as np
import pytest
from fixtures.backends import (
    raw_kv_set,
    raw_seq_counter_key,
    raw_zadd,
    use_redis_family_backend_only,
)
from fixtures.testdata import create_ref

from hetu.common.snowflake_id import SnowflakeID
from hetu.data.backend import Backend, CommitSeqDiverged, RowFormat, Table
from hetu.data.backend.base import index_member_, index_value_bytes_

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


# ============ 增量读 ============


def seqs(rows) -> list[int]:
    return [int(seq) for seq in rows.seq]


class _TamperedReader:
    """包住 `pinned_reader_` 给出的读原语：取行时让指定的行读不到，或者换掉它的字段"""

    def __init__(self, reader, drop: set[int], patch: dict[int, dict[bytes, bytes]]):
        self._reader = reader
        self._drop = drop
        self._patch = patch

    def zrange_bylex_(self, *args):
        return self._reader.zrange_bylex_(*args)

    async def hgetall_many_(self, table_ref, row_ids):
        rows = await self._reader.hgetall_many_(table_ref, row_ids)
        out = []
        for row_id, row in zip(row_ids, rows, strict=True):
            if row_id in self._drop:
                out.append({})
            elif row_id in self._patch:
                out.append({**row, **self._patch[row_id]})
            else:
                out.append(row)
        return out


def tamper(monkeypatch, tbl: Table, drop=(), patch=None) -> None:
    """模拟取行落到落后的副本（drop：读不到）或者读到了不一致的行（patch：换掉字段）"""
    client_cls = cast(Any, type(tbl.backend.servant))
    original = client_cls.pinned_reader_

    async def pinned(self, key):
        return _TamperedReader(await original(self, key), set(drop), patch or {})

    monkeypatch.setattr(client_cls, "pinned_reader_", pinned)


async def test_seq_read_basics(cmd: Table):
    """从 0 读起、按号升序、只读本组；limit；从游标之后读；读到头返回空、没被挡住"""
    await insert(cmd, *[{"owner": 7}] * 3)
    await insert(cmd, *[{"owner": 8}] * 2)
    rows, blocked = await cmd.servant_seq_read(0, group={"owner": 7})
    assert seqs(rows) == [1, 2, 3] and set(rows.owner) == {7} and blocked is None
    assert seqs((await cmd.servant_seq_read(0, group={"owner": 8})).rows) == [1, 2]
    assert seqs((await cmd.servant_seq_read(0, 2, group={"owner": 7})).rows) == [1, 2]
    assert seqs((await cmd.servant_seq_read(2, group={"owner": 7})).rows) == [3]
    rows, blocked = await cmd.servant_seq_read(3, group={"owner": 7})
    assert len(rows) == 0 and blocked is None
    rows, blocked = await cmd.servant_seq_read(0, group={"owner": 9})
    assert len(rows) == 0 and blocked is None


async def test_seq_read_without_groupby(tick: Table):
    await insert(tick, {}, {}, {})
    assert seqs((await tick.servant_seq_read(1)).rows) == [2, 3]


async def test_seq_read_skips_deleted_rows(cmd: Table):
    """
    中间删掉的号：快照里有更大的号，确认不存在，跳过。尾部删掉的号：没有更大的号，什么都确认
    不了，返回空；之后来了新行，一起跳过
    """
    ids = await insert(cmd, *[{"owner": 7}] * 5)
    await delete(cmd, ids[1], ids[3])
    assert seqs((await cmd.servant_seq_read(0, group={"owner": 7})).rows) == [1, 3, 5]
    await delete(cmd, ids[4])
    rows, blocked = await cmd.servant_seq_read(3, group={"owner": 7})
    assert len(rows) == 0 and blocked is None
    await insert(cmd, {"owner": 7})
    assert seqs((await cmd.servant_seq_read(3, group={"owner": 7})).rows) == [6]


@pytest.mark.parametrize("position", [0, 2, 4])
async def test_seq_read_stops_at_unreadable_row(cmd: Table, monkeypatch, position):
    """
    取行读不到（取行落到落后的副本、读完索引之后被删了）：不跳过，停在它前面，记为 blocked。
    恢复之后能读到它
    """
    ids = await insert(cmd, *[{"owner": 7}] * 5)
    tamper(monkeypatch, cmd, drop=[ids[position]])
    rows, blocked = await cmd.servant_seq_read(0, group={"owner": 7})
    assert seqs(rows) == list(range(1, position + 1))
    assert blocked == (position + 1, ids[position])
    monkeypatch.undo()
    rows, blocked = await cmd.servant_seq_read(position, group={"owner": 7})
    assert seqs(rows) == list(range(position + 1, 6)) and blocked is None


async def test_seq_read_confirms_gap_after_late_delete(cmd: Table, monkeypatch):
    """读完索引之后行被删了：这次停住；下次的 ZRANGE 里没有它了，才确认缺口、跳过"""
    ids = await insert(cmd, *[{"owner": 7}] * 4)
    tamper(monkeypatch, cmd, drop=[ids[1]])
    rows, blocked = await cmd.servant_seq_read(0, group={"owner": 7})
    assert seqs(rows) == [1] and blocked == (2, ids[1])
    monkeypatch.undo()
    await delete(cmd, ids[1])
    rows, blocked = await cmd.servant_seq_read(1, group={"owner": 7})
    assert seqs(rows) == [3, 4] and blocked is None


async def test_seq_read_checks_whole_value_segment(cmd: Table, monkeypatch):
    """读到的行在索引上的值段（带组前缀）和 member 对不上（行被挪进了别的组）：按读不到处理"""
    ids = await insert(cmd, *[{"owner": 7}] * 3)
    tamper(monkeypatch, cmd, patch={ids[1]: {b"owner": b"8"}})
    rows, blocked = await cmd.servant_seq_read(0, group={"owner": 7})
    assert seqs(rows) == [1] and blocked == (2, ids[1])


async def test_seq_read_limit_and_anchor_bounds(cmd: Table):
    """limit 与锚点的边界：锚点不算在 limit 里；不给锚点时只读 limit 个"""
    ids = await insert(cmd, *[{"owner": 7}] * 4)
    rows, _ = await cmd.servant_seq_read(1, 3, group={"owner": 7}, after_id=ids[0])
    assert seqs(rows) == [2, 3, 4]
    assert seqs((await cmd.servant_seq_read(0, 3, group={"owner": 7})).rows) == [
        1,
        2,
        3,
    ]
    assert seqs((await cmd.servant_seq_read(0, 1, group={"owner": 7})).rows) == [1]
    rows, _ = await cmd.servant_seq_read(2, 1, group={"owner": 7}, after_id=ids[1])
    assert seqs(rows) == [3]


async def test_seq_read_anchor_matches(cmd: Table):
    """锚点对得上：不返回它，接着往后；锚点被删了、后面也没有行：什么都确认不了，返回空"""
    ids = await insert(cmd, *[{"owner": 7}] * 3)
    rows, _ = await cmd.servant_seq_read(2, group={"owner": 7}, after_id=ids[1])
    assert seqs(rows) == [3]
    await delete(cmd, ids[2])
    rows, blocked = await cmd.servant_seq_read(3, group={"owner": 7}, after_id=ids[2])
    assert len(rows) == 0 and blocked is None


async def test_seq_read_detects_reused_number(cmd: Table):
    """数据回退：游标那一号被删、计数器回到它之前，新行复用了这个号：reused"""
    ids = await insert(cmd, *[{"owner": 7}] * 3)
    await delete(cmd, ids[2])
    raw_kv_set(cmd.backend, raw_seq_counter_key(cmd.backend, cmd, owner=7), b"2")
    (new_id,) = await insert(cmd, {"owner": 7})
    with pytest.raises(CommitSeqDiverged) as err:
        await cmd.servant_seq_read(3, group={"owner": 7}, after_id=ids[2])
    exc = err.value
    assert (exc.reason, exc.seq, exc.expected_id, exc.found_id) == (
        "reused",
        3,
        ids[2],
        new_id,
    )
    assert exc.comp_name == cmd.comp_cls.name_ and exc.group == {"owner": 7}
    assert "reused" in str(exc)
    copy = pickle.loads(pickle.dumps(exc))
    assert (copy.reason, copy.seq, copy.found_id) == ("reused", 3, new_id)


async def test_seq_read_detects_missing_anchor(cmd: Table):
    """游标那一行没了、后面却有号：anchor_missing（按裁剪规则它不会被正常删掉）"""
    ids = await insert(cmd, *[{"owner": 7}] * 4)
    await delete(cmd, ids[1])
    with pytest.raises(CommitSeqDiverged) as err:
        await cmd.servant_seq_read(2, group={"owner": 7}, after_id=ids[1])
    assert (err.value.reason, err.value.seq, err.value.found_id) == (
        "anchor_missing",
        2,
        None,
    )


async def test_seq_read_detects_duplicates(cmd: Table):
    """
    计数器丢了（写成 0）之后又发了一遍号，组里有重号：带锚点读时锚点后面紧跟同号的新行、不带锚点
    读时相邻同号，都是 duplicate（审核实测的场景：不检查的话新行被当成新命令，旧号以下的永远看不到）
    """
    ids = await insert(cmd, *[{"owner": 7}] * 5)
    raw_kv_set(cmd.backend, raw_seq_counter_key(cmd.backend, cmd, owner=7), b"0")
    await insert(cmd, *[{"owner": 7}] * 7)
    with pytest.raises(CommitSeqDiverged) as err:
        await cmd.servant_seq_read(5, group={"owner": 7}, after_id=ids[4])
    assert (err.value.reason, err.value.seq, err.value.expected_id) == (
        "duplicate",
        5,
        ids[4],
    )
    with pytest.raises(CommitSeqDiverged) as err:
        await cmd.servant_seq_read(0, group={"owner": 7})
    assert (err.value.reason, err.value.seq, err.value.expected_id) == (
        "duplicate",
        1,
        ids[0],
    )


async def test_seq_read_ignores_unnumbered_rows(cmd: Table):
    """号 <= 0 的历史行（新加 commit_seq 列时填的 0）不返回，也不挡住后面的行"""
    await insert(cmd, *[{"owner": 7}] * 2)
    master = cmd.backend.master
    zset_key = master.zrange_args_(cmd, "seq", 0, 0, False, {"owner": 7})[0]  # type: ignore[attr-defined]
    for seq, fake_id in ((0, 101), (-3, 102)):
        row = cmd.comp_cls.new_row()
        row.owner, row.seq = 7, seq
        member = index_member_(index_value_bytes_(cmd.comp_cls, "seq", row), fake_id)
        raw_zadd(cmd.backend, zset_key, member)
    rows, blocked = await cmd.servant_seq_read(0, group={"owner": 7})
    assert seqs(rows) == [1, 2] and blocked is None


async def test_seq_read_rejects_bad_arguments(cmd: Table):
    """参数不对：报 TypeError / ValueError。整数参数收 np.integer，after_id 可以是负数"""
    ids = await insert(cmd, *[{"owner": 7}] * 2)
    group = {"owner": 7}
    for after, exc in (
        (-1, ValueError),
        (1 << 53, ValueError),
        (True, TypeError),
        (1.0, TypeError),
        ("1", TypeError),
    ):
        with pytest.raises(exc):
            await cmd.servant_seq_read(cast(Any, after), group=group)
    for limit, exc in ((0, ValueError), (-1, ValueError), (True, TypeError)):
        with pytest.raises(exc):
            await cmd.servant_seq_read(0, limit, group=group)
    with pytest.raises(ValueError, match="after_id"):
        await cmd.servant_seq_read(0, group=group, after_id=ids[0])
    with pytest.raises(TypeError):
        await cmd.servant_seq_read(1, group=group, after_id=cast(Any, np.bool_(True)))
    with pytest.raises(ValueError):
        await cmd.servant_seq_read(0, group=group, row_format=RowFormat.ID_LIST)
    with pytest.raises(ValueError):
        await cmd.servant_seq_read(0)
    with pytest.raises(ValueError):
        await cmd.servant_seq_read(0, group={"owner": 7, "kind": 0})
    # np.integer 照收：直接把上一批的最后一行传回来
    rows, _ = await cmd.servant_seq_read(0, 1, group=group)
    rows, _ = await cmd.servant_seq_read(
        rows[-1].seq, group=group, after_id=rows[-1].id
    )
    assert seqs(rows) == [2]
    # after_id 可以是负数（headless 显式给的 id），对不上就是 reused
    with pytest.raises(CommitSeqDiverged, match="reused"):
        await cmd.servant_seq_read(1, group=group, after_id=-5)


async def test_seq_read_requires_commit_seq(new_component_env, mod_auto_backend):
    from hetu.data import BaseComponent, define_component, property_field

    @define_component(namespace="pytest", force=True)
    class Plain(BaseComponent):
        seq: np.int64 = property_field(0, index=True)

    tbl = make_table(mod_auto_backend(), Plain)
    with pytest.raises(ValueError, match="没有提交序号列"):
        await tbl.servant_seq_read(0)


async def test_seq_read_row_formats(cmd: Table):
    """row_format 同 servant_range：RAW 是原始字符串、TYPED_DICT 按字段类型"""
    await insert(cmd, {"owner": 7}, {"owner": 7}, kind=3)
    raw, _ = await cmd.servant_seq_read(0, group={"owner": 7}, row_format=RowFormat.RAW)
    assert [row["seq"] for row in raw] == ["1", "2"]
    typed, _ = await cmd.servant_seq_read(
        0, group={"owner": 7}, row_format=RowFormat.TYPED_DICT
    )
    assert [(row["seq"], row["kind"]) for row in typed] == [(1, 3), (2, 3)]


@use_redis_family_backend_only
async def test_seq_read_pins_one_cluster_node(cmd: Table, monkeypatch):
    """
    原生 cluster：一次 seq_read 的 ZRANGE 和所有取行都发给同一个节点，servant 连接按
    RANDOM_REPLICA 随机选副本。ROUND_ROBIN_REPLICAS 是确定性轮询，某个副本断了复制时会让消费方
    永久读不到东西（设计稿 §3.2）。测试用的 cluster 没有副本，这里只核对发给了谁、怎么选的
    """
    import redis.asyncio.cluster as rc
    from redis.cluster import LoadBalancingStrategy

    client = cmd.backend.servant
    if not isinstance(client.aio, rc.RedisCluster):  # type: ignore[attr-defined]
        pytest.skip("只有原生 cluster 要钉节点")
    await insert(cmd, *[{"owner": 7}] * 3)
    targets: list = []
    strategies: list = []
    exec_cluster = rc.RedisCluster.execute_command
    exec_pipe = rc.ClusterPipeline.execute_command
    get_node = rc.NodesManager.get_node_from_slot

    async def spy_exec(self, *args, **kwargs):
        targets.append(kwargs.get("target_nodes"))
        return await exec_cluster(self, *args, **kwargs)

    def spy_pipe(self, *args, **kwargs):
        targets.append(kwargs.get("target_nodes"))
        return exec_pipe(self, *args, **kwargs)

    def spy_node(self, slot, read_from_replicas=False, load_balancing_strategy=None):
        strategies.append(load_balancing_strategy)
        return get_node(self, slot, read_from_replicas, load_balancing_strategy)

    monkeypatch.setattr(rc.RedisCluster, "execute_command", spy_exec)
    monkeypatch.setattr(rc.ClusterPipeline, "execute_command", spy_pipe)
    monkeypatch.setattr(rc.NodesManager, "get_node_from_slot", spy_node)
    monkeypatch.setattr(client, "is_servant", True)  # 测试环境的 cluster 没配 servants
    rows, _ = await cmd.servant_seq_read(0, group={"owner": 7})
    assert seqs(rows) == [1, 2, 3]
    assert len(targets) == 4  # 一条 ZRANGE + 三条 HGETALL
    assert targets[0] is not None and all(node is targets[0] for node in targets)
    assert strategies == [LoadBalancingStrategy.RANDOM_REPLICA]
