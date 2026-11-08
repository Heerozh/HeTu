"""
提交序号列（`property_field(commit_seq=True)`）：定义校验、提交时发号（两个后端的提交脚本）、
禁写与分组列锁定、事务内看不到未分配号的行、迁移与重建时的计数器、消费方的推荐写法。
设计稿见 docs/superpowers/specs/2026-11-09-commit-seq-design.md。
"""

import asyncio
import itertools
import json
import logging

import numpy as np
import pytest
from fixtures.backends import (
    SQLITE_BACKENDS,
    raw_index_members,
    raw_kv_get,
    raw_kv_keys,
    raw_kv_set,
    raw_seq_counter_key,
    use_redis_backend_only,
    use_redis_family_backend_only,
)
from fixtures.testdata import create_ref

from hetu.common.snowflake_id import SnowflakeID
from hetu.data.backend import Backend, RaceCondition, TableReference, UniqueViolation
from hetu.data.backend.base import index_member_, to_sortable_bytes
from hetu.data.backend.redis_model import RedisModelClient, msg_packer

SnowflakeID().init(1, 0)


def def_log():
    from hetu.data import BaseComponent, Permission, define_component, property_field

    global Log

    @define_component(namespace="pytest", permission=Permission.USER, force=True)
    class Log(BaseComponent):
        owner: np.int64 = property_field(0, index=True)
        # 每个 owner 各自从 1 连续编号
        seq: np.int64 = property_field(0, commit_seq=True, groupby="owner")
        kind: np.int16 = property_field(0)
        uid: np.int64 = property_field(0, unique=True)

    return Log


def def_event():
    from hetu.data import BaseComponent, Permission, define_component, property_field

    global Event

    @define_component(namespace="pytest", permission=Permission.USER, force=True)
    class Event(BaseComponent):
        # 整张表一个序列
        seq: np.int64 = property_field(0, commit_seq=True)
        kind: np.int16 = property_field(0)

    return Event


@pytest.fixture
async def log_ref(new_component_env, mod_auto_backend) -> TableReference:
    return create_ref(def_log(), mod_auto_backend())


@pytest.fixture
async def event_ref(new_component_env, mod_auto_backend) -> TableReference:
    return create_ref(def_event(), mod_auto_backend())


_serial = itertools.count(1)


def _log(comp, owner: int, **fields):
    """造一行 Log：uid（unique）默认给不重复的值"""
    row = comp.new_row()
    row.owner, row.uid = owner, next(_serial)
    for key, value in fields.items():
        row[key] = value
    return row


async def _insert(backend: Backend, comp, *rows) -> list[int]:
    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        for row in rows:
            await repo.insert(row)
    await backend.wait_for_synced()
    return [int(row.id) for row in rows]


async def _seqs(backend: Backend, comp, owner: int | None = None) -> list[tuple]:
    """库里的 (号, kind)，按号排；分组的组件给 owner"""
    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        if owner is None:
            rows = await repo.range(seq=(-np.inf, np.inf), limit=-1)
        else:
            rows = await repo.range(seq=(-np.inf, np.inf), owner=owner, limit=-1)
    return [(int(row.seq), int(row.kind)) for row in rows]


def _counter(backend: Backend, ref, **group) -> int:
    raw = raw_kv_get(backend, raw_seq_counter_key(backend, ref, **group))
    return 0 if raw is None else int(raw)


# ---------------------------------------------------------------- 定义


def test_define_commit_seq(new_component_env):
    """commit_seq 隐含 index；类属性、schema json 的形状；default 0.0 / False 规范成 0"""
    from hetu.data import BaseComponent, define_component, property_field

    comp = def_log()
    assert comp.commit_seq_ == "seq"
    assert "seq" in comp.indexes_ and "seq" not in comp.uniques_
    assert comp.index_groups_["seq"] == ("owner",)
    props = json.loads(comp.json_)["properties"]
    assert props["seq"]["commit_seq"] is True
    # 没开的列不写这个键：没用它的组件 schema 版本不变
    assert "commit_seq" not in props["owner"]
    assert BaseComponent.load_json(comp.json_).commit_seq_ == "seq"

    def define(default, **kwargs):
        @define_component(namespace="pytest", force=True)
        class G(BaseComponent):
            seq: np.int64 = property_field(default, commit_seq=True, **kwargs)

        return G

    plain = define(0)
    assert plain.commit_seq_ == "seq" and plain.index_groups_["seq"] == ()
    assert json.loads(plain.json_)["properties"]["seq"]["default"] == 0
    assert define(0.0).json_ == define(False).json_ == plain.json_

    @define_component(namespace="pytest", force=True)
    class NoSeq(BaseComponent):
        owner: np.int64 = property_field(0, index=True)

    assert NoSeq.commit_seq_ is None
    assert all(
        "commit_seq" not in p for p in json.loads(NoSeq.json_)["properties"].values()
    )


@pytest.mark.parametrize(
    "fields, match",
    [
        ({"seq": {"default": 0, "dtype": np.int32}}, "只能是np.int64"),
        ({"seq": {"default": 0, "dtype": np.uint64}}, "只能是np.int64"),
        ({"seq": {"default": 0, "dtype": np.float64}}, "只能是np.int64"),
        ({"seq": {"default": 1}}, "default必须是0"),
        ({"seq": {"default": 0, "unique": True}}, "不能同时设置unique"),
        ({"seq": {"default": 0}, "seq2": {"default": 0}}, "最多只能有一个"),
        (
            {"seq": {"default": 0}, "level": {"default": 0, "groupby": "seq"}},
            "不能当分组列",
        ),
    ],
)
def test_define_commit_seq_errors(new_component_env, fields, match):
    """类型只能是 int64、default 只能是 0、不能 unique、最多一个、不能被别的索引当分组列"""
    from hetu.data import BaseComponent, define_component, property_field

    namespace: dict = {"__annotations__": {}}
    for name, spec in fields.items():
        spec = dict(spec)
        dtype = spec.pop("dtype", np.int64)
        default = spec.pop("default")
        commit_seq = not spec.get("groupby")
        namespace["__annotations__"][name] = dtype
        namespace[name] = property_field(default, commit_seq=commit_seq, **spec)
    with pytest.raises(ValueError, match=match):
        define_component(namespace="pytest", force=True)(
            type("Bad", (BaseComponent,), namespace)
        )


def test_define_commit_seq_rls_column(new_component_env):
    """commit_seq 不能是 RLS 的判定列，也不能是 OWNER 组件的 owner"""
    from hetu.data import BaseComponent, Permission, define_component, property_field

    with pytest.raises(ValueError, match="行级权限"):

        @define_component(namespace="pytest", permission=Permission.OWNER, force=True)
        class BadOwner(BaseComponent):
            owner: np.int64 = property_field(0, commit_seq=True)

    with pytest.raises(ValueError, match="行级权限"):

        @define_component(
            namespace="pytest",
            permission=Permission.RLS,
            rls_compare=("le", "seq", "caller"),
            force=True,
        )
        class BadRls(BaseComponent):
            seq: np.int64 = property_field(0, commit_seq=True)

    # 不是 RLS 权限时 rls_compare 不起作用，不拦
    @define_component(
        namespace="pytest", rls_compare=("le", "seq", "caller"), force=True
    )
    class StrayRls(BaseComponent):
        seq: np.int64 = property_field(0, commit_seq=True)

    assert StrayRls.commit_seq_ == "seq"
    # rls_compare 格式不对时，照旧由 RLS 的定义检查给出提示，而不是 IndexError
    with pytest.raises(AssertionError, match="rls_compare"):

        @define_component(
            namespace="pytest",
            permission=Permission.RLS,
            rls_compare=("le",),
            force=True,
        )
        class Malformed(BaseComponent):
            seq: np.int64 = property_field(0, commit_seq=True)


def test_define_commit_seq_point_sub_ignored(new_component_env, caplog):
    """
    point_sub 用不上：号要到提交时才有，发不出值频道。写了警告并忽略，hidden 的也一样（不走
    hidden + point_sub 的报错）；带 groupby 的由复合索引那条警告，只警告一次。index=False 改成 True
    """
    from hetu.data import BaseComponent, define_component, property_field

    with caplog.at_level(logging.WARNING, logger="HeTu.root"):

        @define_component(namespace="pytest", force=True)
        class A(BaseComponent):
            seq: np.int64 = property_field(0, commit_seq=True, point_sub=True)

        @define_component(namespace="pytest", force=True)
        class B(BaseComponent):
            seq: np.int64 = property_field(
                0, commit_seq=True, point_sub=True, hidden=True
            )

        @define_component(namespace="pytest", force=True)
        class C(BaseComponent):
            owner: np.int64 = property_field(0)
            seq: np.int64 = property_field(
                0, commit_seq=True, groupby="owner", point_sub=True
            )

        @define_component(namespace="pytest", force=True)
        class D(BaseComponent):
            seq: np.int64 = property_field(0, commit_seq=True, index=False)

    for comp in (A, B, C):
        assert comp.point_subs_ == frozenset(), comp.name_
        assert json.loads(comp.json_)["properties"]["seq"]["point_sub"] is False
    assert caplog.text.count("commit_seq，point_sub用不上") == 2
    assert caplog.text.count("groupby（复合索引），point_sub用不上") == 1
    assert "seq" in D.indexes_
    assert "commit_seq时，index不能设置为False" in caplog.text


def test_headless_schema_diff_commit_seq(new_component_env):
    """
    headless 的 schema 比对要比 commit_seq（对不上时 headless 的 commit 会把 0 原样写进去，或给
    普通列发号）；不认识的列属性键（更新版本加的）一律判为不一致
    """
    from hetu import headless
    from hetu.data import BaseComponent

    comp = def_log()
    data = json.loads(comp.json_)
    del data["properties"]["seq"]["commit_seq"]
    local = BaseComponent.load_json(json.dumps(data))
    errors, _warns = headless._schema_diff(local, comp.json_)
    assert errors == ["~ col seq.commit_seq: True -> False"]
    assert headless._schema_diff(comp, comp.json_) == ([], [])

    newer = json.loads(comp.json_)
    newer["properties"]["kind"]["future_flag"] = True
    errors, _warns = headless._schema_diff(comp, json.dumps(newer))
    assert errors == ["~ col kind.future_flag: unknown property"]
    # 组件级不认识的键也一样
    newer = json.loads(comp.json_)
    newer["future_table_flag"] = True
    errors, _warns = headless._schema_diff(comp, json.dumps(newer))
    assert errors == ["future_table_flag: unknown component property"]


def test_cli_list_shows_commit_seq(new_component_env, monkeypatch):
    """hetu get --list 输出每列的 commit_seq"""
    from hetu.cli.data import list_components
    from hetu.system import SystemClusters

    comp = def_log()
    monkeypatch.setattr(
        SystemClusters(), "get_components", lambda namespace=None: {comp: 1}
    )
    [listed] = list_components("pytest")
    fields = {f["name"]: f for f in listed["fields"]}
    assert fields["seq"]["commit_seq"] is True
    assert fields["owner"]["commit_seq"] is False


# ---------------------------------------------------------------- 提交时发号


async def test_seq_assigned_on_commit(log_ref, mod_auto_backend):
    """
    号在提交时发：同组同一事务的多行按 insert 的顺序拿到连续的号，各组各自从 1 编号；行里存的号、
    索引 member（P + 号的编码 + 0x00 + id 段）都和 Python 侧的编码一致；计数器是这一组最近发出的号
    """
    backend: Backend = mod_auto_backend()
    comp = log_ref.comp_cls
    a1, a2, a3 = await _insert(
        backend,
        comp,
        _log(comp, 7, kind=1),
        _log(comp, 7, kind=2),
        _log(comp, 7, kind=3),
    )
    (b1,) = await _insert(backend, comp, _log(comp, 8, kind=10))
    (a4,) = await _insert(backend, comp, _log(comp, 7, kind=4))
    assert await _seqs(backend, comp, owner=7) == [(1, 1), (2, 2), (3, 3), (4, 4)]
    assert await _seqs(backend, comp, owner=8) == [(1, 10)]
    assert _counter(backend, log_ref, owner=7) == 4
    assert _counter(backend, log_ref, owner=8) == 1

    def member(owner: int, seq: int, row_id: int) -> bytes:
        prefix = to_sortable_bytes(np.int64(owner))
        return index_member_(prefix + to_sortable_bytes(np.int64(seq)), row_id)

    assert raw_index_members(backend, log_ref, "seq") == sorted(
        [
            member(7, 1, a1),
            member(7, 2, a2),
            member(7, 3, a3),
            member(7, 4, a4),
            member(8, 1, b1),
        ]
    )
    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        assert (await repo.get(seq=3, owner=7)).id == a3
        assert (await repo.get(id=b1)).seq == 1


async def test_seq_without_groupby(event_ref, mod_auto_backend):
    """不带 groupby：整张表一个序列，计数器 key 就是 {cluster_prefix}:seq:{字段名}"""
    backend: Backend = mod_auto_backend()
    comp = event_ref.comp_cls
    for kind in (1, 2):
        row = comp.new_row()
        row.kind = kind
        await _insert(backend, comp, row)
    assert await _seqs(backend, comp) == [(1, 1), (2, 2)]
    master = backend.master
    assert raw_seq_counter_key(backend, event_ref) == (
        master.cluster_prefix(event_ref) + ":seq:seq"
    )
    assert _counter(backend, event_ref) == 2


async def test_failed_commit_consumes_no_number(log_ref, mod_auto_backend):
    """提交检查失败（UNIQUE / RACE）、insert 了又在本事务删掉的行，都不消耗号"""
    from hetu.data.backend import RaceCondition

    backend: Backend = mod_auto_backend()
    comp = log_ref.comp_cls
    first = _log(comp, 7)
    await _insert(backend, comp, first)
    # unique 撞车：确定性冲突，不重试
    dup = _log(comp, 7)
    dup.uid = first.uid
    with pytest.raises(UniqueViolation):
        await _insert(backend, comp, dup)
    assert _counter(backend, log_ref, owner=7) == 1
    # 读到的行被别人改了：RACE
    with pytest.raises(RaceCondition):
        async with backend.session("pytest", 1) as session:
            repo = session.using(comp)
            row = await repo.get(id=first.id)
            async with backend.session("pytest", 1) as other:
                changed = await other.using(comp).get(id=first.id)
                changed.kind = 99
                await other.using(comp).update(changed)
            row.kind = 5
            await repo.update(row)
            await repo.insert(_log(comp, 7))
    assert _counter(backend, log_ref, owner=7) == 1
    # insert 了又删掉的行不占号
    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        gone = _log(comp, 7)
        await repo.insert(gone)
        repo.delete(gone.id)
        await repo.insert(_log(comp, 7, kind=2))
    await backend.wait_for_synced()
    # 第 1 行的 kind 被上面那个并发的事务改成了 99
    assert await _seqs(backend, comp, owner=7) == [(1, 99), (2, 2)]


async def test_seq_allocation_all_or_nothing(log_ref, mod_auto_backend):
    """
    发号全成或全不发：同一次提交里后面的计数器出错（不是整数），前面已经加上去的减回去，行和
    member 都没写；发出的号会到 2^53、或者计数器被改成负数时同样不发
    """
    backend: Backend = mod_auto_backend()
    comp = log_ref.comp_cls
    await _insert(backend, comp, _log(comp, 7))
    key8 = raw_seq_counter_key(backend, log_ref, owner=8)

    async def attempt(match: str) -> None:
        rows = _log(comp, 7), _log(comp, 8)
        # Redis 原样抛 INCRBY 的错误（redis-py 的 ResponseError），SQLite 抛 RuntimeError
        with pytest.raises(Exception, match=match):
            await _insert(backend, comp, *rows)
        async with backend.session("pytest", 1) as session:
            for row in rows:
                assert await session.using(comp).get(id=row.id) is None
        assert _counter(backend, log_ref, owner=7) == 1
        assert await _seqs(backend, comp, owner=7) == [(1, 0)]

    raw_kv_set(backend, key8, b"abc")
    await attempt("not an integer")
    raw_kv_set(backend, key8, b"%d" % (2**53 - 1))
    await attempt("Counter out of range")
    assert _counter(backend, log_ref, owner=8) == 2**53 - 1
    raw_kv_set(backend, key8, b"-5")
    await attempt("Counter out of range")
    assert _counter(backend, log_ref, owner=8) == -5
    assert await _seqs(backend, comp, owner=8) == []


@pytest.mark.parametrize("raw", [b" 5", b"+5", b"05", b"1_0", b"-0", b"9" * 20])
async def test_counter_parsed_like_redis(log_ref, mod_auto_backend, raw):
    """计数器按 Redis INCRBY 的规则认整数：SQLite 也不放过 Python int() 认、Redis 不认的写法"""
    backend: Backend = mod_auto_backend()
    comp = log_ref.comp_cls
    raw_kv_set(backend, raw_seq_counter_key(backend, log_ref, owner=7), raw)
    with pytest.raises(Exception, match="not an integer"):
        await _insert(backend, comp, _log(comp, 7))


@use_redis_family_backend_only
async def test_wrongtype_counter_keeps_redis_error(log_ref, mod_auto_backend):
    """计数器 key 被写成了别的类型：减回前面的计数器，原样抛 Redis 的 WRONGTYPE"""
    backend: Backend = mod_auto_backend()
    comp = log_ref.comp_cls
    await _insert(backend, comp, _log(comp, 7))
    master = backend.master
    master.io.hset(raw_seq_counter_key(backend, log_ref, owner=8), "x", "1")  # type: ignore
    with pytest.raises(Exception, match="WRONGTYPE"):
        await _insert(backend, comp, _log(comp, 7), _log(comp, 8))
    assert _counter(backend, log_ref, owner=7) == 1


@pytest.mark.parametrize(
    "start", [0, 254, 255, 2**31 - 1, 2**32 - 1, 2**48 - 1, 10**14 - 1, 2**53 - 2]
)
async def test_seq_encoding_boundaries(log_ref, mod_auto_backend, start):
    """提交脚本拼的号（行里的十进制串、索引里的 8 字节编码）和 Python 侧逐字节一致"""
    backend: Backend = mod_auto_backend()
    comp = log_ref.comp_cls
    raw_kv_set(backend, raw_seq_counter_key(backend, log_ref, owner=7), b"%d" % start)
    (row_id,) = await _insert(backend, comp, _log(comp, 7))
    prefix = to_sortable_bytes(np.int64(7))
    expected = index_member_(prefix + to_sortable_bytes(np.int64(start + 1)), row_id)
    assert raw_index_members(backend, log_ref, "seq") == [expected]
    async with backend.session("pytest", 1) as session:
        assert (await session.using(comp).get(id=row_id)).seq == start + 1


async def test_hsetseq_number_wins_over_stray_zero(log_ref, mod_auto_backend):
    """HSETSEQ 把号放在 HSET 的最后一对：中间要是还留着这个字段的 0，写进去的也是发出的号"""
    backend: Backend = mod_auto_backend()
    comp = log_ref.comp_cls
    master = backend.master
    assert isinstance(master, RedisModelClient)
    async with backend.session("pytest", 1) as session:
        row = _log(comp, 7)
        await session.using(comp).insert(row)
        keys, payload = master.build_commit_payload_(session.idmap)
        session.discard()
    [cmd] = [cmd for cmd in payload[1] if cmd[0] == "HSETSEQ"]
    assert "seq" not in cmd[6:-2:2]  # Python 已经把行里的 0 去掉了
    cmd[6:6] = ["seq", "0"]  # 模拟漏删
    assert payload[5] == [[raw_seq_counter_key(backend, log_ref, owner=7), 1]]
    resp = await master.commit_script_(keys, [msg_packer.pack(payload)])  # type: ignore
    assert resp == b"committed"
    await backend.wait_for_synced()
    assert await _seqs(backend, comp, owner=7) == [(1, 0)]


async def test_payload_without_commit_seq_unchanged(
    new_component_env, mod_auto_backend
):
    """没有提交序号列的组件，payload 照旧是普通的 HSET，第 6 段为空"""
    from hetu.data import BaseComponent, define_component, property_field

    @define_component(namespace="pytest", force=True)
    class Plain(BaseComponent):
        kind: np.int16 = property_field(0, index=True)

    backend: Backend = mod_auto_backend()
    create_ref(Plain, backend)
    master = backend.master
    assert isinstance(master, RedisModelClient)
    async with backend.session("pytest", 1) as session:
        await session.using(Plain).insert(Plain.new_row())
        _keys, payload = master.build_commit_payload_(session.idmap)
        session.discard()
    # 没有号要发时不带第 6 段：提交脚本读到 nil，不建发号用的表
    assert len(payload) == 5
    assert [cmd[0] for cmd in payload[1]] == ["HSET", "ZADD", "ZADD"]


@pytest.mark.parametrize("backend_name", SQLITE_BACKENDS, indirect=True)
async def test_sqlite_counter_sends_no_notification(log_ref, mod_auto_backend):
    """
    SQLite 按 Redis 产生通知的规则记通知：HSETSEQ 写的行发行频道、组 key 发索引频道；计数器不发
    （HeTu 给 Redis 开的 Kghz 不含 $，INCRBY 不产生通知）
    """
    from hetu.data.backend.sqlite import SQLiteBackendClient
    from hetu.data.backend.sqlite.store import SQLiteStore

    backend: Backend = mod_auto_backend()
    master = backend.master
    assert isinstance(master, SQLiteBackendClient)
    comp = log_ref.comp_cls
    tail = master.run_sync_(SQLiteStore.notify_tail)
    (row_id,) = await _insert(backend, comp, _log(comp, 7))
    fetched, _min_id, _last = master.run_sync_(
        SQLiteStore.notify_fetch, tail, None, 100
    )
    channels = {channel for _id, channel, _payload in fetched}
    prefix = to_sortable_bytes(np.int64(7))
    group_key = master.index_zset_key_(master.index_key(log_ref, "seq"), prefix)
    assert master.row_channel(log_ref, row_id) in channels
    assert f"__keyspace@0__:{group_key}" in channels
    counter_head = master.cluster_prefix(log_ref) + ":seq:"
    assert not [channel for channel in channels if counter_head in channel]


async def test_counter_keys_layout(new_component_env, mod_auto_backend):
    """计数器 key 带分组列名和 P 的完整 hex（长字符串分组值也不取摘要），在 cluster_prefix 下"""
    from hetu.data import BaseComponent, define_component, property_field

    @define_component(namespace="pytest", force=True)
    class Chat(BaseComponent):
        room: "U40" = property_field("")  # type: ignore  # noqa
        zone: np.int32 = property_field(0)
        seq: np.int64 = property_field(0, commit_seq=True, groupby=("room", "zone"))

    backend: Backend = mod_auto_backend()
    ref = create_ref(Chat, backend)
    room = "r" * 40  # P 超过 32 字节：组 key 用摘要，计数器用完整 hex
    for _ in range(2):
        row = Chat.new_row()
        row.room, row.zone = room, 3
        await _insert(backend, Chat, row)
    master = backend.master
    key = raw_seq_counter_key(backend, ref, room=room, zone=3)
    prefix = (room.encode() + b"\x00\x01") + to_sortable_bytes(np.int32(3))
    assert key == f"{master.cluster_prefix(ref)}:seq:seq:room,zone:{prefix.hex()}"
    assert raw_kv_keys(backend, master.cluster_prefix(ref) + ":seq:") == [key]
    assert _counter(backend, ref, room=room, zone=3) == 2


# ---------------------------------------------------------------- 禁写与分组列锁定


async def test_seq_column_not_writable(log_ref, mod_auto_backend):
    """insert 时号必须是 0；update 不能改号；有号的行不能改分组列"""
    backend: Backend = mod_auto_backend()
    comp = log_ref.comp_cls
    (row_id,) = await _insert(backend, comp, _log(comp, 7))
    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        with pytest.raises(ValueError, match="insert 时必须是 0"):
            await repo.insert(_log(comp, 7, seq=5))
        row = await repo.get(id=row_id)
        row.seq = 9
        with pytest.raises(ValueError, match="不能修改"):
            await repo.update(row)
        row = await repo.get(id=row_id)
        row.owner = 8
        with pytest.raises(ValueError, match="不能改分组列"):
            await repo.update(row)
        # 不碰号和分组列的修改照常
        row = await repo.get(id=row_id)
        row.kind = 3
        await repo.update(row)
    await backend.wait_for_synced()
    assert await _seqs(backend, comp, owner=7) == [(1, 3)]


async def test_pending_and_unnumbered_rows_can_change_group(log_ref, mod_auto_backend):
    """
    本事务 insert、还没提交的行可以改分组列，号按最终的组发；库里号 ≤ 0（没有号的历史行）的行也
    可以改分组列
    """
    from fixtures.backends import raw_hset

    backend: Backend = mod_auto_backend()
    comp = log_ref.comp_cls
    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        row = _log(comp, 7)
        await repo.insert(row)
        row.owner = 8
        await repo.update(row)
    await backend.wait_for_synced()
    assert await _seqs(backend, comp, owner=7) == []
    assert await _seqs(backend, comp, owner=8) == [(1, 0)]

    # 造一行没有号的历史行（号是 0，索引按 0 建好）
    (old_id,) = await _insert(backend, comp, _log(comp, 9))
    raw_hset(backend, log_ref, old_id, seq="0")
    backend.get_table_maintenance().do_rebuild_index_(log_ref)
    await backend.wait_for_synced()
    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        old = await repo.get(id=old_id)
        old.owner = 10
        await repo.update(old)
    await backend.wait_for_synced()
    assert await _seqs(backend, comp, owner=9) == []
    assert await _seqs(backend, comp, owner=10) == [(0, 0)]


async def test_upsert_with_commit_seq(log_ref, mod_auto_backend):
    """
    用别的 unique 字段锚定：新建的行照常发号；命中已有行之后改分组列报错；在块里给号赋值，新建时
    报错。不能以提交序号列为锚（不是 unique），-O 下也拦得住（用 raise 不用 assert）
    """
    backend: Backend = mod_auto_backend()
    comp = log_ref.comp_cls

    def upsert(session, uid: int):
        return session.using(comp).upsert(uid=uid)

    async with backend.session("pytest", 1) as session, upsert(session, 1001) as row:
        row.owner, row.kind = 7, 1
    await backend.wait_for_synced()
    assert await _seqs(backend, comp, owner=7) == [(1, 1)]

    with pytest.raises(ValueError, match="不能改分组列"):
        async with (
            backend.session("pytest", 1) as session,
            upsert(session, 1001) as row,
        ):
            row.owner = 8
    with pytest.raises(ValueError, match="insert 时必须是 0"):
        async with (
            backend.session("pytest", 1) as session,
            upsert(session, 1002) as row,
        ):
            row.owner, row.seq = 7, 2
    async with backend.session("pytest", 1) as session:
        with pytest.raises(ValueError, match="不是unique索引"):
            session.using(comp).upsert(seq=1)
        with pytest.raises(ValueError, match="对不上任何索引"):
            session.using(comp).upsert(seq=1, owner=7)
    await backend.wait_for_synced()
    assert await _seqs(backend, comp, owner=7) == [(1, 1)]
    assert _counter(backend, log_ref, owner=7) == 1


async def test_payload_rejects_seq_update_bypassing_repo(log_ref, mod_auto_backend):
    """绕过 repo 直接改缓存里的号：提交时 payload 兜底报错，什么都不写"""
    backend: Backend = mod_auto_backend()
    comp = log_ref.comp_cls
    (row_id,) = await _insert(backend, comp, _log(comp, 7))
    with pytest.raises(ValueError, match="commit_seq 列"):
        async with backend.session("pytest", 1) as session:
            row = await session.using(comp).get(id=row_id)
            row.seq = 5
            session.idmap.update(log_ref, row)
    await backend.wait_for_synced()
    assert await _seqs(backend, comp, owner=7) == [(1, 0)]


# ---------------------------------------------------------------- 事务内


async def test_pending_rows_not_on_seq_index(log_ref, mod_auto_backend):
    """
    本事务 insert 的行还没有号（是 0）：按提交序号列 range / get 查不到它们（(-inf, inf) 也查
    不到），按 id、按别的索引能查到。开着 phantom_check 的 desc + limit 读不包含它，提交照样成功
    """
    backend: Backend = mod_auto_backend()
    comp = log_ref.comp_cls
    await _insert(backend, comp, _log(comp, 7, kind=1), _log(comp, 7, kind=2))
    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        new = _log(comp, 7, kind=3)
        await repo.insert(new)
        latest = await repo.range(seq=(-np.inf, np.inf), owner=7, limit=2, desc=True)
        assert [int(seq) for seq in latest.seq] == [2, 1]
        everything = await repo.range(seq=(-np.inf, np.inf), owner=7, limit=-1)
        assert sorted(int(kind) for kind in everything.kind) == [1, 2]
        assert await repo.get(seq=0, owner=7) is None
        assert (await repo.get(id=new.id)).seq == 0
        by_owner = await repo.range(owner=(7, 7), limit=-1)
        assert sorted(int(kind) for kind in by_owner.kind) == [1, 2, 3]
        # 删掉的库里行照常从结果里拿掉
        first = await repo.get(seq=1, owner=7)
        repo.delete(first.id)
        rest = await repo.range(seq=(-np.inf, np.inf), owner=7, limit=-1)
        assert [int(seq) for seq in rest.seq] == [2]
    await backend.wait_for_synced()
    assert await _seqs(backend, comp, owner=7) == [(2, 2), (3, 3)]


# ---------------------------------------------------------------- 维护


def _redefine_log(extra_field: bool = False, groupby="owner"):
    """重新定义同名的 Log（force 覆盖），可以多一列 extra、换分组列"""
    from hetu.data import BaseComponent, Permission, define_component, property_field

    annotations = {
        "owner": np.int64,
        "guild": np.int64,
        "seq": np.int64,
        "kind": np.int16,
        "uid": np.int64,
    }
    namespace = {
        "owner": property_field(0, index=True),
        "guild": property_field(0),
        "seq": property_field(0, commit_seq=True, groupby=groupby),
        "kind": property_field(0),
        "uid": property_field(0, unique=True),
    }
    if extra_field:
        annotations["extra"] = np.int32
        namespace["extra"] = property_field(7)
    namespace["__annotations__"] = annotations
    return define_component(namespace="pytest", permission=Permission.USER, force=True)(
        type("Log", (BaseComponent,), namespace)
    )


async def _delete_seqs(backend: Backend, comp, owner: int, seqs) -> None:
    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        for seq in seqs:
            row = await repo.get(seq=seq, owner=owner)
            repo.delete(row.id)
    await backend.wait_for_synced()


@pytest.fixture
async def guild_log_ref(new_component_env, mod_auto_backend) -> TableReference:
    return create_ref(_redefine_log(), mod_auto_backend())


async def test_dtype_migration_keeps_counters(
    guild_log_ref, mod_auto_backend, tmp_path
):
    """
    改 dtype 的迁移（这里是加一列，走默认迁移脚本：原表改名到临时表、建新表、拷行、删临时表）之后，
    计数器还在：组内最大号被删了的组、整组删光了的组，都接着原来的号发，不复用
    """
    from hetu.data.backend import Table

    backend: Backend = mod_auto_backend()
    comp = guild_log_ref.comp_cls
    await _insert(backend, comp, *(_log(comp, 7) for _ in range(5)))
    await _insert(backend, comp, *(_log(comp, 8) for _ in range(3)))
    await _delete_seqs(backend, comp, 7, (4, 5))
    await _delete_seqs(backend, comp, 8, (1, 2, 3))

    new_comp = _redefine_log(extra_field=True)
    table = Table(new_comp, "pytest", 1, backend)
    maint = backend.get_table_maintenance()
    status, old_meta = maint.check_table(table)
    assert status == "schema_mismatch"
    assert maint.migration_schema(tmp_path / "app.py", table, old_meta)
    assert maint.check_table(table)[0] == "ok"
    await backend.wait_for_synced()

    await _insert(backend, new_comp, _log(new_comp, 7), _log(new_comp, 8))
    assert [seq for seq, _kind in await _seqs(backend, new_comp, owner=7)] == [
        1,
        2,
        3,
        6,
    ]
    assert [seq for seq, _kind in await _seqs(backend, new_comp, owner=8)] == [4]
    # 临时表名下没有留下计数器
    temp = TableReference(new_comp.duplicate("pytest", "__temp__"), "pytest", 1)
    assert raw_kv_keys(backend, backend.master.all_clusters_prefix(temp)) == []


async def test_cluster_migration_moves_counters(guild_log_ref, mod_auto_backend):
    """换簇（组件名不变）时计数器跟着搬到新簇下，接着发"""
    from hetu.data.backend import Table

    backend: Backend = mod_auto_backend()
    comp = guild_log_ref.comp_cls
    await _insert(backend, comp, *(_log(comp, 7) for _ in range(3)))
    await _delete_seqs(backend, comp, 7, (3,))

    table = Table(comp, "pytest", 2, backend)
    maint = backend.get_table_maintenance()
    status, old_meta = maint.check_table(table)
    assert status == "cluster_mismatch"
    maint.migration_cluster_id(table, old_meta)
    assert raw_kv_keys(backend, backend.master.cluster_prefix(guild_log_ref)) == []
    async with backend.session("pytest", 2) as session:
        await session.using(comp).insert(_log(comp, 7))
    await backend.wait_for_synced()
    async with backend.session("pytest", 2) as session:
        rows = await session.using(comp).range(seq=(-np.inf, np.inf), owner=7, limit=-1)
    assert [int(seq) for seq in rows.seq] == [1, 2, 4]


def _ledger(commit_seq: bool, extra: bool = False, groupby: str = "owner"):
    """
    Ledger：seq 按 owner（或 guild）分组的复合索引，commit_seq 开关可选（迁移前后两个版本），
    extra 多一列（改 dtype，走默认迁移脚本）
    """
    from hetu.data import BaseComponent, define_component, property_field

    annotations = {"owner": np.int64, "guild": np.int64, "seq": np.int64}
    namespace = {
        "owner": property_field(0, index=True),
        "guild": property_field(0),
        "seq": property_field(0, commit_seq=commit_seq, groupby=groupby),
    }
    annotations["kind"], namespace["kind"] = np.int16, property_field(0)
    if extra:
        annotations["extra"], namespace["extra"] = np.int32, property_field(7)
    namespace["__annotations__"] = annotations
    return define_component(namespace="pytest", force=True)(
        type("Ledger", (BaseComponent,), namespace)
    )


async def _fill_ledger(backend: Backend, comp, rows: dict[int, list[int]]) -> None:
    """按 {owner: [号, …]} 写入（commit_seq 关着时号可以随便写）"""
    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        for owner, seqs in rows.items():
            for seq in seqs:
                row = comp.new_row()
                row.owner, row.seq = owner, seq
                await repo.insert(row)
    await backend.wait_for_synced()


def _enable_commit_seq(backend: Backend, tmp_path, **define):
    """
    给 Ledger 已有的 seq 字段打开 commit_seq，跑迁移。只改这一项时 dtype 不变、走 skip 路径；
    define 可以另加 extra（改 dtype，走默认迁移脚本）、换 groupby
    """
    from hetu.data.backend import Table

    new_comp = _ledger(commit_seq=True, **define)
    table = Table(new_comp, "pytest", 1, backend)
    maint = backend.get_table_maintenance()
    status, old_meta = maint.check_table(table)
    assert status == "schema_mismatch"
    return (
        new_comp,
        table,
        lambda: maint.migration_schema(tmp_path / "app.py", table, old_meta),
    )


async def test_enable_commit_seq_on_existing_field(
    new_component_env, mod_auto_backend, tmp_path
):
    """
    把已有的序号字段改成 commit_seq：值保留，计数器校准到每组最大的正数号，之后接着发；≤ 0 的号
    表示没有号，不参与校准。只改了 commit_seq、索引定义没变，也要走重建（否则计数器没人校准）
    """
    backend: Backend = mod_auto_backend()
    ref = create_ref(_ledger(commit_seq=False), backend)
    await _fill_ledger(backend, ref.comp_cls, {7: [1, 2, 5], 8: [3], 9: [0, -1]})

    new_comp, table, migrate = _enable_commit_seq(backend, tmp_path)
    assert migrate()
    await backend.wait_for_synced()
    assert _counter(backend, table, owner=7) == 5
    assert _counter(backend, table, owner=8) == 3
    assert raw_kv_get(backend, raw_seq_counter_key(backend, table, owner=9)) is None
    rows = [new_comp.new_row() for _ in range(3)]
    for row, owner in zip(rows, (7, 8, 9)):
        row.owner = owner
    await _insert(backend, new_comp, *rows)
    assert [int(row.seq) for row in await _read_all(backend, new_comp, 7)] == [
        1,
        2,
        5,
        6,
    ]
    assert [int(row.seq) for row in await _read_all(backend, new_comp, 8)] == [3, 4]
    assert [int(row.seq) for row in await _read_all(backend, new_comp, 9)] == [
        -1,
        0,
        1,
    ]


async def _read_all(backend: Backend, comp, owner: int):
    async with backend.session("pytest", 1) as session:
        return await session.using(comp).range(
            seq=(-np.inf, np.inf), owner=owner, limit=-1
        )


@pytest.mark.parametrize(
    "rows, match",
    [
        ({7: [1, 2, 2], 8: [0, 0]}, "组内重复的号"),
        ({7: [1, 2**53]}, "上限"),
    ],
)
async def test_enable_commit_seq_rejects_bad_numbers(
    new_component_env, mod_auto_backend, tmp_path, rows, match
):
    """
    组内重复的号（会让消费方静默漏行）、到了 2^53 的号：升级报错，旧索引、计数器、meta 都不动。
    重复检查跳过 ≤ 0 的号（新加列时历史行都是 0）
    """
    backend: Backend = mod_auto_backend()
    ref = create_ref(_ledger(commit_seq=False), backend)
    await _fill_ledger(backend, ref.comp_cls, rows)
    before = raw_index_members(backend, ref, "seq")

    _new_comp, table, migrate = _enable_commit_seq(backend, tmp_path)
    with pytest.raises(RuntimeError, match=match):
        migrate()
    assert raw_index_members(backend, ref, "seq") == before
    assert raw_kv_keys(backend, backend.master.cluster_prefix(ref) + ":seq:") == []
    assert backend.get_table_maintenance().check_table(table)[0] == "schema_mismatch"


async def test_dtype_migration_rebuild_failure_refuses_to_start(
    new_component_env, mod_auto_backend, tmp_path
):
    """
    改 dtype 的迁移（加一列）同时给已有字段打开 commit_seq，组内又有重复号：默认迁移脚本已经把
    数据搬完，重建失败时 meta 标成待重建，服务器拒绝启动（check_table 不是 ok），不会带着没建成
    的索引发出重号。修好数据再跑一次 upgrade，按当前定义直接重建
    """
    from hetu.data.backend.base import REBUILD_PENDING_VERSION

    backend: Backend = mod_auto_backend()
    ref = create_ref(_ledger(commit_seq=False), backend)
    await _fill_ledger(backend, ref.comp_cls, {7: [1, 2, 2]})

    new_comp, table, migrate = _enable_commit_seq(backend, tmp_path, extra=True)
    with pytest.raises(RuntimeError, match="组内重复的号"):
        migrate()
    maint = backend.get_table_maintenance()
    status, meta = maint.check_table(table)
    assert status == "schema_mismatch"
    assert meta is not None and meta.version == REBUILD_PENDING_VERSION

    # 修数据：后一个 2 号改成 3 号，再跑一次 upgrade
    rows = [maint.get(table, row_id) for row_id in maint.get_all_row_id(table)]
    twos = sorted((row for row in rows if row.seq == 2), key=lambda row: row.id)
    twos[1].seq = 3
    maint.upsert_row(table, twos[1])
    assert maint.migration_schema(tmp_path / "app.py", table, meta)
    assert maint.check_table(table)[0] == "ok"
    await backend.wait_for_synced()
    assert _counter(backend, table, owner=7) == 3
    row = new_comp.new_row()
    row.owner = 7
    await _insert(backend, new_comp, row)
    assert [int(r.seq) for r in await _read_all(backend, new_comp, 7)] == [1, 2, 3, 4]


async def test_change_groupby_recalibrates_new_counters(
    new_component_env, mod_auto_backend, tmp_path
):
    """
    改 commit_seq 字段的 groupby（owner → guild）：新分组用新的计数器 key，按数据校准，旧 key 留着；
    原来的号在新分组里重复时报错，什么都不动
    """
    backend: Backend = mod_auto_backend()
    ref = create_ref(_ledger(commit_seq=True), backend)
    comp = ref.comp_cls
    for owner, guild in ((7, 1), (7, 1), (8, 2)):
        row = comp.new_row()
        row.owner, row.guild = owner, guild
        await _insert(backend, comp, row)
    old_keys = raw_kv_keys(backend, backend.master.cluster_prefix(ref) + ":seq:")
    assert len(old_keys) == 2

    _new_comp, table, migrate = _enable_commit_seq(backend, tmp_path, groupby="guild")
    assert migrate()
    await backend.wait_for_synced()
    assert _counter(backend, table, guild=1) == 2
    assert _counter(backend, table, guild=2) == 1
    keys = raw_kv_keys(backend, backend.master.cluster_prefix(ref) + ":seq:")
    assert set(old_keys) < set(keys)  # 旧 key 留着

    # 另一份数据：两个 owner 各自的 1 号都在 guild 1，改按 guild 分组就重复了
    ref2 = create_ref(_ledger(commit_seq=True), backend)
    for owner, guild in ((7, 1), (8, 1)):
        row = ref2.comp_cls.new_row()
        row.owner, row.guild = owner, guild
        await _insert(backend, ref2.comp_cls, row)
    before = raw_index_members(backend, ref2, "seq")
    _new_comp, table2, migrate2 = _enable_commit_seq(backend, tmp_path, groupby="guild")
    with pytest.raises(RuntimeError, match="组内重复的号"):
        migrate2()  # guild 1 里两行都是 1 号
    assert raw_index_members(backend, ref2, "seq") == before
    assert backend.get_table_maintenance().check_table(table2)[0] == "schema_mismatch"


@use_redis_backend_only
async def test_counters_calibrated_before_swap(log_ref, mod_auto_backend, monkeypatch):
    """先校准计数器、再换上索引：换上那一步出错时，计数器已经校准好了（只往上调不会发出重号）"""
    backend: Backend = mod_auto_backend()
    comp = log_ref.comp_cls
    await _insert(backend, comp, _log(comp, 7), _log(comp, 7), _log(comp, 7))
    raw_kv_set(backend, raw_seq_counter_key(backend, log_ref, owner=7), b"1")
    maint = backend.get_table_maintenance()
    monkeypatch.setattr(maint, "_RENAME_LUA", "return redis.error_reply('boom')")
    with pytest.raises(Exception, match="boom"):
        maint.do_rebuild_index_(log_ref)
    assert _counter(backend, log_ref, owner=7) == 3


async def test_rebuild_only_raises_counters(log_ref, mod_auto_backend):
    """
    重建只把计数器往上调：比库里最大号大的不动，没有的补上。写坏了的（不是 INCRBY 认的整数，
    比如 "0x10"、"5.5"）直接改写成最大号，两个后端规则相同
    """
    backend: Backend = mod_auto_backend()
    comp = log_ref.comp_cls
    await _insert(backend, comp, _log(comp, 7), _log(comp, 7), _log(comp, 8))
    await _insert(backend, comp, _log(comp, 9), _log(comp, 10))
    raw_kv_set(backend, raw_seq_counter_key(backend, log_ref, owner=7), b"100")
    raw_kv_set(backend, raw_seq_counter_key(backend, log_ref, owner=8), b"0")
    raw_kv_set(backend, raw_seq_counter_key(backend, log_ref, owner=9), b"0x10")
    raw_kv_set(backend, raw_seq_counter_key(backend, log_ref, owner=10), b"5.5")
    backend.get_table_maintenance().do_rebuild_index_(log_ref)
    assert _counter(backend, log_ref, owner=7) == 100
    assert _counter(backend, log_ref, owner=8) == 1
    assert _counter(backend, log_ref, owner=9) == 1
    assert _counter(backend, log_ref, owner=10) == 1


async def test_volatile_flush_restarts_numbering(new_component_env, mod_auto_backend):
    """易失表每次维护清空，计数器一起删掉，号从 1 重来"""
    from hetu.data import BaseComponent, define_component, property_field

    @define_component(namespace="pytest", volatile=True, force=True)
    class Tick(BaseComponent):
        seq: np.int64 = property_field(0, commit_seq=True)

    backend: Backend = mod_auto_backend()
    ref = create_ref(Tick, backend)
    await _insert(backend, Tick, Tick.new_row(), Tick.new_row())
    assert _counter(backend, ref) == 2
    backend.get_table_maintenance().flush(ref)
    assert raw_kv_get(backend, raw_seq_counter_key(backend, ref)) is None
    await _insert(backend, Tick, Tick.new_row())
    assert await _seqs_plain(backend, Tick) == [1]


async def _seqs_plain(backend: Backend, comp) -> list[int]:
    async with backend.session("pytest", 1) as session:
        rows = await session.using(comp).range(seq=(-np.inf, np.inf), limit=-1)
    return [int(seq) for seq in rows.seq]


def test_counter_key_depends_on_group_columns(new_component_env):
    """改了 groupby（owner → guild，同是 int64、P 字节相同）就是另一组计数器 key，不继承旧组的号"""
    by_owner = _redefine_log(groupby="owner")
    by_guild = _redefine_log(groupby="guild")
    prefix = to_sortable_bytes(np.int64(7))
    key_owner = RedisModelClient.seq_counter_key_("p", by_owner, prefix)
    key_guild = RedisModelClient.seq_counter_key_("p", by_guild, prefix)
    assert key_owner == f"p:seq:seq:owner:{prefix.hex()}"
    assert key_guild == f"p:seq:seq:guild:{prefix.hex()}"


# ---------------------------------------------------------------- 并发与消费方


def _def_consumer_tables():
    """消费方用的两张表：游标（每个 owner 一行）、处理记录（log_uid unique，重复处理会撞车）"""
    from hetu.data import BaseComponent, define_component, property_field

    @define_component(namespace="pytest", force=True)
    class LogCursor(BaseComponent):
        owner: np.int64 = property_field(0, unique=True)
        last: np.int64 = property_field(0)

    @define_component(namespace="pytest", force=True)
    class Done(BaseComponent):
        log_uid: np.int64 = property_field(0, unique=True)
        seq: np.int64 = property_field(0)

    @define_component(namespace="pytest", force=True)
    class Hot(BaseComponent):
        hits: np.int64 = property_field(0)

    return LogCursor, Done, Hot


async def _until_committed(make_txn) -> None:
    """RaceCondition 就重试（不退避：测试里要的就是冲突）"""
    while True:
        try:
            await make_txn()
            return
        except RaceCondition:
            await asyncio.sleep(0)


async def _consume_once(backend: Backend, log, cursor_comp, done, owner, limit=3):
    """
    设计稿 §9 的服务端消费写法：从游标往后读，缺号时用一次闭区间 range 确认整段（读到的补上处理，
    读空的由提交时的 CNT 在 master 上确认这一段确实没有行），处理记录写进 Done（同一个事务，重复
    处理会撞 unique）。返回游标的新位置
    """
    async with (
        backend.session("pytest", 1) as session,
        session.using(cursor_comp).upsert(owner=owner) as cursor,
    ):
        repo = session.using(log)
        done_repo = session.using(done)

        async def handle(row):
            record = done.new_row()
            record.log_uid, record.seq = row.uid, row.seq
            await done_repo.insert(record)

        rows = await repo.range(
            seq=(cursor.last + 1, np.inf), owner=owner, limit=limit, phantom_check=False
        )
        expected = cursor.last + 1
        for row in rows:
            if expected < row.seq:  # 中间缺了号：一次闭区间读确认整段
                for missing in await repo.range(
                    seq=(expected, int(row.seq) - 1), owner=owner, limit=-1
                ):
                    await handle(missing)  # range 漏读了它
            await handle(row)
            expected = int(row.seq) + 1
        cursor.last = expected - 1
    return int(cursor.last)


async def _done_seqs(backend: Backend, done) -> list[int]:
    async with backend.session("pytest", 1) as session:
        rows = await session.using(done).range(log_uid=(-np.inf, np.inf), limit=-1)
    return sorted(int(seq) for seq in rows.seq)


async def test_concurrent_producers_gapless(new_component_env, mod_auto_backend):
    """
    多个生产者并发往同一组写，每个事务都改同一行热点行（制造 VER 冲突和重试），中间还夹着因为
    unique 撞车而失败的事务：最终这一组的号正好是 1..N，不重不漏；每个串行的生产者自己拿到的号递增
    """
    backend: Backend = mod_auto_backend()
    log = def_log()
    create_ref(log, backend)
    _cursor, _done, hot = _def_consumer_tables()
    create_ref(hot, backend)
    (hot_id,) = await _insert(backend, hot, hot.new_row())
    producers, per = 5, 6

    async def produce(pid: int):
        for i in range(per):

            async def txn(uid: int = pid * 1000 + i):
                async with backend.session("pytest", 1) as session:
                    row = await session.using(hot).get(id=hot_id)
                    row.hits += 1
                    await session.using(hot).update(row)
                    await asyncio.sleep(0)  # 让别的生产者插进来读同一行
                    await session.using(log).insert(_log(log, 7, kind=pid, uid=uid))

            await _until_committed(txn)
            if i == per // 2:
                # uid 撞车：确定性失败，不消耗号
                with pytest.raises(UniqueViolation):
                    await _insert(backend, log, _log(log, 7, uid=pid * 1000 + i))

    await asyncio.gather(*(produce(pid) for pid in range(producers)))
    await backend.wait_for_synced()
    async with backend.session("pytest", 1) as session:
        rows = await session.using(log).range(seq=(-np.inf, np.inf), owner=7, limit=-1)
        hits = (await session.using(hot).get(id=hot_id)).hits
    assert [int(seq) for seq in rows.seq] == list(range(1, producers * per + 1))
    assert hits == producers * per
    for pid in range(producers):
        mine = [int(uid) for uid, kind in zip(rows.uid, rows.kind) if kind == pid]
        assert mine == sorted(mine), pid


async def test_servant_reads_are_contiguous_prefixes(log_ref, mod_auto_backend):
    """
    并发写入的同时反复从副本读整组：发号和写入在同一次原子执行里完成，副本整块应用每次提交，所以
    每次读到的都是从 1 开始的连续前缀
    """
    backend: Backend = mod_auto_backend()
    comp = log_ref.comp_cls
    total = 40
    done = asyncio.Event()

    async def write():
        for _ in range(total // 2):
            await _insert(backend, comp, _log(comp, 7), _log(comp, 7))
        done.set()

    async def read():
        reads = 0
        while not done.is_set() or reads == 0:
            rows = await backend.servant.range(
                log_ref, "seq", -np.inf, np.inf, limit=-1, group={"owner": 7}
            )
            seqs = [int(seq) for seq in rows.seq]
            assert seqs == list(range(1, len(seqs) + 1))
            reads += 1
            await asyncio.sleep(0)
        return reads

    _none, reads = await asyncio.gather(write(), read())
    assert reads > 0
    await backend.wait_for_synced()
    assert len(await _seqs(backend, comp, owner=7)) == total


async def test_consumers_share_cursor_exactly_once(new_component_env, mod_auto_backend):
    """
    生产者并发写，两个消费方共用一个游标按 §9 的写法消费：每一行正好处理一次（处理记录有 unique，
    重复处理会撞车），没有被越过的
    """
    backend: Backend = mod_auto_backend()
    log = def_log()
    create_ref(log, backend)
    cursor_comp, done, _hot = _def_consumer_tables()
    create_ref(cursor_comp, backend)
    create_ref(done, backend)
    total = 30

    async def produce():
        for _ in range(total // 3):
            await _insert(backend, log, _log(log, 7), _log(log, 7), _log(log, 7))
            await asyncio.sleep(0)

    async def consume():
        positions: list[int] = [0]

        async def txn():
            positions.append(
                await _consume_once(backend, log, cursor_comp, done, owner=7)
            )

        while positions[-1] < total:
            await _until_committed(txn)
            await asyncio.sleep(0)

    await asyncio.gather(produce(), consume(), consume())
    await backend.wait_for_synced()
    assert await _done_seqs(backend, done) == list(range(1, total + 1))


async def test_consumer_recovers_row_missed_by_fetch(
    new_component_env, mod_auto_backend, monkeypatch
):
    """
    取行落到进度不同的副本（这里模拟成取行时读不到中间那一行）：phantom_check=False 时 range 静默
    跳过它，§9 的写法发现缺号后 get 回来补上处理，不会越过
    """
    backend: Backend = mod_auto_backend()
    log = def_log()
    create_ref(log, backend)
    cursor_comp, done, _hot = _def_consumer_tables()
    create_ref(cursor_comp, backend)
    create_ref(done, backend)
    ids = await _insert(backend, log, *(_log(log, 7) for _ in range(4)))
    dropped = {ids[2]}  # 3 号

    def drop_once(client):
        original = client.get_many_array_

        async def patched(ref, row_ids):
            rows, missing = await original(ref, row_ids)
            if ref.comp_cls is log and dropped & set(row_ids):
                lost = dropped.pop()
                rows = rows[rows["id"] != lost]
                missing = [*missing, lost]
            return rows, missing

        monkeypatch.setattr(client, "get_many_array_", patched)

    for client in {id(c): c for c in (backend.master, backend.servant)}.values():
        drop_once(client)
    assert await _consume_once(backend, log, cursor_comp, done, owner=7, limit=10) == 4
    assert not dropped  # 确实漏读过一次
    assert await _done_seqs(backend, done) == [1, 2, 3, 4]


async def test_consumer_skips_permanent_gap(new_component_env, mod_auto_backend):
    """删掉中间的一行留下永久缺口：§9 的写法 get 确认它在 master 上不存在之后跳过，提交成功"""
    backend: Backend = mod_auto_backend()
    log = def_log()
    create_ref(log, backend)
    cursor_comp, done, _hot = _def_consumer_tables()
    create_ref(cursor_comp, backend)
    create_ref(done, backend)
    await _insert(backend, log, *(_log(log, 7) for _ in range(5)))
    await _delete_seqs(backend, log, 7, (3,))
    assert await _consume_once(backend, log, cursor_comp, done, owner=7, limit=10) == 5
    assert await _done_seqs(backend, done) == [1, 2, 4, 5]


async def test_consumer_gap_confirm_on_lagging_replica_races(
    new_component_env, mod_auto_backend, monkeypatch
):
    """
    缺号确认这一步本身也可能落到落后的副本、读空，而 master 上其实有这一行：提交时的 CNT 对不上，
    必须判竞态，不能把这一号跳过去。重试之后照常处理
    """
    backend: Backend = mod_auto_backend()
    log = def_log()
    create_ref(log, backend)
    cursor_comp, done, _hot = _def_consumer_tables()
    create_ref(cursor_comp, backend)
    create_ref(done, backend)
    ids = await _insert(backend, log, *(_log(log, 7) for _ in range(4)))
    dropped = {ids[2]}  # 第一次读取行时漏掉 3 号
    stale = [True]  # 确认缺号的那次读，假装副本还没有 3 号

    def patch(client):
        fetch, read = client.get_many_array_, client.range_read_

        async def patched_fetch(ref, row_ids):
            rows, missing = await fetch(ref, row_ids)
            if ref.comp_cls is log and dropped & set(row_ids):
                lost = dropped.pop()
                rows, missing = rows[rows["id"] != lost], [*missing, lost]
            return rows, missing

        async def patched_read(ref, index_name, left, right, limit, desc, **kwargs):
            row_ids, obs = await read(
                ref, index_name, left, right, limit, desc, **kwargs
            )
            if ref.comp_cls is log and limit == -1 and stale and stale.pop():
                del obs.ids[:], obs.members[:]
                row_ids = []
            return row_ids, obs

        monkeypatch.setattr(client, "get_many_array_", patched_fetch)
        monkeypatch.setattr(client, "range_read_", patched_read)

    for client in {id(c): c for c in (backend.master, backend.servant)}.values():
        patch(client)
    with pytest.raises(RaceCondition):
        await _consume_once(backend, log, cursor_comp, done, owner=7, limit=10)
    assert await _done_seqs(backend, done) == []
    assert await _consume_once(backend, log, cursor_comp, done, owner=7, limit=10) == 4
    assert await _done_seqs(backend, done) == [1, 2, 3, 4]


async def test_consumer_large_gap_is_one_read(
    new_component_env, mod_auto_backend, monkeypatch
):
    """
    头部裁掉一大段之后，新消费方（游标为 0）确认这一整段缺口只读一次：每段缺口一次闭区间读、提交时
    一条 CNT，代价不随缺口长度增长
    """
    backend: Backend = mod_auto_backend()
    log = def_log()
    create_ref(log, backend)
    cursor_comp, done, _hot = _def_consumer_tables()
    create_ref(cursor_comp, backend)
    create_ref(done, backend)
    await _insert(backend, log, *(_log(log, 7) for _ in range(203)))
    await _delete_seqs(backend, log, 7, range(1, 201))  # 裁掉头部 1..200
    calls = []

    def count(client):
        read = client.range_read_

        async def counted(ref, *args, **kwargs):
            if ref.comp_cls is log:
                calls.append(args[:2])
            return await read(ref, *args, **kwargs)

        monkeypatch.setattr(client, "range_read_", counted)

    for client in {id(c): c for c in (backend.master, backend.servant)}.values():
        count(client)
    assert (
        await _consume_once(backend, log, cursor_comp, done, owner=7, limit=10) == 203
    )
    assert len(calls) == 2  # 往后读一次、确认 1..200 这段缺口一次
    assert await _done_seqs(backend, done) == [201, 202, 203]


# ---------------------------------------------------------------- 实现审核补充


async def test_nx_and_cnt_failures_consume_no_number(log_ref, mod_auto_backend):
    """主键撞车（NX）、区间被并发插入（CNT）的提交失败，都不消耗号"""
    backend: Backend = mod_auto_backend()
    comp = log_ref.comp_cls
    (row_id,) = await _insert(backend, comp, _log(comp, 7))
    with pytest.raises(UniqueViolation, match="Key already exists"):
        await _insert(backend, comp, _log(comp, 7, id=row_id))
    assert _counter(backend, log_ref, owner=7) == 1
    with pytest.raises(RaceCondition, match="Range changed"):
        async with backend.session("pytest", 1) as session:
            repo = session.using(comp)
            await repo.range(seq=(1, np.inf), owner=7, limit=-1)
            await _insert(backend, comp, _log(comp, 7))  # 并发插进这个区间
            await repo.insert(_log(comp, 7))
    assert _counter(backend, log_ref, owner=7) == 2
    assert [seq for seq, _kind in await _seqs(backend, comp, owner=7)] == [1, 2]


async def test_payload_rejects_group_move_bypassing_repo(log_ref, mod_auto_backend):
    """绕过 repo 改有号行的分组列：提交时 payload 兜底报错，什么都不写"""
    backend: Backend = mod_auto_backend()
    comp = log_ref.comp_cls
    (row_id,) = await _insert(backend, comp, _log(comp, 7))
    with pytest.raises(ValueError, match="commit_seq 列"):
        async with backend.session("pytest", 1) as session:
            row = await session.using(comp).get(id=row_id)
            row.owner = 8
            session.idmap.update(log_ref, row)
    await backend.wait_for_synced()
    assert await _seqs(backend, comp, owner=7) == [(1, 0)]
    assert await _seqs(backend, comp, owner=8) == []


async def test_pending_insert_cannot_set_seq_by_update(log_ref, mod_auto_backend):
    """本事务 insert 的行也不能用 update 改号"""
    backend: Backend = mod_auto_backend()
    comp = log_ref.comp_cls
    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        row = _log(comp, 7)
        await repo.insert(row)
        row.seq = 5
        with pytest.raises(ValueError, match="不能修改"):
            await repo.update(row)
        session.discard()


async def test_two_seq_tables_in_one_commit(log_ref, mod_auto_backend):
    """同一次提交里两张提交序号表（字段名、分组值都相同）各自计数"""
    from hetu.data import BaseComponent, Permission, define_component, property_field

    @define_component(namespace="pytest", permission=Permission.USER, force=True)
    class Audit(BaseComponent):
        owner: np.int64 = property_field(0, index=True)
        seq: np.int64 = property_field(0, commit_seq=True, groupby="owner")

    backend: Backend = mod_auto_backend()
    comp = log_ref.comp_cls
    audit_ref = create_ref(Audit, backend)
    await _insert(backend, comp, _log(comp, 7), _log(comp, 7))
    async with backend.session("pytest", 1) as session:
        await session.using(comp).insert(_log(comp, 7))
        row = Audit.new_row()
        row.owner = 7
        await session.using(Audit).insert(row)
    await backend.wait_for_synced()
    assert _counter(backend, log_ref, owner=7) == 3
    assert _counter(backend, audit_ref, owner=7) == 1


async def test_headless_session_assigns_seq(log_ref, mod_auto_backend):
    """headless 客户端的写入走同一条提交路径，照常发号"""
    from hetu import headless

    backend: Backend = mod_auto_backend()
    comp = log_ref.comp_cls
    client = await headless.HeadlessClient.from_backend(backend, "pytest", [comp])
    try:
        async with client.session(comp) as session:
            row = _log(comp, 7)
            row.id = 777
            await session[comp].insert(row)
    finally:
        await client.close()
    await backend.wait_for_synced()
    assert await _seqs(backend, comp, owner=7) == [(1, 0)]


async def test_unnumbered_rows_on_the_index(log_ref, mod_auto_backend):
    """
    库里号为 0 的历史行和本事务 insert 的行同组时，get(seq=0) 返回的是历史行；负号的历史行也
    可以换组
    """
    from fixtures.backends import raw_hset

    backend: Backend = mod_auto_backend()
    comp = log_ref.comp_cls
    zero_id, negative_id = await _insert(backend, comp, _log(comp, 7), _log(comp, 7))
    raw_hset(backend, log_ref, zero_id, seq="0")
    raw_hset(backend, log_ref, negative_id, seq="-3")
    backend.get_table_maintenance().do_rebuild_index_(log_ref)
    await backend.wait_for_synced()
    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        await repo.insert(_log(comp, 7))
        assert (await repo.get(seq=0, owner=7)).id == zero_id
        old = await repo.get(id=negative_id)
        old.owner = 9
        await repo.update(old)
    await backend.wait_for_synced()
    assert [seq for seq, _kind in await _seqs(backend, comp, owner=9)] == [-3]
    assert [seq for seq, _kind in await _seqs(backend, comp, owner=7)] == [0, 3]
