"""
提交序号列（`property_field(commit_seq=True)`）：定义校验、提交时发号（两个后端的提交脚本）、
禁写与分组列锁定、事务内看不到未分配号的行、迁移与重建时的计数器、消费方的推荐写法。
设计稿见 docs/superpowers/specs/2026-11-09-commit-seq-design.md。
"""

import itertools
import json
import logging

import numpy as np
import pytest
from fixtures.backends import (
    raw_index_members,
    raw_kv_get,
    raw_kv_keys,
    raw_kv_set,
    raw_seq_counter_key,
)
from fixtures.testdata import create_ref

from hetu.common.snowflake_id import SnowflakeID
from hetu.data.backend import Backend, TableReference, UniqueViolation
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
    member 都没写；计数器到了 2^53 同样不发
    """
    backend: Backend = mod_auto_backend()
    comp = log_ref.comp_cls
    await _insert(backend, comp, _log(comp, 7))
    raw_kv_set(backend, raw_seq_counter_key(backend, log_ref, owner=8), b"abc")
    with pytest.raises(RuntimeError, match="SEQ") as excinfo:
        await _insert(backend, comp, _log(comp, 7), _log(comp, 8))
    assert "not an integer" in str(excinfo.value)
    assert _counter(backend, log_ref, owner=7) == 1
    assert await _seqs(backend, comp, owner=7) == [(1, 0)]

    raw_kv_set(
        backend, raw_seq_counter_key(backend, log_ref, owner=8), b"%d" % (2**53 - 1)
    )
    with pytest.raises(RuntimeError, match="Counter overflow"):
        await _insert(backend, comp, _log(comp, 7), _log(comp, 8))
    assert _counter(backend, log_ref, owner=7) == 1
    assert _counter(backend, log_ref, owner=8) == 2**53 - 1
    assert await _seqs(backend, comp, owner=8) == []


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
    assert payload[5] == []
    assert [cmd[0] for cmd in payload[1]] == ["HSET", "ZADD", "ZADD"]


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
