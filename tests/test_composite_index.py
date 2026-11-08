"""
复合索引（`property_field(groupby=...)`）：定义校验、member 编码的保序与可切分、分组值的解析，
以及在各后端上的组内查询、组内 unique、读己之写、防幻读、维护（重建索引、迁移、headless 比对）。
设计稿见 docs/superpowers/specs/2026-11-08-composite-index-design.md。
"""

import copy
import itertools
import json
import random
from contextlib import ExitStack
from typing import cast

import numpy as np
import pytest
from fixtures.backends import (
    raw_hset,
    raw_index_members,
    raw_index_zset_keys,
    raw_key_count,
    raw_zadd,
    use_redis_backend_only,
    use_redis_family_backend_only,
)
from fixtures.contexts import admin_ctx_
from fixtures.read_counts import count_reads
from fixtures.testdata import create_ref

from hetu.common.snowflake_id import SnowflakeID
from hetu.data.backend import (
    Backend,
    InconsistentRangeRead,
    RaceCondition,
    RowFormat,
    Table,
    TableReference,
    UniqueViolation,
)
from hetu.data.backend.redis_model import RedisModelClient
from hetu.data.sub import SubscriptionBroker

SnowflakeID().init(1, 0)

I64_MIN, I64_MAX = -(2**63), 2**63 - 1


def def_slot():
    from hetu.data import BaseComponent, Permission, define_component, property_field

    global Slot

    @define_component(namespace="pytest", permission=Permission.USER, force=True)
    class Slot(BaseComponent):
        owner: np.int64 = property_field(0, index=True)
        zone: np.int32 = property_field(0)
        server: "U8" = property_field("")  # type: ignore  # noqa
        # 每个 zone 里按 level 排序
        level: np.int32 = property_field(0, groupby="zone")
        # 每个 owner 的背包格子组内唯一
        slot: np.int16 = property_field(0, unique=True, groupby="owner")
        # 字符串分组列、两个分组列
        name: "U8" = property_field("", unique=True, groupby=("server", "zone"))  # type: ignore  # noqa
        score: np.float64 = property_field(0.0, groupby="server")
        qty: np.int32 = property_field(0)

    return Slot


def def_buff():
    from hetu.data import BaseComponent, Permission, define_component, property_field

    global Buff

    @define_component(
        namespace="pytest", permission=Permission.USER, volatile=True, force=True
    )
    class Buff(BaseComponent):
        zone: np.int32 = property_field(0)
        level: np.int32 = property_field(0, groupby="zone")
        hp: np.int32 = property_field(0)

    return Buff


@pytest.fixture
async def slot_ref(new_component_env, mod_auto_backend) -> TableReference:
    return create_ref(def_slot(), mod_auto_backend())


_serial = itertools.count(1)


def _slot(comp, **fields):
    """造一行 Slot：slot（组内 unique）、name（组内 unique）默认给不重复的值"""
    row = comp.new_row()
    n = next(_serial)
    row.slot, row.name = n, f"n{n}"
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


# ---------------------------------------------------------------- 定义


def test_define_groupby(new_component_env):
    """groupby 隐含 index；类属性、schema json 的形状；"zone" 与 ("zone",) 是同一个定义"""
    from hetu.data import BaseComponent, define_component, property_field

    comp = def_slot()
    assert comp.index_groups_["level"] == ("zone",)
    assert comp.index_groups_["name"] == ("server", "zone")
    assert comp.index_groups_["owner"] == ()
    assert comp.index_groups_["id"] == ()
    assert "level" in comp.indexes_ and "name" in comp.uniques_
    # 列 → 用到它的索引
    assert set(comp.column_indexes_["zone"]) == {"level", "name"}
    assert set(comp.column_indexes_["owner"]) == {"owner", "slot"}
    assert "qty" not in comp.column_indexes_
    props = json.loads(comp.json_)["properties"]
    assert props["level"]["groupby"] == ["zone"]
    assert props["name"]["groupby"] == ["server", "zone"]
    # 没用 groupby 的列不写这个键：没用复合索引的组件 schema 版本不变
    assert "groupby" not in props["owner"]
    # load_json 回来是元组
    again = BaseComponent.load_json(comp.json_)
    assert again.index_groups_ == comp.index_groups_

    def define(groupby):
        @define_component(namespace="pytest", force=True)
        class G(BaseComponent):
            zone: np.int32 = property_field(0)
            level: np.int32 = property_field(0, groupby=groupby)

        return G

    assert define("zone").json_ == define(("zone",)).json_ == define(["zone"]).json_


@pytest.mark.parametrize(
    "fields, match",
    [
        ({"level": ("level", "zone")}, "不能按自己分组"),
        ({"level": ("id",)}, "不能是id"),
        ({"level": ("nope",)}, "没有字段`nope`"),
        ({"level": ("zone", "zone")}, "重复"),
        ({"level": ("ratio",)}, "只能是整数、str、bytes"),
        ({"level": ("span",)}, "只能是整数、str、bytes"),
    ],
)
def test_define_groupby_errors(new_component_env, fields, match):
    """分组列必须是本组件的其他字段，类型只能是整数、str、bytes（浮点、timedelta64 不行）"""
    from hetu.data import BaseComponent, define_component, property_field

    with pytest.raises(ValueError, match=match):

        @define_component(namespace="pytest", force=True)
        class Bad(BaseComponent):
            zone: np.int32 = property_field(0)
            ratio: np.float32 = property_field(0)
            span: "m8[s]" = property_field(0)  # type: ignore  # noqa
            level: np.int32 = property_field(0, groupby=fields["level"])


def test_define_groupby_point_sub_ignored(new_component_env, caplog):
    """
    复合索引用不上 point_sub（每组一个 zset，按组订阅本来就只被本组叫醒）：写了警告一次并当作
    没写，不进 point_subs_、schema 里也是 False。本字段或分组列是 hidden 时也一样，不报错
    """
    import logging

    from hetu.data import BaseComponent, define_component, property_field

    with caplog.at_level(logging.WARNING, logger="HeTu.root"):

        @define_component(namespace="pytest", force=True)
        class Rank(BaseComponent):
            zone: np.int32 = property_field(0, hidden=True)
            level: np.int32 = property_field(0, groupby="zone", point_sub=True)
            power: np.int32 = property_field(
                0, hidden=True, groupby="zone", point_sub=True
            )
            owner: np.int64 = property_field(0, point_sub=True)

    assert caplog.text.count("point_sub用不上") == 2
    assert Rank.point_subs_ == {"owner"}
    assert Rank.index_groups_["level"] == ("zone",)
    assert json.loads(Rank.json_)["properties"]["level"]["point_sub"] is False


# ---------------------------------------------------------------- 编码


def def_codecs():
    """几种分组列 / 本字段的类型组合"""
    from hetu.data import BaseComponent, define_component, property_field

    @define_component(namespace="pytest", force=True)
    class CodecA(BaseComponent):
        g: np.int64 = property_field(0)
        f: "U3" = property_field("", groupby="g")  # type: ignore  # noqa

    @define_component(namespace="pytest", force=True)
    class CodecB(BaseComponent):
        g: "U3" = property_field("")  # type: ignore  # noqa
        f: np.float64 = property_field(0.0, groupby="g")

    @define_component(namespace="pytest", force=True)
    class CodecC(BaseComponent):
        g: "S3" = property_field(b"")  # type: ignore  # noqa
        h: "U3" = property_field("")  # type: ignore  # noqa
        f: np.int16 = property_field(0, groupby=("g", "h"))

    @define_component(namespace="pytest", force=True)
    class CodecD(BaseComponent):
        g: np.uint64 = property_field(0)
        h: "S3" = property_field(b"")  # type: ignore  # noqa
        f: "S3" = property_field(b"", groupby=("h", "g"))  # type: ignore  # noqa

    return CodecA, CodecB, CodecC, CodecD


_POOLS = {
    "i": [I64_MIN, -1, 0, 1, 2**31, I64_MAX],
    "i2": [-32768, -1, 0, 1, 32767],
    "u": [0, 1, 255, 2**63, 2**64 - 1],
    "U": ["", "a", "a\x00b", "ab", "\x00a", "\x01", "ÿ", "\U0001f600", "b"],
    "S": [b"", b"a", b"a\x00b", b"\xff", b"\xff\x00a", b"\x00\x01", b"ab", b"\x01"],
    "f": [-np.inf, -1.5, -0.0, 0.0, 1e-300, 2.5, np.inf],
}


def _pool(dtype: np.dtype) -> list:
    if dtype.kind == "i":
        return _POOLS["i2"] if dtype.itemsize == 2 else _POOLS["i"]
    return _POOLS[dtype.kind]


def _split_member(comp, index_name: str, member: bytes) -> tuple:
    """按设计稿 §4.1 的规则把 member 切回 (分组值…, 本字段值, id 串)，验证可唯一切分"""
    from hetu.data.backend.base import from_sortable_bytes

    value_seg, _sep, row_id = member.rpartition(b"\x00")
    pos = 0
    values = []
    for col in comp.index_groups_[index_name]:
        dtype = comp.dtype_map_[col]
        if dtype.kind in "iu":
            raw, pos = value_seg[pos : pos + 8], pos + 8
        else:
            buf = bytearray()
            while True:
                byte = value_seg[pos]
                if byte == 0:
                    nxt = value_seg[pos + 1]
                    pos += 2
                    if nxt == 0x01:
                        break
                    assert nxt == 0xFF, member
                    buf += b"\x00\xff"
                else:
                    buf.append(byte)
                    pos += 1
            raw = bytes(buf)
        values.append(from_sortable_bytes(dtype, raw).item())
    values.append(
        from_sortable_bytes(comp.dtype_map_[index_name], value_seg[pos:]).item()
    )
    return (*values, row_id.decode())


def test_composite_member_order_and_split(new_component_env):
    """
    member 的字节序 = (分组值…, 本字段值, id 十进制串) 的逐列字典序，且 member 能唯一切分。
    值覆盖中间 \\x00、\\x01、\\xff 开头、空串、非 BMP 字符、有符号与无符号极值、±inf、-0.0
    """
    from hetu.data.backend.base import index_value_bytes_, index_value_bytes_list_

    rng = random.Random(20261108)
    for comp in def_codecs():
        index_name = "f"
        columns = (*comp.index_groups_[index_name], index_name)
        rows = np.zeros(80, dtype=comp.dtypes)
        rows["id"] = rng.sample(range(1, 10**6), len(rows))
        for col in columns:
            pool = _pool(comp.dtype_map_[col])
            rows[col] = [rng.choice(pool) for _ in range(len(rows))]
        values = index_value_bytes_list_(comp, index_name, rows)
        # 批量版与逐行版一致
        assert values == [index_value_bytes_(comp, index_name, row) for row in rows]
        members = [v + b"\x00" + b"%d" % i for v, i in zip(values, rows["id"].tolist())]
        # 期望的顺序：numpy 存进去之后的值（结尾 \x00 已去掉）按 python 元组比，id 按十进制串
        keys = [
            (*(rows[col][i].item() for col in columns), str(rows["id"][i]))
            for i in range(len(rows))
        ]
        by_member = sorted(range(len(rows)), key=lambda i: members[i])
        by_key = sorted(range(len(rows)), key=lambda i: keys[i])
        assert by_member == by_key, comp.name_
        # 可唯一切分：切回来的值就是存进去的值（-0.0 与 0.0 编码相同，按值比相等）
        for member, key in zip(members, keys):
            assert _split_member(comp, index_name, member) == key


async def test_single_nul_terminator_counterexample(
    new_component_env, mod_auto_backend
):
    """
    设计稿 §4.2 的反例：分组列 name、本字段 x 是整数。查 name="a"、x 不限时，上界的 x 编码全是
    0xff；如果分组列后面用单个 0x00 当终止符，name="a\\x00b" 的行会落进区间。现在两种行分得开
    """
    from hetu.data import BaseComponent, define_component, property_field

    @define_component(namespace="pytest", force=True)
    class Tagged(BaseComponent):
        name: "U8" = property_field("")  # type: ignore  # noqa
        x: np.int64 = property_field(0, groupby="name")

    backend: Backend = mod_auto_backend()
    ref = create_ref(Tagged, backend)
    rows = []
    for name in ("a", "a\x00b", "a\x00", "ab"):
        row = Tagged.new_row()
        row.name, row.x = name, 5
        rows.append(row)
    a_id, a0b_id, a0_id, ab_id = await _insert(backend, Tagged, *rows)
    servant = backend.servant
    got = await servant.range(
        ref, "x", -np.inf, np.inf, -1, row_format=RowFormat.ID_LIST, group={"name": "a"}
    )
    # "a\x00" 结尾的 \x00 被 numpy 当补齐去掉，存的就是 "a"
    assert sorted(got) == sorted([a_id, a0_id])
    got = await servant.range(
        ref,
        "x",
        -np.inf,
        np.inf,
        -1,
        row_format=RowFormat.ID_LIST,
        group={"name": "a\x00b"},
    )
    assert got == [a0b_id]
    got = await servant.range(
        ref, "x", 5, None, -1, row_format=RowFormat.ID_LIST, group={"name": "ab"}
    )
    assert got == [ab_id]


# ---------------------------------------------------------------- 分组值的解析


def test_resolve_group(new_component_env):
    """分组值按列类型规范化，不能精确表示的一律 ValueError，不会静默回绕或截错组"""
    from hetu.data.backend.base import resolve_group_

    comp = def_slot()
    assert resolve_group_(comp, "level", {"zone": 3}) == (3,)
    assert resolve_group_(comp, "level", {"zone": np.int64(3)}) == (3,)
    assert resolve_group_(comp, "level", {"zone": 3.0}) == (3,)
    assert resolve_group_(comp, "level", {"zone": True}) == (1,)
    # 数字字符串和单列索引的查询值一样解析：先按整数、不行再按小数
    for text in ("3", "3.0", " 3 ", b"3", np.str_("3")):
        assert resolve_group_(comp, "level", {"zone": text}) == (3,), text
    assert resolve_group_(comp, "level", {"zone": "-2147483648"}) == (-(2**31),)
    # 字符串按存储规则：截断到列宽、去掉结尾的 \x00
    assert resolve_group_(comp, "name", {"server": "s1\x00", "zone": 1}) == ("s1", 1)
    assert resolve_group_(comp, "name", {"server": "abcdefghij", "zone": 1}) == (
        "abcdefgh",
        1,
    )
    # 单列索引
    assert resolve_group_(comp, "owner", None) == ()
    assert resolve_group_(comp, "owner", {}) == ()

    bad = [
        ("level", None, "要同时给出"),
        ("level", {}, "要同时给出"),
        ("level", {"zone": 2**31}, "超出"),  # int32 越界，不回绕
        ("level", {"zone": np.int64(2**40)}, "超出"),
        ("level", {"zone": 3.5}, "不是整数"),
        ("level", {"zone": np.nan}, "不是整数"),
        ("level", {"zone": "3.5"}, "不是整数"),
        ("level", {"zone": "nan"}, "不是整数"),
        ("level", {"zone": "inf"}, "不是整数"),
        ("level", {"zone": str(2**31)}, "超出"),
        ("level", {"zone": "abc"}, "不是数字"),
        ("level", {"zone": ""}, "不是数字"),
        ("level", {"zone": "(3"}, "不是数字"),  # 开闭区间的前缀，分组值用不上
        ("level", {"zone": "[3"}, "不是数字"),
        ("level", {"zone": b"\xff"}, "不是数字"),
        ("level", {"zone": None}, "只收数字"),
        ("level", {"zone": [3]}, "只收数字"),
        ("level", {"zone": 1, "owner": 2}, "正好给出"),
        ("level", {1: 3}, "正好给出"),  # 键不是 str
        ("level", [("zone", 3)], "dict"),
        ("name", {"server": b"s1", "zone": 1}, "只收 str"),
        ("name", {"server": "s1"}, "正好给出"),
        ("owner", {"zone": 1}, "不是复合索引"),
        ("nope", None, "没有叫"),
    ]
    for index_name, group, match in bad:
        with pytest.raises(ValueError, match=match):
            resolve_group_(comp, index_name, group)


async def test_numeric_string_group_values(slot_ref, mod_auto_backend):
    """
    整数分组列收数字字符串（同单列索引）：range、组内 unique 点查、upsert 都落到数值对应的组，
    upsert 新建的行里写的是数值
    """
    backend: Backend = mod_auto_backend()
    comp = slot_ref.comp_cls
    (row_id,) = await _insert(
        backend, comp, _slot(comp, owner=7, slot=1, zone=1, level=5)
    )
    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        assert list((await repo.range(level=(0, 10), zone="1")).id) == [row_id]
        got = await repo.range("level", 0, 10, _group={"zone": "1.0"})
        assert list(got.id) == [row_id]
        assert (await repo.get(slot=1, owner="7")).id == row_id
        assert await repo.get(slot=2, owner="7") is None
        async with repo.upsert(slot=2, owner="7") as row:
            row.level, row.name = 6, "upserted"
    await backend.wait_for_synced()
    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        row = await repo.get(slot=2, owner=7)
        assert row is not None and row.owner == 7 and row.level == 6


# ---------------------------------------------------------------- 查询


async def test_range_within_group(slot_ref, mod_auto_backend):
    """组内区间：只返回这一组的行，按本字段排序；limit、desc、开区间、位置参数写法、servant、
    只读索引、维护接口都带 group"""
    backend: Backend = mod_auto_backend()
    comp = slot_ref.comp_cls
    levels = {1: [30, 10, 20], 2: [15, 25], 3: []}
    rows = [_slot(comp, zone=z, level=lv) for z, lvs in levels.items() for lv in lvs]
    await _insert(backend, comp, *rows)

    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        got = await repo.range(level=(-np.inf, np.inf), zone=1, limit=-1)
        assert list(got.level) == [10, 20, 30]
        assert set(got.zone) == {1}
        got = await repo.range(level=(0, 100), zone=1, limit=2, desc=True)
        assert list(got.level) == [30, 20]
        got = await repo.range(level=("(10", 30), zone=1, limit=-1)
        assert list(got.level) == [20, 30]
        got = await repo.range("level", 0, 100, limit=-1, _group={"zone": 2})
        assert list(got.level) == [15, 25]
        assert len(await repo.range(level=(0, 100), zone=3, limit=-1)) == 0
        # 点查
        assert list((await repo.range(level=(25, 25), zone=2)).level) == [25]
        assert (await repo.get(level=20, zone=1)).level == 20
        assert await repo.get(level=20, zone=2) is None

    servant = backend.servant
    got = await servant.range(slot_ref, "level", 0, 100, -1, group={"zone": 1})
    assert list(got.level) == [10, 20, 30]
    pairs = await servant.range_index_(slot_ref, "level", 0, 100, -1, group={"zone": 2})
    assert [int(v) for v, _id in pairs] == [15, 25]
    maint = backend.get_table_maintenance()
    assert len(maint.range(slot_ref, "level", 0, 100, limit=-1, group={"zone": 1})) == 3


async def test_query_errors(slot_ref, mod_auto_backend):
    """复合索引漏给、多给分组值，单列索引给了分组值，kwargs 匹配到多个 / 0 个索引都报 ValueError；
    后端接口不经过 repo 也拦得住（不会用空前缀查到别的组）"""
    backend: Backend = mod_auto_backend()
    comp = slot_ref.comp_cls
    await _insert(backend, comp, _slot(comp, zone=1, level=1))
    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        with pytest.raises(ValueError, match="分组"):
            await repo.range(level=(0, 10))
        with pytest.raises(ValueError, match="正好给出"):
            await repo.range(level=(0, 10), zone=1, owner=2)
        with pytest.raises(ValueError, match="不是复合索引"):
            await repo.range(owner=(0, 10), zone=1)
        with pytest.raises(ValueError, match="分组"):
            await repo.get(level=1)
        with pytest.raises(ValueError, match="对不上任何索引"):
            await repo.get(level=1, owner=2)
        with pytest.raises(ValueError, match="只有一个"):
            await repo.range(zone=1, owner=2)
        # 长度为 2 的字符串分组值不会被当成区间
        got = await repo.range(name=("n0", "n9999"), server="s1", zone=1, limit=-1)
        assert len(got) == 0
    with pytest.raises(ValueError, match="分组"):
        await backend.servant.range(slot_ref, "level", 0, 10)
    with pytest.raises(ValueError, match="分组"):
        backend.servant.check_range_(slot_ref, "level", 0, 10, 10, False)


async def test_kwargs_edge_cases(new_component_env, mod_auto_backend):
    """
    kwargs 写法的几处边角：upsert 只在 unique 索引里匹配；只给一个 kwarg 时可以用 _group 给
    分组值，kwargs 和 _group 同时给分组值报错（不静默丢掉一边）；区间可以是两项的 ndarray
    """
    from hetu.data import BaseComponent, define_component, property_field

    @define_component(namespace="pytest", force=True)
    class Seat(BaseComponent):
        # owner 按 slot 分组（非 unique）、slot 按 owner 分组（组内唯一）：键集合 {slot, owner}
        # 两个索引都能匹配，upsert 只认 unique 的那个
        owner: np.int64 = property_field(0, groupby="slot")
        slot: np.int16 = property_field(0, unique=True, groupby="owner")
        rank: np.int32 = property_field(0, groupby="owner")

    backend: Backend = mod_auto_backend()
    create_ref(Seat, backend)
    async with backend.session("pytest", 1) as session:
        repo = session.using(Seat)
        async with repo.upsert(slot=1, owner=7) as row:
            row.rank = 3
    await backend.wait_for_synced()
    async with backend.session("pytest", 1) as session:
        repo = session.using(Seat)
        assert (await repo.get(slot=1, _group={"owner": 7})).rank == 3
        got = await repo.range(rank=(0, 10), _group={"owner": 7})
        assert list(got.slot) == [1]
        got = await repo.range(rank=np.array([0, 10]), owner=7)
        assert list(got.slot) == [1]
        with pytest.raises(ValueError, match="_group"):
            await repo.range(rank=(0, 10), owner=7, _group={"owner": 8})
        with pytest.raises(ValueError, match="_group"):
            await repo.get(slot=1, owner=7, _group={"owner": 8})


async def test_kwargs_matching_two_indexes(new_component_env, mod_auto_backend):
    """a 按 b 分组、b 又按 a 分组时，get(a=, b=) 两个都能匹配，要用位置参数写法指定"""
    from hetu.data import BaseComponent, define_component, property_field

    @define_component(namespace="pytest", force=True)
    class Cross(BaseComponent):
        a: np.int32 = property_field(0, groupby="b")
        b: np.int32 = property_field(0, groupby="a")

    backend: Backend = mod_auto_backend()
    create_ref(Cross, backend)
    row = Cross.new_row()
    row.a, row.b = 1, 2
    await _insert(backend, Cross, row)
    async with backend.session("pytest", 1) as session:
        repo = session.using(Cross)
        with pytest.raises(ValueError, match="同时匹配"):
            await repo.get(a=1, b=2)
        assert (await repo.get("a", 1, _group={"b": 2})).id == row.id
        assert (await repo.get("b", 2, _group={"a": 1})).id == row.id


# ---------------------------------------------------------------- 写入


async def test_update_group_moves_index(slot_ref, mod_auto_backend):
    """改分组列：行从旧组搬到新组，索引里只留新位置；只改不在索引里的列不动索引"""
    backend: Backend = mod_auto_backend()
    comp = slot_ref.comp_cls
    (row_id,) = await _insert(backend, comp, _slot(comp, zone=1, level=7))
    before = raw_index_members(backend, slot_ref, "level")

    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        row = await repo.get(id=row_id)
        row.qty = 5
        await repo.update(row)
    assert raw_index_members(backend, slot_ref, "level") == before

    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        row = await repo.get(id=row_id)
        row.zone = 2
        await repo.update(row)
    await backend.wait_for_synced()
    assert len(raw_index_members(backend, slot_ref, "level")) == 1
    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        assert len(await repo.range(level=(0, 100), zone=1, limit=-1)) == 0
        assert list((await repo.range(level=(0, 100), zone=2, limit=-1)).id) == [row_id]

    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        assert await repo.get(id=row_id) is not None
        repo.delete(row_id)
    assert raw_index_members(backend, slot_ref, "level") == []


async def test_own_writes_within_group(slot_ref, mod_auto_backend):
    """事务内：insert 进组、从别的组换进来、换出去、删掉，range 按本事务眼里的数据返回"""
    backend: Backend = mod_auto_backend()
    comp = slot_ref.comp_cls
    stay, leave, joiner = await _insert(
        backend,
        comp,
        _slot(comp, zone=1, level=10),
        _slot(comp, zone=1, level=20),
        _slot(comp, zone=2, level=15),
    )
    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        new = _slot(comp, zone=1, level=5)
        await repo.insert(new)
        row = await repo.get(id=leave)
        row.zone = 2
        await repo.update(row)
        row = await repo.get(id=joiner)
        row.zone = 1
        await repo.update(row)
        got = await repo.range(level=(0, 100), zone=1, limit=-1)
        assert list(got.id) == [new.id, stay, joiner]
        got = await repo.range(level=(0, 100), zone=1, limit=2, desc=True)
        assert list(got.id) == [joiner, stay]
        got = await repo.range(level=(0, 100), zone=2, limit=-1)
        assert list(got.id) == [leave]
        repo.delete(stay)
        got = await repo.range(level=(0, 100), zone=1, limit=-1)
        assert list(got.id) == [new.id, joiner]


async def test_phantom_check_is_group_scoped(slot_ref, mod_auto_backend):
    """防幻读的区间限定在组内：别的组插入不让本组的读竞态，本组插入照样竞态"""
    backend: Backend = mod_auto_backend()
    comp = slot_ref.comp_cls
    (row_id,) = await _insert(backend, comp, _slot(comp, zone=1, level=10))

    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        rows = await repo.range(level=(0, 100), zone=1, limit=-1)
        async with backend.session("pytest", 1) as other:
            await other.using(comp).insert(_slot(comp, zone=2, level=50))
        row = rows[0]
        row.qty += 1
        await repo.update(row)

    with pytest.raises(RaceCondition):
        async with backend.session("pytest", 1) as session:
            repo = session.using(comp)
            rows = await repo.range(level=(0, 100), zone=1, limit=-1)
            async with backend.session("pytest", 1) as other:
                await other.using(comp).insert(_slot(comp, zone=1, level=50))
            row = rows[0]
            row.qty += 1
            await repo.update(row)
    assert row_id in rows.id


async def test_unique_within_group(slot_ref, mod_auto_backend):
    """组内唯一：不同组可以同值，同组同值 UniqueViolation；upsert、get 按 (组, 值)；读空后
    被并发插入判竞态；negative cache 带着组，别的组不受影响"""
    backend: Backend = mod_auto_backend()
    comp = slot_ref.comp_cls
    await _insert(
        backend,
        comp,
        _slot(comp, server="s1", zone=1, name="x"),
        _slot(comp, server="s1", zone=2, name="x"),
        _slot(comp, server="s2", zone=1, name="x"),
    )
    with pytest.raises(UniqueViolation):
        await _insert(backend, comp, _slot(comp, server="s1", zone=1, name="x"))

    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        assert (await repo.get(name="x", server="s1", zone=2)).zone == 2
        # 读空登记 negative observation，只对这一组
        assert await repo.get(name="y", server="s1", zone=1) is None
        async with repo.upsert(name="y", server="s1", zone=1) as row:
            row.qty = 9
        assert (await repo.get(name="x", server="s2", zone=1)).server == "s2"
    await backend.wait_for_synced()
    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        got = await repo.get(name="y", server="s1", zone=1)
        assert (got.qty, got.server, got.zone) == (9, "s1", 1)
        # upsert 命中已有行就更新
        async with repo.upsert(name="y", server="s1", zone=1) as row:
            row.qty = 10
    await backend.wait_for_synced()
    async with backend.session("pytest", 1) as session:
        assert (await session.using(comp).get(name="y", server="s1", zone=1)).qty == 10

    # 读空之后被并发插入：基于过期快照的决定，判竞态（重试后会读到对方的行）
    with pytest.raises(RaceCondition):
        async with backend.session("pytest", 1) as session:
            repo = session.using(comp)
            assert await repo.get(name="z", server="s1", zone=1) is None
            async with backend.session("pytest", 1) as other:
                await other.using(comp).insert(
                    _slot(comp, server="s1", zone=1, name="z")
                )
            await repo.insert(_slot(comp, server="s1", zone=1, name="z"))

    # 换组撞上新组里的同值
    async with backend.session("pytest", 1) as session:
        row = await session.using(comp).get(name="x", server="s2", zone=1)
    with pytest.raises(UniqueViolation):
        async with backend.session("pytest", 1) as session:
            repo = session.using(comp)
            row = await repo.get(id=row.id)
            row.server = "s1"
            await repo.update(row)


async def test_unique_point_hit_checks_all_columns(slot_ref, mod_auto_backend):
    """
    unique 点查命中缓存的捷径要求数据库态在所有分组列上都相等：本事务把 R 从 (A,"x") 改到
    (B,"x")，库里 B 组已有 S(B,"x")，查 (B,"x") 不能只返回 [R]
    """
    backend: Backend = mod_auto_backend()
    comp = slot_ref.comp_cls
    r_id, s_id = await _insert(
        backend,
        comp,
        _slot(comp, server="s1", zone=1, name="x"),
        _slot(comp, server="s1", zone=2, name="x"),
    )
    with pytest.raises(UniqueViolation):
        async with backend.session("pytest", 1) as session:
            repo = session.using(comp)
            row = await repo.get(id=r_id)
            row.zone = 2
            await repo.update(row)
            got = await repo.range(name=("x", "x"), server="s1", zone=2, limit=-1)
            assert sorted(got.id) == sorted([r_id, s_id])


async def test_is_unique_conflicts_within_group(slot_ref, mod_auto_backend):
    """
    提前检查 is_unique_conflicts 按 (组, 值) 判断：别的组同值不算冲突；本事务已经从组里挪走
    的值也不算，与提交时一致
    """
    backend: Backend = mod_auto_backend()
    comp = slot_ref.comp_cls
    a_id, _b_id = await _insert(
        backend, comp, _slot(comp, owner=8, slot=1), _slot(comp, owner=8, slot=2)
    )
    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        other_group = _slot(comp, owner=9, slot=2)
        assert await repo.is_unique_conflicts(other_group, insert=True) == (None, False)
        row = await repo.get(id=a_id)
        row.slot = 3
        await repo.update(row)
        refill = _slot(comp, owner=8, slot=1)
        assert await repo.is_unique_conflicts(refill, insert=True) == (None, False)
        clash = _slot(comp, owner=8, slot=2)
        assert await repo.is_unique_conflicts(clash, insert=True) == ("slot", False)
        await repo.insert(refill)


@pytest.mark.parametrize("read", ["get", "range"])
async def test_group_move_after_empty_read_is_race(slot_ref, mod_auto_backend, read):
    """
    组内 unique 点查读空之后，又读到并发插进来占着 (组, 值) 的行，把它挪走、再插入这个值：
    读集自相矛盾，要判竞态（单列的见 test_unique_move.py）
    """
    backend: Backend = mod_auto_backend()
    comp = slot_ref.comp_cls
    with pytest.raises(RaceCondition):
        async with backend.session("pytest", 1) as session:
            repo = session.using(comp)
            if read == "get":
                assert await repo.get(slot=1, owner=7) is None
            else:
                assert len(await repo.range(slot=(1, 1), owner=7)) == 0
            taken = _slot(comp, owner=7, slot=1)
            async with backend.session("pytest", 1) as other:
                await other.using(comp).insert(taken)
            await backend.wait_for_synced()
            mine = await repo.get(id=taken.id)
            assert mine is not None
            mine.slot = 5
            await repo.update(mine)
            await repo.insert(_slot(comp, owner=7, slot=1))


async def test_move_unique_value_within_group(slot_ref, mod_auto_backend):
    """
    同一事务里在组内的行之间挪 unique 值（设计稿 §5.6，单列的见 test_unique_move.py）：挪走
    之后补位、借中间值互换都能提交；挪到别的行占着、这次又没挪走的值照样 UniqueViolation
    """
    backend: Backend = mod_auto_backend()
    comp = slot_ref.comp_cls
    a_id, b_id = await _insert(
        backend, comp, _slot(comp, owner=7, slot=1), _slot(comp, owner=7, slot=2)
    )
    # 挪走之后补位
    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        row = await repo.get(id=a_id)
        row.slot = 3
        await repo.update(row)
        await repo.insert(_slot(comp, owner=7, slot=1))
    # 借中间值互换
    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        a, b = await repo.get(id=a_id), await repo.get(id=b_id)
        a.slot = 99
        await repo.update(a)
        b.slot = 3
        await repo.update(b)
        a.slot = 2
        await repo.update(a)
    await backend.wait_for_synced()
    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        assert (await repo.get(id=a_id)).slot == 2
        assert (await repo.get(id=b_id)).slot == 3
    # 挪到别的行占着的值：B 没在本事务里，本地拦不住，提交时判冲突
    with pytest.raises(UniqueViolation):
        async with backend.session("pytest", 1) as session:
            repo = session.using(comp)
            row = await repo.get(id=a_id)
            row.slot = 3
            await repo.update(row)


async def test_direct_set_rejects_index_columns(new_component_env, mod_auto_backend):
    """direct_set 不许写任何索引列，复合索引的分组列也算：直接写会把索引写坏"""
    backend: Backend = mod_auto_backend()
    comp = def_buff()
    ref = create_ref(comp, backend)
    row = comp.new_row()
    (row_id,) = await _insert(backend, comp, row)
    master = backend.master
    for field in ("zone", "level"):
        with pytest.raises(ValueError, match="direct_set"):
            await master.direct_set(ref, row_id, **{field: "3"})
    assert await master.direct_set(ref, row_id, hp="3")


async def test_commit_routes_to_group_keys(slot_ref, mod_auto_backend):
    """
    复合索引每组一个 zset：ZADD / ZREM / UNIQ 都发到行所在组的 key，换组时 ZREM 发到旧组的 key；
    不登记任何频道，提交时不发 PUBLISH。组里最后一行走了 zset 就没了
    """
    backend: Backend = mod_auto_backend()
    comp = slot_ref.comp_cls
    master = cast(RedisModelClient, backend.master)

    def group_key(index_name: str, group: dict) -> str:
        return master.index_zset_key_(
            master.index_key(slot_ref, index_name),
            master.group_prefix_of_(slot_ref, index_name, group),
        )

    level_key = master.index_key(slot_ref, "level")
    zone1, zone2 = group_key("level", {"zone": 1}), group_key("level", {"zone": 2})
    assert zone1 != zone2 and zone1.startswith(level_key + ":")
    assert master.index_channel(slot_ref, "level", group={"zone": 1}) == (
        f"__keyspace@{master.dbi}__:{zone1}"
    )

    payloads: list[list] = []
    original = master.build_commit_payload_

    def capture(idmap):
        keys, payload = original(idmap)
        payloads.append(payload)
        return keys, payload

    def level_writes() -> list[tuple[str, str]]:
        """最近一次提交里 level 索引的 (命令, key)"""
        return [
            (cmd[0], cmd[1])
            for cmd in payloads[-1][1]
            if cmd[0] in ("ZADD", "ZREM")
            and (cmd[1] == level_key or cmd[1].startswith(level_key + ":"))
        ]

    master.build_commit_payload_ = capture  # type: ignore
    try:
        (row_id,) = await _insert(backend, comp, _slot(comp, zone=1, level=1))
        assert level_writes() == [("ZADD", zone1)]
        uniq_keys = [check[1] for check in payloads[-1][0] if check[0] == "UNIQ"]
        assert group_key("name", {"server": "", "zone": 1}) in uniq_keys
        assert group_key("slot", {"owner": 0}) in uniq_keys
        assert payloads[-1][4] == []  # 不登记频道
        for change, expect in (
            ({"level": 2}, [("ZREM", zone1), ("ZADD", zone1)]),
            ({"zone": 2}, [("ZREM", zone1), ("ZADD", zone2)]),
            ({"qty": 3}, []),
        ):
            async with backend.session("pytest", 1) as session:
                repo = session.using(comp)
                row = await repo.get(id=row_id)
                for key, value in change.items():
                    row[key] = value
                await repo.update(row)
            assert level_writes() == expect, change
            assert payloads[-1][4] == [], change
        assert raw_index_zset_keys(backend, slot_ref, "level") == [zone2]
        async with backend.session("pytest", 1) as session:
            repo = session.using(comp)
            await repo.get(id=row_id)
            repo.delete(row_id)
        assert level_writes() == [("ZREM", zone2)]
    finally:
        del master.build_commit_payload_
    assert raw_index_zset_keys(backend, slot_ref, "level") == []


async def test_range_read_inconsistent_group(slot_ref, mod_auto_backend):
    """
    读取时索引说行在 1 组、行数据却在 2 组（本字段没变）：只比本字段会放过，按整个值段核对才
    发现得了（设计稿 §5.5）。读取期间行被挪走的情形，这里用只改行、不改索引来造
    """
    backend: Backend = mod_auto_backend()
    comp = slot_ref.comp_cls
    (row_id,) = await _insert(backend, comp, _slot(comp, zone=1, level=10))
    raw_hset(backend, slot_ref, row_id, zone="2")
    await backend.wait_for_synced()
    with pytest.raises(InconsistentRangeRead):
        async with backend.session("pytest", 1) as session:
            repo = session.using(comp)
            rows = await repo.range(level=(0, 100), zone=1, limit=-1)
            assert list(rows.id) == [row_id]
            await repo.insert(_slot(comp, zone=5, level=1))  # 有写入才提交


async def test_refill_group_after_moving_out(slot_ref, mod_auto_backend):
    """把占着 (组, 值) 的行换到别的组，再在原组用这个值插一行：同一事务里能提交（§5.6）"""
    backend: Backend = mod_auto_backend()
    comp = slot_ref.comp_cls
    (r_id,) = await _insert(backend, comp, _slot(comp, server="r", zone=1, name="x"))
    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        row = await repo.get(id=r_id)
        row.zone = 2
        await repo.update(row)
        await repo.insert(_slot(comp, server="r", zone=1, name="x"))
    await backend.wait_for_synced()
    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        assert (await repo.get(name="x", server="r", zone=2)).id == r_id
        assert (await repo.get(name="x", server="r", zone=1)).id != r_id


@use_redis_family_backend_only
async def test_composite_reads_never_touch_master(slot_ref, mod_backend_config):
    """复合索引的事务读、按组订阅同样只走副本，master 一次不读（同 test_master_read_budget）"""
    config = copy.deepcopy(mod_backend_config)
    config["master_weight"] = 0
    backend = Backend(config)
    comp = slot_ref.comp_cls
    backend.post_configure(components=[comp])
    broker = SubscriptionBroker(backend)
    try:
        await _insert(
            backend,
            comp,
            _slot(comp, owner=3, zone=1, level=5, slot=1, server="s", name="m1"),
        )
        with ExitStack() as stack:
            counts = count_reads(stack, backend, include_range=True)
            async with backend.session("pytest", 1) as session:
                repo = session.using(comp)
                assert len(await repo.range(level=(0, 10), zone=1)) == 1
                assert await repo.get(slot=1, owner=3) is not None
                assert await repo.get(name="m1", server="s", zone=1) is not None
                assert await repo.get(name="m2", server="s", zone=1) is None
            sub_id, rows = await broker.subscribe_range(
                slot_ref, admin_ctx_(), "level", 0, 10, group={"zone": 1}
            )
            assert sub_id and len(rows) == 1
            assert counts()["master"] == 0, counts()
            assert counts()["servant"] > 0
    finally:
        await broker.close()
        await backend.close()


# ---------------------------------------------------------------- 维护


async def test_rebuild_index_composite(slot_ref, mod_auto_backend):
    """
    重建出的复合索引 member 与 commit 写入的一致；重建遇到组内 unique 冲突报错，所有索引都
    保留旧的（Redis 分两阶段：全部建好、检查通过才统一换）
    """
    backend: Backend = mod_auto_backend()
    comp = slot_ref.comp_cls
    a_id, b_id = await _insert(
        backend,
        comp,
        _slot(comp, owner=1, zone=1, level=5, slot=1, server="s"),
        _slot(comp, owner=1, zone=2, level=6, slot=2, server="s"),
    )
    maint = backend.get_table_maintenance()
    before = {
        name: raw_index_members(backend, slot_ref, name) for name in comp.indexes_
    }
    keys_before = raw_index_zset_keys(backend, slot_ref, "level")
    assert len(keys_before) == 2  # zone 1、zone 2 各一个 zset
    # 残留：改成复合之前的单个 key、已经没有行的组 key。重建会删掉，raw_index_members 合并所有 key
    level_key = backend.master.index_key(slot_ref, "level")
    raw_zadd(backend, level_key, b"stale\x00999")
    raw_zadd(backend, level_key + ":deadbeef", b"stale\x00998")
    assert maint.do_rebuild_index_(slot_ref) == 2
    for name in comp.indexes_:
        assert raw_index_members(backend, slot_ref, name) == before[name], name
    assert raw_index_zset_keys(backend, slot_ref, "level") == keys_before

    # level 改了值（重建的话 level 索引会变），slot 造出组内重复：重建失败，level 也不换
    raw_hset(backend, slot_ref, a_id, level="50")
    raw_hset(backend, slot_ref, b_id, slot="1")
    with pytest.raises(RuntimeError, match="unique") as excinfo:
        maint.do_rebuild_index_(slot_ref)
    assert "owner" in str(excinfo.value)  # 报错里给出 (分组列…, 本字段)
    for name in comp.indexes_:
        assert raw_index_members(backend, slot_ref, name) == before[name], name
    # 失败的重建不留临时 key：表名下只剩两行和各索引的 zset
    index_keys = sum(
        len(raw_index_zset_keys(backend, slot_ref, name)) for name in comp.indexes_
    )
    assert raw_key_count(backend, slot_ref) == 2 + index_keys


async def test_rebuild_index_empty_table_drops_group_keys(slot_ref, mod_auto_backend):
    """表里一行都没有时，索引里剩下的都是残留：复合索引的组 key、单列格式的 key 都删掉"""
    backend: Backend = mod_auto_backend()
    level_key = backend.master.index_key(slot_ref, "level")
    raw_zadd(backend, level_key, b"stale\x00999")
    raw_zadd(backend, level_key + ":deadbeef", b"stale\x00998")
    maint = backend.get_table_maintenance()
    assert maint.do_rebuild_index_(slot_ref) == 0
    assert raw_index_zset_keys(backend, slot_ref, "level") == []


@use_redis_backend_only
async def test_rebuild_index_swap_failure_keeps_old_keys(
    slot_ref, mod_auto_backend, monkeypatch
):
    """
    Redis 重建在换上阶段出错（比如超时）：先 RENAME 换上、后删残留，所以出错的那个索引原来的 key
    还在，回退到旧定义照样能读。重跑一次就恢复，留下的临时 key 在下次重建开头清掉
    """
    backend: Backend = mod_auto_backend()
    comp = slot_ref.comp_cls
    await _insert(
        backend, comp, _slot(comp, zone=1, level=1), _slot(comp, zone=2, level=2)
    )
    maint = backend.get_table_maintenance()
    io = maint.client.io  # type: ignore[attr-defined]
    # 模拟单列改复合：库里 level 只有单列格式的 key
    level_key = backend.master.index_key(slot_ref, "level")
    io.delete(*raw_index_zset_keys(backend, slot_ref, "level"))
    raw_zadd(backend, level_key, b"old\x001")
    original_eval = io.eval

    def flaky_eval(script, numkeys, *names):
        if any(":rebuilding:level:" in name for name in names[:numkeys]):
            raise TimeoutError("boom")
        return original_eval(script, numkeys, *names)

    monkeypatch.setattr(io, "eval", flaky_eval)
    with pytest.raises(TimeoutError, match="boom"):
        maint.do_rebuild_index_(slot_ref)
    assert raw_index_zset_keys(backend, slot_ref, "level") == [level_key]

    monkeypatch.undo()
    assert maint.do_rebuild_index_(slot_ref) == 2
    keys = raw_index_zset_keys(backend, slot_ref, "level")
    assert len(keys) == 2 and level_key not in keys
    index_keys = sum(
        len(raw_index_zset_keys(backend, slot_ref, name)) for name in comp.indexes_
    )
    assert raw_key_count(backend, slot_ref) == 2 + index_keys  # 临时 key 都清掉了


@use_redis_backend_only
async def test_rebuild_index_error_before_first_tmp_key(
    slot_ref, mod_auto_backend, monkeypatch
):
    """
    Redis 重建索引一开始就出错（还没建任何临时 key）：抛出原来的异常；清理没有要删的 key，
    不发不带参数的 UNLINK（那会再抛一个异常，盖住原来的）
    """
    backend: Backend = mod_auto_backend()
    comp = slot_ref.comp_cls
    await _insert(backend, comp, _slot(comp, zone=1, level=1))
    maint = backend.get_table_maintenance()
    io = maint.client.io  # type: ignore[attr-defined]
    original_pipeline, original_delete = io.pipeline, io.unlink
    deletes: list[tuple] = []

    def failing_pipeline(*args, **kwargs):
        pipe = original_pipeline(*args, **kwargs)

        def boom():
            raise ConnectionError("boom")

        pipe.execute = boom
        return pipe

    def recording_delete(*keys):
        deletes.append(keys)
        return original_delete(*keys)

    monkeypatch.setattr(io, "pipeline", failing_pipeline)
    monkeypatch.setattr(io, "unlink", recording_delete)
    with pytest.raises(ConnectionError, match="boom"):
        maint.do_rebuild_index_(slot_ref)
    assert all(deletes), deletes


async def test_migration_adds_groupby_rebuilds_index(filled_item_ref, tmp_path):
    """
    只给已有索引加 groupby（dtype 不变，迁移走 skip 路径）：indexes_ / uniques_ 都不变，也要
    重建索引，否则库里留着单列格式的 member、按组查不到
    """
    backend = filled_item_ref.backend

    from hetu.data import (
        BaseComponent,
        ComponentDefines,
        Permission,
        define_component,
        property_field,
    )

    ComponentDefines().clear_()

    @define_component(namespace="pytest", permission=Permission.OWNER, table_sub=True)
    class ItemNew(BaseComponent):
        owner: np.int64 = property_field(0, unique=False, index=True, point_sub=True)
        model: np.float32 = property_field(0, unique=False, index=True)
        qty: np.int16 = property_field(1, unique=False, index=False)
        level: np.int8 = property_field(1, unique=False, index=False)
        time: np.int64 = property_field(0, unique=True, index=True, groupby="owner")
        name: "U8" = property_field("", unique=True, index=True, point_sub=True)  # type: ignore  # noqa
        used: bool = property_field(False, unique=False, index=True, point_sub=True)

    define = json.loads(ItemNew.json_)
    define["name"] = "Item"
    renamed = BaseComponent.load_json(json.dumps(define))
    new_table = Table(
        renamed, filled_item_ref.instance_name, filled_item_ref.cluster_id, backend
    )
    maint = backend.get_table_maintenance()
    status, old_meta = maint.check_table(new_table)
    assert status == "schema_mismatch"
    assert maint.migration_schema(tmp_path / "test.py", new_table, old_meta)
    assert maint.check_table(new_table)[0] == "ok"

    await backend.wait_for_synced()
    async with backend.session("pytest", 1) as session:
        repo = session.using(renamed)
        assert (await repo.get(time=111, owner=10)).name == "Itm11"
        got = await repo.range(time=(0, 1000), owner=10, limit=-1)
        assert len(got) == 25
        assert len(await repo.range(time=(0, 1000), owner=11, limit=-1)) == 0
        with pytest.raises(ValueError, match="分组"):
            await repo.range(time=(0, 1000))
    # 每组一个 zset，单列格式的 key 删掉了
    time_key = backend.master.index_key(filled_item_ref, "time")
    time_keys = raw_index_zset_keys(backend, filled_item_ref, "time")
    assert time_keys and all(key.startswith(time_key + ":") for key in time_keys)

    # 再去掉 groupby（换回原来的定义）：同样走 skip 路径，索引重建回单列格式
    original = BaseComponent.load_json(filled_item_ref.comp_cls.json_)
    old_table = Table(
        original, filled_item_ref.instance_name, filled_item_ref.cluster_id, backend
    )
    status, old_meta = maint.check_table(old_table)
    assert status == "schema_mismatch"
    assert maint.migration_schema(tmp_path / "test.py", old_table, old_meta)
    assert maint.check_table(old_table)[0] == "ok"
    await backend.wait_for_synced()
    async with backend.session("pytest", 1) as session:
        repo = session.using(original)
        assert (await repo.get(time=111)).name == "Itm11"
        assert len(await repo.range(time=(0, 1000), limit=-1)) == 25
    assert raw_index_zset_keys(backend, filled_item_ref, "time") == [time_key]


def test_headless_schema_diff_groupby(new_component_env):
    """headless 的 schema 比对要比 groupby：对不上时 headless 的 commit 会把索引写坏"""
    from hetu import headless
    from hetu.data import BaseComponent

    comp = def_slot()
    data = json.loads(comp.json_)
    data["properties"]["owner"]["groupby"] = ["zone"]
    local = BaseComponent.load_json(json.dumps(data))
    errors, _warns = headless._schema_diff(local, comp.json_)
    assert errors == ["~ col owner.groupby: False -> ['zone']"]
    errors, _warns = headless._schema_diff(comp, comp.json_)
    assert errors == []
