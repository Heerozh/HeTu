#  """
#  @author: Heerozh (Zhang Jianhao)
#  @copyright: Copyright 2024, Heerozh. All rights reserved.
#  @license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
#  @email: heeroz@gmail.com
#  """
import asyncio
from typing import cast

import msgpack
import numpy as np
import pytest

from hetu.common.snowflake_id import SnowflakeID
from hetu.data.backend import Backend, RowFormat, Table, TableReference
from hetu.data.backend.base import sortable_token, to_sortable_bytes
from hetu.data.backend.idmap import IdentityMap
from hetu.data.backend.redis import RedisBackendClient
from hetu.data.backend.redis_model import RedisModelClient

SnowflakeID().init(1, 0)


async def test_redis_serialize_sortable():
    """测试sortable字段的序列化和反序列化"""
    client = RedisBackendClient.__new__(RedisBackendClient)

    int64_max = 2**63 - 1
    int64_min = -(2**63)
    uint64_max = 2**64 - 1
    uint64_min = 1
    double_int_max = 2**53 - 1 + 0.123
    double_int_min = -(2**53 - 1) + 0.123

    b1 = client.to_sortable_bytes(np.int64(int64_max))
    b2 = client.to_sortable_bytes(np.int64(int64_min))
    assert b1 > b2
    b1 = client.to_sortable_bytes(np.int8(127))
    b2 = client.to_sortable_bytes(np.int8(-1))
    assert b1 > b2
    # test int16 vs int8
    b1 = client.to_sortable_bytes(np.int16(128))
    b2 = client.to_sortable_bytes(np.int8(-1))
    assert b1 > b2

    b1 = client.to_sortable_bytes(np.uint64(uint64_max))
    b2 = client.to_sortable_bytes(np.uint64(uint64_min))
    assert b1 > b2

    b1 = client.to_sortable_bytes(np.float64(double_int_max))
    b2 = client.to_sortable_bytes(np.float64(double_int_min))
    assert b1 > b2
    # 123.456 = 405edd2f1a9fbe77, -0.033468749999999936 = bfa122d0e5604180
    # 405edd2f1a9fbe77 ^ 1 << 63 = c05edd2f1a9fbe77
    # bfa122d0e5604180 ^ 0xFFFFFFFFFFFFFFFF = 405edd2f1a9fbe7f
    b1 = client.to_sortable_bytes(np.float64(123.456))
    b2 = client.to_sortable_bytes(np.float64(-0.033468749999999936))
    assert b1 > b2


async def test_redis_commit_payload(mod_item_model, mod_rls_test_model):
    item_ref = TableReference(mod_item_model, "pytest", 1)
    rls_ref = TableReference(mod_rls_test_model, "pytest", 1)
    client = RedisBackendClient.__new__(RedisBackendClient)
    client.is_servant = False

    # 建立测试数据
    idmap = IdentityMap()
    checks = []
    pushes = []

    def label(comp: str, field: str, rid, op: str) -> str:
        return f"{comp}.{field} id={rid} {op}"

    # insert
    # 插入 item row1
    row = item_ref.comp_cls.new_row()
    row.owner = 10
    row.time = row.owner
    row.name = f"{row.owner}"
    row.model = 123.31
    idmap.add_insert(item_ref, row)
    # 插入的payload应该是这些check和push
    b_rowid = client.to_sortable_bytes(row.id)
    checks.append(
        ["NX", "pytest:Item:{CLU1}:id:" + f"{row.id}"]
        + ["UNIQUE", label("Item", "id", row.id, "insert")]
    )
    checks.append(
        ["UNIQ", "pytest:Item:{CLU1}:index:id"]
        + [b"[" + b_rowid + b"\x00", b"[" + b_rowid + b"\x00\xff"]
        + ["UNIQUE", label("Item", "id", row.id, "insert")]
    )
    checks.append(
        ["UNIQ", "pytest:Item:{CLU1}:index:name", b"[10\x00", b"[10\x00\xff"]
        + ["UNIQUE", label("Item", "name", row.id, "insert")]
    )
    checks.append(
        ["UNIQ", "pytest:Item:{CLU1}:index:time"]
        + [
            b"[\x80\x00\x00\x00\x00\x00\x00\n\x00",
            b"[\x80\x00\x00\x00\x00\x00\x00\n\x00\xff",
        ]
        + ["UNIQUE", label("Item", "time", row.id, "insert")]
    )
    pushes.append(
        ["HSET", "pytest:Item:{CLU1}:id:" + f"{row.id}", "_version", "1"]
        + [
            x
            for k, v in zip(row.dtype.names, map(str, row.item()))  # type: ignore
            if k != "_version"
            for x in (k, v)
        ]
    )
    # insert的索引部分
    pushes.append(
        ["ZADD", "pytest:Item:{CLU1}:index:id"]
        + ["0", b_rowid + b"\x00" + str(row.id).encode()]
    )
    pushes.append(
        ["ZADD", "pytest:Item:{CLU1}:index:model"]
        + ["0", b"\xc0^\xd3\xd7\x00\x00\x00\x00\x00" + str(row.id).encode()]
    )
    pushes.append(
        ["ZADD", "pytest:Item:{CLU1}:index:name"]
        + ["0", b"10\x00" + str(row.id).encode()]
    )
    pushes.append(
        ["ZADD", "pytest:Item:{CLU1}:index:owner"]
        + ["0", b"\x80\x00\x00\x00\x00\x00\x00\n\x00" + str(row.id).encode()]
    )
    pushes.append(
        ["ZADD", "pytest:Item:{CLU1}:index:time"]
        + ["0", b"\x80\x00\x00\x00\x00\x00\x00\n\x00" + str(row.id).encode()]
    )
    pushes.append(
        ["ZADD", "pytest:Item:{CLU1}:index:used"]
        + ["0", b"\x80\x00\x00\x00\x00\x00\x00\x00\x00" + str(row.id).encode()]
    )

    # 插入 item row2
    row = item_ref.comp_cls.new_row()
    row.owner = 11
    row.time = row.owner
    row.model = -123.31
    row.name = f"{row.owner}"
    row.used = True
    idmap.add_insert(item_ref, row)
    # 插入的payload应该是这些check和push
    b_rowid = client.to_sortable_bytes(row.id)
    checks.append(
        ["NX", "pytest:Item:{CLU1}:id:" + f"{row.id}"]
        + ["UNIQUE", label("Item", "id", row.id, "insert")]
    )
    checks.append(
        ["UNIQ", "pytest:Item:{CLU1}:index:id"]
        + [b"[" + b_rowid + b"\x00", b"[" + b_rowid + b"\x00\xff"]
        + ["UNIQUE", label("Item", "id", row.id, "insert")]
    )
    checks.append(
        ["UNIQ", "pytest:Item:{CLU1}:index:name", b"[11\x00", b"[11\x00\xff"]
        + ["UNIQUE", label("Item", "name", row.id, "insert")]
    )
    checks.append(
        ["UNIQ", "pytest:Item:{CLU1}:index:time"]
        + [
            b"[\x80\x00\x00\x00\x00\x00\x00\x0b\x00",
            b"[\x80\x00\x00\x00\x00\x00\x00\x0b\x00\xff",
        ]
        + ["UNIQUE", label("Item", "time", row.id, "insert")]
    )
    pushes.append(
        ["HSET", "pytest:Item:{CLU1}:id:" + f"{row.id}", "_version", "1"]
        + [
            x
            for k, v in zip(row.dtype.names, map(str, row.item()))  # type: ignore
            if k != "_version"
            for x in (k, v)
        ]
    )
    # insert的索引部分
    pushes.append(
        ["ZADD", "pytest:Item:{CLU1}:index:id"]
        + ["0", b_rowid + b"\x00" + str(row.id).encode()]
    )
    pushes.append(
        ["ZADD", "pytest:Item:{CLU1}:index:model"]
        + ["0", b"?\xa1,(\xff\xff\xff\xff\x00" + str(row.id).encode()]
    )
    pushes.append(
        ["ZADD", "pytest:Item:{CLU1}:index:name"]
        + ["0", b"11\x00" + str(row.id).encode()]
    )
    pushes.append(
        ["ZADD", "pytest:Item:{CLU1}:index:owner"]
        + ["0", b"\x80\x00\x00\x00\x00\x00\x00\x0b\x00" + str(row.id).encode()]
    )
    pushes.append(
        ["ZADD", "pytest:Item:{CLU1}:index:time"]
        + ["0", b"\x80\x00\x00\x00\x00\x00\x00\x0b\x00" + str(row.id).encode()]
    )
    pushes.append(
        ["ZADD", "pytest:Item:{CLU1}:index:used"]
        + ["0", b"\x80\x00\x00\x00\x00\x00\x00\x01\x00" + str(row.id).encode()]
    )

    # 插入 rls row1
    row = rls_ref.comp_cls.new_row()
    row.owner = 11
    idmap.add_insert(rls_ref, row)
    # 插入的payload应该是这些check和push
    b_rowid = client.to_sortable_bytes(row.id)
    checks.append(
        ["NX", "pytest:RLSTest:{CLU1}:id:" + f"{row.id}"]
        + ["UNIQUE", label("RLSTest", "id", row.id, "insert")]
    )
    checks.append(
        ["UNIQ", "pytest:RLSTest:{CLU1}:index:id"]
        + [b"[" + b_rowid + b"\x00", b"[" + b_rowid + b"\x00\xff"]
        + ["UNIQUE", label("RLSTest", "id", row.id, "insert")]
    )
    pushes.append(
        ["HSET", "pytest:RLSTest:{CLU1}:id:" + f"{row.id}", "_version", "1"]
        + [
            x
            for k, v in zip(row.dtype.names, map(str, row.item()))  # type: ignore
            if k != "_version"
            for x in (k, v)
        ]
    )
    # insert的索引部分
    pushes.append(
        ["ZADD", "pytest:RLSTest:{CLU1}:index:id"]
        + ["0", b_rowid + b"\x00" + str(row.id).encode()]
    )
    pushes.append(
        ["ZADD", "pytest:RLSTest:{CLU1}:index:owner"]
        + ["0", b"\x80\x00\x00\x00\x00\x00\x00\x0b\x00" + str(row.id).encode()]
    )

    # 插入 rls row2
    row = rls_ref.comp_cls.new_row()
    row.owner = 12
    idmap.add_insert(rls_ref, row)
    # 插入的payload应该是这些check和push
    b_rowid = client.to_sortable_bytes(row.id)
    checks.append(
        ["NX", "pytest:RLSTest:{CLU1}:id:" + f"{row.id}"]
        + ["UNIQUE", label("RLSTest", "id", row.id, "insert")]
    )
    checks.append(
        ["UNIQ", "pytest:RLSTest:{CLU1}:index:id"]
        + [b"[" + b_rowid + b"\x00", b"[" + b_rowid + b"\x00\xff"]
        + ["UNIQUE", label("RLSTest", "id", row.id, "insert")]
    )
    pushes.append(
        ["HSET", "pytest:RLSTest:{CLU1}:id:" + f"{row.id}", "_version", "1"]
        + [
            x
            for k, v in zip(row.dtype.names, map(str, row.item()))  # type: ignore
            if k != "_version"
            for x in (k, v)
        ]
    )
    # insert的索引部分
    pushes.append(
        ["ZADD", "pytest:RLSTest:{CLU1}:index:id"]
        + ["0", b_rowid + b"\x00" + str(row.id).encode()]
    )
    pushes.append(
        ["ZADD", "pytest:RLSTest:{CLU1}:index:owner"]
        + ["0", b"\x80\x00\x00\x00\x00\x00\x00\x0c\x00" + str(row.id).encode()]
    )

    # update 1, change time
    row = item_ref.comp_cls.new_row()
    row.owner = 20
    row.time = row.owner
    row.name = f"{row.owner}"
    row._version = 16
    idmap.add_clean(item_ref, row)
    row.time = 23
    idmap.update(item_ref, row)
    # 更新的payload应该是这些check和push
    checks.append(["VER", "pytest:Item:{CLU1}:id:" + f"{row.id}", "16"])
    checks.append(
        ["UNIQ", "pytest:Item:{CLU1}:index:time"]
        + [
            b"[\x80\x00\x00\x00\x00\x00\x00\x17\x00",
            b"[\x80\x00\x00\x00\x00\x00\x00\x17\x00\xff",
        ]
        + ["UNIQUE", label("Item", "time", row.id, "update")]
    )
    pushes.append(
        ["HSET", "pytest:Item:{CLU1}:id:" + f"{row.id}", "_version", "17"]
        + ["time", "23"]
    )
    # update的index变更
    pushes.append(
        ["ZREM", "pytest:Item:{CLU1}:index:time"]
        + [b"\x80\x00\x00\x00\x00\x00\x00\x14\x00" + str(row.id).encode()]
    )
    pushes.append(
        ["ZADD", "pytest:Item:{CLU1}:index:time"]
        + ["0", b"\x80\x00\x00\x00\x00\x00\x00\x17\x00" + str(row.id).encode()]
    )

    # update 2, change name
    row = item_ref.comp_cls.new_row()
    row.owner = 21
    row.time = row.owner
    row.name = f"{row.owner}"
    row._version = 233
    idmap.add_clean(item_ref, row)
    row.name = "23"
    idmap.update(item_ref, row)
    # 更新的payload应该是这些check和push
    checks.append(["VER", "pytest:Item:{CLU1}:id:" + f"{row.id}", "233"])
    checks.append(
        ["UNIQ", "pytest:Item:{CLU1}:index:name", b"[23\x00", b"[23\x00\xff"]
        + ["UNIQUE", label("Item", "name", row.id, "update")]
    )
    pushes.append(
        ["HSET", "pytest:Item:{CLU1}:id:" + f"{row.id}", "_version", "234"]
        + ["name", "23"]
    )
    pushes.append(
        ["ZREM", "pytest:Item:{CLU1}:index:name"] + [b"21\x00" + str(row.id).encode()]
    )
    pushes.append(
        ["ZADD", "pytest:Item:{CLU1}:index:name"]
        + ["0", b"23\x00" + str(row.id).encode()]
    )

    # delete
    row = item_ref.comp_cls.new_row()
    row.owner = 22
    row.time = row.owner
    row.name = f"{row.owner}"
    row._version = 9
    idmap.add_clean(item_ref, row)
    idmap.mark_deleted(item_ref, row.id)
    # 删除的payload应该是这些check和push
    b_rowid = client.to_sortable_bytes(row.id)
    checks.append(["VER", "pytest:Item:{CLU1}:id:" + f"{row.id}", "9"])
    pushes.append(["DEL", "pytest:Item:{CLU1}:id:" + f"{row.id}"])
    pushes.append(
        ["ZREM", "pytest:Item:{CLU1}:index:id"]
        + [b_rowid + b"\x00" + str(row.id).encode()]
    )
    pushes.append(
        ["ZREM", "pytest:Item:{CLU1}:index:model"]
        + [b"\x80\x00\x00\x00\x00\x00\x00\x00\x00" + str(row.id).encode()]
    )
    pushes.append(
        ["ZREM", "pytest:Item:{CLU1}:index:name"] + [b"22\x00" + str(row.id).encode()]
    )
    pushes.append(
        ["ZREM", "pytest:Item:{CLU1}:index:owner"]
        + [b"\x80\x00\x00\x00\x00\x00\x00\x16\x00" + str(row.id).encode()]
    )
    pushes.append(
        ["ZREM", "pytest:Item:{CLU1}:index:time"]
        + [b"\x80\x00\x00\x00\x00\x00\x00\x16\x00" + str(row.id).encode()]
    )
    pushes.append(
        ["ZREM", "pytest:Item:{CLU1}:index:used"]
        + [b"\x80\x00\x00\x00\x00\x00\x00\x00\x00" + str(row.id).encode()]
    )

    # commit
    json = []

    async def mock_lua_commit(keys, payload_json):
        # 反序列化payload_json
        assert keys[0] == "pytest:Item:{CLU1}:id:1"
        # 比较idmap和idmap_deser是否相等
        json_str = payload_json[0]
        nonlocal json
        json = msgpack.unpackb(json_str, raw=True)
        return b"committed"

    client.lua_commit = mock_lua_commit
    await client.commit(idmap)

    # test
    checks = [  # 先全转换为bytes, 因为msgpack解包后str会变bytes
        [arg.encode() if type(arg) is str else arg for arg in args] for args in checks
    ]
    pushes = [
        [arg.encode() if type(arg) is str else arg for arg in args] for args in pushes
    ]
    for check in json[0]:
        assert check in checks
    for push in json[1]:
        assert push in pushes
    for check in checks:
        assert check in json[0]
    for push in pushes:
        assert push in json[1]

    # 表频道：只给声明了 table_sub 的组件（Item 声明了，RLSTest 没有），每张表一条，
    # payload 为本事务碰到的 row_id 列表
    assert item_ref.comp_cls.table_sub_ and not rls_ref.comp_cls.table_sub_
    touched: dict[bytes, set[bytes]] = {}
    for push in json[1]:
        if push[0] in (b"HSET", b"DEL") and push[1].startswith(b"pytest:Item:"):
            prefix, row_id = push[1].rsplit(b":id:", 1)
            touched.setdefault(prefix + b":table", set()).add(row_id)
    assert touched  # 本测试有insert/update/delete，必然有变动
    table_pubs = {pub[0]: set(msgpack.unpackb(pub[1], raw=True)) for pub in json[3]}
    assert table_pubs == touched
    # 值频道：只给声明了 point_sub 的索引，只发"进入"——从 ZADD 的 push 反推 (索引, 值)，
    # ZREM（离开）不发；一个事务每个 (索引, 值) 一条，不带内容
    point_sub_keys = {
        f"{ref.instance_name}:{ref.comp_cls.name_}:{{CLU1}}:index:{field}".encode()
        for ref in (item_ref, rls_ref)
        for field in ref.comp_cls.point_subs_
    }
    expected_values: set[bytes] = set()
    for push in json[1]:
        if push[0] == b"ZADD" and push[1] in point_sub_keys:
            sortable, _row_id = push[-1].rsplit(b"\x00", 1)
            expected_values.add(push[1] + b":" + sortable_token(sortable).encode())
    assert expected_values
    assert len(json[4]) == len(set(json[4]))
    assert set(json[4]) == expected_values


async def test_redis_commit_check_codes(mod_item_model):
    """commit 的 NX/UNIQ 检查带 RACE/UNIQUE 标记：本事务曾 get 读空的列为 RACE，其余 UNIQUE；
    payload 里竞态类（VER、RACE）全部排在确定性类之前（Lua 首个失败即返回 → RACE 优先）"""
    item_ref = TableReference(mod_item_model, "pytest", 1)
    client = RedisBackendClient.__new__(RedisBackendClient)
    client.is_servant = False
    idmap = IdentityMap()

    # 行 A：get(id=)/get(name=) 读空后 insert → id/name 为 RACE，time 未观察 → UNIQUE
    a = mod_item_model.new_row()
    a.name, a.time = "A", 1
    idmap.mark_absent(item_ref, "id", int(a.id))
    idmap.mark_absent(item_ref, "name", "A")
    idmap.add_insert(item_ref, a)
    # 行 B：盲 insert → 全 UNIQUE
    b = mod_item_model.new_row()
    b.name, b.time = "B", 2
    idmap.add_insert(item_ref, b)
    # 行 C：clean 行改 time 为曾观察不存在的值 → time 为 RACE；VER 恒为竞态类
    c = mod_item_model.new_row()
    c.name, c.time = "C", 3
    idmap.add_clean(item_ref, c)
    idmap.mark_absent(item_ref, "time", 33)
    c2, _ = idmap.get(item_ref, int(c.id))
    assert c2 is not None
    c2.time = 33
    idmap.update(item_ref, c2)

    captured = []

    async def mock_lua_commit(keys, payload_json):
        captured.append(msgpack.unpackb(payload_json[0], raw=True))
        return b"committed"

    client.lua_commit = mock_lua_commit
    await client.commit(idmap)
    checks = captured[0][0]

    def codes(field, rid, op) -> set[bytes]:
        lbl = f"Item.{field} id={rid} {op}".encode()
        return {c[-2] for c in checks if c[0] in (b"NX", b"UNIQ") and c[-1] == lbl}

    assert codes("id", a.id, "insert") == {b"RACE"}  # NX 与 UNIQ(id) 两条都是 RACE
    assert codes("name", a.id, "insert") == {b"RACE"}
    assert codes("time", a.id, "insert") == {b"UNIQUE"}
    assert codes("id", b.id, "insert") == {b"UNIQUE"}
    assert codes("name", b.id, "insert") == {b"UNIQUE"}
    assert codes("time", b.id, "insert") == {b"UNIQUE"}
    assert codes("time", c.id, "update") == {b"RACE"}
    assert [chk for chk in checks if chk[0] == b"VER"]  # 行 C 的版本检查
    # 竞态类全部在前
    is_race = [chk[0] == b"VER" or chk[-2] == b"RACE" for chk in checks]
    assert is_race == sorted(is_race, reverse=True)


async def test_range_observations_to_check(mod_item_model):
    """哪些 range 观察要单独校验区间：unique 点查命中（该行有数据库态）、读空后本事务写入该值
    （insert 或 update 改成该值，且没删掉数据库态为该值的行）由已有检查覆盖，get 命中只保护
    返回的行，其余都要；完全相同的观察只留一条"""
    from hetu.data.backend.idmap import RangeObservation

    item_ref = TableReference(mod_item_model, "pytest", 1)
    idmap = IdentityMap()

    def observe(index_name: str, point, ids=()):
        obs = RangeObservation(index_name, list(ids), (index_name, point), [], point)
        idmap.add_range_observation(item_ref, obs)
        return obs

    def row(name: str, time: int):
        r = mod_item_model.new_row()
        r.name, r.time = name, time
        return r

    # 命中一行、该行有数据库态：覆盖
    hit = row("hit", 1)
    idmap.add_clean(item_ref, hit)
    s1 = observe("name", "hit", [int(hit.id)])
    # 读空后 insert 该值：覆盖
    idmap.mark_absent(item_ref, "name", "new")
    s2_insert = observe("name", "new")
    idmap.add_insert(item_ref, row("new", 2))
    # 读空后把另一行 update 成该值：覆盖
    idmap.mark_absent(item_ref, "time", 30)
    s2_update = observe("time", 30)
    other = row("other", 3)
    idmap.add_clean(item_ref, other)
    changed, _ = idmap.get(item_ref, int(other.id))
    assert changed is not None
    changed.time = 30
    idmap.update(item_ref, changed)
    # 读空、本事务没写这个值：要校验
    idmap.mark_absent(item_ref, "name", "ghost")
    absent_only = observe("name", "ghost")
    # 读空后又删掉一行数据库态为该值的行、再插入该值：读集矛盾，要校验
    idmap.mark_absent(item_ref, "name", "v")
    contradicted = observe("name", "v")
    gone = row("v", 4)
    idmap.add_clean(item_ref, gone)
    idmap.mark_deleted(item_ref, int(gone.id))
    idmap.add_insert(item_ref, row("v", 5))
    # 非 unique 列（没有 point）、区间读：要校验
    nonunique = observe("owner", None, [int(hit.id)])
    ranged = observe("time", None)
    # get 命中：只保护返回的那一行，不校验区间
    got = RangeObservation("owner", [int(hit.id)], ("got",), [], 7, rows_only=True)
    idmap.add_range_observation(item_ref, got)

    # 完全相同的观察去重
    observe("owner", None, [int(hit.id)])
    assert len(idmap.range_observations()[item_ref]) == 8

    to_check = {id(obs) for obs in idmap.range_observations_to_check()[item_ref]}
    assert to_check == {id(absent_only), id(contradicted), id(nonunique), id(ranged)}
    assert not {id(s1), id(s2_insert), id(s2_update), id(got)} & to_check


async def test_commit_script_check_codes(item_ref, mod_auto_backend):
    """
    提交脚本（Redis 的 commit_v2.lua / SQLite 的 Python 版）按 check 携带的 code 回显
    RACE:/UNIQUE: 前缀 + label；同一 payload 内两条同 (索引, 值) 的 UNIQ 兜底返回 UNIQUE:
    （不依赖本地 IdentityMap 检查）。两个后端的返回串逐字节相同
    """
    from hetu.data.backend.redis_model import msg_packer

    backend: Backend = mod_auto_backend()
    client = cast(RedisModelClient, backend.master)

    # 准备一行 name="dup"
    async with backend.session("pytest", 1) as session:
        row = item_ref.comp_cls.new_row()
        row.name = "dup"
        row.time = 1
        await session.using(item_ref.comp_cls).insert(row)

    idx_key = client.index_key(item_ref, "name")
    keys = [client.row_key(item_ref, 1)]

    async def run(checks):
        payload = msg_packer.pack([checks, [], {}, []])
        return await client.commit_script_(keys, [payload])

    uniq = ["UNIQ", idx_key, b"[dup\x00", b"[dup\x00\xff"]
    assert await run([uniq + ["RACE", "Item.name id=7 insert"]]) == (
        b"RACE: Unique violation Item.name id=7 insert"
    )
    assert await run([uniq + ["UNIQUE", "Item.name id=7 update"]]) == (
        b"UNIQUE: Unique violation Item.name id=7 update"
    )
    nx = ["NX", client.row_key(item_ref, int(row.id))]
    assert await run([nx + ["UNIQUE", f"Item.id id={row.id} insert"]]) == (
        f"UNIQUE: Key already exists Item.id id={row.id} insert".encode()
    )
    assert await run([nx + ["RACE", f"Item.id id={row.id} insert"]]) == (
        f"RACE: Key already exists Item.id id={row.id} insert".encode()
    )
    # 去重兜底：第二条同 (索引, 值) 直接 UNIQUE:，即便值在库里不存在
    fresh = ["UNIQ", idx_key, b"[nobody\x00", b"[nobody\x00\xff"]
    resp = await run(
        [
            fresh + ["RACE", "Item.name id=1 insert"],
            fresh + ["UNIQUE", "Item.name id=2 insert"],
        ]
    )
    assert resp == (
        b"UNIQUE: Duplicate unique value within transaction Item.name id=2 insert"
    )
    # 没有冲突：正常提交
    assert await run([fresh + ["UNIQUE", "Item.name id=3 insert"]]) == b"committed"


async def test_commit_script_range_count_check(item_ref, mod_auto_backend):
    """提交脚本的 CNT：ZLEXCOUNT 与期望行数不符返回 RACE: Range changed + label，相符则继续"""
    from hetu.data.backend.redis_model import msg_packer

    backend: Backend = mod_auto_backend()
    client = cast(RedisModelClient, backend.master)
    comp = item_ref.comp_cls

    async with backend.session("pytest", 1) as session:
        for i in range(2):
            row = comp.new_row()
            row.owner, row.time, row.name = 5, i + 1, f"n{i}"
            await session.using(comp).insert(row)

    keys = [client.row_key(item_ref, 1)]
    idx_key = client.index_key(item_ref, "owner")
    lo, hi = client.range_normalize_(comp.dtype_map_["owner"], 5, 5, False)

    async def run(checks):
        payload = msg_packer.pack([checks, [], {}, [], []])
        return await client.commit_script_(keys, [payload])

    assert await run([["CNT", idx_key, lo, hi, 2, "Item.owner"]]) == b"committed"
    assert await run([["CNT", idx_key, lo, hi, 1, "Item.owner"]]) == (
        b"RACE: Range changed Item.owner"
    )


async def test_range_check_payload(item_ref, mod_auto_backend):
    """range 读在 commit 里变成 CNT 检查：格式、截断时收窄的边界、排在全部竞态检查之后 /
    确定性检查之前；unique 点查由 VER / UNIQ 覆盖的（get(unique=) 命中、upsert 两条路径）不带"""
    from unittest.mock import patch

    backend: Backend = mod_auto_backend()
    client = cast(RedisModelClient, backend.master)
    comp = item_ref.comp_cls
    dtypes = comp.dtype_map_

    async with backend.session("pytest", 1) as session:
        for i in range(3):
            row = comp.new_row()
            row.owner, row.time, row.name = 1, i + 1, f"n{i}"
            await session.using(comp).insert(row)

    captured: list = []
    orig_commit_script = client.commit_script_

    async def spy(keys, args):
        captured.append(msgpack.unpackb(args[0], raw=True)[0])
        return await orig_commit_script(keys, args)

    def cnt_checks() -> list:
        return [chk for chk in captured[-1] if chk[0] == b"CNT"]

    def member(field: str, row) -> bytes:
        value = to_sortable_bytes(dtypes[field].type(row[field]))
        return value + b"\x00" + str(int(row.id)).encode()

    owner_key = client.index_key(item_ref, "owner").encode()
    time_key = client.index_key(item_ref, "time").encode()

    with patch.object(client, "commit_script_", new=spy):
        # get(unique=) 命中 → update：命中行的 VER + unique 已经足够，不带 CNT
        async with backend.session("pytest", 1) as session:
            session.only_master = True
            repo = session.using(comp)
            row = await repo.get(name="n0")
            assert row is not None
            row.qty = 5
            await repo.update(row)
        assert cnt_checks() == []

        # upsert 的插入路径（读空 + 带 RACE 的 UNIQ）与更新路径（命中）都不带 CNT
        for qty in (7, 8):
            async with backend.session("pytest", 1) as session:
                session.only_master = True
                async with session.using(comp).upsert(name="up") as row:
                    row.time, row.qty = 100, qty
            assert cnt_checks() == []

        # 区间读后盲插：一条 CNT，排在全部竞态检查之后、确定性检查之前
        async with backend.session("pytest", 1) as session:
            session.only_master = True
            repo = session.using(comp)
            assert len(await repo.range(owner=(1, 1), limit=-1)) == 3
            blind = comp.new_row()
            blind.owner, blind.time, blind.name = 2, 200, "blind"
            await repo.insert(blind)
        lo, hi = client.range_normalize_(dtypes["owner"], 1, 1, False)
        assert cnt_checks() == [[b"CNT", owner_key, lo, hi, 3, b"Item.owner"]]
        order = ["race", "cnt", "strict"]
        kinds = [
            "cnt"
            if chk[0] == b"CNT"
            else "race"
            if chk[0] == b"VER" or chk[-2] == b"RACE"
            else "strict"
            for chk in captured[-1]
        ]
        assert "race" in kinds and "strict" in kinds
        assert kinds == sorted(kinds, key=order.index)

        # 非 unique 列 get 命中：只保护返回的那一行（VER），不带 CNT
        async with backend.session("pytest", 1) as session:
            session.only_master = True
            repo = session.using(comp)
            row = await repo.get(owner=1)
            assert row is not None
            row.qty = 9
            await repo.update(row)
        assert cnt_checks() == []

        # 非 unique 列 get 读空再插入：要校验这个值上仍然没有行（计数 0）
        async with backend.session("pytest", 1) as session:
            session.only_master = True
            repo = session.using(comp)
            assert await repo.get(owner=404) is None
            new = comp.new_row()
            new.owner, new.time, new.name = 404, 40400, "n404"  # time 避开下面的区间
            await repo.insert(new)
        lo, hi = client.range_normalize_(dtypes["owner"], 404, 404, False)
        assert cnt_checks() == [[b"CNT", owner_key, lo, hi, 0, b"Item.owner"]]

        # 降序截断：下界收到最后一个（最小的）member，上界是查询上界
        async with backend.session("pytest", 1) as session:
            session.only_master = True
            repo = session.using(comp)
            rows = await repo.range(time=(0, 1000), limit=2, desc=True)
            assert list(rows.time) == [200, 100]
            extra = comp.new_row()
            extra.owner, extra.time, extra.name = 3, 5000, "extra"
            await repo.insert(extra)
        upper, _ = client.range_normalize_(dtypes["time"], 0, 1000, True)
        assert cnt_checks() == [
            [b"CNT", time_key, b"[" + member("time", rows[-1]), upper, 2, b"Item.time"]
        ]


async def test_insert(item_ref, rls_ref, mod_auto_backend):
    """测试client的commit(insert)/get"""
    # 启动backend
    backend: Backend = mod_auto_backend()
    client = backend.master

    # 测试client的commit(insert)数据，以及get
    idmap = IdentityMap()

    row1 = item_ref.comp_cls.new_row()
    row1.owner = 10
    idmap.add_insert(item_ref, row1)

    row2 = rls_ref.comp_cls.new_row()
    row2.owner = 11
    idmap.add_insert(rls_ref, row2)
    await client.commit(idmap)

    # 测试insert的是否有效
    row_get = await client.get(item_ref, row1.id)
    row1._version += 1
    assert row_get == row1

    row_get = await client.get(rls_ref, row2.id)
    row2._version += 1
    assert row_get == row2


async def test_update_delete(item_ref, rls_ref, mod_auto_backend):
    """测试client的commit(update/delete) get/range"""
    # 启动backend
    backend: Backend = mod_auto_backend()
    client = backend.master

    from hetu.data.backend.idmap import IdentityMap

    # 添加多条数据
    idmap = IdentityMap()
    for i in range(10):
        row1 = item_ref.comp_cls.new_row()
        row1.owner = i
        row1.time = i + 10
        row1.name = f"Item{i + 100}"
        idmap.add_insert(item_ref, row1)
        row2 = rls_ref.comp_cls.new_row()
        row2.owner = i
        idmap.add_insert(rls_ref, row2)

    # update insert的内容
    rows_cache, _, _ = idmap._cache(item_ref)
    row = rows_cache[5]
    row.name = "mid"
    idmap.update(item_ref, row)

    await client.commit(idmap)

    # 开始测试新的事务
    idmap = IdentityMap()

    # 测试range查询
    rows1 = await client.range(item_ref, "time", 13, 16)
    np.testing.assert_array_equal(
        rows1.name,
        [f"Item{3 + 100}", f"Item{4 + 100}", "mid", f"Item{6 + 100}"],
    )

    rows2 = await client.range(rls_ref, "owner", 9, 15)
    np.testing.assert_array_equal(rows2.owner, [9])

    idmap.add_clean(item_ref, rows1)
    idmap.add_clean(rls_ref, rows2)
    # 测试update查询到的数据
    row1 = rows1[rows1.time == 13][0]
    row1.name = "updated"
    idmap.update(item_ref, row1)

    row2 = rows2[0]
    row2.owner = 11
    idmap.update(rls_ref, row2)
    await client.commit(idmap)

    # 测试update后再次查询是否更新了
    rows1 = await client.range(item_ref, "time", 13, 16)
    np.testing.assert_array_equal(
        rows1.name,
        ["updated", f"Item{4 + 100}", "mid", f"Item{6 + 100}"],
    )

    rows2 = await client.range(rls_ref, "owner", 9, 15)
    np.testing.assert_array_equal(rows2.owner, [11])

    # 测试删除
    idmap = IdentityMap()
    idmap.add_clean(item_ref, rows1)
    idmap.add_clean(rls_ref, rows2)

    idmap.mark_deleted(item_ref, rows1[rows1.time == 13]["id"][0])
    idmap.mark_deleted(rls_ref, rows2.id[0])
    await client.commit(idmap)

    # 测试删除后再次查询
    rows1 = await client.range(item_ref, "time", 13, 16)
    np.testing.assert_array_equal(
        rows1.name,
        [f"Item{4 + 100}", "mid", f"Item{6 + 100}"],
    )

    rows2 = await client.range(rls_ref, "owner", 9, 15)
    np.testing.assert_array_equal(rows2.owner, [])


@pytest.mark.timeout(10)
async def test_mq_client(filled_item_ref, mod_auto_backend):
    """测试mq client的订阅是否有效。这里只做基本的测试，更复杂的在综合测试中"""
    backend: Backend = mod_auto_backend()
    servant = backend.servant
    mq = backend.get_mq_client()

    # 获取测试行
    rows = await servant.range(filled_item_ref, "time", 110)
    row = rows[0]
    assert row

    # 测试订阅
    channel_name = servant.row_channel(filled_item_ref, row.id)
    await mq.subscribe(channel_name)

    # 写入数据库查看通知是否生效
    idmap = IdentityMap()
    idmap.add_clean(filled_item_ref, row)
    row.qty = 9999
    idmap.update(filled_item_ref, row)
    await backend.master.commit(idmap)

    # 通知由后端 hub 在后台投递到本地队列，get_message 等到它到齐（同频道重复消息会合并）
    async with asyncio.timeout(2):
        messages = await mq.get_message()

    assert channel_name in messages


async def test_get_many(filled_item_ref, mod_auto_backend):
    """测试get_many：顺序与入参一致，不存在的行为None，各row_format都可用"""
    backend: Backend = mod_auto_backend()
    servant = backend.servant

    rows = await servant.range(filled_item_ref, "time", 110, 120, limit=100)
    ids = [int(r.id) for r in rows]
    assert len(ids) == 11

    # 混入不存在的id，且打乱顺序
    query = [ids[3], 999999999, ids[0], ids[10], 888888888]
    got = await servant.get_many(filled_item_ref, query)
    assert len(got) == 5
    assert got[1] is None and got[4] is None
    assert got[0].id == ids[3] and got[2].id == ids[0] and got[3].id == ids[10]
    assert got[0] == rows[3]

    got = await servant.get_many(filled_item_ref, query, RowFormat.TYPED_DICT)
    assert got[0]["id"] == ids[3] and got[0]["time"] == 113
    assert got[1] is None

    got = await servant.get_many(filled_item_ref, query, RowFormat.RAW)
    assert got[0]["time"] == "113"

    assert await servant.get_many(filled_item_ref, []) == []

    assert await servant.get_many(filled_item_ref, [999999999, 888888888]) == [
        None,
        None,
    ]
    # 同一批的重复 ID 仍是两个独立 record，不能修改一个连带改变另一个。
    duplicates = await servant.get_many(filled_item_ref, [ids[0], ids[0]])
    original = duplicates[1].qty
    duplicates[0].qty = original + 1
    assert duplicates[1].qty == original


async def test_get_many_rows_do_not_pin_batch(filled_item_ref, mod_auto_backend):
    """get_many 返回的每行只占自己的内存：批量解码后不能把整批数组的视图直接交出去，
    否则调用方只留一行，也会拖住整批（比如 headless 轮询里只存有变化的行）"""
    backend: Backend = mod_auto_backend()
    servant = backend.servant
    ids = await servant.range(
        filled_item_ref, "time", 110, 134, limit=100, row_format=RowFormat.ID_LIST
    )
    got = await servant.get_many(filled_item_ref, ids)
    assert len(got) == 25
    for row in got:
        assert row is not None
        assert getattr(row.base, "nbytes", 0) <= row.nbytes


async def test_table_servant_get_many(filled_item_ref):
    """Table.servant_get_many：与 servant_get / servant_range 同款绑定，供非事务批量读"""
    tbl: Table = filled_item_ref
    rows = await tbl.servant_range("time", 110, 120, limit=100)
    ids = [int(r.id) for r in rows]
    query = [ids[3], 999999999, ids[0]]
    got = await tbl.servant_get_many(query)
    assert [None if r is None else int(r.id) for r in got] == [ids[3], None, ids[0]]
    assert got[0] == await tbl.servant_get(ids[3])
    got_dict = await tbl.servant_get_many(query, RowFormat.TYPED_DICT)
    assert got_dict[0]["time"] == 113 and got_dict[1] is None


async def test_range_large(item_ref, mod_auto_backend):
    """大结果集range：超过一个pipeline chunk的行数也能完整、有序读回"""
    backend: Backend = mod_auto_backend()
    n = 2500  # 超过 RedisBackendClient.RANGE_PIPELINE_CHUNK

    async with backend.session("pytest", 1) as session:
        repo = session.using(item_ref.comp_cls)
        for i in range(n):
            row = item_ref.comp_cls.new_row()
            row.name = f"L{i}"
            row.owner = 77
            row.time = 100000 + i
            await repo.insert(row)
    await backend.wait_for_synced()

    rows = await backend.servant.range(item_ref, "owner", 77, limit=n + 10)
    assert len(rows) == n
    assert sorted(int(r.time) for r in rows) == list(range(100000, 100000 + n))

    ids = await backend.servant.range(
        item_ref, "time", 100000, 100000 + n, limit=n, row_format=RowFormat.ID_LIST
    )
    got = await backend.servant.get_many(item_ref, ids)
    assert len(got) == n and all(r is not None for r in got)
    assert [int(r.id) for r in got] == ids


async def test_mq_client_batch_subscribe(filled_item_ref, mod_auto_backend):
    """MQ client 一次订阅/取消多个频道"""
    backend: Backend = mod_auto_backend()
    servant = backend.servant
    mq = backend.get_mq_client()

    rows = await servant.range(filled_item_ref, "time", 110, 115, limit=100)
    channels = [servant.row_channel(filled_item_ref, r.id) for r in rows]
    assert len(channels) == 6

    await mq.subscribe(*channels)
    assert set(channels) <= set(mq.subscribed_channels)

    await mq.unsubscribe(*channels[:4])
    assert not (set(channels[:4]) & set(mq.subscribed_channels))
    assert set(channels[4:]) <= set(mq.subscribed_channels)

    # 空调用不报错
    await mq.subscribe()
    await mq.unsubscribe()
    await mq.close()


async def test_mq_client_table_channel(filled_item_ref, mod_auto_backend):
    """表级频道：一个事务一条消息，payload是变动的row_id集合，跨事务按tick合并"""
    backend: Backend = mod_auto_backend()
    servant = backend.servant
    mq = backend.get_mq_client()

    rows = await servant.range(filled_item_ref, "time", 110, 111, limit=100)
    assert len(rows) == 2
    table_channel = servant.table_channel(filled_item_ref)
    row_channel = servant.row_channel(filled_item_ref, rows[0].id)
    await mq.subscribe(table_channel, row_channel)

    # 一个事务：insert 一行 + update 一行 + delete 一行
    idmap = IdentityMap()
    new_row = filled_item_ref.comp_cls.new_row()
    new_row.name = "TblNew"
    new_row.owner = 10
    new_row.time = 999
    idmap.add_insert(filled_item_ref, new_row)
    idmap.add_clean(filled_item_ref, rows[0])
    rows[0].qty = 1
    idmap.update(filled_item_ref, rows[0])
    idmap.add_clean(filled_item_ref, rows[1])
    idmap.mark_deleted(filled_item_ref, rows[1].id)
    await backend.master.commit(idmap)

    # 等 hub 把这个事务的通知都投递到本地队列
    await asyncio.sleep(0.5)
    async with asyncio.timeout(2):
        messages = await mq.get_message()

    assert messages[table_channel] == {
        str(new_row.id),
        str(rows[0].id),
        str(rows[1].id),
    }
    # 行频道没有payload
    assert row_channel in messages and messages[row_channel] is None
    # 取走后payload不残留
    assert table_channel not in mq.pulled_payload  # type: ignore

    # 取消订阅表级频道后不再收到
    await mq.unsubscribe(table_channel)
    idmap = IdentityMap()
    rows[0]._version += 1
    idmap.add_clean(filled_item_ref, rows[0])
    rows[0].qty = 2
    idmap.update(filled_item_ref, rows[0])
    await backend.master.commit(idmap)
    await asyncio.sleep(0.5)
    async with asyncio.timeout(2):
        messages = await mq.get_message()
    assert table_channel not in messages
    assert row_channel in messages
    await mq.close()


async def test_mq_client_table_channel_requires_table_sub(
    filled_rls_ref, mod_auto_backend
):
    """没声明 table_sub 的组件：commit 不发表频道，行频道照常"""
    backend: Backend = mod_auto_backend()
    servant = backend.servant
    mq = backend.get_mq_client()
    assert filled_rls_ref.comp_cls.table_sub_ is False

    rows = await servant.range(filled_rls_ref, "owner", 10, limit=1)
    assert len(rows) == 1
    table_channel = servant.table_channel(filled_rls_ref)
    row_channel = servant.row_channel(filled_rls_ref, rows[0].id)
    await mq.subscribe(table_channel, row_channel)

    idmap = IdentityMap()
    idmap.add_clean(filled_rls_ref, rows[0])
    rows[0].friend = 12
    idmap.update(filled_rls_ref, rows[0])
    await backend.master.commit(idmap)

    await asyncio.sleep(0.5)
    async with asyncio.timeout(2):
        messages = await mq.get_message()
    assert row_channel in messages
    assert table_channel not in messages
    await mq.close()


def test_sortable_token():
    """索引值频道的 token：数值 8 字节 → 16 位 hex，长字符串 → h + blake2b-128"""
    assert RedisBackendClient.to_sortable_bytes is to_sortable_bytes
    b_int = to_sortable_bytes(np.int64(10))
    assert len(b_int) == 8 and sortable_token(b_int) == "800000000000000a"
    assert sortable_token(to_sortable_bytes(np.int8(1))) == sortable_token(
        to_sortable_bytes(np.int8(True))
    )
    short = to_sortable_bytes(np.str_("好" * 8))  # 24 字节
    assert sortable_token(short) == short.hex()
    long = to_sortable_bytes(np.str_("好" * 11))  # 33 字节
    token = sortable_token(long)
    assert token.startswith("h") and len(token) == 33
    assert token != sortable_token(to_sortable_bytes(np.str_("好" * 11 + "!")))


async def test_mq_client_no_id_value_channel(filled_item_ref, mod_auto_backend):
    """id 索引没有值频道（不能声明 point_sub），要它的名字直接报错；
    行频道和整个 id 索引的频道照常"""
    backend: Backend = mod_auto_backend()
    servant = backend.servant
    mq = backend.get_mq_client()

    rows = await servant.range(filled_item_ref, "time", 122, 122, limit=1)
    assert len(rows) == 1
    row = rows[0]
    with pytest.raises(ValueError, match="point_sub"):
        servant.index_value_channel(filled_item_ref, "id", row.id)
    id_index_chan = servant.index_channel(filled_item_ref, "id")
    row_chan = servant.row_channel(filled_item_ref, row.id)
    await mq.subscribe(id_index_chan, row_chan)

    idmap = IdentityMap()
    idmap.add_clean(filled_item_ref, row)
    idmap.mark_deleted(filled_item_ref, row.id)
    await backend.master.commit(idmap)

    await asyncio.sleep(0.5)
    async with asyncio.timeout(2):
        messages = await mq.get_message()
    assert row_chan in messages and id_index_chan in messages
    await mq.close()


def _raw_value_channel(ref, index_name: str, value) -> str:
    """
    按 commit 的命名规则拼值频道名。没声明 point_sub 的索引没有公开的值频道名
    （index_value_channel 会拒绝），订它是为了证明 commit 没往那儿发
    """
    dtype = ref.comp_cls.dtype_map_[index_name]
    token = sortable_token(to_sortable_bytes(dtype.type(value)))
    return (
        f"{ref.instance_name}:{ref.comp_cls.name_}:{{CLU{ref.cluster_id}}}"
        f":index:{index_name}:{token}"
    )


async def test_mq_client_index_value_channel(filled_item_ref, mod_auto_backend):
    """
    索引值频道：只给声明了 point_sub 的索引发，而且只发"进入"（insert、字段改成该值）；
    离开（delete、字段改走）不发，由订阅者订着的行频道发现。消息不带内容；整索引频道
    不订就收不到
    """
    backend: Backend = mod_auto_backend()
    servant = backend.servant
    mq = backend.get_mq_client()

    rows = await servant.range(filled_item_ref, "time", 120, 121, limit=100)
    assert len(rows) == 2
    chan_10 = servant.index_value_channel(filled_item_ref, "owner", 10)
    chan_11 = servant.index_value_channel(filled_item_ref, "owner", 11)
    # 值先按 dtype 规范化，10 / "10" / 10.0 是同一个频道；命名规则与 commit 一致
    assert chan_10 == servant.index_value_channel(filled_item_ref, "owner", "10")
    assert chan_10 == servant.index_value_channel(filled_item_ref, "owner", 10.0)
    assert chan_10 == _raw_value_channel(filled_item_ref, "owner", 10)
    assert chan_10 != chan_11
    # model 没声明 point_sub：它的新旧值频道都不该有消息
    assert "model" not in filled_item_ref.comp_cls.point_subs_
    model_old = _raw_value_channel(filled_item_ref, "model", rows[0].model)
    model_new = _raw_value_channel(filled_item_ref, "model", 9.5)
    await mq.subscribe(chan_10, chan_11, model_old, model_new)

    # 一个事务：rows[0] owner 10→11 且改 model，删掉 rows[1]（owner 10），插入一行 owner=11
    idmap = IdentityMap()
    new_row = filled_item_ref.comp_cls.new_row()
    new_row.name = "ValNew"
    new_row.owner = 11
    new_row.time = 999
    idmap.add_insert(filled_item_ref, new_row)
    idmap.add_clean(filled_item_ref, rows[0])
    rows[0].owner = 11
    rows[0].model = 9.5
    idmap.update(filled_item_ref, rows[0])
    idmap.add_clean(filled_item_ref, rows[1])
    idmap.mark_deleted(filled_item_ref, rows[1].id)
    await backend.master.commit(idmap)

    await asyncio.sleep(0.5)
    async with asyncio.timeout(2):
        messages = await mq.get_message()
    # 进入 11 的两行（改过来的、新插入的）：一条消息，不带内容
    assert chan_11 in messages and messages[chan_11] is None
    # 离开 10（改走、删除）不发
    assert chan_10 not in messages
    assert model_old not in messages and model_new not in messages
    assert servant.index_channel(filled_item_ref, "owner") not in messages
    assert chan_11 not in mq.pulled_payload  # type: ignore

    # 只改非索引字段：两个值频道都不该有消息
    idmap = IdentityMap()
    rows[0]._version += 1
    idmap.add_clean(filled_item_ref, rows[0])
    rows[0].qty = 2
    idmap.update(filled_item_ref, rows[0])
    await backend.master.commit(idmap)
    await asyncio.sleep(0.5)
    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.5):
            await mq.get_message()
    await mq.close()


async def test_post_configure_explicit_components(mod_auto_backend):
    """post_configure(components=...) 用显式组件列表做 schema 检查，不依赖 SystemClusters。"""
    import json

    from hetu.data.component import BaseComponent

    backend = mod_auto_backend("main")
    # 一个索引列为复数 dtype 的组件；用 load_json 生成而不注册进 ComponentDefines，
    # 避免污染其它 fixture 通过 ComponentDefines().get_all() 做的 mock。
    bad_json = json.dumps(
        {
            "namespace": "pytest",
            "name": "BadComplexIndex",
            "permission": "USER",
            "rls_compare": None,
            "volatile": False,
            "readonly": False,
            "backend": "default",
            "properties": {
                "value": {
                    "default": 0,
                    "unique": False,
                    "index": True,
                    "dtype": "<c16",
                },
                "id": {"default": 0, "unique": True, "index": True, "dtype": "<i8"},
                "_version": {
                    "default": 0,
                    "unique": False,
                    "index": False,
                    "dtype": "<i4",
                },
            },
        }
    )
    bad_cls = BaseComponent.load_json(bad_json)

    # 显式空列表：什么都不检查，正常返回；且不带参数的老用法仍可用
    backend.post_configure(components=[])
    backend.post_configure()
    with pytest.raises(ValueError):
        backend.post_configure(components=[bad_cls])


async def test_client_rejects_unknown_index(item_ref, mod_auto_backend):
    """range 用组件没有的索引名（qty 是普通字段）直接 ValueError，不去库里查"""
    backend: Backend = mod_auto_backend()
    with pytest.raises(ValueError, match="没有索引"):
        await backend.servant.range(item_ref, "qty", 0, 10)
    with pytest.raises(ValueError, match="没有索引"):
        await backend.servant.range(item_ref, "not_a_field", 0, 10)


async def test_commit_without_dirty_rows(mod_auto_backend):
    """client.commit 收到没有任何改动的 IdentityMap 是调用方的错（Session 会先判 is_dirty）"""
    backend: Backend = mod_auto_backend()
    idmap = IdentityMap()
    with pytest.raises(ValueError, match="没有脏数据"):
        await backend.master.commit(idmap)


async def test_repo_rejects_invalid_args(filled_item_ref, mod_auto_backend):
    """SessionRepository 的参数校验：查不存在的索引、update 改 _version、update 什么都没改"""
    backend: Backend = mod_auto_backend()
    comp = filled_item_ref.comp_cls
    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        with pytest.raises(ValueError, match="没有叫 qty 的索引"):
            await repo.get(qty=999)
        with pytest.raises(ValueError, match="没有叫 qty 的索引"):
            await repo.range("qty", 0, 10)

        row = await repo.get(time=110)
        assert row is not None
        with pytest.raises(ValueError, match="No fields changed"):
            await repo.update(row)
        row._version += 1
        with pytest.raises(ValueError, match="_version"):
            await repo.update(row)
        # 校验失败不留脏数据：退出 async with 时没有东西要提交
        assert not session.idmap.is_dirty


async def test_client_calls_after_close(item_ref, mod_auto_backend):
    """backend close 之后，client 的读和取维护对象都抛 ConnectionError，不是底层驱动的怪异报错"""
    backend: Backend = mod_auto_backend("closed_client")
    master, servant = backend.master, backend.servant
    await backend.close()

    with pytest.raises(ConnectionError):
        await servant.get(item_ref, 1)
    with pytest.raises(ConnectionError):
        await servant.get_many(item_ref, [1, 2])
    with pytest.raises(ConnectionError):
        await servant.range(item_ref, "time", 0, 10)
    with pytest.raises(ConnectionError):
        master.get_table_maintenance()
