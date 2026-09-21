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
from fixtures.backends import use_redis_family_backend_only

from hetu.common.snowflake_id import SnowflakeID
from hetu.data.backend import Backend, RowFormat, Table, TableReference
from hetu.data.backend.base import sortable_token, to_sortable_bytes
from hetu.data.backend.idmap import IdentityMap
from hetu.data.backend.redis import RedisBackendClient

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

    # 表级变更通知：每张被改动的表一条，payload为本事务碰到的row_id列表
    touched: dict[bytes, set[bytes]] = {}
    for push in json[1]:
        if push[0] in (b"HSET", b"DEL"):
            prefix, row_id = push[1].rsplit(b":id:", 1)
            touched.setdefault(prefix + b":table", set()).add(row_id)
    assert touched  # 本测试有insert/update/delete，必然有变动
    # 索引值变更通知：每个被 ZADD/ZREM 的 (索引, 值) 一条，payload为该值上变动的row_id列表
    # （insert/delete 是全部索引字段，update 是变更字段的旧值+新值），从 push 反推期望值
    expected_values: dict[bytes, set[bytes]] = {}
    for push in json[1]:
        if push[0] in (b"ZADD", b"ZREM"):
            sortable, row_id = push[-1].rsplit(b"\x00", 1)
            channel = push[1] + b":" + sortable_token(sortable).encode()
            expected_values.setdefault(channel, set()).add(row_id)
    assert expected_values
    published = {pub[0]: set(msgpack.unpackb(pub[1], raw=True)) for pub in json[3]}
    table_pubs = {c: ids for c, ids in published.items() if c.endswith(b":table")}
    value_pubs = {c: ids for c, ids in published.items() if not c.endswith(b":table")}
    assert table_pubs == touched
    assert value_pubs == expected_values


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


@use_redis_family_backend_only
async def test_redis_lua_check_codes(item_ref, mod_auto_backend):
    """Lua 按 check 携带的 code 回显 RACE:/UNIQUE: 前缀 + label；
    同一 payload 内两条同 (索引, 值) 的 UNIQ 兜底返回 UNIQUE:（不依赖本地 IdentityMap 检查）"""
    from hetu.data.backend.redis.client import msg_packer

    backend: Backend = mod_auto_backend()
    client = cast(RedisBackendClient, backend.master)
    assert client.lua_commit is not None

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
        return await client.lua_commit(keys, [payload])  # type: ignore

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


async def test_mq_client_index_value_channel(filled_item_ref, mod_auto_backend):
    """索引值频道：只有该值上有行进出才有消息，payload是这些行的row_id；整索引频道不订就收不到"""
    backend: Backend = mod_auto_backend()
    servant = backend.servant
    mq = backend.get_mq_client()

    rows = await servant.range(filled_item_ref, "time", 120, 121, limit=100)
    assert len(rows) == 2
    chan_10 = servant.index_value_channel(filled_item_ref, "owner", 10)
    chan_11 = servant.index_value_channel(filled_item_ref, "owner", 11)
    # 值先按 dtype 规范化，10 / "10" / 10.0 是同一个频道
    assert chan_10 == servant.index_value_channel(filled_item_ref, "owner", "10")
    assert chan_10 == servant.index_value_channel(filled_item_ref, "owner", 10.0)
    assert chan_10 != chan_11
    await mq.subscribe(chan_10, chan_11)

    # 一个事务：rows[0] owner 10→11，删掉 rows[1]（owner 10），插入一行 owner=11
    idmap = IdentityMap()
    new_row = filled_item_ref.comp_cls.new_row()
    new_row.name = "ValNew"
    new_row.owner = 11
    new_row.time = 999
    idmap.add_insert(filled_item_ref, new_row)
    idmap.add_clean(filled_item_ref, rows[0])
    rows[0].owner = 11
    idmap.update(filled_item_ref, rows[0])
    idmap.add_clean(filled_item_ref, rows[1])
    idmap.mark_deleted(filled_item_ref, rows[1].id)
    await backend.master.commit(idmap)

    await asyncio.sleep(0.5)
    async with asyncio.timeout(2):
        messages = await mq.get_message()
    assert messages[chan_10] == {str(rows[0].id), str(rows[1].id)}
    assert messages[chan_11] == {str(rows[0].id), str(new_row.id)}
    assert servant.index_channel(filled_item_ref, "owner") not in messages
    assert chan_10 not in mq.pulled_payload  # type: ignore

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
    if isinstance(backend.master, RedisBackendClient):
        with pytest.raises(ValueError):
            backend.post_configure(components=[bad_cls])
    else:
        backend.post_configure(components=[bad_cls])  # SQL 系目前没有索引 dtype 限制
