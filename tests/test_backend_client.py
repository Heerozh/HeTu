#  """
#  @author: Heerozh (Zhang Jianhao)
#  @copyright: Copyright 2024, Heerozh. All rights reserved.
#  @license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
#  @email: heeroz@gmail.com
#  """
import msgspec
import numpy as np
from typing import cast
from hetu.data.backend.idmap import IdentityMap

from hetu.data.backend import Backend, RedisBackendClient
from hetu.common.snowflake_id import SnowflakeID

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


async def test_redis_commit_payload(item_ref, rls_ref):
    # 建立测试数据
    idmap = IdentityMap()

    # insert
    row = item_ref.comp_cls.new_row()
    row.owner = 10
    row.time = row.owner
    row.name = f"{row.owner}"
    idmap.add_insert(item_ref, row)
    row = item_ref.comp_cls.new_row()
    row.owner = 11
    row.time = row.owner
    row.name = f"{row.owner}"
    idmap.add_insert(item_ref, row)

    row2 = rls_ref.comp_cls.new_row()
    row2.owner = 11
    row2 = rls_ref.comp_cls.new_row()
    row2.owner = 12
    idmap.add_insert(rls_ref, row2)

    # update
    row = item_ref.comp_cls.new_row()
    row.owner = 20
    row.time = row.owner
    row.name = f"{row.owner}"
    idmap.add_clean(item_ref, row)
    row.time = 23
    idmap.update(item_ref, row)

    row = item_ref.comp_cls.new_row()
    row.owner = 21
    row.time = row.owner
    row.name = f"{row.owner}"
    idmap.add_clean(item_ref, row)
    row.name = "23"
    idmap.update(item_ref, row)

    row = item_ref.comp_cls.new_row()
    row.owner = 22
    row.time = row.owner
    row.name = f"{row.owner}"
    idmap.add_clean(item_ref, row)

    # delete
    idmap.mark_deleted(item_ref, row.id)

    # commit
    json = None
    client = RedisBackendClient.__new__(RedisBackendClient)
    client.is_servant = False

    def test_lua_commit(self, keys, payload_json):
        # 反序列化payload_json
        assert keys[0] == "commit_payload"
        # 比较idmap和idmap_deser是否相等
        json_str = payload_json[0]
        nonlocal json
        json = msgspec.msgpack.decode(json_str)

    client.lua_commit = test_lua_commit
    await client.commit(idmap)

    # test
    checks = []
    pushes = []
    assert json == [checks, pushes]


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
