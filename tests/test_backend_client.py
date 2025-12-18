#  """
#  @author: Heerozh (Zhang Jianhao)
#  @copyright: Copyright 2024, Heerozh. All rights reserved.
#  @license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
#  @email: heeroz@gmail.com
#  """
import numpy as np
import pytest

from hetu.data.backend import Backend, RedisBackendClient, UniqueViolation, random
from hetu.system import define_system, SystemClusters
from hetu.common.snowflake_id import SnowflakeID

SnowflakeID().init(1, 0)


async def test_insert(
    mod_item_model, mod_rls_test_model, mod_auto_backend, env_builder
):
    """测试client的commit(insert)/get"""
    # 建立环境，定义用哪些表
    env_builder(mod_item_model, mod_rls_test_model)

    # 启动backend
    backend: Backend = mod_auto_backend()
    client = backend.master

    from hetu.data.backend.idmap import IdentityMap
    from hetu.data.backend.table import TableReference

    item_ref = TableReference(mod_item_model, "pytest", 1)
    rls_ref = TableReference(mod_rls_test_model, "pytest", 1)
    idmap = IdentityMap()

    # 测试client的commit(insert)数据，以及get
    row1 = mod_item_model.new_row()
    row1.owner = 10
    idmap.add_insert(item_ref, row1)

    row2 = mod_rls_test_model.new_row()
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


async def test_update_delete(
    mod_item_model, mod_rls_test_model, mod_auto_backend, env_builder
):
    """测试client的commit(update/delete) get/range"""
    # 建立环境，定义用哪些表
    env_builder(mod_item_model, mod_rls_test_model)

    # 启动backend
    backend: Backend = mod_auto_backend()
    client = backend.master

    from hetu.data.backend.idmap import IdentityMap
    from hetu.data.backend.table import TableReference

    item_ref = TableReference(mod_item_model, "pytest", 1)
    rls_ref = TableReference(mod_rls_test_model, "pytest", 1)
    idmap = IdentityMap()

    # 添加多条数据
    for i in range(10):
        row1 = mod_item_model.new_row()
        row1.owner = i
        row1.time = i + 10
        row1.name = f"Item{i + 100}"
        idmap.add_insert(item_ref, row1)

        row2 = mod_rls_test_model.new_row()
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
        [f"Item{3 + 100}", f"Item{4 + 100}", f"mid", f"Item{6 + 100}"],
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
        [f"updated", f"Item{4 + 100}", f"mid", f"Item{6 + 100}"],
    )

    rows2 = await client.range(rls_ref, "owner", 9, 15)
    np.testing.assert_array_equal(rows2.owner, [11])

    # 测试删除
    idmap = IdentityMap()
    idmap.add_clean(item_ref, rows1)
    idmap.add_clean(rls_ref, rows2)

    idmap.mark_deleted(item_ref, rows1[rows1.time == 13].id[0])
    idmap.mark_deleted(rls_ref, rows2.id[0])
    await client.commit(idmap)

    # 测试删除后再次查询
    rows1 = await client.range(item_ref, "time", 13, 16)
    np.testing.assert_array_equal(
        rows1.name,
        [f"Item{4 + 100}", f"mid", f"Item{6 + 100}"],
    )

    rows2 = await client.range(rls_ref, "owner", 9, 15)
    np.testing.assert_array_equal(rows2.owner, [])


