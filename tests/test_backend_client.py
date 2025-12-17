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


def test_update_delete(
    mod_item_model, mod_rls_test_model, mod_auto_backend, new_clusters_env
):
    pass
    # 测试client的commit(insert/update/delete)数据，以及get, range查询
