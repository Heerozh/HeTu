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
    mod_item_model, mod_rls_test_model, mod_auto_backend, new_clusters_env
):
    # 需要先定义System以确保Component被注册
    @define_system(
        namespace="TestServer", components=(mod_item_model, mod_rls_test_model)
    )
    async def ref_components(ctx):
        pass

    SystemClusters().build_clusters("TestServer")

    # 启动backend
    backend: Backend = mod_auto_backend()
    client = backend.master

    from hetu.data.backend.idmap import IdentityMap
    from hetu.data.backend.table import TableReference

    item_ref = TableReference(mod_item_model, "TestServer", 1)
    rls_ref = TableReference(mod_rls_test_model, "TestServer", 1)
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
    assert row_get == row1

    # 测试client的commit(insert/update/delete)数据，以及get, range查询
