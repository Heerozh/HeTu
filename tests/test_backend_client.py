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


async def test_table(
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
    client = backend.master_or_servant

    from hetu.data.backend.idmap import IdentityMap
    from hetu.data.backend.table import TableReference

    item_ref = TableReference(mod_item_model, "TestServer", 1)
    rls_ref = TableReference(mod_item_model, "TestServer", 1)
    idmap = IdentityMap()

    # 测试client的commit(insert)数据，以及get
    row = mod_item_model.new_row()
    row.owner = 10
    idmap.add_insert(item_ref, row)

    row = mod_rls_test_model.new_row()
    row.owner = 11
    idmap.add_insert(rls_ref, row)

    await client.commit(idmap)

    # 测试insert的是否有效
    row_get = client.get(item_ref, ids[0])

    # 测试client的commit(insert/update/delete)数据，以及get, range查询
