#  """
#  @author: Heerozh (Zhang Jianhao)
#  @copyright: Copyright 2024, Heerozh. All rights reserved.
#  @license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
#  @email: heeroz@gmail.com
#  """

import pytest
import numpy as np
from hetu.common.snowflake_id import SnowflakeID
from hetu.data.backend import Backend

SnowflakeID().init(1, 0)


# 当前文件不能有其他地方用mod_auto_backend，否则会冲突
@pytest.fixture
def mod_auto_backend():
    pytest.skip("mod_auto_backend 已在本文件禁用")


async def test_volatile_table_flush(auto_backend, new_component_env):
    backend = auto_backend("flush_test")

    from hetu.data import define_component, property_field, BaseComponent
    from hetu.data.backend import TableReference, RaceCondition

    @define_component(namespace="pytest", volatile=True)
    class TempData(BaseComponent):
        data: np.int64 = property_field(0, unique=True)

    temp_table = TableReference(TempData, "test", 1)
    table_maint = backend.get_table_maintenance()
    try:
        table_maint.create_table(temp_table)
    except RaceCondition:
        table_maint.flush(temp_table, force=True)

    async with backend.session("test", 1) as session:
        repo = session.using(TempData)
        for i in range(25):
            row = TempData.new_row()
            row.data = i  # type: ignore # noqa
            await repo.insert(row)

    async with backend.session("test", 1) as session:
        session.only_master = True  # 由于刚插入，可能replica还没同步
        repo = session.using(TempData)
        assert len(await repo.range("id", -np.inf, +np.inf, limit=999)) == 25

    table_maint.flush(temp_table)

    async with backend.session("test", 1) as session:
        session.only_master = True  # 可能replica还没同步
        repo = session.using(TempData)
        assert len(await repo.range("id", -np.inf, +np.inf, limit=999)) == 0


async def test_reconnect(auto_backend, mod_item_model):
    # 因为要用不同的连接flush，所以只能用function scope的auto_backend
    # 且当前文件不能有其他地方用mod_auto_backend，否则会冲突
    from hetu.data.backend import TableReference, RaceCondition

    backend: Backend = auto_backend("flush_test")

    temp_table = TableReference(mod_item_model, "test", 1)
    table_maint = backend.get_table_maintenance()
    try:
        table_maint.create_table(temp_table)
    except RaceCondition:
        table_maint.flush(temp_table, force=True)

    # 初始化测试数据
    row = None
    async with backend.session("test", 1) as session:
        repo = session.using(mod_item_model)
        for i in range(25):
            row = mod_item_model.new_row()
            row.time = i  # 防止unique冲突
            row.name = f"Item_{i}"  # 防止unique冲突
            await repo.insert(row)
    # 等待replica同步
    await backend.wait_for_synced()

    # 测试保存(断开连接）后再读回来
    async with backend.session("test", 1) as session:
        repo = session.using(mod_item_model)
        row = await repo.get(id=row.id)  # type: ignore
        repo.delete(row.id)  # type: ignore
    async with backend.session("test", 1) as session:
        repo = session.using(mod_item_model)
        size = len(await repo.range("id", -np.inf, +np.inf, limit=999))

    # 测试连接关闭
    await backend.close()  # 不close不能重建backend_component_table
    await backend.close()  # 再次close不该报错
    with pytest.raises(ConnectionError):
        backend.post_configure()
    with pytest.raises(ConnectionError):
        await backend.wait_for_synced()
    with pytest.raises(ConnectionError):
        backend.get_mq_client()

    # 重新初始化table和连接后再试
    backend = None  # type: ignore
    table_maint = None
    backend2 = auto_backend("load_test")

    table_maint2 = backend2.get_table_maintenance()
    try:
        table_maint2.create_table(temp_table)
    except RaceCondition:
        pass

    async with backend2.session("test", 1) as session:
        repo = session.using(mod_item_model)
        assert len(await repo.range("id", -np.inf, +np.inf, limit=999)) == size
