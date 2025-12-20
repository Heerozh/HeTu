#  """
#  @author: Heerozh (Zhang Jianhao)
#  @copyright: Copyright 2024, Heerozh. All rights reserved.
#  @license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
#  @email: heeroz@gmail.com
#  """

import pytest
from hetu.data.backend import Session
from hetu.data.backend import RaceCondition
from hetu.data.backend.base import TableMaintenance
from hetu.data.backend.select import SessionSelect


@pytest.fixture(scope="module")
async def mod_item_model(mod_new_component_env):
    """定义测试用的Item组件模型，返回模型类"""
    from hetu.data import define_component, property_field, BaseComponent, Permission
    import numpy as np

    global Item

    @define_component(namespace="pytest", permission=Permission.OWNER)
    class Item(BaseComponent):
        owner: np.int64 = property_field(0, unique=False, index=True)
        model: np.int32 = property_field(0, unique=False, index=True)
        qty: np.int16 = property_field(1, unique=False, index=False)
        level: np.int8 = property_field(1, unique=False, index=False)
        time: np.int64 = property_field(0, unique=True, index=True)
        name: "U8" = property_field("", unique=True, index=True)
        used: bool = property_field(False, unique=False, index=True)

    return Item


@pytest.fixture(scope="module")
async def mod_item_table(mod_auto_backend, mod_item_model) -> RawComponentTable:
    """"""
    backend_component_table, get_or_create_backend = mod_auto_backend

    backend = get_or_create_backend("main")
    test_table = backend_component_table(mod_item_model, "ModItemTestTable", 1, backend)
    test_table.flush(force=True)
    test_table.create_or_migrate()
    return test_table


@pytest.fixture(scope="function")
async def item_select(mod_auto_backend, mod_item_model) -> SessionSelect:
    get_or_create_backend = mod_auto_backend

    # 返回表格session
    backend = get_or_create_backend("main")
    session = Session(backend, "pytest", 1)
    select = session.select(mod_item_model)

    # 初始化表格
    table_maint = backend.get_table_maintenance()
    table_maint.flush(select.ref, force=True)
    try:
        table_maint.create_table(select.ref)
    except RaceCondition:
        pass

    return select


@pytest.fixture(scope="module")
async def mod_rls_test_model(mod_new_component_env):
    """定义测试用RLS的组件模型，返回模型类"""
    from hetu.data import define_component, property_field, BaseComponent, Permission
    import numpy as np

    global RLSTest

    @define_component(
        namespace="pytest",
        permission=Permission.RLS,
        rls_compare=("eq", "friend", "caller"),
    )
    class RLSTest(BaseComponent):
        owner: np.int64 = property_field(0, unique=False, index=True)
        friend: np.int8 = property_field(1, unique=False, index=False)

    return RLSTest


@pytest.fixture(scope="module")
async def mod_rls_test_table(mod_auto_backend, mod_rls_test_model) -> RawComponentTable:
    backend_component_table, get_or_create_backend = mod_auto_backend

    backend = get_or_create_backend("main")
    test_table = backend_component_table(
        mod_rls_test_model, "ModRLSTestTable", 1, backend
    )
    test_table.flush(force=True)
    test_table.create_or_migrate()
    return test_table


@pytest.fixture
async def filled_item_table(mod_item_model, item_table):
    """填充Item测试数据，返回填充后的表格对象"""
    backend = item_table.backend

    # 初始化测试数据
    async with backend.transaction(1) as session:
        tbl = item_table.attach(session)
        for i in range(25):
            row = mod_item_model.new_row()
            row.id = 0
            row.name = f"Itm{i + 10}"
            row.owner = 10
            row.time = i + 110
            row.qty = 999
            await tbl.insert(row)
    # 等待replica同步
    await backend.wait_for_synced()

    return item_table


@pytest.fixture
async def filled_rls_test_table(mod_rls_test_model, mod_rls_test_table):
    """填充RLS测试表格的数据，返回填充后的表格对象"""
    backend = mod_rls_test_table.backend
    # 初始化测试数据
    async with backend.transaction(1) as session:
        tbl = mod_rls_test_table.attach(session)
        for i in range(25):
            row = mod_rls_test_model.new_row()
            row.id = 0
            row.owner = 10
            row.friend = 11
            await tbl.insert(row)
    # 等待replica同步
    await backend.wait_for_synced()

    yield mod_rls_test_table

    mod_rls_test_table.flush(force=True)


@pytest.fixture
def env_builder():
    """
    清理System定义，保证每个测试用例使用干净的System定义环境
    """
    from hetu.system import define_system, SystemClusters

    def _build(*args):
        SystemClusters()._clear()

        # 需要定义System以确保Component被注册，不然Component schema不会加入到lua脚本中
        @define_system(namespace="pytest", components=args)
        async def ref_components(ctx):
            pass

        # 初始化instance & clusters
        SystemClusters().build_clusters("pytest")

    return _build
