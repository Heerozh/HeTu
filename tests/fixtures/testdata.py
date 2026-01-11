#  """
#  @author: Heerozh (Zhang Jianhao)
#  @copyright: Copyright 2024, Heerozh. All rights reserved.
#  @license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
#  @email: heeroz@gmail.com
#  """

import pytest
from hetu.data.backend import TableReference, Table


def def_item():
    from hetu.data import define_component, property_field, BaseComponent, Permission
    import numpy as np

    global Item

    @define_component(namespace="pytest", permission=Permission.OWNER)
    class Item(BaseComponent):
        owner: np.int64 = property_field(0, unique=False, index=True)
        model: np.float32 = property_field(0, unique=False, index=True)
        qty: np.int16 = property_field(1, unique=False, index=False)
        level: np.int8 = property_field(1, unique=False, index=False)
        time: np.int64 = property_field(0, unique=True, index=True)
        name: "U8" = property_field("", unique=True, index=True)  # type: ignore  # noqa
        used: bool = property_field(False, unique=False, index=True)

    return Item


def def_rls_test():
    from hetu.data import define_component, property_field, BaseComponent, Permission
    import numpy as np

    global RLSTest

    @define_component(
        namespace="pytest",
        permission=Permission.RLS,
        volatile=True,
        rls_compare=("eq", "friend", "caller"),
    )
    class RLSTest(BaseComponent):
        owner: np.int64 = property_field(0, unique=False, index=True)
        friend: np.int8 = property_field(1, unique=False, index=False)

    return RLSTest


def create_ref(model, backend) -> TableReference:
    """定义测试用的Item组件模型，创建空表，返回模型引用类。"""
    # 创建空表
    from hetu.data.backend import RaceCondition

    model_ref = TableReference(model, "pytest", 1)
    table_maint = backend.get_table_maintenance()
    try:
        table_maint.create_table(model_ref)
    except RaceCondition:
        table_maint.flush(model_ref, force=True)

    return model_ref


@pytest.fixture(scope="module")
async def mod_item_model(mod_new_component_env):
    """定义测试用的Item组件模型，返回模型引用类。不在数据库创建表"""
    return def_item()


@pytest.fixture(scope="module")
async def mod_rls_test_model(mod_new_component_env):
    """定义测试用的Item组件模型，返回模型引用类。不在数据库创建表"""
    return def_rls_test()


@pytest.fixture(scope="module")
async def mod_item_ref(mod_new_component_env, mod_auto_backend) -> TableReference:
    """定义测试用的Item组件模型，在数据库创建空表，返回模型引用类。"""
    return create_ref(def_item(), mod_auto_backend())


@pytest.fixture(scope="module")
async def mod_rls_test_ref(mod_new_component_env, mod_auto_backend) -> TableReference:
    """定义测试用RLS的组件模型，在数据库创建空表，返回模型类"""
    return create_ref(def_rls_test(), mod_auto_backend())


@pytest.fixture(scope="function")
async def item_ref(new_component_env, mod_auto_backend) -> TableReference:
    """定义测试用的Item组件模型，创建空表，返回模型引用类。"""
    return create_ref(def_item(), mod_auto_backend())


@pytest.fixture(scope="function")
async def rls_ref(new_component_env, mod_auto_backend) -> TableReference:
    """定义测试用RLS的组件模型，创建空表，返回模型类"""
    return create_ref(def_rls_test(), mod_auto_backend())


@pytest.fixture
async def filled_item_ref(item_ref, mod_auto_backend):
    """填充Item测试数据，返回填充后的表格引用对象"""
    backend = mod_auto_backend()

    # 初始化测试数据
    async with backend.session("pytest", 1) as session:
        item_repo = session.using(item_ref.comp_cls)
        for i in range(25):
            row = item_ref.comp_cls.new_row()
            row.name = f"Itm{i + 10}"
            row.owner = 10
            row.time = i + 110
            row.qty = 999
            row.model = float(i) * 0.1
            await item_repo.insert(row)
    # 等待replica同步
    await backend.wait_for_synced()

    return Table(
        item_ref.comp_cls, item_ref.instance_name, item_ref.cluster_id, backend
    )


@pytest.fixture
async def filled_rls_ref(rls_ref, mod_auto_backend):
    """填充RLS测试表格的数据，返回填充后的表格引用对象"""
    backend = mod_auto_backend()
    # 初始化测试数据
    async with backend.session("pytest", 1) as session:
        rls_repo = session.using(rls_ref.comp_cls)
        for i in range(25):
            row = rls_ref.comp_cls.new_row()
            row.owner = 10
            row.friend = 11
            await rls_repo.insert(row)
    # 等待replica同步
    await backend.wait_for_synced()

    return Table(rls_ref.comp_cls, rls_ref.instance_name, rls_ref.cluster_id, backend)
