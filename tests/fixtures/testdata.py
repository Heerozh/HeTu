import pytest

from hetu.data.backend import ComponentTable


@pytest.fixture(scope="module")
async def mod_clear_all_component_define():
    # 此方法mod范围，因此只要依赖与他，就可以每个mod首次都clear
    from hetu.data import ComponentDefines
    ComponentDefines().clear_()


@pytest.fixture(scope="module")
async def mod_item_component(mod_clear_all_component_define):
    from hetu.data import define_component, Property, BaseComponent, Permission
    import numpy as np
    global Item

    @define_component(namespace="pytest", permission=Permission.OWNER)
    class Item(BaseComponent):
        owner: np.int64 = Property(0, unique=False, index=True)
        model: np.int32 = Property(0, unique=False, index=True)
        qty: np.int16 = Property(1, unique=False, index=False)
        level: np.int8 = Property(1, unique=False, index=False)
        time: np.int64 = Property(0, unique=True, index=True)
        name: 'U8' = Property("", unique=True, index=False)
        used: bool = Property(False, unique=False, index=True)

    return Item


@pytest.fixture(scope="module")
async def mod_item_table(mod_auto_backend, mod_item_component) -> ComponentTable:
    backend_component_table, get_or_create_backend = mod_auto_backend

    backend = get_or_create_backend('main')
    test_table = backend_component_table(
        mod_item_component, 'ModItemTestTable', 1, backend)
    test_table.flush(force=True)
    test_table.create_or_migrate()
    return test_table


@pytest.fixture(scope="function")
async def item_table(mod_auto_backend, mod_item_component) -> ComponentTable:
    backend_component_table, get_or_create_backend = mod_auto_backend

    backend = get_or_create_backend('main')
    test_table = backend_component_table(
        mod_item_component, 'ItemTestTable', 1, backend)
    test_table.flush(force=True)
    test_table.create_or_migrate()
    return test_table


@pytest.fixture(scope="module")
async def mod_rls_test_component(mod_clear_all_component_define):
    from hetu.data import define_component, Property, BaseComponent, Permission
    import numpy as np
    global RLSTest

    @define_component(namespace="pytest", permission=Permission.RLS,
                      rls_compare=("eq", "friend", "caller"))
    class RLSTest(BaseComponent):
        owner: np.int64 = Property(0, unique=False, index=True)
        friend: np.int8 = Property(1, unique=False, index=False)

    return RLSTest


@pytest.fixture(scope="module")
async def mod_rls_test_table(mod_auto_backend,
                             mod_rls_test_component) -> ComponentTable:
    backend_component_table, get_or_create_backend = mod_auto_backend

    backend = get_or_create_backend('main')
    test_table = backend_component_table(
        mod_rls_test_component, 'ModRLSTestTable', 1, backend)
    test_table.flush(force=True)
    test_table.create_or_migrate()
    return test_table


@pytest.fixture
async def filled_item_table(mod_item_component, item_table):
    backend = item_table.backend

    # 初始化测试数据
    async with backend.transaction(1) as session:
        tbl = item_table.attach(session)
        for i in range(25):
            row = mod_item_component.new_row()
            row.id = 0
            row.name = f'Itm{i + 10}'
            row.owner = 10
            row.time = i + 110
            row.qty = 999
            await tbl.insert(row)
    # 等待replica同步
    await backend.wait_for_synced()

    return item_table


@pytest.fixture
async def filled_rls_test_table(mod_rls_test_component, mod_rls_test_table):
    backend = mod_rls_test_table.backend
    # 初始化测试数据
    async with backend.transaction(1) as session:
        tbl = mod_rls_test_table.attach(session)
        for i in range(25):
            row = mod_rls_test_component.new_row()
            row.id = 0
            row.owner = 10
            row.friend = 11
            await tbl.insert(row)
    # 等待replica同步
    await backend.wait_for_synced()

    yield mod_rls_test_table

    mod_rls_test_table.flush(force=True)
