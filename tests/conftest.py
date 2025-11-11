from fixtures.backends import *

@pytest.fixture
async def defined_item_component():
    from hetu.data import define_component, Property, BaseComponent, Permission
    import numpy as np
    global Item

    @define_component(namespace="ssw", permission=Permission.OWNER)
    class Item(BaseComponent):
        owner: np.int64 = Property(0, unique=False, index=True)
        model: np.int32 = Property(0, unique=False, index=True)
        qty: np.int16 = Property(1, unique=False, index=False)
        level: np.int8 = Property(1, unique=False, index=False)
        time: np.int64 = Property(0, unique=True, index=True)
        name: 'U8' = Property("", unique=True, index=False)
        used: bool = Property(False, unique=False, index=True)

    return Item


@pytest.fixture
async def item_table(auto_backend, defined_item_component):
    comp_tbl_class, get_or_create_backend = auto_backend

    backend = get_or_create_backend('main')
    item_table = comp_tbl_class(
        defined_item_component, 'ItemTestTable', 1, backend)
    return item_table


@pytest.fixture
async def filled_item_table(auto_backend, defined_item_component, item_table):
    import asyncio
    comp_tbl_class, get_or_create_backend = auto_backend

    backend = get_or_create_backend('main')
    item_table.flush(force=True)
    item_table.create_or_migrate()
    # 初始化测试数据
    async with backend.transaction(1) as session:
        tbl = item_table.attach(session)
        for i in range(25):
            row = defined_item_component.new_row()
            row.id = 0
            row.name = f'Itm{i + 10}'
            row.owner = 10
            row.time = i + 110
            row.qty = 999
            await tbl.insert(row)
    # 等待replica同步
    while True:
        if await backend.synced():
            break
        await asyncio.sleep(0.001)

    yield item_table

    item_table.flush(force=True)