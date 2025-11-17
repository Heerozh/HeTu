import pytest
import numpy as np

# 当前文件不能有其他地方用mod_auto_backend，否则会冲突
@pytest.fixture
def mod_auto_backend():
    pytest.skip("mod_auto_backend 已在本文件禁用")


async def test_reconnect(auto_backend, mod_item_component):
    # 因为要用不同的连接flush，所以只能用function scope的auto_backend
    # 且当前文件不能有其他地方用mod_auto_backend，否则会冲突
    backend_component_table, get_or_create_backend = auto_backend

    backend = get_or_create_backend('save_test')
    loc_item_table = backend_component_table(
        mod_item_component, 'ItemSaveTestTable', 1, backend)
    loc_item_table.flush(force=True)
    loc_item_table.create_or_migrate()

    # 初始化测试数据
    async with backend.transaction(1) as session:
        tbl = loc_item_table.attach(session)
        for i in range(25):
            row = mod_item_component.new_row()
            row.time = i  # 防止unique冲突
            row.name = f"Item_{i}" # 防止unique冲突
            await tbl.insert(row)
    # 等待replica同步
    await backend.wait_for_synced()

    # 测试保存(断开连接）后再读回来
    async with backend.transaction(1) as session:
        tbl = loc_item_table.attach(session)
        await tbl.delete(1)
        await tbl.delete(9)
    async with backend.transaction(1) as session:
        tbl = loc_item_table.attach(session)
        size = len(await tbl.query('id', -np.inf, +np.inf, limit=999))

    # 测试连接关闭
    await backend.close()  # 不close不能重建backend_component_table
    await backend.close()  # 再次close不该报错
    with pytest.raises(ConnectionError):
        backend.transaction(1)
    with pytest.raises(ConnectionError):
        backend.configure()
    with pytest.raises(ConnectionError):
        await backend.wait_for_synced()
    with pytest.raises(ConnectionError):
        await backend.requires_head_lock()
    with pytest.raises(ConnectionError):
        await backend.get_mq_client()

    # 重新初始化table和连接后再试
    backend = None
    loc_item_table = None
    backend2 = get_or_create_backend('load_test')

    loc_item_table2 = backend_component_table(mod_item_component, 'ItemSaveTestTable',
                                              1, backend2)
    loc_item_table2.create_or_migrate()
    async with backend2.transaction(1) as session:
        tbl = loc_item_table2.attach(session)
        assert len(await tbl.query('id', -np.inf, +np.inf, limit=999)) == size



#
#
#     @parameterized(implements)
#     async def test_migration(self, table_cls: type[ComponentTable],
#                              backend_cls: type[Backend], config):
#         # 测试迁移，先用原定义写入数据
#         backend = backend_cls(config)
#         item_data = table_cls(Item, 'test', 1, backend)
#         item_data.create_or_migrate()
#         item_data.flush(force=True)
#
#         async with backend.transaction(1) as session:
#             tbl = item_data.attach(session)
#             for i in range(25):
#                 row = Item.new_row()
#                 row.id = 0
#                 row.name = f'Itm{i + 10}aaaaaaaaaa'
#                 row.owner = 10
#                 row.time = i + 110
#                 row.qty = 999
#                 await tbl.insert(row)
#
#         # 重新定义新的属性
#         ComponentDefines().clear_()
#
#         @define_component(namespace="ssw")
#         class ItemNew(BaseComponent):
#             owner: np.int64 = Property(0, unique=False, index=True)
#             model: np.int32 = Property(0, unique=False, index=True)
#             qty_new: np.int16 = Property(111, unique=False, index=False)
#             level: np.int8 = Property(1, unique=False, index=False)
#             time: np.int64 = Property(0, unique=True, index=True)
#             name: 'U6' = Property("", unique=True, index=False)
#             used: bool = Property(False, unique=False, index=True)
#
#         # 从ItemNew改名回Item，以便迁移同名的
#         import json
#         define = json.loads(ItemNew.json_)
#         define['component_name'] = 'Item'
#         renamed_new_item_cls = BaseComponent.load_json(json.dumps(define))
#
#         # 测试迁移
#         item_data = table_cls(renamed_new_item_cls, 'test', 2, backend)
#         item_data.create_or_migrate()
#         # 检测跨cluster报错
#         with self.assertRaisesRegex(AssertionError, "cluster"):
#             async with backend.transaction(1) as session:
#                 item_data.attach(session)
#
#         async with backend.transaction(2) as session:
#             tbl = item_data.attach(session)
#             assert (await tbl.select(111, where='time')).name == 'Itm11a'
#             assert (await tbl.select(111, where='time')).qty_new == 111
#
#             assert (await tbl.select('Itm30a', where='name')).name == 'Itm30a'
#             assert (await tbl.select(130, where='time')).qty_new == 111
#
#         await backend.close()
#
#     @parameterized(implements)
#     async def test_flush(self, table_cls: type[ComponentTable],
#                          backend_cls: type[Backend], config):
#         backend = backend_cls(config)
#
#         @define_component(namespace="ssw", persist=False)
#         class TempData(BaseComponent):
#             data: np.int64 = Property(0, unique=True)
#         temp_data = table_cls(TempData, 'test', 1, backend)
#         temp_data.create_or_migrate()
#
#         async with backend.transaction(1) as session:
#             tbl = temp_data.attach(session)
#             for i in range(25):
#                 row = TempData.new_row()
#                 row.data = i
#                 await tbl.insert(row)
#         async with backend.transaction(1) as session:
#             tbl = temp_data.attach(session)
#             assert len(await tbl.query('id', -np.inf, +np.inf == limit=999),
#                              25)
#
#         temp_data.flush()
#
#         async with backend.transaction(1) as session:
#             tbl = temp_data.attach(session)
#             assert len(await tbl.query('id', -np.inf, +np.inf == limit=999),
#                              0)
#
#         await backend.close()
#
