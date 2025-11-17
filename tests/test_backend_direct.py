import pytest
import numpy as np
from debugpy.common.messaging import Disconnect


async def test_table_direct_get_set(filled_item_table):
    # 测试direct get/set
    np.testing.assert_array_equal((await filled_item_table.direct_get(1)).qty, 999)
    await filled_item_table.direct_set(1, qty=911)
    np.testing.assert_array_equal((await filled_item_table.direct_get(1)).qty, 911)
    # 测试direct set index and direct query index
    await filled_item_table.direct_set(1, owner=911)
    np.testing.assert_array_equal((await filled_item_table.direct_get(1)).owner, 911)
    np.testing.assert_array_equal((await filled_item_table.direct_query('owner', 911)).owner, 911)

async def test_table_direct_insert_delete(filled_item_table):
    # 测试direct insert and direct delete
    row_ids = await filled_item_table.direct_insert(owner=912, time=912)
    np.testing.assert_array_equal((await filled_item_table.direct_get(row_ids[0])).owner, 912)
    np.testing.assert_array_equal((await filled_item_table.direct_query('owner', 912)).owner, 912)
    await filled_item_table.direct_delete(row_ids[0])

async def test_table_direct_query(filled_item_table):
    # test direct query
    np.testing.assert_array_equal(
        (await filled_item_table.direct_query('name', 'Itm11', 'Itm12')).time,
        [111, 112])











#
#         # 测试更新name后再把所有key删除后index是否正常为空
#         async with backend.transaction(1) as session:
#             tbl = item_data.attach(session)
#             row = await tbl.select(2)
#             row.name = f'TST{row.id}'
#             await tbl.update(row.id, row)
#         async with backend.transaction(1) as session:
#             tbl = item_data.attach(session)
#             rows = await tbl.query('id', -np.inf, +np.inf, limit=999)
#             for row in rows:
#                 await tbl.delete(row.id)
#         # time.sleep(1)  # 等待部分key过期
#         assert backend.io.keys('test:Item:{CLU*') == []
#
#         # close
#         await backend.close()
#
#     @parameterized(implements)
#     async def test_duplicate_op(self, table_cls: type[ComponentTable],
#                                 backend_cls: Type[type[Backend]], config):
#         # 测试重复update
#         backend = backend_cls(config)
#         item_data = table_cls(Item, 'test', 1, backend)
#         item_data.create_or_migrate()
#
#         async with backend.transaction(1) as session:
#             tbl = item_data.attach(session)
#             row = Item.new_row()
#             row.time = 12345
#             await tbl.insert(row)
#             row = Item.new_row()
#             row.time = 22345
#             await tbl.insert(row)
#             self.assertTrue(len(session._stack) > 0)
#
#         # 检测重复删除报错
#         async with backend.transaction(1) as session:
#             tbl = item_data.attach(session)
#             await tbl.delete(1)
#             with self.assertRaisesRegex(KeyError, '重复'):
#                 await tbl.delete(1)
#             await session.end_transaction(discard=True)
#
#         # 检测update没有变化时没有stacked命令
#         async with backend.transaction(1) as session:
#             tbl = item_data.attach(session)
#             row = await tbl.select(2)
#             row.time = 22345
#             await tbl.update(2, row)
#             self.assertTrue(len(session._stack) == 0)
#
#         # 检测重复update/del报错
#         async with backend.transaction(1) as session:
#             tbl = item_data.attach(session)
#             row = await tbl.select(2)
#             row.time = 32345
#             await tbl.update(2, row)
#             with self.assertRaisesRegex(KeyError, '重复'):
#                 await tbl.update(2, row)
#             await session.end_transaction(discard=True)
#
#         async with backend.transaction(1) as session:
#             tbl = item_data.attach(session)
#             row = await tbl.select(2)
#             row.time = 32345
#             await tbl.delete(2)
#             with self.assertRaisesRegex(KeyError, '重复'):
#                 await tbl.delete(2)
#             await session.end_transaction(discard=True)
#
#         async with backend.transaction(1) as session:
#             tbl = item_data.attach(session)
#             row = await tbl.select(2)
#             row.time = 32345
#             await tbl.delete(2)
#             with self.assertRaisesRegex(KeyError, '再次'):
#                 await tbl.update(2, row)
#             await session.end_transaction(discard=True)
#
#         async with backend.transaction(1) as session:
#             tbl = item_data.attach(session)
#             row = await tbl.select(2)
#             row.time = 32345
#             await tbl.update(2, row)
#             with self.assertRaisesRegex(KeyError, '再次'):
#                 await tbl.delete(2)
#             await session.end_transaction(discard=True)
#
#         await backend.close()
#
#     @parameterized(implements)
#     async def test_race(self, table_cls: type[ComponentTable],
#                         backend_cls: type[Backend], config):
#         # 测试竞态，通过2个协程来测试
#         backend = backend_cls(config)
#         item_data = table_cls(Item, 'test', 1, backend)
#         item_data.create_or_migrate()
#
#         # 测试query时，另一个del和update的竞态
#         async with backend.transaction(1) as session:
#             tbl = item_data.attach(session)
#             row = Item.new_row()
#             row.owner = 65535
#             row.name = 'Self'
#             row.time = 233874
#             await tbl.insert(row)
#             row.id = 0
#             row.name = 'ForUpdt'
#             row.time += 1
#             await tbl.insert(row)
#             row.id = 0
#             row.name = 'ForDel'
#             row.time += 1
#             await tbl.insert(row)
#
#         # 重写item_data1的query，延迟2秒
#         def mock_slow_query(_trx: ComponentTransaction):
#             org_query = _trx._db_query
#
#             async def mock_query(*args, **kwargs):
#                 rtn = await org_query(*args, **kwargs)
#                 await asyncio.sleep(0.1)
#                 return rtn
#
#             _trx._db_query = mock_query
#
#         async def query_owner(value):
#             async with backend.transaction(1) as _trx:
#                 _tbl = item_data.attach(_trx)
#                 mock_slow_query(_tbl)
#                 rows = await _tbl.query('owner', value, lock_index=False)
#                 print(rows)
#
#         async def select_owner(value):
#             async with backend.transaction(1) as _trx:
#                 _tbl = item_data.attach(_trx)
#                 mock_slow_query(_tbl)
#                 rows = await _tbl.select(value, 'owner')
#                 print(rows)
#
#         async def del_row(name):
#             async with backend.transaction(1) as _trx:
#                 _tbl = item_data.attach(_trx)
#                 _row = await _tbl.select(name, 'name')
#                 await _tbl.delete(_row.id)
#
#         async def update_owner(name):
#             async with backend.transaction(1) as _trx:
#                 _tbl = item_data.attach(_trx)
#                 _row = await _tbl.select(name, 'name')
#                 _row.owner = 999
#                 await _tbl.update(_row.id, _row)
#
#         # 测试del和query竞态是否激发race condition
#         task1 = asyncio.create_task(query_owner(65535))
#         task2 = asyncio.create_task(del_row('ForDel'))
#         await asyncio.gather(task2)
#         with self.assertRaises(RaceCondition):
#             await task1
#
#         # 测试update和query竞态是否激发race condition
#         task1 = asyncio.create_task(query_owner(65535))
#         task2 = asyncio.create_task(update_owner('ForUpdt'))
#         await asyncio.gather(task2)
#         with self.assertRaises(RaceCondition):
#             await task1
#
#         # 测试update和select竞态是否激发race condition
#         task1 = asyncio.create_task(select_owner(65535))
#         task2 = asyncio.create_task(update_owner('Self'))
#         await asyncio.gather(task2)
#         with self.assertRaises(RaceCondition):
#             await task1
#
#         # 测试del和select竞态是否激发race condition
#         task1 = asyncio.create_task(select_owner(999))
#         task2 = asyncio.create_task(del_row('Self'))
#         await asyncio.gather(task2)
#         with self.assertRaises(RaceCondition):
#             await task1
#
#         # 测试事务提交时unique的RaceCondition, 在end前sleep即可测试
#         async def insert_and_sleep(db, uni_val, sleep):
#             async with backend.transaction(1) as _trx:
#                 _tbl = item_data.attach(_trx)
#                 _row = Item.new_row()
#                 _row.owner = 874233
#                 _row.name = str(uni_val)
#                 _row.time = uni_val
#                 await _tbl.insert(_row)
#                 await asyncio.sleep(sleep)
#
#         # 测试insert不同的值应该没有竞态
#         task1 = asyncio.create_task(insert_and_sleep(item_data, 111111, 0.1))
#         task2 = asyncio.create_task(insert_and_sleep(item_data, 111112, 0.01))
#         await asyncio.gather(task2)
#         await task1
#         # 相同的time会竞态
#         task1 = asyncio.create_task(insert_and_sleep(item_data, 222222, 0.1))
#         task2 = asyncio.create_task(insert_and_sleep(item_data, 222222, 0.01))
#         await asyncio.gather(task2)
#         with self.assertRaises(RaceCondition):
#             await task1
#
#         # 测试事务提交时的watch的RaceCondition
#         async def update_and_sleep(db, sleep):
#             async with backend.transaction(1) as _trx:
#                 _tbl = item_data.attach(_trx)
#                 _row = await _tbl.select('111111', 'name')
#                 _row.time = 874233
#                 await _tbl.update(_row.id, _row)
#                 await asyncio.sleep(sleep)
#
#         task1 = asyncio.create_task(update_and_sleep(item_data, 0.1))
#         task2 = asyncio.create_task(update_and_sleep(item_data, 0.02))
#         await asyncio.gather(task2)
#         with self.assertRaises(RaceCondition):
#             await task1
#
#         # 测试query后该值是否激发竞态
#         async def query_then_update(sleep):
#             async with backend.transaction(1) as _trx:
#                 _tbl = item_data.attach(_trx)
#                 _rows = await _tbl.query('model', 2)
#                 await asyncio.sleep(sleep)
#                 if len(_rows) == 0:
#                     _row = await _tbl.select(0, 'model')
#                     _row.model = 2
#                     await _tbl.update(_row.id, _row)
#
#         task1 = asyncio.create_task(query_then_update(0.1))
#         task2 = asyncio.create_task(query_then_update(0.02))
#         await asyncio.gather(task2)
#         with self.assertRaises(RaceCondition):
#             await task1
#
#         # close backend
#         await backend.close()
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
#     @parameterized(implements)
#     async def test_update_or_insert_race_bug(
#             self, table_cls: type[ComponentTable],backend_cls: type[Backend], config
#     ):
#         # 测试update_or_insert UniqueViolation是否转化为了RaceCondition
#         backend = backend_cls(config)
#         item_data = table_cls(Item, 'test', 1, backend)
#         item_data.create_or_migrate()
#
#         async def main_task():
#             async with backend.transaction(1) as session:
#                 tbl = item_data.attach(session)
#                 async with tbl.update_or_insert('uni_vio', 'name') as row:
#                     await asyncio.sleep(0.1)
#                     row.qty = 1
#
#         async def trouble_task():
#             async with backend.transaction(1) as _trx:
#                 _tbl = item_data.attach(_trx)
#                 row = Item.new_row()
#                 row.name = 'uni_vio'
#                 await _tbl.insert(row)
#
#         task1 = asyncio.create_task(main_task())
#         task2 = asyncio.create_task(trouble_task())
#         await asyncio.gather(task2)
#         with self.assertRaises(RaceCondition):
#             await task1
#
#         # close backend
#         await backend.close()
#
