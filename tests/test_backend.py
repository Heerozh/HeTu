import asyncio
import logging
import time
import unittest
from typing import Type
from unittest import mock

import numpy as np

from hetu.data import define_component, Property, BaseComponent, ComponentDefines, Permission
from hetu.data.backend import (
    RaceCondition, UniqueViolation, ComponentTable, Backend, RedisBackend,
    ComponentTransaction, Subscriptions)
from backend_mgr import UnitTestBackends

logger = logging.getLogger('HeTu')
logger.setLevel(logging.DEBUG)
logging.lastResort.setLevel(logging.DEBUG)
mock_time = mock.Mock()


def parameterized(test_items):
    def wrapper(func):
        async def test_wrapper(self):
            for param in test_items:
                with self.subTest(param[0].__name__):
                    await func(self, *param)

        return test_wrapper

    return wrapper


# 要测试新的backend，请添加backend到UnitTestBackends类中
test_backends = UnitTestBackends()
implements = test_backends.get_all_backends()


class TestBackend(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def build_test_component(cls):
        global Item, SingleUnique

        @define_component(namespace="ssw", permission=Permission.OWNER)
        class Item(BaseComponent):
            owner: np.int64 = Property(0, unique=False, index=True)
            model: np.int32 = Property(0, unique=False, index=True)
            qty: np.int16 = Property(1, unique=False, index=False)
            level: np.int8 = Property(1, unique=False, index=False)
            time: np.int64 = Property(0, unique=True, index=True)
            name: 'U8' = Property("", unique=True, index=False)
            used: bool = Property(False, unique=False, index=True)

        @define_component(namespace="ssw")
        class SingleUnique(BaseComponent):
            name: 'U8' = Property('', unique=True, index=True)
            timestamp: float = Property(0, unique=False, index=True)

    @classmethod
    def setUpClass(cls):
        cls.build_test_component()

    @classmethod
    def tearDownClass(cls):
        test_backends.teardown()

    @parameterized(implements)
    async def test_basic(self, table_cls: type[ComponentTable],
                         backend_cls: Type[type[Backend]], config):
        # 测试连接数据库并创建表
        backend = backend_cls(config)
        item_data = table_cls(Item, 'test', 1, backend)
        item_data.create_or_migrate()
        singular_unique = table_cls(SingleUnique, 'test', 1, backend)
        singular_unique.create_or_migrate()

        # 测试insert是否正确
        async with backend.transaction(1) as trx:
            tbl = singular_unique.attach(trx)
            row = SingleUnique.new_row()
            self.assertIsNot(type(row), np.ndarray)
            await tbl.insert(row)
            row_ids = await trx.end_transaction(False)
        self.assertEqual(row_ids, [1])

        async with backend.transaction(1) as trx:
            tbl = singular_unique.attach(trx)
            result = await tbl.query('id', 0, 2)
            self.assertIs(type(result), np.recarray)

        # 测试可用select_or_create
        async with backend.transaction(1) as trx:
            tbl = singular_unique.attach(trx)
            async with tbl.select_or_create('test', 'name') as row:
                self.assertEqual(row.name, 'test')
            async with tbl.select_or_create('', 'name') as row:
                self.assertEqual(row.id, 1)
            row_ids = await trx.end_transaction(False)
        self.assertEqual(row_ids, [2])

        async with backend.transaction(1) as trx:
            tbl = singular_unique.attach(trx)
            result = await tbl.query('name', 'test')
            self.assertEqual(result.id[0], 2)

        # 测试插入数据
        async with backend.transaction(1) as trx:
            tbl = item_data.attach(trx)
            row = Item.new_row()
            row.name = 'Item1'
            row.owner = 1
            row.time = 1
            await tbl.insert(row)

            row.id = 0
            row.name = 'Item2'
            row.owner = 1
            row.time = 2
            await tbl.insert(row)

            row.id = 0
            row.name = 'Item3'
            row.owner = 2
            row.time = 3
            await tbl.insert(row)
            row_ids = await trx.end_transaction(False)
        self.assertEqual(row_ids, [1, 2, 3])

        async with backend.transaction(1) as trx:
            tbl = item_data.attach(trx)
            result = await tbl.query('id', -np.inf, +np.inf)
            np.testing.assert_array_equal(result.id, [1, 2, 3])
            self.assertEqual((await tbl.select(1)).name, 'Item1')
            # 测试第一行dict select不出来的历史bug
            self.assertEqual((await tbl.select('Item1', 'name')).name, 'Item1')
            self.assertIsNot(type(await tbl.select('Item1', 'name')), np.recarray)

        np.testing.assert_array_equal(
            (await item_data.direct_query('id', -np.inf, +np.inf)).id,
            [1, 2, 3])

        # 测试update是否正确
        async with backend.transaction(1) as trx:
            tbl = item_data.attach(trx)
            row = await tbl.select(1)
            row.qty = 2
            await tbl.update(1, row)
        np.testing.assert_array_equal((await item_data.direct_get(1)).qty, 2)

        # 测试插入Unique重复数据
        async with backend.transaction(1) as trx:
            tbl = item_data.attach(trx)
            row.id = 0
            row.name = 'Item2'
            row.owner = 2
            row.time = 999
            with self.assertRaisesRegex(UniqueViolation, "name"):
                await tbl.insert(row)
            row.id = 0
            row.name = 'Item4'
            row.time = 2
            with self.assertRaisesRegex(UniqueViolation, "time"):
                await tbl.insert(row)
            # 测试能用select_or_create
            async with tbl.select_or_create(1, 'owner') as row:
                self.assertEqual(row.owner, 1)

        # 先插入25条数据
        async with backend.transaction(1) as trx:
            tbl = item_data.attach(trx)
            for i in range(25):
                row.id = 0
                row.name = f'Item{i + 10}'
                row.owner = 10
                row.time = i + 10
                await tbl.insert(row)

        # 测试query
        async with backend.transaction(1) as trx:
            tbl = item_data.attach(trx)
            np.testing.assert_array_equal(
                (await tbl.query('time', 10, 15)).time, range(10, 16))
            np.testing.assert_array_equal(
                (await tbl.query('time', 10, 15, desc=True)).time, range(15, 9, -1))
            self.assertEqual((await tbl.query('owner', 10)).shape[0], 10)
            self.assertEqual((await tbl.query('owner', 10, limit=30)).shape[0], 25)
            self.assertEqual((await tbl.query('owner', 10, limit=8)).shape[0], 8)
            self.assertEqual((await tbl.query('owner', 11)).shape[0], 0)
            # query on unique
            with self.assertRaisesRegex(AssertionError, "str"):
                self.assertEqual((await tbl.query('name', 11)).shape[0], 0)
            self.assertEqual((await tbl.query('name', '11')).shape[0], 0)
            self.assertEqual((await tbl.query('name', "Item11")).time, 11)
            np.testing.assert_array_equal(
                (await tbl.query('name', 'Item11', 'Item12')).time,
                [11, 12])
            # query one row
            self.assertEqual((await tbl.query('time', 11)).name, ['Item11'])
            self.assertEqual(len((await tbl.query('time', 11)).name), 1)

        np.testing.assert_array_equal(
            (await item_data.direct_query('name', 'Item11', 'Item12')).time,
            [11, 12])

        # 测试direct get/set
        np.testing.assert_array_equal((await item_data.direct_get(1)).qty, 2)
        await item_data.direct_set(1, qty=911)
        np.testing.assert_array_equal((await item_data.direct_get(1)).qty, 911)

        # update
        async with backend.transaction(1) as trx:
            tbl = item_data.attach(trx)
            row = (await tbl.query('owner', 10))[0]
            old_name = row.name
            self.assertEqual((await tbl.select(old_name, where='name')).name, old_name)
            row.owner = 11
            row.name = 'updated'
            await tbl.update(row.id, row)
            # 测试能否命中cache
            row = await tbl.select(row.id)
            self.assertEqual(row.name, 'updated')
        async with backend.transaction(1) as trx:
            tbl = item_data.attach(trx)
            row = await tbl.select(row.id)  # 测试用numpy type进行select是否报错
            self.assertEqual(row.name, 'updated')
            self.assertEqual((await tbl.query('owner', row.owner, limit=30)).shape[0], 1)
            self.assertEqual((await tbl.query('owner', 10, limit=30)).shape[0], 24)
            self.assertEqual((await tbl.query('owner', 11)).shape[0], 1)
            self.assertEqual((await tbl.query('owner', 11)).name, 'updated')
            self.assertEqual((await tbl.select('updated', where='name')).name, 'updated')
            self.assertEqual(await tbl.select(old_name, where='name'), None)
            self.assertEqual(len(await tbl.query('id', -np.inf, +np.inf, limit=999)), 28)
        async with backend.transaction(1) as trx:
            tbl = item_data.attach(trx)
            row = await tbl.select(5)
            row.used = True
            await tbl.update(row.id, row)
            row = await tbl.select(7)
            row.used = True
            await tbl.update(row.id, row)
        async with backend.transaction(1) as trx:
            tbl = item_data.attach(trx)
            self.assertEqual(set((await tbl.query('used', True)).id), {5, 7})
            self.assertEqual((await tbl.select('Item11', where='name')).id, 5)
            self.assertEqual((await tbl.select('Item13', where='name')).id, 7)
            np.testing.assert_array_equal((await tbl.query('id', 5, 7, limit=999)).id, [5, 6, 7])

        # delete
        async with backend.transaction(1) as trx:
            tbl = item_data.attach(trx)
            await tbl.delete(5)
            await tbl.delete(7)
            # 测试能否命中cache
            row = await tbl.select(5)
            self.assertIs(row, None)
        async with backend.transaction(1) as trx:
            tbl = item_data.attach(trx)
            self.assertEqual(len(await tbl.query('id', -np.inf, +np.inf, limit=999)), 26)
            self.assertEqual(await tbl.select('Item11', where='name'), None)
            self.assertEqual(await tbl.select('Item13', where='name'), None)
            self.assertEqual((await tbl.query('used', True)).shape[0], 0)

        # 测试插入的字符串超出长度是否截断
        async with backend.transaction(1) as trx:
            tbl = item_data.attach(trx)
            row = Item.new_row()
            row.name = "reinsert2"  # 超出U8长度会被截断
            await tbl.insert(row)
        async with backend.transaction(1) as trx:
            tbl = item_data.attach(trx)
            self.assertIsNot(await tbl.select('reinsert', 'name'), None,
                             "超出U8长度应该要被截断，这里没索引出来说明没截断")
            self.assertEqual((await tbl.select('reinsert', 'name')).id, 29)
            self.assertEqual(len(await tbl.query('id', -np.inf, +np.inf, limit=999)), 27)
        # 测试添加再删除
        async with backend.transaction(1) as trx:
            tbl = item_data.attach(trx)
            for i in range(30):
                row.time = row.id + 100
                row.id = 0
                row.name = f're{i}'
                await tbl.insert(row)
        async with backend.transaction(1) as trx:
            tbl = item_data.attach(trx)
            for i in range(30):
                await tbl.delete(59 - i)  # 再删掉
        async with backend.transaction(1) as trx:
            tbl = item_data.attach(trx)
            np.testing.assert_array_equal((await tbl.query('id', 6, 9, limit=999)).id, [6, 8, 9])
            self.assertEqual(len(await tbl.query('id', -np.inf, +np.inf, limit=999)), 27)

        # 测试保存后再读回来
        async with backend.transaction(1) as trx:
            tbl = item_data.attach(trx)
            await tbl.delete(1)
        async with backend.transaction(1) as trx:
            tbl = item_data.attach(trx)
            size = len(await tbl.query('id', -np.inf, +np.inf, limit=999))
        # 重新初始化table和连接后再试
        await backend.close()
        backend = backend_cls(config)
        item_data = table_cls(Item, 'test', 1, backend)
        item_data.create_or_migrate()
        async with backend.transaction(1) as trx:
            tbl = item_data.attach(trx)
            self.assertEqual(len(await tbl.query('id', -np.inf, +np.inf, limit=999)), size)

        # 测试更新name后再把所有key删除后index是否正常为空
        async with backend.transaction(1) as trx:
            tbl = item_data.attach(trx)
            row = await tbl.select(2)
            row.name = f'TST{row.id}'
            await tbl.update(row.id, row)
        async with backend.transaction(1) as trx:
            tbl = item_data.attach(trx)
            rows = await tbl.query('id', -np.inf, +np.inf, limit=999)
            for row in rows:
                await tbl.delete(row.id)
        # time.sleep(1)  # 等待部分key过期
        self.assertEqual(backend.io.keys('test:Item:{CLU*'), [])

        # close
        await backend.close()

    @parameterized(implements)
    async def test_duplicate_op(self, table_cls: type[ComponentTable],
                                backend_cls: Type[type[Backend]], config):
        # 测试重复update
        backend = backend_cls(config)
        item_data = table_cls(Item, 'test', 1, backend)
        item_data.create_or_migrate()

        async with backend.transaction(1) as trx:
            tbl = item_data.attach(trx)
            row = Item.new_row()
            row.time = 12345
            await tbl.insert(row)
            row = Item.new_row()
            row.time = 22345
            await tbl.insert(row)
            self.assertTrue(len(trx._stack) > 0)

        # 检测重复删除报错
        async with backend.transaction(1) as trx:
            tbl = item_data.attach(trx)
            await tbl.delete(1)
            with self.assertRaisesRegex(KeyError, '重复'):
                await tbl.delete(1)
            await trx.end_transaction(discard=True)

        # 检测update没有变化时没有stacked命令
        async with backend.transaction(1) as trx:
            tbl = item_data.attach(trx)
            row = await tbl.select(2)
            row.time = 22345
            await tbl.update(2, row)
            self.assertTrue(len(trx._stack) == 0)

        # 检测重复update/del报错
        async with backend.transaction(1) as trx:
            tbl = item_data.attach(trx)
            row = await tbl.select(2)
            row.time = 32345
            await tbl.update(2, row)
            with self.assertRaisesRegex(KeyError, '重复'):
                await tbl.update(2, row)
            await trx.end_transaction(discard=True)

        async with backend.transaction(1) as trx:
            tbl = item_data.attach(trx)
            row = await tbl.select(2)
            row.time = 32345
            await tbl.delete(2)
            with self.assertRaisesRegex(KeyError, '重复'):
                await tbl.delete(2)
            await trx.end_transaction(discard=True)

        async with backend.transaction(1) as trx:
            tbl = item_data.attach(trx)
            row = await tbl.select(2)
            row.time = 32345
            await tbl.delete(2)
            with self.assertRaisesRegex(KeyError, '再次'):
                await tbl.update(2, row)
            await trx.end_transaction(discard=True)

        async with backend.transaction(1) as trx:
            tbl = item_data.attach(trx)
            row = await tbl.select(2)
            row.time = 32345
            await tbl.update(2, row)
            with self.assertRaisesRegex(KeyError, '再次'):
                await tbl.delete(2)
            await trx.end_transaction(discard=True)

        await backend.close()

    @parameterized(implements)
    async def test_race(self, table_cls: type[ComponentTable],
                        backend_cls: type[Backend], config):
        # 测试竞态，通过2个协程来测试
        backend = backend_cls(config)
        item_data = table_cls(Item, 'test', 1, backend)
        item_data.create_or_migrate()

        # 测试query时，另一个del和update的竞态
        async with backend.transaction(1) as trx:
            tbl = item_data.attach(trx)
            row = Item.new_row()
            row.owner = 65535
            row.name = 'Self'
            row.time = 233874
            await tbl.insert(row)
            row.id = 0
            row.name = 'ForUpdt'
            row.time += 1
            await tbl.insert(row)
            row.id = 0
            row.name = 'ForDel'
            row.time += 1
            await tbl.insert(row)

        # 重写item_data1的query，延迟2秒
        def mock_slow_query(_trx: ComponentTransaction):
            org_query = _trx._db_query

            async def mock_query(*args, **kwargs):
                rtn = await org_query(*args, **kwargs)
                await asyncio.sleep(0.1)
                return rtn

            _trx._db_query = mock_query

        async def query_owner(value):
            async with backend.transaction(1) as _trx:
                _tbl = item_data.attach(_trx)
                mock_slow_query(_tbl)
                rows = await _tbl.query('owner', value, lock_index=False)
                print(rows)

        async def select_owner(value):
            async with backend.transaction(1) as _trx:
                _tbl = item_data.attach(_trx)
                mock_slow_query(_tbl)
                rows = await _tbl.select(value, 'owner')
                print(rows)

        async def del_row(name):
            async with backend.transaction(1) as _trx:
                _tbl = item_data.attach(_trx)
                _row = await _tbl.select(name, 'name')
                await _tbl.delete(_row.id)

        async def update_owner(name):
            async with backend.transaction(1) as _trx:
                _tbl = item_data.attach(_trx)
                _row = await _tbl.select(name, 'name')
                _row.owner = 999
                await _tbl.update(_row.id, _row)

        # 测试del和query竞态是否激发race condition
        task1 = asyncio.create_task(query_owner(65535))
        task2 = asyncio.create_task(del_row('ForDel'))
        await asyncio.gather(task2)
        with self.assertRaises(RaceCondition):
            await task1

        # 测试update和query竞态是否激发race condition
        task1 = asyncio.create_task(query_owner(65535))
        task2 = asyncio.create_task(update_owner('ForUpdt'))
        await asyncio.gather(task2)
        with self.assertRaises(RaceCondition):
            await task1

        # 测试update和select竞态是否激发race condition
        task1 = asyncio.create_task(select_owner(65535))
        task2 = asyncio.create_task(update_owner('Self'))
        await asyncio.gather(task2)
        with self.assertRaises(RaceCondition):
            await task1

        # 测试del和select竞态是否激发race condition
        task1 = asyncio.create_task(select_owner(999))
        task2 = asyncio.create_task(del_row('Self'))
        await asyncio.gather(task2)
        with self.assertRaises(RaceCondition):
            await task1

        # 测试事务提交时unique的RaceCondition, 在end前sleep即可测试
        async def insert_and_sleep(db, uni_val, sleep):
            async with backend.transaction(1) as _trx:
                _tbl = item_data.attach(_trx)
                _row = Item.new_row()
                _row.owner = 874233
                _row.name = str(uni_val)
                _row.time = uni_val
                await _tbl.insert(_row)
                await asyncio.sleep(sleep)

        # 测试insert不同的值应该没有竞态
        task1 = asyncio.create_task(insert_and_sleep(item_data, 111111, 0.1))
        task2 = asyncio.create_task(insert_and_sleep(item_data, 111112, 0.01))
        await asyncio.gather(task2)
        await task1
        # 相同的time会竞态
        task1 = asyncio.create_task(insert_and_sleep(item_data, 222222, 0.1))
        task2 = asyncio.create_task(insert_and_sleep(item_data, 222222, 0.01))
        await asyncio.gather(task2)
        with self.assertRaises(RaceCondition):
            await task1

        # 测试事务提交时的watch的RaceCondition
        async def update_and_sleep(db, sleep):
            async with backend.transaction(1) as _trx:
                _tbl = item_data.attach(_trx)
                _row = await _tbl.select('111111', 'name')
                _row.time = 874233
                await _tbl.update(_row.id, _row)
                await asyncio.sleep(sleep)

        task1 = asyncio.create_task(update_and_sleep(item_data, 0.1))
        task2 = asyncio.create_task(update_and_sleep(item_data, 0.02))
        await asyncio.gather(task2)
        with self.assertRaises(RaceCondition):
            await task1

        # 测试query后该值是否激发竞态
        async def query_then_update(sleep):
            async with backend.transaction(1) as _trx:
                _tbl = item_data.attach(_trx)
                _rows = await _tbl.query('model', 2)
                await asyncio.sleep(sleep)
                if len(_rows) == 0:
                    _row = await _tbl.select(0, 'model')
                    _row.model = 2
                    await _tbl.update(_row.id, _row)

        task1 = asyncio.create_task(query_then_update(0.1))
        task2 = asyncio.create_task(query_then_update(0.02))
        await asyncio.gather(task2)
        with self.assertRaises(RaceCondition):
            await task1

        # close backend
        await backend.close()

    @parameterized(implements)
    async def test_migration(self, table_cls: type[ComponentTable],
                             backend_cls: type[Backend], config):
        # 测试迁移，先用原定义写入数据
        backend = backend_cls(config)
        item_data = table_cls(Item, 'test', 1, backend)
        item_data.create_or_migrate()
        item_data.flush(force=True)

        async with backend.transaction(1) as trx:
            tbl = item_data.attach(trx)
            for i in range(25):
                row = Item.new_row()
                row.id = 0
                row.name = f'Itm{i + 10}aaaaaaaaaa'
                row.owner = 10
                row.time = i + 110
                row.qty = 999
                await tbl.insert(row)

        # 重新定义新的属性
        ComponentDefines().clear_()

        @define_component(namespace="ssw")
        class ItemNew(BaseComponent):
            owner: np.int64 = Property(0, unique=False, index=True)
            model: np.int32 = Property(0, unique=False, index=True)
            qty_new: np.int16 = Property(111, unique=False, index=False)
            level: np.int8 = Property(1, unique=False, index=False)
            time: np.int64 = Property(0, unique=True, index=True)
            name: 'U6' = Property("", unique=True, index=False)
            used: bool = Property(False, unique=False, index=True)

        # 从ItemNew改名回Item，以便迁移同名的
        import json
        define = json.loads(ItemNew.json_)
        define['component_name'] = 'Item'
        renamed_new_item_cls = BaseComponent.load_json(json.dumps(define))

        # 测试迁移
        item_data = table_cls(renamed_new_item_cls, 'test', 2, backend)
        item_data.create_or_migrate()
        # 检测跨cluster报错
        with self.assertRaisesRegex(AssertionError, "cluster"):
            async with backend.transaction(1) as trx:
                item_data.attach(trx)

        async with backend.transaction(2) as trx:
            tbl = item_data.attach(trx)
            self.assertEqual((await tbl.select(111, where='time')).name, 'Itm11a')
            self.assertEqual((await tbl.select(111, where='time')).qty_new, 111)

            self.assertEqual((await tbl.select('Itm30a', where='name')).name, 'Itm30a')
            self.assertEqual((await tbl.select(130, where='time')).qty_new, 111)

        await backend.close()

    @parameterized(implements)
    async def test_flush(self, table_cls: type[ComponentTable],
                         backend_cls: type[Backend], config):
        backend = backend_cls(config)

        @define_component(namespace="ssw", persist=False)
        class TempData(BaseComponent):
            data: np.int64 = Property(0, unique=True)
        temp_data = table_cls(TempData, 'test', 1, backend)
        temp_data.create_or_migrate()

        async with backend.transaction(1) as trx:
            tbl = temp_data.attach(trx)
            for i in range(25):
                row = TempData.new_row()
                row.data = i
                await tbl.insert(row)
        async with backend.transaction(1) as trx:
            tbl = temp_data.attach(trx)
            self.assertEqual(len(await tbl.query('id', -np.inf, +np.inf, limit=999)),
                             25)

        temp_data.flush()

        async with backend.transaction(1) as trx:
            tbl = temp_data.attach(trx)
            self.assertEqual(len(await tbl.query('id', -np.inf, +np.inf, limit=999)),
                             0)

        await backend.close()

    @parameterized(implements)
    async def test_message_queue(self, table_cls: type[ComponentTable],
                                 backend_cls: type[Backend], config):
        backend = backend_cls(config)
        backend.configure()
        if type(backend) is RedisBackend:
            self.assertEqual(
                backend.io.config_get('notify-keyspace-events')["notify-keyspace-events"],
                "")

        item_data = table_cls(Item, 'test', 1, backend)
        item_data.flush(force=True)
        item_data.create_or_migrate()
        # 初始化测试数据
        async with backend.transaction(1) as trx:
            tbl = item_data.attach(trx)
            for i in range(25):
                row = Item.new_row()
                row.id = 0
                row.name = f'Itm{i + 10}'
                row.owner = 10
                row.time = i + 110
                row.qty = 999
                await tbl.insert(row)

        # 等待replica同步，因为不知道backend的类型，所以直接sleep
        time.sleep(0.5)

        # 初始化订阅器
        sub_mgr = Subscriptions(backend)

        async def puller():
            while True:
                await sub_mgr.mq_pull()
        task = asyncio.create_task(puller())

        # 测试订阅的返回值，和订阅管理器的私有值
        sub_id1, row = await sub_mgr.subscribe_select(item_data, 'admin', 'Itm10', 'name')
        self.assertEqual(row['time'], '110')
        self.assertEqual(sub_id1, 'Item.id[1:None:1][:1]')
        self.assertEqual(sub_mgr._subs[sub_id1].row_id, 1)
        self.assertEqual(len(sub_mgr._mq_client.subscribed_channels), 1)

        sub_id2, rows = await sub_mgr.subscribe_query(
            item_data, 'admin', 'owner', 10, limit=33)
        self.assertEqual(len(rows), 25)
        self.assertEqual(sub_id2, 'Item.owner[10:None:1][:33]')
        self.assertEqual(len(sub_mgr._subs[sub_id2].channels), 25 + 1)  # 加1 index channel
        self.assertEqual(len(sub_mgr._subs[sub_id2].row_subs), 25)
        self.assertEqual(sub_mgr._subs[sub_id2].last_query, {i for i in range(1, 26)})
        first_row_channel = next(iter(sorted(sub_mgr._subs[sub_id2].channels)))
        self.assertEqual(sub_mgr._subs[sub_id2].row_subs[first_row_channel].row_id, 1)
        self.assertEqual(len(sub_mgr._mq_client.subscribed_channels), 26)

        sub_id3, rows = await sub_mgr.subscribe_query(
            item_data, 'admin', 'owner', 10, right=11, limit=44)
        self.assertEqual(len(rows), 25)
        self.assertEqual(sub_id3, 'Item.owner[10:11:1][:44]')

        sub_id4, rows = await sub_mgr.subscribe_query(
            item_data, 'admin', 'owner', 11, right=12, limit=55)
        self.assertEqual(len(rows), 0)
        self.assertEqual(len(sub_mgr._subs[sub_id4].row_subs), 0)
        self.assertEqual(sub_id4, 'Item.owner[11:12:1][:55]')
        self.assertEqual(len(sub_mgr._mq_client.subscribed_channels), 26)

        # 先把mq里的订阅消息都取出来清空
        mq = sub_mgr._mq_client
        try:
            async with asyncio.timeout(0.1):
                await mq.get_message()
                await mq.get_message()
        except TimeoutError:
            pass

        # 测试mq，2次消息应该只能获得1次合并的
        async with backend.transaction(1) as trx:
            tbl = item_data.attach(trx)
            row = await tbl.select(1)
            row.qty = 998
            await tbl.update(1, row)
        await asyncio.sleep(0.01)
        async with backend.transaction(1) as trx:
            tbl = item_data.attach(trx)
            row = await tbl.select(1)
            row.qty = 997
            await tbl.update(1, row)
        mq = sub_mgr._mq_client
        await asyncio.sleep(0.1)
        notified_channels = await mq.get_message()
        self.assertEqual(len(notified_channels), 1)

        # 测试更新消息能否获得
        updates = await sub_mgr.get_updates(timeout=0.1)
        self.assertEqual(len(updates), 0)

        async with backend.transaction(1) as trx:
            tbl = item_data.attach(trx)
            row = await tbl.select(1)
            row.owner = 11
            await tbl.update(1, row)

        updates = await sub_mgr.get_updates()
        self.assertEqual(len(updates), 4)
        self.assertEqual(updates[sub_id1][1]['owner'], '11')  # row订阅数据更新
        self.assertEqual(updates[sub_id2][1], None)           # query 10删除了1
        self.assertEqual(updates[sub_id3][1]['owner'], '11')  # query 10-11更新row数据
        self.assertEqual(updates[sub_id4][1]['owner'], '11')  # query 11-12更新row数据

        # 测试删掉的项目是否成功取消订阅，和增加的成功注册订阅
        self.assertEqual(len(sub_mgr._subs[sub_id2].row_subs), 24)
        self.assertEqual(len(sub_mgr._subs[sub_id4].row_subs), 1)

        # 测试第二次更新cache是否清空了
        async with backend.transaction(1) as trx:
            tbl = item_data.attach(trx)
            row = await tbl.select(1)
            row.owner = 12
            await tbl.update(1, row)

        updates = await sub_mgr.get_updates()
        self.assertEqual(len(updates), 3)
        self.assertEqual(updates[sub_id1][1]['owner'], '12')  # row订阅数据更新
        self.assertEqual(updates[sub_id3][1], None)  # query 10-11删除了1
        self.assertEqual(updates[sub_id4][1]['owner'], '12')  # query 11-12更新row数据

        # 测试取消订阅
        self.assertEqual(len(sub_mgr._subs), 4)
        self.assertEqual(len(sub_mgr._mq_client.subscribed_channels), 26)

        await sub_mgr.unsubscribe(sub_id2)
        self.assertEqual(len(sub_mgr._subs), 3)
        self.assertEqual(len(sub_mgr._mq_client.subscribed_channels), 26)

        await sub_mgr.unsubscribe(sub_id3)
        self.assertEqual(len(sub_mgr._channel_subs), 2)
        self.assertEqual(len(sub_mgr._mq_client.subscribed_channels), 2)

        await sub_mgr.unsubscribe(sub_id1)
        self.assertEqual(len(sub_mgr._channel_subs), 2)
        self.assertEqual(len(sub_mgr._mq_client.subscribed_channels), 2)
        await sub_mgr.unsubscribe(sub_id1)  # 测试重复取消订阅没变化
        self.assertEqual(len(sub_mgr._channel_subs), 2)
        self.assertEqual(len(sub_mgr._mq_client.subscribed_channels), 2)

        await sub_mgr.unsubscribe(sub_id4)
        self.assertEqual(len(sub_mgr._channel_subs), 0)
        self.assertEqual(len(sub_mgr._mq_client.subscribed_channels), 0)

        # 测试owner不符不给订阅
        sub_id5, row = await sub_mgr.subscribe_select(item_data, 10, 1)
        self.assertEqual(sub_id5, None)
        # 测试订阅单行，owner改变后要删除
        sub_id5, row = await sub_mgr.subscribe_select(item_data, 10, 3)
        self.assertEqual(row['owner'], '10')
        async with backend.transaction(1) as trx:
            tbl = item_data.attach(trx)
            row = await tbl.select(3)
            row.owner = 11
            await tbl.update(3, row)
        updates = await sub_mgr.get_updates()
        self.assertEqual(updates[sub_id5][3], None)

        # 测试owner query只传输owner相等的数据
        sub_id6, rows = await sub_mgr.subscribe_query(
            item_data, 10, 'owner', 1, right=20, limit=55)
        self.assertEqual([row['owner'] for row in rows], ['10'] * 23)
        self.assertEqual(len(sub_mgr._subs[sub_id6].row_subs), 23)
        # 测试更新数值，看query的update是否会删除/添加owner相符的
        async with backend.transaction(1) as trx:
            tbl = item_data.attach(trx)
            row = await tbl.select(4)
            row.owner = 11
            await tbl.update(4, row)
        updates = await sub_mgr.get_updates()
        self.assertEqual(len(updates[sub_id6]), 1)
        self.assertEqual(updates[sub_id6][4], None)
        # 因为会注册query的所有结果，不管是不是owner相符，所以注册数量又变成了25，这里就不测试了
        # self.assertEqual(len(sub_mgr._subs[sub_id6].row_subs), 25)
        async with backend.transaction(1) as trx:
            tbl = item_data.attach(trx)
            row = await tbl.select(4)
            row.owner = 10
            await tbl.update(4, row)
        updates = await sub_mgr.get_updates()
        self.assertEqual(len(updates[sub_id6]), 1)
        self.assertEqual(updates[sub_id6][4]['owner'], '10')
        # 测试insert新数据能否得到通知
        async with backend.transaction(1) as trx:
            tbl = item_data.attach(trx)
            new = Item.new_row()
            new.owner = 10
            await tbl.insert(new)
        updates = await sub_mgr.get_updates()
        self.assertEqual(len(updates[sub_id6]), 1)
        self.assertEqual(updates[sub_id6][26]['owner'], '10')

        # 关闭连接
        task.cancel()
        await backend.close()

    @parameterized(implements)
    async def test_update_or_insert_race_bug(
            self, table_cls: type[ComponentTable],backend_cls: type[Backend], config
    ):
        # 测试select_or_create UniqueViolation是否转化为了RaceCondition
        backend = backend_cls(config)
        item_data = table_cls(Item, 'test', 1, backend)
        item_data.create_or_migrate()

        async def main_task():
            async with backend.transaction(1) as trx:
                tbl = item_data.attach(trx)
                async with tbl.select_or_create('uni_vio', 'name') as row:
                    await asyncio.sleep(0.1)
                    row.qty = 1

        async def trouble_task():
            async with backend.transaction(1) as _trx:
                _tbl = item_data.attach(_trx)
                row = Item.new_row()
                row.name = 'uni_vio'
                await _tbl.insert(row)

        task1 = asyncio.create_task(main_task())
        task2 = asyncio.create_task(trouble_task())
        await asyncio.gather(task2)
        with self.assertRaises(RaceCondition):
            await task1

        # close backend
        await backend.close()


if __name__ == '__main__':
    unittest.main()
