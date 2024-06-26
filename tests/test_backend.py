import asyncio
import logging
import time
import unittest
from unittest import mock

import docker
import numpy as np

logger = logging.getLogger('HeTu')
logger.setLevel(logging.DEBUG)
logging.lastResort.setLevel(logging.DEBUG)
mock_time = mock.Mock()

from hetu.data import (
    define_component, Property, BaseComponent, ComponentDefines,
    ComponentBackend, BackendClientPool, ComponentTransaction,
    RedisComponentBackend, RedisBackendClientPool,
    UniqueViolation, RaceCondition
    )


def parameterized(test_items):
    def wrapper(func):
        async def test_wrapper(self):
            for param in test_items:
                with self.subTest(param[0].__name__):
                    await func(self, *param)
        return test_wrapper
    return wrapper


implements = (
    (RedisComponentBackend, RedisBackendClientPool, {"master": "redis://127.0.0.1:23318/0"}),
    # 所有其他类型table和后端在此添加并通过测试，并在下方"# 启动服务器"处启动对应的docker
)


class TestBackend(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def build_test_component(cls):
        global Item, SingleUnique

        @define_component(namespace="ssw")
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

    def __init__(self, module_name='runTest'):
        super().__init__(module_name)
        ComponentDefines().clear_()
        self.build_test_component()

        # docker启动对应的backend
        self.containers = []
        client = docker.from_env()
        # 先删除已启动的
        try:
            client.containers.get('hetu_test_redis').kill()
            client.containers.get('hetu_test_redis').remove()
        except (docker.errors.NotFound, docker.errors.APIError):
            pass
        # 启动服务器
        self.containers.append(
            client.containers.run("redis:latest", detach=True, ports={'6379/tcp': 23318},
                                  name='hetu_test_redis', auto_remove=True)
        )

    def __del__(self):
        for container in self.containers:
            try:
                container.kill()
            except (docker.errors.NotFound, ImportError):
                pass

    @parameterized(implements)
    async def test_basic(self, table_cls: type[ComponentBackend],
                         backend_cls: type[BackendClientPool], config):
        ComponentDefines().clear_()
        self.build_test_component()

        # 测试连接数据库并创建表
        backend = backend_cls(config)
        item_data = table_cls(Item, 'test', 1, backend)
        singular_unique = table_cls(SingleUnique, 'test', 1, backend)

        # 测试insert是否正确
        async with singular_unique.transaction() as tbl:
            row = SingleUnique.new_row()
            self.assertIsNot(type(row), np.ndarray)
            await tbl.insert(row)

        async with singular_unique.transaction() as tbl:
            result = await tbl.query('id', 0, 2)
            self.assertIs(type(result), np.recarray)

        # 测试可用select_or_create
        async with singular_unique.transaction() as tbl:
            row = await tbl.select_or_create('test', 'name')
            self.assertEqual(row.name, 'test')
            row = await tbl.select_or_create('', 'name')
            self.assertEqual(row.id, 1)
            # 测试能否命中cache
            row = await tbl.select(2)
            self.assertEqual(row.id, 2)

        async with singular_unique.transaction() as tbl:
            result = await tbl.query('name', 'test')
            self.assertEqual(result.id[0], 2)

        # 测试插入数据
        async with item_data.transaction() as tbl:
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

        async with item_data.transaction() as tbl:
            result = await tbl.query('id', -np.inf, +np.inf)
            np.testing.assert_array_equal(result.id, [1, 2, 3])
            self.assertEqual((await tbl.select(1)).name, 'Item1')
            # 测试第一行dict select不出来的历史bug
            self.assertEqual((await tbl.select('Item1', 'name')).name, 'Item1')
            self.assertIsNot(type(await tbl.select('Item1', 'name')), np.recarray)

        # 测试插入Unique重复数据
        async with item_data.transaction() as tbl:
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
            # 测试不能用select_or_create
            with self.assertRaisesRegex(AssertionError, "select_or_create"):
                await tbl.select_or_create(1, 'owner')

        # 先插入25条数据
        async with item_data.transaction() as tbl:
            for i in range(25):
                row.id = 0
                row.name = f'Item{i+10}'
                row.owner = 10
                row.time = i+10
                await tbl.insert(row)

        # 测试query
        async with item_data.transaction() as tbl:
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
            # query one row
            self.assertEqual((await tbl.query('time', 11)).name, ['Item11'])
            self.assertEqual(len((await tbl.query('time', 11)).name), 1)

        # update
        async with item_data.transaction() as tbl:
            row = (await tbl.query('owner', 10))[0]
            old_name = row.name
            self.assertEqual((await tbl.select(old_name, where='name')).name, old_name)
            row.owner = 11
            row.name = 'updated'
            await tbl.update(row.id, row)
            # 测试能否命中cache
            row = await tbl.select(row.id)
            self.assertEqual(row.name, 'updated')
        async with item_data.transaction() as tbl:
            row = await tbl.select(row.id)  # 测试用numpy type进行select是否报错
            self.assertEqual(row.name, 'updated')
            self.assertEqual((await tbl.query('owner', row.owner, limit=30)).shape[0], 1)
            self.assertEqual((await tbl.query('owner', 10, limit=30)).shape[0], 24)
            self.assertEqual((await tbl.query('owner', 11)).shape[0], 1)
            self.assertEqual((await tbl.query('owner', 11)).name, 'updated')
            self.assertEqual((await tbl.select('updated', where='name')).name, 'updated')
            self.assertEqual(await tbl.select(old_name, where='name'), None)
            self.assertEqual(len(await tbl.query('id', -np.inf, +np.inf, limit=999)), 28)
        async with item_data.transaction() as tbl:
            row = await tbl.select(5)
            row.used = True
            await tbl.update(row.id, row)
            row = await tbl.select(7)
            row.used = True
            await tbl.update(row.id, row)
        async with item_data.transaction() as tbl:
            self.assertEqual(set((await tbl.query('used', True)).id), {5, 7})
            self.assertEqual((await tbl.select('Item11', where='name')).id, 5)
            self.assertEqual((await tbl.select('Item13', where='name')).id, 7)
            np.testing.assert_array_equal((await tbl.query('id', 5, 7, limit=999)).id, [5, 6, 7])

        # delete
        async with item_data.transaction() as tbl:
            await tbl.delete(5)
            await tbl.delete(7)
            # 测试能否命中cache
            row = await tbl.select(5)
            self.assertEqual(row, 'deleted')
        async with item_data.transaction() as tbl:
            self.assertEqual(len(await tbl.query('id', -np.inf, +np.inf, limit=999)), 26)
            self.assertEqual(await tbl.select('Item11', where='name'), None)
            self.assertEqual(await tbl.select('Item13', where='name'), None)
            self.assertEqual((await tbl.query('used', True)).shape[0], 0)

        # 测试插入的字符串超出长度是否截断
        async with item_data.transaction() as tbl:
            row = Item.new_row()
            row.name = "reinsert2"  # 超出U8长度会被截断
            await tbl.insert(row)
        async with item_data.transaction() as tbl:
            self.assertIsNot(await tbl.select('reinsert', 'name'), None,
                             "超出U8长度应该要被截断，这里没索引出来说明没截断")
            self.assertEqual((await tbl.select('reinsert', 'name')).id, row.id)
            self.assertEqual(len(await tbl.query('id', -np.inf, +np.inf, limit=999)), 27)
        # 测试添加再删除
        async with item_data.transaction() as tbl:
            for i in range(30):
                row.time = row.id+100
                row.id = 0
                row.name = f're{i}'
                await tbl.insert(row)
        async with item_data.transaction() as tbl:
            for i in range(30):
                await tbl.delete(59-i)  # 再删掉
        async with item_data.transaction() as tbl:
            np.testing.assert_array_equal((await tbl.query('id', 6, 9, limit=999)).id, [6, 8, 9])
            self.assertEqual(len(await tbl.query('id', -np.inf, +np.inf, limit=999)), 27)

        # 测试保存后再读回来
        async with item_data.transaction() as tbl:
            await tbl.delete(1)
        async with item_data.transaction() as tbl:
            autoinc = await tbl._backend_get_max_id()
            size = len(await tbl.query('id', -np.inf, +np.inf, limit=999))
        # 重新初始化table和连接后再试
        await backend.close()
        backend = backend_cls(config)
        item_data = table_cls(Item, 'test', 1, backend)
        async with item_data.transaction() as tbl:
            self.assertEqual(await tbl._backend_get_max_id(), autoinc)
            self.assertEqual(len(await tbl.query('id', -np.inf, +np.inf, limit=999)), size)
        await backend.close()

    @parameterized(implements)
    async def test_race(self, table_cls: type[ComponentBackend],
                        backend_cls: type[BackendClientPool], config):
        # 测试竞态，通过2个协程来测试
        ComponentDefines().clear_()
        self.build_test_component()
        backend = backend_cls(config)
        item_data = table_cls(Item, 'test', 1, backend)

        # 测试query时，另一个del和update的竞态
        async with item_data.transaction() as tbl:
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
        def mock_slow_query(trans: ComponentTransaction):
            org_query = trans._backend_query

            async def mock_query(*args, **kwargs):
                rtn = await org_query(*args, **kwargs)
                await asyncio.sleep(1)
                return rtn
            trans._backend_query = mock_query

        async def query_owner(value):
            async with item_data.transaction() as _tbl:
                mock_slow_query(_tbl)
                rows = await _tbl.query('owner', value)
                print(rows)

        async def select_owner(value):
            async with item_data.transaction() as _tbl:
                mock_slow_query(_tbl)
                rows = await _tbl.select(value, 'owner')
                print(rows)

        async def del_row(name):
            async with item_data.transaction() as _tbl:
                _row = await _tbl.select(name, 'name')
                await _tbl.delete(_row.id)

        async def update_owner(name):
            async with item_data.transaction() as _tbl:
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
        async def insert_and_sleep(db, sleep):
            async with db.transaction() as _tbl:
                _row = Item.new_row()
                _row.owner = 874233
                _row.name = 'TstRace'
                _row.time = 874233
                await _tbl.insert(_row)
                await asyncio.sleep(sleep)
        task1 = asyncio.create_task(insert_and_sleep(item_data, 1))
        task2 = asyncio.create_task(insert_and_sleep(item_data, 0.2))
        await asyncio.gather(task2)
        with self.assertRaises(RaceCondition):
            await task1

        # 测试事务提交时的watch的RaceCondition
        async def update_and_sleep(db, sleep):
            async with db.transaction() as _tbl:
                _row = await _tbl.select('TstRace', 'name')
                _row.time = 8742333
                await _tbl.update(_row.id, _row)
                await asyncio.sleep(sleep)
        task1 = asyncio.create_task(update_and_sleep(item_data, 1))
        task2 = asyncio.create_task(update_and_sleep(item_data, 0.2))
        await asyncio.gather(task2)
        with self.assertRaises(RaceCondition):
            await task1

        await backend.close()

    # @mock.patch('hetu.data.backend.redis.datetime', mock_time)
    @parameterized(implements)
    async def test_migration(self, table_cls: type[ComponentBackend],
                             backend_cls: type[BackendClientPool], config):
        # mock_time.now.return_value = datetime.now()
        # # mock_time.now.return_value = datetime.now() + timedelta(days=10)
        # mock_time.fromisoformat = datetime.fromisoformat
        # 测试迁移，先用原定义写入数据
        ComponentDefines().clear_()
        self.build_test_component()

        backend = backend_cls(config)
        item_data = table_cls(Item, 'test', 1, backend)

        async with item_data.transaction() as tbl:
            for i in range(25):
                row = Item.new_row()
                row.id = 0
                row.name = f'Itm{i+10}aaaaaaaaaa'
                row.owner = 10
                row.time = i+110
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
        # 从ItemNew改名回Item
        import json
        define = json.loads(ItemNew.json_)
        define['component_name'] = 'Item'
        renamed_new_item_cls = BaseComponent.load_json(json.dumps(define))

        # 测试迁移
        item_data = table_cls(renamed_new_item_cls, 'test', 2, backend)
        async with item_data.transaction() as tbl:
            self.assertEqual((await tbl.select(111, where='time')).name, 'Itm11a')
            self.assertEqual((await tbl.select(111, where='time')).qty_new, 111)

            self.assertEqual((await tbl.select('Itm30a', where='name')).name, 'Itm30a')
            self.assertEqual((await tbl.select(130, where='time')).qty_new, 111)
        await backend.close()

    @parameterized(implements)
    async def test_benchmark(self, table_cls: type[ComponentBackend],
                             backend_cls: type[BackendClientPool], config):
        # 基本的bench，要求一定的吞吐量
        ComponentDefines().clear_()
        # self.build_test_component()
        #
        # backend = backend_cls(config)
        # item_data = table_cls(Item, 'test', 1, backend)
        #
        # import os
        # print("pid:", os.getpid())
        #
        # async def timeit(func, repeat=1, repeat_mul=1):
        #     s = time.perf_counter()
        #     await asyncio.gather(*[func() for _ in range(repeat)])
        #     cost = time.perf_counter() - s
        #     print(f"{func.__name__} 耗时: {cost:.2f}s, QPS: {repeat*repeat_mul/cost:.0f}/s")
        #     return cost, repeat*repeat_mul / cost
        #
        # # 1. 插入速度
        # async def test_insert_300k(count, starts=0):
        #     semaphore = asyncio.Semaphore(100)
        #     for i in range(count):
        #     async with item_data as tbl:
        #         row = Item.new_row()
        #         row.name = f'Item{i+starts}'
        #         row.owner = i+starts
        #         row.time = i+starts
        #         row.model = i
        #         await item_data.insert(row)
        #
        # t, qps = await timeit(test_insert_300k, 1, 3000)
        # # object array: 12.3秒30w次，平均24000/s (6个index) | 7.8秒30w次，平均38000/s (2个index)
        # # struct array: 18秒 16000/s (6个index) | 11秒30w次，平均28000/s (2个index)
        # self.assertLess(t, 25.0, f"耗时{t:.2f}秒")


if __name__ == '__main__':
    unittest.main()
