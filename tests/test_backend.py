import unittest
import numpy as np
import asyncio
from hetu.data import (
    define_component, Property, BaseComponent, ComponentDefines, RedisComponentTable, RedisBackend,
    UniqueViolation,
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
    (RedisComponentTable, RedisBackend, {"master": "redis://127.0.0.1:23318/0"}),
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
        import docker
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
            container.kill()

    @parameterized(implements)
    async def test_basic(self, table_cls, backend_cls, config):
        ComponentDefines().clear_()
        self.build_test_component()

        # 测试连接数据库并创建表
        backend = backend_cls(config)
        item_data = table_cls(Item, 'test', 1, backend)
        singular_unique = table_cls(SingleUnique, 'test', 1, backend)

        # 测试insert是否正确
        async with singular_unique as tbl:
            row = SingleUnique.new_row()
            self.assertIsNot(type(row), np.ndarray)
            await tbl.insert(row)

        async with singular_unique as tbl:
            result = await tbl.query('id', 0, 2)
            self.assertIs(type(result), np.recarray)

        # 测试可用select_or_create
        async with singular_unique as tbl:
            row = await tbl.select_or_create('test', 'name')
            self.assertEqual(row.name, 'test')
            row = await tbl.select_or_create('', 'name')
            self.assertEqual(row.id, 1)
            # 测试能否命中cache
            row = await tbl.select(2)
            self.assertEqual(row.id, 2)

        async with singular_unique as tbl:
            result = await tbl.query('name', 'test')
            self.assertEqual(result.id[0], 2)

        # 测试插入数据
        async with item_data as tbl:
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

        async with item_data as tbl:
            result = await tbl.query('id', -np.inf, +np.inf)
            np.testing.assert_array_equal(result.id, [1, 2, 3])
            self.assertEqual((await tbl.select(1)).name, 'Item1')
            # 测试第一行dict select不出来的历史bug
            self.assertEqual((await item_data.select('Item1', 'name')).name, 'Item1')
            self.assertIsNot(type(await item_data.select('Item1', 'name')), np.recarray)

        # 测试插入Unique重复数据
        async with item_data as tbl:
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
        async with item_data as tbl:
            for i in range(25):
                row.id = 0
                row.name = f'Item{i+10}'
                row.owner = 10
                row.time = i+10
                await tbl.insert(row)

        # 测试query
        async with item_data as tbl:
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
        async with item_data as tbl:
            row = (await tbl.query('owner', 10))[0]
            old_name = row.name
            self.assertEqual((await tbl.select(old_name, where='name')).name, old_name)
            row.owner = 11
            row.name = 'updated'
            await tbl.update(row.id, row)
            # 测试能否命中cache
            row = await tbl.select(row.id)
            self.assertEqual(row.name, 'updated')
        async with item_data as tbl:
            row = await tbl.select(row.id)  # 测试用numpy type进行select是否报错
            self.assertEqual(row.name, 'updated')
            self.assertEqual((await tbl.query('owner', row.owner, limit=30)).shape[0], 1)
            self.assertEqual((await tbl.query('owner', 10, limit=30)).shape[0], 24)
            self.assertEqual((await tbl.query('owner', 11)).shape[0], 1)
            self.assertEqual((await tbl.query('owner', 11)).name, 'updated')
            self.assertEqual((await tbl.select('updated', where='name')).name, 'updated')
            self.assertEqual(await tbl.select(old_name, where='name'), None)
            self.assertEqual(len(await tbl.query('id', -np.inf, +np.inf, limit=999)), 28)
        async with item_data as tbl:
            row = await tbl.select(5)
            row.used = True
            await tbl.update(row.id, row)
            row = await tbl.select(7)
            row.used = True
            await tbl.update(row.id, row)
        async with item_data as tbl:
            self.assertEqual(set((await tbl.query('used', True)).id), {5, 7})
            self.assertEqual((await tbl.select('Item11', where='name')).id, 5)
            self.assertEqual((await tbl.select('Item13', where='name')).id, 7)
            np.testing.assert_array_equal((await tbl.query('id', 5, 7, limit=999)).id, [5, 6, 7])

        # delete
        async with item_data as tbl:
            await tbl.delete(5)
            await tbl.delete(7)
            # 测试能否命中cache
            row = await tbl.select(5)
            self.assertEqual(row, 'deleted')
        async with item_data as tbl:
            self.assertEqual(len(await tbl.query('id', -np.inf, +np.inf, limit=999)), 26)
            self.assertEqual(await tbl.select('Item11', where='name'), None)
            self.assertEqual(await tbl.select('Item13', where='name'), None)
            self.assertEqual((await tbl.query('used', True)).shape[0], 0)

        # 测试插入的字符串超出长度是否截断
        async with item_data as tbl:
            row = Item.new_row()
            row.name = "reinsert2"  # 超出U8长度会被截断
            await tbl.insert(row)
        async with item_data as tbl:
            self.assertIsNot(await tbl.select('reinsert', 'name'), None,
                             "超出U8长度应该要被截断，这里没索引出来说明没截断")
            self.assertEqual((await tbl.select('reinsert', 'name')).id, row.id)
            self.assertEqual(len(await tbl.query('id', -np.inf, +np.inf, limit=999)), 27)
        # 测试添加再删除
        async with item_data as tbl:
            for i in range(30):
                row.time = row.id+100
                row.id = 0
                row.name = f're{i}'
                await tbl.insert(row)
        async with item_data as tbl:
            for i in range(30):
                await tbl.delete(59-i)  # 再删掉
        async with item_data as tbl:
            np.testing.assert_array_equal((await tbl.query('id', 6, 9, limit=999)).id, [6, 8, 9])
            self.assertEqual(len(await tbl.query('id', -np.inf, +np.inf, limit=999)), 27)

        # 测试保存后再读回来
        async with item_data as tbl:
            await tbl.delete(1)
        async with item_data as tbl:
            autoinc = await tbl._backend_get_max_id()
            size = len(await tbl.query('id', -np.inf, +np.inf, limit=999))
        # 重新初始化table和连接后再试
        backend = backend_cls(config)
        item_data = table_cls(Item, 'test', 1, backend)
        async with item_data as tbl:
            self.assertEqual(await tbl._backend_get_max_id(), autoinc)
            self.assertEqual(len(await tbl.query('id', -np.inf, +np.inf, limit=999)), size)

    @parameterized(implements)
    async def test_race(self, table_cls, backend_cls, config):
        # 测试竞态，通过2个协程来测试

        ComponentDefines().clear_()
        self.build_test_component()
        backend = backend_cls(config)
        item_data1 = table_cls(Item, 'test', 1, backend)
        item_data2 = table_cls(Item, 'test', 1, backend)

        # 测试select时，另一个del和update的竞态
        async with item_data1 as tbl:
            row = Item.new_row()
            row.owner = 65535
            row.name = 'Fixed'
            row.time = 233874
            await tbl.insert(row)
            row.id = 0
            row.name = 'ForUpdate'
            row.time += 1
            await tbl.insert(row)
            row.id = 0
            row.name = 'ForDel'
            row.time += 1
            await tbl.insert(row)

        async def query_rows():
            async with item_data1 as _tbl:
                rows = await _tbl.query('owner', 65535)

        async def del_and_update():
            async with item_data2 as _tbl:
                _row = await _tbl.select('ForDel', 'name')
                await _tbl.delete(_row.id)
                _row = await _tbl.select('ForUpdate', 'name')
                _row.owner = 999
                await _tbl.update(_row.id, _row)

        task1 = asyncio.create_task(query_rows())
        task2 = asyncio.create_task(del_and_update())
        await asyncio.gather(task1, task2)

    @parameterized(implements)
    async def test_migration(self, table_cls, backend_cls, config):
        # 测试迁移
        pass

    @parameterized(implements)
    async def test_benchmark(self, table_cls, backend_cls, config):
        pass


if __name__ == '__main__':
    unittest.main()
