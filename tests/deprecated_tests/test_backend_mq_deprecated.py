#  """
#  @author: Heerozh (Zhang Jianhao)
#  @copyright: Copyright 2024, Heerozh. All rights reserved.
#  @license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
#  @email: heeroz@gmail.com
#  """

import asyncio
import logging
import time
import unittest
from time import time as time_time
from unittest import mock

import numpy as np

from backend_mgr import UnitTestBackends
from hetu.data import define_component, property_field, BaseComponent, Permission
from hetu.data.backend import (
    ComponentTable, Backend, RedisBackend,
    Subscriptions)
from hetu.system import Context

logger = logging.getLogger('HeTu.root')
logger.setLevel(logging.DEBUG)
logging.lastResort.setLevel(logging.DEBUG)
mock_time = mock.Mock()

# todo 此文件已迁移到新的pytest中，此文件保留作为老的集成测试参考，未来会删除，不要再添加内容

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
            owner: np.int64 = property_field(0, unique=False, index=True)
            model: np.int32 = property_field(0, unique=False, index=True)
            qty: np.int16 = property_field(1, unique=False, index=False)
            level: np.int8 = property_field(1, unique=False, index=False)
            time: np.int64 = property_field(0, unique=True, index=True)
            name: 'U8' = property_field("", unique=True, index=False)
            used: bool = property_field(False, unique=False, index=True)

        @define_component(namespace="ssw")
        class SingleUnique(BaseComponent):
            name: 'U8' = property_field('', unique=True, index=True)
            timestamp: float = property_field(0, unique=False, index=True)

    @classmethod
    def setUpClass(cls):
        cls.build_test_component()

    @classmethod
    def tearDownClass(cls):
        test_backends.teardown()

    async def setUpBackend(self, backend: Backend, table_cls):
        backend.configure()
        if type(backend) is RedisBackend:
            self.assertEqual(
                backend.io.config_get('notify-keyspace-events')[
                    "notify-keyspace-events"],
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
        return backend, item_data

    @classmethod
    def setUpAccount(cls):
        admin_ctx = Context(
            caller=None,
            connection_id=0,
            address="NotSet",
            group="admin",
            user_data={},

            timestamp=0,
            retry_count=0,
            transactions={},
            inherited={}
        )
        user10_ctx = Context(
            caller=10,
            connection_id=0,
            address="NotSet",
            group=None,
            user_data={},

            timestamp=0,
            retry_count=0,
            transactions={},
            inherited={}
        )
        return admin_ctx, user10_ctx

    @classmethod
    def setUpSubscription(cls, backend: Backend):
        # 初始化订阅器
        sub_mgr = Subscriptions(backend)

        async def puller():
            while True:
                await sub_mgr.mq_pull()

        return asyncio.create_task(puller()), sub_mgr

    @classmethod
    async def tearDownAll(self, backend, task):
        # 关闭连接
        task.cancel()
        await backend.close()

    @parameterized(implements)
    async def test_message_queue(self, table_cls: type[ComponentTable],
                                 backend_cls: type[Backend], config):
        backend, item_data = await self.setUpBackend(backend_cls(config), table_cls)
        admin_ctx, user10_ctx = self.setUpAccount()
        task, sub_mgr = self.setUpSubscription(backend)

        # todo 这些老的测试要慢慢移到重构的测试中去

        # 测试订阅的返回值，和订阅管理器的私有值
        sub_id1, row = await sub_mgr.subscribe_select(item_data, admin_ctx, 'Itm10',
                                                      'name')
        self.assertEqual(row['time'], 110)
        self.assertEqual(sub_id1, 'Item.id[1:None:1][:1]')
        self.assertEqual(sub_mgr._subs[sub_id1].row_id, 1)
        self.assertEqual(len(sub_mgr._mq_client.subscribed_channels), 1)

        sub_id2, rows = await sub_mgr.subscribe_query(
            item_data, admin_ctx, 'owner', 10, limit=33)
        self.assertEqual(len(rows), 25)
        self.assertEqual(sub_id2, 'Item.owner[10:None:1][:33]')
        self.assertEqual(len(sub_mgr._subs[sub_id2].channels),
                         25 + 1)  # 加1 index channel
        self.assertEqual(len(sub_mgr._subs[sub_id2].row_subs), 25)
        self.assertEqual(sub_mgr._subs[sub_id2].last_query, {i for i in range(1, 26)})
        first_row_channel = next(iter(sorted(sub_mgr._subs[sub_id2].channels)))
        self.assertEqual(sub_mgr._subs[sub_id2].row_subs[first_row_channel].row_id, 1)
        self.assertEqual(len(sub_mgr._mq_client.subscribed_channels), 26)

        sub_id3, rows = await sub_mgr.subscribe_query(
            item_data, admin_ctx, 'owner', 10, right=11, limit=44)
        self.assertEqual(len(rows), 25)
        self.assertEqual(sub_id3, 'Item.owner[10:11:1][:44]')

        sub_id4, rows = await sub_mgr.subscribe_query(
            item_data, admin_ctx, 'owner', 11, right=12, limit=55)
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
        self.assertEqual(updates[sub_id1]["1"]['owner'], 11)  # row订阅数据更新
        self.assertEqual(updates[sub_id2]["1"], None)  # query 10删除了1
        self.assertEqual(updates[sub_id3]["1"]['owner'], 11)  # query 10-11更新row数据
        self.assertEqual(updates[sub_id4]["1"]['owner'], 11)  # query 11-12更新row数据

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
        self.assertEqual(updates[sub_id1]["1"]['owner'], 12)  # row订阅数据更新
        self.assertEqual(updates[sub_id3]["1"], None)  # query 10-11删除了1
        self.assertEqual(updates[sub_id4]["1"]['owner'], 12)  # query 11-12更新row数据

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
        sub_id5, row = await sub_mgr.subscribe_select(item_data, user10_ctx, 1)
        self.assertEqual(sub_id5, None)
        # 测试订阅单行，owner改变后要删除
        sub_id5, row = await sub_mgr.subscribe_select(item_data, user10_ctx, 3)
        self.assertEqual(row['owner'], 10)
        async with backend.transaction(1) as trx:
            tbl = item_data.attach(trx)
            row = await tbl.select(3)
            row.owner = 11
            await tbl.update(3, row)
        updates = await sub_mgr.get_updates()
        self.assertEqual(updates[sub_id5]["3"], None)

        # 测试owner query只传输owner相等的数据
        sub_id6, rows = await sub_mgr.subscribe_query(
            item_data, user10_ctx, 'owner', 1, right=20, limit=55)
        self.assertEqual([row['owner'] for row in rows], [10] * 23)
        self.assertEqual(len(sub_mgr._subs[sub_id6].row_subs), 23)
        # 测试更新数值，看query的update是否会删除/添加owner相符的
        async with backend.transaction(1) as trx:
            tbl = item_data.attach(trx)
            row = await tbl.select(4)
            row.owner = 11
            await tbl.update(4, row)
        updates = await sub_mgr.get_updates()
        self.assertEqual(len(updates[sub_id6]), 1)
        self.assertEqual(updates[sub_id6]["4"], None)
        # 因为会注册query的所有结果，不管是不是owner相符，所以注册数量又变成了25，这里就不测试了
        # self.assertEqual(len(sub_mgr._subs[sub_id6].row_subs), 25)
        async with backend.transaction(1) as trx:
            tbl = item_data.attach(trx)
            row = await tbl.select(4)
            row.owner = 10
            await tbl.update(4, row)
        updates = await sub_mgr.get_updates()
        self.assertEqual(len(updates[sub_id6]), 1)
        self.assertEqual(updates[sub_id6]["4"]['owner'], 10)
        # 测试insert新数据能否得到通知
        async with backend.transaction(1) as trx:
            tbl = item_data.attach(trx)
            new = Item.new_row()
            new.owner = 10
            await tbl.insert(new)
        updates = await sub_mgr.get_updates()
        self.assertEqual(len(updates[sub_id6]), 1)
        self.assertEqual(updates[sub_id6]["26"]['owner'], 10)

        # 关闭连接
        task.cancel()
        await backend.close()

    @mock.patch('time.time', mock_time)
    @parameterized(implements)
    async def test_mq_pull_stack(self, table_cls: type[ComponentTable],
                                 backend_cls: type[Backend], config):
        # 测试mq消息堆积的情况
        mock_time.return_value = time_time()
        backend, item_data = await self.setUpBackend(backend_cls(config), table_cls)
        admin_ctx, user10_ctx = self.setUpAccount()
        task, sub_mgr = self.setUpSubscription(backend)

        # 初始化订阅器
        sub_mgr = Subscriptions(backend)

        await sub_mgr.subscribe_select(item_data, admin_ctx, 'Itm10', 'name')
        await sub_mgr.subscribe_select(item_data, admin_ctx, 'Itm11', 'name')

        # 先pull空
        try:
            async with asyncio.timeout(0.1):
                await sub_mgr.mq_pull()
                await sub_mgr.mq_pull()

        except TimeoutError:
            pass

        # 修改row1，并pull消息
        async with backend.transaction(1) as trx:
            tbl = item_data.attach(trx)
            row = await tbl.select(1)
            row.qty = 998
            await tbl.update(1, row)
        await asyncio.sleep(0.1)
        await sub_mgr.mq_pull()

        # 2分钟后再次修改row1,row2，此时pull应该会删除前一个row1消息，放入后一个row1消息
        mock_time.return_value = time_time() + 200
        async with backend.transaction(1) as trx:
            tbl = item_data.attach(trx)
            row = await tbl.select(1)
            row.qty = 997
            await tbl.update(1, row)
        await asyncio.sleep(0.1)
        await sub_mgr.mq_pull()
        async with backend.transaction(1) as trx:
            tbl = item_data.attach(trx)
            row = await tbl.select(2)
            row.qty = 997
            await tbl.update(2, row)
        await sub_mgr.mq_pull()

        mq = sub_mgr._mq_client
        mock_time.return_value = time_time() + 210
        notified_channels = await mq.get_message()
        self.assertEqual(len(notified_channels), 2)

        # close backend
        await backend.close()


if __name__ == '__main__':
    unittest.main()
