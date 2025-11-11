import logging
import pytest
import asyncio
from hetu.data.backend import ComponentTable
from hetu.data.backend.sub import RowSubscription

logger = logging.getLogger('HeTu.root')
logger.setLevel(logging.DEBUG)
logging.lastResort.setLevel(logging.DEBUG)


@pytest.fixture
async def sub_mgr(mod_auto_backend):
    """初始化订阅管理器的fixture"""
    comp_tbl_class, get_or_create_backend = mod_auto_backend

    from hetu.data.backend import Subscriptions
    # 初始化订阅器
    sub_mgr = Subscriptions(get_or_create_backend('main'))

    yield sub_mgr

    await sub_mgr.close()


@pytest.fixture
async def background_mq_puller_task(sub_mgr):
    """启动一个后台任务不断pull mq消息的fixture"""

    async def puller():
        while True:
            await sub_mgr.mq_pull()

    import asyncio
    task = asyncio.create_task(puller())
    yield task

    task.cancel()


@pytest.fixture
async def admin_ctx():
    from hetu.system import Context
    return Context(
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


@pytest.fixture
async def user_id10_ctx():
    from hetu.system import Context
    return Context(
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


async def test_redis_notify_configuration(mod_redis_backend):
    comp_tbl_class, get_or_create_backend = mod_redis_backend
    backend = get_or_create_backend()

    from hetu.data.backend import RedisBackend
    assert type(backend) is RedisBackend

    # 测试master不应该有通知
    assert backend.io.config_get('notify-keyspace-events')[
               "notify-keyspace-events"] == ""
    # 测试replica应该有通知
    replica_config = await backend.replicas[0].config_get('notify-keyspace-events')
    replica_flags = replica_config["notify-keyspace-events"]
    assert all(flag in replica_flags for flag in list("Kghz"))


async def test_subscribe_select(sub_mgr, filled_item_table, admin_ctx):
    # 测试select订阅的返回值，和订阅管理器的私有值是否正常
    sub_id, row = await sub_mgr.subscribe_select(
        filled_item_table, admin_ctx, 'Itm10', 'name')
    assert row['time'] == 110
    assert sub_id, 'Item.id[1:None:1][:1]'
    assert sub_mgr._subs[sub_id].row_id == 1
    assert len(sub_mgr._mq_client.subscribed_channels) == 1


async def test_subscribe_query(sub_mgr, filled_item_table, admin_ctx):
    # 测试query订阅的返回值，和订阅管理器的私有值是否正常
    sub_id, rows = await sub_mgr.subscribe_query(
        filled_item_table, admin_ctx, 'owner', 10, limit=33)
    assert len(rows) == 25
    assert sub_id == 'Item.owner[10:None:1][:33]'
    assert len(sub_mgr._subs[sub_id].channels) == 25 + 1  # 加1 index channel
    assert len(sub_mgr._subs[sub_id].row_subs) == 25
    assert sub_mgr._subs[sub_id].last_query == {i for i in range(1, 26)}
    first_row_channel = next(iter(sorted(sub_mgr._subs[sub_id].channels)))
    assert sub_mgr._subs[sub_id].row_subs[first_row_channel].row_id == 1
    assert len(sub_mgr._mq_client.subscribed_channels) == 26

    # 换个范围测试, owner只有10, 应该能查询到
    sub_id, rows = await sub_mgr.subscribe_query(
        filled_item_table, admin_ctx, 'owner', 10, right=11, limit=44)
    assert len(rows) == 25
    assert sub_id == 'Item.owner[10:11:1][:44]'

    # 查询超出范围的订阅
    sub_id, rows = await sub_mgr.subscribe_query(
        filled_item_table, admin_ctx, 'owner', 11, right=12, limit=55)
    assert len(rows) == 0
    assert len(sub_mgr._subs[sub_id].row_subs) == 0
    assert sub_id == 'Item.owner[11:12:1][:55]'
    assert len(sub_mgr._mq_client.subscribed_channels) == 26


async def test_subscribe_mq_merge_message(sub_mgr, mod_item_table: ComponentTable,
                                          filled_item_table, background_mq_puller_task):
    backend = sub_mgr._backend
    mq = sub_mgr._mq_client

    # 测试mq，2次消息应该只能获得1次合并的
    async with backend.transaction(1) as session:
        tbl = mod_item_table.attach(session)
        row = await tbl.select(1)
        row.qty = 998
        await tbl.update(1, row)
    await asyncio.sleep(0.01)

    async with backend.transaction(1) as session:
        tbl = mod_item_table.attach(session)
        row = await tbl.select(1)
        row.qty = 997
        await tbl.update(1, row)
    await asyncio.sleep(0.1)

    # mq.get_message必须要后台puller任务在跑，否则消息无法获取
    notified_channels = await mq.get_message()
    assert len(notified_channels) == 1

    # 测试更新消息能否获得，因为我get_message取掉了，应该没有了
    updates = await sub_mgr.get_updates(timeout=0.1)
    assert len(updates) == 0


async def test_subscribe_updates(sub_mgr, filled_item_table, admin_ctx,
                                 background_mq_puller_task):
    backend = sub_mgr._backend

    # 测试4种范围的订阅是否正常工作
    sub_row, _ = await sub_mgr.subscribe_select(
        filled_item_table, admin_ctx, 'Itm10', 'name')
    sub_10, _ = await sub_mgr.subscribe_query(
        filled_item_table, admin_ctx, 'owner', 10, limit=33)
    sub_10_11, _ = await sub_mgr.subscribe_query(
        filled_item_table, admin_ctx, 'owner', 10, right=11, limit=44)
    sub_11_12, _ = await sub_mgr.subscribe_query(
        filled_item_table, admin_ctx, 'owner', 11, right=12, limit=55)

    # 初始数据是25行owner = 10
    assert len(sub_mgr._subs[sub_10].row_subs) == 25
    assert len(sub_mgr._subs[sub_11_12].row_subs) == 0

    # 更改行1的owner从10到11
    async with backend.transaction(1) as session:
        tbl = filled_item_table.attach(session)
        row = await tbl.select(1)
        row.owner = 11
        await tbl.update(1, row)

    # 测试更新
    updates = await sub_mgr.get_updates()
    assert len(updates) == 4
    assert updates[sub_row]["1"]['owner'] == 11  # row订阅数据更新
    assert updates[sub_10]["1"] is None  # query 10删除了1
    assert updates[sub_10_11]["1"]['owner'] == 11  # query 10-11更新row数据
    assert updates[sub_11_12]["1"]['owner'] == 11  # query 11-12更新row数据

    # 测试删掉的项目是否成功取消订阅，和增加的成功注册订阅
    assert len(sub_mgr._subs[sub_10].row_subs) == 24
    assert len(sub_mgr._subs[sub_11_12].row_subs) == 1


async def test_row_subscribe_cache(sub_mgr, filled_item_table, admin_ctx,
                                   background_mq_puller_task):
    backend = sub_mgr._backend

    # row订阅会用全局cache加速相同数据的更新，当一个row更新时，应该cache中有该值
    sub_row, _ = await sub_mgr.subscribe_select(
        filled_item_table, admin_ctx, 'Itm10', 'name')
    sub_10, _ = await sub_mgr.subscribe_query(
        filled_item_table, admin_ctx, 'owner', 10, limit=33)
    sub_10_11, _ = await sub_mgr.subscribe_query(
        filled_item_table, admin_ctx, 'owner', 10, right=11, limit=44)
    sub_11_12, _ = await sub_mgr.subscribe_query(
        filled_item_table, admin_ctx, 'owner', 11, right=12, limit=55)

    # 更改行1的owner从10到11
    async with backend.transaction(1) as session:
        tbl = filled_item_table.attach(session)
        row = await tbl.select(1)
        row.owner = 11
        await tbl.update(1, row)

    # 检测Row cache
    await sub_mgr.get_updates()
    # 由于不同backend的channel名不一样，使用dict的第一个channel
    cache = RowSubscription._RowSubscription__cache
    first_channel = next(iter(cache.keys()))
    assert cache[first_channel]["1"]["owner"] == 11

    # 测试第二次更新cache是否清空了
    async with backend.transaction(1) as trx:
        tbl = filled_item_table.attach(trx)
        row = await tbl.select(1)
        row.owner = 12
        await tbl.update(1, row)

    updates = await sub_mgr.get_updates()
    # 如果数据正确说明更新了
    assert cache[first_channel]["1"]["owner"] == 12
    # 其他顺带检测
    assert len(updates) == 3
    assert updates[sub_row]["1"]['owner'] == 12  # row订阅数据更新
    assert sub_10 not in updates
    assert updates[sub_10_11]["1"] is None  # query 10-11删除了1
    assert updates[sub_11_12]["1"]['owner'] == 12  # query 11-12更新row数据


async def test_cancel_subscribe(sub_mgr, filled_item_table, admin_ctx,
                                   background_mq_puller_task):
    sub_row, _ = await sub_mgr.subscribe_select(
        filled_item_table, admin_ctx, 'Itm10', 'name')
    sub_10, _ = await sub_mgr.subscribe_query(
        filled_item_table, admin_ctx, 'owner', 10, limit=33)
    sub_10_11, _ = await sub_mgr.subscribe_query(
        filled_item_table, admin_ctx, 'owner', 10, right=11, limit=44)
    sub_11_12, _ = await sub_mgr.subscribe_query(
        filled_item_table, admin_ctx, 'owner', 11, right=12, limit=55)

    # 测试取消订阅
    assert len(sub_mgr._subs) == 4
    assert len(sub_mgr._mq_client.subscribed_channels) == 26 # 25行+1个index

    await sub_mgr.unsubscribe(sub_10)
    assert len(sub_mgr._subs) == 3
    assert len(sub_mgr._mq_client.subscribed_channels) == 26 # 其他sub依旧订阅所有行

    await sub_mgr.unsubscribe(sub_row)
    assert len(sub_mgr._subs) == 2
    assert len(sub_mgr._channel_subs) == 26  # 10 row还是被sub_10_11订阅着
    assert len(sub_mgr._mq_client.subscribed_channels) == 26
    # 测试重复取消订阅没变化
    await sub_mgr.unsubscribe(sub_row)
    assert len(sub_mgr._subs) == 2
    assert len(sub_mgr._channel_subs) == 26
    assert len(sub_mgr._mq_client.subscribed_channels) == 26

    await sub_mgr.unsubscribe(sub_10_11)
    assert len(sub_mgr._subs) == 1
    assert len(sub_mgr._channel_subs) == 1
    assert len(sub_mgr._mq_client.subscribed_channels) == 1

    await sub_mgr.unsubscribe(sub_11_12)
    assert len(sub_mgr._subs) == 0
    assert len(sub_mgr._channel_subs) == 0
    assert len(sub_mgr._mq_client.subscribed_channels) == 0
#
#     # 测试owner不符不给订阅
#     sub_id5, row = await sub_mgr.subscribe_select(item_data, user10_ctx, 1)
#     assert sub_id5 == None
#     # 测试订阅单行，owner改变后要删除
#     sub_id5, row = await sub_mgr.subscribe_select(item_data, user10_ctx, 3)
#     assert row['owner'] == 10
#     async with backend.transaction(1) as trx:
#         tbl = item_data.attach(trx)
#         row = await tbl.select(3)
#         row.owner = 11
#         await tbl.update(3, row)
#     updates = await sub_mgr.get_updates()
#     assert updates[sub_id5]["3"] == None
#
#     # 测试owner query只传输owner相等的数据
#     sub_id6, rows = await sub_mgr.subscribe_query(
#         item_data, user10_ctx, 'owner', 1, right=20, limit=55)
#     assert [row['owner'] for row in rows] == [10] * 23
#     assert len(sub_mgr._subs[sub_id6].row_subs) == 23
#     # 测试更新数值，看query的update是否会删除/添加owner相符的
#     async with backend.transaction(1) as trx:
#         tbl = item_data.attach(trx)
#         row = await tbl.select(4)
#         row.owner = 11
#         await tbl.update(4, row)
#     updates = await sub_mgr.get_updates()
#     assert len(updates[sub_id6]) == 1
#     assert updates[sub_id6]["4"] == None
#     # 因为会注册query的所有结果，不管是不是owner相符，所以注册数量又变成了25，这里就不测试了
#     # assert len(sub_mgr._subs[sub_id6].row_subs) == 25
#     async with backend.transaction(1) as trx:
#         tbl = item_data.attach(trx)
#         row = await tbl.select(4)
#         row.owner = 10
#         await tbl.update(4, row)
#     updates = await sub_mgr.get_updates()
#     assert len(updates[sub_id6]) == 1
#     assert updates[sub_id6]["4"]['owner'] == 10
#     # 测试insert新数据能否得到通知
#     async with backend.transaction(1) as trx:
#         tbl = item_data.attach(trx)
#         new = Item.new_row()
#         new.owner = 10
#         await tbl.insert(new)
#     updates = await sub_mgr.get_updates()
#     assert len(updates[sub_id6]) == 1
#     assert updates[sub_id6]["26"]['owner'] == 10
#
#     # 关闭连接
#     task.cancel()
#     await backend.close()
#
# @mock.patch('time.time', mock_time)
# @parameterized(implements)
# async def test_mq_pull_stack(self, table_cls: type[ComponentTable],
#                              backend_cls: type[Backend], config):
#     # 测试mq消息堆积的情况
#     mock_time.return_value = time_time()
#     backend, item_data = await self.setUpBackend(backend_cls(config), table_cls)
#     admin_ctx, user10_ctx = self.setUpAccount()
#     task, sub_mgr = self.setUpSubscription(backend)
#
#     # 初始化订阅器
#     sub_mgr = Subscriptions(backend)
#
#     await sub_mgr.subscribe_select(item_data, admin_ctx, 'Itm10', 'name')
#     await sub_mgr.subscribe_select(item_data, admin_ctx, 'Itm11', 'name')
#
#     # 先pull空
#     try:
#         async with asyncio.timeout(0.1):
#             await sub_mgr.mq_pull()
#             await sub_mgr.mq_pull()
#
#     except TimeoutError:
#         pass
#
#     # 修改row1，并pull消息
#     async with backend.transaction(1) as trx:
#         tbl = item_data.attach(trx)
#         row = await tbl.select(1)
#         row.qty = 998
#         await tbl.update(1, row)
#     await asyncio.sleep(0.1)
#     await sub_mgr.mq_pull()
#
#     # 2分钟后再次修改row1,row2，此时pull应该会删除前一个row1消息，放入后一个row1消息
#     mock_time.return_value = time_time() + 200
#     async with backend.transaction(1) as trx:
#         tbl = item_data.attach(trx)
#         row = await tbl.select(1)
#         row.qty = 997
#         await tbl.update(1, row)
#     await asyncio.sleep(0.1)
#     await sub_mgr.mq_pull()
#     async with backend.transaction(1) as trx:
#         tbl = item_data.attach(trx)
#         row = await tbl.select(2)
#         row.qty = 997
#         await tbl.update(2, row)
#     await sub_mgr.mq_pull()
#
#     mq = sub_mgr._mq_client
#     mock_time.return_value = time_time() + 210
#     notified_channels = await mq.get_message()
#     assert len(notified_channels) == 2
#
#     # close backend
#     await backend.close()
#
