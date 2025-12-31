import logging
import time
from typing import AsyncGenerator

import pytest

from hetu.common.snowflake_id import SnowflakeID
from hetu.data.backend import Backend, Subscriptions
from hetu.data.backend.sub import IndexSubscription, RowSubscription

SnowflakeID().init(1, 0)


@pytest.fixture
async def sub_mgr(mod_auto_backend) -> AsyncGenerator[Subscriptions]:
    """初始化订阅管理器的fixture"""
    from hetu.data.backend import Subscriptions

    # 初始化订阅器
    sub_mgr = Subscriptions(mod_auto_backend("main"))
    # 清空row订阅缓存
    RowSubscription._RowSubscription__cache = {}  # type: ignore

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
    """管理员权限的ctx（连接上下文）"""
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
        inherited={},
    )


@pytest.fixture
async def user_id10_ctx():
    """用户ID为10的ctx（连接上下文）"""
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
        inherited={},
    )


@pytest.fixture
async def user_id11_ctx():
    """用户ID为11的ctx（连接上下文）"""
    from hetu.system import Context

    return Context(
        caller=11,
        connection_id=0,
        address="NotSet",
        group=None,
        user_data={},
        timestamp=0,
        retry_count=0,
        transactions={},
        inherited={},
    )


async def test_redis_notify_configuration(mod_redis_backend):
    """测试redis的notify-keyspace-events配置是否正确"""
    backend: Backend = mod_redis_backend()
    servant = backend.servant
    master = backend.master

    from hetu.data.backend.redis import RedisBackendClient

    assert type(master) is RedisBackendClient and type(servant) is RedisBackendClient

    # 测试master不应该有通知
    assert (
        master.io.config_get("notify-keyspace-events")["notify-keyspace-events"] == ""  # type: ignore
    )
    # 测试replica应该有通知
    replica_config = await servant.aio.config_get("notify-keyspace-events")
    replica_flags = replica_config["notify-keyspace-events"]
    assert all(flag in replica_flags for flag in list("Kghz"))


async def test_subscribe_get(sub_mgr: Subscriptions, filled_item_ref, admin_ctx):
    """测试get订阅的返回值，和订阅管理器的私有值是否正常"""
    sub_id, row = await sub_mgr.subscribe_get(
        filled_item_ref, admin_ctx, "name", "Itm10"
    )
    assert row
    assert row["time"] == 110
    assert sub_id, "Item.id[1:None:1][:1]"

    row_sub = sub_mgr._subs[sub_id]
    assert type(row_sub) is RowSubscription
    assert row_sub.row_id == row["id"]
    assert len(sub_mgr._mq_client.subscribed_channels) == 1


async def test_subscribe_range(sub_mgr: Subscriptions, filled_item_ref, admin_ctx):
    """测试range订阅的返回值，和订阅管理器的私有值是否正常"""
    sub_id, rows = await sub_mgr.subscribe_range(
        filled_item_ref, admin_ctx, "owner", 10, limit=33
    )
    assert len(rows) == 25
    assert sub_id == "Item.owner[10:None:1][:33]"
    assert len(sub_mgr._subs[sub_id].channels) == 25 + 1  # 加1 index channel

    idx_sub = sub_mgr._subs[sub_id]
    assert type(idx_sub) is IndexSubscription

    assert len(idx_sub.row_subs) == 25
    assert idx_sub.last_range_result == {row["id"] for row in rows}
    first_row_channel = next(iter(sorted(idx_sub.channels)))
    assert idx_sub.row_subs[first_row_channel].row_id == rows[0]["id"]
    assert len(sub_mgr._mq_client.subscribed_channels) == 26

    # 换个范围测试, owner只有10, 应该能查询到
    sub_id, rows = await sub_mgr.subscribe_range(
        filled_item_ref, admin_ctx, "owner", 10, right=11, limit=44
    )
    assert len(rows) == 25
    assert sub_id == "Item.owner[10:11:1][:44]"

    # 查询超出范围的订阅，因为默认force开，所以依然有sub_id
    sub_id, rows = await sub_mgr.subscribe_range(
        filled_item_ref, admin_ctx, "owner", 11, right=12, limit=55
    )
    assert len(rows) == 0
    assert sub_id
    idx_sub = sub_mgr._subs[sub_id]
    assert type(idx_sub) is IndexSubscription

    assert len(idx_sub.row_subs) == 0
    assert sub_id == "Item.owner[11:12:1][:55]"
    assert len(sub_mgr._mq_client.subscribed_channels) == 26


async def test_subscribe_mq_merge_message(
    sub_mgr: Subscriptions, filled_item_ref, admin_ctx, background_mq_puller_task
):
    """测试订阅时，mq消息的合批功能"""
    backend = sub_mgr._backend
    mq = sub_mgr._mq_client

    sub_row, _ = await sub_mgr.subscribe_get(
        filled_item_ref, admin_ctx, "name", "Itm10"
    )

    # 测试mq，2次消息应该只能获得1次合并的
    async with backend.session("pytest", 1) as session:
        select = session.select(filled_item_ref.comp_cls)
        row = await select.get(time=110)
        assert row
        row.qty = 998
        await select.update(row)

    async with backend.session("pytest", 1) as session:
        select = session.select(filled_item_ref.comp_cls)
        row = await select.get(time=110)
        assert row
        row.qty = 997
        await select.update(row)
    await backend.wait_for_synced()

    # mq.get_message必须要后台puller任务在跑，否则消息无法获取
    notified_channels = await mq.get_message()
    assert len(notified_channels) == 1

    # 测试更新消息能否获得，因为我get_message取掉了，应该没有了
    updates = await sub_mgr.get_updates(timeout=0.1)
    assert len(updates) == 0


async def test_subscribe_updates(
    sub_mgr, filled_item_table, admin_ctx, background_mq_puller_task
):
    backend = sub_mgr._backend

    # 测试4种范围的订阅是否正常工作
    sub_row, _ = await sub_mgr.subscribe_select(
        filled_item_table, admin_ctx, "Itm10", "name"
    )
    sub_10, _ = await sub_mgr.subscribe_query(
        filled_item_table, admin_ctx, "owner", 10, limit=33
    )
    sub_10_11, _ = await sub_mgr.subscribe_query(
        filled_item_table, admin_ctx, "owner", 10, right=11, limit=44
    )
    sub_11_12, _ = await sub_mgr.subscribe_query(
        filled_item_table, admin_ctx, "owner", 11, right=12, limit=55
    )

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
    assert updates[sub_row]["1"]["owner"] == 11  # row订阅数据更新
    assert updates[sub_10]["1"] is None  # query 10删除了1
    assert updates[sub_10_11]["1"]["owner"] == 11  # query 10-11更新row数据
    assert updates[sub_11_12]["1"]["owner"] == 11  # query 11-12更新row数据

    # 测试删掉的项目是否成功取消订阅，和增加的成功注册订阅
    assert len(sub_mgr._subs[sub_10].row_subs) == 24
    assert len(sub_mgr._subs[sub_11_12].row_subs) == 1


async def test_row_subscribe_cache(
    sub_mgr, filled_item_table, admin_ctx, background_mq_puller_task
):
    backend = sub_mgr._backend

    # row订阅会用全局cache加速相同数据的更新，当一个row更新时，应该cache中有该值
    sub_row, _ = await sub_mgr.subscribe_select(
        filled_item_table, admin_ctx, "Itm10", "name"
    )
    sub_10, _ = await sub_mgr.subscribe_query(
        filled_item_table, admin_ctx, "owner", 10, limit=33
    )
    sub_10_11, _ = await sub_mgr.subscribe_query(
        filled_item_table, admin_ctx, "owner", 10, right=11, limit=44
    )
    sub_11_12, _ = await sub_mgr.subscribe_query(
        filled_item_table, admin_ctx, "owner", 11, right=12, limit=55
    )

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
    assert updates[sub_row]["1"]["owner"] == 12  # row订阅数据更新
    assert sub_10 not in updates
    assert updates[sub_10_11]["1"] is None  # query 10-11删除了1
    assert updates[sub_11_12]["1"]["owner"] == 12  # query 11-12更新row数据


async def test_cancel_subscribe(sub_mgr, filled_item_table, admin_ctx):
    sub_row, _ = await sub_mgr.subscribe_select(
        filled_item_table, admin_ctx, "Itm10", "name"
    )
    sub_10, _ = await sub_mgr.subscribe_query(
        filled_item_table, admin_ctx, "owner", 10, limit=33
    )
    sub_10_11, _ = await sub_mgr.subscribe_query(
        filled_item_table, admin_ctx, "owner", 10, right=11, limit=44
    )
    sub_11_12, _ = await sub_mgr.subscribe_query(
        filled_item_table, admin_ctx, "owner", 11, right=12, limit=55
    )

    # 测试取消订阅
    assert len(sub_mgr._subs) == 4
    assert len(sub_mgr._mq_client.subscribed_channels) == 26  # 25行+1个index

    await sub_mgr.unsubscribe(sub_10)
    assert len(sub_mgr._subs) == 3
    assert len(sub_mgr._mq_client.subscribed_channels) == 26  # 其他sub依旧订阅所有行

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


async def test_subscribe_select_rls(
    sub_mgr, filled_item_table, user_id10_ctx, user_id11_ctx
):
    # 测试owner不符不给订阅
    sub_id, row = await sub_mgr.subscribe_select(filled_item_table, user_id10_ctx, 1)
    assert sub_id is not None

    sub_id, row = await sub_mgr.subscribe_select(filled_item_table, user_id11_ctx, 1)
    assert sub_id is None


async def test_subscribe_query_rls(sub_mgr, filled_item_table, user_id10_ctx):
    # 先改掉一个人的owner值
    backend = sub_mgr._backend
    async with backend.transaction(1) as session:
        tbl = filled_item_table.attach(session)
        row = await tbl.select(3)
        row.owner = 11
        await tbl.update(3, row)

    # 测试owner query只传输owner相等的数据
    sub_id, rows = await sub_mgr.subscribe_query(
        filled_item_table, user_id10_ctx, "owner", 1, right=20, limit=55
    )
    assert [row["owner"] for row in rows] == [10] * 24
    assert len(sub_mgr._subs[sub_id].row_subs) == 24


async def test_select_subscribe_rls_update(
    sub_mgr, filled_item_table, user_id10_ctx, background_mq_puller_task
):
    backend = sub_mgr._backend

    # 测试订阅单行，owner改变后要删除
    sub_id, row = await sub_mgr.subscribe_select(filled_item_table, user_id10_ctx, 3)
    assert row["owner"] == 10
    async with backend.transaction(1) as session:
        tbl = filled_item_table.attach(session)
        row = await tbl.select(3)
        row.owner = 11
        await tbl.update(3, row)
    updates = await sub_mgr.get_updates()
    assert updates[sub_id]["3"] is None


async def test_query_subscribe_rls_lost(
    sub_mgr, filled_item_table, mod_item_model, user_id10_ctx, background_mq_puller_task
):
    backend = sub_mgr._backend

    sub_id, rows = await sub_mgr.subscribe_query(
        filled_item_table, user_id10_ctx, "owner", 1, right=20, limit=55
    )
    assert len(sub_mgr._subs[sub_id].row_subs) == 25

    # 测试更新数值，看query的update是否会删除/添加owner相符的
    async with backend.transaction(1) as session:
        tbl = filled_item_table.attach(session)
        row = await tbl.select(4)
        row.owner = 11
        await tbl.update(4, row)
    updates = await sub_mgr.get_updates()
    assert len(updates[sub_id]) == 1
    assert updates[sub_id]["4"] is None

    # query订阅的原理是只订阅符合rls的行，但如果数值变了导致失去了某行rls并不会管，由行订阅执行处理
    # 所以注册数量25不变。（但是如果获得了新的rls会管）
    assert len(sub_mgr._subs[sub_id].row_subs) == 25


async def test_query_subscribe_rls_gain(
    sub_mgr, filled_item_table, mod_item_model, user_id10_ctx, background_mq_puller_task
):
    # query订阅的rls gain处理的原理是，每次index变化都检查所有未注册行的rls
    # 如果中途获得rls，就加入订阅

    # 先预先取掉一行rls
    backend = sub_mgr._backend
    async with backend.transaction(1) as session:
        tbl = filled_item_table.attach(session)
        row = await tbl.select(4)
        row.owner = 11
        await tbl.update(4, row)

    sub_id, rows = await sub_mgr.subscribe_query(
        filled_item_table, user_id10_ctx, "owner", 1, right=20, limit=55
    )
    assert len(sub_mgr._subs[sub_id].row_subs) == 24

    # 测试改回来是否重新出现
    async with backend.transaction(1) as session:
        tbl = filled_item_table.attach(session)
        row = await tbl.select(4)
        row.owner = 10
        await tbl.update(4, row)
    updates = await sub_mgr.get_updates()
    assert len(updates[sub_id]) == 1
    assert updates[sub_id]["4"]["owner"] == 10

    assert len(sub_mgr._subs[sub_id].row_subs) == 25

    # 测试insert新数据能否得到通知
    async with backend.transaction(1) as session:
        tbl = filled_item_table.attach(session)
        new = mod_item_model.new_row()
        new.owner = 10
        await tbl.insert(new)
    updates = await sub_mgr.get_updates()
    assert len(updates[sub_id]) == 1
    assert updates[sub_id]["26"]["owner"] == 10


async def test_query_subscribe_rls_lost_without_index(
    sub_mgr,
    filled_rls_test_table,
    mod_rls_test_model,
    user_id11_ctx,
    background_mq_puller_task,
):
    # filled_rls_test_table的权限是要求ctx.caller == row.friend
    # 默认数据是owner=10, friend=11
    backend = sub_mgr._backend

    sub_id, rows = await sub_mgr.subscribe_query(
        filled_rls_test_table, user_id11_ctx, "owner", 1, right=20, limit=55
    )
    assert len(sub_mgr._subs[sub_id].row_subs) == 25

    # 去掉一个
    async with backend.transaction(1) as session:
        tbl = filled_rls_test_table.attach(session)
        row = await tbl.select(4)
        row.friend = 12
        await tbl.update(4, row)
    updates = await sub_mgr.get_updates(timeout=5)
    assert len(updates) == 1
    assert len(updates[sub_id]) == 1
    assert updates[sub_id]["4"] is None

    assert len(sub_mgr._subs[sub_id].row_subs) == 25


@pytest.mark.xfail(reason="目前有已知缺陷，未来也许修也许不修", strict=True)
async def test_query_subscribe_rls_gain_without_index(
    sub_mgr,
    filled_rls_test_table,
    mod_rls_test_model,
    user_id11_ctx,
    background_mq_puller_task,
):
    # filled_rls_test_table的权限是要求ctx.caller == row.friend
    # 默认数据是owner=10, friend=11
    # todo 目前的设计是，rls获得并不能正确得到insert通知，除非query的正是rls属性（这里是friend）
    #      未来如果有需要，可以专门做个rls属性watch，变化了则通知所有IndexSubscription检查rls

    # 先预先取掉一行rls
    backend = sub_mgr._backend
    async with backend.transaction(1) as session:
        tbl = filled_rls_test_table.attach(session)
        row = await tbl.select(4)
        row.friend = 12
        await tbl.update(4, row)
        # 修改owner不应该影响rls
        row = await tbl.select(1)
        row.owner = 12
        await tbl.update(1, row)

    sub_id, rows = await sub_mgr.subscribe_query(
        filled_rls_test_table, user_id11_ctx, "owner", 1, right=20, limit=55
    )
    assert len(sub_mgr._subs[sub_id].row_subs) == 24

    # 测试改回来是否重新出现
    async with backend.transaction(1) as session:
        tbl = filled_rls_test_table.attach(session)
        row = await tbl.select(4)
        row.friend = 11
        await tbl.update(4, row)
    updates = await sub_mgr.get_updates(timeout=5)
    assert len(updates) == 1
    assert len(updates[sub_id]) == 1
    assert updates[sub_id]["4"]["friend"] == 11

    assert len(sub_mgr._subs[sub_id].row_subs) == 25


async def test_mq_backlog(
    monkeypatch, sub_mgr, filled_item_table, mod_item_model, admin_ctx
):
    time_time = time.time
    # 测试mq消息堆积的情况
    backend = sub_mgr._backend

    await sub_mgr.subscribe_select(filled_item_table, admin_ctx, "Itm10", "name")
    await sub_mgr.subscribe_select(filled_item_table, admin_ctx, "Itm11", "name")

    # 预约mq_pull，不然前几次my_pull会没反应
    await sub_mgr.mq_pull()
    await sub_mgr.mq_pull()

    # 修改row1，并pull消息
    async with backend.transaction(1) as trx:
        tbl = filled_item_table.attach(trx)
        row = await tbl.select(1)
        row.qty = 998
        await tbl.update(1, row)
    await backend.wait_for_synced()
    await sub_mgr.mq_pull()

    # 2分钟后再次修改row1,row2，此时pull应该会删除前一个row1消息，放入后一个row1消息
    monkeypatch.setattr(time, "time", lambda: time_time() + 200)
    async with backend.transaction(1) as trx:
        tbl = filled_item_table.attach(trx)
        row = await tbl.select(1)
        row.qty = 997
        await tbl.update(1, row)
    await backend.wait_for_synced()
    await sub_mgr.mq_pull()

    async with backend.transaction(1) as trx:
        tbl = filled_item_table.attach(trx)
        row = await tbl.select(2)
        row.qty = 996
        await tbl.update(2, row)
    await backend.wait_for_synced()
    await sub_mgr.mq_pull()

    mq = sub_mgr._mq_client
    monkeypatch.setattr(time, "time", lambda: time_time() + 210)
    notified_channels = await mq.get_message()
    assert len(notified_channels) == 2
