import asyncio
from typing import AsyncGenerator, cast

import pytest
from fixtures.backends import use_redis_family_backend_only

from hetu.common.snowflake_id import SnowflakeID
from hetu.data.backend import Backend, RowFormat
from hetu.data.sub import (
    IndexSubscription,
    RowSubscription,
    SubscriptionBroker,
    TableSubscription,
)

SnowflakeID().init(1, 0)


@pytest.fixture
async def broker(mod_auto_backend) -> AsyncGenerator[SubscriptionBroker]:
    """初始化订阅管理器的fixture"""

    # 初始化订阅器
    broker = SubscriptionBroker(mod_auto_backend("main"))

    yield broker

    await broker.close()


async def wait_until(pred, timeout: float = 2.0):
    """等后端 hub 把通知投递到 mq 的本地队列：轮询直到 pred() 为真"""
    async with asyncio.timeout(timeout):
        while not pred():
            await asyncio.sleep(0.01)


@pytest.fixture
async def admin_ctx():
    """管理员权限的ctx（连接上下文）"""
    from hetu.system import SystemContext

    return SystemContext(
        caller=0,
        connection_id=0,
        address="NotSet",
        group="admin",
        user_data={},
        timestamp=0,
        request=None,  # type: ignore
        systems=None,  # type: ignore
    )


@pytest.fixture
async def user_id10_ctx():
    """用户ID为10的ctx（连接上下文）"""
    from hetu.system import SystemContext

    return SystemContext(
        caller=10,
        connection_id=0,
        address="NotSet",
        group="",
        user_data={},
        timestamp=0,
        request=None,  # type: ignore
        systems=None,  # type: ignore
    )


@pytest.fixture
async def user_id11_ctx():
    """用户ID为11的ctx（连接上下文）"""
    from hetu.system import SystemContext

    return SystemContext(
        caller=11,
        connection_id=0,
        address="NotSet",
        group="",
        user_data={},
        timestamp=0,
        request=None,  # type: ignore
        systems=None,  # type: ignore
    )


@use_redis_family_backend_only
async def test_redis_notify_configuration(mod_auto_backend):
    """测试redis的notify-keyspace-events配置是否正确"""
    backend: Backend = mod_auto_backend()
    servant = backend.servant
    master = backend.master

    from hetu.data.backend.redis import RedisBackendClient

    assert type(master) is RedisBackendClient and type(servant) is RedisBackendClient

    # 测试master不应该有通知（如果有servants时）
    if master.endpoint != servant.endpoint:
        assert (
            master.io.config_get("notify-keyspace-events")["notify-keyspace-events"]  # type: ignore
            == ""
        )
    # 测试replica应该有通知
    replica_config = await servant.aio.config_get("notify-keyspace-events")
    replica_flags = replica_config["notify-keyspace-events"]
    assert all(flag in replica_flags for flag in list("Kz"))


async def test_subscribe_get(broker: SubscriptionBroker, filled_item_ref, admin_ctx):
    """测试get订阅的返回值，和订阅管理器的私有值是否正常"""
    sub_id, row = await broker.subscribe_get(
        filled_item_ref, admin_ctx, "name", "Itm10"
    )
    assert row
    assert row["time"] == 110
    assert sub_id, "Item.id[1:None:1][:1]"
    assert "_version" not in row

    row_sub = cast(RowSubscription, broker._subs[sub_id])
    assert row_sub.row_id == row["id"]
    assert len(broker._mq_client.subscribed_channels) == 1


async def test_subscribe_range(broker: SubscriptionBroker, filled_item_ref, admin_ctx):
    """测试range订阅的返回值，和订阅管理器的私有值是否正常"""
    sub_id, rows = await broker.subscribe_range(
        filled_item_ref, admin_ctx, "owner", 10, limit=33
    )
    assert len(rows) == 25
    assert "_version" not in rows[0]
    assert sub_id == "Item.owner[10:None:1][:33]"
    assert len(broker._subs[sub_id].channels) == 25 + 1  # 加1 个索引值频道（点查询）

    idx_sub = cast(IndexSubscription, broker._subs[sub_id])
    assert type(idx_sub) is IndexSubscription

    assert len(idx_sub.row_subs) == 25
    assert idx_sub.last_range_result == {row["id"] for row in rows}
    first_row_channel = min(idx_sub.row_subs)
    assert idx_sub.row_subs[first_row_channel].row_id == rows[0]["id"]
    assert len(broker._mq_client.subscribed_channels) == 26

    # 换个范围测试, owner只有10, 应该能查询到
    sub_id, rows = await broker.subscribe_range(
        filled_item_ref, admin_ctx, "owner", 10, right=11, limit=44
    )
    assert len(rows) == 25
    assert sub_id == "Item.owner[10:11:1][:44]"

    # 查询超出范围的订阅，因为默认force开，所以依然有sub_id
    sub_id, rows = await broker.subscribe_range(
        filled_item_ref, admin_ctx, "owner", 11, right=12, limit=55
    )
    assert len(rows) == 0
    assert sub_id
    idx_sub = cast(IndexSubscription, broker._subs[sub_id])
    assert type(idx_sub) is IndexSubscription

    assert len(idx_sub.row_subs) == 0
    assert sub_id == "Item.owner[11:12:1][:55]"
    # 25行 + 点查询的索引值频道 + 两个区间查询共用的整索引频道
    assert len(broker._mq_client.subscribed_channels) == 27


async def test_subscribe_mq_merge_message(
    broker: SubscriptionBroker, filled_item_ref, admin_ctx
):
    """测试订阅时，mq消息的合批功能"""
    backend = broker._backend
    mq = broker._mq_client

    sub_row, _ = await broker.subscribe_get(filled_item_ref, admin_ctx, "name", "Itm10")

    # 测试mq，2次消息应该只能获得1次合并的
    async with backend.session("pytest", 1) as session:
        repo = session.using(filled_item_ref.comp_cls)
        row = await repo.get(time=110)
        assert row
        row.qty = 998
        await repo.update(row)

    async with backend.session("pytest", 1) as session:
        repo = session.using(filled_item_ref.comp_cls)
        row = await repo.get(time=110)
        assert row
        row.qty = 997
        await repo.update(row)
    await backend.wait_for_synced()

    # 通知由后端 hub 在后台直接塞进 mq 的本地队列
    notified_channels = await mq.get_message()
    assert len(notified_channels) == 1

    # 测试更新消息能否获得，因为我get_message取掉了，应该没有了
    updates = await broker.get_updates(timeout=0.1)
    assert len(updates) == 0


async def test_subscribe_updates(
    broker: SubscriptionBroker, filled_item_ref, admin_ctx
):
    backend = broker._backend

    # 测试4种范围的订阅是否正常工作
    sub_row, _ = await broker.subscribe_get(filled_item_ref, admin_ctx, "name", "Itm10")
    sub_10, _ = await broker.subscribe_range(
        filled_item_ref, admin_ctx, "owner", 10, limit=33
    )
    sub_10_11, _ = await broker.subscribe_range(
        filled_item_ref, admin_ctx, "owner", 10, right=11, limit=44
    )
    sub_11_12, _ = await broker.subscribe_range(
        filled_item_ref, admin_ctx, "owner", 11, right=12, limit=55
    )
    assert sub_row
    assert sub_10
    assert sub_10_11
    assert sub_11_12

    # 初始数据是25行owner = 10
    assert len(broker._subs[sub_10].row_subs) == 25  # type: ignore
    assert len(broker._subs[sub_11_12].row_subs) == 0  # type: ignore

    # 更改行1的owner从10到11
    async with backend.session("pytest", 1) as session:
        repo = session.using(filled_item_ref.comp_cls)
        row = await repo.get(time=110)
        assert row
        row.owner = 11
        row1_id = row.id
        await repo.update(row)

    # 测试更新
    updates = await broker.get_updates()
    assert len(updates) == 4
    assert updates[sub_row][row1_id]["owner"] == 11  # row订阅数据更新
    assert updates[sub_10][row1_id] is None  # query 10删除了1
    assert updates[sub_10_11][row1_id]["owner"] == 11  # query 10-11更新row数据
    assert updates[sub_11_12][row1_id]["owner"] == 11  # query 11-12更新row数据

    # 测试删掉的项目是否成功取消订阅，和增加的成功注册订阅
    assert len(broker._subs[sub_10].row_subs) == 24  # type: ignore
    assert len(broker._subs[sub_11_12].row_subs) == 1  # type: ignore


async def test_row_subscribe_cache(
    broker: SubscriptionBroker, filled_item_ref, admin_ctx
):
    backend = broker._backend

    # row订阅会用全局cache加速相同数据的更新，当一个row更新时，应该cache中有该值
    sub_row, _ = await broker.subscribe_get(filled_item_ref, admin_ctx, "name", "Itm10")
    sub_10, _ = await broker.subscribe_range(
        filled_item_ref, admin_ctx, "owner", 10, limit=33
    )
    sub_10_11, _ = await broker.subscribe_range(
        filled_item_ref, admin_ctx, "owner", 10, right=11, limit=44
    )
    sub_11_12, _ = await broker.subscribe_range(
        filled_item_ref, admin_ctx, "owner", 11, right=12, limit=55
    )

    assert sub_row
    assert sub_10_11
    assert sub_11_12

    # 更改行1的owner从10到11
    async with backend.session("pytest", 1) as session:
        repo = session.using(filled_item_ref.comp_cls)
        row = await repo.get(time=110)
        assert row
        row.owner = 11
        row1_id = row.id
        await repo.update(row)

    # 检测行缓存：订阅中的行在进程行缓存里（原始 record，含 _version）。
    # 本进程自己的 commit 写穿，get_updates 直接从缓存推，不用再读库
    await broker.get_updates()
    channel = backend.master.row_channel(filled_item_ref, row1_id)
    cache = backend.row_cache
    if cache is not None:
        cached = cache.get(channel)
        assert cached is not None
        assert cached.id == row1_id and cached.owner == 11
        assert "_version" in cached.dtype.names  # type: ignore[operator]

    # 测试第二次更新缓存是否跟着更新
    async with backend.session("pytest", 1) as session:
        repo = session.using(filled_item_ref.comp_cls)
        row = await repo.get(time=110)
        assert row
        row.owner = 12
        await repo.update(row)

    updates = await broker.get_updates()
    if cache is not None:
        cached = cache.get(channel)
        assert cached is not None and cached.owner == 12
    # 其他顺带检测
    assert len(updates) == 3
    assert updates[sub_row][row1_id]["owner"] == 12  # row订阅数据更新
    assert sub_10 not in updates
    assert updates[sub_10_11][row1_id] is None  # query 10-11删除了1
    assert updates[sub_11_12][row1_id]["owner"] == 12  # query 11-12更新row数据


async def test_cancel_subscribe(broker: SubscriptionBroker, filled_item_ref, admin_ctx):
    sub_row, _ = await broker.subscribe_get(filled_item_ref, admin_ctx, "name", "Itm10")
    sub_10, _ = await broker.subscribe_range(
        filled_item_ref, admin_ctx, "owner", 10, limit=33
    )
    sub_10_11, _ = await broker.subscribe_range(
        filled_item_ref, admin_ctx, "owner", 10, right=11, limit=44
    )
    sub_11_12, _ = await broker.subscribe_range(
        filled_item_ref, admin_ctx, "owner", 11, right=12, limit=55
    )

    # 测试取消订阅
    assert len(broker._subs) == 4
    # 25行 + sub_10 的索引值频道 + sub_10_11/sub_11_12 共用的整索引频道
    assert len(broker._mq_client.subscribed_channels) == 27

    await broker.unsubscribe(sub_10)
    assert len(broker._subs) == 3
    # sub_10 的索引值频道释放了，其他sub依旧订阅所有行
    assert len(broker._mq_client.subscribed_channels) == 26

    await broker.unsubscribe(sub_row)
    assert len(broker._subs) == 2
    assert len(broker._channel_subs) == 26  # 10 row还是被sub_10_11订阅着
    assert len(broker._mq_client.subscribed_channels) == 26
    # 测试重复取消订阅没变化
    await broker.unsubscribe(sub_row)
    assert len(broker._subs) == 2
    assert len(broker._channel_subs) == 26
    assert len(broker._mq_client.subscribed_channels) == 26

    await broker.unsubscribe(sub_10_11)
    assert len(broker._subs) == 1
    assert len(broker._channel_subs) == 1
    assert len(broker._mq_client.subscribed_channels) == 1

    await broker.unsubscribe(sub_11_12)
    assert len(broker._subs) == 0
    assert len(broker._channel_subs) == 0
    assert len(broker._mq_client.subscribed_channels) == 0


async def test_subscribe_get_rls(
    broker: SubscriptionBroker, filled_item_ref, user_id10_ctx, user_id11_ctx
):
    # 测试owner不符不给订阅
    sub_id, row = await broker.subscribe_get(
        filled_item_ref, user_id10_ctx, "time", 110
    )
    assert sub_id is not None

    sub_id, row = await broker.subscribe_get(
        filled_item_ref, user_id11_ctx, "time", 110
    )
    assert sub_id is None


async def test_subscribe_range_rls(
    broker: SubscriptionBroker, filled_item_ref, user_id10_ctx
):
    # 先改掉一个人的owner值
    backend = broker._backend
    async with backend.session("pytest", 1) as session:
        repo = session.using(filled_item_ref.comp_cls)
        row = await repo.get(time=113)
        assert row
        row.owner = 11
        await repo.update(row)

    # 测试owner query只传输owner相等的数据
    sub_id, rows = await broker.subscribe_range(
        filled_item_ref, user_id10_ctx, "owner", 1, right=20, limit=55
    )
    assert [row["owner"] for row in rows] == [10] * 24
    assert len(broker._subs[sub_id].row_subs) == 24  # type: ignore


async def test_subscribe_get_rls_update(
    broker: SubscriptionBroker,
    filled_item_ref,
    user_id10_ctx,
):
    backend = broker._backend

    # 测试订阅单行，owner改变后要删除
    sub_id, row = await broker.subscribe_get(
        filled_item_ref, user_id10_ctx, "time", 113
    )
    assert row and sub_id
    assert row["owner"] == 10
    async with backend.session("pytest", 1) as session:
        repo = session.using(filled_item_ref.comp_cls)
        row = await repo.get(time=113)
        assert row
        row.owner = 11
        row3_id = row.id
        await repo.update(row)
    updates = await broker.get_updates()
    assert updates[sub_id][row3_id] is None


async def test_query_subscribe_rls_lost(
    broker: SubscriptionBroker,
    filled_item_ref,
    mod_item_model,
    user_id10_ctx,
):
    backend = broker._backend

    sub_id, rows = await broker.subscribe_range(
        filled_item_ref, user_id10_ctx, "owner", 1, right=20, limit=55
    )
    assert sub_id
    assert len(broker._subs[sub_id].row_subs) == 25  # type: ignore

    # 测试更新数值，看query的update是否会删除/添加owner相符的
    async with backend.session("pytest", 1) as session:
        repo = session.using(filled_item_ref.comp_cls)
        row = await repo.get(time=114)
        assert row
        row.owner = 11
        row4_id = row.id
        await repo.update(row)
    updates = await broker.get_updates()
    assert len(updates[sub_id]) == 1
    assert updates[sub_id][row4_id] is None

    # query订阅的原理是只订阅符合rls的行，但如果数值变了导致失去了某行rls并不会管，由行订阅执行处理
    # 所以注册数量25不变。（但是如果获得了新的rls会管）
    assert len(broker._subs[sub_id].row_subs) == 25  # type: ignore


async def test_query_subscribe_rls_gain(
    broker: SubscriptionBroker,
    filled_item_ref,
    mod_item_model,
    user_id10_ctx,
):
    # query订阅的rls gain处理的原理是，每次index变化都检查所有未注册行的rls
    # 如果中途获得rls，就加入订阅

    # 先预先取掉一行rls
    backend = broker._backend
    async with backend.session("pytest", 1) as session:
        repo = session.using(filled_item_ref.comp_cls)
        row = await repo.get(time=114)
        assert row
        row.owner = 11
        await repo.update(row)

    sub_id, rows = await broker.subscribe_range(
        filled_item_ref, user_id10_ctx, "owner", 1, right=20, limit=55
    )
    assert sub_id
    assert len(broker._subs[sub_id].row_subs) == 24  # type: ignore

    # 测试改回来是否重新出现
    async with backend.session("pytest", 1) as session:
        repo = session.using(filled_item_ref.comp_cls)
        row = await repo.get(time=114)
        assert row
        row.owner = 10
        row4_id = row.id
        await repo.update(row)
    updates = await broker.get_updates()
    assert len(updates[sub_id]) == 1
    assert updates[sub_id][row4_id]["owner"] == 10

    assert len(broker._subs[sub_id].row_subs) == 25  # type: ignore

    # 测试insert新数据能否得到通知
    async with backend.session("pytest", 1) as session:
        repo = session.using(filled_item_ref.comp_cls)
        new = mod_item_model.new_row()
        new.owner = 10
        new_row_id = new.id
        await repo.insert(new)
    updates = await broker.get_updates()
    assert len(updates[sub_id]) == 1
    assert updates[sub_id][new_row_id]["owner"] == 10


async def test_query_subscribe_rls_lost_without_index(
    broker: SubscriptionBroker,
    filled_rls_ref,
    mod_rls_test_model,
    user_id11_ctx,
):
    # filled_rls_ref的权限是要求ctx.caller == row.friend
    # 默认数据是owner=10, friend=11
    backend = broker._backend

    sub_id, rows = await broker.subscribe_range(
        filled_rls_ref, user_id11_ctx, "owner", 1, right=20, limit=55
    )
    assert sub_id
    assert len(broker._subs[sub_id].row_subs) == 25  # type: ignore

    # 去掉一个
    async with backend.session("pytest", 1) as session:
        repo = session.using(filled_rls_ref.comp_cls)
        rows = await repo.range(id=(0, float("inf")), limit=4)
        row = rows[-1]
        assert row
        row.friend = 12
        row4_id = row.id
        await repo.update(row)
    updates = await broker.get_updates(timeout=5)
    assert len(updates) == 1
    assert len(updates[sub_id]) == 1
    assert updates[sub_id][row4_id] is None

    assert len(broker._subs[sub_id].row_subs) == 25  # type: ignore


@pytest.mark.xfail(reason="已知缺陷，未来也许修也许不修", strict=True)
async def test_query_subscribe_rls_gain_without_index(
    broker: SubscriptionBroker,
    filled_rls_ref,
    mod_rls_test_model,
    user_id11_ctx,
):
    # filled_rls_ref的权限是要求ctx.caller == row.friend
    # 默认数据是owner=10, friend=11
    # todo 目前的设计是，rls获得并不能正确得到insert通知，除非订阅的正是rls属性自身（这里是friend）
    #      未来如果有需要，可以专门做个rls属性watch，变化了则通知所有IndexSubscription检查rls

    # 先预先取掉一行rls
    backend = broker._backend
    async with backend.session("pytest", 1) as session:
        repo = session.using(filled_rls_ref.comp_cls)
        rows = await repo.range(id=(0, float("inf")), limit=4)
        row4 = rows[-1]
        assert row4
        row4.friend = 12
        row4_id = row4.id
        await repo.update(row4)
        # 修改owner不应该影响rls
        row1 = rows[0]
        assert row1
        row1.owner = 12
        await repo.update(row1)

    sub_id, rows = await broker.subscribe_range(
        filled_rls_ref, user_id11_ctx, "owner", 1, right=20, limit=55
    )
    assert sub_id
    assert len(broker._subs[sub_id].row_subs) == 24  # type: ignore

    # 测试改回来是否重新出现
    async with backend.session("pytest", 1) as session:
        repo = session.using(filled_rls_ref.comp_cls)
        rows = await repo.range(id=(0, float("inf")), limit=4)
        row4 = rows[-1]
        assert row4
        row4.friend = 11
        await repo.update(row4)
    updates = await broker.get_updates(timeout=5)
    assert len(updates) == 1
    assert len(updates[sub_id]) == 1
    assert updates[sub_id][row4_id]["friend"] == 11

    assert len(broker._subs[sub_id].row_subs) == 25  # type: ignore


@pytest.mark.timeout(30)
async def test_mq_backlog(
    broker: SubscriptionBroker, filled_item_ref, mod_item_model, admin_ctx
):
    # 测试mq消息堆积的情况
    backend = broker._backend
    mq = broker._mq_client

    await broker.subscribe_get(filled_item_ref, admin_ctx, "name", "Itm10")
    await broker.subscribe_get(filled_item_ref, admin_ctx, "name", "Itm11")

    # 修改row1，并pull消息
    async with backend.session("pytest", 1) as session:
        repo = session.using(filled_item_ref.comp_cls)
        row = await repo.get(time=110)
        assert row
        row.qty = 998
        await repo.update(row)
    await backend.wait_for_synced()
    await wait_until(lambda: len(mq.pulled_deque) == 1)

    # 把这条消息的收到时刻拨回200秒前，模拟超过DROP_AFTER没人取的堆积；
    # 再次修改row1、row2，此时pull应该会丢掉前一个row1消息，放入后一个row1消息
    stale_at, stale_channel = mq.pulled_deque[0]
    mq.pulled_deque[0] = (stale_at - 200, stale_channel)
    async with backend.session("pytest", 1) as session:
        repo = session.using(filled_item_ref.comp_cls)
        row = await repo.get(time=110)
        assert row
        row.qty = 997
        await repo.update(row)
    await backend.wait_for_synced()
    # 旧的被丢弃，新的row1消息重新入队
    await wait_until(lambda: mq.pulled_deque and mq.pulled_deque[0][0] > stale_at)
    assert len(mq.pulled_deque) == 1

    async with backend.session("pytest", 1) as session:
        repo = session.using(filled_item_ref.comp_cls)
        rows = await repo.range(id=(0, float("inf")), limit=2)
        row = rows[-1]
        row.qty = 996
        await repo.update(row)
    await backend.wait_for_synced()
    await wait_until(lambda: len(mq.pulled_deque) == 2)

    # get_message只取收到超过1/UPDATE_FREQUENCY的消息：把两条都拨回1秒前
    for i, (received_at, channel) in enumerate(mq.pulled_deque):
        mq.pulled_deque[i] = (received_at - 1, channel)
    notified_channels = await mq.get_message()
    assert len(notified_channels) == 2
    assert not mq.pulled_deque and not mq.pulled_set


# ============================ 点查询按值分频道 ============================


def test_point_query_value():
    """点查询判定：right 省略或与 left 相等；str 的 ( 前缀是开区间；转换失败/NaN 回退"""
    import numpy as np

    from hetu.data.backend import BackendClient

    point = BackendClient.point_query_value_
    i64 = np.dtype(np.int64)
    assert point(i64, 10, None) == 10
    assert point(i64, 10, 10) == 10
    assert point(i64, "10", 10) == 10  # 按 dtype 规范化后比较
    assert point(i64, 10, 11) is None
    assert point(i64, float("inf"), None) is None  # int 索引传 inf 转换失败 → 回退
    f32 = np.dtype(np.float32)
    assert point(f32, float("nan"), None) is None
    assert point(f32, 0.1, 0.1) == np.float32(0.1)
    u8 = np.dtype("U8")
    assert point(u8, "abc", None) == "abc"
    assert point(u8, "[abc", "abc") == "abc"  # [ 前缀剥掉
    assert point(u8, "(abc", "abc") is None  # ( 是开区间
    assert point(u8, "abc", "(abc") is None
    assert point(np.dtype(np.int8), True, None) == 1  # bool 字段定义时已转 int8


async def test_subscribe_point_query_channel(
    broker: SubscriptionBroker, filled_item_ref, admin_ctx
):
    """点查询订"索引=该值"的频道，区间查询订整个索引的频道；同值不同 sub_id 共用一个频道"""
    servant = broker._backend.servant
    value_chan = servant.index_value_channel(filled_item_ref, "owner", 10)
    index_chan = servant.index_channel(filled_item_ref, "owner")
    assert value_chan != index_chan

    sub_a, rows = await broker.subscribe_range(
        filled_item_ref, admin_ctx, "owner", 10, limit=33
    )
    assert sub_a and len(rows) == 25
    idx_a = cast(IndexSubscription, broker._subs[sub_a])
    assert idx_a.index_channel == value_chan
    assert index_chan not in broker._mq_client.subscribed_channels

    # right == left 也是点查询：sub_id 不同，频道相同
    sub_b, _ = await broker.subscribe_range(
        filled_item_ref, admin_ctx, "owner", 10, right=10, limit=33
    )
    assert sub_b and sub_b != sub_a
    assert cast(IndexSubscription, broker._subs[sub_b]).index_channel == value_chan
    assert broker._channel_subs[value_chan] == {sub_a, sub_b}

    # 区间查询订整个索引的频道
    sub_c, _ = await broker.subscribe_range(
        filled_item_ref, admin_ctx, "owner", 10, right=11, limit=33
    )
    assert sub_c
    assert cast(IndexSubscription, broker._subs[sub_c]).index_channel == index_chan
    # 25 行频道 + 1 值频道 + 1 整索引频道
    assert len(broker._mq_client.subscribed_channels) == 27

    # bool 索引字段（定义时转 int8）：True 和 1 是同一个频道
    sub_d, _ = await broker.subscribe_range(
        filled_item_ref, admin_ctx, "used", True, limit=100
    )
    assert sub_d
    assert cast(
        IndexSubscription, broker._subs[sub_d]
    ).index_channel == servant.index_value_channel(filled_item_ref, "used", 1)

    # 值频道按引用计数释放
    await broker.unsubscribe(sub_a)
    assert value_chan in broker._mq_client.subscribed_channels
    await broker.unsubscribe(sub_b)
    assert value_chan not in broker._mq_client.subscribed_channels
    assert index_chan in broker._mq_client.subscribed_channels


async def test_subscribe_point_query_on_id_uses_index_channel(
    broker: SubscriptionBroker, filled_item_ref, admin_ctx
):
    """id 没有值频道：点查 id 订整个 id 索引的频道，该 id 的行被插入/删除时照样能收到"""
    servant = broker._backend.servant
    backend = broker._backend
    comp = filled_item_ref.comp_cls
    rows = await servant.range(filled_item_ref, "time", 123, 123, limit=1)
    assert len(rows) == 1
    row_id = int(rows[0].id)

    sub_id, got = await broker.subscribe_range(filled_item_ref, admin_ctx, "id", row_id)
    assert sub_id and len(got) == 1
    idx_sub = cast(IndexSubscription, broker._subs[sub_id])
    assert idx_sub.index_channel == servant.index_channel(filled_item_ref, "id")

    # 删掉这行：通知从整个 id 索引的频道来，订阅报告该行没了
    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        assert await repo.get(id=row_id) is not None  # delete 要先读进缓存
        repo.delete(row_id)
    async with asyncio.timeout(3):
        updates = await broker.get_updates()
    assert updates == {sub_id: {row_id: None}}

    # 用同一个 id 插回来：又能收到
    async with backend.session("pytest", 1) as session:
        new_row = comp.new_row()
        new_row.id = row_id
        new_row.name = "IdBack"
        new_row.time = 123
        await session.using(comp).insert(new_row)
    async with asyncio.timeout(3):
        updates = await broker.get_updates()
    assert set(updates[sub_id]) == {row_id}
    assert updates[sub_id][row_id]["name"] == "IdBack"


async def test_subscribe_point_query_not_woken_by_other_values(
    broker: SubscriptionBroker, filled_item_ref, admin_ctx
):
    """别的 owner 的行增删不会叫醒点查询订阅；自己的值上有行进出才会"""
    backend = broker._backend
    sub_10, _ = await broker.subscribe_range(
        filled_item_ref, admin_ctx, "owner", 10, limit=33
    )
    sub_10_11, _ = await broker.subscribe_range(
        filled_item_ref, admin_ctx, "owner", 10, right=11, limit=44
    )
    assert sub_10 and sub_10_11

    # 插入一行 owner=11：整索引频道叫醒区间订阅，点查询订阅 owner=10 不动
    async with backend.session("pytest", 1) as session:
        repo = session.using(filled_item_ref.comp_cls)
        row = filled_item_ref.comp_cls.new_row()
        row.name = "Other"
        row.owner = 11
        row.time = 999
        await repo.insert(row)
        new_id = row.id
    updates = await broker.get_updates(timeout=2)
    assert sub_10 not in updates
    assert updates[sub_10_11][new_id]["owner"] == 11
    assert await broker.get_updates(timeout=0.3) == {}

    # 这行 owner 改成 10：进入了点查询的值，两个订阅都收到
    async with backend.session("pytest", 1) as session:
        repo = session.using(filled_item_ref.comp_cls)
        row = await repo.get(id=new_id)
        assert row
        row.owner = 10
        await repo.update(row)
    updates = await broker.get_updates(timeout=2)
    assert updates[sub_10][new_id]["owner"] == 10
    assert updates[sub_10_11][new_id]["owner"] == 10

    # 删掉它：点查询订阅收到删除
    async with backend.session("pytest", 1) as session:
        repo = session.using(filled_item_ref.comp_cls)
        assert await repo.get(id=new_id) is not None  # delete 要求行已在事务缓存里
        repo.delete(new_id)
    updates = await broker.get_updates(timeout=2)
    assert updates[sub_10][new_id] is None
    assert updates[sub_10_11][new_id] is None
    assert len(broker._subs[sub_10].row_subs) == 25  # type: ignore


async def test_subscribe_point_query_string(
    broker: SubscriptionBroker, filled_item_ref, admin_ctx
):
    """字符串索引的点查询：改名只叫醒旧值/新值的订阅者，( 前缀的开区间走整索引频道"""
    backend = broker._backend
    servant = backend.servant
    sub_a, rows = await broker.subscribe_range(
        filled_item_ref, admin_ctx, "name", "Itm10", limit=10
    )
    assert sub_a and len(rows) == 1
    row_id = rows[0]["id"]
    sub_b, rows = await broker.subscribe_range(
        filled_item_ref, admin_ctx, "name", "Itm11", limit=10
    )
    assert sub_b and len(rows) == 1
    # [ 前缀剥掉后与 "Itm10" 同频道；( 前缀是开区间，订整个索引的频道
    sub_a2, _ = await broker.subscribe_range(
        filled_item_ref, admin_ctx, "name", "[Itm10", limit=10
    )
    sub_c, rows = await broker.subscribe_range(
        filled_item_ref, admin_ctx, "name", "(Itm10", right="Itm12", limit=10
    )
    assert sub_a2 and sub_c
    assert {r["name"] for r in rows} == {"Itm11", "Itm12"}
    assert (
        cast(IndexSubscription, broker._subs[sub_a2]).index_channel
        == cast(IndexSubscription, broker._subs[sub_a]).index_channel
    )
    assert cast(
        IndexSubscription, broker._subs[sub_c]
    ).index_channel == servant.index_channel(filled_item_ref, "name")

    # Itm10 改名 Itm99：sub_a/sub_a2 收到删除，sub_b 不醒，区间 sub_c 被整索引频道叫醒但无变化
    async with backend.session("pytest", 1) as session:
        repo = session.using(filled_item_ref.comp_cls)
        row = await repo.get(name="Itm10")
        assert row
        row.name = "Itm99"
        await repo.update(row)
    updates = await broker.get_updates(timeout=2)
    assert updates[sub_a][row_id] is None
    assert updates[sub_a2][row_id] is None
    assert sub_b not in updates
    assert sub_c not in updates

    # 订阅新名字能拿到这行；改回去后新名字的订阅收到删除、旧名字的订阅收到该行
    sub_99, rows = await broker.subscribe_range(
        filled_item_ref, admin_ctx, "name", "Itm99", limit=10
    )
    assert sub_99 and [r["id"] for r in rows] == [row_id]
    async with backend.session("pytest", 1) as session:
        repo = session.using(filled_item_ref.comp_cls)
        row = await repo.get(name="Itm99")
        assert row
        row.name = "Itm10"
        await repo.update(row)
    updates = await broker.get_updates(timeout=2)
    assert updates[sub_99][row_id] is None
    assert updates[sub_a][row_id]["name"] == "Itm10"
    assert sub_b not in updates


# ============================ 整表订阅 ============================


async def test_subscribe_table(broker: SubscriptionBroker, filled_item_ref, admin_ctx):
    """整表订阅：只订一个频道；insert/update/delete 分别推送 行/行/None"""
    backend = broker._backend
    sub_id, rows = await broker.subscribe_table(filled_item_ref, admin_ctx)
    assert sub_id == "Item.table"
    assert len(rows) == 25
    assert "_version" not in rows[0]
    assert broker.count() == (0, 0, 1)
    tbl_sub = cast(TableSubscription, broker._subs[sub_id])
    assert type(tbl_sub) is TableSubscription
    assert tbl_sub.known_ids == {row["id"] for row in rows}
    # 不管多少行，都只有1个频道
    assert len(broker._mq_client.subscribed_channels) == 1
    assert tbl_sub.channels == {backend.servant.table_channel(filled_item_ref)}

    # insert
    async with backend.session("pytest", 1) as session:
        repo = session.using(filled_item_ref.comp_cls)
        row = filled_item_ref.comp_cls.new_row()
        row.name = "TblIns"
        row.owner = 10
        row.time = 500
        await repo.insert(row)
        new_id = row.id
    updates = await broker.get_updates()
    assert updates == {sub_id: {new_id: updates[sub_id][new_id]}}
    assert updates[sub_id][new_id]["name"] == "TblIns"
    assert "_version" not in updates[sub_id][new_id]
    assert new_id in tbl_sub.known_ids

    # update
    async with backend.session("pytest", 1) as session:
        repo = session.using(filled_item_ref.comp_cls)
        row = await repo.get(time=110)
        assert row
        row.qty = 42
        row1_id = row.id
        await repo.update(row)
    updates = await broker.get_updates()
    assert list(updates[sub_id].keys()) == [row1_id]
    assert updates[sub_id][row1_id]["qty"] == 42

    # delete
    async with backend.session("pytest", 1) as session:
        repo = session.using(filled_item_ref.comp_cls)
        row = await repo.get(time=110)
        assert row
        repo.delete(row.id)
    updates = await broker.get_updates()
    assert updates == {sub_id: {row1_id: None}}
    assert row1_id not in tbl_sub.known_ids


async def test_subscribe_table_merge(
    broker: SubscriptionBroker, filled_item_ref, admin_ctx
):
    """同一tick内多个事务的变动合并到一次get_updates；同一行改两次只推最终值"""
    backend = broker._backend
    sub_id, _ = await broker.subscribe_table(filled_item_ref, admin_ctx)
    assert sub_id

    async with backend.session("pytest", 1) as session:
        repo = session.using(filled_item_ref.comp_cls)
        row = await repo.get(time=111)
        assert row
        row.qty = 1
        id_a = row.id
        await repo.update(row)
    async with backend.session("pytest", 1) as session:
        repo = session.using(filled_item_ref.comp_cls)
        row = await repo.get(time=112)
        assert row
        row.qty = 2
        id_b = row.id
        await repo.update(row)
    async with backend.session("pytest", 1) as session:
        repo = session.using(filled_item_ref.comp_cls)
        row = await repo.get(time=111)
        assert row
        row.qty = 3
        await repo.update(row)

    updates = await broker.get_updates()
    assert set(updates[sub_id].keys()) == {id_a, id_b}
    assert updates[sub_id][id_a]["qty"] == 3
    assert updates[sub_id][id_b]["qty"] == 2
    # 合并后没有残留
    updates = await broker.get_updates(timeout=0.3)
    assert updates == {}


async def test_subscribe_table_coexist_range(
    broker: SubscriptionBroker, filled_item_ref, admin_ctx
):
    """同一张表同时有range订阅和整表订阅，两者互不影响，都能收到更新"""
    backend = broker._backend
    sub_range, _ = await broker.subscribe_range(
        filled_item_ref, admin_ctx, "owner", 10, limit=33
    )
    sub_table, _ = await broker.subscribe_table(filled_item_ref, admin_ctx)
    assert sub_range and sub_table
    assert broker.count() == (0, 1, 1)
    # 25行频道 + 1索引值频道（点查询） + 1表级频道
    assert len(broker._mq_client.subscribed_channels) == 27

    async with backend.session("pytest", 1) as session:
        repo = session.using(filled_item_ref.comp_cls)
        row = await repo.get(time=113)
        assert row
        row.qty = 7
        row_id = row.id
        await repo.update(row)
    updates = await broker.get_updates()
    assert updates[sub_range][row_id]["qty"] == 7
    assert updates[sub_table][row_id]["qty"] == 7

    # 取消range订阅，整表订阅不受影响
    await broker.unsubscribe(sub_range)
    assert len(broker._mq_client.subscribed_channels) == 1
    async with backend.session("pytest", 1) as session:
        repo = session.using(filled_item_ref.comp_cls)
        row = await repo.get(time=113)
        assert row
        row.qty = 8
        await repo.update(row)
    updates = await broker.get_updates()
    assert updates == {sub_table: {row_id: updates[sub_table][row_id]}}
    assert updates[sub_table][row_id]["qty"] == 8


async def test_subscribe_table_rls(
    broker: SubscriptionBroker,
    filled_item_ref,
    user_id10_ctx,
):
    """整表订阅对RLS得失都做出反应；从未可见的行的变动不推送"""
    backend = broker._backend
    # Item是OWNER权限，用户10只看得到owner==10的行（初始全部25行）
    sub_id, rows = await broker.subscribe_table(filled_item_ref, user_id10_ctx)
    assert sub_id
    assert len(rows) == 25
    tbl_sub = cast(TableSubscription, broker._subs[sub_id])

    # 失去RLS：owner 10 -> 11，应收到None
    async with backend.session("pytest", 1) as session:
        repo = session.using(filled_item_ref.comp_cls)
        row = await repo.get(time=114)
        assert row
        row.owner = 11
        row4_id = row.id
        await repo.update(row)
    updates = await broker.get_updates()
    assert updates == {sub_id: {row4_id: None}}
    assert row4_id not in tbl_sub.known_ids

    # 从未可见的行(owner=11)被修改：不推送
    async with backend.session("pytest", 1) as session:
        repo = session.using(filled_item_ref.comp_cls)
        row = await repo.get(time=114)
        assert row
        row.qty = 5
        await repo.update(row)
    updates = await broker.get_updates(timeout=0.5)
    assert updates == {}

    # 重新获得RLS：owner 11 -> 10，应收到行数据（range订阅做不到这点）
    async with backend.session("pytest", 1) as session:
        repo = session.using(filled_item_ref.comp_cls)
        row = await repo.get(time=114)
        assert row
        row.owner = 10
        await repo.update(row)
    updates = await broker.get_updates()
    assert updates[sub_id][row4_id]["owner"] == 10
    assert row4_id in tbl_sub.known_ids

    # 新插入一行别人的（owner=11）：不推送；插入自己的：推送
    async with backend.session("pytest", 1) as session:
        repo = session.using(filled_item_ref.comp_cls)
        other = filled_item_ref.comp_cls.new_row()
        other.name, other.owner, other.time = "Other", 11, 601
        await repo.insert(other)
        mine = filled_item_ref.comp_cls.new_row()
        mine.name, mine.owner, mine.time = "Mine", 10, 602
        await repo.insert(mine)
        mine_id = mine.id
    updates = await broker.get_updates()
    assert updates == {sub_id: {mine_id: updates[sub_id][mine_id]}}
    assert updates[sub_id][mine_id]["name"] == "Mine"


async def test_subscribe_table_permission_denied(
    broker: SubscriptionBroker, filled_item_ref
):
    """未登录用户对非EVERYBODY表整表订阅：拒绝，且不占用任何频道/计数"""
    from hetu.system import SystemContext

    anon_ctx = SystemContext(
        caller=0,
        connection_id=0,
        address="NotSet",
        group="",
        user_data={},
        timestamp=0,
        request=None,  # type: ignore
        systems=None,  # type: ignore
    )
    sub_id, rows = await broker.subscribe_table(filled_item_ref, anon_ctx)
    assert sub_id is None
    assert rows == []
    assert broker.count() == (0, 0, 0)
    assert len(broker._mq_client.subscribed_channels) == 0


async def test_subscribe_table_row_cap(mod_auto_backend, filled_item_ref, admin_ctx):
    """表行数超过max_table_rows时拒绝订阅，不占用频道"""
    broker = SubscriptionBroker(mod_auto_backend("main"), max_table_rows=10)
    try:
        sub_id, rows = await broker.subscribe_table(filled_item_ref, admin_ctx)
        assert sub_id is None
        assert rows == []
        assert broker.count() == (0, 0, 0)
        assert len(broker._mq_client.subscribed_channels) == 0

        # 刚好等于上限则允许
        broker._max_table_rows = 25
        sub_id, rows = await broker.subscribe_table(filled_item_ref, admin_ctx)
        assert sub_id
        assert len(rows) == 25
    finally:
        await broker.close()


async def test_subscribe_table_duplicate_and_unsubscribe(
    broker: SubscriptionBroker, filled_item_ref, admin_ctx
):
    """重复订阅返回同一sub_id且不重复计数；取消后频道释放、不再收到更新"""
    backend = broker._backend
    sub_id, _ = await broker.subscribe_table(filled_item_ref, admin_ctx)
    sub_id2, rows2 = await broker.subscribe_table(filled_item_ref, admin_ctx)
    assert sub_id == sub_id2
    assert len(rows2) == 25
    assert broker.count() == (0, 0, 1)
    assert len(broker._mq_client.subscribed_channels) == 1

    await broker.unsubscribe(sub_id)
    assert broker.count() == (0, 0, 0)
    assert len(broker._mq_client.subscribed_channels) == 0
    assert sub_id not in broker._subs

    async with backend.session("pytest", 1) as session:
        repo = session.using(filled_item_ref.comp_cls)
        row = await repo.get(time=115)
        assert row
        row.qty = 9
        await repo.update(row)
    updates = await broker.get_updates(timeout=0.5)
    assert updates == {}

    # 取消后可以重新订阅
    sub_id3, _ = await broker.subscribe_table(filled_item_ref, admin_ctx)
    assert sub_id3 == sub_id
    assert broker.count() == (0, 0, 1)


async def test_subscribe_table_large(broker: SubscriptionBroker, item_ref, admin_ctx):
    """大表整表订阅：几千行也只有1个频道；批量变动一次推送"""
    from hetu.data.backend import Table

    backend = broker._backend
    n = 3000
    async with backend.session("pytest", 1) as session:
        repo = session.using(item_ref.comp_cls)
        for i in range(n):
            row = item_ref.comp_cls.new_row()
            row.name = f"P{i}"
            row.owner = 10
            row.time = 200000 + i
            await repo.insert(row)
    await backend.wait_for_synced()

    table = Table(
        item_ref.comp_cls, item_ref.instance_name, item_ref.cluster_id, backend
    )
    sub_id, rows = await broker.subscribe_table(table, admin_ctx)
    assert sub_id
    assert len(rows) == n
    assert len(broker._mq_client.subscribed_channels) == 1

    # 一个事务改50行
    async with backend.session("pytest", 1) as session:
        repo = session.using(item_ref.comp_cls)
        changed = await repo.range("time", 200000, 200049, limit=50)
        assert len(changed) == 50
        for row in changed:
            row.qty = 77
            await repo.update(row)
    updates = await broker.get_updates()
    assert len(updates[sub_id]) == 50
    assert all(r["qty"] == 77 for r in updates[sub_id].values())


def _count_reads(stack, backend: Backend):
    """把 master 与各 servant 的读方法都包上计数：plain = get / get_many，
    authoritative = get_many_authoritative（Redis 的 get_authoritative 内部就是它）"""
    from unittest.mock import patch

    mocks = {}
    for i, client in enumerate([backend.master, *backend._servants]):
        for meth in ("get", "get_many", "get_many_authoritative"):
            mocks[(i, meth)] = stack.enter_context(
                patch.object(client, meth, wraps=getattr(client, meth))
            )

    def counts():
        c = {"plain": 0, "authoritative": 0}
        for (_, meth), m in mocks.items():
            key = "authoritative" if meth == "get_many_authoritative" else "plain"
            c[key] += m.call_count  # type: ignore[attr-defined]
        return c

    return counts


async def _other_process_update(mod_backend_config, ref, qty: int):
    """用另一个 Backend（模拟别的 worker 进程）改 time=110 那行的 qty"""
    import copy

    other = Backend(copy.deepcopy(mod_backend_config))
    other.post_configure(components=[ref.comp_cls])
    try:
        async with other.session("pytest", 1) as session:
            repo = session.using(ref.comp_cls)
            row = await repo.get(time=110)
            assert row
            row.qty = qty
            await repo.update(row)
        await other.wait_for_synced()
    finally:
        await other.close()


async def test_sub_push_uses_row_cache(
    filled_item_ref, mod_auto_backend, mod_backend_config, admin_ctx
):
    """订阅推送走进程行缓存：别的进程改行，本进程两个连接订着同一行，只有第一个刷新的读库，
    第二个命中；本进程自己改行（commit 写穿）推送 0 次读；推送内容不含 _version"""
    from contextlib import ExitStack

    backend: Backend = mod_auto_backend()
    if backend.row_cache is None:
        return
    broker_a = SubscriptionBroker(backend)
    broker_b = SubscriptionBroker(backend)
    try:
        sub_a, row = await broker_a.subscribe_get(
            filled_item_ref, admin_ctx, "name", "Itm10"
        )
        sub_b, _ = await broker_b.subscribe_get(
            filled_item_ref, admin_ctx, "name", "Itm10"
        )
        assert sub_a and sub_b and row
        row_id = row["id"]

        await _other_process_update(mod_backend_config, filled_item_ref, 501)
        with ExitStack() as stack:
            counts = _count_reads(stack, backend)
            updates_a = await broker_a.get_updates()
            after_a = counts()
            # 副本追上了是 1 次副本读；副本滞后则再补 1 次权威读
            assert 1 <= after_a["plain"] + after_a["authoritative"] <= 2
            updates_b = await broker_b.get_updates()
            assert counts() == after_a  # 第二个连接命中缓存
        assert updates_a[sub_a][row_id]["qty"] == 501
        assert updates_b[sub_b][row_id] == updates_a[sub_a][row_id]
        assert "_version" not in updates_a[sub_a][row_id]

        # 本进程自己改：写穿，两个连接的推送都不读库
        async with backend.session("pytest", 1) as session:
            repo = session.using(filled_item_ref.comp_cls)
            r = await repo.get(time=110)
            assert r
            r.qty = 502
            await repo.update(r)
        with ExitStack() as stack:
            counts = _count_reads(stack, backend)
            updates_a = await broker_a.get_updates()
            updates_b = await broker_b.get_updates()
            assert counts() == {"plain": 0, "authoritative": 0}
        assert updates_a[sub_a][row_id]["qty"] == 502
        assert updates_b[sub_b][row_id]["qty"] == 502
    finally:
        await broker_a.close()
        await broker_b.close()


async def test_sub_push_stale_replica(
    filled_item_ref, mod_auto_backend, mod_backend_config, admin_ctx
):
    """别的进程改行后副本还是旧行：推送识破滞后（版本低于通知），改权威读推新值"""
    from contextlib import ExitStack
    from unittest.mock import patch

    backend: Backend = mod_auto_backend()
    cache = backend.row_cache
    if cache is None:
        return
    broker = SubscriptionBroker(backend)
    try:
        sub, row = await broker.subscribe_get(
            filled_item_ref, admin_ctx, "name", "Itm10"
        )
        assert sub and row
        row_id = row["id"]
        channel = backend.master.row_channel(filled_item_ref, row_id)
        stale = cache.get(channel)  # subscribe_get 的首次权威读已填充
        assert stale is not None

        await _other_process_update(mod_backend_config, filled_item_ref, 601)
        await wait_until(lambda: cache.get(channel) is None)

        async def stale_get_many(ref, ids, row_format=RowFormat.STRUCT):
            return [stale.copy() for _ in ids]

        with ExitStack() as stack:
            for client in [backend.master, *backend._servants]:
                stack.enter_context(
                    patch.object(client, "get_many", side_effect=stale_get_many)
                )
            auth = stack.enter_context(
                patch.object(
                    backend.master,
                    "get_many_authoritative",
                    wraps=backend.master.get_many_authoritative,
                )
            )
            updates = await broker.get_updates()
            assert auth.call_count == 1
        assert updates[sub][row_id]["qty"] == 601
        cached = cache.get(channel)
        assert cached is not None and cached.qty == 601
    finally:
        await broker.close()


async def test_subscribe_get_subscribes_before_read(
    filled_item_ref, filled_rls_ref, mod_auto_backend, admin_ctx, user_id10_ctx
):
    """subscribe_get 先订后读：返回时行已在缓存（首次权威读）；行不存在 / 行级权限不过时
    退订干净，hub 里没有残留频道"""
    backend: Backend = mod_auto_backend()
    cache = backend.row_cache
    broker = SubscriptionBroker(backend)
    try:
        sub_by_id_row = await broker.subscribe_get(
            filled_item_ref, admin_ctx, "name", "Itm11"
        )
        sub1, row = sub_by_id_row
        assert sub1 and row and "_version" not in row
        sub2, row2 = await broker.subscribe_get(
            filled_item_ref, admin_ctx, "id", row["id"]
        )
        assert sub2 and row2 == row
        if cache is not None:
            channel = backend.master.row_channel(filled_item_ref, row["id"])
            cached = cache.get(channel)
            assert cached is not None and cached.name == "Itm11"

        hub = backend.servant._hub  # type: ignore[attr-defined]
        # 行不存在：不留订阅
        assert await broker.subscribe_get(
            filled_item_ref, admin_ctx, "id", 987654321
        ) == (None, None)
        missing = backend.master.row_channel(filled_item_ref, 987654321)
        assert hub is None or missing not in hub.channels
        if cache is not None:
            assert not cache.is_active(missing)
        # 行级权限不过（RLS：friend == caller，行的 friend 是 11，caller 是 10）：不留订阅
        assert await broker.subscribe_get(
            filled_rls_ref, user_id10_ctx, "owner", 10
        ) == (None, None)
        if hub is not None:
            assert not any(
                ch.startswith(
                    f"{filled_rls_ref.instance_name}:{filled_rls_ref.comp_name}:"
                )
                for ch in hub.channels
            )
    finally:
        await broker.close()
