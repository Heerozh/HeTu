import time
from contextvars import ContextVar
from typing import AsyncGenerator, cast

import pytest
from fixtures.backends import use_redis_family_backend_only

from hetu.common.snowflake_id import SnowflakeID
from hetu.data.backend import Backend
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
    # 清空row订阅缓存
    RowSubscription._RowSubscription__cache = ContextVar("user_row_cache")  # type: ignore

    yield broker

    await broker.close()


@pytest.fixture
async def background_mq_puller_task(broker):
    """启动一个后台任务不断pull mq消息的fixture"""

    async def puller():
        while True:
            await broker.mq_pull()

    import asyncio

    task = asyncio.create_task(puller())
    yield task

    task.cancel()


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
    assert all(flag in replica_flags for flag in list("Kghz"))


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
    assert len(broker._subs[sub_id].channels) == 25 + 1  # 加1 index channel

    idx_sub = cast(IndexSubscription, broker._subs[sub_id])
    assert type(idx_sub) is IndexSubscription

    assert len(idx_sub.row_subs) == 25
    assert idx_sub.last_range_result == {row["id"] for row in rows}
    first_row_channel = next(iter(sorted(idx_sub.channels)))
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
    assert len(broker._mq_client.subscribed_channels) == 26


async def test_subscribe_mq_merge_message(
    broker: SubscriptionBroker, filled_item_ref, admin_ctx, background_mq_puller_task
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

    # mq.get_message必须要后台puller任务在跑，否则消息无法获取
    notified_channels = await mq.get_message()
    assert len(notified_channels) == 1

    # 测试更新消息能否获得，因为我get_message取掉了，应该没有了
    updates = await broker.get_updates(timeout=0.1)
    assert len(updates) == 0


async def test_subscribe_updates(
    broker: SubscriptionBroker, filled_item_ref, admin_ctx, background_mq_puller_task
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
    broker: SubscriptionBroker, filled_item_ref, admin_ctx, background_mq_puller_task
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

    # 检测Row cache
    await broker.get_updates()
    # 由于不同backend的channel名不一样，使用dict的第一个channel
    cache = RowSubscription._RowSubscription__cache.get()  # type: ignore
    first_channel = next(iter(cache.keys()))
    assert cache[first_channel][row1_id]["owner"] == 11

    # 测试第二次更新cache是否清空了
    async with backend.session("pytest", 1) as session:
        repo = session.using(filled_item_ref.comp_cls)
        row = await repo.get(time=110)
        assert row
        row.owner = 12
        await repo.update(row)

    updates = await broker.get_updates()
    # 如果数据正确说明更新了
    assert cache[first_channel][row1_id]["owner"] == 12
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
    assert len(broker._mq_client.subscribed_channels) == 26  # 25行+1个index

    await broker.unsubscribe(sub_10)
    assert len(broker._subs) == 3
    assert len(broker._mq_client.subscribed_channels) == 26  # 其他sub依旧订阅所有行

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
    background_mq_puller_task,
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
    background_mq_puller_task,
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
    background_mq_puller_task,
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
    background_mq_puller_task,
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
    background_mq_puller_task,
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
    monkeypatch, broker: SubscriptionBroker, filled_item_ref, mod_item_model, admin_ctx
):
    time_time = time.time
    # 测试mq消息堆积的情况
    backend = broker._backend

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
    await broker.mq_pull()

    # 2分钟后再次修改row1,row2，此时pull应该会删除前一个row1消息，放入后一个row1消息
    monkeypatch.setattr(time, "time", lambda: time_time() + 200)
    async with backend.session("pytest", 1) as session:
        repo = session.using(filled_item_ref.comp_cls)
        row = await repo.get(time=110)
        assert row
        row.qty = 997
        await repo.update(row)
    await backend.wait_for_synced()
    await broker.mq_pull()

    async with backend.session("pytest", 1) as session:
        repo = session.using(filled_item_ref.comp_cls)
        rows = await repo.range(id=(0, float("inf")), limit=2)
        row = rows[-1]
        row.qty = 996
        await repo.update(row)
    await backend.wait_for_synced()
    await broker.mq_pull()

    mq = broker._mq_client
    monkeypatch.setattr(time, "time", lambda: time_time() + 210)
    notified_channels = await mq.get_message()
    assert len(notified_channels) == 2


# ============================ 整表订阅 ============================


async def test_subscribe_table(
    broker: SubscriptionBroker, filled_item_ref, admin_ctx, background_mq_puller_task
):
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
    broker: SubscriptionBroker, filled_item_ref, admin_ctx, background_mq_puller_task
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
    broker: SubscriptionBroker, filled_item_ref, admin_ctx, background_mq_puller_task
):
    """同一张表同时有range订阅和整表订阅，两者互不影响，都能收到更新"""
    backend = broker._backend
    sub_range, _ = await broker.subscribe_range(
        filled_item_ref, admin_ctx, "owner", 10, limit=33
    )
    sub_table, _ = await broker.subscribe_table(filled_item_ref, admin_ctx)
    assert sub_range and sub_table
    assert broker.count() == (0, 1, 1)
    # 25行频道 + 1索引频道 + 1表级频道
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
    background_mq_puller_task,
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
    broker: SubscriptionBroker, filled_item_ref, admin_ctx, background_mq_puller_task
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


async def test_subscribe_table_large(
    broker: SubscriptionBroker, item_ref, admin_ctx, background_mq_puller_task
):
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
