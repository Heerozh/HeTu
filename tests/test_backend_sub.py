import asyncio
from collections.abc import Callable
from contextvars import ContextVar
from typing import AsyncGenerator, cast
from unittest.mock import patch

import pytest
from fixtures.backends import use_redis_family_backend_only
from fixtures.contexts import settled_updates, wait_until

from hetu.common.snowflake_id import SnowflakeID
from hetu.data.backend import Backend
from hetu.data.backend.base import MQClient
from hetu.data.sub import (
    IndexSubscription,
    RowSubscription,
    SubscriptionBroker,
    TableSubscription,
)

SnowflakeID().init(1, 0)
INTERVAL = 1 / MQClient.UPDATE_FREQUENCY


@pytest.fixture
async def broker(mod_auto_backend) -> AsyncGenerator[SubscriptionBroker]:
    """初始化订阅管理器的fixture"""

    # 初始化订阅器
    broker = SubscriptionBroker(mod_auto_backend("main"))
    # 清空row订阅缓存
    RowSubscription._RowSubscription__cache = ContextVar("user_row_cache")  # type: ignore

    yield broker

    await broker.close()


async def updates_until(
    broker: SubscriptionBroker,
    check: Callable[[dict[str, dict]], object],
    merged: dict[str, dict] | None = None,
    timeout: float = 10.0,
) -> dict[str, dict]:
    """
    反复 get_updates，把各 tick 的结果按 sub_id / row_id 合并（后到的覆盖先到的），直到
    check(merged) 里的断言全部通过；超时则再跑一次 check，抛出真正的断言错误。

    一次写入会在行、索引、值等频道上各发一条通知，它们各自入队、各自等合批窗口：负载高时
    （xdist、CI）会被拆到前后几个 tick，SQL 后端 0.1 秒一次的轮询也会把它们拆开，甚至晚到
    下一步。而且不同频道推的东西不一样：行离开范围时，行频道先到只会推新的行数据，要等
    索引/值频道到了才推 None、才撤掉 row_subs。所以只看单次 get_updates、或者只看某订阅
    "出现了"都会偶发失败，要等到断言描述的最终状态。
    """
    merged = {} if merged is None else merged
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        try:
            check(merged)
            return merged
        except AssertionError, KeyError, TypeError:
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise
        for sub_id, rows in (await broker.get_updates(timeout=remaining)).items():
            merged.setdefault(sub_id, {}).update(rows)


async def tick_with(broker: SubscriptionBroker, *channels: str) -> dict[str, dict]:
    """
    等 channels 的通知都进了本连接的本地队列，再把队列里的通知都当作已过合批窗口（同
    test_mq_backlog，拨回收到时间）跑一个 tick：这些频道保证在同一个 tick 里处理。
    """
    mq = broker._mq_client
    await wait_until(lambda: all(ch in mq.pulled_set for ch in channels), timeout=10)
    back = 1 / mq.UPDATE_FREQUENCY
    for i, (received_at, channel) in enumerate(mq.pulled_deque):
        mq.pulled_deque[i] = (received_at - back, channel)
    return await broker.get_updates()


async def count_notifications(broker: SubscriptionBroker, channel: str) -> list[None]:
    """
    关注频道：之后本连接每收到一条该频道的通知，返回的列表就多一个元素。同一频道在本地
    队列里只占一项，要确定几次写入的通知都已到齐（从而落在同一个 tick）只能这样数。
    """
    seen: list[None] = []
    await broker._mq_client.watch(channel, lambda: seen.append(None))
    return seen


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
    # 测试replica应该有通知。keyspace 通知只在 key 所在节点本地产生，集群模式下每个节点
    # （主从都算，订阅可能连在从节点上）都得开，只开默认节点的话别的分片上的行收不到通知
    from redis.cluster import RedisCluster

    io = servant.io
    if isinstance(io, RedisCluster):
        nodes = [(node.name, io.get_redis_connection(node)) for node in io.get_nodes()]
    else:
        nodes = [("standalone", io)]
    for name, node_io in nodes:
        config = cast(dict, node_io.config_get("notify-keyspace-events"))
        flags = config["notify-keyspace-events"]
        assert all(flag in flags for flag in "Kghz"), f"{name}: {flags!r}"


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


async def test_subscribe_get_by_id(
    broker: SubscriptionBroker, filled_item_ref, admin_ctx
):
    """按 id 订阅：返回的行与按索引订阅一样，不含内部的 _version"""
    servant = broker._backend.servant
    row_id = int((await servant.range(filled_item_ref, "time", 110, limit=1))[0].id)
    sub_id, row = await broker.subscribe_get(filled_item_ref, admin_ctx, "id", row_id)
    assert sub_id and row
    assert row["id"] == row_id and row["time"] == 110
    assert "_version" not in row


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

    sub_row, sub_data = await broker.subscribe_get(
        filled_item_ref, admin_ctx, "name", "Itm10"
    )
    assert sub_row and sub_data
    seen = await count_notifications(
        broker, backend.servant.row_channel(filled_item_ref, sub_data["id"])
    )

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
    # 通知由后端 hub 在后台直接塞进 mq 的本地队列：等两条都到了再取，负载高时第二条
    # 可能晚于合批窗口才到，那就不是合批该管的了
    await wait_until(lambda: len(seen) >= 2, timeout=10)

    notified_channels = await mq.get_message()
    assert len(notified_channels) == 1

    # 通知已被上面取掉。第二条是合并进来的：它离弹出不足一个 interval 时（取决于两次提交
    # 花了多久）会补排一次尾随重读，读到的只能是最终值；之后不会再有别的
    updates = await broker.get_updates(timeout=0.3)
    assert all(r["qty"] == 997 for rows in updates.values() for r in rows.values())
    assert await broker.get_updates(timeout=0.3) == {}


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
    def check(updates):
        assert len(updates) == 4
        assert updates[sub_row][row1_id]["owner"] == 11  # row订阅数据更新
        assert updates[sub_10][row1_id] is None  # query 10删除了1
        assert updates[sub_10_11][row1_id]["owner"] == 11  # query 10-11更新row数据
        assert updates[sub_11_12][row1_id]["owner"] == 11  # query 11-12更新row数据
        # 测试删掉的项目是否成功取消订阅，和增加的成功注册订阅
        assert len(broker._subs[sub_10].row_subs) == 24  # type: ignore
        assert len(broker._subs[sub_11_12].row_subs) == 1  # type: ignore

    await updates_until(broker, check)


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

    # 检测Row cache：缓存的是本tick批量预读的原始行（含_version），按行频道存。
    # 缓存每个tick重置，所以要确定行频道的通知落在检查的这个tick里
    row1_channel = backend.servant.row_channel(filled_item_ref, row1_id)
    updates = await tick_with(broker, row1_channel)
    cache = RowSubscription._RowSubscription__cache.get()  # type: ignore
    assert cache[row1_channel]["id"] == row1_id
    assert cache[row1_channel]["owner"] == 11
    assert "_version" in cache[row1_channel]

    # 这次写入在索引、值频道上的通知可能还没处理：等 sub_10 放掉该行、sub_11_12 收进该行
    # 再进下一步，否则它们留到下一步才推
    def settled(updates):
        assert updates[sub_10][row1_id] is None
        assert updates[sub_11_12][row1_id]["owner"] == 11

    await updates_until(broker, settled, merged=updates)

    # 测试第二次更新cache是否清空了
    async with backend.session("pytest", 1) as session:
        repo = session.using(filled_item_ref.comp_cls)
        row = await repo.get(time=110)
        assert row
        row.owner = 12
        await repo.update(row)

    updates = await tick_with(broker, row1_channel)
    # 每个tick重置缓存并重新预读，如果数据正确说明更新了
    cache = RowSubscription._RowSubscription__cache.get()  # type: ignore
    assert cache[row1_channel]["owner"] == 12

    # 其他顺带检测：索引、值频道的通知可能晚一个tick才到，收齐再看
    def check(updates):
        assert len(updates) == 3
        assert updates[sub_row][row1_id]["owner"] == 12  # row订阅数据更新
        assert sub_10 not in updates
        assert updates[sub_10_11][row1_id] is None  # query 10-11删除了1
        assert updates[sub_11_12][row1_id]["owner"] == 12  # query 11-12更新row数据

    await updates_until(broker, check, merged=updates)


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

    def check(updates):
        assert len(updates[sub_id]) == 1
        assert updates[sub_id][row4_id] is None

    await updates_until(broker, check)

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

    def check_gain(updates):
        assert len(updates[sub_id]) == 1
        assert updates[sub_id][row4_id]["owner"] == 10
        assert len(broker._subs[sub_id].row_subs) == 25  # type: ignore

    await updates_until(broker, check_gain)

    # 测试insert新数据能否得到通知
    async with backend.session("pytest", 1) as session:
        repo = session.using(filled_item_ref.comp_cls)
        new = mod_item_model.new_row()
        new.owner = 10
        new_row_id = new.id
        await repo.insert(new)

    def check_insert(updates):
        assert len(updates[sub_id]) == 1
        assert updates[sub_id][new_row_id]["owner"] == 10

    await updates_until(broker, check_insert)


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

    def deleted(updates):
        assert updates == {sub_id: {row_id: None}}

    await updates_until(broker, deleted)

    # 用同一个 id 插回来：又能收到
    async with backend.session("pytest", 1) as session:
        new_row = comp.new_row()
        new_row.id = row_id
        new_row.name = "IdBack"
        new_row.time = 123
        await session.using(comp).insert(new_row)

    def inserted(updates):
        assert set(updates[sub_id]) == {row_id}
        assert updates[sub_id][row_id]["name"] == "IdBack"

    await updates_until(broker, inserted)


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

    def inserted(updates):
        assert sub_10 not in updates
        assert updates[sub_10_11][new_id]["owner"] == 11

    await updates_until(broker, inserted)
    assert await broker.get_updates(timeout=0.3) == {}

    # 这行 owner 改成 10：进入了点查询的值，两个订阅都收到
    async with backend.session("pytest", 1) as session:
        repo = session.using(filled_item_ref.comp_cls)
        row = await repo.get(id=new_id)
        assert row
        row.owner = 10
        await repo.update(row)

    def moved_in(updates):
        assert updates[sub_10][new_id]["owner"] == 10
        assert updates[sub_10_11][new_id]["owner"] == 10

    await updates_until(broker, moved_in)

    # 删掉它：点查询订阅收到删除
    async with backend.session("pytest", 1) as session:
        repo = session.using(filled_item_ref.comp_cls)
        assert await repo.get(id=new_id) is not None  # delete 要求行已在事务缓存里
        repo.delete(new_id)

    def deleted(updates):
        assert updates[sub_10][new_id] is None
        assert updates[sub_10_11][new_id] is None
        assert len(broker._subs[sub_10].row_subs) == 25  # type: ignore

    await updates_until(broker, deleted)


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

    def renamed(updates):
        assert updates[sub_a][row_id] is None
        assert updates[sub_a2][row_id] is None
        assert sub_b not in updates
        assert sub_c not in updates

    await updates_until(broker, renamed)

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

    def renamed_back(updates):
        assert updates[sub_99][row_id] is None
        assert updates[sub_a][row_id]["name"] == "Itm10"
        assert sub_b not in updates

    await updates_until(broker, renamed_back)


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
    seen = await count_notifications(
        broker, backend.servant.table_channel(filled_item_ref)
    )

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
    # 三个事务的通知都到了再取，才能确定它们合进了同一个tick：负载高时（SQL 后端的轮询
    # 恰好切在事务之间时尤其如此）最后一条可能晚于合批窗口才到，那就不是合批该管的了
    await wait_until(lambda: len(seen) >= 3, timeout=10)

    updates = await broker.get_updates()
    assert set(updates[sub_id].keys()) == {id_a, id_b}
    assert updates[sub_id][id_a]["qty"] == 3
    assert updates[sub_id][id_b]["qty"] == 2
    # 合并进来的消息离弹出不足一个 interval 时，它们的行会尾随重读一次（可能重复推送，
    # 内容只能是最终值）；之后没有残留
    updates = await broker.get_updates(timeout=0.3)
    assert all(
        row["qty"] == {id_a: 3, id_b: 2}[row_id]
        for row_id, row in updates.get(sub_id, {}).items()
    )
    assert await broker.get_updates(timeout=0.3) == {}


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

    def both(updates):
        assert updates[sub_range][row_id]["qty"] == 7
        assert updates[sub_table][row_id]["qty"] == 7

    await updates_until(broker, both)

    # 取消range订阅，整表订阅不受影响
    await broker.unsubscribe(sub_range)
    assert len(broker._mq_client.subscribed_channels) == 1
    async with backend.session("pytest", 1) as session:
        repo = session.using(filled_item_ref.comp_cls)
        row = await repo.get(time=113)
        assert row
        row.qty = 8
        await repo.update(row)

    # 上一步行频道的通知可能才到，单独成一个没有订阅者的空 tick
    def table_only(updates):
        assert updates == {sub_table: {row_id: updates[sub_table][row_id]}}
        assert updates[sub_table][row_id]["qty"] == 8

    await updates_until(broker, table_only)


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


async def test_subscribe_get_sees_write_before_subscription_active(
    broker: SubscriptionBroker, filled_item_ref, admin_ctx
):
    """SUBSCRIBE 生效之前落下的写入不能丢：先订后读，返回的行一定已经包含它。
    先读后订的话，这次写入既不在读到的行里、也不会有通知，客户端一直拿着旧行"""
    backend = broker._backend
    comp = filled_item_ref.comp_cls
    servant = backend.servant
    row_id = int((await servant.range(filled_item_ref, "time", 111, limit=1))[0].id)

    mq = broker._mq_client
    real_subscribe = mq.subscribe

    async def write_then_subscribe(*channels: str):
        # 订阅生效之前，别的连接改了这行
        async with backend.session("pytest", 1) as session:
            repo = session.using(comp)
            row = await repo.get(id=row_id)
            assert row
            row.qty = 4321
            await repo.update(row)
        await backend.wait_for_synced()
        await real_subscribe(*channels)

    with patch.object(mq, "subscribe", write_then_subscribe):
        sub_id, row = await broker.subscribe_get(
            filled_item_ref, admin_ctx, "id", row_id
        )
    assert sub_id and row
    assert row["qty"] == 4321


async def test_subscribe_get_registers_before_read(
    broker: SubscriptionBroker, filled_item_ref, admin_ctx
):
    """subscribe_get 订阅生效后必须先登记再读：读期间 get_updates 的 tick 把这行从某个索引
    订阅的范围里放出去时，不能把这个刚订上的频道当没人要退掉——否则新订阅登记在一个连接
    已不再订阅的频道上，之后永远收不到通知"""
    backend = broker._backend
    servant = backend.servant
    sub_10, rows = await broker.subscribe_range(
        filled_item_ref, admin_ctx, "owner", 10, limit=33
    )
    assert sub_10
    row_id = next(r["id"] for r in rows if r["time"] == 110)
    channel = servant.row_channel(filled_item_ref, row_id)

    # 行离开 owner=10 的范围：通知进本连接队列，先不 tick
    async with backend.session("pytest", 1) as session:
        repo = session.using(filled_item_ref.comp_cls)
        row = await repo.get(id=row_id)
        assert row
        row.owner = 11
        await repo.update(row)

    # 让 subscribe_get 的那次读卡住，在它卡住期间跑 tick
    real_get = servant.get
    entered = asyncio.Event()
    release = asyncio.Event()

    async def slow_get(*args, **kwargs):
        if not entered.is_set():
            entered.set()
            await release.wait()
        return await real_get(*args, **kwargs)

    with patch.object(servant, "get", slow_get):
        task = asyncio.create_task(
            broker.subscribe_get(filled_item_ref, admin_ctx, "id", row_id)
        )
        async with asyncio.timeout(15):
            await entered.wait()

            def released(updates):
                assert updates[sub_10][row_id] is None  # sub_10 放掉了这行

            # 行频道先到只会推新的行数据，值频道到了 sub_10 才放掉这行，可能要跑几个 tick
            await updates_until(broker, released)
            release.set()
            sub_row, row = await task
    assert sub_row and row and row["owner"] == 11
    assert channel in broker._mq_client.subscribed_channels
    assert broker._channel_subs[channel] == {sub_row}

    # 新订阅真的收得到后续变更
    async with backend.session("pytest", 1) as session:
        repo = session.using(filled_item_ref.comp_cls)
        row = await repo.get(id=row_id)
        assert row
        row.qty = 123
        await repo.update(row)

    def changed(updates):
        assert updates[sub_row][row_id]["qty"] == 123

    await updates_until(broker, changed)


async def test_subscribe_get_invisible_row_leaves_no_subscription(
    broker: SubscriptionBroker, filled_item_ref, admin_ctx, user_id11_ctx
):
    """行不存在 / 行级权限不过（Item 的 RLS 是 owner == caller，行的 owner 都是 10）：
    先订后读时刚订上的频道要退干净，不留订阅、不占计数"""
    backend = broker._backend
    servant = backend.servant
    row_id = int((await servant.range(filled_item_ref, "time", 110, limit=1))[0].id)

    invisible = [
        (admin_ctx, "id", 987654321),  # 行不存在
        (user_id11_ctx, "time", 110),  # 行级权限不过（按索引定位）
        (user_id11_ctx, "id", row_id),  # 行级权限不过（按 id）
    ]
    for ctx, index_name, value in invisible:
        sub_id, row = await broker.subscribe_get(
            filled_item_ref, ctx, index_name, value
        )
        assert sub_id is None and row is None

    assert not broker._mq_client.subscribed_channels
    assert not broker._subs and not broker._channel_subs
    assert broker.count() == (0, 0, 0)
    hub = servant._hub  # type: ignore[attr-defined]
    for rid in (987654321, row_id):
        assert servant.row_channel(filled_item_ref, rid) not in hub.channels


async def test_subscribe_get_duplicate_of_deleted_row_unsubscribes(
    broker: SubscriptionBroker, filled_item_ref, admin_ctx
):
    """重复订阅一个已经删掉的行：回 (None, None) 的同时得把旧订阅撤掉——客户端拿到 None 就
    认为没有订阅、不会再 unsub，留着的话订阅和频道会挂到连接结束、计数也漂"""
    backend = broker._backend
    sub_id, row = await broker.subscribe_get(
        filled_item_ref, admin_ctx, "name", "Itm10"
    )
    assert sub_id and row
    row_id = row["id"]
    channel = backend.servant.row_channel(filled_item_ref, row_id)

    async with backend.session("pytest", 1) as session:
        repo = session.using(filled_item_ref.comp_cls)
        assert await repo.get(id=row_id)
        repo.delete(row_id)
    updates = await broker.get_updates(timeout=3)
    assert updates[sub_id][row_id] is None

    sub_again, row_again = await broker.subscribe_get(
        filled_item_ref, admin_ctx, "id", row_id
    )
    assert sub_again is None and row_again is None
    assert sub_id not in broker._subs
    assert channel not in broker._channel_subs
    assert channel not in broker._mq_client.subscribed_channels
    assert broker.count() == (0, 0, 0)


# ================= 重读去重：读回的与客户端已有的一样就不推 =================


async def test_reread_of_unchanged_rows_pushes_nothing(
    broker: SubscriptionBroker, filled_item_ref, admin_ctx
):
    """补读、尾随重读大多读回客户端已有的数据：行订阅与范围订阅里的行都不该再推一遍"""
    sub_row, _ = await broker.subscribe_get(filled_item_ref, admin_ctx, "name", "Itm10")
    sub_10, rows = await broker.subscribe_range(
        filled_item_ref, admin_ctx, "owner", 10, limit=33
    )
    assert sub_row and sub_10 and len(rows) == 25
    mq = broker._mq_client
    row_channel = cast(RowSubscription, broker._subs[sub_row]).channel
    # 没有任何写入：各频道当作收到通知；过一会儿行频道再合并进一条，弹出时离它不足一个
    # interval（弹出的时刻会比预定晚一点，挨得太近就不算），还会尾随重读一次
    mq.request_reread(row_channel, *broker._subs[sub_10].channels)
    await asyncio.sleep(INTERVAL * 0.6)
    mq.request_reread(row_channel)
    assert await broker.get_updates(timeout=0.6) == {}


async def test_table_trailing_reread_pushes_no_duplicate(
    broker: SubscriptionBroker, filled_item_ref, admin_ctx
):
    """整表订阅：合并进队头的消息触发尾随重读，读回的行与刚推过的一样，不重复推"""
    backend = broker._backend
    mq = broker._mq_client
    sub_id, _ = await broker.subscribe_table(filled_item_ref, admin_ctx)
    assert sub_id
    table_channel = cast(TableSubscription, broker._subs[sub_id]).table_channel

    async with backend.session("pytest", 1) as session:
        repo = session.using(filled_item_ref.comp_cls)
        row = await repo.get(time=111)
        assert row
        row.qty = 5
        id_a = row.id
        await repo.update(row)
    await wait_until(lambda: table_channel in mq.pulled_set)
    await asyncio.sleep(INTERVAL * 0.6)
    mq.push_pulled_(table_channel, [id_a])  # 合并进队头 → 弹出后尾随重读 id_a

    updates = await broker.get_updates(timeout=2)
    assert updates[sub_id][id_a]["qty"] == 5
    assert await broker.get_updates(timeout=0.6) == {}


async def test_reinserted_row_with_restarted_version_is_pushed(
    broker: SubscriptionBroker, filled_item_ref, admin_ctx
):
    """同 id 删除后重插：_version 从 1 重来，与客户端手里的旧行版本号可能相同（刚插入、
    没改过的行都是 1），去重不能只看版本号，内容不同就得推"""
    backend = broker._backend
    comp = filled_item_ref.comp_cls
    row_id = int((await backend.servant.range(filled_item_ref, "time", 112))[0].id)
    sub_id, row = await broker.subscribe_get(filled_item_ref, admin_ctx, "id", row_id)
    assert sub_id and row and row["name"] == "Itm12"

    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        assert await repo.get(id=row_id)
        repo.delete(row_id)
    async with backend.session("pytest", 1) as session:
        reborn = comp.new_row()
        reborn.id = row_id
        reborn.name = "Reborn"
        reborn.time = 112
        await session.using(comp).insert(reborn)

    updates = await settled_updates(broker, timeout=3)
    assert updates[sub_id][row_id]["name"] == "Reborn"
