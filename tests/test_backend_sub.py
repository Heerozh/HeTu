import asyncio
import time
from collections.abc import Callable
from contextvars import ContextVar
from typing import AsyncGenerator, cast
from unittest.mock import patch

import pytest
from fixtures.backends import use_redis_family_backend_only
from fixtures.contexts import make_ctx, wait_until
from fixtures.testdata import create_ref

from hetu.common.snowflake_id import SnowflakeID
from hetu.data.backend import Backend, RowFormat
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


async def test_query_subscribe_rls_gain_without_index(
    broker: SubscriptionBroker,
    filled_rls_ref,
    mod_rls_test_model,
    user_id11_ctx,
):
    # filled_rls_ref的权限是要求ctx.caller == row.friend
    # 默认数据是owner=10, friend=11
    # 订阅时不可见的行不在初始行里；订阅生效后的补读重跑一次范围比对，把范围内不可见的行
    # 也订上行频道（不推），之后它不经索引变化重新获得 RLS（改的不是被订阅的索引字段），
    # 也能从行频道推出来。以前要等该索引下一次变动才会订上它们

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
    # 补读过后：不可见的 row4 也订上了，但不推给客户端
    assert await broker.get_updates(timeout=0.5) == {}
    assert len(broker._subs[sub_id].row_subs) == 25  # type: ignore

    # 测试改回来是否重新出现
    async with backend.session("pytest", 1) as session:
        repo = session.using(filled_rls_ref.comp_cls)
        rows = await repo.range(id=(0, float("inf")), limit=4)
        row4 = rows[-1]
        assert row4
        row4.friend = 11
        await repo.update(row4)

    def gained(updates):
        assert len(updates) == 1
        assert len(updates[sub_id]) == 1
        assert updates[sub_id][row4_id]["friend"] == 11

    await updates_until(broker, gained)
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
    # 订阅生效后的补读先消化掉，下面直接看本地队列
    assert await broker.get_updates(timeout=0.5) == {}

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
    assert point(i64, float("inf"), None) is None  # int 索引里没有 inf 这个值 → 回退
    assert point(i64, "abc", None) is None  # 解析不了的边界 → 回退，不报错
    f32 = np.dtype(np.float32)
    assert point(f32, float("nan"), None) is None
    assert point(f32, "abc", None) is None
    assert point(f32, 0.1, 0.1) == np.float32(0.1)
    u8 = np.dtype("U8")
    assert point(u8, "abc", None) == "abc"
    assert point(u8, "[abc", "abc") == "abc"  # [ 前缀剥掉
    assert point(u8, "(abc", "abc") is None  # ( 是开区间
    assert point(u8, "abc", "(abc") is None
    assert point(np.dtype(np.int8), True, None) == 1  # bool 字段定义时已转 int8
    # 整数索引：区间里没有这个值的不算点查询，不能截断成整数去订值频道、登记"读空"
    i8 = np.dtype(np.int8)
    assert point(i8, 1.0, None) == 1
    assert point(i8, 1.5, 1.5) is None
    assert point(i8, 200, None) is None  # 越界
    assert point(i8, "1.5", None) is None


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


async def test_point_query_leave_backfills_limit(
    broker: SubscriptionBroker, filled_item_ref, admin_ctx
):
    """
    值频道只发"进入"：点查询结果里的行离开（改走 / 删除）由它自己的行频道发现，重跑比对时
    推 None、退订行频道，并把被 limit 截在外面的行补进来
    """
    backend = broker._backend
    comp = filled_item_ref.comp_cls
    sub_id, rows = await broker.subscribe_range(
        filled_item_ref, admin_ctx, "owner", 10, limit=5
    )
    assert sub_id and len(rows) == 5
    first5 = [int(r["id"]) for r in rows]
    # 与订阅同样的排序，拿到被 limit 截在外面的第 6、7 行
    all_ids = [
        int(i)
        for i in await backend.servant.range(
            filled_item_ref, "owner", 10, limit=100, row_format=RowFormat.ID_LIST
        )
    ]
    assert all_ids[:5] == first5
    sixth, seventh = all_ids[5], all_ids[6]
    moved, deleted = first5[0], first5[1]

    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        row = await repo.get(id=moved)
        assert row
        row.owner = 11
        await repo.update(row)
        assert await repo.get(id=deleted) is not None
        repo.delete(deleted)

    def left_and_backfilled(updates):
        got = updates[sub_id]
        assert got[moved] is None and got[deleted] is None
        assert got[sixth]["owner"] == 10 and got[seventh]["owner"] == 10
        idx_sub = cast(IndexSubscription, broker._subs[sub_id])
        assert idx_sub.last_range_result == set(first5[2:]) | {sixth, seventh}
        assert len(idx_sub.row_subs) == 5

    await updates_until(broker, left_and_backfilled)


async def test_point_query_row_leaving_before_its_channel_is_active(
    broker: SubscriptionBroker, filled_item_ref, admin_ctx
):
    """
    行进入点查询的结果后，它的行频道要到 tick 末尾才订上：读到这行之后、行频道生效之前它又
    离开（这里是删除）的话，行频道的通知收不到，值频道又不发"离开"。行频道订上后得隔一个
    interval 补读一次（同订阅生效后的补读），不然客户端一直留着这行
    """
    backend = broker._backend
    comp = filled_item_ref.comp_cls
    mq = broker._mq_client
    sub_id, rows = await broker.subscribe_range(
        filled_item_ref, admin_ctx, "owner", 10, limit=33
    )
    assert sub_id and len(rows) == 25
    assert await broker.get_updates(timeout=0.5) == {}  # 订阅生效后的补读先消化掉

    async with backend.session("pytest", 1) as session:
        row = comp.new_row()
        row.name, row.owner, row.time = "Blink", 10, 600
        await session.using(comp).insert(row)
        new_id = int(row.id)
    new_chan = backend.servant.row_channel(filled_item_ref, new_id)
    real_subscribe = mq.subscribe

    async def delete_then_subscribe(*channels: str):
        if new_chan in channels:  # tick 末尾订这行的行频道之前，它被删了
            async with backend.session("pytest", 1) as session:
                repo = session.using(comp)
                assert await repo.get(id=new_id) is not None
                repo.delete(new_id)
            await backend.wait_for_synced()
        await real_subscribe(*channels)

    def entered(updates):
        assert updates[sub_id][new_id]["name"] == "Blink"

    with patch.object(mq, "subscribe", delete_then_subscribe):
        await updates_until(broker, entered)

    def left(updates):
        assert updates[sub_id][new_id] is None

    await updates_until(broker, left, timeout=3)
    idx_sub = cast(IndexSubscription, broker._subs[sub_id])
    assert new_id not in idx_sub.last_range_result
    assert new_chan not in mq.subscribed_channels


async def _owner10_with_two_hidden_rows(
    broker: SubscriptionBroker, rls_ref, ctx
) -> tuple[IndexSubscription, list[int]]:
    """
    RLSTest 的点查询 owner=10（user 11 只看得到 friend == 11 的行，默认 25 行都是）：先藏起
    两行再订阅。订阅生效后的补读会把这两行也订上行频道（等它们变得可见时要推），但不推
    """
    backend = broker._backend
    async with backend.session("pytest", 1) as session:
        repo = session.using(rls_ref.comp_cls)
        rows = await repo.range(id=(0, float("inf")), limit=2)
        hidden = [int(r.id) for r in rows]
        for row in rows:
            row.friend = 12
            await repo.update(row)
    sub_id, visible = await broker.subscribe_range(rls_ref, ctx, "owner", 10, limit=33)
    assert sub_id and len(visible) == 23
    assert await broker.get_updates(timeout=0.5) == {}
    idx_sub = cast(IndexSubscription, broker._subs[sub_id])
    assert len(idx_sub.row_subs) == 25
    return idx_sub, hidden


async def test_point_query_hidden_row_change_is_not_pushed(
    broker: SubscriptionBroker, filled_rls_ref, user_id11_ctx
):
    """不可见的行客户端从没拿到过：它变了但仍不可见，不能推 None（等于把它的 id 告诉客户端）"""
    idx_sub, (hidden, _) = await _owner10_with_two_hidden_rows(
        broker, filled_rls_ref, user_id11_ctx
    )
    async with broker._backend.session("pytest", 1) as session:
        repo = session.using(filled_rls_ref.comp_cls)
        row = await repo.get(id=hidden)
        assert row
        row.friend = 13  # 对 user 11 仍不可见
        await repo.update(row)
    assert await broker.get_updates(timeout=1) == {}
    assert len(idx_sub.row_subs) == 25


async def test_point_query_hidden_row_leaving_is_not_pushed(
    broker: SubscriptionBroker, filled_rls_ref, user_id11_ctx
):
    """不可见的行离开点查询的值（改走 / 删除）：退订它的行频道，但不推 None"""
    idx_sub, (moved, deleted) = await _owner10_with_two_hidden_rows(
        broker, filled_rls_ref, user_id11_ctx
    )
    async with broker._backend.session("pytest", 1) as session:
        repo = session.using(filled_rls_ref.comp_cls)
        row = await repo.get(id=moved)
        assert row
        row.owner = 12
        await repo.update(row)
        assert await repo.get(id=deleted) is not None
        repo.delete(deleted)
    assert await broker.get_updates(timeout=1) == {}
    assert len(idx_sub.row_subs) == 23
    assert idx_sub.last_range_result.isdisjoint({moved, deleted})


async def test_range_query_hidden_rows_are_not_pushed(
    broker: SubscriptionBroker, filled_rls_ref, user_id11_ctx
):
    """
    区间查询（订整个索引的频道，不走值频道）同理：订阅生效后的补读把范围内不可见的行也订上
    行频道，它们在不可见期间变化、离开范围（改走 / 删除）都不能推 None
    """
    backend = broker._backend
    comp = filled_rls_ref.comp_cls
    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        rows = await repo.range(id=(0, float("inf")), limit=3)
        changed, moved, deleted = (int(r.id) for r in rows)
        for row in rows:
            row.friend = 12
            await repo.update(row)
    sub_id, visible = await broker.subscribe_range(
        filled_rls_ref, user_id11_ctx, "owner", 9, 10, limit=33
    )
    assert sub_id and len(visible) == 22
    assert await broker.get_updates(timeout=0.5) == {}
    idx_sub = cast(IndexSubscription, broker._subs[sub_id])
    assert idx_sub.point_value is None and len(idx_sub.row_subs) == 25

    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        row = await repo.get(id=changed)
        assert row
        row.friend = 13  # 对 user 11 仍不可见
        await repo.update(row)
        row = await repo.get(id=moved)
        assert row
        row.owner = 12  # 离开 [9, 10]
        await repo.update(row)
        assert await repo.get(id=deleted) is not None
        repo.delete(deleted)
    assert await broker.get_updates(timeout=1) == {}
    assert len(idx_sub.row_subs) == 23
    assert idx_sub.last_range_result.isdisjoint({moved, deleted})


async def test_point_query_on_undeclared_index_falls_back(
    broker: SubscriptionBroker, filled_item_ref, admin_ctx, caplog
):
    """
    索引没声明 point_sub：commit 不发它的值频道，点查询退化为订整个索引的频道（区间订阅的
    行为，结果照样正确），同一组件的同一索引只警告一次
    """
    import logging

    servant = broker._backend.servant
    comp = filled_item_ref.comp_cls
    assert "model" not in comp.point_subs_
    with caplog.at_level(logging.WARNING, logger="HeTu.root"):
        sub_a, rows = await broker.subscribe_range(
            filled_item_ref, admin_ctx, "model", 0.5, limit=10
        )
        sub_b, _ = await broker.subscribe_range(
            filled_item_ref, admin_ctx, "model", 0.6, limit=10
        )
    assert sub_a and sub_b and len(rows) == 1
    index_chan = servant.index_channel(filled_item_ref, "model")
    assert cast(IndexSubscription, broker._subs[sub_a]).index_channel == index_chan
    assert cast(IndexSubscription, broker._subs[sub_b]).index_channel == index_chan
    warns = [r for r in caplog.records if "point_sub" in r.getMessage()]
    assert len(warns) == 1, [r.getMessage() for r in warns]
    assert "model" in warns[0].getMessage()

    # 功能不受影响：另一行的 model 改成 0.5 → 进入；再改走 → 离开
    backend = broker._backend
    other_id = None
    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        row = await repo.get(name="Itm30")
        assert row
        other_id = int(row.id)
        row.model = 0.5
        await repo.update(row)

    def moved_in(updates):
        assert updates[sub_a][other_id]["model"] == pytest.approx(0.5)

    await updates_until(broker, moved_in)

    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        row = await repo.get(id=other_id)
        assert row
        row.model = 7.0
        await repo.update(row)

    def moved_out(updates):
        assert updates[sub_a][other_id] is None

    await updates_until(broker, moved_out)


async def test_index_value_channel_requires_point_sub(mod_auto_backend, item_ref):
    """值频道只给声明了 point_sub 的索引发：订一个没人会发的频道是 bug，直接报错"""
    servant = mod_auto_backend().servant
    assert "owner" in item_ref.comp_cls.point_subs_
    servant.index_value_channel(item_ref, "owner", 10)
    with pytest.raises(ValueError, match="point_sub"):
        servant.index_value_channel(item_ref, "model", 0.5)


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
    # 合并后没有残留：合并进来的消息离弹出不足一个 interval 时它们的行会尾随重读一次，
    # 读回的与刚推过的一样，不重复推
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


async def test_subscribe_table_requires_table_sub(
    broker: SubscriptionBroker, filled_rls_ref, admin_ctx, caplog
):
    """组件没声明 table_sub：commit 不发它的表频道，整表订阅被拒绝，不占任何频道/计数"""
    import logging

    assert filled_rls_ref.comp_cls.table_sub_ is False
    with caplog.at_level(logging.WARNING, logger="HeTu.root"):
        sub_id, rows = await broker.subscribe_table(filled_rls_ref, admin_ctx)
    assert sub_id is None
    assert rows == []
    assert broker.count() == (0, 0, 0)
    assert len(broker._mq_client.subscribed_channels) == 0
    assert any("table_sub" in r.getMessage() for r in caplog.records)


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


async def test_subscribe_get_reply_not_overtaken_by_older_inflight_push(
    broker: SubscriptionBroker, filled_item_ref, admin_ctx
):
    """
    subscribe_get 登记后、读回之前，一个 tick 已经拿更早预读的旧行替这个新订阅算好了推送，
    却在 sub 回复发出之后才送达：客户端最终手里是旧行。之后的补读、通知读回的都和回复里的
    一样（pushed 记的是回复那份），去重把纠正挡掉，客户端一直停在旧行，直到这行再变
    """
    backend = broker._backend
    servant = backend.servant
    mq = broker._mq_client
    comp = filled_item_ref.comp_cls
    sub_10, rows = await broker.subscribe_range(
        filled_item_ref, admin_ctx, "owner", 10, limit=33
    )
    assert sub_10
    assert await broker.get_updates(timeout=0.5) == {}  # 订阅生效后的补读先消化掉
    row_id = next(r["id"] for r in rows if r["time"] == 111)

    async def set_qty(qty: int):
        async with backend.session("pytest", 1) as session:
            repo = session.using(comp)
            row = await repo.get(id=row_id)
            assert row
            row.qty = qty
            await repo.update(row)
        await backend.wait_for_synced()

    # sub_10 订着这行的行频道：这次写入的通知进本连接队列，先不 tick
    await set_qty(5)

    # subscribe_get 登记完，卡在读上
    real_get = servant.get
    reading = asyncio.Event()
    release_read = asyncio.Event()

    async def slow_get(*args, **kwargs):
        if not reading.is_set():
            reading.set()
            await release_read.wait()
        return await real_get(*args, **kwargs)

    # tick 算完各订阅的推送、到末尾批量订阅时卡住：推送还没交给客户端
    real_subscribe = mq.subscribe
    computed = asyncio.Event()
    release_tick = asyncio.Event()

    async def slow_subscribe(*channels: str):
        computed.set()
        await release_tick.wait()
        await real_subscribe(*channels)

    try:
        async with asyncio.timeout(15):
            with patch.object(servant, "get", slow_get):
                get_task = asyncio.create_task(
                    broker.subscribe_get(filled_item_ref, admin_ctx, "id", row_id)
                )
                await reading.wait()
                with patch.object(mq, "subscribe", slow_subscribe):
                    tick = asyncio.create_task(broker.get_updates(timeout=10))
                    await computed.wait()
                    await set_qty(6)  # subscribe_get 读回之前又写了一次
                    release_read.set()
                    sub_row, row = await get_task  # sub 回复先发给客户端
                    release_tick.set()
                    pushed = await tick  # tick 的推送随后才送达
    finally:
        release_read.set()
        release_tick.set()
    assert sub_row and row and row["qty"] == 6
    assert pushed[sub_10][row_id]["qty"] == 5  # 这个 tick 确实是在订阅读期间处理的

    # 客户端按收到的先后应用：先是 sub 回复，再是 tick 的推送
    client: dict[str, dict] = {sub_row: {row_id: row}}
    for sub_id, sub_rows in pushed.items():
        client.setdefault(sub_id, {}).update(sub_rows)

    def caught_up(updates):
        assert updates[sub_row][row_id]["qty"] == 6

    await updates_until(broker, caught_up, merged=client, timeout=3)


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

    def reborn_pushed(updates):
        assert updates[sub_id][row_id]["name"] == "Reborn"

    await updates_until(broker, reborn_pushed)


# ====== 订阅生效后的补读：初始读落在还没应用某次写入的副本上，也不会一直旧 ======


def _lagging_once(real, stale):
    """包一个读方法：第一次调用返回 stale 的拷贝（落在滞后副本上），之后照常读"""
    lagging = True

    async def read(*args, **kwargs):
        nonlocal lagging
        if lagging:
            lagging = False
            return [dict(r) for r in stale] if isinstance(stale, list) else dict(stale)
        return await real(*args, **kwargs)

    return read


async def test_subscribe_get_followup_reread_fixes_lagging_initial_read(
    broker: SubscriptionBroker, filled_item_ref, admin_ctx
):
    """订阅生效前的写入不会再有通知；初始读又落在还没应用它的副本上时，订阅生效后隔一个
    interval 补读一次，客户端最终拿到新值"""
    backend = broker._backend
    servant = backend.servant
    row_id = int((await servant.range(filled_item_ref, "time", 111))[0].id)
    stale = await servant.get(filled_item_ref, row_id, RowFormat.TYPED_DICT)
    assert stale and stale["qty"] == 999
    async with backend.session("pytest", 1) as session:
        repo = session.using(filled_item_ref.comp_cls)
        row = await repo.get(id=row_id)
        assert row
        row.qty = 4321
        await repo.update(row)
    await backend.wait_for_synced()

    with patch.object(servant, "get", _lagging_once(servant.get, stale)):
        sub_id, row = await broker.subscribe_get(
            filled_item_ref, admin_ctx, "id", row_id
        )
    assert sub_id and row and row["qty"] == 999

    def caught_up(updates):
        assert updates[sub_id][row_id]["qty"] == 4321

    await updates_until(broker, caught_up)


async def test_subscribe_range_followup_reread_fixes_lagging_initial_read(
    broker: SubscriptionBroker, filled_item_ref, admin_ctx
):
    """subscribe_range 的初始读漏了新进入范围的行、读到了旧内容（先读后订的间隙，或落在
    滞后副本上）：订阅生效后补读一次——重跑范围比对、重读各行"""
    backend = broker._backend
    servant = backend.servant
    comp = filled_item_ref.comp_cls
    stale_rows = await servant.range(
        filled_item_ref, "owner", 10, limit=33, row_format=RowFormat.TYPED_DICT
    )
    assert len(stale_rows) == 25
    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        row = await repo.get(time=110)
        assert row
        row.qty = 55
        changed_id = row.id
        await repo.update(row)
        late = comp.new_row()
        late.name, late.owner, late.time = "Late", 10, 700
        await repo.insert(late)
        late_id = late.id
    await backend.wait_for_synced()

    with patch.object(servant, "range", _lagging_once(servant.range, stale_rows)):
        sub_id, rows = await broker.subscribe_range(
            filled_item_ref, admin_ctx, "owner", 10, limit=33
        )
    assert sub_id and len(rows) == 25
    assert late_id not in {r["id"] for r in rows}

    def caught_up(updates):
        assert updates[sub_id][changed_id]["qty"] == 55
        assert updates[sub_id][late_id]["name"] == "Late"

    await updates_until(broker, caught_up)


async def test_subscribe_table_initial_read_waits_lag_budget(
    broker: SubscriptionBroker, filled_item_ref, admin_ctx
):
    """整表订阅不补读（整表重读太贵）：改为先订阅，生效后隔一个 interval 才全量读，
    生效前在别的节点上已应用的写入这时副本也已应用"""
    servant = broker._backend.servant
    mq = broker._mq_client
    acked_at: float | None = None
    read_at: float | None = None
    real_subscribe, real_range = mq.subscribe, servant.range

    async def timed_subscribe(*channels):
        nonlocal acked_at
        await real_subscribe(*channels)
        acked_at = time.monotonic()

    async def timed_range(*args, **kwargs):
        nonlocal read_at
        read_at = read_at or time.monotonic()
        return await real_range(*args, **kwargs)

    with (
        patch.object(mq, "subscribe", timed_subscribe),
        patch.object(servant, "range", timed_range),
    ):
        sub_id, rows = await broker.subscribe_table(filled_item_ref, admin_ctx)
    assert sub_id and len(rows) == 25
    assert acked_at is not None and read_at is not None
    assert read_at - acked_at >= INTERVAL * 0.9


async def test_subscribe_table_defers_notifications_during_initial_read(
    broker: SubscriptionBroker, filled_item_ref, admin_ctx
):
    """整表订阅初始化期间（已生效、初始全量读还没完成）弹出的通知不能被消费掉：客户端还没
    拿到 sub_id 会丢弃那次推送，而初始读又未必包含那次写入。攒起来，初始读完成后重读"""
    backend = broker._backend
    servant = backend.servant
    comp = filled_item_ref.comp_cls
    stale_rows = await servant.range(
        filled_item_ref,
        "id",
        float("-inf"),
        float("inf"),
        limit=100,
        row_format=RowFormat.TYPED_DICT,
    )
    real_range = servant.range
    reading = asyncio.Event()
    release = asyncio.Event()

    async def slow_lagging_range(*args, **kwargs):
        if not reading.is_set():  # 初始全量读：卡住，并且落在滞后副本上
            reading.set()
            await release.wait()
            return [dict(r) for r in stale_rows]
        return await real_range(*args, **kwargs)

    with patch.object(servant, "range", slow_lagging_range):
        task = asyncio.create_task(broker.subscribe_table(filled_item_ref, admin_ctx))
        async with asyncio.timeout(3):
            await reading.wait()
        # 初始读卡着的时候有人改了一行
        async with backend.session("pytest", 1) as session:
            repo = session.using(comp)
            row = await repo.get(time=111)
            assert row
            row.qty = 31
            id_a = row.id
            await repo.update(row)
        # 推送循环照常在跑：它弹出这条通知时订阅还在初始化
        early = await broker.get_updates(timeout=0.5)
        release.set()
        sub_id, rows = await task
    assert sub_id and early == {}
    assert {r["id"]: r["qty"] for r in rows}[id_a] == 999  # 初始行是旧的

    def caught_up(updates):
        assert updates[sub_id][id_a]["qty"] == 31

    await updates_until(broker, caught_up)


async def test_subscribe_followup_reread_pushes_nothing_when_fresh(
    broker: SubscriptionBroker, filled_item_ref, admin_ctx
):
    """初始读已经是最新时，补读读回的一样，不产生任何推送"""
    sub_row, _ = await broker.subscribe_get(filled_item_ref, admin_ctx, "name", "Itm10")
    sub_10, _ = await broker.subscribe_range(
        filled_item_ref, admin_ctx, "owner", 10, limit=33
    )
    sub_tbl, _ = await broker.subscribe_table(filled_item_ref, admin_ctx)
    assert sub_row and sub_10 and sub_tbl
    assert await broker.get_updates(timeout=0.6) == {}


# ====== pubsub 断线重订：这段时间的写入没有通知，重订生效后 RESYNC 补读 ======


async def _write_while_deaf(broker: SubscriptionBroker, channel: str, write):
    """模拟断线：本连接暂时退订 channel，期间的写入收不到通知，然后订回来"""
    mq = broker._mq_client
    await mq.unsubscribe(channel)
    await write()
    await broker._backend.wait_for_synced()
    await mq.subscribe(channel)


async def test_subscribe_table_resync(
    broker: SubscriptionBroker, filled_item_ref, admin_ctx
):
    """整表订阅收到 RESYNC（这段时间的变更不可知）：整表重读，推所有可见行，已知但
    读不到的行推 None"""
    backend = broker._backend
    comp = filled_item_ref.comp_cls
    sub_id, rows = await broker.subscribe_table(filled_item_ref, admin_ctx)
    assert sub_id and len(rows) == 25
    table_channel = cast(TableSubscription, broker._subs[sub_id]).table_channel
    ids: dict[str, int] = {}

    async def write():
        async with backend.session("pytest", 1) as session:
            repo = session.using(comp)
            row = await repo.get(time=111)
            assert row
            row.qty = 77
            ids["a"] = row.id
            await repo.update(row)
            gone = await repo.get(time=112)
            assert gone
            ids["b"] = gone.id
            repo.delete(gone.id)
            new = comp.new_row()
            new.name, new.owner, new.time = "New", 10, 800
            await repo.insert(new)
            ids["c"] = new.id

    await _write_while_deaf(broker, table_channel, write)
    assert await broker.get_updates(timeout=0.3) == {}, "断线期间不该收到通知"
    broker._mq_client.push_pulled_(table_channel, [MQClient.RESYNC])  # hub 重订后分发的

    def resynced(updates):
        assert updates[sub_id][ids["a"]]["qty"] == 77
        assert updates[sub_id][ids["b"]] is None
        assert updates[sub_id][ids["c"]]["name"] == "New"

    await updates_until(broker, resynced)
    tbl_sub = cast(TableSubscription, broker._subs[sub_id])
    assert ids["b"] not in tbl_sub.known_ids and ids["c"] in tbl_sub.known_ids


@use_redis_family_backend_only
async def test_row_subscription_catches_up_after_pubsub_resubscribe(
    broker: SubscriptionBroker, filled_item_ref, admin_ctx
):
    """行订阅：断线重订期间的写入没有通知，重订全部生效后 hub 分发 RESYNC，补读追上"""
    backend = broker._backend
    sub_id, row = await broker.subscribe_get(
        filled_item_ref, admin_ctx, "name", "Itm10"
    )
    assert sub_id and row
    assert await broker.get_updates(timeout=0.5) == {}  # 订阅生效后的补读先消化掉
    channel = cast(RowSubscription, broker._subs[sub_id]).channel

    async def write():
        async with backend.session("pytest", 1) as session:
            repo = session.using(filled_item_ref.comp_cls)
            target = await repo.get(id=row["id"])
            assert target
            target.qty = 66
            await repo.update(target)

    await _write_while_deaf(broker, channel, write)
    assert await broker.get_updates(timeout=0.3) == {}, "断线期间不该收到通知"
    hub = backend.servant._hub  # type: ignore[attr-defined]
    hub._on_resubscribed([channel])  # AsyncKeyspacePubSub 重订全部生效时的回调

    def caught_up(updates):
        assert updates[sub_id][row["id"]]["qty"] == 66

    await updates_until(broker, caught_up)


async def test_subscribe_table_resync_rls(
    broker: SubscriptionBroker, filled_item_ref, user_id10_ctx
):
    """整表重同步也按 RLS 判定：断线期间失去可见性的已知行推 None、移出已知集合；
    一直不可见的行（别人的）不推"""
    backend = broker._backend
    comp = filled_item_ref.comp_cls
    async with backend.session("pytest", 1) as session:
        other = comp.new_row()
        other.name, other.owner, other.time = "Other", 11, 601
        await session.using(comp).insert(other)
        other_id = int(other.id)
    await backend.wait_for_synced()

    sub_id, rows = await broker.subscribe_table(filled_item_ref, user_id10_ctx)
    assert sub_id and len(rows) == 25
    tbl_sub = cast(TableSubscription, broker._subs[sub_id])
    lost: dict[str, int] = {}

    async def write():
        async with backend.session("pytest", 1) as session:
            repo = session.using(comp)
            row = await repo.get(time=114)
            assert row
            row.owner = 11
            lost["id"] = int(row.id)
            await repo.update(row)
            row = await repo.get(id=other_id)
            assert row
            row.qty = 5
            await repo.update(row)

    await _write_while_deaf(broker, tbl_sub.table_channel, write)
    assert await broker.get_updates(timeout=0.3) == {}, "断线期间不该收到通知"
    broker._mq_client.push_pulled_(tbl_sub.table_channel, [MQClient.RESYNC])

    def resynced(updates):
        assert updates[sub_id][lost["id"]] is None

    updates = await updates_until(broker, resynced)
    assert other_id not in updates[sub_id]
    assert lost["id"] not in tbl_sub.known_ids and other_id not in tbl_sub.known_ids


async def test_subscribe_table_resync_at_row_cap_keeps_unread_known_rows(
    mod_auto_backend, filled_item_ref, admin_ctx
):
    """整表重同步按 id 升序读满上限时，后面可能还有行没读到：读到的最大 id 之后的已知行
    不能当作删除推 None，之前读不到的已知行照常判删"""
    backend = mod_auto_backend("main")
    comp = filled_item_ref.comp_cls
    broker = SubscriptionBroker(backend, max_table_rows=25)
    try:
        sub_id, rows = await broker.subscribe_table(filled_item_ref, admin_ctx)
        assert sub_id and len(rows) == 25
        ids = sorted(int(row["id"]) for row in rows)
        tbl_sub = cast(TableSubscription, broker._subs[sub_id])

        async def write():
            # 删掉最小的已知行，再插两行 id 比所有已知行都小的：表里 26 行，重读只读到
            # 前 25 行，最大的已知行 ids[-1] 落在上限之外
            async with backend.session("pytest", 1) as session:
                repo = session.using(comp)
                assert await repo.get(id=ids[0])
                repo.delete(ids[0])
                for i in (1, 2):
                    row = comp.new_row(id_=i)
                    row.name, row.owner, row.time = f"Low{i}", 10, 900 + i
                    await repo.insert(row)

        await _write_while_deaf(broker, tbl_sub.table_channel, write)
        broker._mq_client.push_pulled_(tbl_sub.table_channel, [MQClient.RESYNC])

        def resynced(updates):
            assert updates[sub_id][ids[0]] is None
            assert updates[sub_id][1]["name"] == "Low1"

        updates = await updates_until(broker, resynced)
        assert ids[-1] not in updates[sub_id], "上限之外没读到的行被当成删除了"
        assert ids[-1] in tbl_sub.known_ids and ids[0] not in tbl_sub.known_ids
    finally:
        await broker.close()


# ============ 权限、重复订阅、初始读失败回滚等边界 ============


@pytest.fixture
async def admin_only_ref(new_component_env, mod_auto_backend):
    """ADMIN 权限的组件（只有管理员能读），建表并写一行 key=1"""
    import numpy as np

    from hetu.data import BaseComponent, Permission, define_component, property_field
    from hetu.data.backend import Table

    @define_component(namespace="pytest", permission=Permission.ADMIN)
    class AdminOnly(BaseComponent):
        key: np.int64 = property_field(0, unique=True, index=True)

    backend: Backend = mod_auto_backend()
    ref = create_ref(AdminOnly, backend)
    async with backend.session("pytest", 1) as session:
        row = AdminOnly.new_row()
        row.key = 1
        await session.using(AdminOnly).insert(row)
    await backend.wait_for_synced()
    return Table(AdminOnly, ref.instance_name, ref.cluster_id, backend)


async def test_admin_component_rejects_non_admin(
    broker: SubscriptionBroker, admin_only_ref, admin_ctx, user_id10_ctx
):
    """ADMIN 权限的组件：非管理员（已登录的也不行）的行、范围、整表订阅都拒绝，不读库、
    不占频道和计数；管理员照常订阅"""
    servant = broker._backend.servant
    row_id = int((await servant.range(admin_only_ref, "key", 1, limit=1))[0].id)

    for ctx in (user_id10_ctx, make_ctx()):
        assert await broker.subscribe_get(admin_only_ref, ctx, "key", 1) == (None, None)
        assert await broker.subscribe_get(admin_only_ref, ctx, "id", row_id) == (
            None,
            None,
        )
        assert await broker.subscribe_range(admin_only_ref, ctx, "key", 0, 9) == (
            None,
            [],
        )
        assert await broker.subscribe_table(admin_only_ref, ctx) == (None, [])
    assert broker.count() == (0, 0, 0)
    assert not broker._mq_client.subscribed_channels

    sub_id, row = await broker.subscribe_get(admin_only_ref, admin_ctx, "key", 1)
    assert sub_id and row and row["id"] == row_id


async def test_anonymous_get_and_range_denied(
    broker: SubscriptionBroker, filled_item_ref
):
    """未登录连接对非 EVERYBODY 组件的行订阅、范围订阅：拒绝，且不占用任何频道/计数
    （整表订阅见 test_subscribe_table_permission_denied）"""
    anon_ctx = make_ctx()
    assert await broker.subscribe_get(filled_item_ref, anon_ctx, "time", 110) == (
        None,
        None,
    )
    assert await broker.subscribe_range(filled_item_ref, anon_ctx, "owner", 10) == (
        None,
        [],
    )
    assert broker.count() == (0, 0, 0)
    assert not broker._mq_client.subscribed_channels


async def test_subscribe_get_by_index_not_found(
    broker: SubscriptionBroker, filled_item_ref, admin_ctx
):
    """按非 id 索引订阅、没有匹配的行：回 (None, None)，不订任何频道"""
    sub_id, row = await broker.subscribe_get(
        filled_item_ref, admin_ctx, "name", "NoSuchItem"
    )
    assert sub_id is None and row is None
    assert broker.count() == (0, 0, 0)
    assert not broker._mq_client.subscribed_channels


async def test_subscribe_get_duplicate_returns_latest_or_revokes(
    broker: SubscriptionBroker, filled_item_ref, user_id10_ctx
):
    """重复订阅同一行：行仍可见时回同一 sub_id 和库里的最新行（不含 _version），不重复
    计数；行已对 caller 不可见（失去 RLS）时回 (None, None)，并撤掉旧订阅"""
    backend = broker._backend
    comp = filled_item_ref.comp_cls
    sub_id, row = await broker.subscribe_get(
        filled_item_ref, user_id10_ctx, "name", "Itm10"
    )
    assert sub_id and row
    row_id = row["id"]

    async def modify(**fields):
        async with backend.session("pytest", 1) as session:
            repo = session.using(comp)
            target = await repo.get(id=row_id)
            assert target
            for key, value in fields.items():
                setattr(target, key, value)
            await repo.update(target)
        await backend.wait_for_synced()

    await modify(qty=5)
    sub_again, row_again = await broker.subscribe_get(
        filled_item_ref, user_id10_ctx, "id", row_id
    )
    assert sub_again == sub_id
    assert row_again and row_again["qty"] == 5 and "_version" not in row_again
    assert broker.count() == (1, 0, 0)

    await modify(owner=11)
    sub_lost, row_lost = await broker.subscribe_get(
        filled_item_ref, user_id10_ctx, "id", row_id
    )
    assert sub_lost is None and row_lost is None
    assert sub_id not in broker._subs
    assert broker.count() == (0, 0, 0)
    assert not broker._mq_client.subscribed_channels


async def test_subscribe_get_read_failure_rolls_back(
    broker: SubscriptionBroker, filled_item_ref, admin_ctx
):
    """先订后读的那次读抛异常（如后端连接断了）：刚登记的订阅和频道撤干净，异常照抛"""
    servant = broker._backend.servant
    row_id = int((await servant.range(filled_item_ref, "time", 110, limit=1))[0].id)

    with (
        patch.object(servant, "get", side_effect=ConnectionError("read failed")),
        pytest.raises(ConnectionError, match="read failed"),
    ):
        await broker.subscribe_get(filled_item_ref, admin_ctx, "id", row_id)
    assert broker.count() == (0, 0, 0)
    assert not broker._subs and not broker._channel_subs
    assert not broker._mq_client.subscribed_channels


async def test_subscribe_range_force_false_and_duplicate(
    broker: SubscriptionBroker, filled_item_ref, admin_ctx, user_id11_ctx
):
    """force=False 时没有（可见的）行就不订阅；默认 force=True 空结果也订。重复订阅同一
    查询回同一 sub_id 和当前的行，不重复计数"""
    assert await broker.subscribe_range(
        filled_item_ref, admin_ctx, "owner", 99, force=False
    ) == (None, [])
    # 有行，但对 caller 都不可见（RLS 过滤后为空）
    assert await broker.subscribe_range(
        filled_item_ref, user_id11_ctx, "owner", 10, force=False
    ) == (None, [])
    assert broker.count() == (0, 0, 0)
    assert not broker._mq_client.subscribed_channels

    sub_empty, rows = await broker.subscribe_range(
        filled_item_ref, admin_ctx, "owner", 99
    )
    assert sub_empty and rows == []

    sub_id, rows = await broker.subscribe_range(
        filled_item_ref, admin_ctx, "owner", 10, limit=33
    )
    channels = set(broker._mq_client.subscribed_channels)
    sub_again, rows_again = await broker.subscribe_range(
        filled_item_ref, admin_ctx, "owner", 10, limit=33
    )
    assert sub_again == sub_id
    assert {r["id"] for r in rows_again} == {r["id"] for r in rows}
    assert "_version" not in rows_again[0]
    assert broker.count() == (0, 2, 0)
    assert set(broker._mq_client.subscribed_channels) == channels


async def _overfill_item_table(backend, item_ref):
    """filled_item_ref 有 25 行，再插一行让它超过 max_table_rows=25"""
    comp = item_ref.comp_cls
    async with backend.session("pytest", 1) as session:
        row = comp.new_row()
        row.name, row.owner, row.time = "Extra", 10, 700
        await session.using(comp).insert(row)
    await backend.wait_for_synced()


async def test_subscribe_table_duplicate_over_row_cap_revokes_old_sub(
    mod_auto_backend, filled_item_ref, admin_ctx
):
    """重复整表订阅时表已超过行数上限：与首次订阅超限一样回 (None, [])，并撤掉旧订阅。
    客户端拿到 None 就认为没有订阅、不会再来 unsub（同 subscribe_get 重复订阅时行已
    不可见），不撤的话旧订阅和表频道会挂到连接结束，还占着整表订阅数的名额"""
    backend = mod_auto_backend("main")
    broker = SubscriptionBroker(backend, max_table_rows=25)
    try:
        sub_id, rows = await broker.subscribe_table(filled_item_ref, admin_ctx)
        assert sub_id and len(rows) == 25
        await _overfill_item_table(backend, filled_item_ref)

        assert await broker.subscribe_table(filled_item_ref, admin_ctx) == (None, [])
        assert sub_id not in broker._subs
        assert broker.count() == (0, 0, 0)
        assert not broker._mq_client.subscribed_channels
    finally:
        await broker.close()


async def test_subscribe_table_duplicate_over_row_cap_spares_new_sub(
    mod_auto_backend, filled_item_ref, admin_ctx
):
    """重复整表订阅的后半段（服务器里在后台跑）重读完之前，旧订阅已被退订、同一 sub_id
    又登记了新的订阅：重读发现超限时只撤它认出的那个旧订阅，新登记的不能被它撤掉"""
    backend = mod_auto_backend("main")
    broker = SubscriptionBroker(backend, max_table_rows=25)
    try:
        sub_id, _ = await broker.subscribe_table(filled_item_ref, admin_ctx)
        assert sub_id
        await _overfill_item_table(backend, filled_item_ref)

        reread = await broker.begin_subscribe_table(filled_item_ref, admin_ctx)
        # 后半段还没跑，接收协程先处理了客户端的 unsub 和新的 sub
        await broker.unsubscribe(sub_id)
        finish_new = await broker.begin_subscribe_table(filled_item_ref, admin_ctx)
        new_sub = broker._subs[sub_id]

        assert await reread == (None, [])
        assert broker._subs.get(sub_id) is new_sub, "新登记的订阅被旧的重读撤掉了"
        # 新订阅自己的后半段照常收尾：表超限，它自己撤掉
        assert await finish_new == (None, [])
        assert broker.count() == (0, 0, 0)
    finally:
        await broker.close()


async def test_subscribe_table_cancelled_during_initial_read_rolls_back(
    broker: SubscriptionBroker, filled_item_ref, admin_ctx
):
    """整表订阅在初始全量读完成前被取消（连接断开）：已登记的订阅与频道撤干净"""
    servant = broker._backend.servant
    entered = asyncio.Event()

    async def stuck_range(*args, **kwargs):
        entered.set()
        await asyncio.Event().wait()

    with patch.object(servant, "range", stuck_range):
        task = asyncio.create_task(broker.subscribe_table(filled_item_ref, admin_ctx))
        async with asyncio.timeout(5):
            await entered.wait()
        assert broker.count() == (0, 0, 1), "订阅应在读之前就已登记"
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert broker.count() == (0, 0, 0)
    assert not broker._subs and not broker._channel_subs
    assert not broker._mq_client.subscribed_channels


async def test_index_sub_skips_row_gone_before_read(
    broker: SubscriptionBroker, filled_item_ref, admin_ctx
):
    """范围比对发现新行，批量读时它已被删（range 与 get_many 之间）：不推、不订它的行频道，
    也不记进上次的比对结果——之后它若还在范围里，下次比对照常当作新行推"""
    backend = broker._backend
    servant = backend.servant
    comp = filled_item_ref.comp_cls
    sub_id, _ = await broker.subscribe_range(
        filled_item_ref, admin_ctx, "owner", 10, limit=33
    )
    assert sub_id
    assert await broker.get_updates(timeout=0.5) == {}  # 订阅生效后的补读先消化掉
    idx_sub = cast(IndexSubscription, broker._subs[sub_id])

    async with backend.session("pytest", 1) as session:
        new = comp.new_row()
        new.name, new.owner, new.time = "New", 10, 700
        await session.using(comp).insert(new)
        new_id = int(new.id)
    new_channel = servant.row_channel(filled_item_ref, new_id)

    real_get_many = servant.get_many
    vanished: list[int] = []

    async def get_many_vanished(ref, ids, *args, **kwargs):
        rows = await real_get_many(ref, ids, *args, **kwargs)
        if new_id in {int(i) for i in ids}:
            vanished.append(new_id)
        return [None if int(i) == new_id else r for i, r in zip(ids, rows)]

    with patch.object(servant, "get_many", get_many_vanished):
        async with asyncio.timeout(10):
            while not vanished:
                assert await broker.get_updates(timeout=0.5) == {}
    assert new_id not in idx_sub.last_range_result
    assert new_channel not in idx_sub.row_subs
    assert new_channel not in broker._channel_subs

    broker._mq_client.request_reread(idx_sub.index_channel)

    def pushed(updates):
        assert updates[sub_id][new_id]["name"] == "New"

    await updates_until(broker, pushed)
    assert new_channel in idx_sub.row_subs


async def test_subscription_rejects_foreign_channel(
    broker: SubscriptionBroker, filled_item_ref, admin_ctx
):
    """订阅对象只认自己的频道：收到别的频道是分发记账错了，抛出而不是推错数据。
    整表订阅收到空 payload（表级频道消息的 payload 非法时）什么也不读"""
    tbl_id, _ = await broker.subscribe_table(filled_item_ref, admin_ctx)
    idx_id, _ = await broker.subscribe_range(filled_item_ref, admin_ctx, "owner", 10)
    tbl_sub = cast(TableSubscription, broker._subs[tbl_id])
    idx_sub = cast(IndexSubscription, broker._subs[idx_id])

    with pytest.raises(RuntimeError, match="bogus:channel"):
        await tbl_sub.get_updated("bogus:channel", {"1"})
    with pytest.raises(RuntimeError, match="bogus:channel"):
        await idx_sub.get_updated("bogus:channel")

    with patch.object(broker._backend.servant, "get_many") as get_many:
        for payload in (None, set()):
            assert await tbl_sub.get_updated(tbl_sub.table_channel, payload) == (
                set(),
                set(),
                {},
            )
        get_many.assert_not_called()
