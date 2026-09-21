"""
进程共享的通知接收器（Redis 的 PubSubHub / SQL 的 SQLNotifyHub）：
多个连接订阅同一频道只向后端订阅一次，各自都能收到更新，最后一个退订才真正退订。
"""

import asyncio
from contextvars import ContextVar
from typing import cast

from hetu.common.snowflake_id import SnowflakeID
from hetu.data.backend import Backend
from hetu.data.sub import RowSubscription, SubscriptionBroker
from hetu.system import SystemContext

SnowflakeID().init(1, 0)


def _admin_ctx() -> SystemContext:
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


def _hub(backend: Backend):
    hub = backend.servant._hub  # type: ignore[attr-defined]
    assert hub is not None
    return hub


async def _update_qty(backend: Backend, ref, qty: int):
    async with backend.session("pytest", 1) as session:
        repo = session.using(ref.comp_cls)
        row = await repo.get(time=110)
        assert row
        row.qty = qty
        await repo.update(row)
    await backend.wait_for_synced()


async def _get_updates(broker: SubscriptionBroker):
    async with asyncio.timeout(3):
        return await broker.get_updates()


async def test_hub_shared_subscription(filled_item_ref, mod_auto_backend):
    backend: Backend = mod_auto_backend()
    RowSubscription._RowSubscription__cache = ContextVar("user_row_cache")  # type: ignore
    ctx = _admin_ctx()
    broker_a = SubscriptionBroker(backend)
    broker_b = SubscriptionBroker(backend)

    hub = _hub(backend)
    pubsub = getattr(hub, "_pubsub", None)  # Redis 才有
    sends = 0
    if pubsub is not None:
        real_subscribe = pubsub.subscribe

        async def counting_subscribe(*channels):
            nonlocal sends
            sends += 1
            await real_subscribe(*channels)

        pubsub.subscribe = counting_subscribe

    # 两个连接订阅同一行：hub 里计数 2，向后端只订阅一次
    sub_a, row = await broker_a.subscribe_get(filled_item_ref, ctx, "name", "Itm10")
    assert sub_a and row
    sends_after_first = sends
    sub_b, _ = await broker_b.subscribe_get(filled_item_ref, ctx, "name", "Itm10")
    assert sub_b
    channel = cast(RowSubscription, broker_a._subs[sub_a]).channel
    assert hub.subscriber_count(channel) == 2
    assert channel in hub.channels
    if pubsub is not None:
        assert sends == sends_after_first, "同一频道第二个订阅者不应再发 SUBSCRIBE"
        assert channel in pubsub.subscribed

    # 一次写入，两个连接都收到
    await _update_qty(backend, filled_item_ref, 998)
    updates_a = await _get_updates(broker_a)
    updates_b = await _get_updates(broker_b)
    assert updates_a[sub_a][row["id"]]["qty"] == 998
    assert updates_b[sub_b][row["id"]]["qty"] == 998

    # 关掉 A，B 仍然收得到；频道还在 hub 里
    await broker_a.close()
    assert hub.subscriber_count(channel) == 1
    await _update_qty(backend, filled_item_ref, 997)
    updates_b = await _get_updates(broker_b)
    assert updates_b[sub_b][row["id"]]["qty"] == 997

    # 最后一个也关掉，hub 里没有这个频道了，后端也退订了
    await broker_b.close()
    assert hub.subscriber_count(channel) == 0
    assert channel not in hub.channels
    if pubsub is not None:
        assert channel not in pubsub.subscribed
        pubsub.subscribe = real_subscribe


async def test_hub_mq_client_close_releases_only_own(filled_item_ref, mod_auto_backend):
    """一个连接关闭只撤销自己的登记，不影响别的连接对同一频道的订阅"""
    backend: Backend = mod_auto_backend()
    hub = _hub(backend)
    servant = backend.servant
    rows = await servant.range(filled_item_ref, "time", 110, 112, limit=10)
    channels = [servant.row_channel(filled_item_ref, r.id) for r in rows]
    assert len(channels) == 3

    mq1 = backend.get_mq_client()
    mq2 = backend.get_mq_client()
    await mq1.subscribe(*channels)
    await mq2.subscribe(channels[0])
    assert hub.subscriber_count(channels[0]) == 2
    assert hub.subscriber_count(channels[1]) == 1

    await mq1.close()
    assert not mq1.subscribed_channels
    assert hub.subscriber_count(channels[0]) == 1
    assert hub.subscriber_count(channels[1]) == 0
    assert mq2.subscribed_channels == {channels[0]}

    # 关闭后再订阅应报错
    try:
        await mq1.subscribe(channels[2])
    except ConnectionError:
        pass
    else:
        raise AssertionError("closed MQClient must reject subscribe")

    await mq2.close()
    assert hub.subscriber_count(channels[0]) == 0
