"""
worker 级订阅器 SubscriptionHub：同一 worker 的连接共用一个 MQ 队列和处理循环（设计稿
docs/superpowers/specs/2026-09-28-worker-subscriptions-design.md）。连真实后端、按后端参数化；
协程交错的情形在 test_backend_sub_race.py 里用假 pubsub 测。
"""

import asyncio
from collections.abc import AsyncGenerator, Callable
from contextlib import ExitStack
from contextvars import ContextVar
from typing import cast
from unittest.mock import patch

import pytest
from fixtures.contexts import settled_updates, wait_until

from hetu.common.snowflake_id import SnowflakeID
from hetu.data.backend import Backend
from hetu.data.backend.base import HubMQClient, MQClient
from hetu.data.sub import RowSubscription, SubscriptionBroker, SubscriptionHub

SnowflakeID().init(1, 0)
INTERVAL = 1 / MQClient.UPDATE_FREQUENCY


@pytest.fixture
async def hub(mod_auto_backend) -> AsyncGenerator[SubscriptionHub]:
    """本用例独享的 hub（后台循环）：同模块别的用例留下的订阅不会干扰计数"""
    RowSubscription._RowSubscription__cache = ContextVar("user_row_cache")  # type: ignore
    hub = SubscriptionHub(mod_auto_backend("main"))
    yield hub
    await hub.close()


async def _set_qty(backend: Backend, ref, qty: int) -> int:
    """改 time=110 那行的 qty，返回它的 id"""
    async with backend.session("pytest", 1) as session:
        repo = session.using(ref.comp_cls)
        row = await repo.get(time=110)
        assert row
        row.qty = qty
        await repo.update(row)
    return int(row.id)


def _count_sub_reads(stack: ExitStack, backend: Backend) -> Callable[[], int]:
    """数订阅的读（各 servant 的 get / get_many）。事务读走 get_many_array_ / range_read_，不计"""
    mocks = []
    for servant in backend._servants:  # type: ignore[reportPrivateUsage]
        for meth in ("get", "get_many"):
            mocks.append(
                stack.enter_context(
                    patch.object(servant, meth, wraps=getattr(servant, meth))
                )
            )
    return lambda: sum(mock.call_count for mock in mocks)


async def test_brokers_share_backend_hub(mod_auto_backend):
    """同一 backend 上的门面默认共用一个 hub，挂在 backend 上"""
    backend: Backend = mod_auto_backend("main")
    a = SubscriptionBroker(backend)
    b = SubscriptionBroker(backend)
    assert a._hub is b._hub
    assert a._hub is SubscriptionHub.of(backend)
    assert backend.sub_hub_ is a._hub
    await a.close()
    await b.close()


async def test_notification_reaches_worker_once(
    hub: SubscriptionHub, filled_item_ref, admin_ctx
):
    """几个连接订同一行：通知接收器只登记 hub 这一个 MQClient，一条通知在 worker 内只分发一次，
    各连接都收到推送（今天是每个连接一个 MQClient，各分发一次）"""
    backend = hub._backend
    brokers = [SubscriptionBroker(backend, hub=hub) for _ in range(3)]
    sub_ids = []
    for broker in brokers:
        sub_id, _ = await broker.subscribe_get(filled_item_ref, admin_ctx, "time", 110)
        assert sub_id
        sub_ids.append(sub_id)
    channel = cast(RowSubscription, brokers[0]._subs[sub_ids[0]]).channel
    receiver = cast(HubMQClient, hub.mq)._hub
    assert receiver.subscriber_count(channel) == 1

    with (
        patch.object(receiver, "_dispatch", wraps=receiver._dispatch) as dispatch,
        patch.object(hub.mq, "push_pulled_", wraps=hub.mq.push_pulled_) as push,
    ):
        row_id = await _set_qty(backend, filled_item_ref, 555)
        for broker, sub_id in zip(brokers, sub_ids):
            updates = await settled_updates(broker, timeout=3)
            assert updates[sub_id][row_id]["qty"] == 555
    dispatched = [c.args[0] for c in dispatch.call_args_list].count(channel)
    pushed = [c.args[0] for c in push.call_args_list].count(channel)
    assert dispatched >= 1
    assert pushed == dispatched, "每条通知只该进 hub 的队列一次"
    for broker in brokers:
        await broker.close()


async def test_same_row_read_once_per_tick(
    hub: SubscriptionHub, filled_item_ref, admin_ctx
):
    """5 个连接订同一行，一次写入：worker 内每个 tick 只读这行一次（今天每个连接各读一次）"""
    backend = hub._backend
    brokers = [SubscriptionBroker(backend, hub=hub) for _ in range(5)]
    sub_ids = []
    for broker in brokers:
        sub_id, _ = await broker.subscribe_get(filled_item_ref, admin_ctx, "time", 110)
        sub_ids.append(sub_id)
    await asyncio.sleep(INTERVAL * 3)  # 订阅生效后的补读先消化掉

    with ExitStack() as stack:
        reads = _count_sub_reads(stack, backend)
        row_id = await _set_qty(backend, filled_item_ref, 556)
        for broker, sub_id in zip(brokers, sub_ids):
            updates = await settled_updates(broker, timeout=3)
            assert updates[sub_id][row_id]["qty"] == 556
        total = reads()
    # 一次写入在行频道上可能有不止一条通知（合并后尾随重读一次），但读的次数与连接数无关
    assert 1 <= total <= 2, total
    for broker in brokers:
        await broker.close()


async def test_subscribe_reread_targets_only_new_subscription(
    hub: SubscriptionHub, filled_item_ref, admin_ctx
):
    """B 订阅生效后的补读只让 B 的订阅重读；A 订着同样的频道，不跟着重跑（设计稿 §4.4）"""
    backend = hub._backend
    a = SubscriptionBroker(backend, hub=hub)
    b = SubscriptionBroker(backend, hub=hub)
    sub_a, _ = await a.subscribe_range(
        filled_item_ref, admin_ctx, "owner", 10, limit=30
    )
    assert sub_a
    await asyncio.sleep(INTERVAL * 3)  # A 自己的补读先消化掉
    a_sub = a._subs[sub_a]
    with patch.object(a_sub, "get_updated", wraps=a_sub.get_updated) as spy_a:
        sub_b, _ = await b.subscribe_range(
            filled_item_ref, admin_ctx, "owner", 10, limit=30
        )
        assert sub_b
        b_sub = b._subs[sub_b]
        channels = len(b_sub.channels)
        assert channels == 26  # 索引频道 + 25 行
        with patch.object(b_sub, "get_updated", wraps=b_sub.get_updated) as spy_b:
            await wait_until(lambda: spy_b.call_count >= channels, timeout=3)
            await asyncio.sleep(INTERVAL * 2)
        assert spy_b.call_count == channels
    assert spy_a.call_count == 0, "B 的补读让 A 也重跑了"
    await a.close()
    await b.close()


async def test_manual_hub_keeps_notifications_until_get_updates(
    mod_auto_backend, filled_item_ref, admin_ctx
):
    """手动模式不起后台循环：不调 get_updates，通知就留在队列里（现有订阅用例依赖这一点）"""
    backend: Backend = mod_auto_backend("main")
    RowSubscription._RowSubscription__cache = ContextVar("user_row_cache")  # type: ignore
    hub = SubscriptionHub(backend, autostart=False)
    broker = SubscriptionBroker(backend, hub=hub)
    sub_id, _ = await broker.subscribe_get(filled_item_ref, admin_ctx, "time", 110)
    assert sub_id
    channel = cast(RowSubscription, broker._subs[sub_id]).channel
    assert await broker.get_updates(timeout=INTERVAL * 3) == {}  # 补读，读回一样不推

    row_id = await _set_qty(backend, filled_item_ref, 557)
    await wait_until(lambda: channel in hub.mq.pulled_set, timeout=3)
    await asyncio.sleep(INTERVAL * 2)
    assert channel in hub.mq.pulled_set, "没人调 get_updates 时不能被弹出"
    updates = await settled_updates(broker, timeout=3)
    assert updates[sub_id][row_id]["qty"] == 557
    await broker.close()
    await hub.close()


async def test_backend_close_closes_hub(mod_backend_config):
    """Backend.close() 先关掉挂在它上面的 hub：处理循环结束，backend 不再持有它"""
    backend = Backend(mod_backend_config)
    broker = SubscriptionBroker(backend)
    hub = broker._hub
    task = hub._task
    assert task is not None and not task.done()
    await backend.close()
    assert task.done()
    assert backend.sub_hub_ is None


async def test_loop_restarts_on_next_attach(
    hub: SubscriptionHub, filled_item_ref, admin_ctx
):
    """处理循环意外结束（这里直接取消它）后，下一次订阅会重新拉起，推送照常"""
    task = hub._task
    assert task is not None
    task.cancel()
    await asyncio.wait([task])
    broker = SubscriptionBroker(hub._backend, hub=hub)
    sub_id, _ = await broker.subscribe_get(filled_item_ref, admin_ctx, "time", 110)
    assert sub_id
    restarted = hub._task
    assert restarted is not None and restarted is not task and not restarted.done()
    row_id = await _set_qty(hub._backend, filled_item_ref, 558)
    updates = await settled_updates(broker, timeout=3)
    assert updates[sub_id][row_id]["qty"] == 558
    await broker.close()
