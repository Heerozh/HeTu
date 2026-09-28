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
from hetu.data.sub import (
    IndexSubscription,
    RowSubscription,
    SubscriptionBroker,
    SubscriptionHub,
    TableSubscription,
)

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


def _fail_get_many_once(stack: ExitStack, backend: Backend) -> list[int]:
    """各 servant 的 get_many（订阅读行）下一次调用抛读错误，之后照常；返回记失败次数的列表。
    事务读走 get_many_array_，不受影响"""
    failures: list[int] = []
    for servant in backend._servants:  # type: ignore[reportPrivateUsage]
        real = servant.get_many

        async def flaky(*args, _real=real, **kwargs):
            if not failures:
                failures.append(1)
                raise ConnectionError("read failed")
            return await _real(*args, **kwargs)

        stack.enter_context(patch.object(servant, "get_many", flaky))
    return failures


async def test_prefetch_failure_falls_back_to_row_reads(
    hub: SubscriptionHub, filled_item_ref, admin_ctx
):
    """预读行出错（Redis 抖动）：这张表不填缓存，订阅各自单行读，推送照常到达，连接不断（设计稿 §6）"""
    backend = hub._backend
    broker = SubscriptionBroker(backend, hub=hub)
    sub_id, _ = await broker.subscribe_get(filled_item_ref, admin_ctx, "time", 110)
    assert sub_id
    await asyncio.sleep(INTERVAL * 3)  # 订阅生效后的补读先消化掉
    with ExitStack() as stack:
        failures = _fail_get_many_once(stack, backend)
        row_id = await _set_qty(backend, filled_item_ref, 559)
        updates = await settled_updates(broker, timeout=3)
    assert failures == [1]
    assert updates[sub_id][row_id]["qty"] == 559
    await broker.close()


async def test_range_read_failure_keeps_state_for_retry(
    hub: SubscriptionHub, filled_item_ref, admin_ctx
):
    """范围订阅重跑比对时读新进入的行出错：比对的状态不能先改掉，重试要能再算出同样的进出，
    新行照常推到（以前出错就断连接、状态跟着丢，现在是重读，见设计稿 §6）"""
    backend = hub._backend
    comp = filled_item_ref.comp_cls
    broker = SubscriptionBroker(backend, hub=hub)
    sub_id, rows = await broker.subscribe_range(
        filled_item_ref, admin_ctx, "owner", 10, limit=30
    )
    assert sub_id and len(rows) == 25
    await asyncio.sleep(INTERVAL * 3)  # 订阅生效后的补读先消化掉
    with ExitStack() as stack:
        failures = _fail_get_many_once(stack, backend)
        async with backend.session("pytest", 1) as session:
            repo = session.using(comp)
            row = comp.new_row()
            row.name, row.owner, row.time = "New", 10, 999
            await repo.insert(row)
        new_id = int(row.id)
        updates = await settled_updates(broker, timeout=3)
    assert failures == [1]
    assert updates[sub_id][new_id]["name"] == "New"
    await broker.close()


def _poison_rows(stack: ExitStack, backend: Backend, bad_ids: set[int]) -> None:
    """
    订阅读行（各 servant 的 get / get_many）碰到 bad_ids 里的行就抛解码错误，别的行照常：
    模拟一行坏数据（比如 direct_set 不做类型检查写进去的值，TYPED_DICT 解码时 ValueError）
    """
    for servant in backend._servants:  # type: ignore[reportPrivateUsage]
        real_get, real_many = servant.get, servant.get_many

        async def get(ref, row_id, *args, _real=real_get, **kwargs):
            if int(row_id) in bad_ids:
                raise ValueError("could not convert string to float: 'full'")
            return await _real(ref, row_id, *args, **kwargs)

        async def get_many(ref, row_ids, *args, _real=real_many, **kwargs):
            row_ids = list(row_ids)
            if bad_ids.intersection(int(i) for i in row_ids):
                raise ValueError("could not convert string to float: 'full'")
            return await _real(ref, row_ids, *args, **kwargs)

        stack.enter_context(patch.object(servant, "get", get))
        stack.enter_context(patch.object(servant, "get_many", get_many))


async def test_unreadable_row_does_not_hold_back_its_batch(
    mod_auto_backend, filled_item_ref, admin_ctx
):
    """
    一行读不出来（坏数据解码失败）：只卡住订了它的订阅，与它同一批弹出的别的行照常推送。以前
    预读出错整批原样重新入队，同批的行跟着一次次重试，永远推不出去（设计稿 §6）
    """
    backend: Backend = mod_auto_backend("main")
    RowSubscription._RowSubscription__cache = ContextVar("user_row_cache")  # type: ignore
    hub = SubscriptionHub(
        backend, autostart=False
    )  # 手动驱动 tick，两条通知保证同一批弹出
    broker = SubscriptionBroker(backend, hub=hub)
    try:
        sub_bad, bad = await broker.subscribe_get(
            filled_item_ref, admin_ctx, "time", 112
        )
        sub_ok, ok = await broker.subscribe_get(filled_item_ref, admin_ctx, "time", 113)
        assert sub_bad and bad and sub_ok and ok
        assert await broker.get_updates(timeout=INTERVAL * 3) == {}  # 补读先消化掉
        async with backend.session("pytest", 1) as session:
            repo = session.using(filled_item_ref.comp_cls)
            for row_id, qty in ((bad["id"], 71), (ok["id"], 72)):
                row = await repo.get(id=row_id)
                assert row
                row.qty = qty
                await repo.update(row)
        mq = hub.mq
        channels = [
            cast(RowSubscription, broker._subs[sub_id]).channel
            for sub_id in (sub_bad, sub_ok)
        ]
        await wait_until(lambda: all(ch in mq.pulled_set for ch in channels), timeout=3)
        for i, (received_at, channel) in enumerate(mq.pulled_deque):
            mq.pulled_deque[i] = (received_at - INTERVAL, channel)  # 都已过合批窗口

        with ExitStack() as stack:
            _poison_rows(stack, backend, {int(bad["id"])})
            updates = await settled_updates(broker, timeout=3)
        assert updates.get(sub_ok, {}).get(ok["id"], {}).get("qty") == 72
        assert sub_bad not in updates
    finally:
        await broker.close()
        await hub.close()


class _DeadServant:
    """挂掉的副本：读都抛连接错误，其余（频道名等）照原来的副本"""

    def __init__(self, real):
        self._real = real

    def __getattr__(self, name):
        return getattr(self._real, name)

    async def get(self, *args, **kwargs):
        raise ConnectionError("replica down")

    async def get_many(self, *args, **kwargs):
        raise ConnectionError("replica down")

    async def range(self, *args, **kwargs):
        raise ConnectionError("replica down")


async def test_subscription_moves_off_a_dead_servant(
    hub: SubscriptionHub, filled_item_ref, admin_ctx
):
    """
    范围 / 整表订阅的读固定走订阅时选的副本：它长时间挂掉时，出错重试要换一个副本，不能每个
    interval 都打同一个死节点、永远恢复不了（以前出错就断开连接，客户端重连时会重新选副本）
    """
    backend = hub._backend
    comp = filled_item_ref.comp_cls
    broker = SubscriptionBroker(backend, hub=hub)
    idx_id, rows = await broker.subscribe_range(
        filled_item_ref, admin_ctx, "owner", 10, limit=30
    )
    tbl_id, _ = await broker.subscribe_table(filled_item_ref, admin_ctx)
    assert idx_id and tbl_id and len(rows) == 25
    await asyncio.sleep(INTERVAL * 3)  # 订阅生效后的补读先消化掉
    idx_sub = cast(IndexSubscription, broker._subs[idx_id])
    tbl_sub = cast(TableSubscription, broker._subs[tbl_id])
    dead = _DeadServant(idx_sub.servant)
    idx_sub.servant = tbl_sub.servant = dead  # type: ignore[assignment]
    for row_sub in idx_sub.row_subs.values():
        row_sub.servant = dead  # type: ignore[assignment]

    async with backend.session("pytest", 1) as session:
        row = comp.new_row()
        row.name, row.owner, row.time = "Survivor", 10, 777
        await session.using(comp).insert(row)
        new_id = int(row.id)
    updates = await settled_updates(broker, timeout=3)
    assert updates.get(idx_id, {}).get(new_id, {}).get("name") == "Survivor"
    assert updates.get(tbl_id, {}).get(new_id, {}).get("name") == "Survivor"
    await broker.close()
