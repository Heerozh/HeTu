"""
worker 级订阅器 SubscriptionHub：同一 worker 的连接共用一个 MQ 队列和处理循环（设计稿
docs/superpowers/specs/2026-09-28-worker-subscriptions-design.md）。连真实后端、按后端参数化；
协程交错的情形在 test_backend_sub_race.py 里用假 pubsub 测。
"""

import asyncio
import logging
from collections.abc import AsyncGenerator, Callable
from contextlib import ExitStack
from contextvars import ContextVar
from typing import cast
from unittest.mock import patch

import pytest
import redis
from fixtures.contexts import settled_updates, user_ctx_, wait_until

from hetu.common.snowflake_id import SnowflakeID
from hetu.data.backend import Backend, RowFormat
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
    """B 订阅生效后的补读只让 B 的订阅重读；A 订着同样的频道，不跟着重跑（设计稿 §4.4）。
    两个查询不同（limit）、结果与频道相同，不共享"""
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
            filled_item_ref, admin_ctx, "owner", 10, limit=31
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
    """挂掉的副本：读都抛连接错误（记下次数），其余（频道名等）照原来的副本"""

    def __init__(self, real):
        self._real = real
        self.reads = 0

    def __getattr__(self, name):
        return getattr(self._real, name)

    def _down(self):
        self.reads += 1
        return ConnectionError("replica down")

    async def get(self, *args, **kwargs):
        raise self._down()

    async def get_many(self, *args, **kwargs):
        raise self._down()

    async def range(self, *args, **kwargs):
        raise self._down()


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


async def test_initial_read_retries_on_another_servant(
    hub: SubscriptionHub, filled_item_ref, admin_ctx
):
    """
    订阅时选中的副本挂了：初始读出错不直接失败（以前连接当场断开，客户端重连、重订一遍），换一个
    随机副本重试，回复照常（设计稿 2026-09-29 §3.4）
    """
    backend = hub._backend
    real = backend.servant
    dead = _DeadServant(real)
    row_id = int((await real.range(filled_item_ref, "time", 110, limit=1))[0].id)
    broker = SubscriptionBroker(backend, hub=hub)
    with patch.object(backend, "_servants", [dead]):  # 前半段选中的是挂掉的副本
        finishes = [
            await broker.begin_subscribe_get(filled_item_ref, admin_ctx, "id", row_id),
            await broker.begin_subscribe_range(
                filled_item_ref, admin_ctx, "owner", 10, limit=30
            ),
            await broker.begin_subscribe_table(filled_item_ref, admin_ctx),
        ]
    async with asyncio.timeout(5):
        (get_id, row), (idx_id, rows), (tbl_id, tbl_rows) = [await f for f in finishes]
    assert get_id and row and row["id"] == row_id
    assert idx_id and len(rows) == 25
    assert tbl_id and len(tbl_rows) == 25
    assert dead.reads == 3
    for sub_id in (get_id, idx_id, tbl_id):
        assert cast(RowSubscription, broker._subs[sub_id]).servant is real
    await broker.close()


async def test_initial_read_gives_up_after_retries(
    hub: SubscriptionHub, filled_item_ref, admin_ctx
):
    """初始读一直出错：重试 INIT_RETRIES 次仍失败才抛给调用方（服务器里是断开连接），订阅撤干净"""
    backend = hub._backend
    dead = _DeadServant(backend.servant)
    hub.INIT_RETRIES = 2
    broker = SubscriptionBroker(backend, hub=hub)
    with (
        patch.object(backend, "_servants", [dead]),
        pytest.raises(ConnectionError, match="replica down"),
    ):
        async with asyncio.timeout(5):
            await broker.subscribe_range(filled_item_ref, admin_ctx, "owner", 10)
    assert dead.reads == 3
    assert broker.count() == (0, 0, 0) and not broker._subs
    assert not hub._channel_subs and not hub.mq.subscribed_channels
    await broker.close()


@pytest.mark.parametrize("backend_name", ["redis", "valkey"], indirect=True)
async def test_hub_moves_subscriptions_off_a_servant_whose_pubsub_dropped(
    mod_backend_config, filled_item_ref, admin_ctx
):
    """
    worker 级订阅器的订阅通知分到各副本订阅（这里把主库也当一个副本）。某个副本的 pubsub 连接断了
    （这里 CLIENT KILL 掉主库上的 pubsub 连接）：它上面的频道换到另一个副本订阅、补读，推送照常。
    以前订阅器建的时候随机绑一个副本、终身不换，它断了整个 worker 的推送都停
    """
    config = {
        **mod_backend_config,
        "servants": [*mod_backend_config["servants"], mod_backend_config["master"]],
    }
    backend = Backend(config)
    backend.post_configure([filled_item_ref.comp_cls])
    replica, master = backend._servants  # type: ignore[reportPrivateUsage]
    hub = SubscriptionHub(backend)
    broker = SubscriptionBroker(backend, hub=hub)
    sub_id, rows = await broker.subscribe_range(
        filled_item_ref, admin_ctx, "owner", 10, limit=30
    )
    assert sub_id and len(rows) == 25
    await asyncio.sleep(INTERVAL * 3)  # 订阅生效后的补读先消化掉
    routes = cast(HubMQClient, hub.mq)._route  # type: ignore[reportPrivateUsage]
    on_master = {ch for ch, h in routes.items() if h is master._hub}  # type: ignore[attr-defined]
    assert on_master and set(routes) - on_master, "订阅没有分到两个副本上"

    redis.Redis.from_url(config["master"]).client_kill_filter(_type="pubsub")
    await wait_until(
        lambda: all(routes.get(ch) is replica._hub for ch in on_master),  # type: ignore[attr-defined]
        timeout=5,
    )
    comp = filled_item_ref.comp_cls
    async with backend.session("pytest", 1) as session:
        row = comp.new_row()
        row.name, row.owner, row.time = "Survivor", 10, 779
        await session.using(comp).insert(row)
        new_id = int(row.id)
    updates = await settled_updates(broker, timeout=3)
    assert updates.get(sub_id, {}).get(new_id, {}).get("name") == "Survivor"
    await broker.close()
    await hub.close()
    await backend.close()


# ============ 同一查询在 worker 内共享一个订阅（设计稿 2026-09-29） ============


def _read_counter(stack: ExitStack, backend: Backend) -> Callable[[], dict[str, int]]:
    """按方法数订阅的读（各 servant 的 range / get / get_many）。事务读走 get_many_array_ /
    range_read_，不计"""
    mocks: dict[str, list] = {"range": [], "get": [], "get_many": []}
    for servant in backend._servants:  # type: ignore[reportPrivateUsage]
        for meth, spies in mocks.items():
            spies.append(
                stack.enter_context(
                    patch.object(servant, meth, wraps=getattr(servant, meth))
                )
            )
    return lambda: {meth: sum(m.call_count for m in ms) for meth, ms in mocks.items()}


async def _insert_item(backend: Backend, ref, **fields) -> int:
    comp = ref.comp_cls
    async with backend.session("pytest", 1) as session:
        row = comp.new_row()
        for name, value in fields.items():
            setattr(row, name, value)
        await session.using(comp).insert(row)
    return int(row.id)


async def _updates_until(
    broker: SubscriptionBroker, check: Callable[[dict], object], timeout: float = 5
) -> dict[str, dict]:
    """反复取推送、按 sub_id / row_id 合并，直到 check(合并结果) 为真"""
    merged: dict[str, dict] = {}
    async with asyncio.timeout(timeout):
        while not check(merged):
            for sub_id, rows in (await broker.get_updates()).items():
                merged.setdefault(sub_id, {}).update(rows)
    return merged


async def test_same_query_is_one_subscription_per_worker(
    hub: SubscriptionHub, filled_item_ref, admin_ctx
):
    """
    N 个连接订同一查询：worker 里只有一个订阅对象，初始读一次；之后每条消息在 worker 内的读、比对
    也只做一次，与连接数无关，各连接都收到推送（以前每个连接一个订阅对象，各读各比）
    """
    backend = hub._backend
    brokers = [SubscriptionBroker(backend, hub=hub) for _ in range(5)]
    with ExitStack() as stack:
        reads = _read_counter(stack, backend)
        replies = [
            await b.subscribe_range(filled_item_ref, admin_ctx, "owner", 10, limit=30)
            for b in brokers
        ]
        assert reads()["range"] == 1, reads()
    (sub_id,) = {sub_id for sub_id, _ in replies}
    assert sub_id
    assert len({id(b._subs[sub_id]) for b in brokers}) == 1
    assert all(len(rows) == 25 and rows == replies[0][1] for _, rows in replies)
    await asyncio.sleep(INTERVAL * 3)  # 订阅生效后的补读先消化掉

    with ExitStack() as stack:
        reads = _read_counter(stack, backend)
        new_id = await _insert_item(
            backend, filled_item_ref, name="New", owner=10, time=900
        )
        for broker in brokers:
            updates = await settled_updates(broker, timeout=3)
            assert updates[sub_id][new_id]["name"] == "New"
        counts = reads()
    # ZRANGE（合并进队头的话再尾随重读一次）、读新行、新行频道订上后补读：与连接数无关
    assert counts["range"] <= 2 and counts["get"] + counts["get_many"] <= 3, counts
    for broker in brokers:
        await broker.close()


async def test_late_joiner_gets_what_members_hold_without_reading(
    hub: SubscriptionHub, filled_item_ref, admin_ctx
):
    """
    后加入的连接：不读库、不补读，回复就是已有成员客户端手里的内容（它的回复 + 之后收到的推送），
    按索引顺序排
    """
    backend = hub._backend
    comp = filled_item_ref.comp_cls
    a = SubscriptionBroker(backend, hub=hub)
    sub_id, rows = await a.subscribe_range(
        filled_item_ref, admin_ctx, "owner", 10, limit=30
    )
    assert sub_id
    held = {row["id"]: row for row in rows}
    await asyncio.sleep(INTERVAL * 3)  # 订阅生效后的补读先消化掉

    # 已有成员手里的内容变了：改一行、删一行、进来一行
    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        changed = await repo.get(time=111)
        gone = await repo.get(time=112)
        assert changed and gone
        changed.qty = 1
        await repo.update(changed)
        repo.delete(gone.id)
        new = comp.new_row()
        new.name, new.owner, new.time = "New", 10, 901
        await repo.insert(new)
    changed_id, gone_id, new_id = int(changed.id), int(gone.id), int(new.id)

    def all_seen(merged):
        got = merged.get(sub_id, {})
        return (
            got.get(changed_id, {}).get("qty") == 1
            and gone_id in got
            and got[gone_id] is None
            and new_id in got
        )

    for row_id, row in (await _updates_until(a, all_seen))[sub_id].items():
        if row is None:
            held.pop(row_id, None)
        else:
            held[row_id] = row

    with ExitStack() as stack:
        reads = _read_counter(stack, backend)
        b = SubscriptionBroker(backend, hub=hub)
        sub_b, rows_b = await b.subscribe_range(
            filled_item_ref, admin_ctx, "owner", 10, limit=30
        )
        assert reads() == {"range": 0, "get": 0, "get_many": 0}
    assert sub_b == sub_id
    assert {row["id"]: row for row in rows_b} == held
    order = await backend.servant.range(
        filled_item_ref, "owner", 10, limit=30, row_format=RowFormat.ID_LIST
    )
    assert [row["id"] for row in rows_b] == order, "快照没按索引顺序排"
    assert await b.get_updates(timeout=INTERVAL * 3) == {}, "加入者不该有补读推送"
    await a.close()
    await b.close()


async def test_snapshot_order_follows_index_changes(
    hub: SubscriptionHub, filled_item_ref, admin_ctx
):
    """
    后加入者的回复按最近一次 ZRANGE 的顺序排，排好的列表缓存到快照下次变动（后加入者共用）：索引
    字段变了、行在结果里挪了位置，之后加入的连接拿到新顺序
    """
    backend = hub._backend
    comp = filled_item_ref.comp_cls
    a, b, c = (SubscriptionBroker(backend, hub=hub) for _ in range(3))
    query = ("time", 100, 200, 30)
    sub_id, rows = await a.subscribe_range(filled_item_ref, admin_ctx, *query)
    assert sub_id and len(rows) == 25
    await asyncio.sleep(INTERVAL * 3)  # 订阅生效后的补读先消化掉
    assert await b.subscribe_range(filled_item_ref, admin_ctx, *query) == (
        sub_id,
        rows,
    )
    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        first = await repo.get(time=110)
        assert first
        first.time = 150
        await repo.update(first)
    moved = int(first.id)
    sub = cast(IndexSubscription, a._subs[sub_id])
    await wait_until(lambda: sub.order[-1] == moved, timeout=3)
    sub_c, rows_c = await c.subscribe_range(filled_item_ref, admin_ctx, *query)
    order = await backend.servant.range(
        filled_item_ref, *query, row_format=RowFormat.ID_LIST
    )
    assert sub_c == sub_id and [row["id"] for row in rows_c] == order
    assert rows_c[-1]["time"] == 150
    for broker in (a, b, c):
        await broker.close()


async def test_joiners_during_init_share_one_read(
    hub: SubscriptionHub, filled_item_ref, admin_ctx
):
    """几个连接同时订同一查询（初始化期间陆续加入）：初始读只有一次，回复都是就绪时的快照，之后的
    更新都收得到"""
    backend = hub._backend
    brokers = [SubscriptionBroker(backend, hub=hub) for _ in range(4)]
    with ExitStack() as stack:
        reads = _read_counter(stack, backend)
        # 前半段都做完（登记）才让出事件循环：初始化还没开始跑，后三个都在初始化期间加入
        finishes = [
            await b.begin_subscribe_range(
                filled_item_ref, admin_ctx, "owner", 10, limit=30
            )
            for b in brokers
        ]
        async with asyncio.timeout(5):
            replies = await asyncio.gather(*finishes)
        assert reads()["range"] == 1, reads()
    (sub_id,) = {sub_id for sub_id, _ in replies}
    assert sub_id
    assert all(len(rows) == 25 and rows == replies[0][1] for _, rows in replies)
    new_id = await _insert_item(
        backend, filled_item_ref, name="New", owner=10, time=902
    )
    for broker in brokers:
        updates = await settled_updates(broker, timeout=3)
        assert updates[sub_id][new_id]["name"] == "New"
        await broker.close()


async def test_shared_subscription_not_established_answers_none_to_all(
    hub: SubscriptionHub, filled_item_ref, admin_ctx
):
    """
    订阅不成立：订一行不存在的行、整表超过行数上限，初始化期间加入的几个连接都回 None，不留订阅、
    不留共享登记与频道；后加入的连接按自己的上限判断（表已超过它的上限就不加入）
    """
    backend = hub._backend
    brokers = [
        SubscriptionBroker(backend, hub=hub, max_table_rows=10) for _ in range(3)
    ]
    finishes = [
        await b.begin_subscribe_get(filled_item_ref, admin_ctx, "id", 987654321)
        for b in brokers
    ]
    assert await asyncio.gather(*finishes) == [(None, None)] * 3
    finishes = [
        await b.begin_subscribe_table(filled_item_ref, admin_ctx) for b in brokers
    ]
    assert await asyncio.gather(*finishes) == [(None, [])] * 3
    assert all(b.count() == (0, 0, 0) and not b._subs for b in brokers)
    assert not hub._shared and not hub._channel_subs
    assert not hub.mq.subscribed_channels

    roomy = SubscriptionBroker(backend, hub=hub)
    tbl_id, rows = await roomy.subscribe_table(filled_item_ref, admin_ctx)
    assert tbl_id and len(rows) == 25
    assert await brokers[0].subscribe_table(filled_item_ref, admin_ctx) == (None, [])
    assert brokers[0].count() == (0, 0, 0)
    assert set(roomy._subs[tbl_id].members) == {roomy}
    await roomy.close()
    for broker in brokers:
        await broker.close()


async def test_force_false_member_does_not_join_empty_shared_range(
    hub: SubscriptionHub, filled_item_ref, admin_ctx
):
    """range 的 force=False：共享订阅的快照为空时这个连接不加入，回 (None, [])；已有成员照旧"""
    backend = hub._backend
    a = SubscriptionBroker(backend, hub=hub)
    b = SubscriptionBroker(backend, hub=hub)
    sub_id, rows = await a.subscribe_range(filled_item_ref, admin_ctx, "owner", 99)
    assert sub_id and rows == []
    assert await b.subscribe_range(
        filled_item_ref, admin_ctx, "owner", 99, force=False
    ) == (None, [])
    assert b.count() == (0, 0, 0) and not b._subs
    assert set(a._subs[sub_id].members) == {a}
    await a.close()
    await b.close()


async def test_force_false_joiner_rechecks_a_stale_empty_snapshot(
    hub: SubscriptionHub, filled_item_ref, admin_ctx
):
    """
    共享范围订阅的快照为空，但刚有一行进入范围、通知还没处理（快照落后于提交至少一个 interval）：
    force=False 的后加入者不能按快照回"没有"，要等订阅重读一次索引频道再判，这时有行，加入并回这行。
    以前据快照回 (None, [])、不加入，随后推来的这行它再也收不到（dev 在订阅时读库）
    """
    backend = hub._backend
    a = SubscriptionBroker(backend, hub=hub)
    b = SubscriptionBroker(backend, hub=hub)
    sub_id, rows = await a.subscribe_range(filled_item_ref, admin_ctx, "owner", 98)
    assert sub_id and rows == []
    await asyncio.sleep(INTERVAL * 3)  # 订阅生效后的补读先消化掉
    new_id = await _insert_item(
        backend, filled_item_ref, name="Late", owner=98, time=903
    )
    sub_b, rows_b = await b.subscribe_range(
        filled_item_ref, admin_ctx, "owner", 98, force=False
    )
    assert sub_b == sub_id and [row["id"] for row in rows_b] == [new_id]
    assert set(a._subs[sub_id].members) == {a, b}
    await a.close()
    await b.close()


async def test_force_false_joiner_is_not_answered_from_parked_snapshot(
    hub: SubscriptionHub, filled_item_ref, admin_ctx
):
    """
    共享范围订阅的成员推送卡着（待发区没取走）：新行进入范围的通知攒着没读，快照一直是空的。force=False
    的后加入者要等订阅重读再判：加入它就不再"全都卡着"，重读照常进行，它拿到这行
    """
    backend = hub._backend
    a = SubscriptionBroker(backend, hub=hub)
    b = SubscriptionBroker(backend, hub=hub)
    sub_id, rows = await a.subscribe_range(filled_item_ref, admin_ctx, "owner", 97)
    row_id = int((await backend.servant.range(filled_item_ref, "time", 110))[0].id)
    get_id, _row = await a.subscribe_get(filled_item_ref, admin_ctx, "id", row_id)
    assert sub_id and rows == [] and get_id
    await asyncio.sleep(INTERVAL * 3)  # 订阅生效后的补读先消化掉
    await _set_qty(backend, filled_item_ref, 571)
    await wait_until(lambda: a._outbox, timeout=3)  # a 卡着，不来取
    sub = a._subs[sub_id]
    new_id = await _insert_item(
        backend, filled_item_ref, name="Parked", owner=97, time=904
    )
    await wait_until(lambda: sub in hub._parked, timeout=3)
    sub_b, rows_b = await b.subscribe_range(
        filled_item_ref, admin_ctx, "owner", 97, force=False
    )
    assert sub_b == sub_id and [row["id"] for row in rows_b] == [new_id]
    await a.close()
    await b.close()


async def test_get_joiner_rechecks_a_stale_empty_snapshot(
    hub: SubscriptionHub, filled_item_ref, admin_ctx
):
    """
    共享行订阅的行被删过（快照为空，成员还在），随后同一个 id 又插回来、通知还没处理：后加入者要等订阅
    重读一次行频道再判，这时行在，加入并回这行（以前据快照回 None）；重复订阅也一样，不能据快照撤掉
    已有的订阅
    """
    backend = hub._backend
    comp = filled_item_ref.comp_cls
    a = SubscriptionBroker(backend, hub=hub)
    b = SubscriptionBroker(backend, hub=hub)
    row_id = await _insert_item(
        backend, filled_item_ref, name="Back", owner=96, time=905
    )
    sub_id, row = await a.subscribe_get(filled_item_ref, admin_ctx, "id", row_id)
    assert sub_id and row

    async def delete_then_insert_back():
        """删掉这行、等 a 收到 None（快照清空），再用同一个 id 插回同样的内容"""
        async with backend.session("pytest", 1) as session:
            repo = session.using(comp)
            assert await repo.get(id=row_id)
            repo.delete(row_id)
        await _updates_until(a, lambda m: m.get(sub_id, {}).get(row_id, 0) is None)
        assert a._subs[sub_id].snapshot == {}
        async with backend.session("pytest", 1) as session:
            again = comp.new_row(id_=row_id)
            again.name, again.owner, again.time = "Back", 96, 905
            await session.using(comp).insert(again)

    await delete_then_insert_back()
    assert await a.subscribe_get(filled_item_ref, admin_ctx, "id", row_id) == (
        sub_id,
        row,
    ), "重复订阅据落后的快照撤掉了已有的订阅"
    await delete_then_insert_back()
    assert await b.subscribe_get(filled_item_ref, admin_ctx, "id", row_id) == (
        sub_id,
        row,
    )
    assert set(a._subs[sub_id].members) == {a, b}
    await a.close()
    await b.close()


async def test_duplicate_of_shared_subscription_answers_snapshot(
    hub: SubscriptionHub, filled_item_ref, admin_ctx
):
    """同一连接重复订阅共享的订阅：回快照（订阅已推给客户端的内容），不读库，不重复计数"""
    backend = hub._backend
    row_id = int((await backend.servant.range(filled_item_ref, "time", 110))[0].id)
    a = SubscriptionBroker(backend, hub=hub)
    get_id, row = await a.subscribe_get(filled_item_ref, admin_ctx, "id", row_id)
    idx_id, rows = await a.subscribe_range(
        filled_item_ref, admin_ctx, "owner", 10, limit=30
    )
    tbl_id, tbl_rows = await a.subscribe_table(filled_item_ref, admin_ctx)
    assert get_id and idx_id and tbl_id
    await asyncio.sleep(INTERVAL * 3)  # 订阅生效后的补读先消化掉
    with ExitStack() as stack:
        reads = _read_counter(stack, backend)
        assert await a.subscribe_get(filled_item_ref, admin_ctx, "id", row_id) == (
            get_id,
            row,
        )
        assert await a.subscribe_range(
            filled_item_ref, admin_ctx, "owner", 10, limit=30
        ) == (idx_id, rows)
        assert await a.subscribe_table(filled_item_ref, admin_ctx) == (
            tbl_id,
            tbl_rows,
        )
        assert reads() == {"range": 0, "get": 0, "get_many": 0}
    assert a.count() == (1, 1, 1)
    await a.close()


async def test_rls_subscriptions_are_not_shared(
    hub: SubscriptionHub, filled_rls_ref, admin_ctx, user_id11_ctx
):
    """RLS 组件的订阅按连接私有，各自按自己的 ctx 判可见；admin 的订阅不按行判定，照样共享"""
    backend = hub._backend
    u11, u10, a1, a2 = (SubscriptionBroker(backend, hub=hub) for _ in range(4))
    s11, rows11 = await u11.subscribe_range(
        filled_rls_ref, user_id11_ctx, "owner", 10, limit=30
    )
    s10, rows10 = await u10.subscribe_range(
        filled_rls_ref, user_ctx_(10), "owner", 10, limit=30
    )
    assert s11 and s10 and s11 == s10
    assert len(rows11) == 25 and rows10 == []  # 行的 friend 都是 11
    assert u11._subs[s11] is not u10._subs[s10]
    sa, rows_a = await a1.subscribe_range(
        filled_rls_ref, admin_ctx, "owner", 10, limit=30
    )
    sb, _ = await a2.subscribe_range(filled_rls_ref, admin_ctx, "owner", 10, limit=30)
    assert sa and sb and len(rows_a) == 25
    assert a1._subs[sa] is a2._subs[sb]
    assert a1._subs[sa] is not u11._subs[s11]
    for broker in (u11, u10, a1, a2):
        await broker.close()


async def test_share_key_tells_queries_apart(
    hub: SubscriptionHub, filled_item_ref, admin_ctx
):
    """
    共享键按查询本身，不按 sub_id：right=None（点查询）与字符串 "None" 拼出同一个 sub_id，却是两个
    查询，不能共享；1 与 1.0 分开（只是少共享些）；desc=1 与 True 是同一个查询
    """
    backend = hub._backend
    brokers = [SubscriptionBroker(backend, hub=hub) for _ in range(6)]
    a, b, c, d, e, f = brokers
    sa, rows_a = await a.subscribe_range(
        filled_item_ref, admin_ctx, "name", "Itm10", limit=30
    )
    sb, rows_b = await b.subscribe_range(
        filled_item_ref, admin_ctx, "name", "Itm10", "None", limit=30
    )
    assert sa and sb and sa == sb  # 都拼成 [Itm10:None:1]
    assert a._subs[sa] is not b._subs[sb]
    assert len(rows_a) == 1 and len(rows_b) == 25

    # 客户端传来的 desc 可能是 1
    sc, _ = await c.subscribe_range(
        filled_item_ref,
        admin_ctx,
        "owner",
        10,
        desc=1,  # type: ignore[arg-type]
    )
    sd, _ = await d.subscribe_range(filled_item_ref, admin_ctx, "owner", 10, desc=True)
    assert sc and sd and sc == sd
    assert c._subs[sc] is d._subs[sd]

    se, _ = await e.subscribe_range(filled_item_ref, admin_ctx, "owner", 10)
    sf, _ = await f.subscribe_range(filled_item_ref, admin_ctx, "owner", 10.0)
    assert se and sf and se != sf
    assert e._subs[se] is not f._subs[sf]
    for broker in brokers:
        await broker.close()


async def test_shared_init_failure_fails_every_waiter(
    hub: SubscriptionHub, filled_item_ref, admin_ctx
):
    """共享订阅初始化重试用尽：初始化期间加入的成员都失败（服务器里各自断开），订阅、登记撤干净"""
    backend = hub._backend
    hub.INIT_RETRIES = 1
    dead = _DeadServant(backend.servant)
    brokers = [SubscriptionBroker(backend, hub=hub) for _ in range(3)]
    with patch.object(backend, "_servants", [dead]):
        finishes = [
            await b.begin_subscribe_range(
                filled_item_ref, admin_ctx, "owner", 10, limit=30
            )
            for b in brokers
        ]
        async with asyncio.timeout(5):
            results = await asyncio.gather(*finishes, return_exceptions=True)
    assert all(isinstance(r, ConnectionError) for r in results), results
    assert dead.reads == 2  # 一个订阅：初始读 + 重试 1 次
    assert all(b.count() == (0, 0, 0) for b in brokers)
    assert not hub._shared and not hub._channel_subs
    for broker in brokers:
        await broker.close()


@pytest.mark.parametrize("limit", ["abc", None, 1.5, True])
async def test_malformed_range_limit_fails_in_first_half(
    hub: SubscriptionHub, filled_item_ref, admin_ctx, limit
):
    """
    range 的 limit 不是整数（客户端乱传）：前半段当场报错（服务器里断开连接），不登记、不读库。以前
    前半段不校验 limit，登记后在后台读 4 次（初始读 + 重试 3 次，约 0.7 秒）才失败，还记着"换副本
    重试"的错误日志，这期间这个连接后面的回复都在等它
    """
    backend = hub._backend
    broker = SubscriptionBroker(backend, hub=hub)
    with ExitStack() as stack:
        reads = _read_counter(stack, backend)
        with pytest.raises((TypeError, ValueError)):
            await broker.begin_subscribe_range(
                filled_item_ref, admin_ctx, "owner", 10, None, limit
            )
        assert reads()["range"] == 0
    assert broker.count() == (0, 0, 0) and not broker._subs
    await broker.close()


async def test_table_subscriptions_with_different_caps_are_not_shared(
    hub: SubscriptionHub, filled_item_ref, admin_ctx
):
    """
    整表订阅的行数上限（max_table_rows）也是查询的一部分：共享的整表订阅按建它的连接的上限读初始行、
    做 RESYNC。以前小上限的连接先建、大上限的在初始化期间加入，两个都回 None（表超过了小上限），
    大上限的本该拿到全表；之后 RESYNC 也只读到小上限那么多行
    """
    backend = hub._backend
    small = SubscriptionBroker(backend, hub=hub, max_table_rows=10)
    big = SubscriptionBroker(backend, hub=hub, max_table_rows=100_000)
    finishes = [
        await small.begin_subscribe_table(filled_item_ref, admin_ctx),
        await big.begin_subscribe_table(filled_item_ref, admin_ctx),
    ]
    async with asyncio.timeout(5):
        (small_id, small_rows), (big_id, big_rows) = await asyncio.gather(*finishes)
    assert small_id is None and small_rows == []
    assert big_id and len(big_rows) == 25
    await small.close()
    await big.close()


class _LaggingServant:
    """落后的副本：行还没复制过来（get 读回 None），其余照原来的副本"""

    def __init__(self, real):
        self._real = real

    def __getattr__(self, name):
        return getattr(self._real, name)

    async def get(self, *args, **kwargs):
        return None


async def test_private_duplicate_waits_for_the_first_init(
    hub: SubscriptionHub, filled_rls_ref, user_id11_ctx
):
    """
    RLS 私有订阅的重复订阅，在第一次订阅还没初始化完时就到了（客户端一帧里 WatchRow 两次）：要等
    第一次的结果。第一次读到落后的副本、行不存在，订阅不成立、撤掉，重复订阅也回 None。以前重复
    订阅不等，换个副本读到了行，回复带着 sub_id，服务端却什么也没登记，客户端再也收不到这行的推送
    """
    backend = hub._backend
    real = backend.servant
    row = (
        await real.range(
            filled_rls_ref, "owner", 10, limit=1, row_format=RowFormat.TYPED_DICT
        )
    )[0]
    broker = SubscriptionBroker(backend, hub=hub)
    with patch.object(backend, "_servants", [_LaggingServant(real)]):
        first = await broker.begin_subscribe_get(
            filled_rls_ref, user_id11_ctx, "id", row["id"]
        )
    dup = await broker.begin_subscribe_get(
        filled_rls_ref, user_id11_ctx, "id", row["id"]
    )
    async with asyncio.timeout(5):
        assert await asyncio.gather(first, dup) == [(None, None), (None, None)]
    assert broker.count() == (0, 0, 0) and not broker._subs
    await broker.close()


async def test_duplicate_started_after_subscription_closed_does_not_hang(
    hub: SubscriptionHub, filled_item_ref, admin_ctx
):
    """
    重复订阅的后半段在已有订阅关闭之后才开始跑（初始化还没完成，客户端就 unsub 了）：回 None，不能
    一直等下去（初始化已经取消，没人会再交代它）
    """
    broker = SubscriptionBroker(hub._backend, hub=hub)
    first = await broker.begin_subscribe_range(
        filled_item_ref, admin_ctx, "owner", 10, limit=30
    )
    dup = await broker.begin_subscribe_range(
        filled_item_ref, admin_ctx, "owner", 10, limit=30
    )
    (sub_id,) = broker._subs
    await broker.unsubscribe(sub_id)
    async with asyncio.timeout(5):
        assert await first == (None, [])
        assert await dup == (None, [])
    await broker.close()


# ==== 栅栏（等推送的 RPC，设计稿 docs/superpowers/specs/2026-10-10-rpcs-sync-design.md）====


def test_fence_delay_covers_notify_path():
    """栅栏入队前的余量要盖住"commit 返回 → 通知进本地队列"：SQLite 每 interval/2 轮询一次通知表"""
    from hetu.data.backend.sqlite.mq import SQLiteMQClient

    assert MQClient.FENCE_DELAY == 0.02
    assert SQLiteMQClient.FENCE_DELAY > 0.5 * INTERVAL


async def _subscribed_row(
    hub: SubscriptionHub, ref, ctx
) -> tuple[SubscriptionBroker, str, str]:
    """新门面订 time=110 那行，返回 (门面, sub_id, 行频道)"""
    broker = SubscriptionBroker(hub._backend, hub=hub)
    sub_id, _ = await broker.subscribe_get(ref, ctx, "time", 110)
    assert sub_id
    channel = cast(RowSubscription, broker._subs[sub_id]).channel
    return broker, sub_id, channel


class _ListHandler(logging.Handler):
    """直接挂在 HeTu.root 上收日志：起服类测试会把它的 propagate 关掉，caplog 收不到"""

    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


async def test_fence_fires_after_earlier_notifications_are_delivered(
    hub: SubscriptionHub, filled_item_ref, admin_ctx
):
    """栅栏触发时，它入队之前已进队列的通知都已处理完、推送已交到待发区"""
    broker, sub_id, channel = await _subscribed_row(hub, filled_item_ref, admin_ctx)
    await asyncio.sleep(INTERVAL * 3)  # 订阅生效后的补读先消化掉
    assert not broker.has_updates_()
    row_id = await _set_qty(hub._backend, filled_item_ref, 777)
    # 这次写入的通知进了 hub 的队列之后再放栅栏：栅栏只管它之前到达的
    await wait_until(lambda: channel in hub.mq.pulled_set)
    seen: list[dict] = []

    def on_fire() -> None:
        seen.append({sid: dict(rows) for sid, rows in broker._outbox.items()})

    hub.fence_(on_fire)
    await wait_until(lambda: seen, timeout=3)
    assert seen[0][sub_id][row_id]["qty"] == 777
    await broker.close()


async def test_fence_alone_fires_after_delay_and_one_interval(hub: SubscriptionHub):
    """只有栅栏：隔 FENCE_DELAY 入队、满一个 interval 弹出之后才触发，不会更早"""
    loop = asyncio.get_running_loop()
    fired_at: list[float] = []
    start = loop.time()
    hub.fence_(lambda: fired_at.append(loop.time()))
    await wait_until(lambda: fired_at, timeout=3)
    elapsed = fired_at[0] - start
    expected = hub.mq.FENCE_DELAY + INTERVAL
    # 定时器可能按时钟精度早到一点
    assert expected - 0.02 <= elapsed < expected + 1


async def test_fence_does_not_wait_for_later_notifications(
    hub: SubscriptionHub, filled_item_ref, admin_ctx
):
    """栅栏只管它入队之前到达的通知：之后才到的，可以在它触发之后才处理"""
    broker, sub_id, _channel = await _subscribed_row(hub, filled_item_ref, admin_ctx)
    await asyncio.sleep(INTERVAL * 3)
    seen: list[dict] = []
    with patch.object(hub.mq, "FENCE_DELAY", 0):
        hub.fence_(lambda: seen.append(dict(broker._outbox)))
    await wait_until(lambda: hub.mq.pulled_deque)  # 栅栏键入队了
    await asyncio.sleep(INTERVAL / 2)
    row_id = await _set_qty(hub._backend, filled_item_ref, 888)
    await wait_until(lambda: seen, timeout=3)
    assert sub_id not in seen[0]
    updates = await settled_updates(broker, timeout=3)
    assert updates[sub_id][row_id]["qty"] == 888
    await broker.close()


async def test_fence_key_mixed_with_notifications_and_directed_rereads(
    mod_auto_backend, filled_item_ref, admin_ctx
):
    """栅栏键和真实通知、定向补读同一批弹出：各自照常处理，栅栏在本批交付之后触发"""
    hub = SubscriptionHub(mod_auto_backend("main"), autostart=False)
    broker, sub_id, channel = await _subscribed_row(hub, filled_item_ref, admin_ctx)
    row_id = await _set_qty(hub._backend, filled_item_ref, 555)
    await wait_until(lambda: channel in hub.mq.pulled_set)
    hub.reread_for(broker._subs[sub_id], channel)
    queued = len(hub.mq.pulled_deque)
    seen: list[dict] = []
    with patch.object(hub.mq, "FENCE_DELAY", 0):
        hub.fence_(lambda: seen.append(dict(broker._outbox)))
    await wait_until(lambda: len(hub.mq.pulled_deque) == queued + 1)
    await asyncio.sleep(INTERVAL * 1.5)  # 都满期：下面一次弹出同一批
    loop = asyncio.get_running_loop()
    assert await hub.step_(loop.time() + 3, lambda: False)
    assert seen and seen[0][sub_id][row_id]["qty"] == 555
    await broker.close()
    await hub.close()


async def test_fence_fires_when_the_tick_fails(
    mod_auto_backend, filled_item_ref, admin_ctx
):
    """栅栏和别的通知同一批弹出、这个 tick 处理出错（bug）：栅栏照常触发"""
    hub = SubscriptionHub(mod_auto_backend("main"), autostart=False)
    broker, _sub_id, channel = await _subscribed_row(hub, filled_item_ref, admin_ctx)
    await _set_qty(hub._backend, filled_item_ref, 444)
    await wait_until(lambda: channel in hub.mq.pulled_set)
    queued = len(hub.mq.pulled_deque)
    fired: list[int] = []
    with patch.object(hub.mq, "FENCE_DELAY", 0):
        hub.fence_(lambda: fired.append(1))
    await wait_until(lambda: len(hub.mq.pulled_deque) == queued + 1)
    await asyncio.sleep(INTERVAL * 1.5)
    loop = asyncio.get_running_loop()
    with (
        patch.object(hub, "_prefetch_rows", side_effect=RuntimeError("boom")),
        pytest.raises(RuntimeError, match="boom"),
    ):
        await hub.step_(loop.time() + 3, lambda: False)
    assert fired == [1]
    await broker.close()
    await hub.close()


async def test_fence_callback_error_does_not_break_others(hub: SubscriptionHub):
    """一个栅栏的回调抛异常：记日志，同批别的栅栏、之后的 tick 照常"""
    handler = _ListHandler()
    logging.getLogger("HeTu.root").addHandler(handler)
    try:

        def broken() -> None:
            raise RuntimeError("fence callback boom")

        fired: list[int] = []
        hub.fence_(broken)
        hub.fence_(lambda: fired.append(1))
        await wait_until(lambda: fired, timeout=3)
        hub.fence_(lambda: fired.append(2))
        await wait_until(lambda: len(fired) == 2, timeout=3)
    finally:
        logging.getLogger("HeTu.root").removeHandler(handler)
    assert any(
        r.exc_info and "fence callback boom" in str(r.exc_info[1])
        for r in handler.records
    )


async def test_fence_fires_on_hub_close(mod_auto_backend):
    """hub 关闭时还没触发的栅栏都触发：等着 sync 的连接不会一直等"""
    hub = SubscriptionHub(mod_auto_backend("main"))
    fired: list[int] = []
    hub.fence_(lambda: fired.append(1))
    await hub.close()
    await asyncio.sleep(0)
    assert fired == [1]
    # 关闭之后再放的栅栏也会触发（不会一直不来）
    hub.fence_(lambda: fired.append(2))
    await wait_until(lambda: len(fired) == 2, timeout=1)


async def test_fence_safety_timer(hub: SubscriptionHub):
    """栅栏键迟迟没被处理（这里让它 60 秒后才入队）：保险定时器到时照样触发，且只触发一次"""
    fired: list[int] = []
    with (
        patch.object(hub.mq, "FENCE_DELAY", 60),
        patch.object(hub, "FENCE_TIMEOUT_INTERVALS", 2),
    ):
        hub.fence_(lambda: fired.append(1))
    await wait_until(lambda: fired, timeout=1)
    await asyncio.sleep(INTERVAL * 3)
    assert fired == [1]


async def test_fence_restarts_a_stopped_loop(hub: SubscriptionHub):
    """处理循环停了（被取消）：fence_ 把它重新拉起，不靠保险定时器"""
    task = hub._task
    assert task is not None
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    fired: list[int] = []
    hub.fence_(lambda: fired.append(1))
    # 保险定时器要 2 秒，1 秒内触发说明是处理循环弹出了栅栏键
    await wait_until(lambda: fired, timeout=1)


async def test_broker_sync_hands_ids_to_sender(hub: SubscriptionHub):
    """sync_：栅栏触发时 sync_id 交给发送循环（叫醒它），不算订阅推送"""
    broker = SubscriptionBroker(hub._backend, hub=hub)
    woke: list[int] = []
    broker.bind_sender_(lambda: woke.append(1))
    broker.idle_(True)
    broker.sync_(7)
    broker.sync_(8)
    await wait_until(broker.has_synced_, timeout=3)
    got: list[int] = []
    await wait_until(
        lambda: got.extend(broker.take_synced_()) or len(got) == 2, timeout=3
    )
    assert got == [7, 8]
    assert woke
    assert not broker.has_updates_()
    assert not broker.has_synced_()
    await broker.close()


@pytest.mark.timeout(20)
async def test_broker_sync_does_not_spin_get_updates(mod_auto_backend):
    """
    手动模式：get_updates 驱动的 tick 里栅栏触发了，但 sync 不是订阅推送：get_updates 到时返回空。
    has_updates_ 算上 sync 的话，step_ 每次都当场返回，get_updates 不让出事件循环地空转
    """
    hub = SubscriptionHub(mod_auto_backend("main"), autostart=False)
    broker = SubscriptionBroker(hub._backend, hub=hub)
    broker.sync_(1)
    assert await broker.get_updates(timeout=hub.mq.FENCE_DELAY + INTERVAL * 3) == {}
    assert broker.take_synced_() == [1]
    await broker.close()
    await hub.close()


async def test_broker_sync_after_close_is_dropped(hub: SubscriptionHub):
    """连接关了：在途的栅栏触发时不再记 sync_id，关闭之后再 sync_ 直接忽略"""
    broker = SubscriptionBroker(hub._backend, hub=hub)
    broker.sync_(1)
    await broker.close()
    broker.sync_(2)
    await asyncio.sleep(hub.mq.FENCE_DELAY + INTERVAL * 2)
    assert not broker.has_synced_()
    assert broker.take_synced_() == []
