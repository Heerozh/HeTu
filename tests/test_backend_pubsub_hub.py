"""
进程共享的通知接收器（Redis 的 PubSubHub / SQL 的 SQLNotifyHub）：
多个连接订阅同一频道只向后端订阅一次，各自都能收到更新，最后一个退订才真正退订。
"""

import asyncio
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
    ctx = _admin_ctx()
    broker_a = SubscriptionBroker(backend)
    broker_b = SubscriptionBroker(backend)

    hub = _hub(backend)
    pubsub = getattr(hub, "_pubsub", None)  # Redis 才有
    real_subscribe = None if pubsub is None else pubsub.subscribe
    sends = 0
    if pubsub is not None and real_subscribe is not None:

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
    if pubsub is not None and real_subscribe is not None:
        assert channel not in pubsub.subscribed
        pubsub.subscribe = real_subscribe


async def test_hub_mq_client_close_releases_only_own(filled_item_ref, mod_auto_backend):
    """一个连接关闭只撤销自己的登记，不影响别的连接对同一频道的订阅"""
    backend: Backend = mod_auto_backend()
    servant = backend.servant
    rows = await servant.range(filled_item_ref, "time", 110, 112, limit=10)
    channels = [servant.row_channel(filled_item_ref, r.id) for r in rows]
    assert len(channels) == 3

    mq1 = backend.get_mq_client()
    mq2 = backend.get_mq_client()
    hub = _hub(backend)  # hub 随第一个 mq_client 懒创建，不能依赖前面的测试建好它
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


async def test_watch_channel_callback_bypasses_client_queue(
    filled_item_ref, mod_auto_backend
):
    """broker.watch_channel：服务端内部关注的频道收到通知只回调，不进客户端推送队列；
    回调异常不影响后续通知；连接关闭随 mq_client 一起退订"""
    backend: Backend = mod_auto_backend()
    servant = backend.servant
    ref = filled_item_ref
    row = await servant.get(ref, (await servant.range(ref, "time", 110, limit=1))[0].id)
    assert row is not None
    channel = servant.row_channel(ref, int(row.id))

    broker = SubscriptionBroker(backend)  # hub 随第一个 mq_client 懒创建
    hub = _hub(backend)
    hits: list[int] = []

    def on_change():
        hits.append(1)
        if len(hits) == 1:
            raise RuntimeError("callback boom")  # 第一次故意抛，hub 不能因此停摆

    await broker.watch_channel(channel, on_change)
    assert hub.subscriber_count(channel) == 1

    await _update_qty(backend, ref, 5)
    async with asyncio.timeout(3):
        while not hits:
            await asyncio.sleep(0.02)
    # 没进推送队列：客户端侧拿不到任何更新
    assert channel not in broker._mq_client.pulled_set
    assert await broker.get_updates(timeout=0.3) == {}

    # 回调抛过异常后，后续通知照常到达
    await _update_qty(backend, ref, 6)
    async with asyncio.timeout(3):
        while len(hits) < 2:
            await asyncio.sleep(0.02)

    await broker.close()
    assert hub.subscriber_count(channel) == 0


async def test_watch_and_client_subscription_share_channel(
    filled_item_ref, mod_auto_backend
):
    """服务端关注（watch_channel）和客户端订阅落在同一频道：通知既回调也推给客户端；
    客户端退订不能把关注一起退掉，关注只随连接关闭退订"""
    backend: Backend = mod_auto_backend()
    ctx = _admin_ctx()
    broker = SubscriptionBroker(backend)
    hub = _hub(backend)
    hits: list[int] = []

    async def wait_hits(count: int):
        async with asyncio.timeout(3):
            while len(hits) < count:
                await asyncio.sleep(0.02)

    sub_id, row = await broker.subscribe_get(filled_item_ref, ctx, "name", "Itm10")
    assert sub_id and row
    channel = cast(RowSubscription, broker._subs[sub_id]).channel
    await broker.watch_channel(channel, lambda: hits.append(1))
    assert hub.subscriber_count(channel) == 1  # 同一连接只登记一次

    # 同一条变更：关注回调到了，客户端也照常收到 updt
    await _update_qty(backend, filled_item_ref, 501)
    await wait_hits(1)
    updates = await _get_updates(broker)
    assert updates[sub_id][row["id"]]["qty"] == 501

    # 客户端退订：hub 里的登记还在，关注照常
    await broker.unsubscribe(sub_id)
    assert hub.subscriber_count(channel) == 1
    await _update_qty(backend, filled_item_ref, 502)
    await wait_hits(2)
    assert await broker.get_updates(timeout=0.3) == {}  # 客户端不再收到

    # 再订回来又能收到，关注也没丢
    sub_id2, _ = await broker.subscribe_get(filled_item_ref, ctx, "name", "Itm10")
    assert sub_id2
    await _update_qty(backend, filled_item_ref, 503)
    await wait_hits(3)
    updates = await _get_updates(broker)
    assert updates[sub_id2][row["id"]]["qty"] == 503

    await broker.close()
    assert hub.subscriber_count(channel) == 0


async def _wait_until(pred, timeout: float = 3.0):
    async with asyncio.timeout(timeout):
        while not pred():
            await asyncio.sleep(0.01)


async def test_row_cache_activation(
    filled_item_ref, mod_auto_backend, mod_backend_config
):
    """行频道在 SUBSCRIBE ack 后于 RowCache 激活（floor 未知），索引 / 值 / 表级频道不激活；
    别的进程写入的通知带版本：逐出缓存行并抬 floor；本进程写入则写穿；
    两个 broker 退一个仍激活，最后一个退订才失活清行"""
    import copy

    from hetu.data.backend.rowcache import UNKNOWN
    from hetu.data.sub import IndexSubscription, TableSubscription

    backend: Backend = mod_auto_backend()
    cache = backend.row_cache
    ctx = _admin_ctx()
    broker_a = SubscriptionBroker(backend)
    sub_a, row = await broker_a.subscribe_get(filled_item_ref, ctx, "name", "Itm10")
    assert sub_a and row
    if cache is None:  # SQL 后端不参与行缓存
        await broker_a.close()
        return
    channel = cast(RowSubscription, broker_a._subs[sub_a]).channel
    assert cache.is_active(channel)
    assert cache.lease(channel) is not None
    # subscribe_get 那次读走副本、不入缓存：激活了但还什么都不知道
    assert cache.floor(channel) is UNKNOWN and cache.get(channel) is None

    sub_r, _ = await broker_a.subscribe_range(
        filled_item_ref, ctx, "owner", 10, limit=5
    )
    assert sub_r
    idx_sub = cast(IndexSubscription, broker_a._subs[sub_r])
    assert not cache.is_active(idx_sub.index_channel)
    assert all(cache.is_active(ch) for ch in idx_sub.row_subs)  # 范围内的行也激活
    sub_t, _ = await broker_a.subscribe_table(filled_item_ref, ctx)
    assert sub_t
    tbl_sub = cast(TableSubscription, broker_a._subs[sub_t])
    assert not cache.is_active(tbl_sub.table_channel)

    # 本进程写一次：commit 写穿把行放进缓存（订阅时的那次读不入缓存）
    row_id = int(row["id"])
    await _update_qty(backend, filled_item_ref, 994)
    await _get_updates(broker_a)
    rec = cache.get(channel)
    assert rec is not None
    old_version = int(rec["_version"])

    # 别的进程（另一个 Backend）改行：通知逐出缓存行，floor 抬到新版本
    other = Backend(copy.deepcopy(mod_backend_config))
    other.post_configure(components=[filled_item_ref.comp_cls])
    try:
        await _update_qty(other, filled_item_ref, 995)
    finally:
        await other.close()
    await _wait_until(lambda: cache.get(channel) is None)
    assert cache.floor(channel) == old_version + 1
    await _get_updates(broker_a)  # 消费掉通知

    # 本进程自己改行：commit 写穿，通知到达后仍在
    await _update_qty(backend, filled_item_ref, 996)
    cached = cache.get(channel)
    assert cached is not None and cached.qty == 996
    assert int(cached["_version"]) == old_version + 2
    await _get_updates(broker_a)
    assert cache.get(channel) is not None

    # 两个 broker 订同一行：退一个仍激活；最后一个退订才失活并清行
    broker_b = SubscriptionBroker(backend)
    sub_b, _ = await broker_b.subscribe_get(filled_item_ref, ctx, "id", row_id)
    assert sub_b
    rec = await backend.row_reader.get(filled_item_ref, row_id, backend.servant)
    assert rec is not None and cache.get(channel) is not None
    await broker_a.close()
    assert cache.is_active(channel) and cache.get(channel) is not None
    await broker_b.close()
    assert not cache.is_active(channel) and cache.get(channel) is None


async def test_row_cache_pubsub_reset(filled_item_ref, mod_auto_backend):
    """pubsub 节点失效：hub 把自己激活的频道全部失活（缓存清空）；恢复订阅后重新激活、
    floor 回到未知；之后通知仍能到达"""
    from hetu.data.backend.rowcache import UNKNOWN

    backend: Backend = mod_auto_backend()
    cache = backend.row_cache
    if cache is None:
        return
    ctx = _admin_ctx()
    broker = SubscriptionBroker(backend)
    sub, row = await broker.subscribe_get(filled_item_ref, ctx, "name", "Itm10")
    assert sub and row
    channel = cast(RowSubscription, broker._subs[sub]).channel
    row_id = int(row["id"])
    hub = _hub(backend)

    # 订阅时的读不入缓存，先用一次本进程的写（commit 写穿）把行放进缓存
    await _update_qty(backend, filled_item_ref, 993)
    await _get_updates(broker)
    # 直接调 hub 的钩子（模拟节点失效 / 恢复）
    assert cache.get(channel) is not None
    hub._on_reset()
    assert not cache.is_active(channel) and cache.get(channel) is None
    hub._on_restored()
    assert cache.is_active(channel) and cache.floor(channel) is UNKNOWN

    # 真实断连：关掉节点 pubsub 连接让监听协程异常 → on_reset → resubscribe_all → on_restored
    pubsub = hub._pubsub
    # 重新激活后 floor 又是未知，读不入缓存；用一次本进程的写把行写穿回去
    await _update_qty(backend, filled_item_ref, 995)
    await _get_updates(broker)
    assert cache.get(channel) is not None
    for res in list(pubsub.node_resources.values()):
        await res["pubsub"].connection.disconnect()
    await _wait_until(lambda: cache.get(channel) is None, timeout=5)
    await _wait_until(
        lambda: cache.is_active(channel) and channel in pubsub.subscribed, timeout=10
    )
    assert cache.floor(channel) is UNKNOWN
    # 恢复后的通知照常到达
    await _update_qty(backend, filled_item_ref, 994)
    updates = await _get_updates(broker)
    assert updates[sub][row_id]["qty"] == 994
    await broker.close()


async def test_row_evicted_before_table_wakeup(
    filled_item_ref, mod_auto_backend, mod_backend_config
):
    """同一次 commit 的通知里行频道那条必须排在表级 / 索引值频道之前：表级通知一到 hub 就
    把连接叫醒，醒来的 TableSubscription / IndexSubscription 会去读这次变更的行，此时行还
    没被逐出的话读到的是缓存里的旧行，推给客户端后也不会再有第二次通知来纠正。
    现在这个顺序只靠 get_message 的 100ms 合批窗口挡着，事件循环卡一下就不成立"""
    import copy
    from unittest.mock import patch

    backend: Backend = mod_auto_backend()
    cache = backend.row_cache
    if cache is None:  # SQL 后端不参与行缓存
        return
    ctx = _admin_ctx()
    broker = SubscriptionBroker(backend)
    sub_row, row = await broker.subscribe_get(filled_item_ref, ctx, "name", "Itm10")
    assert sub_row and row
    sub_tbl, _ = await broker.subscribe_table(filled_item_ref, ctx)
    assert sub_tbl
    channel = cast(RowSubscription, broker._subs[sub_row]).channel
    table_channel = backend.master.table_channel(filled_item_ref)
    # 订阅时的读不入缓存，先用一次本进程的写（commit 写穿）把行放进缓存
    await _update_qty(backend, filled_item_ref, 992)
    await _get_updates(broker)
    assert cache.get(channel) is not None

    # 记录表级通知被塞进本连接队列（= 连接被叫醒）的那一刻，缓存里这行的样子
    mq = broker._mq_client
    real_push = mq.push_pulled_
    seen: list = []

    def spy(channel_name, payload_ids):
        if channel_name == table_channel:
            seen.append(cache.get(channel))
        return real_push(channel_name, payload_ids)

    other = Backend(copy.deepcopy(mod_backend_config))
    other.post_configure(components=[filled_item_ref.comp_cls])
    try:
        with patch.object(mq, "push_pulled_", spy):
            await _update_qty(other, filled_item_ref, 993)
            await _wait_until(lambda: len(seen) > 0, timeout=5)
    finally:
        await other.close()
    assert seen[0] is None, "表级通知叫醒连接时，这行必须已经被逐出"
    await broker.close()


async def test_pubsub_connection_tcp_keepalive(filled_item_ref, mod_auto_backend):
    """pubsub 连接常驻只读，半开 TCP 连接只能靠内核 keepalive 判死：连接参数里必须显式开着，
    不依赖 redis-py 的版本默认值（7.x 默认关）"""
    backend: Backend = mod_auto_backend()
    hub = _hub(backend)
    pubsub = getattr(hub, "_pubsub", None)
    if pubsub is None:
        return  # SQL 后端没有 pubsub 连接
    broker = SubscriptionBroker(backend)
    sub, _ = await broker.subscribe_get(filled_item_ref, _admin_ctx(), "name", "Itm10")
    assert sub
    try:
        for res in pubsub.node_resources.values():
            conn = res["pubsub"].connection
            assert conn is not None
            if hasattr(conn, "socket_keepalive"):  # unix socket 连接没有
                assert conn.socket_keepalive is True
    finally:
        await broker.close()
