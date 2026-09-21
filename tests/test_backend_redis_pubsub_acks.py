"""
AsyncKeyspacePubSub / PubSubHub 的 ack 状态机：不连 Redis，用假的节点 pubsub 记录发出的
SUBSCRIBE/UNSUBSCRIBE，ack 由测试手动投递，验证进程共享的 ack future 在调用方被取消、
发送失败等交错下的行为。
"""

import asyncio
from collections.abc import AsyncIterator

import pytest
from redis.asyncio import Redis

from hetu.data.backend.redis.mq import PubSubHub, RedisMQClient
from hetu.data.backend.redis.pubsub import AsyncKeyspacePubSub


class FakeNodePubSub:
    """redis-py PubSub 的替身：记录发出的命令，ack 由测试投递；subscribe 可被闸门卡住"""

    def __init__(self):
        self.commands: list[tuple[str, tuple[str, ...]]] = []
        self.inbox: asyncio.Queue[dict] = asyncio.Queue()
        self.gate = asyncio.Event()
        self.gate.set()
        self.entered = asyncio.Event()  # 有调用方进入了 subscribe（可能卡在闸门上）
        self.fail_next: Exception | None = None

    async def subscribe(self, *channels: str):
        self.entered.set()
        await self.gate.wait()
        if self.fail_next is not None:
            exc, self.fail_next = self.fail_next, None
            raise exc
        self.commands.append(("subscribe", channels))

    async def unsubscribe(self, *channels: str):
        self.commands.append(("unsubscribe", channels))

    async def listen(self) -> AsyncIterator[dict]:
        while True:
            yield await self.inbox.get()

    def ack(self, mtype: str, channel: str):
        self.inbox.put_nowait({"type": mtype, "channel": channel.encode(), "data": 1})

    async def aclose(self):
        pass

    def sent(self, mtype: str) -> list[str]:
        return [ch for cmd, chans in self.commands if cmd == mtype for ch in chans]


def attach_fake_node(pubsub: AsyncKeyspacePubSub) -> FakeNodePubSub:
    node = FakeNodePubSub()
    pubsub.node_resources["standalone"] = {"client": node, "pubsub": node}
    pubsub._spawn_listener("standalone", node)  # type: ignore[reportPrivateUsage]
    return node


def make_pubsub() -> tuple[AsyncKeyspacePubSub, FakeNodePubSub]:
    pubsub = AsyncKeyspacePubSub(Redis(host="127.0.0.1", port=1))  # 只占位，不会连
    return pubsub, attach_fake_node(pubsub)


def make_hub() -> tuple[PubSubHub, FakeNodePubSub]:
    hub = PubSubHub(Redis(host="127.0.0.1", port=1))
    return hub, attach_fake_node(hub._pubsub)  # type: ignore[reportPrivateUsage]


async def settle():
    """让已就绪的 task 都跑一步"""
    for _ in range(3):
        await asyncio.sleep(0)


async def test_cancelled_waiter_keeps_shared_ack_future_alive():
    """两个调用方等同一个频道的 ack，先来的被取消不能连累后来的"""
    pubsub, node = make_pubsub()
    t1 = asyncio.create_task(pubsub.subscribe("X"))
    await settle()
    assert node.sent("subscribe") == ["X"]
    t2 = asyncio.create_task(pubsub.subscribe("X"))
    await settle()
    assert node.sent("subscribe") == ["X"], "搭车的不应再发 SUBSCRIBE"

    t1.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t1
    await settle()
    assert not t2.done(), "共享的 future 不能被 t1 的取消连带取消"
    fut = pubsub._pending_subscribe["X"]  # type: ignore[reportPrivateUsage]
    assert not fut.done()

    node.ack("subscribe", "X")
    async with asyncio.timeout(1):
        await t2
    assert "X" in pubsub.subscribed
    assert not pubsub._pending_subscribe  # type: ignore[reportPrivateUsage]
    await pubsub.close()


async def test_cancel_during_send_still_sends_subscribe():
    """调用方在 SUBSCRIBE 发送中途被取消：命令照样发出去，搭车的等到 ack 正常返回"""
    pubsub, node = make_pubsub()
    node.gate.clear()
    t1 = asyncio.create_task(pubsub.subscribe("X"))
    async with asyncio.timeout(1):
        await node.entered.wait()
    t2 = asyncio.create_task(pubsub.subscribe("X"))
    await settle()

    t1.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t1
    await settle()
    fut = pubsub._pending_subscribe["X"]  # type: ignore[reportPrivateUsage]
    assert not fut.done(), "取消不能把 CancelledError 塞进共享的 future"

    node.gate.set()
    await settle()
    assert node.sent("subscribe") == ["X"]
    node.ack("subscribe", "X")
    async with asyncio.timeout(1):
        await t2
    assert "X" in pubsub.subscribed
    await pubsub.close()


async def test_send_failure_fails_every_waiter():
    """SUBSCRIBE 发送失败：发起者和搭车者都拿到异常，频道不留在待确认表里"""
    pubsub, node = make_pubsub()
    node.gate.clear()
    node.fail_next = ConnectionError("boom")
    t1 = asyncio.create_task(pubsub.subscribe("X"))
    async with asyncio.timeout(1):
        await node.entered.wait()
    t2 = asyncio.create_task(pubsub.subscribe("X"))
    await settle()

    node.gate.set()
    with pytest.raises(ConnectionError):
        await t1
    with pytest.raises(ConnectionError):
        await t2
    assert "X" not in pubsub.subscribed
    assert "X" not in pubsub._channel_node  # type: ignore[reportPrivateUsage]

    # 之后可以重新订阅
    t3 = asyncio.create_task(pubsub.subscribe("X"))
    await settle()
    assert node.sent("subscribe") == ["X"]
    node.ack("subscribe", "X")
    async with asyncio.timeout(1):
        await t3
    await pubsub.close()


async def test_hub_add_cancelled_releases_only_own_registration():
    """连接 A 等 ack 时被拆：只撤 A 自己的登记，搭车的连接 B 照常订上"""
    hub, node = make_hub()
    mq_a, mq_b = RedisMQClient(hub), RedisMQClient(hub)
    t_a = asyncio.create_task(mq_a.subscribe("X"))
    await settle()
    t_b = asyncio.create_task(mq_b.subscribe("X"))
    await settle()
    assert hub.subscriber_count("X") == 2

    t_a.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t_a
    await settle()
    assert hub.subscriber_count("X") == 1
    assert not t_b.done()
    assert node.sent("unsubscribe") == [], "还有 B 在订，不能退订"

    node.ack("subscribe", "X")
    async with asyncio.timeout(1):
        await t_b
    assert mq_b.subscribed_channels == {"X"}
    assert not mq_a.subscribed_channels

    close = asyncio.create_task(mq_b.close())
    await settle()
    assert node.sent("unsubscribe") == ["X"]
    node.ack("unsubscribe", "X")
    async with asyncio.timeout(1):
        await close
    assert hub.subscriber_count("X") == 0
    await hub.close()


async def test_hub_add_cancelled_last_subscriber_unsubscribes_in_background():
    """唯一的订阅者等 ack 时被拆：SUBSCRIBE 已发出，得在后台把它退掉，不留没人收的订阅"""
    hub, node = make_hub()
    mq = RedisMQClient(hub)
    t = asyncio.create_task(mq.subscribe("X"))
    await settle()
    assert node.sent("subscribe") == ["X"]

    t.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t
    assert hub.subscriber_count("X") == 0
    node.ack("subscribe", "X")
    await settle()
    assert node.sent("unsubscribe") == ["X"]
    node.ack("unsubscribe", "X")
    await settle()
    assert "X" not in hub.channels
    await hub.close()


async def test_hub_background_unsubscribe_skips_resubscribed_channel():
    """后台退订排队期间又有连接订了同一频道：不能把新订阅退掉"""
    hub, node = make_hub()
    mq_a, mq_b = RedisMQClient(hub), RedisMQClient(hub)
    t_a = asyncio.create_task(mq_a.subscribe("X"))
    await settle()
    # 只让 A 跑一步处理取消：退订任务此时已排队但还没跑
    t_a.cancel()
    await asyncio.sleep(0)
    assert t_a.cancelled()
    assert hub.subscriber_count("X") == 0

    # B 这时订上（直接 await：同步跑到等 ack 为止，登记先于退订任务跑起来），
    # 它搭的是 A 那条还没 ack 的 SUBSCRIBE 的车，退订任务跑起来后必须放弃
    async def ack_later():
        await settle()
        node.ack("subscribe", "X")

    acker = asyncio.create_task(ack_later())
    async with asyncio.timeout(1):
        await mq_b.subscribe("X")
    await acker
    assert hub.subscriber_count("X") == 1
    assert node.sent("subscribe") == ["X"]
    assert node.sent("unsubscribe") == []
    await hub.close()


async def test_hub_piggyback_sees_send_failure():
    """搭车的连接必须看到发起者的发送失败，而不是以为自己订上了"""
    hub, node = make_hub()
    mq_a, mq_b = RedisMQClient(hub), RedisMQClient(hub)
    node.gate.clear()
    node.fail_next = ConnectionError("boom")
    t_a = asyncio.create_task(mq_a.subscribe("X"))
    async with asyncio.timeout(1):
        await node.entered.wait()
    t_b = asyncio.create_task(mq_b.subscribe("X"))
    await settle()
    assert hub.subscriber_count("X") == 2

    node.gate.set()
    with pytest.raises(ConnectionError):
        await t_a
    with pytest.raises(ConnectionError):
        await t_b
    assert hub.subscriber_count("X") == 0
    assert not mq_a.subscribed_channels and not mq_b.subscribed_channels
    await hub.close()


async def test_hub_resends_for_channel_whose_subscribe_failed():
    """发起者的 SUBSCRIBE 失败、等它的人还没来得及撤登记时又来了新订阅者：要重新发"""
    hub, node = make_hub()
    mq_a, mq_b, mq_c = RedisMQClient(hub), RedisMQClient(hub), RedisMQClient(hub)
    node.gate.clear()
    node.fail_next = ConnectionError("boom")
    t_a = asyncio.create_task(mq_a.subscribe("X"))
    async with asyncio.timeout(1):
        await node.entered.wait()
    t_b = asyncio.create_task(mq_b.subscribe("X"))
    await settle()

    # 放行后发送 task 先跑一步把 future 置为失败；A、B 的唤醒排在本协程之后
    node.gate.set()
    await asyncio.sleep(0)
    assert not t_a.done() and not t_b.done()
    t_c = asyncio.create_task(mq_c.subscribe("X"))
    with pytest.raises(ConnectionError):
        await t_a
    with pytest.raises(ConnectionError):
        await t_b
    await settle()
    assert node.sent("subscribe") == ["X"], "C 发现频道在 Redis 侧没订上，重新发"
    assert hub.subscriber_count("X") == 1
    node.ack("subscribe", "X")
    async with asyncio.timeout(1):
        await t_c
    assert mq_c.subscribed_channels == {"X"}
    await hub.close()


async def test_unsubscribe_before_subscribe_ack_leaves_no_stale_subscribed():
    """SUBSCRIBE 还没 ack 就退订：迟到的 subscribe ack 不能把频道重新算作已订阅，
    否则下一个订阅者会被"已订阅"短路、不再发 SUBSCRIBE，永远收不到通知"""
    pubsub, node = make_pubsub()
    t = asyncio.create_task(pubsub.subscribe("X"))
    await settle()
    u = asyncio.create_task(pubsub.unsubscribe("X"))
    await settle()
    assert node.commands == [("subscribe", ("X",)), ("unsubscribe", ("X",))]
    async with asyncio.timeout(1):
        await t  # 退订让它正常返回，不报错

    node.ack("subscribe", "X")
    await settle()
    assert "X" not in pubsub.subscribed, "过时的 subscribe ack 被当真了"
    node.ack("unsubscribe", "X")
    async with asyncio.timeout(1):
        await u
    assert "X" not in pubsub.subscribed

    # 下一个订阅者必须真的再发一次 SUBSCRIBE
    t2 = asyncio.create_task(pubsub.subscribe("X"))
    await settle()
    assert node.sent("subscribe") == ["X", "X"]
    node.ack("subscribe", "X")
    async with asyncio.timeout(1):
        await t2
    assert "X" in pubsub.subscribed
    await pubsub.close()


async def test_resubscribe_while_unsubscribe_pending_sends_again():
    """UNSUBSCRIBE 还没 ack 又订回来：要重新发 SUBSCRIBE，unsubscribe ack 不能把它抹掉"""
    pubsub, node = make_pubsub()
    t = asyncio.create_task(pubsub.subscribe("X"))
    await settle()
    node.ack("subscribe", "X")
    async with asyncio.timeout(1):
        await t
    u = asyncio.create_task(pubsub.unsubscribe("X"))
    await settle()
    t2 = asyncio.create_task(pubsub.subscribe("X"))
    await settle()
    assert node.sent("subscribe") == ["X", "X"]

    node.ack("unsubscribe", "X")
    async with asyncio.timeout(1):
        await u
    await settle()
    assert not t2.done()
    node.ack("subscribe", "X")
    async with asyncio.timeout(1):
        await t2
    assert "X" in pubsub.subscribed
    await pubsub.close()


async def test_hub_unsubscribe_racing_own_pending_subscribe_does_not_raise():
    """同一连接：subscribe 还在等 ack，另一个协程（客户端 unsub）把它退订了。
    等 ack 的 subscribe 必须正常返回（否则 get_updates 会把整个连接断掉），
    且不能把刚退掉的频道又记回 subscribed"""
    hub, node = make_hub()
    mq = RedisMQClient(hub)
    t = asyncio.create_task(mq.subscribe("X"))
    await settle()
    assert node.sent("subscribe") == ["X"]
    u = asyncio.create_task(mq.unsubscribe("X"))
    await settle()
    assert node.sent("unsubscribe") == ["X"]

    async with asyncio.timeout(1):
        await t
    assert "X" not in mq.subscribed_channels
    assert hub.subscriber_count("X") == 0

    node.ack("subscribe", "X")
    node.ack("unsubscribe", "X")
    async with asyncio.timeout(1):
        await u
    assert "X" not in hub._pubsub.subscribed  # type: ignore[reportPrivateUsage]
    await hub.close()


async def test_unsubscribe_ack_timeout_keeps_shared_future_usable(monkeypatch):
    """UNSUBSCRIBE 等 ack 超时：只是本调用方不等了，共享的 future 不能被取消后还留在
    待确认表里，否则之后再退订同一频道的人会莫名收到 CancelledError"""
    monkeypatch.setattr("hetu.data.backend.redis.pubsub.UNSUBSCRIBE_ACK_TIMEOUT", 0.05)
    pubsub, node = make_pubsub()
    t = asyncio.create_task(pubsub.subscribe("X"))
    await settle()
    node.ack("subscribe", "X")
    async with asyncio.timeout(1):
        await t

    async with asyncio.timeout(1):
        await pubsub.unsubscribe("X")  # 不投 ack，超时后只记日志正常返回
    fut = pubsub._pending_unsubscribe["X"]  # type: ignore[reportPrivateUsage]
    assert not fut.done(), "超时不能连带取消共享的 future"

    # 再退订一次（如别的连接刚订上又退掉），ack 到了正常返回
    u2 = asyncio.create_task(pubsub.unsubscribe("X"))
    await settle()
    assert not u2.done()
    node.ack("unsubscribe", "X")
    async with asyncio.timeout(1):
        await u2
    assert "X" not in pubsub._pending_unsubscribe  # type: ignore[reportPrivateUsage]
    await pubsub.close()
