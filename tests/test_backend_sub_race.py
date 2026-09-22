"""
SubscriptionBroker.get_updates 一个 tick 内的 await 间隙与接收协程（客户端的 sub/unsub）
交错时的行为。不连数据库：订阅对象用可控的替身，mq 走假的 pubsub 节点。
"""

import asyncio
from collections.abc import Mapping
from types import SimpleNamespace
from typing import Any, cast

from fixtures.fake_pubsub import FakeNodePubSub, make_hub, settle

from hetu.data.backend import Backend
from hetu.data.backend.redis.mq import RedisMQClient
from hetu.data.sub import BaseSubscription, SubscriptionBroker


class FakeSub(BaseSubscription):
    """可控的订阅：get_updated 可卡在闸门上，返回预设的 (新增频道, 移除频道, 更新)"""

    def __init__(self, channels: set[str]):
        self._channels = set(channels)
        self.gate = asyncio.Event()
        self.gate.set()
        self.entered = asyncio.Event()
        self.new: set[str] = set()
        self.rem: set[str] = set()
        self.updates: dict[int, dict[str, Any] | None] = {}

    async def get_updated(
        self, channel: str, payload: set[str] | None = None, prefetched=None
    ) -> tuple[set[str], set[str], Mapping[int, dict[str, Any] | None]]:
        self.entered.set()
        await self.gate.wait()
        self._channels |= self.new
        self._channels -= self.rem
        return set(self.new), set(self.rem), dict(self.updates)

    @property
    def channels(self) -> set[str]:
        return set(self._channels)


def make_broker() -> tuple[SubscriptionBroker, RedisMQClient, FakeNodePubSub]:
    hub, node = make_hub()
    mq = RedisMQClient(hub)
    mq.UPDATE_FREQUENCY = 1000  # type: ignore[reportAttributeAccessIssue]  通知入队后马上能取走
    # row_reader：真 Backend 上是 CachedRowReader；这里没有缓存也没有读，给个 None 占位即可
    backend = SimpleNamespace(get_mq_client=lambda: mq, servant=None, row_reader=None)
    broker = SubscriptionBroker(cast(Backend, backend))
    return broker, mq, node


async def register(
    broker: SubscriptionBroker, node: FakeNodePubSub, sub_id: str, sub: FakeSub
):
    """照 subscribe_get / subscribe_range 的顺序登记一个订阅：先订频道再记账"""
    channels = sorted(sub.channels)
    t = asyncio.create_task(broker._mq_client.subscribe(*channels))  # type: ignore[reportPrivateUsage]
    await settle()
    for channel in channels:
        node.ack("subscribe", channel)
    async with asyncio.timeout(1):
        await t
    broker._subs[sub_id] = sub  # type: ignore[reportPrivateUsage]
    for channel in channels:
        broker._channel_subs.setdefault(channel, set()).add(sub_id)  # type: ignore[reportPrivateUsage]


async def finish(task: asyncio.Task, node: FakeNodePubSub):
    """等 task 结束，期间把它发出的 SUBSCRIBE/UNSUBSCRIBE 都 ack 掉（否则要干等超时）"""
    acked = {"subscribe": len(node.sent("subscribe"))}
    acked["unsubscribe"] = len(node.sent("unsubscribe"))
    async with asyncio.timeout(1):
        while not task.done():
            for mtype, done in acked.items():
                sent = node.sent(mtype)
                for channel in sent[done:]:
                    node.ack(mtype, channel)
                acked[mtype] = len(sent)
            await asyncio.sleep(0.001)
    return task.result()


async def unsubscribe_and_ack(
    broker: SubscriptionBroker, node: FakeNodePubSub, sub_id: str
):
    """接收协程处理客户端 unsub：退订要等 UNSUBSCRIBE ack，由这里投递"""
    await finish(asyncio.create_task(broker.unsubscribe(sub_id)), node)


async def close_all(broker: SubscriptionBroker, node: FakeNodePubSub):
    await finish(asyncio.create_task(broker.close()), node)


async def test_released_channel_resubscribed_during_tick_is_kept():
    """tick 内索引订阅 I1 丢掉行 X、I2 新增行 Y，等 Y 的 SUBSCRIBE ack 期间接收协程
    又为 X 登记了新的行订阅 R：tick 末尾不能按旧名单把 X 退掉"""
    broker, mq, node = make_broker()
    i1 = FakeSub({"idx1", "X"})
    i2 = FakeSub({"idx2"})
    await register(broker, node, "I1", i1)
    await register(broker, node, "I2", i2)
    i1.rem = {"X"}
    i2.new = {"Y"}

    mq.push_pulled_("idx1", None)
    mq.push_pulled_("idx2", None)
    tick = asyncio.create_task(broker.get_updates())
    async with asyncio.timeout(1):
        while "Y" not in node.sent("subscribe"):
            await asyncio.sleep(0.001)
    assert "X" not in broker._channel_subs  # type: ignore[reportPrivateUsage]

    # 接收协程：客户端 subscribe_get 命中了行 X（mq 从没退订过 X，这次订阅无任何动作）
    r = FakeSub({"X"})
    await register(broker, node, "R", r)
    assert node.sent("subscribe").count("X") == 1, "X 从没退订过，不该再发"

    node.ack("subscribe", "Y")
    await finish(tick, node)
    assert node.sent("unsubscribe") == [], "X 又有人订了，不能退"
    assert "X" in mq.subscribed_channels
    assert broker._channel_subs["X"] == {"R"}  # type: ignore[reportPrivateUsage]
    await close_all(broker, node)


async def test_unsub_of_later_sub_in_snapshot_during_tick_is_skipped():
    """两个订阅共享一个频道，tick 处理第一个时（查库中）客户端 unsub 了第二个：
    第二个已从 _subs 弹掉，tick 应跳过它，而不是 KeyError 把连接断掉"""
    broker, mq, node = make_broker()
    subs = {"S1": FakeSub({"C"}), "S2": FakeSub({"C"})}
    for sub_id, sub in subs.items():
        await register(broker, node, sub_id, sub)
        sub.updates = {sub_id: {"id": sub_id}}  # type: ignore[dict-item]
        sub.gate.clear()

    mq.push_pulled_("C", None)
    tick = asyncio.create_task(broker.get_updates())
    # 同一频道下订阅的处理顺序取决于 set 的迭代顺序：先等出来谁在查库，再 unsub 另一个
    async with asyncio.timeout(1):
        while not any(sub.entered.is_set() for sub in subs.values()):
            await asyncio.sleep(0.001)
    first_id = next(sub_id for sub_id, sub in subs.items() if sub.entered.is_set())
    second_id = next(sub_id for sub_id in subs if sub_id != first_id)
    await unsubscribe_and_ack(broker, node, second_id)  # C 还有人在订，不会真退
    assert node.sent("unsubscribe") == []

    subs[first_id].gate.set()
    updates = await finish(tick, node)
    assert updates == {first_id: subs[first_id].updates}
    assert not subs[second_id].entered.is_set()
    await close_all(broker, node)


async def test_sub_unsubscribed_during_its_own_get_updated_leaves_no_trace():
    """索引订阅查库期间被客户端 unsub：查库结果里的新增/移除行不能再记账
    （否则给一个已不存在的订阅登记频道、还为它发 SUBSCRIBE），更新也不再推"""
    broker, mq, node = make_broker()
    s1 = FakeSub({"idx", "X"})
    await register(broker, node, "S1", s1)
    s1.new = {"Y"}
    s1.rem = {"X"}
    s1.updates = {7: {"id": 7}}
    s1.gate.clear()

    mq.push_pulled_("idx", None)
    tick = asyncio.create_task(broker.get_updates())
    async with asyncio.timeout(1):
        await s1.entered.wait()
    await unsubscribe_and_ack(broker, node, "S1")
    assert sorted(node.sent("unsubscribe")) == ["X", "idx"]
    assert "S1" not in broker._subs  # type: ignore[reportPrivateUsage]

    s1.gate.set()
    updates = await finish(tick, node)
    assert updates == {}
    assert "Y" not in node.sent("subscribe"), "为已不存在的订阅发了 SUBSCRIBE"
    assert broker._channel_subs == {}  # type: ignore[reportPrivateUsage]
    assert mq.subscribed_channels == set()
    await close_all(broker, node)
