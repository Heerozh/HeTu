"""
worker 级订阅器（SubscriptionHub + 每连接的 SubscriptionBroker 门面）在各种协程交错下的行为：
tick 里的 await 间隙与接收协程（客户端的 sub/unsub）交错、几个连接共用一个 MQClient 时的订阅 /
退订 / 取消。不连数据库：订阅对象用可控的替身，mq 走假的 pubsub 节点。
"""

import asyncio
from collections.abc import Mapping
from types import SimpleNamespace
from typing import Any, cast

import pytest
from fixtures.fake_pubsub import FakeNodePubSub, make_hub, settle

from hetu.data.backend import Backend
from hetu.data.backend.redis.mq import RedisMQClient
from hetu.data.sub import BaseSubscription, SubscriptionBroker, SubscriptionHub


class FakeSub(BaseSubscription):
    """可控的订阅：get_updated 可卡在闸门上，返回预设的 (新增频道, 移除频道, 更新)，并记下调用"""

    def __init__(self, channels: set[str]):
        self._channels = set(channels)
        self.gate = asyncio.Event()
        self.gate.set()
        self.entered = asyncio.Event()
        self.calls: list[tuple[str, set[str] | None]] = []
        self.new: set[str] = set()
        self.rem: set[str] = set()
        self.updates: dict[int, dict[str, Any] | None] = {}

    async def get_updated(
        self, channel: str, payload: set[str] | None = None
    ) -> tuple[set[str], set[str], Mapping[int, dict[str, Any] | None]]:
        self.calls.append((channel, payload))
        self.entered.set()
        await self.gate.wait()
        self._channels |= self.new
        self._channels -= self.rem
        return set(self.new), set(self.rem), dict(self.updates)

    @property
    def channels(self) -> set[str]:
        return set(self._channels)


# 一个 tick：get_updates 碰到没有任何更新的批次会接着等下一批，给个总时长让它结束
TICK = 0.3


def make_brokers(
    n: int, autostart: bool = False
) -> tuple[SubscriptionHub, list[SubscriptionBroker], RedisMQClient, FakeNodePubSub]:
    """n 个门面共用一个 hub；autostart=False 时由门面的 get_updates 驱动 tick"""
    pubsub_hub, node = make_hub()
    mq = RedisMQClient(pubsub_hub)
    mq.UPDATE_FREQUENCY = 1000  # type: ignore[reportAttributeAccessIssue]  通知入队后马上能取走
    backend = cast(Backend, SimpleNamespace(get_mq_client=lambda: mq, servant=None))
    hub = SubscriptionHub(backend, autostart=autostart)
    return hub, [SubscriptionBroker(backend, hub=hub) for _ in range(n)], mq, node


def make_broker() -> tuple[SubscriptionBroker, RedisMQClient, FakeNodePubSub]:
    _hub, (broker,), mq, node = make_brokers(1)
    return broker, mq, node


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


async def register(
    broker: SubscriptionBroker, node: FakeNodePubSub, sub_id: str, sub: FakeSub
):
    """照 subscribe_get / subscribe_range 的顺序登记一个订阅：先订频道（生效），再登记到门面"""
    await finish(asyncio.create_task(broker._attach(sub_id, sub)), node)


async def unsubscribe_and_ack(
    broker: SubscriptionBroker, node: FakeNodePubSub, sub_id: str
):
    """接收协程处理客户端 unsub：退订要等 UNSUBSCRIBE ack，由这里投递"""
    await finish(asyncio.create_task(broker.unsubscribe(sub_id)), node)


async def close_all(broker: SubscriptionBroker, node: FakeNodePubSub):
    await finish(asyncio.create_task(broker.close()), node)


async def close_hub(hub: SubscriptionHub, node: FakeNodePubSub):
    await finish(asyncio.create_task(hub.close()), node)


async def wait_sent(node: FakeNodePubSub, mtype: str, channel: str):
    async with asyncio.timeout(1):
        while channel not in node.sent(mtype):
            await asyncio.sleep(0.001)


async def test_released_channel_resubscribed_during_tick_is_kept():
    """tick 内索引订阅 I1 丢掉行 X、I2 新增行 Y，等 Y 的 SUBSCRIBE ack 期间接收协程
    又为 X 登记了新的行订阅 R：tick 末尾不能按旧名单把 X 退掉"""
    broker, mq, node = make_broker()
    hub = broker._hub
    i1 = FakeSub({"idx1", "X"})
    i2 = FakeSub({"idx2"})
    await register(broker, node, "I1", i1)
    await register(broker, node, "I2", i2)
    i1.rem = {"X"}
    i2.new = {"Y"}

    mq.push_pulled_("idx1", None)
    mq.push_pulled_("idx2", None)
    tick = asyncio.create_task(broker.get_updates(timeout=TICK))
    await wait_sent(node, "subscribe", "Y")
    assert "X" not in hub._channel_subs

    # 接收协程：客户端 subscribe_get 命中了行 X（mq 从没退订过 X，这次订阅无任何动作）
    r = FakeSub({"X"})
    await register(broker, node, "R", r)
    assert node.sent("subscribe").count("X") == 1, "X 从没退订过，不该再发"

    node.ack("subscribe", "Y")
    await finish(tick, node)
    assert node.sent("unsubscribe") == [], "X 又有人订了，不能退"
    assert "X" in mq.subscribed_channels
    assert hub._channel_subs["X"] == {r}
    await close_all(broker, node)


async def test_unsub_of_another_sub_during_tick_drops_its_updates():
    """两个订阅共享一个频道，tick 处理它们时（都在查库）客户端 unsub 了第二个：
    第二个的结果丢掉，频道第一个还在用、不退订；第一个照常推送"""
    broker, mq, node = make_broker()
    subs = {"S1": FakeSub({"C"}), "S2": FakeSub({"C"})}
    for sub_id, sub in subs.items():
        await register(broker, node, sub_id, sub)
        sub.updates = {int(sub_id[1]): {"id": sub_id}}
        sub.gate.clear()

    mq.push_pulled_("C", None)
    tick = asyncio.create_task(broker.get_updates(timeout=TICK))
    async with asyncio.timeout(1):
        for sub in subs.values():
            await sub.entered.wait()
    await unsubscribe_and_ack(broker, node, "S2")  # C 还有人在订，不会真退
    assert node.sent("unsubscribe") == []

    for sub in subs.values():
        sub.gate.set()
    updates = await finish(tick, node)
    assert updates == {"S1": subs["S1"].updates}
    await close_all(broker, node)


async def test_sub_unsubscribed_during_its_own_get_updated_leaves_no_trace():
    """索引订阅查库期间被客户端 unsub：查库结果里的新增/移除行不能再记账
    （否则给一个已不存在的订阅登记频道、还为它发 SUBSCRIBE），更新也不再推"""
    broker, mq, node = make_broker()
    hub = broker._hub
    s1 = FakeSub({"idx", "X"})
    await register(broker, node, "S1", s1)
    s1.new = {"Y"}
    s1.rem = {"X"}
    s1.updates = {7: {"id": 7}}
    s1.gate.clear()

    mq.push_pulled_("idx", None)
    tick = asyncio.create_task(broker.get_updates(timeout=TICK))
    async with asyncio.timeout(1):
        await s1.entered.wait()
    await unsubscribe_and_ack(broker, node, "S1")
    assert sorted(node.sent("unsubscribe")) == ["X", "idx"]
    assert "S1" not in broker._subs

    s1.gate.set()
    updates = await finish(tick, node)
    assert updates == {}
    assert "Y" not in node.sent("subscribe"), "为已不存在的订阅发了 SUBSCRIBE"
    assert hub._channel_subs == {}
    assert mq.subscribed_channels == set()
    await close_all(broker, node)


async def test_attach_returns_inactive_until_broker_registers():
    """hub.attach 返回时订阅仍未生效（active=False），tick 不处理它：active 要由门面在登记的同一个
    同步段里置。由订阅任务自己置的话，门面登记之前 tick 算好的推送会因 sub_id 未登记被丢掉，
    指纹却已更新，之后的补读读回一样也不再推（设计稿 §4.6）"""
    hub, (broker,), mq, node = make_brokers(1)
    sub = FakeSub({"X"})
    await finish(asyncio.create_task(hub.attach(sub, broker, "S")), node)
    assert not sub.active
    mq.push_pulled_("X", None)
    assert await broker.get_updates(timeout=TICK) == {}
    assert sub.calls == []
    await close_hub(hub, node)


async def test_attach_in_flight_keeps_channel_from_unsubscribe():
    """连接 B 正在 attach 订阅 R（频道 X、Y，Y 的 SUBSCRIBE 还没回来），tick 里 I1 把 X 放出范围：
    X 上有 R 的占位，不能退订；R 订上之后 X 的通知照常交给它（设计稿 §4.6）"""
    hub, (a, b), mq, node = make_brokers(2)
    i1 = FakeSub({"idx1", "X"})
    await register(a, node, "I1", i1)
    i1.rem = {"X"}

    r = FakeSub({"X", "Y"})
    attaching = asyncio.create_task(b._attach("R", r))
    await wait_sent(node, "subscribe", "Y")

    mq.push_pulled_("idx1", None)
    await finish(asyncio.create_task(a.get_updates(timeout=TICK)), node)
    assert i1.calls == [("idx1", None)]
    assert node.sent("unsubscribe") == [], "X 上有 R 的占位，不能退订"

    node.ack("subscribe", "Y")
    await finish(attaching, node)
    assert {"X", "Y"} <= mq.subscribed_channels
    assert hub._channel_subs["X"] == {r}
    mq.push_pulled_("X", None)
    await finish(asyncio.create_task(b.get_updates(timeout=TICK)), node)
    assert ("X", None) in r.calls
    await close_hub(hub, node)


async def test_cancelled_attach_does_not_revoke_other_connection():
    """连接 A、B 同时 attach 同一个新频道 X（B 搭 A 发出的 SUBSCRIBE），A 在等 ack 时被取消
    （连接断开）：共用的 MQClient 对 X 的登记不能被撤掉，B 照常收到 X 的通知（设计稿 §4.6）"""
    hub, (a, b), mq, node = make_brokers(2)
    sa, sb = FakeSub({"X"}), FakeSub({"X"})
    ta = asyncio.create_task(a._attach("A", sa))
    await wait_sent(node, "subscribe", "X")
    tb = asyncio.create_task(b._attach("B", sb))
    await settle()
    ta.cancel()
    with pytest.raises(asyncio.CancelledError):
        await ta
    node.ack("subscribe", "X")
    await finish(tb, node)

    assert "X" in mq.subscribed_channels
    assert mq._hub.subscriber_count("X") == 1
    assert node.sent("unsubscribe") == []
    assert sa.closed and not sa.active
    assert hub._channel_subs["X"] == {sb}
    mq.push_pulled_("X", None)
    await finish(asyncio.create_task(b.get_updates(timeout=TICK)), node)
    assert ("X", None) in sb.calls
    await close_hub(hub, node)


async def test_new_channel_gets_reread_while_its_subscribe_is_in_flight():
    """tick 里订阅 S 新增频道 X 时，X 的 SUBSCRIBE 正由别的连接的 attach 发出、还没回来：
    MQClient.subscribed 里已经有 X，但订阅还没生效。S 读 X 在前、生效在后，其间的写入没有
    通知，仍要给 S 定向补读（设计稿 §4.4）"""
    hub, (a, b), mq, node = make_brokers(2)
    s = FakeSub({"idx"})
    await register(a, node, "S", s)
    s.new = {"X"}

    p = FakeSub({"X"})
    attaching = asyncio.create_task(b._attach("P", p))
    await wait_sent(node, "subscribe", "X")
    assert "X" in mq.subscribed_channels

    mq.push_pulled_("idx", None)
    tick = asyncio.create_task(a.get_updates(timeout=TICK))
    async with asyncio.timeout(1):
        await s.entered.wait()
    await settle()
    assert not tick.done()  # tick 末尾在等 X 的 SUBSCRIBE（搭 P 发出的那个）
    node.ack("subscribe", "X")
    await finish(attaching, node)
    await finish(tick, node)
    assert ("X", None) in s.calls, "X 订阅生效前 S 读过它，要定向补读"
    await close_hub(hub, node)


async def test_updates_are_delivered_when_the_tick_ends():
    """一个 tick 里一个订阅处理完、另一个还在查库：算好的更新先暂存，整个 tick 结束才交给连接
    （get_updates 拿到的总是完整的 tick）"""
    hub, (a,), mq, node = make_brokers(1, autostart=True)
    fast, slow = FakeSub({"C"}), FakeSub({"C"})
    await register(a, node, "F", fast)
    await register(a, node, "S", slow)
    fast.updates = {1: {"id": 1}}
    slow.updates = {2: {"id": 2}}
    slow.gate.clear()

    mq.push_pulled_("C", None)
    async with asyncio.timeout(1):
        await slow.entered.wait()
    await settle()
    assert fast.calls == [("C", None)]
    assert await a.get_updates(timeout=TICK) == {}, "tick 还没结束，不能先交出一部分"
    slow.gate.set()
    async with asyncio.timeout(1):
        assert await a.get_updates() == {"F": {1: {"id": 1}}, "S": {2: {"id": 2}}}
    await close_hub(hub, node)


async def test_undrained_updates_merge_across_ticks():
    """连接没来取（推送阻塞）时，几个 tick 的更新在待发区按 sub_id / row_id 合并，后到的覆盖先到的"""
    hub, (a,), mq, node = make_brokers(1, autostart=True)
    s = FakeSub({"C"})
    await register(a, node, "S", s)
    s.updates = {1: {"v": 1}, 2: {"v": 1}}
    mq.push_pulled_("C", None)
    async with asyncio.timeout(1):
        while not a._outbox:
            await asyncio.sleep(0.001)
    s.updates = {1: {"v": 2}}
    mq.push_pulled_("C", None)
    async with asyncio.timeout(1):
        while a._outbox["S"][1] != {"v": 2}:
            await asyncio.sleep(0.001)
    assert await a.get_updates(timeout=TICK) == {"S": {1: {"v": 2}, 2: {"v": 1}}}
    await close_hub(hub, node)


async def test_unsubscribe_drops_undelivered_updates():
    """已交到待发区、还没被取走的更新，退订后不再推"""
    hub, (a,), mq, node = make_brokers(1, autostart=True)
    s = FakeSub({"C"})
    await register(a, node, "S", s)
    s.updates = {1: {"v": 1}}
    mq.push_pulled_("C", None)
    async with asyncio.timeout(1):
        while not a._outbox:
            await asyncio.sleep(0.001)
    await unsubscribe_and_ack(a, node, "S")
    assert await a.get_updates(timeout=TICK) == {}
    await close_hub(hub, node)


async def test_resubscribe_with_same_id_mid_tick_skips_old_updates():
    """tick 里订阅 S 的更新已暂存、tick 还没结束时，客户端退订 S 又用同一个 sub_id 重订成 S2：
    S 的更新不能当作 S2 的推出去"""
    hub, (a,), mq, node = make_brokers(1, autostart=True)
    s, g = FakeSub({"C"}), FakeSub({"C"})
    await register(a, node, "S", s)
    await register(a, node, "G", g)
    s.updates = {1: {"old": True}}
    g.gate.clear()  # 卡住 G，tick 结束不了

    mq.push_pulled_("C", None)
    async with asyncio.timeout(1):
        await g.entered.wait()
    await settle()
    assert s.calls == [("C", None)]
    await unsubscribe_and_ack(a, node, "S")
    await register(a, node, "S", FakeSub({"D"}))
    g.gate.set()
    assert await a.get_updates(timeout=TICK) == {}
    await close_hub(hub, node)


async def test_manual_hub_runs_one_tick_at_a_time():
    """手动模式下两个门面并发驱动同一个 hub：不会同时跑两个 tick"""
    hub, (a, b), mq, node = make_brokers(2)
    sa, sb = FakeSub({"C1"}), FakeSub({"C2"})
    await register(a, node, "A", sa)
    await register(b, node, "B", sb)
    sa.updates = {1: {"a": 1}}
    sb.updates = {2: {"b": 1}}
    sa.gate.clear()

    mq.push_pulled_("C1", None)
    ta = asyncio.create_task(a.get_updates(timeout=1))
    async with asyncio.timeout(1):
        await sa.entered.wait()
    mq.push_pulled_("C2", None)
    tb = asyncio.create_task(b.get_updates(timeout=1))
    await asyncio.sleep(0.05)
    assert not sb.entered.is_set(), "A 驱动的 tick 还没结束，B 不能另起一个"
    sa.gate.set()
    assert await ta == {"A": {1: {"a": 1}}}
    assert await tb == {"B": {2: {"b": 1}}}
    await close_hub(hub, node)
