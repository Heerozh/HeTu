"""
worker 级订阅器（SubscriptionHub + 每连接的 SubscriptionBroker 门面）在各种协程交错下的行为：
tick 里的 await 间隙与接收协程（客户端的 sub/unsub）交错、几个连接共用一个 MQClient 时的订阅 /
退订 / 取消。不连数据库：订阅对象用可控的替身，mq 走假的 pubsub 节点。
"""

import asyncio
import itertools
import logging
from collections.abc import Mapping
from types import SimpleNamespace
from typing import Any, cast

import pytest
from fixtures.contexts import wait_until
from fixtures.fake_pubsub import (
    FakeNodePubSub,
    attach_fake_node,
    make_hub,
    route_channels,
    settle,
)

from hetu.data.backend import Backend
from hetu.data.backend.redis.mq import RedisMQClient
from hetu.data.sub import (
    BaseSubscription,
    RowSubscription,
    SubscriptionBroker,
    SubscriptionHub,
)


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


class FlakySub(FakeSub):
    """前 fails 次 get_updated 读库出错（模拟 Redis 抖动），之后照常"""

    def __init__(self, channels: set[str], fails: int = 1):
        super().__init__(channels)
        self.fails = fails

    async def get_updated(
        self, channel: str, payload: set[str] | None = None
    ) -> tuple[set[str], set[str], Mapping[int, dict[str, Any] | None]]:
        if self.fails > 0:
            self.fails -= 1
            self.calls.append((channel, payload))
            raise ConnectionError("read failed")
        return await super().get_updated(channel, payload)


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


def open_sub(broker: SubscriptionBroker, sub_id: str, sub: FakeSub) -> asyncio.Task:
    """订阅的前半段（登记，初始化交给 hub）当场做完；返回后半段（等初始化完成）的任务"""
    return asyncio.create_task(broker._finish(broker._open(sub_id, sub)))


async def register(
    broker: SubscriptionBroker, node: FakeNodePubSub, sub_id: str, sub: FakeSub
):
    """照 subscribe_get / subscribe_range 的流程登记一个订阅：前半段登记，hub 里初始化（替身只订上
    频道，没有初始行、不补读），完成时生效"""
    await finish(open_sub(broker, sub_id, sub), node)


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
    hub.SUBSCRIBE_WAIT_INTERVALS = (
        1000  # tick 末尾等 Y 的 ack（interval 1ms，最多等 1 秒）
    )
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


async def test_attach_returns_inactive_until_init_completes():
    """hub.attach_ 返回时（频道已生效）行 / 范围订阅仍未生效（active=False），tick 不处理它：初始化
    完成时才由 hub 置，与把结果交给等着的成员在同一个同步段里，推送都排在成员拿到的结果之后
    （设计稿 2026-09-29 §3.4）"""
    hub, (broker,), mq, node = make_brokers(1)
    sub = FakeSub({"X"})
    await finish(asyncio.create_task(hub.attach_(sub)), node)
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
    attaching = open_sub(b, "R", r)
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
    ta = open_sub(a, "A", sa)
    await wait_sent(node, "subscribe", "X")
    tb = open_sub(b, "B", sb)
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
    hub.SUBSCRIBE_WAIT_INTERVALS = (
        1000  # tick 末尾等 X 的 ack（interval 1ms，最多等 1 秒）
    )
    s = FakeSub({"idx"})
    await register(a, node, "S", s)
    s.new = {"X"}

    p = FakeSub({"X"})
    attaching = open_sub(b, "P", p)
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


async def test_stalled_connection_is_not_read_until_it_drains():
    """
    连接的推送卡住（客户端网络拥塞：ws.send 阻塞，push_queue 满了，没人来 get_updates 取待发区）：
    不再为它读库、比对，通知先攒着，等它取走待发区再重读，推最新的。以前照样每个 tick 读、合并进
    待发区，过载时该卸掉的副本读和 CPU 照常消耗，范围订阅的待发区随行进出无上限增长（dev 不调
    get_updates 就不读）
    """
    hub, (a,), mq, node = make_brokers(1, autostart=True)
    s = FakeSub({"C"})
    await register(a, node, "S", s)
    s.updates = {1: {"v": 1}, 2: {"v": 1}}
    mq.push_pulled_("C", None)
    await wait_until(lambda: a._outbox)  # 交到待发区了，连接卡着没来取
    reads = len(s.calls)
    for v in range(2, 5):
        s.updates = {1: {"v": v}}
        mq.push_pulled_("C", None)
        await asyncio.sleep(0.01)
    assert len(s.calls) == reads, "连接卡着没来取，还在为它读"

    # 连接恢复：先取走卡住前的那批，攒着的通知随即重读，推最新的
    assert await a.get_updates(timeout=TICK) == {"S": {1: {"v": 1}, 2: {"v": 1}}}
    assert await a.get_updates(timeout=TICK) == {"S": {1: {"v": 4}}}
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


async def _deliver_one_update(autostart: bool) -> dict[str, dict]:
    """订一个频道、来一条通知，返回连接拿到的更新：走一遍 hub 的 tick（处理、暂存、交付）"""
    hub, (broker,), mq, node = make_brokers(1, autostart=autostart)
    sub = FakeSub({"C"})
    await register(broker, node, "S", sub)
    sub.updates = {1: {"v": 1}}
    mq.push_pulled_("C", None)
    try:
        return await broker.get_updates(timeout=TICK)
    finally:
        await close_hub(hub, node)


@pytest.mark.parametrize("autostart", [True, False], ids=["loop", "manual"])
def test_tick_runs_on_uvloop(autostart: bool):
    """
    生产在 Linux / macOS 上跑 uvloop（Sanic 默认启用），hub 的 tick 在它上面照常处理、交付。
    Windows 没有 uvloop 跳过（本机的异步用例跑在 conftest 的 UvloopSignatureLoop 上兜着）
    """
    uvloop = pytest.importorskip("uvloop")
    updates = asyncio.run(
        _deliver_one_update(autostart), loop_factory=uvloop.new_event_loop
    )
    assert updates == {"S": {1: {"v": 1}}}


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


# ============ 错误处理（设计稿 §6）：重读，不断开 ============


async def test_failed_read_is_retried_without_raising(caplog):
    """订阅读库出错（Redis 抖动）：不抛给连接、不断开，记日志；一个 interval 后给它定向重读
    这个频道（表级频道带原来的 row_id），这次读成功照常推送"""
    broker, mq, node = make_broker()
    row, table = FlakySub({"R"}), FlakySub({"T"})
    await register(broker, node, "R", row)
    await register(broker, node, "T", table)
    row.updates = {1: {"v": 1}}
    table.updates = {5: {"v": 5}}
    mq.push_pulled_("R", None)
    mq.push_pulled_("T", ["5"])
    with caplog.at_level(logging.ERROR, logger="HeTu.root"):
        updates = await broker.get_updates(timeout=TICK)
    assert updates == {"R": {1: {"v": 1}}, "T": {5: {"v": 5}}}
    assert row.calls == [("R", None), ("R", None)]
    assert table.calls == [("T", {"5"}), ("T", {"5"})]
    assert any(r.levelno >= logging.ERROR for r in caplog.records)
    await close_all(broker, node)


async def test_repeated_read_errors_are_logged_rate_limited(caplog):
    """Redis 挂着时每个 tick 都会出错、都会重试：错误日志限流，间隔内只记一条（带栈），别刷屏"""
    broker, mq, node = make_broker()
    sub = FlakySub({"C"}, fails=5)
    await register(broker, node, "S", sub)
    sub.updates = {1: {"v": 1}}
    mq.push_pulled_("C", None)
    with caplog.at_level(logging.ERROR, logger="HeTu.root"):
        updates = await broker.get_updates(timeout=1)
    assert updates == {"S": {1: {"v": 1}}}
    assert len(sub.calls) == 6
    errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert len(errors) == 1
    assert errors[0].exc_info is not None
    await close_all(broker, node)


async def test_failed_subscribe_of_new_channel_is_repaired():
    """tick 末尾订阅新进入范围的行频道失败：不抛给连接；频道留在频道表里重新入队，之后弹出时
    先补订，订上之后再让订着它的订阅重读"""
    broker, mq, node = make_broker()
    hub = broker._hub
    s = FakeSub({"idx"})
    await register(broker, node, "S", s)
    s.new = {"X"}
    node.fail_next = ConnectionError("send failed")  # 下一次 SUBSCRIBE 发送失败
    mq.push_pulled_("idx", None)
    await finish(asyncio.create_task(broker.get_updates(timeout=TICK)), node)
    # 发送失败的那次没记进 sent，补订的这次记上了
    assert node.sent("subscribe").count("X") == 1
    assert "X" in mq.subscribed_channels
    assert hub._channel_subs["X"] == {s}
    assert ("X", None) in s.calls, "补订之后要重读，失败期间的写入没有通知"
    await close_all(broker, node)


# ============ tick 末尾的订阅 / 退订不能卡住整个 worker 的交付 ============


async def test_tick_delivers_without_waiting_for_unsubscribe_ack():
    """行离开范围、tick 末尾退订没人要的行频道：交付不等 UNSUBSCRIBE 回来。它对交付毫无作用，
    ack 迟迟不来时要等满 UNSUBSCRIBE_ACK_TIMEOUT（5 秒），worker 里所有连接的推送都跟着卡住"""
    broker, mq, node = make_broker()
    s = FakeSub({"idx", "X"})
    await register(broker, node, "S", s)
    s.rem = {"X"}
    s.updates = {1: None}
    mq.push_pulled_("idx", None)
    async with asyncio.timeout(1):
        updates = await broker.get_updates(timeout=TICK)  # UNSUBSCRIBE 一直不 ack
    assert updates == {"S": {1: None}}
    await wait_sent(node, "unsubscribe", "X")  # 退订照常发出，只是交付不等它
    node.ack("unsubscribe", "X")
    await close_all(broker, node)


async def test_released_channel_resubscribed_during_background_unsubscribe():
    """tick 把 X 放出范围、退订放在后台：退订还没回来时接收协程又为 X 登记了行订阅 R。X 最终仍
    订着、已生效，R 收得到 X 的通知"""
    broker, mq, node = make_broker()
    hub = broker._hub
    i1 = FakeSub({"idx", "X"})
    await register(broker, node, "I1", i1)
    i1.rem = {"X"}
    mq.push_pulled_("idx", None)
    assert await broker.get_updates(timeout=TICK) == {}
    await wait_sent(node, "unsubscribe", "X")  # 后台退订已发出，ack 还没回来

    r = FakeSub({"X"})
    await register(broker, node, "R", r)  # 重新发出的 SUBSCRIBE X 由 finish 投递 ack
    node.ack("unsubscribe", "X")
    await settle()
    assert "X" in mq.subscribed_channels and "X" in hub._effective
    assert hub._channel_subs["X"] == {r}
    mq.push_pulled_("X", None)
    await broker.get_updates(timeout=TICK)
    assert ("X", None) in r.calls
    await close_all(broker, node)


async def test_tick_waits_for_subscribe_ack_at_most_an_interval():
    """行进入范围、tick 末尾订阅它的行频道：SUBSCRIBE 的 ack 迟迟不来（比如节点的 pubsub 连接
    半开，要靠 TCP keepalive 才发现）时最多等一个 interval 就交付，不能冻住整个 worker 的推送；
    ack 回来之后照常给新增它的订阅定向补读"""
    broker, mq, node = make_broker()
    s = FakeSub({"idx"})
    await register(broker, node, "S", s)
    s.new = {"X"}
    s.updates = {1: {"v": 1}}
    mq.push_pulled_("idx", None)
    async with asyncio.timeout(1):
        updates = await broker.get_updates(timeout=TICK)  # SUBSCRIBE X 还没 ack
    assert updates == {"S": {1: {"v": 1}}}

    s.new, s.updates = set(), {}
    await wait_sent(node, "subscribe", "X")
    node.ack("subscribe", "X")
    assert await broker.get_updates(timeout=TICK) == {}
    assert ("X", None) in s.calls, "SUBSCRIBE 回来之后要给新增它的订阅定向补读"
    await close_all(broker, node)


async def test_repair_does_not_block_the_tick():
    """补订（此前订阅失败的频道弹出时先补订）不等 SUBSCRIBE 回来：本批别的订阅照常处理、交付，
    补订的频道订上之后再重读"""
    broker, mq, node = make_broker()
    hub = broker._hub
    s = FakeSub({"idx"})
    other = FakeSub({"C"})
    await register(broker, node, "S", s)
    await register(broker, node, "O", other)
    loop = asyncio.get_running_loop()
    async with asyncio.timeout(3):
        # 跑一个 tick：S 新增 X，tick 末尾订阅 X 发送失败，X 按真实频道重新入队
        s.new = {"X"}
        node.fail_next = ConnectionError("send failed")
        mq.push_pulled_("idx", None)
        assert await hub.step_(loop.time() + 1, lambda: False)
        await wait_until(lambda: "X" in mq.pulled_set)
        assert "X" not in mq.subscribed_channels and hub._channel_subs["X"] == {s}

        # X 与 C 的通知同一批弹出：补订 X 的 SUBSCRIBE 一直不 ack，C 照常交付
        s.new = set()
        other.updates = {2: {"v": 2}}
        mq.push_pulled_("C", None)
        await asyncio.sleep(0.01)  # 两项都过了合批窗口
        assert await broker.get_updates(timeout=TICK) == {"O": {2: {"v": 2}}}

        await wait_sent(node, "subscribe", "X")
        node.ack("subscribe", "X")
        assert await broker.get_updates(timeout=TICK) == {}
    assert ("X", None) in s.calls, "补订上之后要重读"
    await close_all(broker, node)


# ============ 几个连接共用 hub 的 MQClient：重叠的订阅不能互相回滚 ============


class OrderedFakeSub(FakeSub):
    """频道按给定顺序订（attach 按 sub.channels 的顺序发，决定先发哪个节点）"""

    def __init__(self, channels: list[str]):
        super().__init__(set(channels))
        self._order = list(channels)

    @property
    def channels(self) -> set[str]:
        return cast(set[str], [ch for ch in self._order if ch in self._channels])


async def ack_until_done(node: FakeNodePubSub, *tasks: asyncio.Task) -> list[Any]:
    """等这些 task 都结束，期间把发出的 SUBSCRIBE/UNSUBSCRIBE 都 ack 掉；返回结果或异常"""
    acked = {mtype: len(node.sent(mtype)) for mtype in ("subscribe", "unsubscribe")}
    async with asyncio.timeout(1):
        while not all(task.done() for task in tasks):
            for mtype, done in acked.items():
                sent = node.sent(mtype)
                for channel in sent[done:]:
                    node.ack(mtype, channel)
                acked[mtype] = len(sent)
            await asyncio.sleep(0.001)
    return list(await asyncio.gather(*tasks, return_exceptions=True))


async def drain_acks(node: FakeNodePubSub):
    """跑一会儿，期间发出的（含后台退订的）SUBSCRIBE/UNSUBSCRIBE 都 ack 掉"""
    await ack_until_done(node, asyncio.create_task(asyncio.sleep(0.05)))


def assert_active_channels_subscribed(hub: SubscriptionHub, mq: RedisMQClient):
    """已登记到门面（active）的订阅，它的频道都真的订着：没有返回成功却收不到通知的订阅"""
    for channel, subs in hub._channel_subs.items():
        if any(sub.active for sub in subs):
            assert channel in mq.subscribed_channels, channel
            assert mq._hub.subscriber_count(channel) == 1, channel


async def test_failed_attach_does_not_revoke_channel_another_attach_waits_on():
    """
    连接 A 订 [C, D]，D 在另一个连不上的节点上；C 的 SUBSCRIBE 已经回来时连接 B 订 C。A 失败回滚
    不能把 B 靠着的 C 一起撤掉、B 却以为订上了，再也收不到 C 的通知：B 要么跟着失败（客户端知道），
    要么 C 真的订着（设计稿 §4.6）
    """
    hub, (a, b), mq, node = make_brokers(2)
    hub.INIT_RETRIES = 0  # A 的订阅一出错就失败（不重试）
    pubsub = cast(Any, mq._hub)._pubsub  # PubSubHub 的 AsyncKeyspacePubSub
    n2 = attach_fake_node(pubsub, "n2")
    route_channels(pubsub, {"D": "n2"})
    n2.gate.clear()  # 连 D 所在的节点：一直连不上
    sa = OrderedFakeSub(["C", "D"])
    ta = open_sub(a, "A", sa)
    await wait_sent(node, "subscribe", "C")
    node.ack("subscribe", "C")
    await settle()
    sb = FakeSub({"C"})
    tb = open_sub(b, "B", sb)
    await settle()

    n2.fail_next = ConnectionError("connect to n2 failed")
    n2.gate.set()
    ra, rb = await ack_until_done(node, ta, tb)
    await drain_acks(node)
    assert isinstance(ra, ConnectionError)
    if isinstance(rb, BaseException):
        assert not sb.active
    else:
        assert sb.active
        assert "C" in mq.subscribed_channels, "B 以为订上了，C 却被 A 的回滚撤掉了"
    assert_active_channels_subscribed(hub, mq)
    await close_hub(hub, node)


async def test_attach_right_after_failed_subscribe_is_not_left_unsubscribed():
    """
    连接 A 订 T 的 SUBSCRIBE 发送失败，A 的回滚还没跑完时连接 B 也来订 T：B 不能返回成功却其实
    没订上（A 回滚的后台退订会把 B 重新发出的那次 SUBSCRIBE 当作成功结算掉）
    """
    hub, (a, b), mq, node = make_brokers(2)
    hub.INIT_RETRIES = 0  # A 的订阅一出错就失败（不重试）
    sa, sb = FakeSub({"T"}), FakeSub({"T"})
    b_attach: list[asyncio.Task] = []
    real_subscribe = node.subscribe

    async def fail_first_and_race(*channels: str):
        if not b_attach:  # A 那次：发送失败的同一刻，B 的订阅请求到了
            b_attach.append(open_sub(b, "B", sb))
            raise ConnectionError("send failed")
        await real_subscribe(*channels)

    node.subscribe = fail_first_and_race  # type: ignore[method-assign]
    ta = open_sub(a, "A", sa)
    async with asyncio.timeout(1):
        while not b_attach:
            await asyncio.sleep(0.001)
    ra, rb = await ack_until_done(node, ta, b_attach[0])
    await drain_acks(node)
    assert isinstance(ra, ConnectionError)
    if isinstance(rb, BaseException):
        assert not sb.active
    else:
        assert sb.active
        assert "T" in mq.subscribed_channels, "B 以为订上了，T 却已被退订"
    assert_active_channels_subscribed(hub, mq)
    await close_hub(hub, node)


async def test_repair_waits_for_subscribe_to_take_effect_before_reading():
    """
    tick 末尾订阅新行 X 失败、X 重新入队；弹出前连接 B 订 X，SUBSCRIBE 发出去还没回来。补订看的
    不能是"SUBSCRIBE 发没发"（MQClient.subscribed）：那样 S 当场就读 X，早于任何订阅生效，其间的
    写入没有通知；B 的 SUBSCRIBE 随后失败的话 X 就成了没人订、队列里也没有的孤儿，S 的客户端一直
    持有旧行。要等 X 真的订上之后再读（设计稿 §6）
    """
    hub, (a, b), mq, node = make_brokers(2)
    hub.INIT_RETRIES = 0  # B 的订阅一出错就失败（不重试）
    s = FakeSub({"idx"})
    await register(a, node, "S", s)
    loop = asyncio.get_running_loop()

    async def one_tick():
        await asyncio.sleep(0.005)  # 队列里的项都过了合批窗口
        await hub.step_(loop.time() + 0.05, lambda: False)

    async with asyncio.timeout(3):
        # tick N：S 新增 X，tick 末尾订阅 X 发送失败，X 按真实频道重新入队
        s.new = {"X"}
        node.fail_next = ConnectionError("send failed")
        mq.push_pulled_("idx", None)
        await one_tick()
        await wait_until(lambda: "X" in mq.pulled_set)
        s.new = set()

        # 连接 B 订 X：SUBSCRIBE 卡在发送上（之后也会失败）
        node.gate.clear()
        node.entered.clear()
        node.fail_next = ConnectionError("send failed again")
        tb = open_sub(b, "P", FakeSub({"X"}))
        await node.entered.wait()
        assert "X" in mq.subscribed_channels  # 发出去了，但还没生效

        # tick N+1：弹出 X，S 不能现在就读
        await one_tick()
        assert ("X", None) not in s.calls, "X 的订阅还没生效就读了"

        # B 的 SUBSCRIBE 失败：X 只剩 S 在要，要补订上，生效之后 S 再读
        node.gate.set()
        with pytest.raises(ConnectionError):
            await tb
        acked = 0
        while ("X", None) not in s.calls:
            sent = node.sent("subscribe")
            for channel in sent[acked:]:
                node.ack("subscribe", channel)
            acked = len(sent)
            await one_tick()
    assert "X" in mq.subscribed_channels and "X" in hub._effective
    await close_hub(hub, node)


async def test_repeated_read_errors_back_off():
    """
    同一个订阅接连读出错（副本挂着、坏数据）：重试间隔指数退避（1、2、4… 个 interval，封顶），
    别每个 interval 都去打同一个出错的节点；读成功后恢复正常节奏
    """
    broker, mq, node = make_broker()
    mq.UPDATE_FREQUENCY = 20  # type: ignore[reportAttributeAccessIssue]  interval 50ms
    sub = FlakySub({"C"}, fails=4)
    await register(broker, node, "S", sub)
    sub.updates = {1: {"v": 1}}
    loop = asyncio.get_running_loop()
    stamps: list[float] = []
    real = sub.get_updated

    async def stamped(channel, payload=None):
        stamps.append(loop.time())
        return await real(channel, payload)

    sub.get_updated = stamped  # type: ignore[method-assign]
    mq.push_pulled_("C", None)
    async with asyncio.timeout(5):
        assert await broker.get_updates(timeout=5) == {"S": {1: {"v": 1}}}
    assert len(stamps) == 5
    gaps = [b - a for a, b in itertools.pairwise(stamps)]
    # 第 n 次失败后隔 2^(n-1) 个 interval 再读（call_later 可能早触发一个时钟精度，留余量）
    assert gaps[2] >= 0.15 and gaps[3] >= 0.35, gaps
    await close_all(broker, node)


class _TableRef:
    """预读只用到 table_ref 的 comp_cls.is_rls() 和可哈希"""

    class comp_cls:
        @staticmethod
        def is_rls() -> bool:
            return False

    def __init__(self, name: str):
        self.comp_name = name


async def test_prefetch_reads_tables_concurrently():
    """一批通知涉及几张表：各表的预读 get_many 并发发出，不按表串行排队（否则 worker 里每个连接的
    交付都要等所有表的往返加起来）"""
    in_flight = peak = 0

    class SlowServant:
        async def get_many(self, ref, row_ids, row_format):
            nonlocal in_flight, peak
            in_flight += 1
            peak = max(peak, in_flight)
            await asyncio.sleep(0.02)  # 一次往返
            in_flight -= 1
            return [{"id": row_id} for row_id in row_ids]

    servant = SlowServant()
    pubsub_hub, node = make_hub()
    mq = RedisMQClient(pubsub_hub)
    backend = cast(Backend, SimpleNamespace(get_mq_client=lambda: mq, servant=servant))
    hub = SubscriptionHub(backend, autostart=False)
    work: dict[BaseSubscription, list[tuple[str, set[str] | None]]] = {}
    for i in range(3):
        ref = cast(Any, _TableRef(f"T{i}"))
        work[RowSubscription(ref, cast(Any, servant), None, f"row{i}", i)] = [
            (f"row{i}", None)
        ]
    RowSubscription.reset_cache_()
    await hub._prefetch_rows(work)
    assert peak == 3, "各表的预读串行了"
    await close_hub(hub, node)


async def test_new_channel_gets_reread_when_its_subscribe_lands_mid_tick():
    """
    tick 里订阅 S 读了新进入范围的行 X；别的连接订 X 的 SUBSCRIBE 在 S 读完之后、tick 结束之前
    回来。S 读 X 在前、X 生效在后，其间的写入没有通知，仍要给 S 定向补读：fresh 要按 tick 开始时
    的生效状态判断，不能按 tick 末尾（设计稿 §4.4）
    """
    hub, (a, b), mq, node = make_brokers(2)
    s = FakeSub({"idx"})
    await register(a, node, "S", s)
    s.new = {"X"}
    s.gate.clear()  # S 读完、还没返回
    mq.push_pulled_("idx", None)
    tick = asyncio.create_task(a.get_updates(timeout=TICK))
    async with asyncio.timeout(1):
        await s.entered.wait()
    await register(b, node, "P", FakeSub({"X"}))  # X 的 SUBSCRIBE 这时回来，生效
    assert "X" in hub._effective
    s.gate.set()
    await finish(tick, node)
    s.new = set()
    await finish(asyncio.create_task(a.get_updates(timeout=TICK)), node)
    assert ("X", None) in s.calls, "X 在 S 读过之后才生效，要给 S 定向补读"
    await close_hub(hub, node)


# ============ tick 里逃出 _process 的异常（bug）：不能留下半截的 tick ============


class BadStageSub(FakeSub):
    """get_updated 正常返回，但暂存它的更新时出错（模拟 _process 里 get_updated 之外的 bug）"""


def break_stage_for(hub: SubscriptionHub, bad: BaseSubscription):
    real = hub._stage

    def stage(tick, sub, updates):
        if sub is bad:
            raise RuntimeError("bug in bookkeeping")
        return real(tick, sub, updates)

    hub._stage = stage  # type: ignore[method-assign]


def hub_errors(caplog) -> list[str]:
    return [r.getMessage() for r in caplog.records if r.name == "HeTu.root"]


async def test_escaped_error_in_eager_task_is_logged(caplog):
    """
    缓存命中、当场跑完的订阅（eager task）处理时逃出异常：要记日志，本 tick 别的订阅照常交付
    （以前 eager 完成的 task 不进 pending，它的异常没人取，悄无声息地丢了）
    """
    broker, mq, node = make_broker()
    hub = broker._hub
    good, bad = FakeSub({"C"}), BadStageSub({"C"})
    await register(broker, node, "G", good)
    await register(broker, node, "B", bad)
    good.updates, bad.updates = {1: {"v": 1}}, {2: {"v": 2}}
    break_stage_for(hub, bad)
    mq.push_pulled_("C", None)
    with caplog.at_level(logging.ERROR, logger="HeTu.root"):
        updates = await finish(
            asyncio.create_task(broker.get_updates(timeout=TICK)), node
        )
    assert updates == {"G": {1: {"v": 1}}}
    assert any("bug in bookkeeping" in msg for msg in hub_errors(caplog))
    await close_all(broker, node)


async def test_escaped_error_in_pending_task_does_not_abort_the_tick(caplog):
    """
    要读库的订阅（挂起的 task）处理时逃出异常：不能中止整个 tick。别的还在读的订阅等它们跑完、
    照常交付，tick 末尾照常订上新增的频道（以前 gather 一抛出 tick 就结束了：_settle_channels 被
    跳过，新行的频道永远订不上；没跑完的 task 之后把更新暂存进已经交付过的 tick，推送丢了、
    指纹却已推进）
    """
    broker, mq, node = make_broker()
    hub = broker._hub
    slow, bad = FakeSub({"C"}), BadStageSub({"C"})
    await register(broker, node, "S", slow)
    await register(broker, node, "B", bad)
    slow.new, slow.updates = {"Y"}, {1: {"v": 1}}
    bad.updates = {2: {"v": 2}}
    slow.gate.clear()
    bad.gate.clear()
    break_stage_for(hub, bad)
    mq.push_pulled_("C", None)
    tick = asyncio.create_task(broker.get_updates(timeout=1))
    async with asyncio.timeout(1):
        await slow.entered.wait()
        await bad.entered.wait()
    bad.gate.set()  # 出错的那个先跑完
    await settle()
    slow.gate.set()
    with caplog.at_level(logging.ERROR, logger="HeTu.root"):
        assert await finish(tick, node) == {"S": {1: {"v": 1}}}
    await wait_sent(node, "subscribe", "Y")
    node.ack("subscribe", "Y")
    await wait_until(lambda: "Y" in hub._effective)
    assert any("bug in bookkeeping" in msg for msg in hub_errors(caplog))
    await close_all(broker, node)


async def test_processing_loop_restarts_after_it_dies(monkeypatch):
    """后台处理循环因 bug 意外结束：过一会儿自己重新拉起，不用等下一次有连接订阅"""
    monkeypatch.setattr("hetu.data.sub._RUN_RESTART_DELAY", 0.01, raising=False)
    hub, (a,), mq, node = make_brokers(1, autostart=True)
    s = FakeSub({"C"})
    await register(a, node, "S", s)
    real = mq.get_message
    broken = []

    async def get_message_once_broken():
        if not broken:
            broken.append(1)
            raise RuntimeError("bug in the loop")
        return await real()

    mq.get_message = get_message_once_broken  # type: ignore[method-assign]
    first = hub._task
    assert first is not None
    hub._task = None  # 按新的 get_message 重新起一个，让它死掉
    first.cancel()
    await asyncio.wait([first])
    hub._start()
    dead = hub._task
    assert dead is not None
    async with asyncio.timeout(1):
        await asyncio.wait([dead])
    s.updates = {1: {"v": 1}}
    mq.push_pulled_("C", None)
    async with asyncio.timeout(1):
        assert await a.get_updates(timeout=1) == {"S": {1: {"v": 1}}}
    await close_hub(hub, node)


async def test_log_error_survives_broken_translation(monkeypatch, caplog):
    """翻译出来的日志模板占位符对不上（.po 由 CD 机翻同步）：记日志不能抛，退回原文"""
    hub, _brokers, _mq, _node = make_brokers(0)
    monkeypatch.setattr("hetu.data.sub._", lambda s: "{bogus} " + s)
    with caplog.at_level(logging.ERROR, logger="HeTu.root"):
        hub._log_error("出错了", RuntimeError("boom"))
    assert any("boom" in r.getMessage() for r in caplog.records)


# ============ 拆连接：先撤内部关注，两个退订并发；关闭后不再接受订阅 / 关注 ============


def make_broker_with_watch() -> tuple[
    SubscriptionHub, SubscriptionBroker, RedisMQClient, FakeNodePubSub
]:
    """hub 与门面的内部关注（watch_channel）各用一个 MQClient，挂在同一个假 pubsub 上（同生产）"""
    pubsub_hub, node = make_hub()
    hub_mq = RedisMQClient(pubsub_hub)
    hub_mq.UPDATE_FREQUENCY = 1000  # type: ignore[reportAttributeAccessIssue]
    clients = iter([hub_mq])
    backend = cast(
        Backend,
        SimpleNamespace(
            get_mq_client=lambda: next(clients, None) or RedisMQClient(pubsub_hub),
            servant=None,
        ),
    )
    hub = SubscriptionHub(backend, autostart=False)
    return hub, SubscriptionBroker(backend, hub=hub), hub_mq, node


def notify(node: FakeNodePubSub, channel: str):
    node.inbox.put_nowait({"type": "message", "channel": channel.encode(), "data": b""})


async def test_close_disarms_watch_before_waiting_for_unsubscribe():
    """
    拆连接时先撤内部关注：等订阅退订回来的期间同一用户在别处登录（owner 值频道来了通知），
    顶号回调不能再触发（它会去 master 核一次，结果随即因为在拆被丢掉，白读一次 master）
    """
    hub, broker, _mq, node = make_broker_with_watch()
    fired: list[None] = []
    await finish(
        asyncio.create_task(broker.watch_channel("W", lambda: fired.append(None))),
        node,
    )
    await register(broker, node, "S", FakeSub({"X"}))
    closing = asyncio.create_task(broker.close())
    await wait_sent(node, "unsubscribe", "X")  # 退订发出去了，ack 还没回来
    notify(node, "W")
    await settle()
    assert fired == [], "拆连接期间顶号回调还挂着"
    for channel in node.sent("unsubscribe"):
        node.ack("unsubscribe", channel)
    await ack_until_done(node, closing)
    await close_hub(hub, node)


async def test_close_unsubscribes_concurrently():
    """拆连接时订阅的退订与内部关注的退订并发发出，只等一个往返（以前串行，最坏各等满 5 秒）"""
    hub, broker, _mq, node = make_broker_with_watch()
    await finish(asyncio.create_task(broker.watch_channel("W", lambda: None)), node)
    await register(broker, node, "S", FakeSub({"X"}))
    closing = asyncio.create_task(broker.close())
    async with asyncio.timeout(1):
        while not {"X", "W"} <= set(node.sent("unsubscribe")):
            await asyncio.sleep(0.001)  # 两个都发出了，都还没 ack
    for channel in node.sent("unsubscribe"):
        node.ack("unsubscribe", channel)
    await ack_until_done(node, closing)
    await close_hub(hub, node)


async def test_closed_broker_rejects_watch_and_subscribe():
    """关闭之后再关注 / 订阅直接拒绝：不能新建一个永不关闭的 MQClient 并登记回调，也不用先订上再撤"""
    hub, broker, _mq, node = make_broker_with_watch()
    await close_all(broker, node)
    async with asyncio.timeout(1):  # 以前会订上去、一直等 ack
        with pytest.raises(ConnectionError):
            await broker.watch_channel("W", lambda: None)
        with pytest.raises(ConnectionError):
            broker._open("S", FakeSub({"X"}))
    assert node.sent("subscribe") == []
    assert broker._watch_mq is None
    await close_hub(hub, node)


# ============ worker 级的处理循环不能继承第一个连接的 contextvars ============


async def test_hub_loop_does_not_inherit_the_first_connection_context():
    """
    处理循环在第一个连接的协程里懒建（SubscriptionHub.of）：不能继承那个连接的 contextvars。
    否则此后整个 worker 的订阅日志都带着这个连接的身份（id / IP），它的 Request 也一直被 hub 任务
    的 Context 引用着释放不掉；循环因故重新拉起时同理
    """
    from hetu.safelogging.filter import log_contex_var

    first_conn = "[1001|203.0.113.7|0]"
    token = log_contex_var.set(first_conn)  # 第一个连接的日志上下文
    try:
        hub, (a,), mq, node = make_brokers(1, autostart=True)
        s = FakeSub({"C"})
        await register(a, node, "S", s)
        assert hub._task is not None
        assert hub._task.get_context().get(log_contex_var) != first_conn

        seen: list[str] = []
        real = s.get_updated

        async def spy(channel, payload=None):
            seen.append(log_contex_var.get())
            return await real(channel, payload)

        s.get_updated = spy  # type: ignore[method-assign]
        mq.push_pulled_("C", None)
        await a.get_updates(timeout=TICK)
        assert seen and seen[0] != first_conn, "hub 的日志带着第一个连接的身份"

        # 循环因故结束、下一次（另一个连接的）attach 重新拉起：同样不继承
        old = hub._task
        old.cancel()
        await asyncio.wait([old])
        await register(a, node, "S2", FakeSub({"D"}))
        assert hub._task is not old
        assert hub._task.get_context().get(log_contex_var) != first_conn
    finally:
        log_contex_var.reset(token)
    await close_hub(hub, node)


# ============ 错误日志：按类别限流，静默后补一条恢复日志 ============


def hub_records(caplog, level: int = logging.ERROR) -> list[logging.LogRecord]:
    return [r for r in caplog.records if r.name == "HeTu.root" and r.levelno >= level]


async def test_new_kind_of_error_is_not_muted_by_a_chronic_one(caplog):
    """
    一个订阅持续出错（每个 interval 重试一次）时又冒出另一类错误：新的那类要照样记下来（带栈），
    不能因为限流窗口被慢性错误占着就只算进"另有 N 次出错未记"
    """
    broker, mq, node = make_broker()
    chronic = FlakySub({"C"}, fails=10**6)
    await register(broker, node, "S", chronic)
    mq.push_pulled_("C", None)
    with caplog.at_level(logging.ERROR, logger="HeTu.root"):
        await broker.get_updates(timeout=0.05)  # 慢性错误开始刷

        class OtherError(Exception):
            pass

        other = FakeSub({"D"})

        async def fail_otherwise(channel, payload=None):
            raise OtherError("a different failure")

        other.get_updated = fail_otherwise  # type: ignore[method-assign]
        await register(broker, node, "O", other)
        mq.push_pulled_("D", None)
        await broker.get_updates(timeout=0.05)
    messages = [r.getMessage() for r in hub_records(caplog)]
    assert any("read failed" in m for m in messages)
    new_kind = [
        r for r in hub_records(caplog) if "a different failure" in r.getMessage()
    ]
    assert new_kind, "新的一类错误被慢性错误的限流窗口吞了"
    assert new_kind[0].exc_info is not None
    await close_all(broker, node)


async def test_errors_that_stop_get_a_recovery_log(caplog, monkeypatch):
    """出错停下来之后补一条恢复日志，带上最后一条日志之后压下的次数（同 SQLite 轮询）"""
    monkeypatch.setattr("hetu.data.sub._ERROR_LOG_INTERVAL", 0.1)
    broker, mq, node = make_broker()
    sub = FlakySub({"C"}, fails=5)
    await register(broker, node, "S", sub)
    sub.updates = {1: {"v": 1}}
    mq.push_pulled_("C", None)
    with caplog.at_level(logging.INFO, logger="HeTu.root"):
        assert await broker.get_updates(timeout=1) == {"S": {1: {"v": 1}}}
        await asyncio.sleep(0.3)  # 静默超过一个限流间隔
    recovered = [
        r.getMessage()
        for r in hub_records(caplog, logging.INFO)
        if r.levelno < logging.ERROR and "✅" in r.getMessage()
    ]
    assert recovered, "出错停了没有恢复日志"
    await close_all(broker, node)


async def test_loop_fallback_log_does_not_claim_requeue(caplog):
    """处理循环兜底记的 tick 异常：那批通知已经弹出、丢了，日志不能说"已重新入队稍后重试\""""
    hub, (a,), mq, node = make_brokers(1, autostart=True)
    await register(a, node, "S", FakeSub({"C"}))
    real = hub._collect
    broken: list[None] = []

    def collect_once_broken(batch):
        if not broken:
            broken.append(None)
            raise RuntimeError("bug in collect")
        return real(batch)

    hub._collect = collect_once_broken  # type: ignore[method-assign]
    with caplog.at_level(logging.ERROR, logger="HeTu.root"):
        mq.push_pulled_("C", None)
        await wait_until(lambda: broken)
        await asyncio.sleep(0.01)
    messages = [r.getMessage() for r in hub_records(caplog)]
    assert any("bug in collect" in m for m in messages)
    assert not any("已重新入队" in m for m in messages if "bug in collect" in m)
    await close_hub(hub, node)


async def test_broker_warns_when_a_connection_subscribes_too_many_channels(caplog):
    """
    单个连接订阅的频道数超过 MAX_SUBSCRIBED 时告警（只告警）。告警在门面按连接算：订阅都走 worker
    级订阅器的一个 MQClient，它订的是整个 worker 的频道，别的连接订得再多也不算这个连接的
    """
    hub, (a, b), _mq, node = make_brokers(2)
    a.MAX_SUBSCRIBED = 2
    with caplog.at_level(logging.WARNING, logger="HeTu.root"):
        await register(b, node, "B", FakeSub({"X", "Y", "Z"}))
        await register(a, node, "A1", FakeSub({"C1", "C2"}))
        assert not [r for r in caplog.records if "MAX_SUBSCRIBED" in r.getMessage()]
        await register(a, node, "A2", FakeSub({"C3"}))
    warns = [r for r in caplog.records if "MAX_SUBSCRIBED" in r.getMessage()]
    assert len(warns) == 1
    await close_hub(hub, node)


# ============ 订阅在 hub 里初始化（设计稿 2026-09-29 §3.3、§3.4） ============


class SlowInitSub(FakeSub):
    """初始化订上频道后卡在"读初始行"上，放行后回一行"""

    def __init__(self, channels: set[str]):
        super().__init__(channels)
        self.reading = asyncio.Event()
        self.release = asyncio.Event()
        self.init_cancelled = False

    async def initialize_(self, hub: SubscriptionHub) -> list[dict[str, Any]] | None:
        await hub.attach_(self)
        self.reading.set()
        try:
            await self.release.wait()
        except asyncio.CancelledError:
            self.init_cancelled = True
            raise
        return [{"id": 1}]


class FailingInitSub(FakeSub):
    """初始化总是读出错（副本挂着）；记下重试前换过的副本"""

    def __init__(self, channels: set[str]):
        super().__init__(channels)
        self.attempts = 0
        self.servants: list[Any] = []

    def use_servant_(self, servant) -> None:
        self.servants.append(servant)

    async def initialize_(self, hub: SubscriptionHub) -> list[dict[str, Any]] | None:
        self.attempts += 1
        await hub.attach_(self)
        raise ConnectionError("replica down")


async def test_sub_is_not_processed_until_its_init_completes():
    """初始化期间（频道已订上、初始行还没读回）订阅不生效：tick 跳过它的通知，不会有推送抢在回复
    前面；初始化完成后照常处理"""
    broker, mq, node = make_broker()
    s = SlowInitSub({"X"})
    second = open_sub(broker, "S", s)
    await finish(asyncio.create_task(s.reading.wait()), node)
    assert "X" in mq.subscribed_channels and not s.active
    mq.push_pulled_("X", None)
    assert await broker.get_updates(timeout=TICK) == {}
    assert s.calls == []

    s.release.set()
    async with asyncio.timeout(1):
        assert await second == [{"id": 1}]
    assert s.active
    s.updates = {1: {"v": 1}}
    mq.push_pulled_("X", None)
    assert await broker.get_updates(timeout=TICK) == {"S": {1: {"v": 1}}}
    await close_all(broker, node)


async def test_leaving_during_init_cancels_it():
    """初始化期间成员走了（客户端 unsub）：后半段当场回 None，不用等初始化；成员撤空时初始化取消、
    占位撤掉、频道退订"""
    broker, mq, node = make_broker()
    hub = broker._hub
    s = SlowInitSub({"X"})
    second = open_sub(broker, "S", s)
    await finish(asyncio.create_task(s.reading.wait()), node)
    await unsubscribe_and_ack(broker, node, "S")
    async with asyncio.timeout(1):
        assert await second is None
    assert s.init_cancelled and s.closed and not s.active
    assert hub._channel_subs == {}
    assert "X" not in mq.subscribed_channels
    assert broker.count() == (0, 0, 0)
    await close_all(broker, node)


async def test_init_retries_a_failed_subscribe():
    """初始化时 SUBSCRIBE 出错（节点抖动）：hub 退避重试，回复照常到达（以前当场失败、断开连接）"""
    broker, mq, node = make_broker()
    node.fail_next = ConnectionError("send failed")
    s = FakeSub({"X"})
    assert await finish(open_sub(broker, "S", s), node) == []
    assert s.active and "X" in mq.subscribed_channels
    assert node.sent("subscribe") == ["X"]  # 失败的那次没记进 sent
    await close_all(broker, node)


async def test_init_gives_up_after_retries():
    """初始化一直出错：每次重试前换一个副本、退避，重试 INIT_RETRIES 次仍失败才让等着的成员失败；
    订阅、占位、频道都撤干净"""
    broker, mq, node = make_broker()
    hub = broker._hub
    hub.INIT_RETRIES = 2
    s = FailingInitSub({"X"})
    (result,) = await ack_until_done(node, open_sub(broker, "S", s))
    await drain_acks(node)
    assert isinstance(result, ConnectionError)
    assert s.attempts == 3
    assert len(s.servants) == 2
    assert "S" not in broker._subs and broker.count() == (0, 0, 0)
    assert hub._channel_subs == {}
    assert "X" not in mq.subscribed_channels
    await close_all(broker, node)


# ============ 同一查询共享一个订阅：加入、离开、背压（设计稿 2026-09-29 §3.2、§3.5、§3.7） ============

KEY = ("K",)


def share_sub(
    broker: SubscriptionBroker, sub_id: str, make, key: tuple = KEY
) -> tuple[BaseSubscription, asyncio.Task]:
    """按共享键找或建订阅并加入（前半段），返回订阅和后半段的任务"""
    p = broker._subscribe(sub_id, key, make)
    return p.sub, asyncio.create_task(broker._finish(p))


async def shared_by(
    node: FakeNodePubSub, *brokers: SubscriptionBroker, sub: FakeSub
) -> FakeSub:
    """这些连接都以 sub_id "S" 加入同一个共享订阅（第一个建它），等初始化完成"""
    for broker in brokers:
        joined, task = share_sub(broker, "S", lambda: sub)
        assert joined is sub
        await finish(task, node)
    return sub


async def test_joiner_after_staging_gets_the_update_only_in_its_snapshot():
    """
    tick 中途加入共享订阅，它这次的更新已经暂存、tick 还没结束：快照里已含这次更新，交付时不会
    再收到一次；已有成员照常收到
    """
    hub, (a, b), mq, node = make_brokers(2)
    s = await shared_by(node, a, sub=FakeSub({"C"}))
    g = FakeSub({"C"})
    await register(a, node, "G", g)  # 同频道的另一个订阅，卡住 tick 的结尾
    s.updates = {1: {"id": 1, "v": 1}}
    g.gate.clear()
    mq.push_pulled_("C", None)
    tick = asyncio.create_task(a.get_updates(timeout=TICK))
    async with asyncio.timeout(1):
        await g.entered.wait()
    await settle()
    assert s.calls == [("C", None)]  # S 这次已处理、暂存
    joined, task = share_sub(b, "S", lambda: FakeSub({"C"}))
    assert joined is s
    async with asyncio.timeout(1):
        assert await task == [{"id": 1, "v": 1}]
    g.gate.set()
    assert await tick == {"S": {1: {"id": 1, "v": 1}}}
    assert await b.get_updates(timeout=TICK) == {}, "快照里已有的更新又交付了一次"
    await close_hub(hub, node)


async def test_joiner_while_notification_is_read_gets_the_update():
    """tick 中途加入共享订阅，这次的更新还没暂存（订阅还在读库）：快照里没有，交付时收到"""
    hub, (a, b), mq, node = make_brokers(2)
    s = await shared_by(node, a, sub=FakeSub({"C"}))
    s.updates = {1: {"id": 1, "v": 1}}
    s.gate.clear()
    mq.push_pulled_("C", None)
    tick = asyncio.create_task(a.get_updates(timeout=TICK))
    async with asyncio.timeout(1):
        await s.entered.wait()
    joined, task = share_sub(b, "S", lambda: FakeSub({"C"}))
    assert joined is s
    async with asyncio.timeout(1):
        assert await task == []
    s.gate.set()
    assert await tick == {"S": {1: {"id": 1, "v": 1}}}
    assert await b.get_updates(timeout=TICK) == {"S": {1: {"id": 1, "v": 1}}}
    await close_hub(hub, node)


async def test_last_member_leaving_closes_shared_subscription():
    """成员一个个走：还有成员时订阅照旧；最后一个走了订阅关闭、从共享登记撤掉、频道退订。之后再订
    同一查询是新建的订阅"""
    hub, (a, b), mq, node = make_brokers(2)
    s = await shared_by(node, a, b, sub=FakeSub({"C"}))
    assert set(s.members) == {a, b}
    await unsubscribe_and_ack(a, node, "S")
    assert not s.closed and hub.shared_(KEY) is s
    assert "C" in mq.subscribed_channels
    await unsubscribe_and_ack(b, node, "S")
    assert s.closed and hub.shared_(KEY) is None
    assert "C" not in mq.subscribed_channels and hub._channel_subs == {}

    again = FakeSub({"C"})
    assert await shared_by(node, a, sub=again) is again
    await close_hub(hub, node)


async def test_shared_init_goes_on_until_every_member_leaves():
    """初始化期间：有成员走了（它的后半段当场回 None），初始化照常给别的成员；成员全走了才取消"""
    hub, (a, b), mq, node = make_brokers(2)
    s = SlowInitSub({"X"})
    _, ta = share_sub(a, "S", lambda: s)
    joined, tb = share_sub(b, "S", lambda: SlowInitSub({"X"}))
    assert joined is s
    await finish(asyncio.create_task(s.reading.wait()), node)
    await unsubscribe_and_ack(a, node, "S")
    async with asyncio.timeout(1):
        assert await ta is None
    assert not s.init_cancelled and not tb.done()
    s.release.set()
    async with asyncio.timeout(1):
        assert await tb == [{"id": 1}]
    assert s.active and set(s.members) == {b}

    t = SlowInitSub({"Y"})
    _, tc = share_sub(a, "T", lambda: t, key=("K2",))
    joined, td = share_sub(b, "T", lambda: SlowInitSub({"Y"}), key=("K2",))
    assert joined is t
    await finish(asyncio.create_task(t.reading.wait()), node)
    await unsubscribe_and_ack(a, node, "T")
    await unsubscribe_and_ack(b, node, "T")
    assert await tc is None and await td is None
    assert t.init_cancelled and t.closed
    assert "Y" not in mq.subscribed_channels
    await close_hub(hub, node)


async def test_join_rereads_what_stalled_members_parked():
    """
    共享订阅的成员推送全都卡着：先不读、攒着通知（按订阅攒）。有新成员加入时它没卡着，攒着的通知
    全部定向重读，新成员拿到的快照接着动；卡着的老成员待发区按 row_id 合并
    """
    hub, (a, b), mq, node = make_brokers(2, autostart=True)
    s = await shared_by(node, a, sub=FakeSub({"C"}))
    s.updates = {1: {"id": 1, "v": 1}}
    mq.push_pulled_("C", None)
    await wait_until(lambda: a._outbox)  # 交到待发区了，a 卡着没来取
    reads = len(s.calls)
    s.updates = {1: {"id": 1, "v": 2}}
    mq.push_pulled_("C", None)
    await asyncio.sleep(0.02)
    assert len(s.calls) == reads, "成员全都卡着，还在读"

    joined, task = share_sub(b, "S", lambda: FakeSub({"C"}))
    assert joined is s
    assert await task == [{"id": 1, "v": 1}]
    assert await b.get_updates(timeout=TICK) == {"S": {1: {"id": 1, "v": 2}}}
    assert await a.get_updates(timeout=TICK) == {"S": {1: {"id": 1, "v": 2}}}
    await close_hub(hub, node)


async def test_any_member_draining_rereads_parked_notifications():
    """全员卡着时攒下的通知：任何一个成员取走待发区就全部重读，卡着的成员待发区按 row_id 合并"""
    hub, (a, b), mq, node = make_brokers(2, autostart=True)
    s = await shared_by(node, a, b, sub=FakeSub({"C"}))
    s.updates = {1: {"id": 1, "v": 1}}
    mq.push_pulled_("C", None)
    await wait_until(lambda: a._outbox and b._outbox)  # 都交到了，都卡着
    reads = len(s.calls)
    s.updates = {1: {"id": 1, "v": 2}}
    mq.push_pulled_("C", None)
    await asyncio.sleep(0.02)
    assert len(s.calls) == reads, "成员全都卡着，还在读"

    assert await a.get_updates(timeout=TICK) == {"S": {1: {"id": 1, "v": 1}}}
    assert await a.get_updates(timeout=TICK) == {"S": {1: {"id": 1, "v": 2}}}
    assert await b.get_updates(timeout=TICK) == {"S": {1: {"id": 1, "v": 2}}}
    await close_hub(hub, node)


async def test_draining_rereads_only_its_own_parked_subscriptions():
    """
    攒着的通知按成员连接索引：连接取走待发区时只重读它在的、攒着通知的订阅（不扫它的全部订阅），
    别的连接的照旧攒着；成员退订时撤掉它的索引，不留引用
    """
    hub, (a, b), mq, node = make_brokers(2, autostart=True)
    s1, s2 = FakeSub({"C1"}), FakeSub({"C2"})
    await register(a, node, "S1", s1)
    await register(b, node, "S2", s2)
    s1.updates = {1: {"v": 1}}
    s2.updates = {2: {"v": 1}}
    mq.push_pulled_("C1", None)
    mq.push_pulled_("C2", None)
    await wait_until(lambda: a._outbox and b._outbox)  # 都交到了，都卡着
    s1.updates = {1: {"v": 2}}
    s2.updates = {2: {"v": 2}}
    mq.push_pulled_("C1", None)
    mq.push_pulled_("C2", None)
    await wait_until(lambda: s1 in hub._parked and s2 in hub._parked)
    assert hub._parked_by == {a: {s1}, b: {s2}}

    assert await a.get_updates(timeout=TICK) == {"S1": {1: {"v": 1}}}
    assert await a.get_updates(timeout=TICK) == {"S1": {1: {"v": 2}}}
    assert s2 in hub._parked and hub._parked_by == {b: {s2}}
    await unsubscribe_and_ack(b, node, "S2")
    assert not hub._parked and not hub._parked_by
    await close_hub(hub, node)


async def test_unsubscribe_emptying_outbox_rereads_parked_notifications():
    """
    连接卡着（待发区有 S1 的更新没人取）时 S2 的通知攒着没读；客户端退订 S1 清空了待发区，连接不算卡着了，
    S2 攒着的通知要重读、推出去。否则只有取走待发区时才重读，而待发区已经空了，S2 的这次变动一直推不出去
    """
    hub, (a,), mq, node = make_brokers(1, autostart=True)
    s1, s2 = FakeSub({"C1"}), FakeSub({"C2"})
    await register(a, node, "S1", s1)
    await register(a, node, "S2", s2)
    s1.updates = {1: {"v": 1}}
    mq.push_pulled_("C1", None)
    await wait_until(lambda: a._outbox)  # 交到了，没人取：卡着
    s2.updates = {2: {"v": 1}}
    mq.push_pulled_("C2", None)
    await wait_until(lambda: s2 in hub._parked)

    await unsubscribe_and_ack(a, node, "S1")
    assert not a._outbox
    assert await a.get_updates(timeout=TICK) == {"S2": {2: {"v": 1}}}
    await close_hub(hub, node)


class ScriptedSub(FakeSub):
    """按频道返回预设的更新（script）；gates 里的频道卡在各自的闸门上，进入时置 reached[频道]"""

    def __init__(
        self, channels: set[str], script: dict[str, dict[int, dict[str, Any] | None]]
    ):
        super().__init__(channels)
        self.script = script
        self.gates: dict[str, asyncio.Event] = {}
        self.reached = {channel: asyncio.Event() for channel in channels}

    async def get_updated(
        self, channel: str, payload: set[str] | None = None
    ) -> tuple[set[str], set[str], Mapping[int, dict[str, Any] | None]]:
        self.calls.append((channel, payload))
        self.reached[channel].set()
        gate = self.gates.get(channel)
        if gate is not None:
            await gate.wait()
        return set(), set(), dict(self.script.get(channel, {}))


async def _leave_while_row_moves_out(
    rejoin_before_removal: bool,
) -> tuple[dict[str, dict], list[dict[str, Any]], dict[str, dict]]:
    """
    A、B 共享 S。一批里行频道 R 先暂存 {1: v1}，索引频道 I 读的期间 B 退订（S 还有 A），I 随后把行 1
    移出范围（暂存 None、快照里删掉）。B 在 tick 结束前重订：rejoin_before_removal 为真时在 I 暂存之前，
    否则之后。返回 (A 拿到的更新, B 重订的回复, B 之后拿到的更新)
    """
    hub, (a, b), mq, node = make_brokers(2)
    v1 = {"id": 1, "v": 1}
    s = ScriptedSub({"R", "I"}, {"R": {1: v1}, "I": {1: None}})
    await shared_by(node, a, b, sub=s)
    g = FakeSub({"I"})  # 同批的另一个订阅，卡住 tick 的结尾
    await register(a, node, "G", g)
    s.gates["I"] = asyncio.Event()
    g.gate.clear()
    mq.push_pulled_("R", None)
    mq.push_pulled_("I", None)
    await asyncio.sleep(0.01)  # 都过了合批间隔，同一批弹出
    tick = asyncio.create_task(a.get_updates(timeout=TICK))
    async with asyncio.timeout(1):
        await s.reached["I"].wait()
        await g.entered.wait()
    assert s.snapshot == {1: v1}
    await unsubscribe_and_ack(b, node, "S")  # A 还订着，订阅不关
    if not rejoin_before_removal:
        s.gates["I"].set()
        await wait_until(lambda: s.snapshot == {})
    joined, task = share_sub(b, "S", lambda: FakeSub({"R", "I"}))
    assert joined is s
    async with asyncio.timeout(1):
        reply = await task
    s.gates["I"].set()
    g.gate.set()
    got_a = await tick
    got_b = await b.get_updates(timeout=TICK)
    await close_hub(hub, node)
    return got_a, reply, got_b


async def test_rejoin_within_tick_skips_updates_staged_before_leaving():
    """
    同一 tick 里退订又重订同一个共享查询：退订前暂存给它的更新不能再交给它。以前交付时按订阅对象认，
    B 重订后退订前那份 {1: v1} 照样交给它，排在回复（快照里已没有行 1）后面，客户端留着一行订阅已不再
    跟踪的幽灵行
    """
    got_a, reply, got_b = await _leave_while_row_moves_out(rejoin_before_removal=False)
    assert got_a == {"S": {1: None}}
    assert reply == []
    assert got_b == {}, "退订前暂存的旧行交给了重订的连接"


async def test_rejoin_within_tick_gets_what_changes_after_it():
    """同一 tick 里退订又重订：重订时快照里还有行 1，之后它被移出范围，重订的连接要收到 None"""
    got_a, reply, got_b = await _leave_while_row_moves_out(rejoin_before_removal=True)
    assert got_a == {"S": {1: None}}
    assert reply == [{"id": 1, "v": 1}]
    assert got_b == {"S": {1: None}}


# ============ hub 的兜底：初始化漏出的 BaseException、处理循环意外结束 ============


class StrayCancelInitSub(FakeSub):
    """初始化时后端调用里漏出 CancelledError（本任务并没有被取消），前 stray 次如此"""

    def __init__(self, channels: set[str], stray: int):
        super().__init__(channels)
        self.stray = stray
        self.attempts = 0

    async def initialize_(self, hub: SubscriptionHub) -> list[dict[str, Any]] | None:
        self.attempts += 1
        await hub.attach_(self)
        if self.attempts <= self.stray:
            raise asyncio.CancelledError()
        return []


@pytest.mark.parametrize("stray", [1, 100], ids=["once", "always"])
async def test_stray_cancelled_error_in_init_is_handled(stray: int):
    """
    初始化时后端调用里漏出 CancelledError（本任务没被取消）：当作出错，换副本重试；重试用尽让等着的
    成员都失败、撤掉共享登记。以前只兜 Exception，初始化任务以"取消"结束：等着的成员一直等（服务器里
    它们的发送循环卡在占位上，之后的回复、推送全堵住），之后同一查询的订阅者也都加入这个死订阅
    """
    hub, (a, b), _mq, node = make_brokers(2)
    hub.INIT_RETRIES = 2
    s = StrayCancelInitSub({"X"}, stray)
    _, ta = share_sub(a, "S", lambda: s)
    joined, tb = share_sub(b, "S", lambda: StrayCancelInitSub({"X"}, stray))
    assert joined is s
    results = await ack_until_done(node, ta, tb)
    if stray == 1:
        assert results == [[], []]
        assert s.attempts == 2 and s.active
    else:
        assert all(type(r) is not asyncio.CancelledError for r in results), results
        assert all(isinstance(r, Exception) for r in results), results
        assert s.attempts == 3 and not s.active
        assert hub.shared_(KEY) is None
    await close_hub(hub, node)


async def test_join_restarts_a_dead_processing_loop():
    """
    处理循环意外结束（这里直接取消它）后，新连接加入已有的共享订阅也要把它拉起来。以前只有新建订阅
    时才拉起：热门查询都是加入，循环一直停着，整个 worker 没有推送
    """
    hub, (a, b), mq, node = make_brokers(2, autostart=True)
    s = await shared_by(node, a, sub=FakeSub({"C"}))
    task = hub._task
    assert task is not None
    task.cancel()
    await asyncio.wait([task])
    joined, tb = share_sub(b, "S", lambda: FakeSub({"C"}))
    assert joined is s
    await finish(tb, node)
    assert hub._task is not task and hub._task is not None and not hub._task.done()
    s.updates = {1: {"id": 1}}
    mq.push_pulled_("C", None)
    async with asyncio.timeout(1):
        assert await b.get_updates() == {"S": {1: {"id": 1}}}
    await close_hub(hub, node)


# ============ 后半段认的是那一次登记：退订后重订同一查询会重新加入同一个订阅对象 ============


async def test_stale_second_half_leaves_rejoined_registration_alone():
    """
    A 加入初始化中的共享订阅 S（B 撑着它），后半段在等。A 退订（它的后半段回 None）又立刻重订，
    重新加入 S，旧的后半段这时才跑：它不能把重订的登记当成自己的撤掉。以前按订阅对象认，
    _subs["S"] 又是 S，旧的后半段拿着 None 就把新登记退订了
    """
    hub, (a, b), _mq, node = make_brokers(2)
    s = SlowInitSub({"X"})
    _, tb = share_sub(b, "S", lambda: s)
    joined, stale = share_sub(a, "S", lambda: SlowInitSub({"X"}))
    assert joined is s
    await finish(asyncio.create_task(s.reading.wait()), node)
    await a.unsubscribe("S")  # B 还订着，不退频道，也不让出事件循环
    _, again = share_sub(a, "S", lambda: SlowInitSub({"X"}))
    async with asyncio.timeout(1):
        assert await stale is None
    assert a._subs.get("S") is s, "旧的后半段撤掉了重订的登记"
    s.release.set()
    async with asyncio.timeout(1):
        assert await again == [{"id": 1}]
        assert await tb == [{"id": 1}]
    assert set(s.members) == {a, b}
    await close_hub(hub, node)


async def test_stale_second_half_with_rows_is_not_counted_twice():
    """
    A 的后半段已经拿到初始化结果、还没跑，A 退订又重订、重新加入 S：旧的后半段回 None，不再按
    这次订阅计频道数（以前两个后半段各加一次，退订只减一次，告警用的频道数越积越多）
    """
    hub, (a, b), _mq, node = make_brokers(2)
    s = SlowInitSub({"X"})
    _, tb = share_sub(b, "S", lambda: s)
    _, stale = share_sub(a, "S", lambda: SlowInitSub({"X"}))
    await finish(asyncio.create_task(s.reading.wait()), node)
    s.release.set()
    await asyncio.sleep(0)  # 初始化完成、结果交给了 A 的 future；A 的后半段还没跑
    assert s.ready and not stale.done()
    await a.unsubscribe("S")
    _, again = share_sub(a, "S", lambda: SlowInitSub({"X"}))
    async with asyncio.timeout(1):
        assert await stale is None
        assert await again == [{"id": 1}]
        assert await tb == [{"id": 1}]
    assert a._channel_count == 1
    await unsubscribe_and_ack(a, node, "S")
    assert a._channel_count == 0
    await close_hub(hub, node)


async def test_waiting_after_hub_close_does_not_hang():
    """hub 关闭之后再等一个订阅的初始化结果（重复订阅的后半段这时才开始跑）：抛 ConnectionError，
    不能一直挂着（close 已经把当时等着的都交代了，之后来的没人管）"""
    hub, (a,), _mq, node = make_brokers(1)
    s = SlowInitSub({"X"})
    first = open_sub(a, "S", s)
    await finish(asyncio.create_task(s.reading.wait()), node)
    await close_hub(hub, node)
    with pytest.raises(ConnectionError):
        await first
    with pytest.raises(ConnectionError):
        async with asyncio.timeout(1):
            await hub.wait_(s, a)


# ============ 合批窗口：通知接连不断时 tick 不能一条一个 ============


def time_ticks(hub: SubscriptionHub) -> list[tuple[float, float]]:
    """记下 hub 每个 tick 开始、结束时的 loop 时间（tick 结束时追加）"""
    spans: list[tuple[float, float]] = []
    tick = hub._tick
    loop = asyncio.get_running_loop()

    async def timed(batch):
        start = loop.time()
        try:
            return await tick(batch)
        finally:
            spans.append((start, loop.time()))

    hub._tick = timed  # type: ignore[method-assign]
    return spans


async def test_continuous_notifications_are_batched_into_spaced_ticks():
    """
    通知接连不断时，MQClient 每次只弹出刚满一个 interval 的那一两条。以前 hub 弹一批跑一个 tick、
    跑完马上弹下一批：实测 2000 连接、每秒 2000 次写入时每秒约 1500 个 tick、平均每个 2 条，建任务、
    等订阅回执、预读往返这些每 tick 的固定开销被放大，私有订阅反而比每连接一个队列更费 CPU。
    相邻两个 tick 的开始至少隔 TICK_SPACING_INTERVALS 个 interval，这期间满期的通知攒成一批
    """
    hub, _brokers, mq, node = make_brokers(0, autostart=True)
    mq.UPDATE_FREQUENCY = 100  # type: ignore[reportAttributeAccessIssue]  interval 10ms
    hub.TICK_SPACING_INTERVALS = 3  # 30ms：拉开到计时误差之外
    spacing = 3 * hub.interval
    spans = time_ticks(hub)
    for i in range(40):
        mq.push_pulled_(f"C{i}", None)
        await asyncio.sleep(0.003)
    async with asyncio.timeout(3):
        while mq.pulled_deque:
            await asyncio.sleep(0.01)
    await asyncio.sleep(0.05)  # 最后一个 tick 跑完才记下
    starts = [start for start, _end in spans]
    gaps = [b - a for a, b in itertools.pairwise(starts)]
    assert gaps, "通知只够跑一个 tick，测不出间隔"
    assert min(gaps) >= spacing - 0.002, (
        f"相邻 tick 只隔了 {min(gaps) * 1000:.1f}ms（{len(starts)} 个 tick）"
    )
    await close_hub(hub, node)


async def test_isolated_notification_is_not_delayed_by_tick_spacing():
    """合批窗口只在通知接连不断时起作用：隔了一阵才来的一条，满一个 interval 就处理，不多等"""
    hub, _brokers, mq, node = make_brokers(0, autostart=True)
    mq.UPDATE_FREQUENCY = 100  # type: ignore[reportAttributeAccessIssue]  interval 10ms
    hub.TICK_SPACING_INTERVALS = 10  # 100ms
    spans = time_ticks(hub)
    loop = asyncio.get_running_loop()
    mq.push_pulled_("A", None)
    await wait_until(lambda: spans)
    await asyncio.sleep(0.2)  # 空闲超过窗口
    pushed = loop.time()
    mq.push_pulled_("B", None)
    await wait_until(lambda: len(spans) == 2)
    delay = spans[1][0] - pushed
    assert delay < hub.interval + 0.05, (
        f"隔了一阵才来的通知多等了：{delay * 1000:.1f}ms 才处理"
    )
    await close_hub(hub, node)


async def test_slow_tick_is_not_followed_by_extra_wait():
    """tick 本身已经超过合批窗口时，下一个 tick 不再多等：窗口从上一个 tick 开始时算"""
    hub, (broker,), mq, node = make_brokers(1, autostart=True)
    mq.UPDATE_FREQUENCY = 100  # type: ignore[reportAttributeAccessIssue]  interval 10ms
    hub.TICK_SPACING_INTERVALS = 3  # 30ms
    slow = FakeSub({"S"})
    await register(broker, node, "S", slow)
    slow.gate.clear()
    spans = time_ticks(hub)
    mq.push_pulled_("S", None)
    async with asyncio.timeout(1):
        await slow.entered.wait()
    mq.push_pulled_("N", None)  # tick 卡着的期间满期
    await asyncio.sleep(0.08)  # 这个 tick 远超窗口
    slow.gate.set()
    await wait_until(lambda: len(spans) >= 2)
    (start1, end1), (start2, _end2) = spans[0], spans[1]
    assert end1 - start1 > 0.06
    assert start2 - end1 < 0.015, f"慢 tick 之后又等了 {(start2 - end1) * 1000:.1f}ms"
    await close_hub(hub, node)


# ============ 服务端发送循环直接取待发区（设计稿 2026-09-29-push-path-and-gc §2） ============


async def test_idle_sender_is_woken_once_per_idle_wait():
    """
    发送循环空闲等着时交来更新，叫醒它一次；同一段空闲里再交来的只合并进待发区，不重复叫。取走、再空闲
    之后，又能叫醒。待发区空时 take_updates_ 不等待，返回空
    """
    hub, (a,), mq, node = make_brokers(1, autostart=True)
    s = FakeSub({"C"})
    await register(a, node, "S", s)
    woken: list[int] = []
    a.bind_sender_(lambda: woken.append(1))
    assert a.take_updates_() == {}

    a.idle_(True)
    s.updates = {1: {"v": 1}}
    mq.push_pulled_("C", None)
    await wait_until(lambda: a._outbox)
    assert woken == [1]
    s.updates = {1: {"v": 2}}
    mq.push_pulled_("C", None)
    await wait_until(lambda: a._outbox.get("S") == {1: {"v": 2}})
    assert woken == [1], "同一段空闲里重复叫醒"

    a.idle_(False)
    assert a.take_updates_() == {"S": {1: {"v": 2}}}
    a.idle_(True)
    s.updates = {1: {"v": 3}}
    mq.push_pulled_("C", None)
    await wait_until(lambda: a._outbox)
    assert woken == [1, 1]
    await close_hub(hub, node)


async def test_busy_sender_with_outbox_is_stalled_and_draining_rereads():
    """
    发送循环空闲等着时交来、还没取走的不算卡着；醒来之后没在空闲等待（卡在 ws.send 上）而待发区有东西才算
    卡着，通知攒着不读。取走待发区时重读攒着的，推最新的
    """
    hub, (a,), mq, node = make_brokers(1, autostart=True)
    s = FakeSub({"C"})
    await register(a, node, "S", s)
    a.bind_sender_(lambda: None)
    a.idle_(True)
    s.updates = {1: {"v": 1}}
    mq.push_pulled_("C", None)
    await wait_until(lambda: a._outbox)
    assert not a.stalled_(), "发送循环空闲等着，交来的还没取就算卡着了"

    a.idle_(False)
    assert a.stalled_()
    reads = len(s.calls)
    s.updates = {1: {"v": 2}}
    mq.push_pulled_("C", None)
    await wait_until(lambda: s in hub._parked)
    assert len(s.calls) == reads, "卡着还在读"

    assert a.take_updates_() == {"S": {1: {"v": 1}}}
    a.idle_(True)
    await wait_until(lambda: a._outbox)  # 攒着的重读了
    assert a.take_updates_() == {"S": {1: {"v": 2}}}
    await close_hub(hub, node)


async def test_failing_wake_does_not_break_delivery_to_other_connections():
    """
    发送循环的叫醒函数是在 hub 的交付循环里同步调用的：它抛异常不能打断 hub 给别的连接交付（不兜的话这个
    tick 排在后面的连接都拿不到更新，订阅的指纹却已推进，这次推送就丢了）；这次没叫醒，下次交来时还要再叫
    （不兜的话叫醒标记一直留着，这段空闲里再也叫不醒）
    """
    hub, (a, b), mq, node = make_brokers(2, autostart=True)
    s = await shared_by(node, a, b, sub=FakeSub({"C"}))  # 交付时先轮到 a
    calls: list[int] = []

    def broken_wake() -> None:
        calls.append(1)
        raise RuntimeError("sender gone")

    a.bind_sender_(broken_wake)
    a.idle_(True)
    s.updates = {1: {"id": 1, "v": 1}}
    mq.push_pulled_("C", None)
    assert await b.get_updates(timeout=TICK) == {"S": {1: {"id": 1, "v": 1}}}, (
        "a 的叫醒出错，排在后面的 b 没拿到更新"
    )
    assert calls == [1]
    assert a._outbox == {"S": {1: {"id": 1, "v": 1}}}

    s.updates = {1: {"id": 1, "v": 2}}
    mq.push_pulled_("C", None)
    await wait_until(lambda: a._outbox.get("S") == {1: {"id": 1, "v": 2}})
    assert calls == [1, 1], "叫醒失败后一直不再叫"
    await close_hub(hub, node)
