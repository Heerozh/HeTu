"""
MQClient 挂在几个副本（servant）的通知接收器上（`Backend.get_mq_client`）：一个本地队列，频道按
哈希分到各副本订阅；某个副本订阅失败、或它的 pubsub 断线时，换到别的副本。不连 Redis，用假的节点
pubsub。

以前 worker 级订阅器的 MQClient 建的时候随机绑一个副本、终身不换：这个副本挂掉，整个 worker 的
订阅推送全停、新订阅全失败，客户端重连也换不了（还是同一个订阅器）；负载也全压在这一个副本上。
"""

import asyncio
from collections.abc import Awaitable
from typing import Any

import pytest
from fixtures.fake_pubsub import FakeNodePubSub, attach_fake_node, make_hub, settle

from hetu.data.backend.base import HubMQClient, MQClient
from hetu.data.backend.redis.mq import PubSubHub, RedisMQClient

ROWS = [f"__keyspace@0__:pytest:Item:{{CLU1}}:id:{i}" for i in range(24)]
TABLES = [f"pytest:Item{i}:{{CLU1}}:table" for i in range(16)]
WATCHED = [f"pytest:Connection:{{CLU1}}:index:owner:{i}" for i in range(16)]


def make_mq(n: int = 2) -> tuple[RedisMQClient, list[PubSubHub], list[FakeNodePubSub]]:
    """挂在 n 个假副本上的 MQClient；通知入队后马上能取走"""
    pairs = [make_hub() for _ in range(n)]
    hubs = [hub for hub, _node in pairs]
    mq = RedisMQClient(*hubs)
    mq.UPDATE_FREQUENCY = 1000  # type: ignore[reportAttributeAccessIssue]
    return mq, hubs, [node for _hub, node in pairs]


async def acked(nodes: list[FakeNodePubSub], aw: Awaitable[Any]) -> Any:
    """等 aw 完成，期间把各节点发出的 SUBSCRIBE / UNSUBSCRIBE 都 ack 掉"""
    task = asyncio.ensure_future(aw)
    done: dict[tuple[int, str], int] = {}
    async with asyncio.timeout(3):
        while not task.done():
            for i, node in enumerate(nodes):
                for mtype in ("subscribe", "unsubscribe"):
                    sent = node.sent(mtype)
                    for channel in sent[done.get((i, mtype), 0) :]:
                        node.ack(mtype, channel)
                    done[i, mtype] = len(sent)
            await asyncio.sleep(0.001)
    return task.result()


def kill(hub: PubSubHub, node: FakeNodePubSub) -> None:
    """副本挂掉：它的 pubsub 连接断开，之后重连上的节点订阅 / 退订都立刻报错（连接被拒）"""
    pubsub = hub._pubsub  # type: ignore[reportPrivateUsage]

    async def refuse(*channels: str):
        raise ConnectionError("servant down")

    def reconnect():
        dead = attach_fake_node(pubsub)
        dead.subscribe = refuse  # type: ignore[method-assign]
        dead.unsubscribe = refuse  # type: ignore[method-assign]

    pubsub.standalone_connect = reconnect  # type: ignore[method-assign]
    node.fail()


def route(mq: HubMQClient) -> dict[str, Any]:
    """频道 → 订在哪个 hub 上"""
    return mq._route  # type: ignore[reportPrivateUsage]


async def close_all(mq: MQClient, hubs: list[PubSubHub]):
    """先关 hub（清掉登记、停掉重连），再关 MQClient：断线的假节点上退订要等 ack 超时"""
    for hub in hubs:
        await hub.close()
    await mq.close()


async def test_channels_spread_over_servants():
    """频道分到各副本订阅，每个频道只订在一个副本上；不同的 MQClient（不同 worker）分法不同"""
    mq, hubs, nodes = make_mq()
    await acked(nodes, mq.subscribe(*ROWS))
    on = [set(node.sent("subscribe")) for node in nodes]
    assert on[0] and on[1], "频道全订在一个副本上"
    assert not on[0] & on[1]
    assert on[0] | on[1] == set(ROWS)
    assert mq.subscribed_channels == set(ROWS)

    other, other_hubs, other_nodes = make_mq()
    await acked(other_nodes, other.subscribe(*ROWS))
    placement = [set(node.sent("subscribe")) for node in other_nodes]
    assert placement != on, "同一频道在每个 worker 都落在同一个副本上，热点频道压不散"
    await close_all(other, other_hubs)
    await close_all(mq, hubs)


async def test_subscribe_falls_back_to_another_servant():
    """分到某个副本的频道在那里订阅失败（连不上）：换到另一个副本订上，subscribe 照常返回；出错的
    副本冷却一段时间，新频道先不分给它"""
    mq, hubs, nodes = make_mq()
    nodes[0].fail_next = ConnectionError("servant down")
    await acked(nodes, mq.subscribe(*ROWS))
    assert set(nodes[1].sent("subscribe")) == set(ROWS)
    await settle()
    assert all(hubs[0].subscriber_count(ch) == 0 for ch in ROWS)
    assert all(route(mq)[ch] is hubs[1] for ch in ROWS)

    await acked(nodes, mq.subscribe(*TABLES))
    assert not set(nodes[0].sent("subscribe")) & set(TABLES)
    await close_all(mq, hubs)


async def test_subscribe_raises_when_every_servant_fails():
    """所有副本都订不上才抛出，已经在别处订上的一并撤掉（不留半截登记）"""
    mq, hubs, nodes = make_mq()

    async def refuse(*channels: str):
        raise ConnectionError("servant down")

    for node in nodes:
        node.subscribe = refuse  # type: ignore[method-assign]
    with pytest.raises(ConnectionError):
        await acked(nodes, mq.subscribe(*ROWS))
    await settle()
    assert mq.subscribed_channels == set()
    assert not route(mq)
    assert all(hub.subscriber_count(ch) == 0 for hub in hubs for ch in ROWS)
    await close_all(mq, hubs)


async def test_lost_servant_channels_move_and_are_reread():
    """
    副本 1 的 pubsub 断线：它上面的频道换到副本 2 订阅，订上之后各补一条通知让订阅者重读（断线期间的
    写入没有通知）；表级频道带 RESYNC，同 pubsub 断线重订。服务端内部关注的频道也换过去，回调补触发
    一次。副本 1 上不再留登记
    """
    mq, hubs, nodes = make_mq()
    fired: list[str] = []
    await acked(nodes, mq.subscribe(*ROWS, *TABLES))
    for channel in WATCHED:
        await acked(nodes, mq.watch(channel, lambda ch=channel: fired.append(ch)))
    on_first = {ch for ch, hub in route(mq).items() if hub is hubs[0]}
    assert on_first & set(ROWS) and on_first & set(WATCHED)

    kill(hubs[0], nodes[0])
    async with asyncio.timeout(3):
        while not on_first <= set(nodes[1].sent("subscribe")):
            await asyncio.sleep(0.001)
    for channel in on_first:
        nodes[1].ack("subscribe", channel)
    async with asyncio.timeout(1):
        got = await mq.get_message()
    moved_subs = on_first - set(WATCHED)
    assert set(got) == moved_subs
    for channel in moved_subs:
        expect = {MQClient.RESYNC} if channel in TABLES else None
        assert got[channel] == expect, channel
    assert sorted(fired) == sorted(on_first & set(WATCHED))
    await settle()
    assert all(route(mq)[ch] is hubs[1] for ch in on_first)
    assert all(hubs[0].subscriber_count(ch) == 0 for ch in on_first)
    await close_all(mq, hubs)


async def test_channel_unsubscribed_while_moving_is_not_left_on_new_servant():
    """换副本途中客户端退订了某个频道：新副本上订好之后也要撤掉，不能留下没人要的订阅"""
    mq, hubs, nodes = make_mq()
    await acked(nodes, mq.subscribe(*ROWS))
    on_first = {ch for ch, hub in route(mq).items() if hub is hubs[0]}
    victim = next(iter(on_first))
    kill(hubs[0], nodes[0])
    async with asyncio.timeout(3):
        while victim not in nodes[1].sent("subscribe"):
            await asyncio.sleep(0.001)
    await mq.unsubscribe(victim)
    for channel in on_first:
        nodes[1].ack("subscribe", channel)
    await acked(nodes[1:], asyncio.sleep(0.05))
    assert hubs[1].subscriber_count(victim) == 0
    assert hubs[0].subscriber_count(victim) == 0
    assert victim not in route(mq)
    assert all(route(mq)[ch] is hubs[1] for ch in on_first - {victim})
    await close_all(mq, hubs)


async def test_subscribe_of_a_moving_channel_waits_for_the_move():
    """正在换副本的频道又被订阅：等换完再按新的副本登记，不去碰断线的副本"""
    mq, hubs, nodes = make_mq()
    await acked(nodes, mq.subscribe(*ROWS))
    on_first = {ch for ch, hub in route(mq).items() if hub is hubs[0]}
    channel = next(iter(on_first))
    kill(hubs[0], nodes[0])
    async with asyncio.timeout(3):
        while channel not in nodes[1].sent("subscribe"):
            await asyncio.sleep(0.001)
    again = asyncio.create_task(mq.subscribe(channel))
    await settle()
    assert not again.done()
    await acked(nodes[1:], again)
    assert route(mq)[channel] is hubs[1]
    await close_all(mq, hubs)


async def test_close_releases_channels_on_every_servant():
    """关闭时各副本上的登记都撤掉（含换过副本的）"""
    mq, hubs, nodes = make_mq()
    await acked(nodes, mq.subscribe(*ROWS))
    kill(hubs[0], nodes[0])
    await acked(nodes[1:], asyncio.sleep(0.1))
    await acked(nodes[1:], mq.close())
    assert all(hub.subscriber_count(ch) == 0 for hub in hubs for ch in ROWS)
    for hub in hubs:
        await hub.close()


async def test_single_servant_leaves_recovery_to_the_hub():
    """只挂一个副本时没有别处可换：断线仍由它自己重连重订、补发 RESYNC，不另起换副本"""
    mq, hubs, nodes = make_mq(1)
    await acked(nodes, mq.subscribe(*ROWS))
    kill(hubs[0], nodes[0])
    await asyncio.sleep(0.05)
    assert all(hubs[0].subscriber_count(ch) == 1 for ch in ROWS)
    await close_all(mq, hubs)
