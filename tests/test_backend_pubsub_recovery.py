"""
pubsub 节点失效 → 重订阅恢复：不连 Redis，用假的节点 pubsub 控制失效与 ack 的时机，
验证恢复期间又有节点失效时频道不会被永远漏掉，恢复期间退订的频道也不会被订回来。
"""

import asyncio

from fixtures.fake_pubsub import (
    FakeNodePubSub,
    attach_fake_node,
    make_hub,
    make_pubsub,
    settle,
)

from hetu.data.backend.base import MQClient
from hetu.data.backend.redis.mq import RedisMQClient
from hetu.data.backend.redis.pubsub import AsyncKeyspacePubSub

ROW_A = "pytest:Item:{CLU1}:id:1"
ROW_B = "pytest:Item:{CLU1}:id:2"


async def _subscribe_acked(
    pubsub: AsyncKeyspacePubSub, node: FakeNodePubSub, *channels: str
):
    t = asyncio.create_task(pubsub.subscribe(*channels))
    await settle()
    for channel in channels:
        node.ack("subscribe", channel)
    async with asyncio.timeout(1):
        await t


async def test_second_node_failure_during_resubscribe_is_not_lost():
    """恢复流程跑着的时候节点又失效：第二次失效前刚 ack 的频道以前会留在已订阅集合里，
    重试时被当作已订阅跳过、永远订不回来；现在每次失效都清掉已订阅集合并回重订名单，
    恢复流程要等它们全部重新 ack 才算完成"""
    pubsub, node = make_pubsub()
    await _subscribe_acked(pubsub, node, ROW_A, ROW_B)
    assert pubsub._subscribed == {ROW_A, ROW_B}

    # 第一次失效：恢复流程在 node2 上重订 A、B；A 先 ack
    node.fail()
    await settle()
    node2 = attach_fake_node(pubsub)
    await settle()
    assert sorted(node2.sent("subscribe")) == [ROW_A, ROW_B]
    node2.ack("subscribe", ROW_A)
    await settle()
    assert ROW_A in pubsub._subscribed

    # 第二次失效（node2 也断了）：A 刚 ack 的订阅也随连接没了
    node2.fail()
    await settle()
    assert ROW_A not in pubsub._subscribed
    node3 = attach_fake_node(pubsub)
    async with asyncio.timeout(3):
        # 退避重试后 A、B 都要在 node3 上重发
        while sorted(node3.sent("subscribe")) != [ROW_A, ROW_B]:
            await asyncio.sleep(0.05)
    node3.ack("subscribe", ROW_A)
    node3.ack("subscribe", ROW_B)
    await settle()
    assert pubsub._subscribed == {ROW_A, ROW_B}
    await pubsub.close()


async def test_unsubscribed_during_resubscribe_is_not_resubscribed():
    """恢复流程还没订回来的频道在这期间被退订了：之后的重试不能再把它订回来
    （没人要的订阅只会白收消息）"""
    pubsub, node = make_pubsub()
    await _subscribe_acked(pubsub, node, ROW_A, ROW_B)

    node.fail()
    await settle()
    node2 = attach_fake_node(pubsub)
    await settle()
    assert sorted(node2.sent("subscribe")) == [ROW_A, ROW_B]
    # 恢复流程等 ack 期间 A 被退订
    t = asyncio.create_task(pubsub.unsubscribe(ROW_A))
    await settle()
    node2.ack("unsubscribe", ROW_A)
    async with asyncio.timeout(1):
        await t

    # B 还没 ack，node2 就断了：重试只该订 B
    node2.fail()
    await settle()
    node3 = attach_fake_node(pubsub)
    async with asyncio.timeout(3):
        while not node3.sent("subscribe"):
            await asyncio.sleep(0.05)
    await settle()
    assert node3.sent("subscribe") == [ROW_B]
    node3.ack("subscribe", ROW_B)
    await settle()
    assert pubsub._subscribed == {ROW_B}
    await pubsub.close()


# ============ 重订全部生效后：断线期间的写入没有通知，交给上层补读 ============


async def test_resubscribe_reports_recovered_channels():
    """恢复流程确认全部重订生效后回调 on_resubscribed，交出这批频道；途中退订的不在其中"""
    reported: list[set[str]] = []
    pubsub, node = make_pubsub(
        on_resubscribed=lambda chans: reported.append(set(chans))
    )
    await _subscribe_acked(pubsub, node, ROW_A, ROW_B)

    node.fail()
    await settle()
    node2 = attach_fake_node(pubsub)
    await settle()
    assert sorted(node2.sent("subscribe")) == [ROW_A, ROW_B]
    t = asyncio.create_task(pubsub.unsubscribe(ROW_B))  # 恢复途中 B 没人要了
    await settle()
    node2.ack("unsubscribe", ROW_B)
    async with asyncio.timeout(1):
        await t
    assert reported == [], "还没全部生效就回调了"
    node2.ack("subscribe", ROW_A)
    await settle()
    assert reported == [{ROW_A}]
    await pubsub.close()


# 按真实命名：行、索引是 keyspace 通知；值频道、表级频道是 commit 主动 PUBLISH 的普通频道
KS_ROW = "__keyspace@0__:pytest:Item:{CLU1}:id:1"
KS_INDEX = "__keyspace@0__:pytest:Item:{CLU1}:index:owner"
VALUE = "pytest:Item:{CLU1}:index:owner:800000000000000a"
TABLE = "pytest:Item:{CLU1}:table"


async def test_hub_dispatches_resync_after_resubscribe():
    """hub 收到重订生效的回调：给仍有人订的频道各分发一条通知，各连接 interval 后重读。
    只有表级频道带 RESYNC（它的 payload 本来就是 row_id 集合，整表订阅据此整表重同步）；
    行 / 索引 / 值频道的 payload 照约定是 None。服务端内部 watch 的频道回调也照常触发
    （断线期间丢的顶号通知由此补查一次）"""
    hub, node = make_hub()
    mq = RedisMQClient(hub)
    fired: list[str] = []
    channels = (KS_ROW, KS_INDEX, VALUE, TABLE)
    t = asyncio.gather(
        mq.subscribe(*channels), mq.watch(ROW_B, lambda: fired.append(ROW_B))
    )
    await settle()
    for channel in (*channels, ROW_B):
        node.ack("subscribe", channel)
    async with asyncio.timeout(1):
        await t

    node.fail()
    await settle()
    node2 = attach_fake_node(hub._pubsub)  # type: ignore[reportPrivateUsage]
    async with asyncio.timeout(3):
        while sorted(node2.sent("subscribe")) != sorted((*channels, ROW_B)):
            await asyncio.sleep(0.05)
    for channel in (*channels, ROW_B):
        node2.ack("subscribe", channel)
    async with asyncio.timeout(1):
        assert await mq.get_message() == {
            KS_ROW: None,
            KS_INDEX: None,
            VALUE: None,
            TABLE: {MQClient.RESYNC},
        }
    assert fired == [ROW_B]
    await hub.close()
