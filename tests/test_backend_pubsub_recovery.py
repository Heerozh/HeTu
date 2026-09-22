"""
pubsub 节点失效 → 重订阅恢复期间的行缓存激活状态机：不连 Redis，用假的节点 pubsub
控制失效与 ack 的时机，验证只有订阅确实生效（ack 到了、节点没失效）的频道才能在缓存里
激活，以及恢复期间又有节点失效时频道不会被永远漏掉。
"""

import asyncio

from fixtures.fake_pubsub import attach_fake_node, make_hub, settle

from hetu.data.backend.redis.mq import RedisMQClient
from hetu.data.backend.rowcache import RowCache

ROW_A = "pytest:Item:{CLU1}:id:1"
ROW_B = "pytest:Item:{CLU1}:id:2"


async def test_restored_activates_only_acked_channels():
    """节点恢复：resubscribe_all 的 ack 先到、别的连接刚发出 SUBSCRIBE 还没 ack 的频道
    不能跟着被激活（ack 之前的写入没有通知），要等它自己的 add 拿到 ack"""
    cache = RowCache()
    hub, node = make_hub(cache)
    pubsub = hub._pubsub  # type: ignore[reportPrivateUsage]
    mq_a, mq_b = RedisMQClient(hub), RedisMQClient(hub)

    t = asyncio.create_task(mq_a.subscribe(ROW_A))
    await settle()
    node.ack("subscribe", ROW_A)
    async with asyncio.timeout(1):
        await t
    assert cache.is_active(ROW_A)

    # 节点失效：全部失活；恢复流程另起一个节点重订 A
    node.fail()
    await settle()
    assert not cache.is_active(ROW_A) and not pubsub.is_subscribed(ROW_A)
    node2 = attach_fake_node(pubsub)
    await settle()
    assert node2.sent("subscribe") == [ROW_A]
    # 恢复期间连接 B 订 B：自己发 SUBSCRIBE，还没 ack
    t_b = asyncio.create_task(mq_b.subscribe(ROW_B))
    await settle()
    assert node2.sent("subscribe") == [ROW_A, ROW_B]

    # A 的 ack 先到 → on_restored：只激活 A，B 还没 ack 不能激活
    node2.ack("subscribe", ROW_A)
    await settle()
    assert cache.is_active(ROW_A)
    assert ROW_B in hub.channels and not cache.is_active(ROW_B)
    assert not t_b.done()
    # B 的 ack 到了才激活
    node2.ack("subscribe", ROW_B)
    async with asyncio.timeout(1):
        await t_b
    assert cache.is_active(ROW_B)
    await hub.close()


async def test_second_node_failure_during_resubscribe_is_not_lost():
    """恢复流程跑着的时候节点又失效：第二次失效前刚 ack 的频道以前会留在已订阅集合里，
    重试时被当作已订阅跳过、永远订不回来；现在每次失效都清掉已订阅集合并回重订名单，
    恢复流程要等它们全部重新 ack 才算完成，期间不激活"""
    cache = RowCache()
    hub, node = make_hub(cache)
    pubsub = hub._pubsub  # type: ignore[reportPrivateUsage]
    mq = RedisMQClient(hub)

    t = asyncio.create_task(mq.subscribe(ROW_A, ROW_B))
    await settle()
    node.ack("subscribe", ROW_A)
    node.ack("subscribe", ROW_B)
    async with asyncio.timeout(1):
        await t
    assert cache.is_active(ROW_A) and cache.is_active(ROW_B)

    # 第一次失效：恢复流程在 node2 上重订 A、B；A 先 ack
    node.fail()
    await settle()
    node2 = attach_fake_node(pubsub)
    await settle()
    assert sorted(node2.sent("subscribe")) == [ROW_A, ROW_B]
    node2.ack("subscribe", ROW_A)
    await settle()
    assert pubsub.is_subscribed(ROW_A) and not cache.is_active(ROW_A)

    # 第二次失效（node2 也断了）：A 刚 ack 的订阅也随连接没了
    node2.fail()
    await settle()
    assert not pubsub.is_subscribed(ROW_A) and not cache.is_active(ROW_A)
    node3 = attach_fake_node(pubsub)
    async with asyncio.timeout(3):
        # 退避重试后 A、B 都要在 node3 上重发
        while sorted(node3.sent("subscribe")) != [ROW_A, ROW_B]:
            await asyncio.sleep(0.05)
    # 只 ack 一个：还没恢复完，不激活
    node3.ack("subscribe", ROW_B)
    await settle()
    assert not cache.is_active(ROW_B)
    node3.ack("subscribe", ROW_A)
    await settle()
    assert cache.is_active(ROW_A) and cache.is_active(ROW_B)
    assert pubsub.subscribed == {ROW_A, ROW_B}
    await hub.close()
