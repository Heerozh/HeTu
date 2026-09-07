"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

from typing import cast

import pytest
from fixtures.backends import use_redis_family_backend_only

from hetu.data.backend.redis import RedisBackendClient


@use_redis_family_backend_only
@pytest.mark.timeout(30)  # 这个时间算上backend启动时间，所以要很长
async def test_backend_redis_pubsub(mod_auto_backend):
    """测试自己的redis异步pubsub功能，主要支持cluster。"""
    backend = mod_auto_backend()

    from hetu.data.backend.redis.pubsub import AsyncKeyspacePubSub

    client: RedisBackendClient = cast(RedisBackendClient, backend.master)
    pubsub = AsyncKeyspacePubSub(client.aio)

    await pubsub.subscribe("key{1}")
    await pubsub.subscribe("key{2}")

    redis_client = client.io
    redis_client.publish("key{1}", b"1")
    redis_client.publish("key{2}", b"2")

    msg1 = await pubsub.get_message()
    msg2 = await pubsub.get_message()
    assert msg1["data"] == b"1"
    assert msg2["data"] == b"2"


@use_redis_family_backend_only
@pytest.mark.timeout(30)
async def test_backend_redis_pubsub_batch(mod_auto_backend):
    """批量订阅/取消订阅：多个频道一次下发，取消后不再收到消息，且发回订阅时的节点。"""
    import asyncio

    backend = mod_auto_backend()

    from hetu.data.backend.redis.pubsub import AsyncKeyspacePubSub

    client: RedisBackendClient = cast(RedisBackendClient, backend.master)
    pubsub = AsyncKeyspacePubSub(client.aio)

    # 分散在不同slot上的频道，一次订阅
    channels = [f"batch{{{i}}}" for i in range(20)]
    await pubsub.subscribe(*channels)
    assert set(channels) <= pubsub.subscribed
    assert all(ch in pubsub._channel_node for ch in channels)

    redis_client = client.io
    for ch in channels:
        redis_client.publish(ch, ch.encode())
    got = set()
    for _ in channels:
        async with asyncio.timeout(5):
            msg = await pubsub.get_message()
        got.add(msg["data"])
    assert got == {ch.encode() for ch in channels}

    # 一次取消一半，再发布，只应收到未取消的那一半
    removed, kept = channels[:10], channels[10:]
    await pubsub.unsubscribe(*removed)
    assert not (set(removed) & pubsub.subscribed)
    assert not any(ch in pubsub._channel_node for ch in removed)
    for ch in channels:
        redis_client.publish(ch, ch.encode())
    got = set()
    for _ in kept:
        async with asyncio.timeout(5):
            msg = await pubsub.get_message()
        got.add(msg["data"])
    assert got == {ch.encode() for ch in kept}
    # 队列里不应再有被取消频道的消息
    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.5):
            await pubsub.get_message()

    await pubsub.close()
