"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import redis
import redis.cluster
from typing import cast
import pytest
import asyncio

from fixtures.backends import use_redis_family_backend_only
from hetu.data.backend import RedisBackendClient


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

    await asyncio.sleep(1)

    redis_client = client.io
    redis_client.publish("key{1}", b"1")
    redis_client.publish("key{2}", b"2")

    msg1 = await pubsub.get_message()
    msg2 = await pubsub.get_message()
    assert msg1["data"] == b"1"
    assert msg2["data"] == b"2"
