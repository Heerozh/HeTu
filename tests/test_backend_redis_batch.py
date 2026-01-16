import asyncio
import time
from typing import cast

from fixtures.backends import use_redis_family_backend_only

from hetu.data.backend.redis import RedisBackendClient
from hetu.data.backend.redis.batch import RedisBatchedClient


@use_redis_family_backend_only
async def test_redis_cache_basic(mod_auto_backend):
    backend = mod_auto_backend()
    client: RedisBackendClient = cast(RedisBackendClient, backend.master)
    redis_client = client.aio

    # Prepare data
    key = "test:cache:hash"
    field = "foo"
    value = "bar"
    await redis_client.hset(key, mapping={field: value})  # type: ignore

    zkey = "test:cache:zset"
    await redis_client.zadd(zkey, {"m1": 1, "m2": 2})

    # Initialize Cache
    # Use small wait_time for faster tests
    cache = RedisBatchedClient([redis_client])

    # 1. Test hgetall
    res = await cache.hgetall(key)
    assert res == {field.encode(): value.encode()}

    # 2. Test zrange
    res_z = await cache.zrange(zkey, start=0, end=-1, withscores=True)
    # redis-py zrange withscores returns list of tuples [(member, score), ...]
    # check structure
    assert len(res_z) == 2
    assert res_z[0][0] == b"m1"

    # 3. Test Batching
    # Modify backend directly
    await redis_client.hset(key, mapping={"foo": "new_bar"})  # type: ignore

    # Launch multiple requests concurrently
    tasks = []
    start_time = time.time()
    for _ in range(5):
        # 必须不await而用task，让worker暂时不执行，让队列先满
        tasks.append(cache.hgetall(key))

    results = await asyncio.gather(*tasks)
    end_time = time.time()
    assert end_time - start_time <= 0.05  # Should be batched without sleep
    for r in results:
        assert r == {field.encode(): b"new_bar"}

    # Optional: Check if batching actually happened?
    # Hard without mocking the pipeline, but functionality is verified if results are correct.
