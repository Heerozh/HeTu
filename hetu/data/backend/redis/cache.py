# 这是一个增加Redis请求吞吐量的类，通过缓存，以及合批，减少与Redis服务器的交互次数。

# 首先我们的项目只用到了如下2个redis请求，aio就是redis-py的异步客户端：
# await aio.hgetall(key)
# await aio.zrange(name=idx_key, **cmds)
#
# 所以我们可以包装一个类，实现这2个异步请求，但是增加缓存和合批功能。
#
# 缓存：
# 缓存用cachetools.TTLCache，采取短 TTL + 随机抖动，我们只需要缓存0.5秒的数据，减少短期的重复请求。
# 由于TTL较短，所以不需要考虑缓存一致性问题。
#
# 合批：
# 合批采取时间窗口合批的方式，将同一时间窗口内的请求通过pipeline合并成一个请求发送给Redis服务器。
# 用户在调用await aio.hgetall/zrange时，是往一个队列里放future请求，然后等待结果返回。
# 消费者则堵塞在队列get上，直到有任何消息放入，这样没消息时消费者不工作。
# get成功后先看自己的队列里的请求是否超过设定水位，如果超过则立即循环get发送请求，否则等待一个时间窗口（比如10ms），
# 然后无论水位多少立即发送请求。

import asyncio
from typing import Any
from cachetools import TTLCache
from redis.asyncio import Redis


class RedisCache:
    def __init__(self, redis: Redis, batch_limit: int = 100, wait_time: float = 0.01):
        self._redis = redis
        self._queue = asyncio.Queue()
        self._cache = TTLCache(maxsize=10000, ttl=0.5)
        self._batch_limit = batch_limit
        self._wait_time = wait_time
        asyncio.create_task(self._worker())

    async def hgetall(self, key: str) -> dict:
        return await self._enqueue("hgetall", f"h:{key}", key)

    async def zrange(self, name: str, **kwargs) -> list:
        # Sort kwargs to ensure deterministic cache key
        cache_key = f"z:{name}:{sorted(kwargs.items())}"
        return await self._enqueue("zrange", cache_key, name, **kwargs)

    async def _enqueue(self, cmd: str, cache_key: str, key: str, **kwargs) -> Any:
        # Cache Hit
        if (res := self._cache.get(cache_key)) is not None:
            return res

        # Cache Miss
        future = asyncio.get_running_loop().create_future()
        await self._queue.put((cmd, key, kwargs, future, cache_key))
        return await future

    async def _worker(self):
        while True:
            # Block until at least one request arrives
            item = await self._queue.get()
            batch = [item]

            # If High Water Mark not reached, wait for time window
            if self._queue.qsize() < self._batch_limit:
                await asyncio.sleep(self._wait_time)

            # Drain queue (Batching)
            while not self._queue.empty():
                batch.append(self._queue.get_nowait())
                self._queue.task_done()
            self._queue.task_done()

            if not batch:
                continue

            try:
                pipe = self._redis.pipeline()
                for cmd, key, kwargs, _, _ in batch:
                    if cmd == "hgetall":
                        pipe.hgetall(key)
                    elif cmd == "zrange":
                        pipe.zrange(name=key, **kwargs)

                results = await pipe.execute()

                for i, res in enumerate(results):
                    _, _, _, fut, ckey = batch[i]
                    if not fut.done():
                        self._cache[ckey] = res
                        fut.set_result(res)
            except Exception as e:
                for _, _, _, fut, _ in batch:
                    if not fut.done():
                        fut.set_exception(e)
