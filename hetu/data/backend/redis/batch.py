"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

from asyncio.queues import Queue


import asyncio
from typing import Any
from cachetools import TTLCache
from redis.asyncio import Redis, RedisCluster


class RedisBatchedClient:
    """
    这是一个增加Redis请求吞吐量的类，通过缓存，以及合批，减少与Redis服务器的交互次数。

    短 TTL 缓存：
    采取短 TTL（最好再加个随机抖动），我们只需要缓存0.1秒的数据，减少短期的重复请求。
    由于TTL较短，所以不需要考虑缓存失效通知。0.2秒是System重试的典型时间窗口，事务冲突时缓存已失效。
    后期可以实验调平不同的值，实现cache命中和事务冲突的平衡。

    合批：
    将同一时间窗口内的请求通过pipeline合并成一个请求发送给Redis服务器，如果超过水位则立即发送请求。
    """

    def __init__(
        self,
        redis: Redis | RedisCluster,
        batch_limit: int = 10,
        wait_time: float = 0.1,
        cache_ttl: float = 0.1,
    ):
        self._redis = redis
        self._queue: Queue[tuple] = asyncio.Queue()
        self._cache = TTLCache(maxsize=10000, ttl=cache_ttl)
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
