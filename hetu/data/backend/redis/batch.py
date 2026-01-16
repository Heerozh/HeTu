"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import random
from asyncio.queues import Queue


import asyncio
from typing import Any

# from cachetools import TTLCache
from redis.asyncio import Redis, RedisCluster


class RedisBatchedClient:
    """
    这是一个增加Redis请求吞吐量的类，通过缓存，以及合批，减少与Redis服务器的交互次数。

    短 TTL 的 LRU 缓存：
    采取短 TTL，减少短期的重复请求。如果遇到事务冲突，则通知缓存把事务相关的key满门抄斩。
    取消❌，缓存会导致客户端收到订阅更新时，获得的还是老数据，而让订阅通知和缓存失效绑定太不灵活。
    读取其实是小事，河图可以用读写分离无损扩容，无非是浪费点内网带宽罢了。

    合批：
    如果前一个请求未完成，后续累积请求都将通过pipeline合并成一个请求发送给Redis服务器。
    如果请求没有累积，则不会合批而是立即执行。
    """

    # _global_cache = TTLCache(maxsize=10000, ttl=10)

    @classmethod
    def create_cache(cls, maxsize: int = 10000, ttl: int = 10):
        # cls._global_cache = TTLCache(maxsize=maxsize, ttl=ttl)
        pass

    def __init__(self, replicas: list[Redis | RedisCluster]):
        self._replicas = replicas
        self._pipe = []
        self._queue: Queue[tuple] = asyncio.Queue()
        self._worker_task = None

    @classmethod
    def invalidate_cache(cls, keys: set[str]):
        # for key in keys:
        #     cls._global_cache.pop(key, None)
        pass

    async def hgetall(self, key: str) -> dict:
        return await self._enqueue("hgetall", key, key)

    async def zrange(self, name: str, **kwargs) -> list:
        # Sort kwargs to ensure deterministic cache key
        return await self._enqueue("zrange", None, name, **kwargs)

    async def _enqueue(
        self, cmd: str, cache_key: str | None, key: str, **kwargs
    ) -> Any:
        # Cache Hit
        # if cache_key:
        #     if (res := self._global_cache.get(cache_key)) is not None:
        #         return res

        # Cache Miss
        # Start worker if not running
        if self._worker_task is None:
            self._worker_task = asyncio.create_task(self._worker())

        # Enqueue request
        future = asyncio.get_running_loop().create_future()
        await self._queue.put((cmd, key, kwargs, future, cache_key))
        return await future

    async def _worker(self):
        while True:
            # Block until at least one request arrives
            item = await self._queue.get()
            batch = [item]

            # Drain queue (Batching)
            while not self._queue.empty():
                batch.append(self._queue.get_nowait())
                self._queue.task_done()
            self._queue.task_done()

            if not batch:
                continue

            try:
                pipe = random.choice(self._replicas).pipeline()
                for cmd, key, kwargs, _, _ in batch:
                    if cmd == "hgetall":
                        pipe.hgetall(key)
                    elif cmd == "zrange":
                        pipe.zrange(name=key, **kwargs)

                results = await pipe.execute()

                for i, res in enumerate(results):
                    _, _, _, fut, ckey = batch[i]
                    if not fut.done():
                        # if ckey:
                        #     self._global_cache[ckey] = res
                        fut.set_result(res)
            except Exception as e:
                for _, _, _, fut, _ in batch:
                    if not fut.done():
                        fut.set_exception(e)
