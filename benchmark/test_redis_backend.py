import asyncio
import time
import docker
import numpy as np

from hetu.data import (
    define_component, Property, BaseComponent,
    RedisComponentBackend, RedisBackendClientPool,
    RaceCondition
)


@define_component(namespace="ssw")
class Item(BaseComponent):
    owner: np.int64 = Property(0, unique=False, index=True)
    model: np.int32 = Property(0, unique=False, index=True)
    qty: np.int16 = Property(1, unique=False, index=False)
    level: np.int8 = Property(1, unique=False, index=False)
    time: np.int64 = Property(0, unique=True, index=True)
    name: 'U16' = Property("", unique=True, index=False)
    used: bool = Property(False, unique=False, index=True)


async def timeit(func, repeat=1, repeat_mul=1, concurrency=100, *args):
    # 限制并发。不能用batching， batch慢，因为要等所有完成，而不是完成一个继续下一个
    semaphore = asyncio.Semaphore(concurrency)

    async def limited_run(*_args):
        async with semaphore:
            return await func(*_args)

    s = time.perf_counter()
    retry = await asyncio.gather(*[limited_run(i, *args) for i in range(repeat)])
    cost = time.perf_counter() - s
    print(f"{func.__name__}*{repeat} 耗时: {cost:.2f}s, QPS: {repeat * repeat_mul / cost:.0f}/s"
          f", 事务冲突次数: {sum(retry)}")
    return cost, repeat * repeat_mul / cost


async def run_bench():
    backend = RedisBackendClientPool({"master": "redis://127.0.0.1:23318/0"})
    item_data = RedisComponentBackend(Item, 'test', 1, backend)
    # clean db
    async with item_data.transaction() as _tbl:
        _rows = await _tbl.query('id', -np.inf, +np.inf)
        for _row in _rows:
            await _tbl.delete(_row.id)
        print(f'clean {len(_rows)} rows')

    import os
    print("pid:", os.getpid())

    async def test_insert(i):
        retry = 0
        while True:
            try:
                async with item_data.transaction() as tbl:
                    row = Item.new_row()
                    row.name = f'Item{i}'
                    row.owner = i
                    row.time = i
                    row.model = i
                    await tbl.insert(row)
            except RaceCondition as e:
                print(e)
                retry += 1
                continue
            return retry

    t, qps = await timeit(test_insert, 30000, 1)
    # 单worker 1000/s


if __name__ == '__main__':
    # 启动local redis docker
    client = docker.from_env()
    try:
        client.containers.get('hetu_test_redis').kill()
        client.containers.get('hetu_test_redis').remove()
    except (docker.errors.NotFound, docker.errors.APIError):
        pass
    container = client.containers.run(
        "redis:latest", detach=True, ports={'6379/tcp': 23318},
        name='hetu_test_redis', auto_remove=True)

    asyncio.run(run_bench())
    container.kill()
