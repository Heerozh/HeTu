import asyncio
import time
import docker
import numpy as np
import argparse

from hetu.data import (
    define_component, Property, BaseComponent,
    RedisComponentBackend, RedisClientPool,
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


async def run_bench(inst, redis_address):
    backend = RedisClientPool({"master": redis_address})
    item_data = RedisComponentBackend(Item, inst, 1, backend)
    # clean db
    keys = backend.io.keys(f'{item_data._root_prefix}*')
    if keys:
        backend.io.delete(*keys)
    print(f'clean {len(keys)} rows')

    import os
    print("pid:", os.getpid())

    async def test_insert(i):
        retry = 0
        while True:
            try:
                async with backend.transaction(1) as trans:
                    tbl = item_data.attach(trans)
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

    async def test_select(i):
        retry = 0
        while True:
            try:
                async with backend.transaction(1) as trans:
                    tbl = item_data.attach(trans)
                    row = await tbl.select(i+1)
            except RaceCondition as e:
                print(e)
                retry += 1
                continue
            return retry

    async def test_update(i):
        retry = 0
        while True:
            try:
                async with backend.transaction(1) as trans:
                    tbl = item_data.attach(trans)
                    row = await tbl.select(i+1)
                    row.name = f'Itm{i}'
                    await tbl.update(row.id, row)
            except RaceCondition as e:
                print(e)
                retry += 1
                continue
            return retry

    async def test_delete(i):
        retry = 0
        while True:
            try:
                async with backend.transaction(1) as trans:
                    tbl = item_data.attach(trans)
                    row = await tbl.select(i+1)
                    await tbl.delete(row.id)
            except RaceCondition as e:
                print(e)
                retry += 1
                continue
            return retry

    async def test_direct_update(i):
        name = await backend.aio.hget(f'{item_data._key_prefix}{i+1}', 'name')
        await backend.aio.hset(f'{item_data._key_prefix}{i+1}', 'name', f'It2{i}')
        return 0

    async def test_direct_update_pipeline(i):
        p = backend.aio.pipeline()
        await p.watch(f'{item_data._key_prefix}{i+1}')
        name = await p.hget(f'{item_data._key_prefix}{i+1}', 'name')
        p.multi()
        p.hset(f'{item_data._key_prefix}{i+1}', 'name', f'It2{i}')
        await p.execute()
        await p.reset()
        return 0

    async def test_query_1(i):
        retry = 0
        while True:
            try:
                async with backend.transaction(1) as trans:
                    tbl = item_data.attach(trans)
                    rows = await tbl.query("owner", i, i+10, limit=1)
            except RaceCondition as e:
                print(e)
                retry += 1
                continue
            return retry

    async def test_query_10(i):
        retry = 0
        while True:
            try:
                async with backend.transaction(1) as trans:
                    tbl = item_data.attach(trans)
                    rows = await tbl.query("owner", i, i+10, limit=10)
            except RaceCondition as e:
                print(e)
                retry += 1
                continue
            return retry

    t, qps = await timeit(test_insert, 3000, 1)
    # 单worker 1000/s
    # assert qps >= 500, 'benchmark redis太慢，检查下'

    t, qps = await timeit(test_select, 6000, 1)
    # 单worker pipeline get 2000/s

    t, qps = await timeit(test_update, 3000, 1)
    # 单worker 1000/s

    t, qps = await timeit(test_query_1, 3000, 1)
    # 单worker 1500/s

    t, qps = await timeit(test_query_10, 3000, 1)
    # 单worker 300/s

    t, qps = await timeit(test_delete, 3000, 1)
    # 单worker 1000/s

    t, qps = await timeit(test_direct_update, 6000, 1)
    # 单worker 3000/s，因为没有watch，所以较高

    t, qps = await timeit(test_direct_update_pipeline, 6000, 1)
    # 单worker 2000/s，因为watch多了1次io，且redis也会因为watch降低性能。去掉watch和test_direct_update一样

    # 等完成分发后再测试整个流程的


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='hetu', description='Hetu Data Server')
    parser.add_argument("--instance_name", default="test")
    parser.add_argument("--address")
    args = parser.parse_args()

    # 启动local redis docker
    address = args.address
    if args.address is None:
        address = 'redis://127.0.0.1:23318/0'
        client = docker.from_env()
        try:
            client.containers.get('hetu_test_redis').kill()
            client.containers.get('hetu_test_redis').remove()
        except (docker.errors.NotFound, docker.errors.APIError):
            pass
        container = client.containers.run(
            "redis:latest", detach=True, ports={'6379/tcp': 23318},
            name='hetu_test_redis', auto_remove=True)

    asyncio.run(run_bench(args.instance_name, address))
    if args.address is None:
        container.kill()
