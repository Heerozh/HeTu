"""
redis-py 异步连接池对比：每条命令在客户端进程里花多少 CPU、开了多少条连接。

  blocking  redis-py BlockingConnectionPool(max_connections)：满了排队，但每次取/还都过
            asyncio.Condition + 定时器
  plain     redis-py ConnectionPool(2**31)：redis-py 7.x 的默认行为，并发多少就开多少条连接
  hetu      HeTuConnectionPool(max_connections)：满了才排队，平时取/还不付排队的代价

每组 3 轮取中位数，单元格为 "CPU µs/次 / 连接数"。并发超过 max_connections 的那组
能看到排队路径的开销。

用法：
    cd benchmark
    uv run python redis_pool_bench.py --url redis://127.0.0.1:6379/0

会往库里写 100 个 `hetu:poolbench:*` 哈希键。
"""

import argparse
import asyncio
import statistics
import time

import redis.asyncio as ra

from hetu.data.backend.redis.pool import HeTuConnectionPool

KEYS = [f"hetu:poolbench:{{CLU0}}:id:{i}" for i in range(100)]


def make_client(kind: str, url: str, max_connections: int) -> ra.Redis:
    if kind == "blocking":
        pool = ra.BlockingConnectionPool.from_url(
            url, max_connections=max_connections, timeout=5
        )
    elif kind == "plain":
        pool = ra.ConnectionPool.from_url(url, max_connections=2**31)
    elif kind == "hetu":
        pool = HeTuConnectionPool.from_url(
            url, max_connections=max_connections, timeout=5
        )
    else:
        raise ValueError(kind)
    return ra.Redis.from_pool(pool)


async def prepare(url: str):
    r = ra.Redis.from_url(url)
    async with r.pipeline(transaction=False) as p:
        for i, k in enumerate(KEYS):
            p.hset(
                k,
                mapping={
                    "id": str(10**17 + i),
                    "_version": "3",
                    "owner": "12345",
                    "name": f"Hero{i}",
                    "x": "12.5",
                    "y": "33.25",
                    "hp": "100",
                },
            )
        await p.execute()
    await r.aclose()


async def op_hgetall(r: ra.Redis, i: int):
    await r.hgetall(KEYS[i % 100])  # type: ignore[misc]


async def op_pipe1(r: ra.Redis, i: int):
    async with r.pipeline(transaction=False) as p:
        p.hgetall(KEYS[i % 100])
        await p.execute()


async def op_pipe10(r: ra.Redis, i: int):
    async with r.pipeline(transaction=False) as p:
        for j in range(10):
            p.hgetall(KEYS[(i + j) % 100])
        await p.execute()


OPS = {"hgetall": op_hgetall, "pipe1": op_pipe1, "pipe10": op_pipe10}


async def run_one(kind, url, max_connections, op, conc, total) -> tuple[float, int]:
    r = make_client(kind, url, max_connections)
    fn = OPS[op]
    await asyncio.gather(*(fn(r, i) for i in range(conc)))  # 预热，建好连接
    per_task = total // conc

    async def worker(w: int):
        base = w * per_task
        for i in range(per_task):
            await fn(r, base + i)

    cpu0 = time.process_time()
    await asyncio.gather(*(worker(w) for w in range(conc)))
    cpu = time.process_time() - cpu0
    pool = r.connection_pool
    n_conn = len(pool._available_connections) + len(pool._in_use_connections)
    await r.aclose()
    return cpu * 1e6 / (per_task * conc), n_conn


async def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])  # type: ignore[union-attr]
    ap.add_argument("--url", default="redis://127.0.0.1:6379/0")
    ap.add_argument("--max-connections", type=int, default=64)
    ap.add_argument("--ops", type=int, default=40000, help="每组总命令数")
    args = ap.parse_args()

    await prepare(args.url)
    kinds = ["blocking", "plain", "hetu"]
    over = args.max_connections * 2
    plan = [
        ("hgetall", 32, args.ops),
        ("pipe1", 32, args.ops),
        ("pipe10", 32, args.ops // 5),
        ("hgetall", over, args.ops),  # 并发超过上限：排队路径
    ]
    print(f"{'op':<8} {'conc':>4} " + " ".join(f"{k:>14}" for k in kinds))
    for op, conc, total in plan:
        cells = []
        for kind in kinds:
            samples, n_conn = [], 0
            for _ in range(3):
                us, n_conn = await run_one(
                    kind, args.url, args.max_connections, op, conc, total
                )
                samples.append(us)
            cells.append(f"{statistics.median(samples):7.1f}/{n_conn:<4d}")
        print(f"{op:<8} {conc:>4} " + " ".join(f"{c:>14}" for c in cells), flush=True)


if __name__ == "__main__":
    asyncio.run(main())
