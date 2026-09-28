"""
一个副本周期性变慢（CLIENT PAUSE）时，订阅推送的延迟：按订阅的读绑在哪个副本上分组看，慢副本会不会
把绑在健康副本上的订阅也拖慢（worker 级订阅器的 tick 屏障：一个 tick 里最慢的读决定这批何时交付）。
同一脚本跑两份代码（PYTHONPATH 指向另一个 worktree 时跑那份），进程内 N 个 SubscriptionBroker。

- 每个 broker 订 Item.owner == 自己（点查询；范围订阅的读固定走订阅时随机选的副本）；
- 写进程按 RATE 插入物品（行里带 ts）；只统计插入（值频道通知 → _rerange，读走订阅绑定的副本）；
- 慢副本：每 PERIOD 秒 CLIENT PAUSE PAUSE_MS ALL 一次。

需要：主 23400、副本 23401、副本 23402（后者被暂停）。

用法：
    uv run python benchmark/sub_servant_slow.py --pause-ms 0            # 基线
    uv run python benchmark/sub_servant_slow.py --pause-ms 150 --period 0.3
    uv run python benchmark/sub_servant_slow.py --pause-ms 150 --pin-mq slow  # 通知全走慢副本

会 FLUSHALL 主库。
"""

import argparse
import asyncio
import contextlib
import multiprocessing as mp
import os
import random
import threading
import time

import redis
import sub_scenarios_app as app

import hetu.data.sub as sub_mod
from hetu.common.snowflake_id import SnowflakeID
from hetu.data.backend import Backend
from hetu.data.component import ComponentDefines
from hetu.data.sub import SubscriptionBroker
from hetu.manager import ComponentTableManager
from hetu.system import SystemClusters, SystemContext

MASTER = "redis://127.0.0.1:23400/0"
FAST = "redis://127.0.0.1:23401/0"
SLOW = "redis://127.0.0.1:23402/0"


def setup() -> None:
    import logging

    logging.getLogger("HeTu.root").setLevel(logging.CRITICAL)
    if SystemClusters().get_clusters(app.NAMESPACE) is None:
        SystemClusters().build_clusters(app.NAMESPACE)
        SystemClusters().build_endpoints()


def make(servants: list[str]) -> tuple[Backend, ComponentTableManager]:
    backend = Backend({"type": "redis", "master": MASTER, "servants": servants})
    backend.post_configure(ComponentDefines().get_all())
    mgr = ComponentTableManager(app.NAMESPACE, app.INSTANCE, {"default": backend})
    mgr.check_and_create_new_tables()
    return backend, mgr


def ctx_of(uid: int) -> SystemContext:
    return SystemContext(
        caller=uid,
        connection_id=uid,
        address="bench",
        group="guest",
        user_data={},
        timestamp=0,
        request=None,  # type: ignore[arg-type]
        systems=None,  # type: ignore[arg-type]
    )


def writer(users: int, rate: float, cpus: set[int], stop) -> None:
    os.sched_setaffinity(0, cpus)
    asyncio.run(_writer(users, rate, stop))


async def _writer(users: int, rate: float, stop) -> None:
    setup()
    SnowflakeID().init(1010, 0)
    backend, mgr = make([FAST])
    table = mgr.get_table(app.Item)
    assert table
    rng = random.Random(7)
    interval = 1 / rate
    nxt = time.perf_counter()
    while not stop.is_set():
        nxt += interval
        if (d := nxt - time.perf_counter()) > 0:
            await asyncio.sleep(d)
        async with table.session() as s:
            row = app.Item.new_row()
            row.owner = rng.randrange(1, users + 1)
            row.ts = time.time()
            await s.using(app.Item).insert(row)
    await backend.close()


def pauser(pause_ms: int, period: float, stop: threading.Event) -> None:
    r = redis.Redis.from_url(SLOW)
    while not stop.is_set():
        with contextlib.suppress(Exception):
            r.execute_command("CLIENT", "PAUSE", pause_ms, "ALL")
        stop.wait(period)


def pct(xs: list[float], p: float) -> float:
    if not xs:
        return float("nan")
    s = sorted(xs)
    return s[min(len(s) - 1, int(len(s) * p))]


async def main(args) -> None:
    os.sched_setaffinity(0, {args.cpu})
    setup()
    SnowflakeID().init(1011, 0)
    redis.Redis.from_url(MASTER).flushall()
    await asyncio.sleep(0.5)
    backend, mgr = make([FAST, SLOW])
    if args.subscribe_wait is not None:
        sub_mod.SubscriptionHub.SUBSCRIBE_WAIT_INTERVALS = args.subscribe_wait
    if args.pin_mq:
        # 订阅通知全走一个副本：复现"随机绑一个副本"的情形（分到各副本之前的做法）
        want = FAST if args.pin_mq == "fast" else SLOW
        target = next(sv for sv in backend._servants if sv.urls[0] == want)  # type: ignore[attr-defined]
        backend.get_mq_client = lambda: target.get_mq_client()  # type: ignore[method-assign]
    table = mgr.get_table(app.Item)
    assert table
    async with table.session() as s:
        for uid in range(1, args.conns + 1):
            row = app.Item.new_row()
            row.owner = uid
            await s.using(app.Item).insert(row)
    await backend.wait_for_synced()

    pinned: dict[int, str] = {}
    lat: dict[str, list[float]] = {"fast": [], "slow": []}
    window = [float("inf")]
    brokers = []
    tasks = []

    async def consume(uid: int, b: SubscriptionBroker) -> None:
        while True:
            updates = await b.get_updates()
            now = time.time()
            for rows in updates.values():
                for row in rows.values():
                    if row is not None and float(row["ts"]) >= window[0]:
                        lat[pinned[uid]].append(now - float(row["ts"]))

    for uid in range(1, args.conns + 1):
        b = SubscriptionBroker(backend)
        sid, _rows = await b.subscribe_range(
            table, ctx_of(uid), "owner", uid, limit=100
        )
        assert sid
        servant = b._subs[sid].servant  # type: ignore[attr-defined]
        pinned[uid] = "fast" if servant.urls[0] == FAST else "slow"
        brokers.append(b)
        tasks.append(asyncio.create_task(consume(uid, b)))
    n_fast = sum(v == "fast" for v in pinned.values())
    print(f"conns={args.conns} 读绑健康副本 {n_fast}、绑慢副本 {args.conns - n_fast}")
    await asyncio.sleep(2)

    stop_w = mp.Event()
    wp = mp.Process(
        target=writer, args=(args.conns, args.rate, {12, 13}, stop_w), daemon=True
    )
    wp.start()
    stop_p = threading.Event()
    await asyncio.sleep(2)
    if args.pause_ms > 0:
        threading.Thread(
            target=pauser, args=(args.pause_ms, args.period, stop_p), daemon=True
        ).start()
    await asyncio.sleep(1)
    window[0] = time.time()
    await asyncio.sleep(args.duration)
    stop_w.set()
    stop_p.set()
    await asyncio.sleep(2)
    for k in ("fast", "slow"):
        xs = lat[k]
        print(
            f"  读绑{'健康' if k == 'fast' else '慢'}副本: n={len(xs):6d} "
            f"p50={pct(xs, 0.5) * 1000:7.1f}ms p90={pct(xs, 0.9) * 1000:7.1f}ms "
            f"p99={pct(xs, 0.99) * 1000:7.1f}ms max={max(xs, default=float('nan')) * 1000:7.1f}ms"
        )
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    for b in brokers:
        await b.close()
    await backend.close()
    wp.join(timeout=5)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])  # type: ignore[union-attr]
    ap.add_argument("--conns", type=int, default=400)
    ap.add_argument("--rate", type=float, default=200, help="每秒插入几件物品")
    ap.add_argument("--duration", type=float, default=20)
    ap.add_argument("--pause-ms", type=int, default=150)
    ap.add_argument("--period", type=float, default=0.3)
    ap.add_argument(
        "--pin-mq", choices=("fast", "slow"), default="", help="订阅通知全走这个副本"
    )
    ap.add_argument("--cpu", type=int, default=0, help="订阅进程绑哪个核")
    ap.add_argument(
        "--subscribe-wait",
        type=float,
        default=None,
        help="覆盖 SubscriptionHub.SUBSCRIBE_WAIT_INTERVALS（tick 末尾等 SUBSCRIBE 的上限）",
    )
    mp.set_start_method("fork")
    asyncio.run(main(ap.parse_args()))
