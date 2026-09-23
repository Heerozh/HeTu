"""
订阅预算测量：给定 C 个连接 × 每连接 S 行订阅 × 每秒 W 次写入，测出订阅层的单位成本。

模型
----
- 行分成 Z 个 zone，每个 zone 恰好 S 行（R = Z*S），每个连接 subscribe_range 自己 zone 的
  全部 S 行（AOI 模型：同屏玩家互相可见）。C 个连接均摊到 Z 个 zone，所以一次写入会扇出到
  K = C/Z 个连接（fan-out）。
- 每个连接 = 一个 SubscriptionBroker = 一条独立 Redis pubsub 连接 + puller/consumer 协程，
  与 hetu/server/websocket.py 一致，只是不经过 WebSocket/pipeline（那段用 sub_budget_ws.py
  测）。连接可以分到 --procs 个进程里（每个进程一个事件循环 ≈ 一个 hetu worker）。
- 写入由独立进程完成（走 Session.commit 同款 Lua 提交路径），所以订阅进程的 CPU 统计里只有
  订阅侧成本。行里带 ts 字段（写入时间戳），交付时用来算写→交付延迟。
- --move-ratio 让一部分写入改 zone（AOI 跨区），触发索引订阅的 ZRANGE 对比 + 行频道增删。

输出
----
- 静态成本：每个 (频道, 订阅者) 对在 Redis 侧 / Python 侧占多少内存
- 动态成本：每交付一次更新（一行变更推给一个连接）的 Python CPU µs、Redis 副本 CPU µs、
  Redis 命令数；每收到一条 pubsub 通知（被合批掉的也算）的 Python CPU µs
- 交付率（交付/应交付），写→交付延迟 p50/p90/p99，积压（最老未处理通知的年龄），事件循环卡顿

用法
----
    cd benchmark
    uv run python sub_budget.py --conns 200 --zones 20 --limit 50 --writes 300 --duration 15
    uv run python sub_budget.py --conns 800 --zones 80 --limit 50 --procs 4 --writes 1500
    uv run python sub_budget.py --conns 1000 --zones 100 --limit 50 --writes 0 --duration 3  # 只测静态

会 FLUSHALL 指定的 Redis，请用专用实例（默认 127.0.0.1:23400 主 / 23401 副本）。
"""

import argparse
import asyncio
import csv
import logging
import multiprocessing as mp
import os
import random
import statistics
import time
import tracemalloc
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import redis

import hetu
from hetu.common.snowflake_id import SnowflakeID
from hetu.data.backend import Backend, RaceCondition
from hetu.data.component import ComponentDefines
from hetu.data.sub import SubscriptionBroker
from hetu.manager import ComponentTableManager
from hetu.system import SystemClusters, SystemContext

NAMESPACE = "subbench"
INSTANCE = "subbench"


@hetu.define_component(
    namespace=NAMESPACE, volatile=True, permission=hetu.Permission.EVERYBODY
)
class Actor(hetu.BaseComponent):
    """模拟场景里的一个可见实体：zone 索引用于 AOI 订阅，x/y/hp 是被更新的字段"""

    # point_sub：每个连接点查询订阅自己的 zone，只被本 zone 的进出叫醒
    zone: np.int32 = hetu.property_field(0, index=True, point_sub=True)
    x: np.float32 = hetu.property_field(0)
    y: np.float32 = hetu.property_field(0)
    hp: np.int32 = hetu.property_field(100)
    ts: np.float64 = hetu.property_field(0)  # 写入时刻 time.time()，算延迟用


@hetu.define_system(
    namespace=NAMESPACE, components=(Actor,), permission=hetu.Permission.EVERYBODY
)
async def _pin_actor(ctx: hetu.SystemContext):
    """只是为了让 Actor 进簇"""


# ---------------------------------------------------------------------------
# 公共初始化（所有进程都要跑一遍）
# ---------------------------------------------------------------------------


def setup_registry() -> None:
    logging.getLogger("HeTu.root").setLevel(logging.ERROR)
    logging.getLogger("hetu").setLevel(logging.ERROR)
    if SystemClusters().get_clusters(NAMESPACE) is None:
        SystemClusters().build_clusters(NAMESPACE)
        SystemClusters().build_endpoints()


def make_backend(master: str, replica: str) -> Backend:
    config = {"type": "redis", "master": master, "servants": [replica]}
    backend = Backend(config)
    backend.post_configure(ComponentDefines().get_all())
    return backend


def make_tables(backend: Backend) -> ComponentTableManager:
    tbl_mgr = ComponentTableManager(NAMESPACE, INSTANCE, {"default": backend})
    tbl_mgr.check_and_create_new_tables()
    return tbl_mgr


def make_ctx() -> SystemContext:
    return SystemContext(
        caller=0,
        connection_id=0,
        address="bench",
        group="guest",
        user_data={},
        timestamp=0,
        request=None,  # type: ignore[arg-type]
        systems=None,  # type: ignore[arg-type]
    )


def redis_snapshot(url: str) -> dict[str, float]:
    r = redis.Redis.from_url(url)
    info = r.info()
    r.close()
    return {
        "cpu": float(info["used_cpu_user"]) + float(info["used_cpu_sys"]),
        "cmds": float(info["total_commands_processed"]),
        "net_out": float(info["total_net_output_bytes"]),
        "net_in": float(info["total_net_input_bytes"]),
        "mem": float(info["used_memory"]),
        "pubsub_channels": float(info.get("pubsub_channels", 0)),
        "clients": float(info["connected_clients"]),
    }


def pct(samples: list[float], p: float) -> float:
    if not samples:
        return float("nan")
    s = sorted(samples)
    return s[min(len(s) - 1, int(len(s) * p))]


async def prepare_data(args) -> list[int]:
    """清库、建表、灌 Z×S 行，返回全部行 id"""
    setup_registry()
    SnowflakeID().init(1, 0)
    backend = make_backend(args.master, args.replica)
    from hetu.data.backend.redis import RedisBackendClient

    assert isinstance(backend.master, RedisBackendClient)
    backend.master.io.flushall()
    await asyncio.sleep(0.5)
    table = make_tables(backend).get_table(Actor)
    assert table
    ids: list[int] = []
    for z in range(args.zones):
        async with table.session() as s:
            repo = s.using(Actor)
            for _ in range(args.limit):
                row = Actor.new_row()
                row.zone = z
                await repo.insert(row)
                ids.append(int(row.id))
    await backend.wait_for_synced()
    await backend.close()
    return ids


# ---------------------------------------------------------------------------
# 写进程
# ---------------------------------------------------------------------------


def writer_proc(
    master: str,
    replica: str,
    ids: list[int],
    rate: float,
    seed: int,
    written: Any,
    raced: Any,
    stop: Any,
    coroutines: int,
    zones: int = 1,
    move_ratio: float = 0.0,
) -> None:
    asyncio.run(
        _writer_main(
            master,
            replica,
            ids,
            rate,
            seed,
            written,
            raced,
            stop,
            coroutines,
            zones,
            move_ratio,
        )
    )


async def _writer_main(
    master,
    replica,
    ids,
    rate,
    seed,
    written,
    raced,
    stop,
    coroutines,
    zones,
    move_ratio,
):
    setup_registry()
    backend = make_backend(master, replica)
    table = make_tables(backend).get_table(Actor)
    assert table
    per_co_rate = rate / coroutines if rate > 0 else 0

    async def one_writer(co_seed: int):
        rng = random.Random(co_seed)
        interval = 1 / per_co_rate if per_co_rate > 0 else 0
        next_t = time.perf_counter()
        local_written = 0
        local_raced = 0
        last_flush = time.perf_counter()
        while not stop.is_set():
            row_id = rng.choice(ids)
            try:
                async with table.session() as s:
                    repo = s.using(Actor)
                    row = await repo.get(id=row_id)
                    assert row is not None
                    row.x = rng.random() * 1000
                    row.y = rng.random() * 1000
                    row.ts = time.time()
                    if move_ratio > 0 and rng.random() < move_ratio:
                        row.zone = (int(row.zone) + rng.randrange(1, zones)) % zones
                    await repo.update(row)
                local_written += 1
            except RaceCondition:
                local_raced += 1
            now = time.perf_counter()
            if now - last_flush > 0.5:
                with written.get_lock():
                    written.value += local_written
                with raced.get_lock():
                    raced.value += local_raced
                local_written = local_raced = 0
                last_flush = now
            if interval:
                next_t += interval
                delay = next_t - time.perf_counter()
                if delay > 0:
                    await asyncio.sleep(delay)
                elif delay < -1.0:
                    next_t = time.perf_counter()  # 追不上就放弃追赶，避免爆发
            else:
                await asyncio.sleep(0)
        with written.get_lock():
            written.value += local_written
        with raced.get_lock():
            raced.value += local_raced

    await asyncio.gather(*(one_writer(seed * 1000 + i) for i in range(coroutines)))
    await backend.close()


# ---------------------------------------------------------------------------
# 订阅进程：模拟连接 + 统计
# ---------------------------------------------------------------------------


@dataclass
class Stats:
    delivered: int = 0  # 交付的行更新数（一行推给一个连接算 1）
    deleted: int = 0  # 交付的行删除/离开范围数
    notified: int = (
        0  # hub 从 Redis 收到的 pubsub 消息条数（含被合批掉的；一条可分发给多个连接）
    )
    lat_samples: list[float] = field(default_factory=list)
    lat_seen: int = 0
    loop_lag: list[float] = field(default_factory=list)
    backlog_age: list[float] = field(default_factory=list)

    def add_latency(self, v: float, k: int = 100_000):
        # 蓄水池采样，防止样本爆内存
        self.lat_seen += 1
        if len(self.lat_samples) < k:
            self.lat_samples.append(v)
        else:
            j = random.randrange(self.lat_seen)
            if j < k:
                self.lat_samples[j] = v

    def reset(self):
        self.delivered = self.deleted = self.notified = 0
        self.lat_samples.clear()
        self.lat_seen = 0
        self.loop_lag.clear()
        self.backlog_age.clear()


def subscriber_proc(args, proc_idx: int, zones: list[int], ready, go, stop, result_q):
    asyncio.run(_subscriber_main(args, proc_idx, zones, ready, go, stop, result_q))


async def _subscriber_main(args, proc_idx, zones, ready, go, stop, result_q):
    """一个进程 = 一个事件循环，承载 len(zones) 个连接"""
    setup_registry()
    backend = make_backend(args.master, args.replica)
    table = make_tables(backend).get_table(Actor)
    assert table
    ctx = make_ctx()
    limit = args.limit
    import psutil

    proc = psutil.Process(os.getpid())

    # 建立连接（broker）并订阅，同时测静态成本
    rss0 = proc.memory_info().rss
    tracemalloc.start()
    tm0, _ = tracemalloc.get_traced_memory()
    brokers: list[SubscriptionBroker] = []
    t0 = time.perf_counter()
    for z in zones:
        broker = SubscriptionBroker(backend)
        sub_id, rows = await broker.subscribe_range(table, ctx, "zone", z, limit=limit)
        assert sub_id and len(rows) == limit, (sub_id, len(rows))
        brokers.append(broker)
    subscribe_time = time.perf_counter() - t0
    tm1, _ = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    rss1 = proc.memory_info().rss

    stats = Stats()
    running = True

    # 统计本进程 hub 从 Redis 收到的 pubsub 消息条数（一条消息可分发给本进程多个连接）
    hub = backend._servants[0]._hub  # type: ignore[attr-defined]
    assert hub is not None
    hub_on_message = hub._on_message

    def counting_on_message(msg):
        stats.notified += 1
        hub_on_message(msg)

    hub._pubsub.on_message = counting_on_message

    async def consumer(broker: SubscriptionBroker):
        while running:
            updates = await broker.get_updates()
            now = time.time()
            for rows in updates.values():
                for row in rows.values():
                    if row is None:
                        stats.deleted += 1
                    else:
                        stats.delivered += 1
                        stats.add_latency(now - float(row["ts"]))

    async def sampler():
        # 事件循环卡顿 + 积压年龄，每 100ms 采一次
        while running:
            t = time.perf_counter()
            await asyncio.sleep(0.1)
            stats.loop_lag.append(time.perf_counter() - t - 0.1)
            now = time.monotonic()
            oldest = now
            for b in brokers:
                dq = b._mq_client.pulled_deque  # (收到时刻 monotonic, 频道名) 的 FIFO
                if dq:
                    oldest = min(oldest, dq[0][0])
            stats.backlog_age.append(now - oldest)

    tasks = [asyncio.create_task(sampler())]
    for b in brokers:
        tasks.append(asyncio.create_task(consumer(b)))

    with ready.get_lock():
        ready.value += 1
    # 等主进程发令开始计时窗口
    while not go.is_set():
        await asyncio.sleep(0.05)
    stats.reset()
    prof = None
    if args.profile and proc_idx == 0:
        import cProfile

        prof = cProfile.Profile()
        prof.enable()
    cpu_a = time.process_time()
    wall_a = time.perf_counter()
    while not stop.is_set():
        await asyncio.sleep(0.05)
    wall = time.perf_counter() - wall_a
    cpu = time.process_time() - cpu_a
    if prof is not None:
        prof.disable()
        import pstats

        pstats.Stats(prof).sort_stats("tottime").print_stats(30)

    running = False
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    for b in brokers:
        await b.close()
    await backend.close()

    result_q.put(
        {
            "conns": len(zones),
            "subscribe_s": subscribe_time,
            "tm_bytes": tm1 - tm0,
            "rss_bytes": rss1 - rss0,
            "wall": wall,
            "cpu": cpu,
            "delivered": stats.delivered,
            "deleted": stats.deleted,
            "notified": stats.notified,
            "lat": stats.lat_samples,
            "loop_lag": stats.loop_lag,
            "backlog_age": stats.backlog_age,
        }
    )


# ---------------------------------------------------------------------------
# 主进程编排
# ---------------------------------------------------------------------------


def run(args: argparse.Namespace) -> dict[str, Any]:
    zones, limit, conns, procs = args.zones, args.limit, args.conns, args.procs
    ids = asyncio.run(prepare_data(args))
    hot_ids = ids if args.hot_rows <= 0 else random.sample(ids, args.hot_rows)

    # 1. 起订阅进程，等全部订阅完成
    rep0 = redis_snapshot(args.replica)
    ready = mp.Value("q", 0)
    go = mp.Event()
    stop = mp.Event()
    result_q: Any = mp.Queue()
    all_zones = [i % zones for i in range(conns)]
    sprocs: list[mp.Process] = []
    for i in range(procs):
        zs = all_zones[i::procs]
        if not zs:
            continue
        p = mp.Process(
            target=subscriber_proc, args=(args, i, zs, ready, go, stop, result_q)
        )
        p.start()
        sprocs.append(p)
    t0 = time.time()
    while ready.value < len(sprocs):
        time.sleep(0.2)
        if time.time() - t0 > 600:
            raise TimeoutError("subscriber procs not ready")
    time.sleep(1.0)
    rep1 = redis_snapshot(args.replica)
    channel_pairs = conns * (limit + 1)  # 每连接: S 个行频道 + 1 个索引频道

    # 2. 起写进程
    written = mp.Value("q", 0)
    raced = mp.Value("q", 0)
    wstop = mp.Event()
    wprocs: list[mp.Process] = []
    if args.writers > 0 and (args.writes > 0 or args.unlimited):
        per_proc_rate = args.writes / args.writers if args.writes > 0 else 0
        for w in range(args.writers):
            p = mp.Process(
                target=writer_proc,
                args=(
                    args.master,
                    args.replica,
                    hot_ids,
                    per_proc_rate,
                    w + 1,
                    written,
                    raced,
                    wstop,
                    args.writer_coroutines,
                    zones,
                    args.move_ratio,
                ),
            )
            p.start()
            wprocs.append(p)
        time.sleep(2.0)  # 等写进程连上并热身

    # 3. 计时窗口
    with written.get_lock():
        written.value = 0
    with raced.get_lock():
        raced.value = 0
    rep_a = redis_snapshot(args.replica)
    mas_a = redis_snapshot(args.master)
    wall_a = time.perf_counter()
    go.set()
    time.sleep(args.duration)
    stop.set()
    wall = time.perf_counter() - wall_a
    rep_b = redis_snapshot(args.replica)
    mas_b = redis_snapshot(args.master)
    with written.get_lock():
        n_written = written.value
    with raced.get_lock():
        n_raced = raced.value

    # 4. 收尾
    wstop.set()
    for p in wprocs:
        p.join(timeout=10)
    results = [result_q.get(timeout=120) for _ in sprocs]
    for p in sprocs:
        p.join(timeout=30)

    # 5. 汇总
    delivered = sum(r["delivered"] for r in results)
    deleted = sum(r["deleted"] for r in results)
    notified = sum(r["notified"] for r in results)
    cpu = sum(r["cpu"] for r in results)  # 所有订阅进程 CPU 秒之和
    lat: list[float] = []
    loop_lag: list[float] = []
    backlog: list[float] = []
    for r in results:
        lat.extend(r["lat"])
        loop_lag.extend(r["loop_lag"])
        backlog.extend(r["backlog_age"])
    fanout = conns / zones
    writes_ps = n_written / wall
    # 应交付：每次写入扇出到 K 个连接（改 zone 的写在旧 zone 是删除、新 zone 是新增，各 K）
    offered = writes_ps * fanout
    delivered_ps = delivered / wall
    rep_cpu = rep_b["cpu"] - rep_a["cpu"]
    rep_cmds = rep_b["cmds"] - rep_a["cmds"]
    mas_cpu = mas_b["cpu"] - mas_a["cpu"]
    mas_cmds = mas_b["cmds"] - mas_a["cmds"]
    per_row_hz = writes_ps / len(hot_ids) if hot_ids else 0
    res: dict[str, Any] = {
        # 参数
        "conns": conns,
        "procs": len(sprocs),
        "zones": zones,
        "limit": limit,
        "rows": zones * limit,
        "hot_rows": len(hot_ids),
        "fanout": fanout,
        "move_ratio": args.move_ratio,
        "writers": len(wprocs),
        "duration_s": round(wall, 1),
        # 静态成本
        "subscribe_ms_per_conn": round(
            sum(r["subscribe_s"] for r in results) / conns * 1000, 1
        ),
        "redis_bytes_per_chan_sub": round((rep1["mem"] - rep0["mem"]) / channel_pairs),
        "py_bytes_per_chan_sub": round(
            sum(r["tm_bytes"] for r in results) / channel_pairs
        ),
        "rss_bytes_per_conn": round(sum(r["rss_bytes"] for r in results) / conns),
        "redis_pubsub_channels": int(rep1["pubsub_channels"]),
        "redis_clients": int(rep1["clients"]),
        # 负载
        "writes_ps": round(writes_ps),
        "raced": n_raced,
        "per_row_write_hz": round(per_row_hz, 2),
        "offered_ps": round(offered),
        "notified_ps": round(notified / wall),
        "delivered_ps": round(delivered_ps),
        "deleted_ps": round(deleted / wall),
        "deliver_ratio": round(delivered_ps / offered, 3) if offered else float("nan"),
        # 单位成本（CPU 为所有订阅进程之和）
        "py_cores": round(cpu / wall, 3),
        "py_cores_per_proc": round(cpu / wall / len(sprocs), 3),
        "py_us_per_delivered": round(cpu * 1e6 / delivered, 1) if delivered else None,
        "py_us_per_notified": round(cpu * 1e6 / notified, 1) if notified else None,
        "replica_cores": round(rep_cpu / wall, 3),
        "replica_us_per_delivered": (
            round(rep_cpu * 1e6 / delivered, 1) if delivered else None
        ),
        "replica_cmds_ps": round(rep_cmds / wall),
        "replica_cmds_per_delivered": (
            round(rep_cmds / delivered, 2) if delivered else None
        ),
        "replica_net_out_MBps": round(
            (rep_b["net_out"] - rep_a["net_out"]) / wall / 1e6, 2
        ),
        "master_cores": round(mas_cpu / wall, 3),
        "master_cmds_ps": round(mas_cmds / wall),
        # 延迟 / 健康
        "lat_p50_ms": round(pct(lat, 0.5) * 1000, 1),
        "lat_p90_ms": round(pct(lat, 0.9) * 1000, 1),
        "lat_p99_ms": round(pct(lat, 0.99) * 1000, 1),
        "lat_max_ms": round(max(lat, default=float("nan")) * 1000, 1),
        "loop_lag_p99_ms": round(pct(loop_lag, 0.99) * 1000, 1),
        "backlog_age_max_ms": round(max(backlog, default=0) * 1000, 1),
        "backlog_age_mean_ms": round(
            statistics.fmean(backlog) * 1000 if backlog else 0, 1
        ),
    }
    return res


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])  # type: ignore[union-attr]
    ap.add_argument("--master", default="redis://127.0.0.1:23400/0")
    ap.add_argument("--replica", default="redis://127.0.0.1:23401/0")
    ap.add_argument("--conns", type=int, default=100, help="模拟连接数 C")
    ap.add_argument(
        "--procs", type=int, default=1, help="订阅进程数（每个≈一个 worker）"
    )
    ap.add_argument("--zones", type=int, default=10, help="zone 数 Z，扇出 K=C/Z")
    ap.add_argument(
        "--limit", type=int, default=50, help="每连接订阅行数 S（=每 zone 行数）"
    )
    ap.add_argument("--writes", type=float, default=1000, help="目标总写入/秒，0=不写")
    ap.add_argument(
        "--unlimited", action="store_true", help="写入不限速（--writes 0 时）"
    )
    ap.add_argument(
        "--hot-rows", type=int, default=0, help="只写这么多行（0=全部行均匀写）"
    )
    ap.add_argument("--move-ratio", type=float, default=0.0, help="改 zone 的写入占比")
    ap.add_argument("--writers", type=int, default=2, help="写进程数")
    ap.add_argument("--writer-coroutines", type=int, default=8, help="每写进程协程数")
    ap.add_argument("--duration", type=float, default=15, help="计时窗口秒数")
    ap.add_argument("--csv", default="", help="追加结果到此 CSV")
    ap.add_argument("--tag", default="", help="写进 CSV 的备注")
    ap.add_argument(
        "--profile", action="store_true", help="0 号订阅进程跑 cProfile 并打印"
    )
    args = ap.parse_args()

    res = run(args)
    res["tag"] = args.tag
    width = max(len(k) for k in res)
    for k, v in res.items():
        print(f"{k:<{width}} : {v}")
    if args.csv:
        new = not os.path.exists(args.csv)
        with open(args.csv, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(res.keys()))
            if new:
                w.writeheader()
            w.writerow(res)


if __name__ == "__main__":
    mp.freeze_support()
    main()
