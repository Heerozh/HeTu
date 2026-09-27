"""
同查询扇出（全服聊天）测量：很多连接订同一个范围查询时，一条消息的订阅成本。

模型
----
- 一个进程 = 一个 worker：C 个 SubscriptionBroker（= C 个连接，不经过 WebSocket），每个都
  subscribe_range(ChatMessage, "id", 0, MAX, LIMIT, desc=True)——examples/chat 客户端
  "最近 N 条消息"的写法。
- 先灌 LIMIT 条消息，再按 RATE 条/秒插入 M 条（同进程写，写入本身的成本可忽略）。每条消息
  让每个连接收到"新行进入 + 最旧一行离开"。
- 默认开订阅读合并（同一 backend 的连接共享通知触发的读，见
  docs/superpowers/specs/2026-09-28-sub-shared-reads-design.md）；--no-share 关掉对比。

输出
----
每条消息 × 每连接的 worker CPU、读副本的 CPU 与出网字节、各读命令的次数；共享读层的计数；
每连接常驻内存、订阅耗时。副本的 CPU 含网络收发，Docker/WSL2 下偏高。

用法
----
    cd benchmark
    uv run python sub_fanout_chat.py --conns 200 --limit 1024
    uv run python sub_fanout_chat.py --conns 200 --limit 1024 --no-share
    uv run python sub_fanout_chat.py --conns 200 --limit 50 --profile

会 FLUSHALL 指定的 Redis，请用专用实例（默认 127.0.0.1:23400 主 / 23401 副本，同 sub_budget.py）。
"""

import argparse
import asyncio
import cProfile
import gc
import logging
import pstats
import time

import numpy as np
import psutil
import redis

import hetu
from hetu.common.snowflake_id import SnowflakeID
from hetu.data.backend import Backend
from hetu.data.component import ComponentDefines
from hetu.data.shared_reads import SharedReads
from hetu.data.sub import SubscriptionBroker
from hetu.manager import ComponentTableManager
from hetu.system import SystemClusters, SystemContext

NAMESPACE = "chatbench"
INSTANCE = "chatbench"
MAX_ID = 2**63 - 1
READ_COMMANDS = ("zrange", "hgetall", "subscribe", "unsubscribe")


@hetu.define_component(
    namespace=NAMESPACE, volatile=True, permission=hetu.Permission.EVERYBODY
)
class ChatMessage(hetu.BaseComponent):
    owner: np.int64 = hetu.property_field(0, index=True)
    name: str = hetu.property_field("", dtype="U32")
    text: str = hetu.property_field("", dtype="U256")
    kind: str = hetu.property_field("chat", dtype="U16")
    created_at_ms: np.int64 = hetu.property_field(0, index=True)


@hetu.define_system(
    namespace=NAMESPACE, components=(ChatMessage,), permission=hetu.Permission.EVERYBODY
)
async def _pin_chat(ctx: hetu.SystemContext):
    """只是为了让 ChatMessage 进簇"""


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


def snapshot(r: redis.Redis) -> dict:
    """读副本（servant）上的 CPU、出网字节与读命令统计"""
    commands = r.info("commandstats")
    cpu = r.info("cpu")
    out = {
        "cpu": float(cpu["used_cpu_user"]) + float(cpu["used_cpu_sys"]),
        "net_out": float(r.info("stats")["total_net_output_bytes"]),
    }
    for name in READ_COMMANDS:
        stat = commands.get(f"cmdstat_{name}", {"calls": 0, "usec": 0})
        out[name] = (int(stat["calls"]), int(stat["usec"]))
    return out


async def run(args) -> None:
    logging.getLogger("HeTu.root").setLevel(logging.ERROR)
    if SystemClusters().get_clusters(NAMESPACE) is None:
        SystemClusters().build_clusters(NAMESPACE)
        SystemClusters().build_endpoints()
    SnowflakeID().init(1, 0)

    redis.Redis.from_url(args.master).flushall()
    servant_url = args.replica or args.master
    servant_io = redis.Redis.from_url(servant_url)
    config = {"type": "redis", "master": args.master}
    if args.replica:
        config["servants"] = [args.replica]
    backend = Backend(config)
    backend.post_configure(ComponentDefines().get_all())
    tables = ComponentTableManager(NAMESPACE, INSTANCE, {"default": backend})
    tables.check_and_create_new_tables()
    table = tables.get_table(ChatMessage)
    assert table

    async def say(i: int) -> None:
        async with table.session() as session:
            row = ChatMessage.new_row()
            row.owner = i % 1000 + 1
            row.name = f"user{i % 1000}"
            row.text = f"hello world message number {i} " * 3
            row.created_at_ms = int(time.time() * 1000)
            await session.using(ChatMessage).insert(row)

    for i in range(args.limit):
        await say(i)
    await backend.wait_for_synced()

    ctx = make_ctx()
    gc.collect()
    rss0 = psutil.Process().memory_info().rss
    brokers: list[SubscriptionBroker] = []
    t0 = time.perf_counter()
    for _ in range(args.conns):
        reads = SharedReads(backend, share=False) if args.no_share else None
        broker = SubscriptionBroker(backend, shared_reads=reads)
        sub_id, rows = await broker.subscribe_range(
            table, ctx, "id", 0, MAX_ID, args.limit, True
        )
        assert sub_id and len(rows) == args.limit, (sub_id, len(rows))
        brokers.append(broker)
    subscribe_ms = (time.perf_counter() - t0) * 1000 / args.conns
    gc.collect()
    rss_kb = (psutil.Process().memory_info().rss - rss0) / 1024 / args.conns

    added = [0] * len(brokers)
    running = True

    async def consume(idx: int, broker: SubscriptionBroker) -> None:
        while running:
            for rows in (await broker.get_updates()).values():
                added[idx] += sum(row is not None for row in rows.values())

    tasks = [asyncio.create_task(consume(i, b)) for i, b in enumerate(brokers)]

    def reads_so_far() -> int:
        s = snapshot(servant_io)
        return s["zrange"][0] + s["hgetall"][0]

    # 订阅生效后的补读消化完：读命令数 1 秒内不再变化
    last = -1
    while (now := reads_so_far()) != last:
        last = now
        await asyncio.sleep(1.0)
    for i in range(len(added)):
        added[i] = 0
    stats = SharedReads.of(backend).stats
    stats.clear()

    profile = cProfile.Profile() if args.profile else None
    before = snapshot(servant_io)
    cpu0 = time.process_time()
    if profile:
        profile.enable()
    for i in range(args.msgs):
        await say(args.limit + i)
        await asyncio.sleep(1 / args.rate)
    deadline = time.monotonic() + 20
    while min(added) < args.msgs and time.monotonic() < deadline:
        await asyncio.sleep(0.05)
    await asyncio.sleep(1.0)  # 新行频道的补读、退订都算进来
    if profile:
        profile.disable()
    cpu = time.process_time() - cpu0
    after = snapshot(servant_io)

    running = False
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    for broker in brokers:
        await broker.close()
    await backend.close()

    m, c = args.msgs, args.conns
    print(
        f"conns={c} limit={args.limit} msgs={m} rate={args.rate}/s "
        f"share={not args.no_share} replica={bool(args.replica)}"
    )
    print(f"  subscribe        : {subscribe_ms:.1f} ms/conn, RSS {rss_kb:.0f} KB/conn")
    print(f"  delivered        : min {min(added)} / max {max(added)} (expect {m})")
    print(f"  worker CPU       : {cpu * 1e6 / m / c:.0f} us per (msg, conn)")
    print(
        f"  servant CPU      : {(after['cpu'] - before['cpu']) * 1000 / m:.2f} ms/msg"
    )
    print(
        f"  servant net out  : {(after['net_out'] - before['net_out']) / m / 1024:.1f}"
        " KB/msg"
    )
    for name in READ_COMMANDS:
        calls = after[name][0] - before[name][0]
        usec = after[name][1] - before[name][1]
        if calls:
            print(f"  {name:<16} : {calls / m:.1f} calls/msg, {usec / m:.0f} us/msg")
    if not args.no_share:
        print(f"  shared reads     : {dict(stats)}")
    if profile:
        pstats.Stats(profile).sort_stats("cumulative").print_stats(30)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])  # type: ignore[union-attr]
    ap.add_argument("--master", default="redis://127.0.0.1:23400/0")
    ap.add_argument("--replica", default="", help="读副本；不给则读 master")
    ap.add_argument("--conns", type=int, default=200, help="连接数（同一 worker）")
    ap.add_argument("--limit", type=int, default=1024, help="每个连接订阅最近多少条")
    ap.add_argument("--msgs", type=int, default=10, help="计时窗口里发多少条消息")
    ap.add_argument("--rate", type=float, default=2.0, help="每秒几条消息")
    ap.add_argument("--no-share", action="store_true", help="关掉订阅读合并对比")
    ap.add_argument("--profile", action="store_true", help="计时窗口里跑 cProfile")
    asyncio.run(run(ap.parse_args()))


if __name__ == "__main__":
    main()
