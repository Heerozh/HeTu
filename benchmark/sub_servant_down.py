"""
两个副本、其中一个挂掉（docker stop）时订阅层的表现：已有订阅还能不能收到推送、新订阅成功率、副本恢复
后能否自愈。同一脚本跑两份代码（PYTHONPATH 指向另一个 worktree 时跑那份），进程内 20 个
SubscriptionBroker，不经过 WebSocket。

需要：主 23400、副本 23401（容器 hetu-bench-replica）、副本 23402（容器 hetu-bench-replica2）。副本要带
--notify-keyspace-events 启动（见 sub_scenarios_ws.py），不然重启后不再发 keyspace 通知，看起来像没自愈。

用法：
    uv run python benchmark/sub_servant_down.py            # 杀掉通知订得最多的那个副本
    uv run python benchmark/sub_servant_down.py --kill 23401

会 FLUSHALL 主库。
"""

import argparse
import asyncio
import contextlib
import logging
import subprocess
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
REPLICAS = {
    "redis://127.0.0.1:23401/0": "hetu-bench-replica",
    "redis://127.0.0.1:23402/0": "hetu-bench-replica2",
}
MAX_ID = 2**63 - 1
N = 20


def ctx() -> SystemContext:
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


async def docker(*args: str) -> None:
    await asyncio.to_thread(
        subprocess.run, ["docker", *args], check=True, capture_output=True
    )


def pubsub_channels(url: str) -> int:
    return int(redis.Redis.from_url(url).info().get("pubsub_channels", 0))


async def try_subscribe(backend: Backend, table) -> tuple[int, dict[str, int]]:
    """新建 N 个连接各订一次，返回成功数和各类错误数"""
    ok = 0
    errors: dict[str, int] = {}
    for _ in range(N):
        broker = SubscriptionBroker(backend)
        try:
            async with asyncio.timeout(8):
                sub_id, _rows = await broker.subscribe_range(
                    table, ctx(), "id", 0, MAX_ID, 10, True
                )
            ok += bool(sub_id)
        except Exception as e:  # noqa: BLE001
            errors[type(e).__name__] = errors.get(type(e).__name__, 0) + 1
        with contextlib.suppress(Exception):
            await broker.close()
    return ok, errors


async def main(args) -> None:
    logging.getLogger("HeTu.root").setLevel(logging.CRITICAL)
    if SystemClusters().get_clusters(app.NAMESPACE) is None:
        SystemClusters().build_clusters(app.NAMESPACE)
        SystemClusters().build_endpoints()
    SnowflakeID().init(1020, 0)
    redis.Redis.from_url(MASTER).flushall()
    await asyncio.sleep(0.5)
    backend = Backend({"type": "redis", "master": MASTER, "servants": list(REPLICAS)})
    backend.post_configure(ComponentDefines().get_all())
    mgr = ComponentTableManager(app.NAMESPACE, app.INSTANCE, {"default": backend})
    mgr.check_and_create_new_tables()
    table = mgr.get_table(app.ChatMessage)
    assert table

    async def say(i: int) -> int:
        async with table.session() as s:
            row = app.ChatMessage.new_row()
            row.owner = 1
            row.text = f"m{i}"
            row.ts = time.time()
            await s.using(app.ChatMessage).insert(row)
        return int(row.id)

    for i in range(20):
        await say(i)
    await backend.wait_for_synced()

    got: dict[int, set[int]] = {}

    async def consume(i: int, b: SubscriptionBroker) -> None:
        # 旧代码读出错时 get_updates 会抛（生产里那个连接会被断开）：记下来，别让消费协程悄悄死掉
        while True:
            try:
                updates = await b.get_updates()
            except Exception:  # noqa: BLE001
                got.setdefault(i, set()).add(-1)
                return
            for rows in updates.values():
                for rid, row in rows.items():
                    if row is not None:
                        got.setdefault(i, set()).add(int(rid))

    brokers = []
    tasks = []
    for i in range(N):
        b = SubscriptionBroker(backend)
        sid, _rows = await b.subscribe_range(table, ctx(), "id", 0, MAX_ID, 10, True)
        assert sid
        brokers.append(b)
        tasks.append(asyncio.create_task(consume(i, b)))
    await asyncio.sleep(0.5)
    load = {url: pubsub_channels(url) for url in REPLICAS}
    code = "worker 级订阅器" if hasattr(sub_mod, "SubscriptionHub") else "每连接订阅"
    print(f"code={code}  各副本订着的频道数: {load}")
    victim_url = next(
        (u for u in REPLICAS if args.kill and u.endswith(f":{args.kill}/0")),
        max(load, key=lambda u: load[u]),
    )
    victim = REPLICAS[victim_url]
    print(f"docker stop {victim} ({victim_url})")
    await docker("stop", "-t", "0", victim)
    await asyncio.sleep(1.0)

    mid = await say(100)
    await asyncio.sleep(2.0)
    recv = sum(mid in got.get(i, ()) for i in range(N))
    died = sum(-1 in got.get(i, ()) for i in range(N))
    print(f"[副本挂着] 已有订阅收到新消息: {recv}/{N}（读出错断开的 {died} 个）")
    ok, errors = await try_subscribe(backend, table)
    print(f"[副本挂着] 新订阅成功: {ok}/{N} {errors}")

    await docker("start", victim)
    t0 = time.time()
    while time.time() - t0 < 30:
        with contextlib.suppress(Exception):
            info = redis.Redis.from_url(victim_url).info("replication")
            if info.get("master_link_status") == "up":
                break
        await asyncio.sleep(0.5)
    await asyncio.sleep(8.0)  # pubsub 退避重订最多 5 秒
    mid2 = await say(200)
    await asyncio.sleep(2.0)
    recv2 = sum(mid2 in got.get(i, ()) for i in range(N))
    print(f"[副本恢复] 已有订阅收到新消息: {recv2}/{N}")
    ok2, errors2 = await try_subscribe(backend, table)
    print(f"[副本恢复] 新订阅成功: {ok2}/{N} {errors2}")
    print(f"各副本订着的频道数: { {url: pubsub_channels(url) for url in REPLICAS} }")

    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    for b in brokers:
        await b.close()
    await backend.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])  # type: ignore[union-attr]
    ap.add_argument(
        "--kill", default="", help="杀哪个副本（端口）；默认通知订得最多的那个"
    )
    asyncio.run(main(ap.parse_args()))
