"""
大量连接下的订阅压测：真实 hetu 服务器（taskset 绑核）+ 真实 WebSocket 客户端（jsonb + zlib + crypto
全套握手、elevate 登录、订阅），写进程直写 Redis（Session.commit 同款 Lua 提交）。

场景
----
- item：每个连接登录为不同用户，订 Item.owner == 自己（点查询，OWNER 权限，owner 声明了 point_sub），
  每人 ITEMS 件物品。写进程随机给用户插入 / 删除物品（--item-op churn，每人保持 MIN..MAX 件），或只改
  已有物品（--item-op update，没有频道增删）。
- chat：每个连接登录后订 ChatMessage 最近 LIMIT 条（id 倒序，同 examples/chat）。写进程按速率发消息。

每个速率一步：写进程跑 warmup + duration 秒，计时窗口内统计：
- 服务器 worker 的 CPU 时间与 user 态周期 / 指令数（`perf stat`；笔记本 P 核频率随负载变，CPU 时间
  跨负载、跨代码比较会失真，比开销看周期数），GC 耗时，订阅 tick 的次数与每 tick 条数；
- Redis 主 / 副本的 CPU 与命令数；
- 客户端交付数与写→收到的延迟（item 的删除按行 id 对上写进程记下的时刻）；
- RPC 往返探针（一个不订阅的连接每 PROBE_MS 发一次 ping）：订阅负载下事件循环的响应；
步末停写，等 worker 闲下来（积压消化完），核对一致性：每个客户端手里的行 == master 上的行。

等推送的调用（--rpcs-rate，设计稿 docs/superpowers/specs/2026-10-10-rpcs-sync-design.md §8.2）：前
--rpcs-conns 个连接在压测负载之上按总速率调 rpcs_write（一次只有一个在途），改自己一件物品（kind = -nonce）；
chat 场景再发一条聊天，这些连接另订自己的物品。客户端记回复、这次调用的推送（item 是物品行，chat 是聊天行）、
sync 各自到达的时刻：sync 先于推送的比例、sync 比推送晚多少、完成延迟。--rpcs-cmd rpc 发普通 rpc 作对照
（量栅栏的额外开销）。worker 里另记 δ（commit 返回 → 物品行的通知进 hub 的 MQ 队列）、栅栏键实际入队的时刻
（fencestats）。这些调用的推送不计入场景本身的交付统计。

对比两份代码：--server-pythonpath 指向另一份代码的 worktree（如 dev），只有服务器跑它，客户端 / 写进程
两边一样。

环境（结果见 sub_scenarios_result.md）
----
Redis 用 docker 的 host 网络（避开 docker-proxy），各绑一个核；副本要带 --notify-keyspace-events，重启后
才不会丢（HeTu 只在启动时 CONFIG SET）：

    docker run -d --name hetu-bench-master --network host --cpuset-cpus 2 redis:latest \\
        redis-server --port 23400 --save "" --appendonly no
    docker run -d --name hetu-bench-replica --network host --cpuset-cpus 3 redis:latest \\
        redis-server --port 23401 --save "" --appendonly no --replicaof 127.0.0.1 23400 \\
        --notify-keyspace-events ghzK

CPU 分配的默认值按 Intel 混合架构笔记本（P 核 0-3、E 核 4-11、低功耗 E 核 12-15）：worker 绑 CPU0，
RPC 探针 CPU1，Redis 主 / 副本 CPU2 / 3，客户端进程 E 核，写进程低功耗 E 核。py-spy 采样要 ptrace 权限，
app 里已对 worker 调了 prctl(PR_SET_PTRACER_ANY)；py-spy 在 Python 3.14 上会把 C 层时间算到协程帧
上，只作参考。

用法
----
    uv run python benchmark/sub_scenarios_ws.py --scenario item --rates 500,1000,2000,4000 \\
        --tag branch --out /tmp/sub_scenarios.jsonl
    uv run python benchmark/sub_scenarios_ws.py --scenario chat --rates 0.5,1,2,5 \\
        --server-pythonpath ../hetu-dev --tag dev --out /tmp/sub_scenarios.jsonl
    uv run python benchmark/sub_scenarios_ws.py --scenario item --rates 2000 \\
        --rpcs-rate 200 --rpcs-conns 100 --tag rpcs --out /tmp/sub_scenarios.jsonl
    uv run python benchmark/sub_scenarios_ws.py --show /tmp/sub_scenarios.jsonl

会 FLUSHALL --master 指定的 Redis，请用专用实例。
"""

import argparse
import asyncio
import contextlib
import json
import logging
import multiprocessing as mp
import os
import random
import socket
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from typing import Any

import psutil
import redis
import sub_scenarios_app as app
import websockets.asyncio.client
from cryptography.hazmat.primitives.asymmetric.x25519 import (
    X25519PrivateKey,
)

from hetu.common.snowflake_id import SnowflakeID
from hetu.data.backend import Backend, RaceCondition, RowFormat
from hetu.data.component import ComponentDefines
from hetu.manager import ComponentTableManager
from hetu.server import pipeline
from hetu.system import SystemClusters

HERE = os.path.dirname(os.path.abspath(__file__))
MAX_ID = 2**63 - 1
LAT_RESERVOIR = 50_000
REDIS_CMDS = ("zrange", "hgetall", "subscribe", "unsubscribe", "evalsha", "publish")
# --show 默认打印的列
SHOW_KEYS = {
    "item": (
        "tag,item_op,target_rate,write_rate,worker_cores,worker_kcycles_u_per_op,"
        "ticks_ps,items_per_tick,ins_lat_p50_ms,ins_lat_p99_ms,del_lat_p99_ms,"
        "rpc_rtt_p99_ms,rpc_rtt_max_ms,replica_cmds_ps,cons_bad_conns"
    ),
    "chat": (
        "tag,target_rate,msgs,worker_cores,worker_mcycles_u_per_msg,"
        "worker_us_per_msg_conn,lat_p50_ms,lat_p99_ms,fanout_done_p50_ms,"
        "fanout_done_p99_ms,rpc_rtt_p99_ms,rpc_rtt_max_ms,worker_rss_mb,cons_bad_conns"
    ),
}
# 带 --rpcs-rate 的结果另打印这些列
RPCS_SHOW_KEYS = (
    "tag,scenario,target_rate,rc_cmd,rc_rate,rc_ok,worker_gcycles_u_ps,rc_rsp_p50_ms,"
    "rc_push_p50_ms,rc_push_p99_ms,rc_done_p50_ms,rc_done_p99_ms,rc_sync_minus_push_p1_ms,"
    "rc_sync_minus_push_p50_ms,rc_sync_before_push,rc_push_missing,"
    "rc_push_missing_gone,fs_delta_p50_ms,"
    "fs_delta_p99_ms,fs_delta_max_ms,fs_g_p99_ms,fs_miss,fs_lost"
)


def setup_registry() -> None:
    logging.getLogger("HeTu.root").setLevel(logging.ERROR)
    logging.getLogger("hetu").setLevel(logging.ERROR)
    if SystemClusters().get_clusters(app.NAMESPACE) is None:
        SystemClusters().build_clusters(app.NAMESPACE)
        SystemClusters().build_endpoints()


def pin(cpus: list[int] | None) -> None:
    if cpus:
        os.sched_setaffinity(0, set(cpus))


def make_backend(args) -> Backend:
    backend = Backend(
        {"type": "redis", "master": args.master, "servants": args.replicas}
    )
    backend.post_configure(ComponentDefines().get_all())
    return backend


def make_tables(backend: Backend) -> ComponentTableManager:
    mgr = ComponentTableManager(app.NAMESPACE, app.INSTANCE, {"default": backend})
    mgr.check_and_create_new_tables()
    return mgr


def pct(samples: list[float], p: float) -> float:
    if not samples:
        return float("nan")
    s = sorted(samples)
    return s[min(len(s) - 1, int(len(s) * p))]


class Reservoir:
    """蓄水池采样，防止延迟样本爆内存"""

    def __init__(self, k: int = LAT_RESERVOIR):
        self.k = k
        self.seen = 0
        self.samples: list[float] = []

    def add(self, v: float) -> None:
        self.seen += 1
        if len(self.samples) < self.k:
            self.samples.append(v)
        else:
            j = random.randrange(self.seen)
            if j < self.k:
                self.samples[j] = v


# ---------------------------------------------------------------------------
# 数据准备
# ---------------------------------------------------------------------------


async def prepare_data(args) -> dict[int, list[int]]:
    """清库、建表、灌初始数据。item 返回 {user: [item_id]}，chat 返回 {}"""
    setup_registry()
    SnowflakeID().init(1000, 0)
    redis.Redis.from_url(args.master).flushall()
    await asyncio.sleep(0.5)
    backend = make_backend(args)
    tables = make_tables(backend)
    items: dict[int, list[int]] = {}
    if args.scenario == "item":
        table = tables.get_table(app.Item)
        assert table
        sem = asyncio.Semaphore(32)

        async def seed(uid: int) -> None:
            async with sem:
                rows = []
                async with table.session() as s:
                    repo = s.using(app.Item)
                    for k in range(args.items):
                        row = app.Item.new_row()
                        row.owner = uid
                        row.kind = k
                        await repo.insert(row)
                        rows.append(row)
                items[uid] = [int(r.id) for r in rows]

        await asyncio.gather(*(seed(u) for u in range(1, args.conns + 1)))
    else:
        # 等推送的调用改的是自己的物品，chat 场景也给每个调用的连接灌一件
        if args.rpcs_rate > 0:
            item_tbl = tables.get_table(app.Item)
            assert item_tbl
            for uid in range(1, args.rpcs_conns + 1):
                async with item_tbl.session() as s:
                    row = app.Item.new_row()
                    row.owner = uid
                    await s.using(app.Item).insert(row)
        table = tables.get_table(app.ChatMessage)
        assert table
        for i in range(args.limit + 64):
            async with table.session() as s:
                row = app.ChatMessage.new_row()
                row.owner = i % 1000 + 1
                row.name = f"user{i % 1000}"
                row.text = f"hello world message number {i} " * 3
                row.created_at_ms = int(time.time() * 1000)
                await s.using(app.ChatMessage).insert(row)
    await backend.wait_for_synced()
    await backend.close()
    return items


# ---------------------------------------------------------------------------
# 写进程
# ---------------------------------------------------------------------------


def writer_proc(args, widx: int, rate: float, items: dict[int, list[int]], stop, out_q):
    pin(args.writer_cpus)
    asyncio.run(_writer_main(args, widx, rate, items, stop, out_q))


async def _writer_main(args, widx, rate, items, stop, out_q):
    setup_registry()
    SnowflakeID().init(1001 + widx, 0)
    backend = make_backend(args)
    tables = make_tables(backend)
    item_tbl = tables.get_table(app.Item)
    chat_tbl = tables.get_table(app.ChatMessage)
    assert item_tbl and chat_tbl
    coroutines = args.writer_coroutines if args.scenario == "item" else 1
    per_co = rate / coroutines
    users = list(items)
    # (op, row_id, 提交前的时刻)
    ops: list[tuple[str, int, float]] = []
    raced = 0
    lo, hi = args.items_min, args.items_max
    busy: set[int] = set()  # 同一用户同时只有一个写，免得互相冲突

    async def item_op(rng: random.Random) -> None:
        nonlocal raced
        for _ in range(8):
            uid = rng.choice(users)
            if uid not in busy:
                break
        else:
            return
        busy.add(uid)
        try:
            mine = items[uid]
            n = len(mine)
            t = time.time()
            if args.item_op == "update":
                rid = mine[rng.randrange(n)]
                async with item_tbl.session() as s:
                    repo = s.using(app.Item)
                    old = await repo.get(id=rid)
                    if old is None:
                        return
                    old.count = int(old.count) + 1
                    old.ts = t
                    await repo.update(old)
                ops.append(("upd", rid, t))
            elif n <= lo or (n < hi and rng.random() < 0.5):
                async with item_tbl.session() as s:
                    row = app.Item.new_row()
                    row.owner = uid
                    row.kind = rng.randrange(1000)
                    row.ts = t
                    await s.using(app.Item).insert(row)
                mine.append(int(row.id))
                ops.append(("ins", int(row.id), t))
            else:
                rid = mine.pop(rng.randrange(n))
                try:
                    async with item_tbl.session() as s:
                        repo = s.using(app.Item)
                        old = await repo.get(id=rid)
                        if old is None:
                            return
                        repo.delete(rid)
                except BaseException:
                    mine.append(rid)
                    raise
                ops.append(("del", rid, t))
        except RaceCondition:
            raced += 1
        finally:
            busy.discard(uid)

    seq = 0

    async def chat_op(rng: random.Random) -> None:
        nonlocal seq
        seq += 1
        t = time.time()
        async with chat_tbl.session() as s:
            row = app.ChatMessage.new_row()
            row.owner = rng.randrange(1, 1000)
            row.name = f"user{row.owner}"
            row.text = f"hello world message number {seq} " * 3
            row.created_at_ms = int(t * 1000)
            row.ts = t
            await s.using(app.ChatMessage).insert(row)
        ops.append(("ins", int(row.id), t))

    op = item_op if args.scenario == "item" else chat_op

    async def idle() -> None:
        while not stop.is_set():
            await asyncio.sleep(0.2)

    async def one(co: int) -> None:
        rng = random.Random(widx * 1000 + co)
        interval = 1 / per_co
        next_t = time.perf_counter() + rng.random() * interval
        while not stop.is_set():
            delay = next_t - time.perf_counter()
            if delay > 0:
                await asyncio.sleep(delay)
            elif delay < -1.0:
                next_t = time.perf_counter()  # 追不上就放弃追赶，避免爆发
            next_t += interval
            await op(rng)

    if rate > 0:
        await asyncio.gather(*(one(c) for c in range(coroutines)))
    else:  # 只量等推送的调用：不写
        await idle()
    await backend.close()
    out_q.put((widx, ops, raced, {u: items[u] for u in users}))


# ---------------------------------------------------------------------------
# 客户端进程
# ---------------------------------------------------------------------------


def _bytes(data: str | bytes) -> bytes:
    return data.encode() if isinstance(data, str) else data


_PIPE: Any = None


def _pipeline():
    """每个进程一套 pipeline 层：层实例不带连接状态（服务端也是各连接共用），省得每条连接重建"""
    global _PIPE
    if _PIPE is None:
        pipe = pipeline.MessagePipeline()
        pipe.add_layer(pipeline.JSONBinaryLayer())
        pipe.add_layer(pipeline.ZlibLayer())
        crypto = pipeline.CryptoLayer()
        pipe.add_layer(crypto)
        _PIPE = (pipe, crypto)
    return _PIPE


async def ws_connect(url: str):
    """连上并握手（同 tests/test_websocket.py），返回 (ws, pipeline, ctx)"""
    ws = await websockets.asyncio.client.connect(
        url, max_size=None, open_timeout=120, ping_interval=None
    )
    pipe, crypto = _pipeline()
    pvt = X25519PrivateKey.generate()
    hs = [b""] * pipe.num_handshake_layers
    hs[-1] = pvt.public_key().public_bytes_raw()
    await ws.send(pipe.encode(None, hs))
    reply = pipe.decode(None, _bytes(await ws.recv()))
    assert isinstance(reply, list)
    ctx, _ = pipe.handshake(reply)
    ctx[-1] = crypto.client_handshake(pvt.private_bytes_raw(), reply[-1])
    return ws, pipe, ctx


class Caller:
    """
    一条连接上的等推送调用（--rpcs-rate）：按速率调 rpcs_write，一次只有一个在途（UI 等它完成才恢复按钮），
    记下回复、这次调用的推送、sync 各自到达的时刻（离发出的秒数）。推送按 nonce 认：item 是物品行的
    kind == -nonce，chat 是聊天行的 name == "r{uid}:{nonce}"
    """

    def __init__(self, args, uid: int, active: asyncio.Event):
        self.uid = uid
        self.cmd = args.rpcs_cmd
        self.chat = args.scenario == "chat"
        self.interval = args.rpcs_conns / args.rpcs_rate
        self.active = active
        self.items: set[int] = set()  # 自己的物品（item 场景就是订阅状态本身）
        self.nonce = 0
        self.inflight: dict[str, Any] | None = None
        self.done: asyncio.Future | None = None
        # nonce -> 记录，等这次调用的推送；10 秒没来就不等了
        self.pending: dict[int, dict[str, Any]] = {}
        self.records: list[dict[str, Any]] = []

    async def run(self, ws, pipe, ctx) -> None:
        rng = random.Random(self.uid)
        loop = asyncio.get_running_loop()
        next_t = 0.0
        while True:
            if not self.active.is_set():
                await self.active.wait()
                next_t = time.perf_counter() + rng.random() * self.interval
            delay = next_t - time.perf_counter()
            if delay > 0:
                await asyncio.sleep(delay)
            elif delay < -1.0:
                next_t = time.perf_counter()  # 追不上就放弃追赶
            next_t += self.interval
            if not self.active.is_set() or not self.items:
                continue
            rid = rng.choice(tuple(self.items))
            self.nonce += 1
            n = self.nonce
            rec: dict[str, Any] = {
                "n": n,
                "rid": rid,
                "gone": False,  # 推送到之前这件物品被写进程删了（推送里只有删除）
                "t": time.time(),
                "p0": time.perf_counter(),
                "ok": None,
                "rsp": None,
                "push": None,
                "sync": None,
                "timeout": False,
            }
            self.pending[n] = rec
            self.inflight = rec
            self.done = done = loop.create_future()
            if self.cmd == "rpcs":
                req = ["rpcs", n, "rpcs_write", n, rid, self.chat]
            else:
                req = ["rpc", "rpcs_write", n, rid, self.chat]
            await ws.send(pipe.encode(ctx, req))
            try:
                await asyncio.wait_for(done, 10)
            except TimeoutError:
                rec["timeout"] = True
            self.records.append(rec)
            cutoff = time.perf_counter() - 10
            for k in [k for k, r in self.pending.items() if r["p0"] < cutoff]:
                del self.pending[k]

    def _finish(self) -> None:
        self.inflight = None
        if self.done is not None and not self.done.done():
            self.done.set_result(None)

    def on_reply(self, msg: list, now: float) -> None:
        """rsp / rej / err / sync"""
        rec = self.inflight
        if rec is None:
            return
        kind = msg[0]
        if kind == "rsp":
            rec["rsp"] = now - rec["p0"]
            rec["ok"] = isinstance(msg[1], dict) and bool(msg[1].get("ok"))
            if self.cmd == "rpc" or rec["sync"] is not None:
                self._finish()
        elif kind in ("rej", "err"):
            rec["rsp"] = now - rec["p0"]
            rec["ok"] = False
            self._finish()
        elif kind == "sync" and msg[1] == rec["n"]:
            rec["sync"] = now - rec["p0"]
            if rec["rsp"] is not None:
                self._finish()

    def on_push(self, n: int, now: float) -> None:
        rec = self.pending.pop(n, None)
        if rec is not None and rec["push"] is None:
            rec["push"] = now - rec["p0"]

    def on_gone(self, rid: int) -> None:
        """物品被删了：还在等它的推送的调用不会再等到"""
        for rec in self.pending.values():
            if rec["rid"] == rid and rec["push"] is None:
                rec["gone"] = True

    def take(self, w0: float, w1: float) -> list[tuple]:
        """窗口内发出的调用：(ok, 回复, 推送, sync, 超时, 物品被删)，秒，没到的是 None"""
        out = [
            (r["ok"], r["rsp"], r["push"], r["sync"], r["timeout"], r["gone"])
            for r in self.records
            if w0 <= r["t"] < w1
        ]
        self.records.clear()
        return out


def client_proc(args, cidx: int, uids: list[int], url: str, ready, ctrl):
    pin([args.client_cpus[cidx % len(args.client_cpus)]])
    asyncio.run(_client_main(args, uids, url, ready, ctrl))


async def _client_main(args, uids, url, ready, ctrl):
    """一个进程承载 len(uids) 个连接；主进程经 ctrl 管道发命令：window / report / state / stop"""
    setup_registry()
    window = [float("inf"), float("inf")]  # 本步计时窗口（写入时刻）
    stats: dict[str, Any] = {}

    def reset() -> None:
        stats.update(
            ins=0,
            upd=0,
            dup=0,
            dels=0,
            frames=0,
            lat=Reservoir(),
            del_recv={},  # row_id -> 收到 None 的时刻
            msg_max={},  # chat：row_id -> 本进程各连接收到它的最大延迟
            msg_cnt={},  # chat：row_id -> 本进程收到它的连接数
        )

    reset()
    states: dict[int, set[int]] = {}  # uid -> 手里的 row_id（只记 id，省内存）
    sem = asyncio.Semaphore(args.connect_concurrency)
    errors: list[str] = []
    chat = args.scenario == "chat"
    calls_on = asyncio.Event()  # 等推送的调用：主进程在每步的写入期间打开
    callers: list[Caller] = []
    caller_tasks: list[asyncio.Task] = []

    async def one(uid: int) -> None:
        caller = None
        item_sub = None
        async with sem:
            ws, pipe, ctx = await ws_connect(url)
            await ws.send(pipe.encode(ctx, ["rpc", "login", uid]))
            msg = pipe.decode(ctx, _bytes(await ws.recv()))
            assert isinstance(msg, list) and msg[0] == "rsp", msg
            if chat:
                req = ["sub", "ChatMessage", "range", "id", 0, MAX_ID, args.limit, True]
            else:
                req = ["sub", "Item", "range", "owner", uid, None, args.limit, False]
            await ws.send(pipe.encode(ctx, [*req, True]))
            msg = pipe.decode(ctx, _bytes(await ws.recv()))
            assert isinstance(msg, list) and msg[0] == "sub", msg[:2]
            state = states[uid] = {int(r["id"]) for r in msg[2]}
            if args.rpcs_rate > 0 and uid <= args.rpcs_conns:
                caller = Caller(args, uid, calls_on)
                if chat:  # chat 场景另订自己的物品，调用改的是它
                    req = ["sub", "Item", "range", "owner", uid, None, 10, False, True]
                    await ws.send(pipe.encode(ctx, req))
                    msg = pipe.decode(ctx, _bytes(await ws.recv()))
                    assert isinstance(msg, list) and msg[0] == "sub", msg[:2]
                    item_sub = msg[1]
                    caller.items = {int(r["id"]) for r in msg[2]}
                else:
                    caller.items = state
                callers.append(caller)
                caller_tasks.append(asyncio.create_task(caller.run(ws, pipe, ctx)))
            with ready.get_lock():
                ready.value += 1
        mine = f"r{uid}:"
        try:
            async for raw in ws:
                msg = pipe.decode(ctx, _bytes(raw))
                if not isinstance(msg, list):
                    continue
                if msg[0] != "updt":
                    if caller is not None:
                        caller.on_reply(msg, time.perf_counter())
                    continue
                now = time.time()
                now_pc = time.perf_counter()
                stats["frames"] += 1
                if item_sub is not None and msg[1] == item_sub:
                    assert caller is not None
                    for key, row in msg[2].items():
                        if row is None:
                            caller.items.discard(int(key))
                        else:
                            caller.items.add(int(key))
                    continue
                for key, row in msg[2].items():
                    rid = int(key)
                    if row is None:
                        state.discard(rid)
                        if caller is not None and not chat:
                            caller.on_gone(rid)
                        if not chat and now >= window[0]:
                            stats["del_recv"][rid] = now
                        stats["dels"] += 1
                        continue
                    # 等推送的调用写的行：不计入场景的交付统计
                    if chat and row.get("kind") == "rpcs":
                        state.add(rid)
                        if caller is not None and row["name"].startswith(mine):
                            caller.on_push(int(row["name"][len(mine) :]), now_pc)
                        continue
                    if not chat and int(row["kind"]) < 0:
                        state.add(rid)
                        if caller is not None:
                            caller.on_push(-int(row["kind"]), now_pc)
                        continue
                    ts = float(row["ts"])
                    in_window = window[0] <= ts < window[1]
                    if rid in state:
                        if in_window:
                            stats["upd"] += 1
                            stats["lat"].add(now - ts)
                        else:
                            stats["dup"] += 1
                        continue
                    state.add(rid)
                    if not in_window:
                        continue
                    stats["ins"] += 1
                    lat = now - ts
                    stats["lat"].add(lat)
                    if chat:
                        mm = stats["msg_max"]
                        if lat > mm.get(rid, -1.0):
                            mm[rid] = lat
                        stats["msg_cnt"][rid] = stats["msg_cnt"].get(rid, 0) + 1
        except websockets.exceptions.ConnectionClosed as e:
            errors.append(f"uid {uid} closed: {e}")

    tasks = [asyncio.create_task(one(u)) for u in uids]
    loop = asyncio.get_running_loop()
    while True:
        cmd = await loop.run_in_executor(None, ctrl.recv)
        if cmd[0] == "window":
            window[0], window[1] = cmd[1], cmd[2]
            reset()
            for c in callers:
                c.records.clear()
            ctrl.send("ok")
        elif cmd[0] == "calls":
            if cmd[1]:
                calls_on.set()
            else:
                calls_on.clear()
            ctrl.send("ok")
        elif cmd[0] == "report":
            s = dict(stats)
            s["lat"] = stats["lat"].samples
            s["errors"] = list(errors)
            s["alive"] = sum(not t.done() for t in tasks)
            s["rpcs"] = [r for c in callers for r in c.take(window[0], window[1])]
            ctrl.send(s)
            reset()
            window[0] = window[1] = float("inf")
        elif cmd[0] == "state":
            ctrl.send({u: set(st) for u, st in states.items()})
        elif cmd[0] == "stop":
            for t in (*caller_tasks, *tasks):
                t.cancel()
            await asyncio.gather(*caller_tasks, *tasks, return_exceptions=True)
            ctrl.send("bye")
            return


def probe_proc(args, url: str, ctrl):
    pin(args.probe_cpus)
    asyncio.run(_probe_main(args, url, ctrl))


async def _probe_main(args, url, ctrl):
    """
    RPC 往返探针：一个不订阅的连接，每 PROBE_MS 发一次 ping 测往返。主进程也经它调诊断 endpoint
    （("rpc", [endpoint, *args])，结果原样回传），和 ping 串行，不打乱往返计时
    """
    setup_registry()
    ws, pipe, ctx = await ws_connect(url)
    rtts: list[tuple[float, float]] = []
    running = True
    want = asyncio.Event()
    answered = asyncio.Event()
    box: list[Any] = []
    request: list[Any] = []

    async def rpc(payload: list) -> Any:
        await ws.send(pipe.encode(ctx, ["rpc", *payload]))
        while True:
            msg = pipe.decode(ctx, _bytes(await ws.recv()))
            if isinstance(msg, list) and msg[0] == "rsp":
                return msg[1]

    async def loop_ping():
        while running:
            if want.is_set():
                want.clear()
                box.append(await rpc(request[0]))
                answered.set()
            t0 = time.perf_counter()
            ts = time.time()
            await rpc(["ping"])
            rtts.append((ts, time.perf_counter() - t0))
            await asyncio.sleep(args.probe_ms / 1000)

    task = asyncio.create_task(loop_ping())
    loop = asyncio.get_running_loop()
    while True:
        cmd = await loop.run_in_executor(None, ctrl.recv)
        if cmd[0] == "report":
            w0, w1 = cmd[1], cmd[2]
            ctrl.send([r for t, r in rtts if w0 <= t < w1])
            rtts.clear()
        elif cmd[0] == "rpc":
            request[:] = [list(cmd[1])]
            box.clear()
            want.set()
            await answered.wait()
            answered.clear()
            ctrl.send(box[0] if box else None)
        elif cmd[0] == "stop":
            running = False
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await ws.close()
            ctrl.send("bye")
            return


def call(conn, *cmd) -> Any:
    conn.send(cmd)
    return conn.recv()


# ---------------------------------------------------------------------------
# 服务器与观测
# ---------------------------------------------------------------------------


def write_config(args, workdir: str) -> str:
    servants = "\n".join(f"      - {url}" for url in args.replicas)
    cfg = f"""
APP_FILE: {os.path.join(HERE, "sub_scenarios_app.py")}
NAMESPACE: {app.NAMESPACE}
INSTANCES:
  - {app.INSTANCE}
LISTEN: 127.0.0.1:{args.port}
WORKER_NUM: {args.workers}
DEBUG: false
ACCESS_LOG: false
STARTUP_TIMEOUT: 120
MAX_ROW_SUBSCRIPTION: 100000
MAX_INDEX_SUBSCRIPTION: 1000
PACKET_LAYERS:
  - type: jsonb
  - type: zlib
    level: 1
  - type: crypto
BACKENDS:
  Redis:
    type: Redis
    master: {args.master}
    servants:
{servants}
"""
    path = os.path.join(workdir, "server.yml")
    with open(path, "w", encoding="utf-8") as f:
        f.write(cfg)
    return path


def wait_port(port: int, timeout: float = 120) -> None:
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return
        except OSError:
            time.sleep(0.3)
    raise TimeoutError(f"server port {port} not ready")


def proc_cpu(procs: list[psutil.Process]) -> float:
    total = 0.0
    for p in procs:
        try:
            t = p.cpu_times()
            total += t.user + t.system
        except psutil.Error:
            pass
    return total


def wait_idle(workers: list[psutil.Process], timeout: float) -> float:
    """等 worker 闲下来（1 秒内 CPU 低于 5%），返回等了多少秒"""
    t0 = time.time()
    while True:
        c0 = proc_cpu(workers)
        time.sleep(1.0)
        if proc_cpu(workers) - c0 < 0.05:
            return time.time() - t0 - 1.0
        if time.time() - t0 > timeout:
            print("drain timeout", flush=True)
            return time.time() - t0


def redis_snap(urls: list[str]) -> dict[str, Any]:
    """这些节点的 CPU、命令数、出网字节与几种命令的调用次数（多个节点求和）"""
    out: dict[str, Any] = {
        "cpu": 0.0,
        "cmds": 0.0,
        "net_out": 0.0,
        "pubsub_channels": 0,
        "clients": 0,
        **{name: 0 for name in REDIS_CMDS},
    }
    for url in urls:
        r = redis.Redis.from_url(url)
        info = r.info()
        cmd = r.info("commandstats")
        r.close()
        out["cpu"] += float(info["used_cpu_user"]) + float(info["used_cpu_sys"])
        out["cmds"] += float(info["total_commands_processed"])
        out["net_out"] += float(info["total_net_output_bytes"])
        out["pubsub_channels"] += int(info.get("pubsub_channels", 0))
        out["clients"] += int(info["connected_clients"])
        for name in REDIS_CMDS:
            out[name] += int(cmd.get(f"cmdstat_{name}", {"calls": 0})["calls"])
    return out


class PerfStat:
    """`perf stat` 数 worker 的 user 态周期与指令（混合架构只取 P 核的计数）"""

    def __init__(self, workers: list[psutil.Process], seconds: float):
        self.proc = subprocess.Popen(
            [
                "perf",
                "stat",
                "-x",
                ",",
                "-e",
                "cycles:u,instructions:u",
                "-p",
                ",".join(str(p.pid) for p in workers),
                "--",
                "sleep",
                str(seconds),
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )

    def result(self) -> dict[str, float]:
        counts: dict[str, float] = {}
        try:
            _out, err = self.proc.communicate(timeout=60)
        except Exception as e:  # noqa: BLE001 没装 perf 等：不影响别的统计
            print("perf stat failed", e, flush=True)
            return counts
        for line in err.splitlines():
            parts = line.split(",")
            if len(parts) > 2 and parts[0][:1].isdigit() and "cpu_atom" not in parts[2]:
                ev = (
                    parts[2]
                    .replace("cpu_core/", "")
                    .replace("/u", "")
                    .replace(":u", "")
                )
                counts[ev] = counts.get(ev, 0.0) + float(parts[0])
        return counts


def pyspy_record(pid: int, seconds: float, out: str, lines: bool) -> subprocess.Popen:
    return subprocess.Popen(
        [
            os.path.join(os.path.dirname(sys.executable), "py-spy"),
            "record",
            "--pid",
            str(pid),
            "--duration",
            str(int(seconds)),
            "--rate",
            "200",
            "--nonblocking",
            *([] if lines else ["--function"]),
            "--format",
            "raw",
            "-o",
            out,
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.STDOUT,
    )


def summarize_pyspy(path: str, top: int = 40) -> dict[str, Any]:
    """py-spy --format raw（折叠栈 "f1;f2;... count"）：按帧统计 self / inclusive 采样数"""
    self_cnt: dict[str, int] = {}
    incl_cnt: dict[str, int] = {}
    total = 0
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            stack, _, cnt = line.rstrip().rpartition(" ")
            if not stack:
                continue
            n = int(cnt)
            total += n
            frames = stack.split(";")
            for fr in set(frames):
                incl_cnt[fr] = incl_cnt.get(fr, 0) + n
            self_cnt[frames[-1]] = self_cnt.get(frames[-1], 0) + n
    return {
        "total": total,
        "self": sorted(self_cnt.items(), key=lambda kv: -kv[1])[:top],
        "incl": sorted(incl_cnt.items(), key=lambda kv: -kv[1])[: top * 2],
    }


async def check_consistency(args, states: dict[int, set[int]]) -> dict[str, Any]:
    """客户端手里的行 vs master 上的行"""
    setup_registry()
    backend = make_backend(args)
    tables = make_tables(backend)
    master = backend.master
    diffs: list[tuple[int, int]] = []
    if args.scenario == "item":
        table = tables.get_table(app.Item)
        assert table
        sem = asyncio.Semaphore(64)

        async def one(uid: int, have: set[int]) -> tuple[int, int]:
            async with sem:
                ids = await master.range(
                    table, "owner", uid, uid, 1000, False, RowFormat.ID_LIST
                )
            want = {int(i) for i in ids}
            return len(want - have), len(have - want)

        diffs = await asyncio.gather(*(one(u, h) for u, h in states.items()))
    else:
        table = tables.get_table(app.ChatMessage)
        assert table
        ids = await master.range(
            table, "id", 0, MAX_ID, args.limit, True, RowFormat.ID_LIST
        )
        want = {int(i) for i in ids}
        diffs = [(len(want - have), len(have - want)) for have in states.values()]
    await backend.close()
    return {
        "bad_conns": sum(bool(m or e) for m, e in diffs),
        "missing_rows": sum(m for m, _e in diffs),
        "extra_rows": sum(e for _m, e in diffs),
    }


@dataclass
class Harness:
    """一次运行：服务器、客户端进程、探针，以及跨步的物品表"""

    args: argparse.Namespace
    workdir: str
    items: dict[int, list[int]]
    server: subprocess.Popen | None = None
    sproc: psutil.Process | None = None
    workers: list[psutil.Process] = field(default_factory=list)
    clients: list[tuple[Any, Any]] = field(default_factory=list)
    probe: tuple[Any, Any] | None = None
    base: dict[str, Any] = field(default_factory=dict)

    @property
    def probe_conn(self) -> Any:
        assert self.probe is not None
        return self.probe[1]

    def start_server(self) -> None:
        args = self.args
        cfg = write_config(args, self.workdir)
        env = dict(os.environ, PYTHONUTF8="1")
        if args.server_pythonpath:
            env["PYTHONPATH"] = args.server_pythonpath
        log = open(os.path.join(self.workdir, "server.log"), "w", encoding="utf-8")  # noqa: SIM115
        cmd = [sys.executable, "-m", "hetu", "start", "--config", cfg]
        cpus = ",".join(map(str, args.server_cpus))
        self.server = subprocess.Popen(
            ["taskset", "-c", cpus, *cmd],
            cwd=self.workdir,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        log.close()  # 子进程已经拿到了文件描述符
        self.sproc = psutil.Process(self.server.pid)
        wait_port(args.port)
        time.sleep(2)
        # WORKER_NUM=1 时 Sanic 单进程运行，主进程就是 worker；多 worker 时是子进程
        self.workers = (
            [self.sproc] if args.workers == 1 else self.sproc.children(recursive=True)
        )
        assert self.workers, "no server worker found"

    def connect_clients(self) -> None:
        """连接 + 登录 + 订阅，量订阅阶段的成本；再起 RPC 探针、量空闲底噪"""
        args = self.args
        url = f"ws://127.0.0.1:{args.port}/hetu/{app.INSTANCE}"
        ready: Any = mp.Value("q", 0)
        uids = list(range(1, args.conns + 1))
        cpu0 = proc_cpu(self.workers)
        rep0, mas0 = redis_snap(args.replicas), redis_snap([args.master])
        t0 = time.time()
        for i in range(args.client_procs):
            parent, child = mp.Pipe()
            p = mp.Process(
                target=client_proc,
                args=(args, i, uids[i :: args.client_procs], url, ready, child),
                daemon=True,
            )
            p.start()
            self.clients.append((p, parent))
        while ready.value < args.conns:
            time.sleep(0.2)
            if time.time() - t0 > 900:
                raise TimeoutError(f"only {ready.value}/{args.conns} subscribed")
        subscribe_s = time.time() - t0
        wait_idle(self.workers, 300)  # 订阅生效后的补读消化完
        sub_cpu = proc_cpu(self.workers) - cpu0
        rep1, mas1 = redis_snap(args.replicas), redis_snap([args.master])
        self.base = {
            "subscribe_s": round(subscribe_s, 1),
            "subscribe_settled_s": round(time.time() - t0, 1),
            "subscribe_cpu_ms_per_conn": round(sub_cpu * 1000 / args.conns, 2),
            "subscribe_replica_cpu_s": round(rep1["cpu"] - rep0["cpu"], 2),
            "subscribe_master_cpu_s": round(mas1["cpu"] - mas0["cpu"], 2),
            "worker_rss_mb": round(
                sum(p.memory_info().rss for p in self.workers) / 2**20, 1
            ),
            "replica_pubsub_channels": rep1["pubsub_channels"],
        }
        print("subscribed:", json.dumps(self.base), flush=True)

        parent, child = mp.Pipe()
        pp = mp.Process(target=probe_proc, args=(args, url, child), daemon=True)
        pp.start()
        self.probe = (pp, parent)
        time.sleep(2)
        c0 = proc_cpu(self.workers)
        time.sleep(3.0)
        self.base["idle_worker_cores"] = round((proc_cpu(self.workers) - c0) / 3, 4)

    def start_writers(self, rate: float) -> tuple[Any, Any, list[Any]]:
        args = self.args
        uids = list(range(1, args.conns + 1))
        stop = mp.Event()
        out_q: Any = mp.Queue()
        procs = []
        for w in range(args.writers):
            part = (
                {u: self.items[u] for u in uids[w :: args.writers]}
                if self.items
                else {}
            )
            p = mp.Process(
                target=writer_proc,
                args=(args, w, rate / args.writers, part, stop, out_q),
                daemon=True,
            )
            p.start()
            procs.append(p)
        return stop, out_q, procs

    def run_step(self, step: int, rate: float) -> dict[str, Any]:
        args = self.args
        stop, out_q, wprocs = self.start_writers(rate)
        rpcs_on = args.rpcs_rate > 0
        if rpcs_on:
            for _p, conn in self.clients:
                assert call(conn, "calls", True) == "ok"
        time.sleep(args.warmup)
        w0 = time.time()
        w1 = w0 + args.duration
        for _p, conn in self.clients:
            assert call(conn, "window", w0, w1) == "ok"
        if rpcs_on:
            call(self.probe_conn, "rpc", ["fencestats", True])
        gc_a = call(self.probe_conn, "rpc", ["gcstats"])
        tk_a = call(self.probe_conn, "rpc", ["tickstats"])
        cpu_a = proc_cpu(self.workers)
        rep_a, mas_a = redis_snap(args.replicas), redis_snap([args.master])
        wall_a = time.perf_counter()
        perf = PerfStat(self.workers, args.duration)
        spy = None
        spy_out = os.path.join(self.workdir, f"pyspy_step{step}.txt")
        if step in args.pyspy_steps:
            target = max(self.workers, key=lambda p: p.memory_info().rss)
            spy = pyspy_record(target.pid, args.duration, spy_out, args.pyspy_lines)
        prof_out = os.path.join(self.workdir, f"cprofile_step{step}.prof")
        prof_until = None
        if step in args.cprofile_steps:
            call(self.probe_conn, "rpc", ["prof", "start"])
            prof_until = time.time() + args.cprofile_secs
        freqs: list[float] = []
        next_sample = 0.0
        while time.time() < w1:
            if prof_until is not None and time.time() >= prof_until:
                prof_until = None
                call(self.probe_conn, "rpc", ["prof", "stop", prof_out])
            if time.time() >= next_sample:  # 笔记本 P 核频率：看有没有降频
                next_sample = time.time() + 0.5
                for c in args.server_cpus:
                    path = f"/sys/devices/system/cpu/cpu{c}/cpufreq/scaling_cur_freq"
                    with open(path, encoding="ascii") as fh:
                        freqs.append(int(fh.read()) / 1e6)
            time.sleep(0.05)
        wall = time.perf_counter() - wall_a
        cpu = proc_cpu(self.workers) - cpu_a
        gc_b = call(self.probe_conn, "rpc", ["gcstats"])
        tk_b = call(self.probe_conn, "rpc", ["tickstats"])
        fs = call(self.probe_conn, "rpc", ["fencestats", True]) if rpcs_on else None
        rep_b, mas_b = redis_snap(args.replicas), redis_snap([args.master])
        rss = sum(p.memory_info().rss for p in self.workers)
        stop.set()
        if rpcs_on:
            for _p, conn in self.clients:
                assert call(conn, "calls", False) == "ok"
        counts = perf.result()
        ops: list[tuple[str, int, float]] = []
        raced = 0
        for _widx, wops, wraced, witems in (out_q.get(timeout=60) for _ in wprocs):
            ops.extend(wops)
            raced += wraced
            self.items.update(witems)
        for p in wprocs:
            p.join(timeout=10)
        # 等积压消化完再收交付、核对
        drain_s = wait_idle(self.workers, args.drain_timeout)
        if spy is not None:
            spy.wait(timeout=120)
        creps = [call(conn, "report") for _p, conn in self.clients]
        rtts = call(self.probe_conn, "report", w0, w1)
        states: dict[int, set[int]] = {}
        for _p, conn in self.clients:
            states.update(call(conn, "state"))
        cons = asyncio.run(check_consistency(args, states))

        win_ops = [o for o in ops if w0 <= o[2] < w1]
        lat: list[float] = [v for r in creps for v in r["lat"]]
        res: dict[str, Any] = {
            "scenario": args.scenario,
            "tag": args.tag,
            "conns": args.conns,
            "limit": args.limit,
            "workers": args.workers,
            "servants": len(args.replicas),
            "step": step,
            "target_rate": rate,
            "write_rate": round(len(win_ops) / args.duration, 2),
            "raced": raced,
            **self.base,
            "worker_cores": round(cpu / wall, 3),
            "worker_gcycles_u_ps": round(counts.get("cycles", 0) / wall / 1e9, 3),
            "worker_ginstr_u_ps": round(counts.get("instructions", 0) / wall / 1e9, 3),
            "worker_rss_mb_end": round(rss / 2**20, 1),
            "server_cpu_ghz_avg": round(sum(freqs) / len(freqs), 2) if freqs else None,
            "gc_cores": round((gc_b["t"] - gc_a["t"]) / wall, 3),
            "gc_ms_per_s_by_gen": [
                round((b - a) * 1000 / wall, 1)
                for a, b in zip(gc_a["t_gen"], gc_b["t_gen"], strict=True)
            ],
            "ticks_ps": round((tk_b["n"] - tk_a["n"]) / wall, 1),
            "items_per_tick": round(
                (tk_b["items"] - tk_a["items"]) / max(1, tk_b["n"] - tk_a["n"]), 2
            ),
            "replica_cores": round((rep_b["cpu"] - rep_a["cpu"]) / wall, 3),
            "master_cores": round((mas_b["cpu"] - mas_a["cpu"]) / wall, 3),
            "replica_cmds_ps": round((rep_b["cmds"] - rep_a["cmds"]) / wall),
            "master_cmds_ps": round((mas_b["cmds"] - mas_a["cmds"]) / wall),
            "replica_net_out_MBps": round(
                (rep_b["net_out"] - rep_a["net_out"]) / wall / 1e6, 2
            ),
            "drain_s": round(drain_s, 1),
            "frames_ps": round(sum(r["frames"] for r in creps) / wall),
            "dup_rows": sum(r["dup"] for r in creps),
            "client_errors": sum(len(r["errors"]) for r in creps),
            "clients_alive": sum(r["alive"] for r in creps),
            **{f"cons_{k}": v for k, v in cons.items()},
            "rpc_rtt_p50_ms": round(pct(rtts, 0.5) * 1000, 2),
            "rpc_rtt_p99_ms": round(pct(rtts, 0.99) * 1000, 2),
            "rpc_rtt_max_ms": round(max(rtts, default=float("nan")) * 1000, 2),
        }
        for name in REDIS_CMDS:
            for label, a, b in (("rep", rep_a, rep_b), ("mas", mas_a, mas_b)):
                if calls := b[name] - a[name]:
                    res[f"{label}_{name}_ps"] = round(calls / wall)
        if args.scenario == "item":
            res.update(item_metrics(args, win_ops, creps, lat, cpu, counts))
        else:
            res.update(chat_metrics(args, win_ops, creps, lat, cpu, counts))
        if rpcs_on:
            recs = [r for c in creps for r in c["rpcs"]]
            res.update(rpcs_metrics(args, recs, fs))
        if spy is not None and os.path.exists(spy_out):
            res["pyspy"] = spy_out
            with open(spy_out + ".summary.json", "w", encoding="utf-8") as f:
                json.dump(summarize_pyspy(spy_out), f, ensure_ascii=False, indent=1)
        return res

    def close(self) -> None:
        for p, conn in self.clients:
            with contextlib.suppress(Exception):
                call(conn, "stop")
            p.join(timeout=10)
            if p.is_alive():
                p.kill()
        if self.probe is not None:
            with contextlib.suppress(Exception):
                call(self.probe_conn, "stop")
            self.probe[0].join(timeout=10)
        if self.server is None or self.sproc is None:
            return
        try:
            children = self.sproc.children(recursive=True)
        except psutil.Error:
            children = []
        self.server.terminate()
        for child in children:
            try:
                child.terminate()
            except psutil.Error:
                pass
        try:
            self.server.wait(timeout=30)
        except subprocess.TimeoutExpired:
            self.server.kill()
        log_path = os.path.join(self.workdir, "server.log")
        with open(log_path, encoding="utf-8", errors="replace") as fh:
            bad = [
                ln for ln in fh if any(k in ln for k in ("❌", "ERROR", "Traceback"))
            ]
        print(f"server log: {log_path}（错误 {len(bad)} 条）", flush=True)
        for ln in bad[:15]:
            print("  ", ln.rstrip()[:300], flush=True)


def item_metrics(args, win_ops, creps, lat, cpu, counts) -> dict[str, Any]:
    n_ins = sum(o[0] == "ins" for o in win_ops)
    n_del = sum(o[0] == "del" for o in win_ops)
    n_upd = sum(o[0] == "upd" for o in win_ops)
    del_recv: dict[int, float] = {}
    for r in creps:
        del_recv.update(r["del_recv"])
    del_lat = [
        del_recv[rid] - t for op, rid, t in win_ops if op == "del" and rid in del_recv
    ]
    ops_done = n_ins + n_del + n_upd
    return {
        "item_op": args.item_op,
        "expected_ins": n_ins,
        "delivered_ins": sum(r["ins"] for r in creps),
        "expected_del": n_del,
        "delivered_del": len(del_lat),
        "expected_upd": n_upd,
        "delivered_upd": sum(r["upd"] for r in creps),
        "ins_lat_p50_ms": round(pct(lat, 0.5) * 1000, 1),
        "ins_lat_p99_ms": round(pct(lat, 0.99) * 1000, 1),
        "ins_lat_max_ms": round(max(lat, default=float("nan")) * 1000, 1),
        "del_lat_p50_ms": round(pct(del_lat, 0.5) * 1000, 1),
        "del_lat_p99_ms": round(pct(del_lat, 0.99) * 1000, 1),
        "worker_us_per_op": round(cpu * 1e6 / ops_done, 1) if ops_done else None,
        "worker_kcycles_u_per_op": (
            round(counts.get("cycles", 0) / ops_done / 1e3, 1) if ops_done else None
        ),
    }


def chat_metrics(args, win_ops, creps, lat, cpu, counts) -> dict[str, Any]:
    n_msgs = sum(o[0] == "ins" for o in win_ops)
    msg_max: dict[int, float] = {}
    msg_cnt: dict[int, int] = {}
    for r in creps:
        for rid, v in r["msg_max"].items():
            msg_max[rid] = max(v, msg_max.get(rid, -1.0))
        for rid, c in r["msg_cnt"].items():
            msg_cnt[rid] = msg_cnt.get(rid, 0) + c
    fan = list(msg_max.values())  # 每条消息送到全部连接用了多久
    return {
        "msgs": n_msgs,
        "expected_deliveries": n_msgs * args.conns,
        "delivered": sum(r["ins"] for r in creps),
        "msgs_fully_delivered": sum(c >= args.conns for c in msg_cnt.values()),
        "lat_p50_ms": round(pct(lat, 0.5) * 1000, 1),
        "lat_p99_ms": round(pct(lat, 0.99) * 1000, 1),
        "fanout_done_p50_ms": round(pct(fan, 0.5) * 1000, 1),
        "fanout_done_p99_ms": round(pct(fan, 0.99) * 1000, 1),
        "worker_ms_per_msg": round(cpu * 1e3 / n_msgs, 1) if n_msgs else None,
        "worker_us_per_msg_conn": (
            round(cpu * 1e6 / n_msgs / args.conns, 1) if n_msgs else None
        ),
        "worker_mcycles_u_per_msg": (
            round(counts.get("cycles", 0) / n_msgs / 1e6, 1) if n_msgs else None
        ),
    }


def rpcs_metrics(args, recs: list, fs: dict | None) -> dict[str, Any]:
    """等推送的调用：客户端 (ok, 回复, 推送, sync, 超时) 的统计，加上 worker 里的栅栏时序（fs_*）"""

    def ms(vals: list[float], p: float) -> float:
        return round(pct(vals, p) * 1000, 1)

    ok = [r for r in recs if r[0]]
    rsp = [r[1] for r in recs if r[1] is not None]
    push = [r[2] for r in ok if r[2] is not None]
    res: dict[str, Any] = {
        "rc_cmd": args.rpcs_cmd,
        "rc_conns": args.rpcs_conns,
        "rc_target": args.rpcs_rate,
        "rc_calls": len(recs),
        "rc_rate": round(len(recs) / args.duration, 1),
        "rc_ok": len(ok),
        "rc_timeouts": sum(bool(r[4]) for r in recs),
        "rc_rsp_p50_ms": ms(rsp, 0.5),
        "rc_rsp_p99_ms": ms(rsp, 0.99),
        "rc_push_p50_ms": ms(push, 0.5),
        "rc_push_p99_ms": ms(push, 0.99),
        "rc_push_missing": sum(r[2] is None for r in ok),
        # 其中物品在推送之前被写进程删了：推送里只有删除，等不到这次的值
        "rc_push_missing_gone": sum(r[2] is None and r[5] for r in ok),
    }
    if args.rpcs_cmd == "rpcs":
        done = [r[3] for r in recs if r[3] is not None]
        both = [r for r in ok if r[2] is not None and r[3] is not None]
        diff = [r[3] - r[2] for r in both]
        res.update(
            rc_done_p50_ms=ms(done, 0.5),
            rc_done_p99_ms=ms(done, 0.99),
            rc_done_max_ms=round(max(done, default=float("nan")) * 1000, 1),
            # sync 比这次调用的推送晚多少（负 = sync 先到）
            rc_sync_minus_push_min_ms=round(min(diff, default=float("nan")) * 1000, 1),
            rc_sync_minus_push_p1_ms=ms(diff, 0.01),
            rc_sync_minus_push_p50_ms=ms(diff, 0.5),
            rc_sync_minus_push_p99_ms=ms(diff, 0.99),
            # 不算物品被删的（没有这次调用的推送可等）
            rc_sync_before_push=sum(
                r[3] is not None and not r[5] and (r[2] is None or r[2] > r[3])
                for r in ok
            ),
            rc_sync_before_rsp=sum(
                r[1] is not None and r[3] is not None and r[3] < r[1] for r in recs
            ),
        )
    else:
        done = rsp
        gap = [r[2] - r[1] for r in ok if r[1] is not None and r[2] is not None]
        res.update(
            rc_done_p50_ms=ms(done, 0.5),
            rc_done_p99_ms=ms(done, 0.99),
            rc_done_max_ms=round(max(done, default=float("nan")) * 1000, 1),
            # 今天的情形：回复比推送早多少
            rc_push_minus_rsp_p50_ms=ms(gap, 0.5),
            rc_push_minus_rsp_p99_ms=ms(gap, 0.99),
            rc_rsp_before_push=sum(
                r[1] is not None and not r[5] and (r[2] is None or r[2] > r[1])
                for r in ok
            ),
        )
    if fs:
        res.update(fs_n=fs["n"], fs_miss=fs["miss"], fs_merged=fs["merged"])
        res["fs_lost"] = fs["lost"]
        for name, key in (("delta", "delta_ms"), ("g", "g_ms"), ("late", "late_ms")):
            for q, v in fs[key].items():
                res[f"fs_{name}_{q}_ms"] = v
    return res


def show(path: str, keys: str) -> None:
    """按场景打印结果文件里的主要列"""
    with open(path, encoding="utf-8") as f:
        rows = [json.loads(line) for line in f if line.strip()]
    for scenario in ("item", "chat"):
        picked = [r for r in rows if r["scenario"] == scenario]
        if not picked:
            continue
        cols = (keys or SHOW_KEYS[scenario]).split(",")
        table = [[str(r.get(c, "")) for c in cols] for r in picked]
        widths = [max(len(c), *(len(t[i]) for t in table)) for i, c in enumerate(cols)]
        print(f"== {scenario}")
        print("  ".join(c.ljust(w) for c, w in zip(cols, widths, strict=True)))
        for t in table:
            print("  ".join(v.ljust(w) for v, w in zip(t, widths, strict=True)))
    picked = [r for r in rows if "rc_calls" in r]
    if picked and not keys:
        cols = RPCS_SHOW_KEYS.split(",")
        table = [[str(r.get(c, "")) for c in cols] for r in picked]
        widths = [max(len(c), *(len(t[i]) for t in table)) for i, c in enumerate(cols)]
        print("== rpcs")
        print("  ".join(c.ljust(w) for c, w in zip(cols, widths, strict=True)))
        for t in table:
            print("  ".join(v.ljust(w) for v, w in zip(t, widths, strict=True)))


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])  # type: ignore[union-attr]
    ap.add_argument("--scenario", choices=("item", "chat"))
    ap.add_argument("--show", default="", help="只打印这个结果文件（JSONL）的主要列")
    ap.add_argument("--show-keys", default="", help="--show 打印哪些列（逗号分隔）")
    ap.add_argument("--tag", default="", help="写进结果的标签（区分代码版本等）")
    ap.add_argument("--master", default="redis://127.0.0.1:23400/0")
    ap.add_argument(
        "--replica",
        default="redis://127.0.0.1:23401/0",
        help="servant，逗号分隔可给多个",
    )
    ap.add_argument("--port", type=int, default=23466)
    ap.add_argument("--workers", type=int, default=1, help="服务器 WORKER_NUM")
    ap.add_argument("--conns", type=int, default=2000)
    ap.add_argument(
        "--limit", type=int, default=0, help="item 默认 100，chat 默认 1024"
    )
    ap.add_argument("--items", type=int, default=30, help="item：每人初始物品数")
    ap.add_argument("--items-min", type=int, default=20)
    ap.add_argument("--items-max", type=int, default=40)
    ap.add_argument(
        "--item-op",
        choices=("churn", "update"),
        default="churn",
        help="item：churn=插入/删除；update=只改已有物品（没有频道增删）",
    )
    ap.add_argument(
        "--rates", default="", help="逗号分隔的写入速率（次/秒，chat 为条/秒）"
    )
    ap.add_argument("--warmup", type=float, default=5)
    ap.add_argument("--duration", type=float, default=30, help="每步计时窗口秒数")
    ap.add_argument("--drain-timeout", type=float, default=120)
    ap.add_argument("--writers", type=int, default=0, help="写进程数（item 默认 4）")
    ap.add_argument("--writer-coroutines", type=int, default=16)
    ap.add_argument("--client-procs", type=int, default=8)
    ap.add_argument("--connect-concurrency", type=int, default=8, help="每个客户端进程")
    ap.add_argument("--probe-ms", type=float, default=20)
    ap.add_argument(
        "--rpcs-rate",
        type=float,
        default=0,
        help="等推送的调用（rpcs_write）的总速率（次/秒），0 不调",
    )
    ap.add_argument(
        "--rpcs-conns", type=int, default=100, help="前这么多个连接发等推送的调用"
    )
    ap.add_argument(
        "--rpcs-cmd",
        choices=("rpcs", "rpc"),
        default="rpcs",
        help="rpc：同样的调用发普通 rpc，作对照（不等推送，量栅栏的额外开销）",
    )
    ap.add_argument("--server-cpus", default="0")
    ap.add_argument("--client-cpus", default="4,5,6,7,8,9,10,11")
    ap.add_argument("--writer-cpus", default="12,13,14,15")
    ap.add_argument("--probe-cpus", default="1")
    ap.add_argument(
        "--server-pythonpath", default="", help="服务器改跑这份代码（另一个 worktree）"
    )
    ap.add_argument(
        "--pyspy-steps", default="", help="这些步（0 起）的窗口里 py-spy 采样"
    )
    ap.add_argument(
        "--pyspy-lines", action="store_true", help="py-spy 按行不按函数聚合"
    )
    ap.add_argument(
        "--cprofile-steps", default="", help="这些步的窗口里 worker 内 cProfile"
    )
    ap.add_argument("--cprofile-secs", type=float, default=10)
    ap.add_argument(
        "--workdir", default="", help="服务器日志、采样文件放这里（默认临时目录）"
    )
    ap.add_argument("--out", default="", help="每步结果（一行一个 JSON）追加到这个文件")
    args = ap.parse_args()

    def cpus(s: str) -> list[int]:
        return [int(x) for x in s.split(",") if x != ""]

    args.server_cpus = cpus(args.server_cpus)
    args.client_cpus = cpus(args.client_cpus)
    args.writer_cpus = cpus(args.writer_cpus)
    args.probe_cpus = cpus(args.probe_cpus)
    args.pyspy_steps = set(cpus(args.pyspy_steps))
    args.cprofile_steps = set(cpus(args.cprofile_steps))
    args.replicas = [u for u in args.replica.split(",") if u]
    if not args.limit:
        args.limit = 100 if args.scenario == "item" else 1024
    if not args.rates:
        args.rates = "500,1000,2000" if args.scenario == "item" else "0.5,1,2,5"
    if not args.writers:
        args.writers = 4 if args.scenario == "item" else 1
    return args


def main() -> None:
    args = parse_args()
    if args.show:
        show(args.show, args.show_keys)
        return
    if not args.scenario:
        raise SystemExit("--scenario 或 --show 二选一")
    workdir = args.workdir or tempfile.mkdtemp(prefix=f"hetu_sub_{args.scenario}_")
    os.makedirs(workdir, exist_ok=True)
    print(f"workdir {workdir}", flush=True)
    items = asyncio.run(prepare_data(args))
    harness = Harness(args, workdir, items)
    try:
        harness.start_server()
        harness.connect_clients()
        for step, rate in enumerate(float(x) for x in args.rates.split(",")):
            print(f"--- step {step}: rate {rate}/s", flush=True)
            res = harness.run_step(step, rate)
            print(json.dumps(res, ensure_ascii=False), flush=True)
            if args.out:
                with open(args.out, "a", encoding="utf-8") as f:
                    f.write(json.dumps(res, ensure_ascii=False) + "\n")
    finally:
        harness.close()


if __name__ == "__main__":
    mp.set_start_method("fork")  # 子进程继承已建好的组件注册表
    main()
