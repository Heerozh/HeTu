"""
sub_budget.py 的端到端版本：起一个真实的 hetu 服务器（1 个 worker），用真实 WebSocket
客户端（jsonb + zlib + crypto 全套 pipeline）订阅，测量**含 WebSocket 推送在内**的每次
交付成本，与 sub_budget.py 的进程内数字对照，差值就是 Sanic/ws/pipeline 那一段。

模型与 sub_budget.py 相同（Z 个 zone × S 行，C 个连接均摊，扇出 K=C/Z，写进程直写）。
服务器 worker 的 CPU 用 psutil 读子进程树；交付数和延迟由客户端进程统计。

用法
----
    cd benchmark
    uv run python sub_budget_ws.py --conns 80 --zones 8 --limit 50 --writes 300 --duration 15

Redis 连接数：每个 worker 只有 1 条 pubsub 连接 + 有界读池（BACKENDS.max_connections），与 ws 连接数无关。
"""

import argparse
import asyncio
import multiprocessing as mp
import os
import random
import socket
import subprocess
import sys
import tempfile
import time
from typing import Any

import psutil
import sub_budget as base
import websockets.asyncio.client
from nacl.public import PrivateKey

from hetu.server import pipeline

# ---------------------------------------------------------------------------
# 客户端进程
# ---------------------------------------------------------------------------


def client_proc(
    url: str,
    zones: list[int],
    limit: int,
    ready: Any,
    stop: Any,
    window: Any,
    result_q: Any,
) -> None:
    asyncio.run(_client_main(url, zones, limit, ready, stop, window, result_q))


async def _client_main(url, zones, limit, ready, stop, window, result_q):
    # zlib 层的预共享字典由本进程已注册的组件生成，必须和服务器一致
    base.setup_registry()
    delivered = 0
    deleted = 0
    frames = 0
    lat: list[float] = []
    lat_seen = 0

    async def one_client(zone: int):
        nonlocal delivered, deleted, frames, lat_seen
        ws = await websockets.asyncio.client.connect(
            url, max_size=None, open_timeout=60
        )
        pipe = pipeline.MessagePipeline()
        pipe.add_layer(pipeline.JSONBinaryLayer())
        pipe.add_layer(pipeline.ZlibLayer())
        crypto = pipeline.CryptoLayer()
        pipe.add_layer(crypto)
        # 握手（与 tests/test_websocket.py 一致）
        pvt = PrivateKey.generate()
        hs = [b""] * pipe.num_handshake_layers
        hs[-1] = pvt.public_key.encode()
        await ws.send(pipe.encode(None, hs))
        reply = pipe.decode(None, await ws.recv())
        assert isinstance(reply, list)
        ctx, _ = pipe.handshake(reply)
        ctx[-1] = crypto.client_handshake(pvt.encode(), reply[-1])
        # 订阅本 zone 的全部 S 行
        await ws.send(
            pipe.encode(
                ctx, ["sub", "Actor", "range", "zone", zone, None, limit, False, True]
            )
        )
        msg = pipe.decode(ctx, await ws.recv())
        assert isinstance(msg, list) and msg[0] == "sub" and len(msg[2]) == limit, msg[
            :2
        ]
        with ready.get_lock():
            ready.value += 1
        try:
            async for raw in ws:
                msg = pipe.decode(ctx, raw)
                if not isinstance(msg, list) or msg[0] != "updt":
                    continue
                now = time.time()
                if not (window[0] <= now < window[1]):
                    continue
                frames += 1
                for row in msg[2].values():
                    if row is None:
                        deleted += 1
                    else:
                        delivered += 1
                        lat_seen += 1
                        v = now - float(row["ts"])
                        if len(lat) < 50_000:
                            lat.append(v)
                        else:
                            j = random.randrange(lat_seen)
                            if j < 50_000:
                                lat[j] = v
        except websockets.exceptions.ConnectionClosed:
            pass

    tasks = [asyncio.create_task(one_client(z)) for z in zones]
    while not stop.is_set():
        await asyncio.sleep(0.2)
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    result_q.put((delivered, deleted, frames, lat))


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------


def write_config(args, workdir: str) -> str:
    app_file = os.path.abspath(base.__file__).replace("\\", "/")
    cfg = f"""
APP_FILE: {app_file}
NAMESPACE: {base.NAMESPACE}
INSTANCES:
  - {base.INSTANCE}
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
      - {args.replica}
"""
    path = os.path.join(workdir, "sub_budget_ws.yml")
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
            time.sleep(0.5)
    raise TimeoutError(f"server port {port} not ready")


def tree_cpu(proc: psutil.Process) -> float:
    total = 0.0
    for p in [proc, *proc.children(recursive=True)]:
        try:
            t = p.cpu_times()
            total += t.user + t.system
        except psutil.Error:
            pass
    return total


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])  # type: ignore[union-attr]
    ap.add_argument("--master", default="redis://127.0.0.1:23400/0")
    ap.add_argument("--replica", default="redis://127.0.0.1:23401/0")
    ap.add_argument("--port", type=int, default=23466)
    ap.add_argument("--workers", type=int, default=1, help="服务器 worker 数")
    ap.add_argument("--conns", type=int, default=80)
    ap.add_argument("--zones", type=int, default=8)
    ap.add_argument("--limit", type=int, default=50)
    ap.add_argument("--writes", type=float, default=300)
    ap.add_argument("--hot-rows", type=int, default=0)
    ap.add_argument("--writers", type=int, default=2)
    ap.add_argument("--writer-coroutines", type=int, default=8)
    ap.add_argument("--client-procs", type=int, default=4)
    ap.add_argument("--duration", type=float, default=15)
    ap.add_argument("--tag", default="")
    args = ap.parse_args()

    workdir = tempfile.mkdtemp(prefix="hetu_sub_budget_ws_")
    ids = asyncio.run(base.prepare_data(args))
    hot_ids = ids if args.hot_rows <= 0 else random.sample(ids, args.hot_rows)

    # 起服务器
    cfg = write_config(args, workdir)
    env = dict(os.environ, PYTHONUTF8="1")
    server_log = open(os.path.join(workdir, "server.log"), "w", encoding="utf-8")  # noqa: SIM115 进程结束后再关
    server = subprocess.Popen(
        [sys.executable, "-m", "hetu", "start", "--config", cfg],
        cwd=workdir,
        env=env,
        stdout=server_log,
        stderr=subprocess.STDOUT,
    )
    sproc = psutil.Process(server.pid)
    try:
        wait_port(args.port)
        time.sleep(3)  # 等 worker 建表、加载完

        # 起客户端
        url = f"ws://127.0.0.1:{args.port}/hetu/{base.INSTANCE}"
        ready = mp.Value("q", 0)
        stop = mp.Event()
        window = mp.Array("d", [float("inf"), float("inf")])
        result_q: Any = mp.Queue()
        all_zones = [i % args.zones for i in range(args.conns)]
        cprocs = []
        for i in range(args.client_procs):
            zs = all_zones[i :: args.client_procs]
            if not zs:
                continue
            p = mp.Process(
                target=client_proc,
                args=(url, zs, args.limit, ready, stop, window, result_q),
            )
            p.start()
            cprocs.append(p)
        t0 = time.time()
        while ready.value < args.conns:
            time.sleep(0.2)
            if time.time() - t0 > 120:
                raise TimeoutError(
                    f"only {ready.value}/{args.conns} clients subscribed"
                )
        subscribe_time = time.time() - t0

        # 起写进程
        written = mp.Value("q", 0)
        raced = mp.Value("q", 0)
        wstop = mp.Event()
        wprocs = []
        if args.writes > 0:
            for w in range(args.writers):
                p = mp.Process(
                    target=base.writer_proc,
                    args=(
                        args.master,
                        args.replica,
                        hot_ids,
                        args.writes / args.writers,
                        w + 1,
                        written,
                        raced,
                        wstop,
                        args.writer_coroutines,
                    ),
                )
                p.start()
                wprocs.append(p)
            time.sleep(2.0)

        # 计时窗口
        with written.get_lock():
            written.value = 0
        rep_a = base.redis_snapshot(args.replica)
        cpu_a = tree_cpu(sproc)
        wall_a = time.perf_counter()
        window[0] = time.time()
        window[1] = window[0] + args.duration
        time.sleep(args.duration)
        wall = time.perf_counter() - wall_a
        cpu = tree_cpu(sproc) - cpu_a
        rep_b = base.redis_snapshot(args.replica)
        with written.get_lock():
            n_written = written.value

        # 收尾
        wstop.set()
        for p in wprocs:
            p.join(timeout=10)
        time.sleep(1.0)  # 让窗口末尾的推送到达
        stop.set()
        delivered = deleted = frames = 0
        lat: list[float] = []
        for _ in cprocs:
            d, dl, fr, ls = result_q.get(timeout=30)
            delivered += d
            deleted += dl
            frames += fr
            lat.extend(ls)
        for p in cprocs:
            p.join(timeout=10)
    finally:
        try:
            children = sproc.children(recursive=True)
        except psutil.Error:
            children = []
        server.terminate()
        for child in children:
            try:
                child.terminate()
            except psutil.Error:
                pass
        try:
            server.wait(timeout=15)
        except subprocess.TimeoutExpired:
            server.kill()
        server_log.close()

    fanout = args.conns / args.zones
    writes_ps = n_written / wall
    offered = writes_ps * fanout
    delivered_ps = delivered / wall
    rep_cpu = rep_b["cpu"] - rep_a["cpu"]
    res: dict[str, Any] = {
        "conns": args.conns,
        "zones": args.zones,
        "limit": args.limit,
        "fanout": fanout,
        "hot_rows": len(hot_ids),
        "server_workers": args.workers,
        "duration_s": round(wall, 1),
        "subscribe_s": round(subscribe_time, 2),
        "writes_ps": round(writes_ps),
        "raced": raced.value,
        "offered_ps": round(offered),
        "delivered_ps": round(delivered_ps),
        "deliver_ratio": round(delivered_ps / offered, 3) if offered else float("nan"),
        "frames_ps": round(frames / wall),
        "rows_per_frame": round(delivered / frames, 2) if frames else None,
        "server_cores": round(cpu / wall, 3),
        "server_us_per_delivered": round(cpu * 1e6 / delivered, 1)
        if delivered
        else None,
        "replica_cores": round(rep_cpu / wall, 3),
        "replica_cmds_ps": round((rep_b["cmds"] - rep_a["cmds"]) / wall),
        "lat_p50_ms": round(base.pct(lat, 0.5) * 1000, 1),
        "lat_p90_ms": round(base.pct(lat, 0.9) * 1000, 1),
        "lat_p99_ms": round(base.pct(lat, 0.99) * 1000, 1),
        "lat_max_ms": round(max(lat, default=float("nan")) * 1000, 1),
        "tag": args.tag,
        "server_log": os.path.join(workdir, "server.log"),
    }
    width = max(len(k) for k in res)
    for k, v in res.items():
        print(f"{k:<{width}} : {v}")


if __name__ == "__main__":
    mp.freeze_support()
    main()
