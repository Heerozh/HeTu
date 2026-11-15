"""Connection storm benchmark: thousands of clients (re)connecting at the same moment.

建连风暴压测：几千个客户端同时连上来（重连风暴、开服），看建连要多久、会不会失败、连接在各 worker 上分得
匀不匀、之后稳态 RPC 的延迟，以及前置 Caddy 时它回了什么。设计稿
docs/superpowers/specs/2026-11-14-aiohttp-migration-design.md §12.4、§12.5 的数据出自这里。

一次运行：
1. 起 hetu（--workers 个 worker，整个进程树绑在 --server-cpu 上），等每个 worker 都开好服。--restart
   时反过来：客户端先开始连，服务器在风暴开始的那一刻才启动；
2. --clients 个客户端进程同一时刻开始，各开 --conns 个连接（每个进程最多 --concurrency 个同时在建），
   每个连接做 HeTu 握手（jsonb + zlib + crypto）、调一次 hello_world，记下从风暴开始到这次调用返回的
   时间。连不上（被拒、超时、HTTP 5xx）时等 --retry-delay 秒重试，之后每次翻倍、封顶 --retry-max；
   SDK 的默认值是 1 秒起、30 秒封顶，不加抖动。不给 --retry-delay 就不重试；
3. --stall N：风暴开始时把第一个 worker SIGSTOP N 秒（模拟 worker 卡住：长 GC、阻塞的代码）；
4. 全部连上（或放弃）后，数各 worker 手上的连接，记下内核监听队列溢出等 TCP 计数器；
5. 稳态：所有连接按泊松过程定速调 hello_world（每个连接 --rate 次/秒），延迟从排定的时刻算起（含排队），
   跑 --steady 秒。--trickle N：稳态期间再陆续新开 N 个连接，看新连接分给了谁。

--caddy：客户端经 Caddy 连 HeTu（docker 镜像 --caddy-image，绑在 --caddy-cpus 上），Caddy 的配置同运维
文档的参考配置（dynamic a、least_conn、lb_try_duration 5s、fail_duration 30s、dial_timeout 2s），结果里
记下 Caddy 回的状态码和错误。

每次运行还记下内核里实际的监听队列长度（ss），和压测之外的进程占了多少核（others_cores，比如别的会话在
跑测试），占得多时数据不可信。

Redis 自己起，比如：
  docker run -d --rm --name hetu-bench-storm-redis --network host --cpuset-cpus 0 \\
    redis:latest redis-server --port 16390 --save '' --appendonly no
用法（--output 必须是不存在的目录）：
  python benchmark/connect_storm.py --output /tmp/storm/burst-1 --backlog 128
  python benchmark/connect_storm.py --output /tmp/storm/stall-1 --stall 3
  python benchmark/connect_storm.py --output /tmp/storm/restart-1 --restart \\
    --retry-delay 0.05 --retry-max 0.05 --retry-jitter 0.2
  python benchmark/connect_storm.py --output /tmp/storm/caddy-1 --caddy --retry-delay 1
"""

import argparse
import asyncio
import json
import os
import random
import re
import signal
import socket
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path
from typing import cast

import psutil

REPO = Path(__file__).resolve().parents[1]
CADDY_CONTAINER = "hetu-bench-storm-caddy"
# 替压测干活的进程（容器里的 Redis、Caddy，docker 转发日志），不算"压测之外"
BENCH_HELPERS = {
    "redis-server",
    "caddy",
    "dockerd",
    "containerd",
    "containerd-shim",
    "containerd-shim-runc-v2",
}
NSTAT_KEYS = [
    "TcpExtListenOverflows",
    "TcpExtListenDrops",
    "TcpExtSyncookiesSent",
    "TcpExtSyncookiesRecv",
    "TcpExtTCPReqQFullDoCookies",
    "TcpExtTCPReqQFullDrop",
    "TcpEstabResets",
    "TcpOutRsts",
    "TcpAttemptFails",
    "TcpExtTCPAbortOnData",
    "TcpExtTCPAbortOnTimeout",
    "TcpExtTCPTimeouts",
    "TcpRetransSegs",
]
# 运维文档 docs/zh/operations.md 的参考配置，只把 HeTu 节点换成本机、加上访问日志。localhost 还会解析出
# ::1，HeTu 只听 127.0.0.1，所以只要 IPv4（生产里内网 DNS 名只有 A 记录）
CADDYFILE = """{{
	admin off
	auto_https off
}}

:{caddy_port} {{
	log {{
		output stdout
		format json
	}}
	reverse_proxy {{
		dynamic a {{
			name localhost
			port {port}
			refresh 2s
			versions ipv4
		}}
		lb_policy least_conn
		lb_try_duration 5s
		fail_duration 30s
		stream_close_delay 2h
		transport http {{
			dial_timeout 2s
		}}
	}}
}}
"""


def parse_args():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--server-root", type=Path, default=REPO, help="服务器跑这份代码")
    ap.add_argument(
        "--output", type=Path, required=True, help="本次运行的目录，必须不存在"
    )
    ap.add_argument("--redis", default="redis://127.0.0.1:16390/0")
    ap.add_argument("--port", type=int, default=18470)
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--server-cpu", default="1,2,3")
    ap.add_argument(
        "--backlog", type=int, help="LISTEN_BACKLOG，不给就用服务器的默认值"
    )
    ap.add_argument("--clients", type=int, default=8, help="客户端进程数")
    ap.add_argument("--client-cpus", default="4,5,6,7,8,9,10,11")
    ap.add_argument("--conns", type=int, default=750, help="每个客户端进程的连接数")
    ap.add_argument(
        "--concurrency", type=int, default=750, help="每个客户端进程同时在建的连接数"
    )
    ap.add_argument("--open-timeout", type=float, default=60, help="一次尝试最多等几秒")
    ap.add_argument(
        "--retry-delay", type=float, default=0, help="第一次重试前等几秒，0 为不重试"
    )
    ap.add_argument(
        "--retry-max", type=float, default=30, help="重试间隔翻倍到这里封顶"
    )
    ap.add_argument(
        "--retry-jitter", type=float, default=0, help="每次重试前再随机多等 0~N 秒"
    )
    ap.add_argument(
        "--give-up", type=float, default=120, help="风暴开始后过了几秒还没连上就放弃"
    )
    ap.add_argument(
        "--stall", type=float, default=0, help="风暴开始时把第一个 worker 暂停几秒"
    )
    ap.add_argument(
        "--restart", action="store_true", help="客户端先连，服务器风暴开始时才启动"
    )
    ap.add_argument("--rate", type=float, default=10, help="稳态时每个连接每秒调几次")
    ap.add_argument("--steady", type=float, default=10, help="稳态跑几秒")
    ap.add_argument("--trickle", type=int, default=0, help="稳态期间陆续新开的连接总数")
    ap.add_argument("--caddy", action="store_true", help="客户端经 Caddy 连")
    ap.add_argument("--caddy-port", type=int, default=18480)
    ap.add_argument("--caddy-cpus", default="12,13,14,15")
    ap.add_argument("--caddy-image", default="caddy:2")
    ap.add_argument("--client", type=int, default=-1, help=argparse.SUPPRESS)
    args = ap.parse_args()
    if args.restart and args.retry_delay <= 0:
        ap.error("--restart 时服务器还没起来，客户端得重试，请给 --retry-delay")
    return args


def pct(xs, p):
    if not xs:
        return None
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(len(xs) * p))]


def rounded(x, n=3):
    return None if x is None else round(x, n)


def pct_ms(xs, p):
    v = pct(xs, p)
    return None if v is None else round(v * 1000, 2)


def write_json(path: Path, data) -> None:
    """先写临时文件再改名：另一个进程轮询这个文件时不会读到半截"""
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data))
    tmp.rename(path)


def wait_file(path: Path, timeout: float):
    deadline = time.time() + timeout
    while not path.exists():
        if time.time() > deadline:
            raise TimeoutError(path)
        time.sleep(0.02)
    return json.loads(path.read_text())


# ---------------------------------------------------------------- 客户端进程


async def client_main(args):
    import websockets
    from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey
    from websockets.exceptions import InvalidStatus

    from hetu.server import pipeline

    port = args.caddy_port if args.caddy else args.port
    url = f"ws://127.0.0.1:{port}/hetu/bench"
    out = args.output
    rng = random.Random(args.client)

    async def await_file(path: Path, timeout: float = 600):
        # 等的时候事件循环照常转：服务器每 20 秒 ping 一次，等 pong 10 秒，卡住事件循环的客户端会被断开
        deadline = time.time() + timeout
        while not path.exists():
            if time.time() > deadline:
                raise TimeoutError(path)
            await asyncio.sleep(0.02)
        return json.loads(path.read_text())

    async def connect():
        ws = await websockets.connect(
            url,
            open_timeout=args.open_timeout,
            ping_interval=None,
            compression=None,
            max_size=None,
        )
        try:
            pipe = pipeline.MessagePipeline()
            pipe.add_layer(pipeline.JSONBinaryLayer())
            pipe.add_layer(pipeline.ZlibLayer())
            crypto = pipeline.CryptoLayer()
            pipe.add_layer(crypto)
            key = X25519PrivateKey.generate()
            msg = [b""] * pipe.num_handshake_layers
            msg[-1] = key.public_key().public_bytes_raw()
            await ws.send(pipe.encode(None, msg))
            reply = cast(list[bytes], pipe.decode(None, cast(bytes, await ws.recv())))
            ctx, _ = pipe.handshake(reply)
            ctx[-1] = crypto.client_handshake(key.private_bytes_raw(), reply[-1])
        except BaseException:
            await ws.close()
            raise
        return ws, pipe, ctx

    async def call(conn):
        ws, pipe, ctx = conn
        await ws.send(pipe.encode(ctx, ["rpc", "hello_world"]))
        reply = cast(list, pipe.decode(ctx, cast(bytes, await ws.recv())))
        assert reply[1] == ["世界收到"], reply

    def describe(e: Exception) -> tuple[str, bool]:
        """
        失败的短描述，以及要不要重试。同 SDK：被拒、超时、HTTP 5xx、连接中途断开都重试；HTTP 4xx 和回复内容
        不对（AssertionError）不重试
        """
        if isinstance(e, InvalidStatus):
            status = e.response.status_code
            return f"HTTP {status}", status >= 500
        return type(e).__name__, not isinstance(e, AssertionError)

    (out / f"ready-{args.client}").write_text("")
    start = (await await_file(out / "start.json"))["start"]
    await asyncio.sleep(max(0.0, start - time.time()))
    sem = asyncio.Semaphore(args.concurrency)

    async def one():
        failures: list[str] = []
        delay = args.retry_delay
        async with sem:
            while True:
                try:
                    conn = await connect()
                    await call(conn)
                    return conn, time.time() - start, failures, None
                except Exception as e:  # noqa: BLE001 记下来，按 describe 决定重试还是放弃
                    what, retry = describe(e)
                    failures.append(what)
                    late = time.time() - start > args.give_up
                    if not retry or args.retry_delay <= 0 or late:
                        return None, time.time() - start, failures, repr(e)[:160]
                    await asyncio.sleep(delay + rng.uniform(0, args.retry_jitter))
                    delay = min(delay * 2, args.retry_max)

    results = await asyncio.gather(*(one() for _ in range(args.conns)))
    conns = [r[0] for r in results if r[0] is not None]
    write_json(
        out / f"connected-{args.client}.json",
        {
            "lat": [r[1] for r in results if r[0] is not None],
            "failed": sum(1 for r in results if r[0] is None),
            "error_samples": [r[3] for r in results if r[3]][:5],
            # 第一次就没连上的连接数（不管最后连没连上），以及所有失败尝试按原因的计数
            "first_try_failed": sum(1 for r in results if r[2]),
            "failure_kinds": Counter(f for r in results for f in r[2]),
            "attempts": Counter(str(len(r[2]) + (r[0] is not None)) for r in results),
        },
    )

    steady = await await_file(out / "steady.json")
    t0, t1 = steady["start"], steady["start"] + steady["duration"]
    lats: list[float] = []
    steady_errors = 0
    extra = []
    trickle_errors = 0

    async def drive(conn):
        nonlocal steady_errors
        due = t0 + rng.expovariate(args.rate)
        while due < t1:
            await asyncio.sleep(max(0.0, due - time.time()))
            try:
                await call(conn)
            except Exception:  # noqa: BLE001 只计数，这个连接不再用
                steady_errors += 1
                return
            lats.append(time.time() - due)
            due += rng.expovariate(args.rate)

    async def trickle():
        nonlocal trickle_errors
        n = args.trickle // args.clients
        span = steady["duration"] - 1.0
        for k in range(n):
            due = t0 + (k + rng.random()) * span / n
            await asyncio.sleep(max(0.0, due - time.time()))
            try:
                conn = await connect()
                await call(conn)
                extra.append(conn)
            except Exception:  # noqa: BLE001 只计数
                trickle_errors += 1

    await asyncio.sleep(max(0.0, t0 - time.time()))
    trickle_task = asyncio.create_task(trickle())
    await asyncio.gather(*(drive(c) for c in conns))
    await trickle_task
    write_json(
        out / f"steady-{args.client}.json",
        {"lat": lats, "errors": steady_errors, "trickle_errors": trickle_errors},
    )
    await await_file(out / "close.json")
    for ws, _pipe, _ctx in conns + extra:
        await ws.close()


# ---------------------------------------------------------------- 编排


def nstat() -> dict[str, int]:
    out = subprocess.run(
        ["nstat", "-az", *NSTAT_KEYS], capture_output=True, text=True, check=False
    ).stdout
    return {
        p[0]: int(p[1])
        for p in (line.split() for line in out.splitlines())
        if p and p[0].startswith("Tcp")
    }


def effective_backlog(port: int):
    """内核里监听 socket 的队列上限（ss 的 Send-Q），读不到时是 None"""
    try:
        out = subprocess.run(
            ["ss", "-ltnH", f"sport = :{port}"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        ).stdout
    except OSError, subprocess.TimeoutExpired:
        return None
    values = [int(line.split()[2]) for line in out.splitlines() if line.strip()]
    return values[0] if len(values) == 1 else values or None


def cpu_snapshot() -> dict[int, tuple[str, float]]:
    snap = {}
    for p in psutil.process_iter(["name", "cpu_times"]):
        times = p.info["cpu_times"]
        if times is not None and p.info["name"] not in BENCH_HELPERS:
            snap[p.pid] = (p.info["name"], times.user + times.system)
    return snap


def others_usage(before, after, elapsed: float, exclude: set[int]):
    """压测之外的进程在这段时间里平均占了几个核，和占得最多的三个"""
    used: Counter[str] = Counter()
    for pid, (name, t) in after.items():
        if pid in before and pid not in exclude:
            used[name] += t - before[pid][1]
    total = sum(used.values()) / elapsed
    top = [(name, round(t / elapsed, 2)) for name, t in used.most_common(3)]
    return round(total, 2), top


def worker_conns(workers, port: int) -> list[int]:
    return [
        sum(
            1
            for c in w.net_connections(kind="tcp")
            if c.status == "ESTABLISHED" and c.laddr.port == port
        )
        for w in workers
    ]


def start_caddy(args) -> None:
    caddyfile = args.output / "Caddyfile"
    caddyfile.write_text(CADDYFILE.format(caddy_port=args.caddy_port, port=args.port))
    subprocess.run(
        ["docker", "rm", "-f", CADDY_CONTAINER], capture_output=True, check=False
    )
    subprocess.run(
        [
            "docker",
            "run",
            "-d",
            "--rm",
            "--name",
            CADDY_CONTAINER,
            "--network",
            "host",
            "--cpuset-cpus",
            args.caddy_cpus,
            "-v",
            f"{caddyfile}:/etc/caddy/Caddyfile:ro",
            args.caddy_image,
        ],
        check=True,
        capture_output=True,
    )
    deadline = time.time() + 30
    while True:
        try:
            socket.create_connection(("127.0.0.1", args.caddy_port), timeout=1).close()
            return
        except OSError:
            if time.time() > deadline:
                raise RuntimeError("caddy did not start") from None
            time.sleep(0.1)


def stop_caddy(args) -> None:
    logs = subprocess.run(
        ["docker", "logs", CADDY_CONTAINER], capture_output=True, text=True, check=False
    )
    (args.output / "caddy.log").write_text(logs.stdout + logs.stderr)
    subprocess.run(
        ["docker", "stop", "-t", "2", CADDY_CONTAINER], capture_output=True, check=False
    )


def caddy_summary(args, start: float) -> dict:
    """
    Caddy 回的状态码、错误，以及按秒的时间线。访问日志在请求结束时才写：失败的按回应的时刻记，101（ws 建好）
    按开始的时刻（结束时刻减去持续时间）记
    """
    status: Counter[str] = Counter()
    errors: Counter[str] = Counter()
    timeline: dict[int, Counter[str]] = {}
    for line in (args.output / "caddy.log").read_text(errors="replace").splitlines():
        try:
            entry = json.loads(line)
        except ValueError:
            continue
        name = entry.get("logger", "")
        if name.startswith("http.log.access"):
            code = str(entry.get("status"))
            status[code] += 1
            at = entry.get("ts", 0)
            if code == "101":
                at -= entry.get("duration", 0)
            timeline.setdefault(int(at - start), Counter())[code] += 1
        elif name.startswith("http.log.error"):
            msg = re.sub(r"\d+\.\d+\.\d+\.\d+:\d+", "ADDR", str(entry.get("msg", "")))
            errors[f"{entry.get('status')} {msg}"] += 1
    return {
        "status": dict(status),
        "errors": dict(errors.most_common(8)),
        "timeline": {str(k): dict(v) for k, v in sorted(timeline.items())},
    }


def main(args):
    args.output.mkdir(parents=True, exist_ok=False)
    out = args.output
    root = args.server_root.resolve()
    backlog = f"LISTEN_BACKLOG: {args.backlog}\n" if args.backlog else ""
    cfg = out / "server.yml"
    cfg.write_text(f"""APP_FILE: {root / "benchmark/server/app.py"}
NAMESPACE: bench
INSTANCES: [bench]
LISTEN: 127.0.0.1:{args.port}
WORKER_NUM: {args.workers}
{backlog}DEBUG: false
CLIENT_SEND_LIMITS: []
SERVER_SEND_LIMITS: []
MAX_ANONYMOUS_CONNECTION_BY_IP: 0
PACKET_LAYERS:
  - type: jsonb
  - type: zlib
    level: 1
  - type: crypto
BACKENDS:
  Redis:
    type: Redis
    master: {args.redis}
""")
    env = dict(os.environ, PYTHONPATH=str(root))
    log = (out / "server.log").open("w")
    server = None
    workers: list[psutil.Process] = []
    clients: list[subprocess.Popen] = []
    ready_at: list[float] = []

    def launch_server():
        return subprocess.Popen(
            [
                "taskset",
                "-c",
                args.server_cpu,
                sys.executable,
                "-m",
                "hetu",
                "start",
                "--config",
                str(cfg),
            ],
            cwd=out,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )

    def wait_workers() -> list[psutil.Process]:
        # 记下每个 worker 开好服的时刻（轮询日志，精度约 10ms）
        assert server is not None
        deadline = time.time() + 60
        while (
            n := (out / "server.log")
            .read_text(errors="replace")
            .count("已启动 bench 应用")
        ) < args.workers:
            ready_at.extend([time.time()] * (n - len(ready_at)))
            if server.poll() is not None or time.time() > deadline:
                raise RuntimeError("server failed to start; see server.log")
            time.sleep(0.01)
        ready_at.extend([time.time()] * (args.workers - len(ready_at)))
        time.sleep(0.5)
        found = [
            p
            for p in psutil.Process(server.pid).children(recursive=True)
            if "spawn_main" in " ".join(p.cmdline())
            and "resource_tracker" not in " ".join(p.cmdline())
        ]
        assert len(found) == args.workers, [p.cmdline() for p in found]
        return found

    try:
        if args.caddy:
            start_caddy(args)
        if not args.restart:
            server = launch_server()
            workers = wait_workers()
        cpus = args.client_cpus.split(",")
        for i in range(args.clients):
            clients.append(
                subprocess.Popen(
                    [
                        "taskset",
                        "-c",
                        cpus[i % len(cpus)],
                        sys.executable,
                        __file__,
                        *sys.argv[1:],
                        "--client",
                        str(i),
                    ],
                    env=env,
                    stdout=(out / f"client-{i}.log").open("w"),
                    stderr=subprocess.STDOUT,
                )
            )
        while len(list(out.glob("ready-*"))) < args.clients:
            if any(c.poll() is not None for c in clients):
                raise RuntimeError("client failed; see client-*.log")
            time.sleep(0.05)
        tcp_before = nstat()
        cpu_before, cpu_t0 = cpu_snapshot(), time.time()
        start = time.time() + 1.0
        write_json(out / "start.json", {"start": start})
        if args.restart:
            time.sleep(max(0.0, start - time.time()))
            server = launch_server()
            workers = wait_workers()
        backlog_now = effective_backlog(args.port)
        if args.stall:
            time.sleep(max(0.0, start - time.time()))
            os.kill(workers[0].pid, signal.SIGSTOP)
            time.sleep(args.stall)
            os.kill(workers[0].pid, signal.SIGCONT)
        timeout = args.give_up + args.open_timeout + 60
        connected = [
            wait_file(out / f"connected-{i}.json", timeout) for i in range(args.clients)
        ]
        tcp_after = nstat()
        time.sleep(0.5)
        dist = worker_conns(workers, args.port)
        pid_conns = {w.pid: d for w, d in zip(workers, dist, strict=True)}
        ready_pids = re.findall(
            r"进程\[(\d+)\] 已启动 bench 应用",
            (out / "server.log").read_text(errors="replace"),
        )
        steady_start = time.time() + 1.0
        write_json(
            out / "steady.json", {"start": steady_start, "duration": args.steady}
        )
        time.sleep(max(0.0, steady_start - time.time()))
        busy0 = [sum(w.cpu_times()[:2]) for w in workers]
        time.sleep(args.steady)
        busy = [sum(w.cpu_times()[:2]) - b for w, b in zip(workers, busy0, strict=True)]
        steady = [
            wait_file(out / f"steady-{i}.json", args.steady + 120)
            for i in range(args.clients)
        ]
        dist_after = worker_conns(workers, args.port)
        assert server is not None
        exclude = {os.getpid(), server.pid, *(c.pid for c in clients)}
        exclude |= {p.pid for p in psutil.Process(server.pid).children(recursive=True)}
        others = others_usage(cpu_before, cpu_snapshot(), time.time() - cpu_t0, exclude)
        write_json(out / "close.json", {})
        for c in clients:
            c.wait(timeout=60)
        lat = [x for c in connected for x in c["lat"]]
        slat = [x for s in steady for x in s["lat"]]
        failure_kinds: Counter[str] = Counter()
        attempts: Counter[str] = Counter()
        for c in connected:
            failure_kinds.update(c["failure_kinds"])
            attempts.update(c["attempts"])
        result = {
            "backlog": args.backlog,
            "backlog_in_kernel": backlog_now,
            "stall": args.stall,
            "restart": args.restart,
            "caddy": args.caddy,
            "retry": [args.retry_delay, args.retry_max, args.retry_jitter],
            "connected": len(lat),
            "failed": sum(c["failed"] for c in connected),
            "error_samples": [e for c in connected for e in c["error_samples"]][:3],
            "first_try_failed": sum(c["first_try_failed"] for c in connected),
            "failure_kinds": dict(failure_kinds),
            "attempts": dict(sorted(attempts.items(), key=lambda kv: int(kv[0]))),
            "connect_p50_s": rounded(pct(lat, 0.5)),
            "connect_p90_s": rounded(pct(lat, 0.9)),
            "connect_p99_s": rounded(pct(lat, 0.99)),
            "connect_max_s": rounded(max(lat) if lat else None),
            # 各 worker 的连接数；stall 时第一个是被暂停过的那个
            "worker_conns": dist,
            # 同上，按开好服的先后排（日志里"进程[pid] 已启动"的顺序）
            "worker_conns_by_ready": [pid_conns.get(int(p)) for p in ready_pids],
            "workers_ready_s": [round(t - start, 3) for t in ready_at]
            if args.restart
            else None,
            "tcp": {
                k: tcp_after.get(k, 0) - tcp_before.get(k, 0)
                for k in NSTAT_KEYS
                if tcp_after.get(k, 0) != tcp_before.get(k, 0)
            },
            "steady_qps": round(len(slat) / args.steady),
            "steady_p50_ms": pct_ms(slat, 0.5),
            "steady_p99_ms": pct_ms(slat, 0.99),
            "steady_p999_ms": pct_ms(slat, 0.999),
            "steady_errors": sum(s["errors"] for s in steady),
            # 稳态时各 worker 占几个核，顺序同 worker_conns
            "steady_worker_cores": [round(b / args.steady, 2) for b in busy],
            "trickle_dist": [a - b for a, b in zip(dist_after, dist, strict=True)]
            if args.trickle
            else None,
            "trickle_errors": sum(s["trickle_errors"] for s in steady),
            "others_cores": others[0],
            "others_top": others[1],
        }
        if args.caddy:
            stop_caddy(args)
            result["caddy_log"] = caddy_summary(args, start)
        (out / "result.json").write_text(
            json.dumps(result, indent=2, ensure_ascii=False)
        )
        print(json.dumps(result, ensure_ascii=False))
    finally:
        for c in clients:
            if c.poll() is None:
                c.kill()
        if server is not None and server.poll() is None:
            os.killpg(server.pid, signal.SIGINT)
            try:
                server.wait(timeout=60)
            except subprocess.TimeoutExpired:
                os.killpg(server.pid, signal.SIGKILL)
        if args.caddy:
            subprocess.run(
                ["docker", "rm", "-f", CADDY_CONTAINER],
                capture_output=True,
                check=False,
            )
        log.close()


if __name__ == "__main__":
    parsed = parse_args()
    if parsed.client >= 0:
        asyncio.run(client_main(parsed))
    else:
        main(parsed)
