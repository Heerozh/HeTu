"""Measure RPC throughput over real WebSockets with a frozen client.

真实 WebSocket RPC 基准；固定客户端源码，避免把客户端优化算作服务端收益。
Run with --help. Requires an isolated Redis database. Logs/results go to --output.

--perf 让每个窗口另用 perf stat 数服务器的 user 态周期和指令，并给出运行频率相对 TSC 的倍数（cycles / ref-cycles，
用来确认两次运行的频率一样；scaling_cur_freq 掺了空闲时的读数，不是运行频率）。
每个窗口还记下服务器那个核在 /proc/stat 里的空闲、软中断占比（loopback 收包的软中断不算进进程的
CPU 时间），以及每次 RPC 的读写类系统调用数。

--rate 改成定速（开环）：每个连接按泊松过程排发送时刻，延迟从排定的时刻算起（含排队），每次调用都
记延迟。默认是闭环满载，延迟每连接每 64 次抽一次，那时的延迟主要由吞吐决定。
"""

import argparse
import asyncio
import json
import os
import random
import signal
import socket
import statistics
import subprocess
import sys
import time
from pathlib import Path

import psutil
from seed_get2_rows import seed_get2_rows, verify_get2_rows
from seed_get_rows import seed_get_rows
from seed_range50_rows import seed_range50_rows, verify_range50_rows


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server-root", type=Path, default=Path.cwd())
    parser.add_argument("--client-root", type=Path, required=True)
    parser.add_argument(
        "--workload",
        choices=("hello_world", "get", "get2_update2", "range50_update2"),
        default="hello_world",
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--redis", default="redis://127.0.0.1:16389/0")
    parser.add_argument("--port", type=int, default=18466)
    parser.add_argument("--processes", type=int, default=4)
    parser.add_argument("--connections", type=int, default=16, help="per process")
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--seconds", type=float, default=10)
    parser.add_argument("--warmup", type=float, default=3)
    parser.add_argument(
        "--server-cpu",
        default="3",
        help="服务器绑这些核（逗号分隔）。多 worker 时各 worker 不单独绑核，在这些核上由系统调度",
    )
    parser.add_argument("--workers", type=int, default=1, help="服务器的 WORKER_NUM")
    parser.add_argument("--client-cpus", default="4,6,8,10")
    parser.add_argument("--perf", action="store_true")
    parser.add_argument(
        "--rate",
        type=float,
        default=0,
        help="定速，所有连接合计每秒调用数；0 为闭环满载",
    )
    parser.add_argument("--profile", action="store_true")
    parser.add_argument("--profile-cpu", type=int, default=1)
    parser.add_argument("--client", type=int, default=-1, help=argparse.SUPPRESS)
    args = parser.parse_args()
    args.server_cpus = [int(x) for x in args.server_cpu.split(",")]
    return args


def cpu_environment(cpu):
    """记录混合核心与电源策略，便于识别跨运行环境变化。

    Record hybrid CPU topology and power policy alongside each measurement.
    """
    paths = {
        "p_cores": "/sys/bus/event_source/devices/cpu_core/cpus",
        "e_cores": "/sys/bus/event_source/devices/cpu_atom/cpus",
        "platform_profile": "/sys/firmware/acpi/platform_profile",
        "governor": f"/sys/devices/system/cpu/cpu{cpu}/cpufreq/scaling_governor",
        "energy_performance_preference": (
            f"/sys/devices/system/cpu/cpu{cpu}/cpufreq/energy_performance_preference"
        ),
    }
    return {
        name: Path(path).read_text().strip()
        for name, path in paths.items()
        if Path(path).exists()
    }


def perf_stat(pids, seconds):
    """数这些进程（pid，逗号分隔）接下来 seconds 秒的 user 态周期和指令，加总。

    Count user-mode cycles and instructions of the given pids for the next seconds.
    """
    return subprocess.Popen(
        [
            "perf",
            "stat",
            "-x",
            ",",
            "-e",
            "cycles:u,instructions:u,ref-cycles:u",
            "-p",
            str(pids),
            "--",
            "sleep",
            str(seconds),
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )


def perf_counts(proc):
    """读 perf_stat 的结果；混合架构的 P 核、E 核计数相加，没数到的事件不出现。

    Parse perf_stat output, summing P-core and E-core counts on hybrid CPUs.
    """
    _out, err = proc.communicate(timeout=60)
    counts = {}
    for line in err.splitlines():
        parts = line.split(",")
        if len(parts) > 2 and parts[0][:1].isdigit():
            event = parts[2].replace("cpu_core/", "").replace("cpu_atom/", "")
            event = event.replace("/u", "").replace(":u", "")
            counts[event] = counts.get(event, 0.0) + float(parts[0])
    return counts


def core_times(cpu):
    """/proc/stat 里这个核的累计时间：user nice system idle iowait irq softirq steal ...

    Cumulative /proc/stat times of one CPU.
    """
    with open("/proc/stat") as f:
        for line in f:
            if line.startswith(f"cpu{cpu} "):
                return [int(x) for x in line.split()[1:]]
    raise LookupError(cpu)


def snapshot(procs, cpus):
    """窗口起止时记下服务器各进程的 CPU 时间、服务器各核的 /proc/stat、读写类系统调用数（都是加总）。

    Snapshot the summed CPU time, /proc/stat of the server cores and syscall counts.
    """
    cpu = 0.0
    syscalls = 0
    for proc in procs:
        times = proc.cpu_times()
        cpu += times.user + times.system
        with open(f"/proc/{proc.pid}/io") as f:
            io = dict(line.split(": ") for line in f.read().splitlines())
        syscalls += int(io["syscr"]) + int(io["syscw"])
    core = [sum(col) for col in zip(*(core_times(c) for c in cpus), strict=True)]
    return {"wall": time.monotonic(), "cpu": cpu, "core": core, "syscalls": syscalls}


def server_processes(server):
    """服务器的进程树：单 worker 时就是它自己，多 worker 时还有各 worker（和 multiprocessing 的辅助进程）"""
    proc = psutil.Process(server.pid)
    return [proc, *proc.children(recursive=True)]


def worker_connections(procs, port):
    """各进程手上已建立的、本端口是 port 的 TCP 连接数（看连接在各 worker 间分得匀不匀）"""
    counts = {}
    for proc in procs:
        conns = proc.net_connections(kind="tcp")
        counts[proc.pid] = sum(
            1 for c in conns if c.status == "ESTABLISHED" and c.laddr.port == port
        )
    return counts


async def client(args):
    # Import only after PYTHONPATH has selected the frozen client checkout.
    import logging

    from benchmark import ya_hetu_rpc as rpc

    connection = rpc.connection
    name = "exchange" if args.workload == "get2_update2" else args.workload
    benchmark = getattr(rpc, f"benchmark_{name}", None)
    if benchmark is None:
        benchmark = getattr(rpc, f"benchmark_{args.workload}")
    expected = 0 if args.workload == "get" else "世界收到"
    random.seed(20260925 + args.client)

    def verify(result):
        if args.workload in ("get2_update2", "range50_update2"):
            assert type(result) is int and result >= 0, result
        else:
            assert result == expected, result

    logging.getLogger("HeTu.root").setLevel(logging.WARNING)
    loop = asyncio.get_running_loop()
    start = loop.create_future()
    ready = 0
    counts = [0] * args.rounds
    retries = [0] * args.rounds
    latencies = [[] for _ in counts]
    cpu_start = None
    completed = 0

    async def one(k):
        nonlocal ready, cpu_start, completed
        fixture = connection()
        conn = await anext(fixture)
        try:
            verify(await benchmark(conn))
            completed += 1
            ready += 1
            windows = await start
            last_end = windows[-1][1]
            i = 0
            seen = 0
            # 定速：泊松过程排发送时刻；上一次调用拖过了下一个时刻就马上发，延迟照样从排定的时刻算
            per_conn = args.rate / (args.processes * args.connections)
            rng = random.Random(20260925_000 + args.client * 1000 + k)
            due = time.monotonic()
            while True:
                if per_conn > 0:
                    due += rng.expovariate(per_conn)
                    delay = due - time.monotonic()
                    if delay > 0:
                        await asyncio.sleep(delay)
                    before = due
                else:
                    before = time.monotonic()
                if before >= last_end:
                    break
                result = await benchmark(conn)
                after = time.monotonic()
                verify(result)
                completed += 1
                while i < len(windows) and after >= windows[i][1]:
                    i += 1
                if i < len(windows) and windows[i][0] <= before:
                    if cpu_start is None:
                        cpu_start = time.process_time()
                    counts[i] += 1
                    if args.workload in ("get2_update2", "range50_update2"):
                        retries[i] += result
                    seen += 1
                    if per_conn > 0 or seen % 64 == 0:
                        latencies[i].append((after - before) * 1000)
        finally:
            await fixture.aclose()
            await conn[0].close()

    tasks = [asyncio.create_task(one(k)) for k in range(args.connections)]
    while ready < args.connections:
        for task in tasks:
            if task.done():
                task.result()
        await asyncio.sleep(0.02)
    (args.output / f"ready-{args.client}").touch()
    schedule = args.output / "schedule.json"
    while not schedule.exists():
        await asyncio.sleep(0.02)
    start.set_result(json.loads(schedule.read_text()))
    await asyncio.gather(*tasks)
    result = {
        "counts": counts,
        "completed_including_warmup": completed,
        "retries": retries,
        "latencies_ms": latencies,
        "cpu_seconds": time.process_time() - (cpu_start or 0),
    }
    (args.output / f"client-{args.client}.json").write_text(json.dumps(result))


def main(args):
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    root = args.server_root.resolve()
    frozen = args.client_root.resolve()
    config = args.output / "server.yml"
    config.write_text(f"""APP_FILE: {root / "benchmark/server/app.py"}
NAMESPACE: bench
INSTANCES: [bench]
LISTEN: 127.0.0.1:{args.port}
WORKER_NUM: {args.workers}
DEBUG: false
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
    cmd = [sys.executable, "-m", "hetu", "start", "--config", str(config)]
    if args.profile:
        cmd = [
            str(Path(sys.executable).parent / "py-spy"),
            "record",
            "-f",
            "speedscope",
            "-o",
            str(args.output / "profile.json"),
            "-r",
            "19",
            "--nonblocking",
            "--",
            *cmd,
        ]
    if args.profile and args.workers > 1:
        raise SystemExit("--profile 只支持单 worker")
    cmd = ["taskset", "-c", args.server_cpu, *cmd]
    children = []
    logs = []
    perfs = []
    server = None
    try:
        log = (args.output / "server.log").open("w")
        logs.append(log)
        server = subprocess.Popen(
            cmd,
            cwd=args.output,
            env=env,
            stdout=log,
            stderr=log,
            start_new_session=True,
        )
        deadline = time.monotonic() + 60
        while True:
            if server.poll() is not None:
                raise RuntimeError("Server failed; see server.log")
            try:
                with socket.create_connection(("127.0.0.1", args.port), timeout=0.2):
                    break
            except OSError:
                if time.monotonic() > deadline:
                    raise TimeoutError("Server startup")
                time.sleep(0.1)
        # 多 worker 时端口由管理进程先 bind，worker 后起来：等每个 worker 都打出开好服的那行日志
        while (args.output / "server.log").read_text(errors="replace").count(
            "已启动 bench 应用"
        ) < args.workers:
            if server.poll() is not None:
                raise RuntimeError("Server failed; see server.log")
            if time.monotonic() > deadline:
                raise TimeoutError("Worker startup")
            time.sleep(0.1)
        seeded = {}
        if args.workload == "get":
            seed_get_rows(root, args.redis)
        elif args.workload == "get2_update2":
            seeded = seed_get2_rows(root, args.redis)
        elif args.workload == "range50_update2":
            seeded = seed_range50_rows(root, args.redis)
        client_cpus = args.client_cpus.split(",")
        if args.profile:
            # The Python child already inherited server_cpu. Keep the sampler
            # off that CPU so it doesn't compete with the server event loop.
            psutil.Process(server.pid).cpu_affinity([args.profile_cpu])
        for i in range(args.processes):
            env = dict(
                os.environ,
                PYTHONPATH=str(frozen),
                HETU_URL=f"ws://127.0.0.1:{args.port}/hetu/bench",
            )
            log = (args.output / f"client-{i}.log").open("w")
            logs.append(log)
            cmd = [
                "taskset",
                "-c",
                client_cpus[i % len(client_cpus)],
                sys.executable,
                str(Path(__file__).resolve()),
                "--client",
                str(i),
                "--client-root",
                str(frozen),
                "--output",
                str(args.output),
                "--connections",
                str(args.connections),
                "--rounds",
                str(args.rounds),
                "--workload",
                args.workload,
                "--rate",
                str(args.rate),
                "--processes",
                str(args.processes),
            ]
            children.append(
                subprocess.Popen(cmd, cwd=args.output, env=env, stdout=log, stderr=log)
            )
        while len(list(args.output.glob("ready-*"))) < args.processes:
            if any(p.poll() is not None for p in children):
                raise RuntimeError("Client failed; see client logs")
            if time.monotonic() > deadline:
                raise TimeoutError("Client startup")
            time.sleep(0.1)
        first = time.monotonic() + args.warmup
        windows = [
            (
                first + i * (args.seconds + 1),
                first + i * (args.seconds + 1) + args.seconds,
            )
            for i in range(args.rounds)
        ]
        tmp = args.output / "schedule.tmp"
        tmp.write_text(json.dumps(windows))
        tmp.rename(args.output / "schedule.json")
        procs = server_processes(server)
        if args.profile:
            procs = [next(p for p in procs[0].children() if "python" in p.name())]
        affinity = {
            "server": [p.cpu_affinity() for p in procs],
            "clients": [psutil.Process(p.pid).cpu_affinity() for p in children],
        }
        assert all(a == args.server_cpus for a in affinity["server"]), affinity
        for i, actual in enumerate(affinity["clients"]):
            assert actual == [int(client_cpus[i % len(client_cpus)])], affinity
        environment_start = cpu_environment(args.server_cpus[0])
        connections = None
        cpu_before = None
        cpu_after = None
        round_cpu_start = [None] * args.rounds
        round_cpu_end = [None] * args.rounds
        round_perf = [None] * args.rounds
        while any(p.poll() is None for p in children):
            now = time.monotonic()
            for i, (begin, end) in enumerate(windows):
                if round_cpu_start[i] is None and now >= begin:
                    round_cpu_start[i] = snapshot(procs, args.server_cpus)
                    if args.perf:
                        round_perf[i] = perf_stat(
                            ",".join(str(p.pid) for p in procs), args.seconds
                        )
                        perfs.append(round_perf[i])
                    if connections is None:
                        connections = worker_connections(procs, args.port)
                if round_cpu_end[i] is None and now >= end:
                    round_cpu_end[i] = snapshot(procs, args.server_cpus)
            if cpu_before is None and now >= first:
                cpu_before = sum(sum(p.cpu_times()[:2]) for p in procs)
            if cpu_after is None and now >= windows[-1][1]:
                cpu_after = sum(sum(p.cpu_times()[:2]) for p in procs)
            if now > windows[-1][1] + 30:
                raise TimeoutError("Client completion")
            time.sleep(0.05)
        if any(p.returncode != 0 for p in children):
            raise RuntimeError("Client failed; see client logs")
        if cpu_after is None:
            cpu_after = sum(sum(p.cpu_times()[:2]) for p in procs)
        assert cpu_before is not None
        if round_cpu_end[-1] is None:
            round_cpu_end[-1] = snapshot(procs, args.server_cpus)
        results = [
            json.loads((args.output / f"client-{i}.json").read_text())
            for i in range(args.processes)
        ]
        rounds = []
        for i in range(args.rounds):
            samples = sorted(x for r in results for x in r["latencies_ms"][i])
            qps = sum(r["counts"][i] for r in results) / args.seconds
            begin, end = round_cpu_start[i], round_cpu_end[i]
            wall = end["wall"] - begin["wall"]
            cpu_fraction = (end["cpu"] - begin["cpu"]) / wall
            core = [b - a for a, b in zip(begin["core"], end["core"], strict=True)]
            core_total = sum(core[:8])  # user..steal；guest 已经算在 user 里
            counts = perf_counts(round_perf[i]) if round_perf[i] else {}
            # perf 数的时长与窗口相同（起点晚不到一个轮询间隔），按窗口的调用数折算
            rpcs = qps * args.seconds
            rounds.append(
                {
                    "qps": qps,
                    "successful_rpcs": sum(r["counts"][i] for r in results),
                    "retries": sum(r["retries"][i] for r in results),
                    "retries_per_rpc": sum(r["retries"][i] for r in results)
                    / sum(r["counts"][i] for r in results),
                    "server_cpu_percent": cpu_fraction * 100,
                    "server_cpu_us_per_rpc": cpu_fraction * 1e6 / qps,
                    "p50_ms": samples[len(samples) // 2],
                    "p99_ms": samples[int(len(samples) * 0.99)],
                    "p999_ms": samples[int(len(samples) * 0.999)],
                    "max_ms": samples[-1],
                    "latency_samples": len(samples),
                    "server_kcycles_u_per_rpc": counts["cycles"] / rpcs / 1e3
                    if "cycles" in counts
                    else None,
                    "server_kinstructions_u_per_rpc": counts["instructions"]
                    / rpcs
                    / 1e3
                    if "instructions" in counts
                    else None,
                    "server_freq_vs_tsc": counts["cycles"] / counts["ref-cycles"]
                    if counts.get("ref-cycles")
                    else None,
                    "server_core_idle_percent": 100 * (core[3] + core[4]) / core_total,
                    "server_core_softirq_percent": 100 * core[6] / core_total,
                    "server_syscalls_per_rpc": (end["syscalls"] - begin["syscalls"])
                    / wall
                    / qps,
                }
            )
        data_check = {}
        if seeded:
            verify_rows = (
                verify_range50_rows
                if args.workload == "range50_update2"
                else verify_get2_rows
            )
            data_check = verify_rows(args.redis, seeded)
            if args.workload == "range50_update2":
                increments = sum(r["version_increments"] for r in data_check.values())
                completed = sum(r["completed_including_warmup"] for r in results)
                assert increments == completed * 2, (increments, completed)
        result = {
            "data_check": data_check,
            "completed_including_warmup": sum(
                r["completed_including_warmup"] for r in results
            ),
            "config": {
                k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()
            },
            "rounds": rounds,
            "verified_cpu_affinity": affinity,
            # 有连接的进程各分到多少个（管理进程、multiprocessing 的辅助进程手上是 0，不列）
            "worker_connections": sorted(
                (n for n in (connections or {}).values() if n), reverse=True
            ),
            "cpu_environment_start": environment_start,
            "cpu_environment_end": cpu_environment(args.server_cpus[0]),
            "median_qps": statistics.median(r["qps"] for r in rounds),
            "medians": {
                key: statistics.median(r[key] for r in rounds)
                for key in rounds[0]
                if all(isinstance(r[key], (int, float)) for r in rounds)
            },
            "server_cpu_percent": 100
            * (cpu_after - cpu_before)
            / (windows[-1][1] - first),
            "client_cpu_seconds": [r["cpu_seconds"] for r in results],
        }
        (args.output / "result.json").write_text(json.dumps(result, indent=2))
        print(json.dumps(result, indent=2), flush=True)
    finally:
        for perf in perfs:
            if perf.poll() is None:
                perf.terminate()
            perf.wait(timeout=10)
        for child in children:
            if child.poll() is None:
                child.terminate()
            child.wait(timeout=10)
        if server is not None and server.poll() is None:
            # py-spy needs SIGINT to finish its profile; signal its whole group.
            os.killpg(server.pid, signal.SIGINT)
            try:
                server.wait(timeout=20)
            except subprocess.TimeoutExpired:
                os.killpg(server.pid, signal.SIGKILL)
                server.wait()
        for log in logs:
            log.close()


if __name__ == "__main__":
    arguments = parse_args()
    if arguments.client >= 0:
        asyncio.run(client(arguments))
    else:
        main(arguments)
