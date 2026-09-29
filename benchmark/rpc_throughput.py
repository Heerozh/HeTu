"""Measure RPC throughput over real WebSockets with a frozen client.

真实 WebSocket RPC 基准；固定客户端源码，避免把客户端优化算作服务端收益。
Run with --help. Requires an isolated Redis database. Logs/results go to --output.
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
    parser.add_argument("--server-cpu", type=int, default=3)
    parser.add_argument("--client-cpus", default="4,6,8,10")
    parser.add_argument("--profile", action="store_true")
    parser.add_argument("--profile-cpu", type=int, default=1)
    parser.add_argument("--client", type=int, default=-1, help=argparse.SUPPRESS)
    return parser.parse_args()


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

    async def one():
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
            while True:
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
                    if seen % 64 == 0:
                        latencies[i].append((after - before) * 1000)
        finally:
            await fixture.aclose()
            await conn[0].close()

    tasks = [asyncio.create_task(one()) for _ in range(args.connections)]
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
WORKER_NUM: 1
DEBUG: false
ACCESS_LOG: false
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
    cmd = ["taskset", "-c", str(args.server_cpu), *cmd]
    children = []
    logs = []
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
        proc = psutil.Process(server.pid)
        if args.profile:
            proc = next(p for p in proc.children() if "python" in p.name())
        affinity = {
            "server": proc.cpu_affinity(),
            "clients": [psutil.Process(p.pid).cpu_affinity() for p in children],
        }
        assert affinity["server"] == [args.server_cpu], affinity
        for i, actual in enumerate(affinity["clients"]):
            assert actual == [int(client_cpus[i % len(client_cpus)])], affinity
        environment_start = cpu_environment(args.server_cpu)
        cpu_before = None
        cpu_after = None
        round_cpu_start = [None] * args.rounds
        round_cpu_end = [None] * args.rounds
        while any(p.poll() is None for p in children):
            now = time.monotonic()
            for i, (begin, end) in enumerate(windows):
                if round_cpu_start[i] is None and now >= begin:
                    round_cpu_start[i] = (now, sum(proc.cpu_times()[:2]))
                if round_cpu_end[i] is None and now >= end:
                    round_cpu_end[i] = (now, sum(proc.cpu_times()[:2]))
            if cpu_before is None and now >= first:
                cpu_before = sum(proc.cpu_times()[:2])
            if cpu_after is None and now >= windows[-1][1]:
                cpu_after = sum(proc.cpu_times()[:2])
            if now > windows[-1][1] + 30:
                raise TimeoutError("Client completion")
            time.sleep(0.05)
        if any(p.returncode != 0 for p in children):
            raise RuntimeError("Client failed; see client logs")
        if cpu_after is None:
            cpu_after = sum(proc.cpu_times()[:2])
        assert cpu_before is not None
        if round_cpu_end[-1] is None:
            round_cpu_end[-1] = (time.monotonic(), cpu_after)
        results = [
            json.loads((args.output / f"client-{i}.json").read_text())
            for i in range(args.processes)
        ]
        rounds = []
        for i in range(args.rounds):
            samples = sorted(x for r in results for x in r["latencies_ms"][i])
            qps = sum(r["counts"][i] for r in results) / args.seconds
            begin, end = round_cpu_start[i], round_cpu_end[i]
            cpu_fraction = (end[1] - begin[1]) / (end[0] - begin[0])
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
            "cpu_environment_start": environment_start,
            "cpu_environment_end": cpu_environment(args.server_cpu),
            "median_qps": statistics.median(r["qps"] for r in rounds),
            "server_cpu_percent": 100
            * (cpu_after - cpu_before)
            / (windows[-1][1] - first),
            "client_cpu_seconds": [r["cpu_seconds"] for r in results],
        }
        (args.output / "result.json").write_text(json.dumps(result, indent=2))
        print(json.dumps(result, indent=2), flush=True)
    finally:
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
