"""
拆解"在 Lua 里发一条通知"在 Redis 主线程上的成本，结论见 redis_publish_cost_result.md。

每次 EVALSHA 在 Lua 里循环 N 次同一种操作，减去只拼 key、不调用任何命令的 noop 循环，
得到每次操作的纯成本：
  exists   : redis.call 一个最轻的读命令，即 redis.call 本身的固定开销（对照组）
  publish  : redis.call("PUBLISH", 行key, 1字节消息)
  spublish : redis.call("SPUBLISH", ...)
  hset     : redis.call("HSET", key, "name", "XYZ")，分别在 keyspace 通知 关 / 开(Kh) /
             开且实例上有一个无关订阅者 三种状态下测，差值就是一次 keyspace 通知的成本

指标：
  主线程 us/次 : INFO 的 used_cpu_*_main_thread，跨平台，但受 CPU 频率影响
  指令/周期 每次 : 给了 --perf-pid 时用 `perf stat -p` 数 redis 进程的用户态指令/周期，
                   与频率无关，同一版本 redis 多轮结果逐位一致（仅 Linux，需装 perf）

测试 key / 频道前缀为 hetu_probe:，结束后删除；notify-keyspace-events 结束后恢复原值。
请对一个没有业务流量的 Redis 跑。挂上副本再跑一次，可以看出 PUBLISH 的复制开销。

用法:
  uv run python redis_publish_cost.py --url redis://127.0.0.1:6379/0
  # Linux 下与频率无关的指令数（redis 以当前用户运行，或 perf_event_paranoid 允许）：
  uv run python redis_publish_cost.py --perf-pid $(pgrep -f "redis-server.*6379")
  # 混合架构 CPU（大小核）上请把 redis 绑到同一类核，否则指令在两类 PMU 间来回切
"""

import argparse
import os
import signal
import subprocess
import tempfile
import time

from redis_commit_cost import cmd, connect, info, main_thread_cpu, resp

LUA = r"""
local op = ARGV[1]
local n = tonumber(ARGV[2])
local base = tonumber(ARGV[3])
local msg = ARGV[4]
local call = redis.call
local prefix = "hetu_probe:IntTable:{CLU0}:id:"
if op == "noop" then
    local x
    for i = 1, n do x = prefix .. (base + i) end
elseif op == "exists" then
    for i = 1, n do call("EXISTS", prefix .. (base + i)) end
elseif op == "publish" then
    for i = 1, n do call("PUBLISH", prefix .. (base + i), msg) end
elseif op == "spublish" then
    for i = 1, n do call("SPUBLISH", prefix .. (base + i), msg) end
elseif op == "hset" then
    for i = 1, n do call("HSET", prefix .. (base + i), "name", "XYZ") end
end
return 1
"""
KEY_PREFIX = "hetu_probe:IntTable:{CLU0}:id:"
N = 200  # 每次 EVALSHA 循环的操作数
N_KEYS = 20000
PIPELINE = 100

# (操作, notify-keyspace-events, 实例上是否挂一个无关订阅者)
VARIANTS = [
    ("noop", "", False),
    ("exists", "", False),
    ("publish", "", False),
    ("publish", "", True),
    ("spublish", "", False),
    ("hset", "", False),
    ("hset", "Kh", False),
    ("hset", "Kh", True),
]


def run_calls(sock, sha: str, op: str, calls: int) -> None:
    batch = []
    for c in range(calls):
        base = (c * N) % N_KEYS
        batch.append(resp("EVALSHA", sha, "0", op, N, base, "\x02"))
        if len(batch) == PIPELINE or c == calls - 1:
            sock.sendall(b"".join(batch))
            expect = b":1\r\n" * len(batch)
            got = b""
            while len(got) < len(expect):
                chunk = sock.recv(65536)
                if not chunk:
                    raise RuntimeError("connection closed")
                got += chunk
            if got != expect:
                raise RuntimeError(got[:200])
            batch = []


class Perf:
    """perf stat -p 的包装，返回还原后的原始计数（未跑满全程的计数器 perf 会按比例放大）"""

    def __init__(self, pid: int):
        fd, self.out = tempfile.mkstemp(suffix=".csv")
        os.close(fd)
        self.proc = subprocess.Popen(
            [
                "perf",
                "stat",
                "-x,",
                "-e",
                "instructions:u,cycles:u",
                "-o",
                self.out,
                "-p",
                str(pid),
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        time.sleep(0.3)

    def stop(self) -> tuple[float, float]:
        self.proc.send_signal(signal.SIGINT)
        self.proc.wait()
        inst = cyc = 0.0
        with open(self.out) as f:
            lines = f.readlines()
        os.unlink(self.out)
        for line in lines:
            parts = line.strip().split(",")
            if len(parts) < 3 or not parts[0] or parts[0].startswith("<"):
                continue
            pct = float(parts[4]) if len(parts) > 4 and parts[4] else 100.0
            raw = float(parts[0]) * pct / 100.0
            if "/instructions/" in parts[2] or parts[2].startswith("instructions"):
                inst += raw
            elif "/cycles/" in parts[2] or parts[2].startswith("cycles"):
                cyc += raw
        return inst, cyc


def measure(sock, sha, op, calls, perf_pid) -> tuple[float, float, float]:
    """返回每次操作的 (主线程 us, 指令, 周期)，未开 perf 时后两者为 0"""
    perf = Perf(perf_pid) if perf_pid else None
    cpu0 = main_thread_cpu(info(sock))
    run_calls(sock, sha, op, calls)
    cpu1 = main_thread_cpu(info(sock))
    inst, cyc = perf.stop() if perf else (0.0, 0.0)
    ops = calls * N
    return (cpu1 - cpu0) / ops * 1e6, inst / ops, cyc / ops


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    ap.add_argument("--url", default="redis://127.0.0.1:6379/0")
    ap.add_argument("--perf-pid", type=int, default=0, help="redis-server 的 pid")
    ap.add_argument("--calls", type=int, default=10000, help="每组 EVALSHA 次数")
    ap.add_argument("--rounds", type=int, default=3)
    args = ap.parse_args()

    s = connect(args.url)
    sha = cmd(s, "SCRIPT", "LOAD", LUA).split(b"\r\n")[1].decode()
    orig_ks = (
        cmd(s, "CONFIG", "GET", "notify-keyspace-events").split(b"\r\n")[-2].decode()
    )
    sub = None
    res: dict[tuple, list[tuple[float, float, float]]] = {}
    try:
        # 预热：先把 N_KEYS 个 hash 建好，之后的 hset 都是覆盖写
        cmd(s, "CONFIG", "SET", "notify-keyspace-events", "")
        run_calls(s, sha, "hset", N_KEYS // N)
        for _ in range(args.rounds):
            for op, ks, with_sub in VARIANTS:
                cmd(s, "CONFIG", "SET", "notify-keyspace-events", ks)
                if with_sub and sub is None:
                    sub = connect(args.url)
                    sub.sendall(resp("SUBSCRIBE", "hetu_probe:unrelated"))
                    sub.recv(65536)
                elif not with_sub and sub is not None:
                    sub.close()
                    sub = None
                    time.sleep(0.2)
                res.setdefault((op, ks, with_sub), []).append(
                    measure(s, sha, op, args.calls, args.perf_pid)
                )
    finally:
        if sub is not None:
            sub.close()
        cmd(s, "CONFIG", "SET", "notify-keyspace-events", orig_ks)
        keys = [KEY_PREFIX + str(i) for i in range(1, N_KEYS + 1)]
        for j in range(0, len(keys), 1000):
            cmd(s, "DEL", *keys[j : j + 1000])

    noop = res[("noop", "", False)]
    base = [sum(v[k] for v in noop) / len(noop) for k in range(3)]
    print(
        f"每次操作的纯成本，已减去 noop（Lua 循环 + 拼 key：{base[0]:.3f} us"
        + (f"，{base[1]:,.0f} 指令" if args.perf_pid else "")
        + f"），{args.rounds} 轮平均\n"
    )
    print("| 操作 | keyspace | 实例上有无关订阅者 | 主线程 us/次 | 指令/次 | 周期/次 |")
    print("|:---|:---|:---|---:|---:|---:|")
    for (op, ks, with_sub), vals in res.items():
        if op == "noop":
            continue
        avg = [sum(v[k] for v in vals) / len(vals) - base[k] for k in range(3)]
        perf_cols = f"{avg[1]:,.0f} | {avg[2]:,.0f}" if args.perf_pid else "- | -"
        print(
            f"| {op} | {ks or '关'} | {'有' if with_sub else '无'} | "
            f"{avg[0]:.3f} | {perf_cols} |"
        )
    print(f"\n已恢复 notify-keyspace-events={orig_ks!r}，已删除 {KEY_PREFIX}* 测试 key")


if __name__ == "__main__":
    main()
