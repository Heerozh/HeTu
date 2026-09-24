"""
量 Redis master 执行 HeTu commit 的成本，以及压测时 Redis 主线程是否已饱和。

两个子命令：

  replay  用 pipeline 批量回放与 HeTu 同构的 commit_v2.lua 调用（摊薄网络系统调用，让主线程
          被 EVALSHA 本身占满），比较不同版本 commit payload 的每次 commit 主线程 CPU：
            old      : VER + HSET                                   （f9b6493，无 PUBLISH）
            main     : VER + HSET + PUBLISH 表频道                   （PR #136 起每次 commit 都发）
            head     : VER + HSET + PUBLISH 行频道 + PUBLISH 表频道   （perf/row-cache）
            optin    : VER + HSET，没声明 table_sub / point_sub 的组件（perf/publish-opt-in）
            optin_val: optin + 每行一条扁平的值频道 PUBLISH（point_sub 索引有行"进入"时）
          rows=1 对应 get_then_update 的一次 commit，rows=2 对应 get2_update2。
          payload 按 perf/publish-opt-in 之后的格式 [checks, pushes, deleted, table_pubs,
          value_chans]；old/main/head 的通知放在 table_pubs 里，与 Lua 的表频道调用点同一格式。
          测试 key 前缀为 hetu_probe:，结束后删除；notify-keyspace-events 结束后恢复原值。
          请对一个没有业务流量的 Redis 跑（会把主线程压满几十秒）。

  watch   每秒采样一次：主线程 CPU busy、每秒命令数、evalsha / publish 每秒次数与 usec_per_call。
          压测（ya）时另开终端运行，busy 接近 1.0 说明 Redis master 已是瓶颈。

用法:
  uv run python redis_commit_cost.py replay --url redis://127.0.0.1:6379/0
  uv run python redis_commit_cost.py watch  --url redis://127.0.0.1:6379/0
"""

import argparse
import multiprocessing as mp
import socket
import time
from pathlib import Path
from urllib.parse import unquote, urlparse

import msgpack

LUA_PATH = (
    Path(__file__).resolve().parent.parent / "hetu/data/backend/redis/commit_v2.lua"
)
N_KEYS = 30000
BATCH = 200
TABLES = ["hetu_probe:IntTable:{CLU0}", "hetu_probe:StrTable:{CLU0}"]


def resp(*args) -> bytes:
    out = [b"*%d\r\n" % len(args)]
    for a in args:
        b = a if isinstance(a, bytes) else str(a).encode()
        out.append(b"$%d\r\n%s\r\n" % (len(b), b))
    return b"".join(out)


def read_reply(sock: socket.socket) -> bytes:
    buf = b""
    while b"\r\n" not in buf:
        buf += sock.recv(65536)
    if buf[:1] == b"$":
        n = int(buf[1 : buf.index(b"\r\n")])
        need = buf.index(b"\r\n") + 2 + max(n, 0) + (2 if n >= 0 else 0)
        while len(buf) < need:
            buf += sock.recv(65536)
    if buf[:1] == b"-":
        raise RuntimeError(buf.decode(errors="replace").strip())
    return buf


def connect(url: str) -> socket.socket:
    u = urlparse(url)
    s = socket.create_connection((u.hostname or "127.0.0.1", u.port or 6379))
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    if u.password:
        if u.username:
            s.sendall(resp("AUTH", unquote(u.username), unquote(u.password)))
        else:
            s.sendall(resp("AUTH", unquote(u.password)))
        read_reply(s)
    db = (u.path or "/0").strip("/") or "0"
    s.sendall(resp("SELECT", db))
    read_reply(s)
    return s


def cmd(sock: socket.socket, *args) -> bytes:
    sock.sendall(resp(*args))
    return read_reply(sock)


def info(sock: socket.socket) -> dict:
    raw = cmd(sock, "INFO", "all").decode(errors="replace")
    d = {}
    for line in raw.splitlines():
        if ":" in line and not line.startswith("#"):
            k, v = line.split(":", 1)
            d[k] = v
    return d


def main_thread_cpu(d: dict) -> float:
    return float(d["used_cpu_user_main_thread"]) + float(d["used_cpu_sys_main_thread"])


def cmdstat(d: dict, name: str) -> tuple[int, float]:
    v = d.get(f"cmdstat_{name}")
    if not v:
        return 0, 0.0
    kv = dict(x.split("=") for x in v.split(","))
    return int(kv["calls"]), float(kv["usec"])


# ------------------------------------------------------------------ replay


def rid(i: int) -> int:
    return 7300000000000000000 + i


def row_key(t: int, i: int) -> str:
    return f"{TABLES[t]}:id:{rid(i)}"


def payload(variant: str, rows: int, i: int) -> bytes:
    pack = lambda o: msgpack.packb(o, use_bin_type=False)  # 与 HeTu 的 Packer 一致
    checks, pushes, pubs_row, pubs_tbl, value_chans = [], [], [], [], []
    for t in range(rows):
        key = row_key(t, i)
        checks.append(["VER", key, "2"])
        pushes.append(["HSET", key, "_version", "2", "name", "XYZ"])
        pubs_row.append([key, pack(3)])
        pubs_tbl.append([f"{TABLES[t]}:table", pack([str(rid(i))])])
        # 值频道名与 HeTu 同形：{prefix}:index:{字段}:{16 位 hex 的 sortable token}
        value_chans.append(f"{TABLES[t]}:index:owner:{(1 << 63) + i:016x}")
    pubs = {"main": pubs_tbl, "head": pubs_row + pubs_tbl}.get(variant, [])
    if variant != "optin_val":
        value_chans = []
    return pack([checks, pushes, {}, pubs, value_chans])


def replay_worker(args) -> int:
    url, variant, rows, sha, seconds, seed = args
    s = connect(url)
    batches = []
    for b in range(64):
        cmds = []
        for j in range(BATCH):
            i = (seed * 7919 + b * BATCH + j) % N_KEYS
            cmds.append(
                resp("EVALSHA", sha, "1", row_key(0, i), payload(variant, rows, i))
            )
        batches.append(b"".join(cmds))
    expect = b"$9\r\ncommitted\r\n" * BATCH
    done, k = 0, 0
    end = time.time() + seconds
    while time.time() < end:
        s.sendall(batches[k % len(batches)])
        k += 1
        got = b""
        while len(got) < len(expect):
            chunk = s.recv(1 << 20)
            if not chunk:
                raise RuntimeError("connection closed")
            got += chunk
        if got != expect:
            raise RuntimeError(got[:200])
        done += BATCH
    s.close()
    return done


def replay(url: str, seconds: float, procs: int) -> None:
    s = connect(url)
    orig_ks = (
        cmd(s, "CONFIG", "GET", "notify-keyspace-events").split(b"\r\n")[-2].decode()
    )
    for t in range(2):
        pipe = []
        for i in range(N_KEYS):
            pipe.append(
                resp(
                    "HSET",
                    row_key(t, i),
                    "_version",
                    "2",
                    "id",
                    str(rid(i)),
                    "name",
                    "ABC",
                )
            )
            if len(pipe) == 1000:
                s.sendall(b"".join(pipe))
                got = b""
                while got.count(b"\r\n") < len(pipe):
                    got += s.recv(65536)
                pipe = []
    sha = cmd(s, "SCRIPT", "LOAD", LUA_PATH.read_bytes()).split(b"\r\n")[1].decode()
    combos = [
        ("old", "Kghz"),
        ("main", "Kghz"),
        ("optin", "Kghz"),
        ("optin_val", "Kghz"),
        ("head", "Kz"),
        ("head", "Kghz"),
    ]
    print(f"原 notify-keyspace-events = {orig_ks!r}\n")
    print(
        "| rows | payload | keyspace | commits/s | 主线程 us/commit | evalsha usec_per_call |"
    )
    print("|---:|:---|:---|---:|---:|---:|")
    try:
        for rows in (1, 2):
            for variant, ks in combos:
                cmd(s, "CONFIG", "SET", "notify-keyspace-events", ks)
                c0, u0 = cmdstat(info(s), "evalsha")
                i0 = info(s)
                t0 = time.time()
                with mp.Pool(procs) as pool:
                    counts = pool.map(
                        replay_worker,
                        [(url, variant, rows, sha, seconds, p) for p in range(procs)],
                    )
                t1 = time.time()
                i1 = info(s)
                c1, u1 = cmdstat(i1, "evalsha")
                total = sum(counts)
                cpu = main_thread_cpu(i1) - main_thread_cpu(i0)
                print(
                    f"| {rows} | {variant} | {ks} | {total / (t1 - t0):,.0f} | "
                    f"{cpu / total * 1e6:.3f} | {(u1 - u0) / max(c1 - c0, 1):.3f} |",
                    flush=True,
                )
    finally:
        cmd(s, "CONFIG", "SET", "notify-keyspace-events", orig_ks)
        for t in range(2):
            keys = [row_key(t, i) for i in range(N_KEYS)]
            for j in range(0, len(keys), 1000):
                cmd(s, "DEL", *keys[j : j + 1000])
        print(
            f"\n已恢复 notify-keyspace-events={orig_ks!r}，已删除 hetu_probe: 测试 key"
        )


# ------------------------------------------------------------------- watch


def watch(url: str, interval: float) -> None:
    s = connect(url)
    prev = info(s)
    t_prev = time.time()
    print(
        "time      main_busy   ops/s   evalsha/s  evalsha_us  publish/s  hgetall/s  zrange/s"
    )
    while True:
        time.sleep(interval)
        cur = info(s)
        now = time.time()
        dt = now - t_prev
        if int(cur["total_commands_processed"]) < int(prev["total_commands_processed"]):
            # 期间有人 CONFIG RESETSTAT，计数器归零，这一格没法算
            print(f"{time.strftime('%H:%M:%S')}  (stats reset)", flush=True)
            prev, t_prev = cur, now
            continue
        busy = (main_thread_cpu(cur) - main_thread_cpu(prev)) / dt
        ops = (
            int(cur["total_commands_processed"]) - int(prev["total_commands_processed"])
        ) / dt
        rates = {}
        for name in ("evalsha", "publish", "hgetall", "zrange"):
            c1, u1 = cmdstat(cur, name)
            c0, u0 = cmdstat(prev, name)
            rates[name] = ((c1 - c0) / dt, (u1 - u0) / max(c1 - c0, 1))
        print(
            f"{time.strftime('%H:%M:%S')}  {busy:9.2f}  {ops:8.0f}  {rates['evalsha'][0]:9.0f}"
            f"  {rates['evalsha'][1]:10.2f}  {rates['publish'][0]:9.0f}  {rates['hgetall'][0]:9.0f}"
            f"  {rates['zrange'][0]:8.0f}",
            flush=True,
        )
        prev, t_prev = cur, now


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    ap.add_argument("mode", choices=["replay", "watch"])
    ap.add_argument("--url", default="redis://127.0.0.1:6379/0")
    ap.add_argument("--seconds", type=float, default=6, help="replay 每组的时长（秒）")
    ap.add_argument("--procs", type=int, default=4, help="replay 并发连接（进程）数")
    ap.add_argument("--interval", type=float, default=1, help="watch 采样间隔（秒）")
    args = ap.parse_args()
    if args.mode == "replay":
        replay(args.url, args.seconds, args.procs)
    else:
        watch(args.url, args.interval)


if __name__ == "__main__":
    main()
