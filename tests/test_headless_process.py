"""
headless client 的进程级验收：必须在干净的子进程里跑，因为 pytest 主进程早已被其它测试
import 过 sanic、初始化过 SnowflakeID，`sys.modules` / 单例状态都不干净。
"""

import json
import os
import subprocess
import sys
import textwrap

from fixtures.backends import use_redis_backend_only


def _run_py(code: str, *args: str) -> dict:
    """在干净子进程里跑一段代码，代码最后一行须 print 一个 json dict。"""
    env = {**os.environ, "PYTHONUTF8": "1"}
    proc = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code), *args],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=180,
        env=env,
    )
    assert proc.returncode == 0, f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    return json.loads(proc.stdout.strip().splitlines()[-1])


def test_import_hetu_is_light():
    """`import hetu` 不得加载 sanic，也不得加载任何后端（redis / sqlite，按 alias 懒加载）。"""
    out = _run_py(
        """
        import json, sys
        import hetu
        mods = ("sanic", "redis", "hetu.data.backend.sqlite")
        print(json.dumps({m: (m in sys.modules) for m in mods}))
        """
    )
    assert out == {
        "sanic": False,
        "redis": False,
        "hetu.data.backend.sqlite": False,
    }, out


@use_redis_backend_only
def test_backend_factory_lazy_loads_builtin_alias(ses_redis_service, backend_name):
    """未显式 import 子包时，`Backend({"type": "redis"})` 也要能按 alias 懒加载并工作。"""
    redis_url, _replica = ses_redis_service
    out = _run_py(
        """
        import asyncio, json, sys
        import hetu
        from hetu.data.backend import Backend
        before = "hetu.data.backend.redis" in sys.modules

        async def main():
            backend = Backend({"type": "redis", "master": sys.argv[1]})
            await backend.close()

        asyncio.run(main())
        print(json.dumps({
            "redis_pkg_before": before,
            "redis_pkg_after": "hetu.data.backend.redis" in sys.modules,
            "sqlite_pkg": "hetu.data.backend.sqlite" in sys.modules,
        }))
        """,
        redis_url,
    )
    assert out == {
        "redis_pkg_before": False,
        "redis_pkg_after": True,
        "sqlite_pkg": False,
    }


@use_redis_backend_only
def test_connect_resources(mod_test_app, mod_tbl_mgr, mod_backend_config, backend_name):
    """验收 6 / R7：干净子进程里 connect 不 import sanic / 别的后端、不初始化
    SnowflakeID、耗时 < 1 s、常驻内存增量 < 10 MB（相对已 import redis 后端的基线）。"""
    out = _run_py(
        """
        import asyncio, json, sys, time
        import hetu
        import hetu.headless
        import hetu.data.backend.redis  # 内存基线：含 redis 后端（懒加载后 import hetu 本身不含）
        try:
            import psutil
        except ImportError:
            psutil = None
        rss = (lambda: psutil.Process().memory_info().rss) if psutil else (lambda: 0)
        cfg = json.loads(sys.argv[1])

        async def main():
            before = rss()
            t = time.perf_counter()
            client = await hetu.headless.connect(
                cfg, "server1", ["HeadlessCommand", "HeadlessSim"]
            )
            elapsed = time.perf_counter() - t
            # 真用一下：不发号地写一行再读回
            Sim = client.table("HeadlessSim").comp_cls
            async with client.session("HeadlessSim") as s, s[Sim].upsert(id=-9001) as row:
                row.system_id = 9001
                row.owner_host = "subprocess"
            row = await client.backend.master.get(client.table("HeadlessSim"), -9001)
            after = rss()
            await client.close()
            from hetu.common.snowflake_id import SnowflakeID
            print(json.dumps({
                "sanic": "sanic" in sys.modules,
                "sqlite_pkg": "hetu.data.backend.sqlite" in sys.modules,
                "connect_seconds": elapsed,
                "rss_delta_mb": (after - before) / 1e6,
                "rss_total_mb": after / 1e6,
                "psutil": psutil is not None,
                "worker_id": SnowflakeID().worker_id,
                "host": str(row.owner_host),
            }))

        asyncio.run(main())
        """,
        json.dumps(mod_backend_config),
    )
    print(out)
    assert out["sanic"] is False and out["sqlite_pkg"] is False
    assert out["worker_id"] == -1, "headless 不得初始化 SnowflakeID"
    assert out["host"] == "subprocess"
    assert out["connect_seconds"] < 1.0, out
    if out["psutil"]:
        # cluster 模式下 redis-py 每个节点各一套连接池，客户端对象本身就重一档
        limit = 20 if mod_backend_config.get("raw_clustering") else 10
        assert out["rss_delta_mb"] < limit, out
