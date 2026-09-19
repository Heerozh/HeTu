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
    """`import hetu` 不得加载 sanic，也不得加载 redis / sqlalchemy（后端按 alias 懒加载）。"""
    out = _run_py(
        """
        import json, sys
        import hetu
        print(json.dumps({m: (m in sys.modules) for m in ("sanic", "redis", "sqlalchemy")}))
        """
    )
    assert out == {"sanic": False, "redis": False, "sqlalchemy": False}, out


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
            "sqlalchemy": "sqlalchemy" in sys.modules,
        }))
        """,
        redis_url,
    )
    assert out == {
        "redis_pkg_before": False,
        "redis_pkg_after": True,
        "sqlalchemy": False,
    }
