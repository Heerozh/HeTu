"""
pytest 插件：审计"绕过 master_or_servant 负载均衡、直接落到 master 上的读"。

本项目的设计约束是**能不读 master 的就不读 master**（见 CLAUDE.md）：普通读走
`Backend.master_or_servant` 加权随机摊到副本上，只有少数明确的例外才直接指定 master。
`tests/test_arch_master_reads.py`（源码清单）与 `tests/test_master_read_budget.py`
（几条典型路径的次数预算）平时守着这条线；这个脚本用来**摸底**：跑一遍测试，把实际落到
master 上的读按调用点聚合出来，看有没有计划外的来源。改动读路径之后值得跑一次。

用法（从仓库根目录，`tools` 不是包，靠 PYTHONPATH 让 pytest 找到它）：

    HETU_TEST_BACKENDS=redis PYTHONPATH=tools uv run pytest tests/ -q -p master_read_audit

结果默认写到 master_reads.txt，可用 HETU_AUDIT_OUT 改路径。

原理两步：
1. 把 `Backend.master_or_servant` 钉死在 servant 上（等价 `master_weight: 0`），于是 master
   客户端上还发生的读一定是代码显式指定的，不会和负载均衡撞上的混在一起；
2. 给 `BackendClient` 的读方法打点，只记 `is_servant=False` 的调用，按调用点聚合。

注意：第 1 步会让"写完立刻读"的测试读不到自己刚写的数据（副本还没同步），所以审计这一趟
**会有测试失败，是预期的**——只看报告，别拿这趟的红绿做结论。
"""

import collections
import os
import random
import traceback

READ_METHODS = ("get", "get_many", "range", "range_read_")
COUNTS: collections.Counter = collections.Counter()

# 这些是读路径自己的中转帧，调用点要继续往上找
_INTERNAL = (
    "backend/session.py",
    "backend/repo.py",
)


def _call_site() -> str:
    """取第一个不在读路径内部的帧，连同它上面两层，拼成可读的调用点"""
    frames = [
        f
        for f in traceback.extract_stack()[:-2]
        if ("hetu" in f.filename or "tests" in f.filename)
        and "master_read_audit" not in f.filename
    ]
    picked = []
    for f in reversed(frames):
        rel = "/".join(f.filename.replace("\\", "/").split("/")[-2:])
        picked.append(f"{rel}:{f.lineno} {f.name}")
        if not any(part in rel for part in _INTERNAL):
            break
    return " <- ".join(picked[-3:][::-1])


def pytest_configure(config):
    from hetu.data.backend import Backend
    from hetu.data.backend.base import BackendClient

    def master_or_servant(self):
        return random.choice(self._servants)

    Backend.master_or_servant = property(master_or_servant)

    for cls in BackendClient.__subclasses__() + [BackendClient]:
        for name in READ_METHODS:
            orig = getattr(cls, name, None)
            if orig is None or getattr(orig, "_audited", False):
                continue

            def make(orig=orig, name=name):
                async def wrapper(self, *a, **kw):
                    if not self.is_servant:
                        COUNTS[(name, _call_site())] += 1
                    return await orig(self, *a, **kw)

                wrapper._audited = True
                return wrapper

            setattr(cls, name, make())


def pytest_sessionfinish(session, exitstatus):
    out = os.environ.get("HETU_AUDIT_OUT", "master_reads.txt")
    lines = [f"{n:>6}  {meth:<24} {site}" for (meth, site), n in COUNTS.most_common()]
    total = sum(COUNTS.values())
    with open(out, "w", encoding="utf-8") as f:
        f.write(f"master 上的读调用总数: {total}\n\n" + "\n".join(lines) + "\n")
    print(f"\n[audit] master 读 {total} 次，明细写入 {out}")
