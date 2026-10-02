"""
设计约束守护：**能不读 master 的就不读 master**。

普通读一律走 `Backend.master_or_servant`（加权随机，把负载摊到副本上）或 `backend.servant`；
直接指定 master 的读是例外，必须在下面的允许清单里写明理由。本测试用源码扫描守住"新增一处
直接读 master 要先想清楚"这道门槛；具体路径上真实发生了几次 master 读，由
`test_master_read_budget.py` 在运行时断言。

`Session.only_master`（headless 默认开）经 `Session.master_or_servant` 属性切到 master，
不是写死的 master 读，扫描看不到它；它是调用方显式要求的强一致读。

新加了直接读 master 的代码而这里挂了，先问自己：
- 这个读能接受副本滞后吗？能就改用 `master_or_servant` / `servant`；
- 不能（正确性依赖最新值）就把它加进 ALLOWED 并写清理由，让下一个人能复核。
"""

import importlib.util
import re
from pathlib import Path

HETU_ROOT = Path(__file__).resolve().parent.parent / "hetu"
AUDIT_PLUGIN = Path(__file__).resolve().parent.parent / "tools" / "master_read_audit.py"

# 读方法（写方法不在本约束内：写本来就只能去 master）
READ_METHODS = (
    "get",
    "get_many",
    "get_many_array_",
    "range",
    "range_read_",
    "range_index_",
)
# `xxx.master.get(`、`self._master.range(`、`backend.master.get_many(`……
PATTERN = re.compile(
    r"(?:\.|\b)_?master\s*\.\s*(" + "|".join(READ_METHODS) + r")\s*\(",
)

# {(相对路径, 方法): 理由}。key 不含行号，挪动代码不会误报
ALLOWED: dict[tuple[str, str], str] = {
    (
        "data/backend/snowflake_timestamp.py",
        "get",
    ): "开服读时钟高水位：读到滞后值会把 ID 发重，每进程只读一两次",
    (
        "endpoint/connection.py",
        "get",
    ): (
        "kicked()：顶号核查不能用滞后数据，只在收到顶号通知时、已登录连接拆连接时（决定断线 "
        "System 以谁的身份跑）才读；RPC 热路径用的是 servant_get"
    ),
}


def _hits() -> list[tuple[str, str, int, str]]:
    found = []
    for path in sorted(HETU_ROOT.rglob("*.py")):
        rel = path.relative_to(HETU_ROOT).as_posix()
        for lineno, line in enumerate(
            path.read_text(encoding="utf-8").splitlines(), start=1
        ):
            code = line.split("#", 1)[0]
            match = PATTERN.search(code)
            if match:
                found.append((rel, match.group(1), lineno, line.strip()))
    return found


def test_no_unlisted_master_reads():
    """hetu/ 下不允许出现清单外的、直接指定 master 的读"""
    unlisted = [
        (rel, meth, lineno, text)
        for rel, meth, lineno, text in _hits()
        if (rel, meth) not in ALLOWED
    ]
    assert not unlisted, "发现未登记的 master 读（详见本文件顶部说明）：\n" + "\n".join(
        f"  {rel}:{lineno}  {text}" for rel, meth, lineno, text in unlisted
    )


def test_allowlist_has_no_stale_entries():
    """清单里的条目都还存在：代码改掉之后要顺手把豁免删掉，别让清单越滚越大"""
    live = {(rel, meth) for rel, meth, _, _ in _hits()}
    stale = sorted(set(ALLOWED) - live)
    assert not stale, f"ALLOWED 里这些条目对应的代码已经没了，请删除：{stale}"


def test_audit_plugin_wraps_every_backend_read_method(monkeypatch):
    """
    摸底用的 tools/master_read_audit 要包住每个具体后端真正执行的读方法。读方法定义在中间
    基类（RedisModelClient）或具体后端上，只包 BackendClient 的直接子类会漏掉它们，审计就
    永远报 0 次 master 读
    """
    from hetu.data.backend import Backend
    from hetu.data.backend.base import BackendClient
    from hetu.data.backend.redis import RedisBackendClient
    from hetu.data.backend.sqlite import SQLiteBackendClient

    spec = importlib.util.spec_from_file_location("master_read_audit", AUDIT_PLUGIN)
    assert spec and spec.loader
    audit = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(audit)

    # pytest_configure 会改全局的类：先记下原样（包括类上本来没有的），用例结束时还原
    monkeypatch.setattr(
        Backend, "master_or_servant", vars(Backend)["master_or_servant"]
    )
    tree, pending = [], [BackendClient]
    while pending:
        cls = pending.pop()
        tree.append(cls)
        pending.extend(cls.__subclasses__())
    saved = [
        (cls, name, vars(cls).get(name)) for cls in tree for name in audit.READ_METHODS
    ]

    try:
        audit.pytest_configure(None)
        missed = [
            f"{cls.__name__}.{name}"
            for cls in (RedisBackendClient, SQLiteBackendClient)
            for name in audit.READ_METHODS
            if not getattr(getattr(cls, name), "_audited", False)
        ]
    finally:
        for cls, name, orig in saved:
            if orig is not None:
                setattr(cls, name, orig)
            elif name in vars(cls):
                delattr(cls, name)
    assert not missed, f"这些读方法没被审计包住：{missed}"
