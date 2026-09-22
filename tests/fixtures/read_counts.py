"""
统计一次操作里后端客户端的读调用次数，master 与 servant 分开计。

用于两件事：
- 行缓存的命中/往返次数断言（`plain` / `authoritative`）；
- master 读预算断言（`master_*`）：本项目的设计约束是"能不读 master 的就不读 master"，
  普通事务读走 `Backend.master_or_servant` 加权随机，只有少数明确的例外才直接指定 master。
  预算测试请用 `master_weight: 0` 的配置建 backend，让加权随机永远选不中 master，这样
  master 上剩下的读一定是代码显式指定的，计数才是确定的（见 test_master_read_budget.py）。

`authoritative` 只包 `get_many_authoritative`：`get_authoritative` 内部就是调它，
两个都包会把一次往返算两次。
"""

from collections.abc import Callable
from contextlib import ExitStack
from unittest.mock import patch

from hetu.data.backend import Backend

PLAIN_METHODS = ("get", "get_many")
RANGE_METHODS = ("range",)
AUTHORITATIVE_METHODS = ("get_many_authoritative",)


def count_reads(
    stack: ExitStack,
    backend: Backend,
    *,
    include_range: bool = False,
    by_role: bool = False,
) -> Callable[[], dict[str, int]]:
    """
    包住 backend 的 master 与各 servant 的读方法，返回 counts()：调用它得到当前计数。

    counts() 默认只有两个 key（行缓存测试用，可以直接和字面量比）：
    - `plain`：所有客户端的 get / get_many 合计
    - `authoritative`：权威读（master 上的 Lua HGETALL）

    by_role=True 时再多三个 key（master 读预算测试用）：`master` / `master_authoritative`
    / `servant`。include_range=True 时把索引读（`range`）也算进 `plain` / `master`。
    """
    plain_methods = PLAIN_METHODS + (RANGE_METHODS if include_range else ())
    mocks: dict[tuple[bool, str], object] = {}
    clients = [(False, backend.master)] + [
        (True, servant)
        for servant in backend._servants  # type: ignore[reportPrivateUsage]
    ]
    for i, (is_servant, client) in enumerate(clients):
        for meth in plain_methods + AUTHORITATIVE_METHODS:
            if not hasattr(client, meth):
                continue
            mocks[(is_servant, f"{i}:{meth}")] = stack.enter_context(
                patch.object(client, meth, wraps=getattr(client, meth))
            )

    def counts() -> dict[str, int]:
        c = {"plain": 0, "authoritative": 0}
        if by_role:
            c |= {"master": 0, "master_authoritative": 0, "servant": 0}
        for (is_servant, key), mock in mocks.items():
            n = mock.call_count  # type: ignore[attr-defined]
            meth = key.split(":", 1)[1]
            if meth in AUTHORITATIVE_METHODS:
                c["authoritative"] += n
                if by_role and not is_servant:
                    c["master_authoritative"] += n
            else:
                c["plain"] += n
                if by_role:
                    c["master" if not is_servant else "servant"] += n
        return c

    return counts
