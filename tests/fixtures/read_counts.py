"""
统计一次操作里后端客户端的读调用次数，master 与 servant 分开计。

用于 master 读预算断言：本项目的设计约束是"能不读 master 的就不读 master"，普通事务读走
`Backend.master_or_servant` 加权随机，只有少数明确的例外才直接指定 master。预算测试请用
`master_weight: 0` 的配置建 backend，让加权随机永远选不中 master，这样 master 上剩下的读
一定是代码显式指定的，计数才是确定的（见 test_master_read_budget.py）。
"""

from collections.abc import Callable
from contextlib import ExitStack
from unittest.mock import patch

from hetu.data.backend import Backend

READ_METHODS = ("get", "get_many")
RANGE_METHODS = ("range", "range_read_")  # 事务里的 range 走 range_read_


def count_reads(
    stack: ExitStack,
    backend: Backend,
    *,
    include_range: bool = False,
) -> Callable[[], dict[str, int]]:
    """
    包住 backend 的 master 与各 servant 的读方法，返回 counts()：调用它得到当前计数，
    key 为 `master` / `servant`。include_range=True 时把索引读（`range`）也算进去。
    """
    methods = READ_METHODS + (RANGE_METHODS if include_range else ())
    mocks: list[tuple[bool, object]] = []
    clients = [(False, backend.master)] + [
        (True, servant)
        for servant in backend._servants  # type: ignore[reportPrivateUsage]
    ]
    for is_servant, client in clients:
        for meth in methods:
            mock = stack.enter_context(
                patch.object(client, meth, wraps=getattr(client, meth))
            )
            mocks.append((is_servant, mock))

    def counts() -> dict[str, int]:
        c = {"master": 0, "servant": 0}
        for is_servant, mock in mocks:
            c["servant" if is_servant else "master"] += mock.call_count  # type: ignore[attr-defined]
        return c

    return counts
