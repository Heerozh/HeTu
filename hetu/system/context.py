"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Callable

from ..endpoint import Context


if TYPE_CHECKING:
    from ..data import BaseComponent
    from ..data.backend import SessionRepository


@dataclass
class SystemContext(Context):
    # 事务变量
    # 当前事务冲突重试次数
    race_count: int = 0
    # 当前事务的repo实例
    repo: dict[type[BaseComponent], SessionRepository] = field(default_factory=dict)
    # 继承的父事务函数
    depend: dict[str, Callable] = field(default_factory=dict)

    async def session_commit(self) -> None:
        """
        提前显式结束事务，提交所有写入操作。如果遇到事务冲突，会抛出异常，因此后续的代码行不会执行。
        注意：调用完 `session_commit`，`ctx` 将不再能够读写 `repo` 。且后续不再属于事务，
             也就是说遇到宕机/crash可能导致整个函数执行不完全。

        Returns
        -------
        如果有任何其他失败，抛出以下异常：redis.exceptions，RaceCondition。
        异常一般无需特别处理，系统的默认处理方式为：遇到RaceCondition异常，上游系统会自动重试。
        其他任何异常会记录日志并断开客户端连接。
        """
        first_repo = next(iter(self.repo.values()), None)
        self.repo = {}
        if first_repo is not None:
            await first_repo.session.commit()

    async def session_discard(self) -> None:
        """
        提前显式结束事务，放弃所有写入操作。
        注意：调用完 `session_discard`，`ctx` 将不再能够读写 `repo` 。且后续不再属于事务，
             也就是说遇到宕机/crash可能导致整个函数执行不完全。
        """
        first_repo = next(iter(self.repo.values()), None)
        self.repo = {}
        if first_repo is not None:
            first_repo.session.discard()
