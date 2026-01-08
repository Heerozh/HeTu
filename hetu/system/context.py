"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

from dataclasses import dataclass, field
from types import FunctionType

import numpy as np

from ..endpoint import Context
from ..data.backend import SessionSelect

from ..data import BaseComponent


@dataclass
class SystemContext(Context):
    # 事务变量
    # 当前事务冲突重试次数
    retry_count: int = 0
    # 当前事务的Select实例
    select: dict[type[BaseComponent], SessionSelect] = field(default_factory=dict)
    # 继承的父事务函数
    depend: dict[str, FunctionType] = field(default_factory=dict)
    # 限制变量
    max_row_sub: int = 0  # 行订阅限制
    max_index_sub: int = 0  # 索引订阅限制

    def configure(
        self, client_limits, server_limits, max_row_sub=1000, max_index_sub=50
    ):
        """配置System选项"""
        super().configure(client_limits, server_limits)
        self.max_row_sub = max_row_sub
        self.max_index_sub = max_index_sub

    def rls_check(
        self,
        component: type[BaseComponent],
        row: np.record | np.ndarray | np.recarray | dict,
    ) -> bool:
        """检查当前用户对某个component的权限"""
        # 非rls权限通过所有rls检查。要求调用此方法前，首先要由tls(表级权限)检查通过
        if not component.is_rls():
            return True
        # admin组拥有所有权限
        if self.is_admin():
            return True
        assert component.rls_compare_
        rls_func, comp_attr, ctx_attr = component.rls_compare_
        b = getattr(self, ctx_attr, np.nan)
        a = type(b)(
            row.get(comp_attr, np.nan)
            if type(row) is dict
            else getattr(row, "owner", np.nan)
        )
        return bool(rls_func(a, b))

    async def end_transaction(self, discard: bool = False):
        """
        提前显式结束事务，提交所有写入操作。如果遇到事务冲突，会抛出异常，因此后续的代码行不会执行。
        主要用于获取插入后的row id。
        注意：调用完 `end_transaction`，`ctx` 将不再能够读写 `components` 。且后续不再属于事务，
             也就是说遇到宕机/crash可能导致整个函数执行不完全。

        Parameters
        ----------
        discard: bool
            默认为False，设为True为放弃当前事务，不提交

        Returns
        -------
        insert_ids: None | list[int]
            如果事务执行成功，返回所有insert的row id列表，按调用顺序。如果没有insert操作，返回空List
            如果discard或者已经提交过，返回None
            如果有任何其他失败，抛出以下异常：redis.exceptions，RaceCondition。
            异常一般无需特别处理，系统的默认处理方式为：遇到RaceCondition异常，上游系统会自动重试。其他任何异常会
            记录日志并断开客户端连接。

        Examples
        --------
        获得插入后的row id：
        >>> @define_system(components=(Item, ))
        ... async def create_item(ctx):
        ...     ctx[Item].insert(...)
        ...     inserted_ids = await ctx.end_transaction(discard=False)
        ...     ctx.user_data['my_id'] = inserted_ids[0]  # 如果事务冲突，这句不会执行

        """
        comp_trx = next(iter(self.transactions.values()), None)
        if comp_trx is not None:
            self.transactions = {}
            return await comp_trx.attached.end_transaction(discard)
        return None
