"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""
from dataclasses import dataclass
from typing import Callable

import numpy as np

from ..data import BaseComponent
from ..data.backend import ComponentTransaction


@dataclass
class Context:
    # 通用变量
    caller: int | None  # 调用方的user id，如果你执行过`elevate()`，此值为传入的`user_id`
    connection_id: int  # 调用方的connection id
    address: str | None  # 调用方的ip
    group: str | None  # 所属组名，目前只用于判断是否admin
    user_data: dict  # 当前连接的用户数据，可自由设置，在所有System间共享
    # 事务变量
    timestamp: int  # 调用时间戳
    retry_count: int  # 当前事务冲突重试次数
    transactions: dict[type[BaseComponent], ComponentTransaction]  # 当前事务的Table实例
    inherited: dict[str, callable]  # 继承的父事务函数
    # 让system调用其他system还有个办法，在这里可以做一个list存放一些快速延后调用，让外面的executor负责调用，
    # 但考虑了下作用不大，如果system有写入需求，必定不希望丢失，存放到list很可能就因为断线或服务器宕机，造成调用本身丢失，
    # 如果没有写入需求，自然可以用一般函数通过direct_get来获取数据，不需要做成system
    # queued_calls: list[any]   # 延后调用的队列
    # 限制变量
    idle_timeout: int = 0  # 闲置超时时间
    client_limits: list[list[int]] = ()  # 客户端消息发送限制（次数）
    server_limits: list[list[int]] = ()  # 服务端消息发送限制（次数）
    max_row_sub: int = 0  # 行订阅限制
    max_index_sub: int = 0  # 索引订阅限制

    def __str__(self):
        return f"[{self.connection_id}|{self.address}|{self.caller}]"

    def __getitem__(self,
                    item: type[BaseComponent] | str) -> ComponentTransaction | Callable:
        if type(item) is str:
            return self.inherited[item]
        else:
            return self.transactions[item]

    def is_admin(self):
        return True if self.group and self.group.startswith("admin") else False

    def rls_check(self, component: type[BaseComponent],
                  row: np.record | np.ndarray | np.recarray | dict) -> bool:
        """检查当前用户对某个component的权限"""
        # 非rls权限通过所有rls检查。要求调用此方法前，首先要由tls(表级权限)检查通过
        if not component.is_rls():
            return True
        # admin组拥有所有权限
        if self.is_admin():
            return True
        rls_func, comp_attr, ctx_attr = component.rls_compare_
        b = getattr(self, ctx_attr, np.nan)
        a = type(b)(
            row.get(comp_attr, np.nan)
            if type(row) is dict else getattr(row, 'owner', np.nan)
        )
        return bool(rls_func(a, b))

    def configure(self, idle_timeout, client_limits, server_limits, max_row_sub,
                  max_index_sub):
        """配置连接选项"""
        self.idle_timeout = idle_timeout
        self.client_limits = client_limits
        self.server_limits = server_limits
        self.max_row_sub = max_row_sub
        self.max_index_sub = max_index_sub

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
