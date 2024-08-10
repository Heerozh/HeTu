"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: MIT 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""
from dataclasses import dataclass
from typing import Callable

from ..data import BaseComponent
from ..data.backend import ComponentTransaction


@dataclass
class Context:
    # 全局变量
    caller: int | None  # 调用方的user id，如果你执行过`elevate()`，此值为传入的`user_id`
    connection_id: int  # 调用方的connection id
    group: str | None  # 所属组名，目前只用于判断是否admin
    user_data: dict  # 当前连接的用户数据，可自由设置，在所有System间共享
    # 事务变量
    timestamp: int  # 调用时间戳
    retry_count: int  # 当前事务冲突重试次数
    transactions: dict[type[BaseComponent], ComponentTransaction]  # 当前事务的Table实例
    inherited: dict[str, callable]  # 继承的父事务函数

    def __str__(self):
        return f"conn: {self.connection_id}, caller: {self.caller}"

    def __getitem__(self, item: type[BaseComponent] | str) -> ComponentTransaction | Callable:
        if type(item) is str:
            return self.inherited[item]
        else:
            return self.transactions[item]

    async def end_transaction(self, discard: bool = False):
        comp_trx = next(iter(self.transactions.values()), None)
        if comp_trx is not None:
            self.transactions = {}
            return await comp_trx.attached.end_transaction(discard)
