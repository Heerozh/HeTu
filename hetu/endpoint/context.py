"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import numpy as np
from dataclasses import dataclass, field
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from ..data.component import BaseComponent


@dataclass
class Context:
    # 通用变量
    # 调用方的user id，如果你执行过`elevate()`，此值为传入的`user_id`
    caller: int | None
    connection_id: int  # 调用方的connection id
    address: str | None  # 调用方的ip
    group: str | None  # 所属组名，目前只用于判断是否admin
    user_data: dict[str, Any]  # 当前连接的用户数据，可自由设置，在所有System间共享
    # 请求变量
    timestamp: float  # 调用时间戳
    # 限制变量
    # 客户端消息发送限制（次数）
    client_limits: list[list[int]] = field(default_factory=list)
    # 服务端消息发送限制（次数）
    server_limits: list[list[int]] = field(default_factory=list)
    max_row_sub: int = 0  # 行订阅限制
    max_index_sub: int = 0  # 索引订阅限制

    def __str__(self):
        return f"[{self.connection_id}|{self.address}|{self.caller}]"

    def is_admin(self):
        return True if self.group and self.group.startswith("admin") else False

    def configure(self, client_limits, server_limits, max_row_sub, max_index_sub):
        """配置连接选项"""
        self.client_limits = client_limits
        self.server_limits = server_limits
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
        if np.isnan(b):
            b = self.user_data.get(ctx_attr, np.nan)
        a = type(b)(
            row.get(comp_attr, np.nan)
            if type(row) is dict
            else getattr(row, "owner", np.nan)
        )
        return bool(rls_func(a, b))
