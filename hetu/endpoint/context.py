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
    from sanic import Request
    from ..system.caller import SystemCaller


@dataclass
class Context:
    """
    Endpoint调用时的上下文，由engine创建并作为 `ctx` 参数传入Endpoint函数；
    `SystemContext` 继承自此类。包含调用方身份、当前连接的用户数据，
    以及消息发送和订阅数量限制等。
    """

    caller: int
    """调用方的user id；未登录为0；执行过 `elevate()` 后为传入的 `user_id` 。"""

    connection_id: int
    """调用方的connection id。"""

    address: str
    """调用方的IP地址。"""

    group: str
    """所属组名，目前只用于判断是否admin。"""

    user_data: dict[str, Any]
    """当前连接的用户数据，可自由设置，在所有System间共享。"""

    timestamp: float
    """调用时间戳。"""

    request: Request
    """framework原始请求对象，unsafe，普通业务代码请避免直接使用。"""

    systems: SystemCaller
    """全局System管理器，unsafe，可通过 `ctx.systems.call(...)` 调用其他System。"""

    client_limits: list[list[int]] = field(default_factory=list)
    """客户端消息发送限制（次数）。"""

    server_limits: list[list[int]] = field(default_factory=list)
    """服务端消息发送限制（次数）。"""

    max_row_sub: int = 0
    """行订阅数量限制。"""

    max_index_sub: int = 0
    """索引订阅数量限制。"""

    def __str__(self):
        return f"[{self.connection_id}|{self.address}|{self.caller}]"

    def is_admin(self):
        return True if self.group.startswith("admin") else False

    def configure(self, client_limits, server_limits, max_row_sub, max_index_sub):
        """
        配置当前连接的限流与订阅配额。

        此方法通常在连接建立后调用，用于把 websocket/app 配置中的连接级限制
        写入 `Context`，供后续消息收发和订阅逻辑直接读取。

        Parameters
        ----------
        client_limits: list[list[int]]
            客户端向服务端发送消息的频率限制。每项格式为
            ``[最大消息数, 统计时间(秒)]``；空列表表示不限制。
            一般对应配置项 `CLIENT_SEND_LIMITS`。
        server_limits: list[list[int]]
            服务端向客户端发送消息的频率限制。每项格式同上；
            一般对应配置项 `SERVER_SEND_LIMITS`。
        max_row_sub: int
            当前连接允许的最大行订阅数量。一般对应配置项
            `MAX_ROW_SUBSCRIPTION`。
        max_index_sub: int
            当前连接允许的最大索引订阅数量。一般对应配置项
            `MAX_INDEX_SUBSCRIPTION`。

        Notes
        -----
        本方法只负责保存传入值，不做校验、排序或拷贝。
        `client_limits` 和 `server_limits` 应按统计时间从小到大排列，
        因为后续限流检查会使用最后一项的时间窗口作为计数重置基准。
        """
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
