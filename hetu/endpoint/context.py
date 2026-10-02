"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import numpy as np

if TYPE_CHECKING:
    from sanic import Request

    from ..data.component import BaseComponent
    from ..system.caller import SystemCaller

# rls_check 用的"未找到"哨兵；不能用 np.nan 当哨兵，因为 np.isnan 对字符串/None 会抛 TypeError
_UNSET = "__UNSET__"


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
    """所属组名，用于判断是否admin（`is_admin()`）、GM（`is_gm()`）。"""

    user_data: dict[str, Any]
    """当前连接的用户数据，可自由设置，在所有System间共享。"""

    timestamp: float
    """调用时间戳。"""

    request: Request
    """framework原始请求对象，unsafe，普通业务代码请避免直接使用。"""

    systems: SystemCaller
    """全局System管理器，unsafe，可通过 `ctx.systems.call(...)` 调用其他System。"""

    guard_state: dict[str, Any] = field(default_factory=dict)
    """每连接的 guard 私有状态（如 rate_limit 的窗口计数）；纯内存、不跨连接。"""

    client_limits: list[list[int]] = field(default_factory=list)
    """客户端消息发送限制（次数）。"""

    server_limits: list[list[int]] = field(default_factory=list)
    """服务端消息发送限制（次数）。"""

    max_row_sub: int = 0
    """行订阅数量限制。"""

    max_index_sub: int = 0
    """索引订阅数量限制。"""

    max_table_sub: int = 0
    """整表订阅数量限制。"""

    def __str__(self):
        return f"[{self.connection_id}|{self.address}|{self.caller}]"

    def is_admin(self):
        """
        是否管理员连接（group 以 "admin" 开头）：可调用 ADMIN、GM 权限的 System/Endpoint，
        可订阅 ADMIN 组件，订阅时不受 RLS 限制。这是后台管理工具用的 root 级权限，不要给游戏
        客户端的连接，游戏里的管理权限用 GM（`is_gm()`）。
        Whether this is an admin connection (group starts with "admin"): root-level, for
        back-office tools only, never for game-client connections (those use GM).
        """
        return self.group.startswith("admin")

    def is_gm(self):
        """
        是否 GM 连接（group 以 "gm" 开头）：登录后可调用 `Permission.GM` 的 System/Endpoint，
        读数据和普通玩家一样。
        Whether this is a GM connection (group starts with "gm").
        """
        return self.group.startswith("gm")

    def configure(
        self, client_limits, server_limits, max_row_sub, max_index_sub, max_table_sub=0
    ):
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
        max_table_sub: int
            当前连接允许的最大整表订阅数量。一般对应配置项
            `MAX_TABLE_SUBSCRIPTION`。

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
        self.max_table_sub = max_table_sub

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
        # ctx_attr 先查 Context 属性，没有再查 user_data。用哨兵判断"是否存在"，
        # 避免对字符串/None 值调用 np.isnan 抛 TypeError。
        b = getattr(self, ctx_attr, _UNSET)
        if b is _UNSET:
            b = self.user_data.get(ctx_attr, np.nan)
        # struct 行也要按配置的 comp_attr 取值（与 dict 分支一致），不能写死 "owner"，
        # 否则字段名非 owner 的自定义 RLS 会拿错列判断权限。
        try:
            a = type(b)(
                row.get(comp_attr, np.nan)
                if type(row) is dict
                else getattr(row, comp_attr, np.nan)
            )
        except TypeError, ValueError:
            # 行里的值转不成 ctx 那边的类型（比如某个 System 把 user_data 里比较用的值置成了
            # None）：按不可见处理。判定出错不能当作有权限；抛出的话订阅这一批整个失败，每次
            # 重试都一样
            return False
        return bool(rls_func(a, b))
