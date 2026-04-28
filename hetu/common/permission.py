"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

from enum import IntEnum


class Permission(IntEnum):
    """
    客户端访问权限级别。

    用于 `define_endpoint` / `define_system` 暴露给客户端的调用权限，也用于
    `define_component` 声明Component数据的读取权限。服务端内部代码不受这些权限
    限制；业务逻辑中的写入权限和更细粒度安全检查仍需要自行实现。
    """

    EVERYBODY = 1
    """任何客户端连接都允许访问；适合公开状态、公告、配置等低敏感数据。"""

    USER = 2
    """仅已登录客户端允许访问；要求 `ctx.caller` 有有效用户id。"""

    OWNER = 3
    """Component行级读取权限；等同于 `RLS` 且预设 `rls_compare=("eq", "owner", "caller")`。"""

    RLS = 4
    """Component自定义行级读取权限；具体比较逻辑由 `rls_compare` 参数定义。"""

    ADMIN = 999
    """仅管理员连接允许访问；要求 `ctx.is_admin()` 返回True。"""

