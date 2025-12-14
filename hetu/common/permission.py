"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

from enum import IntEnum


class Permission(IntEnum):
    EVERYBODY = 1
    USER = 2
    OWNER = 3  # 同RLS权限，只是预设了rls_compare为('eq', 'owner', 'caller')
    RLS = 4  # 由rls_compare参数(operator_function, component_property_name, context_property_name)决定具体的rls逻辑
    ADMIN = 999

