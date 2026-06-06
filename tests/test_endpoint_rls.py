#  """
#  @author: Heerozh (Zhang Jianhao)
#  @copyright: Copyright 2024, Heerozh. All rights reserved.
#  @license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
#  @email: heeroz@gmail.com
#  """

import numpy as np

from hetu.common.snowflake_id import SnowflakeID
from hetu.data import BaseComponent, Permission, define_component, property_field
from hetu.endpoint.context import Context

SnowflakeID().init(1, 0)


def _make_ctx(caller=0, address="0.0.0.0", group="guest", user_data=None):
    return Context(
        caller=caller,
        connection_id=0,
        address=address,
        group=group,
        user_data=user_data or {},
        timestamp=0.0,
        request=None,  # type: ignore  # rls_check 不使用
        systems=None,  # type: ignore  # rls_check 不使用
    )


def test_rls_check_struct_uses_configured_field(new_component_env):
    """发现8回归：rls_check 的 struct 分支必须用配置的 comp_attr，而不是写死 'owner'。
    否则对『字段名不是 owner 的自定义 RLS』会拿错列判断，导致误放/误拒。"""

    @define_component(
        namespace="pytest",
        permission=Permission.RLS,
        rls_compare=("eq", "friend", "caller"),
    )
    class FriendRLS(BaseComponent):
        owner: np.int64 = property_field(0, unique=False, index=True)
        friend: np.int64 = property_field(0, unique=False, index=False)

    ctx = _make_ctx(caller=11)

    # struct 行：friend 匹配、owner 不匹配 → 应放行（旧代码读 owner=0 → 误拒）
    allow = FriendRLS.new_row()
    allow.friend = 11
    allow.owner = 0
    assert ctx.rls_check(FriendRLS, allow) is True

    # struct 行：friend 不匹配、owner 恰好匹配 → 应拒绝（旧代码读 owner=11 → 误放，安全风险）
    deny = FriendRLS.new_row()
    deny.friend = 99
    deny.owner = 11
    assert ctx.rls_check(FriendRLS, deny) is False

    # dict 路径作为对照（用的是 comp_attr），新旧都正确
    assert ctx.rls_check(FriendRLS, {"friend": 11, "owner": 0}) is True
    assert ctx.rls_check(FriendRLS, {"friend": 99, "owner": 11}) is False


def test_rls_check_string_ctx_attr_no_crash(new_component_env):
    """发现8回归：ctx_attr 指向字符串型 Context 属性时 rls_check 不应崩溃。
    旧代码 np.isnan(b) 对字符串会抛 TypeError。"""

    @define_component(
        namespace="pytest",
        permission=Permission.RLS,
        rls_compare=("eq", "token", "address"),
    )
    class AddrRLS(BaseComponent):
        owner: np.int64 = property_field(0, unique=False, index=True)
        token: "U16" = property_field("", unique=False, index=True)  # type: ignore  # noqa

    ctx = _make_ctx(caller=1, address="1.2.3.4")

    # 旧代码在 np.isnan(ctx.address="1.2.3.4") 处抛 TypeError；修复后应正常比较
    assert ctx.rls_check(AddrRLS, {"token": "1.2.3.4", "owner": 0}) is True
    assert ctx.rls_check(AddrRLS, {"token": "9.9.9.9", "owner": 0}) is False
