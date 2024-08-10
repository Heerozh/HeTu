"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: MIT 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""
import numpy as np
import time
from .context import Context
from ..data import BaseComponent, define_component, Property, Permission
from ..system import define_system

SYSTEM_CALL_IDLE_TIMEOUT = 60 * 2


@define_component(namespace='HeTu', persist=False, permission=Permission.ADMIN)
class Connection(BaseComponent):
    owner: np.int64 = Property(0, index=True)
    address: str = Property('', dtype='<U32')  # 连接地址
    device: str = Property('', dtype='<U32')  # 物理设备名
    device_id: str = Property('', dtype='<U128')  # 设备id
    admin: str = Property('', dtype='<U16')  # 是否是admin
    created: np.double = Property(0)  # 连接创建时间
    last_active: np.double = Property(0)  # 最后活跃时间
    received_msgs: np.int32 = Property(0)  # 收到的消息数, 用来判断flooding攻击
    invalid_msgs: np.int32 = Property(0)  # 无效消息数, 用来判断flooding攻击


@define_system(namespace='global', permission=Permission.ADMIN, components=(Connection,))
async def new_connection(ctx: Context, address: str):
    row = Connection.new_row()
    row.owner = 0
    row.created = time.time()
    row.last_active = row.created
    row.address = address
    await ctx[Connection].insert(row)
    row_ids = await ctx.end_transaction()
    ctx.connection_id = row_ids[0]


@define_system(namespace='global', permission=Permission.ADMIN, components=(Connection,))
async def del_connection(ctx: Context):
    try:
        await ctx[Connection].delete(ctx.connection_id)
    except KeyError:
        pass


@define_system(namespace='global', permission=Permission.ADMIN, components=(Connection,))
async def elevate(ctx: Context, user_id: int, kick_logged_in=True):
    """
    提升到User权限。如果该连接已提权，或user_id已在其他连接登录，返回False。
    如果成功，则ctx.caller会被设置为user_id，同时事务结束，之后将无法调用ctx[Components]。

    kick_logged_in:
        如果user_id已在其他连接登录，则标记该连接断开并返回True，该连接将在客户端调用任意System时被关闭。

    """
    assert ctx.connection_id != 0, "请先初始化连接"

    # 如果当前连接已提权
    if ctx.caller is not None and ctx.caller > 0:
        return False, 'CURRENT_CONNECTION_ALREADY_ELEVATED'
    # 如果此用户已经登录
    logged_conn = await ctx[Connection].select(user_id, 'owner')
    if logged_conn is not None:
        now = time.time()
        # 如果要求强制踢人，或者该连接last_active时间已经超时（说明服务器强关数据残留了）
        if kick_logged_in or now - logged_conn.last_active > SYSTEM_CALL_IDLE_TIMEOUT:
            logged_conn.owner = 0  # 去掉该连接的owner，当该连接下次执行System时会被关闭
            await ctx[Connection].update(logged_conn.id, logged_conn)
        else:
            return False, 'USER_ALREADY_LOGGED_IN'

    # 在数据库中关联connection和user
    conn = await ctx[Connection].select(ctx.connection_id)
    conn.owner = user_id
    await ctx[Connection].update(ctx.connection_id, conn)

    # 如果事务成功，则设置ctx.caller (end_transaction事务冲突时会跳过后面代码)
    await ctx.end_transaction()
    ctx.caller = user_id
    return True, 'SUCCESS'

# todo 新建个connection的文件专门负责，附带kick，flood check等一系列方法
