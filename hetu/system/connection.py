"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: MIT 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""
import logging
import time

import numpy as np

from .context import Context
from ..data import BaseComponent, define_component, Property, Permission
from ..manager import ComponentTableManager
from ..system import define_system

logger = logging.getLogger('HeTu')


@define_component(namespace='HeTu', persist=False, permission=Permission.ADMIN)
class Connection(BaseComponent):
    owner: np.int64 = Property(0, index=True)
    address: str = Property('', dtype='<U32')  # 连接地址
    device: str = Property('', dtype='<U32')  # 物理设备名
    device_id: str = Property('', dtype='<U128')  # 设备id
    admin: str = Property('', dtype='<U16')  # 是否是admin
    created: np.double = Property(0)  # 连接创建时间
    last_active: np.double = Property(0)  # 最后活跃时间


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
        if kick_logged_in or now - logged_conn.last_active > ctx.idle_timeout:
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

    # 已登录用户扩张限制
    ctx.server_limits = [[limit[0] * 10, limit[1]] for limit in ctx.server_limits]
    ctx.client_limits = [[limit[0] * 10, limit[1]] for limit in ctx.client_limits]
    ctx.max_row_sub *= 100
    ctx.max_index_sub *= 100

    return True, 'SUCCESS'


class ConnectionAliveChecker:
    """
    连接合规性检查，主要检查连接是否存活
    """

    def __init__(self, comp_mgr: ComponentTableManager):
        self.conn_tbl = comp_mgr.get_table(Connection)
        self.last_active_cache = 0

    async def is_illegal(self, ctx: Context, info: str):
        # 直接数据库检查connect数据是否是自己(可能被别人踢了)，以及要更新last activate
        conn_tbl = self.conn_tbl
        caller, conn_id = ctx.caller, ctx.connection_id
        if caller and caller > 0:
            # 此方法无法通过事务，这里判断通过后可能有其他连接踢了你，等于同时可能有2个连接在执行1个用户的事务，但
            # 问题不大，因为事务是有冲突判断的。不冲突的事务就算一起执行也没啥问题。
            conn = await conn_tbl.direct_get(conn_id)
            if conn is None or conn.owner != caller:
                logger.warning(
                    f"⚠️ [📞Executor] 当前连接数据已删除，可能已被踢出，将断开连接。调用：{info}")
                return True

        # idle时间内只往数据库写入5次last_active，防止批量操作时频繁更新
        now = time.time()
        if now - self.last_active_cache > (ctx.idle_timeout / 5):
            await conn_tbl.direct_set(ctx.connection_id, last_active=now)
            self.last_active_cache = now


class ConnectionFloodChecker:
    def __init__(self):
        self.received_msgs = 0  # 收到的消息数, 用来判断flooding攻击
        self.received_start_time = time.time()
        self.sent_msgs = 0  # 发送的消息数，用来判断订阅攻击
        self.sent_start_time = time.time()

    def received(self, count=1):
        self.received_msgs += count

    def sent(self, count=1):
        self.sent_msgs += count

    def send_limit_reached(self, ctx: Context, info: str):
        now = time.time()
        sent_elapsed = now - self.sent_start_time
        for limit in ctx.server_limits:
            if self.sent_msgs > limit[0] and sent_elapsed < limit[1]:
                logger.warning(
                    f"⚠️ [📞Executor] 发送消息数过多，可能是订阅攻击，将断开连接。调用：{info}")
                return True
        if sent_elapsed > ctx.server_limits[-1][1]:
            self.sent_msgs = 0
            self.sent_start_time = now
        return False

    def recv_limit_reached(self, ctx: Context, info: str):
        now = time.time()
        received_elapsed = now - self.received_start_time
        for limit in ctx.client_limits:
            if self.received_msgs > limit[0] and received_elapsed < limit[1]:
                logger.warning(
                    f"⚠️ [📞Executor] 收到消息数过多，可能是flood攻击，将断开连接。调用：{info}")
                return True
        if received_elapsed > ctx.server_limits[-1][1]:
            self.received_msgs = 0
            self.received_start_time = now
        return False
