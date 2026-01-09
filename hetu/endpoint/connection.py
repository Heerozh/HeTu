"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import logging
import time
from typing import Any, TYPE_CHECKING

import numpy as np

from hetu.data.backend import RaceCondition

from .context import Context
from ..data import BaseComponent, define_component, property_field, Permission
from ..safelogging.filter import ContextFilter

if TYPE_CHECKING:
    from ..data.backend import Backend
    from ..manager import ComponentTableManager

logger = logging.getLogger("HeTu.root")
replay = logging.getLogger("HeTu.replay")

MAX_ANONYMOUS_CONNECTION_BY_IP = 0  # 占位符，实际由Config里修改
ENDPOINT_CALL_IDLE_TIMEOUT = 0  # 占位符，实际由Config里修改


@define_component(namespace="core", volatile=True, permission=Permission.ADMIN)
class Connection(BaseComponent):
    owner: np.int64 = property_field(0, index=True)
    address: str = property_field("", dtype="<U32", index=True)  # 连接地址
    device: str = property_field("", dtype="<U32")  # 物理设备名
    device_id: str = property_field("", dtype="<U128")  # 设备id
    admin: str = property_field("", dtype="<U16")  # 是否是admin
    created: np.double = property_field(0)  # 连接创建时间
    last_active: np.double = property_field(0)  # 最后活跃时间


# todo 所有定义为HeTu命名空间的表，需要强制引用，就按独立隔离的簇引用好了
#      可以改个名字，比如叫core之类？
# 必须引用Connection组件，否则不会在数据库中创建此组件的表
# @define_system(namespace="core", permission=None, components=(Connection,))
# async def pin_connection_reference(ctx):
#     pass


async def new_connection(address: str) -> int:
    """通过connection component分配自己一个连接id，如果失败，或事务冲突，Raise各种异常"""
    assert Connection.hosted_, "未初始化ComponentTableManager，无法使用Connection组件"
    async with Connection.hosted_.session() as session:
        repo = session.using(Connection)
        # 服务器自己的（future call之类的localhost）连接不应该受IP限制
        if MAX_ANONYMOUS_CONNECTION_BY_IP and address not in ["localhost", "127.0.0.1"]:
            same_ips = await repo.range("address", address, limit=1000)
            same_ip_guests = same_ips[same_ips.owner == 0]
            if len(same_ip_guests) > MAX_ANONYMOUS_CONNECTION_BY_IP:
                msg = f"⚠️ [📞Executor] [非法操作] 同一IP匿名连接数过多({len(same_ips)})，可能是攻击。"
                logger.warning(msg)
                raise RuntimeError(msg)

        row = Connection.new_row()
        row.owner = 0
        row.created = time.time()
        row.last_active = row.created
        row.address = address
        await repo.insert(row)

    return row.id


async def del_connection(connection_id: int) -> None:
    assert Connection.hosted_, "未初始化ComponentTableManager，无法使用Connection组件"
    async with Connection.hosted_.session() as session:
        repo = session.using(Connection)
        connection = await repo.get(id=connection_id)
        if connection is not None:
            connection.delete(connection_id)


async def elevate(ctx: Context, user_id: int, kick_logged_in=True):
    """
    提升到User权限。如果该连接已提权，或user_id已在其他连接登录，返回False。
    如果成功，则ctx.caller会被设置为user_id，同时事务结束，之后将无法调用ctx[Components]。

    kick_logged_in:
        如果user_id已在其他连接登录，则标记该连接断开并返回True，该连接将在客户端调用任意System时被关闭。

    """
    assert ctx.connection_id != 0, "请先初始化连接"
    assert Connection.hosted_, "未初始化ComponentTableManager，无法使用Connection组件"

    # 如果当前连接已提权
    if ctx.caller is not None and ctx.caller > 0:
        return False, "CURRENT_CONNECTION_ALREADY_ELEVATED"

    for _ in range(5):  # todo 改成async for语法
        try:
            async with Connection.hosted_.session() as session:
                repo = session.using(Connection)
                # 如果此用户已经登录
                already_logged = await repo.get(owner=user_id)
                if already_logged is not None:
                    now = time.time()
                    # 如果要求强制踢人，或者该连接last_active时间已经超时（说明服务器强关数据残留了）
                    if (
                        kick_logged_in
                        or now - already_logged.last_active > ENDPOINT_CALL_IDLE_TIMEOUT
                    ):
                        # 去掉该连接的owner，当该连接下次执行System时会被关闭
                        already_logged.owner = 0
                        await repo.update(already_logged)
                    else:
                        return False, "USER_ALREADY_LOGGED_IN"

                # 在数据库中关联connection和user
                conn = await repo.get(id=ctx.connection_id)
                if not conn:
                    return False, "CONNECTION_NOT_FOUND"
                conn.owner = user_id
                await repo.update(conn)
        except RaceCondition as _:
            continue

    # 如果事务成功，则设置ctx.caller (事务冲突时会跳过后面代码)
    ctx.caller = user_id

    # 已登录用户扩张限制
    ctx.server_limits = [[limit[0] * 10, limit[1]] for limit in ctx.server_limits]
    ctx.client_limits = [[limit[0] * 10, limit[1]] for limit in ctx.client_limits]
    ctx.max_row_sub *= 50
    ctx.max_index_sub *= 50

    ContextFilter.set_log_context(str(ctx))
    return True, "SUCCESS"


class ConnectionAliveChecker:
    """
    连接合规性检查，主要检查连接是否存活
    """

    def __init__(self, comp_mgr: ComponentTableManager):
        self.conn_tbl = comp_mgr.get_table(Connection)
        self.last_active_cache = 0

    async def is_illegal(self, ctx: Context, ex_info: Any):
        # 直接数据库检查connect数据是否是自己(可能被别人踢了)，以及要更新last activate
        conn_tbl = self.conn_tbl
        caller, conn_id = ctx.caller, ctx.connection_id
        if caller and caller > 0:
            # 此方法无法通过事务，这里判断通过后可能有其他连接踢了你，等于同时可能有2个连接在执行1个用户的事务，但
            # 问题不大，因为事务是有冲突判断的。不冲突的事务就算一起执行也没啥问题。
            conn = await conn_tbl.direct_get(conn_id)
            if conn is None or conn.owner != caller:
                err_msg = f"⚠️ [📞Executor] 当前连接数据已删除，可能已被踢出，将断开连接。调用：{ex_info}"
                replay.info(err_msg)
                logger.warning(err_msg)
                return True

        # idle时间内只往数据库写入5次last_active，防止批量操作时频繁更新
        now = time.time()
        if now - self.last_active_cache > (ENDPOINT_CALL_IDLE_TIMEOUT / 5):
            await conn_tbl.direct_set(ctx.connection_id, last_active=now)
            self.last_active_cache = now
        return False


# todo last_active超时的连接，要定时任务统一批量删除


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
        if not ctx.server_limits:
            return False
        now = time.time()
        sent_elapsed = now - self.sent_start_time
        for limit in ctx.server_limits:
            if self.sent_msgs > limit[0] and sent_elapsed < limit[1]:
                err_msg = (
                    f"⚠️ [📞Executor] [非法操作] "
                    f"发送消息数过多({self.sent_msgs} in {sent_elapsed:0.2f}s)，"
                    f"可能是订阅攻击，将断开连接。调用：{info}"
                )
                replay.info(err_msg)
                logger.warning(err_msg)
                return True
        if sent_elapsed > ctx.server_limits[-1][1]:
            self.sent_msgs = 0
            self.sent_start_time = now
        return False

    def recv_limit_reached(self, ctx: Context, info: str):
        if not ctx.client_limits:
            return False
        now = time.time()
        received_elapsed = now - self.received_start_time
        for limit in ctx.client_limits:
            if self.received_msgs > limit[0] and received_elapsed < limit[1]:
                err_msg = (
                    f"⚠️ [📞Executor] [非法操作] "
                    f"收到消息数过多({self.received_msgs} in {received_elapsed:0.2f}s)，"
                    f"可能是flood攻击，将断开连接。调用：{info}"
                )
                replay.info(err_msg)
                logger.warning(err_msg)
                return True
        if received_elapsed > ctx.client_limits[-1][1]:
            self.received_msgs = 0
            self.received_start_time = now
        return False
