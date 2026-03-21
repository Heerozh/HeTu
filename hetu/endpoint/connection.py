"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import logging
import time
from typing import TYPE_CHECKING

import numpy as np

from hetu.data.backend import RaceCondition
from hetu.data.backend.base import RowFormat

from ..data import BaseComponent, Permission, define_component, property_field
from ..i18n import _
from ..safelogging.filter import ContextFilter
from .context import Context

if TYPE_CHECKING:
    from ..data.backend.table import Table
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


async def new_connection(tbl_mgr: ComponentTableManager, address: str) -> int:
    """
    通过connection component分配自己一个连接id，如果失败，Raise各种异常
    此方法不会事务冲突，因为只插入Connection，且Connection没有Unique属性。
    """
    table = tbl_mgr.get_table(Connection)
    assert table, _("未初始化ComponentTableManager，无法使用Connection组件")

    # 不会事务冲突只会连接错误
    async with table.session() as session:
        repo = session.using(Connection)
        # 服务器自己的（future call之类的localhost）连接不应该受IP限制
        if MAX_ANONYMOUS_CONNECTION_BY_IP and address not in ["localhost", "127.0.0.1"]:
            same_ips = await repo.range("address", address, limit=1000)
            same_ip_guests = same_ips[same_ips.owner == 0]
            if len(same_ip_guests) > MAX_ANONYMOUS_CONNECTION_BY_IP:
                msg = _(
                    "⚠️ [📞Endpoint] [非法操作] 同一IP匿名连接数过多({count})，可能是攻击。"
                ).format(count=len(same_ips))
                logger.warning(msg)
                raise RuntimeError(msg)

        row = Connection.new_row()
        row.owner = 0
        row.created = time.time()
        row.last_active = row.created
        row.address = address
        await repo.insert(row)

    # 等待数据同步完成，防止后续ConnectionAliveChecker.is_illegal操作找不到关键连接数据
    await table.backend.wait_for_synced()
    return row.id


async def del_connection(tbl_mgr: ComponentTableManager, connection_id: int) -> None:
    table = tbl_mgr.get_table(Connection)
    assert table, _("未初始化ComponentTableManager，无法使用Connection组件")

    async for attempt in table.session().retry(5):
        async with attempt as session:
            repo = session.using(Connection)
            connection = await repo.get(id=connection_id)
            if connection is not None:
                repo.delete(connection_id)


async def elevate(ctx: Context, user_id: int, kick_logged_in=True):
    """
    提升到User权限。如果该连接已提权，或user_id已在其他连接登录，返回False。
    如果成功，则ctx.caller会被设置为user_id，同时事务结束，之后将无法调用ctx[Components]。

    kick_logged_in:
        如果user_id已在其他连接登录，则标记该连接断开并返回True，该连接将在客户端调用任意Endpoint时被关闭。

    Notes
    -----
    本方法是一个独立的事务，如果你在System中(另一个事务中）调用此方法，父事务回退时不会回退此方法的结果。

    """
    # 以下方法可以考虑在system里面再写一套？
    # 不过这个是endpoint负责的东西，用system写会导致互相耦合。但这里已经用了ctx.systems了。
    # 另外elevate加到system里需要用depends调用，作为新人教程第一步太复杂了。
    # 但是现在新人教程也可以用ctx.systems.call("elevate", ...)来调用这个函数了。
    assert ctx.connection_id != 0, _("请先初始化连接")
    tbl_mgr: ComponentTableManager = ctx.systems.tbl_mgr
    table = tbl_mgr.get_table(Connection)
    assert table, _("未初始化ComponentTableManager，无法使用Connection组件")

    # 如果当前连接已提权
    if ctx.caller:
        return False, "CURRENT_CONNECTION_ALREADY_ELEVATED"

    async for attempt in table.session().retry(5):
        async with attempt as session:
            repo = session.using(Connection)
            # 如果此用户已经登录
            already_logged = await repo.get(owner=user_id)
            if already_logged is not None:
                idle = time.time() - already_logged.last_active
                # 如果要求强制踢人，或者该连接last_active时间已经超时（说明服务器强关数据残留了）
                if kick_logged_in or idle > ENDPOINT_CALL_IDLE_TIMEOUT:
                    # 去掉该连接的owner，当该连接下次执行Endpoint时会被关闭
                    already_logged.owner = 0
                    await repo.update(already_logged)
                else:
                    return False, "USER_ALREADY_LOGGED_IN"

            # 在数据库中关联connection和user
            conn = await repo.get(id=ctx.connection_id)
            if not conn:
                # 连接断开时才会找不到conn，但是连接断开时会cancel所有task，不应该到这
                # 所以大部分情况还是race condition导致，也就是还没读到新建的连接
                raise RaceCondition(_("连接数据不存在，可能是写入未同步，重试"))
                # return False, "CONNECTION_NOT_FOUND"
            conn.owner = user_id
            await repo.update(conn)

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

    def __init__(self, tbl_mgr: ComponentTableManager):
        table = tbl_mgr.get_table(Connection)
        assert table
        self.conn_tbl: Table = table
        self.last_active_cache = 0

    async def is_illegal(self, ctx: Context, ex_info: str):
        # 直接数据库检查connect数据是否是自己(可能被别人踢了)，以及要更新last activate
        conn_tbl = self.conn_tbl
        caller, conn_id = ctx.caller, ctx.connection_id
        if caller:
            # 此方法无法通过事务，这里判断通过后可能有其他连接踢了你，等于同时可能有2个连接在执行1个用户的事务，但
            # 问题不大，因为事务是有冲突判断的。不冲突的事务就算一起执行也没啥问题。
            conn = await conn_tbl.servant_get(conn_id, RowFormat.STRUCT)
            # 连接断开时才会conn is None，但是连接断开时会cancel所有task，不应该到这
            # 所以大部分情况是servant还没有从master同步数据过来，如果有些数据库wait sync无效，
            # 这里可以考虑重试几次
            if conn is None or conn.owner != caller:
                err_msg = _(
                    "⚠️ [📞Endpoint] 当前连接数据已删除，可能已被踢出，将断开连接。调用：{ex_info}"
                ).format(ex_info=ex_info)
                replay.info(err_msg)
                logger.warning(err_msg)
                return True

        # idle时间内只往数据库写入5次last_active，防止批量操作时频繁更新
        now = time.time()
        if now - self.last_active_cache > (ENDPOINT_CALL_IDLE_TIMEOUT / 5):
            await conn_tbl.direct_set(conn_id, last_active=str(now))
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
                err_msg = _(
                    "⚠️ [📞Endpoint] [非法操作] "
                    "发送消息数过多({sent_msgs} in {sent_elapsed}s)，"
                    "可能是订阅攻击，将断开连接。调用：{info}"
                ).format(
                    sent_msgs=self.sent_msgs,
                    sent_elapsed=f"{sent_elapsed:0.2f}",
                    info=info,
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
                err_msg = _(
                    "⚠️ [📞Endpoint] [非法操作] "
                    "收到消息数过多({received_msgs} in {received_elapsed}s)，"
                    "可能是flood攻击，将断开连接。调用：{info}"
                ).format(
                    received_msgs=self.received_msgs,
                    received_elapsed=f"{received_elapsed:0.2f}",
                    info=info,
                ) 
                replay.info(err_msg)
                logger.warning(err_msg)
                return True
        if received_elapsed > ctx.client_limits[-1][1]:
            self.received_msgs = 0
            self.received_start_time = now
        return False
