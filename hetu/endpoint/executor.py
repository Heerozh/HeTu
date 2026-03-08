"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import logging
from time import time as now
from typing import TYPE_CHECKING

from ..common import Permission
from ..safelogging.filter import ContextFilter
from .connection import ConnectionAliveChecker, del_connection, new_connection
from .context import Context
from .definer import EndpointDefine, EndpointDefines

if TYPE_CHECKING:
    from ..manager import ComponentTableManager
    from .response import ResponseToClient

logger = logging.getLogger("HeTu.root")
replay = logging.getLogger("HeTu.replay")


class EndpointExecutor:
    """
    每个连接一个EndpointExecutor实例。
    """

    def __init__(
        self, namespace: str, tbl_mgr: ComponentTableManager, context: Context
    ) -> None:
        self.namespace = namespace
        self.tbl_mgr = tbl_mgr
        self.alive_checker = ConnectionAliveChecker(self.tbl_mgr)
        self.context = context

    async def initialize(self, address: str):
        """初始化连接，分配connection id，如果失败则raise异常"""
        if self.context.connection_id != 0:
            return
        # 通过connection component分配自己一个连接id
        conn_id = await new_connection(self.tbl_mgr, address)
        if not conn_id:
            raise RuntimeError("连接初始化失败，new_connection调用失败")
        self.context.connection_id = conn_id
        self.context.address = address
        ContextFilter.set_log_context(str(self.context))

    async def terminate(self):
        """删除连接，失败不抛出异常"""
        if self.context.connection_id == 0:
            return
        # 释放connection
        await del_connection(self.tbl_mgr, self.context.connection_id)

    def execute_check(self, endpoint: str, args: tuple) -> EndpointDefine | None:
        """检查调用是否合法"""
        context = self.context
        namespace = self.namespace

        # 读取保存的endpoint define
        ep = EndpointDefines().get_endpoint(namespace, endpoint)
        if not ep:
            err_msg = (
                f"⚠️ [📞Endpoint] [非法操作] {context} | "
                f"不存在的Endpoint, 检查是否非法调用：{namespace}.{endpoint}"
            )
            replay.info(err_msg)
            logger.warning(err_msg)
            return None

        # 检查权限是否符合
        match ep.permission:
            case Permission.USER:
                if not context.caller:
                    err_msg = (
                        f"⚠️ [📞Endpoint] [非法操作] {context} | "
                        f"{endpoint}无调用权限，检查是否非法调用：{args}"
                    )
                    replay.info(err_msg)
                    logger.warning(err_msg)
                    return None
            case Permission.ADMIN:
                if not context.is_admin():
                    err_msg = (
                        f"⚠️ [📞Endpoint] [非法操作] {context} | "
                        f"{endpoint}无调用权限，检查是否非法调用：{args}"
                    )
                    replay.info(err_msg)
                    logger.warning(err_msg)
                    return None

        # 检测args数量是否对得上
        if not (ep.arg_count - ep.defaults_count - 1 <= len(args) <= ep.arg_count - 1):
            err_msg = (
                f"❌ [📞Endpoint] [非法操作] {context} | "
                f"{namespace}.{endpoint}参数数量不对，检查客户端代码。"
                f"要求{ep.arg_count - ep.defaults_count - 1}-{ep.arg_count - 1}个参数, "
                f"传入了{len(args)}个。"
                f"调用内容：{args}"
            )
            replay.info(err_msg)
            logger.warning(err_msg)
            return None

        return ep

    async def execute_(
        self, ep: EndpointDefine, *args
    ) -> tuple[bool, ResponseToClient | None]:
        """
        实际调用逻辑，无任何检查
        调用成功返回True，Endpoint返回值
        遇到异常则记录error日志，并返回False，None，表示内部失败或非法调用，此时需要立即调用
        terminate断开连接
        """
        # 开始调用
        ep_name = ep.func.__name__
        logger.debug("⌚ [📞Endpoint] 调用Endpoint: %s", ep_name)

        # 初始化context值
        context = self.context
        context.timestamp = now()

        # 调用Endpoint
        try:
            # 执行
            rtn = await ep.func(context, *args)
            # logger.debug(f"✅ [📞Endpoint] 调用Endpoint成功: {ep_name}")
            return True, rtn
        except Exception as e:
            err_msg = (
                f"❌ [📞Endpoint] [调用异常] {context} | {ep_name}{args}，"
                f"异常：{type(e).__name__}:{e}"
            )
            replay.info(err_msg)
            logger.exception(err_msg)
            return False, None
        finally:
            pass

    async def execute(
        self, endpoint: str, *args
    ) -> tuple[bool, ResponseToClient | None]:
        """
        调用Endpoint，返回True表示调用成功，
        返回False表示内部失败或非法调用，此时需要立即调用terminate断开连接
        """
        # 检查call参数和call权限
        ep = self.execute_check(endpoint, args)
        if ep is None:
            return False, None

        # 直接数据库检查connect数据是否是自己(可能被别人踢了)，以及要更新last activate
        illegal = await self.alive_checker.is_illegal(
            self.context, f"{self.namespace}.{endpoint}"
        )
        if illegal:
            return False, None

        # 开始调用
        return await self.execute_(ep, *args)
