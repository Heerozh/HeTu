"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: MIT 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""
import asyncio
import logging
import random
import time
from dataclasses import dataclass

from .connection import Connection
from .context import Context
from ..data import Permission
from ..data.backend import RaceCondition
from ..manager import ComponentTableManager
from ..system import SystemClusters, SystemDefine

logger = logging.getLogger('HeTu')


@dataclass
class SystemCall:
    system: str  # 目标system名
    args: tuple  # 目标system参数


class SystemResult:
    pass


class ResponseToClient(SystemResult):
    """回报message给客户端，注意必须是json可以序列化的数据"""

    def __init__(self, message: list | dict):
        self.message = message


class SystemExecutor:
    """
    每个连接一个SystemExecutor实例。
    """

    def __init__(self, namespace: str, comp_mgr: ComponentTableManager):
        self.namespace = namespace
        self.comp_mgr = comp_mgr
        self.last_active = 0
        self.context = Context(
            caller=None,
            connection_id=0,
            group=None,
            user_data={},

            timestamp=0,
            retry_count=0,
            transactions={},
            inherited={}
        )

    async def initialize(self, address: str):
        if self.context.connection_id != 0:
            return
        # 通过connection component分配自己一个连接id
        sys = SystemClusters().get_system(self.namespace, 'new_connection')
        ok, _ = await self._execute(sys, address)
        if not ok:
            raise Exception("连接初始化失败，new_connection调用失败")

    async def terminate(self):
        if self.context.connection_id == 0:
            return
        # 释放connection
        sys = SystemClusters().get_system(self.namespace, 'del_connection')
        await self._execute(sys)

    def call_check(self, call: SystemCall) -> SystemDefine | None:
        """检查调用是否合法"""
        # 读取保存的system define
        sys = SystemClusters().get_system(self.namespace, call.system)
        if not sys:
            logger.warning(f"⚠️ [📞Executor] 不存在的System, 检查是否非法调用：{call}")
            return None

        context = self.context
        # 检查权限是否符合
        match sys.permission:
            case Permission.USER:
                if context.caller is None or context.caller == 0:
                    logger.warning(f"⚠️ [📞Executor] {call.system}无调用权限，检查是否非法调用：{call}")
                    return None
            case Permission.ADMIN:
                if context.group is None or not context.group.startswith("admin"):
                    logger.warning(f"⚠️ [📞Executor] {call.system}无调用权限，检查是否非法调用：{call}")
                    return None

        # 检测args数量是否对得上
        if len(call.args) < (sys.arg_count - sys.defaults_count - 3):
            logger.warning(f"❌ [📞Executor] {call.system}参数数量不对，检查客户端代码。"
                           f"要求{sys.arg_count - sys.defaults_count}个参数, "
                           f"传入了{len(call.args)}个。"
                           f"调用内容：{call}")
            return None

        return sys

    async def _execute(self, sys: SystemDefine, *args) -> tuple[bool, dict | None]:
        """
        实际调用逻辑，无任何检查
        调用成功返回True，System返回值
        只有事务冲突超出重试次数时返回False, None
        """
        # 开始调用
        sys_name = sys.func.__name__
        # logger.debug(f"⌚ [📞Executor] 调用System: {sys_name}")

        # 初始化context值
        context = self.context
        context.retry_count = 0
        context.timestamp = time.time()
        context.inherited = {}
        context.transactions = {}

        # 获取system引用的第一个component的backend，system只能引用相同backend的组件，所以都一样
        comp_mgr = self.comp_mgr
        first_comp = next(iter(sys.full_components), None)
        backend = first_comp and comp_mgr.get_table(first_comp).backend or None

        # 复制inherited函数
        for base_name in sys.full_bases:
            context.inherited[base_name] = SystemClusters().get_system(
                self.namespace, base_name).func

        # 调用系统
        while context.retry_count < sys.max_retry:
            # 开始新的事务，并attach components
            trx = None
            if len(sys.full_components) > 0:
                trx = backend.transaction(sys.cluster_id)
                for comp in sys.full_components:
                    tbl = comp_mgr.get_table(comp)
                    context.transactions[comp] = tbl.attach(trx)
            # 执行system和事务
            try:
                rtn = await sys.func(context, *args)
                if trx is not None:
                    await trx.end_transaction(discard=False)
                # logger.debug(f"✅ [📞Executor] 调用System成功: {sys_name}")
                return True, rtn
            except RaceCondition:
                context.retry_count += 1
                # 重试时sleep一段时间，可降低再次冲突率约90%。
                # delay增加会降低冲突率，但也会增加rtt波动。除1:-94%, 2:-91%, 5: -87%, 10: -85%
                delay = random.random() / 5
                logger.debug(f"⌚ [📞Executor] 调用System遇到竞态: {sys_name}，{delay}秒后重试")
                await asyncio.sleep(delay)
                continue
            except Exception as e:
                logger.exception(f"❌ [📞Executor] 系统调用异常，调用：{sys_name}{args}，异常：{e}")
                return False, None
            finally:
                if trx is not None:
                    # 上面如果执行过end_transaction了，那么这句不生效的，主要用于保证连接关闭
                    await trx.end_transaction(discard=True)

        logger.debug(f"✅ [📞Executor] 调用System失败, 超过{sys_name}重试次数{sys.max_retry}")
        return False, None

    async def execute(self, call: SystemCall) -> tuple[bool, dict | None]:
        """
        调用System，返回True表示调用成功，
        返回False表示内部失败或非法调用，此时需要立即调用terminate断开连接
        """
        # 检查call参数和call权限
        sys = self.call_check(call)
        if sys is None:
            return False, None

        # 直接数据库检查connect数据是否是自己(可能被别人踢了)，以及要更新last activate
        # 此方法无法通过事务，判断后有其他进程修改了conn.owner问题也不大
        conn_tbl = self.comp_mgr.get_table(Connection)
        caller, conn_id = self.context.caller, self.context.connection_id
        if caller and caller > 0:
            conn = await conn_tbl.direct_get(conn_id)
            if conn is None or conn.owner != caller:
                logger.warning(f"⚠️ [📞Executor] 当前连接数据已删除，可能已被踢出，将断开连接。调用：{call}")
                return False, None
        # 每2秒更新次last_active，防止批量操作时频繁更新
        now = time.time()
        if now - self.last_active > 30:
            await conn_tbl.direct_set(self.context.connection_id, last_active=now)
            self.last_active = now

        # 开始调用
        return await self._execute(sys, *call.args)

    async def exec(self, name: str, *args):
        """execute的便利调用方法"""
        return await self.execute(SystemCall(name, args))
