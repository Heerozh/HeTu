"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: MIT 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""
import numpy as np
from dataclasses import dataclass
from typing import Callable
import logging
import traceback
import asyncio
import random
import time
from ..data import BaseComponent, define_component, Property, Permission
from ..data.backend import RaceCondition, ComponentTransaction
from ..manager import ComponentTableManager
from ..system import SystemClusters, define_system, SystemDefine

logger = logging.getLogger('HeTu')

SYSTEM_CALL_TIMEOUT = 60 * 2


@dataclass
class Context:
    # 全局变量
    caller: int | None  # 调用方的user id，如果你执行过`elevate()`，此值为传入的`user_id`
    connection_id: int  # 调用方的connection id
    group: str | None  # 所属组名，目前只用于判断是否admin
    user_data: dict  # 当前连接的用户数据，可自由设置，在所有System间共享
    # 事务变量
    timestamp: int  # 调用时间戳
    retry_count: int  # 当前事务冲突重试次数
    transactions: dict[type[BaseComponent], ComponentTransaction]  # 当前事务的Table实例
    inherited: dict[str, callable]  # 继承的父事务函数

    def __getitem__(self, item: type[BaseComponent] | str) -> ComponentTransaction | Callable:
        if type(item) is str:
            return self.inherited[item]
        else:
            return self.transactions[item]

    async def end_transaction(self, discard: bool = False):
        comp_trx = next(iter(self.transactions.values()), None)
        if comp_trx is not None:
            self.transactions = {}
            return await comp_trx.attached.end_transaction(discard)


@dataclass
class SystemCall:
    system: str  # 目标system名
    args: tuple  # 目标system参数


class SystemResult:
    pass


class SystemResponse(SystemResult):
    """回报message给客户端，注意必须是json可以序列化的数据"""
    def __init__(self, message: list | dict):
        self.message = message


@define_component(namespace='HeTu', persist=False)
class Connection(BaseComponent):
    owner: np.int64 = Property(0, index=True)
    address: str = Property('', dtype='<U32')  # 连接地址
    device: str = Property('', dtype='<U32')  # 物理设备名
    device_id: str = Property('', dtype='<U128')  # 设备id
    admin: str = Property('', dtype='<U16')  # 是否是admin
    created: np.double = Property(0)  # 连接创建时间
    last_active: np.double = Property(0)  # 最后活跃时间
    received_msgs: np.int32 = Property(0)  # 收到的消息数, 用来判断fooding攻击
    invalid_msgs: np.int32 = Property(0)  # 无效消息数, 用来判断fooding攻击


@define_system(namespace='__auto__', permission=Permission.EVERYBODY, components=(Connection,))
async def new_connection(ctx: Context, address: str):
    row = Connection.new_row()
    row.owner = 0
    row.created = time.time()
    row.last_active = row.created
    row.address = address
    await ctx[Connection].insert(row)
    row_ids = await ctx.end_transaction()
    ctx.connection_id = row_ids[0]


@define_system(namespace='__auto__', permission=Permission.EVERYBODY, components=(Connection,))
async def del_connection(ctx: Context):
    try:
        await ctx[Connection].delete(ctx.connection_id)
    except KeyError:
        pass


@define_system(namespace='__auto__', permission=Permission.EVERYBODY, components=(Connection,))
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
        if kick_logged_in or now - logged_conn.last_active > SYSTEM_CALL_TIMEOUT:
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


class SystemExecutor:
    """
    每个连接一个SystemExecutor实例。
    """

    def __init__(self, namespace: str):
        self.namespace = namespace
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
            logger.warning(f"⚠️ [📞Worker] 不存在的System, 检查是否非法调用：{call}")
            return None

        context = self.context
        # 检查权限是否符合
        match sys.permission:
            case Permission.USER:
                if context.caller is None or context.caller == 0:
                    logger.warning(f"⚠️ [📞Worker] {call.system}无调用权限，检查是否非法调用：{call}")
                    return None
            case Permission.ADMIN:
                if context.group is None or not context.group.startswith("admin"):
                    logger.warning(f"⚠️ [📞Worker] {call.system}无调用权限，检查是否非法调用：{call}")
                    return None

        # 检测args数量是否对得上
        if len(call.args) < (sys.arg_count - sys.defaults_count - 3):
            logger.warning(f"❌ [📞Worker] {call.system}参数数量不对，检查客户端代码。"
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
        logger.debug(f"⌚ [📞Worker] 调用System: {sys_name}")

        # 初始化context值
        context = self.context
        context.retry_count = 0
        context.timestamp = time.time()
        context.inherited = {}
        context.transactions = {}

        first_comp = next(iter(sys.full_components), None)
        backend = first_comp and ComponentTableManager().get_table(first_comp).backend or None

        # 复制inherited函数
        for inh_name in sys.full_inherits:
            context.inherited[inh_name] = SystemClusters().get_system(self.namespace, inh_name).func

        # 调用系统
        comp_mgr = ComponentTableManager()
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
                logger.debug(f"✅ [📞Worker] 调用System成功: {sys_name}")
                return True, rtn
            except RaceCondition:
                context.retry_count += 1
                delay = random.random() / 5  # 重试时为了防止和另一个再次冲突，用随机值0-0.2秒范围
                logger.debug(f"⌚ [📞Worker] 调用System遇到竞态: {sys_name}，{delay}秒后重试")
                await asyncio.sleep(delay)
                continue
            except Exception as e:
                logger.exception(f"❌ [📞Worker] 系统调用异常，调用：{sys_name}{{{args}}}，异常：{e}")
                logger.exception(traceback.format_exc())
                logger.exception("------------------------")
                return False, None

        logger.debug(f"✅ [📞Worker] 调用System失败, 超过{sys_name}重试次数{sys.max_retry}")
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
        conn_tbl = ComponentTableManager().get_table(Connection)
        caller, conn_id = self.context.caller, self.context.connection_id
        if caller and caller > 0:
            conn = await conn_tbl.direct_get(conn_id)
            if conn is None or conn.owner != caller:
                logger.warning(f"⚠️ [📞Worker] 当前连接数据已删除，可能已被踢出，将断开连接。调用：{call}")
                return False, None
        await conn_tbl.direct_set(self.context.connection_id, last_active=time.time())

        # 开始调用
        return await self._execute(sys, *call.args)

    async def exec(self, name: str, *args):
        """execute的便利调用方法"""
        return await self.execute(SystemCall(name, args))
