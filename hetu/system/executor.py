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
from datetime import datetime
from ..data import BaseComponent, define_component, Property, Permission
from ..data.backend import RaceCondition, ComponentTransaction
from ..manager import ComponentTableManager
from ..system import SystemClusters, define_system

logger = logging.getLogger('HeTu')


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
    row.created = datetime.now().timestamp()
    row.last_active = row.created
    row.address = address
    await ctx[Connection].insert(row)
    row_ids = await ctx.end_transaction()
    ctx.connection_id = row_ids[0]


@define_system(namespace='__auto__', permission=Permission.EVERYBODY, components=(Connection,))
async def del_connection(ctx: Context):
    await ctx[Connection].delete(ctx.connection_id)


@define_system(namespace='__auto__', permission=Permission.EVERYBODY, components=(Connection,))
async def elevate(ctx: Context, user_id: int):
    """
    提升到User权限。如果该连接已提权，或user_id已在其他连接登录，返回False。
    如果成功，则ctx.caller会被设置为user_id，同时事务结束，之后将无法调用ctx[Components]
    """
    assert ctx.connection_id != 0, "请先初始化连接"

    # 如果当前连接已提权
    if ctx.caller is not None and ctx.caller > 0:
        return False, 'CURRENT_CONNECTION_ALREADY_ELEVATED'
    # 如果此用户已经登录
    exist, _ = await ctx[Connection].is_exist(user_id, 'owner')
    if exist:
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
        ok, _ = await self.run_('new_connection', address)
        if not ok:
            raise Exception("连接初始化失败，new_connection调用失败")

    async def terminate(self):
        if self.context.connection_id == 0:
            return
        # 释放connection
        await self.run_('del_connection')

    async def run(self, call: SystemCall) -> tuple[bool, dict | None]:
        # 读取保存的system define
        sys = SystemClusters().get_system(self.namespace, call.system)
        if not sys:
            logger.warning(f"⚠️ [📞Worker] 不存在的System, 检查是否非法调用：{call}")
            return False, None

        context = self.context
        # 检查权限是否符合
        match sys.permission:
            case Permission.USER:
                if context.caller is None or context.caller == 0:
                    logger.warning(f"⚠️ [📞Worker] {call.system}无调用权限，检查是否非法调用：{call}")
                    return False, None
            case Permission.ADMIN:
                if context.group is None or not context.group.startswith("admin"):
                    logger.warning(f"⚠️ [📞Worker] {call.system}无调用权限，检查是否非法调用：{call}")
                    return False, None

        # 检测args数量是否对得上
        if len(call.args) < (sys.arg_count - sys.defaults_count - 3):
            logger.warning(f"❌ [📞Worker] {call.system}参数数量不对，检查客户端代码。"
                           f"要求{sys.arg_count - sys.defaults_count}个参数, "
                           f"传入了{len(call.args)}个。"
                           f"调用内容：{call}")
            return False, None

        logger.debug(f"⌚ [📞Worker] 调用System: {call.system}")

        # 初始化context值
        context.retry_count = 0
        context.timestamp = datetime.now()
        context.inherited = {}
        context.transactions = {}

        first_comp = next(iter(sys.full_components), None)
        backend = first_comp and ComponentTableManager().get_table(first_comp).backend or None
        comp_mgr = ComponentTableManager()

        # 复制inherited函数
        for inh_name in sys.full_inherits:
            context.inherited[inh_name] = SystemClusters().get_system(self.namespace, inh_name).func

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
                rtn = await sys.func(context, *call.args)
                if trx is not None:
                    await trx.end_transaction(discard=False)
                logger.debug(f"✅ [📞Worker] 调用System成功: {call.system}")
                return True, rtn
            except RaceCondition:
                context.retry_count += 1
                delay = random.random() / 5  # 重试时为了防止和另一个再次冲突，用随机值0-0.2秒范围
                logger.debug(f"⌚ [📞Worker] 调用System遇到竞态: {call.system}，{delay}秒后重试")
                await asyncio.sleep(delay)
                continue
            except Exception as e:
                logger.exception(f"❌ [📞Worker] 系统调用异常，调用：{call}，异常：{e}")
                logger.exception(traceback.format_exc())
                logger.exception("------------------------")
                return False, None

        logger.debug(f"✅ [📞Worker] 调用System失败, 超过{call.system}重试次数{sys.max_retry}")
        return False, None

    async def run_(self, name: str, *args):
        return await self.run(SystemCall(name, args))
