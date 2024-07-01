"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: MIT 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""
import numpy as np
from dataclasses import dataclass
from ..data import (BaseComponent, define_component, Property, Permission,
                    RaceCondition, ComponentTransaction)
from ..manager import ComponentTableManager
from ..system import SystemClusters
import logging
import traceback
import asyncio
import random
from datetime import datetime
logger = logging.getLogger('HeTu')


@dataclass
class Context:
    caller: int | None  # 调用方的entity id
    connection_id: int  # 调用方的connection id
    timestamp: int      # 调用时间戳
    group: str | None   # 所属组名，目前只用于判断是那组admin
    retry_count: int    # 当前事务冲突重试次数
    transactions: dict[type[BaseComponent], ComponentTransaction]  # 当前事务的Table实例
    inherited: dict[str, callable]     # 继承的父事务函数

    def __getattr__(self, item: type[BaseComponent]) -> ComponentTransaction:
        return self.transactions[item]

    async def end_transaction(self, discard: bool = False):
        comp_trx = next(iter(self.transactions.values()), None)
        if comp_trx is not None:
            return await comp_trx.attached.end_transaction(discard)


@dataclass
class SystemCall:
    system: str        # 目标system名
    args: tuple        # 目标system参数


@define_component(namespace='HeTu', persist=False)
class Connection(BaseComponent):
    owner: np.int64 = Property(0, index=True)
    address: 'U32' = Property('', index=True)  # 连接地址
    device: 'U32' = Property('', index=True)  # 物理设备名
    device_id: 'U128' = Property('', index=True)  # 设备id
    admin: bool = Property(False, index=True)  # 是否是admin
    created: np.int64 = Property(0, index=True)  # 连接创建时间
    last_active: np.double = Property(0, index=True)  # 最后活跃时间
    received_msgs: np.int32 = Property(0, index=True)  # 收到的消息数, 用来判断fooding攻击
    invalid_msgs: np.int32 = Property(0, index=True)  # 无效消息数, 用来判断fooding攻击


class SystemDispatcher:
    """
    每个连接一个SystemDispatcher实例。
    """
    def __init__(self, namespace: str):
        self.namespace = namespace
        self.context = Context(
            caller=None,
            connection_id=0,
            timestamp=0,
            group=None,
            retry_count=0,
            transactions={},
            inherited={}
        )

    async def initialization(self):
        # 通过connection component分配自己一个连接id
        # 这代码有点长，是不是可以包装下？
        connection_table = ComponentTableManager().get_table(Connection)
        while True:
            trx, tbl = connection_table.new_transaction()
            try:
                async with trx:
                    row = Connection.new_row()
                    row.owner = 0
                    row.created = datetime.now().timestamp()
                    await tbl.insert(row)
                    row_ids = await trx.end_transaction(False)
                break
            except RaceCondition:
                continue
        connection_id = row_ids[0]

        # 好像component的namespace无所谓，只要系统namespace是auto就行？
        self.context.connection_id = connection_id

    async def dispatch(self, call: SystemCall) -> tuple[bool, dict | None]:
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

        first_comp = next(iter(sys.full_components), None)
        backend = first_comp and ComponentTableManager().get_table(first_comp).backend or None
        comp_mgr = ComponentTableManager()

        # 复制inherited函数
        for inh_name in sys.full_inherits:
            context.inherited[inh_name] = SystemClusters().get_system(self.namespace, inh_name).func

        # 调用系统
        while context.retry_count < sys.max_retry:
            # 开始新的事务，并attach components
            context.transactions = {}
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
                delay = random.random() / 5   # 重试时为了防止和另一个再次冲突，用随机值0-0.2秒范围
                await asyncio.sleep(delay)
                continue
            except Exception as e:
                logger.exception(f"❌ [📞Worker] 系统调用异常，调用：{call}，异常：{e}")
                logger.exception(traceback.format_exc())
                logger.exception("------------------------")
                return False, None

        logger.debug(f"✅ [📞Worker] 调用System失败, 超过{call.system}重试次数{sys.max_retry}")
        return False, None


