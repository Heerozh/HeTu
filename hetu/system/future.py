"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""
import logging
import random
import time
import uuid

import numpy as np
from datetime import datetime

from .context import Context
from .execution import ExecutionLock
from ..data import BaseComponent, define_component, Property, Permission
from ..manager import ComponentTableManager
from ..system import define_system, SystemClusters, SystemDefine
from ..system.definer import SYSTEM_NAME_MAX_LEN
from ..safelogging.filter import ContextFilter

SYSTEM_CLUSTERS = SystemClusters()
logger = logging.getLogger('HeTu.root')
replay = logging.getLogger('HeTu.replay')

# 首先是create一个未来调用,
#    存到数据库, 包含到期时间索引
# 每个worker在服务器启动时开一个后台task，
#   head启动时清空excitor的已执行id数据，只包括excitor的uuid不存在FutureCalls里的
#   随机休眠一段时间，减少竞态
#   循环开始
#     休眠1秒
#     循环所有FutureCalls副本
#       对该副本创建事务
#       query limit=1获得到期任务
#       update到期的任务scheduled属性为新的timeout时间，如果为0则删除任务
#       break
#     执行到期的任务
#     看执行的结果正常的话，则删除任务
#     不正常则log并不管继续循环

@define_component(namespace='HeTu', persist=True, permission=Permission.ADMIN)
class FutureCalls(BaseComponent):
    owner: np.int64 = Property(0, index=True)                     # 创建方
    uuid: str = Property('', dtype='<U32', unique=True)           # 唯一标识
    system: str = Property('', dtype=f'<U{SYSTEM_NAME_MAX_LEN}')  # 目标system名
    args: str = Property('', dtype='<U1024')                      # 目标system参数
    recurring: bool = Property(False)                             # 是否永不结束重复触发
    created: np.double = Property(0)                              # 创建时间
    last_run: np.double = Property(0)                             # 最后执行时间
    scheduled: np.double = Property(0, index=True)                # 计划执行时间
    timeout: np.int32 = Property(60)                              # 再次调用时间（秒）


# permission设为admin权限阻止客户端调用
@define_system(namespace='global', permission=Permission.ADMIN, components=(FutureCalls,))
async def create_future_call(ctx: Context, at: float, system: str, *args, timeout: int = 60,
                             recurring: bool = False):
    """
    创建一个未来调用，到约定时间后会由内部进程执行该System。未来调用储存在FutureCalls组件中，服务器重启不会丢失。
    timeout不为0时，则保证目标System事务一定成功，且只执行一次。
    只执行一次的保证使用call_lock实现，要求定义System时开启call_lock。

    Notes
    -----
    * System执行时的Context是内部服务，而不是用户连接，无法获取用户ID，要自己作为参数传入
    * 触发精度<=1秒，由每个Node每秒运行一次循环检查并触发

    Parameters
    ----------
    ctx: Context
        创建方context
    at: float
        正数是执行的绝对时间(POSIX时间戳)；负数是相对时间，表示延后几秒执行。
    system: str
        未来调用的目标system名
    *args
        目标system的参数
    timeout: int
        再次调用时间（秒）。超过这个时间依然没有System事务成功的记录，就会再次触发调用。
        如果设为0，则不保证任务成功。比如执行时遇到服务器宕机/Crash，则未来调用丢失。

        如果timeout设的太低，再次触发时前一次还未完成，会引起事务竞态，其中一个事务会被抛弃。
        * 注意：抛弃的只有事务(所有ctx[components]的操作)，修改全局变量、写入文件等操作是永久的
        * 注意：`ctx.retry_count`只是事务冲突的计数，timeout引起的再次触发会从0重新计数
    recurring: bool
        设置后，将永不删除此未来调用，每次执行后按timeout时间再次执行。

    Returns
    -------
    返回未来调用的uuid

    Examples
    --------
    >>> @define_system(namespace='test', permission=Permission.ADMIN)
    ... def test_future_call(ctx: Context, *args):
    ...     print('Future call test', args)
    >>> @define_system(namespace='test', permission=Permission.ADMIN, bases=('create_future_call:test') )
    ... def test_future_create(ctx: Context):
    ...     ctx['create_future_call:test'](ctx, -10, 'test_future_call', 'arg1', 'arg2', timeout=5)

    示例中，`bases`继承使用':'符号创建了`create_future_call`的test副本。
    继承System会和对方的簇合并，而`create_future_call`是常用System，所以使用副本避免System簇过于集中，
    增加backend的扩展性，具体参考簇相关的文档。

    """
    # 参数检查
    timeout = max(timeout, 5) if timeout != 0 else 0
    at = time.time() + abs(at) if at <= 0 else at

    if len(args_str := ','.join(map(str, args))) > 1024:
        raise ValueError(f"args长度超过1024字符: {len(args_str)}")

    # 读取保存的system define，检查是否开了call lock
    sys = SYSTEM_CLUSTERS.get_system(system)
    if not sys:
        raise RuntimeError(f"⚠️ [📞Future] [致命错误] 不存在的System {system}")
    lk = any(comp == ExecutionLock or comp.master_ == ExecutionLock for comp in sys.full_components)
    if not lk:
        raise RuntimeError(f"⚠️ [📞Future] [致命错误] System {system} 定义未开启 call_lock")

    # 创建
    _uuid = uuid.uuid4().hex
    async with ctx[FutureCalls].update_or_insert(_uuid, 'uuid') as row:
        row.owner = ctx.caller
        row.system = system
        row.args = args_str
        row.recurring = recurring
        row.created = time.time()
        row.last_run = 0
        row.scheduled = at
        row.timeout = timeout


async def pop_expired_future_call(table):
    """
    从FutureCalls表中取出最早到期的任务，如果到期则返回，否则返回None
    """
    # 取出最早到期的任务
    while True:
        try:
            async with self._backend.transaction(self._cluster_id) as trx:
                tbl = self.attach(trx)
                row = await tbl.select(row_id)
                if row is None:
                    raise KeyError(f"direct_set: row_id {row_id} 不存在")
                for prop, value in kwargs.items():
                    if prop in self._component_cls.prop_idx_map_:
                        row[prop] = value
                await tbl.update(row_id, row)
            return
        except RaceCondition:
            await asyncio.sleep(random.random() / 5)
            continue
        except Exception:
            await trx.end_transaction(discard=True)
            raise

async def future_call_worker(app):
    """
    未来调用的worker，每个Node启动时会开一个，执行到期的未来调用。
    """

    from hetu.system import  SystemExecutor, SystemCall, ResponseToClient
    from hetu.data.backend import Subscriptions, Backend, HeadLockFailed

    import asyncio

    # 随机sleep一段时间，减少竞态
    await asyncio.sleep(random.random())

    # 启动时清空executor的已执行uuid数据，只包括不存在FutureCalls里的


    # 初始化worker的执行器
    comp_mgr = app.ctx.comp_mgr
    executor = SystemExecutor(app.config['NAMESPACE'], comp_mgr)
    await executor.initialize('localhost')
    logger.info(f"🔗 [⚙️Future] 新Worker：{asyncio.current_task().get_name()}")
    # 不能通过subscriptions订阅组件获取调用的更新，因为订阅消息可能丢失，导致部分任务可能卡很久不执行
    while True:
        await asyncio.sleep(1)

        # 获得所有FutureCalls和副本的到期数据
        if not (expired := pop_expired_future_call(FutureCalls)):
            for suffix, comp in FutureCalls.instances_.items():
                if expired := pop_expired_future_call(comp):
                    break
        if not expired:
            continue
        # 执行到期的未来调用
        call = SystemCall(data[1], tuple(data[2:]))
        ok, res = await executor.execute(call)
        if replay.level < logging.ERROR:  # 如果关闭了replay，为了速度不执行下面的字符串序列化
            replay.info(f"[SystemResult][{data[1]}]({ok}, {str(res)})")



    # # 通过订阅获取到期调用，增加一些检查的错开事件，防止所有worker都在竞态
    # subscriptions = Subscriptions(app.ctx.default_backend)
    # table = comp_mgr.get_table(FutureCalls)
    # await subscriptions.subscribe_query(
    #     table, 'admin', 'scheduled',
    #     0, 0xFFFFFFFF, limit=1, force=True)
    #
    # while True:
    #     # mq_pull大部分情况下只在收到通知后返回
    #     不行，订阅的消息是可能丢失的，不是保证一定能收到的，可能会导致丢失任务（如果订阅index，不会丢弃，但是可能任务执行时间卡很久）。
    #     还是做成每秒检查，简洁清晰？
    #
    #     await subscriptions.mq_pull()
    #     if subscriptions.
    #     不过subscipts也是用sleep来定时取数据的，不如我sleep query



