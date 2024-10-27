"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""
import asyncio
import datetime
import logging
import random
import time
import uuid

import numpy as np
import warnings

from .context import Context
from .definer import define_system, SystemClusters, SYSTEM_NAME_MAX_LEN
from .execution import ExecutionLock
from .executor import SystemExecutor
from ..data import BaseComponent, define_component, Property, Permission
from ..data.backend import ComponentTable

SYSTEM_CLUSTERS = SystemClusters()
logger = logging.getLogger('HeTu.root')
replay = logging.getLogger('HeTu.replay')


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
    创建一个未来调用任务，到约定时间后会由内部进程执行该System。未来调用储存在FutureCalls组件中，服务器重启不会丢失。
    timeout不为0时，则保证目标System事务一定成功，且只执行一次。
    只执行一次的保证通过call_lock引发的事务冲突实现，要求定义System时开启call_lock。

    Notes
    -----
    * System执行时的Context是内部服务，而不是用户连接，无法获取用户ID，要自己作为参数传入
    * 触发精度<=1秒，由每个Worker每秒运行一次循环检查并触发

    Parameters
    ----------
    ctx: Context
        System默认变量
    at: float
        正数是执行的绝对时间(POSIX时间戳)；负数是相对时间，表示延后几秒执行。
    system: str
        未来调用的目标system名
    *args
        目标system的参数，注意，只支持可以通过repr转义为string并不丢失信息的参数，比如基础类型。
    timeout: int
        再次调用时间（秒）。如果超过这个时间System调用依然没有成功，就会再次触发调用。
        注意：代码错误/数据库错误也会引发timeout重试。如果是代码错误，虽然重试大概率还是失败，但任务并不会丢失，
             等程序员修复完代码任务会再次伟大

        如果设为0，则不重试，因此不保证任务成功，甚至会丢失。执行时遇到任何错误/程序关闭/Crash，则未来调用丢失。

        如果timeout再次触发时前一次执行还未完成，会引起事务竞态，其中一个事务会被抛弃。如果前一次已经成功执行，
        call_lock会触发，跳过执行。
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

    args_str = repr(args)
    if len(args_str) > 1024:
        raise ValueError(f"args长度超过1024字符: {len(args_str)}")

    try:
        revert = eval(args_str)
    except Exception as e:
        raise AssertionError("args无法通过eval还原") from e
    assert revert == args, "args通过eval还原丢失了信息"

    assert not recurring or timeout != 0, "recurring=True时timeout不能为0"

    # 读取保存的system define，检查是否开了call lock
    sys = SYSTEM_CLUSTERS.get_system(system)
    if not sys:
        raise RuntimeError(f"⚠️ [⚙️Future] [致命错误] 不存在的System {system}")
    lk = any(comp == ExecutionLock or comp.master_ == ExecutionLock for comp in sys.full_components)
    if not lk:
        raise RuntimeError(f"⚠️ [⚙️Future] [致命错误] System {system} 定义未开启 call_lock")

    if sys.permission == Permission.USER:
        warnings.warn(f"⚠️ [⚙️Future] [警告] 未来任务的目标 {system} 为{sys.permission.name}权限，"
                      f"建议设为Admin防止客户端随意调用。"
                      f"且未来调用为后台任务，执行时Context无用户信息")
    elif sys.permission != Permission.ADMIN:
        warnings.warn(f"⚠️ [⚙️Future] [警告] 未来任务的目标 {system} 为{sys.permission.name}权限，"
                      f"建议设为Admin防止客户端随意调用。")

    # 创建
    _uuid = uuid.uuid4().hex
    async with ctx[FutureCalls].update_or_insert(_uuid, 'uuid') as row:
        row.owner = ctx.caller or -1
        row.system = system
        row.args = args_str
        row.recurring = recurring
        row.created = time.time()
        row.last_run = 0
        row.scheduled = at
        row.timeout = timeout

    return _uuid


async def clean_expired_call_locks(comp_mgr):
    """清空超过7天的call_lock的已执行uuid数据，只有服务器非正常关闭才可能遗留这些数据，因此只需服务器启动时调用。"""
    for comp in [ExecutionLock] + list(ExecutionLock.instances_.values()):
        tbl = comp_mgr.get_table(comp)
        if tbl is None: # 说明项目没任何地方引用此Component
            continue
        backend = tbl.backend
        deleted = 0
        while True:
            async with backend.transaction(tbl.cluster_id) as trx:
                tbl_trx = tbl.attach(trx)
                rows = await tbl_trx.query(
                    'called',
                    left=0, right=time.time() - datetime.timedelta(days=7).total_seconds(),
                    limit=1000)
                # 循环每行数据，删除
                for row in rows:
                    await tbl_trx.delete(row.id)
                deleted += len(rows)
                if len(rows) == 0:
                    break
        logger.info(f"🔗 [⚙️Future] 释放了 {comp.component_name_} 的 {deleted} 条过期数据")


async def sleep_for_upcoming(tbl: ComponentTable):
    """等待下一个即将到期的任务，返回是否有任务"""
    # query limit=1 获得即将到期任务(1秒内）
    calls = await tbl.direct_query('scheduled', left=0, right=time.time() + 1, limit=1,
                                   row_format='raw')
    # 如果无任务，则sleep并continue
    if not calls:
        await asyncio.sleep(1)
        return False

    # sleep将到期时间
    seconds_left = float(calls[0]['scheduled']) - time.time()
    await asyncio.sleep(seconds_left)
    return True


async def pop_upcoming_call(tbl: ComponentTable):
    """取出并修改到期任务"""
    async with tbl.backend.transaction(tbl.cluster_id) as trx:
        tbl_trx = tbl.attach(trx)
        # 取出最早到期的任务
        now = time.time()
        calls = await tbl_trx.query('scheduled', left=0, right=now + 0.1, limit=1)
        # 检查可能被其他worker消费了
        if calls.size == 0:
            return None
        call = calls[0]
        # update到期的任务scheduled属性+timeout时间，如果为0则删除任务
        if call.timeout == 0:
            await tbl_trx.delete(call.id)
        else:
            call.scheduled = now + call.timeout
            call.last_run = now
            await tbl_trx.update(call.id, call)
    return call


async def exec_future_call(call: np.record, executor: SystemExecutor, tbl: ComponentTable):
    # 准备System
    sys = SYSTEM_CLUSTERS.get_system(call.system)
    if not sys:
        logger.error(f"❌ [⚙️Future] 不存在的System, 检查是否代码修改删除了该System：{call.system}")
        return False
    args = eval(call.args)
    # 循环任务和立即删除的任务都不需要lock
    req_call_lock = not call.recurring and call.timeout != 0
    # 执行
    if req_call_lock:
        ok, res = await executor.execute_(sys, *args, uuid=call.uuid)
    else:
        ok, res = await executor.execute_(sys, *args)
    if replay.level < logging.ERROR:  # 如果关闭了replay，为了速度不执行下面的字符串序列化
        replay.info(f"[SystemResult][{call.system}]({ok}, {str(res)})")
    # 执行成功后，删除未来调用。如果代码错误/数据库错误，会下次重试
    if ok and req_call_lock:
        async with tbl.backend.transaction(tbl.cluster_id) as trx:
            tbl_trx = tbl.attach(trx)
            await tbl_trx.delete(call.id)
        # 再删除call_lock uuid数据，只有ok的执行才有call lock
        await executor.remove_call_lock(call.system, call.uuid)
    return True


async def future_call_task(app):
    """
    未来调用的后台task，每个Worker启动时会开一个，执行到期的未来调用。
    """
    comp_mgr = app.ctx.comp_mgr

    # 启动时清空超过7天的call_lock的已执行uuid数据
    await clean_expired_call_locks(comp_mgr)

    # 随机sleep一段时间，错开各worker的执行时间
    await asyncio.sleep(random.random())

    # 初始化task的执行器
    executor = SystemExecutor(app.config['NAMESPACE'], comp_mgr)
    await executor.initialize('localhost')
    logger.info(f"🔗 [⚙️Future] 新Task：{asyncio.current_task().get_name()}")
    # 获取所有未来调用组件
    comp_tables = [comp_mgr.get_table(FutureCalls)]
    if comp_tables[0] is None:  # 可能主组件没人使用
        comp_tables = []
    comp_tables += [comp_mgr.get_table(comp) for comp in FutureCalls.instances_.values()]
    # 不能通过subscriptions订阅组件获取调用的更新，因为订阅消息不保证可靠会丢失，导致部分任务可能卡很久不执行
    # 所以这里使用最基础的，每一段时间循环的方式
    while True:
        # 随机选一个未来调用组件
        tbl = random.choice(comp_tables)
        try:
            # 等待0-1秒直到下一个即将到期的任务，如果没有任务则重新循环
            if not await sleep_for_upcoming(tbl):
                continue

            # 取出并修改到期任务的事务, 此时如果服务器关闭，事务还未执行到提交，任何数据不会丢失
            if not (call := await pop_upcoming_call(tbl)):
                continue

            # 执行任务, 此时call已被取出，如果服务器关闭/数据库断线，timeout=0的任务会丢失
            await exec_future_call(call, executor, tbl)
        except asyncio.CancelledError:
            break
        except Exception as e:
            # 遇到backend断线正常，其他异常不应该发生
            err_msg = f"❌ [⚙️Future] Task执行异常：{e}"
            logger.exception(err_msg)
