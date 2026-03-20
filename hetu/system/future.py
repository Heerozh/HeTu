"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import asyncio
import logging
import random
import time
import warnings
from typing import TYPE_CHECKING

import numpy as np

from hetu.data.backend import RowFormat

from ..data import BaseComponent, Permission, define_component, property_field
from ..endpoint.definer import ENDPOINT_NAME_MAX_LEN
from ..i18n import _
from .caller import SystemCaller
from .context import SystemContext
from .definer import SystemClusters, define_system
from .lock import SystemLock, clean_expired_call_locks

if TYPE_CHECKING:
    from ..data.backend.table import Table

SYSTEM_CLUSTERS = SystemClusters()
logger = logging.getLogger("HeTu.root")
replay = logging.getLogger("HeTu.replay")


@define_component(namespace="HeTu", permission=Permission.ADMIN)
class FutureCalls(BaseComponent):
    owner: np.int64 = property_field(0, index=True)  # 创建方
    system: str = property_field("", dtype=f"<U{ENDPOINT_NAME_MAX_LEN}")  # 目标system名
    args: str = property_field("", dtype="<U1024")  # 目标system参数
    recurring: bool = property_field(False)  # 是否永不结束重复触发
    created: np.double = property_field(0)  # 创建时间
    last_run: np.double = property_field(0)  # 最后执行时间
    scheduled: np.double = property_field(0, index=True)  # 计划执行时间
    timeout: np.int32 = property_field(60)  # 再次调用时间（秒）


# permission设为admin权限阻止客户端调用
@define_system(namespace="global", permission=None, components=(FutureCalls,))
async def create_future_call(
    ctx: SystemContext,
    at: float,
    system: str,
    *args,
    timeout: int = 60,
    recurring: bool = False,
):
    """
    创建一个未来调用任务，到约定时间后会由内部进程执行该System。
    未来调用储存在FutureCalls组件中，服务器重启不会丢失。
    timeout不为0时，则保证目标System事务一定成功，且只执行一次。
    只执行一次的保证通过call_lock引发的事务冲突实现，会强制要求定义System时开启call_lock。

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
        注意：代码错误/数据库错误也会引发timeout重试。如果是代码错误，虽然重试大概率还是失败，
             但任务并不会丢失，等程序员修复完代码任务会再次伟大

        如果设为0，则不重试，因此不保证任务成功，甚至会丢失。执行时遇到任何错误/程序关闭/Crash，
        则未来调用丢失。

        如果timeout再次触发时前一次执行还未完成，会引起事务竞态，其中一个事务会被抛弃。
        如果前一次已经成功执行，call_lock会触发，跳过执行。
        * 注意：抛弃的只有事务(所有ctx.repo[components]的操作)，修改全局变量、写入文件等操作是永久的
        * 注意：`ctx.race_count`只是事务冲突的计数，timeout引起的再次触发会从0重新计数
    recurring: bool
        设置后，将永不删除此未来调用，每次执行后按timeout时间再次执行。

    Returns
    -------
    返回未来调用的uuid: int

    Examples
    --------
    >>> import hetu
    >>> @hetu.define_system(namespace='test', permission=None)
    ... def test_future_call(ctx: hetu.SystemContext, *args):
    ...     # do ctx.repo[...] operations
    ...     print('Future call test', args)
    >>> @hetu.define_system(namespace='test', permission=hetu.Permission.USER, depends=('create_future_call:test') )
    ... def test_future_create(ctx: hetu.SystemContext):
    ...     ctx.depend['create_future_call:test'](ctx, -10, 'test_future_call', 'arg1', 'arg2', timeout=5)

    示例中，`depends`依赖使用':'符号创建了`create_future_call`的test副本。
    继承System会和对方的簇合并，而`create_future_call`是常用System，所以使用副本避免System簇过于集中，
    增加backend的扩展性，具体参考簇相关的文档。

    """
    # 参数检查
    timeout = max(timeout, 5) if timeout != 0 else 0
    at = time.time() + abs(at) if at <= 0 else at

    args_str = repr(args)
    if len(args_str) > 1024:
        raise ValueError(
            _("args长度超过1024字符: {length}").format(length=len(args_str))
        )

    try:
        revert = eval(args_str)
    except Exception as e:
        raise AssertionError(_("args无法通过eval还原")) from e
    assert revert == args, _("args通过eval还原丢失了信息")

    assert not recurring or timeout != 0, _("recurring=True时timeout不能为0")

    # 读取保存的system define，检查是否开了call lock
    sys = SYSTEM_CLUSTERS.get_system(system)
    if not sys:
        raise RuntimeError(
            _("⚠️ [⚙️Future] [致命错误] 不存在的System {system}").format(system=system)
        )
    lk = any(
        comp == SystemLock or comp.master_ == SystemLock for comp in sys.full_components
    )
    if not lk:
        raise RuntimeError(
            _("⚠️ [⚙️Future] [致命错误] System {system} 定义未开启 call_lock").format(
                system=system
            )
        )

    if sys.permission == Permission.USER:
        warnings.warn(
            _(
                "⚠️ [⚙️Future] [警告] 未来任务的目标 {system} 为{permission}权限，"
                "建议设为None防止客户端调用。"
                "且未来调用为后台任务，执行时Context无用户信息"
            ).format(system=system, permission=sys.permission.name)
        )
    elif sys.permission != Permission.ADMIN and sys.permission is not None:
        warnings.warn(
            _(
                "⚠️ [⚙️Future] [警告] 未来任务的目标 {system} 为{permission}权限，"
                "建议设为None防止客户端调用。"
            ).format(system=system, permission=sys.permission.name)
        )

    # 创建
    row = FutureCalls.new_row()
    row.owner = ctx.caller or -1
    row.system = system
    row.args = args_str
    row.recurring = recurring
    row.created = time.time()
    row.last_run = 0
    row.scheduled = at
    row.timeout = timeout
    await ctx.repo[FutureCalls].insert(row)
    return row.id


async def sleep_for_upcoming(tbl: Table):
    """等待下一个即将到期的任务，返回是否有任务"""
    # query limit=1 获得即将到期任务(1秒内）
    calls = await tbl.servant_range(
        "scheduled", left=0, right=time.time() + 1, limit=1, row_format=RowFormat.RAW
    )
    # 如果无任务，则sleep并continue
    if not calls:
        await asyncio.sleep(1)
        return False

    # sleep将到期时间
    seconds_left = float(calls[0]["scheduled"]) - time.time()
    await asyncio.sleep(seconds_left)
    return True


async def pop_upcoming_call(tbl: Table):
    """取出并修改到期任务"""
    async with tbl.session() as session:
        repo = session.using(tbl.comp_cls)
        # 取出最早到期的任务
        now = time.time()
        calls = await repo.range(scheduled=(0, now + 0.1), limit=1)
        # 检查可能被其他worker消费了
        if calls.size == 0:
            return None
        call = calls[0]
        # update到期的任务scheduled属性+timeout时间，如果为0则删除任务
        if call.timeout == 0:
            repo.delete(call.id)
        else:
            call.scheduled = now + call.timeout
            call.last_run = now
            await repo.update(call)
    return call


async def exec_future_call(call: np.record, caller: SystemCaller, tbl: Table):
    # 准备System
    sys = SYSTEM_CLUSTERS.get_system(call.system)
    if not sys:
        logger.error(
            _(
                "❌ [⚙️Future] 不存在的System, 检查是否代码修改删除了该System：{system}"
            ).format(system=call.system)
        )
        return False
    args = eval(call.args)
    # 循环任务和立即删除的任务都不需要lock
    req_call_lock = not call.recurring and call.timeout != 0
    # 执行
    ok = False
    res = None
    try:
        if req_call_lock:
            res = await caller.call_(sys, *args, uuid=str(call.id))
        else:
            res = await caller.call_(sys, *args)
        ok = True
    except Exception as e:
        err_msg = _(
            "❌ [⚙️Future] 未来调用System异常，调用：{system}{args}，异常：{exc}"
        ).format(system=call.system, args=args, exc=f"{type(e).__name__}:{e}")
        logger.exception(err_msg)
    # 如果关闭了replay，为了速度不执行下面的字符串序列化
    if replay.level < logging.ERROR:
        replay.info(f"[SystemResult][{call.system}]({ok}, {str(res)})")
    # 执行成功后，删除未来调用。如果代码错误/数据库错误，会下次重试
    if ok and req_call_lock:
        async with tbl.session() as session:
            repo = session.using(tbl.comp_cls)
            get_4_del = await repo.get(id=call.id)
            if get_4_del:
                repo.delete(get_4_del.id)
        # 再删除call_lock uuid数据，只有ok的执行才有call lock
        await caller.remove_call_lock(call.system, str(call.id))
    return True


async def future_call_task(app):
    """
    未来调用的后台task，每个Worker启动时会开一个，执行到期的未来调用。
    """
    # 获取当前协程任务, 自身算是一个协程1
    current_task = asyncio.current_task()
    assert current_task, "Must be called in an asyncio task"
    logger.info(
        _("🔗 [⚙️Future] 新Task：{task_name}").format(task_name=current_task.get_name())
    )

    # 启动时清空超过7天的call_lock的已执行uuid数据
    for tbl_mgr in app.ctx.table_managers.values():
        await clean_expired_call_locks(tbl_mgr)

    # 随机sleep一段时间，错开各worker的执行时间
    await asyncio.sleep(random.random())

    # 初始化Context
    context = SystemContext(
        caller=0,
        connection_id=0,
        address="localhost",
        group="guest",
        user_data={},
        timestamp=0,
        request=None,  # type: ignore
        systems=None,  # type: ignore
    )

    # 初始化task的执行器
    callers = {
        instance: SystemCaller(app.config["NAMESPACE"], tbl_mgr, context)
        for instance, tbl_mgr in app.ctx.table_managers.items()
    }

    # 获取所有未来调用组件
    future_call_tables: list[Table] = []
    for tbl_mgr in app.ctx.table_managers.values():
        main_table = tbl_mgr.get_table(FutureCalls)
        if main_table is not None:  # 可能主组件没人使用
            future_call_tables.append(main_table)
        duplicates = FutureCalls.get_duplicates(tbl_mgr.namespace).values()
        future_call_tables += [
            tbl_mgr.get_table(comp)
            for comp in duplicates
            if tbl_mgr.get_table(comp) is not None
        ]

    # 不能通过SubscriptionBroker订阅组件获取调用的更新，因为订阅消息不保证可靠会丢失，
    # 导致部分任务可能卡很久不执行，所以这里使用最基础的，每一段时间循环的方式
    # 如果有很多个instance，可能worker个task来不及处理这么多future表?
    # 应该不会，如果堆积，sleep_for_upcoming并不会sleep，会循环到处理完的
    while True:
        # 随机选一个未来调用组件
        tbl = random.choice(future_call_tables)
        try:
            # 等待0-1秒直到下一个即将到期的任务，如果没有任务则重新循环
            if not await sleep_for_upcoming(tbl):
                continue

            # 取出并修改到期任务的事务, 此时如果服务器关闭，事务还未执行到提交，任何数据不会丢失
            if not (call := await pop_upcoming_call(tbl)):
                continue

            # 执行任务, 此时call已被取出，如果服务器关闭/数据库断线，timeout=0的任务会丢失
            await exec_future_call(call, callers[tbl.instance_name], tbl)
        except asyncio.CancelledError:
            break
        except Exception as e:
            err_msg = _("❌ [⚙️Future] Task执行异常：{exc}").format(
                exc=f"{type(e).__name__}:{e}"
            )
            logger.exception(err_msg)
