"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import ast
import asyncio
import hashlib
import logging
import random
import time
import warnings
from typing import TYPE_CHECKING

import numpy as np

from ..data import BaseComponent, Permission, define_component, property_field
from ..data.backend import RaceCondition, RowFormat
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


def _key_to_id(key: str) -> int:
    """把 ensure_future_call 的 key 稳定映射到负数 id，用作 FutureCalls 主键去重。

    必须跨进程/重启确定性（不能用内置 hash()，其按进程加盐）；雪花 id 恒正，
    负数区间专供 keyed 行，二者不会相撞。

    Stably map a key to a negative id for primary-key dedup of FutureCalls. Must be
    deterministic across processes/restarts (builtin hash() is per-process salted);
    snowflake ids are always positive, so the negative range is reserved for keyed rows.
    """
    digest = hashlib.blake2b(key.encode("utf-8"), digest_size=8).digest()
    h = int.from_bytes(digest, "big")  # 0 .. 2**64 - 1
    return -(h >> 1) - 1  # -> [-(2**63), -1]：恒负、非 0、落在 int64 范围内


def _build_future_row(
    ctx: SystemContext,
    at: float,
    system: str,
    args: tuple,
    *,
    timeout: int = 60,
    recurring: bool = False,
    id_: int | None = None,
) -> np.record:
    """校验参数并组装一条 FutureCalls 行（不插入）。

    create_future_call / ensure_future_call 共用。``id_`` 为 None 时用雪花 id；
    给定时（ensure 的确定性 id）用显式 id。

    Validate args and build (not insert) a FutureCalls row, shared by
    create_future_call / ensure_future_call. ``id_`` None -> snowflake id; given ->
    explicit id (ensure's deterministic id).
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
        revert = ast.literal_eval(args_str)
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
    row = FutureCalls.new_row(id_=id_)
    row.owner = ctx.caller or -1
    row.system = system
    row.args = args_str
    row.recurring = recurring
    row.created = time.time()
    row.last_run = 0
    row.scheduled = at
    row.timeout = timeout
    return row


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
    ... async def test_future_call(ctx: hetu.SystemContext, *args):
    ...     # do ctx.repo[...] operations
    ...     print('Future call test', args)
    >>> @hetu.define_system(namespace='test', permission=hetu.Permission.USER, depends=('create_future_call:test',) )
    ... async def test_future_create(ctx: hetu.SystemContext):
    ...     await ctx.depend['create_future_call:test'](ctx, -10, 'test_future_call', 'arg1', 'arg2', timeout=5)

    示例中，`depends`依赖使用':'符号创建了`create_future_call`的test副本。
    继承System会和对方的簇合并，而`create_future_call`是常用System，所以使用副本避免System簇过于集中，
    增加backend的扩展性，具体参考簇相关的文档。

    """
    row = _build_future_row(ctx, at, system, args, timeout=timeout, recurring=recurring)
    await ctx.repo[FutureCalls].insert(row)
    return row.id


@define_system(namespace="global", permission=None, components=(FutureCalls,))
async def ensure_future_call(
    ctx: SystemContext,
    key: str,
    at: float,
    system: str,
    *args,
    timeout: int = 60,
    recurring: bool = False,
):
    """按 key 等幂地确保一个未来调用存在；已存在则原样保留（不更新参数），返回其 id。

    与 create_future_call 相同，但用 key 做幂等：同一 key 多次调用只会创建一条。
    适合在 on_start System 里播种"开机即起"的全局循环任务（recurring=True），
    服务器重启多次也不会重复堆积。

    幂等通过 key 的确定性 id 复用 FutureCalls 主键唯一性实现：已存在则直接返回（不写入，
    事务空提交），并发同 key 的多余插入会撞主键引发事务竞态并自动重试，最终只保留一条。

    * 同create_future_call，目标system必须开启call_lock。

    Idempotently ensure a single future call exists, keyed by ``key``; if it already
    exists, keep it as-is (params are NOT updated) and return its id. Useful for seeding
    a server-wide recurring background task from an on_start system without piling up
    duplicates across restarts.

    Parameters
    ----------
    ctx: Context
        System默认变量
    key: str
        幂等键。同一 key 只会存在一条未来调用。
    at: float
        同 create_future_call：正数为绝对 POSIX 时间戳；负数为相对延后秒数。
    system: str
        未来调用的目标 system 名。
    *args
        目标 system 的参数（须能 repr 往返还原，如基础类型）。
    timeout: int
        再次调用时间（秒），含义同 create_future_call；recurring=True 时不能为 0。
    recurring: bool
        设置后永不删除，按 timeout 周期重复触发。

    Returns
    -------
    返回未来调用的 id: int（由 key 推导的确定性负数 id）

    Examples
    --------
    >>> import hetu
    >>> @hetu.define_system(namespace='game', permission=None, call_lock=True,
    ...                     components=(World,))
    ... async def world_tick(ctx): ...
    >>> @hetu.define_system(namespace='game', permission=None, on_start=True,
    ...                     depends=('ensure_future_call:game',))
    ... async def boot(ctx):
    ...     await ctx.depend['ensure_future_call:game'](
    ...         ctx, 'game:world_tick', -30, 'world_tick', recurring=True, timeout=30)

    开服时 on_start 跑一次 → ensure 幂等 → 重启 N 次也只有一条 → 由 future_call_task
    每 ~1 秒轮询、全局只一个 worker 执行、timeout 重试、重启不丢。
    """
    fid = _key_to_id(key)
    if await ctx.repo[FutureCalls].get(id=fid):
        return fid  # 已存在 → no-op（无写入，commit 空转，安全）
    row = _build_future_row(
        ctx, at, system, args, timeout=timeout, recurring=recurring, id_=fid
    )
    await ctx.repo[FutureCalls].insert(row)
    return fid


@define_system(namespace="global", permission=None, components=(FutureCalls,))
async def cancel_future_call(ctx: SystemContext, key: str) -> bool:
    """按 key 删除 ensure_future_call 创建的未来调用（停止 / 重配循环任务）。

    返回 True 表示存在并已删除，False 表示该 key 没有对应的未来调用。重配间隔等参数：
    先 cancel 再 ensure（ensure 是 ensure-exists，不会就地改参数）。

    Cancel a keyed future call created by ensure_future_call. Returns True if it existed
    and was deleted, False otherwise. To reconfigure (e.g. change interval): cancel then
    ensure again.

    Parameters
    ----------
    ctx: Context
        System默认变量
    key: str
        要删除的未来调用的幂等键，与 ensure_future_call 的 key 一致。

    Returns
    -------
    bool: 是否存在并删除
    """
    fid = _key_to_id(key)
    if not await ctx.repo[FutureCalls].get(id=fid):
        return False
    ctx.repo[FutureCalls].delete(fid)
    return True


# 没有到期调用时最多睡多久：新建的调用最迟这么久后被发现
MAX_IDLE_SLEEP = 1.0
# 每张表一轮最多连续取出执行这么多条就去看下一张表，免得一张表的积压饿死别的表
DRAIN_PER_TABLE = 64
# 看到有到期的却一条没取到（被别的 worker 抢先、副本还没同步）时，隔这么久再扫，免得空转
CONTENDED_RETRY_DELAY = 0.05


async def next_due(tbl: Table, horizon: float) -> float | None:
    """
    horizon 之前最早到期的一条调用的 scheduled，没有返回 None。

    只读 servant 上的 scheduled 索引（索引 member 里就编着值），不读行：先读索引再读行是两次
    往返，中间这条可能已被别的 worker 取走、scheduled 改到了 timeout 之后，按读到的值去睡就会
    睡过头（timeout 长的 recurring 调用能睡一小时）。
    """
    upcoming = await tbl.backend.servant.range_index_(
        tbl, "scheduled", 0, horizon, limit=1
    )
    return float(upcoming[0][0]) if upcoming else None


# 出队时从最早到期的这么多条里随机挑一条：多个 worker 同时出队时不全挤在最早那一条上撞车
POP_CANDIDATES = 8
# 出队撞竞态（被别的 worker 抢先）最多换几次；都没抢到就放弃，下一轮再取
POP_ATTEMPTS = 5
# 撞竞态后换一条之前随机等这么久以内，错开同时撞车的 worker
POP_RACE_JITTER = 0.005


async def pop_upcoming_call(tbl: Table) -> np.record | None:
    """
    取出一条到期的调用：scheduled 顺延 timeout 秒作为租约（到时还没执行完、没删掉就会重投），
    timeout 为 0 的直接删。没有到期的、或到期的都被别的 worker 抢走了，返回 None。
    """
    comp_cls = tbl.comp_cls
    for _attempt in range(POP_ATTEMPTS):
        now = time.time()
        # 候选只读索引拿 id，不进事务：区间里不断有新的到期调用插进来，读进事务会反复判竞态
        candidates = await tbl.backend.master_or_servant.range(
            tbl, "scheduled", 0, now + 0.1, POP_CANDIDATES, False, RowFormat.ID_LIST
        )
        if not candidates:
            return None
        call = None
        try:
            async with tbl.session() as session:
                repo = session.using(comp_cls)
                call = await repo.get(id=random.choice(candidates))
                # 读索引和读行是两次往返，中间这条可能已被别的 worker 取走（scheduled 已顺延）
                # 或执行完删掉了。读回的行必须仍然到期：提交时的版本校验只保证读回之后没人再改
                # 它，拦不住读回之前就被取走的，那样两个 worker 会各执行一遍
                if call is None or call.scheduled > now + 0.1:
                    continue
                if call.timeout == 0:
                    repo.delete(call.id)
                else:
                    call.scheduled = now + call.timeout
                    call.last_run = now
                    await repo.update(call)
        except RaceCondition:
            # 提交前被别的 worker 抢先取走了：换一条，不用指数退避
            await asyncio.sleep(random.random() * POP_RACE_JITTER)
            continue
        except Exception as e:
            # call 出了本函数就没了，挂到异常上，任务循环记的 traceback 末尾才有是哪条调用。
            # scheduled/last_run 已被上面就地改成要写入的值，不打
            if call is not None:
                e.add_note(
                    _(
                        "[⚙️Future] 正在取出的调用：{system}{args}，id={id}，"
                        "recurring={recurring}，timeout={timeout}"
                    ).format(
                        system=call.system,
                        args=call.args,
                        id=call.id,
                        recurring=call.recurring,
                        timeout=call.timeout,
                    )
                )
            raise
        return call
    return None


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
    args = ast.literal_eval(call.args)
    # 循环任务和立即删除的任务都不需要lock
    req_call_lock = not call.recurring and call.timeout != 0
    # 执行
    ok = False
    res = None
    # 未来调用不走Endpoint，请求时间戳要自己打，否则System读到的ctx.timestamp恒为0
    caller.context.timestamp = time.time()
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
        replay.info(f"[SystemResult][{call.system}]({ok}, {res!s})")
    # 执行成功后，删除未来调用。如果代码错误/数据库错误，会下次重试
    if ok and req_call_lock:
        # 读到滞后副本上的旧版本、或这条刚被超时重投改了版本，会撞竞态：重试，别让它变成任务
        # 循环里的一条错误（还会跳过下面的收尾）
        async for attempt in tbl.session().retry(3):
            async with attempt as session:
                repo = session.using(tbl.comp_cls)
                if get_4_del := await repo.get(id=call.id):
                    repo.delete(get_4_del.id)
        # 再删除call_lock uuid数据，只有ok的执行才有call lock
        await caller.remove_call_lock(call.system, str(call.id))
    return True


async def run_due_calls(tables: list[Table], callers: dict[str, SystemCaller]) -> float:
    """
    扫一遍所有未来调用表，取出并执行已到期的调用，返回距下次该扫的秒数：执行过调用就是 0（马上
    再扫，可能还有），否则睡到最早的下一条，最多 MAX_IDLE_SLEEP 秒。

    到期的表连续取出执行，不能每处理一条就换表、碰上空表就睡：表一多（主表 + 各个副本），吞吐
    就塌成 worker 数 / (表数-1) 条每秒。表按随机顺序扫，各 worker 不总从同一张表开始抢。
    """
    executed = 0
    wake = time.time() + MAX_IDLE_SLEEP
    for tbl in random.sample(tables, len(tables)):
        now = time.time()
        due = await next_due(tbl, now + MAX_IDLE_SLEEP)
        if due is None:
            continue
        if due > now:
            wake = min(wake, due)
            continue
        popped = 0
        for _i in range(DRAIN_PER_TABLE):
            if not (call := await pop_upcoming_call(tbl)):
                break
            popped += 1
            await exec_future_call(call, callers[tbl.instance_name], tbl)
        if not popped:
            # 看到到期的却一条没取到：被别的 worker 抢先了，或副本上的索引还没跟上
            wake = min(wake, time.time() + CONTENDED_RETRY_DELAY)
        executed += popped
    return 0.0 if executed else max(0.0, wake - time.time())


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
    # 导致部分任务可能卡很久不执行，所以这里使用最基础的，每一段时间循环的方式：
    # 每轮扫完所有表，执行到期的；全都没到期才睡，睡到最早的下一条（最多 1 秒）
    while True:
        try:
            # 取出调用的事务提交前服务器关闭，任何数据不会丢失；取出后执行前关闭，
            # timeout=0 的调用会丢失，其余的 timeout 后重投
            await asyncio.sleep(await run_due_calls(future_call_tables, callers))
        except asyncio.CancelledError:
            break
        except Exception as e:
            err_msg = _("⚠️ [⚙️Future] Task执行异常，将再次重试：{exc}").format(
                exc=f"{type(e).__name__}:{e}"
            )
            logger.exception(err_msg)
            # 出错后退避再重试：持续性错误（如关服时后端已关闭，_ensure_open 在任何 await
            # 之前同步抛出）会让本循环永不挂起、饿死事件循环，连 Sanic 的取消都送不进来。
            try:
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                break
