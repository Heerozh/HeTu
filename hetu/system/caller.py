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
from typing import TYPE_CHECKING

from .lock import SystemLock
from ..common.slowlog import SlowLog
from ..data.backend import RaceCondition
from .definer import SystemClusters, SystemDefine

if TYPE_CHECKING:
    from ..endpoint.response import ResponseToClient
    from ..manager import ComponentTableManager
    from .context import SystemContext


logger = logging.getLogger("HeTu.root")
replay = logging.getLogger("HeTu.replay")
SYSTEM_CLUSTERS = SystemClusters()
SystemClusters = None
SLOW_LOG = SlowLog()


class SystemCaller:
    """
    每个连接一个SystemCaller实例。
    """

    def __init__(
        self, namespace: str, comp_mgr: ComponentTableManager, context: SystemContext
    ):
        self.namespace = namespace
        self.comp_mgr = comp_mgr
        self.context = context

    def call_check(self, system: str) -> SystemDefine | None:
        """检查调用是否合法"""
        context = self.context
        # 读取保存的system define
        sys = SYSTEM_CLUSTERS.get_system(system)
        if not sys:
            err_msg = f"⚠️ [📞Executor] [非法操作] {context} | 不存在的System, 检查是否非法调用：{system}"
            replay.info(err_msg)
            logger.warning(err_msg)
            return None

        return sys

    async def call_(
        self, sys: SystemDefine, *args, uuid: str = ""
    ) -> tuple[bool, ResponseToClient | None]:
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
        context.race_count = 0
        context.repo = {}
        context.depend = {}

        # 获取system引用的第一个component的session，system只能引用相同session的组件，所以都一样
        comp_mgr = self.comp_mgr
        first_comp = next(iter(sys.full_components), None)
        first_table = first_comp and comp_mgr.get_table(first_comp) or None
        assert first_table, f"for typing。System {sys_name} 没有引用任何Component"
        session = first_table.session()

        # 复制inherited函数
        for dep_name in sys.full_depends:
            base, _, _ = dep_name.partition(":")
            dep_sys = SYSTEM_CLUSTERS.get_system(base)
            assert dep_sys, f"for typing。System {sys_name} 依赖的System {base} 不存在"
            context.depend[dep_name] = dep_sys.func

        start_time = time.perf_counter()
        # 调用系统
        while context.race_count < sys.max_retry:
            # 开始新的事务，并attach components
            await session.__aenter__()
            for comp in sys.full_components:
                context.repo[comp] = session.using(comp)
            # 执行system和事务
            try:
                # 先检查uuid是否执行过了
                if uuid and await context.repo[SystemLock].get(uuid=uuid):
                    replay.info(f"[UUIDExist][{sys_name}] 该uuid {uuid} 已执行过")
                    logger.debug(
                        f"⌚ [📞Executor] 调用System遇到重复执行: {sys_name}，{uuid} 已执行过"
                    )
                    return True, None
                # 执行
                rtn = await sys.func(context, *args)
                # 标记uuid已执行
                if uuid:
                    async with context.repo[SystemLock].upsert(uuid=uuid) as lock:
                        lock.caller = context.caller
                        lock.called = time.time()
                        lock.name = sys_name
                # 执行事务
                await session.commit()
                # logger.debug(f"✅ [📞Executor] 调用System成功: {sys_name}")
                return True, rtn
            except RaceCondition:
                context.race_count += 1
                # 重试时sleep一段时间，可降低再次冲突率约90%。
                # delay增加会降低冲突率，但也会增加rtt波动。除1:-94%, 2:-91%, 5: -87%, 10: -85%
                delay = random.random() / 5
                replay.info(f"[RaceCondition][{sys_name}]{delay:.3f}s retry")
                logger.debug(
                    f"⌚ [📞Executor] 调用System遇到竞态: {sys_name}，{delay}秒后重试"
                )
                await asyncio.sleep(delay)
                continue
            except Exception as e:
                err_msg = f"❌ [📞Executor] 系统调用异常，调用：{sys_name}{args}，异常：{type(e).__name__}:{e}"
                replay.info(err_msg)
                logger.exception(err_msg)
                return False, None
            finally:
                # 上面如果执行过commit了，那么这句也无害
                session.discard()
                # 记录时间和重试次数到内存
                elapsed = time.perf_counter() - start_time
                SLOW_LOG.log(elapsed, sys_name, context.race_count)

        logger.debug(
            f"✅ [📞Executor] 调用System失败, 超过{sys_name}重试次数{sys.max_retry}"
        )
        return False, None

    async def call(
        self, system: str, *args, uuid: str = ""
    ) -> tuple[bool, ResponseToClient | None]:
        """
        调用System，返回True表示调用成功，
        返回False表示内部失败或非法调用，此时需要立即调用terminate断开连接
        """
        # 检查call参数和call权限
        sys = self.call_check(system)
        if sys is None:
            return False, None

        # 开始调用
        return await self.call_(sys, *args, uuid=uuid)

    async def remove_call_lock(self, system: str, uuid: str):
        """删除call lock"""
        sys = SYSTEM_CLUSTERS.get_system(system)
        assert sys

        # 找到第一个lock组件
        def is_lock(_comp):
            return _comp == SystemLock or _comp.master_ == SystemLock

        comp = next((x for x in sys.full_components if is_lock(x)), None)
        assert comp
        comp_mgr = self.comp_mgr
        table = comp_mgr.get_table(comp)
        assert table

        # 删除lock
        async with table.session() as session:
            repo = session.using(comp)
            row = await repo.get(uuid=uuid)
            if row:
                row.delete(row.id)
