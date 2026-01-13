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
    System调用器。该调用器实例每个用户连接独立持有一个。
    """

    def __init__(
        self, namespace: str, comp_mgr: ComponentTableManager, context: SystemContext
    ):
        self.namespace = namespace
        self.comp_mgr = comp_mgr
        self.context = context

    @classmethod
    def call_check(cls, system: str) -> SystemDefine:
        """检查system是否存在，并发挥对应的SystemDefine"""
        # 读取保存的system define
        sys = SYSTEM_CLUSTERS.get_system(system)
        if not sys:
            raise ValueError(f"不存在的System, 检查是否非法调用：{system}")

        return sys

    async def call_(
        self, sys: SystemDefine, *args, uuid: str = ""
    ) -> ResponseToClient | None:
        """
        实际调用逻辑，无任何检查
        调用成功返回System返回值
        事务冲突超出重试次数时Raise RuntimeError
        """
        # 开始调用
        sys_name = sys.func.__name__
        # logger.debug(f"⌚ [📞System] 调用System: {sys_name}")

        # 初始化context值
        context = self.context
        context.race_count = 0
        context.repo = {}
        context.depend = {}

        # 获取system引用的第一个component的session，system只能引用相同session的组件，所以都一样
        comp_mgr = self.comp_mgr
        first_comp = next(iter(sys.full_components), None)
        first_table = first_comp and comp_mgr.get_table(first_comp) or None
        assert first_table, f"TYPING不该走到: System {sys_name} 没有引用任何Component"
        session = first_table.session()

        # 设置context.repo
        for comp in sys.full_components:
            repo = session.using(comp)
            master = comp.master_ or comp
            context.repo[master] = repo
        if uuid and SystemLock not in context.repo:
            raise ValueError(
                f"调用System {sys_name} 时使用了uuid防重复功能，但该System没有定call_lock=True"
            )

        # 复制inherited函数
        for dep_name in sys.full_depends:
            base, _, _ = dep_name.partition(":")
            dep_sys = SYSTEM_CLUSTERS.get_system(base)
            assert dep_sys, f"TYPING不该走到: System {sys_name} 的依赖 {base} 找不到"
            context.depend[dep_name] = dep_sys.func

        start_time = time.perf_counter()
        # 调用系统
        while context.race_count < sys.max_retry:
            # 开始新的事务，并attach components
            await session.__aenter__()

            # 执行system和事务
            try:
                # 先检查uuid是否执行过了
                if uuid and await context.repo[SystemLock].get(uuid=uuid):
                    replay.info(f"[UUIDExist][{sys_name}] 该uuid {uuid} 已执行过")
                    logger.debug(
                        f"⌚ [📞System] 调用System遇到重复执行: {sys_name}，{uuid} 已执行过"
                    )
                    return None
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
                # logger.debug(f"✅ [📞System] 调用System成功: {sys_name}")
                return rtn
            except RaceCondition:
                context.race_count += 1
                # 重试时sleep一段时间，可降低再次冲突率约90%。
                # delay增加会降低冲突率，但也会增加rtt波动。除1:-94%, 2:-91%, 5: -87%, 10: -85%
                delay = random.random() / 5
                replay.info(f"[RaceCondition][{sys_name}]{delay:.3f}s retry")
                logger.debug(
                    f"⌚ [📞System] 调用System遇到竞态: {sys_name}，{delay}秒后重试"
                )
                await asyncio.sleep(delay)
                continue
            except Exception as _:
                # err_msg = f"嵌套系统调用异常，调用：{sys_name}{args}，异常：{type(e).__name__}:{e}"
                raise
            finally:
                # 上面如果执行过commit了，那么这句也无害
                session.discard()
                # 记录时间和重试次数到内存
                elapsed = time.perf_counter() - start_time
                SLOW_LOG.log(elapsed, sys_name, context.race_count)

        raise RuntimeError(f"调用System失败, 超过{sys_name}重试次数{sys.max_retry}")

    async def call(self, system: str, *args, uuid: str = "") -> ResponseToClient | None:
        """
        调用一个System。
        服务器会启动一个数据库事务Session，执行System内的所有数据库操作。
        如果遇到事务冲突，则会自动重试，直到成功或超过最大重试次数为止。

        Parameters
        ----------
        system : str
            要调用的System名称
        *args
            传递给System的参数
        uuid : str, optional
            本次调用的唯一标识符，用于防止重复调用（默认不启用）。
            如果提供了uuid，系统会在调用前检查该uuid是否已经执行过，
            使用本功能也需要System定义时`call_lock`设为`True`。

            主要用于未来调用的幂等性，或者你需要嵌套执行System，保证其中一个只执行一次等特殊情况，

        Returns
        -------
        如果成功则返回System的返回值。
        抛出异常表示非法调用，或内部失败（代码错误，数据库错误），会自动记录日志。可以不处理扔给
        上级Endpoint，Endpoint收到异常会立即调用terminate断开客户端SDK的连接。

        See Also
        --------
        hetu.system.future.create_future_call : 创建未来调用
        """
        # 检查call参数和call权限
        sys = self.call_check(system)

        # 开始调用
        return await self.call_(sys, *args, uuid=uuid)

    async def remove_call_lock(self, system: str, uuid: str):
        """
        删除call lock。此方法开发者无需调用，由系统内部管理。
        call lock本身是易失表，可以通过维护工具定期清理数据，并不需要特地remove。
        """
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
                repo.delete(row.id)
