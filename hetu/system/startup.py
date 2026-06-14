"""
App 启动钩子：worker 启动时（开始收连接前）幂等执行所有
``@define_system(on_start=True)`` 的 System。

App startup hook: idempotently run every system marked ``on_start=True`` when a
worker starts, before it accepts connections.

@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2025, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import logging

from ..i18n import _

logger = logging.getLogger("HeTu.root")

# 启动钩子调用锁的固定 uuid。每个 on_start System 各有独立的 SystemLock 副本表
# （副本名含 System 名），所以此处用常量即可保证按 System、按 instance 去重。
_ON_START_UUID = "@on_start"


async def run_startup_systems(namespace: str, table_managers: dict) -> None:
    """对每个 instance 幂等执行所有 ``on_start=True`` 的 System。

    通过 SystemLock(uuid) 去重：整集群、跨重启只会成功提交一次；多 worker 并发由乐观锁
    收敛到恰好一次提交。任一 System 抛出非竞态异常都会向上传播，由调用方
    （``worker_start``）据此中止启动。

    Run every system marked ``on_start=True`` once per instance, deduplicated via
    SystemLock so it commits exactly once across the cluster and across restarts.
    Any non-race exception propagates so the caller can abort worker startup.
    """
    from .caller import SystemCaller
    from .context import SystemContext
    from .definer import SystemClusters

    startup_systems = SystemClusters().get_startup_systems(namespace)
    if not startup_systems:
        return

    # 内部服务身份：无用户、无连接（与 future_call_task 一致）
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

    for instance, tbl_mgr in table_managers.items():
        caller = SystemCaller(namespace, tbl_mgr, context)
        context.systems = caller
        for sys_name in startup_systems:
            logger.info(
                _(
                    "🚀 [⚙️Startup] instance={instance} 执行启动System：{sys_name}"
                ).format(instance=instance, sys_name=sys_name)
            )
            await caller.call(sys_name, uuid=_ON_START_UUID)
