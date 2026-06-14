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
import uuid

from ..i18n import _

logger = logging.getLogger("HeTu.root")

# 传递 per-boot uuid 的 config 键。由 main 进程（CLI）每次开服生成一次，写入 config 后
# 经 AppLoader 的 factory 透传给每个 worker 进程，使同一次开服的所有 worker 共享同一值。
ON_START_UUID_CONFIG_KEY = "ON_START_UUID"


def make_boot_uuid() -> str:
    """生成一个本次开服(boot)的唯一标识，用作 on_start 去重的 SystemLock uuid。

    长度需 <= 32（SystemLock.uuid 为 <U32），取 16 位 hex 足够避免开服间碰撞。
    """
    return uuid.uuid4().hex[:16]


async def run_startup_systems(
    namespace: str, table_managers: dict, boot_uuid: str
) -> None:
    """对每个 instance 执行所有 ``on_start=True`` 的 System，每次开服一次。

    ``boot_uuid`` 是本次开服的唯一标识，作为 SystemLock 的去重 uuid：同一次开服的所有
    worker 传入相同值 → 整集群只成功提交一次（并发由乐观锁收敛）；下次开服传入新值 →
    再次执行。任一 System 抛出非竞态异常都会向上传播，由调用方（``worker_start``）据此
    中止启动。

    Run every system marked ``on_start=True`` once per server boot. ``boot_uuid``
    identifies this boot and is used as the SystemLock dedup key: all workers of the
    same boot pass the same value (so it commits exactly once cluster-wide), and a new
    boot passes a new value (so it runs again). Non-race exceptions propagate so the
    caller can abort worker startup.
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
            await caller.call(sys_name, uuid=boot_uuid)
