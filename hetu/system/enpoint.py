"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import logging
from typing import TYPE_CHECKING, Callable

from ..common.permission import Permission

if TYPE_CHECKING:
    from hetu.endpoint.response import EndpointResponse

    from .context import SystemContext

logger = logging.getLogger("HeTu.root")
replay = logging.getLogger("HeTu.replay")


def create_system_endpoint(system: str, permission: Permission) -> Callable:
    """自动生成的直接调用System的Endpoint"""

    async def system_endpoint(ctx: SystemContext, *args) -> None | EndpointResponse:
        # 检查权限是否符合
        match permission:
            case Permission.USER:
                if ctx.caller is None or ctx.caller == 0:
                    err_msg = (
                        f"⚠️ [📞Executor] [非法操作] {ctx} | "
                        f"{system}无调用权限，检查是否非法调用：{args}"
                    )
                    replay.info(err_msg)
                    logger.warning(err_msg)
                    return
            case Permission.ADMIN:
                if not ctx.is_admin():
                    err_msg = (
                        f"⚠️ [📞Executor] [非法操作] {ctx} | "
                        f"{system}无调用权限，检查是否非法调用：{args}"
                    )
                    replay.info(err_msg)
                    logger.warning(err_msg)
                    return
        ctx.systems.call(system, *args)

    return system_endpoint
