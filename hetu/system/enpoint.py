"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import logging
from typing import TYPE_CHECKING, Callable


if TYPE_CHECKING:
    from ..endpoint.response import EndpointResponse
    from .context import SystemContext

logger = logging.getLogger("HeTu.root")
replay = logging.getLogger("HeTu.replay")


def create_system_endpoint(system: str) -> Callable:
    """自动生成的直接调用System的Endpoint"""

    async def system_endpoint(ctx: SystemContext, *args) -> None | EndpointResponse:
        return await ctx.systems.call(system, *args)

    system_endpoint.__name__ = system
    return system_endpoint
