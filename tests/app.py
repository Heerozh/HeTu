import logging

import numpy as np

from hetu.data import BaseComponent, Property, define_component, Permission
from hetu.system import define_system, Context

logger = logging.getLogger('HeTu.root')


# ============================


# 首先定于game_short_name的namespace，让CONFIG_TEMPLATE.yml能启动
@define_system(namespace="game_short_name")
async def do_nothing(ctx: Context, sleep):
    pass


# ============================


@define_component(namespace="pytest", force=True, permission=Permission.OWNER)
class RLSComp(BaseComponent):
    owner: np.int64 = Property(0, unique=True)
    value: np.int32 = Property(100)


@define_system(
    namespace="pytest",
    components=(RLSComp,),
    permission=Permission.USER,
    call_lock=True
)
async def add_rls_comp_value(ctx: Context, value):
    async with ctx[RLSComp].update_or_insert(ctx.caller, 'owner') as row:
        row.value -= value
    return row.value


# ============================
