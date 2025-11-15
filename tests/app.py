import logging
import asyncio

import numpy as np

from hetu.data import BaseComponent, Property, define_component, Permission
from hetu.system import define_system, Context, ResponseToClient

logger = logging.getLogger('HeTu.root')


# ============================


# 首先定于game_short_name的namespace，让CONFIG_TEMPLATE.yml能启动
@define_system(namespace="game_short_name")
async def do_nothing(ctx: Context, sleep):
    pass


# ============================


@define_system(namespace='pytest', permission=Permission.EVERYBODY, bases=('elevate', ))
async def login(ctx: Context, user_id, kick_logged_in=True):
    await ctx['elevate'](ctx, user_id, kick_logged_in)
    return ResponseToClient({'id': ctx.caller})


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


@define_system(
    namespace="pytest",
    components=(RLSComp,),
    permission=Permission.USER
)
async def test_rls_comp_value(ctx: Context, hp):
    row = await ctx[RLSComp].select(ctx.caller, 'owner')
    assert row.value == hp


# ============================


@define_component(namespace="pytest", force=True)
class IndexComp1(BaseComponent):
    owner: np.int64 = Property(0, unique=True)
    value: float = Property(0, index=True)

@define_component(namespace="pytest", force=True)
class IndexComp2(BaseComponent):
    owner: np.int64 = Property(0, unique=True)
    name: 'U8' = Property("", unique=True)


@define_system(
    namespace="pytest",
    components=(IndexComp1, IndexComp2),
)
async def create_row(ctx: Context, owner, v1, v2):
    row1 = IndexComp1.new_row()
    row1.value = v1
    row1.owner = owner
    await ctx[IndexComp1].insert(row1)

    async with ctx[IndexComp2].update_or_insert(owner, 'owner') as row2:
        row2.name = f"User_{v2}"

# 测试bug用
@define_system(
    namespace="pytest",
    components=(IndexComp2, ),
)
async def create_row_2_upsert(ctx, owner, v2):
    async with ctx[IndexComp2].update_or_insert(owner, "owner") as row:
        row.name = f"User_{v2}"
    async with ctx[IndexComp2].update_or_insert(owner, "owner") as row:
        row.name = f"User_{v2}"


@define_system(
    namespace="pytest",
    components=(RLSComp, IndexComp1, IndexComp2),
    bases=("add_rls_comp_value",),
)
async def composer_system(ctx: Context):
    rls_comp_value = await ctx['add_rls_comp_value'](ctx, 10)

    await asyncio.sleep(0.1)

    self_comp1 = await ctx[IndexComp1].select(ctx.caller, 'owner')
    self_comp2 = await ctx[IndexComp2].select(ctx.caller, 'owner')

    targets1 = await ctx[IndexComp1].query(
        'value', self_comp1.value - 10, self_comp1.value + 10)
    targets2 = await ctx[IndexComp2].query('name', self_comp2.name, "[User_d")
    targets_id = set(targets1.owner) & set(targets2.owner)

    for target_id in targets_id:
        target = await ctx[IndexComp2].select(target_id, 'owner')
        logger.info(f"[{target.name}]")


# ============================


@define_system(
    namespace="pytest",
    permission=Permission.EVERYBODY,
    components=(IndexComp1, ),
)
async def race(ctx: Context, sleep):
    async with ctx[IndexComp1].upsert(3, 'owner') as row:
        print(ctx, 'selected', row)
        await asyncio.sleep(sleep)
        row.value = sleep
