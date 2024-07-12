from hetu.data import BaseComponent, Property, define_component, Permission
from hetu.system import define_system, Context
import numpy as np
import asyncio
import logging
logger = logging.getLogger('HeTu')


# 定义组件

@define_component(namespace="ssw", force=True)
class Position(BaseComponent):
    owner: np.int64 = Property(0, unique=True)
    x: float = Property(0, index=True)
    y: float = Property(0, index=True)


@define_component(namespace="ssw", force=True)
class MP(BaseComponent):
    owner: np.int64 = Property(0, unique=True)
    value: np.int32 = Property(100)


@define_component(namespace="ssw", force=True, permission=Permission.OWNER)
class HP(BaseComponent):
    owner: np.int64 = Property(0, unique=True)
    value: np.int32 = Property(100)


@define_component(namespace="ssw", force=True)
class Users(BaseComponent):
    entity_id: np.int64 = Property(0, unique=True)
    name: 'U8' = Property("", unique=True)


# ====================================================
# 定义系统

@define_system(
    namespace="ssw",
    components=(MP,),
)
async def use_mp(ctx: Context, value):
    async with ctx[MP].select_or_create(ctx.caller, 'owner') as row:
        row.value -= value
    return row.value


@define_system(namespace='ssw', permission=Permission.EVERYBODY, inherits=('elevate', ))
async def login(ctx: Context, user_id):
    await ctx['elevate'](ctx, user_id)


# -------------------------


@define_system(
    namespace="ssw",
    components=(HP,),
    permission=Permission.USER
)
async def use_hp(ctx: Context, value):
    async with ctx[HP].select_or_create(ctx.caller, 'owner') as row:
        row.value -= value
    return row.value


@define_system(
    namespace="ssw",
    components=(HP, Users),
    permission=Permission.USER
)
async def test_hp(ctx: Context, hp):
    row = await ctx[HP].select(ctx.caller, 'owner')
    assert row.value == hp


# -------------------------

@define_system(
    namespace="ssw",
    components=(Position, Users),
)
async def create_user(ctx: Context, user_id, x, y):
    usr = Users.new_row()
    usr.name = f"User_{user_id}"
    usr.entity_id = user_id
    await ctx[Users].insert(usr)

    async with ctx[Position].select_or_create(user_id, 'owner') as pos:
        pos.x = x
        pos.y = y


@define_system(
    namespace="ssw",
    components=(Position, Users),
    inherits=('use_mp', )
)
async def magic(ctx: Context):
    mp = await ctx['use_mp'](ctx, 10)
    if mp < 0:
        return False
    await asyncio.sleep(3)
    self_pos = await ctx[Position].select(ctx.caller, 'owner')
    targets_inx = await ctx[Position].query('x', self_pos.x-10, self_pos.x+10)
    targets_iny = await ctx[Position].query('y', self_pos.y-10, self_pos.y+10)
    targets_id = set(targets_inx.owner) & set(targets_iny.owner)

    await ctx['use_mp'](ctx, 90)

    for target_id in targets_id:
        target = await ctx[Users].select(target_id, 'entity_id')
        logger.info(target.name)


@define_system(
    namespace="ssw",
    permission=Permission.EVERYBODY,
    components=(Position, Users),
)
async def race(ctx: Context, sleep):
    _rows = await ctx[Position].query('owner', 3)
    await asyncio.sleep(sleep)
    _row = await ctx[Position].select(3, 'owner')
    _row.x = 2
    await ctx[Position].update(_row.id, _row)
