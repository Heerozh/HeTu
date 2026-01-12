#  """
#  @author: Heerozh (Zhang Jianhao)
#  @copyright: Copyright 2024, Heerozh. All rights reserved.
#  @license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
#  @email: heeroz@gmail.com
#  """

import logging
import asyncio

import numpy as np

import hetu


logger = logging.getLogger("HeTu.root")


# ============================


# 首先定于game_short_name的namespace，让CONFIG_TEMPLATE.yml能启动
@hetu.define_endpoint(namespace="game_short_name")
async def do_nothing(ctx: hetu.EndpointContext, sleep):
    pass


# ============================


@hetu.define_endpoint(namespace="pytest", permission=hetu.Permission.EVERYBODY)
async def login(ctx: hetu.SystemContext, user_id, kick_logged_in=True):
    ok, reason = await hetu.elevate(ctx, user_id, kick_logged_in)
    return hetu.ResponseToClient({"id": ctx.caller})


# ============================


@hetu.define_component(namespace="pytest", force=True, permission=hetu.Permission.OWNER)
class RLSComp(hetu.BaseComponent):
    owner: np.int64 = hetu.property_field(0, unique=True)
    value: np.int32 = hetu.property_field(100)


@hetu.define_system(
    namespace="pytest",
    components=(RLSComp,),
    permission=hetu.Permission.USER,
    call_lock=True,
)
async def add_rls_comp_value(ctx: hetu.SystemContext, value):
    async with ctx.repo[RLSComp].upsert(owner=ctx.caller) as row:
        row.value += value
    return row.value


@hetu.define_system(
    namespace="pytest", components=(RLSComp,), permission=hetu.Permission.USER
)
async def test_rls_comp_value(ctx: hetu.SystemContext, value):
    row = await ctx.repo[RLSComp].get(owner=ctx.caller)
    assert row
    print("test_rls_comp_value", row, value)
    assert row.value == value


@hetu.define_system(
    namespace="pytest",
    permission=hetu.Permission.EVERYBODY,
    depends=("create_future_call:copy1",),
)
async def add_rls_comp_value_future(ctx: hetu.SystemContext, value, recurring):
    return await ctx.depend["create_future_call:copy1"](
        ctx, -1, "add_rls_comp_value", value, timeout=10, recurring=recurring
    )


# ---------------------------------


@hetu.define_system(
    namespace="pytest",
    permission=hetu.Permission.USER,
    depends=("add_rls_comp_value:copy1",),
)
async def add_rls_comp_value_copy(ctx: hetu.SystemContext, value):
    return await ctx.depend["add_rls_comp_value:copy1"](ctx, value)


@hetu.define_system(
    namespace="pytest",
    permission=hetu.Permission.USER,
    depends=("test_rls_comp_value:copy1",),
)
async def test_rls_comp_value_copy(ctx: hetu.SystemContext, value):
    return await ctx.depend["test_rls_comp_value:copy1"](ctx, value)


# ============================


@hetu.define_component(namespace="pytest", force=True)
class IndexComp1(hetu.BaseComponent):
    owner: np.int64 = hetu.property_field(0, unique=True)
    value: float = hetu.property_field(0, index=True)


@hetu.define_component(namespace="pytest", force=True)
class IndexComp2(hetu.BaseComponent):
    owner: np.int64 = hetu.property_field(0, unique=True)
    name: str = hetu.property_field("", unique=True, dtype="U8")


@hetu.define_system(
    namespace="pytest",
    permission=hetu.Permission.USER,
    components=(IndexComp1, IndexComp2),
)
async def create_row(ctx: hetu.SystemContext, owner, v1, v2):
    row1 = IndexComp1.new_row()
    row1.value = v1
    row1.owner = owner
    await ctx.repo[IndexComp1].insert(row1)

    async with ctx.repo[IndexComp2].upsert(owner=owner) as row2:
        row2.name = f"User_{v2}"


# 测试bug用
@hetu.define_system(
    namespace="pytest",
    permission=hetu.Permission.USER,
    components=(IndexComp2,),
)
async def create_row_2_upsert(ctx, owner, v2):
    # 连续upsert 2次同样的数据，因为内容相同，第二次不应该违反unique
    async with ctx.repo[IndexComp2].upsert(owner=owner) as row:
        row.name = f"User_{v2}"
    async with ctx.repo[IndexComp2].upsert(owner=owner) as row:
        row.name = f"User_{v2}"


@hetu.define_system(
    namespace="pytest",
    components=(RLSComp, IndexComp1, IndexComp2),
    permission=hetu.Permission.USER,
    depends=(add_rls_comp_value,),
)
async def composer_system(ctx: hetu.SystemContext):
    rls_comp_value = await add_rls_comp_value(ctx, 10)

    await asyncio.sleep(0.1)

    self_comp1 = await ctx.repo[IndexComp1].get(owner=ctx.caller)
    self_comp2 = await ctx.repo[IndexComp2].get(owner=ctx.caller)
    assert self_comp1 and self_comp2

    targets1 = await ctx.repo[IndexComp1].range(
        "value", self_comp1.value - 10, self_comp1.value + 10
    )
    targets2 = await ctx.repo[IndexComp2].range("name", self_comp2.name, "[User_d")
    targets_id = set(targets1.owner) & set(targets2.owner)

    for target_id in targets_id:
        target = await ctx.repo[IndexComp2].get(owner=target_id)
        assert target
        logger.info(f"[{target.name}]")


# ============================


@hetu.define_system(
    namespace="pytest",
    permission=hetu.Permission.EVERYBODY,
    components=(IndexComp1,),
)
async def race_upsert(ctx: hetu.SystemContext, sleep):
    async with ctx.repo[IndexComp1].upsert(owner=3) as row:
        print(ctx, "race_upsert get", row)
        await asyncio.sleep(sleep)
        row.value = sleep


@hetu.define_system(
    namespace="pytest",
    permission=hetu.Permission.EVERYBODY,
    components=(IndexComp1,),
)
async def race_range(ctx: hetu.SystemContext, sleep):
    _rows = await ctx.repo[IndexComp1].range(owner=(3, 3))
    await asyncio.sleep(sleep)
    _rows[0].value = sleep
    await ctx.repo[IndexComp1].update(_rows[0])
