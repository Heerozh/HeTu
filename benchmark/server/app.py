#  """
#  @author: Heerozh (Zhang Jianhao)
#  @copyright: Copyright 2024, Heerozh. All rights reserved.
#  @license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
#  @email: heeroz@gmail.com
#  """

import hetu
import numpy as np
import random
import string


@hetu.define_component(namespace="bench", volatile=True)
class StrTable(hetu.BaseComponent):
    name: "<U16" = hetu.property_field("", unique=True)
    number: np.int32 = hetu.property_field(0)


@hetu.define_component(
    namespace="bench", volatile=True, permission=hetu.Permission.EVERYBODY
)
class IntTable(hetu.BaseComponent):
    number: np.int32 = hetu.property_field(0, unique=True)
    name: "<U16" = hetu.property_field("Unnamed")


@hetu.define_system(
    namespace="bench", components=(IntTable,), permission=hetu.Permission.EVERYBODY
)
async def just_get(ctx: hetu.SystemContext, number):
    row = await ctx.repo[IntTable].get(id=number)
    return hetu.ResponseToClient([ctx.race_count])


@hetu.define_system(
    namespace="bench", components=(IntTable,), permission=hetu.Permission.EVERYBODY
)
async def upsert(ctx: hetu.SystemContext, number):
    async with ctx.repo[IntTable].upsert(number=number) as row:
        row.name = "".join(random.choices(string.ascii_uppercase + string.digits, k=3))
    return hetu.ResponseToClient([ctx.race_count])


@hetu.define_system(
    namespace="bench",
    components=(StrTable, IntTable),
    permission=hetu.Permission.EVERYBODY,
)
async def exchange_data(ctx: hetu.SystemContext, name, number):
    async with ctx.repo[StrTable].upsert(name=name) as name_row:
        async with ctx.repo[IntTable].upsert(number=number) as number_row:
            name_row.number, number_row.name = number, name
    return hetu.ResponseToClient([ctx.race_count])


@hetu.define_endpoint(namespace="bench", permission=hetu.Permission.EVERYBODY)
async def hello_world(ctx: hetu.SystemContext):
    return hetu.ResponseToClient(["世界收到"])


@hetu.define_endpoint(
    namespace="bench",
    permission=hetu.Permission.EVERYBODY,
)
async def login_test(ctx: hetu.EndpointContext, user_id):
    await hetu.elevate(ctx, user_id, kick_logged_in=True)
    return hetu.ResponseToClient([0])
