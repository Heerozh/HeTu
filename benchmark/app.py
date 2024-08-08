import hetu
import numpy as np
import random
import string


@hetu.data.define_component(namespace='bench', persist=False)
class StrTable(hetu.data.BaseComponent):
    name: '<U16' = hetu.data.Property('', unique=True)
    number: np.int32 = hetu.data.Property(0)


@hetu.data.define_component(namespace='bench', persist=False, permission=hetu.data.Permission.EVERYBODY)
class IntTable(hetu.data.BaseComponent):
    number: np.int32 = hetu.data.Property(0, unique=True)
    name: '<U16' = hetu.data.Property('Unnamed')


@hetu.system.define_system(namespace='bench', components=(IntTable, ),
                           permission=hetu.data.Permission.EVERYBODY)
async def select_and_update(ctx: hetu.system.Context, number):
    async with ctx[IntTable].select_or_create(number, 'number') as row:
        row.name = ''.join(random.choices(string.ascii_uppercase + string.digits, k=3))
    return hetu.system.ResponseToClient([ctx.retry_count])


@hetu.system.define_system(namespace='bench', components=(StrTable, IntTable),
                           permission=hetu.data.Permission.EVERYBODY)
async def exchange_data(ctx: hetu.system.Context, name, number):
    async with ctx[StrTable].select_or_create(name, 'name') as name_row:
        async with ctx[IntTable].select_or_create(number, 'number') as number_row:
            name_row.number, number_row.name = number, name
    return hetu.system.ResponseToClient([ctx.retry_count])


@hetu.system.define_system(namespace="bench", permission=hetu.data.Permission.EVERYBODY)
async def hello_world(ctx: hetu.system.Context):
    return hetu.system.ResponseToClient(['世界只有一个！但是河图可以模拟很多个！'])


@hetu.system.define_system(namespace="bench", permission=hetu.data.Permission.EVERYBODY,
                           bases=('elevate',))
async def login_test(ctx: hetu.system.Context, user_id):
    await ctx['elevate'](ctx, user_id, kick_logged_in=True)
    return hetu.system.ResponseToClient([ctx.retry_count])
