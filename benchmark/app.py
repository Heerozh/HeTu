import hetu
import numpy as np
import random
import string


@hetu.data.define_component(namespace='bench')
class NameTable(hetu.data.BaseComponent):
    name: '<U16' = hetu.data.Property('', unique=True)
    number: np.int32 = hetu.data.Property(0)


@hetu.data.define_component(namespace='bench')
class NumberTable(hetu.data.BaseComponent):
    number: np.int32 = hetu.data.Property(0, unique=True)
    name: '<U16' = hetu.data.Property('Unnamed')


@hetu.system.define_system(namespace='bench', components=(NumberTable, ),
                           permission=hetu.data.Permission.EVERYBODY)
async def select_and_update(ctx: hetu.system.Context, number):
    async with ctx[NumberTable].select_or_create(number, 'number') as row:
        row.name = ''.join(random.choices(string.ascii_uppercase + string.digits, k=16))
    return hetu.system.ResponseToClient([number])


@hetu.system.define_system(namespace='bench', components=(NameTable, NumberTable),
                           permission=hetu.data.Permission.EVERYBODY)
async def exchange_data(ctx: hetu.system.Context, name, number):
    async with ctx[NameTable].select_or_create(name, 'name') as name_row:
        async with ctx[NumberTable].select_or_create(number, 'number') as number_row:
            name_row.number, number_row.name = number, name


@hetu.system.define_system(namespace="bench", permission=hetu.data.Permission.EVERYBODY,
                           inherits=('elevate',))
async def login_test(ctx: hetu.system.Context, user_id):
    await ctx['elevate'](ctx, user_id, kick_logged_in=True)
