import numpy as np

from hetu.data import define_component, Property, Permission, BaseComponent
from hetu.system import define_system, Context


# 通过@define_component修饰，表示Position结构是一个组件
@define_component(namespace='ssw', permission=Permission.USER)
class Position(BaseComponent):
    x: np.float32 = Property(default=0)  # 定义Position.x为np.float32类型，默认值为0
    y: np.float32 = Property(default=0)
    owner: np.int64 = Property(default=0, unique=True)  # 开启unique索引


# permission定义为任何人可调用，inherits引用另一个hetu内置System：elevate
@define_system(namespace="ssw", permission=Permission.EVERYBODY, inherits=('elevate',))
async def login_test(ctx: Context, user_id):
    await ctx['elevate'](ctx, user_id, kick_logged_in=True)


# 移动逻辑，操作Position组件，将当前用户的Position移动到新位置
@define_system(namespace="ssw", components=(Position,), permission=Permission.USER,)
async def move_to(ctx: Context, x, y):
    # 客户端传入的参数（x, y）都要验证合法性，防止用户修改数据，这里省略。
    # 在Position表（组件）中查询或创建owner=ctx.caller的行，然后修改x和y
    async with ctx[Position].select_or_create(ctx.caller, where='owner') as pos:
        pos.x = x
        pos.y = y
        # with结束后会自动提交修改
