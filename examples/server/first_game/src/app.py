#  """
#  @author: Heerozh (Zhang Jianhao)
#  @copyright: Copyright 2024, Heerozh. All rights reserved.
#  @license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
#  @email: heeroz@gmail.com
#  """


import numpy as np

import hetu


# 通过@define_component修饰，表示Position结构是一个组件
@hetu.define_component(namespace="ssw", permission=hetu.Permission.USER)
class Position(hetu.BaseComponent):
    # 定义Position.x为np.float32类型，默认值为0
    x: np.float32 = hetu.property_field(default=0)
    y: np.float32 = hetu.property_field(default=0)
    owner: np.int64 = hetu.property_field(default=0, unique=True)  # 开启unique索引


# permission定义为任何人可调用，bases引用另一个hetu内置System：elevate
@hetu.define_system(namespace="ssw", permission=hetu.Permission.EVERYBODY)
async def login_test(ctx: hetu.SystemContext, user_id):
    await hetu.elevate(ctx, user_id, kick_logged_in=True)


# 移动逻辑，操作Position组件，将当前用户的Position移动到新位置
@hetu.define_system(
    namespace="ssw",
    components=(Position,),
    permission=hetu.Permission.USER,
)
async def move_to(ctx: hetu.SystemContext, x, y):
    # 客户端传入的参数（x, y）都要验证合法性，防止用户修改数据，这里省略。
    # 在Position表（组件）中查询或创建owner=ctx.caller的行，然后修改x和y
    async with ctx.repo[Position].upsert(owner=ctx.caller) as pos:
        pos.x = x
        pos.y = y
        # with结束后会自动提交修改
