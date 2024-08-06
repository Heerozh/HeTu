[![Coverage Status](https://coveralls.io/repos/github/Heerozh/HeTu/badge.svg?branch=master)](https://coveralls.io/github/Heerozh/HeTu?branch=master)

> [!NOTE]  
> 仍在开发中，80%...

# 河图HeTu

河图是一个轻量化的分布式游戏服务器引擎。集成了数据库概念，因此开发和维护极其便捷，适用于从万人 MMO 到多人联机的各种场景。

基于 ECS(Entity-Component-System) 结构，采用 Python 语言，支持各种数据科学库，拥抱未来。
具体性能见下方[性能测试](#性能测试)。

代码注释和文档都以中文为主，不鹊巢鸠占，方便中文用户开发。

## 游戏服务器引擎，也可称为数据库

河图把数据查询接口"暴露"给游戏客户端，客户端可以通过SDK直接向服务器 select，query 查询，并自动订阅同步，
所以河图自称为数据库。
写入操作通过调用 System，也就是游戏的逻辑代码，表现为数据库的一种数据功能。

开发简单、透明，没有复杂的API调用，同时隐去了恼人的事务、线程冲突等问题，降低心智负担。

这种结构可以大幅减少游戏服务器和客户端的开发量。

## 手把手快速示例

登录，并在地图上移动的简单示例，服务器只要20行代码，0配置文件。

### 首先定义组件

河图的数据表结构（Schema），可通过代码完成定义。

为了描述玩家的坐标，我们定义一个名为`Position`的组件（可理解为表），通过`owner`属性将其关联到玩家ID。
组件的权限设为`Permission.USER`，所有登录的客户端都可直接向河图查询该组件。

```Python
import numpy as np
from hetu.data import define_component, Property, Permission, BaseComponent

# 通过@define_component修饰，表示Position结构是一个组件
@define_component(namespace='ssw', permission=Permission.USER)
class Position(BaseComponent):
    x: np.float32 = Property(default=0)  # 定义Position.x为np.float32类型，默认值为0
    y: np.float32 = Property(default=0)  
    owner: np.int64 = Property(default=0, unique=True)  # 开启unique索引
```

* ⚠️ 警告: 不要创建名叫Player的大表，而是把Player的不同属性拆成不同的组件，比如这里坐标就单独是一个组件，
然后通过`owner`属性关联到Player身上。大表会严重影响性能和扩展性。

### 然后写System

定义`login`System（可理解为服务器逻辑），我们使用异步`aiohttp`验证`token`，这样等待网络IO的时候不堵塞Worker进程。

注意这里引入了System“继承”的概念，我们继承了内置System `elevate`提权。
System函数是不可以直接调用的，想要调用其他System，必须通过参数`inherits`继承。

`elevate`会把当前连接提权到USER组，并关联`user_id`，参数`kick_logged_in`为`Ture`表示如果该用户已登陆了，
则断开他的另一个连接。

```Python
from hetu.system import define_system, Context
import aiohttp

# permission定义为任何人可调用，inherits引用另一个hetu内置System：elevate
@define_system(namespace="ssw", permission=Permission.EVERYBODY, inherits=('elevate',))
async def login(ctx: Context, token):  # 客户端传入参数为`token`
    async with aiohttp.ClientSession() as session:
        async with session.post('https://api.sso.yoursite.com', json={'token': token}) as response:
            if response.status == 200:
                user_id = int(response.json()['user_id'])
                # 提权当前连接到User组，并关联user_id。以后ctx.caller就是user_id。
                await ctx['elevate'](ctx, user_id, kick_logged_in=True)
```

上面只是演示，为了方便测试，这里我们让客户端直接传入user_id，省去验证过程。

```Python
@define_system(namespace="ssw", permission=Permission.EVERYBODY, inherits=('elevate',))
async def login_test(ctx: Context, user_id): 
    await ctx['elevate'](ctx, user_id, kick_logged_in=True)
```

然后是玩家移动逻辑`move_to`，通过参数`components`引用要操作的表，这里我们操作玩家位置数据`Position`。
`permission`设置为只有USER组的用户才能调用，也就是只有执行过`elevate`的用户才能调用，
`ctx.caller`就是`elevate`传入的`user_id`。

```Python
@define_system(
    namespace="ssw",
    components=(Position,),   # 定义System引用的表
    permission=Permission.USER,     
)
async def move_to(ctx: Context, new_pos):
    # todo: 客户端传入的参数（new_pos）都要验证合法性，防止用户修改数据，这里省略。
    # 在Position表（组件）中查询或创建owner=ctx.caller的行，然后修改x和y
    async with ctx[Position].select_or_create(ctx.caller, where='owner') as pos:
        pos.x = new_pos.x
        pos.y = new_pos.y
        # with结束后会自动提交修改
```

服务器就完成了，我们不需要传输数据的代码，这由客户端来完成。

# Copyright & Thanks

Copyright (C) 2023-2024, by Zhang Jianhao (heeroz@gmail.com), All rights reserved.


