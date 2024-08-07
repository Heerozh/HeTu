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

> ⚠️ 警告: 不要创建名叫Player的大表，而是把Player的不同属性拆成不同的组件，比如这里坐标就单独是一个组件，
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
async def move_to(ctx: Context, x, y):
    # todo: 客户端传入的参数（x, y）都要验证合法性，防止用户修改数据，这里省略。
    # 在Position表（组件）中查询或创建owner=ctx.caller的行，然后修改x和y
    async with ctx[Position].select_or_create(ctx.caller, where='owner') as pos:
        pos.x = x
        pos.y = y
        # with结束后会自动提交修改
```

服务器就完成了，我们不需要传输数据的代码，这由客户端来完成。

把以上内容存到app.py文件（或分成多个文件，然后在入口app.py文件`import`他们）。

让我们用docker启动，安装Docker Desktop后，在任何系统下都只需下列命令即可：
(注意，你可能需要外网访问能力)

todo 搞一个集成的docker，包含数据库和服务器一起的, redis可以用-v <data-dir>:/data来保存到本地
Run redis-server with persistent data directory. (creates dump.rdb)
docker run -d -p 6379:6379 -v <data-dir>:/data --name redis dockerfile/redis

假设你的app.py文件放在./app/目录下
```bash
docker run --name walk_game -p 2468:2468 -v ./app/:/app hetu:latest --debug=True --namespace=ssw --instance=inst_name
````
* -p 是映射本地端口到hetu容器端口，本地端口可随意指定，比如要修改成443端口就使用-p 443:2468
* -v 是映射本地目录到hetu容器目录(可映射/app和/data目录)，本地目录可以随意指定。
* 如果映射了/data目录，那么数据库文件将存放到你的本地目录，而不是docker容器内部，方便你更换和升级容器
* hetu:latest 是使用最新版hetu镜像
* --debug=True 是开启调试模式，用于生成自签SSL证书
* --namespace 是启动app.py中哪个namespace下的System
* --instance 是实例名，数据都储存在该实例名下

### 客户端代码部分

首先在Unity中导入客户端SDK，点“Window”->“Package Manager”->“+加号”->“Add package from git URL”
![菜单](https://github.com/Heerozh/HeTu/blob/media/sdk1.png)
![第二步](https://github.com/Heerozh/HeTu/blob/media/sdk2.png)

然后输入安装地址：`https://github.com/Heerozh/HeTu.git?path=/ClientSDK/unity/cn.hetudb.clientsdk`

> 如果没外网可用国内镜像`https://gitee.com/heerozh/hetu.git?path=/ClientSDK/unity/cn.hetudb.clientsdk`

然后在场景中新建个空对象，添加如下脚本：

```c#
using System.Collections.Generic;
using UnityEngine;
using HeTu;
using Random = UnityEngine.Random;

public class FirstGame : MonoBehaviour
{
    public GameObject playerPrefab;
    CharacterController _characterController;
    readonly Dictionary<long, GameObject> _players = new ();
    IndexSubscription<DictComponent> _allPlayerData;
    long _selfID = 0;
    
    // 在场景中生成玩家代码
    void AddPlayer(DictComponent row)
    {
        GameObject player = Instantiate(playerPrefab, 
            new Vector3(float.Parse(row["x"]), 0.5f, float.Parse(row["y"])), 
            Quaternion.identity);
        _players[long.Parse(row["owner"])] = player;
    }

    async void Start()
    {
        _characterController = gameObject.GetComponent<CharacterController>();
        _selfID = Random.Range(1, 20); // 随机登录1-20号玩家

        HeTuClient.Instance.SetLogger(Debug.Log, Debug.LogError, Debug.Log);
        // 连接河图，我们没有await该异步Task，暂时先射后不管
        var task = HeTuClient.Instance.Connect("ws://127.0.0.1:2466/hetu",
            Application.exitCancellationToken);
        
        // 调用登录，调用会在连接完成后执行
        HeTuClient.Instance.CallSystem("login_test", _selfID);
        
        // 向数据库订阅owner=1-20的玩家数据。在河图里，查询就是订阅
        _allPlayerData = await HeTuClient.Instance.Query(
            "Position", "owner", 1, 20, 100);
        // 把查询到的玩家加到场景中，这些是首次数据，后续的更新要靠OnUpdate回调
        foreach(var data in _allPlayerData.Rows.Values)
            if (long.Parse(data["owner"]) != _selfID) AddPlayer(data);
        
        // 当有新玩家Position数据创建时(新玩家创建)
        _allPlayerData.OnInsert += (sender, rowID) => {
            AddPlayer(sender.Rows[rowID]);
        };
        // 当有玩家删除时，我们没有删除玩家Position数据的代码，所以这里永远不会被调用
        _allPlayerData.OnDelete += (sender, rowID) => {
        };
        // 当有玩家Position组件的任意属性变动时会被调用（这也是每个Component属性要少的原因）
        _allPlayerData.OnUpdate += (sender, rowID) => {
            var data = sender.Rows[rowID];
            if (long.Parse(data["owner"]) == _selfID) return;
            // 为了方便演示，前面Query时没有带类型，所以这里都要进行类型转换。生成客户端类型见build相关说明。
            _players[long.Parse(data["owner"])].transform.position = new Vector3(
                float.Parse(data["x"]), 0.5f, float.Parse(data["y"]));
        };
    
        // 在最后await Connect的Task。该task会堵塞直到断线
        await task;
        Debug.Log("连接断开");
    }

    void Update()
    {
        // 获得输入变化
        var vec = new Vector3(Input.GetAxis("Horizontal"), 0, Input.GetAxis("Vertical"));
        vec *= (Time.deltaTime * 10.0f);
        _characterController.Move(vec);
        // 向服务器发送自己的新位置
        if (vec != Vector3.zero)
        {
            HeTuClient.Instance.CallSystem("move_to",
                gameObject.transform.position.x, gameObject.transform.position.z);
        }
    }
}
```

以上，你的简单的地图移动小游戏就完成了。你可以启动多个客户端，每个客户端都会看到互相之间的移动。

完整示例代码见examples目录的first_game。


## 代码规范

按照python的标准代码规范，PEP8，注释要求为中文。

# Copyright & Thanks

Copyright (C) 2023-2024, by Zhang Jianhao (heeroz@gmail.com), All rights reserved.


