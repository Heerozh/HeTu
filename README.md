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
> [!WARNING]  
> 不要创建名叫Player的大表，而是把Player的不同属性拆成不同的组件，比如这里坐标就单独是一个组件，
然后通过`owner`属性关联到Player身上。大表会严重影响性能和扩展性。

### 然后写System

#### login登录逻辑

Login逻辑可用内置System `elevate`完成，`elevate`会把当前连接提权到USER组，并关联`user_id`。

System包含事务处理，不可直接函数调用，想要调用其他System，必须通过参数`bases`继承。

```Python
from hetu.system import define_system, Context

# permission定义为任何人可调用
@define_system(namespace="ssw", permission=Permission.EVERYBODY, bases=('elevate',))
async def login_test(ctx: Context, user_id): 
    # 提权以后ctx.caller就是user_id。
    await ctx['elevate'](ctx, user_id, kick_logged_in=True)
```
我们让客户端直接传入user_id，省去验证过程。实际应该传递token验证。

#### move_to移动逻辑

然后是玩家移动逻辑`move_to`，通过参数`components`引用要操作的表，这里我们操作玩家位置数据`Position`。

`permission`设置为只有USER组的用户才能调用，
`ctx.caller`就是之前`elevate`传入的`user_id`。

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

把以上内容存到`.\app\app.py`文件（或分成多个文件，然后在入口`app.py`文件`import`他们）。

#### 启动服务器

安装Docker Desktop后，直接在任何系统下执行一行命令即可（需要外网访问能力）：

```bash
docker run --rm -p 2466:2466 -v .\app:/app -v .\data:/data heerozh/hetu:latest start --namespace=ssw --instance=walking
````
* `-p` 是映射本地端口到hetu容器端口，比如要修改成443端口就使用`-p 443:2466`
* `-v` 是映射本地目录到hetu容器目录(`/app`和`/data`和`/logs`目录)
* 其他参数见帮助`docker run --rm heerozh/hetu:latest start --help`

### 客户端代码部分

首先在Unity中导入客户端SDK，点“Window”->“Package Manager”->“+加号”->“Add package from git URL”

<img src="https://github.com/Heerozh/HeTu/blob/media/sdk1.png" width="306.5" height="156.5"/>
<img src="https://github.com/Heerozh/HeTu/blob/media/sdk2.png" width="208.5" height="162.5"/>

然后输入安装地址：`https://github.com/Heerozh/HeTu.git?path=/ClientSDK/unity/cn.hetudb.clientsdk`

> 如果没外网可用国内镜像`https://gitee.com/heerozh/hetu.git?path=/ClientSDK/unity/cn.hetudb.clientsdk`

然后在场景中新建个空对象，添加脚本，首先是连接服务器并登录：

```c#
public class FirstGame : MonoBehaviour
{
    public long SelfID = 1;  // 不同客户端要登录不同ID
    async void Start()
    {
        HeTuClient.Instance.SetLogger(Debug.Log, Debug.LogError, Debug.Log);
        // 连接河图，这是异步函数，不await就是射后不管
        HeTuClient.Instance.Connect("ws://127.0.0.1:2466/hetu",
            Application.exitCancellationToken);
            
        // 调用登录，会启动线程在后台发送
        HeTuClient.Instance.CallSystem("login_test", SelfID);
        
        await SubscribeOthersPositions();
    }
}
```
然后在玩家移动后往服务器发送新的坐标：

```c#
    void Update()
    {
        // 获得输入变化
        var vec = new Vector3(Input.GetAxis("Horizontal"), 0, Input.GetAxis("Vertical"));
        vec *= (Time.deltaTime * 10.0f);
        transform.position += vec;
        // 向服务器发送自己的新位置
        if (vec != Vector3.zero)
            HeTuClient.Instance.CallSystem("move_to", transform.position.x, transform.position.z);
    }
}
```

最后就是显示其他玩家的实时位置，可以在任意`async`函数中进行。

```c#
    async void SubscribeOthersPositions()
    {
        // 向数据库订阅owner=1-999的玩家数据。在河图里，查询就是订阅
        // 这里也可以用Query<Position>()强类型查询，类型可通过build生成
        _allPlayerData = await HeTuClient.Instance.Query(
            "Position", "owner", 1, 999, 100);
        // 把查询到的玩家加到场景中
        foreach(var data in _allPlayerData.Rows.Values)
            AddPlayer(data);
        
        // 当有新Position行创建时(新玩家)
        _allPlayerData.OnInsert += (sender, rowID) => {
            AddPlayer(sender.Rows[rowID]);
        };
        // 当有玩家删除时
        _allPlayerData.OnDelete += (sender, rowID) => {
        };
        // 当有玩家Position组件的任意属性变动时（这也是Component属性要少的原因）
        _allPlayerData.OnUpdate += (sender, rowID) => {
            var data = sender.Rows[rowID];
            // 前面Query时没有带类型，所以数据都是字符串型
            var playerID = long.Parse(data["owner"]);
            var position = new Vector3(float.Parse(data["x"]), 0.5f, float.Parse(data["y"])
            MovePlayer(playerID, position);
        };
    }
```

以上，你的简单的地图移动小游戏就完成了。你可以启动多个客户端，每个客户端都会看到互相之间的移动。

完整示例代码见examples目录的first_game。


## 性能测试


### 配置：

|       |                     服务器 型号 |                                  设置 |   
|:------|---------------------------:|------------------------------------:|
| 河图    |           ecs.ic5.16xlarge |           64核，关SSL，参数: --workers=76 |
| Redis |     redis.shard.small.2.ce |             单可用区，双机热备，非Cluster，内网直连 |   
| 跑分程序  |                         本地 |         参数： --clients=1000 --time=5 |        

### 基准：

直接在Redis上压测以下最少事务指令作为基准，这指令序列等价于之后的select + update测试：

```redis
ZRANGE, WATCH, HGETALL, MULTI, HSET, EXEC
```

Redis基准性能CPS(每秒调用次数)结果为：


|         | direct redis(Calls) |
|:--------|--------------------:|
| Avg(每秒) |            30,345.2 |


### 测试河图性能：

- hello world测试: 序列化并返回hello world，主要消耗在json和zlib压缩
- select + update：单Component获取行并写入行操作，表总数据量3W行。

CPS(每秒调用次数)测试结果为：

|         | hello world(Calls) | select + update(Calls) | select\*2 + update\*2(Calls) |
|:--------|-------------------:|-----------------------:|-----------------------------:|
| Avg(每秒) |            125,117 |               30,285.1 |                     16,112.7 |
| CPU负载   |               100% |                    65% |                          45% |
| Redis负载 |                 0% |                   100% |                         100% |

以上测试为单Component，多个Component有机会（但不多）通过Redis Cluster扩展。

### 单连接性能：

测试程序使用`--clients=1`参数测试，未用满CPU，主要测试RTT：

|         | hello world(Calls) | select + update(Calls) | select\*2 + update\*2(Calls) |
|:--------|-------------------:|-----------------------:|-----------------------------:|
| Avg(每秒) |               1823 |                231.217 |                      153.244 |
| RTT(毫秒) |               0.54 |                   4.25 |                          6.7 |


### 关于Python性能

河图是分布式的，吞吐量上限不受制于逻辑代码，而受制于后端Redis，语言性能只影响RTT。作为参考，Python性能大概是PHP7水平。

之前一直用LuaJIT，虽然速度很快，但代码实在是太繁重了。

考虑到现在的CPU价格远低于开发人员成本，快速迭代，数据分析，无缝AI等特性具有优势。


## 安装和启动

### 整合版启动

使用hetu的docker镜像，此镜像内部集成了Redis，部署方便，适合千人在线的网络游戏，或开发测试用。

```bash
docker run --rm -v .\本地app目录/app:/app -v .\本地数据目录:/data -p 2466:2466 heerozh/hetu:latest start --namespace=namespace --instance=server_name
```
其他参数可用`docker run --rm heerozh/hetu:latest --help`查看，

### 分布式部署

对于大型网络游戏，redis可以自己单独部署，以实现横向扩展。

Redis部署，我们推荐用master+多机只读replica的分布式架构，这里我们只演示启动一个master：

```bash
docker run --name backend-redis -p 6379:6379 -v .\data/:/data redis:latest redis-server --save 60 1
```

然后再运行hetu镜像，开启standalone只启动河图，并使用config.py作为配置文件：

```bash
docker run --rm -p 2466:2466 -v .\本地目录\app:/app heerozh/hetu:latest start --config /app/config.py --standalone
```
配置方法具体见CONFIG_TEMPLATE.py文件。

可以启动多台hetu standalone服务器，然后用反向代理对连接进行负载均衡。
后续启动的服务器需要把`--head`参数设为`False`，以防止它们进行数据库初始化工作（主要是重建索引，删除临时数据等）。


### 原生启动！

如果是开发机，为了调试方便，可以用原生方式。

先安装[miniconda](https://mirrors.tuna.tsinghua.edu.cn/anaconda/miniconda/)，
Conda是软件堆栈管理器，含有编译好的Python任意版本，河图需要Python3.11.3以上版本。

服务器部署可用安装脚本：
```shell
mkdir -p ~/miniconda3
wget https://mirrors.tuna.tsinghua.edu.cn/anaconda/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda3/miniconda.sh
bash ~/miniconda3/miniconda.sh -b -u -p ~/miniconda3
rm -rf ~/miniconda3/miniconda.sh
~/miniconda3/bin/conda init bash
exec bash
```

创建新的Python环境：

```shell
conda create -n hetu python=3.11
```

别忘了激活环境:
```shell
conda activate hetu
````

然后用传统pip方式安装河图到当前环境：

```shell
pip install git+https://github.com/Heerozh/HeTu.git
````
国内镜像地址：`pip install https://gitee.com/heerozh/hetu.git`

然后：
```bash
hetu start --app-file=/path/to/app.py --db=redis://127.0.0.1:6379/0 --namespace=ssw --instance=server_name
```
其他参数见`hetu start --help`，比如可以用`hetu start --config ./config.py`方式启动，
配置模板见CONFIG_TEMPLATE.py文件。

另外别忘了还要自己安装和启动Redis。


## 数据库文档：

文档链接在这：建设中...

## 代码规范

按照python的标准代码规范，PEP8，注释要求为中文。

# Copyright & Thanks

Copyright (C) 2023-2024, by Zhang Jianhao (heeroz@gmail.com), All rights reserved.


