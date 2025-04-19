[![codecov](https://codecov.io/github/Heerozh/HeTu/graph/badge.svg?token=YFPF963NB0)](https://codecov.io/github/Heerozh/HeTu)

> [!NOTE]
> 内测中，正在公司内部开发使用

# 🌌 河图 HeTu

河图是一个开源轻量化的分布式游戏服务器引擎。集成了数据库概念，适用于从万人 MMO 到多人联机的各种场景。
开发简单、透明，没有复杂的 API 调用，高数据一致性保证，同时隐去了恼人的事务、线程冲突等问题。

基于 ECS(Entity-Component-System) 概念，采用 Python 语言，支持各种数据科学库，拥抱未来。
具体性能见下方[性能测试](#性能测试)。

开源并免费，欢迎贡献代码。商业使用只需在 Credits 中注明即可。

## 游戏服务器引擎，也可称为数据库

河图把数据查询接口"暴露"给游戏客户端，客户端通过 SDK 直接进行 select，query 查询，并订阅同步，
所以河图自称为数据库。写入操作通过 System，也就是服务器的逻辑代码。

这种结构可以大幅减少游戏服务器和客户端的开发量。

## 🔰 手把手快速示例

一个登录，并在地图上移动的简单示例。首先是服务器端部分，服务器只要 20 行代码，0 配置文件：

### 定义组件（Component）

河图的数据表结构（Schema），可通过代码完成定义。

为了描述玩家的坐标，我们定义一个名为`Position`的组件（可理解为表），通过`owner`属性将其关联到玩家 ID。
组件的权限设为`Permission.USER`，所有登录的客户端都可直接向河图查询该组件。

```Python
import numpy as np
from hetu.data import define_component, Property, Permission, BaseComponent


# 通过@define_component修饰，表示Position结构是一个组件
@define_component(namespace='ssw', permission=Permission.USER)
class Position(BaseComponent):
    x: np.float32 = Property(default=0)  # 定义Position.x为np.float32类型，默认值为0
    y: np.float32 = Property(default=0)  # 只能定义为c类型(np类型)
    owner: np.int64 = Property(default=0, unique=True)  # 开启unique索引
```

> [!WARNING]
> 不要创建名叫 Player 的大表，而是把 Player 的不同属性拆成不同的组件，比如这里坐标就单独是一个组件，
> 然后通过`owner`属性关联到 Player 身上。大表会严重影响性能和扩展性。

### 然后写 System（逻辑）

#### move_to 移动逻辑

玩家移动逻辑`move_to`通过`define_system`定义，参数`components`引用要操作的表，这里我们操作玩家位置数据`Position`。

`permission`设置为只有 USER 组的用户才能调用，
`ctx.caller`是登录用户的 id，此 id 稍后登录时会通过`elevate`方法决定。

```Python
@define_system(
    namespace="ssw",
    components=(Position,),  # 定义System引用的表
    permission=Permission.USER,
)
async def move_to(ctx: Context, x, y):
    # 在Position表（组件）中查询或创建owner=ctx.caller的行，然后修改x和y
    async with ctx[Position].update_or_insert(ctx.caller, where='owner') as pos:
        pos.x = x
        pos.y = y
        # with结束后会自动提交修改
```

客户端通过`HeTuClient.Instance.CallSystem("move_to", x, y)`调用`move_to`方法，数据变更会自动推送给所有关注此行数据的客户端。

#### Login 登录逻辑

我们定义一个`login_test`System，作为客户端登录接口。

河图有个内部 System 叫`elevate`可以帮我们完成登录，它会把当前连接提权到 USER 组，并关联`user_id`。

> [!NOTE]
> 什么是内部 System?
> 内部 System 为 Admin 权限的 System，客户端不可调用。

> [!NOTE]
> 为什么要通过内部 System？直接函数调用不行么？
> 任何函数方法，如果牵涉到数据库操作，都需要通过 System 走事务。
> 想要调用其他 System，必须通过参数`bases`继承。

```Python
from hetu.system import define_system, Context

# permission定义为任何人可调用
@define_system(namespace="ssw", permission=Permission.EVERYBODY, bases=('elevate',))
async def login_test(ctx: Context, user_id):
    # 提权以后ctx.caller就是user_id。
    await ctx['elevate'](ctx, user_id, kick_logged_in=True)
```

我们让客户端直接传入 user_id，省去验证过程。实际应该传递 token 验证。

服务器就完成了，我们不需要传输数据的代码，因为河图是个“数据库”，客户端可直接查询。

把以上内容存到`.\app\app.py`文件（或分成多个文件，然后在入口`app.py`文件`import`他们）。

#### 启动服务器

安装 Docker Desktop 后，直接在任何系统下执行一行命令即可（需要海外网）：

```bash
cd examples/server/first_game
docker run --rm -p 2466:2466 -v .\app:/app -v .\data:/data heerozh/hetu:latest start --namespace=ssw --instance=walking
```

- `-p` 是映射本地端口:到 hetu 容器端口，比如要修改成 443 端口就使用`-p 443:2466`
- `-v` 是映射本地目录:到容器目录，需映射`/app`代码目录，`/data`快照目录。`/logs`目录可选
- 其他参数见帮助`docker run --rm heerozh/hetu:latest start --help`

### 客户端代码部分

河图 Unity SDK 使用 UniTask 库，基于 async/await，支持老版本 Unity 和 WebGL 平台。

首先在 Unity 中导入客户端 SDK，点“Window”->“Package Manager”->“+加号”->“Add package from git URL”

<img src="https://github.com/Heerozh/HeTu/blob/media/sdk1.png" width="306.5" height="156.5"/>
<img src="https://github.com/Heerozh/HeTu/blob/media/sdk2.png" width="208.5" height="162.5"/>

然后输入安装地址：`https://github.com/Heerozh/HeTu.git?path=/ClientSDK/unity/cn.hetudb.clientsdk`

> 如果没外网可用国内镜像
> `https://gitee.com/heerozh/hetu.git?path=/ClientSDK/unity/cn.hetudb.clientsdk`

然后在场景中新建个空对象，添加脚本，首先是连接服务器并登录：

```c#
using Cysharp.Threading.Tasks;

public class FirstGame : MonoBehaviour
{
    public long SelfID = 1;  // 不同客户端要登录不同ID
    async void Start()
    {
        HeTuClient.Instance.SetLogger(Debug.Log, Debug.LogError, Debug.Log);
        // 连接河图，这其实是异步函数，我们没await，实际效果类似射后不管
        HeTuClient.Instance.Connect("ws://127.0.0.1:2466/hetu",
            this.GetCancellationTokenOnDestroy());

        // 调用登录System，相关封包会启动线程在后台发送
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
        // 向数据库订阅owner=1-999的玩家数据。
        // 这里也可以用Query<Position>()强类型查询，类型可通过build生成
        _allPlayerData = await HeTuClient.Instance.Query(
            "Position", "owner", 1, 999, 100);
        // 把查询到的玩家加到场景中
        foreach(var data in _allPlayerData.Rows.Values)
            AddPlayer(data);  // 代码省略

        // 当有新Position行创建时(新玩家)
        _allPlayerData.OnInsert += (sender, rowID) => {
            AddPlayer(sender.Rows[rowID]);
        };
        // 当有玩家删除时
        _allPlayerData.OnDelete += (sender, rowID) => {
            // 代码省略
        };
        // 当_allPlayerData数据中有任何行发生变动时（任何属性变动都会触发整行事件，这也是Component属性要少的原因）
        _allPlayerData.OnUpdate += (sender, rowID) => {
            var data = sender.Rows[rowID];
            var playerID = long.Parse(data["owner"]);  // 前面Query时没有带类型，所以数据都是字符串型
            var position = new Vector3(float.Parse(data["x"]), 0.5f, float.Parse(data["y"])
            MovePlayer(playerID, position);
        };
    }
```

以上，你的简单的地图移动小游戏就完成了。你可以启动多个客户端，每个客户端都会看到互相之间的移动。

完整示例代码见 examples 目录的 first_game。

## 📊 性能测试

### 配置：

|          |                 服务器 型号 |                            设置 |   
|:---------|-----------------------:|------------------------------:|
| 河图       |        ecs.c7.16xlarge | 32核64线程，默认配置，参数: --workers=76 |
| Redis7.0 | redis.shard.small.2.ce |       单可用区，双机热备，非Cluster，内网直连 |   
| 跑分程序     |                     本地 |   参数： --clients=1000 --time=5 |        

### Redis 对照：

先压测 Redis，看看 Redis 的性能上限作为对照，这指令序列等价于之后的"select + update"测试项目：

```redis
ZRANGE, WATCH, HGETALL, MULTI, HSET, EXEC
```

CPS(每秒调用次数)结果为：

|         | direct redis(Calls) |
|:--------|--------------------:|
| Avg(每秒) |            30,345.2 |

- ARM 版的 Redis 性能，hset/get 性能一致，但牵涉 zrange 和 multi 指令后性能低 40%，不建议
- 各种兼容 Redis 指令的数据库，并非 Redis，不可使用，可能有奇怪 BUG

### 测试河图性能：

- hello world 测试：序列化并返回 hello world。
- select + update：单 Component，随机单行读写，表 3W 行。

CPS(每秒调用次数)测试结果为：

| Time     | hello world(Calls) | select + update(Calls) | select*2 + update*2(Calls) | select(Calls) |
|:---------|-------------------:|-----------------------:|---------------------------:|--------------:|
| Avg(每秒)  |            222,443 |               33,900.6 |                   18,237.6 |      90,979.6 |
| CPU负载    |                99% |                    50% |                        40% |           70% |
| Redis负载  |                 0% |                    99% |                        99% |           99% |

以上测试为单 Component，多个 Component 有机会（要低耦合度）通过 Redis Cluster 扩展。

### 单连接性能：

测试程序使用`--clients=1`参数测试，单线程同步堵塞模式，主要测试 RTT：

| Time     | hello world(Calls) | select + update(Calls) | select*2 + update*2(Calls) | select(Calls) |
|:---------|-------------------:|-----------------------:|---------------------------:|--------------:|
| Avg(每秒)  |           8,738.96 |               1,034.67 |                     632.65 |      1,943.82 |
| RTT(ms)  |            0.11443 |               0.966495 |                    1.58065 |       0.51445 |
    

### 关于 Python 性能

首先河图是异步+分布式的，吞吐量和 RTT 都不受制于语言，而受制于后端 Redis。作为参考，Python 性能大概是 PHP7 水平。

之前基于性能选择过 LuaJIT，但 Lua 写起来并不轻松，社区也差。考虑到现在的 CPU 价格远低于开发人员成本，快速迭代，数据分析，无缝 AI，社区活跃的宛如人肉 JIT 的 Python，更具有优势。

HeTu 未来会支持 Rust 代码，可提供 Native 的性能（实现中)，况且 Component 本来就是 C 结构。

## ⚙️ 服务器安装

### 容器启动

使用 hetu 的 docker 镜像，此镜像内部集成了 Redis，适合快速开始。

```bash
docker run --rm -v .\本地app目录/app:/app -v .\本地数据目录:/data -p 2466:2466 heerozh/hetu:latest start --namespace=namespace --instance=server_name
```

其他参数可用`docker run --rm heerozh/hetu:latest --help`查看，

也可以使用 Standalone 模式，只启动河图，不启动 Redis。

```bash
docker run --rm -p 2466:2466 -v .\本地目录\app:/app heerozh/hetu:latest start --config /app/config.yml --standalone
```

可以启动多台 hetu standalone 服务器，然后用反向代理对连接进行负载均衡。

后续启动的服务器需要把`--head`参数设为`False`，以防进行数据库初始化工作（重建索引，删除临时数据）。

### 原生启动！

容器一般有 30%的性能损失，为了性能，也可以用原生方式。

先安装[miniconda](https://mirrors.tuna.tsinghua.edu.cn/anaconda/miniconda/)
软件管理器，含有编译好的 Python 任意版本，河图需要 Python3.12.5 以上版本。

服务器部署可用安装脚本（清华镜像）：

```shell
mkdir -p ~/miniconda3
wget https://mirrors.tuna.tsinghua.edu.cn/anaconda/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda3/miniconda.sh
bash ~/miniconda3/miniconda.sh -b -u -p ~/miniconda3
rm -rf ~/miniconda3/miniconda.sh
~/miniconda3/bin/conda init bash
exec bash
```

然后创建新的 Python 环境：

```shell
conda create -n hetu python=3.12
```

别忘了激活环境:

```shell
conda activate hetu
```

然后用传统 pip 方式安装河图到当前环境：

```shell
pip install git+https://github.com/Heerozh/HeTu.git
```

国内镜像地址：`pip install git+https://gitee.com/heerozh/hetu.git`

还要部署 Redis，持久化模式，这里跳过。

大功告成：

```bash
hetu start --app-file=/path/to/app.py --db=redis://127.0.0.1:6379/0 --namespace=ssw --instance=server_name
```

其他参数见`hetu start --help`，比如可以用`hetu start --config ./config.yml`方式启动，
配置模板见 CONFIG_TEMPLATE.yml 文件。

### 内网离线环境

想要在内网设置环境，外网机执行上述原生启动步骤后，把 miniconda 的整个安装目录复制过去即可。

### 生产部署

生产环境下，除了执行上述一种启动步骤外，还要建议设立一层反向代理，并进行负载均衡。

Redis 推荐用 master+多机只读 replica 的分布式架构，数据订阅都可分流到 replica，大幅降低 master 负载。

反向代理选择：

- Caddy: 自动 https 证书，自动反代头设置和合法验证，可通过 api 调用动态配置负载均衡
  - 命令行：`caddy reverse-proxy --from 你的域名.com --to hetu服务器1_ip:8000 --to hetu服务器2_ip:8000`
- Nginx: 老了，配置复杂，且歧义多，不推荐

## ⚙️ 客户端 SDK 安装

### C# SDK

此 SDK 基于.Net WebSocket 和多线程，也支持 Unity 2022 及以上版本（除 WebGL 平台）

可直接使用`ClientSDK/csharp/HeTuClient.cs`。

### Unity SDK

Unity SDK 支持 Unity 2018.3 及以上版本，含所有平台（包括 WebGL），基于 UnityWebSocket 和 UniTask，已内置在 SDK 库中。

在 Unity Package Manager 中使用以下地址安装：
`https://github.com/Heerozh/HeTu.git?path=/ClientSDK/unity/cn.hetudb.clientsdk`

如果项目已有 UniTask 依赖，可以择一删除。

### TypeScript SDK

用法和接口几个 SDK 都基本一致，但 TS 的可以省去本地类型转换，比 C# 方便。

`npm install --save Heerozh/HeTu#npm`

## 📚 文档：

由于结构简单，只有几个类方法，具体可以直接参考代码文档注释，建议通过 github 的 AI 直接询问。

如果日后接口方法变多时，会有详细文档。

## 🗯 讨论

前往 github discussions

## ⚖️ 代码规范

按照 python 的标准代码规范，PEP8，注释要求为中文。

# ©️ Copyright & Thanks

Copyright (C) 2023-2025, by Zhang Jianhao (heeroz@gmail.com), All rights reserved.
