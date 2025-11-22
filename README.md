[![codecov](https://codecov.io/github/Heerozh/HeTu/graph/badge.svg?token=YFPF963NB0)](https://codecov.io/github/Heerozh/HeTu)

> [!NOTE]
> 内测中，正在公司内部开发使用

 [ <img src="https://devin.ai/favicon.ico" style="height: 1em;"/> English Summary (AI) ](https://deepwiki.com/Heerozh/HeTu)

# 🌌 河图 HeTu

河图是一个分布式游戏服务器引擎。类似supabase，但专为游戏轻量化设计。

- 高开发效率：透明，直接写逻辑，无需关心数据库，事务/线程冲突等问题。
- Python 语言：支持各种数据科学库，拥抱未来。
- 高性能：高并发异步架构 + Redis 后端，数据库操作性能约10x倍于supabase等。
- Unity客户端SDK：支持C# Reactive，调用简单，基于服务器推送的天然响应式，视图与业务解耦。

具体性能见下方[性能测试](#-性能测试)。

## 实时数据库

河图把数据库只读接口"暴露"给游戏客户端，客户端通过 SDK 在 RLS(行级权限) 下可安全的进行 select/query 订阅。
订阅后数据自动同步，底层由数据库写入回调实现，无需轮询，响应速度<1ms。

写入操作只能由服务器的逻辑代码执行，客户端通过RPC远程调用。类似BaaS的储存过程，但更易写。

## 开源免费

欢迎贡献代码。商业使用只需在 Credits 中注明即可。

## 🔰 快速示例（30行）

一个登录，并在地图上移动的简单示例。

### 定义组件（Component）

为了描述玩家的坐标，我们定义一个名为`Position`的组件（可理解为表Schema），通过`owner`属性将其关联到玩家 ID。
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

### 定义 System（逻辑）

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
    # 注：可简写为ctx[Position].upsert
    async with ctx.session.select(Position).upsert(ctx.caller, where='owner') as pos:
        pos.x = x
        pos.y = y
        # with结束后会自动提交修改
```

客户端通过`HeTuClient.Instance.CallSystem("move_to", x, y)`可直接调用。

#### Login 登录逻辑

我们定义一个`login_test`System，作为客户端登录接口。

河图有个内部 System 叫`elevate`可以帮我们完成登录，它会把当前连接提权到 USER 组，并关联`user_id`。

> [!NOTE]
> 什么是内部 System? 如何调用？
> 内部 System 为 Admin 权限的 System，用户不可调用。
> System都牵涉到数据库事务操作，因此须通过参数`bases`继承，让事务连续。

```Python
from hetu.system import define_system, Context
from hetu.system import elevate

# permission定义为任何人可调用
@define_system(namespace="ssw", permission=Permission.EVERYBODY, bases=(elevate,))
async def login_test(ctx: Context, user_id):
    # 提权以后ctx.caller就是user_id。
    await elevate(ctx, user_id, kick_logged_in=True)
```

我们让客户端直接传入 user_id，省去验证过程。实际应该传递 token 验证。

服务器就完成了，我们不需要传输数据的代码，因为河图是个“数据库”，客户端可直接查询。

把以上内容存到`.\src\app.py`文件（或分成多个文件，然后在入口`app.py`文件`import`他们）。

#### 启动服务器

详见 [安装](#%EF%B8%8F-安装) 部分：

```bash
# 安装Docker Desktop后，启动Redis服务器(开发环境用，需外网）
docker run -d --rm --name hetu-redis -p 6379:6379 redis:latest
# 启动你的App服务器
cd examples/server/first_game
uv run hetu start --app-file=./src/app.py --db=redis://127.0.0.1:6379/0 --namespace=ssw --instance=walking
```

### 客户端代码部分

河图 Unity SDK 基于 async/await，支持 Unity 2018 以上 和 WebGL 平台。

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

        // 调用登录System，连接成功后会在后台发送
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
```

最后就是显示其他玩家的实时位置，我们通过订阅回调，自动获取玩家数据更新。

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
        // 当_allPlayerData数据中有任何行发生变动时
        //（任何属性变动都会触发整行事件，这也是Component属性要少的原因）
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
| 河图       |        ecs.c8a.16xlarge | 32核64线程，默认配置，参数: --workers=76 |
| Redis7.0 | redis.shard.small.2.ce |       单可用区，双机热备，非Cluster，内网直连 |   
| 跑分程序     |                     本地 |   参数： --clients=1000 --time=5 |        

### Redis 对照：

先压测 Redis，看看 Redis 的性能上限作为对照，这指令序列等价于之后的"select + update"测试项目：

```redis
ZRANGE, WATCH, HGETALL, MULTI, HSET, EXEC
```

CPS(每秒调用次数)结果为：

| Time\Calls | ZRANG...EXEC |
| :--------- | -----------: |
| Avg(每秒)  |     30,345.2 |

- ARM 版的 Redis 性能，hset/get 性能一致，但牵涉 zrange 和 multi 指令后性能低 40%，不建议
- 各种兼容 Redis 指令的数据库，并非 Redis，不可使用，可能有奇怪 BUG

### 测试河图性能：

- hello world 测试：序列化并返回 hello world。
- select + update：单 Component，随机单行读写，表 3W 行。

CPS(每秒调用次数)测试结果为：

| Time     | hello world(Calls) | select + update(Calls) | select*2 + update*2(Calls) | select(Calls) |
|:---------|-------------------:|-----------------------:|---------------------------:|--------------:|
| Avg(每秒)  |            404,670 |               39,530.3 |                   20,458.3 |       102,799 |
| CPU负载    |                99% |                    34% |                        26% |           65% |
| Redis负载  |                 0% |                    99% |                        99% |           99% |

* _以上测试为单 Component，多个 Component 有机会（要低耦合度）通过 Redis Cluster 扩展。_
* _在Docker中压测，hello world结果为314,241（需要关闭bridge网络--net=host），其他项目受限数据库性能，不影响。_

### 单连接性能：

测试程序使用`--clients=1`参数测试，单线程同步堵塞模式，主要测试 RTT：

| Time     |  hello world(Calls) | select + update(Calls) | select*2 + update*2(Calls) | select(Calls) |
|:---------|--------------------:|-----------------------:|---------------------------:|--------------:|
| Avg(每秒)  |            14,353.7 |               1,142.13 |                    698.544 |      2,142.06 |
| RTT(ms)  |           0.0696686 |               0.875555 |                    1.43155 |      0.466841 |
    

### 关于 Python 性能

不用担心 Python 的性能。CPU 价格已远低于开发人员成本，快速迭代，数据分析，AI 生态更具有优势。

现在 Python 社区活跃，宛如人肉JIT，且在异步+分布式架构下，吞吐量和 RTT 都不受制于语言，而受制于后端 Redis。

### Native 计算

由于 Component 数据本来就是 NumPy C 结构，可以使用LuaJIT的FFI，以极低代价调用 C/Rust 代码：
```python
from cffi import FFI
ffi = FFI()
ffi.cdef("""
    void process(char* data); // char*需转换成Position*
""")
c_lib = ffi.dlopen('lib.dll')

# 获取Array of Position
rows = await ctx[Position].query('x', pos.x - 10, pos.x + 10)
c_lib.process(ffi.from_buffer("float[]", rows))  # 无拷贝，传递指针
await ctx[Position].update_rows(rows)
```

注意，你的 C 代码不一定比 NumPy 自带的方法更优，类似这种二次索引在Python下支持SIMD更快：`rows.x[rows.x >= 10] -= 10`


## ⚙️ 安装

开发环境建议用 uv 包管理安装。 Windows可在命令行执行：
```bash
winget install --id=astral-sh.uv  -e
```

新建你的项目目录，在目录中初始化uv（最低版本需求 `3.13`）：

```shell
uv init --python "3.14"
```

此后你的项目就由uv管理，类似npm，然后把河图添加到你的项目依赖中：

```shell
uv add hetudb
```

还要部署 Redis，开启持久化模式，这里跳过。

启动河图：

```bash
uv run hetu start --app-file=./app.py --db=redis://127.0.0.1:6379/0 --namespace=ssw --instance=server_name
```

其他参数见`hetu start --help`，比如可以用`hetu start --config ./config.yml`方式启动，
配置模板见 CONFIG_TEMPLATE.yml 文件。

### 内网离线开发环境

uv会把所有依赖放在项目目录下（.venv），因此很简单，外网机执行上述步骤后，把整个项目目录复制过去即可。

内网建议跳过uv直接用`source .venv/bin/activate` (或`.\.venv\Scripts\activate.ps1`) 激活环境使用。

## 🎉 生产部署

生产环境推荐用 Docker 部署或 pip 直接安装，这2种都有国内镜像源。

### Docker 部署

安装 Docker，详见[阿里云镜像](https://help.aliyun.com/zh/ecs/user-guide/install-and-use-docker):

```bash
#更新包管理工具
sudo apt-get update
#添加Docker软件包源
sudo apt-get -y install apt-transport-https ca-certificates curl software-properties-common
sudo curl -fsSL http://mirrors.cloud.aliyuncs.com/docker-ce/linux/debian/gpg | sudo apt-key add -
sudo add-apt-repository -y "deb [arch=$(dpkg --print-architecture)] http://mirrors.cloud.aliyuncs.com/docker-ce/linux/debian $(lsb_release -cs) stable"
#安装Docker社区版本，容器运行时containerd.io，以及Docker构建和Compose插件
sudo apt-get -y install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
```

在你的项目目录下，创建 `Dockerfile` 文件，内容如下：

```Dockerfile
# 如果是阿里云内网请用 registry-vpc.cn-shanghai.aliyuncs.com/heerozh/hetu:latest
FROM registry.cn-shanghai.aliyuncs.com/heerozh/hetu:latest

WORKDIR /app

COPY . .
RUN pip install .

ENTRYPOINT ["hetu", "start", "--config=./config.yml"]
```

这里使用的是国内镜像，国外可用 [Docker Hub 的镜像](https://hub.docker.com/r/heerozh/hetu)。
`hetu:latest`表示最新版本，你也可以指定版本号。

注意你的项目目录格式得符合src-layout，不然RUN pip install .会失败。

然后执行：

```bash
# 编译你的应用镜像
docker build -t app_image_name .
# 启动你的应用
docker run -it --rm -p 2466:2466 --name server_name app_image_name --head=True
```

使用 Docker 的目的是为了河图的灵活启停特性，可以设置一台服务器为常驻包年服务器，其他都用9折的抢占服务器，然后用反向代理对连接进行负载均衡。

后续启动的服务器需要把`--head`参数设为`False`，以防进行数据库初始化工作（重建索引，删除临时数据）。

### pip 原生部署

容器一般有 20% 的性能损失，常驻服务器可以用pip的方式部署 (无须安装uv)，且pip在国内云服务器都自带加速镜像。

原生部署困难处在于如何安装高版本 python，建议通过清华miniconda源安装，uv、pyenv等都需要海外网。

```bash
# 通过miniconda安装python 3.14
mkdir -p ~/miniconda3
wget https://mirrors.tuna.tsinghua.edu.cn/anaconda/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda3/miniconda.sh
bash ~/miniconda3/miniconda.sh -b -u -p ~/miniconda3
rm -rf ~/miniconda3/miniconda.sh
~/miniconda3/bin/conda init bash
exec bash

# 然后创建新的Python环境：
conda create -n hetu python=3.14

# 进入项目目录
cd your_app_directory
# 每次执行python指令前都要执行此命令激活环境
conda activate hetu  
# 根据项目pyproject.toml安装依赖，河图应该在其中
pip install .
# 启动河图
hetu start --config=./config.yml --head=True
```

### Redis部署

Redis 配置只要开启持久化即可。 推荐用 master+多机只读 replica 的分布式架构，数据订阅都可分流到 replica，大幅降低 master 负载。

> [!NOTE]
> * 不要使用兼容 Redis
> * 不要使用非直连的 Redis

### 负载均衡

生产环境下，对河图还要建议设立一层反向代理，并进行负载均衡。

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

> [!NOTE]
> 如果使用 Unity 6 及以上版本，SDK 使用Unity 原生 Async 库，可以直接删除 UniTask 目录。

### TypeScript SDK

用法和接口和之前的 Unity 示例基本一致，安装：

`npm install --save Heerozh/HeTu#npm`

用法：
```typescript
import { HeTuClient, ZlibProtocol, BrowserWebSocket, logger as HeTuLogger } from "hetu-sdk";
HeTuLogger.setLevel(-1) // 设置日志级别
HeTuClient.setProtocol(new ZlibProtocol()) // 设置压缩协议
HeTuClient.connect(new BrowserWebSocket('ws://127.0.0.1:2466/hetu'))

// 订阅行 (类似select * from HP where owner=100)
const sub1 = await HeTuClient.select('HP', 100, 'owner')

// 订阅索引 (类似select * form Position where x >=0 and x <= 10 limit 100)
// 并注册更新回调
const sub2 = await HeTuClient.query('Position', 'x', 0, 10, 100)
sub2!.onInsert = (sender, rowID) => {
    newPlayer = sender.rows.get(rowID)?.owner
}    
sub2!.onDelete = (sender, rowID) => {
    removedPlayer = sender.rows.get(rowID)?.owner
}
sub2!.onUpdate = (sender, rowID) => {
    const data = sender.rows.get(rowID)
}
// 调用远端函数
HeTuClient.callSystem('move_user', ...)
// 取消订阅，在这之前数据有变更都会对订阅推送
sub1.dispose()
sub2.dispose()
// 退出        
HeTuClient.close()
```

## 📚 文档：

由于结构简单，只有几个类方法，具体可以直接参考代码文档注释，建议通过 github 的 AI 直接询问。

如果日后接口方法变多时，会有详细文档。

## 🗯 讨论

前往 github discussions

## ⚖️ 代码规范

按照 python 的标准代码规范，PEP8，注释要求为中文。

# ©️ Copyright & Thanks

Copyright (C) 2023-2025, by Zhang Jianhao (heeroz@gmail.com), All rights reserved.
