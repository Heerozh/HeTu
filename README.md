[![codecov](https://codecov.io/github/Heerozh/HeTu/graph/badge.svg?token=YFPF963NB0)](https://codecov.io/github/Heerozh/HeTu)

> [!NOTE]
> 内测中，正在公司内部开发使用

[ <img src="https://devin.ai/favicon.ico" style="height: 1em;"/> English Summary (AI) ](https://deepwiki.com/Heerozh/HeTu)

# 🌌 河图 HeTu

河图是一个可自动伸缩的高性能游戏服务器引擎，采用现代“数据驱动开发”范式，
但重高频 RPC 和强状态的内存计算。

90%代码由古法编程实现，并且都经过精心设计。

- 高开发效率：2-Tier（两层模式），透明，直接写逻辑，无需关心数据库，事务/线程冲突等问题。
- Python 语言：支持各种AI数据科学库，拥抱未来。
- 高性能：高并发异步架构 + Redis 后端，数据库操作性能约 10x 倍于 supabase 等。
- Stateful：不同于其他同类平台只专注数据，河图专注有状态的长连接计算。
- Unity 客户端 SDK：支持 C# Reactive，调用简单，基于服务器推送的天然响应式，视图与业务解耦。

具体性能见下方[性能测试](#-性能测试)。

## Schema即API

河图把数据库只读接口"暴露"给游戏客户端，客户端通过 SDK 在 RLS(行级权限) 下可安全的进行
get/range 订阅。
订阅后数据自动同步，底层由数据库写入回调实现，无需轮询，响应速度<1ms。

写入操作只能由服务器的逻辑代码执行，客户端通过 RPC 远程调用。类似储存过程，但更易写。

## 开源免费

欢迎贡献代码。商业使用只需在 Credits 中注明即可。

## 🔰 快速示例

一个简单的聊天室， 服务器代码：

```Python
# 定义一个 Component，类似数据库表，属性即字段
@hetu.define_component(namespace="Chat", permission=hetu.Permission.EVERYBODY)
class ChatMessage(hetu.BaseComponent):
    owner: np.int64 = hetu.property_field(0, index=True)
    text: str = hetu.property_field("", dtype="U256")


# 定义一个 System，客户端通过 RPC 调用，其中可以做游戏逻辑或写入数据库
@hetu.define_system(
    namespace="Chat", components=(ChatMessage,), permission=hetu.Permission.USER
)
async def user_chat(ctx: hetu.SystemContext, text: str):
    assert ctx.caller, "call user_login first"
    row = ChatMessage.new_row()
    row.owner = ctx.caller
    row.text = text
    await ctx.repo[ChatMessage].insert(row)
```

客户端代码：

```csharp
// UI成员，一个发送按钮，一个聊天列表
private ListView _messageList;
private Button _composerSend;

// 发送：
_composerSend.clicked += () => {  
    _ = HeTuClient.Instance.CallSystem("user_chat", text); 
}

// 聊天列表，订阅服务器推送，自动更新UI
_messageList.itemsSource = _messages;

var messageSub = await HeTuClient.Instance.Range<ChatMessage>("id", 0, long.MaxValue, 1024);
messageSub.AddTo(gameObject); // 绑定生命周期，go销毁自动取消订阅
messageSub.ObserveAdd()  // 监听新增消息事件
    .Subscribe(msg =>
    {
        _messages.Add(msg);
        _messageList.Rebuild();
        _messageList.ScrollToItem(_messages.Count - 1);
    })
    .AddTo(ref messageSub.DisposeBag);

```

详见示例代码： [examples/chat/server/src/app.py](examples/chat/server/src/app.py)

#### 启动服务器

详见 [安装](#%EF%B8%8F-安装) 部分：

```bash
cd examples/chat/server
# 演示使用sqlite测试数据库，可直接启动
uv run hetu start --config=./config.yml
```

### 客户端代码部分

河图 Unity SDK 基于 async/await，支持 Unity 2018 以上 和 WebGL 平台。

首先在 Unity 中导入客户端 SDK，点“Window”->“Package Manager”->“+加号”->“Add package
from
git URL”

<img src="https://github.com/Heerozh/HeTu/blob/media/sdk1.png" width="306.5" height="156.5"/>
<img src="https://github.com/Heerozh/HeTu/blob/media/sdk2.png" width="208.5" height="162.5"/>

然后输入安装地址：
`https://github.com/Heerozh/HeTu.git?path=/ClientSDK/unity/cn.hetudb.clientsdk`

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
        // 连接河图，这其实是异步函数，我们没await，实际效果类似射后不管
        HeTuClient.Instance.Connect("ws://127.0.0.1:2466/hetu",
            this.GetCancellationTokenOnDestroy());

        // 调用登录System，连接成功后会在后台发送
        HeTuClient.Instance.CallSystem("login", SelfID);

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

|          |                          服务器 型号 |                                           设置 |
|:---------|--------------------------------:|---------------------------------------------:|
| 河图       |                cs.c9ae.16xlarge |             32 核 64 线程，默认配置，参数: --workers=76 |
| Redis7.0 | redis.shard.with.proxy.small.ce |                           最低配, 单可用区，4节点 读写分离 |
| 跑分程序     |                              本地 | cli： uv run ya ya_hetu_rpc.py -n 1200 -t 1.1 |

### 压测结果：

- hello world 测试：序列化并返回 hello world。
- get + update：单 Component，随机单行读写，表 3W 行。
- get*2 + update*2：同上，只是做2次
- get：单 Component，随机单行读，表 3W 行。

CPS(每秒调用次数)测试结果为：

| Time     | hello world(Calls) | get + update(Calls) | get*2 + update*2(Calls) | get(Calls) |
|:---------|-------------------:|--------------------:|------------------------:|-----------:|
| Avg(每秒)  |          1,200,929 |              90,776 |                  54,260 |    422,817 |
| CPU 负载   |                98% |                 88% |                     78% |        98% |
| Redis 负载 |                 0% |                 97% |                     90% |        41% |

- _以上测试为单 Component，受限于Master写入io。多个 Component 有机会（要低耦合度）通过
  Redis Cluster 扩展。_

### 单连接性能：

测试程序使用`-n 1 -p 1`参数测试，单线程同步堵塞模式，主要测试 RTT：

| Time        | hello world(Calls) | get + update(Calls) | get*2 + update*2(Calls) | get(Calls) |
|:------------|-------------------:|--------------------:|------------------------:|-----------:|
| Avg(每秒)     |             29,921 |               1,498 |                     827 |      5,704 |
| K90 RTT(ms) |               0.03 |                0.69 |                    1.33 |       0.18 |

### 关于 Python 性能

不用担心 Python 的性能。CPU 价格已远低于开发人员成本，快速迭代，数据分析，AI 生态更具有优势。

现在 Python 社区活跃，宛如人肉 JIT，且在异步+分布式架构下，吞吐量和 RTT 都不受制于语言，而受制于后端
Redis。

### Native 计算

由于 Component 数据本来就是 NumPy C 结构，可以使用 LuaJIT 的 FFI，以极低代价调用 C/Rust
代码：

```python
from cffi import FFI

ffi = FFI()
ffi.cdef("""
    void process(char* data); // char*需转换成Position*
""")
c_lib = ffi.dlopen('lib.dll')

# 获取Array of Position
rows = await ctx.repo[Position].range('x', pos.x - 10, pos.x + 10)
c_lib.process(ffi.from_buffer("float[]", rows))  # 无拷贝，传递指针
await ctx.range[Position].update_rows(rows)
```

注意，你的 C 代码不一定比 NumPy 自带的方法更优，类似这种二次索引在 Python 下支持 SIMD
更快：
`rows.x[rows.x >= 10] -= 10`

## ⚙️ 安装

开发环境建议用 uv 包管理安装。 Windows 可在命令行执行：

```bash
winget install --id=astral-sh.uv -e
```

新建你的项目目录，在目录中初始化 uv（最低版本需求 `3.13`）：

```shell
uv init --python "3.14"
```

此后你的项目就由 uv 管理，类似 npm，然后把河图添加到你的项目依赖中：

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

uv 会把所有依赖放在项目目录下（.venv），因此很简单，外网机执行上述步骤后，把整个项目目录复制过去即可。

内网建议跳过 uv 直接用`source .venv/bin/activate` (或`.\.venv\Scripts\activate.ps1`)
激活环境使用。

## 🎉 生产部署

生产环境推荐用 Docker 部署或 pip 直接安装，这 2 种都有国内镜像源。

### Docker 部署

安装 Docker，详见
[阿里云镜像](https://help.aliyun.com/zh/ecs/user-guide/install-and-use-docker):

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

注意你的项目目录格式得符合 src-layout，不然 RUN pip install .会失败。

然后执行：

```bash
# 编译你的应用镜像
docker build -t app_image_name .
# 启动你的应用
docker run -it --rm -p 2466:2466 --name server_name app_image_name
```

使用 Docker 的目的是为了河图的灵活启停特性，可以设置一台服务器为常驻包年服务器，其他都用
9 折的抢占服务器，然后用反向代理对连接进行负载均衡。

### pip 原生部署

容器一般有 20% 的性能损失，常驻服务器可以用 pip 的方式部署 (无须安装 uv)，且 pip
在国内云服务器都自带加速镜像。

原生部署困难处在于如何安装高版本 python，建议通过清华 miniconda 源安装，uv、pyenv
等都需要海外网。

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
# 启动河图，用 `python -O` 方式在生产环境启动，以去掉assert提升性能
python -O -m hetu start --config=./config.yml
```

> [!NOTE]
> 如果要使用 uv，需要代理：
>
> ```bash
> export SOCKS_PROXY=socks5://localhost:1080
> export ALL_PROXY=$SOCKS_PROXY
> curl -LsSf https://astral.sh/uv/install.sh | sh
> source $HOME/.local/bin/env
> ```

### Redis 部署

Redis 配置只要开启持久化即可。 推荐用 master+多机只读 replica 的分布式架构，数据订阅都可分流到
replica，大幅降低 master 负载。

Redis只使用了基础特性，因此支持各种Fork，比如ValKey，阿里云Tair等。
同时还支持Redis Proxy，Redis Cluster。

### 负载均衡

生产环境下，对河图还要设立一层反向代理，并进行负载均衡。

反向代理选择：

- Caddy: 自动 https 证书，自动反代头设置和合法验证，可通过 api 调用动态配置负载均衡
    - 命令行：
      `caddy reverse-proxy --from 你的域名.com --to hetu服务器1_ip:8000 --to hetu服务器2_ip:8000`
- Nginx: 老了，配置复杂歧义多，不推荐

## ⚙️ 客户端 SDK 安装

### C# SDK

此 SDK 基于.Net WebSocket 和多线程，也支持 Unity 2022 及以上版本（除 WebGL 平台）

可直接使用`ClientSDK/csharp/HeTuClient.cs`。

### Unity SDK

Unity SDK 支持 Unity 2018.3 及以上版本，含所有平台（包括 WebGL），基于 UnityWebSocket 和
UniTask，已内置在 SDK 库中。

在 Unity Package Manager 中使用以下地址安装：
`https://github.com/Heerozh/HeTu.git?path=/ClientSDK/unity/cn.hetudb.clientsdk`

如果项目已有 UniTask 依赖，可以择一删除。

> [!NOTE]
> 如果使用 Unity 6 及以上版本，SDK 使用 Unity 原生 Async 库，可以直接删除 UniTask 目录。

## 📚 文档：

由于结构简单，只有几个类方法，具体可以直接参考代码文档注释，建议通过 github 的 AI 直接询问。

如果日后接口方法变多时，会有详细文档。

## 🗯 讨论

前往 github discussions

## ⚖️ 代码规范

使用basedPyright和PyCharm进行代码检查，Ruff进行格式化。

Docstring要求为中文英文双语。

# ©️ Copyright & Thanks

Copyright (C) 2023-2025, by Zhang Jianhao (heeroz@gmail.com), All rights reserved.
