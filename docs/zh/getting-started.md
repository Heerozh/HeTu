---
title: "快速开始"
description: "安装 HeTu，运行你的第一个服务器，并连接客户端。"
type: docs
weight: 10
prev: /
next: tutorial/chat-room
---

本页将带你从一个空目录开始，到一个运行中的 HeTu 服务器，并有一个客户端连接到它。预计大约需要 10–15 分钟。

## 前置条件

- **Python 3.14 或更高版本。** HeTu 使用了较新的类型特性和异步改进。旧版本无法运行。
- **Redis（首次运行可选）。** 内置了 SQLite 后端用于本地实验；在上生产之前不需要 Redis。
- **一个 Unity 项目，或其他受支持的 SDK** 用于客户端侧（本页的代码片段中使用 Unity）。

## 1. 安装 `uv` 并创建项目

推荐的包管理器是 `uv`。在 Windows 上：

```powershell
winget install --id=astral-sh.uv -e
```

在 macOS / Linux 上：

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

然后初始化项目：

```bash
mkdir my-game-server && cd my-game-server
uv init --python "3.14"
uv add hetudb
```

之后，`uv run hetu --help` 应该会打印 HeTu 的 CLI 用法。

### 或者：使用 `hetu init` 搭建项目

为了避免手动设置，`hetu init` 命令会搭建一个可直接运行的项目——它会运行 `uv init`、添加 `hetudb` 依赖，并在 `src/` 下写入一个起始 `app.py`（一个最小的 `login` System 以及一个 `on_disconnect` 处理器）以及一个 `config.yml`：

```bash
uvx --from hetudb hetu init my-game-server
cd my-game-server
uv run hetu start --config=config.yml
```

`hetu init` 可以安全地重复运行：它会跳过任何已存在的文件，并且绝不会覆盖你的代码。生成的 `config.yml` 使用本地 SQLite 数据库，因此项目启动时不需要运行任何额外服务——生产环境请将其 `BACKENDS` 部分切换为 Redis。下面的编号演练会手动构建一个项目，这样你可以看到每一部分如何组合在一起。

## 2. 项目结构

HeTu 项目使用 **src-layout**：你的应用代码位于 `src/` 下，这能让导入保持明确，并使项目准备好用于 Docker 镜像中的 `pip install .`（生产环境相关内容见[运维](operations.md)）。

```
my-game-server/
├── pyproject.toml
└── src/
    ├── my_game_server/
    │    ├── __init__.py        
    │    ├── components.py
    │    ├── systems.py
    │    ├── endpoints.py
    │    └── etc....
    └── app.py          # entry point
```

`uv init` 默认创建一个扁平布局，因此请创建 `src/` 目录，并将 `hello.py`/`main.py`（或它生成的任何桩文件）移开。

## 3. 定义你的第一个 Component 和 System

将以下内容放入 `src/app.py`：

```python
import hetu
import numpy as np


@hetu.define_component(namespace="Hello", permission=hetu.Permission.EVERYBODY)
class Greeting(hetu.BaseComponent):
    owner: np.int64 = hetu.property_field(0, index=True)
    text: str = hetu.property_field("", dtype="U64")


@hetu.define_system(
    namespace="Hello", components=(Greeting,), permission=hetu.Permission.EVERYBODY
)
async def say_hello(ctx: hetu.SystemContext, name: str):
    row = Greeting.new_row()
    row.owner = ctx.caller or 0
    row.text = f"Hello, {name}!"
    await ctx.repo[Greeting].insert(row)
```

这就是整个服务器。`Greeting` 是一张带类型的表；`say_hello` 是一个 RPC 入口点，用于向其中插入一行。

## 4. 启动服务器

对于仅本地运行，使用捆绑的 SQLite 后端：

```bash
uv run hetu start \
  --app-file=./src/app.py \
  --db=sqlite:///./hetu.db \
  --namespace=Hello \
  --instance=dev
```

你应该会看到 Sanic 的启动横幅以及一行 `WebSocket listening on
ws://0.0.0.0:2466`。

如果你想要改用 Redis（首次运行之后推荐），请在本地安装 Redis 并替换为：

```bash
--db=redis://127.0.0.1:6379/0
```

## 5. 从客户端调用你的 System

### Unity (C#)

通过 Unity Package Manager 安装 Unity SDK：

> **Window → Package Manager → + → Add package from git URL**
>
> `https://github.com/Heerozh/HeTu.git?path=/ClientSDK/unity/cn.hetudb.clientsdk`

然后，在任意 MonoBehaviour 中：

```csharp
// Connect is a blocked async function, so we use fire and forget.
_ = HeTuClient.Instance.Connect("ws://127.0.0.1:2466/hetu/Hello");
await HeTuClient.Instance.CallSystem("say_hello", "world");
```

## 6. 验证是否成功

调用 `say_hello` 后，该行会存在于 SQLite 文件（或 Redis）中。你可以通过添加一个临时客户端订阅来证明：

```csharp
var sub = await HeTuClient.Instance.WatchRange<Greeting>("id", 0, long.MaxValue, 100);
sub.AddTo(gameObject); // dont forget! Otherwise, you will receive a warning about GC leaks when you stop playing.
sub.ObserveAdd().Subscribe(row => Debug.Log(row.text));
```

现在，每次新的 `say_hello` 调用都应该会在 Unity 控制台记录 `Hello, world!`。

## 接下来

- **[教程：聊天室](tutorial/chat-room.md)** — 一个真实的、多用户应用，演练订阅、权限以及典型项目形态。
- **[概念](concepts.md)** — 深入了解底层实际发生的事情：ECS 集群、乐观事务、订阅代理。
- **[运维](operations.md)** — 当你准备好部署时。
