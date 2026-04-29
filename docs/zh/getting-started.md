---
title: "入门指南"
description: "安装 HeTu，运行你的第一个服务器，并连接一个客户端。"
type: docs
weight: 10
prev: /
next: tutorial/chat-room
---

本页面将带你从一个空目录开始，搭建一个运行中的 HeTu 服务器，并连接一个客户端。预计耗时约 10–15 分钟。

## 前提条件

- **Python 3.14 或更新版本**。HeTu 使用了最新的类型特性和异步改进。旧版本无法运行。
- **Redis（首次运行可选）**。内置了 SQLite 后端用于本地实验；在生产环境前不需要 Redis。
- **一个 Unity 项目或其他受支持的 SDK** 用于客户端（本页面在代码片段中使用 Unity）。

## 1. 安装 `uv` 并创建一个项目

推荐的包管理器是 `uv`。在 Windows 上：

```powershell
winget install --id=astral-sh.uv -e
```

在 macOS / Linux 上：

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

然后初始化一个项目：

```bash
mkdir my-game-server && cd my-game-server
uv init --python "3.14"
uv add hetudb
```

之后，执行 `uv run hetu --help` 应该能打印出 HeTu 的 CLI 用法。

## 2. 项目结构

HeTu 项目使用 **src 布局**：你的应用代码位于 `src/` 下，这使导入清晰明确，并且使项目在 Docker 镜像中准备好使用 `pip install .`（关于生产环境的故事请参见[运维](operations.md)）。

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
    └── app.py          # 入口点
```

`uv init` 默认创建扁平布局，因此请创建 `src/` 目录并将 `hello.py`/`main.py`（或它生成的任何存根）移开。

## 3. 定义你的第一个组件和系统

将以下代码放入 `src/app.py`：

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

这就是整个服务器。`Greeting` 是一个类型化的表；`say_hello` 是一个 RPC 入口点，它向该表中插入一行。

## 4. 启动服务器

对于仅限本地的运行，使用内置的 SQLite 后端：

```bash
uv run hetu start \
  --app-file=./src/app.py \
  --db=sqlite:///./hetu.db \
  --namespace=Hello \
  --instance=dev
```

你应该会看到 Sanic 的启动横幅以及一行 `WebSocket listening on ws://0.0.0.0:2466`。

如果你想要使用 Redis（建议在首次运行后使用），请本地安装 Redis 并替换为：

```bash
--db=redis://127.0.0.1:6379/0
```

## 5. 从客户端调用你的系统

### Unity (C#)

通过 Unity Package Manager 安装 Unity SDK：

> **Window → Package Manager → + → Add package from git URL**
>
> `https://github.com/Heerozh/HeTu.git?path=/ClientSDK/unity/cn.hetudb.clientsdk`

然后在任何 MonoBehaviour 中：

```csharp
// Connect 是一个阻塞的异步函数，所以我们使用 fire and forget。
_ = HeTuClient.Instance.Connect("ws://127.0.0.1:2466/hetu/Hello");
await HeTuClient.Instance.CallSystem("say_hello", "world");
```

## 6. 验证是否成功

调用 `say_hello` 后，该行会存在于 SQLite 文件（或 Redis）中。你可以通过添加一个临时客户端订阅来证明：

```csharp
var sub = await HeTuClient.Instance.Range<Greeting>("id", 0, long.MaxValue, 100);
sub.AddTo(gameObject); // 别忘了！否则，当你停止播放时会收到关于 GC 泄漏的警告。
sub.ObserveAdd().Subscribe(row => Debug.Log(row.text));
```

现在，每次新的 `say_hello` 调用都应该在 Unity 控制台中记录 `Hello, world!`。

## 接下来做什么

- **[教程：聊天室](tutorial/chat-room.md)** —— 一个真实的多用户应用，用于练习订阅、权限和典型的项目结构。
- **[概念](concepts.md)** —— 幕后实际发生了什么：ECS 集群、乐观事务、订阅代理。
- **[运维](operations.md)** —— 当你准备好部署时。
