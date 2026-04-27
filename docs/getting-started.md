---
title: "Getting Started"
description: "Install HeTu, run your first server, and connect a client."
type: docs
weight: 10
prev: /
next: tutorial/chat-room
---

This page takes you from a blank directory to a running HeTu server with a
client connected to it. Plan on roughly 10–15 minutes.

## Prerequisites

- **Python 3.14 or newer.** HeTu uses recent typing features and async
  improvements. Older versions will not work.
- **Redis (optional for the first run).** A SQLite backend is included for
  local experiments; you do not need Redis until you go to production.
- **A Unity project, or other supported SDK** for the client side  (this page uses Unity
  in the snippets).

## 1. Install `uv` and create a project

The recommended package manager is `uv`. On Windows:

```powershell
winget install --id=astral-sh.uv -e
```

On macOS / Linux:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Then initialize a project:

```bash
mkdir my-game-server && cd my-game-server
uv init --python "3.14"
uv add hetudb
```

After this, `uv run hetu --help` should print HeTu's CLI usage.

## 2. Project layout

HeTu projects use a **src-layout**: your application code lives under `src/`,
which keeps imports unambiguous and makes the project ready for `pip install .`
in a Docker image (see [Operations](operations.md) for the production story).

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

`uv init` creates a flat layout by default, so create the `src/` directory
and move `hello.py`/`main.py` (or whatever stub it generated) out of the way.

## 3. Define your first Component and System

Put this in `src/app.py`:

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

That's the entire server. `Greeting` is a typed table; `say_hello` is an RPC
entry point that inserts a row into it.

## 4. Start the server

For a local-only run, use the bundled SQLite backend:

```bash
uv run hetu start \
  --app-file=./src/app.py \
  --db=sqlite:///./hetu.db \
  --namespace=Hello \
  --instance=dev
```

You should see Sanic's startup banner and a `WebSocket listening on
ws://0.0.0.0:2466` line.

If you want Redis instead (recommended past the first run), install Redis
locally and substitute:

```bash
--db=redis://127.0.0.1:6379/0
```

## 5. Call your System from a client

### Unity (C#)

Install the Unity SDK via the Unity Package Manager:

> **Window → Package Manager → + → Add package from git URL**
>
> `https://github.com/Heerozh/HeTu.git?path=/ClientSDK/unity/cn.hetudb.clientsdk`

Then, in any MonoBehaviour:

```csharp
// Connect is a blocked async function, so we use fire and forget.
_ = HeTuClient.Instance.Connect("ws://127.0.0.1:2466/hetu/Hello");
await HeTuClient.Instance.CallSystem("say_hello", "world");
```

## 6. Verify it worked

After calling `say_hello`, the row exists in the SQLite file (or Redis). You
can prove it by adding a temporary client subscription:

```csharp
var sub = await HeTuClient.Instance.Range<Greeting>("id", 0, long.MaxValue, 100);
sub.AddTo(gameObject); // dont forget! Otherwise, you will receive a warning about GC leaks when you stop playing.
sub.ObserveAdd().Subscribe(row => Debug.Log(row.text));
```

Each new `say_hello` call should now log `Hello, world!` to the Unity console.

## What's next

- **[Tutorial: Chat Room](tutorial/chat-room.md)** — a real, multi-user app
  that exercises subscriptions, permissions, and the typical project shape.
- **[Concepts](concepts.md)** — what's actually happening under the hood:
  ECS clusters, optimistic transactions, the subscription broker.
- **[Operations](operations.md)** — when you're ready to deploy.
