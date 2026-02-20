# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in
this repository.

## Project Overview

HeTu (河图) 是一个高性能、多进程分布式的 game server engine，基于 ECS
(Entity-Component-System) architecture，并使用 Redis 作为 backend。它通过
WebSocket 暴露类似 database 的接口，client 可订阅数据变更并通过 RPC 调用
server-side logic。

```bash
uv sync --group dev          # 安装依赖
uv run ruff check .          # 代码检查（Lint）
uv run ruff format .         # 代码格式化
uv run basedpyright          # 类型检查
uv run pytest tests/         # 运行全部测试
uv run pytest tests/test_backend_basic.py  # 运行单个测试文件
uv run pytest tests/test_backend_basic.py::test_name  # 运行单个测试
uv run pytest --cov-config=.coveragerc --cov=hetu tests/  # 覆盖率
```

需要 Python 3.14。测试依赖 Docker (Redis)。为保持与 CI 一致，请设置
`HETU_TEST_BACKENDS=redis`。

## Architecture

### Core ECS Pattern

三个核心抽象对应 ECS，通过 decorators 定义：

- **Component** (`@define_component`)：数据 schema 由 NumPy structured arrays
  支撑（类似 C struct 的 row）。定义在用户 app 代码中，存储在 Redis。每个
  component 都是一张 table，通过 `property_field()` 定义带类型的 columns。
  支持字段 `unique`/`index`。权限级别：`EVERYBODY`、`USER`、`OWNER`/`RLS`、
  `ADMIN`。

- **System** (`@define_system`)：在 transaction 中操作 Components 的
  server-side logic function。System 声明其引用的 Components；engine 会将引用
  Component 重叠的 Systems 归为“co-location clusters”（`SystemClusters`），用于
  transaction isolation。遇到 `RaceCondition` 时 transaction 会自动重试。

- **Endpoint** (`@define_endpoint`)：更底层的连接处理器。System 本质上是
  Endpoint 的特化形式，并带有 transaction 支持。

### Data Flow

```
Client (Unity/JS/C#) ──WebSocket──► Sanic Worker ──► EndpointExecutor
                                                         │
                                          ┌──────────────┤
                                          ▼              ▼
                                    SystemCaller    SubscriptionBroker
                                          │              │
                                    Session/Repo    MQClient (pub/sub)
                                          │              │
                                          ▼              ▼
                                        Redis ◄──────────┘
```

1. Client 通过 WebSocket 连接到 `/hetu/<db_name>`，并经过 message pipeline
   （jsonb → zlib → crypto）。
2. RPC 调用（`callSystem`）经由 `EndpointExecutor` → `SystemCaller` 路由，后者会
   打开 `Session`（transaction）、为每个 Component 创建 `SessionRepository`、执行
   System function 并 commit。
3. 数据订阅（`select`/`query`）会创建 `RowSubscription`/`IndexSubscription` 对象，
   由 `SubscriptionBroker` 管理；其监听 Redis pub/sub 的变更通知并将更新推送给
   client。

### Backend Layer (`hetu/data/backend/`)

- `Backend`：管理 master + servant（read replica）连接，使用 weighted random
  selection。
- `BackendClient` / `BackendClientFactory`：抽象 DB client；Redis 实现在
  `backend/redis/`。
- `Session`：transaction manager，使用 optimistic concurrency（通过
  `IdentityMap` 检测冲突并抛出 `RaceCondition`）。
- `SessionRepository`：Session 内按 Component 进行 CRUD（`get`、`range`、
  `upsert`、`insert`、`delete`、`update_rows`）。
- `Table` / `TableReference`：Component 到 backend 的映射，由
  `ComponentTableManager` 管理。
- `MQClient`：每个连接一个 message queue，用于 subscription notification。

### Server Layer (`hetu/server/`)

- 基于 Sanic（async web framework）。每个 worker process 独立运行。
- `main.py`：worker 入口 —— 初始化 backends、SnowflakeID、
  ComponentTableManagers、SystemClusters。
- `websocket.py`：WebSocket handler —— 为每个连接创建 `EndpointExecutor`、
  `SystemCaller`、`SubscriptionBroker`。
- `receiver.py`：message dispatcher —— 路由 `rpc`、`sub`、`unsub`、`sel` 命令。
- `pipeline/`：分层 message processing（jsonb serialization、zlib/brotli/zstd
  compression、ChaCha20 encryption）。各层通过带 `alias` 的
  `__init_subclass__` 自动注册。

### Key Patterns

- **Singleton metaclass** (`common/singleton.py`)：用于 `SystemClusters`、
  `ComponentDefines`、`SnowflakeID`。
- **Factory pattern with auto-registration**：`BackendClientFactory`、
  `MessageProcessLayerFactory` —— subclass 会自动注册。
- **SnowflakeID**：分布式唯一 ID 生成，通过 `WorkerKeeper` 基于 Redis 管理
  worker IDs。
- **FutureCalls**：内置的定时/周期性 System 调用，作为 Component 存储在
  `HeTu` namespace。

## Module Map

| Module                   | Role                                                           |
|--------------------------|----------------------------------------------------------------|
| `hetu/data/component.py` | `@define_component`, `BaseComponent`, `property_field`         |
| `hetu/system/definer.py` | `@define_system`, `SystemClusters`（cluster grouping）           |
| `hetu/system/caller.py`  | `SystemCaller` —— 执行 System 并支持 transaction retry              |
| `hetu/system/context.py` | `SystemContext` —— 带 `repo` dict 的 transaction context         |
| `hetu/endpoint/`         | `@define_endpoint`, `Context`, `elevate()`, `EndpointExecutor` |
| `hetu/data/backend/`     | `Backend`, `Session`, `SessionRepository`, `Table`             |
| `hetu/data/sub.py`       | `SubscriptionBroker`, `RowSubscription`, `IndexSubscription`   |
| `hetu/server/`           | Sanic workers、WebSocket handler、message pipeline               |
| `hetu/manager.py`        | `ComponentTableManager` —— 将 Components 映射到 backend Tables     |
| `hetu/cli/`              | CLI commands：`start`、`migrate`、`build`                         |
| `hetu/sourcegen/`        | Client SDK code generation（C#）                                 |

## Conventions

- Commit 前缀：`ENH:`、`BUG:`、`MAINT:`
- 4 空格缩进，行长 88（Ruff）
- `snake_case` functions、`PascalCase` classes、`UPPER_SNAKE_CASE` constants
- public APIs 使用中英双语 docstrings
- 测试文件：`test_*.py`，fixtures 在 `tests/fixtures/`
- pytest 配置中使用 `asyncio_mode = "auto"`；fixture/test 的 loop scope 为 `module`

## Rule

- Always use Context7 MCP when I need library/API documentation, code generation, setup
  or configuration steps without me having to explicitly ask.