# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in
this repository.

## Project Overview

HeTu (河图) 是一个高性能、多进程分布式的 game server engine，基于 ECS
(Entity-Component-System) architecture，并使用 Redis 作为 backend。它通过 WebSocket 暴露类似
database 的接口，client 可订阅数据变更并通过 RPC 调用 server-side logic。

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

需要 Python 3.14。测试依赖 Docker（用于启动 Redis/Valkey/Postgres/MariaDB 容器；SQLite 后端无需
Docker）。`HETU_TEST_BACKENDS` 接受逗号分隔的子集， 取值范围：`redis`、`valkey`、
`redis_cluster`、`postgres`、`sqlite`、`mariadb`。 未设置时跑全部后端；一般TDD时只需跑`redis`
，CI 在 `push` 到非 `main`
分支时也会限制为 `redis` 为了快速验证。

## Architecture

### Core ECS Pattern

三个核心抽象对应 ECS，通过 decorators 定义：

- **Component** (`@define_component`)：数据 schema 由 NumPy structured arrays 支撑（类似 C
  struct 的 row）。定义在用户 app 代码中，存储在 Redis。每个 component 都是一张 table，通过
  `property_field()` 定义带类型的 columns。 支持字段 `unique`/`index`。权限级别：
  `EVERYBODY`、`USER`、`OWNER`/`RLS`、
  `ADMIN`（`OWNER` = 行级私有：自动只让 client 订阅到 `owner==ctx.caller` 的行， 等价
  `rls_compare=("eq", "owner", "caller")`，组件需有 `owner` 字段；服务端代码不受限）。

- **System** (`@define_system`)：在 transaction 中操作 Components 的 server-side logic
  function。System 声明其引用的 Components；engine 会将引用 Component 重叠的 Systems
  归为“co-location clusters”（`SystemClusters`），用于 transaction isolation。遇到
  `RaceCondition` 时 transaction 会自动重试。

- **Endpoint** (`@define_endpoint`)：更底层的连接处理器。System 本质上是 Endpoint
  的特化形式，并带有 transaction 支持。

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

1. Client 通过 WebSocket 连接到 `/hetu/<instance_name>`，并经过 message pipeline （jsonb →
   zlib → crypto）。
2. RPC 调用（`callSystem`）经由 `EndpointExecutor` → `SystemCaller` 路由，后者会 打开
   `Session`（transaction）、为每个 Component 创建 `SessionRepository`、执行 System function
   并 commit。
3. 数据订阅（`select`/`query`）会创建 `RowSubscription`/`IndexSubscription` 对象， 由
   `SubscriptionBroker` 管理；其监听 Redis pub/sub 的变更通知并将更新推送给 client。

### Backend Layer (`hetu/data/backend/`)

- `Backend`：管理 master + servant（read replica）连接，使用 weighted random selection。
- `BackendClient` / `BackendClientFactory`：抽象 DB client。生产推荐 Redis （
  `backend/redis/`，含 cluster 支持）；`backend/sql/` 提供 SQLAlchemy based
  实现（PostgreSQL/SQLite/MariaDB），仅用于开发或低订阅负载场景；
- `Session`：transaction manager，使用 optimistic concurrency（通过
  `IdentityMap` 检测冲突并抛出 `RaceCondition`）。
- `SessionRepository`：Session 内按 Component 进行 CRUD（`get`、`range`、
  `upsert`、`insert`、`delete`、`update_rows`）。
- `Table` / `TableReference`：Component 到 backend 的映射，由
  `ComponentTableManager` 管理。
- `MQClient`：每个连接一个本地 message queue，用于 subscription notification；后端每个 worker
  只有一个共享的通知接收器（Redis `PubSubHub` 一条 pubsub 连接 / SQL `SQLNotifyHub` 一个轮询任务）
  按频道分发到各连接的队列。

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
- **SnowflakeID**：分布式唯一 ID 生成，通过 `WorkerKeeper` 基于 Redis 管理 worker IDs。
- **FutureCalls**：内置的定时/周期性 System 调用，作为 Component 存储在
  `HeTu` namespace。

## Module Map

| Module                   | Role                                                                                    |
|--------------------------|-----------------------------------------------------------------------------------------|
| `hetu/data/component.py` | `@define_component`, `BaseComponent`, `property_field`                                  |
| `hetu/system/definer.py` | `@define_system`, `SystemClusters`（cluster grouping）                                  |
| `hetu/system/caller.py`  | `SystemCaller` —— 执行 System 并支持 transaction retry                                  |
| `hetu/system/context.py` | `SystemContext` —— 带 `repo` dict 的 transaction context                                |
| `hetu/endpoint/`         | `@define_endpoint`, `Context`, `elevate()`, `EndpointExecutor`                          |
| `hetu/data/backend/`     | `Backend`, `Session`, `SessionRepository`, `Table`                                      |
| `hetu/data/sub.py`       | `SubscriptionBroker`, `RowSubscription`, `IndexSubscription`                            |
| `hetu/server/`           | Sanic workers、WebSocket handler、message pipeline                                      |
| `hetu/manager.py`        | `ComponentTableManager` —— 将 Components 映射到 backend Tables                          |
| `hetu/cli/`              | CLI commands：`start`（启动服务）、`upgrade`（schema 迁移）、`build`（生成 client SDK） |
| `hetu/sourcegen/`        | Client SDK code generation（C#）；由 `hetu build` 调用                                  |
| `hetu/safelogging/`      | 进程安全的日志 queue/listener；通过 YAML 配置见 hetu/CONFIG_TEMPLATE.yml                |
| `hetu/i18n/`             | gettext 风格的翻译，所有用户可见字符串都包在 `_("...")` 中                              |

## Conventions

- 4 空格缩进，行长 88（Ruff）
- `snake_case` functions、`PascalCase` classes、`UPPER_SNAKE_CASE` constants
- public APIs 使用中英双语 docstrings
- 测试文件：`test_*.py`，fixtures 在 `tests/fixtures/`
- pytest 配置中使用 `asyncio_mode = "auto"`；fixture/test 的 loop scope 为 `module`
- 若在 Windows 上看到  `UnicodeEncodeError: 'gbk' codec can't encode character`，Agent
  等会捕获输出的 环境下跑 `pytest -s`，请加 `PYTHONUTF8=1` 前缀。

## Sandbox (sbx) 环境

当在 `sbx run claude` 启动的 sandbox 中工作（`IS_SANDBOX=1`）时， 全新系统首次一定要提权执行
`agent_install.sh` 脚本，否则可能会：

- 如果uv run报错，可能是因为用了host的venv目录，脚本会设置UV_PROJECT_ENVIRONMENT
- 如果sudo显示: The "no new privileges" flag is set, which prevents sudo from running as
  root. 这是需要用户关闭Claude Code的sandbox，安装脚本会关闭，或可提权执行

已知问题：windows下sbx里pytest比较耗时，整个项目完整测试要397.96s（6分钟），请合理安排测试项目

## Rule

- Always use Context7 MCP when I need library/API documentation, code generation, setup
  or configuration steps without me having to explicitly ask.

设计约束：

重要：架构受限Redis的master节点性能，尽可能不需要master读的地方都不要通过master，
      使用master_or_servant随机选节点的方法，其中master被随机选中的概率是可调的。
      比如事务中所有repo.get()读操作可以是servant读，如果读到了旧数据有乐观锁。

这条约束由两个测试守着，新增直接读 master 的代码会挂在这里：

- `tests/test_arch_master_reads.py`：扫 `hetu/` 下所有 `xxx.master.get/get_many/range(`
  形态的直接 master 读，不在文件里的 `ALLOWED` 清单中就失败。确实必须读 master 的，把它加进
  清单并写明理由（当前 2 处：雪花 ID 读时钟水位、顶号核查；`only_master` 事务经
  `Session.master_or_servant` 属性切到 master，扫描看不到）。
- `tests/test_master_read_budget.py`：用 `master_weight: 0` 的 backend（加权随机永远选不中
  master，于是 master 上剩下的读一定是代码显式指定的），钉死几条典型路径允许的 master 读
  次数。改动读路径导致某条路多读一次 master 会在这里暴露。

摸底用 `PYTHONPATH=tools uv run pytest tests/ -q -p master_read_audit`，它按调用点聚合出
实际落到 master 上的读；用法见 `tools/master_read_audit.py` 文件头。