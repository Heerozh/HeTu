# Repository Guidelines

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
uv run pytest -n 8 tests/     # 多进程并行测试（pytest-xdist）
```

需要 Python 3.14。测试依赖 Docker（用于启动 Redis/Valkey 容器；SQLite 后端无需 Docker）。
`HETU_TEST_BACKENDS` 接受逗号分隔的子集，取值范围：`redis`、`valkey`、`redis_cluster`、`sqlite`。
未设置时跑全部后端；一般TDD时只需跑`redis`（或免 Docker 的 `sqlite`），CI 在 `push` 到非 `main`
分支时也会限制为 `redis` 为了快速验证。

测试容器按 pytest 进程隔离（`tests/fixtures/docker_infra.py`）：每个进程（含 xdist 的每个
worker）用带会话 ID 的容器名、docker 随机分配的端口启动自己的一套容器，所以多个 worktree /
终端可以同时跑测试。`-n` 并行时默认按（测试文件, 后端）分组调度，每个 worker 只启动自己用到
的后端；worker 多、机器忙时少数时序敏感的测试（订阅推送、sleep 精度）偶尔会抖，用 `--lf` 重跑
确认。被强杀的测试进程留下的容器会在下次跑测试时自动回收（只回收属主进程已退出的）；手动
清理：`docker rm -f $(docker ps -aq --filter label=hetu.test)`。

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
- `BackendClient` / `BackendClientFactory`：抽象 DB client。生产用 Redis（`backend/redis/`，
  含 cluster 支持）；`backend/sqlite/` 是只给开发用的 SQLite 后端，在 SQLite 上模拟 Redis 的
  数据模型（行表 / lex zset / commit_v2.lua 的 Python 版 / keyspace 通知），行为与 Redis 一致、
  不考虑性能。两者共用 `redis_model.py` 里的纯逻辑（key 布局、索引编码、commit payload）；
- `Session`：transaction manager，使用 optimistic concurrency（通过
  `IdentityMap` 检测冲突并抛出 `RaceCondition`）。
- `SessionRepository`：Session 内按 Component 进行 CRUD（`get`、`range`、
  `upsert`、`insert`、`delete`、`update_rows`）。
- `Table` / `TableReference`：Component 到 backend 的映射，由
  `ComponentTableManager` 管理。
- `MQClient`：每个连接一个本地 message queue，用于 subscription notification；后端每个
  worker 只有一个共享的通知接收器
  （Redis `PubSubHub` 一条 pubsub 连接 / SQLite `SQLiteNotifyHub` 一个通知表轮询任务）
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

## Rule

设计约束：

重要：架构受限Redis的master节点性能，主要通过读写分离扩展性能。所以：

- 尽可能不要使用master的cpu，不需要master读的地方都不要通过master，
  使用master_or_servant随机选节点的方法，其中master被随机选中的概率是可调的。
  比如事务中所有repo.get ()读操作可以是servant读，如果读到了旧数据有乐观锁。
  `tests/test_arch_master_reads.py` 负责守门，摸底用 `tools/master_read_audit`。
- 副本间应隔离，不要使用会消耗所有副本甚至master cpu的指令，比如PUBLISH。
