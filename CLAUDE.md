# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in
this repository.

## Project Overview

HeTu (河图) is a high-performance, auto-scaling game server engine built on an ECS (
Entity-Component-System) architecture with Redis as the backend. It exposes a
database-like interface over WebSocket, where clients subscribe to data changes and call
server-side logic via RPC.

## Commands

```bash
uv sync --group dev          # Install dependencies
uv run ruff check .          # Lint
uv run ruff format .         # Format
uv run basedpyright          # Type check
uv run pytest tests/         # Run all tests
uv run pytest tests/test_backend_basic.py  # Run single test file
uv run pytest tests/test_backend_basic.py::test_name  # Run single test
uv run pytest --cov-config=.coveragerc --cov=hetu tests/  # Coverage
```

Python 3.14 required. Tests need Docker (Redis). Set `HETU_TEST_BACKENDS=redis` for CI
parity.

## Architecture

### Core ECS Pattern

The three core abstractions mirror ECS, defined via decorators:

- **Component** (`@define_component`): Data schema backed by NumPy structured arrays (
  C-struct-like rows). Defined in user app code, stored in Redis. Each component is a
  table with typed columns via `property_field()`. Supports `unique`/`index` on fields.
  Permission levels: `EVERYBODY`, `USER`, `OWNER`/`RLS`, `ADMIN`.

- **System** (`@define_system`): Server-side logic functions that operate on Components
  within transactions. Systems declare which Components they reference; the engine
  groups Systems with overlapping Components into "co-location clusters" (
  `SystemClusters`) for transaction isolation. Transactions auto-retry on
  `RaceCondition`.

- **Endpoint** (`@define_endpoint`): Lower-level connection handlers. Systems are
  actually a specialized form of Endpoint with transaction support.

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

1. Clients connect via WebSocket to `/hetu/<db_name>`, go through a message pipeline (
   jsonb → zlib → crypto).
2. RPC calls (`callSystem`) route through `EndpointExecutor` → `SystemCaller`, which
   opens a `Session` (transaction), creates `SessionRepository` per Component, executes
   the System function, and commits.
3. Data subscriptions (`select`/`query`) create `RowSubscription`/`IndexSubscription`
   objects managed by `SubscriptionBroker`, which listens to Redis pub/sub for change
   notifications and pushes updates to clients.

### Backend Layer (`hetu/data/backend/`)

- `Backend`: Manages master + servant (read replica) connections with weighted random
  selection.
- `BackendClient` / `BackendClientFactory`: Abstract DB client; Redis implementation in
  `backend/redis/`.
- `Session`: Transaction manager with optimistic concurrency (detects conflicts via
  `IdentityMap`, raises `RaceCondition`).
- `SessionRepository`: Per-Component CRUD within a Session (`get`, `range`, `upsert`,
  `insert`, `delete`, `update_rows`).
- `Table` / `TableReference`: Component-to-backend mapping, managed by
  `ComponentTableManager`.
- `MQClient`: Per-connection message queue for subscription notifications.

### Server Layer (`hetu/server/`)

- Built on Sanic (async web framework). Each worker process runs independently.
- `main.py`: Worker entry — initializes backends, SnowflakeID, ComponentTableManagers,
  SystemClusters.
- `websocket.py`: WebSocket handler — creates per-connection `EndpointExecutor`,
  `SystemCaller`, `SubscriptionBroker`.
- `receiver.py`: Message dispatcher — routes `rpc`, `sub`, `unsub`, `sel` commands.
- `pipeline/`: Layered message processing (jsonb serialization, zlib/brotli/zstd
  compression, ChaCha20 encryption). Layers register via `__init_subclass__` with
  `alias`.

### Key Patterns

- **Singleton metaclass** (`common/singleton.py`): Used by `SystemClusters`,
  `ComponentDefines`, `SnowflakeID`.
- **Factory pattern with auto-registration**: `BackendClientFactory`,
  `MessageProcessLayerFactory` — subclasses register themselves automatically.
- **SnowflakeID**: Distributed unique ID generation with `WorkerKeeper` managing worker
  IDs via Redis.
- **FutureCalls**: Built-in scheduled/recurring System calls, stored as a Component in
  the `HeTu` namespace.

## Module Map

| Module                   | Role                                                           |
|--------------------------|----------------------------------------------------------------|
| `hetu/data/component.py` | `@define_component`, `BaseComponent`, `property_field`         |
| `hetu/system/definer.py` | `@define_system`, `SystemClusters` (cluster grouping)          |
| `hetu/system/caller.py`  | `SystemCaller` — executes Systems with transaction retry       |
| `hetu/system/context.py` | `SystemContext` — transaction context with `repo` dict         |
| `hetu/endpoint/`         | `@define_endpoint`, `Context`, `elevate()`, `EndpointExecutor` |
| `hetu/data/backend/`     | `Backend`, `Session`, `SessionRepository`, `Table`             |
| `hetu/data/sub.py`       | `SubscriptionBroker`, `RowSubscription`, `IndexSubscription`   |
| `hetu/server/`           | Sanic workers, WebSocket handler, message pipeline             |
| `hetu/manager.py`        | `ComponentTableManager` — maps Components to backend Tables    |
| `hetu/cli/`              | CLI commands: `start`, `migrate`, `build`                      |
| `hetu/sourcegen/`        | Client SDK code generation (C#)                                |

## Conventions

- Commit prefixes: `ENH:`, `BUG:`, `MAINT:`
- 4-space indent, line length 88 (Ruff)
- `snake_case` functions, `PascalCase` classes, `UPPER_SNAKE_CASE` constants
- Bilingual (Chinese/English) docstrings for public APIs
- Test files: `test_*.py`, fixtures in `tests/fixtures/`
- `asyncio_mode = "auto"` in pytest config; fixture/test loop scope is `module`
