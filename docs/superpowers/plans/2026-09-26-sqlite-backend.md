# SQLite 开发后端实施计划

> **For agentic workers:** 按任务顺序实施，每个任务一个（或几个）提交，步骤用 `- [ ]` 勾选跟踪。

**Goal:** 删除通用 SQL 后端，新写只支持 SQLite、行为尽可能和 Redis 一致的开发后端；测试只剩 redis / valkey /
redis_cluster / sqlite，SQLite 的 9 条 xfail 转绿；移出 SQLAlchemy 与全部 SQL 驱动。

**Architecture:** Redis 客户端里与 redis-py 无关的纯逻辑抽到 `hetu/data/backend/redis_model.py` 的
`RedisModelClient`，Redis / SQLite 两个客户端继承它。SQLite 在一条专用线程里用标准库 `sqlite3` 模拟 Redis 的
hash（每组件一张行表）、lex zset（全局 BLOB 表）、带过期的字符串、commit_v2.lua（Python 版）、keyspace 通知与
PUBLISH（通知表，跨进程轮询）。

**Tech Stack:** Python 3.14、标准库 sqlite3（SQLite 3.53）、NumPy、msgpack、pytest（Redis 需 Docker）。

**设计依据:** `docs/superpowers/specs/2026-09-26-sqlite-backend-design.md`

## 全局约定

- 测试：TDD 用 `HETU_TEST_BACKENDS=sqlite`（免 Docker）与 `redis`；每个任务收尾跑
  `HETU_TEST_BACKENDS=redis,valkey,redis_cluster,sqlite uv run pytest -n 8 tests/`。PG / MariaDB 不再跑
  （Task 3 删掉）。Windows 下加 `PYTHONUTF8=1`。
- Lint / 类型检查只查自己改的文件：`uv run ruff format <f> && uv run ruff check <f> && uv run basedpyright <f>`，
  与 HEAD 比告警数不增。
- 提交前缀沿用仓库惯例：`refactor(redis):` / `feat(sqlite):` / `test:` / `docs:` / `build:`。
- 基线：dev `30cb1ca9` 上 redis / valkey / redis_cluster / sqlite 全量的结果记在本文末尾。

---

## Task 1: 抽出共享模块 `redis_model.py`（Redis 纯搬迁）

**Files:** 新增 `hetu/data/backend/redis_model.py`；改 `redis/client.py`、`redis/maint.py`、`redis/mq.py`、
`base.py`。

- [x] `BackendClient.__init_subclass__` 改成有 `alias` 才注册。
- [x] `RedisModelClient(BackendClient)`：key / 频道命名（`dbi` 由子类设置）、`to_sortable_bytes` 别名、
  `row_decode_` / `rows_decode_`、`range_normalize_` / `make_zrange_cmd_`、区间两端与空区间判定、由 member 生成
  `RangeObservation`、`_range_checks`、`build_commit_payload_`（原 `commit` 的组装部分，一字不改）、
  `raise_for_commit_response_`、`commit`（组装 → `commit_script_` → 映射）、`direct_set` 的参数校验、
  `_get_referred_components`、索引 dtype 的 schema 检查、重建索引的 member 计算。`msg_packer` 也搬过去，
  `redis/client.py` 照旧能 import 到。
- [x] `RedisBackendClient(RedisModelClient, alias="redis")` 只留 redis-py I/O；`commit_script_` 转调
  `lua_commit`。读路径（`get` / `_hgetall_many` / `get_many` / `range`）代码不动。
- [x] `MQHub` 增加共用的 `is_table_channel_` / `decode_table_payload_` / `_resync_`（原
  `PubSubHub._on_resubscribed` 的分发部分），`PubSubHub` 改用它们。
- [x] 验证：redis / valkey / redis_cluster / sqlite 全绿（sqlite 此时仍是旧后端）；Redis range / commit 的
  Python 侧无额外开销（读路径未改，commit 多一层 await，可忽略）。
- [x] 提交：`refactor(redis): 抽出 Redis 数据模型的纯逻辑到 redis_model，供 SQLite 后端共用`

## Task 2a: 新 SQLite 后端 + 测试切换

**Files:** 新增 `hetu/data/backend/sqlite/{__init__,store,commit,client,maint,mq}.py`；改 `base.py`
（`_BUILTIN_MODULES`、`client_class`、`check_config_` 钩子）、`backend/__init__.py`（配置检查、不再改调用方的
servants 列表）、`tests/fixtures/backends.py` 与相关用例。

- [x] `store.py`：连接与 PRAGMA、库文件标识（`application_id` / `user_version`、旧库与外来库报错）、内部表；
  行表（HGETALL / HGET / HSET upsert + 缺表建表 + 缺列加列 / DEL / EXISTS / 列出 id、只差大小写报错）、zset
  （ZADD / ZREM 返回是否真变了、ZRANGE BYLEX 含 REV 与 LIMIT、ZLEXCOUNT、按前缀删 / 改名）、kv（带过期，
  NX 设置、按值删除）、meta、通知表（插入、按游标取、最小 id、序号、按 id 前缀清理）、读写事务。
- [x] `commit.py`：commit_v2.lua 的逐段移植，返回与 Lua 字节相同的串；按 Redis 规则收集要发的频道并写通知表。
- [x] `client.py`：DSN 解析、配置检查钩子（servants 非空报错、忽略 Redis 专用项）、专用线程、事件循环断言、
  `post_configure` / `is_synced` / `close`、读路径、`commit_script_`、`direct_set`（不发通知）、
  `get_table_maintenance` / `get_mq_client`。
- [x] `maint.py`：逐项对应 `RedisTableMaintenance`，维护锁。
- [x] `mq.py`：`SQLiteNotifyHub`（沿用 `SQLNotifyHub` 的登记 / 水位 / 轮询 / 退避，游标推到快照表尾，游标之后的
  通知被清理时补发 `resync_`）+ `SQLiteMQClient`。
- [x] 夹具：`mod_sqlite_backend`、`backend_config_by_name` 改用 `type: sqlite`。
- [x] 去掉 §2.2 的 9 条 SQLite xfail；PG / MariaDB 的 xfail 改成显式的 `("postgres", "mariadb")`。
- [x] 删掉只对旧后端有意义、会在新后端上挂的 sqlite 用例：两条 NOCASE collation 用例；
  `test_sql_rejects_uint64_above_bigint` 只留给 PG / MariaDB（Task 3 删）；复数索引的 schema 检查用例按
  `RedisModelClient` 判断。
- [x] 交错提交的碰头 helper：有 `commit_script_` 的后端都 patch 它。
- [x] 验证：redis / valkey / redis_cluster / sqlite 全绿（1575 passed / 5 skipped，原 9 条 xfail 转为通过）。
- [x] 提交：`feat(sqlite): …`

> 顺序调整：Redis 专属用例扩到 sqlite、`test_backend_sqlite.py` 放到 Task 3 之后（Task 2b），免得中间态给
> PG / MariaDB 加临时的跳过标记。

## Task 3: 删除通用 SQL 后端与 PG / MariaDB

- [x] 删 `hetu/data/backend/sql/`、`tests/test_backend_sql.py`；`tests/fixtures/sql_service.py` →
  `sqlite_service.py`；夹具去掉 postgres / mariadb，`SQL_BACKENDS` → `SQLITE_BACKENDS`；删 PG / MariaDB 的 xfail、
  `xfail_on_backends`（没人用了）、`sa_exc` 等 import；碰头 helper 去掉旧 SQL 分支。
- [x] `BackendClientFactory`：去掉 `sql`，`type: SQL` 报改名提示。
- [x] CLI：`infer_backend_type_from_db_url`、`hetu init` 生成 `type: SQLite`；`hetu.testing.Sandbox`；
  `CONFIG_TEMPLATE.yml`（BACKENDS 注释、删 aiosqlite 日志条目）；`examples/chat/server/config.yml`；
  `worker_keeper.py` 的说明；对应测试（`test_cli_init`、`test_migration`、`test_safelogging`、
  `test_headless_process`、`test_common`）。
- [x] 依赖：`uv remove sqlalchemy aiosqlite asyncpg aiomysql pymysql psycopg psycopg-binary`，
  `uv sync --all-packages --group dev`。
- [x] 验证：全绿（1554 passed / 1 skipped）；`hetu/`、`tests/`、`pyproject.toml` 搜不到这些库名。
- [x] 提交：`refactor!: 删除通用 SQL 后端，SQLite 改用新后端（type: SQLite）`、`build: 移出 SQLAlchemy 与 SQL 驱动`

## Task 2b: 测试扩面（Task 3 之后）

- [ ] Redis 专属用例扩到 sqlite（spec §7.4），加测试 helper 读原始索引 member / 改行字段。
- [ ] `tests/test_backend_sqlite.py`（spec §7.5），其中 hub 并发单测从 `test_backend_sql.py` 改写迁来。
- [ ] 验证：全绿。
- [ ] 提交：`test: …`

## Task 4: direct_set 通知契约

- [ ] `BackendClient.direct_set` docstring：维护类写入，不改 `_version`、不保证触发订阅通知。
- [ ] `test_endpoint_connection.py::test_owner_value_channel_ignores_own_heartbeat` 的注释。
- [ ] 提交：`docs(backend): direct_set 不保证触发订阅通知`

## Task 5: 文档

- [ ] `docs/zh` 与 `docs/en`（同一提交）：`_index` / `concepts` / `advanced` / `operations` / `getting-started` /
  `tutorial/chat-room` 里 SQL 后端相关的说法。
- [ ] `repo.py` / `idmap.py` / `base.py` docstring 里的 SQL 说明；`AGENTS.md`；重新生成 `docs/api/`
  （`uv run python scripts/gen_api_docs.py`）。
- [ ] 提交：`docs: SQLite 开发后端`

---

## 基线与实测

（实施时填写）
