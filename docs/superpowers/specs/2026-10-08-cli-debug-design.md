# 命令行调试入口（`hetu call` / `get` / `range` / `shell`）— 设计稿

- 日期：2026-10-08
- 状态：已实现（分支 `feat/cli-debug`）；§10 的决定已确认，与本稿的出入见 §11
- 影响范围：新增 `hetu/local.py`（进程内应用运行时，CLI 与 `Sandbox` 共用）、
  `hetu/data/backend/snowflake_lease.py`（发号租约，服务器与 CLI 共用）、四个 CLI 子命令
  （`hetu/cli/call.py`、`data.py`、`shell.py`、`console.py`）；核心层小改（`Session` 提交观察
  钩子、worker id 分配器的 tool 模式、SQLite KV 续期原语、`Backend.post_configure` 可跳过
  servant、端点权限判定抽成纯函数、`HeadlessClient.session` 用表自己的 backend）；
  `server/main.py` 的发号流程改用 `SnowflakeLease`（行为不变）；`Sandbox` 改为 `LocalApp`
  子类；`cli/base.py` 的配置定位（`$HETU_CONFIG`、`./config.yml`、SQLite 相对路径按配置目录
  解析）；`Dockerfile`；文档与测试。
  **不涉及客户端 SDK、不涉及 schema 迁移、不改 `commit_v2.lua`、不给服务器加任何网络入口。**

## 1. 背景与目标

### 1.1 目标

HeTu 目前没有从命令行调用 System 的入口。目标是让大模型（以及人）在命令行里调试游戏：

- 以 admin 或指定玩家身份调用任意 System，拿到**原始返回值和完整 traceback**；
- 直接查看组件数据（不做 RLS 过滤）；
- 输出机器可读（stdout 一行 JSON），退出码区分错误类别；
- 开发库（SQLite，不必开服）和生产库（Redis）都能用，且在生产上有写保护和审计。

v1 不做：调用纯 `@define_endpoint`（依赖连接的端点，见 §8）、观测订阅推送、经网络远程调用。

### 1.2 为什么是"进程内直连后端"，而不是给服务器加带令牌的 RPC 入口

- **安全**：能跑 CLI 的人已经拿到了配置解析出的数据库地址和口令（或注入它们的环境变量）
  以及 app 代码，本来就是数据库 root；直连不给线上多开攻击面。文档的说法是"没有单独基于
  令牌的管理员端点"（`docs/zh/advanced.md:231`），admin 只给后台工具，本设计与之一致。走
  WebSocket 就得给服务器加一个令牌提权入口，而且生产上一直开着。
- **推送照常**：写入走与 System 完全相同的 `Session.commit()`。Redis 的行 / 索引变更由副本
  产生 keyspace 通知，表频道由 `commit_v2.lua` PUBLISH；SQLite 在同一个写事务里写通知表
  （`data/backend/sqlite/commit.py` 的 `run_commit`），由服务器进程轮询。在线客户端收到的
  推送与正常调用没有区别，`hetu.headless` 靠的也是这一点。
- **对大模型更友好**：`EndpointExecutor.execute_` 吞掉 Endpoint 里的异常只记日志，debug 模式
  回给客户端的 `err` 帧也只有 `"类型: 消息"`，没有 traceback（`endpoint/executor.py:179-192`、
  `server/receiver.py:75-78`）；System 的普通返回值在线路上只剩 `"ok"`（`receiver.py:81-88`）。
  进程内调用能拿到完整 traceback 和原始返回值。
- **不需要开服**：SQLite 开发库也能用；每次 `hetu call` 都是新进程，跑的是本地最新代码
  （这是双刃剑，见 §4）。

### 1.3 关键现状发现（均已核对，决定了实现路线）

1. **权限只在端点层**：`SystemCaller.call` 只查 System 是否存在（`system/caller.py:74-83`），
   权限检查在 `EndpointExecutor.execute_check`（`endpoint/executor.py:85-126`）。所以"以 admin
   调用"的实际作用只是让 `ctx.is_admin()` 为真。生产里 USER 端点不放行 caller=0
   （`executor.py:90-98`）——用 caller=0 跑 USER System 是线上不可能出现的状态，而
   `upsert(owner=ctx.caller)` 这类写法会因此在库里悄悄建出 owner=0 的行。
2. **没有连接不是新约束**：future call 与 on_start 的执行上下文本来就是 `connection_id=0`、
   `request=None`（`system/future.py:542-551`、`system/startup.py:71-80`）。`elevate` 断言
   `connection_id != 0`（`endpoint/connection.py:122`），会直接报错；把 `connection_id` 当值用
   的代码不会报错，只会走错分支。
3. **`Sandbox` 能复用但是测试专用**：`call_system / get / must_get / range / insert / upsert`
   可以直接拿来，但它只连一个 SQLite（`testing/__init__.py:212-213`）、自动建表（`:218`）、
   固定 `worker_id=1` 且不租约（`:158`、`:184-185`）、带测试用的注册表重载逻辑（`:188-209`）、
   有 `flush()`（`:459-464`）。这些都不能进生产路径。
4. **服务器的发号流程**是：租 worker id → 读时间戳高水位 → `SnowflakeID.init(lease=keeper)`
   → 预留水位 → 两个后台循环（5 秒续约、5 秒预留水位）→ 关服时先写精确水位再释放租约
   （`server/main.py:131-167`、`:170-184`、`:293-346`）。水位表固定用 `INSTANCES[0]` 的
   `WorkerLease`（`main.py:131`）。
5. **只租 id 不接水位会重号**：`SnowflakeID.init(id, -1)` 从"当前时间 + 10 秒"开始发号
   （`common/snowflake_id.py:108-111`）。这样的进程正常退出、释放租约后，10 秒内拿到同一
   id 的进程读到的还是旧水位，会落在它用过的毫秒上。CLI 常在另一台机器上跑，两边的时钟差
   会放大这个窗口。
6. **围栏 45 秒跳闸**：租约 TTL 60 秒、余量 15 秒（`redis/worker_keeper.py:18-23`），持有租约
   超过 45 秒不续约就 `WorkerLeaseExpired`；续约发现被抢时抛 `SystemExit`
   （`redis/worker_keeper.py:232-251`）。所以每个持租约的命令都要续约，不止 shell。
7. **`RedisWorkerKeeper.get_worker_id` 不适合 CLI**：它先对 0~1023 逐个串行
   `GETEX key EX 60` 找"自己的旧 id"，再从 0 往上 `SET NX`（`redis/worker_keeper.py:163-200`）。
   CLI 每次都是新 pid：(a) 第一轮必然 1024 次全落空，远端 Redis 上要好几秒；(b) GETEX 会把
   **所有**现存租约的 TTL 刷回 60 秒，包括已死 worker 的。CLI 只要每分钟调用一次以上，死租约
   就永不过期，`hetu upgrade` 会一直以为有服务器在跑（`cli/migrate.py:101-118`）。
8. **SQLite 的 worker id**：`FixedWorkerKeeper` 取 `SANIC_WORKER_IDENTIFIER` 里的数字，单进程
   模式退化为 0（`data/backend/worker_keeper.py:75-79`）。CLI 进程没有这个变量，拿到 0，与
   服务器撞号。只预留一个固定 id 也不够：大模型常并行执行命令，两个 CLI 用同一个 id 照样重号，
   还违反水位的单写者前提（`snowflake_timestamp.py:71-78`）。SQLite store 已有带过期的 KV
   原语 `kv_set_nx / kv_get / kv_delete_if`（`sqlite/store.py:692-720`，维护锁在用），只缺一个
   "值相符才续期"。
9. **表结构检查有两套**：服务器的 `check_table` 比 `cluster_id` 和组件 JSON 的 md5
   （`data/backend/base.py:1009-1016`）；headless 的 `_schema_diff` 只比数据布局，cluster_id
   取自 meta（`headless.py:125-174`、`:277-294`）。CLI 跑 System 必须在本地
   `build_clusters`，cluster_id 是本地算的；算错了 Redis key 就错（读出来是空、写进没人看的
   key），所以必须用 `check_table`。md5 还覆盖 `table_sub / point_sub / hidden`，这几项决定
   commit 发哪些通知。
10. **提交只有一条路**：`Session.commit()` → `master.commit(idmap)`（`session.py:93-108`），
    `RetryAttempt` 也走 `session.commit()`（`session.py:163-178`）。
    `IdentityMap.get_commit_rows()` 是纯函数（`idmap.py:839-911`），提交前读一遍写集没有副作用。
11. **配置**：`yamlloader.Loader` 支持 `${VAR}` / `${VAR:-默认值}` 插值和 `!eval` /
    `!include`。`APP_FILE` 的相对路径按配置目录解析（`cli/base.py:26-34`），但 SQLite 的
    `sqlite:///./hetu.db` 相对**进程当前目录**（`sqlite/client.py:58`），而 `hetu init`
    生成的正是这个值（`cli/init.py:315`），`sqlite3.connect` 遇到不存在的文件会新建。
    目前没有 `$HETU_CONFIG`。
12. **日志**：`cli/migrate.py:18-21` 在 import 时就把 `HeTu.root` 和 `logging.lastResort` 设成
    DEBUG，`cli/__init__.py` 又 import 它，所以每个 hetu 命令都受影响。配置模板的 console
    handler 写 `ext://sys.stdout`（`CONFIG_TEMPLATE.yml:209`）；replay 默认关闭
    （`safelogging/default.py` 里 `HeTu.replay` 为 ERROR）；replay 的文件 handler 自注"不是进程
    安全的"（`CONFIG_TEMPLATE.yml:219`）。
13. **序列化**：线上 msgpack 就是无参的 `msgspec.msgpack.Encoder()`
    （`server/pipeline/jsonb.py:25-26`），`numpy.int64` 会直接 `TypeError`（已实测）；
    `print(np.record)` 只有值、没有字段名（已实测）；`json.dumps(float("nan"))` 输出非法 JSON
    `NaN`。
14. **`Dockerfile` 默认命令起不来**：`CMD ["start", "--config /app/config.yml"]` 是单个参数，
    argparse 报 `unrecognized arguments`（已实测）。镜像 `WORKDIR /`。
15. **任何 hetu 命令都会加载 sanic**：`cli/start.py:18-26` 顶层 import sanic 和 `hetu.server`，
    而 `cli/__init__.py` 导入全部命令模块。
16. **`elevate` 默认顶号**：`kick_logged_in=True`（`connection.py:102`、`:139-142`）。以后若照
    `Sandbox.call` 给 CLI 加 Endpoint 路径，会把线上真在线的玩家踢下线（见 §8）。
17. **`on_server_setup` 只在服务器里跑**（`webext.py:108-141`），app 常用它挂
    `before_server_start` 做每 worker 初始化，CLI 里不会执行。
18. **区间语法**：`str` 值以 `[` / `(` 开头会被当作闭 / 开区间标记剥掉
    （`data/backend/base.py:251-265`），`get(name="[GM]Bob")` 实际查的是 `GM]Bob`。

### 1.4 设计决策（本稿默认值）

- **D1** 进程内直连后端；v1 只调 System。
- **D2** 共用核心 `LocalApp`（`hetu/local.py`），`Sandbox` 改为其子类；`flush()` 只留在
  `Sandbox`。
- **D3** 身份默认值按 System 的 permission 推断，USER 必须显式给玩家 id；显式给出的身份照办，
  生产端点不会放行时给 warning。
- **D4** 发号流程抽成 `SnowflakeLease`，服务器与 CLI 共用；CLI 用 tool 模式分配 worker id
  （Redis 从 1023 往下 `SET NX`、不做复用扫描；SQLite 在 [1000, 1023] 里用 KV 租约），水位照接。
- **D5** 表结构用 `check_table` 按需校验（只查本次碰到的表）；`hetu get/range` 走 headless
  按组件名读，不 import app，默认读 servant。
- **D6** stdout 只有一行 JSON；日志和 `print` 全进 stderr；强制 UTF-8。
- **D7** 写保护：`--dry-run`，加配置项 `CLI_ALLOW_WRITE`（默认 true，可在部署配置里关掉）；配置
  `DEBUG` 关闭时真写入会给出警告。
- **D8** 审计写独立的 JSONL 文件，挂在提交观察钩子上，不用 replay 日志。
- **D9** 配置定位：`--config` > 命令行参数模式 > `$HETU_CONFIG` > `./config.yml`；SQLite
  相对路径改为按配置目录解析（服务器一起改）；CLI 不新建库文件。

## 2. 命令行接口

### 2.1 总览

```text
hetu call  [公共选项] [身份选项] [--args-file PATH|-] [--uuid U] [--dry-run] [--timeout SEC]
           SYSTEM [ARG ...]
hetu call  --list [公共选项]
hetu get   [公共选项] [--master] [--fields F1,F2] COMPONENT FIELD=VALUE
hetu get   --list [公共选项]
hetu range [公共选项] [--master] [--fields F1,F2] [--limit N] [--desc]
           COMPONENT INDEX LEFT [RIGHT]
hetu shell [公共选项] [--dry-run] [-c CODE | FILE | -]

公共选项：--config PATH  --instance NAME  -v / -vv
         无配置文件时：--app-file  --namespace  --db（同 hetu upgrade）
身份选项：--as UID  --group NAME  --user-data JSON|@FILE
```

- 选项可以写在位置参数后面（`hetu call add_gold 1001 500 --dry-run`），负数按位置参数处理；
  以 `-` 开头的非数字参数要放在 `--` 后面（`hetu call rename 1001 -- -abc`）。
- 用法示例：

  ```bash
  hetu call --list                          # 先看有哪些 System
  hetu get --list                           # 组件、字段、索引
  hetu call add_gold 1001 500               # ADMIN 级 System，默认以 admin 身份
  hetu call --as 1001 buy_item 3 2          # USER 级 System 必须给玩家 id
  hetu call --as 1001 --group gm gm_kick 2002
  hetu call send_mail --args-file args.json # 复杂参数走文件，避开 PowerShell 引号问题
  hetu call add_gold 1001 500 --dry-run     # 执行但不提交，看它会写什么
  hetu get Player owner=1001
  hetu range Item owner 1001 --limit 50
  hetu shell -c "await call_system('add_gold', 1001, 500); show(await get('Player', owner=1001))"
  ```

### 2.2 配置定位与加载（所有子命令共用，`start` / `upgrade` 一起改）

定位顺序：

1. `--config PATH`；
2. 命令行参数模式：显式给了 `--app-file`、`--namespace`、`--db` 中任意一个，就按参数拼配置
   （`start` / `upgrade` 的现有模式；新命令同样支持这组参数）。各命令在此模式下的必填项：
   `call` / `shell` 要 `--app-file --namespace --instance --db`，两个 `--list` 要
   `--app-file --namespace`，`get` / `range` 要 `--db --instance`。`--instance` 单独出现不触发
   此模式，配置文件模式下它用来选实例。`start` / `upgrade` 的 `--app-file`、`--db` 现在有默认值，
   要改成在参数模式内部补默认值，才能判断是否显式给出。后端类型按 URL 推断：把
   `infer_backend_type_from_db_url` 从 `start.py` 挪到 `cli/base.py`，顺手修掉 `upgrade` 写死
   `"type": "Redis"` 的问题；
3. `$HETU_CONFIG`；
4. 当前目录的 `config.yml`；
5. 都没有 → 退出码 3，提示用 `--config` 或 `HETU_CONFIG`。

加载：新增 `read_config_file(path) -> dict`（`cli/base.py`），`start`（再包成 Sanic
`Config`）、`upgrade` 和新命令共用：

- 用 `yamlloader.Loader`（环境变量插值、`!eval`、`!include`）；
- `APP_FILE` 用 `resolve_app_file` 按配置目录解析（现状）；
- **SQLite 的相对路径**（`master` / `servants` 里的 `sqlite:///./x.db`、`sqlite:///x.db`）也按
  配置目录解析成绝对路径。这改变了服务器的语义（以前相对进程当前目录）。为了平滑过渡：
  `start` / `upgrade` 发现"旧语义下的文件存在、新路径下的文件不存在"时打 warning，给出两个
  路径，不自动搬文件。命令行参数模式（`--db`）没有配置文件，仍按当前目录解析。

`--instance` 默认取 `INSTANCES[0]`，不在 `INSTANCES` 里报用法错误（退出码 2）。

每条输出都带 `target`：配置文件绝对路径、namespace、instance、各后端地址（Redis 口令打码，
SQLite 给绝对路径）。`${VAR:-默认值}` 因为环境变量缺失而悄悄落到 localhost 时，一眼能看出来：

```json
"target": {"config": "/srv/game/config.yml", "namespace": "game", "instance": "server1",
           "backends": {"main": "redis://:***@10.0.0.5:6379/0"}}
```

**CLI 不新建 SQLite 库文件**：打开前检查文件存在，不存在则退出码 3，提示"库文件不存在：
<绝对路径>（服务器还没在这个库上启动过，或配置路径不对）"。

**Docker**：镜像加 `ENV HETU_CONFIG=/app/config.yml`，`CMD` 改为 `["start"]`（顺带修好
§1.3 #14）。之后 `docker exec <容器> hetu call ...` 开箱即用。

### 2.3 `hetu call`

1. 定位、加载配置，设置进程环境（§3.8）。
2. `open_local_app(config, instance=..., mint_ids=True, address="cli")`（§3.3）：加载
   APP_FILE、建簇、连后端、租 worker id。加载 APP_FILE 抛异常属于代码问题，按退出码 1 输出
   异常与 traceback。
3. 在主 namespace（含 global）里找 System，找不到 → 退出码 2，附 `difflib.get_close_matches`
   的近似名。
4. 解析参数（§2.5），按 `arg_count / defaults_count` 检查个数，公式同 `execute_check`
   （`executor.py:129`），不符 → 退出码 2。
5. 确定身份（§2.4）。
6. 按写模式装上提交观察钩子（§2.10、§3.7），在 `asyncio.timeout(--timeout)` 里执行
   `ctx.systems.call(system, *args, uuid=--uuid)`。
7. 输出（§2.9），释放租约，按 §2.9 的规则退出。

### 2.4 身份

库 API `LocalApp.call_system(..., caller=None, group=None)` 在没给身份时按下表推默认值，
`hetu call` 的 `--as` / `--group` 原样传进去：

| System 的 permission       | 没给 `--as` / `--group` 时            | 说明                                       |
|----------------------------|---------------------------------------|--------------------------------------------|
| `None`（内部 System）、`ADMIN` | caller=0，group=`"admin"`          | 内部 System 没有端点；ADMIN 端点要求 admin |
| `GM`                       | caller=0，group=`"admin"`             | GM 端点放行 admin（`executor.py:99-108`）  |
| `EVERYBODY`                | caller=0，group=`"guest"`             | 等同匿名连接（`server/websocket.py:124`）         |
| `USER`                     | **报错**（退出码 2），要求 `--as UID` | 见 §1.3 #1                                 |

- 给了 `--as UID`、没给 `--group` → group=`"guest"`，模拟真实玩家。不用 admin：System 里
  `if ctx.is_admin()` 之类的放行会让调试的不是玩家路径。注意 admin 组的 `ctx.is_gm()` 为 False。
- 给了 `--group`、没给 `--as` → caller=0（USER 仍然报错）。
- 显式给出的身份一律照办。若 System 有端点（permission 不为 None）而生产端点检查不会放行这个
  身份，输出加一条 `warnings`。判定逻辑从 `execute_check` 抽成纯函数
  `permission_allows(permission, caller, group) -> bool`（放 `endpoint/executor.py`），两处
  共用，避免两份逻辑漂移。
- caller 是推断出来的 0、而 System 读了 `ctx.caller` 时，也加一条 warning（启发式：扫描 System
  函数及其嵌套函数代码对象的 `co_names` 里有没有 `caller`）。内部 System（如
  `on_disconnect`）和 GM System 常拿 `ctx.caller` 定位行或记录操作人，caller=0 时结果多半不是
  想要的。
- `--user-data JSON|@FILE` 成为 `ctx.user_data`。登录时写进去的数据 CLI 无从得知，需要时手动给。
- 其余上下文：`connection_id=0`、`request=None`、`address="cli"`、`timestamp=time.time()`。
- shell 里的 `call_system` 就是库 API，默认规则相同。`Sandbox` 覆盖为宽松默认（caller=0、
  group=`"guest"`），单测写法不受影响；它现在的 group 默认是 `""`，改成 `"guest"` 与线上一致
  （见 §10）。

### 2.5 参数解析

对每个位置参数：

1. System 对应位置的形参注解是 `str` → 原样传字符串（`"123"`、`"true"` 不会被改成数字和布尔）。
   注解用 `inspect.signature(func, annotation_format=annotationlib.Format.STRING)` 读，
   不对前向引用求值，和 `"str"` 比较。
2. 否则 `json.loads(arg)`，成功就用解析结果。
3. 解析失败：去掉前导空白后以 `{`、`[` 或 `"` 开头 → 退出码 2，提示"参数看起来是 JSON 但
   解析失败（PowerShell 可能吃掉了引号），改用 --args-file 或 stdin"；否则原样当字符串。

`--args-file PATH` 读一个 JSON 数组作为全部参数，`-` 表示 stdin；与位置参数互斥。按
`utf-8-sig` 读，容忍 PowerShell `Out-File` 写出的 BOM。

### 2.6 列表：`hetu call --list` / `hetu get --list`

两者都不连数据库，只加载 app 并建簇。

`hetu call --list`：

```json
{"ok": true, "target": {...},
 "systems": [
   {"name": "add_gold", "permission": "ADMIN",
    "params": [{"name": "uid", "annotation": "int", "required": true},
               {"name": "amount", "annotation": "int", "required": false, "default": 100}],
    "components": ["Player"], "depends": [], "call_lock": false, "on_start": false,
    "builtin": false, "doc": "docstring 第一行"}],
 "endpoints": [
   {"name": "login_by_token", "permission": "EVERYBODY", "params": [...], "doc": "...",
    "callable": false}]}
```

- 不列 `__core_pin_system_*`（`system/definer.py:150-161`）。
- `builtin`：函数定义在 `hetu.` 模块里（`create_future_call` / `ensure_future_call` /
  `cancel_future_call`）。
- `components` 取 `full_components`，去掉 `SystemLock` 的副本；`call_lock` 即是否含
  `master_ is SystemLock` 的副本；permission 为 None 时输出 `null`。
- `endpoints` 列出本 namespace 的纯 `@define_endpoint`（`EndpointDefines` 里不在 System 表中的
  名字），让大模型知道它们存在；v1 一律 `callable: false`。

`hetu get --list`：

```json
{"ok": true, "target": {...},
 "components": [
   {"name": "Player", "namespace": "game", "permission": "USER", "volatile": false,
    "backend": "main", "cluster_id": 3,
    "fields": [{"name": "owner", "dtype": "<i8", "default": 0, "unique": true,
                "index": true, "hidden": false}]}]}
```

来源是 `SystemClusters().get_components(namespace)`（组件 → cluster_id，含被 pin 进每个
namespace 的 core 组件）。`get` 只接受 `id` 或带索引的字段，没有这份清单，大模型不知道能查什么。

### 2.7 `hetu get` / `hetu range`

- **不 import app**：用 `HeadlessClient.resolve_tables_` 按组件名解析，schema 取自服务器 meta。
  app 代码 import 报错、本地定义正改到一半时照样能看数据——这正是大模型在调试的时候。只读
  路径不调 `post_configure`（不需要 Lua，也不需要 keyspace 配置）。
- 多后端：按配置顺序在各后端上找 meta，第一个找到的为准；都没有 → `TableNotFound`，退出码 3。
- `hetu get COMP FIELD=VALUE`：只能一个字段，必须是 `id` 或带索引的字段（否则退出码 2，并列出
  可查字段）。值按 meta 里的列 dtype 转换：整数列转 int（接受 `"1001"`）、浮点列转 float、
  布尔列只认 `true/false/1/0`、`U` 列原样、bytes 列按 UTF-8 编码。`U` 列的值以 `[` 或 `(`
  开头时自动在前面补一个 `[`，保证按字面值点查（§1.3 #18）。
- `hetu range COMP INDEX LEFT [RIGHT]`：语义同 `repo.range`：省略 RIGHT 等于精确匹配 LEFT，
  `(` / `[` 前缀表示开 / 闭区间（不自动转义，文档写明）。`--limit` 默认 10（同 `repo.range`）；
  实际多取一行以准确给出 `truncated`，只返回 limit 行；`--limit -1` 不限条数。`--desc` 降序。
- 默认读 servant：`id` 用 `servant_get`，其他字段用 `servant_range(field, v, v, limit=1)`。
  `--master` 改为在 `client.session(comp)`（`only_master=True`）里读，用于刚写完马上读。
- `--fields F1,F2` 只输出指定列。hidden 列照样输出（admin 视角，不做 RLS）。
- 输出：

  ```json
  {"ok": true, "target": {...}, "component": "Player", "read_from": "servant",
   "row": {"id": 1234, "owner": 1001, "gold": 500}}
  {"ok": true, "target": {...}, "component": "Item", "read_from": "servant",
   "rows": [...], "count": 10, "truncated": true}
  ```

### 2.8 `hetu shell`

- 与 `hetu call` 同一个 `LocalApp`（也租 worker id）。预置名字：`app`、`client`
  （即 `app.client`）、`call_system`、`get`、`must_get`、`insert`、`upsert`、`show`、`np`、
  `hetu`、`asyncio`。`range` **不**预置成裸名字，否则会遮住内置的 `range`，用 `app.range(...)`。
- `call` 故意绑定为一个直接报错的函数："`call` 留给以后的 Endpoint 路径（与 `Sandbox.call`
  同义），跑 System 请用 `call_system`"。这样 shell 和 `Sandbox` 的同名方法含义一致，学一次就够。
- `call_system` 的返回值与 `Sandbox` 相同：默认是客户端实际收到的内容，要原始返回值传
  `raw=True`。
- `get` / `app.range` 与 `Sandbox` 相同：在事务里读 master，读得到刚写的数据。
- 代码来源：`-c CODE`、`FILE`、`-`（stdin）；都没给时，stdin 是终端就进交互模式，否则读 stdin。
- 非交互：整段源码用 `ast.PyCF_ALLOW_TOP_LEVEL_AWAIT` 编译，在事件循环里执行；最后一条语句是
  表达式时，对它的值调用 `show()`。抛异常 → traceback 写 stderr，退出码按 §2.9 的映射。
- 交互：REPL 跑在单独线程，事件循环留在主线程（参照 CPython `asyncio/__main__.py`），空闲时
  租约循环照常运行；displayhook 对 `np.record` / `recarray` 调用 `show()`。
- 代码留下的后台任务在关闭后端之前取消（与 §2.9 的 System 后台任务同一套收尾）。
- `show(x)`：把 `to_jsonable(x)`（§2.9）按缩进 2、`ensure_ascii=False` 打到 stdout。
- 进程内不会重新加载代码（`build_clusters` 每进程只能调一次），改了代码请重开 shell。
- `--dry-run` 和写保护对整个会话生效，包括用户代码里直接 `client.session` 的写入。审计记录
  执行的代码（不超过 4 KB；超过则记 sha256 与文件路径）。
- stdout 归用户代码使用，不套 JSON 信封；HeTu 自己的日志照样写 stderr。

### 2.9 输出格式与退出码

`call` / `get` / `range` / 两个 `--list` 的 stdout 恰好一行 JSON（UTF-8，
`ensure_ascii=False`）。shell 不受此约束。

`hetu call` 成功：

```json
{"ok": true, "target": {...}, "system": "add_gold",
 "identity": {"caller": 0, "group": "admin", "derived": true},
 "result": {"ResponseToClient": ["ok", 500]}, "client": ["ok", 500], "wire_error": null,
 "retries": 0, "elapsed_ms": 12.5, "dry_run": false, "writes": [...], "warnings": []}
```

- `result`：System 的原始返回值经 `to_jsonable`。`ResponseToClient` 记为
  `{"ResponseToClient": message}`，`RejectResponse` 记为
  `{"RejectResponse": {"code": ..., "reason": ...}}`。
- `client`：客户端 SDK 实际收到的内容，按 `receiver.rpc()` 的规则包装后再过一遍线上同款
  msgpack（`Sandbox._wire_roundtrip / _to_client_payload` 搬到 `LocalApp`）：
  `ResponseToClient` → 往返后的 message；`RejectResponse` → `{"rej": code}`（线上走 rej 帧，
  不是 rsp）；其他 → `"ok"`。
  往返失败 → `client: null`，`wire_error` 给出异常（如
  `"TypeError: Encoding objects of type numpy.int64 is unsupported"`），说明线上这条回复发不出去。
- `retries`：`ctx.race_count`。`elapsed_ms`：只算 System 调用本身。
- `writes`：见 §2.10。`warnings`：身份不被生产端点放行、System 留下了后台任务等。

失败：

```json
{"ok": false, "target": {...}, "error_type": "UniqueViolation", "error": "消息",
 "traceback": "完整 traceback", "writes": [...], "warnings": []}
```

失败时也带 `writes`：嵌套的 `ctx.systems.call` 各自独立提交，外层失败前可能已经写进去一部分。
`traceback` 只在退出码 1（代码或调用失败）时给出；用法错误、环境未就绪只给消息。

`to_jsonable` 规则：带字段名的 `np.record` / `np.void` → dict；结构化 ndarray / recarray →
dict 列表；其他 ndarray → list；`np.generic` → `.item()`；NaN / ±inf → `"NaN"` /
`"Infinity"` / `"-Infinity"`；bytes 能按 UTF-8 解码就转 str，否则 `{"__bytes__": base64}`；
tuple / set / frozenset → list；dict 的非 str 键转 str；其他对象 → `{"__repr__": repr(x)}`。

退出码：

| 码 | 含义                                                                                  |
|----|---------------------------------------------------------------------------------------|
| 0  | 成功                                                                                  |
| 1  | 代码或调用失败：加载 APP_FILE 抛异常、System 抛异常、竞态超过重试次数、超时              |
| 2  | 用法错误：参数、身份、System / 组件不存在、字段不可查                                   |
| 3  | 环境未就绪：配置、连接、库文件不存在、表不存在或结构 / 簇不一致、租不到 worker id 或租约丢失、写保护拒绝、审计写不进 |

退出码按异常类型映射，与异常在哪里抛出无关（例如嵌套调用里遇到 `TableNotReady` 也是 3）。
映射到 3 的只有 HeTu 自己定义的环境类异常（`TableNotReady`、`TableNotFound`、
`CliWriteForbidden`、`WorkerLeaseExpired`、租约相关异常等）和后端连接异常（如
`redis.exceptions.ConnectionError`）；app 代码自己抛的内置 `ConnectionError` 之类仍是 1。

`--timeout` 默认 30 秒，0 为不限，只包住 System 调用。超时 → 退出码 1，`error_type` 为
`"Timeout"`；若超时发生在提交途中，结果未知，错误消息要说明这一点（让调用方去看 `writes`、
审计或数据）。默认 `retry=9999`、每次最多 sleep 0.2 秒（`system/caller.py:165`），没有超时可能
空转很久，而 agent 的工具超时一到会直接杀进程，最后那行 JSON 就出不来了。

System 返回后（失败、超时也一样），若它 `create_task` 出去的任务还没结束：取消并等它们结束
（最多 5 秒），并加一条 warning："System 留下 N 个后台任务，CLI 退出时已取消（在服务器里它们会
继续跑）"。收尾顺序：取消后台任务 → 写精确水位、释放租约 → 关闭后端 → 写审计 end，后台任务的
finally 还能读写数据库。收尾出错（释放租约、关连接失败）只告警，不把已经完成的调用报成失败。

### 2.10 写保护、dry-run 与写集

每个进程处在三种写模式之一：

| 模式      | 何时                                       | 提交时                                                      |
|-----------|--------------------------------------------|-------------------------------------------------------------|
| `commit`  | 默认，且配置允许写                         | 正常提交，记录写集                                          |
| `dry_run` | `--dry-run`                                | 记录写集，不提交，session 按已提交清理                      |
| `forbid`  | `CLI_ALLOW_WRITE` 为 false 且没有 `--dry-run` | 有脏行就抛 `CliWriteForbidden`，不碰后端                  |

- `CLI_ALLOW_WRITE`：新配置项，默认 true。要禁止 CLI 写某个库，就在那份部署配置里写
  `CLI_ALLOW_WRITE: false`。开关在部署配置里，命令行只能通过 `--dry-run` 让它更安全，不能放开。
  命令行参数模式没有配置文件，按默认值 true 处理。
- 配置的 `DEBUG` 关闭（或参数模式下没有 `DEBUG`）时，每次命令第一次真提交后，输出的 `warnings`
  和 stderr 各加一条："DEBUG 关闭的配置（按生产库对待）上 CLI 刚刚真写入了数据；要禁止，在配置
  里设 CLI_ALLOW_WRITE: false"。只读调用、dry-run 不警告。
- `forbid`：`CliWriteForbidden` 不是 `RaceCondition`，System 不重试，直接失败，退出码 3，提示
  "加 --dry-run，或在配置里设 CLI_ALLOW_WRITE: true"。只读的 System 照常可用。注意 `--uuid`
  本身会写调用锁行，所以 forbid 下带 `--uuid` 的调用一定失败。
- `dry_run` 的局限（文档写明）：不做乐观锁和 unique 校验，看不到 `RaceCondition` /
  `UniqueViolation`；同一次调用里后续的读看不到"写进去"的数据；嵌套 System 的提交同样被丢弃；
  System 里的外部副作用（HTTP、文件）照常发生；提前 `ctx.session_commit()` 后继续执行的代码，
  面对的是没有提交的状态。
- `writes`：每次提交一项：

  ```json
  "writes": [
    {"instance": "server1", "cluster": 3, "committed": true,
     "tables": {"Player": {
        "insert": [{"id": 1234, "owner": 1001, "gold": 0}],
        "update": [{"id": 5678, "changes": {"gold": [500, 1000]}}],
        "delete": [{"id": 9012, "owner": 1002, "gold": 7}]}}}]
  ```

  值按组件 dtype 还原成 JSON 类型（`get_dirty_rows()` 给的是提交用的字符串：布尔按
  `"True" / "False"` 还原，bytes 列按 `to_jsonable` 规则）；每张表每种操作最多列 20 行，多的给
  `"omitted": N`。所有更新都改回了原值的提交（脏行列表全空）不列出。
- `committed`：真提交为 `true`，dry-run 为 `false`；提交途中被取消（`--timeout`、租约丢失）或
  连接出错时为 `"unknown"`——可能已经生效，要直接查数据。后端拒绝的提交（`RaceCondition` /
  `UniqueViolation`，什么都没写）不列出。
- 机制见 §3.7。

### 2.11 审计

- 文件：新配置项 `CLI_AUDIT_LOG`，默认 `logs/hetu_cli_audit.jsonl`（相对配置目录；命令行参数
  模式下相对当前目录），设为 `""` 关闭。只追加的 JSONL，每行用 `O_APPEND` 打开后一次
  `os.write`；不轮转（需要时用外部 logrotate 的 copytruncate）。
- 记录三种事件，公共字段：`ts`（带时区的 ISO 时间）、`run`（uuid4）、`user`
  （`getpass.getuser()`）、`host`、`pid`、`cwd`、`config`、`namespace`、`instance`、
  `worker_id`：
  - `start`：命令行 `argv`（数据库地址打码口令，每项最多 1 KB）、命令、System 名、参数（repr，
    最多 1 KB）、身份、写模式；shell 记执行的代码。`argv` 只记在这一条，不在每条记录里重复；
  - `commit`：每次提交的 instance、cluster、`committed`（同 `writes`，含 `"unknown"`）、各表按
    操作分组的**全部** id（`writes` 每种操作只列 20 行，审计不截断）；
  - `end`：`ok`、`error_type`、`elapsed_ms`。
- 只有可能写入的 `call` 和 `shell` 记审计；`get` / `range` / `--list` 只读，不记。
- 挂在提交观察钩子上，所以 shell 里直接 `client.session` 的写入、嵌套调用的写入都有记录。
- `start` 记录写不进 → 退出码 3，不在没有审计的情况下操作；之后的写入失败只在 stderr 告警
  （提交已经发生，撤不回）。
- 不用 replay 日志：它默认关闭；要写它就得加载配置里的 `LOGGING`，而其中 console handler 写
  stdout，会破坏 JSON 输出；它的文件 handler 不是进程安全的，与服务器的监听进程同时轮转同一个
  文件，Windows 上可能让服务器轮转失败（§1.3 #12）。
- 审计落在跑 CLI 的那台机器上，服务器那边不会有任何痕迹。生产上请在服务器容器里跑
  （`/app` 是 volume，默认路径落在 `/app/logs/`）。写进后端是后续工作（§8）。

## 3. 实现设计

### 3.1 模块划分

```text
hetu/local.py                         LocalApp、open_local_app、resolve_identity、
                                      load_app_module、TableNotReady、IdentityRequired
hetu/data/backend/snowflake_lease.py  SnowflakeLease（服务器与 CLI 共用）
hetu/cli/base.py                      配置定位与加载（扩充）
hetu/cli/console.py                   进程环境、to_jsonable、输出与退出码、写模式与写集、审计、
                                      CliWriteForbidden
hetu/cli/call.py                      CallCommand（含 --list）
hetu/cli/data.py                      GetCommand、RangeCommand（含 get --list）
hetu/cli/shell.py                     ShellCommand
```

`hetu/__init__.py` 不 import `hetu.local`，保持 `import hetu` 轻量（`test_headless_process.py`
守门）。

### 3.2 共用核心 `LocalApp` 与 `Sandbox`

```python
class LocalApp:
    """进程内的 HeTu 应用运行时：簇已建好、后端已连好，提供与 Sandbox 相同的调用 / 读写 API。"""

    namespace: str
    instance_name: str
    backends: dict[str, Backend]
    tbl_mgr: ComponentTableManager
    client: HeadlessClient          # get / range / insert / upsert 经它；多表事务 client.session(A, B)
    address: str                    # 作为 ctx.address：CLI 为 "cli"，Sandbox 为 "sandbox"

    def __init__(self, namespace, instance_name, backends, tbl_mgr, *,
                 verify_tables: bool, address: str = "local"): ...
    def resolve_identity(self, system: str, caller: int | None,
                         group: str | None) -> tuple[int, str]: ...   # §2.4；Sandbox 覆盖为宽松
    def new_context(self, caller: int, group: str,
                    user_data: dict | None = None) -> SystemContext: ...
    async def call_system(self, system: str, *args, caller: int | None = None,
                          group: str | None = None, user_data: dict | None = None,
                          uuid: str = "", raw: bool = False) -> Any: ...
    def to_client_payload(self, rtn: Any) -> Any: ...    # 原 Sandbox._to_client_payload
    async def get(...) / must_get(...) / range(...) / insert(...) / upsert(...): ...  # 原样搬来
    async def aclose(self) -> None: ...                  # 先释放租约，再关全部后端
```

- `client` 是 `HeadlessClient` 的子类，`table()` 先过按需校验（§3.4），`explicit_ids_only=False`，
  `owns_backend=False`（后端由 `LocalApp.aclose()` 统一关闭）。
- `new_context` 里 `ctx.systems` 是 `SystemCaller` 的子类，`call_` 之前对 `sys.full_components`
  做按需校验。嵌套的 `ctx.systems.call` 用的是同一个 caller，同样会校验。
- `Sandbox(LocalApp)`：保留 `create()`（注册表逻辑、临时 SQLite、建表、固定 worker id）、
  `call()`（Endpoint 路径）和 `flush()`；`verify_tables=False`；宽松的 `resolve_identity`；
  `address="sandbox"`。`Sandbox.__init__(namespace, instance_name, backend, tbl_mgr)` 与
  `sb.backend` 保持不变（内部以 `{"default": backend}` 交给 `LocalApp`）。现有方法的签名与报错
  不变，只有两处：`call_system` 的 group 由写死的 `""` 改为宽松默认 `"guest"`，`caller` 默认值
  由 `0` 改为 `None`（宽松规则下即 0）。`tests/test_testing_sandbox.py` 必须原样通过。
- **`flush()` 永远不出现在 `LocalApp` 上**；shell 不暴露任何清表入口。

### 3.3 打开生产应用：`open_local_app`

```python
async def open_local_app(config: dict, *, instance: str | None = None,
                         mint_ids: bool = True, address: str = "local") -> LocalApp: ...
```

1. `load_app_module(config["APP_FILE"])`：与服务器相同（`spec_from_file_location("HeTuApp")`、
   登记 `sys.modules["HeTuApp"]`）。这段代码现在 `server/main.py:357-363` 与
   `cli/migrate.py:121-127` 各有一份，抽成这一个函数三处共用。
2. `SystemClusters().build_clusters(NAMESPACE)`。不调 `build_endpoints`（v1 用不到）。
3. 后端：照 `start_backends`（`main.py:89-98`）为 `BACKENDS` 每项建 `Backend`，第一项为
   default。SQLite 先检查库文件存在（§2.2）。
4. `ComponentTableManager(NAMESPACE, instance, backends)`。不建表、不迁移。
5. 每个后端 `post_configure(servants=False)`：只做 master 一侧（加载 Lua、探测 HSETEX、索引
   dtype 检查）；跳过 servant 一侧——它会对每个节点 CONFIG GET，必要时还 CONFIG SET
   （`redis/client.py:302-332`），而 CLI 不订阅任何东西。
6. `mint_ids=True` 时：租约表取
   `ComponentTableManager(NAMESPACE, INSTANCES[0], backends).get_table(WorkerLease)`，先校验它
   （`check_table` 不为 `"ok"` 即 `TableNotReady`）；然后
   `SnowflakeLease(create_worker_keeper(lease_tbl.backend, os.getpid(), tool=True), lease_tbl)`，
   `await lease.__aenter__()`（拿 id、接水位、起两个循环）。`LocalApp` 持有它，`aclose()` 时释放。
   租约丢失（`on_lost`）时，CLI 取消主任务，输出 `error_type: "LeaseLost"`，退出码 3。
7. 任一步失败：关掉已打开的东西再抛出。

### 3.4 按需表结构校验

- `LocalApp._verify(comp)`：每个组件第一次用到时调 `maint.check_table(tbl)`，结果缓存。不为
  `"ok"` → `TableNotReady(comp_name, status)`，附处理建议：
  - `not_exists`："库里没有这张表：先用当前代码启动一次服务器（会建新表）"；
  - `schema_mismatch` / `cluster_mismatch`："本地代码与库里的表结构 / 簇不一致：先
    `hetu upgrade`，或用与线上一致的代码版本跑 CLI"。
- 入口：System 调用前校验全部 `full_components`；`client.table()`；打开时校验租约表。
- 为什么按需而不是启动时全量：生产上只为碰到的表各读一次 meta；正在改的无关组件不会把所有
  命令都卡住。这样做是安全的：本地簇编号整体漂移时，碰到的表自己的 `cluster_id` 也对不上，
  照样拦得住。
- 嵌套调用里抛出的 `TableNotReady` 会从外层 System 冒出来，整个事务放弃。

### 3.5 `SnowflakeLease`（服务器与 CLI 共用）

```python
class SnowflakeLease:
    """一个进程的雪花发号租约：worker id + 时间戳高水位 + 续约 / 预留两个后台循环。"""

    def __init__(self, keeper: WorkerKeeper, lease_tbl: Table) -> None: ...
    async def acquire(self, *, wait: bool) -> int: ...
    async def renew_forever(self, on_lost: Callable[[], None]) -> None: ...
    async def reserve_forever(self) -> None: ...
    async def release(self) -> None: ...
    async def __aenter__(self) -> Self: ...      # acquire(wait=False) + 起两个循环任务
    async def __aexit__(self, *exc) -> None: ...  # 取消循环 + release
```

- `acquire`：`keeper.get_worker_id()`；遇到 `KeyError` 时，`wait=True`（服务器）照现在每秒重试
  （`main.py:135-147`），`wait=False`（CLI）直接抛出；然后读水位 →
  `SnowflakeID().init(worker_id, last, lease=keeper)` → 预留水位（失败只告警）。
- `renew_forever` 就是现在 `worker_keeper_renewal` 的循环体（`main.py:317-346`）：每 5 秒
  `keep_alive`；`RedisConnectionError` 记错误后继续；`SystemExit` → 调 `on_lost()` 后退出；
  `CancelledError` → 退出；其他异常 `logger.exception` 后继续。
- `reserve_forever` 就是现在的 `snowflake_timestamp_save`（`main.py:293-314`）。
- `release` 就是现在 `close_backends` 的前半段（`main.py:174-184`）：写精确水位（失败只告警），
  释放 worker id。
- 服务器侧：`start_backends` / `close_backends` 签名与对外行为不变（含
  `app.ctx.default_backend`）。`start_backends` 建 `SnowflakeLease` 并
  `acquire(wait=True)`，存到 `app.ctx.snowflake_lease`（取代只在 `main.py` 里用的
  `worker_keeper` / `snowflake_ts_keeper` 两个属性）；`worker_keeper_renewal(app)` /
  `snowflake_timestamp_save(app)` 保留为薄包装（`renew_forever(on_lost=app.m.restart)` 等），
  `worker_main` 照旧 `add_task`；`close_backends` 调 `release()` 后关后端。
  `test_common.py` 里的租约 / 水位 / 围栏用例原样通过。

### 3.6 worker id 的 tool 模式

`create_worker_keeper(backend, pid, *, tool=False)`：

| 后端   | 服务器（`tool=False`，现状）                                 | CLI（`tool=True`，新增）                                     |
|--------|--------------------------------------------------------------|--------------------------------------------------------------|
| Redis  | `RedisWorkerKeeper`：先 GETEX 扫一遍找自己的旧 id，再从 0 往上 SET NX | 同一个类加 `tool=True`：node_id 带前缀 `cli:`；不做复用扫描；从 1023 往下 SET NX；续约、释放、围栏不变 |
| SQLite | `FixedWorkerKeeper`：Sanic 进程序号（0~N-1），无租约          | 新 `SQLiteToolWorkerKeeper`：在 [1000, 1023] 里从上往下 `kv_set_nx("snowflake:worker:<id>", node_id, ttl=60)`；续期用新原语 `kv_expire_if`，释放用 `kv_delete_if`；围栏同 Redis |

- Redis 不做复用扫描的原因见 §1.3 #7。从上往下分配让 CLI 远离服务器从 0 往上占用的 id，常见
  情况下一次 SET NX 就拿到。
- `_owner_exited`（只在 Windows 上判断）解析 node_id 前先去掉 `cli:` 前缀。
- 新增 `live_worker_leases(backend) -> dict[int, str]`（id → 持有者），`live_worker_ids` 保留为它
  的包装。SQLite 上它现在能返回 CLI 的租约（逐个 `kv_get` 预留段的 24 个 key；服务器 worker
  照旧看不到）。`hetu upgrade` 把 CLI
  持有的单独列出："其中 N 个是 hetu call / shell 进程，等它们结束即可（异常退出的最多 60 秒
  过期）"。SQLite 上 upgrade 也因此会拒绝在 CLI 运行时执行。
- `FixedWorkerKeeper` 的 Sanic 序号 ≥ 1000 时报错，保证预留段干净（实际不可能有那么多 worker）。
- SQLite store 新增 `kv_expire_if(key, value, ttl) -> bool`：一个写事务里，值相符且未过期就更新
  `expire_at` 并返回 True，否则 False。异步调用走现有的 `client.run_()`。
- 同一个 SQLite 库上最多 24 个并发的 CLI 进程，超出 → 退出码 3，提示"并发的 hetu call / shell
  太多"。
- `WORKER_ID_EXPIRE_SEC`、`FENCE_MARGIN_SEC` 挪到与后端无关的 `data/backend/worker_keeper.py`，
  SQLite 的 keeper 不必 import redis 模块。
- 水位：CLI 照服务器那样用 `INSTANCES[0]` 的 `WorkerLease`（每个 worker id 一行）走
  `SnowflakeTimestampKeeper`。这一步不能省，原因见 §1.3 #5。

### 3.7 提交观察钩子

`hetu/data/backend/session.py`：

```python
CommitFn = Callable[[IdentityMap], Awaitable[None]]
commit_observer: ContextVar[Callable[[Session, CommitFn], Awaitable[None]] | None] = ContextVar(
    "hetu_commit_observer", default=None
)


class Session:
    async def commit(self) -> None:
        if self._idmap.is_dirty:
            observer = commit_observer.get()
            if observer is None:
                await self._master.commit(self._idmap)
            else:
                await observer(self, self._master.commit)
        self.clean()
```

- 观察者决定是否真提交（dry-run 不提交），可以抛异常（forbid），并通过
  `session.idmap.get_dirty_rows()`（纯函数）记录写集。真实提交返回后才标 `committed`；遇到
  `RaceCondition` 时 System 会重试，那一次不算已提交。
- 作用域用 ContextVar：CLI 只在用户工作（System 调用、shell 代码）期间设置它。租约的两个循环
  任务创建得更早，复制的上下文里没有它，所以水位写入不会被拦截或记账。服务器从不设置它，每次
  提交只多一次 `ContextVar.get()`。
- 覆盖所有走提交的路径：System 事务、嵌套调用、`elevate` / `new_connection`、
  `HeadlessSession`、`RetryAttempt`。不覆盖 `direct_set`（非事务，只用于易失的
  `Connection.last_active` 与水位，`ctx.repo` 不暴露它）。

### 3.8 进程环境：stdout、日志、编码

每个新命令开始时（import app 之前）：

1. `sys.stdout` / `sys.stderr` 都 `reconfigure(encoding="utf-8", errors="backslashreplace")`。
   日志和报错里有 emoji 与中文，Windows 的 GBK 管道会让 CLI 在打印自己的错误时抛
   `UnicodeEncodeError`。
2. 记下 `real_stdout`，之后把 `sys.stdout` 指向 `sys.stderr`，覆盖 app 的 import、System 执行和
   库里的一切 `print`；最后那行 JSON 写到 `real_stdout`。shell 的用户代码仍用真正的 stdout。
3. 日志由 CLI 自己配置，**不套用配置文件的 `LOGGING`**：一个写 stderr 的 `StreamHandler`；
   `HeTu.root` 默认 WARNING，`-v` 为 INFO，`-vv` 为 DEBUG；`HeTu.replay` 设
   `propagate=False` 且不挂 handler（`-vv` 时挂到 stderr）。同时把 `cli/migrate.py` 模块级的
   `logger.setLevel(DEBUG)` / `lastResort.setLevel(DEBUG)` 挪进 `MigrateCommand.execute`，
   import `hetu.cli` 不再改动全局日志设置。
4. `cli/start.py` 顶层的 sanic / `hetu.server` import 挪进 `execute()`，call / get / range /
   shell 进程不加载 sanic（子进程测试守门）。

### 3.9 核心层改动清单（都是小改，服务器行为不变）

1. `Session.commit` 加 `commit_observer`（§3.7）。
2. `Backend.post_configure(components=None, *, servants=True)`：`servants=False` 跳过 servant 一侧。
3. `endpoint/executor.py`：抽出 `permission_allows(permission, caller, group)`，`execute_check`
   改用它（行为不变，OWNER / RLS / 未知级别照旧失败关闭）。
4. `HeadlessClient.session` 用表自己的 backend，而不是 `client.backend`（headless 只有一个后端，
   结果相同；`LocalApp` 的多后端配置需要它。同簇的组件必然在同一后端，`build_clusters` 已保证）。
5. `data/backend/worker_keeper.py`：`create_worker_keeper(..., tool=False)`、
   `SQLiteToolWorkerKeeper`、`live_worker_leases`、`FixedWorkerKeeper` 的范围检查、共享的 TTL
   常量。
6. `data/backend/redis/worker_keeper.py`：tool 模式、`_owner_exited` 处理 `cli:` 前缀、
   `live_worker_leases`。
7. `data/backend/sqlite/store.py`：`kv_expire_if`。
8. 新增 `data/backend/snowflake_lease.py`；`server/main.py` 改用它（行为不变）。
9. `hetu/testing/__init__.py`：`Sandbox(LocalApp)`。
10. `cli/base.py`：配置定位、`read_config_file`、SQLite 路径解析、`infer_backend_type_from_db_url`；
    `start.py` / `migrate.py` 改用它们；修掉 migrate 的日志副作用；`start.py` 延迟 import sanic。
11. `load_app_module` 供 server / migrate / local 共用。
12. 所有用户可见字符串走 `_()`；argparse 的 help 里的 `%` 写成 `%%`
    （`test_i18n_catalogs.py` 守住译文）。译文目录照常由 `autolang sync` 生成，不手改。

## 4. 与服务器运行环境的差异（必须写进文档）

- **CLI 跑的是本地代码**。开发时服务器可能还在跑旧代码：`WORKER_NUM=1` 不会自动重载
  （`cli/start.py:300`），所以"CLI 验证通过"不等于客户端那条路径已经生效，要重启服务器。生产上
  从开发机的代码跑，等于拿没部署的逻辑写生产数据，表结构校验拦不住逻辑上的差异；请在部署好的
  容器里跑（`docker exec`）。代码指纹比对见 §8。
- **没有连接**：`connection_id=0`、`request=None`；调 `elevate` 的 System 会失败（断言）；登录时
  写进 `user_data` 的数据没有，需要时用 `--user-data` 给。
- `on_server_setup` / `before_server_start` 里的初始化在 CLI 里不执行，依赖它们建立的全局状态
  的 System 会出问题。
- System 里 `create_task` 出去的后台任务会在 CLI 退出时被取消。
- 经 CLI 创建的 FutureCall 由在跑的服务器执行；没有服务器在跑就一直等着。
- on_start System 不会自动跑，需要时显式调用。
- `hetu get` / `range` 默认读 servant，刚写完马上读请加 `--master`。
- dry-run 的局限见 §2.10。

## 5. 正确性与安全分析

- **订阅一致性**：提交路径与 System 相同；commit 负载按本地组件定义构造，而 `check_table` 保证
  本地定义的 md5 与服务器 meta 一致，所以发出的频道与服务器完全相同。
- **雪花号唯一**：CLI 持有独占租约（Redis `SET NX`；SQLite 在预留段内 KV `SET NX`），续约和释放
  都是 CAS；最后一次成功续约 45 秒后围栏拒绝发号；水位的 load / reserve / save 让下一个拿到
  同一 id 的进程从安全的位置开始，跨机器、有时钟差时也成立。并发的 CLI 进程不会共用 id。
- **Redis 上的租约开销**：拿 id 通常 1 次 `SET NX`（高位 id 被占时才多试几次），加 1 次水位读、
  1 次水位写；续约每 5 秒 1 次 GETEX；释放 1 次 EVAL 加 1 次水位写。不再扫描、刷新别人的租约。
- **租约丢失**（CLI 冻住超过 60 秒，id 被别人拿走）：45 秒时围栏已经拒绝发号；续约发现后
  `on_lost` 中止命令，退出码 3。不会重号。
- **进程被强杀**：租约最多 60 秒过期；周期预留的水位盖住了已发出的 id；`hetu upgrade` 最多等
  60 秒，提示里会说明是 CLI 持有。
- **簇 / 结构不一致**：每张表第一次用之前都校验，错误的 cluster_id 到不了 Redis key。
- **写保护**在唯一的提交入口执行，经 `ctx.repo` 的写入都绕不过去；只有绕开 `ctx.repo` 去调内部
  `Table.direct_set` 的非公开用法拦不住。开关在部署配置里，命令行参数只能通过 `--dry-run` 让它
  更严。
- **默认身份**不会悄悄产生 owner=0 的垃圾行（USER 必须 `--as`）。
- **master 负载**：读默认走 servant；meta 只为碰到的表读；不做 servant CONFIG；租约开销最小。
  System 内的事务读与线上一样按 `master_or_servant` 选节点。不新增直接读 master 的代码，也不新增
  PUBLISH（`test_arch_master_reads.py`、`test_arch_publish.py` 守门）。
- **与 `hetu upgrade` 并发**：有 CLI 租约存活时 upgrade 拒绝执行（Redis，以及现在的 SQLite）。
- **stdout 纯净与编码**：见 §3.8。

## 6. 测试计划

SQLite 部分不需要 Docker；Redis 部分用现有 fixture（`mod_auto_backend` 等）。子进程测试沿用
`tests/test_headless_process.py` 的 `_run_py` 写法。

- **`tests/test_local_app.py`**：
  1. 在按服务器方式（`ComponentTableManager.check_and_create_new_tables()`）建好表的 instance 上
     `open_local_app`：`call_system` 有返回、写入可见；`get / range / insert / upsert` 与
     `Sandbox` 行为一致。
  2. 身份：USER 不给 caller → `IdentityRequired`；ADMIN / None / GM → `(0, "admin")`；
     EVERYBODY → `(0, "guest")`；只给 caller → group `"guest"`。`permission_allows` 与
     `execute_check` 在全部 permission × 身份组合上结果一致（表驱动）。
  3. 按需校验：用改过 properties 的 `load_json` 造同名类 → 只在用到时报
     `TableNotReady(schema_mismatch)`，没用到的不一致组件不挡路；手改 meta 的 cluster_id →
     `cluster_mismatch`；缺表 → `not_exists`，且之后表仍不存在。
  4. 提交观察钩子：dry-run 后数据不变且写集正确；forbid 下会写的 System 抛
     `CliWriteForbidden`、只读 System 正常；`RaceCondition` 重试时只记成功那次；租约循环的写入
     不被拦截。
- **`tests/test_common.py` 增补**（keeper）：
  5. Redis tool 模式：先拿到 1023；不 GETEX 别人的 key（预置的一条陈旧租约 TTL 持续下降）；
     node_id 带 `cli:` 前缀；`_owner_exited` 认得前缀；释放是 CAS。
  6. `SQLiteToolWorkerKeeper`：两个 keeper 拿到 [1000, 1023] 内不同的 id；`keep_alive` 延长
     过期；被抢 → `SystemExit`；释放；第 25 个 → `KeyError`；`live_worker_leases` 看得到；
     `kv_expire_if` 的语义。
  7. `SnowflakeLease`：拿到 / 释放一轮后写入了精确水位；现有的
     `test_restart_resumes_from_snowflake_watermark`、`test_boot_has_no_snowflake_clamp` 及 keeper
     / 围栏用例原样通过。
- **`tests/test_cli_debug.py`**（子进程，临时 SQLite 项目：config.yml + 一个在 import 时和
  System 里都会 `print` 的 app 文件）：
  8. `hetu call` 端到端：stdout 恰好一行合法 JSON；退出码 0 / 1 / 2 / 3 分别对应成功、System
     抛异常、USER 不给 `--as`、缺表；`result` / `client` / `wire_error`（System 返回含
     `np.int64` 的 `ResponseToClient`）；`retries`；超时；`DEBUG` 关闭时真写入有警告、只读调用没有。
  9. 用 `PYTHONIOENCODING=gbk` 模拟 Windows 管道：输出仍是合法的 UTF-8 JSON，没有
     `UnicodeEncodeError`。
  10. 两个 `hetu call` 进程并发向同一个 SQLite 库插行：都成功、id 不重复；两个 worker id 在
      `WorkerLease` 里的水位都不低于各自最后发出的 id 的时间戳。
  11. 参数：像 JSON 却解析失败 → 退出码 2；`str` 注解的形参保留 `"123"`；`--args-file -` 从
      stdin 读带 BOM 的 UTF-8；尾随 `--dry-run`；负数参数。
  12. 配置定位：`--config` > 参数模式 > `HETU_CONFIG` > `./config.yml`；换个工作目录运行时
      SQLite 相对路径按配置目录解析；库文件不存在 → 退出码 3 且没有建出文件；`target` 里 Redis
      口令被打码。
  13. 故意弄坏 APP_FILE 后 `hetu get` / `range` 照常工作；`truncated` 准确；以 `[` 开头的字符串
      按字面值命中；`--master`。
  14. `--list`：不含 core pin System，内置 System 带 `builtin`，纯 Endpoint 列出且
      `callable: false`；`get --list` 的字段与索引。
  15. 审计：有 start / commit / end 三行；shell 里直接写入也产生 commit 记录；审计路径不可写 →
      退出码 3。
  16. shell：`-c` 里顶层 await，最后一个表达式自动 `show`；脚本文件；stdin；`call(...)` 的报错
      提示；抛异常 → 退出码 1。
  17. `hetu call --list` 之后子进程里 `"sanic" not in sys.modules`。
- **回归**：`test_testing_sandbox.py`、`test_cli_start.py`、`test_migration.py`、
  `test_common.py`、`test_arch_*.py`、`test_headless*.py` 原样通过；Redis 变体在 CI 里跑
  （推送到非 main 分支时只跑 redis）。

## 7. 文档

- `hetu/skills/building-on-hetu/SKILL.md` 新增一节 "Debugging a running game
  (`hetu call` / `hetu get` / `hetu range` / `hetu shell`)"：跑的是本地代码、不代表服务器已重载；
  先 `call --list` / `get --list`；身份规则；JSON 字段与退出码；`--dry-run` 与写保护；读默认走
  servant（`--master`）；PowerShell 用 `--args-file` / stdin，`-` 开头的参数放 `--` 后面；
  range 值的 `[` / `(` 前缀；§4 的差异。CLI 一节同步更新。
- `docs/zh/operations.md` / `docs/en/operations.md` 新增"命令行调试"：生产用法（`docker exec`）、
  `CLI_ALLOW_WRITE`、审计文件、代码版本错位、与 `hetu upgrade` 的关系。
- `hetu/CONFIG_TEMPLATE.yml`：加 `CLI_ALLOW_WRITE`、`CLI_AUDIT_LOG` 及注释；注明 SQLite 相对路径
  按配置目录解析。
- `AGENTS.md`：模块表加 `hetu/local.py`、`hetu/data/backend/snowflake_lease.py`；CLI 命令列表加
  `call / get / range / shell`。
- `scripts/api_extras.py` 加 `local` topic（`LocalApp`、`open_local_app`、`TableNotReady`、
  `IdentityRequired`），重新生成 `docs/api/`；`hetu/llms.txt` 加链接。
- `hetu/testing/__init__.py` 的模块文档注明 `Sandbox` 是 `LocalApp` 的子类。

## 8. 取舍与边界（YAGNI）/ 后续工作

v1 不做：

- **纯 Endpoint 调用**。以后加的时候有三个坑：(1) 不能 `elevate`，它默认顶号，会把线上真在线的
  玩家踢下线（`connection.py:102`、`:139-142`）；应直接插一行 owner=uid 的临时 `Connection`、
  设好 `ctx.caller`、用完删掉（易失表，残留行会被 `elevate` 的空闲判定当作过期）。(2) 需要一个
  替身 `request` 对象，`Sandbox.call` 同样传的是 None（`testing/__init__.py:247`）。
  (3) `EndpointExecutor.execute_` 吞异常，要直接调 `ep.func` 或在外面截住异常才有 traceback。
- **`hetu watch`**（观测推送）：以后可在进程内复用 `SubscriptionHub` / `MQClient`。
- **代码指纹比对**：服务器启动时把 System 字节码与组件 JSON 的 hash 写进后端，CLI 比对，不一致
  时告警，非 SQLite 后端直接拒绝。
- **审计写进后端**（多机共享的痕迹）。
- `hetu set` / `insert` 之类的写子命令（用 shell）。
- shell 内热重载代码。
- 为 JS 消费者把大整数输出成字符串的选项。

## 9. 主要改动文件清单

新增：

- `hetu/local.py`
- `hetu/data/backend/snowflake_lease.py`
- `hetu/cli/console.py`、`hetu/cli/call.py`、`hetu/cli/data.py`、`hetu/cli/shell.py`
- `tests/test_local_app.py`、`tests/test_cli_debug.py`
- `docs/superpowers/specs/2026-10-08-cli-debug-design.md`（本文件）

修改：

- `hetu/cli/__init__.py`（注册新命令）、`hetu/cli/base.py`、`hetu/cli/start.py`、`hetu/cli/migrate.py`
- `hetu/server/main.py`
- `hetu/testing/__init__.py`
- `hetu/headless.py`（`session` 用表的 backend）
- `hetu/endpoint/executor.py`
- `hetu/data/backend/__init__.py`、`session.py`、`worker_keeper.py`、`redis/worker_keeper.py`、
  `sqlite/store.py`
- `Dockerfile`
- `hetu/CONFIG_TEMPLATE.yml`、`hetu/skills/building-on-hetu/SKILL.md`、`AGENTS.md`、
  `hetu/llms.txt`、`docs/zh|en/operations.md`、`scripts/api_extras.py`、`docs/api/`
- `tests/test_common.py`

## 10. 待确认的新决定

评审结论：除第 1 项按评审意见修改外，其余按本稿默认值落地。

1. ~~`CLI_ALLOW_WRITE` 仅当全部后端是 SQLite 时默认 true~~ → 已定：默认 true，`DEBUG` 关闭时
   真写入给出警告（§2.10）。
2. 服务器的 SQLite 相对路径语义改为相对配置目录，旧文件存在时打 warning（§2.2）。另一个选择是
   保持相对当前目录，CLI 只做"文件不存在就报错并给出绝对路径"。
3. `Sandbox` 的 group 默认值由 `""` 改为 `"guest"`（§3.2）。
4. shell 里 `call` 这个名字保留给以后的 Endpoint 路径、现在调用直接报错，而不是作为
   `call_system` 的别名（§2.8）。
5. 审计的 `start` 记录写不进就中止命令（§2.11）。
6. SQLite 上为 CLI 预留 [1000, 1023]，最多 24 个并发 CLI 进程（§3.6）。
7. `hetu get` / `range` 默认读 servant，`--master` 时读 master（§2.7）。
8. `--timeout` 默认 30 秒（§2.9）。
9. 名字：`LocalApp` / `hetu/local.py`（原稿暂名 AppClient，容易和 `HeadlessClient`、游戏客户端
   混淆）。
10. 配置定位顺序，含 `start` / `upgrade` 也回落到 `$HETU_CONFIG` 和 `./config.yml`（§2.2）。

## 11. 实现记录（与本稿的出入）

- 租约常量（`WORKER_ID_EXPIRE_SEC` / `FENCE_MARGIN_SEC` / `WORKER_ID_KEY`）与新增的
  `TOOL_NODE_PREFIX` / `TOOL_WORKER_ID_FLOOR` 放在 `hetu/common/snowflake_id.py`（与 `WorkerKeeper`
  基类同处，Redis 与 SQLite 的 keeper 都不必互相 import）；Windows 上"本机已退出进程"的判定挪到
  `hetu/common/helper.py` 的 `lease_owner_exited`，Redis 模块里的 `_owner_exited` 保留为它的别名。
- `UsageError` 定义在 `hetu/cli/base.py`（`pick_instance` 也要用），`console.py` 再导入。
- 新增 `SystemClusters.systems_of(namespace)`（只读副本），供 `hetu call --list` 用，不碰私有表。
- `hetu.local` 另外公开 `build_app_registry`（`--list` 只建簇不连库）与 `check_backend_files`。
- 写集里超过 20 行时的计数键是 `insert_omitted` / `update_omitted` / `delete_omitted`。
- `hetu.i18n` 导入时打的"Use language ..."提示改写 stderr：它在 `import hetu` 时就打印，写 stdout
  会破坏"stdout 恰好一行 JSON"。
- `traceback` 只在退出码 1 时输出（§2.9 已同步）。
