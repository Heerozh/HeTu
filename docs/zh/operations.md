---
title: "运维指南"
description: "生产环境部署、Redis拓扑、负载均衡以及 hetu 命令行工具。"
type: docs
weight: 40
prev: advanced
---

将开发环境投入生产所需的一切：部署选项、Redis 拓扑、反向代理设置以及 CLI 参考。

## 部署选项

### Docker（推荐）

发布的镜像（`heerozh/hetu:latest`，中国大陆用户可使用镜像 `registry.cn-shanghai.aliyuncs.com/heerozh/hetu:latest`）提供了预装 HeTu 的 Python 3.14 环境。您的项目可以扩展此镜像：

```dockerfile
FROM heerozh/hetu:latest
# 或使用此阿里云镜像（适用于上海地域）
# FROM registry.cn-shanghai.aliyuncs.com/heerozh/hetu:latest

WORKDIR /app

COPY . .
RUN pip install .

ENTRYPOINT ["hetu", "start", "--config=./config.yml"]
```

您的项目必须遵循 **src-layout** 以确保 `pip install .` 成功执行（需要 `pyproject.toml` 和 `src/<package>/` 目录）。

构建并运行：

```bash
docker build -t my-game .
docker run -it --rm -p 2466:2466 --name game-srv my-game
```

Docker 工作流的主要目的是让您能够在反向代理后面运行廉价的 Spot 实例：容器可以随时创建或销毁，代理会自动增减它们。

### `pip` 原生安装（无容器）

对于长期运行的专用主机，使用原生 `pip` 安装可以避免约 20% 的容器开销。安装 Python 3.14（通过 `uv`、`conda` 或操作系统包管理器），然后：

```bash
cd your_app_directory
pip install .
# 生产环境建议使用 python -O 以禁用断言（小幅性能提升）
python -O -m hetu start --config=./config.yml
```

## Redis 拓扑

### 主节点 + 只读副本（基准方案）

推荐的起点：一个 Redis 主节点负责写入，多个只读副本用于订阅扇出。[`SubscriptionBroker`](concepts.md#subscriptions) 从任意副本读取，因此增加副本可以线性扩展订阅吞吐量，而无需触碰主节点。

在 HeTu 的配置中，后端声明如下：

```yaml
backends:
  backend_name:
    type: Redis                        # 后端类型
    master: redis://127.0.0.1:6379/0   # 主服务器，唯一地址
    servants: [ ]                       # 只读副本；读取请求在其间随机负载均衡
    # URL 格式：redis://[[username]:password]@host:6379/0  （用户名可以为空）
```

所有 `System` 写入均针对**主节点**执行；ClientSDK 的订阅/查询（以及部分 `System` 读取）则针对**随机副本**执行。这种分离使得单个 HeTu 部署能够承载大量用户，同时保持数据一致性。

**关于主节点和副本**

- 根据需要添加任意数量的 `servants`；每个都是一个从主节点同步的 Redis 只读副本。
- `servants` 是可选的。留空即表示单主模式——适合小型游戏。
- 请保守设置副本的 Redis `client-output-buffer-limit`；过大的缓冲区限制在订阅突发时可能导致 Redis 内存溢出（OOM）。
- 副本需要开启 `notify-keyspace-events`（HeTu 启动时有权限就会自行 `CONFIG SET`，没权限会告警）。
- **复制延迟预算**：订阅通知不带内容，服务端收到后去（代理拓扑下由代理选的）副本重读。每条通知之后、
  订阅生效和 pubsub 断线重连之后，服务端都至少隔 `1/UPDATE_FREQUENCY`（默认 100ms）再读一次，
  所以副本复制延迟只要低于它，客户端最终一定拿到最新值。延迟超过它（副本 CPU 打满、全量同步、
  网络抖动）时，客户端可能残留旧数据直到该行下次变更——订阅推送因此只保证约 99% 的情况是最新的。
  请监控副本的复制延迟（云厂商控制台的"同步延迟"之类），让它远低于 100ms。
- 每个登录连接还订阅 `Connection` 表 `owner == 本用户` 的索引值频道，被顶号时服务器据此主动断开它，RPC 路径上
  不再每次读库；通知丢失时由 `CONNECTION_ALIVE_RECHECK_INTERVAL`（默认 5 秒）兜底重查。断开前先发
  WebSocket close 码 `4001`、原因 `kicked`，客户端可据此提示"账号已在别处登录"，并且不要自动重连（重连会重新
  登录，把对方顶掉）。

**Redis 连接预算**

- 每个工作进程对每台副本（集群模式下每个 Redis 节点）只保持**一条** pub/sub 连接，由它把通知分发给该进程的
  全部 WebSocket 连接；另加一个有上限的短命读写连接池（`max_connections`，默认 64，满了排队）。
  所以每台副本的连接数 ≈ `workers × (1 + max_connections)`，与在线用户数无关。
- 主节点连接数同样 ≈ `workers × (1 + max_connections)`，池由该 worker 的所有并发 System 调用共享。
- Redis 单实例连接数上限约为 10K，按默认值算一个实例可挂 ~150 个 worker。要扩展的是订阅**吞吐量**而不是
  连接数：增加 `servants`，每个 worker 的 pub/sub 连接和读取会随机分摊到各副本上。

### 即插即用的替代方案

相同的 wire 协议意味着您可以替换为：

- **ValKey** — Redis 的开源分支，遵循相同的协议。
- **阿里云 Tair** — 托管的 Redis 兼容服务，支持代理。
- **Redis Proxy** — 用于在多个 Redis 实例前进行透明分片。

### 持久化

在 Redis 端启用 AOF 或 RDB；HeTu 不会替您选择，但它期望后端能够在重启后持续存活。纯易失性配置会在部署之间丢失状态。

### Redis 集群

HeTu 仅使用 Redis 的基本功能（哈希、有序集合、发布/订阅），此外，组件集群的概念正是为服务数据库集群而设计的，因此 Redis 集群无需特殊配置即可工作。对于大多数项目，读写分离已经足够；只有当单个分片的写入吞吐量成为瓶颈时才需要考虑集群。

不过，我们不建议使用原生 Redis 集群，而是推荐使用 Redis Proxy 来实现相同的功能，因为这样更容易管理集群级别的读写分离。

为了将来能轻松迁移，请在设计 `Components` 时考虑分片：

- HeTu 通过计算 `Systems` 在 `Components` 上的重叠来分布数据——这种重叠形成了**System 集群**，并固定到某个分片。
- `Components` 拆分得越细，获得的 System 集群就越多，集群性能扩展也越好。
- 这并不是自动的。在编写代码时，请主动留意**中心 `Components`**——一个被许多不相关的 `Systems` 引用的单个 `Component` 会将所有内容折叠成一个巨大的集群。
- 避免宽泛的“上帝表”。将每种属性拆分到各自的 `Component` 中。
- 在开发过程中使用 `hetu` CLI 检查每个集群的大小，及早发现中心化增长。

## 负载均衡

### Caddy（推荐）

推荐的方案是在 Docker Swarm 中以**控制器模式**运行 `caddy-docker-proxy`。HeTu 容器会携带 Caddy 能够读取的标签，从而自动注册到反向代理池中。当容器停止时，Caddy 会将其移除；容器启动时自动添加。通过 Let's Encrypt 自动提供免费 TLS。

这种方式非常适用于抢占式/Spot 实例：游戏服务器集群不断更新，而代理则保持稳定的客户端访问端点。游戏客户端需要自动重连，使用官方 SDK 即可轻松实现。

如果您不想运行 Swarm，可以直接从自己的编排代码与 Caddy 的管理 API 交互。

*您可以使用 HeTu 服务器的根路径（https://localhost/）作为健康检查端点。*

### 为什么不选择 Nginx

Nginx 也能工作，但其配置语法对于 HeTu 所鼓励的动态增删模式来说，显得冗长且容易出错。Caddy 更适合这种工作负载。

## 数据迁移

每当 `app.py` 的变更导致线上后端无法直接提供服务时，就需要执行迁移：

- **`System` 引用发生变化** — `Systems` 被重新分组到不同的共置集群。
- **`Component` 模式发生变化** — 增加、删除、重命名、修改列类型或索引变更。

两者都通过 `hetu upgrade` 驱动，但行为差别很大。

### 集群重排 — 自动处理

当仅集群分组发生变化时，无需移动行数据。`upgrade` 会将表重命名为新的集群 ID，至此完成——无需审核脚本，也无数据丢失风险。

如果有 [`hetu.headless`](advanced.md#非服务器进程读写表hetuheadless) 进程连着同一个后端，迁移后要重启它们：它们按启动时读到的集群 ID 写表，不重启会把数据写到旧前缀下。

### Schema 变更 — 通过迁移脚本

当 `Component` 的 schema 发生变化时，首次运行 `upgrade` 会在 `<your-app-dir>/maint/migration/` 下生成一个默认迁移脚本——每个 `Component` 对应一个文件，按 schema 哈希版本管理。接下来：

1. **大多数情况 — `upgrade` 自动完成。** 新增列会使用每个属性的默认值填充；可无损转换的类型变更（如 `int32 → int64`）会自动应用。生成的脚本会在同一次 `upgrade` 调用中执行；只需在之后提交该文件，以确保每个环境都以相同方式迁移。

2. **有损情况 — 需要手动干预。** 如果某列被删除或类型变更无法安全转换，脚本的 `prepare()` 会返回 `unsafe`，`upgrade` 拒绝继续执行。两种选择：

    - **编辑生成的脚本。** 常见的情况是“删除 + 添加”实际上是一个重命名——修改脚本的 `upgrade()` 主体，在删除旧列之前将旧列数据复制到新列。
    - **使用 `--drop-data` 强制迁移。** 直接丢弃受影响的属性。请勿在生产环境中使用。

易失组件（`volatile=True`）不走迁移脚本：它的数据每次 `upgrade` 都会被清空，所以 schema 一变，`upgrade` 就直接按新定义重建表，删列、改类型也不需要 `--drop-data`。

将 `maint/migration/` 下的所有内容提交到您的仓库，这样部署环境不会重新生成（并可能偏离）您已经审核过的脚本。

目前不支持降级，未来可能会添加此功能。

## `hetu` CLI

`hetu` 命令（通过 `uv run hetu` 运行）是您的运维入口点：`start`（启动）、`upgrade`（迁移）、`build`
（生成客户端代码），以及调试用的 `call` / `get` / `range` / `shell`（见 [命令行调试](#命令行调试)）。

需要配置的子命令按这个顺序找配置：`--config` > 命令行参数模式（给了 `--app-file` / `--namespace` /
`--db` 之一）> 环境变量 `HETU_CONFIG` > 当前目录的 `config.yml`。官方 Docker 镜像设了
`HETU_CONFIG=/app/config.yml`。配置里 `APP_FILE` 与 SQLite 库文件（`sqlite:///./hetu.db`）的相对路径都按
**配置文件所在目录**解析，与在哪个目录运行无关。

### `hetu start`

服务器可以用**两种互斥模式**启动：从 YAML 配置文件启动，或完全通过 CLI 标志启动。它们不能混合使用——当提供 `--config` 时，所有其他标志将被忽略。

#### 模式 1 — 配置文件（生产环境推荐）

```bash
hetu start --config=./config.yml
```

所有配置从 YAML 文件读取。参见下面的 [配置文件](#configuration-file) 获取最小示例，以及 [`hetu/CONFIG_TEMPLATE.yml`](https://github.com/Heerozh/HeTu/blob/main/hetu/CONFIG_TEMPLATE.yml) 获取完整 schema。

#### 模式 2 — CLI 标志（无配置文件）

用于无需 YAML 的临时启动：

```bash
hetu start --app-file=./app.py --namespace=my_game --instance=server1 \
    --db=redis://127.0.0.1:6379/0 --port=2466
```

此模式下 `--app-file`、`--namespace` 和 `--instance` 是必需的。

| 标志                | 默认值                    | 用途                                                                                             |
|---------------------|---------------------------|--------------------------------------------------------------------------------------------------|
| `--app-file FILE`   | `/app/app.py`             | `app.py` 的路径（包含组件和系统定义）                                                              |
| `--namespace NAME`  | —                         | 要运行的 `app.py` 中的命名空间                                                                     |
| `--instance NAME`   | —                         | 逻辑实例 ID（每个运行进程需要一个唯一 ID，用于雪花算法 worker 分配）                                  |
| `--port PORT`       | `2466`                    | WebSocket 监听端口                                                                                 |
| `--db URL`          | `redis://127.0.0.1:6379/0`| 后端 DSN；scheme 选择后端（`redis://`、`rediss://`、开发用的 `sqlite:///<库文件>`）                  |
| `--workers N`       | `4`                       | Worker 进程数（经验值：`CPU * 1.2`）                                                                |
| `--debug 0/1/2`     | `0`                       | `1` 启用热重载 + 详细日志；`2` 额外启用 Python 协程调试（慢 90%）                                    |
| `--cert DIR`        | `""`                      | TLS 证书目录，或 `auto` 使用自签名证书；通常建议在反向代理处终止 TLS                                  |
| `--authkey KEY`     | `""`                      | 加密层握手签名的认证密钥；留空禁用                                                                  |

运行 `hetu start --help` 获取完整列表。

### `hetu upgrade`

Schema 迁移。在部署修改了 `Component` 形状（增加列、更改数据类型、新索引）的版本之前运行它。它会比较后端当前的 schema 与 `app.py` 中定义的 schema，并应用差异。

与 `hetu start` 类似，它接受配置文件**或**直接 CLI 标志（二选一）：

```bash
# 模式 1 — 使用配置文件
hetu upgrade --config=./config.yml

# 模式 2 — 使用 CLI 标志
hetu upgrade --app-file=./app.py --namespace=my_game --instance=server1 \
    --db=redis://127.0.0.1:6379/0
```

两种模式下都可以使用的额外标志：

- `-y` — 跳过数据备份确认提示（用于 CI/CD）。
- `--drop-data` — 通过丢弃无法迁移的数据强制迁移。**请勿在生产环境中使用。**
- `--no-rebuild-index` — 跳过重建索引（见下）。

如果您不运行 `upgrade`，`hetu start` 在检测到 schema 不匹配时会拒绝启动。

`upgrade` 默认每次都按行数据重建持久 `Component` 的索引，修掉索引残留：比如维护脚本只删了行、没删
索引，服务器日志会报"索引和行数据对不上"，读到这一行的 `System` 一直重试。重建要**停服**执行（扫描行
与覆盖索引之间的写入会丢），每个索引建好后才原子替换旧索引，中途失败旧索引原样保留。数据量大时重建较慢，
可以用 `--no-rebuild-index` 跳过。

`upgrade` 开始前会检查有没有服务器还在运行（Redis 后端看 worker 租约），有就直接退出（退出码 1），什么都
不动：迁移、清空易失表、重建索引在服务器运行时执行都会写坏数据。服务器是异常退出的，等租约过期（最多 60
秒）后再试。Windows 上本机已经退出的服务器不用等：Sanic 在 Windows 上停 worker 是硬杀，租约来不及释放，
所以这种租约不算。SQLite 后端的服务器没有租约、检查不出来，请自己确认已经停服。`hetu call` / `hetu shell`
也持有租约（Redis 与 SQLite 都是），它们在跑时 `upgrade` 同样拒绝执行，提示里会单独列出。

### `hetu build`

根据服务器端的 `Component` 定义生成客户端 SDK 代码（带类型的 C# 类）。每当 `Component` 发生更改时运行一次，并将输出提交到客户端项目中。与 `start` 和 `upgrade` 不同，此命令没有配置文件模式——仅支持 CLI 标志：

```bash
hetu build --app-file=./app.py --namespace=my_game \
    --output=../client/Generated/Components.cs
```

`--namespace` 和 `--output` 是必需的；`--app-file` 默认为 `/app/app.py`。

这样可以保持客户端和服务器端 schema 同步，而无需手写样板代码。

## 命令行调试

`hetu call` / `get` / `range` / `shell` 在**本进程里直连配置好的后端**，以 admin 或指定玩家身份调用
System、查看组件数据，不经过服务器、不需要开服（SQLite 开发库也能用）。写入走与 System 相同的提交路径，
在线客户端照常收到订阅推送。能跑它们的人手里本来就有数据库地址和口令，所以不给服务器加任何新的网络入口。

```bash
hetu call --list                          # 全部 System：参数、权限、引用的组件、文档第一行
hetu get --list                           # 组件、字段、索引
hetu call add_gold 1001 500               # ADMIN 级 System，默认以 admin 身份
hetu call --as 1001 buy_item 3 2          # USER 级 System 必须给玩家 id
hetu call --as 1001 --group gm gm_kick 2002
hetu call send_mail --args-file args.json # 参数从 JSON 数组文件读（- 为 stdin）
hetu call add_gold 1001 500 --dry-run     # 执行但不提交，看它会写什么
hetu get Player owner=1001                # 按 id 或带索引的字段读一行
hetu range Item owner 1001 --limit 50     # 省略右界即精确匹配；( / [ 前缀表示开 / 闭区间
hetu shell -c "await call_system('add_gold', 1001, 500); show(await get('Player', owner=1001))"
```

- **输出**：`call` / `get` / `range` / `--list` 的 stdout 恰好一行 JSON，日志和 app 里的 `print` 都在
  stderr。`call` 给出 System 的原始返回值 `result`、客户端实际会收到的 `client`（序列化不了时给
  `wire_error`）、竞态重试次数、这次调用的写集 `writes`、`target`（配置文件、实例、口令打码后的后端
  地址）。失败时给 `error_type`、`error`，代码出错时还有完整 `traceback`。提交途中被打断（超时等）的
  那次在 `writes` 里 `committed` 为 `"unknown"`：可能已经生效，重跑前先查数据。
- **退出码**：0 成功；1 代码或调用失败（System 抛异常、超时）；2 用法错误（参数、身份、名字不存在）；
  3 环境未就绪（找不到配置、库文件或表不存在、表结构与代码不一致、写保护拒绝）。
- **身份**：没给 `--as` / `--group` 时按 System 的权限推断：ADMIN、GM 与内部 System（`permission=None`）
  用 admin，EVERYBODY 用 guest，**USER 必须 `--as`**（线上 USER 端点不放行未登录连接，用 0 号身份跑会在库里
  建出 owner=0 的行）。给了 `--as` 时 group 默认 guest。没有连接：`ctx.connection_id` 为 0、`ctx.request`
  为 None，调 `elevate` 的 System 会失败；登录时写进 `ctx.user_data` 的数据用 `--user-data` 手动给。
- **参数**：每个参数先按 JSON 解析，解析不了当字符串；形参注解为 `str` 的不解析。看起来像 JSON 却解析失败
  的直接报错（PowerShell 常把引号吃掉），这时改用 `--args-file` 或 stdin。以 `-` 开头的非数字参数放在
  `--` 后面。
- **读**：`get` / `range` 不加载 app，schema 取自库里的表 meta，app 代码改坏了也能看数据；默认读副本，
  刚写完马上读用 `--master`。`range` 默认最多 10 行，输出 `truncated` 表示是否还有更多。
- **表结构**：`call` / `shell` 不建表、不迁移；用到的表在库里不存在、或与本地代码的结构 / 簇不一致时直接
  报错，先启动一次服务器（建新表）或 `hetu upgrade`。
- **`hetu shell`**：预置与测试用 `Sandbox` 同名的 `call_system` / `get` / `must_get` / `insert` /
  `upsert`，以及 `app.range` 与 `show()`（按 JSON 打印，numpy 行带字段名），支持顶层 await。代码来自
  `-c`、脚本文件或 stdin；都没给时进交互模式。脚本按 `__main__` 执行（有 `__file__`）；交互模式下
  Ctrl+C 只中断正在跑的语句，Ctrl+D 退出。改了代码要重开 shell。

**跑的是本地代码**：`hetu call` 每次都重新 import 本地的 app，不代表服务器已经加载了同样的代码——
单 worker 的服务器不会自动重载，验证客户端那条路径要重启服务器。生产上请在部署好的容器里跑
（`docker exec <容器> hetu call ...`），别从开发机的代码直接写生产库：表结构校验拦不住逻辑上的差异。
另外：System 里 `create_task` 出去的后台任务在命令退出时会被取消；`on_server_setup` 里挂的初始化不会执行；
经 CLI 创建的 FutureCall 由在跑的服务器执行。

**写保护与审计**：配置项 `CLI_ALLOW_WRITE`（默认 true）设为 false 后，CLI 只能读或 `--dry-run`，真写入会
报错（退出码 3）；`DEBUG` 关闭的配置上真写入会给出警告。`call` 与 `shell` 的每次运行都追加记录到
`CLI_AUDIT_LOG`（默认配置目录下的 `logs/hetu_cli_audit.jsonl`）：谁、在哪台机器、什么命令和参数（数据库
口令打码）、每次提交改了哪些行、结果。审计写在运行 CLI 的机器上，服务器那边没有痕迹。

## 配置文件

完整 schema 请参见 [`hetu/CONFIG_TEMPLATE.yml`](https://github.com/Heerozh/HeTu/blob/main/hetu/CONFIG_TEMPLATE.yml)。

具体定义在文档注释中有详细说明。

## 下一步

- **[API 参考](api/)** — 在 `app.py` 中会接触到的所有公开符号。
- [README](https://github.com/Heerozh/HeTu/blob/main/README.md) 包含性能基准测试（中文），适用于容量规划场景。
