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

**Redis 连接预算**

- 副本连接数 ≈ HeTu ClientSDK 连接数（在线用户数）。
- 主节点连接数 ≈ `workers × 每个 worker 的并发 System 调用数`。
- Redis 单实例连接数上限约为 10K。如果您的并发在线用户数接近此值，请通过增加更多 `servants` 来扩展。

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

### Schema 变更 — 通过迁移脚本

当 `Component` 的 schema 发生变化时，首次运行 `upgrade` 会在 `<your-app-dir>/maint/migration/` 下生成一个默认迁移脚本——每个 `Component` 对应一个文件，按 schema 哈希版本管理。接下来：

1. **大多数情况 — `upgrade` 自动完成。** 新增列会使用每个属性的默认值填充；可无损转换的类型变更（如 `int32 → int64`）会自动应用。生成的脚本会在同一次 `upgrade` 调用中执行；只需在之后提交该文件，以确保每个环境都以相同方式迁移。

2. **有损情况 — 需要手动干预。** 如果某列被删除或类型变更无法安全转换，脚本的 `prepare()` 会返回 `unsafe`，`upgrade` 拒绝继续执行。两种选择：

    - **编辑生成的脚本。** 常见的情况是“删除 + 添加”实际上是一个重命名——修改脚本的 `upgrade()` 主体，在删除旧列之前将旧列数据复制到新列。
    - **使用 `--drop-data` 强制迁移。** 直接丢弃受影响的属性。请勿在生产环境中使用。

将 `maint/migration/` 下的所有内容提交到您的仓库，这样部署环境不会重新生成（并可能偏离）您已经审核过的脚本。

目前不支持降级，未来可能会添加此功能。

## `hetu` CLI

`hetu` 命令（通过 `uv run hetu` 运行）是您的运维入口点。三个子命令：

### `hetu start`

服务器可以用**两种互斥模式**启动：从 YAML 配置文件启动，或完全通过 CLI 标志启动。它们不能混合使用——当提供 `--config` 时，所有其他标志将被忽略。

#### 模式 1 — 配置文件（生产环境推荐）

```bash
hetu start --config=./config.yml
```

所有配置从 YAML 文件读取。参见下面的 [配置文件](#configuration-file) 获取最小示例，以及 [`CONFIG_TEMPLATE.yml`](https://github.com/Heerozh/HeTu/blob/main/CONFIG_TEMPLATE.yml) 获取完整 schema。

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
| `--db URL`          | `redis://127.0.0.1:6379/0`| 后端 DSN；scheme 选择后端（`redis://`、`sqlite:///`、`postgresql://`、`mysql://`等）                |
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

如果您不运行 `upgrade`，`hetu start` 在检测到 schema 不匹配时会拒绝启动。

### `hetu build`

根据服务器端的 `Component` 定义生成客户端 SDK 代码（带类型的 C# 类）。每当 `Component` 发生更改时运行一次，并将输出提交到客户端项目中。与 `start` 和 `upgrade` 不同，此命令没有配置文件模式——仅支持 CLI 标志：

```bash
hetu build --app-file=./app.py --namespace=my_game \
    --output=../client/Generated/Components.cs
```

`--namespace` 和 `--output` 是必需的；`--app-file` 默认为 `/app/app.py`。

这样可以保持客户端和服务器端 schema 同步，而无需手写样板代码。

## 配置文件

完整 schema 请参见 [`CONFIG_TEMPLATE.yml`](https://github.com/Heerozh/HeTu/blob/main/CONFIG_TEMPLATE.yml)。

具体定义在文档注释中有详细说明。

## 下一步

- **[API 参考](api/)** — 在 `app.py` 中会接触到的所有公开符号。
- [README](https://github.com/Heerozh/HeTu/blob/main/README.md) 包含性能基准测试（中文），适用于容量规划场景。
