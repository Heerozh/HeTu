# Redis 上"发一条通知"的成本：别在热写路径上随手加 PUBLISH

## 背景

HeTu 的每个事务最终在 Redis **master** 上执行一次 `commit_v2.lua`（EVALSHA）：
Phase 1 做乐观锁检查（`VER` 等），Phase 2 执行写入（`HSET`/`ZADD`/...），
Phase 3 对 payload 里带的每一条通知执行 `redis.call("PUBLISH", ...)`。

读可以分给副本，写不能，所以 **master 主线程每次 commit 花多少 CPU，直接决定整个集群的写吞吐
上限**。每次 commit 的 master 开销涨 x%，master 打满时写吞吐大约掉 x/(1+x)。

这份数据的起因：`perf/row-cache` 分支（efea8ef）让 commit 给**每个改动行**额外 PUBLISH 一条
行频道通知（带新版本号），替代原来由 Redis 自动发的 keyspace 通知。生产压测写吞吐下降约 18%。
下面拆开看这 18% 从哪来、PUBLISH 为什么贵、换成 SPUBLISH 或 keyspace 有没有用。

## 结论

- **commit 里每多一条 `redis.call("PUBLISH")`，master 多花约 0.9~1.3 万条指令（约 0.45~0.6 µs）。**
  单行 commit（main 形态）本身也就 6.7~8.5 万条指令、3.4~4.0 µs，所以"每行一条 PUBLISH"就是
  **每次 commit +14%~19%**，副本越多越贵。master 主要跑 commit 时（读走副本 / 读写分离），
  这一项就能让写吞吐掉 12%~16%。
- **贵的不是"发布"本身**（查订阅者字典只要几百条指令），而是：
  1. `redis.call` 的命令调度：参数转 Redis 对象、查命令表、ACL、`call()` 统计、回复转回 Lua，
     约 3k 指令，跟调一次 `EXISTS` 一样；
  2. 复制：非 cluster 模式下 PUBLISH 会被 `forceCommandPropagation(c, PROPAGATE_REPL)`
     **强制写进复制流**，每条约 1.5k 指令（还不含内核里写副本 socket 的开销），副本越多越贵；
  3. 把频道名、消息经 msgpack payload 带进 Lua：解包建表、建字符串、遍历、GC，约 6k 指令
     （commit 级实测的每行增量减去单次 PUBLISH 的成本推算得出）。
- **SPUBLISH 不是解药。** 非 cluster 模式下 `spublishCommand` 和 `publishCommand` 只差一个
  `sharded` 参数，同样被强制复制，实测成本一样。它只解决原生 cluster 模式下 PUBLISH 经
  cluster bus 广播到所有节点的问题。
- **keyspace 通知便宜得多**：它由写命令的 C 代码顺手发出，不走命令调度，**也不进复制流**
  （每个副本应用写入时自己生成）。但它不带内容，而且并非免费：
  - Redis ≥ 8.8：实例上一个订阅者都没有时直接跳过（≈0），有任何订阅者时约 2.2k 指令；
  - Redis ≤ 8.6（含 7.x、阿里云 7.0）：没有这个快速路径，只要开了对应事件类，
    **每次写都付全价**（7.4 上约 2.9k 指令），有没有人订阅都一样。

## 规则

1. **不要在 commit 的 Lua（或任何热写路径）里"按行"PUBLISH。** 按事务、按表合并成一条。
2. 需要通知时的优先顺序：
   1. 复用已经在发的消息，往里加字段（比如在表频道消息里带上版本号），不新增 PUBLISH；
   2. 只需要"有变化"信号的，用 keyspace 通知，并且只在副本上开（HeTu 的 `configure_servant`
      只配 servant）。它在副本上生成，不占 master；
   3. 每事务 / 每表一条 PUBLISH；
   4. 实在要每行一条：消息内容从已有的 push 里取（行 key、`_version` 本来就在
      `["HSET", key, "_version", ver, ...]` 里），不要在 payload 里另塞一份；也不要在 Lua 里
      拼字符串，拼一个 key 约 4.5k 指令，比 PUBLISH 本身还贵。按下面的拆解数据**估算**，
      这样每行能从约 10.7k 指令降到约 4.5k（未实测）。
3. **别指望 SPUBLISH 降成本。**
4. **keyspace flag 按需开。** Redis < 8.8 上 master 开了 `h`/`g` 这类高频事件，每次 HSET/DEL 都要付
   约 2.9k 指令。`configure_servant` 只加 flag、不减 flag，改了目标 flag 之后旧 flag 要手动清理；
   云厂商实例的参数通常对 master 和副本一起生效，也要留意。
5. **改了 commit 路径就跑一次 `redis_commit_cost.py replay`**，把新形态加成一个 payload 变体，
   跟现状比较每次 commit 的主线程 µs。

## 数据

所有数字都是一台笔记本上测的，**看相对值**，绝对值不代表生产。环境与方法见文末。

### 1. commit 级：`redis_commit_cost.py replay`

用 pipeline 批量回放与 HeTu 同构的 `commit_v2.lua` 调用，把 master 主线程压满，比较三种
payload：

- **old**：VER + HSET，无 PUBLISH（f9b6493 时的形态）
- **main**：+ 每张表一条表级频道 PUBLISH（dev 当前形态）
- **head**：+ 每行一条行频道 PUBLISH（`perf/row-cache` efea8ef 的形态）

rows=1 对应 `get_then_update` 的一次 commit，rows=2 对应 `get2_update2`。
格式为"每次 commit 主线程 µs（master 用户态指令数）"，百分比是 head 比 main。

Redis 8.10.0，master 绑 P 核：

| rows | 副本数 | old | main | head | head 比 main |
|---:|---:|---:|---:|---:|---:|
| 1 | 0 | 2.85（58.0k） | 3.37（67.3k） | 3.83（76.3k） | +13.9% |
| 1 | 1 | 3.12（60.7k） | 3.78（72.4k） | 4.35（83.1k） | +15.1% |
| 1 | 3 | 3.30（62.3k） | 4.05（74.4k） | 4.64（85.3k） | +14.7% |
| 2 | 0 | 4.46（86.4k） | 5.51（106.4k） | 6.30（123.4k） | +14.4% |
| 2 | 1 | 4.95（92.7k） | 6.20（115.7k） | 7.20（136.1k） | +16.2% |
| 2 | 3 | 5.27（94.7k） | 6.53（116.5k） | 7.77（139.5k） | +19.0% |

- main 比 old 多出来的就是表级 PUBLISH，一样贵：rows=1 +18%~23%，rows=2 +23%~25%。
- 每多一条 PUBLISH：无副本约 0.47 µs，1 个副本约 0.57 µs，3 个副本约 0.6 µs（rows=2 时
  head 比 main 多 0.79 → 1.00 → 1.24 µs）。
- 8.10 上 head 用 `Kghz` 或 `Kz` 结果一样（差 < 0.5%），因为测试实例上没有订阅者，keyspace
  通知走了快速路径。

Redis 7.4.11，挂 1 个副本（只看指令数），同时对比 master 上的 keyspace 配置：

| rows | old | main（Kghz） | head（Kz） | head（Kghz） |
|---:|---:|---:|---:|---:|
| 1 | 70.5k | 85.0k | 95.5k（+12.3%） | 98.4k（+15.7%） |
| 2 | 110.7k | 138.5k | 158.3k（+14.3%） | 164.0k（+18.4%） |

7.x 没有"无订阅者跳过"，`Kghz` 的 `h` 事件每行固定多约 2.9k 指令。升级后 master 仍留着
`Kghz`（`configure_servant` 只加不减）时，head 同时付 keyspace 和 PUBLISH 两份，
rows=2 达到 +18.4%。

### 2. 单次操作拆解：`redis_publish_cost.py`

每次 EVALSHA 在 Lua 里循环 200 次同一种操作，减去只拼 key、不调用命令的空循环，
得到每次操作的纯成本（master 用户态指令）。Redis 8.10.2 / 7.4.11，不加载模块，固定在一个核上；
每组 3 轮平均，同一配置下各轮指令数几乎相同。

| 每次操作 | 8.10 无副本 | 8.10 1 个副本 | 7.4 无副本 |
|:--|--:|--:|--:|
| `redis.call("EXISTS")`（对照：`redis.call` 本身的开销） | 2,943 | 2,944 | 3,727 |
| `redis.call("PUBLISH")` | 2,796 | 4,335 | 3,896 |
| 同上，实例上挂着一个无关订阅者 | 3,078 | 4,613 | 4,133 |
| `redis.call("SPUBLISH")` | 2,955 | 4,492 | 3,365 |
| `redis.call("HSET")`，keyspace 通知关闭 | 4,319 | 6,189 | 5,195 |
| HSET 触发的 keyspace 通知，实例上无订阅者 | +12 | +12 | **+2,895** |
| HSET 触发的 keyspace 通知，实例上有订阅者 | +2,233 | +2,233 | +3,157 |
| 对照：Lua 里拼一个 key（空循环本身） | 4,490 | 4,490 | 3,951 |

- PUBLISH 跟 EXISTS 差不多：成本几乎全是 `redis.call` 的调度，发布动作本身只占几百条指令。
- 挂上副本后 PUBLISH / SPUBLISH 都多了约 1.5k（强制复制）。keyspace 通知不变，因为它不复制。
  HSET 多的约 1.9k 是正常写复制，任何方案都要付。
- SPUBLISH 和 PUBLISH 的差别在 ±15% 以内（sharded 通道不做 pattern 匹配，7.4 上略省），
  但一样要复制，不改变结论。
- 7.4 没有"无订阅者跳过"：开了 `h` 事件后，没人订阅也要 +2.9k。

### 3. 端到端：HeTu 服务器 A/B

HeTu 8 个 worker，`perf/row-cache` 的分叉点 cd3b980（main 形态）对比该分支 efea8ef（head 形态）。
同一个 Redis 8.10，挂 1 个副本，**master 同时负责读**（单节点部署）。
数值为每次 RPC 的用户态开销，两轮平均，两轮之间相差 1%~2%。

| | get_then_update | get2_update2 | get（对照） |
|:--|--:|--:|--:|
| master 指令 | 102.1k → 112.7k（+10.4%） | 173.7k → 195.5k（+12.6%） | +0.6% |
| master 周期 | +6.0% | +9.1% | 0% |
| 副本周期 | +13% | +16% | – |
| HeTu worker 周期 | +1.6% | +2.5% | 0% |
| 复制流字节 | 213 → 287 B | 400 → 548 B | – |

- master 每次 RPC 多出来的指令数（每行约 10.6k），跟第 1 节测出的每行 PUBLISH 成本基本一致；
  读路径（get）没有变化。
- 单节点部署时 master 大部分时间花在读和逐条收发网络请求上，同样的增量只占 +6%~9% 的周期。
  master 越是只跑 commit（读写分离），占比越接近第 1 节的 +14%~19%。
- 副本也要执行复制过来的每一条 PUBLISH，读走副本的部署里这部分会挤占副本的读能力。

### 4. 源码依据（Redis 8.10.0）

`src/pubsub.c`：

```c
void publishCommand(client *c) {
    ...
    int receivers = pubsubPublishMessageAndPropagateToCluster(c->argv[1],c->argv[2],0);
    if (!server.cluster_enabled)
        forceCommandPropagation(c,PROPAGATE_REPL);
    addReplyLongLong(c,receivers);
}

void spublishCommand(client *c) {
    int receivers = pubsubPublishMessageAndPropagateToCluster(c->argv[1],c->argv[2],1);
    if (!server.cluster_enabled)
        forceCommandPropagation(c,PROPAGATE_REPL);
    addReplyLongLong(c,receivers);
}
```

`src/notify.c` 的 `notifyKeyspaceEventImpl`：

```c
    /* If notifications for this class of events are off, return ASAP. */
    if (!(server.notify_keyspace_events & type)) return;

    /* If there are no Pub/Sub subscribers (neither pattern nor channel),
     * skip the remaining notification work since nobody would receive it. */
    if (dictSize(server.pubsub_patterns) == 0 && kvstoreSize(server.pubsub_channels) == 0)
        return;
```

第二个判断在 7.0.15、7.4.0、8.0.0、8.2.0、8.4.0、8.6.0 的源码里都没有，8.8.0 才加入。

## 测试环境与方法

- 机器：笔记本 Intel Core Ultra X7 358H（4 P 核 + 8 E 核 + 4 LP E 核），Linux 7.2（CachyOS），
  Docker host 网络。Redis 8.10.0（第 1、3 节）/ 8.10.2（第 2 节），均为 `redis:latest`；
  另有 7.4.11（`redis:7-alpine`）。均关闭 RDB/AOF。
- 电源：`power-saver` 下 `intel_lpmd` 会把所有进程（含容器）限制在 4 个 LP E 核上。
  第 1、3 节测试期间切到 `performance`：master 绑 P 核（CPU 3，4.8 GHz），副本绑其他 P 核，
  压测客户端和 HeTu worker 绑 E 核。第 2 节只看指令数，在 `power-saver` 下把 Redis 绑在一个
  LP E 核上跑，所以不列 µs。
- 指标：
  - 第 1 节的 µs 用 `INFO` 的 `used_cpu_*_main_thread` 算，master 被压满、频率稳定，
    两轮重复相差 < 1%。
  - 第 3 节端到端时 P 核频率随整机功耗在 3.35~4.32 GHz 之间漂，CPU 时间不可比，
    所以改用 `perf stat -e instructions:u,cycles:u -p <pid>` 的用户态指令/周期。
  - `perf_event_paranoid=2` 下只能数用户态，内核里的 socket 读写（包括写副本）没算，
    真实开销比表中略高。
- 大小核上用 `perf stat -p` 时，进程如果在两类核之间迁移，`cpu_core`/`cpu_atom` 两个计数器
  会各自按运行比例被放大，直接相加会重复计数。最好把 Redis 绑到同一类核上（`--cpuset-cpus`）；
  `redis_publish_cost.py` 也会按输出里的百分比把计数还原后再相加。
- `redis:latest` 镜像默认加载 search/ReJSON/timeseries/bf 等模块，每条命令约多 75 条指令，
  可以忽略。第 2 节用 `--entrypoint redis-server` 启动，不加载模块。

## 复现

在 `benchmark/` 目录下，对一个没有业务流量的 Redis 运行：

```bash
# commit 级：old / main / head 三种 payload
uv run python redis_commit_cost.py replay --url redis://127.0.0.1:6379/0

# 单次操作拆解；Linux 下加 --perf-pid 得到与频率无关的指令数
# （redis 进程需与当前用户同 uid，或放宽 perf_event_paranoid）
uv run python redis_publish_cost.py --url redis://127.0.0.1:6379/0 --perf-pid <redis-server pid>

# 带副本的对比：再起一个 redis-server --replicaof <master> 后重跑上面两条
```
