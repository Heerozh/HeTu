# 订阅预算测量结果（sub_budget.py / sub_budget_ws.py）

回答的问题：预估 N 人在线时，每人给多少订阅合适？这和数据更新频率是什么关系？

结论先行：**订阅数本身几乎不要钱，要钱的是"交付"**——一行被写一次 × 订阅了这行的连接数。
预算要按"每秒交付数"来定，而不是按"每人几行"。索引订阅分两种：点查询（`owner=me`、`zone=z`）
只在自己那个值有行进出时才被叫醒，可以随便用；区间查询会被该索引任何值的变动叫醒，
放在热索引上要按订阅者数付费。

## 环境

- Windows 11，9950X3D（32 线程），Redis 8.10.1 跑在 Docker Desktop（WSL2），master + 1 replica
- Python 3.14，redis-py 8.1.0 + hiredis 3.4.1，HeTu `perf/redis-rtt` @ 05b0747
- Docker on Windows 的网络栈让每次 Redis 往返都偏慢，Linux 裸机上 Redis 侧数字预计好 2~3 倍；
  Python 侧数字受影响较小

## 成本模型

一次写入 → Redis 通知（行/整索引是 keyspace 事件，表级和索引值频道是 commit 主动 PUBLISH）→
每个 worker 一条 pubsub 连接（`PubSubHub`）收到后按频道表塞进本进程内各连接的本地队列 →
连接按 `UPDATE_FREQUENCY`（10Hz）tick 合批 → 本 tick 变更的行按表一次 `get_many` 重读 → 推给客户端。

| 单位事件 | worker CPU | 副本 CPU | 说明 |
|---|---|---|---|
| **交付** 1 行更新给 1 个连接 | ~100µs（3k/秒时）<br>~65µs（1 万/秒以上，get_many 摊薄）<br>含 ws+jsonb+zlib+crypto 推送后 ~100µs | ~17µs | 主开销。同一订阅同 tick 的多行合成一帧推送 |
| **通知** 1 条 Redis 消息（含被合批吞掉的） | ~12µs/worker | ~12µs | 本 worker 只解析一次再分发；高频写同一行时客户端只拿到 ≤6 次/秒，多出来的通知只付这个钱 |
| **索引重查** 1 次（区间订阅或点查询被叫醒） | ~180µs | ~30µs | ZRANGE 比对 + 进出行的读取/推送/行频道增删 |
| 静态：1 个 (频道, 订阅) 对 | ~2.8KB | ~200B/(频道, worker) | 每连接 RSS ~48KB（含 50 行订阅）；订阅 50 行的 range 约 7ms 建立；Redis 侧频道按 worker 去重 |

合批效果：`UPDATE_FREQUENCY=10` 但 `get_message` 的窗口实际是 1~2 个 interval，
实测同一行对同一连接**最高约 6 次/秒**交付（20 行 × 90Hz 写入 → 每 (行,连接) 6 次/秒）。
延迟下限也来自这里：写→客户端 p50 ~150ms、健康时 p99 ~220ms。

索引订阅被叫醒的规则：
- **点查询**（`right` 省略或 `left == right`）订 `{prefix}:index:{name}:{值token}` 频道，
  只有该值上有行插入/删除、或某行该字段变成/不再是该值时才重查一次；
- **区间查询**订整个索引的 keyspace 频道，该索引上任何值的写入都会叫醒它。tick 合批把它压到
  每个订阅者最多 ~6~10 次重查/秒，所以热索引上每个区间订阅者的上限约 1.8ms CPU/秒。

## 天花板（本机）

| 资源 | 100% 时 | 建议预算（≈60%，p99 延迟 < 300ms） |
|---|---|---|
| 1 个 worker 事件循环 | ~16k 交付/秒（进程内）<br>~11k 交付/秒（含 ws 推送） | **~6k 交付/秒** |
| 1 个 Redis 副本 | ~55k 交付/秒（按 17µs/交付外推，本机未打满） | **~30k 交付/秒**，即约 5 个满载 worker |
| Redis master | ~78µs/commit → 1 核 ~12k commit/秒 | 与订阅无关，按写入量算 |
| Redis 连接数 | workers × (每节点 1 条 pubsub + 读池上限 `max_connections`，默认 64) | 与在线人数无关 |

超过 worker 预算的表现：交付率下降（合批吞掉）、写→客户端延迟从 ~150ms 涨到秒级、
积压年龄（最老未处理通知）超过 1s；超过 `DROP_AFTER`（2 分钟）的通知会被丢弃。

## 预算公式

记：N 在线人数、S 每人订阅行数、F 每行写频率（Hz，有效值 ≤6）、K_r 订阅了行 r 的连接数（AOI 同屏人数）。

```
D = Σ_rows min(F_r, 6) × K_r  ≈ N × S × F                      # 交付/秒（主开销）
X = Σ_rows max(F_r - 6, 0) × min(K_r, workers)                  # 被合批吞掉的通知/秒（只有热行才有）
R = 区间订阅数 × min(该索引写频率, 10) + Σ_值 该值写频率 × 订阅该值的连接数   # 索引重查/秒

worker 核数 ≈ (D × 100µs + X × 12µs + R × 180µs) / 1e6 / 0.6
副本个数    ≈ D / 30k
```

反过来给每人定预算：`每人每秒可收的行更新 ≈ 6000 × workers / N`，再拆成 S × F。

### 算例

- 32 worker、N=1000：每人 192 行更新/秒 → S=50 行 × F=4Hz，或 S=100 × 2Hz。
- N=1000、S=50、F=1Hz（如 1Hz 位置同步）：D=50k/秒 → 约 9 个 worker 核、2 个副本。
- 同上但 F=5Hz：D=250k/秒 → ~42 核 + 9 副本。高频位置同步要么降 S（AOI 半径）、降 F
  （服务端节流/插值），要么改走非订阅通道。
- 同屏 M 人互相可见（每人都订阅这 M 行）：D = M² × F。M=100、F=1Hz 就是 1 万交付/秒 = 1 个核跑满，
  按 60% 预算要 2 个 worker。
- 背包 `range(Item, owner=me)`（点查询）：别人捡道具不打扰你，每次自己背包变化 1 次重查（~180µs），
  可忽略。同样的需求若写成区间查询，全服拾取 ≥10 次/秒后每个在线玩家固定付 ~1.8ms CPU/秒，
  1000 人就是 1.8 核纯空转。
- AOI 跨区：一次跨区叫醒旧 zone + 新 zone 各 K 个连接各重查一次，R = 2K × 跨区次数/秒。

## 实测数据

### 进程内（sub_budget.py，不含 ws）

K = 扇出（每次写入通知的连接数）= conns/zones，每连接点查询订阅自己 zone 的 50 行。

| tag | conns×procs | K | 写/秒 | 应交付/秒 | 交付/秒 | 交付率 | worker核/进程 | µs/交付 | 副本核 | p50 ms | p99 ms |
|---|---|---|---|---|---|---|---|---|---|---|---|
| K10_W300 | 200×1 | 10 | 297 | 2969 | 2870 | 0.97 | 0.29 | 100 | 0.08 | 156 | 218 |
| K10_W500 | 200×1 | 10 | 492 | 4923 | 4660 | 0.95 | 0.44 | 94 | 0.11 | 156 | 223 |
| K10_W1000 | 200×1 | 10 | 1010 | 10098 | 8765 | 0.87 | 0.62 | 70 | 0.16 | 149 | 221 |
| K10_W2000 | 200×1 | 10 | 1974 | 19740 | 15528 | 0.79 | 0.99 | 64 | 0.25 | 189 | 372 |
| K10_hot20_W2000（20 行 90Hz） | 200×1 | 10 | 1880 | 18796 | 1083 | 0.06 | 0.34 | — | 0.16 | 8* | 46* |
| P4_K10_W2400 | 800×4 | 10 | 2408 | 24076 | 22008 | 0.91 | 0.57 | 104 | 0.37 | 153 | 221 |
| K10_W300_move10（10% 写改 zone，30 次/秒） | 200×1 | 10 | 298 | 2976 | 2851 | 0.96 | 0.41 | 143 | 0.12 | 156 | 3077** |
| K10_W300_move50（50% 写改 zone，150 次/秒） | 200×1 | 10 | 298 | 2976 | 2939 | 0.99 | 0.58 | 196 | 0.17 | 146 | 6801** |

\* 热行的 ts 被后续写入覆盖，延迟测的是"最后一次写→交付"，偏小。
\*\* 基准模型的假象：zone 满员（= limit）时新进入的行被 limit 截掉，等有人离开才补进来，带着旧 ts。

- 成本按交付数算：写得少扇出大和写得多扇出小，只要应交付数一样开销就一样。
- 通知成本：hot20 组 1.88 万通知/秒只交付 1083，CPU 0.34 核 → 每条通知 ≈12µs（本 worker 只解析一次）。
- 跨区：一次跨区只叫醒旧 zone + 新 zone 的 2K=20 个连接，通知数每次 2 条；move50 比基线多出的 0.29 核
  是真实工作：每个连接每 tick 一次重查（约 1600 次/秒）加 1621 行/秒的进出推送。
- 静态：Redis 连接数 = 5（1 条 pubsub + 读池 + 写进程），与连接数无关；每连接 RSS 48KB；
  订阅建立 7ms/连接；副本 pubsub 频道数按 worker 去重。

### 端到端（sub_budget_ws.py，真实 hetu 服务器 1 worker + jsonb/zlib/crypto ws 客户端）

| tag | conns | 应交付/秒 | 交付/秒 | 交付率 | 行/帧 | server 核 | µs/交付 | p50 ms | p99 ms |
|---|---|---|---|---|---|---|---|---|---|
| ws_K10_W500 | 120 | 4925 | 4436 | 0.90 | 3.8 | 0.45 | 100 | 152 | 210 |
| ws_K10_W800 | 120 | 7839 | 6656 | 0.85 | 5.2 | 0.59 | 88 | 145 | 208 |
| ws_K10_W1500 | 120 | 14788 | 11324 | 0.77 | 4.4 | 0.98 | 87 | 165 | 295 |

同一订阅同 tick 的多行合成一帧，ws + pipeline 的成本按帧摊，行越密每行摊得越少。

## 注意事项

1. 区间查询仍是整索引广播：热索引上每个区间订阅者最多 ~10 次重查/秒，随订阅者数线性增长。
   高频跨区的区间 AOI 要么改点查询，要么按地图/区拆组件缩小订阅者数。
2. 点查询的值频道由 commit 发出，老版本的写入方（老 worker、老 headless 进程）不发，混跑时
   新 worker 上的点订阅会漏通知，写入方要一起升级；`maint.py` 的离线维护路径不发表级/值频道通知。
3. Windows 下 Sanic 强制 `WindowsSelectorEventLoopPolicy`，`select()` 上限 512 fd，
   单 worker 约 150~200 个连接就崩（"too many file descriptors in select()"）。Linux 无此问题。
4. Redis 副本单线程，按 17µs/交付约 55k 交付/秒到顶，多副本靠 `servants` 随机分摊；
   写入侧 master 每 commit 会多发每个索引字段一条值频道 PUBLISH，无订阅者时是一次 dict 查找。

## 复现

```bash
# 专用 Redis（会 FLUSHALL）
docker network create hetu_bench_net
docker run -d --name hetu_bench_redis --network hetu_bench_net --hostname redis-master -p 23400:6379 redis:latest
docker run -d --name hetu_bench_redis_replica --network hetu_bench_net -p 23401:6379 redis:latest \
    redis-server --replicaof redis-master 6379 --replica-read-only yes

cd benchmark
uv run python sub_budget.py --conns 200 --zones 20 --limit 50 --writes 300 --duration 15
uv run python sub_budget.py --conns 800 --zones 80 --limit 50 --procs 4 --writes 2400 --writers 4
uv run python sub_budget.py --conns 200 --zones 20 --limit 50 --hot-rows 20 --writes 2000
uv run python sub_budget.py --conns 200 --zones 20 --limit 50 --writes 300 --move-ratio 0.1
uv run python sub_budget_ws.py --conns 120 --zones 12 --limit 50 --writes 500 --duration 15
```

Windows 下 `asyncio.sleep` 粒度约 15ms，每个写协程最多 ~60 写/秒，要更高写入量请加 `--writer-coroutines`。
Linux 上建议用 `sub_budget_ws.py --workers N` 直接测多 worker。
