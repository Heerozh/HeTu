# benchmark_get2_update2 优化报告

日期：2026-09-26（初次采样始于 09-25）。优化提交：`28963cf`。

分支：`perf/get2-update2`。基线：`c20f95300c94c46fd44f7e34a3261147325054b2`。

本次优化的是 `benchmark/ya_hetu_rpc.py::benchmark_get2_update2` 对应的 `exchange_data` 完整 RPC：两次唯一索引查询、读取两行、修改两行、原子提交，包含真实 WebSocket 和 jsonb → zlib(level=1) → crypto 管道。


正式 A/B/B/A 对照中，吞吐中位数 **1,699 → 2,160 RPC/s，提升 27.1%**；每请求服务端 CPU **548.91 → 430.39 μs，减少 21.6%**。主要收益来自减少 NumPy 在单行事务中的数组构造和筛选开销。

| 指标 | 基线 | 优化后 |
|---|---:|---:|
| 6 个窗口 QPS 中位数 | 1,699.1 | 2,160.2 |
| QPS 最小–最大 | 1,660.6–1,717.1 | 2,156.3–2,178.4 |
| 每 RPC 服务端 CPU（窗口中位数） | 548.91 μs | 430.39 μs |
| 采样延迟 p50 | 18.80 ms | 14.86 ms |
| 采样延迟 p99 | 20.78 ms | 16.27 ms |
| 成功 RPC / 事务重试次数 | 101,730 / 30 | 129,833 / 45 |

延迟为各版本六个窗口合并后的采样分位数，分别有 1,562 / 1,984 个样本；重试次数/成功 RPC 约为 0.0295% / 0.0347%。未发生请求失败，行数和索引基数验证均通过。

| 按执行顺序分批（每批 3×10 秒） | 三个窗口 QPS | 中位数 |
|---|---|---:|
| 基线 A | 1708.9 / 1701.9 / 1717.1 | 1708.9 |
| 优化 A | 2178.4 / 2170.0 / 2158.2 | 2170.0 |
| 优化 B | 2162.2 / 2158.2 / 2156.3 | 2158.2 |
| 基线 B | 1696.3 / 1660.6 / 1688.2 | 1688.2 |

两组优化结果都高于全部基线窗口；这是本机短时对照的观察结果，并非跨硬件或生产环境的保证。

## 改动

1. `SessionRepository._range_rows`：只读取一行时，直接把 record 加入 IdentityMap，并直接复制为单元素 recarray 返回，省掉两次 `np.stack`，同时使用现有的首行缓存快路径，省掉空数组创建和 `np.append`。调用方返回行、工作缓存、提交校验使用的原值副本仍彼此独立。
2. `IdentityMap.get_dirty_rows`：单行缓存直接根据已知行状态分派，省去 `np.isin` 和数组复制；多行缓存只筛选一次所有脏行，再分 INSERT / UPDATE / DELETE，继续用 NumPy 排除大量 CLEAN 行。字段序列化、bytes 原样处理、变更字段检测、改回原值时不发空更新均保留。

读节点仍由 `master_or_servant` 选择；同一索引的查 ID 与读行仍落在同一节点。没有新增 master 查询、发布通知或放松事务一致性检查。业务 RPC、客户端协议和冻结客户端源码均未改变。

## 测量条件

- Intel Core Ultra X7 358H；Linux 7.2.6-1-cachyos，P 核由 `/sys/bus/event_source/devices/cpu_core/cpus` 确认为 0–3。
- 单 Sanic worker 固定 CPU 3；独立 Redis 容器固定 CPU 2，host network，仅绑定 loopback；两个客户端进程固定 CPU 0/1，每进程 16 连接，一问一答。
- `platform_profile=performance`、EPP=performance；governor 名称为 powersave（如实记录，不在任务中修改系统电源配置）。
- Python 3.14.7，NumPy 2.5.3，redis-py 8.1.0，hiredis 3.4.2，Sanic 25.12.1，Redis 8.10.2。
- 每次服务启动后，完整填充 IntTable 的 30,000 个 number 值、StrTable 全部 36³=46,656 个 name 值及各自索引，确保 upsert 已有行；版本从 1 开始。压测后检查行数、索引基数和抽样版本增长。
- 客户端始终从冻结的基线源码加载；固定每进程随机种子，但并发调度和执行请求数量仍随速度变化。没有通过改客户端来抬高服务端性能。
- 正式运行不启用 profiler；建连、填充和 5 秒预热不计入窗口。每批 3×10 秒，窗口间隔 1 秒持续负载。顺序为基线 A → 优化 A → 优化 B → 基线 B。服务端 CPU 来自 psutil 进程 CPU 时间计数；延迟每 64 次采样一次。
- 初期使用 Docker 端口映射的探测结果（约 1,630 RPC/s）不计入最终对照；正式对照两边均使用 host network。


## 迭代与采样证据

第一轮只处理单行读取构造，探索批次中位数 1,921.5 RPC/s；第二轮加入脏行提取优化后为 2,159.1 RPC/s。探索批次用于选择方向，未并入正式对照，也不据此独立分摊每项最终收益。之后重新采样，再用上面的 A/B/B/A 做确认。

py-spy 0.4.2，19 Hz、`--nonblocking`，40 秒负载窗口、5 秒预热；采样器固定 CPU 0，服务端仍为 CPU 3。采样运行包含启动与预热，仅选取含 `client_handler` / `websocket_connection` 的请求栈：基线 764 个、优化后 757 个。总采样为 858 / 874 个，工具报告 1 / 3 次采样错误。**带 profiler 的 QPS 不用于收益计算**。

| 请求栈占比 | 基线 | 优化后 |
|---|---:|---:|
| NumPy 叶子帧 | 30.9% | 16.4% |
| IdentityMap 自身叶子帧 | 10.2% | 9.8% |
| redis-py / hiredis 叶子帧 | 21.1% | 22.2% |
| HeTu Redis backend 叶子帧 | 9.7% | 11.6% |
| `get_dirty_rows` 调用栈包含率 | 12.3% | 5.5% |
| `_range_rows` 调用栈包含率 | 49.7% | 42.8% |

前四行互不重叠；后两行是调用栈包含率，会包含下游函数，不能相加。样本量只适合排序热点，不是精确 CPU 归因；其他模块相对占比上升也不代表其绝对开销增加。

`get_dirty_rows` 另做了同一 P 核的函数级补充测量（5 批中位数，单位 μs/调用）：

| 缓存行 / 脏行 | 基线 | 优化后 |
|---|---:|---:|
| 1 / 1 | 19.29 | 7.54 |
| 1000 / 1 | 89.20 | 41.42 |
| 1000 / 1000 | 7124.46 | 6641.99 |
| 1000 / 0 | 69.06 | 23.48 |

这些是内存函数计时，不代表端到端吞吐；用于检查此次重写没有把成本转嫁给大量 CLEAN 行或批量更新场景。

## 验证

- 完整 Redis：`HETU_TEST_BACKENDS=redis .venv/bin/pytest tests/ -q -n 8`：**619 passed, 3 skipped**。
- Redis + SQLite 的 IdentityMap、Session、竞态、字段类型、master 读与发布约束：**181 passed, 1 xfailed**。
- 新增回归覆盖单行索引查询返回值与缓存、原值的相互隔离，以及混合行状态、更新后改回原值。
- 核心两文件与新填充工具的 basedpyright：0 errors / 0 warnings；新增/修改压测工具 Ruff 通过；所有修改 Python 文件格式检查及 `git diff --check` 通过。
- 全仓 Ruff 有 300 项既有诊断，与冻结基线逐项对比无新增。全仓 basedpyright 基线为 177 errors / 1 warning；新增填充工具曾有一项 Mapping 标注错误，修复后针对该文件复查通过，其余诊断与基线一致。没有把全仓静态检查描述为通过。

## 复现与原始记录

逐窗口吞吐、测试配置和采样摘要见上文。完整原始计量、客户端延迟、server/client 日志和 py-spy speedscope 文件留在本机 `/tmp/hetu-get2-results/`（临时目录可能被系统清理）。

以下命令在仓库根目录执行，使用本次 `.venv`；每次生成新的输出目录。Redis 仅用于压测，测试结束会停止该容器。

```bash
perf_base=$(mktemp -d /tmp/hetu-get2-base-XXXXXX)
perf_results=$(mktemp -d /tmp/hetu-get2-results-XXXXXX)
git archive c20f95300c94c46fd44f7e34a3261147325054b2 | tar -x -C "$perf_base"

docker run -d --rm --name hetu-get2-repro --cpuset-cpus 2 --network host \
  redis:latest --bind 127.0.0.1 --port 16389 --save '' --appendonly no

for run in base-a final-a final-b base-b; do
  case "$run" in
    base-*) perf_server="$perf_base" ;;
    final-*) perf_server="$PWD" ;;
  esac
  taskset -c 0,1 .venv/bin/python benchmark/rpc_throughput.py \
    --workload get2_update2 --server-root "$perf_server" \
    --client-root "$perf_base" --output "$perf_results/$run" \
    --processes 2 --connections 16 --server-cpu 3 --client-cpus 0,1 \
    --rounds 3 --seconds 10 --warmup 5
done

docker stop hetu-get2-repro
```

`redis:latest` 本次实际为 Redis 8.10.2，镜像 ID 为 `sha256:718f745deb7dfefeac6eed7041fc7ec9476b50e61b247932682457c41adafa0e`；跨时间复现应匹配这一版本与依赖。另启新输出目录，加 `--profile --profile-cpu 0 --rounds 1 --seconds 40` 可采样。

## 尚未优化与范围限制

- 每次成功且需要更新的 RPC 通常仍有两次 ZRANGE、两次 HGETALL 和一次提交 Lua 往返；副本分摊读负载和提交版本检查维持原状。
- redis-py 连接池、发送、响应解析、超时与重试占据较大份额；本轮未绕过库的连接生命周期或故障恢复语义。
- 剩余 NumPy record 字段访问、版本/索引校验和序列化仍有成本；样本已经分散到多个较小步骤，本轮停止继续做零碎改写。
- 未优化 WebSocket、加密压缩、Redis Lua 提交脚本、索引结构、跨请求批处理或业务逻辑。
- 正式数字仅代表单 worker、本机 Redis、32 并发连接、固定小行的稳定更新阶段。没有据此外推多 worker 集群、远程网络、真实副本滞后、冷启动插入或高冲突负载。
