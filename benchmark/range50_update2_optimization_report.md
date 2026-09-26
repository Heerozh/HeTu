**`benchmark_range50_update2` 吞吐优化报告**

日期：2026-09-26；分支：`perf/range_benchmark`；引擎基线：`23f4c53667b3e885f146bccc05d2436b0c174f9c`（`dev`）。本报告针对当前代码的 `IntTable.number` 范围读和两行 `name` 更新，包含真实 WebSocket、jsonb → zlib(level=1) → crypto、事务提交。

四批交替对照的吞吐中位数 **842.1 → 1,068.2 RPC/s，提升 26.9%**；每 RPC 服务端 CPU **1,111.5 → 875.0 μs，减少 21.3%**。两版均使用同一份修正后的业务 RPC、客户端和数据填充脚本；收益仅比较引擎实现。

| 指标 | 旧引擎 | 优化后 |
|---|---:|---:|
| 六个窗口 QPS 中位数 | 842.1 | 1,068.2 |
| QPS 最小–最大 | 835.8–847.6 | 1,058.4–1,074.5 |
| 每 RPC 服务端 CPU，中位数 | 1,111.5 μs | 875.0 μs |
| 服务端单核 CPU，中位数 | 93.6% | 93.4% |
| 采样延迟 p50 | 35.61 ms | 27.55 ms |
| 采样延迟 p99 | 163.85 ms | 42.59 ms |
| 计时窗口成功 RPC / 重试 | 50,511 / 936 | 64,055 / 1,197 |

| 执行顺序，每批 3×10 秒 | 三个窗口 RPC/s | 批次中位数 |
|---|---|---:|
| 旧引擎 A | 845.4 / 835.8 / 847.6 | 845.4 |
| 优化 A | 1,069.3 / 1,071.2 / 1,074.5 | 1,071.2 |
| 优化 B | 1,058.4 / 1,065.1 / 1,067.0 | 1,065.1 |
| 旧引擎 B | 838.2 / 841.2 / 842.9 | 841.2 |

全部优化窗口高于全部旧引擎窗口。两版重试次数/成功请求分别约 1.85% / 1.87%。延迟每 64 次请求采样一次，分别得到 763 / 966 个样本；p99 受冲突后的随机退避影响，只作为本次观察值。逐窗口 CPU、延迟、亲和性、数据校验和采样摘要保存在 [range50_update2_results.json](range50_update2_results.json)。原始日志和 speedscope 文件在本机 `/tmp/hetu-range50-pr-results/`，临时目录可能被清理。

新压测路径原本不能可靠测量 50 读 / 2 写：现有填充工具只写 `IntTable` 行 hash，缺少 `number` 索引；随机窄区间可能少于两行；随机 `name` 与原值相同时 `repo.update()` 会拒绝空修改。现补齐 30,000 行及其全部索引，查询下界最多到第 29,951 行、上界至少比下界大 49，服务端要求恰好返回 50 行；若新旧 `name` 相同则改为另一个值。`number` 保持不变，查询始终读满 50 行。每轮重建隔离压测表，结束后全量验证 30,000 行、`id` 和 `number` 索引；成功请求（含建连探测和预热）×2 必须等于全表版本增量。没有失败请求。

引擎改动分两轮：

1. `IdentityMap.add_clean` 首批多行数据直接建立工作缓存，原值快照一次整批复制，避免先建空数组再 `np.append` 及逐行复制；输入、返回行、缓存和提交校验使用的原值仍相互隔离。`SessionRepository._range_rows` 对已知 dtype 的记录列表直接构造结构化数组，替代通用 `np.stack`。
2. Redis `get_many(STRUCT)` 把同一批原始行一次构造成结构化数组，减少每行的数组分配和视图创建；保留 bytes 原样、UTF-8 容错、顺序、缺失行 `None` 及重复 ID 的独立行视图。单行、RAW、TYPED_DICT 仍走原解码路径。

修复后的 5 秒冒烟约 1,093 RPC/s，仅用于确认路径跑通，不计入正式收益。基线和最终版各做一次 py-spy 0.4.2、19 Hz、`--nonblocking`、25 秒负载采样；仅保留含 WebSocket 请求处理的栈。带 profiler 的吞吐不计入对照。

| 请求栈占比 | 旧引擎 | 优化后 |
|---|---:|---:|
| NumPy 叶子帧 | 26.1% | 17.8% |
| IdentityMap 叶子帧 | 12.0% | 6.3% |
| redis-py / hiredis 叶子帧 | 22.3% | 28.5% |
| `add_clean` 调用栈包含率 | 11.6% | 1.8% |
| `_range_rows` 调用栈包含率 | 65.7% | 56.8% |
| `_range_checks` 调用栈包含率 | 8.0% | 11.7% |

基线 / 优化后总采样为 562 / 563，筛选出的请求样本为 525 / 505。前三项叶子分类互不重叠；后三项包含下游调用，不可相加。其他路径相对占比上升不表示其绝对耗时增加；样本用于热点排序。

测试使用 Intel Core Ultra X7 358H：P 核 0–3。单 Sanic worker 固定 CPU 3，独立 Redis 容器固定 CPU 2（host network、loopback、无持久化），两个客户端各固定 CPU 0/1，每进程 16 个 call-response 连接。`platform_profile=performance`，服务端 P 核 EPP=performance，governor 报 powersave。Python 3.14.7、NumPy 2.5.3、redis-py 8.1.0、hiredis 3.4.2、Sanic 25.12.1、websockets 17.1；Redis 镜像 ID `sha256:718f745deb7dfefeac6eed7041fc7ec9476b50e61b247932682457c41adafa0e`。每批重新启动服务、重建数据，4 秒预热后测 3×10 秒，窗口间隔 1 秒持续负载；正式对照不启用 profiler。

验证：共享引擎改动的完整 Redis 测试为 **646 passed，5 skipped**；补回的批量缓存、范围返回值与字段边界定向测试为 **16 passed，1 xfailed**。当前 `IntTable` 负载的真实 RPC 冒烟及四批正式对照均通过全量数据和两行更新核对。新增压测 runner / 种子脚本的 Ruff 与 basedpyright 均通过，修改文件格式和 `git diff --check` 通过。

剩余开销主要是一次索引 ZRANGE、50 次流水线 HGETALL 的 redis-py 命令处理和响应解析，以及提交前对 50 行的版本、索引和区间防幻读检查；本轮未改变这些一致性语义，也未增加 master 读或发布命令。仍未优化 Redis Lua 提交、WebSocket 管道或跨请求批处理。直接调用 `get_many` 并长期只保留返回批次中的单行，会使该行引用继续持有整批 NumPy 数组；各行可独立修改。结果仅代表本机单 worker、单 Redis、32 连接、固定小行和已填充索引，不能外推到多 worker、远程 Redis 或真实副本延迟。

复现命令，在仓库根目录执行；使用独立 Redis，匹配上述镜像及依赖版本：

```bash
range_base=$(mktemp -d /tmp/hetu-range50-base-XXXXXX)
range_results=$(mktemp -d /tmp/hetu-range50-results-XXXXXX)
git archive 23f4c53667b3e885f146bccc05d2436b0c174f9c | tar -x -C "$range_base"
cp benchmark/server/app.py "$range_base/benchmark/server/app.py"
cp benchmark/ya_hetu_rpc.py "$range_base/benchmark/ya_hetu_rpc.py"
docker run -d --rm --name hetu-range50-repro --cpuset-cpus 2 --network host \
  redis:latest --bind 127.0.0.1 --port 16389 --save '' --appendonly no
for range_run in base-a final-a final-b base-b; do
  case "$range_run" in
    base-*) range_server="$range_base" ;;
    final-*) range_server="$PWD" ;;
  esac
  taskset -c 0,1 .venv/bin/python benchmark/rpc_throughput.py \
    --workload range50_update2 --server-root "$range_server" \
    --client-root "$range_base" --output "$range_results/$range_run" \
    --server-cpu 3 --client-cpus 0,1 --processes 2 --connections 16 \
    --rounds 3 --seconds 10 --warmup 4
done
docker stop hetu-range50-repro
```
