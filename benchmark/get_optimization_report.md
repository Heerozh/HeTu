# benchmark_get 吞吐优化记录

日期：2026-09-25。分支：`perf/get-throughput`。基线提交：`d75c33fbed65580712ffa3f629bf544ff75a7907`。压测校验、缓存快路径、行解码、P 核压测默认值分别提交为 `74ea63d`、`3a01afe`、`612cd54`、`19b7122`。

`benchmark_get()` 发送 `just_get(id)`，服务端经 System Session 和 `repo.get(id=...)` 从 Redis 读一整行。原实现每次命中新事务的第一行，都建立空 `recarray` 再 `np.append`；Redis 的整行解码还先复制默认行、逐字段写入。现在第一行直接建立独立的工作缓存和原值副本，整行一次构造为结构化记录。读仍由 `master_or_servant` 选择节点，事务缓存、更新检测和读空观察语义不变。

## 测量

| 版本 | 6 个窗口 QPS 中位数 | 每请求服务端 CPU | p50 | p99 |
|---|---:|---:|---:|---:|
| 基线 | 9,139 | 99.47 μs | 7.045 ms | 8.417 ms |
| 修改后 | 11,127 | 81.21 μs | 5.813 ms | 6.515 ms |

吞吐提升 **21.8%**，每请求服务端 CPU 减少 **18.4%**。按「基线 A → 修改 A → 修改 B → 基线 B」顺序，各批 3 × 6 秒；基线两批中位数 8,807 / 9,340 QPS，修改后 11,283 / 10,992 QPS。基线六窗口范围 8,698–9,408，修改后 10,860–11,328 QPS。拆开测量时，单独缓存快路径约 10,852 QPS，单独解码优化约 9,574 QPS（各 2 × 6 秒）；这些短批次只用于判断贡献，不与上表合并。

本机 `/sys/bus/event_source/devices/cpu_core/cpus` 报告 P 核为 0–3，`platform_profile=performance`、`energy_performance_preference=performance`。单 Sanic worker 绑定 P 核 CPU 3，四个独立客户端进程分别绑定 E 核 CPU 4、6、8、10，每进程 16 条连接，一问一答。Redis 容器限制在 P 核 CPU 2；loopback、`jsonb → zlib(level=1) → crypto`、独立 Redis 实例、30,000 个已填充的 `IntTable` 行。客户端始终从冻结的基线源码加载。建连、填充和预热不计入窗口。服务端 CPU 时间来自 psutil 进程计数。

最初把测试行写入了错误的 `{CLU0}`，导致几乎全是 Redis 读空；实际 `IntTable` 位于 `{CLU2}`。这批无效数据已丢弃。正式工具会从服务端 Component 簇定义计算正确的簇号，并在服务启动清空 volatile 表后填充行；`just_get` 读空时直接失败，防止吞吐数字再次被读空污染。填充只覆盖主键读取所需的行 hash，没有建立 `number` 的辅助索引，因此这组结果只代表 `get(id=...)`。

## py-spy 热点与后续试验

基线和修改后各采样一次，每次 20 秒正式运行、5 秒预热；py-spy 以 19 Hz 运行并绑定 P 核 CPU 1，服务端仍绑定 CPU 3。仅保留含 `client_handler` 或 `websocket_connection` 的请求栈，基线 433 个样本、修改后 363 个样本。下表的叶子帧分类互不重叠；`add_clean` 为调用栈包含率，会与其下的 NumPy 样本重叠。采样只用于找热点，**带 profiler 的 QPS 不用于计算上面的吞吐提升**。

| 请求栈样本占比 | 基线 | 修改后 |
|---|---:|---:|
| NumPy 叶子帧 | 15.2% | 4.1% |
| `IdentityMap.add_clean` 调用栈包含率 | 16.6% | 10.7% |
| redis-py / hiredis 叶子帧 | 22.6% | 26.4% |
| WebSocket / 消息管道叶子帧 | 17.1% | 22.6% |
| `IdentityMap` 自身叶子帧 | 6.0% | 10.2% |

NumPy 份额下降与第一行缓存快路径一致；Redis 和 WebSocket 的相对份额随其他开销减少而上升，不表示它们绝对变慢。样本量有限，百分比仅用于热路径排序。

测过但未保留的尝试：成功提交后跳过 `SystemCaller` 再次 `discard()`，第一组对照约 +2%，反序组没有稳定收益；另一种单行 NumPy 分配方式的中位数提升不到 1%，p99 反而变差。两项均已撤回。独立 P 核顺序读测试中，小表 `HMGET` 约 58.6 μs/次，`HGETALL` 约 56.0 μs/次，故未替换 Redis 命令。

剩余主要开销在 redis-py 的连接、发送、解析和重试（修改后请求栈叶子帧约 26%），以及 Sanic WebSocket 收发和消息管道（约 23%）。保留 redis-py 的连接池与重试语义、原有消息协议和一问一答模式。`IdentityMap` 自身约 10% 的叶子帧仍可进一步研究，但本轮更小的构造改写没有稳定收益。未测试多 worker、远程网络、真实副本延迟或不同大小的 Component 行。

复现：先准备独立 Redis、基线代码和相同的读空校验，再交替运行：

```bash
docker run -d --rm --name hetu-get-bench -p 127.0.0.1:16389:6379 redis:latest
docker update --cpuset-cpus 2 hetu-get-bench
mkdir -p /tmp/hetu-get-base
git archive d75c33fbed65580712ffa3f629bf544ff75a7907 | tar -x -C /tmp/hetu-get-base
cp benchmark/server/app.py /tmp/hetu-get-base/benchmark/server/app.py

.venv/bin/python benchmark/rpc_throughput.py \
  --workload get --server-root /tmp/hetu-get-base --client-root /tmp/hetu-get-base \
  --output /tmp/get-base-a --rounds 3 --seconds 6 --warmup 3 \
  --server-cpu 3 --client-cpus 4,6,8,10

.venv/bin/python benchmark/rpc_throughput.py \
  --workload get --server-root "$PWD" --client-root /tmp/hetu-get-base \
  --output /tmp/get-final-a --rounds 3 --seconds 6 --warmup 3 \
  --server-cpu 3 --client-cpus 4,6,8,10
```

每次使用新的 `--output` 目录；换顺序再测一批。`hello_world` 仍是工具默认负载。结果限于单 worker、本机网络、固定小行、无副本延迟的环境，不能直接推算多 worker 或远程 Redis 的吞吐。

## 验证

- `HETU_TEST_BACKENDS=redis .venv/bin/pytest tests/test_component_define.py tests/test_backend_idmap.py tests/test_backend_client.py tests/test_backend_session_basic.py tests/test_backend_session_race.py tests/test_backend_sub.py -q`：186 通过。
- `HETU_TEST_BACKENDS=sqlite .venv/bin/pytest tests/test_backend_client.py tests/test_backend_session_basic.py -q`：64 通过、4 跳过。
- `HETU_TEST_BACKENDS=redis .venv/bin/pytest tests/test_arch_master_reads.py tests/test_master_read_budget.py -q`：5 通过，主节点读路由预算未回退。
- 原 `hello_world` 工具模式冒烟运行通过；修改过的核心模块与压测工具定向类型检查 0 错误。
- 修改文件格式检查与 `git diff --check` 通过。Ruff 对修改文件报 30 个既有诊断，与基线数量一致，没有新增诊断；`benchmark/server/app.py` 的两个 dtype 注解是既有 basedpyright 解析错误。
