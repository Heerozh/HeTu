# hello_world 吞吐优化报告

日期：2026-09-25。分支：`codex/hello-world-throughput`。基线：`461e67278c2e52249c46d58dd51c108d240d88c3`。

**性能模式、固定同一 P 核实测：吞吐从 30,647 提升到 41,517 QPS，增加 35.5%；每请求服务端 CPU 成本从 30.22 降到 22.09 μs，减少 26.9%。**

正式数据来自切换性能模式后的整套重新测量。服务端始终固定到 P 核 CPU 3，客户端分别固定到 E 核 CPU 4、6、8、10；每次运行都通过 psutil 核验实际亲和性，并记录运行前后的电源策略，所有正式运行均一致。基线和最终版本各测两个批次，每批 5 × 10 秒；下表为窗口中位数，中间轮次各测 3 个窗口。

| 版本 | 窗口数 | QPS | 相对基线 | CPU μs/RPC | p50 ms | p99 ms |
|---|---:|---:|---:|---:|---:|---:|
| 基线 | 10 | 30,647 | +0.0% | 30.22 | 1.980 | 2.717 |
| 第 1 轮：日志按需构造 | 3 | 32,091 | +4.7% | 28.86 | 1.885 | 2.695 |
| 第 2 轮：加密实现替换 | 3 | 37,282 | +21.6% | 24.68 | 1.618 | 2.434 |
| 第 3 轮（最终）：流式接收 | 10 | 41,517 | +35.5% | 22.09 | 1.463 | 2.155 |

顺序：基线 A → 第 1 轮 → 第 2 轮 → 最终 A → 基线 B → 最终 B。基线 A/B 的批次中位数为 30,340 / 30,899 QPS；最终 A/B 为 41,419 / 41,614 QPS。基线全部窗口范围 29,585–31,010，最终范围 40,547–42,248 QPS。服务端 CPU 利用率约 92%，各客户端进程约 35–47%，压测端有余量。

主要收益来自加密实现替换（相对第 1 轮 +16.2%）和接收调度（相对第 2 轮再 +11.4%）；日志优化幅度较小。

[正式原始数据](results/hello_world_20260925/measurements.json)、[汇总](results/hello_world_20260925/summary.json)、[环境与检查结果](results/hello_world_20260925/environment.json)。前轮 CPU 4（E 核）的结果单独存于 [previous_e_core](results/hello_world_20260925/previous_e_core/measurements.json)，未与此次 P 核结果混算；当时未记录电源模式，约 +30.8% 的旧结果不作为本报告主结论。

保留的三轮修改：

1. **跳过未启用日志的构造。** Endpoint debug 日志与收发 replay 日志按实际启用级别判断，关闭时不再翻译、格式化或字符串化参数。相关文件：`hetu/endpoint/executor.py`、`hetu/server/receiver.py`、`hetu/server/websocket.py`。
2. **替换逐包加密执行实现。** 每连接复用一个 `cryptography.ChaCha20Poly1305` 对象，代替每包进入 PyNaCl 的 Python/CFFI 包装。握手仍使用原有 Curve25519 + Blake2b；方向前缀、88 位递增计数、16 字节认证标签和线协议一致；认证失败保持 `nacl.exceptions.CryptoError` 接口。新增直接依赖 `cryptography>=45.0.0`，锁文件使用 50.0.1。相关文件：`hetu/server/pipeline/crypto.py`。
3. **减少每包接收调度。** 使用 Sanic 的公开 `recv_streaming()` 完整收集一条消息后，再执行原有解密/解压/反序列化，避免普通 `recv()` 的额外 Task 和 `asyncio.wait`。单帧直接取出，多帧拼接，每次耗尽分片迭代器。发送队列、回复顺序、权限、守卫和限流保持原实现。相关文件：`hetu/server/receiver.py`。已核对依赖最低版本 [Sanic 25.3.0 源码](https://github.com/sanic-org/sanic/blob/v25.3.0/sanic/server/websockets/impl.py)，该公开接口已存在。

`py-spy` 复核（性能模式、同一 P 核）：基线与最终版本分别采到 567 / 560 个样本，其中筛选出 365 / 365 个收发调用栈样本。筛选包含 `client_handler` 或 `websocket_connection`，剔除初始化、握手与清理；包含预热阶段。下表是该子集的 **inclusive 占比**，父子调用重叠，不能相加或当作整进程 CPU 占比；采样只用于定位热点，不用于计算吞吐提升。

| 调用路径 | 基线 | 最终 |
|---|---:|---:|
| 加解密层（含底层调用） | 24.9% | 12.9% |
| PyNaCl AEAD 包装 | 20.5% | 0.0% |
| asyncio.wait | 13.4% | 0.0% |
| Sanic recv_streaming | 0.0% | 23.0% |
| Sanic send | 10.7% | 10.4% |
| zlib 编解码 | 4.7% | 7.7% |
| JSONB 编解码 | 1.4% | 1.9% |

PyNaCl 逐包包装和接收分支的 `asyncio.wait` 已从热点栈消失。流式接收内部的消息组装与 Queue、加密原生调用、WebSocket 发送仍占较大份额；JSONB 占比较小，因此没有继续改其复制路径。

原始 Speedscope 文件：[基线](results/hello_world_20260925/baseline.speedscope.json)、[最终](results/hello_world_20260925/final.speedscope.json)；[基线统计](results/hello_world_20260925/baseline_profile_summary.json)、[最终统计](results/hello_world_20260925/final_profile_summary.json)。

测试条件：

- Linux 7.2.6、Intel Core Ultra X7 358H、Python 3.14.7；单 Sanic worker。内核 PMU 列出的 P 核为 0–3，E 核为 4–15；CPU 3 为单独物理核心。
- `platform_profile=performance`、`energy_performance_preference=performance`；驱动 `intel_pstate`，其 governor 字段为 `powersave`。记录值见环境及每批原始结果。
- 4 个独立客户端进程，每进程 16 个连接，共 64 连接，一问一答。使用原 `benchmark_hello_world()` 与 `connection()`；客户端源码始终固定在基线快照，所有响应均验证为“世界收到”。
- 完整 `jsonb → zlib(level=1) → crypto` 管道；独立 Redis 容器，收发限流关闭、replay ERROR、普通日志 INFO。
- 建连/握手不计时，统一预热 5 秒，每窗口 10 秒，窗口间继续运行 1 秒。延迟每连接每 64 次调用抽样一次；表中 p50/p99 是每窗口采样分位数的中位数。CPU 成本由 psutil 的进程 CPU 时间计算，不是网络往返耗时。
- Sanic 25.12.1、websockets 17.1、PyNaCl 1.6.2、cryptography 50.0.1、msgspec 0.21.1、uvloop 0.22.1、py-spy 0.4.2。
- 正式吞吐测量不启用 profiler。采样另跑，py-spy 19 Hz、nonblocking，采样器固定 CPU 12，避免与服务端争用同一核心。探索阶段的高频采样显著干扰过吞吐，因此带 profiler 的 QPS 均不用于提升计算。

验证命令：

```bash
HETU_TEST_BACKENDS=redis .venv/bin/pytest tests/test_pipeline.py tests/test_endpoint_connection.py tests/test_endpoint_rls.py tests/test_system_guards.py tests/test_websocket.py -q
```

**112 项通过。** 新增覆盖旧 PyNaCl 逐字节互通（双方向、连续 nonce、空包至 64 KiB）、错误密钥/方向、nonce 溢出、真实 WebSocket 分片及后续消息、半包断线后 Connection 行清理、累计消息大小限制。既有篡改、重放、非法帧、踢人、订阅回复顺序和关服清理用例也通过。测试输出包含依赖弃用等警告。

全仓 `ruff check .` 仍有 301 项问题，`basedpyright` 仍有 176 项错误和 1 项警告；已与基线逐项比较，无新增诊断。修改文件格式检查、`git diff --check` 通过；新增压测工具以及修改的接收/加密模块定向类型检查通过。

尚未优化：

- **框架内的帧处理与调度。** 仍有帧解析、掩码、消息组装、streaming 内部 Queue 分配、发送和事件循环开销；未替换 Sanic 或接入私有协议接口。
- **zlib 压缩与解压。** 保留流式压缩和压缩率统计；没有通过关闭压缩或加密获得吞吐提升。
- **发送队列。** 它承担订阅与 RPC 排序、背压及 Future 占位，未引入绕过队列直发的复杂路径。
- **小热点。** JSONB 中间复制、Endpoint 定义查找和零碎对象分配占比较小，按“只优化大头”未改。
- **数据库与业务负载。** Redis CRUD、System 事务、冲突重试和订阅扇出不在本次无事务 hello_world 路径的优化范围内。

边界：这是固定单 P 核、loopback、小包、64 连接的比较；未测多 worker 扩展、真实网络、Windows 或大消息吞吐。不能把 +35.5% 直接乘到历史 64 核数据或所有业务 RPC 上。新增原生依赖需随正常依赖安装发布。

复现方式（Linux，已安装项目开发依赖；按机器实际 P/E 核编号调整显式绑核参数）：

```bash
mkdir -p /tmp/hetu-rpc-base
git archive 461e67278c2e52249c46d58dd51c108d240d88c3 | tar -x -C /tmp/hetu-rpc-base
docker run -d --rm --name hetu-rpc-bench -p 127.0.0.1:16389:6379 redis:latest

.venv/bin/python benchmark/rpc_throughput.py \
  --server-root /tmp/hetu-rpc-base --client-root /tmp/hetu-rpc-base \
  --output /tmp/rpc-baseline-a --rounds 5 --seconds 10 --warmup 5 \
  --server-cpu 3 --client-cpus 4,6,8,10

.venv/bin/python benchmark/rpc_throughput.py \
  --server-root "$PWD" --client-root /tmp/hetu-rpc-base \
  --output /tmp/rpc-final-a --rounds 5 --seconds 10 --warmup 5 \
  --server-cpu 3 --client-cpus 4,6,8,10

# 更换 output 目录交替重跑；每个 output 必须是新目录。
# 单独采样时加 --profile --profile-cpu 12，输出 Speedscope 格式 profile.json。
docker stop hetu-rpc-bench
```

工具：[rpc_throughput.py](rpc_throughput.py)。保留真实网络和原压测函数，新增固定客户端版本、多进程调度、统一测量窗口、CPU 亲和性核验、电源策略记录和 profiler 支持。未缓存 hello_world 响应，也未改为批量请求。
