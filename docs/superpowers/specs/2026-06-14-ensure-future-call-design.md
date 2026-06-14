# 等幂 keyed 未来调用（ensure_future_call / cancel_future_call）— 设计稿

- 日期：2026-06-14
- 状态：已与用户确认设计，待写实现计划
- 影响范围：服务端 `hetu/system/future.py`（新增两个 System + 抽取共享 helper）；
  `docs/api/system.md` / `docs/api/_index.md`（API 文档）；
  `tests/test_system_future.py`（测试）。**不涉及客户端、不涉及 schema 迁移。**

## 1. 背景与目标

用户在河图上需要"后台 background task"：开服后**循环执行某些 System**（如世界 tick、
全服结算、定时广播、每日重置）。

语义经确认为：**全局每周期一次**（整集群每周期只跑一次，需事务化、可靠重试、重启不丢）
—— 这正是现有 `recurring` FutureCall 的模型。Sanic `add_task` 被排除：它每 worker 一份、
无 System 事务上下文、不持久化，只适合 worker 本地杂活（框架内部 `worker_keeper` 续约即此
用法），喂给"全局定时跑游戏逻辑"是错配。

### 关键现状发现（决定了实现路线）

两块拼图其实**已经存在**，缺的只是把它们粘起来的"幂等播种"：

- **`on_start=True`**（`hetu/system/startup.py`）：每次开服跑一次，用 `boot_uuid` 作
  SystemLock 去重，整集群只提交一次；下次开服换新 uuid 再跑。是"每次开服"的载体。
- **`recurring=True` 的 FutureCall**（`hetu/system/future.py`）：持久化、按 `timeout` 周期
  触发、`timeout` 重试保证可靠、重启不丢、在 System 事务里执行；由每 worker 的
  `future_call_task` 每 ~1s 轮询、事务抢占、**到点全局只一个 worker 执行**。

缺口：`on_start` 每次开服都会跑，若在其中 `create_future_call(recurring=True)`，**每次重启都
新插一条** → 重启 N 次堆 N 条。痛点 = `create_future_call` 只能创建、无法"确保仅存在一条"。

### 支撑去重机制的底层事实（均已核对）

- `BaseComponent.new_row(id_=...)` 接受**显式 id**（`hetu/data/component.py:226`）。
- 插入时若 id 主键已存在，lua 返回 `"RACE: Key already exists"`
  （`commit_v2.lua:37-38`）→ `client.py:830` 抛 `RaceCondition` → `SystemCaller` 自动重试
  整个事务。
- `session.commit()` 有 `if is_dirty` 守卫（`session.py:95`）：**无写入的事务 commit 为空
  操作**，不会触发底层"没有脏数据需要提交"报错。
- 雪花 id **恒为正**（`next_id = (timestamp-EPOCH)<<22 | worker<<12 | seq`）。负数 id 区间
  天然空闲，可作 ensured 行的命名空间，与雪花 id 完全隔离。

### 已确认的设计决策

- **方案 B：只做等幂 helper**。框架不做自动播种 / reconcile / prune；用户自己在 `on_start`
  System 里调，掌控生命周期。（即不做"方案 A：`recurring=` kwarg"。）
- **去重机制：确定性 id 复用主键**。`key` 经稳定哈希映射到负数 id，靠 FutureCalls 现有主键
  唯一性去重。**不加字段、不改 schema、无迁移**。
- **API 形态：独立 `ensure_future_call`**（与 `create_future_call` 语义分离），外加**薄一层
  `cancel_future_call`**（按 key 删除，用于停止 / 重配 recurring）。
- **语义为 ensure-exists 而非 ensure-matches**：已存在则原样保留，不更新参数。
- **沿用 `create_future_call` 的校验与约束**：args `repr` 往返、目标 System 存在、目标需开
  `call_lock`、permission 警告；精度 ~1s、间隔下限 5s。

## 2. 设计

### 2.1 确定性 id

新增内部 helper（`future.py`）：

```python
def _key_to_id(key: str) -> int:
    """把 key 稳定映射到负数 id 区间，用作 FutureCalls 主键去重。
    必须跨进程/跨重启确定性（不可用内置 hash()，其按进程加盐）。
    """
    # 例：blake2b 取 64 位 → 强制落到 [-(2**63), -1]，避开 0 与正数（雪花）区间
    h = int.from_bytes(hashlib.blake2b(key.encode(), digest_size=8).digest(), "big")
    return -(h >> 1) - 1
```

- 命名空间隔离：雪花 id 恒正，ensured 行恒负，二者不可能相撞。
- 碰撞概率：63 位空间，n 个键 → ~ `n²/2⁶⁴`，实务可忽略（百万键约 3e-8）。两个**不同** key
  撞同一 id 才会误判"已存在"，概率极低，记为已知边界（见 §5）。

### 2.2 `ensure_future_call`（新 System）

签名与 `create_future_call` 一致，**前置一个 `key` 参数**：

```python
@define_system(namespace="global", permission=None, components=(FutureCalls,))
async def ensure_future_call(ctx, key, at, system, *args, timeout=60, recurring=False):
    """按 key 等幂确保一个未来调用存在。已存在则原样保留（ensure-exists，不改参数）。
    返回确定性 id。"""
    fid = _key_to_id(key)
    if await ctx.repo[FutureCalls].get(id=fid):
        return fid                      # 已存在 → no-op（commit 空转，安全）
    # 复用 create_future_call 的校验/建行逻辑（抽成共享 _build_future_row）
    row = _build_future_row(ctx, at, system, args, timeout, recurring, id_=fid)
    await ctx.repo[FutureCalls].insert(row)
    return fid
```

并发同 key：两个调用都 get 到"不存在" → 都 `insert(id=fid)` → 一个成功，另一个撞主键 →
`RaceCondition` → 自动重试 → 这次 get 命中 → 返回 `fid`（skip）。**任意 caller 调用都幂等**，
不绑定"必须在 on_start 里调"。

### 2.3 `cancel_future_call`（新 System，薄）

```python
@define_system(namespace="global", permission=None, components=(FutureCalls,))
async def cancel_future_call(ctx, key) -> bool:
    """按 key 删除未来调用（停止 / 重配 recurring）。返回是否存在并删除。"""
    fid = _key_to_id(key)
    if not await ctx.repo[FutureCalls].get(id=fid):
        return False
    ctx.repo[FutureCalls].delete(fid)
    return True
```

### 2.4 共享 helper 抽取（横切下沉）

把 `create_future_call` 现有的：`timeout`/`at` 归一、args `repr` 往返校验、目标 System
存在 / `call_lock` / permission 警告、组装 FutureCalls 行 —— 抽成内部
`_build_future_row(ctx, at, system, args, timeout, recurring, id_=None) -> row`：

- `id_` 给定 → `new_row(id_=id_)`；否则 `new_row()`（雪花），即 `create_future_call` 行为
  **保持不变**（仍用雪花 id，不传 `id_`）。
- 两个 caller 共用同一段校验，避免两份逻辑漂移。

`create_future_call` 改为：`row = _build_future_row(...); await repo.insert(row); return row.id`。

### 2.5 用户用法（开机即起的循环后台任务）

```python
@define_system(namespace="game", permission=None, call_lock=True,
               components=(FutureCalls, World))
async def world_tick(ctx):
    ...  # 每 tick 的全局逻辑

@define_system(namespace="game", permission=None, on_start=True,
               depends=("ensure_future_call:game",), components=(...))
async def boot(ctx):
    ctx.depend["ensure_future_call:game"](
        ctx, key="world_tick", at=-30, system="world_tick", recurring=True, timeout=30)
```

- 开服 → `on_start` 跑一次 → ensure 幂等 → 重启 N 次也只有一条 → `future_call_task` 每 ~1s
  轮询、全局只一个 worker 执行、`timeout` 重试、重启不丢。
- `on_start` 与 `recurring` **可组合**：on_start 的 `boot` 负责幂等播种；recurring 由
  `future_call_task` 周期驱动。
- **改间隔**：`cancel_future_call(key)` 再 `ensure_future_call(key, ..., timeout=新值)`。

## 3. 正确性与并发分析

- **skip 分支无写入**：`is_dirty=False`，commit 空转（`session.py:95`），不报"没有脏数据"。
- **插入竞态**：主键冲突 → `RaceCondition` → 自动重试 → skip。无需 unique 字段。
- **为什么必须用确定性 id 而非 check-then-insert**：乐观锁无法保护"读到不存在"的 phantom
  （两事务都读到空、各插不同 id 都会提交成功），唯有主键唯一性能拦住并发同 key 插入。这正是
  选确定性 id 的根因。
- **跨 instance 隔离**：FutureCalls 是 per-instance 表；同一 key 在不同 instance → 不同表、
  同一 fid 互不影响。
- **负数 / 哈希 id 兼容性**：FutureCalls 为 HeTu 内部 ADMIN 组件，无人对其 id 做雪花解码
  （已核 `snowflake_id.py` 无解码逻辑、全仓无 id 正负断言）；recurring 执行不取 call_lock
  （`req_call_lock = not recurring and timeout != 0`），非 recurring 的 ensured 行其
  `uuid=str(负数 id)` 仍 ≤32 字符，落 `SystemLock.uuid`（`<U32`）无虞。

## 4. 测试计划（扩展 `tests/test_system_future.py`）

- **幂等**：同 key 连调两次 `ensure_future_call` → 只产生一条 FutureCalls 行（按 fid get
  唯一）；两次返回 id 相同。
- **真执行**：ensure 一条 `at=-1` 的一次性调用 → 到点被 `future_call_task` 执行；
  `recurring=True` → 周期重复执行。
- **重启语义**：模拟"再次开服"再 ensure 同 key → 仍只有一条（不新增）。
- **并发幂等**：并发多次 ensure 同 key（制造 `RaceCondition`）→ 最终仅一条、无异常逃逸。
- **cancel**：cancel 已存在 key → `True` 且行被删，之后 ensure 同 key 可重新创建；cancel
  不存在 key → `False`。
- **改间隔**：`ensure(timeout=5)` → `cancel` → `ensure(timeout=10)` → 行的 `timeout` 为 10。
- **校验沿用**：目标 System 不存在 / 未开 `call_lock` → 与 `create_future_call` 同样报错
  （验证共享 helper 生效）。
- **确定性 id**：`_key_to_id` 跨调用稳定、恒为负、非 0。

## 5. 取舍与边界（YAGNI）

- **ensure-exists 非 ensure-matches**：已存在不自动更新参数（要改 → cancel 再 ensure）；不引入
  `force`/`update` 标志，留待将来。
- **不做框架级自动播种 / reconcile / prune**（即方案 A 的 `recurring=` kwarg）：用户用
  `on_start` + ensure 自行组织；孤儿行由用户用 cancel 清理。
- **不做 cron / 对齐到"每天 3 点"**：先固定间隔，daily reset 用 `86400s` 近似，未来可加对齐
  选项。
- **哈希碰撞**（63 位）概率可忽略，记为已知边界；若将来需绝对杜绝，可改为新增 unique `key`
  字段（需迁移）—— 本期不做。
- **key 不落库**（只进 id 哈希），故无长度 / 字符限制，也无需迁移。

## 6. 主要改动文件清单

- `hetu/system/future.py`：新增 `_key_to_id`、`ensure_future_call`、`cancel_future_call`；
  抽取共享 `_build_future_row`（`create_future_call` 复用，行为不变）。
- `docs/api/system.md` + `docs/api/_index.md`：补 `ensure_future_call` / `cancel_future_call`
  文档（与 `create_future_call` 并列）。
- `tests/test_system_future.py`：上述测试。
- （可选，随手）`hetu/llms.txt` / docs 教程：补"开机即起后台任务"用法片段。
