# unique 检查下沉到 commit（insert / update 不再远程预检）— 设计稿

- 日期：2026-09-20
- 状态：待用户确认设计，确认后写实现计划
- 取代：同日的《unique 预检合批》设计稿（已废弃，未实施）
- 影响范围：`hetu/data/backend/redis/commit_v2.lua`、`hetu/data/backend/redis/client.py`
  （commit）、`hetu/data/backend/sql/client.py`（commit）、`hetu/data/backend/idmap.py`
  （新增一个查询方法）、`hetu/data/backend/repo.py`（insert / update 去掉远程预检）、
  `hetu/data/backend/base.py`（异常 docstring）；测试与文档见 §6、§7。
  **不涉及 schema、不涉及客户端协议、不涉及订阅。**

## 1. 背景与目标

这是"减少 Redis 往返"系列的第一步。`SessionRepository.insert` / `update` 现在在写入前
逐字段到远程确认 unique 值没被占用（`repo.py:64-88`），每个变更的 unique 字段 1 次往返，
`id` 也是 unique 字段，所以**每次 insert 至少 1 次**。实测（临时 Redis，挂 `redis-py`
计数器）：

| 操作 | 现在 RTT | 命令序列 | 本 spec 后 |
|---|---:|---|---:|
| `insert` 无用户 unique 字段 + commit | 2 | ZRANGE(`id`) + EVALSHA | **1** |
| `insert` 1 个 unique + commit | 3 | ZRANGE×2 + EVALSHA | **1** |
| `insert` 2 个 unique + commit | 4 | ZRANGE×3 + EVALSHA | **1** |
| `get(name=)` + update 改 unique 字段 + commit | 4 | ZRANGE + HGETALL + ZRANGE + EVALSHA | 3 |
| `upsert(name=)` 插入路径 + commit | 4 | ZRANGE×3 + EVALSHA | 2 |
| `upsert(id=)` 插入路径 + commit | 3 | HGETALL + ZRANGE + EVALSHA | 2 |
| SystemLock：`get(uuid=)` 空 + `upsert(uuid=)` + commit | 5 | ZRANGE×4 + EVALSHA | 3 |

关键事实：**commit 阶段的合批 unique 检查已经存在**——`commit_v2.lua` 对每个 insert /
update 的 unique 字段做 `UNIQ` 检查、对 insert 的主键做 `NX` 检查，整个事务原子、一次往返。
预检只剩两个作用：

1. 早失败（不用跑完 System 体）；
2. 区分 `UniqueViolation`（确定性冲突，不重试）与 `RaceCondition`（重试）。commit 现在把
   `UNIQUE` 一律映射成 `RaceCondition`（`client.py:937-939`），这之所以正确，全靠预检已
   把确定性冲突拦在前面。`test_unique_violate_bug` 就是这个区分没做好时"无限重试卡死"
   的历史。

**目标**：把作用 2 挪到 commit（每条检查带上"是否曾观察其不存在"的标记，冲突时据此返回
`RACE` 或 `UNIQUE`），删除远程预检，insert / update 的预检往返降为 **0**。作用 1 以
显式 `get`（推荐写法）或保留的 `is_unique_conflicts`（可选）替代。

### 与其他库对齐

河图的 Session / IdentityMap 是 Unit of Work 设计，同类的 SQLAlchemy、Hibernate / JPA、
EF Core 全部在 flush / commit 时才报唯一冲突，写路径上没有预检；有预检的库（Rails
`validates uniqueness`、Django `validate_unique`）都是可选的表单校验层，并注明不防并发。
check-then-insert 的惯用法（Django `get_or_create`、Rails `create_or_find_by`）与河图
`upsert` 同构：先读、撞了重读，而不是预检。

### 已确认的设计决策

- **唯一规则**：commit 时主键 / unique 冲突，若本事务曾 `get` 观察到该 (列, 值) 不存在
  （`idmap.mark_absent`）→ `RaceCondition`；否则 → `UniqueViolation`。规则对 `id` 与用户
  unique 列一视同仁，不给雪花 id 开特例（见 §4 "雪花 id 撞主键"）。
- **同事务内重复仍在 `insert()` / `update()` 时立即报** `UniqueViolation`——本地 IdentityMap
  检查是 0 往返，且 Lua 的 checks 跑在 pushes 之前，看不到同一批数据，必须靠本地拦。
- **RACE 优先于 UNIQUE**：一个事务里同时存在竞态冲突与确定性冲突时判竞态（保住 `upsert`
  锚定列与其他 unique 列同时撞车时"重试后转 update"的语义，`repo.py:124-127` 的 docstring
  要求）。代价最多多一次重试。
- **`is_unique_conflicts` 保留为公开的可选提前检查**，默认路径不再调用；语义与实现不变。
- **异常从 `insert()` / `update()` 移到 `commit()`**，这是本设计唯一的契约变化（§5）。

## 2. 已确认的事实（均已核对）

- `commit_v2.lua` Phase 1 顺序执行 checks，**遇到第一个失败就 return**；`UNIQ` 已处理
  `deleted`（本事务删除的行不算冲突，`commit_v2.lua:52-71`）。Phase 2 才执行 pushes。
- `client.py:commit` 组 checks 的顺序：逐表 → insert（`NX`、`UNIQ`×字段）→ update（`VER`、
  `UNIQ`×变更字段）→ delete（`VER`）→ 最后纯读行的 `VER`（`client.py:867-917`）。insert
  的 `UNIQ` 覆盖含 `id` 在内的全部 unique 字段；update 只覆盖变更字段（`_unique_meet`
  只遍历 `_row.items()`）。
- 字段迭代顺序 = `properties_` 顺序 = 按名排序（`component.py:194`；`get_dirty_rows`
  用 `row.dtype.names` 建 dict），所以同一行多列冲突时报出的字段名是确定的。
- 响应映射在 `client.py:932-941`：`RACE*` → `RaceCondition`，`UNIQUE*` → `RaceCondition`，
  其他 → `RuntimeError`。
- `IdentityMap._absent`（`idmap.py:47-51`）以 `(index_name, 归一化值)` 记录 negative
  observation；`mark_absent` 只由等值精确 `get` 读空时调用（主键或 unique 列，
  `repo.py:242-248`）；`observed_absent` 做同样归一化后查集合。IdentityMap 持有 typed
  的 `np.record` 缓存行，能直接用 typed 值查 `_absent`，无需从 commit 的 str dict 反推。
- SQL 后端的 commit（`sql/client.py:920-1140`）在 `aio.begin()` 事务里：先 SELECT 校验
  纯读行版本，**先执行 delete**（注释：避免 insert / update 撞上本事务将删除的数据），再
  update、insert；`IntegrityError` 且是唯一冲突 → 一律 `RaceCondition`（`:1031-1033`、
  `:1072-1074`）。unique 列在 SQL 表上有真实的 UNIQUE 约束（`:222`）。
- `RetryAttempt.__aexit__`（`session.py:158-176`）与 `SystemCaller.call_`
  （`caller.py:129-141`）只捕获 `RaceCondition`；`UniqueViolation` 直接向上冒泡——与今天
  从 `insert()` 抛出时的传播路径完全相同。
- `async with session:` 包起来的调用（sandbox `sb.insert`、headless）异常从 `__aexit__`
  的 commit 抛出，对调用方仍是同一个 `await`；`hetu/testing/__init__.py:420` 与
  `docs/zh/advanced.md:573` 承诺的"撞了抛 `UniqueViolation`"继续成立。
- 仓库、docs、examples 中**没有** `try: await repo.insert(...) except UniqueViolation`
  这种在事务体内捕获的写法；文档惯用法是先 `get`（`docs/zh/_index.md:96`：防幻读用
  unique）。
- `5975362` 起 commit 还为每个变动的 (索引, 值) 组一条 PUBLISH（`value_pubs`，由
  `_exc_index` 顺带记录，Lua Phase 3 发出，供索引点查询订阅用）。它与 checks 无关，本 spec
  不碰；但 `commit()` 的 helper 签名会同时被两件事改动，实现时注意别互相覆盖。
- `tests/test_backend_client.py::test_redis_commit_payload` 用成员判断（`check in json[0]`）
  逐条核对 checks / pushes / publishes 的元组格式，改 checks 格式必须同步改它；它不依赖顺序。
- `tests/test_backend_session_basic.py:141-144` mock 的 `_remote_has_unique_conflicts`
  是不存在的属性名（真名 `remote_has_unique_conflicts_`），从未生效。

## 3. 设计

### 3.1 判定规则

```
insert()/update() 时：本地 IdentityMap 有同值行（排除已删）      → UniqueViolation（立即）
commit 时：主键已存在 / unique 值已被占用（排除本事务删除的行）
    若 (列, 值) ∈ 本事务 absent 观察                              → RaceCondition
    否则                                                          → UniqueViolation
commit 时：同一 payload 里两条 UNIQ 指向同一 (索引, 值)              → UniqueViolation（兜底）
```

多条冲突同时存在：先报竞态类（`VER`、带 absent 标记的 `NX` / `UNIQ`），再报确定性类。

### 3.2 IdentityMap：提供 absent 标记

```python
def get_absent_unique_fields(self) -> dict[TableReference, dict[int, set[str]]]:
    """
    对每个待 INSERT / UPDATE 的行，返回其 unique 列（含 id）中本事务曾观察"该值不存在"
    （见 `mark_absent`）的列集合。commit 据此把主键 / unique 冲突判为 `RaceCondition`
    （基于过期快照的乐观并发失败，重试可解）而非 `UniqueViolation`（确定性冲突）。
    返回 {TableReference: {row_id: {field, ...}}}，没有 absent 列的行不出现。
    """
```

实现：只遍历 `_absent` 里有记录的表，对该表缓存中状态为 INSERT / UPDATE 的行，逐 unique
列查 `(field, _norm_value(row[field]))` 是否在集合内。absent 集合只在 `get` 读空时增长，
通常 0～2 条，开销可忽略。

### 3.3 Redis commit（`client.py` + `commit_v2.lua`）

**checks 元组格式**（`VER` 不变）：

```
["NX",   key,                          code, label]
["UNIQ", idx_key, start_val, end_val,  code, label]
["VER",  key, expected_version]
```

- `code` ∈ `"RACE"` / `"UNIQUE"`：冲突时 Lua 返回串的前缀，由 Python 按 §3.1 决定。
- `label`：定位信息，形如 `Item.name id=123 insert` / `Item.time id=456 update`；主键
  `NX` 用 `Item.id id=123 insert`。只做消息用，Lua 原样回显。

**排序**：Python 组两个列表 `race_checks`（所有 `VER`、`code=="RACE"` 的 `NX`/`UNIQ`）与
`strict_checks`（`code=="UNIQUE"` 的），payload 里 `checks = race_checks + strict_checks`。
Lua"首个失败即返回"于是天然实现 RACE 优先。表内 / 行内的相对顺序保持现状（字段按名）。

**Lua 改动**（Phase 1）：

```lua
local seen_uniq = {}
...
elseif op == "NX" then
    if redis_call("EXISTS", check[2]) == 1 then
        return check[3] .. ": Key already exists " .. check[4]
    end
elseif op == "UNIQ" then
    local idx_key, start_val, end_val, code, label = check[2], check[3], check[4], check[5], check[6]
    local dup = idx_key .. "\0" .. start_val
    if seen_uniq[dup] then
        return "UNIQUE: Duplicate unique value within transaction " .. label
    end
    seen_uniq[dup] = true
    local res = redis_call("ZRANGE", idx_key, start_val, end_val, "BYLEX", "LIMIT", 0, 1)
    if #res > 0 then
        local row_id = string_match(res[1], ".*%z(.*)$")
        if not deleted[row_id] then
            return code .. ": Unique violation " .. label
        end
    end
end
```

Phase 2 / 3 不动。

**响应映射**（`client.py:932-941`）：

```python
if resp.startswith("RACE"):
    raise RaceCondition(resp)
elif resp.startswith("UNIQUE"):
    raise UniqueViolation(resp)      # 原来是 RaceCondition
else:
    raise RuntimeError(...)
```

`_unique_meet` / `_key_must_not_exist` 增加 `absent: set[str]` 与 `label` 参数，
`commit()` 开头取一次 `idmap.get_absent_unique_fields()`，逐行查 `absent_by_ref[ref][row_id]`。

### 3.4 SQL commit（`sql/client.py`）

在 `aio.begin()` 事务内、**delete 之后、update / insert 之前**增加显式唯一性检查（与 Lua
"checks 先于 pushes、deleted 不算冲突"对齐，SQL 里 delete 已先执行所以 SELECT 自然看不到）：

1. 收集：对每个 insert 行的全部 unique 列（含 `id`）、每个 update 行的变更 unique 列，
   按 `(ref, field)` 分组成 `{typed_value: (row_id, is_race)}`。
2. 每组一条 `SELECT id, <field> FROM t WHERE <field> IN (...)`（`id` 列用
   `WHERE id IN (...)`）。找到的行若 `id == row_id`（自身）则忽略——此情况意味着该行被并发
   改成了同值，交给后面 UPDATE 的版本条件报 `RaceCondition`。
3. 汇总冲突：任一 `is_race` → `RaceCondition`；否则有冲突 → `UniqueViolation`；消息格式
   与 §3.6 一致。
4. 现有 `IntegrityError` → `RaceCondition` 兜底**保留**：它只会在 SELECT 与 INSERT 之间被
   并发写入时触发；重试后 SELECT 会把它判成确定性冲突，不会无限重试。

参数化测试（`HETU_TEST_BACKENDS` 含 sqlite / postgres / mariadb）覆盖此路径。

### 3.5 SessionRepository（`repo.py`）

- `insert(row)`：保留 `_version == 0` 断言、`explicit_ids_only` 检查；`changed = _get_changed_fields(row)`
  与 `"id" in changed` 断言；**只做** `_local_has_unique_conflicts(row, changed & uniques_)`
  → 冲突抛 `UniqueViolation`（消息注明 within transaction）；然后 `idmap.add_insert`。
  不再调用 `is_unique_conflicts`。方法保持 `async def`（API 兼容），只是不再 await。
- `update(row)`：同理，只做本地检查。
- `is_unique_conflicts(row, insert=False)`、`remote_has_unique_conflicts_`、
  `_local_has_unique_conflicts`：**代码不动**，前者 docstring 改为"可选的提前检查（每个字段
  1 次往返）；insert / update 默认不调用，等价判定在 commit 时由后端原子执行"。
- `_raise_unique_conflict`：只剩本地路径使用，简化为直接抛 `UniqueViolation`（或删除，
  由实现决定）。
- `UpsertContext.__aexit__` 里关于"锚定字段冲突判 Race"的注释改为指向 commit 的判定。
- `get()` 不动：`mark_absent` 仍是标记的唯一来源。

### 3.6 错误信息

- `UniqueViolation`：`"UNIQUE: Unique violation Item.name id=123 insert"`（Redis 原样回显）；
  SQL 拼成同样形式。必含：组件名、字段名、行 id、insert / update。现有测试只用
  `match="name"` / `match="time"` 匹配字段名，格式兼容。
- `RaceCondition`：前缀 `RACE:`，其余同上；`VER` 消息不变。
- 本地重复：`"Insert failed: row.name violates a unique index (duplicate within transaction)"`。

## 4. 正确性与并发分析

| 场景 | 现在 | 改后 |
|---|---|---|
| 盲 insert，值已存在 | 预检 → `UniqueViolation`（insert 处） | commit `UNIQUE` → `UniqueViolation`（commit 处），不重试 |
| `get` 读空 → 被抢先 → insert | 预检 → `RaceCondition`（insert 处） | commit `RACE` → `RaceCondition` → 重试 → `get` 命中走另一分支 |
| 盲 insert，预检后、commit 前被抢先 | commit `UNIQUE` → `RaceCondition` → 重试 → 预检 → `UniqueViolation` | commit `UNIQUE` → `UniqueViolation`，**少一次重试** |
| `upsert` 锚定列被抢先 | `RaceCondition` → 重试转 update | 同（锚定列必经 `get` 读空 → absent） |
| 锚定列 + 其他 unique 列同时撞车 | `RaceCondition`（absent 先查） | `RaceCondition`（race_checks 在前） |
| 同事务两行同 unique 值 | 本地 → `UniqueViolation` | 同；Lua 另有 payload 内去重兜底 |
| 删 A(name=x) 再 insert B(name=x) | 通过（`is_deleted` / Lua `deleted`） | 同（SQL 靠 delete 先执行） |
| A: x→y 且 insert B(name=x) | 拒绝（预检查到 A） | 拒绝（Lua checks 先于 pushes / SQL SELECT 先于 UPDATE），本来就不支持 |
| headless 显式 id 撞主键，无 `get` | `UniqueViolation` | `NX` strict → `UniqueViolation`（`advanced.md:573` 承诺保持） |
| `ensure_future_call` / `upsert(id=)` 撞主键 | `RaceCondition` → 重试 → get 命中 | 同（`get(id=)` 读空 → absent） |
| 雪花 id 撞主键（worker id 重复等 bug） | `RACE` → 重试换新号，**掩盖 bug** | `NX` strict → `UniqueViolation`，System 失败并记日志。有意为之：这是租约围栏该拦的故障，不该被静默重试吞掉 |

- **重试收敛**：`RaceCondition` 只在 absent 标记存在时产生；重试后 `get` 必然命中（值已
  存在）→ 不再 insert 同值 → 不再产生同一冲突。确定性冲突不重试。无无限重试路径。
- **快照一致性提升**：预检读的是 `master_or_servant`（可能滞后的 replica），commit 在
  master 上判定，减少"预检没看到、commit 撞上"的二次往返。
- **cluster 模式**：checks 仍在同一 `{CLU}` slot 的 Lua 里执行，无跨 slot；payload 只多了
  两个短字符串。
- **本地检查不可省**：Lua checks 在 pushes 之前评估索引，同批两行同值互相看不见；
  IdentityMap 检查 0 往返，保留。Lua 的 `seen_uniq` 只是二道防线（如运维工具直接构造
  idmap 提交）。
- **副作用**：确定性冲突要跑完整个 System 体才失败，只浪费错误路径；文档已要求非事务副作用
  放在 `session_commit()` 之后，无新风险。

## 5. 用户可见变化与文档

- `insert()` / `update()` 不再抛远程冲突；`UniqueViolation` / `RaceCondition` 改由
  `commit()`（`async with session` 退出、`SystemCaller` 提交、`ctx.session_commit()`）抛出。
  在事务体内 `try: await repo.insert(row) except UniqueViolation` 的写法失效；替代写法是
  先 `get`（读空自动登记 absent，撞车自动重试），或调用 `is_unique_conflicts`。
- `base.py` 的 `RaceCondition` / `UniqueViolation` docstring 重写：说明判定在 commit 时
  进行、判定规则、同事务重复的立即报错；`BackendClient.commit` / `Session.commit` /
  `SystemContext.session_commit` 的 Exceptions 段补 `UniqueViolation`。
- `docs/zh/_index.md` "事务冲突与重试"补一条："unique 冲突在提交时检查：本事务曾 `get`
  观察其不存在的判竞态重试，否则以 `UniqueViolation` 失败；要在事务内分支处理，先
  `get`。"（en 由 `scripts/translate_new_content.py` 同步。）
- `docs/zh/concepts.md:33` "外加插入时的唯一性检查" → "外加提交时的唯一性检查"。
- `docs/api/*.md` 用 `uv run python scripts/gen_api_docs.py` 重新生成。

## 6. 测试计划

`tests/test_backend_client.py`

- `test_redis_commit_payload`：期望的 `NX` / `UNIQ` 元组补 `code` / `label`；增加一组
  "先 `get` 读空再 insert"的行，断言其 `NX` / `UNIQ` 的 `code == "RACE"`，未观察过的行为
  `"UNIQUE"`；断言 checks 中所有 `RACE` 类排在 `UNIQUE` 类之前。
- Lua 兜底：直接构造含两条同 (索引, 值) `UNIQ` 的 payload 调 `lua_commit` → 返回串以
  `UNIQUE:` 开头（Redis 专属）。

`tests/test_backend_session_basic.py`

- `test_insert_unique`：本地重复段保持"insert 处抛"，删掉无效 mock；"与库中既有数据冲突"
  段改为 `with pytest.raises(UniqueViolation, match=...)` 包住整个 `async with session`
  （commit 处抛）。update 改成既有 unique 值同理。
- 新增 **commit 判定规则**用例（各后端参数化）：
  1. 盲 insert 既有 `name` → `UniqueViolation`，且用 `Session.retry(3)` 包住时**不重试**
     （计数 attempt == 1）；
  2. `get(name=x)` 读空 → 另一 session 插入 x → insert x → `RaceCondition`；用
     `Session.retry` 包住则重试后 `get` 命中、整体成功；
  3. 锚定列 absent + 另一 unique 列既有值同时冲突 → `RaceCondition`（优先级）；对照：
     只有非 absent 列冲突 → `UniqueViolation`；
  4. headless（`explicit_ids_only=True`）显式 id 撞主键：无 `get` → `UniqueViolation`；
     先 `get(id=)` 读空再撞 → `RaceCondition`；
  5. 同事务两行同 unique 值 → `insert()` 处立即 `UniqueViolation`（现有
     `test_unique_batch_add_in_same_session_bug` 已覆盖，保留）。
- 现有 `test_upsert`、`test_unique_remove_then_add_bug`、`test_session_insert_then_upsert`、
  `test_explicit_ids_only` 不改，作语义回归。

`tests/test_backend_session_race.py`

- `test_insert_after_get_none_is_race`：断言不变（异常从 `async with` 出来），注释改为
  "commit 时判为 Race"。`test_update_or_insert_race`、`test_retry_generator` 不改。

`tests/test_system_executor.py`

- `test_unique_violate_bug` / `bug2` 不改：仍要求日志有 `UniqueViolation`、无
  `RaceCondition`——这正是 commit 判定不重试的验收。

`tests/test_backend_sql.py`：如无现成 unique 冲突用例，上面参数化用例已覆盖；另加一条
"SELECT 找到自身行时不误判"（并发把本行改成目标值 → 期望 `RaceCondition` 而非
`UniqueViolation`）。

## 7. 取舍与边界（YAGNI）

- **不做 `insert(row, check=True)` 之类新参数**：要提前检查用现有 `is_unique_conflicts`。
- **不做预检合批 / `range_many`**：预检删除后无此需求；订阅层若将来需要再单独提。
- **`get()` 的 negative cache**（同一事务内重复 `get` 同一读空值不再打远程，SystemLock
  流程可再省 1 次）、**跳过 alive 检查**等属后续 spec，与本 spec 正交。
- **雪花 id 撞主键改为硬失败**：见 §4 表末行，是有意的行为变化；若用户希望保留"换号重试"，
  可在 `get_absent_unique_fields` 之外给 `id` 加 `not explicit_ids_only → RACE` 的特例，
  本 spec 默认不加。
- **float32 unique 列的 absent 匹配**：`mark_absent` 存的是用户传入的 Python float，
  `observed_absent` 用 np.float32 的 `.item()` 比较，精度不同可能匹配不上（判成
  `UniqueViolation`）。这是现有 `is_unique_conflicts` 就有的边界，本 spec 不扩大也不修，
  记录在案。
- **Lua 仍是首个失败即返回**：不汇总全部冲突（RACE 优先靠排序实现），保持脚本简单。

## 8. 主要改动文件清单

- `hetu/data/backend/redis/commit_v2.lua`：`NX` / `UNIQ` 读 `code` / `label` 回显；
  `seen_uniq` 去重。
- `hetu/data/backend/redis/client.py`：`commit()` 取 absent 标记、组 `race_checks` /
  `strict_checks`、新元组格式；响应映射 `UNIQUE` → `UniqueViolation`（import）。
- `hetu/data/backend/sql/client.py`：commit 内新增唯一性 SELECT 检查与判定。
- `hetu/data/backend/idmap.py`：`get_absent_unique_fields()`。
- `hetu/data/backend/repo.py`：`insert` / `update` 去远程预检；docstring；
  `is_unique_conflicts` 标注为可选。
- `hetu/data/backend/base.py`、`session.py`、`hetu/system/context.py`：异常 docstring。
- 测试：§6。
- 文档：§5（`docs/zh/_index.md`、`docs/zh/concepts.md`、`docs/api/*` 重新生成）。
