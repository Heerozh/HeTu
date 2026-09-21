# unique 检查下沉到 commit 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `SessionRepository.insert/update` 不再逐字段远程预检 unique（每次 insert 至少省 1 RTT），冲突判定挪到 `commit()`：每条 `NX`/`UNIQ` 检查带 `RACE`/`UNIQUE` 标记（该 (列, 值) 是否曾被本事务 `get` / 等值 `range` 观察为不存在），Redis Lua / SQL SELECT 冲突时按标记抛 `RaceCondition`（重试）或 `UniqueViolation`（不重试）；同事务内重复仍在 `insert()/update()` 本地立即抛。

**Architecture:** 标记来源是已有的 `IdentityMap._absent`，新增 `IdentityMap.get_absent_unique_fields()` 从 typed 缓存行算出 `{ref: {row_id: {field}}}`。Redis 端 Python 把 checks 分成竞态类（`VER` + RACE 标记的 `NX`/`UNIQ`）与确定性类，拼接后交给 Lua "首个失败即返回"，天然 RACE 优先；Lua 另按 `(idx_key, start_val)` 去重兜底。SQL 端在事务内 delete 之后、update/insert 之前做显式 SELECT 检查，收集全部冲突后 RACE 优先抛出，`IntegrityError → RaceCondition` 兜底保留。`is_unique_conflicts` 保留为公开可选预检。

**Tech Stack:** Python 3.14、NumPy structured arrays、Redis Lua（cmsgpack）、SQLAlchemy async、pytest（`asyncio_mode=auto`，Redis 需 Docker，sqlite 免 Docker）。

**设计依据:** `docs/superpowers/specs/2026-09-20-unique-check-at-commit-design.md`

## 全局约定

- TDD 用 Redis：`HETU_TEST_BACKENDS=redis uv run pytest ...`；SQL 用 `HETU_TEST_BACKENDS=sqlite`
  （免 Docker）；收尾全后端跑一次。Windows 下 `pytest -s` 加 `PYTHONUTF8=1`。
- Lint/类型检查**只查自己改的文件**：`uv run ruff format <f> && uv run ruff check <f> && uv run basedpyright <f>`，
  与 HEAD 比告警数不增即可，不要对整个包跑 `--fix`。
- 提交前缀沿用 `git log` 惯例：`perf(idmap):` / `perf(redis):` / `perf(sql):` / `perf(repo):` / `docs:`。
- 中间态说明：Task 2/3 完成后后端已按新规则映射异常，但 `repo.py` 预检仍在（先拦确定性冲突），现有
  session 测试保持绿；Task 4 才删预检。别在中间态长期停留。
- 开工第一步：按仓库惯例把本计划存一份到 `docs/superpowers/plans/2026-09-21-unique-check-at-commit.md`
  （格式对齐 `2026-06-14-ensure-future-call.md`，任务/步骤用 `- [ ]`），随实施勾选。

---

## Task 1: `IdentityMap.get_absent_unique_fields()`（纯新增）

**Files:** `hetu/data/backend/idmap.py`（`observed_absent` 之后插入；顺带改 `_absent` 注释 47-51 行的
"用途"两行为"commit 时…判 RaceCondition 而非 UniqueViolation（见 get_absent_unique_fields）"）；
`tests/test_backend_idmap.py`（追加用例，需补 `import numpy as np`）。

- [x] 基线：`HETU_TEST_BACKENDS=redis uv run pytest tests/test_backend_idmap.py tests/test_backend_client.py tests/test_backend_session_basic.py tests/test_backend_session_race.py tests/test_system_executor.py -q` 全绿。
- [x] 先写测试 `test_get_absent_unique_fields(mod_item_model)`：空 idmap → `{}`；只有 `mark_absent` 没缓存行
  → `{}`（不能 KeyError）；`mark_absent(id=100)`+`mark_absent(name="a")` 后 `add_insert(id_=100, name="a", time=1)`
  → `{100: {"id","name"}}`；未观察的 insert 行、值命中但 CLEAN 的行、`mark_deleted` 的行都不出现；
  `add_clean` 后 `mark_absent(time=44)` 再 `get` → 改 `time=44` → `update` → `{103: {"time"}}`；
  `mark_absent(time=np.int64(7))` 与行值 7 能对上。期望：`{ref: {100: {"id","name"}, 103: {"time"}}}`。
- [x] 实现（`RowState` 已在同文件）：

```python
def get_absent_unique_fields(self) -> dict[TableReference, dict[int, set[str]]]:
    """
    对每个待 INSERT / UPDATE 的行，返回其 unique 列（含 id）中本事务曾观察"该值不存在"
    （见 `mark_absent`）的列集合。commit 据此把主键 / unique 冲突判为 `RaceCondition`
    （基于过期快照的乐观并发失败，重试可解）而非 `UniqueViolation`（确定性冲突）。

    Returns
    -------
    {TableReference: {row_id: {field, ...}}}，没有 absent 列的行不出现。
    """
    ret: dict[TableReference, dict[int, set[str]]] = {}
    for table_ref, absent in self._absent.items():
        states = self._row_states.get(table_ref)
        if not absent or not states:
            continue
        dirty_ids = [
            rid for rid, st in states.items()
            if st == RowState.INSERT or st == RowState.UPDATE
        ]
        # 只查 absent 里出现过的 unique 列，通常 0~2 个
        candidates = table_ref.comp_cls.uniques_ & {field for field, _ in absent}
        if not dirty_ids or not candidates:
            continue
        cache = self._row_cache[table_ref]
        rows_absent: dict[int, set[str]] = {}
        for row in cache[np.isin(cache["id"], dirty_ids)]:
            fields = {f for f in candidates if (f, self._norm_value(row[f])) in absent}
            if fields:
                rows_absent[int(row["id"])] = fields
        if rows_absent:
            ret[table_ref] = rows_absent
    return ret
```

- [x] `uv run pytest tests/test_backend_idmap.py -q` 全绿；lint；commit `perf(idmap): 新增 get_absent_unique_fields，供 commit 判定 unique 冲突是否为竞态`。

---

## Task 2: Redis commit + Lua

**Files:** `hetu/data/backend/redis/commit_v2.lua`（Phase 1）；`hetu/data/backend/redis/client.py`
（import 23-29；`commit` 782-941）；`tests/test_backend_client.py`；`tests/test_backend_session_race.py`。

新 checks 元组（`VER` 不变）：`["NX", key, code, label]`、`["UNIQ", idx_key, start, end, code, label]`，
`code ∈ {"RACE","UNIQUE"}`，`label = f"{comp_name}.{field} id={row_id} {op}"`（op = insert/update；主键
NX 的 field 为 `id`）。

- [x] **改 `test_redis_commit_payload` 期望（先红）**：测试开头加
  `def label(comp, field, rid, op): return f"{comp}.{field} id={rid} {op}"`；全部 `NX`/`UNIQ` 期望按同一
  模式补两个元素，例如 item row1 的
  `checks.append(["UNIQ", "pytest:Item:{CLU1}:index:name", b"[10\x00", b"[10\x00\xff"])` →
  `... + ["UNIQUE", label("Item", "name", row.id, "insert")]`；`NX` 同理 `+ ["UNIQUE", label("Item", "id", row.id, "insert")]`；
  RLSTest 行用 `"RLSTest"`；两处 update 用 `"update"`（字段分别为 `time`、`name`）。该测试所有行都是盲写，
  code 全为 `"UNIQUE"`。
- [x] **新增 `test_redis_commit_check_codes(mod_item_model)`**（同文件，mock `lua_commit` 抓 payload）：
  行 A `mark_absent(id)`+`mark_absent(name)` 后 insert → `id`/`name` 的 NX/UNIQ 为 RACE、`time` UNIQUE；
  行 B 盲 insert → 全 UNIQUE；行 C `add_clean` 后 `mark_absent(time=33)`、`get` 改 `time=33`、`update` →
  `time` RACE。断言用 label 反查：
  `codes = lambda f, rid, op: {c[-2] for c in checks if c[0] in (b"NX", b"UNIQ") and c[-1] == f"Item.{f} id={rid} {op}".encode()}`；
  排序：`is_race = [c[0] == b"VER" or c[-2] == b"RACE" for c in checks]; assert is_race == sorted(is_race, reverse=True)`。
- [x] **Lua**（`VER`/`EX`、Phase 2/3 不动；`local next` 不再用可删）：

```lua
if checks then
    -- 顺序即优先级：Python 把竞态类排前面，首个失败即返回 → 同时存在两类冲突时先报 RACE
    local seen_uniq = {}   -- 同 payload 内两条 UNIQ 指向同一 (索引, 值) 的兜底
    for _, check in ipairs(checks) do
        local op = check[1]
        if op == "VER" then ...原样...
        -- ["NX", key, code, label]
        elseif op == "NX" then
            if redis_call("EXISTS", check[2]) == 1 then
                return check[3] .. ": Key already exists " .. check[4]
            end
        elseif op == "EX" then ...原样...
        -- ["UNIQ", index_key, start_val, end_val, code, label]
        elseif op == "UNIQ" then
            local idx_key, start_val, end_val = check[2], check[3], check[4]
            local code, label = check[5], check[6]
            local dup = idx_key .. "\0" .. start_val
            if seen_uniq[dup] then
                return "UNIQUE: Duplicate unique value within transaction " .. label
            end
            seen_uniq[dup] = true
            local res = redis_call("ZRANGE", idx_key, start_val, end_val, "BYLEX", "LIMIT", 0, 1)
            if #res > 0 then
                local row_id = string_match(res[1], ".*%z(.*)$")
                if not deleted[row_id] then       -- 本事务删除的行不算冲突
                    return code .. ": Unique violation " .. label
                end
            end
        end
    end
end
```

- [x] **`client.py`**：import 加 `UniqueViolation`；`commit` docstring 的 Exceptions 段改为两条（RaceCondition：
  版本不符、或冲突命中本事务曾 `get` 观察不存在的值；UniqueViolation：值已被占用且从未观察其不存在，不重试）。
  `checks` 拆成 `race_checks` / `strict_checks`；`dirties` 之后 `absent_by_ref = idmap.get_absent_unique_fields()`；
  闭包改为：

```python
def _key_must_not_exist(_key: str, _race: bool, _label: str):
    (race_checks if _race else strict_checks).append(
        ["NX", _key, "RACE" if _race else "UNIQUE", _label]
    )

def _version_must_match(_key: str, _old_version):
    race_checks.append(["VER", _key, _old_version])   # 恒为竞态类

def _unique_meet(_unique_fields, _dtype_map, _idx_prefix, _row, _absent: set[str],
                 _comp_name: str, _row_id: str, _op: str):
    for _field, _value in _row.items():
        if _field in _unique_fields:
            ...原有 _idx_key/_start_val/_end_val 计算...
            _race = _field in _absent
            (race_checks if _race else strict_checks).append([
                "UNIQ", _idx_key, _start_val, _end_val,
                "RACE" if _race else "UNIQUE", f"{_comp_name}.{_field} id={_row_id} {_op}",
            ])
```

  逐表循环里 `absent_rows = absent_by_ref.get(ref, {})`；insert：`absent = absent_rows.get(int(row_id), set())`，
  `_key_must_not_exist(key, "id" in absent, f"{comp_cls.name_}.id id={row_id} insert")`，
  `_unique_meet(..., insert, absent, comp_cls.name_, row_id, "insert")`；update 同理传 `"update"`
  （注意 `get_dirty_rows` 的 `row_id` 是 str，要 `int()`）。打包前 `checks = race_checks + strict_checks`。
  响应映射：`UNIQUE` 前缀改抛 `UniqueViolation(resp)`（原 `RaceCondition`），注释说明"确定性冲突不重试"。
- [x] **新增 Lua 回显/去重用例 `test_redis_lua_check_codes`**（`@use_redis_family_backend_only`，需 Docker）：
  先用 session 插一行 `name="dup"`；`from hetu.data.backend.redis.client import msg_packer`，
  `run = lambda checks: client.lua_commit([client.row_key(item_ref, 1)], [msg_packer.pack([checks, [], {}, []])])`；
  断言 `UNIQ`+`RACE` → `b"RACE: Unique violation <label>"`，`UNIQ`+`UNIQUE` → `b"UNIQUE: Unique violation <label>"`，
  `NX` 于已存在 key → `b"UNIQUE: Key already exists <label>"`，两条同 (索引, 不存在的值) 的 UNIQ →
  `b"UNIQUE: Duplicate unique value within transaction <第二条 label>"`。
- [x] **`test_unique_commit_race` 改期望**：两处 `pytest.raises(RaceCondition, match="UNIQUE")` →
  `pytest.raises(UniqueViolation, match="UNIQUE")`，import 相应改，docstring 改为"盲写（未 get 观察其不存在）
  撞上并发已提交的同值 → 确定性 UniqueViolation，不重试"。此用例在 SQL 后端要到 Task 3 才绿。
- [x] 验证：`HETU_TEST_BACKENDS=redis uv run pytest tests/test_backend_client.py tests/test_backend_session_basic.py tests/test_backend_session_race.py tests/test_system_executor.py -q` 全绿；
  再跑 `HETU_TEST_BACKENDS=redis_cluster uv run pytest tests/test_backend_client.py -q`（label/code 走 ARGV，
  不引入跨 slot）。lint；commit `perf(redis): commit 的 NX/UNIQ 检查带 RACE/UNIQUE 标记，冲突按标记映射异常，Lua 去重兜底`。

---

## Task 3: SQL commit 显式唯一性检查

**Files:** `hetu/data/backend/sql/client.py`（import 24-30；新增 `_check_unique_conflicts` 放在
`_is_unique_violation`(~718) 之后；`commit` 920-1140）；`tests/test_backend_session_race.py`。

- [x] **先写测试 `test_update_to_value_set_by_concurrent_self_update_is_race(item_ref, mod_auto_backend)`**
  （各后端参数化）：插一行 `name="self", time=1`；session s `get(name="self")`，嵌套 session s2 把同一行
  `time=5` 提交，s 再 `row.time=5; update` → 退出时 `pytest.raises(RaceCondition, match="Version")`。
  Redis 靠 `VER` 排在 `UNIQ` 前；SQL 靠 SELECT 跳过自身行 → UPDATE rowcount 0。
  `HETU_TEST_BACKENDS=redis,sqlite uv run pytest tests/test_backend_session_race.py -q -k "self_update or unique_commit_race"`：
  redis 两条绿，sqlite 的 `test_unique_commit_race` 红（仍抛 RaceCondition）。
- [x] **实现**：import 加 `UniqueViolation`、`AsyncConnection`；新增：

```python
async def _check_unique_conflicts(self, conn: AsyncConnection, dirties, absent_by_ref) -> None:
    """
    commit 事务内的显式唯一性检查，在 delete 之后、update / insert 之前执行（与 Redis Lua
    "checks 先于 pushes、本事务删除的行不算冲突"对齐：delete 已先执行，SELECT 自然看不到）。
    insert 行查全部 unique 列（含 id），update 行只查变更的 unique 列，按 (ref, field) 分组各
    SELECT 一次。update 行查到自身（并发把本行改成了同值）则忽略，交给 UPDATE 的版本条件报
    RaceCondition。汇总冲突：任一"曾 get 观察其不存在"的列 → RaceCondition（RACE 优先）；否则
    → UniqueViolation。SELECT 与写入之间被并发抢先仍由 IntegrityError → RaceCondition 兜底。
    """
    race: list[str] = []
    strict: list[str] = []
    for ref, (inserts, (old_rows, new_rows), _deletes) in dirties.items():
        comp_cls = ref.comp_cls
        dtype_map = comp_cls.dtype_map_
        absent_rows = absent_by_ref.get(ref, {})

        def _norm(field, value):   # 两侧都过 dtype，保证查回的值能对上本地 key
            dtype = dtype_map[field]
            return dtype.type(self._coerce_scalar(dtype, value)).item()

        wanted: dict[str, dict[Any, tuple[int, bool, str]]] = {}   # field -> value -> (row_id, is_race, op)
        for row in inserts:
            rid = int(row["id"]); absent = absent_rows.get(rid, set())
            for f in sorted(comp_cls.uniques_):
                wanted.setdefault(f, {})[_norm(f, row[f])] = (rid, f in absent, "insert")
        for old_row, changed in zip(old_rows, new_rows):
            rid = int(old_row["id"]); absent = absent_rows.get(rid, set())
            for f in sorted(comp_cls.uniques_):
                if f in changed:
                    wanted.setdefault(f, {})[_norm(f, changed[f])] = (rid, f in absent, "update")
        if not wanted:
            continue
        table = self.component_table(ref)
        for f, by_value in wanted.items():
            col = table.c[f]
            rows = (await conn.execute(sa.select(table.c.id, col).where(col.in_(list(by_value))))).all()
            for found_id, found_value in rows:
                hit = by_value.get(_norm(f, found_value))
                if hit is None:
                    # 数据库按自身相等语义命中但本地对不上（如大小写不敏感 collation）：
                    # 单候选可归因，否则交给 UNIQUE 约束兜底
                    if len(by_value) != 1:
                        continue
                    hit = next(iter(by_value.values()))
                rid, is_race, op = hit
                if op == "update" and int(found_id) == rid:
                    continue   # 自身行：由 UPDATE 的版本条件报 Race
                msg = f"Unique violation {comp_cls.name_}.{f} id={rid} {op}"
                (race if is_race else strict).append(msg)
    if race:
        raise RaceCondition("RACE: " + race[0])
    if strict:
        raise UniqueViolation("UNIQUE: " + strict[0])
```

  `commit` 内 `dirties` 之后取 `absent_by_ref = idmap.get_absent_unique_fields()`；在 deletes 循环结束、
  updates 循环开始之间插入 `await self._check_unique_conflicts(conn, dirties, absent_by_ref)`。两处
  `IntegrityError → RaceCondition` **保留**，注释补"SELECT 检查与写入之间被并发抢先才会到这里，重试后
  SELECT 给出确定判定"。消息格式必须与 Lua 一致（`test_unique_commit_race` 靠 `match="UNIQUE"`）。
- [x] 验证：`HETU_TEST_BACKENDS=sqlite uv run pytest tests/test_backend_session_race.py tests/test_backend_session_basic.py tests/test_backend_client.py tests/test_system_executor.py -q` 全绿；
  有 Docker 再跑 `HETU_TEST_BACKENDS=postgres,mariadb ... tests/test_backend_session_race.py tests/test_backend_session_basic.py -q`。
  lint；commit `perf(sql): commit 内显式 SELECT 检查主键/unique 冲突，按 absent 标记判 Race/UniqueViolation`。

---

## Task 4: `repo.py` 去掉远程预检 + session 测试

**Files:** `hetu/data/backend/repo.py`；`tests/test_backend_session_basic.py`；`tests/test_backend_session_race.py`。

- [x] **重写 `test_insert_unique`**：删 141-144 行无效 mock（属性名不存在，从未生效）；本地重复段保持在
  `insert()/update()` 处抛、不改；"与库中既有数据冲突"段（167-189 行）改为 `with pytest.raises(UniqueViolation, match=...)`
  包住整个 `async with backend.session(...)`（`match="name"`、`match="time"`、update 段 `match="time"`），
  块内 `await item_repo.insert(row)` 注释"此处不再抛"。
- [x] **新增 4 个规则用例**（import 改 `from hetu.data.backend import Backend, RaceCondition, UniqueViolation`；
  每个测试的表是空的，直接用固定 time 值）：
  1. `test_unique_blind_insert_is_violation_no_retry`：库中已有 `name="taken"`；`async for attempt in session.retry(3)`
     内盲 insert `name="taken"` → 外层 `pytest.raises(UniqueViolation, match="name")`，`attempts == 1`。
  2. `test_unique_after_get_none_is_race_and_retries`：`only_master=True`，`get(name="x")` 读空 → 另一 session
     插 `x` → insert `x` → 退出抛 `RaceCondition`；再用 `retry(3)`：第 1 轮 `get(name="y")` 读空则另起 session
     插 `y` 再 insert，命中则 `qty += 1; update` → `attempts == 2`，最终 `qty == 2`。
  3. `test_unique_race_has_priority_over_violation`：库中已有 `time=100`；`get(name="anchor")` 读空 → 并发
     session 插 `anchor, time=101` → insert `anchor, time=100`（name 竞态 + time 确定性）→ `RaceCondition`；
     对照：盲 insert `fresh, time=100` → `UniqueViolation(match="time")`。
  4. `test_unique_explicit_id_pk_conflict`（`explicit_ids_only=True`）：库中已有 `id=-77`；盲 insert `id=-77` →
     `UniqueViolation(match=r"Item\.id")`；`get(id=-78)` 读空 → 并发插 `-78` → insert `-78` →
     `RaceCondition(match=r"Item\.id")`。
  说明：1～3 只断言类型和结果，预检在/不在都过（它们是规则的回归护栏）；4 靠消息 `Item.id` 在 Task 4 前红。
- [x] **改 `repo.py`**：`from .base import RowFormat, UniqueViolation`（`RaceCondition` 不再使用，去掉，否则 F401）；
  删 `_raise_unique_conflict`；`is_unique_conflicts` docstring 开头加"可选的提前检查（每字段 1 次往返）：
  insert/update 默认不再调用，等价判定在 commit 时由后端原子执行（见 `IdentityMap.get_absent_unique_fields`）"；
  `insert`/`update` 改为只做本地检查：

```python
# insert：保留 _version 断言与 explicit_ids_only 检查，然后
changed_fields = self._get_changed_fields(row)
assert "id" in changed_fields, _("session中已存在该row id({row_id})，插入操作必须没有旧数据。").format(row_id=row.id)
# 本地（同事务）unique 检查，0 往返；与库中既有数据的冲突由 commit 判定
if field := self._local_has_unique_conflicts(row, changed_fields & self.ref.comp_cls.uniques_):
    raise UniqueViolation(f"Insert failed: row.{field} violates a unique index (duplicate within transaction)")
self._session.idmap.add_insert(self.ref, row)
# update：原有 id/_version/无变更三项检查不动，把 is_unique_conflicts 调用换成同样的本地检查，消息 "Update failed: ..."
```

  docstring 说明新语义（commit 时：曾 `get` 观察不存在 → RaceCondition，否则 UniqueViolation；要提前确认可调
  `is_unique_conflicts`）；`UpsertContext.__aexit__` 512-515 行注释改指向 commit 判定与 `get_absent_unique_fields`。
- [x] **等值 `range` 读空也登记 absent**（用户拍板：与 `get` 对称，保住"用 range 做点查再写"的写法）。
  `repo.range` 在拿到 `row_ids` 后加：

```python
# 等值点查（left == right 闭区间，不带 "("/"[" 前缀）unique 列读空：与 get 一样登记 negative
# observation，让"先 range 确认不存在再写"的写法撞车时判竞态而非 UniqueViolation。
# 区间查询不登记：区间无穷且本就不保证事务内可见性。
if (
    not row_ids
    and index_name in comp_cls.uniques_
    and (_right is None or _left == _right)
    and not (isinstance(_left, (str, bytes)) and _left[:1] in ("(", "[", b"(", b"["))
):
    self._session.idmap.mark_absent(self.ref, index_name, _left)
```

  新增用例 `test_unique_after_range_none_is_race`：`only_master=True`，`range(name=("x", "x"), limit=1)` 读空 →
  另一 session 插 `x` → insert `x` → 退出抛 `RaceCondition`；对照：`range(time=(0, 1000))` 区间读空后另一
  session 插 `time=5` → 盲 insert `time=5` → `UniqueViolation`（区间不登记）。
- [x] `test_insert_after_get_none_is_race` 的 docstring/注释"insert 时"改"commit 时"，断言不动。
- [x] 验证：`HETU_TEST_BACKENDS=redis uv run pytest tests/test_backend_session_basic.py tests/test_backend_session_race.py tests/test_system_executor.py tests/test_system_future.py tests/test_headless.py tests/test_testing_sandbox.py -q` 全绿
  ——特别看 `test_unique_violate_bug`/`bug2`（日志有 `UniqueViolation`、无 `RaceCondition`，即 commit 判定不重试的验收）、
  `test_upsert`、`test_unique_remove_then_add_bug`、`test_session_insert_then_upsert`、`test_explicit_ids_only`、
  `test_update_or_insert_race`、`test_retry_generator`。再 `HETU_TEST_BACKENDS=sqlite` 跑同一组。
  lint；commit `perf(repo): insert/update 去掉远程 unique 预检（0 RTT），冲突由 commit 判定`。

---

## Task 5: docstring、文档、API 重生成、收尾

**Files:** `hetu/data/backend/base.py`（`RaceCondition` 60-76、`UniqueViolation` 79-96、`BackendClient.commit` 473-483）；
`hetu/data/backend/session.py`（`Session.commit` 91-99）；`hetu/system/context.py`（`session_commit` 46-48）；
`hetu/headless.py:331`；`docs/zh/_index.md`(96 后)、`docs/en/_index.md`(113 后)、`docs/zh/concepts.md:33`、
`docs/en/concepts.md:43-44`、`docs/zh/tutorial/chat-room.md:65`、`docs/en/tutorial/chat-room.md:67`；
`hetu/llms.txt:53`；spec §6；`todo.md`。

- [x] `UniqueViolation` docstring 重写：判定在两处——`insert/update` 只查本地（同事务重复立即抛）、`commit()`
  原子查主键与 unique（曾 `get` 观察不存在 → 改抛 `RaceCondition`；两类并存竞态优先）；消息含组件、字段、行 id、
  操作；要分支处理先 `get`，或调 `is_unique_conflicts`。`RaceCondition` 第 3/4 条触发场景合并为"主键/unique
  被占且本事务曾 `get` 观察其不存在（`upsert` 锚定字段是典型）"。`BackendClient.commit`、`Session.commit` 的
  Exceptions 段与 Task 2 的 Redis `commit` docstring 同文；`session_commit` 加 `UniqueViolation`（不重试）。
- [x] `hetu/headless.py:331` "get / range / unique 预检" → "get / range"。
- [x] 文档：`docs/zh/_index.md` 96 行后加子项"unique 冲突在提交时检查：本事务曾 `get`（或等值 `range`）观察其
  不存在的（如 `upsert` 锚定字段）判为竞态、自动重试；否则以 `UniqueViolation` 失败、不重试。要在事务内对'已存在'
  分支处理、或靠扫描推导 unique 值再写入，先 `get` 该值再写。"
  `docs/en/_index.md` 对应英文；`concepts.md` zh "插入时的唯一性检查"→"提交时的唯一性检查"、en "check on insert"→
  "check at commit"；chat-room 教程 zh "在插入时强制唯一性"→"在提交时强制唯一性"、en "at insert time"→"at commit time"。
  `hetu/llms.txt:53` Exceptions 条目补 `UniqueViolation`。（`scripts/translate_new_content.py` 依赖外部 API，
  这几处英文手改。）
- [x] `uv run python scripts/gen_api_docs.py`；`git diff --stat docs/api` 应只涉及 `exceptions.md`、`system.md`、`headless.md`。
- [x] spec `docs/superpowers/specs/2026-09-20-unique-check-at-commit-design.md`：§3.5 "`get()` 不动"改为
  "`get()` 不动；等值 `range` 读空也登记 absent"；§6 补 `test_unique_commit_race` 改期望与
  `test_unique_after_range_none_is_race`；状态改"已实施"。`todo.md` 第 12 条 `[x→spec]` → `[x]`。
- [x] 全量回归：`HETU_TEST_BACKENDS=redis,redis_cluster,valkey,postgres,sqlite,mariadb uv run pytest tests -q`
  （需 Docker 全家桶；至少 `redis,sqlite`；全量约 6 分钟）。lint；commit `docs: unique 冲突改为提交时判定——异常/commit docstring、_index/concepts/tutorial 中英、API 重生成`。

---

## 各步会变红的现有测试

| 步骤 | 测试 | 原因 | 处理 |
|---|---|---|---|
| Task 2 | `test_backend_client.py::test_redis_commit_payload` | checks 元组 2/4 元 → 4/6 元 | 同任务先改期望 |
| Task 2（Redis）/ Task 3（SQL） | `test_backend_session_race.py::test_unique_commit_race` | 盲写撞并发提交：`UNIQUE` 现映射 `UniqueViolation` | 改期望 |
| Task 4 | `test_backend_session_basic.py::test_insert_unique` 第二段 | 异常从 `insert()` 挪到 `async with` 退出 | 同任务重写 |
| 不变 | `test_insert_after_get_none_is_race`、`test_update_or_insert_race`、`test_retry_generator`、`test_unique_violate_bug`/`bug2`、`test_explicit_ids_only`、`test_unique_remove_then_add_bug`、`test_unique_batch_add_in_same_session_bug` | 类型与传播路径不变（`RetryAttempt`/`SystemCaller` 的 `except RaceCondition` 同样包住 commit） | 回归 |

## 风险与注意

- msgpack：`msg_packer = Packer(use_bin_type=False)`，str 与 bytes 都打成 raw，cmsgpack 解成 Lua string，`code`/`label`
  与 `seen_uniq` 键拼接无问题；不要改 packer 参数。
- SQL 只能对 update 行跳过"自身行"；insert 行 `id` 查到自身恰是主键冲突（headless 显式 id 场景），不能跳。
- NX 与 UNIQ 的 Lua 消息措辞不同（`Key already exists` / `Unique violation`），主键冲突用例用 `match=r"Item\.id"`。
- `get_by_id()` 不登记 absent（只有 `get()` 登记），`get_by_id(x)` 读空后 insert 同 id 撞主键判 `UniqueViolation`，
  与现有 `observed_absent` 语义一致。
- 雪花 id 撞主键（worker id 重复等 bug）现在是 `UniqueViolation` 硬失败而非换号重试——spec §4 有意为之。
- MariaDB 大小写不敏感 collation 等"库认为相等、本地不相等"的多候选场景退回 `IntegrityError → RaceCondition`
  兜底重试到 `max_retry`；属 spec §7 记录的边界。

## 端到端验证

1. 上述各任务的 pytest 命令 + 收尾全后端回归。
2. RTT 复测：`docker run -d --rm --name hetu_rtt_probe -p 23999:6379 redis:latest`，然后
   `PYTHONUTF8=1 uv run python "C:\Users\heero\AppData\Local\Temp\claude\C--xsoft-HeTu\9a9cfb4f-b977-4a70-8dcf-f4599bc33fff\scratchpad\rtt_probe.py"`
   （探针挂在 redis-py 的 `execute_command`/`Pipeline.execute` 上）。期望：insert 无论几个 unique 字段 = 1
   （`EVALSHA`），`upsert(name=)` 插入路径 = 2，SystemLock 式 = 3，`get(id=)+update 非 unique` 仍 = 2；
   完事 `docker rm -f hetu_rtt_probe`。
3. `uv run python scripts/gen_api_docs.py` 后检查 `docs/api/system.md` 里 `insert` 段不再出现 `_raise_unique_conflict`。
