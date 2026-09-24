# `range` 提交时校验区间（防幻读）— 设计稿

- 日期：2026-09-24
- 状态：实施中（§9 各项已定）
- 分支：`feat/range-phantom-check`（基于 dev `9de231c`）
- 影响范围：`hetu/data/backend/repo.py`（`range` 记录区间观察、新参数）、
  `hetu/data/backend/idmap.py`（存观察、跳过规则、一致性标记）、
  `hetu/data/backend/redis/client.py` + `commit_v2.lua`（读取带回 member、commit 前一致性核对、
  `CNT` 检查）、`hetu/data/backend/sql/client.py`（读取记录查询参数、commit 内重跑比对）、
  `hetu/data/backend/base.py`（新接口、异常 docstring）、两个后端的 `range_normalize_`
  （前置修复 desc 开闭）、`hetu/endpoint/connection.py` 与 `hetu/system/future.py`（关闭开关）、
  `benchmark/redis_commit_cost.py`（新 payload 变体）、文档与测试。
- **不改**：客户端协议、订阅语义、schema、`get(id=)` 路径、只读事务的行为。

## 1. 背景与目标

### 1.1 问题

用户最自然的"查不到就插、查到就改"写法，今天是错的：

```python
@define_component(namespace="game", permission=Permission.OWNER)
class Item(BaseComponent):
    owner: np.int64 = property_field(0, index=True)
    template: np.int32 = property_field(0)
    qty: np.int32 = property_field(0)


async def add_item(ctx, tpl: int, n: int):
    items = await ctx.repo[Item].range(owner=(ctx.caller, ctx.caller), limit=-1)
    hit = items[items.template == tpl]
    if len(hit) == 0:
        row = Item.new_row()
        row.owner, row.template, row.qty = ctx.caller, tpl, n
        await ctx.repo[Item].insert(row)
    else:
        row = hit[0]
        row.qty += n
        await ctx.repo[Item].update(row)
```

commit 只校验"读到的行"的版本（纯读行 `VER`，`redis/client.py:1024-1029`），"区间里没有某行"
这件事没有东西可校验：

1. **并发**：两个事务都读到"没有"，各插一行，都提交成功。
2. **副本滞后**（更常见）：`range` 走 `master_or_servant`（`repo.py:351`），同一玩家连续两次
   调用，第二次可能读到还没同步第一次插入的副本，单线程也会插出重复。HeTu 没有 read-your-writes。

现在文档的说法是"防幻读请使用 `unique` 约束代替索引检查"（`docs/zh/_index.md:94-96`）。这对
"每人每模板一行"有效（组合键 unique + `upsert`），但用户想不到；计数类约束（背包上限、每人最多
一个进行中的任务、同 IP 连接数）也表达不了。

### 1.2 目标

- 写事务里的每次 `range` 读，commit 时若"同样的查询现在会返回不同的行"，判 `RaceCondition`
  自动重试。上面的朴素写法直接变对，副本滞后一并挡住。
- 只读事务零成本（它们不 commit）。不增加往返：校验放进现有那一次 EVALSHA / SQL 提交事务。
  master 上每个被校验的区间一次 O(log N)。最常见的形状（`get(id=)`、`get(unique=)`、`upsert`）
  不增加 master 开销。
- 默认开启，热点区间可显式关闭。

### 1.3 非目标

组合 unique 索引、改 `range` 默认 `limit`、订阅、SQL 后端的严格隔离（见 §8）。

## 2. 已确认的事实（均已核对）

- **读路径**：`SessionRepository.range`（`repo.py:270-400`）先 `master_or_servant.range(..., ID_LIST)`
  拿 id，再对缓存未命中的 id 调 `master_or_servant.get_many`。两次各自随机选节点，可能不是同一个。
  `get_many` 读不到的行（ZRANGE 与读行之间被删）直接跳过（`repo.py:386-393`）。命中缓存的行用
  缓存里的（含本事务修改，DELETE 状态排除）。
- **`get` 与 absent**：非 unique 列的 `get` 就是 `range(limit=1)`（`repo.py:248`）。unique 列
  `get` 读空、unique 列等值 `range` 读空会 `mark_absent`（`repo.py:251-255`、`:355-363`），
  commit 时该值的 `UNIQ` 冲突判 RACE。
- **Redis commit**：Lua Phase 1 顺序执行 checks、首个失败即返回，checks 全部先于 pushes
  （`commit_v2.lua`）。Python 侧 `race_checks` 排在 `strict_checks` 之前
  （`redis/client.py:930-933`、`:1031`）。纯读行逐行 `VER`（HGET）（`:1024-1029`）。
- **Redis 索引结构**：每个索引一个 zset，score 全 0，member 为
  `to_sortable_bytes(值) + b"\x00" + ascii(id)`，按 BYLEX 查（`redis/client.py:888-913`、
  `range_normalize_` `:606-657`）。索引 key 与行 key 同一 `{CLU}` hash tag，Lua 已在 `UNIQ`
  里直接读索引 key（cluster 下同 slot）。
- **SQL**：`range` 按 `(col, id)` 排序，desc 时两列都 desc（`sql/client.py:1031-1038`）。commit 在
  `aio.begin()` 里依次执行：纯读行版本 SELECT（普通 SELECT、不加锁）→ delete → unique 检查 →
  update → insert（`sql/client.py:1162-1287`）；缺表时建表后整段重试一次（`:1328-1339`）。
  float32 列映射为 `sa.Float(precision=24)`（`:65`），在 MariaDB 上是单精度 FLOAT，拿读回的值做
  等值比较不可靠。
- **重试**：System 由 `SystemCaller.call_` 捕获 `RaceCondition` 重试（默认 9999 次，
  `caller.py:100-155`）；`Session.retry()` 同理；裸 `async with session` 直接抛给调用方。
  `Session.commit` 只在有脏数据时提交（`session.py:104-105`）。
- **框架内部在写事务里调 `SessionRepository.range` 的三处**：
  - `endpoint/connection.py:61` `new_connection`：同 IP 匿名连接数上限检查，裸 session、**无重试**。
  - `system/future.py:343` `pop_upcoming_call`：取最早到期的未来调用，`retry(2)`，异常由任务循环
    记日志后退避（`future.py:471-479`）。
  - `system/lock.py:48` `clean_expired_call_locks`：删 7 天前的 `SystemLock`，裸 session、无重试，
    每个 worker 启动时都跑。

  其余调用（`sub.py`、`testing/__init__.py`、`repo.remote_has_unique_conflicts_`）是 client 直接读
  或只读 session，不受影响。
- **成本数据**（`benchmark/redis_publish_cost_result.md`）：commit 里一次 `redis.call` 的调度约
  3k 条指令（与 `EXISTS` 相当）；单行 commit 约 6.7~8.5 万条；payload 里的字符串进 Lua 也要花指令。
  该报告规则 5：改了 commit 路径要跑 `benchmark/redis_commit_cost.py replay`，加一个 payload 变体对比。
- **测试环境**：redis / valkey fixture 带一个副本（`tests/fixtures/backends.py:23-28`），默认
  `master_weight=1.0` 与副本等权，测试里同样有副本滞后。master 读预算与审计工具只统计 client 的
  `get` / `get_many` / `range` 调用（`tests/fixtures/read_counts.py`、`tools/master_read_audit.py`），
  commit 内的校验不在统计口径内。
- **既有 bug**：两个后端的 `range_normalize_` 在 `desc=True` 且两端开闭不同时开闭互换，
  `left="(5", right="10"` 实际查成 `[5, 10)`。已直接调用两边的 `range_normalize_` 验证；两端同开
  或同闭时正确。位置：`redis/client.py:649-652`、`sql/client.py:735-736`。订阅侧的区间查询也走这里。

## 3. 设计

### 3.1 判定规则

对写事务里每次开启校验的 `range` 读 R，定义它的**观察区间**：

- 未截断（`limit < 0`，或返回 id 数 < `limit`）：整个查询区间。
- 截断（返回 id 数 == `limit`）：从查询起点到最后一个返回行（按索引顺序，含该行）。截断时本事务
  看到的只是"前 limit 行"，区间后面新增的行本来就不在它的观察里，不算冲突。

commit 时，观察区间内的行集合必须仍等于 R 读到的 id 集合，且这些行都没被改过（后者就是现有
`VER`），否则判 `RaceCondition`。`limit == 0` 或关闭开关时不记录观察。

### 3.2 Redis：为什么"计数不变 + 版本不变"就够

设 R 在索引快照里读到 member 集合 S（k 个），观察区间为 I。若 commit 时（Lua 原子执行，记为 t_c）：

- (a) S 里每一行的 `VER` 通过；
- (b) 本事务手里每一行的数据（即 `VER` 钉住的那个版本）与快照里的 member 一致；
- (c) `ZLEXCOUNT(I) == k`，

那么由 (a)(b)，t_c 时 S 的每个 member 仍在 I 内；再由 (c)，I 内恰好是 S。任何"删掉 / 改走一行"
都会让 (a) 失败，任何"新进一行"都会让 (c) 失败。

(a)(c) 在 master 上做，(b) 是 worker 上的纯计算。(b) 不能省：ZRANGE 与随后的 `get_many` 不原子，
还可能打到不同节点。中间有行被改走的话，"一出一进"能让 (a)(c) 都通过，本事务却漏看了新行。

取行时读不到的 id（中间被删），直接判不一致，不去推算期望计数。截断读时少了一行，"前 limit 行"
的观察就失真了；统一判不一致最简单。这个窗口只有亚毫秒，多一次重试可以忽略。

### 3.3 读取侧：`SessionRepository.range`

签名加一个关键字参数（名字待拍板，§9）：

```python
async def range(self, index_name=None, _left=None, _right=None, limit=10,
                desc=False, phantom_check: bool = True, **kwargs) -> np.recarray
```

`phantom_check=True` 且 `limit != 0` 时，改调后端新接口 `range_read_`（§3.5 / §3.6），拿回 id
列表和一个 `RangeObservation`。其余流程（缓存命中、`get_many` 批量取、`mark_absent`）不变。之后：

- `get_many` 有 id 读不到 → `obs.missing = True`；
- unique 列的等值点查（判定同 `point_query_value_`）→ `obs.point = 规范化后的值`，供 §3.7 用；
- `idmap.add_range_observation(self.ref, obs)`。

`phantom_check=False` 走原来的 `client.range(..., ID_LIST)`，不记录观察。返回的行照样进 IdentityMap、
照样做 `VER`，只是不管区间里新增的行。

`get()` 内部调 `range(limit=1)`，不开放开关（§8）。

### 3.4 IdentityMap

```python
@dataclass(slots=True)
class RangeObservation:
    index_name: str
    ids: list[int]  # 索引快照里读到的 id（按索引顺序），len 即期望计数
    bounds: tuple  # 后端相关的校验参数，commit 原样取用（§3.5 / §3.6）
    members: list[bytes] | None = None  # Redis：ZRANGE 原样 member；SQL 为 None
    point: object | None = None  # unique 列等值点查的值
    missing: bool = False  # 取行时有 id 读不到
```

- `add_range_observation(ref, obs)`：与 `add_clean` 一样断言同一事务组，而且观察也参与组的判定。
  否则一个只 range 了别的 cluster 空表的事务，会在 cluster 下拼出跨 slot 的脚本。同一
  `(ref, index_name, bounds)` 重复观察时，`ids` 相同就去重，不同就记为不一致（同一事务两次读
  同一区间，结果不同）。
- `range_observations()`：返回全部观察 `{ref: [obs]}`。
- `inconsistent_range()`：有 `missing` 或重复观察不一致时返回定位串，否则 None。两个后端 commit
  开头都先调它，非 None 就直接抛 `RaceCondition`，不连数据库。
- `db_row(ref, row_id)`：该行在本事务里的数据库态，即 `_row_clean` 里的副本。INSERT 行或不在缓存
  时返回 None。
- `implied_by_unique(ref, obs)`：§3.7 的 S1 / S2，仅 Redis 使用。

### 3.5 Redis

**读取** `range_read_(ref, index_name, left, right, limit, desc)`：复用 `range_normalize_` /
`make_zrange_cmd_` 发同一条 ZRANGE，保留 member：

```python
members = await aio.zrange(name=idx_key, **self.make_zrange_cmd_(b_left, b_right, desc, limit))
ids = [int(m.rsplit(b"\x00", 1)[-1]) for m in members]
lo, hi = (b_right, b_left) if desc else (b_left, b_right)  # ZLEXCOUNT 要 min, max
if 0 < limit == len(members):  # 截断：收到最后一个 member
    if desc:
        lo = b"[" + members[-1]
    else:
        hi = b"[" + members[-1]
return ids, RangeObservation(index_name, ids, (lo, hi), members=members)
```

**commit 前一致性核对**：在 worker 上、发 EVALSHA 之前做 §3.2 (b)。对每个观察逐个 `(id, member)`：

- 行状态为 INSERT 的跳过，交给 `NX` 判定。否则盲插一个已存在的显式 id 会从 `UniqueViolation`
  变成无限 RACE。
- 其余取 `idmap.db_row`：为 None，或 `to_sortable_bytes(dtype.type(row[index]))` 不等于 member 的
  值段，就抛 `RaceCondition("RACE: Inconsistent range read Item.owner id=123")`，不发往 master。

**checks**：每个不被 S1 / S2 覆盖的观察追加一条

```
["CNT", idx_key, lo, hi, count, label]    # label 如 "Item.owner"
```

放在 `race_checks` 末尾：

- 在现有 `VER` / `NX` / `UNIQ(RACE)` 之后，现有冲突的报错信息不变；
- 在 `strict_checks` 之前：基于过时区间做的决定撞上 unique 时应该重试，而不是报 `UniqueViolation`。

**Lua**（Phase 1 新分支）：

```lua
-- 检查区间行数（防幻读）
-- 格式: ["CNT", index_key, min, max, expected_count, label]
elseif op == "CNT" then
    if redis_call("ZLEXCOUNT", check[2], check[3], check[4]) ~= check[5] then
        return "RACE: Range changed " .. check[6]
    end
```

ZLEXCOUNT 是读命令；脚本按效果复制，它不进复制流。

### 3.6 SQL

**读取** `range_read_`：把 `range` 里构造 `SELECT id ... WHERE ... ORDER BY ... LIMIT` 的部分抽成
`_range_id_stmt(table_ref, index_name, left, right, limit, desc)`，读取与 commit 共用。观察记录原始
参数 `bounds = (left, right, limit, desc)`，`members=None`。

**commit**：在纯读行版本检查之后、delete 之前（这样看到的是本事务写入前的状态），对**每个**观察
用 `_range_id_stmt` 重跑，`set(结果) != set(obs.ids)` 就抛 `RaceCondition("RACE: Range changed Item.owner")`。

SQL 比较精确的 id 集合，不像 Redis 那样只比计数，原因有三：

- 截断时若用 `(col, id) <= (最后值, 最后 id)` 计数，要对读回的列值做等值比较。MariaDB 单精度
  FLOAT 对不上，会变成**每次都失败的无限重试**。重跑同一条语句没有这个问题。
- 精确集合也覆盖了"读取中途行被改走 / 删掉"，SQL 不需要 §3.2 (b) 的核对，只保留 `missing` 判定。
  也因此 S1 / S2 不用于 SQL：跳过重跑就丢了这层保护。
- SQL 后端的定位是开发 / 低负载，O(k) 的重跑可以接受。

缺表处理：观察涉及的表并入 `refs`，缺表重试时一起建。

**隔离强度**：与现有纯读行检查一样，是提交事务里的普通 SELECT。PG / MariaDB 默认隔离级别下，
"检查之后、提交之前"仍有窄窗口。记录在案，不在本 spec 解决（§8）。

### 3.7 跳过规则：已有检查能推出结论时不发 CNT（仅 Redis）

只对 unique 列的**等值点查**观察生效（`obs.point is not None`）：

- **S1 点查命中**：计数 1，且命中行在 IdentityMap 里有数据库态（会做 `VER`）。命中行的 `VER` 与
  (b) 保证它在 t_c 仍是这个值，unique 保证不会有第二行，区间不可能变。
- **S2 点查读空**：计数 0，本事务有 INSERT 行、或改了该列的 UPDATE 行，把该列写成了这个值，
  并且本事务没有删除任何数据库态为这个值的行。

  这个值已有一条带 RACE 标记的 `UNIQ` 检查（读空时登记过 absent），它在 t_c 保证"除本事务删除的
  行外，没有这个值"；再排除"删了这个值的行"，就等价于计数 0。反过来，若本事务删了这样的行，
  说明它先看到"没有"、后又读到"有"，读集本身不一致，应当保留 CNT 让它 RACE。

S1 / S2 覆盖了 `get(unique=)` → update、`upsert` 的两条路径，以及 `SystemLock` 的
`get(uuid=)` + `upsert(uuid=)`，这些形状的 commit 因此不增加 master 开销。§3.2 (b) 的核对与
`missing` 判定对所有观察照做（只花 worker）。

### 3.8 关闭开关与框架内部调用点

- `new_connection`（`connection.py:61`）→ `phantom_check=False`：
  - 这是防攻击的粗略计数，不需要精确；
  - 它没有重试，同 IP 并发连接一多就会互相 RACE、连接失败；洪峰时重试还会放大负载。
  - 这样 docstring 里"此方法不会事务冲突"继续成立。
- `pop_upcoming_call`（`future.py:343`）→ `phantom_check=False`：选"最早到期的一条"不依赖区间里没有
  别的行，抢同一条由 `VER` 管。不关的话，并发创建已到期的调用会让 `retry(2)` 更容易耗尽、刷错误日志。
- `clean_expired_call_locks`（`lock.py:48`）不改：区间是 7 天前，新插入的锁 `called=now`，落不进来。
  它无重试、多 worker 同时删会 `VER` 冲突，这是既有问题，与本 spec 无关（§8）。

### 3.9 前置修复：desc 区间开闭

观察区间直接复用 `range_normalize_` 的边界，所以先把它修对（可单独一个提交）：

- Redis（`redis/client.py:649-652`）：按"上界 / 下界"角色取后缀，不再在 desc 时交换后缀。desc 时
  `left` 是上界、`right` 是下界：

  ```python
  if desc:
      ls = b"\x00\xff" if li else b"\x00"  # 上界：闭 → 含该值全部 id
      rs = b"\x00" if ri else b"\x00\xff"  # 下界：闭 → 从该值第一个 id 起
  else:
      ls = b"\x00" if li else b"\x00\xff"
      rs = b"\x00\xff" if ri else b"\x00"
  ```

- SQL（`sql/client.py:735-736`）：删掉 `if desc: li, ri = ri, li`。值交换之后，`li` 已经属于上界
  `left`，`range` 与 `clamp_uint64_bounds_` 也都按这个约定使用。

订阅侧的区间查询也走这两个函数，一并修正。

### 3.10 错误信息

- 区间变化：`RACE: Range changed Item.owner`（Redis 由 Lua 返回，SQL 拼成同样形式）。
- 读取不一致：`RACE: Inconsistent range read Item.owner id=123`（两个后端都在连数据库前抛出）。
- 建议 `SystemCaller` 的"遇到竞态"debug 日志带上异常消息，方便用户从日志里找出需要关校验的
  热点区间（可选，一行改动）。

## 4. 正确性与并发分析

| 场景 | 现在 | 改后 |
|---|---|---|
| range 读空 → 并发插入同 owner → 本事务 insert | 两行都提交（重复） | `CNT` 失败 → 重试 → 读到 → update |
| 上次调用刚插入，本次 range 读到滞后副本 → insert | 重复 | master 计数不符 → 重试 |
| 并发插入落在查询区间外 | 通过 | 通过 |
| 截断读，插入落在最后一个返回行之后 | 通过 | 通过（不在观察内） |
| 截断读，插入落在最后一个返回行之前 | 通过（幻读） | RACE |
| 背包上限：range 后 `len < cap` 才插 | 并发下可超限 | RACE → 重试时看到已满 |
| 本事务删除 / 修改返回的行 | 通过 | 通过（Redis checks 先于 pushes；SQL 先校验后删） |
| 返回的行被并发删除 / 改走 | RACE（VER） | RACE（VER，信息不变） |
| ZRANGE 与取行之间行被改走 / 删除 | 结果里可能有不满足条件的行、或少一行 | 写事务 RACE；只读事务不变 |
| 只读 System | 不校验 | 不校验 |
| 热点区间：读最新 N 条再插入 | 通过 | RACE 增多 → `phantom_check=False` |
| `get(unique=)` → update / `upsert` / SystemLock | 现有规则 | 不变，且不加 CNT（S1 / S2） |
| 非 unique 列 `get` 为 None → insert | 并发下重复 | RACE → 重试 |
| 过时区间 + 盲写撞 unique | `UniqueViolation` | 先 RACE 重试一次，区间一致后再 `UniqueViolation` |
| 盲插已存在的显式 id，同时 range 到它 | `UniqueViolation` | 同（INSERT 行不参与 (b) 核对） |

- **重试收敛**：RACE 之后重新读区间，拿到新快照；只有同一区间被持续写入时才会反复冲突（热点区间），
  这正是要提供开关的原因。确定性冲突最多多一次重试，不会无限重试。
- **优先级**：CNT 排在现有 race 检查之后，现有测试断言的 `match="Version"` 等信息不受影响。
- **cluster**：索引 key 与行 key 同 slot；观察参与事务组判定（§3.4），不会跨 slot。
- **副本**：读仍全部走 `master_or_servant`，不新增 master 读调用。滞后副本造成的不一致在 commit 时
  变成 RACE，这正是目标之一。

## 5. 代价

- **master（Redis）**：每个需校验的观察多一条 `CNT`，包括一次 `redis.call` 调度（约 3k 指令）、
  跳表 O(log N)、payload 里几个短字符串，合计约 1 万条指令量级。对单行 commit（6.7~8.5 万）是
  +10%~20% 量级，**待实测**。
  - 只有"写事务 + 做了 S1 / S2 覆盖不到的 range"才付。
  - 扫描型事务本来就为每个读到的行付一条 `VER`，多一条 `CNT` 占比很小。
  - S1 / S2 覆盖的常见形状为 0。
  - 可选优化：`label` 不进 payload，由 Lua 返回失败检查的序号、Python 还原定位串，省一个字符串；
    用 benchmark 决定要不要做。
  - 实施时按报告规则 5，在 `redis_commit_cost.py replay` 里加一个带 `CNT` 的变体，与 `optin` 对比，
    数字写进 §10。
- **worker（Redis）**：写事务 commit 前，对观察到的每行算一次 `to_sortable_bytes` 并比较，O(k)。
  只读事务不付。
- **SQL**：每个观察在提交事务里重跑一次 id 查询，O(log N + k)。
- **往返不变**：Redis 仍是一次 EVALSHA，SQL 仍是一个提交事务。

## 6. 用户可见变化与文档

- **行为变化**：写事务里的 `range`（及非 unique 列的 `get`），在区间被并发写入、或读到滞后副本时，
  commit 会抛 `RaceCondition`。System 自动重试；裸 `Session` 用户看到的异常与其他 RACE 相同。
- `SessionRepository.range` docstring：
  - 新增 `phantom_check` 说明；
  - 截断语义：只保护看到的前 limit 行；
  - 提示"用 range 判断存在性，必须保证没被截断（`limit=-1`，或检查 `len(rows) < limit`）"。
- `get` 的 docstring 补一句：非 unique 列同样会在提交时校验。
- `base.py` 的 `RaceCondition`，以及 `BackendClient.commit` / `Session.commit` /
  `SystemContext.session_commit` 的 Exceptions 段，补上"range 读过的区间有变化"。
- `docs/zh/_index.md:94-96` 改写这条"例外"：
  - range 除了返回的行，还会在提交时校验查询区间，往区间里新增行（幻读）也会冲突重试；
  - 截断读只保护看到的前 limit 行；
  - 热点区间、且逻辑不依赖"区间里没有别的行"的，传 `phantom_check=False`；
  - 去掉"防幻读请使用 unique 约束代替索引检查"。
- `docs/zh/concepts.md:112`："检查每个行的版本"改为"检查每个读写过的行的版本，以及 range 读过的区间"。
- `docs/zh/advanced.md` 新增小节"查不到就插入：两种写法"：
  - 朴素的 range 写法现在是正确的，但 master 成本随读到的行数线性增长；
  - 热路径推荐 unique 锚定 + `upsert`，只碰一行。附组合键例子（`slot = f"{owner}:{template}"`）
    及注意事项：换主人时同步改键、老数据先回填。
- en 文档由 `scripts/translate_new_content.py` 同步；`docs/api/*.md` 用
  `uv run python scripts/gen_api_docs.py` 重新生成。

## 7. 测试计划

并发先后一律用嵌套 session 排定，不靠 sleep（沿用 `test_version_race` 的做法）。除注明外，各后端
参数化（`mod_auto_backend`）。

`tests/test_backend_session_race.py`（新增）

1. `test_range_phantom_insert_is_race`：外层 `range(owner=(x, x))` 读空 → 内层插入 owner=x 并提交 →
   外层 insert → `RaceCondition(match="Range")`。
2. `test_range_phantom_retry_converges`：朴素 `add_item` 用 `Session.retry` 包住，中途插入同模板行 →
   最终只有一行，数量是两次之和。
3. `test_range_insert_outside_no_race`：内层插入 owner=y → 外层正常提交。
4. `test_range_truncated`：用取值互不相同的索引（`time`），`limit=2` 读到两行（区间内还有更多）。
   内层插入排在最后返回行之后 → 通过；排在之前 → RACE。asc、desc 各一组。
5. `test_range_own_delete_no_false_race`：外层 range 后删掉其中一行、再 insert → 正常提交。
6. `test_range_phantom_check_off`：`phantom_check=False` 下重复用例 1 → 正常提交（同现在）。
7. `test_range_read_only_unaffected`：只读事务中途被插入 → 不抛。
8. `test_range_missing_row_is_race`：包住 `get_many`，在它执行前让内层删掉区间里的一行 → 外层写入后
   提交 → `RaceCondition(match="Inconsistent")`；只读事务不抛。
9. `test_range_moved_row_is_race`（Redis）：同上，改成把区间内一行的索引值改走、再插入一行新行
   （"一出一进"）→ `RaceCondition`。
10. `test_nonunique_get_none_then_insert_is_race`：`get(owner=x)` 为 None → 内层插入 → 外层 insert → RACE。
11. `test_range_float32_index_truncated_no_livelock`（SQL 重点，mariadb 必跑）：float32 索引上的截断读，
    无并发写 → 一次提交成功。

`tests/test_backend_client.py`

- `test_redis_commit_payload`：增加一个 range 观察，断言 `["CNT", idx_key, lo, hi, count, label]` 的格式
  与位置（在全部现有 race 检查之后、strict 检查之前）；截断时 `hi == b"[" + 最后 member`（desc 时是 `lo`）。
- S1 / S2：`get(unique=)` 命中后 update、`upsert` 插入路径、`upsert` 更新路径 → payload 里没有 `CNT`；
  S2 的反例（同时删了一行该值的行）→ 有 `CNT`。
- Lua：直接构造一条计数不符的 `CNT` → 返回串以 `RACE: Range changed` 开头（Redis 专属）。

`tests/test_backend_session_basic.py`

- desc 开闭修复：`range(time=("(110", "115"), desc=True)`、`("110", "(115")`、两端都开、两端都闭，各后端
  结果与对应 asc 查询的集合相同、顺序相反。
- 同一事务两次读同一区间、中间被插入 → commit RACE（重复观察不一致）。

框架内部

- `new_connection`：设置 `MAX_ANONYMOUS_CONNECTION_BY_IP`，并发 N 个同 IP 连接 → 无 `RaceCondition`。
- `pop_upcoming_call`：现有 `tests/test_system_future.py` 回归。

回归

- 全量跑 `HETU_TEST_BACKENDS=redis`。重点看"写完不等同步、在另一个事务里 range + 写"的用例：副本滞后
  现在会让它们 RACE，这正是新检查要挡的情况。按用例补 `wait_for_synced()` / `only_master` / 重试包装，
  不改断言。然后再跑一遍全部后端。
- `tests/test_arch_master_reads.py`、`tests/test_master_read_budget.py` 应该不受影响（没有新增 client 读调用）。

## 8. 取舍与边界（YAGNI）

- **Redis 不在 Lua 里精确比对 member 集合**：那样可以省掉 §3.2 (b)，但 master 要多花 O(k)（ZRANGE +
  k 次比较 + payload 带 k 个 member）。本项目 master CPU 是瓶颈，所以把 O(k) 放到 worker，master 只做
  O(log N)。
- **`get()` 不开放开关**：unique 列由 S1 / S2 覆盖；非 unique 列的 `get` 是 `limit=1` 的截断读，只有插入
  排在第一行之前才冲突，很少发生。有需要再加。
- **不改默认 `limit=10`**：截断导致的漏查是单线程下的确定性 bug，本检查救不了，靠文档（§6）。以后可以
  考虑在"写事务里截断读之后、往同一区间插入"时打 debug 警告。
- **不做组合 unique 索引**：与本 spec 正交；unique 锚定 + `upsert` 已能表达"每人每模板一行"。
- **SQL 隔离窗口**：与现有纯读行检查同等强度，SQL 后端定位是开发 / 低负载。以后要收紧可以考虑：
  MariaDB 用锁定读的间隙锁，PG 用 SERIALIZABLE。
- **`clean_expired_call_locks` 无重试**：既有问题，另提。
- **观察去重粒度**：按 `(ref, index, bounds)` 精确匹配，不合并重叠区间。一个事务读同一张表的很多区间
  时，每个区间一条 `CNT`，量级与这些读本身的 `VER` 相当。

## 9. 已定事项

1. **默认开启**：重复发道具是悄无声息的经济 bug，多出来的重试至少能在慢日志的 race_count 里看到。
2. **参数名** `phantom_check`。
3. **做 S2**：它覆盖了 SystemLock 与 upsert 插入这些高频路径。
4. **§3.9 的 desc 修复**不单独提 PR，在本分支顺手修掉。

## 10. 实测（实施后补）

`redis_commit_cost.py replay`：`optin` vs `optin + 1 CNT` 的每次 commit 主线程 µs / 指令数。

## 11. 主要改动文件清单

- `hetu/data/backend/idmap.py`：`RangeObservation`、`add_range_observation`、`range_observations`、
  `inconsistent_range`、`db_row`、`implied_by_unique`；观察参与事务组判定。
- `hetu/data/backend/repo.py`：`range(phantom_check=True)`，记录观察与 `missing` / `point`；docstring。
- `hetu/data/backend/base.py`：`range_read_` 接口；`RaceCondition` 与 commit 的 Exceptions docstring。
- `hetu/data/backend/redis/client.py`：`range_read_`、commit 前一致性核对、`CNT` 检查、`range_normalize_`
  desc 修复。
- `hetu/data/backend/redis/commit_v2.lua`：`CNT` 分支。
- `hetu/data/backend/sql/client.py`：抽出 `_range_id_stmt`、`range_read_`、commit 内重跑比对、缺表
  refs、`range_normalize_` desc 修复。
- `hetu/endpoint/connection.py`、`hetu/system/future.py`：`phantom_check=False`。
- `hetu/system/caller.py`（可选）：竞态 debug 日志带上异常消息。
- `benchmark/redis_commit_cost.py`：新增带 `CNT` 的 payload 变体。
- 测试：§7。文档：§6。
