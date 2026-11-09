# 提交时不校验的读（`repo.untracked`）设计稿

- 日期：2026-11-09
- 分支：`feat/untracked-reads`（从 origin/dev `fce72f5b` 拉；#217 合并后跟着前移到 `6fcf08ad`，核对见 §14）
- 状态：**搁置**（2026-11-09），等有具体的服务端用例再做，原因见 §15。此前已过两路独立审核（§13）、§11 三项用户已定；
  §5.1、§5.2 两项顺带修复和本功能无关，从 dev 另做（§15.4）
- 来源：commit_seq 设计稿（`2026-11-09-commit-seq-design.md`）§10、§14.4。服务端消费方每读一行，提交时 master 上多一条 VER；用户定"以后单独做，get 和 range 一起做"
- 用语：接口叫 `untracked`，用户文档里叫"提交时不校验的读"（贴近现有文档的说法）

## 1. 背景

事务里的读，读到什么都会登记，提交时校验：

| 读法 | 登记 | 提交时 |
|---|---|---|
| 读回的行（`get_by_id` / `get` / `range` 从库里取回的） | 放进缓存（`IdentityMap.add_clean`），记下读取时的 `_version` | master 上每行一条 VER（`get_clean_rows`），版本对不上判 `RaceCondition` |
| `range` 的区间（`phantom_check=True`，默认） | 区间观察（`add_range_observation`） | worker 上先做读取一致性核对（§4.4）；master 上一条 CNT：区间里的行和读到的不一样就判竞态。unique 点查已由别的检查保证时免掉（`_implied_by_unique`），被别的观察包含的去重（`_drop_covered`） |
| `get` 按非 id 索引读空 | 区间观察（`rows_only=False`） | 同上，一条 CNT：这个值上仍然没有行 |
| `get` 按非 id 索引命中 | 区间观察（`rows_only=True`） | 只做读取一致性核对，不发 CNT，靠返回那一行的 VER |
| `get`（含 `get(id=)`）按主键 / unique 读空；`range` 的 unique 点查读空 | 读空记录（`mark_absent`） | 本事务写入这个值撞车时判竞态，而不是 `UniqueViolation`（`get_absent_unique_fields`） |

读空记录只有 `get` 会看（同一事务里再读同一个值直接返回 None）；`get_by_id` 既不看也不登记，`range` 只登记不看。
（这是改动前的现状；`get_by_id` 改成和 `get(id=)` 一样，见 §5.2。）

这让"读了再据此写"的事务不会建立在旧数据上。但有一类读不需要这些保护：读到的值旧一点、或者读到之后被别人改了，都
不影响这次写入对不对。比如：

- **只增不改的日志**：commit_seq 的服务端消费方往后读日志，一次读 100 行，提交时 master 上就多 100 条 VER。实测
  （Redis 8.10.3，§9）每条 VER 约 0.6µs，单行提交的整个脚本约 3.9µs；读 100 行写 1 行的提交约 90µs，不校验这 100 行
  约 20µs。
- **配置、展示用的数据**（排行榜、别人的公开资料）：只拿来展示或做参考。被别人改了，整个 `System` 也要重试，白白浪费。

只读事务本来就不提交（`Session.commit` 只在有脏数据时才发），所以这些代价只出现在"读了一些、又写了别的"的事务里。

## 2. 目标与非目标

目标：

- 事务里可以按 id、按索引读行、读区间，**什么都不登记**：提交时不为这些读做任何校验，并发修改它们也不会让这次
  事务判竞态。
- 读到的仍然是本事务眼里的数据（读己之写）。没有并发改动时，结果和同名的普通读一样；读的过程中遇到并发改动，普通读
  靠提交时判竞态兜底，不校验的读当场处理（§4.4），不返回不满足查询条件的行。
- 调用处一眼能看出是这种读，代码审查时能搜到。

非目标：

- **跨簇读**：组件照样要在 `System` 的 `components` 里声明，簇照样合并（§4.6）。
- **一致的快照读**：同一事务里的两次读可能落在复制进度不同的副本上，前后不一致；不做。
- **不登记的写、不登记的 upsert**。
- **把返回的数组设成只读**（`writeable=False`）：普通读返回的也是可写的拷贝。
- **LocalApp、`hetu get` / `hetu range`**：每次调用都是一个只读事务，本来就不提交，不加。

## 3. 用户接口

### 3.1 用法

```python
@hetu.define_system(namespace="ssw", components=(Player, Config, Notice))
async def post_notice(ctx: hetu.SystemContext, text: str):
    me = await ctx.repo[Player].get(id=ctx.caller)  # 要据此写的：普通读
    rate = await ctx.repo[Config].untracked.get(key="notice_cost")  # 只做参考：不校验
    ...
```

- `SessionRepository.untracked`：属性，返回这个 repo 上不校验的读 `UntrackedReader`。每次访问新建一个（轻量，
  `__slots__`）：缓存在 repo 上会形成 repo ↔ reader 的引用环，每次 `System` 调用的 repo 都要等循环 GC 回收。
- `UntrackedReader` 只有三个方法：
  - `get_by_id(row_id)`
  - `get(index_name=None, query_value=None, _group=None, **kwargs)`
  - `range(index_name=None, _left=None, _right=None, limit=10, desc=False, *, _group=None, **kwargs)`
- 参数和 `SessionRepository` 的同名方法一样，只有两处不同：
  - `range` 没有 `phantom_check`。kwargs 里出现 `phantom_check` 时（照现有文档照抄过来很常见），先于其他解析抛
    `TypeError`，说明它提交时什么都不校验。保留名和普通 `range` 完全一样，不新增保留名。否则单列索引上会报"不是
    复合索引，不能给分组值"，复合索引上报分组值不对，显式写法下直接静默忽略。
  - `_group` 改成仅限关键字：去掉 `phantom_check` 之后，第 6 个位置参数会从 `phantom_check` 变成 `_group`，照普通
    `range` 的位置写法传参会绑错。
- 没有写方法。
- headless 的 `s[Comp]`、`session.using(Comp)` 返回的都是 `SessionRepository`，同样有 `.untracked`。
- `UntrackedReader` 从 `hetu.data.backend` 导出，方便写类型标注。
- 文档建议在调用处写全 `.untracked.`，不要存进变量，保住"能搜到"的好处。

### 3.2 为什么是 `repo.untracked`，不加参数

- **不和 `phantom_check` 冲突**：写成 `range(..., track=False)` 的话，默认的 `phantom_check=True` 只能静默忽略或者
  报错，都别扭。
- **调用处一眼能看出来，也能搜到**（`.untracked.`）：这种读法用错了是静默的数据错误，代码审查时要能找出来。
- **上面没有写方法**，不会让人以为能拿它改数据。
- **不新增 kwargs 保留名**：参数写法要占用一个名字（比如 `track`），叫这个名字的字段就没法用 kwargs 写法查询。

不选的做法：

- **参数写法**（`get(..., track=False)`）：改动最小，缺点见上。
- **整段切换模式**（`async with ctx.untracked():`）：块里调用的辅助函数会被悄悄带进去。
- **声明层面**（`define_system(..., peek=(X,))`，或者组件级的开关）：同一个组件上，消费方既要不校验的读（往后读），
  又要校验的读（确认缺口、最后一行），声明层面做不到；跨簇读另说（§4.6）。

类名用 `UntrackedReader` 不用 `UntrackedView`：这个库里到处是 numpy，"view" 容易让人以为返回的是不拷贝的视图。

### 3.3 对照

| 写法 | 返回行的版本（VER） | 区间（CNT） | 读空记录 |
|---|---|---|---|
| `range(...)` | 校验 | 校验 | 只登记（unique 点查） |
| `range(..., phantom_check=False)` | 校验 | 不校验 | 只登记（unique 点查） |
| `untracked.range(...)` | 不校验 | 不校验 | 不看、不登记 |
| `get(...)`（含 `get(id=)`） | 校验 | 非 id 索引读空时校验 | 看、登记（主键 / unique） |
| `untracked.get(...)` | 不校验 | 不校验 | 只看、不登记 |
| `get_by_id(...)` | 校验 | — | 看、登记（同 `get(id=)`，§5.2） |
| `untracked.get_by_id(...)` | 不校验 | — | 只看、不登记 |

## 4. 语义

### 4.1 读到的是什么

和同名的普通读一样，只是不登记：

- **先看本事务的缓存**：读过的行（被普通读登记过的）、本事务 insert / update / delete 的行，都按本事务眼里的值匹配。
  缓存里有的行，返回缓存里那份的拷贝。
- **读空记录**：`untracked.get`、`untracked.get_by_id` 和普通读一样会看，普通读登记过"这个值不存在"的（主键 /
  unique）直接返回 None；`untracked.range` 和普通 range 一样不看。
- **range 合并本事务的改动**（`LocalIndex`）：排序、截断、`limit` 都和普通 range 一样；unique 点查命中缓存时直接
  返回（`_unique_point_hit`）；`limit=0` 不去数据库。
- **提交序号列（`commit_seq`）的索引**：本事务 insert 的行还没有号，不在结果里，同普通读。
- **去数据库时走 `Session.master_or_servant`**：按权重随机选 master 或副本；`only_master` 的 session（headless 默认）
  读 master。查索引和取行用同一个节点，同普通读。
- **不可重复读**：读回的行不进缓存，同一行再读（不管是不校验的读还是普通读）都会再去数据库，可能读到更新的版本；
  读空也不会被记住。循环里反复读同一条配置，每次都是一次往返，普通读只有第一次去数据库。

为什么不绕过缓存、直接读库：同一个事务里会同时出现同一行的两个版本（缓存里那份和刚读回的那份），本事务改过的行，
直接读库还会读到改之前的值。按本事务眼里的数据返回，和普通读一致，最好推断。

### 4.2 不登记什么

- **读回的行不放进缓存**（不调 `add_clean`）：提交时没有它们的 VER。
- **不登记区间观察**（不调 `add_range_observation`）：没有 CNT，也没有提交时的读取一致性核对（改到读的时候做，§4.4）。
- **读空不登记**（不调 `mark_absent`）：之后本事务写入这个值、撞上库里已有的行时，按 `UniqueViolation` 处理，不判竞态。
- **已有的登记不撤销**：这一行之前被普通读读过（在缓存里），照样返回缓存里那份，提交时照样校验。

所以同一行：

- **先 untracked 读、再普通读**：普通读去库里读（可能读到更新的版本），然后登记；
- **先普通读、再 untracked 读**：返回缓存里那份。之后它被别人改了，提交照样判竞态。

### 4.3 读到的行怎么用

- **不能直接 `update` / `delete`**：两者都要求这一行在缓存里，也就是被普通读读过。要改，先用普通读再读一次。
- **不在 update 时自动登记**：
  - update 现在不用访问数据库，自动登记要再读一次库、比对 `_version`；
  - 手工拼出来、`_version` 不对的行，会从直接报错变成一直重试。
- **改报错信息**（异常类型不变，文案包 `_()`）：
  - `update` 的 `LookupError: Cannot update: row id not found in cache.`、`delete` 的 `LookupError`：repo 判断不出这一行
    从哪来，文案列出几种可能（`untracked` 读到的、上一个事务或上一次重试留下的、`new_row` 造了还没 insert 的），
    再给出改法（先用普通读读进本事务）。
  - `delete` 现在把"不在缓存"和"已删除"合成一条，拆开。
  - 先 untracked 读、再普通读拿到了新版本，然后拿 untracked 那份去 update：现在报 `ValueError: Cannot update _version field.`，
    用户根本没碰 `_version`。改成说明传进来的是旧拷贝、`_version` 和本事务读到的对不上。
- **返回的是拷贝**，改它不影响缓存，同普通读。

### 4.4 读的过程中遇到并发改动

查索引和取行是两条命令，中间可能有提交。普通读把这种情况交给提交时的读取一致性核对（`_range_checks`：取行读不到、
或者读回的行的索引值和索引里的不一样，判 `InconsistentRangeRead`）。不校验的读没有提交时的核对，改在读的时候处理，
只核对从数据库取回的行（缓存里的行是本事务眼里的数据，由它们自己的 VER 兜底）：

- **`untracked.get`（非 id 索引）**：取行读不到，或者读回的行已经不在这个值上，当场抛 `InconsistentRangeRead`，
  `System` 重试。否则会返回一行不满足查询条件的数据，或者一个从来没有出现过的 None（比如一次提交里删掉旧行、插入
  新行，查到旧 id、取行读不到），用户代码可能把它当成"没配置、用默认值"。结果和普通读一样：普通读也是重试，只是
  等到提交时。
- **`untracked.range`**：取行读不到的行跳过（同 `phantom_check=False`）；读回的行已经不在这个区间的丢掉。返回的
  每一行都满足查询条件，但不一定是某一刻的完整区间（可能只看到一次提交的一部分），**返回行数少于 `limit` 不代表
  读全了**。不抛的原因：读写分离代理按命令分流时，取行常落到更落后的副本上，读不到最新的几行；对往后读的消费方来说
  这只是缺号，由缺口确认处理，抛的话会反复重试。
- **`get_by_id` / `get(id=)`**：只读一个 key，读到的 None 就是那一刻的状态。

### 4.5 什么时候能用

判断准则：**读完到提交之间，别人改了这一行，你写进去的东西还对吗？** 对，才能用。

能用：

- 只增不改的日志（commit_seq 消费方往后读，§6）；
- 配置、展示用的数据。

不能用：

- **读出来算完再写回**（余额、库存、计数）：会丢更新；据此写别的行，会写偏斜。
- **由读到的结果推导要写入的值**：下一个编号、max + 1、靠扫描保证唯一。
- **"读空就 insert"**：
  - unique 值：并发插入之后，本事务的 insert 抛 `UniqueViolation`（不重试），而不是判竞态、重试后走 update；
  - 非 unique 值：会插入重复的行。
- **用读到的结果决定能不能做**（权限、冷却、次数限制、资格）：判断依据可能已经变了。
- **commit_seq 消费方的游标、缺口确认**：必须照常登记（§6）。文档示例里加注释，防止有人顺手也改成 untracked。
- **`depends` 的子 `System` 用 untracked 读出来、父 `System` 拿去做判断**：同样不受保护。
- **`replay.log` 里 `RaceCondition` 多，就改成 untracked**：反模式。先找冲突的原因（区间读得太宽、读了不需要读的行），
  不依赖区间的 `range` 先试 `phantom_check=False`。

另外要写明：

- **不解决合簇**（§4.6）："配置"这个例子容易让人以为能借此避开被到处引用的中心表，不能。
- **只读的 `System` 用不用都一样**：它不提交。别到处加。
- **不可重复读**（§4.1）；**可能只看到一次提交的一部分**（§4.4）。

### 4.6 和簇（co-location）的关系

表的 key 带着簇 id（`SessionRepository` 的 `TableReference` 用的是 session 的 `cluster_id`），组件必须在 `System` 的
`components` 里声明才能读，簇照样合并。所以不校验的读不能用来读别的簇的组件。让只读的依赖不参与合簇，是声明层面的
另一个改动（要按组件自己的簇建 `TableReference`、`SystemClusters` 区分只读依赖），不在本次范围。

### 4.7 只读事务、LocalApp、headless

- 没写东西的事务不提交，用不用 untracked 都一样。
- LocalApp 的 `get` / `range`、`hetu get` / `hetu range`：每次调用都是只读事务，不加。
- headless 默认 `only_master`：读 master，读到的是最新的，但照样不校验。
- 库外消费方（游标存在库外、不在事务里）用 `Table.servant_seq_read`（#217，见
  `2026-11-09-commit-seq-third-party-design.md`），和本功能无关。文档里写明分工：事务里、游标存在库里的用 §6.3 的写法，
  游标在库外的用 `servant_seq_read`。

## 5. 实现

只改 `hetu/data/backend/repo.py`，外加导出和 API 文档登记。idmap、session、后端、提交 payload、Lua 都不动。分三步，
前两步是 §11 定下的顺带修改，各自单独提交，放在 untracked 之前（untracked 直接建在改好的读路径上，不用先写一版
旧语义的内部方法）。搁置后前两步从 dev 另做，见 §15.4。

### 5.1 同一事务里并发读同一行（现有 bug，§11 第 3 项）

- 现象：同一事务里 `asyncio.gather` 两个普通读读同一行（`get`、`get_by_id`、`range` 都会），两个协程都没命中缓存、
  都去读库，后一个 `add_clean` 撞上前一个放进去的行，抛 `ValueError: Row with id [...] already exists in cache`。
  四个后端都复现（审核员 A）。
- 修法：读库回来之后，按读库之前的顺序再查一次（id 读：缓存、读空记录；`_range_read`：缓存）。这时缓存里已经有的行
  以缓存为准，读回的那份丢掉，只把缓存里还没有的放进缓存。
  - 和顺序执行一致：后到的那个读晚一点开始的话，本来就会命中缓存。
  - 缓存里已经是删除状态的：`get` / `get_by_id` 返回 None，`range` 不放进结果，同命中缓存。
  - **版本不一样也用缓存里那份，不当场判竞态**（细化了 §11 第 3 项当时的说法"版本不一样就判竞态"）：版本号随机
    起步、会回绕，分不出新旧；读回的可能来自更落后的副本，这时缓存里那份才对。缓存里那份的 VER 提交时照常校验，
    真被改过的照样判竞态。
  - id 读回来时读空记录已经有了（另一个协程读空登记的）：返回 None，读回的行丢掉，同顺序执行。
- 只处理 `add_clean` 撞车这一种。gather 里一边读、一边写同一张表的其他交错（比如读索引期间别的协程改了这张表）不在
  本次范围：结果按读开始时本事务的改动合并，提交时照常校验。
- untracked 读按同一条规则（缓存里有就用缓存的），只是缓存里没有的也不放进去。

### 5.2 `get_by_id` 和 `get(id=)` 读空语义统一（§11 第 2 项）

- `get_by_id(x)` 改成和 `get(id=x)` 走同一个内部方法：缓存 → 读空记录 → 读库，读空登记读空记录（`mark_absent`）。
- 行为变化（docstring 写明）：
  - 同一事务里 `get_by_id` 读空之后再读同一个 id，直接返回 None（以前每次都读库）；
  - `get_by_id` 读空之后本事务 insert 同一个 id、撞上库里已有的行：判竞态重试（以前抛 `UniqueViolation`）；
  - 先 `get(id=x)` 读空、再 `get_by_id(x)`：返回 None（以前会去读库，读到的话缓存里同时有"不存在"的登记和这一行）。
- 引擎内部没有调用 `get_by_id` 的地方（只有 3 个测试文件），用户文档也没提它（只在 API 文档里）。
- `RaceCondition` 的 docstring 里"本事务此前曾 `get`（或等值 `range`）观察到该值不存在"加上 `get_by_id`。

### 5.3 `untracked`

- `_get_by_id(row_id, track)`：§5.2 之后 `get_by_id`、`get(id=)` 共用；原来的 `_fetch_by_id` 并进来。`track=False` 时
  读空不 `mark_absent`、读回的不 `add_clean`。
- `get` 拆出 `_get(index_name, query_value, group, kwargs, track)`：
  - id：转 `_get_by_id(row_id, track)`；
  - 非 id：缓存（`filter` / `match`）、提交序号列的排除、读空记录（unique）照旧；`_range_read(..., track=track)`；
    `track=False` 时不设 `rows_only`、不登记观察、不 `mark_absent`，并按 §4.4 核对，对不上抛 `InconsistentRangeRead`。
- `range` 拆出 `_range(..., phantom_check, track)`：`track=False` 时不登记观察。
- `_range_read(..., track=True)`：`track=False` 时：
  - unique 点查读空不 `mark_absent`，读回的行不 `add_clean`；
  - 读回的行按 §4.4 核对：读不到的已经由 `get_many_array_` 跳过；索引值对不上的（`index_value_bytes_` 对比
    `member_value_`，同 `_range_checks`），range 丢掉、get 抛。
  - 观察照样算出来，由调用方丢掉，省得维护两份代码。
- `UntrackedReader`（`__slots__ = ("_repo",)`）：三个方法分别转调 `_get_by_id(track=False)`、`_get(track=False)`、
  `_range(phantom_check=False, track=False)`；`range` 先检查 kwargs 里的 `phantom_check`。
- `SessionRepository.untracked`：每次访问返回新的 `UntrackedReader`，带返回类型标注（类型检查器能推出
  `ctx.repo[X].untracked.range(...)` 返回 `np.recarray`）。
- 报错信息按 §4.3 改。
- docstring：中英双语（AGENTS.md 约定）。写清和普通读的差别、能用和不能用，完整的参数说明指回普通读。引用写成
  "`SessionRepository` 的 `get`"，不写 `` `SessionRepository.get` ``：API 文档生成器只给单个标识符加链接。
- `hetu/data/backend/__init__.py` 导出 `UntrackedReader`；`scripts/api_extras.py` 登记
  `hetu.data.backend.repo.UntrackedReader`，放在 system 页。

## 6. commit_seq 消费方的写法

### 6.1 往后读不校验的话，正常运行时仍然正确

两路审核都实测过（sqlite、带副本的 redis）：取行漏掉的行靠缺口确认补上，永久缺口确认后跳过，两个消费方共用游标、
生产者同时在写，每行恰好处理一次。推理上也成立：

- 往后读漏掉的行（取行落到进度不同的副本、读索引和取行之间被删）表现为缺号，由缺口确认补上，或者在 master 上确认不存在。
- 缺口确认的区间上界比 `row.seq` 小。`row.seq` 是从副本读到的，副本只复制 master，所以 master 上这个号已经提交、
  计数器已经越过这一段：这一段 master 上没有行，以后也不会有。
- 游标行照常登记，同一个消费方的多个实例不会重复处理。
- 副本延迟、读写分离代理按命令分流、原生 cluster 多副本、裁剪头部、游标冲突后重试，都不影响上面几条。

### 6.2 但会失去数据回退时的保护

原写法（`range(..., phantom_check=False)`）里，往后读到的**每一行**提交时都有 VER。主库故障切换丢了写入、或者主库
从 AOF / RDB 重启丢了最后一段写入（commit_seq §4.5 的前两种），副本还没跟上新的 master 的那段时间里，可能读到 master
上已经没有的行：

- 原写法：这些行的 VER 对不上，判竞态重试，等副本同步好；
- 往后读全部不校验：
  - 消费方处理了 master 上没有的行，游标越过这些号；
  - 新的 master 之后重新发出同样的号给新行，这些新行被永久跳过；
  - 在丢失的号下面做的缺口确认，会把"这一段以后也没有行"错误地确认成立。

两路审核都用"副本上有、master 上没了，之后重新发号"的模拟复现了：原写法第一次判竞态、重试后全部正确；全部不校验时
提交成功，新发的号永远没被处理。

这会推翻现有文档的两句话：`docs/zh/advanced.md`（en 同）库外消费方一节末尾"**服务端消费方**的游标和日志在同一个事务里，
会一起回退，不用这样核对"（#217 之前在第三方消费方表格下面）、commit_seq 设计稿 §4.5"服务端消费方……不受影响"。

### 6.3 补法：最后一行照常校验

往后读不校验，只把这一批的最后一行用普通读读进事务：

```python
async with ctx.repo[LogCursor].upsert(owner=owner) as cursor:  # 游标：照常校验
    log = ctx.repo[ChangeLog]
    rows = await log.untracked.range(
        seq=(cursor.last + 1, np.inf), owner=owner, limit=100
    )
    # 最后一行照常校验：它在 master 上，这一批就都在 master 上（不能省，见文档）
    if len(rows) == 0 or await log.get(id=int(rows[-1].id)) is None:
        return  # 没有新日志；或者这次 get 落到的副本还没复制到这一行，下次再读
    expected = cursor.last + 1
    for row in rows:
        if expected < row.seq:  # 中间缺了号：一次闭区间读确认整段（照常校验）
            for missing in await log.range(
                seq=(expected, row.seq - 1), owner=owner, limit=-1
            ):
                handle(missing)
        handle(row)
        expected = row.seq + 1
    cursor.last = expected - 1
```

- **为什么够**：异步复制下，任何节点的数据都是旧 master 提交流的一个前缀；AOF / RDB 重启后也是一个前缀。组内的号按
  提交顺序发，所以这一批里只要有丢了的行，号最大的那一行一定也丢了。最后一行在 master 上过了 VER，这一批就都在
  master 上，master 的计数器也已经越过这个号，缺口确认的推理继续成立。按 id 读，所以新 master 重新发出同一个号给
  别的行时也认得出来。
- **代价**：每批多 1 条 VER、1 次往返。最新的那行还没复制到这次 `get` 落到的副本上时，这一轮没有进展（只影响进度）。
  副本断了复制、代理又没摘掉它时，落到它上面的那一轮也没有进展；原写法落到它上面时一样读不到新行（第三方稿审核 A1
  说的那种副本），要修副本。
- 用 `get(id=...)` 不用 `get_by_id`：用户文档里只有 `get(id=...)` 的写法；§5.2 之后两者一样。
- 两路审核都实测过：模拟场景下第一次判竞态，之后新发的号照常处理；提交里只有游标和最后一行 2 条 VER。
- 有了这一行，6.2 那两句话对这种写法仍然成立；文档要写明"往后读不校验时这一行不能省，否则数据回退后会永久漏行"。

### 6.4 其他

- 日志行写入后还会被改、或者读到之后被删：不校验的读可能处理到旧版本、处理到刚被删的行。commit_seq 的日志通常只增
  不改，裁剪只删所有消费方都越过的头部。
- 文档的默认写法用 §6.3（§11 第 1 项，用户已定）。原写法（普通 `range`，每行一条 VER）在文档里提一句：更简单，
  master 不紧张、批不大时也可以用。

## 7. 文档

zh / en 手工同步，放在同一个提交里，不跑 `translate_new_content.py`。

- zh/en `_index.md`「事务冲突与重试」：加一条 `repo.untracked`，写清不校验什么、判断准则。
- zh/en `concepts.md`：
  - "提交时检查每个行的版本"（zh 约 120 行、en 约 232-236 行）加限定；
  - "需要强一致的判断请放在 System 里读"（zh 约 158 行、en 约 293-296 行）加一句：用普通读，不要用 untracked。
- zh/en `advanced.md`：
  - 新一节「提交时不校验的读」，放在「关掉区间校验」之后：用法、§3.3 对照表、§4.5 能用 / 不能用、§4.3 改行方式、
    §4.4 并发改动时的行为；
  - 「查不到就插入」的"成本随行数增长"（zh 约 306 行、en 约 512 行）：写明这里不能改用 untracked；
  - binlog 消费方（zh 约 364-404 行、en 约 590-650 行）：写法按 §6.3 改；`phantom_check=False`、"代价"两条跟着改；按
    §6.2 补"往后读全部不校验会怎样"；库外消费方一节末尾那句（zh 约 511 行、en 约 810-811 行）加限定；
  - 开头的目录加上新的一节（现在的目录本来就缺 binlog、headless 两节，en 版有排版断行，一起补齐）。
- `hetu/llms.txt`：Advanced 的摘要（#217 加了库外消费方）后面加上"提交时不校验的读（`repo.untracked`）"。
- `hetu/skills/building-on-hetu/SKILL.md`：RaceCondition 那一条后加 untracked 的用法、判断准则和禁忌；CRUD 列表
  加上；Commit sequence 那一条的服务端写法按 §6.3 改（#217 在它后面加了库外消费方，不动）。
- docstring：
  - `RaceCondition`（"纯读的行同样参与乐观锁校验"）、`UniqueViolation`（"请先 get 该值"）加限定：untracked 读不算；
    `RaceCondition` 的读空那条加上 `get_by_id`（§5.2）；
  - `SessionRepository` 的 `get_by_id`：写明读空的行为（§5.2）；
  - `SessionRepository` 的 `get` / `range` / `get_by_id`、`phantom_check` 参数说明加一句指向 untracked。
- `docs/api`：上面的 docstring 改完后重新生成（CI 检查）。
- commit_seq 设计稿：§4.5 加限定；§14.4 指向本稿。

## 8. 测试

新文件 `tests/test_untracked_reads.py`，以 sqlite、redis 共跑为主，valkey、redis_cluster 也跑。§5.1、§5.2 的用例跟着各自
的提交，放在现有的 session 测试文件里。

**§5.1 并发读同一行**：

- A1. `asyncio.gather` 两个读读同一行：`get_by_id`、`get(id=)`、`get`（unique、非 unique）、`range` 两两组合，不报错、
  拿到同一份，提交时这一行只有一条 VER；
- A2. 读库期间本事务把这一行读进来并 update / delete（注入在读库回来之前）：返回缓存里的当前值、None、不在结果里；
- A3. 读回的版本和缓存里的不一样（两次读之间别的事务改了它）：返回缓存那份，提交判竞态；
- A4. id 读回来时读空记录已经有了（另一个协程读空登记的）：返回 None，读回的行不进缓存。

**§5.2 `get_by_id` 读空**：

- B1. `get_by_id` 读空之后再读同一个 id：返回 None，不去数据库；
- B2. `get_by_id` 读空，别的事务插入同一个 id，本事务再 insert 这个 id：判竞态（以前是 `UniqueViolation`）；
- B3. `get(id=)` 读空后 `get_by_id` 返回 None，反过来也一样。

**不登记**：

1. untracked 的 `get_by_id`、`get`（id、unique、非 unique、复合索引）、`range`（含 unique 点查、`limit=0`）之后，
   缓存里没有行、没有区间观察、没有读空记录，`first_reference()` 为 None。
2. 读到的行被并发修改，本事务写别的行照常提交（对照 `test_stale_read_race`）。
3. 区间里被并发插入，提交不判竞态（对照 `test_range_phantom_insert_is_race`）。
4. unique 读空之后被并发插入同值，本事务 insert 同值抛 `UniqueViolation`，不是 `RaceCondition`（get 和 range 点查各一条）。
5. 提交 payload 里只有写入那一行的检查。

**结果和普通读一致**（用两个独立 session、做相同的写入来比；同一个 session 里先后读，结果本来就可能不同）：

6. 本事务 insert 的行、update 进 / 出区间的行、delete 的行、改了 unique 值的行；
7. `limit` 截断、`desc`、开区间与本事务的行合并；
8. 提交序号列的索引上，本事务 insert 的行不在结果里；复合索引、复合 unique 索引的组；
9. 缓存里的行返回缓存那份；之前被普通读读过、后来被别人改了：返回旧的那份，提交照样判竞态（登记不撤销）；
10. `untracked.get` 看读空记录（普通 `get` 登记过就直接返回 None，不读库）；`untracked.get_by_id`、`untracked.range` 不看。
    可以复用 `test_backend_range_own_writes.py` 的场景。

**和普通读交替**：

11. 先 untracked 读、再普通读：普通读去库里读、登记；
12. untracked 读到的行 `update` / `delete`：`LookupError`，文案按 §4.3；拿旧拷贝 update 的 `ValueError` 文案。

**并发改动**（`_intrude_before_row_fetch` 一类的注入）：

13. `untracked.range`：取行读不到的跳过、索引值对不上的丢掉，提交不判竞态；
14. `untracked.get`：取行读不到、索引值对不上（含"一次提交里删旧行、插新行"）抛 `InconsistentRangeRead`；
15. 查索引和取行在同一个节点（参数化 `test_backend_session_race.py` 里现有的同节点用例）。

**接口**：

16. `untracked.range(..., phantom_check=...)` 抛 `TypeError`（kwargs 写法、显式写法、复合索引各一条）；`_group` 只能按关键字传；
17. 参数校验同普通读（索引名不对、复合索引缺分组值、`limit` 类型不对）；
18. 返回的是拷贝：改返回值，缓存和数据库态都不变；
19. 同一事务里 `asyncio.gather` 几个 untracked 读、untracked 和普通读混着跑；
20. headless session 的 `s[Comp].untracked`；
21. `System` 级端到端：`SystemCaller` 或 Sandbox 里 `ctx.repo[X].untracked`，读到的行被并发改、`System` 不重试。

**master 读与 commit_seq 消费方**：

22. `test_master_read_budget.py` 加一条 untracked 读的路径，预算 0（照 #217 的 `test_seq_polling_never_touches_master`：
    `master_weight=0` 加 `count_reads`）；
23. `tests/test_commit_seq.py` 的消费方用例（`_consume_once` 那一批）参数化成两种写法（原写法、§6.3 的写法）都跑；
24. "副本上有、master 上没了，之后重新发号"的模拟：原写法和 §6.3 的写法判竞态、之后全部正确；往后读全部不校验时
    提交成功、新发的号被跳过（把 §6.2 的取舍钉住）。

## 9. 性能

- 实测（Redis 8.10.3，审核员 B，`benchmark/redis_commit_cost.py replay` 压满主线程）：单行提交主线程约 3.9µs；
  每条 VER 约 0.54–0.62µs；每条 CNT 约 1.4µs。走真实路径（不走 pipeline）每条 VER 约 0.72µs；读 100 行写 1 行的提交
  约 90µs，模拟不校验后约 20µs。
- 每读回一行省：客户端一次缓存拷贝；master 上一条 VER。
- 多花的：读的时候要做的一致性核对（§4.4，每行一次索引值比较，原来在提交时做，挪到读的时候）；不进缓存，同一行再读
  要再去数据库（§4.1）。
- 实现时给 `benchmark/redis_commit_cost.py` 的 replay 加 `scan100` / `scan100_untracked` 两种形状，结果写进实现记录。

## 10. 定下的做法

（2026-11-09 搁置，见 §15：第 8 条由 §15.5 取代；第 9、10 条移出本稿，从 dev 另做，见 §15.4。）

1. 接口名 `untracked`，类名 `UntrackedReader`，从 `hetu.data.backend` 导出；用户文档叫"提交时不校验的读"。
2. 做成 repo 上的属性，不加参数；每次访问新建，不缓存。
3. 读缓存，但不写缓存；不撤销已有的登记；`untracked.get` 看读空记录，`untracked.get_by_id` / `untracked.range` 不看
   （都同对应的普通读）。
4. update / delete 拿到不在缓存里的行：只改报错信息，不自动登记。
5. 读的过程中遇到并发改动：`untracked.get` 当场抛 `InconsistentRangeRead`；`untracked.range` 跳过读不到的、丢掉已经
   不在区间的。
6. `untracked.range` 遇到 `phantom_check` 抛 `TypeError`，`_group` 仅限关键字。
7. 不做跨簇读、快照读、只读数组。
8. commit_seq 消费方的文档默认写法用 §6.3（往后读不校验、最后一行用 `get(id=...)` 照常校验），原写法提一句。
9. `get_by_id` 改成和 `get(id=)` 一样（§5.2），单独提交。
10. 修同一事务并发读同一行的 `add_clean` 撞车（§5.1），单独提交；读库回来以缓存为准，版本不一样也不当场判竞态。

## 11. 待用户定（2026-11-09 已定：三项都按推荐，见 §10 第 8–10 条）

1. **commit_seq 消费方的文档默认写法**：
   - (a) **§6.3 的写法**（往后读不校验、最后一行照常校验）：每批的提交从约 90µs 降到约 20µs（100 行时），数据回退时的
     保护不变；多 1 次往返，偶尔一轮没有进展；要多理解一行。
   - (b) **原写法**（`range(..., phantom_check=False)`），把 §6.3 的写法作为"master CPU 是瓶颈时"的可选优化：最简单，
     每消费一行 master 多约 0.6µs。
   - 我推荐 (a)：做这个功能本来就是为了消费方的这笔开销，而 §6.3 的写法不损失正确性。
2. **`get_by_id` 和 `get(id=)` 的读空语义要不要统一**（审核发现的现有差异）：现在 `get(id=X)` 读空会登记、同一事务里
   再读直接返回 None、之后 insert X 撞车判竞态；`get_by_id(X)` 不登记、不看，撞车抛 `UniqueViolation`。用户多半以为
   两者一样，"`get_by_id` 读空就 insert"的写法撞车时不会重试。
   - 我推荐统一成 `get(id=)` 的语义，作为本 PR 里单独的一个提交（行为变更，带回归测试、改 docstring）。
   - 不统一的话，`untracked.get_by_id` 照现在的 `get_by_id`。
3. **顺带发现的现有 bug**：同一事务里用 `asyncio.gather` 让两个普通读读同一行，会抛
   `ValueError: Row with id [...] already exists in cache`（两个协程都没命中缓存、都去读库，后一个 `add_clean` 时撞上
   前一个放进去的行）。`get`、`get_by_id`、`range` 都会，四个后端都复现。修不修、放不放在本 PR 里？

## 12. 实施顺序（每一步都带测试）

（2026-11-09 搁置，见 §15：第 2、3 步从 dev 另做；其余等重新做时按 §15.5 的范围重排。）

1. 本设计稿。
2. 同一事务并发读同一行（§5.1）；§8 A1–A4。
3. `get_by_id` 读空语义同 `get(id=)`（§5.2）；§8 B1–B3。
4. `repo.py`：拆出带 `track` 的内部方法、`UntrackedReader`、读的时候的一致性核对、报错信息（§5.3）；§8 第 1–21 条。
5. master 读预算；commit_seq 消费方用例参数化、数据回退的模拟（§8 第 22–24 条）。
6. 文档、docstring、API 文档。
7. 性能实测、实现记录。

## 13. 审核记录（2026-11-09）

两路独立审核：A 对照代码查语义和正确性，B 看接口、误用风险、文档、测试计划和 commit_seq 消费方的写法。两人都只拿到
本稿和代码，都写了临时用例实测（A：sqlite、redis、valkey、redis_cluster；B：sqlite、带副本的 redis，另跑了提交开销的
基准），没有改仓库。

**核实无误的部分**：

- 不调 `add_clean` / `add_range_observation` / `mark_absent`，这些读在提交时就没有任何检查：提交时的检查只来自缓存行、
  读空记录、区间观察三处；`_implied_by_unique`、`LocalIndex.moved`、UPDATE 的旧值、Lua 里的豁免都只用普通读或写入
  放进缓存的行。A 实测 13 种读法加并发改动，提交里只有写入那一行的 1 条 VER。
- 读己之写：A 在 Item（36 个查询）、commit_seq 复合索引（10 个）、复合 unique 索引（14 个）上比过，两种顺序都和普通读
  逐字节一致；普通读的结果不依赖登记这一步。
- 返回的数组不和缓存共用内存；`asyncio.gather` 几个 untracked 读没问题；master_weight=0 时 master 读为 0，`only_master`
  时读 master；查索引和取行用同一个节点。
- 视图优于参数写法；类型检查器能推出 `ctx.repo[X].untracked.range(...)` 的返回类型；griffe 会把属性收进 API 文档。
- §6 在正常运行时成立（见 §6.1）；`TableReference` 用 session 的 `cluster_id`，§4.6 成立；只读事务不提交。

**按审核意见修改的部分**：

1. 【高 / 中，两路都报】往后读全部不校验时，数据回退的保护丢得比初稿写的多（每一行的 VER，不只是后面那一行；AOF / RDB
   重启也算），还会推翻现有文档两句话：重写 §6.2，加 §6.3 的补法（两路都实测过），文档清单加上那两句话，默认写法交给
   用户（§11 第 1 项）。
2. 【中，两路都报】初稿说 `get_by_id` 看读空记录、按 §5 拆的话会悄悄改掉普通 `get_by_id`：改正 §1、§3.3、§4.1，§5 写明
   语义逐字不变，统一与否交给用户（§11 第 2 项）。
3. 【中 / 低】给 `untracked.range` 传 `phantom_check=` 报错误导人、显式写法静默忽略；去掉一个参数后位置参数会绑错：
   加 `TypeError`，`_group` 仅限关键字（§3.1）。
4. 【中】§4.5 的能用 / 不能用不完整：补上推导写入值、游标与缺口确认、`depends`、反模式、不解决合簇、只读 System、
   判断准则、写全 `.untracked.`。
5. 【中】文档清单漏了 concepts.md 两处、`RaceCondition` / `UniqueViolation` 的 docstring、"成本随行数增长"、目录、
   SessionRepository 的 docstring、SKILL.md 的 CRUD 列表：补进 §7。
6. 【中 / 低】测试计划缺消费方写法参数化、数据回退的模拟、gather、System 级端到端、拷贝、同节点、`limit=0`、等价性的
   比较方法：补进 §8。
7. 【低】untracked `get` 取行读不到时会返回一个从来没有出现过的 None：改成当场抛 `InconsistentRangeRead`（§4.4）。
   顺着这条，自查时又发现：普通读提交时会核对读回的行的索引值，不校验的读没有这一步，会返回不满足查询条件的行。
   一并改到读的时候做：get 抛，range 丢掉（§4.4、§5）。
8. 【低】不可重复读、同一行反复读的往返开销：写进 §4.1、§9。
9. 【低】报错信息：repo 判断不出行从哪来，文案要列出几种可能；`delete` 的两种情况拆开；拿旧拷贝 update 时的
   `Cannot update _version field.` 看不懂；包 `_()`（§4.3）。
10. 【低】性能数字：每条 VER 从"约 1µs"改成实测的约 0.6µs，单行提交约 3.9µs，补上 100 行的实测（§1、§9）。
11. 【低】缓存 reader 会形成引用环：改成每次访问新建（§3.1）。类名从 `UntrackedView` 改成 `UntrackedReader`；从
    `hetu.data.backend` 导出；用户文档叫"提交时不校验的读"；docstring 的引用写法照顾 API 文档的自动链接。
12. 【低】§3.1 的示例用了没声明的组件、没定义的变量；§1 的表漏了 CNT 的免除与去重、get 命中仍做读取一致性核对：改正。

**没有采纳 / 不在本次范围**：

- A 提的"untracked `get` 取行读不到时当场重读一次"：选了直接抛 `InconsistentRangeRead`，和普通读的结局一样（重试），
  代码少一条分支。
- B 提的现有问题：普通 `range` 的显式写法静默丢掉多余的 kwargs（`repo.range("time", 0, 10, foo=3)` 照常返回）。
  不在本次范围，记一笔。
- 声明层面的只读依赖（不参与合簇）：另一个更大的改动（§4.6）。

## 14. #217 合并之后的核对（2026-11-09）

#217（提交序号列的库外消费方，`servant_seq_head(_many)` / `servant_seq_read` / `CommitSeqDiverged`）在本稿审核期间合并进
dev（`6fcf08ad`）。本分支还没有自己的提交，跟着前移即可。逐项核对：

- 代码：#217 没动 `repo.py`、`idmap.py`、`session.py`、提交路径，§5 不受影响。新加的读都在 `Table` / `BackendClient` 上，
  不在事务里。
- 用户文档：binlog 一节加了「库外消费方：`servant_seq_read`」小节，原来第三方消费方表格和下面那句话改写后挪到小节
  末尾，§6.2、§7 的引用跟着改。服务端消费方的写法、缺口确认、"代价"那条没动。
- 分工：库外消费方不在事务里，和 untracked 无关；文档写明各用什么（§4.7）。
- `hetu/llms.txt`、SKILL.md、`scripts/api_extras.py`、`hetu/data/backend/__init__.py` 都加了东西，本功能的改动接在后面（§5.3、§7）。
- 测试：master 读守门的方法清单多了 `seq_*`；`test_master_read_budget.py` 有了 `master_weight=0` 加 `count_reads` 的写法，
  §8 第 22 条照它写。
- 第三方稿审核 A1（cluster 的 `ROUND_ROBIN_REPLICAS` 遇到断了复制的副本）：服务端消费方的原写法落到那种副本上本来就
  读不到新行，§6.3 多一条同样的路径，写进 §6.3 的代价。
- commit_seq 设计稿 §14.3 被 #217 更正（游戏项目的消费方其实在库外），§14.4 仍是本稿的来源，§7 计划的改动不变。
  （这一条的影响当时估轻了：它去掉了本功能最初的出发点，见 §15.1。）

## 15. 搁置（2026-11-09）

核对 #217 之后用户定：本功能搁置，等有具体的服务端用例再做。本稿保留在 `feat/untracked-reads` 上。

### 15.1 出发点没有了

- 本功能的出发点是 commit_seq 设计稿 §14.4：服务端消费方往后读日志，每读一行，提交时 master 上多一条 VER（本稿 §1
  的第一个例子）。它建立在 §14.3 原来的前提"游戏项目的消费方目前在服务端"上。
- #217 期间这个前提被更正：游戏命令表的消费方在库外，用 `servant_seq_read` 读。这种读不进事务、不提交，走
  `Backend.servant`（没配 servants 时连 master 的地址，推荐的部署里那是代理，读落到副本），本来就不碰 master 上的
  提交检查，这个功能对它一点开销都省不了。
- §14 最后一条当时只记了"§14.4 仍是本稿的来源"，把这一条的影响估轻了。

### 15.2 剩下的用处

- 以后写在 `System` 里、游标存在库里的日志消费方：master 上的检查从每行一条降到每批两条（游标和最后一行，§6.3）。
  目前没有这样的消费方。
- 写事务里只做参考的读（配置、排行榜、别人的公开资料）：每读一行省一条 VER，约 0.6µs。更有价值的是，这些行被别人
  频繁改时，读它们的 `System` 不会跟着判竞态重试。前提是真有这种热点行，目前也没有实例。

### 15.3 为什么搁置

没有具体用户时不划算：

- 用错了是静默的数据错误（§4.5）：读完到提交之间那一行被改了，写进去的东西就不对，提交时也不会判竞态；
- 要写二十多条测试（§8）、改一整套文档（§7）。

### 15.4 两项顺带修复照做

两者都是独立存在的问题，和本功能无关，从 origin/dev 另拉分支做，不放在本分支：

- 同一事务里并发读同一行（§5.1）：已经改完，四个后端的测试全部通过。
  - 提交 `8825e58a` 当时建在本稿的提交之上，后来从本分支拿掉了，现在只在 reflog 里。
  - 除了 §5 说的 `repo.py`，它还给 `IdentityMap` 加了 `has_any`。
  - 它的 commit message 和测试注释引用了本稿 §5.1；本稿不在 dev 上，挪过去时要改。
- `get_by_id` 和 `get(id=)` 读空语义统一（§5.2）：改动很小。`8825e58a` 已经把两条路径并成
  `_get_by_id(row_id, absent)`，`get_by_id` 传 `absent=False` 保持现状；统一就是改成 `True`，再加 §8 B1–B3，改
  docstring 和 API 文档。

### 15.5 重新做的条件和范围

如果有热点行导致 `System` 频繁重试，就值得做，但缩小范围：

- 只做接口、核心测试和一节文档；
- commit_seq 消费方的文档默认写法改回原写法（`range(..., phantom_check=False)`），§6.3 的混合写法只作为可选优化，
  因为服务端消费方现在只是假设。这一条取代 §10 第 8 条。

到时 §3–§6 的设计和 §13 的审核结论仍然可用；§7、§8 按缩小后的范围重挑。
