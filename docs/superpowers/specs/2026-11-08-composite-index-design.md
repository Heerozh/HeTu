# 复合索引（`groupby`）— 设计稿

- 日期：2026-11-08
- 状态：实施中。已过一轮独立审核（§16）；§13 是已定事项，§14.1 用户选了方案 (b)，设计见 §5.6
- 分支：`feat/composite-index`（基于 dev `9aaac1ce`）
- 影响范围：
  - 定义：`hetu/data/component.py`（`property_field(groupby=)`、定义校验、schema json、新类属性）
  - 存储与事务：
    - `hetu/data/backend/redis_model.py`：member 与区间编码、commit payload、读取一致性核对、`direct_set` 校验、`_schema_checking`
    - `base.py`：后端索引 API 加 `group`、频道 API、迁移时的索引比对
    - `idmap.py`、`repo.py`
    - 重建索引：`redis/maint.py`、`sqlite/maint.py`、`sqlite/store.py`
  - 订阅与协议：`hetu/data/sub.py`、`hetu/server/receiver.py`
  - 外围：
    - `hetu/headless.py`：schema 比对
    - `hetu/local.py`：只改文档
    - `hetu/cli/data.py`
    - `hetu/server/pipeline/zstd.py`：训练样本
  - C# SDK：
    - `ClientSDK/unity/cn.hetudb.clientsdk/HeTu/` 下的 `ClientBase.cs`、`UnityClient.cs`、`SessionClientBase.cs`、`SessionClient.cs`、`Subscription.cs`
    - `ClientSDK/csharp/HeTu.Client/HeadlessHeTuClient.cs`
    - Editor 的 `HeTuPostStationWindow.cs`
    - `package.json` 的版本号
  - 测试、文档
- **不改**：
  - 提交脚本 `redis/commit_v2.lua` 与 `sqlite/commit.py` 不为复合索引改动：member、区间两端、UNIQ / CNT 的参数都在 Python 里算好，
    脚本只当字节串处理。两边只各改两处：
    - UNIQ 豁免本次提交要 ZREM 的 member，修掉"同事务挪 unique 值"（§5.6）；
    - Phase 3 的一段注释（§8.4）。
  - 单列索引的存储格式、查询结果、通知、sub_id、线协议。有两处有意的行为变化，都是修既有问题：
    - update 时值的编码没变，就不再发 ZREM / ZADD / UNIQ（§5.1），修掉 NaN 变号后撞上自己报 UniqueViolation；
    - `_schema_checking` 改成白名单（§8.4），datetime64 / timedelta64 的索引列在启动时就报错，而今天要到提交时才崩。
  - 不新增 master 读，不新增 PUBLISH 调用点：复合索引的组频道走现有的 `value_chans`。

## 1. 背景

现在每个索引只有一列，索引名就是字段名。Redis 上一个索引是一个 zset：score 恒为 0，按 BYLEX 查，member 是
`to_sortable_bytes(值) + 00 + 十进制 id`。`indexes_`、`uniques_`、`point_subs_`、`hidden_fields_` 都以字段名为键。

"某个 zone 里按 level 排序""某个服务器内名字唯一"这类需求，今天只有两种写法，都有代价：

- **单索引 + numpy 二次筛选**（`docs/zh/advanced.md` 推荐的写法）：
  - 要多读不相干的行；
  - 防幻读的观察区间按整个 level 区间算，别的 zone 写入也会让这个事务 RaceCondition。
- **把几列拼成一个字符串当 unique 键**（同一篇文档里的变通写法）：
  - 要多存一列，拼接格式自己维护；
  - 数值要排序还得自己补齐位数。

另外，下一步的"提交序号列"（`commit_seq`：提交成功才发号、按组连续）要建在按组有序的索引上，见 §12。

## 2. 目标与非目标

目标：

- 在字段上声明 `groupby`，得到"按分组列分组、组内按本字段排序"的索引。查询方式是"分组列全部等值 + 本字段区间"。
- `unique=True` 加 `groupby` 表示组内唯一。`get`、`upsert`、冲突检查都按 (组, 值) 判断。
- 分组列可以是整数（含 bool）、str、bytes。分组列可以修改，索引会跟着搬到新组。
- 开了 `point_sub=True` 的复合索引按组发通知，按组订阅只会被本组的写入叫醒。
- 客户端（C# SDK）能订阅复合索引，旧客户端不受影响。
- 迁移、重建索引、headless 的 schema 校验、CLI 都认识复合索引。

非目标（第一版不做；以后放宽也不会破坏兼容）：

- 只给部分分组列的查询（最左前缀）、在分组列上给区间、跨组扫描；
- 组件级具名索引、同一个字段建多个索引；
- 浮点分组列（§3.1）；
- `commit_seq` 本身（另写 spec，建在本设计之上）；
- §14 列出的既有问题。

## 3. 用户接口

### 3.1 声明

```python
@define_component
class Item(BaseComponent):
    zone: np.int32 = property_field(0, index=True)
    level: np.int32 = property_field(0, index=True, groupby="zone")
    name: str = property_field("", dtype="U16", unique=True, groupby="zone")  # 组内唯一
    rank: np.int32 = property_field(0, groupby=("server", "guild"))            # 多个分组列
    ...
```

- `groupby: str | tuple[str, ...] | list[str] = ()`，单个字符串等价于只有一项的元组。
  - 给了就建复合索引。索引名仍是本字段名，列依次是 (分组列…, 本字段)。一个字段仍然只有一个索引。
- 隐含 `index=True`，规则和 unique、point_sub 一样：显式写了 `index=False` 时警告，并改成 True。
- `unique=True`：组内唯一，不同组可以同值。
- `point_sub=True`：按组发通知，见 §4.6、§6.3。
- 定义时的校验：
  - 位置：在 `warp` 里、所有字段都经过 `_normalize_prop` 之后做（bool 转 int8 是逐字段在 `_normalize_prop` 里做的）。
  - 一律 `raise ValueError`，不用 assert（`-O` 下 assert 会被去掉）。
  - 分组列必须是本组件定义过的字段，不能是本字段、`id`、`_version`，也不能重复。
  - 分组列的 dtype 只能是 kind `i`、`u`、`U`、`S`。
    - 必须按 `dtype.kind` 判断，不能用 `issubdtype(np.integer)`：timedelta64（kind `m`）会被它判成整数，
      而 `to_sortable_bytes` 处理不了 timedelta64。
    - 浮点不能做分组列：在浮点上按等值分组没有意义，还会遇到 NaN、-0.0 的相等问题——数据库按编码比较，
      缓存按 numpy 的 `==` 比较，两边结果会不一致。
  - 本字段的 dtype 只能是 kind `i`、`u`、`f`、`U`、`S`，和单列索引同一份白名单（§8.4）。
  - hidden 与 point_sub 的互斥扩展到分组列：本字段或任一分组列是 hidden，就不能开 point_sub。
    客户端不能按 hidden 列查询，组频道不会有人订。
- schema json：
  - `groupby` 写在本字段的属性里，统一写成列表（`"groupby": ["zone"]`）。这样 `"zone"` 和 `("zone",)` 得到同一个 schema 版本。
  - 只在非空时写，做法同 `hidden`（`component.py:229`）：没用复合索引的组件 schema 版本不变，不用迁移。
  - 旧版 HeTu 读到带 `groupby` 的 meta，会在 `Property(**prop)` 处报 TypeError（列级出现未知键）。所以新旧版本不能混跑同一个库，
    要一起升级，文档里要写明。
- 新增的类属性，由 `load_json` 生成：
  - `index_groups_: dict[str, tuple[str, ...]]`：索引名 → 分组列（按声明顺序）。每个索引都有一项，单列索引是 `()`。
  - `column_indexes_: dict[str, tuple[str, ...]]`：列名 → 用到这一列的所有索引名，包括它自己的单列索引和以它为分组列的复合索引。
    update 时判断要改哪些索引、`direct_set` 判断要拒绝哪些列，都查这一张表。

### 3.2 服务端查询

```python
await repo.range(level=(10, 20), zone=3)
await repo.range("level", 10, 20, _group={"zone": 3})
await repo.get(name="abc", zone=3)
await repo.get("name", "abc", _group={"zone": 3})
async with repo.upsert(name="abc", zone=3) as row:
    ...
```

**kwargs 写法怎么匹配索引**：

- `range`：恰好有一个 kwarg 的值是区间，它就是索引字段；其余 kwarg 必须正好是这个索引的全部分组列，值是标量。
  - "值是区间"指 `isinstance(v, (tuple, list))` 且长度为 2，并且排除 str / bytes / `np.generic`。
  - 现在的代码（`repo.py:387`）会解包任何长度为 2 的可迭代对象，`"s1"`、`b"ab"` 这样的分组值会被误认成区间，所以要改。
- `get` / `upsert`：kwargs 的键集合必须正好是某个索引的 {本字段} ∪ 分组列。
  - 匹配到 0 个，报 ValueError；
  - 匹配到多于 1 个，也报 ValueError，提示改用位置参数写法。例子：`a` 按 `b` 分组、`b` 又按 `a` 分组；
  - `upsert` 还要求匹配到的索引是 unique。
- 单列索引只给一个 kwarg，和现在完全一样。

**位置参数写法**：用新增的 `_group: Mapping[str, Any] | None = None`。下划线开头和 `_left` 一样，是为了避免和用户字段重名。

**分组值的解析**：所有入口共用一个 helper，`resolve_group_(comp_cls, index_name, group) -> tuple`。
它按分组列的声明顺序，返回规范化后的 Python 值；不合法就抛 ValueError。规则如下：

- **group 本身**：
  - 必须是键全为 str 的 Mapping。键的类型要先检查，再排序或比较；msgspec 解出来的键可能是 int、bytes、tuple，混在一起排序会抛 TypeError。
  - 键集合必须正好等于分组列。
  - `{}` 和 None 等价。
  - 复合索引没给 group（或者少给、多给），以及单列索引给了 group，都报 ValueError。
    提示语例如："level 是按 zone 分组的索引，要同时给出 zone=…"。
- **整数列**（kind `i` / `u`）：
  - 收 Python 的 int / bool、numpy 的整数 / bool 标量，以及刚好是整数的 float（Python 或 numpy）；
  - 精确值必须落在 `np.iinfo` 范围内，否则报 ValueError；
  - 不收字符串；CLI 自己先转换（§3.3）。
  - 必须先用精确值判断，不能先 `dtype.type()`：numpy 会静默回绕或截断。
    实测 `np.int8(np.int64(300))` 得 44，`np.int8(1.5)` 得 1；而 `np.int8(300)` 抛的是 OverflowError，它不是 ValueError，会落到 1011。
- **U 列**：只收 str。`np.str_(b"ab")` 会变成 `"b'ab'"`，必须事先拦住。
- **S 列**：收 bytes，或者能按 ASCII 编码的 str。
- **字符串的规范化**：按存储规则，用 `np.array(v, dtype=列dtype)[()]` 再 `.item()`。
  - 效果：超出列宽的部分截断，结尾的 `\x00` 去掉，和写进库里的值完全一致。
  - 不能用 `dtype.type(v)`：它不截断，`np.dtype("U4").type("abcdefgh")` 仍是 8 个字符。
  - 截断意味着前 N 个字符相同的长值会落到同一组。存储本来就分不开它们，文档里要提醒分组值不要超过列宽。
- **其他值**（None、list、dict、NaN 等）一律报 ValueError。

**哪些地方要用 `resolve_group_`**：

- repo 的 get / range / upsert；
- `idmap.match` 与 negative observation 的键（§5.2）；
- 订阅门面（§6.1）、CLI（§3.3）；
- 后端的汇合点：`zrange_args_` 与 `index_query_keys_`（`redis_model.py:326-346`、`:639-650`）也调用它兜底。
  这样即使入口处漏调了 helper，复合索引也不会用空 P 去查、读到别的组的 member。

**本字段 f 的值**：仍走现有的 `range_normalize_` 规则，单列索引的行为不变，包括 §14.3 的既有问题。

**upsert 新建的行**：写入规范化后的分组值，不写原值。

**后端 API**：

- 加仅限关键字的参数 `group: Mapping[str, Any] | None = None`。
  - 涉及：`BackendClient` 的 `range`、`range_index_`、`range_read_`、`check_range_`、`index_query_keys_`，以及 `TableMaintenance.range`。
  - `Table.servant_range` 只是绑定到 `BackendClient.range`（`table.py:72-73`），自动带上，不用改。
  - 必须是仅限关键字的参数：现有调用点有的按位置传 `row_format`，比如 CLI 的 servant 路径。
- repo 自己做缓存匹配时要用规范化后的元组，所以也会调一次 `resolve_group_`。解析两次只是几次字典查找，可以接受；
  如果实测在热路径上明显，再加一个内部的"已解析"类型。

**透传的入口**：`LocalApp`、`Sandbox`、`hetu shell` 的 `get / range / upsert` 直接透传给 repo，只改文档。

### 3.3 客户端与 CLI

- 线协议（§7）：`range` 和 `get` 消息末尾追加 `group`（map）。旧客户端照旧发，不受影响。
- C# SDK（§7.3）：
  - `WatchRange<T>(index, left, right, limit, desc=false, force=true, componentName=null, group=null)`；
  - `WatchRow<T>(index, value, componentName=null, group=null)`；
  - group 的类型是 `IReadOnlyDictionary<string, object>`；
  - 会话层、重连恢复、Headless 客户端、PostStation 都同步加上。
- CLI（`hetu/cli/data.py`）：
  - `hetu get COMP name=abc zone=3`：
    - query 参数从 `nargs="?"`（:305）改成 `nargs="*"`；
    - 按 §3.2 的规则匹配索引；匹配到多个时，用新增的 `--index NAME` 指定。
  - `hetu range COMP level 10 20 --group zone=3`：`--group` 可以重复给多个。
  - 分组值的转换：
    - 不能走 `convert_value(point=True)`（:38-69）。它对 str 会处理 `[` / `(` 前缀（`[abc` 会变成 `[[abc`），整数列还会原样返回带前缀的串；
    - 改用一个按字面值转换的函数：整数要精确，str 和 bytes 原样；转换后再交给 `resolve_group_`。
  - 读取路径：`--master` 路径（:143-149）传 `_group`；servant 路径用关键字参数 `group=`。
  - `check_index`（:72-84）的报错文案要说明复合索引要带哪些分组列。
  - `hetu get --list`：每列多输出一个 `groupby`。

## 4. 编码

### 4.1 member

复合索引 I 有分组列 g1…gk 和本字段 f。一行的 member 是：

```
member = E(g1) ‖ … ‖ E(gk) ‖ enc(f) ‖ 00 ‖ 十进制 id
P      = E(g1) ‖ … ‖ E(gk)          # 组前缀
```

- `enc(x) = to_sortable_bytes(np 标量)` 是现有的编码（`base.py:137`）：
  - 整数：8 字节大端，有符号数翻转符号位；
  - str 用 UTF-8，bytes 原样；两者都把 `00` 转义成 `00 ff`；
  - 结尾的 `\x00` 被 numpy 当补齐去掉，bcb7fc4f 的用例守着这一点。
- 分组列的编码：
  - 整数分组列：`E(g) = enc(g)`，定长 8 字节；
  - str / bytes 分组列：`E(g) = enc(g) ‖ 00 01`。
- 分组值先经 `resolve_group_` 规范化（§3.2），再转成该列 dtype 的 numpy 标量去编码。行里存的分组值直接取 numpy 标量编码。
- 单列索引就是 k = 0 的特例，格式不变。
- 一律从 numpy 标量出发编码（`to_sortable_bytes` / `to_sortable_bytes_list`），不能用 `bytes(row[col])`、
  `str(row[col])` 自己拼，否则结尾 `\x00` 的处理会和存储不一致。

### 4.2 为什么列与列之间不能用单个 00 当终止符

反例：索引 (name, x)，x 是 int64。查 name="a"、x 不限：

- 上界是 `[` + `61 00 | ff ff ff ff ff ff ff ff` + `00 ff`，其中 x 取 int64 最大值，编码全是 ff。
- name="a\x00b" 那行的 member 是 `61 00 ff 62 00 …`。
- 前三个字节和上界相同，第四个字节 62 < ff。于是这行落进区间，被当成 name="a" 的行查出来。

浮点列的 +inf 和 NaN 编码也以 ff 开头，同样会出问题。

单列索引用单个 00 没问题：00 后面紧跟的是十进制 id，第一个字节是数字或 `-`（0x2d–0x39），不可能是 ff。
所以本字段 f（最后一列）沿用单个 00，只有分组列用两字节的 `00 01`。CockroachDB 的 memcomparable 编码也是这个做法。

### 4.3 保序与无歧义

- **定长列（整数）**：按字节比较就是按数值比较；长度固定，不需要分隔。
- **变长列**：记 T = `00 01`，esc 为 `00 → 00 ff` 的转义。
  - 对字节串 a < b，有 esc(a)‖T < esc(b)‖T：
    - a 是 b 的真前缀：设 b 的下一个字节为 c。c 不是 00 时，T 的第一个字节 00 < c。c 是 00 时，esc 后是 `00 ff`，
      和 T 的 `00 01` 比第二个字节，01 < ff。
    - 第一个不同的字节满足 a[i] < b[i]：a[i] 是 00 时，esc 成 `00 ff`，而 b[i] > 00；否则字节原样保留，直接成立。
  - 当 x ≠ y 时，E(x) 不会是 E(y) 的真前缀：`00 01` 只会作为终止符出现，转义后的 00 后面一定跟着 ff，所以从左往右切分是唯一的。
  - U 列的 UTF-8 字节序就是码点序，和 numpy 比较 U 的顺序一致。
- 所以 member 的字节序就是 (g1, …, gk, f, id 的十进制串) 的逐列字典序。同组同值的行按 id 的十进制串排序，和单列索引现在一样。
- 已验证：审核时跑了性质测试（§16），300 种 schema、每种 60 行，排序、区间查询、UNIQ、切分全部一致。

### 4.4 区间两端

都是现有规则前面插上 P：

- 组内区间：
  - 下界：`[` + P + enc(lo) + (闭区间 `00` / 开区间 `00 ff`)；
  - 上界：`[` + P + enc(hi) + (闭区间 `00 ff` / 开区间 `00`)。
  - 整数区间的规范化、传反报错、空区间判定只作用在 f 上，逻辑不变；`range_normalize_` 加一个前缀参数即可。
- UNIQ：start = `[` + P + enc(v) + `00`，end = `[` + P + enc(v) + `00 ff`。
- `RangeObservation` 的 `bounds` 和 `query` 都带着 P。
  - `_drop_covered` 按 `(index_name, query)` 归组，所以不同组的观察不会被合并。
  - CNT、`shrink_observation_`、`_covers` 都不用改。
- 结果：防幻读的观察区间限定在组内，别的组的写入不会让本组的读 RaceCondition（§10 有用例）。

### 4.5 解码

- `range_index_`：先 rpartition 掉 `00 + id`，再去掉值段前 len(P) 个字节，用 `from_sortable_bytes(dtype_f, …)` 得到 f 的值。
  返回格式不变：`[(f 的值, id)]`。
- 重建索引、读取一致性核对都按行重新算 member 来比较，不需要解析多列。

### 4.6 频道

- **整个索引的频道**：仍是索引 key 的 keyspace 通知，不变。
- **组频道**：只有开了 point_sub 的复合索引才有，名字是 `{idx_key}:{sortable_token(P)}`。
  - 行"进入"某组时，commit 往 `value_chans` 里加这一组的频道。"进入"指 insert，或者 update 后值段变了（改了分组列或本字段，§5.1）。
  - 一个事务里每个 (索引, 组) 一条，消息为空串。
- **单列索引的值频道**：不变。一个索引要么是单列、要么是复合，两种频道不会出现在同一个索引上。
- **频道 API**：
  - 新增 `index_group_channel(table_ref, index_name, group)`。遇到单列索引，或者没开 point_sub 的复合索引，报 ValueError。
  - `index_value_channel` 遇到复合索引报 ValueError，提示改用 `index_group_channel`。现在它只检查 point_sub（`base.py:386-395`、
    `redis_model.py:141-159`），会给复合索引算出一个提交时永远不发的频道，调用方会静默收不到通知。
  - `websocket.py` 里 `index_value_channel(conn_tbl, "owner", user_id)` 这类单列调用不变。

## 5. 写入路径

### 5.1 commit payload（`build_commit_payload_`）

- **insert**：
  - 每个索引按整行算 member，ZADD；
  - unique 索引发 UNIQ，复合索引的 UNIQ 带 P；
  - 开了 point_sub 的复合索引登记组频道。
- **update**：`get_commit_rows` 给的是整行旧值，加上只含改动列的新值（`idmap.py:889-898`）。
  - 先合成新行：在旧值上覆盖改动列。
  - 受影响的索引 = 各改动列在 `column_indexes_` 里对应的索引，取并集。
  - 对每个受影响的索引，比较新旧值段（P + enc(f)）。只有不同时才处理：ZREM 旧的、ZADD 新的；unique 的发 UNIQ（按新值）；
    point_sub 的登记"进入"的频道（单列是值频道，复合是新组的组频道）。
  - 单列索引和复合索引走同一套逻辑。对单列索引，只有编码没变而字段被判成"改了"的情况，行为和今天不同。
    这种情况只出现在 NaN 变号：`changed_fields` 按字节判成"改了"，但编码统一了 NaN 的符号位。今天会发 UNIQ 撞上自己，
    抛 UniqueViolation；改后什么都不发。
  - 这里必须改的原因：现在的 `_exc_index` 按"改了的字段名就是索引名"来判断（`redis_model.py:827`），只改分组列时，复合索引不会更新。
- **delete**：按整行旧值算 member，ZREM。
- RACE / UNIQUE 的判定规则不变（本事务曾读空的 unique 值发生冲突判竞态），只是复合索引的"值"换成 (分组值…, 值) 元组（§5.2）。
- 提交脚本不用改：UNIQ 取 id 用的是 `.*%z(.*)$` / `rsplit(b"\x00", 1)`，复合 member 仍以 `00 + 十进制 id` 结尾，照样成立。
  `seen_uniq` 的键里也带着 P。

### 5.2 事务内（`IdentityMap`）

- `_sync_local_index`：判断 update 的行"改没改这个索引"，从只比一列（`idmap.py:750`）改成比较新旧值段。
- 排序键都走 `index_sort_keys_`，改成算复合 member。`LocalIndex` 的 placed / moved 逻辑不变。
- `match(ref, index_name, value, group=())`：
  - 按本字段和所有分组列一起筛；
  - 分组值用 `resolve_group_` 规范化之后的值，再用 numpy 的 `==` 比较；
  - 分组列只有整数和字符串，`==` 的结果和编码相等一致（numpy 数组的 `==` 忽略结尾 `\x00`）。
- negative observation 的键：集中成一个 helper 来构造，例如 `absent_key_(comp_cls, index_name, value, group)`。
  - 单列索引仍是 `(index_name, v)`，复合索引是 `(index_name, (g1…gk, v))`；元组每一项都是规范化后的 Python 值。
  - 所有调用点都要改用它：
    - `mark_absent`：`repo.py:295`、`:522`；
    - `observed_absent`：`repo.py:173`、`:278`，`idmap.py:766`；
    - `get_absent_unique_fields`：`idmap.py:567`；
    - `_unique_writes`：`idmap.py:792-797`。
  - 只要有一处还存成标量 v，`get(name="x", zone=3)` 读空之后，`zone=4` 的同名查询也会被 negative cache 判成不存在（跨组污染）。
- `RangeObservation.point`：只有本字段是点查时才是 `(组…, v)` 元组；区间查询仍是 None。否则 `_implied_by_unique` 会把区间当成点查。

### 5.3 `SessionRepository`

- `insert / update` 的本地 unique 检查：对每个受影响的 unique 索引调用 `idmap.filter(ref, **{列: 值 …})`，`filter` 已经支持多条件。
- `is_unique_conflicts`、`remote_has_unique_conflicts_`：按受影响的 unique 索引判断，远程点查带 group。
- `get`：按 §3.2 匹配索引；缓存命中、negative cache、`_range_read` 都带 group。
- `range`：`_unique_point_hit`、`_range_read`、`_moved_in_range` 都带 group。
  - `_unique_point_hit` 核对数据库态时（现在是 `repo.py:456-459` 只比 `db_row[index_name] != point`），要求 db_row 在该索引的
    **所有列**上都等于 (组…, 值)，才算命中。
  - 反例：本事务把 R 从 (A,"x") 改到 (B,"x")，而库里 B 组已有 S(B,"x")。如果只比 f，`range(name=("x","x"), zone=B)` 会命中缓存，
    只返回 [R]，不读库，也不登记观察。
- `upsert`：`UpsertContext` 记下规范化后的 group，新建的行同时写入本字段和分组列。

### 5.4 `direct_set`

`check_direct_set_` 现在只拒绝 `prop in indexes_`（`redis_model.py:1035`），分组列可以被直接写入，从而把复合索引写坏。

改成拒绝 `column_indexes_` 里的任何列。顺带补上"`direct_set` 拒绝写索引列"的用例，现在没有。

### 5.5 读取一致性核对（`_range_checks`）

`_range_checks`（`redis_model.py:675-722`）在提交前核对：range 读到的每一行，本事务手里的数据和读取时索引里的 member 是否一致。
现在它只比 `row[obs.index_name]`（:703-704），要改成比完整的值段 P(row) + enc(row[f])。

反例（只比 f 时会放过）：

- ZRANGE 在 A 组读到 R；
- 随后另一个事务把 R 挪到 B 组（f 不变），同时往 A 组的区间里插入 R2；
- HGETALL 读到的是已经在 B 组的 R；
- CNT 不变（R 出、R2 进），VER 校验的又是挪走之后的版本；
- 结果事务基于"R 在 A 组"这个从来不成立的读提交成功。

这正是 :680-683 的注释要防的情况。

### 5.6 同事务里挪 unique 值（§14.1 的方案 (b)）

**现象**：同一个事务里在行之间挪 unique 值会抛 UniqueViolation。单列 unique 也一样。

- 例子：A 的 slot 从 1 改成 3，再插一行占 slot 1；两行借一个中间值互换 slot；换组之后在原组补人。
- 原因：提交脚本的 UNIQ 查的是写入前的库，只豁免本事务删掉的行（`deleted`）。

**改法**：UNIQ 查到已有的 member，并且它的行不在 `deleted` 里时，再看本次 pushes 里有没有 ZREM 掉这个 member。有就不算冲突。

- 集合的建法：第一次用到时，从 pushes 里的 ZREM 命令建一个集合，键是 `索引 key .. "\0" .. member`。
  - payload 格式不变，也不多带字节。
  - 常见路径（UNIQ 没查到已有 member）零开销。
- 键必须带索引 key：同一行的两个 unique 列同值时，member 字节完全一样（`enc(5) ‖ 00 ‖ id`）。只按 member 豁免的话，
  挪走 a 列的 5 会把 b 列的 5 也一起豁免掉。
  - 索引 key 里不会有 `\0`，所以用 `\0` 拼接没有歧义。`seen_uniq` 也是这样拼的。

- 为什么安全：
  - ZREM 只针对本事务 update / delete 的库里行，这些行都有 VER 钉住；
  - 所以提交时库里这行的 member，一定就是 Python 按 clean 行算出来、要 ZREM 的那个；
  - 它在这次提交里会被移走，新值不会和它重复。
- 不能直接把被挪的行放进 `deleted`：`deleted` 按行 id 豁免，被挪的行可能还带着别的 unique 值，会被一起豁免掉。
- 两个提交脚本各加一处判断，`tests/test_backend_client.py` 的检查码用例两边共跑。

## 6. 订阅

### 6.1 门面（`SubscriptionBroker`）

- **参数**：`subscribe_range / begin_subscribe_range` 和 `subscribe_get / begin_subscribe_get` 都在末尾加 `group=None`。
  receiver 按位置转发，所以必须追加在最后。
- **校验**：
  - 在前半段（`begin_subscribe_*`）用 `resolve_group_` 校验，不合法就抛 ValueError，按非法请求断开（4002）。
  - 前半段之后抛出的异常一律落到 1011（`websocket.py:396-405`），也会进入 hub 的初始化重试，所以校验必须放在前半段。
- **get 订阅**：
  - `begin_subscribe_get` 的 `id` 分支（`sub.py:2333-2334`）不走 `servant.range`，会静默忽略 group，要显式拒绝。
  - 非 id 的复合索引走 `servant.range(…, group=…)`。
- **hidden**：`_hidden_query` 改成本字段或任一分组列是 hidden 就拒绝。现在它只看索引名。get 和 range 两条路径都调用它
  （`sub.py:2327`、`:2565`），改这一个函数两边都覆盖。
- **`check_range_`**：带上 group，当场校验。
- **sub_id 与共享键**：两者都用 group 的**原值**，不规范化，也就是键按名字排序后的 `k=repr(v)`。
  - 这样守住 `_share_key` 的不变式："同键一定同 sub_id"（`sub.py:2185`）。`join_` 让一个连接只占一格（`sub.py:911`），
    如果共享键比 sub_id 粗，同一连接用两个 sub_id 加入同一个订阅时会互相覆盖：一个永远收不到推送，退订任意一个另一个也会断。
  - left / right 现在就是这么做的（`sub.py:2625` 用 `repr` 原值）。`{zone: True}` 与 `{zone: 1}`、`3` 与 `3.0` 会成为两个订阅，
    只是少共享一些，结果都对。
  - sub_id 格式：单列索引不变；复合索引在末尾追加 `{k1=repr(v1),k2=repr(v2)}`。repr 会给字符串加引号并转义，值里有 `,`、`=`、`}`
    也不会撞，是单射。
  - `_share_key` 的 range 查询元组加上 `tuple((k, repr(v)) for k in sorted(group))`。
  - P、组频道、查询用的都是规范化后的值（§3.2）。
- **`_reread_range`**：现在按位置展开 `*query`，加 group 后改成关键字传参。

### 6.2 `IndexSubscription`

- `query_param` 加 `group`，`initialize_ / _rerange` 透传给 `servant.range`。初始化失败重试时，`query_param` 带着 group 重建。
- **频道选择**（`begin_subscribe_range` 里的 `make()`）：
  - 复合索引开了 point_sub：订组频道；
  - 复合索引没开 point_sub：订整个索引的频道，并警告一次。警告复用 `warn_point_sub_fallback_`，文案说明"按组订阅会被所有组的写入叫醒"；
  - 单列索引：不变。
- **离开判定**：
  - 现在 `get_updated`（`sub.py:396`）只在 `self.point_value is not None` 时才调 `_left_`，而 `_left_`（:406-412）按单列判断：
    `dtype.type(row[index_name]) != point_value`。
  - 复合索引没有单一的点值。如果不改门槛，组频道订阅永远不会进 `_left_`。后果：
    - 换组的行会以新组的值推给旧组的客户端，并且一直留在结果里；
    - 删除时 limit 截断的结果不会补位。
  - 改法：
    - `IndexSubscription` 加一个标记 `enter_only`：订的频道只报"进入"（值频道或组频道）时为 True；
    - 订阅时用 `index_query_keys_(…, group)` 预先算好 [lo, hi]；
    - `:396` 改成看 `enter_only`；
    - `_left_` 改成：行被删了，或者行的排序键不在 [lo, hi] 里。
  - 单列点查询是它的特例：`enter_only` 为 True，[lo, hi] 就是那个点。
- **limit 截断的结果里，同组的行换了顺序**：update 改了本字段，值段变了，commit 会发组频道，订阅会 `_rerange`，不会漏。
  [lo, hi] 与 desc 无关。
- **索引比行旧**：会短暂推出带新组值的行，下一次重读会更正。这和现有 point_sub 的行为一致。
- RLS 按行判定，不变。

### 6.3 组频道的触发规则汇总

| 事件 | commit 发什么 | 订阅方怎么发现 |
|---|---|---|
| insert 进组 | 该组的频道 | 组频道 → `_rerange` |
| update 改本字段（同组内） | 该组的频道 | 组频道 → `_rerange` |
| update 改分组列（换组） | 新组的频道 | 新组：组频道；旧组：行频道 → `_left_` |
| delete | 不发 | 行频道 → `_left_` |
| 只改了不在这个索引里的列 | 不发 | 行频道，推送新行 |

## 7. 线协议与 C# SDK

### 7.1 消息格式

- range：`["sub", 组件名, "range", index, left, right, limit, desc, force, group]`，5–10 项。
  - group 缺省、为 None 或 `{}`，都表示不带 group。
  - C# 现在总是发满 9 项；新 SDK 只在有 group 时才发第 10 项。
- get：`["sub", 组件名, "get", index, value, group]`，5–6 项。
- `receiver.py:213-226` 的长度校验跟着改：range 是 5–10 项，get 是 5–6 项。
- 回复、推送的格式不变。

### 7.2 兼容与升级顺序

- 旧客户端不发 group，查单列索引，行为不变。
- **把正在使用的单列索引改成复合索引，是破坏性变更**：
  - 服务端所有不带 group 的调用都会抛 ValueError。
  - 旧客户端的订阅会被 4002 断开，而 SDK 把 4000–4999 当作终态：抛 `HeTuConnectionRejectedException`，不重连，会话进入 Faulted。
    "要带 group"的提示只写进服务端日志，客户端只能看到 rejected。
  - 建议的做法：在新字段上建复合索引，客户端全部换完之后，再删掉旧索引。这一点写进文档。
- **新 SDK 带 group 连旧服务器**：同样会被 4002 断开。SDK 的 `package.json` 按 `ClientSDK/unity/CLAUDE.md` 的约定升版本，
  并注明需要的最低服务器版本。

### 7.3 C# SDK 要改的地方

下面的路径前缀都是 `ClientSDK/unity/cn.hetudb.clientsdk/HeTu/`。

- **入口处理**：所有接收 group 的公开方法，一进来就做两件事。
  - 类型检查：键是 string；值只能是 int / long / bool / 刚好是整数的 double / string / byte[]。不合法就抛 ArgumentException，
    免得一次 4002 让整个会话进入终态。
  - 拷贝：把 group 拷成一个新的、按键排好序的 `Dictionary<string, object>`（排序用 `StringComparer.Ordinal`），后面的
    payload、`RestoreGroup`、pending 闭包、predictID 都用这份拷贝。
    - 这样调用方事后改字典不会影响已发出的订阅；
    - MessagePack 序列化的是普通 Dictionary（任意 `IReadOnlyDictionary` 实现能否被序列化，审核时存疑）；
    - `InspectorTrace` 只认 `IDictionary`，用拷贝它也能正常记录。
- **"id" 上不许带 group**：`WatchRow` 在 "id" 上会走预测键（`ClientBase.cs:670-679`），group 会被忽略。带了 group 就抛 ArgumentException。
- **`MakeSubId`（`ClientBase.cs:578-581`）**：有 group 时追加一段 group。
  - 要求：不同的查询一定得到不同的键，而且这个键不能等于任何查询的服务端 sub_id。客户端键和服务端 sub_id 在同一个查找空间里：
    `TryGet` 先查 `_subscriptions`（存的是服务端 sub_id），再查别名（`Subscription.cs:957-968`）。
  - 格式：
    - group 段以一个服务端从不输出的标记开头；
    - 值带类型标记，并加长度前缀或转义，例如 `n:3` / `s:<字节长度>:<值>` / `b:<hex>`；
    - 键用 `StringComparer.Ordinal` 排序。
  - 反例：只用"键排序 + 原样拼接"的格式会撞。
    - `{b:"4142"}` 和 `{b: byte[]{0x41,0x42}}` 拼出同一个键，但服务端把它们当成不同的值；
    - 两个 str 分组列时，`{a:"x,b=y", b:"z"}` 和 `{a:"x", b:"y,b=z"}` 拼出同一个键。
  - left / right 的格式不变（单列查询的客户端键不变），既有的碰撞问题记在 §14.4。
- **客户端键与服务端 sub_id 不必逐字一致**（审核已核实）：
  - 客户端先用 predictID 查重；没查到就发 sub，用服务端回来的 sub_id 登记核心（`ClientBase.cs:828-853`）；
  - 推送按服务端 sub_id 分发，退订按核心的 SubId；
  - `AddAlias(predictID, subID)`（:856-858）让下一次同样的查询能命中；
  - 会话层的快路径查不到时，落到 transport，再由别名兜底（`SessionClientBase.cs:495-544`、`:1062-1076`）。
- **payload**：
  - `WatchRowSync / WatchRowCoreSync` 的 payload（:682）、`WatchRangeSync / WatchRangeCoreSync` 的 payload（:809-813）、
    `ConfigureRestoreQuery`（:851）带上 group；
  - 基类的字典版重载也要加：`WatchRowSync`（:739-743）、`WatchRangeSync(componentName, …)`（:877-881）。
- **`UnityClient.cs`**：`WatchRow<T>`（:330-369，字典版 :380-387）、`WatchRange<T>`（:427-470，字典版 :485-494）。
- **会话层**：
  - `SessionClientBase.cs`：`IHeTuSessionTransport` 接口（:122-142）、会话 `WatchRow`（:418-478）、
    会话 `WatchRange`（:480-544，pending 和查重键用的是 `MakeSubId`）；
  - `SessionClient.cs`（:302-352、:549-573）。
- **重连恢复**：`Subscription.cs` 的 `ManagedIndexCore` 加 `RestoreGroup`（:781-789、:835-850、:870-909）。别名表在 :933-996。
  行订阅按 id 恢复（:732-765），不需要 group。
- **Headless**：`ClientSDK/csharp/HeTu.Client/HeadlessHeTuClient.cs` 的 `WatchRange<T>`（:156-168）、`WatchRow<T>`（:193-204）。
- **Editor**：`HeTuPostStationWindow.cs` 加一个 Group 输入框（JSON 对象）。`ParseJsonValue` 已经能解析出字典和列表。
- **测试**：
  - 假 transport 跟着加参数：`Tests/Editor/SessionClientBaseTest.cs`、`Tests/HeTu/SessionClientFacadeTest.cs`。
  - 单列的客户端键和服务端 sub_id 格式都不变，所以 `SubscriptionHandleTest`、`InspectorTraceTest`、`HeadlessHeTuClientTests`
    里写死的 sub_id 不用改。
  - 按 `ClientSDK/unity/CLAUDE.md`，`UnityClient.cs`、`SessionClient.cs`、PostStation、`Tests/HeTu/SessionClientFacadeTest.cs`
    只能在 Unity 里编译，所以除了不开 Editor 跑的那部分测试，还要在 Unity 里编译、跑一遍。

## 8. 维护

### 8.1 迁移

- 只改索引声明、dtype 不变的迁移走 skip 路径。是否重建索引由 `_finish_schema_migration`（`base.py:1133-1160`）比较
  `(indexes_, uniques_)` 决定。
  - 给已有索引加上或去掉 `groupby` 时，这两项都不变，所以不会重建，库里会留下旧格式的 member。
  - `hetu upgrade` 默认会在迁移后全量重建索引（`cli/migrate.py:197-203`、`manager.py:125-133`）。所以这个问题只在用
    `--no-rebuild-index`，或者在程序里直接调 `create_or_migrate_all` 时才会出现。
  - 改法仍然需要：比较 `(indexes_, uniques_, index_groups_)`。
- dtype 变化的路径每一级都会重建索引（`hetu/data/migration.py:214`），不用改。
- 重建时 unique 冲突的报错文案（`redis/maint.py:270-274`、`sqlite/maint.py:261-269`）要加上一种原因：去掉 unique 字段的 `groupby`，
  等于把组内唯一变成全局唯一。
- `default_migration.py:102-109` 的 unique 警告只对新增的列生效。复合 unique 的情况，文案写"组内唯一"。

### 8.2 重建索引

- Redis（`redis/maint.py:230-283`）：
  - 读取：每行 HGET 一个字段，改成 HMGET 该索引的所有列。
  - 改成两阶段：先把所有索引都建到临时 key 并通过 unique 检查，再统一 RENAME。
    - 现在是"建一个、换一个"。如果第二个索引遇到组内 unique 冲突，第一个已经换成复合格式，而 meta 没更新
      （`base.py:1148-1155`）。这时回退代码，再用 `--no-rebuild-index` 升级，第一个索引就是旧定义读不懂的格式。
- SQLite（`sqlite/maint.py:237-295`）：
  - `row_field_values` 改成能一次取多列；
  - SQLite 本来就是全部建好之后，在一个事务里整体替换（:289-294）。
- `rebuild_member_` 改成按多列算 member，规则同 §4.1，和 commit 写入的一致。`test_backend_dtypes.py` 里"重建出的 member
  与 commit 写入的一致"的用例要扩展到复合索引。
- unique 查重按 member 的值段（去掉 `00 + id`）去重，报错信息给出 (分组列…, 本字段)。

### 8.3 headless

`headless.py` 的 `_schema_diff` 逐列比较的键是 `("dtype", "unique", "index", "point_sub", "hidden")`（:160），要加上 `groupby`。
它用 `.get(key, False)` 取值，缺键时照样能比。

不加的后果：本地类和服务器 meta 的分组定义对不上时比对会放行，而 headless 的 commit 按本地类维护索引，会把服务器上的索引写坏。

### 8.4 其他

- `_schema_checking`（`redis_model.py:74-96`）现在是黑名单：只拒绝复数和 object。
  - 改成按 kind 的白名单：索引列（含本字段）为 `iufUS`，分组列为 `iuUS`。
  - 这顺带修了一个既有问题：datetime64 / timedelta64 的单列索引今天要到提交时才在 `to_sortable_bytes` 里崩，改后在启动时就报错。
- 训练样本：`hetu/server/pipeline/zstd.py:87-96` 和 `benchmark/hypothesis/ya_compressors.py` 里加几条带 group 的 sub_id。只影响压缩率。
- `tests/test_arch_publish.py`：模块说明和 `ALLOWED_LUA` 的理由文案里写的是"(索引, 值)"，要补上组频道。规则本身不改。
- `commit_v2.lua` Phase 3 的注释：值频道那一条补上组频道。只改注释。
- 新增的 `_()` 文案不用手改 .po，发版时 CD 会同步。

## 9. 性能

- **member 变长**：每个整数分组列多 8 字节，每个 str / bytes 分组列多"编码长度 + 2"字节。
  - 存疑：小 zset 更容易超过 `zset-max-listpack-value`（默认 64 字节）而转成 skiplist，内存会多一些。大索引本来就是 skiplist，不受影响。
- **commit**：
  - 每个受影响的索引仍是一条 ZADD / ZREM，update 只在值段变了时才发，redis.call 的条数和单列索引一样。
  - 组频道只在声明了 point_sub 时才发，一个事务里每个 (索引, 组) 一条 PUBLISH。
  - 代价要写进 `property_field` 的 point_sub 文档：开了 point_sub 的复合索引，同组内本字段每变一次都要发一条，例如"score 按 season
    分组"时每次改分都会发。这是正确性必需的：组外截断窗口之外的行，可能因为改分挤进窗口。
  - 对比现在推荐的写法（season 单列 point_sub 加 numpy 筛选）：那种写法只有 insert 和换 season 才发。
- **约束**：不新增 master 读（`test_arch_master_reads.py`），不新增 PUBLISH 调用点（`test_arch_publish.py`），提交脚本不变。
- **Python 开销**：排序键按列向量化编码，再逐行拼接；update 查 `column_indexes_` 只是字典查找。
- **和"单索引 + numpy 筛选"比**：只读本组的行；防幻读的区间限定在组内，减少无谓的 RaceCondition。

## 10. 测试

以两个后端（redis、sqlite）共跑的用例为主。

**编码**：

- 性质测试：随机生成各种分组列 / 本字段 dtype 组合的行，验证 member 的字节序等于规范化后元组的逐列顺序，并且 member 能唯一切分。
  - 整数：各种宽度，有符号和无符号；
  - U / S：中间带 `\x00`、结尾带 `\x00`、空串、以 `\xff` 开头的 bytes、非 BMP 字符；
  - 本字段：±inf、NaN、-0.0；
  - 负数 id。
- §4.2 的反例做成回归用例。
- `to_sortable_bytes_list` 的批量结果与逐个调用一致。

**解析与报错**（`resolve_group_`）：

- 整数越界、不是整数的 float（3.5）、字符串，都报 ValueError；numpy 宽类型（int64 的 70000 给 int16 列）不会静默回绕。
- U 列给 bytes、None、list，都报 ValueError。
- 超长字符串按列宽截断；结尾 `\x00` 被去掉。
- 键不是 str、少键、多键、`{}`。
- kwargs 匹配到多个索引；kwargs 写法里长度为 2 的 str / bytes 分组值不会被当成区间。
- 单列索引给了 group；后端入口（`zrange_args_`）漏给 group 时报错。

**查询**：

- 组内区间：limit、desc、开闭区间、±inf；
- 组内点查、`range_index_`、空组。

**读己之写**：本事务 insert、update（同组改本字段、换组）、delete 之后的 range 与 get，以及 limit 截断。

**防幻读与一致性**：

- 别的组插入不会让本组的读竞态；本组插入照样竞态；
- `InconsistentRangeRead`，包括 §5.5 的"读取期间换组"。

**组内 unique**：

- 不同组同值可以；同组同值抛 UniqueViolation；
- 读空之后被并发插入，判 RaceCondition；
- `upsert` 按 (组, 值)；`get` 的 negative cache 带组，`zone=3` 读空不影响 `zone=4`；`is_unique_conflicts`；
- 换组时撞上新组里的同值；§5.3 的 `_unique_point_hit` 反例；
- NaN 变号不再撞自己（单列与复合都测）；
- §5.6 的"同事务挪值"：挪槽位后补位、两行互换、换组后原组补人都能提交（单列与复合都测，两个后端共跑）；
  挪到别的行已占着、且这次没有挪走的值，照样报 UniqueViolation；
  同一行两个 unique 列同值时，挪走一列的值不会豁免另一列。

**direct_set**：拒绝写分组列和索引列。

**迁移**：

- 给已有索引加上或去掉 `groupby` 之后会重建，包括用 `--no-rebuild-index` 的路径；
- 重建时遇到组内 unique 冲突要报错，所有索引都保留旧的（Redis 两阶段）。

**headless**：`_schema_diff` 能发现 `groupby` 不一致。

**订阅**：

- 组频道只被本组的写入叫醒；
- 换组、删除、移出区间由行频道发现并补位，limit、desc 都要测。可以对照现有的 `test_point_query_leave_backfills_limit`
  （`test_backend_sub.py:1008`）和 `…_row_leaving_before_its_channel_is_active`（:1053），各写一个组频道版本；
- 同组内换顺序；
- RLS 私有订阅里，有不可见的行占着 limit；
- 没开 point_sub 时的退化与警告；hidden 分组列被拒；
- 复合索引调 `index_value_channel`、单列索引调 `index_group_channel` 都报错（参照 `test_backend_sub.py:1293`）；
- 共享键的不变式：同一连接用等价但类型不同的值订两次（`{zone: True}` 与 `{zone: 1}`），两边都收到推送，退订一个不影响另一个；
- 带 group 的 get 订阅；`id` 上带 group 被拒。

**发布次数**（`test_commit_publish_budget`，复合索引）：

- 开 point_sub 时：insert、同组改本字段、换组（只发新组）各 1 条；只改别的列、delete 为 0 条；一个事务改同组多行为 1 条；
- 没开 point_sub 时为 0 条。

**master 读**：`test_master_read_budget.py` 补复合索引的路径。

**线协议**（`test_websocket.py`）：

- 带 group 的 range / get 消息、长度校验；
- 非法 group 断开 4002 而不是 1011，并且不进入 hub 的初始化重试（不出现"初始化订阅出错"日志，也没有约 0.7 秒的延迟）；
- 旧格式消息不变。

**Redis cluster**：组频道按 slot 路由（`test_backend_redis_cluster_pubsub.py`）。

**C# SDK**：

- `MakeSubId` 带 group 不碰撞：§7.3 的两个反例，以及 byte[] 与 hex 字符串；
- 调用之后再改 group 字典不影响订阅；
- 客户端类型检查；`WatchRow` 在 "id" 上带 group 被拒；
- payload；会话层带 group 恢复；Headless 客户端；
- 不开 Editor 能跑的部分按 `ClientSDK/unity/CLAUDE.md` 跑，其余在 Unity 里编译、跑。

**CLI**：

- `get` 多个 `FIELD=VALUE`、`--index`；
- `range --group`；分组值以 `[` 开头时按字面值处理；
- `--master` 路径、`--list`。

**架构守门**：`test_arch_publish.py`、`test_arch_master_reads.py` 的规则不改，照样要通过。

## 11. 文档

zh 和 en 在同一个提交里手工同步。不要跑 `translate_new_content.py`，它会自动 commit 并 push。

- `docs/{zh,en}/concepts.md`：
  - 索引一节加复合索引；
  - 订阅一节加按组订阅与 point_sub 组频道；
  - point_sub 的代价一段（zh:181-183）补上复合索引的情况（§9）；
  - hidden 规则补上分组列；
  - 新旧版本不能混跑。
- `docs/{zh,en}/advanced.md`：
  - 原来推荐"单索引 + numpy 筛选"和"拼接字符串当复合键"的地方，改成推荐复合索引；numpy 筛选保留，作为补充手段；
  - headless 一节（zh:623）的比对项加上 `groupby`。
- `docs/{zh,en}/unity-client.md`：`WatchRange / WatchRow` 的 group 参数、示例、重连恢复，以及最低服务器版本。
- `docs/{zh,en}/operations.md`：
  - CLI 的 `get`、`range --group`、`--index`；
  - 加或去掉 `groupby` 时不要用 `--no-rebuild-index`（zh:239-245）；
  - §7.2 的破坏性变更与升级顺序。
- `docs/api/`：由 `scripts/gen_api_docs.py` 从 docstring 生成。要改的 docstring：
  - `property_field`、`get`、`range`、`upsert`；
  - `sub.py:2505-2517`、`base.py:367-384`；
  - "如何复合条件查询"的推荐写法（`base.py:693-701`、`redis_model.py:562-568`）。
- 其他：`hetu/llms.txt`、`hetu/skills/building-on-hetu/SKILL.md`、`hetu/skills/hetu-csharp-client/SKILL.md`、
  `ClientSDK/unity/cn.hetudb.clientsdk/README.md`、`ClientSDK/unity/CLAUDE.md`。

## 12. 与 commit_seq 的关系

`commit_seq` 就是"本字段在提交时由 Lua 发号"的复合索引：

- Python 按 §4.1 算好 P，Lua 发号后拼上 enc(seq) ‖ 00 ‖ id；
- 消费方的按组订阅用 §4.6 的组频道。

本设计为它留下的前提：

- member 的布局是 P 在前、本字段在后。P 只依赖分组列，提交前就能算出来。
- 组频道只依赖 P，不依赖本字段的值。

`commit_seq` 自己的规则在它的 spec 里定，包括：不登记 unique、分组列不可改、未分配号的行不进结果、计数器。

## 13. 已定事项

1. 用字段级 `groupby` 声明，索引名就是本字段名，一个字段只有一个索引。（用户定）
2. 第一版包含 str / bytes 分组列、组内 unique、point_sub 组频道、客户端订阅。（用户定）
3. 查询必须给出全部分组列。部分前缀、分组列上的区间、跨组扫描以后再做，放宽不会破坏兼容。
4. 分组列只能是 kind `i` / `u` / `U` / `S`，不支持浮点（§3.1）。
5. 分组列之间的终止符用 `00 01`，本字段沿用单个 `00` 加 id。
6. 开了 point_sub 的复合索引按组发频道，不按 (组, 值) 发。
7. 单列索引的存储、查询、通知、sub_id、线协议、客户端键都不变；提交脚本只加 §5.6 的 UNIQ 豁免。
8. 分组值的解析见 §3.2：
   - 类型走白名单；
   - 整数必须是精确值且在范围内，否则报 ValueError；
   - 字符串按存储规则规范化：截断到列宽、去掉结尾的 `\x00`。
   
   这一条取代了初稿的"越界返回空"。
9. sub_id 和共享键都用 group 原值的 repr，P、频道、查询用规范化后的值。
10. update 时单列索引和复合索引走同一套逻辑，值段变了才发 ZREM / ZADD / UNIQ / 频道（顺带修掉 NaN 变号撞自己）。
11. `_schema_checking` 改成按 kind 的白名单。
12. Redis 重建索引改成两阶段：全部建好、检查通过之后，再统一 RENAME。
13. C# SDK 在入口处做类型检查、拷贝并排序 group；`MakeSubId` 的 group 段单射，并带一个服务端从不输出的标记。
14. 同事务挪 unique 值按 §5.6 修（用户选了方案 (b)）。

## 14. 已知问题与待定项

1. **已定：方案 (b)，设计见 §5.6。下面保留当时的分析。同一事务里在行之间挪 unique 值会抛 UniqueViolation。**
   - 现象：这是既有行为，单列也一样。比如：
     - A 的 slot 从 1 改成 3，再插一行占 slot 1；
     - 两行借一个中间值互换 slot；
     - 换组后在原组补人。
     
     审核时在 SQLite 上实测，前两种都抛 UniqueViolation；先删 A 再插则能提交。
   - 原因：
     - 提交脚本的 UNIQ 查的是写入前的库，只豁免本事务删除的行（`commit_v2.lua:61-73`、`sqlite/commit.py:68-79`）；
     - 本地检查能过，因为缓存里已经是新值。
   - 影响：组内唯一最常见的用法，比如背包槽位、座位、排位，都会遇到这类写法。UniqueViolation 是确定性错误，不会重试。
   - 方案：
     - (a) 写进已知问题和文档，补用例锁定现状。
     - (b) Python 把本次 payload 要 ZREM 的 member 一并传给提交脚本，UNIQ 命中这些 member 时不算冲突。
       - 为什么安全：被挪的行都有 VER 钉住，库里它的 member 一定就是按 clean 行算出的那个。
       - 不能直接把被挪的行放进 `deleted`：它可能还带着别的 unique 值，会被一起豁免。
       - 代价：Lua 和 SQLite 两边各改几行，打破 §13.7 的"提交脚本不变"；单列的同类问题一起修好。
2. 删除一个索引后，旧的 `…:index:<名>` zset 不会被删除：`do_rebuild_index_` 只遍历当前的 `indexes_`。
   - 复合索引本身不受影响：单列改复合、复合改单列都沿用同一个 key 名，重建时会覆盖。
   - 但把整个索引删掉仍会残留。dtype 变化的迁移经过 `__temp__` 改名再删表，会顺带清掉残留，只有 skip 路径会留下。
3. 本字段 f 的值超出列宽时查不到。
   - 原因：`range_normalize_` 用 `dtype.type()`，不截断。
   - 后果：同一个超长值第二次 upsert 会抛 UniqueViolation。
   - 这是单列索引既有的问题，复合索引的本字段一样。分组值已经按 §3.2 截断，不受影响。建议另行修复。
4. C# `MakeSubId` 的 left / right 有既有的碰撞问题：
   - byte[] 插值得到 `System.Byte[]`；
   - `right ?? "None"` 和字符串 "None" 撞键。
   
   本次不动单列查询的客户端键，建议另行修复。

## 15. 实施顺序（建议每一步都带测试）

1. 定义：`Property.groupby`、定义校验、schema json、`index_groups_` / `column_indexes_`、`_schema_checking` 白名单、headless 比对、迁移比对。
2. 编码与解析：`resolve_group_`、复合 member、区间两端、解码、频道名与频道 API，以及编码和解析的用例。
3. 写入：commit payload 的 insert / update / delete（含单列共用的"值段变了才发"）、UNIQ、组频道、`direct_set`，以及 §5.6 的 UNIQ 豁免。
4. 事务读：`IdentityMap`（match、absent 键 helper、LocalIndex）、`SessionRepository` 的 get / range / upsert / 冲突检查、
   `_unique_point_hit`、`_range_checks`。
5. 维护：两个后端的重建索引（Redis 两阶段）、`TableMaintenance.range`、CLI。
6. 订阅：门面（校验、sub_id、共享键、hidden、get 的 id 分支）、`IndexSubscription`（`enter_only` 与 `_left_`）。
7. 线协议、C# SDK（入口检查、`MakeSubId`、重载、会话恢复）、PostStation、版本号，以及 Unity 编译。
8. 文档。

## 16. 审核记录

2026-11-08 做了两路独立审核：一路看存储、编码、事务，一路看订阅、协议、SDK、CLI。审核员只拿到本文与代码。

**核实无误的部分**：

- §4 编码的保序与无歧义。存储一路跑了性质测试：
  - 300 种 schema、每种 60 行；分组列有 int8 / int64 / uint64 / int32 / U4 / S4，本字段有 i8 / f8 / U4 / S4 / u1 / i2；
  - 值覆盖中间 `\x00`、`\x01`、`\xff`、空串、非 BMP 字符、±inf、NaN、-0.0、负 id；
  - 结果：排序、3000 次开闭区间查询、UNIQ、切分还原全部一致。
- §4.2 的反例成立。
- 提交脚本取 id 的方式对复合 member 照样成立。
- 组频道"只报进入、离开靠行频道"在 limit、desc、换组、删除、RLS、共享订阅、尾随重读、初始化重试下都不漏推送，
  前提是 §6.2 的门槛修好。
- §7.3 的说法成立：客户端键不必与服务端 sub_id 逐字一致。
- 没有新增 master 读，没有新增 PUBLISH 调用点。

**按审核意见修改的部分**（对应本文的位置）：

- 分组值的解析整体重写（§3.2、§13.8）。初稿写的"先 `dtype.type()` 再编码"有三个问题：整数会静默回绕或截断，字符串不会截断，
  OverflowError 会落到 1011。
- sub_id 与共享键统一用原值的 repr（§6.1、§13.9），守住"同键必同 sub_id"。
- `_left_` 的调用门槛改用 `enter_only`（§6.2）。
- C# `MakeSubId` 的 group 段改成单射、带标记（§7.3）；入口做类型检查、拷贝并排序；补上字典版重载；"id" 上拒绝 group；加上 Unity 编译这一步。
- 后端汇合点兜底校验 group（§3.2）；`index_value_channel` / `index_group_channel` 互相拒绝对方的索引（§4.6）。
- `_unique_point_hit` 核对所有列（§5.3）；`_range_checks` 按完整值段核对（§5.5）；negative observation 的键集中构造，
  `RangeObservation.point` 只在点查时为元组（§5.2）。
- update 路径统一成"值段变了才发"（§5.1）。
- kwargs 区间按类型判定（§3.2）。
- 定义校验按 kind 判断、放在 `warp` 里，`groupby` 统一写成列表（§3.1）；`_schema_checking` 改成白名单（§8.4）。
- Redis 重建改成两阶段（§8.2）。
- §8.1 的背景更正：`hetu upgrade` 默认全量重建。
- 补充：point_sub 的 PUBLISH 量与文档说明（§9）；兼容与升级顺序、SDK 版本号（§7.2）；CLI 的细节（§3.3）；测试与文档的遗漏（§10、§11）。
- 行号更正：`Subscription.cs` 的别名表到 :996；`Table.servant_range` 是绑定，不用改。

**没有采纳的部分**：

- "顺手修 C# left / right 的既有碰撞"：会改变单列查询的客户端键，不在本次范围，记在 §14.4。
- 超长的分组值：审核给了"截断"和"报错"两种做法，这里选了截断，与存储一致。

**交给用户决定的**：§14.1，同一事务里挪 unique 值。用户选了方案 (b)，设计写在 §5.6。
