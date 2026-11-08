# 提交序号列（`commit_seq`）— 设计稿

- 日期：2026-11-09
- 状态：已实现（分支 `feat/commit-seq`），实现记录见 §17，实现审核见 §18。设计过了一轮独立审核（§16），已按审核意见改写。
  - §13 是定下来的做法；
  - §14 是用户的决定（2026-11-09 全部定完）。
- 分支：`feat/commit-seq`（基于 dev `aaf30a73`）
- 前置：
  - 复合索引（PR #211，`2026-11-08-composite-index-design.md`，下称"复合索引稿"）。它的 §12 给本设计留的两个前提都已满足：
    - member 的布局是 P 在前、本字段在后；
    - 组 key 只依赖分组列。
  - 定宽 id 段（PR #214，`INDEX_FORMAT = 2`）。
- 影响范围：
  - 定义：`hetu/data/component.py`（`property_field(commit_seq=)`、定义校验、schema json、类属性 `commit_seq_`）
  - 提交：
    - `hetu/data/backend/redis_model.py`：payload、`raise_for_commit_response_`
    - `redis/commit_v2.lua`
    - `sqlite/commit.py`、`sqlite/store.py`（计数器原语）
  - 事务：`repo.py`（禁写、分组列锁定、upsert 锚定检查改成 raise）、`idmap.py`（未分配的行不进索引结果）
  - 维护：
    - `base.py`：迁移时的比较
    - `redis/maint.py`、`sqlite/maint.py`：改名时计数器的去留、重建时校准计数器、SQLite 的 drop / rename
    - `hetu/data/default_migration.py`
  - 外围：`hetu/headless.py`（schema 比对）、`hetu/cli/data.py`（`--list`）
  - 测试、文档（含 `repo.get` / `range` 等 docstring）
- **不改**：
  - 索引的存储格式（member、组 key、`INDEX_FORMAT`）；
  - 查询 API、订阅、线协议、C# SDK；
  - 不新增 PUBLISH；事务路径上不新增 master 读。

## 1. 背景

游戏项目里有一种类似 binlog 的数据同步：写入方的组件带一个 `seq_id`，消费方记下"消费到了第几号"，要求号连续、没有缺口。

现在号是在提交之前发的（事务外的计数器，或者事务里先拿号），有两个问题：

- **乱序**：多个进程同时写，2 号的事务可能先提交、1 号后提交。消费方读到 2 而没有 1，只能停下来等。
- **永久缺口**：拿了号的事务因冲突重试或者失败了，这个号就没了，消费方永远等不到。

在事务里读改写一行计数器能避免缺口，但同一组的写入都会读写这一行，会全部撞 VER、互相重试。

`commit_seq` 把发号挪进提交脚本：所有检查通过之后才发号，发号和写入在同一次原子执行里完成。

## 2. 目标与非目标

目标：

- 字段上声明 `commit_seq=True`，提交成功时由提交脚本发号。可以和 `groupby` 一起用，每组各自从 1 编号。
- 满足 §4 列出的保证：
  - 提交成功才发号；
  - 组内连续，按提交顺序；
  - 只增不减；
  - 在同一个节点上读到 k 号时，1..k-1 号一定已经可见。
- 发号不进读集，不引入新的竞争：同组的并发写入不会因为发号而 RaceCondition。
- 这一列不许用户写；有分组时，已经有号的行不能改分组列。
- 两个后端行为一致；迁移、重建索引时，计数器能保住，并能校准到库里已有的数据。

非目标（第一版不做）：

- 在 System 里拿到刚发的号（§14.1）；
- 一个组件有多个 `commit_seq` 列；
- 给历史行补号（要的话写迁移脚本，见 §8.1）；
- 跨表、跨组的统一顺序；
- 消费方的游标管理、恰好一次的消费语义。这些属于应用层，§9 给推荐写法；
- 不登记版本的读（get 和 range，§10、§14.4）；
- 整体抬高计数器的运维工具（§4.5）。

## 3. 用户接口

### 3.1 声明

```python
@define_component(namespace="ssw", permission=Permission.OWNER)
class ChangeLog(BaseComponent):
    owner: np.int64 = property_field(0)
    seq: np.int64 = property_field(0, commit_seq=True, groupby="owner")  # 每个 owner 从 1 连续编号
    kind: np.int16 = property_field(0)
    payload: str = property_field("", dtype="U128")


@define_component(namespace="ssw")
class WorldEvent(BaseComponent):
    seq: np.int64 = property_field(0, commit_seq=True)  # 整张表一个序列
    ...
```

- **放在 `_normalize_prop` 里、排在 hidden + point_sub 那条报错之前处理**（`component.py:662-668`）：
  - 隐含 `index=True`。显式写了 `index=False` 时警告，并改成 True，规则同 unique、groupby。带 groupby 的是复合索引，不带的是单列索引。
  - 写了 `point_sub=True`：警告并忽略。排在那条报错之前，是为了 `hidden=True, point_sub=True` 的组合也走这条，而不是直接报错。
    - 带 groupby 的，复合索引本来就会忽略 point_sub（`component.py:635-642`）。
    - 不带 groupby 的，值频道要在 Python 里按值命名，而值要到提交脚本里才有，发不出来。整个索引的区间订阅照常靠索引 key 的 keyspace 通知。
  - default（实现放在下面 `warp` 阶段的 `_commit_seq_define_check` 里，行为一样）：
    - 必须等于 0。0 表示"还没发号"，号从 1 开始。
    - 写成 `0.0`、`False` 也按 0 处理，json 里统一写成 int 0。否则同一个定义会算出不同的 schema 版本。
- **放在 `warp` 里、所有字段都经过 `_normalize_prop` 之后校验**，同 `_groupby_define_check`（`component.py:729`）。一律 `raise ValueError`：
  - dtype 必须是 int64（kind `i`、8 字节）：
    - 提交脚本里的数是 Lua 的 double，2^53 以内精确；int64 的编码固定是"0x80 + 低 7 字节"（§5.2）。
    - 窄整数会在类型上限溢出，提交脚本还得按 dtype 判断上限；无符号的编码又是另一套。第一版只收一种。
  - 不能同时写 `unique=True`：
    - 组内唯一由发号本身保证。
    - 登记成 unique 的话，未分配的行都是 0：本地 unique 检查会让同组的第二次 insert 报 UniqueViolation，提交时每行还要白跑一次 UNIQ。
    - unique 附带的 upsert、negative cache 对它也没有意义：新行的号要提交时才有，没法按号 upsert。
  - 一个组件最多一个 `commit_seq` 字段。
  - `commit_seq` 字段不能出现在任何索引的 groupby 里。
    - insert 时，别的索引的 member 都是用 Python 手里的行（号还是 0）算的，只有 `commit_seq` 自己的 ZADD 交给提交脚本做。
    - 拿它当分组列的复合索引，会把行放进"号为 0"的那一组。之后会出现几种错误：
      - 按真实的号查不到；
      - 按 0 查会一直报 InconsistentRangeRead；
      - update 时 ZREM 删不掉这个 member，留下孤儿。
  - 不能是 `rls_compare` 的列，也不能是 OWNER 组件的 `owner`：按发出来的号判定可见性没有意义。
  - 分组列的规则同复合索引稿 §3.1：kind `iuUS`，可以多列，可以是 hidden。
- `hidden=True` 可以：服务端照常查，客户端不能按它订阅（复合索引稿 §6.1 的 `_hidden_query`）。
- schema json：
  - 只在开启时写 `"commit_seq": true`，做法同 hidden、groupby（`component.py:261-264`）。没用它的组件，schema 版本不变。
  - 旧版 HeTu 的服务器和 `LocalApp` 读到这个键会报 TypeError，或者版本号对不上，从而拒绝启动。
  - 旧版 headless 客户端不会报错，原因见 §8.3。所以新旧版本不能混跑，headless 客户端也要一起升级，文档里写明。
- 新类属性 `commit_seq_: str | None`，由 `load_json` 生成：有这个字段时是字段名，没有时是 None。它的分组列就是 `index_groups_[commit_seq_]`。

### 3.2 写入规则

- **insert**：`commit_seq` 列必须是 0，否则报 ValueError。用 raise，不用 assert（`repo.py:818` 检查 `_version` 用的是 assert，`-O` 下会被去掉）。
- **update**：
  - 改了 `commit_seq` 列：报 ValueError，做法同 `_version`（`repo.py:870-871`）。
  - 改了它的分组列，并且这一行在库里已经有号（`idmap.db_row` 不为 None，且其中的号 > 0）：报 ValueError。
    这一行带着旧组的号，搬到新组会和新组里的号重复。
  - 库里号 ≤ 0 的历史行（§8.1）没有号，可以改分组列。
  - 本事务 insert、还没提交的行也可以改分组列：号在提交时按最终的分组值发。
- **upsert**：走 insert / update，规则同上。
  - 不能以 `commit_seq` 列为锚，因为它不是 unique。
  - 现在拦住这种写法的是 `repo.py:914-916` 的 assert，`-O` 下会被去掉，要改成 raise ValueError。这条对所有非 unique 的锚定字段都生效。
- **delete**：照常。号不回收（§4）。
- **direct_set**：已经拒绝写索引列和分组列（`column_indexes_`，`redis_model.py:1185-1190`），不用改。
- **维护工具**：`TableMaintenance.upsert_row`、迁移脚本照常整行写入，之后的重建索引会校准计数器（§8.2）。
- **payload 兜底**：`build_commit_payload_` 里，update 改了号、或者改了有号（> 0）的行的分组列时直接报错，防的是绕过 repo 的写入路径；号 ≤ 0 的历史行换组照常放行。
  insert 不需要兜底：提交脚本写的号排在 HSET 参数的最后，一定会覆盖行里的值（§6.1）。

### 3.3 事务里看到的

- insert 之后、提交之前，行里的 `commit_seq` 列一直是 0。提交之后 session 已经清空，System 手里的行对象也还是 0。
- **未分配号的行不在这个索引上**：
  - 按这个索引 range、get 都查不到它们，`(-inf, inf)` 也查不到；按别的索引、按 id 能查到，值是 0。
  - 库里号为 0 的历史行，照常可以按 0 查到。"0"的这两种含义要在文档里写清楚。
  - 理由：未分配的行在索引里的位置要到提交时才定。按 0 放进结果的话，它会排在组的最前面；`desc=True` 取"最新 N 条"时，会被当成最老的一行。
  - 实现改两处：
    - `_sync_local_index`（`idmap.py:771`）对 `commit_seq` 索引，不 place 本事务 insert 的行（:792）；
    - `get` 的缓存匹配（`repo.py:392-399`，`idmap.match` / `filter`）在 `commit_seq` 索引上排除 INSERT 状态的行。
- 提交之后读回来的行、订阅推送的行，带的都是真正的号。
- `repo.get` / `repo.range` 的 docstring 现在写着"本事务新 insert 的行能查到"（`repo.py:332`、`:463-466`），要补上这条例外（§12）。

### 3.4 查询与订阅

和复合索引、单列索引完全一样，不加新 API：

```python
rows = await repo.range(seq=(cursor + 1, np.inf), owner=7, limit=100)
```

C# 用 `WatchRange<T>("seq", left, right, limit, group: ...)`。

- 带 groupby 的：按组订阅订组 key 的 keyspace 通知，只被本组的写入叫醒，提交时不发 PUBLISH（复合索引稿 §4.6）。
- 不带 groupby 的：订整个索引 key 的 keyspace 通知。

## 4. 保证

记 G 为一个组。不带 groupby 时，整张表是一个组。

1. **提交成功才发号**。以下情况都不消耗号：
   - Phase 1 的检查失败（RACE / UNIQUE）、自动重试、insert 了又在本事务删掉的行；
   - 发号阶段自身失败：任何一个计数器出错或越界，已经加上去的都会减回去，这次提交一个号都不发（§6.2）。
2. **不重号**：
   - INCRBY 是原子的；
   - 每个号只写进一行；
   - 已经有号的行不能改分组列；
   - 定义上不允许出现重复，重建时发现的历史重复按 §8.2 处理。
3. **按提交顺序**：G 里号小的行先提交。同一个事务里同组的多行，按 insert 的顺序拿到连续的号（§6.1）。
4. **按节点的前缀可见**：在任何一个节点上（master 或某个副本），只要能读到 G 的 k 号，G 里 1..k-1 号的行（被删掉的除外）就一定也已经可见。
   - 原因：
     - 发 k 号的那次提交，晚于发 1..k-1 号的那些提交，而且是一整次原子执行。
     - 副本按 master 的顺序整块应用每次提交：Redis 7 起，脚本一律按效果复制，包在 MULTI/EXEC 里。SQLite 是同一个库文件里的写事务。
   - 但 HeTu 的一次 range 读，不一定落在同一个节点上：
     - 显式配置了 servants 的主从：查索引和取行用的是同一个节点（`_range_read`），读到的就是连续前缀。
     - 原生 cluster，且每个分片有两个以上副本：servant 用 `ROUND_ROBIN_REPLICAS`（`redis/client.py:197`）。redis-py 每一条读命令都单独轮询选副本，pipeline 里的也一样，所以取行会被拆到复制进度不同的副本上。
     - 读写分离代理按命令分流：同上。
   - 只要索引来自一个节点、所有行来自另一个节点，结果仍然是连续前缀（两个前缀的交集还是前缀）。洞只出现在取行被拆开的时候，而且是临时的。
   - 结论：消费方要能处理临时的洞，§9 的写法会在提交时确认。库外的消费方不在事务里，改用一条 ZRANGE 的快照作证据（2026-11-09 补，见 `2026-11-09-commit-seq-third-party-design.md` §3，下称"第三方稿"）。
5. **只增不减**。删行不会让计数器回退，重建索引只会把计数器往上调。号可能被复用的情况有这些：
   - 异步复制下发生故障切换：新主库少了最后几次提交；
   - 主库崩溃后，用 AOF（everysec）或 RDB 重启：同上；
   - 整库从备份恢复：同上；
   - 易失表每次维护都会清空，号从 1 重来；
   - 内存淘汰策略是 `allkeys-*`：计数器可能被淘汰（数据行同样会被淘汰，是既有问题），运维文档要求不用这类策略；
   - 只导入了行，或者手工删了计数器：计数器落后于数据，会发出重号。跑 `hetu upgrade` 校准即可（§8.2）。

   前三种是数据和计数器一起回退，两者仍然一致，`hetu upgrade` 的校准不会改动它们。受影响的是回退之前已经读过那些号的消费方：
   - 服务端消费方在同一个事务里读日志、写游标，两张表在同一个 co-location 簇、同一个 slot，游标会跟着一起回退，不受影响；
   - 易失表要看游标：日志是易失表、游标表是持久的，维护后游标要跟着清；
   - 游标存在库外的第三方消费方，会把复用的号当成已经消费过的。它要核对雪花 id，发现组内最大号小于游标时要报警（§9）。接口和写法见第三方稿（2026-11-09 补）。
   - 第一版不做"整体抬高计数器"的运维工具。
6. **库里仍可能出现缺口**，来源有三种：
   - 删中间的行；
   - Phase 2 写入中途出运行时错误（发号之后才写行）。只在 Redis 上会出现：SQLite 会把整个写事务回滚（§6.3）；
   - 数据损坏。

   缺口会让"只推进连续前缀"的消费方停在缺口前面。§9 的写法可以在提交时确认缺口是永久的，然后安全地跳过它。

## 5. 存储

### 5.1 计数器

- **key**：
  - 不带 groupby：`{cluster_prefix}:seq:{字段名}`；
  - 带 groupby：`{cluster_prefix}:seq:{字段名}:{分组列名，按声明顺序用逗号连接}:{P 的完整 hex}`。P 是复合索引的组前缀。
  - 不用组 key 的 `sortable_token`：超过 32 字节的 P 会被它取成 blake2b-128 摘要。摘要碰撞时，组 key 共用无害，因为 member 带着 P；计数器共用就不行了，两组的号会交错，每组都出现缺口。完整 hex 从构造上就排除了碰撞，代价只在长字符串分组值上：每组一个更长的 key。
  - key 里带分组列名：改了 groupby（比如从 owner 改成 guild，都是 int64）之后，新组的 P 可能和旧组的 P 字节相同。不带列名的话，新组会继承旧组的计数器，开头出现一段缺口。带上列名，groupby 一改就是另一组 key，旧计数器变成残留（§8.1）。
- **类型**：Redis 字符串，值是这个组最近发出的号（十进制）。key 不存在等于 0。用 INCRBY。用字符串、不用每个索引一个 hash，原因有两条：
  - HeTu 自己在 keyspace 通知配置里追加的是 `Kghz`（`redis/client.py:307-332`），不含 `$`，所以 INCRBY 不产生通知；HINCRBY 则会让 master 和每个副本各多一条没人订的通知。
    - 节点原来就配了 `$`（比如 `KEA`）时，INCRBY 也会发通知，但不会比 HINCRBY 多。
  - 用字符串，SQLite 可以直接用现成的 KV 表来模拟（§6.3）。
- **组里的行删光了，计数器也留着**：组 zset 会自动消失，计数器不能跟着删，否则号会从 1 重来。
- **不用"组内最大号 + 1"**：
  - MySQL MyISAM 的组内自增就是这么做的，删掉组内最大的那行之后，号会被复用。
  - 在每组一个 zset 的结构下更糟：组删空了，zset 就没了，号从 1 重来。
  - 对消费方来说，这都是"已经越过的号上来了一条新数据"，会被直接跳过。
- **生命周期**：
  - 带同一个 `{CLU<id>}` hash tag：cluster 下和行、组 key 在同一个 slot。
  - **换簇**（组件名不变，`migration_cluster_id`）时搬走：
    - Redis 的 `do_rename_table_` 搬的是 `{cluster_prefix}:*`（`redis/maint.py:161-179`），计数器就在里面；
    - SQLite 要同样带上（§6.3）。
  - **改名成别的组件名**时不搬，计数器留在原名下：
    - 默认迁移脚本先把原表改名成临时表 `Comp:__temp__`，建好新表、拷完行之后，再删掉临时表（`default_migration.py:138-166`）；
    - 如果计数器跟着改名搬走，就会和临时表一起被删掉（两路审核都在 Redis 上实测过：计数器全部变成 None）；
    - 留在原名下，新表的 cluster_prefix 和原表相同，计数器原地接着用；
    - 两个后端规则相同：`do_rename_table_` 只在 from、to 的组件名相同时才搬计数器。
  - `do_drop_table_` 删的是 `all_clusters_prefix*`（`redis/maint.py:218`），易失表 flush 时计数器一起删掉。
  - 重建索引的 `_table_keys`（`redis/maint.py:243-271`）只认 `:id:` / `:index:` / `:rebuilding:`，不会误删计数器。
  - `duplicate` 出来的副本表有自己的 cluster_prefix，各自计数。
- **内存**：每组一个 key，审核实测 `MEMORY USAGE` 是 67 字节，加上字典开销约 98 字节。组数等于出现过的分组值个数，组删空了也不减少。按对局、按会话这类用完即弃的值分组时，计数器会一直增长，文档要提醒。清理工具第一版不做。

### 5.2 member 与组 key

不变（复合索引稿 §4.1、PR #214）：

- member = P ‖ enc(seq) ‖ `00` ‖ id 段（16 位小写 hex）；
- 组 key = `{idx_key}:{sortable_token(P)}`，不带 groupby 时就是 idx_key；
- `INDEX_FORMAT` 不变。

enc(seq) 是 int64 的 `to_sortable_bytes`，即 seq + 2^63 的 8 字节大端。1 ≤ seq < 2^53 时，它就是 `0x80` 后面跟 seq 的低 7 字节大端。

提交脚本只负责算 enc(seq) 并拼接。P、组 key、id 段、计数器 key 都由 Python 算好传进去。

## 6. 提交

### 6.1 payload（`build_commit_payload_`）

- **第 6 段 `seq_allocs`（新增）**：`[[计数器 key, 本次要发几个], …]`，按第一次出现的顺序排。没有 `commit_seq` 组件时，这一段为空。
- **`commit_seq` 组件的 insert：HSET 换成 HSETSEQ**：

  ```
  ["HSETSEQ", 下标, 组 key, P, id 段, 行 key, "_version", v, f1, v1, …, 字段名, 占位]
  ```

  - 下标：这一行用 `seq_allocs` 的第几项，从 1 开始，对应 Lua 数组的下标。
  - 行 key 之后就是原来 HSET 的参数，`commit_seq` 字段从中间去掉，改成放在最后一对，值是占位。提交脚本把占位换成发出的号。
    - 放在最后，是因为 HSET 遇到同名字段时取最后一个值。即使中间漏删了一个 0，最后写进去的也是发出的号。
  - 这一行在 `commit_seq` 索引上的 ZADD 不再由 Python 单独发，由 HSETSEQ 一起做。
  - 其他索引的 ZADD、UNIQ、表频道、值频道照常。
- **顺序**：
  - insert 按缓存顺序遍历（`idmap.py:950`），也就是 insert 调用的顺序。同组同一个事务的多行，按这个顺序拿到连续的号。
  - 本事务 insert 之后又删掉的行，在 `mark_deleted` 时就从缓存里去掉了，不会出现在 payload 里，不占号。
- **update、delete 不变**：
  - update 改不了这一列，也改不了有号行的分组列（§3.2），`touched` 不会包含 `commit_seq` 索引；万一包含了，就按 §3.2 的兜底报错。
  - delete 按库里的号 ZREM。

### 6.2 提交脚本（`commit_v2.lua`）

Phase 1 不变。Phase 2 开头先把这次要发的号全部拿到，要么全成，要么全不发：

```lua
local seq_next = nil
if seq_allocs then  -- 没有号要发时 payload 不带这一段，下面的表和循环都不建
    seq_next = {}
    for i, alloc in ipairs(seq_allocs) do
        local last = redis.pcall("INCRBY", alloc[1], alloc[2])
        local failed = type(last) == "table"  -- pcall 出错时返回 {err = ...}
        -- 发出的号要在 [1, 2^53) 里：再往上 Lua 的 double 不精确；计数器被手工改成负数时号 <= 0
        if failed or last >= 9007199254740992 or last < alloc[2] then
            -- 把已经加上去的减回去（这一个加成功了也减），这次提交一个号都不发
            for j = 1, failed and i - 1 or i do
                redis_call("DECRBY", seq_allocs[j][1], seq_allocs[j][2])
            end
            if failed then
                return last  -- 原样返回 Redis 的错误（WRONGTYPE、OOM、READONLY……）
            end
            return "SEQ: Counter out of range " .. alloc[1]
        end
        seq_next[i] = last - alloc[2] + 1
    end
end
```

push 循环认 HSETSEQ：

```lua
for _, cmd in ipairs(pushes) do
    if seq_next and cmd[1] == "HSETSEQ" then
        local n = seq_next[cmd[2]]
        seq_next[cmd[2]] = n + 1
        cmd[#cmd] = string.format("%.0f", n)
        redis_call("HSET", unpack(cmd, 6))
        -- 号的 8 字节编码在这里内联拼（同 to_sortable_bytes），然后
        redis_call("ZADD", cmd[3], 0, cmd[4] .. 编码 .. "\0" .. cmd[5])
    else
        redis_call(unpack(cmd))
    end
end
```

- **不用提交序号列的提交不多付开销**（实现审核测出来的）：所有提交都走这个脚本。初版每次执行都建发号用的闭包和表、
  payload 总带空的第 6 段，单行 update 在 master 上慢了约 15%。现在只有要发号时才带第 6 段、才建表。
- **号的编码**：用纯 Lua 算术拼 8 字节，不依赖 `struct` 库。n < 2^53 时，`%` 和除法都是精确的。
  - 审核时在 Redis 8.10 上实测过：在 1、255、256、2^31、2^32、2^48、1e14、2^53-1 这些值上，和 `to_sortable_bytes(np.int64(n))` 逐字节一致。
- **号转成字符串用 `string.format("%.0f", n)`**：
  - 不用 `%d`：Lua 5.1 的 `%d` 先转成 C 的 long，在 long 为 32 位的平台上（比如 Windows 原生的 Redis 兼容实现），2^31 以上的值会截断。
  - 不用 `tostring` 或 `..`：Lua 5.1 的数转字符串用 `%.14g`，≥ 1e14 的值会变成科学计数法。
  - 审核实测：在 Redis 8.10 上，`%.0f` 和 `%d` 对 1 到 2^53-1 的各个测试值，输出逐字节相同。
- **发号阶段全成或全不发**：
  - INCRBY 用 `redis.pcall`。任何一个计数器出错或越界，就把前面已经加上去的减回去。
  - 出错时原样返回 Redis 的错误（redis-py 照旧抛 WRONGTYPE、OOM、READONLY 这些异常，运维不会被误导成计数器坏了）；
    只有发出的号不在 [1, 2^53) 时返回 `SEQ`。
  - 只有失败路径多花开销，成功路径和原来一样。
  - 原来没有的计数器 key，DECRBY 回 0 之后会留着，值是 0，和 key 不存在等价。
- **先发号、再写入**：
  - Lua 运行时出错不会回滚已经执行的写入。发号排在所有写入之前，Phase 2 写到一半出错时，坏情况是留下缺口，而不是发出重号。
  - 缺口会让消费方停在缺口前面，但 §9 能确认之后跳过；重号会让消费方静默漏掉一行，所以宁可留缺口。
- **开销**：每个计数器 key 每次提交一条 INCRBY；HSET、ZADD 的条数和普通 insert 一样。
- **Phase 3 不变**，不发 PUBLISH。
- **`raise_for_commit_response_`**（`redis_model.py:1136-1146`）加一个 `SEQ` 分支，抛 RuntimeError。这是确定性错误，不重试；报错信息里带上计数器 key 和出错原因。

### 6.3 SQLite（`sqlite/commit.py`、`sqlite/store.py`、`sqlite/maint.py`）

`commit.py` 逐段对应 Lua：

- `run_commit` 读第 6 段，缺省为空。
- Phase 2 开头：先读出各计数器的当前值、检查越界，全部通过之后再写入。失败时什么都不留，和 Lua 的减回效果一致
  （Redis 会留下值为 0 的 key，语义等价）。
  - 计数器按 Redis string2ll 的规则认整数（可选负号加不以 0 开头的数字，或者就是 "0"，在 int64 范围里），
    Python 的 `int()` 会放过 " 5"、"+5"、"05"、"1_0"。
  - 不是整数、加了会溢出 int64 时，Redis 是 INCRBY 报错（原样抛出），这里返回同样内容的 SEQ 失败串。
- `_run_pushes` 认 HSETSEQ：
  - 用发出的号替换占位，hset 这一行；同名字段取最后一个值，和 Redis 一致；
  - zadd 组 key。
- 通知按 Redis 的规则：
  - 行 key 发 keyspace 通知；
  - 组 key 真加进了 member 才发；
  - 计数器不发：HeTu 默认的 `Kghz` 不含 `$`。
- 计数器存在 KV 表 `__hetu_kv` 里（`store.py:227`）：`expire_at` 为 NULL，值是十进制的字节串。
- Phase 2 出错时两边行为不同：
  - SQLite 的 `write_txn` 会整个回滚（`store.py:262-284`）；
  - Redis 会留下已经执行的写入。
  - 所以测试不能在 SQLite 上断言出现缺口。

`store.py` 新增原语，都在调用方的写事务里执行：

- `kv_get_int(key) -> int | None`：不存在为 0，不是 Redis 认的整数为 None
- `kv_set(key, value)`
- `kv_delete_prefix(prefix) -> int`
- `kv_rename_prefix(old, new)`
- `kv_items_with_prefix(prefix)`

按前缀匹配沿用 `zdelete_prefix` 的做法：用 substr 比较，不用 LIKE，因为组件名里的 `_` 是 LIKE 的通配符。

`sqlite/maint.py`：

- `do_rename_table_`（:188）：组件名相同时（换簇）搬计数器，不同时不搬，规则同 Redis（§5.1）；
- `do_drop_table_`（:230）：删掉这张表的计数器。

## 7. 事务内（`idmap.py`、`repo.py`）

- **禁写**（§3.2）：
  - `repo.insert`：在 `idmap.add_insert` 之前检查。
  - `repo.update`：在算出 `changed_fields` 之后检查，和 `_version` 的检查放在一起。
  - 分组列和 `db_row` 比，不和缓存比：本事务 insert 的行没有 `db_row`；库里号 ≤ 0 的行也放行。
- **未分配的行不进索引结果**（§3.3）：改两处。
- **其余不用改**：
  - `_range_checks` 只核对有数据库态的行，它们带着真正的号；
  - `_unique_point_hit`、absent 键只对 unique 索引生效，`commit_seq` 不是 unique；
  - `LocalIndex` 的 moved（本事务删掉的库里行）按库里的号算排序键，照常工作。

## 8. 维护

### 8.1 迁移

- 打开或关闭 `commit_seq` 都会改变 schema 版本，要跑 `hetu upgrade`。
- **`_finish_schema_migration` 的比较元组要加上 `commit_seq_`**（`base.py:1504-1508`）。
  - 给已经有索引、groupby 也一样的字段打开 `commit_seq` 时，`indexes_` / `uniques_` / `index_groups_` 都不变，不会重建索引，计数器也就没人校准，会从 0 开始发，发出和已有数据重复的号。
  - 默认的 `hetu upgrade` 之后还会跑一遍 `rebuild_index_all`（`cli/migrate.py:197-203`），所以这个问题只出现在用了 `--no-rebuild-index`，或者在程序里直接调用 `create_or_migrate_all` 的时候。
  - 这条比较仍然要加：`_finish_schema_migration` 本身不看 `--no-rebuild-index`。
- **改 dtype 的迁移**（加列、删列、改类型都会走默认迁移脚本）：按 §5.1 的规则，改名到临时表时不搬计数器，计数器留在原名下，新表接着用。
  - 组删空了、组内最大号被删了的组，迁移之后都接着原来的号发。§11 有用例覆盖。
- **自定义迁移脚本**：
  - 照模板写：把原表改名到临时表，拷完之后删临时表。不要直接删原表名下的数据。
  - 改了组件名或字段名的话，旧计数器会变成残留，新计数器按数据校准，被删掉的尾部号可能会被复用。文档写明。
- **把已有的序号字段改成 `commit_seq`**（游戏项目的迁移路径）：
  - 已有的值保留，重建时计数器校准到每组最大的正数号，之后从最大号加一接着发。
  - 已有数据里的缺口不处理；组内重复按 §8.2 处理。
  - 已有的值 ≥ 2^53 时（比如旧值是雪花 id 或纳秒时间戳），`hetu upgrade` 报错，要求先写迁移脚本重新编号。
    不拦的话，校准会把计数器设到上限以上，之后这一组的每次提交都会失败。
- **新加一个 `commit_seq` 列**：
  - 默认迁移把历史行填成 0。≤ 0 的号都表示"没有号"：重建校准和重复检查都跳过它们，消费方从 1 开始读，也会跳过。
  - 要给历史行编号，就写迁移脚本（比如每组按 id 顺序分配），之后的重建会校准计数器。
  - `default_migration.py` 对新加的 `commit_seq` 列打一条警告，说明这一点。
- **改 `commit_seq` 字段的 groupby**：
  - 计数器 key 里带着分组列名（§5.1），新的分组对应新的一组 key，按数据校准，旧 key 变成残留。
  - 原有的号是按旧分组编的，到了新分组里多半会重复，按 §8.2 处理。实际上应该写迁移脚本重新编号。
- **关掉 `commit_seq`**：计数器 key 留着不删。
  - 之后用同样的 groupby 再打开，key 相同，接着用；
  - 删掉的话，没有行的组会从 1 重来。
  - 关掉之后、或者字段删掉之后剩下的计数器，记为已知残留（§12）。

### 8.2 重建索引时校准计数器

- 两个后端的 `do_rebuild_index_`（`redis/maint.py:274`、`sqlite/maint.py:251`）本来就按行重算 member。建 `commit_seq` 索引的时候，顺手统计每组的最大号和组内重复，只看 > 0 的号。
- **在建的阶段检查**，任何一条不通过，所有旧索引和计数器都原样保留：
  - 最大号 ≥ 2^53：报错（§8.1）；
  - 组内重复：报错并列出重复的组（§14.2）。
  - "原样保留"只在不经过迁移脚本的路径上成立（只改了索引定义、`commit_seq`，或者单纯重建）。加删改了列时，
    迁移脚本已经把数据搬到新定义下、写了新版本的 meta，旧索引也跟着临时表删掉了，之后才重建。这时重建失败，
    `MigrationScript.upgrade` 把 meta 的版本号写成 `REBUILD_PENDING_VERSION`，服务器拒绝启动；修好数据后再跑
    `hetu upgrade`，`_finish_schema_migration` 认出这个版本号，按当前定义直接重建（实现审核发现，§18）。
  - 卡在待重建之后又改了定义的，`MigrationScript` 按 meta 里那个定义算出的版本号找迁移脚本：脚本按
    `*_v{旧版本}_to_v*.py` 查找、不看组件名，哨兵是所有组件共用的，拿它找会捡到别的组件的脚本。
- **先校准、再换上索引**：把计数器设成 max(现有值, 最大号)，只升不降，然后才 RENAME。
  - 这个顺序是为了防进程中途被杀：换上之后、校准之前被杀的话，服务器照样能启动，计数器却低于库里的数据，会发出重号。
  - 只往上调计数器，永远不会产生重号。
  - Redis 用一段 Lua 分批执行"比较后 SET"；SQLite 在替换索引的同一个写事务里做。
- 没有行的组，计数器不动。行数为 0 的表也不碰计数器：易失表的清空走的是 drop，计数器跟着一起删。
- **什么时候需要校准**：
  - 只导入了行、或者手工删了计数器之后，跑一次 `hetu upgrade`（默认全量重建）；
  - 整库从备份恢复时，数据和计数器是一致的，校准什么都不会做，也不需要做。

### 8.3 headless、CLI、其他

- **headless**：
  - `headless.py` 的 `_schema_diff`（:162）比对项加上 `commit_seq`。
  - 同时改成：遇到不认识的列属性键，直接判为不一致，这样以后再加新属性，就能拦住旧客户端。
  - 旧版 headless 拦不住：它传本地类时，只用原始 JSON 比较固定的几项（`headless.py:138-166`），不会对服务器 meta 调用 `Property(**prop)`。比对通过之后，它会照旧写入号为 0 的行、直接 ZADD，绕过计数器，消费方永远看不到这些行。所以文档要点明：headless 客户端必须一起升级。
- **CLI**：`hetu get --list`（`cli/data.py:356`）多输出一列 `commit_seq`。
- **不用改**：训练样本、`test_arch_publish.py`、`test_arch_master_reads.py`。

## 9. 消费方的写法（写进文档）

服务端消费：

```python
# LogCursor 是消费方自己的游标表，owner 是 unique；一个游标对应一个逻辑上的消费方
async with ctx.repo[LogCursor].upsert(owner=owner) as cursor:
    log = ctx.repo[ChangeLog]
    rows = await log.range(
        seq=(cursor.last + 1, np.inf), owner=owner, limit=100, phantom_check=False
    )
    expected = cursor.last + 1
    for row in rows:
        if expected < row.seq:  # 中间缺了号：一次闭区间读确认整段
            for missing in await log.range(
                seq=(expected, row.seq - 1), owner=owner, limit=-1
            ):
                handle(missing)  # range 漏读了它：取行落到了进度不同的副本
        handle(row)
        expected = row.seq + 1
    cursor.last = expected - 1
```

（初稿是逐号 `get` 确认，代价随缺口长度线性增长：头部裁掉 5000 行时，一次消费在 Redis 上要 3.9 秒、提交脚本
占用 master 9.6ms。实现审核改成每段缺口一次闭区间读，见 §18。）

- **为什么用 `phantom_check=False`**：
  - 没读满 limit 时，观察区间开到 +inf。从读到提交之间，同组每来一条新日志，消费方的提交都会 RaceCondition 重试。
  - 消费方的逻辑不依赖"区间里没有别的行"，这正是 `range` 文档说可以关掉的场景。
  - 读到的行仍然有 VER 校验。一个游标对应一个逻辑上的消费方，同一个消费方的多个实例可以共用它：游标行的 VER
    防止它们同时推进、重复处理。
- **为什么跳过缺口是安全的**（实测过提交里的检查）：
  - 确认缺口的那次 range 开着区间校验（默认），提交时变成一条 CNT，在 master 上确认这一段确实没有行；
  - 区间的上界比一个已经提交的号小，新写入的行进不来，不会因为别的写入白白判竞态；
  - 后面那一行是读进来的库里行，提交时它的 VER 确认它在 master 上已经提交；
  - 号按提交顺序发：后面的号已经提交、前面这一段在 master 上没有行，以后也不会再有（§4.5 的回退除外）；
  - 副本落后导致读空、而 master 上其实有行时，CNT 对不上，判竞态重试；取行时漏读，报 InconsistentRangeRead；
  - 不新增 master 读：检查都在提交脚本里。每段缺口提交时多一条 CNT，和缺口长度无关。
- **注意**：
  - `phantom_check=False` 时，range 取行读不到的行会被静默跳过：`repo.py:604-605` 只在登记了观察时才判竞态。所以缺号时一定要按上面的写法确认，不能直接越过。
  - `handle` 里如果有外部副作用（发奖励、调别的服务），重试时会重复执行，要自己做幂等。
  - 每读一行，提交时 master 多一条 VER（§10）。
- **客户端消费**：
  - 用 `WatchRange("seq", cursor + 1, inf, limit, group: …)`。窗口满了 limit 之后，用新的游标重新订阅，并退订旧的。
  - 游标只沿连续的号推进：同一批推送按号排好，一个号接一个号地处理，遇到缺号就停、后面的先缓存。订阅重读时取行也可能
    落到进度不同的副本、漏掉中间一行（`sub.py` 的 `_rerange` 读不到的行先丢掉，等下一批推送补上），直接把游标推到这一批
    最大的号会永远漏掉它。
  - 这个组的每一行都必须对这个客户端可见，比如 OWNER 组件加 `groupby="owner"`。RLS 不可见的行会占着 limit 名额、不推送，客户端会永远停在它前面。做不到全部可见的，改用服务端消费。
  - 客户端没法在事务里确认缺口。只能保证服务端不删中间的行，或者缺号一直不补时，调一个服务端 System 来确认并跳过缺口。
  - 想拿到刚插入那一行的号：System 返回的行里号是 0，可以订阅这一组，再用 `CallSystemAwaitPush`（`rpcs`）调用，按返回的行 id
    找推送来的那一行。前提是新行落在订阅的窗口里（比如 `desc` 订最新几行），窗口满了就不会推送。
- **裁剪**：只删所有消费方都已经越过的头部。删中间的行会留下洞；服务端消费方能按上面的写法确认后跳过，客户端消费方会停在洞前面。
- **易失表**：每次维护后，号从 1 重来，消费方的游标要跟着清。
- **第三方消费方**（游标存在库外，比如把日志同步到外部系统）：
  - 2026-11-09 补：下面这张核对表作废，接口和写法改见第三方稿 §6。
    原来的表依赖读不到的计数器（"组内最大号"），而且把临时漏读当成删除。第三方消费方的裁剪只能删到快照里存的游标为止（第三方稿 §6 的注意）。
  - ~~号可能被复用（§4.5）。所以除了号，还要记下那一行的雪花 id，读的时候把游标所在那一号一起读回来核对：~~
    - ~~id 对不上：号被复用了，往回找到 id 还对得上的位置，从那里继续；~~
    - ~~读不到，组内最大号 ≥ 游标：那一行被删了（比如裁剪），照常往后读；~~
    - ~~读不到，组内最大号 < 游标：数据回退了，报警。~~
  - 服务端消费方不需要这样做，原因见 §4.5。

## 10. 性能

- **写入方**：
  - 每个计数器 key 每次提交多一条 INCRBY。审核实测：多一条 INCRBY 加上拼号，每次提交从约 2.6µs 涨到约 3.7µs；
  - HSET、ZADD 的条数和普通 insert 一样：号拼进了原来那条 HSET；
  - Python 侧少算一个 member：`commit_seq` 索引的 member 由提交脚本拼。
- **服务端消费方**：
  - 读进事务的每一行，提交时都有一条 VER，Lua 里各做一次 HGET。审核实测：消费 100 行，提交里有 100 条 VER；
  - `phantom_check=False` 只去掉 CNT，不去掉 VER；
  - 按"每多一条检查约 +1µs"估算，每消费一行，master 多约 1µs，约为写入方单行提交成本的两成多；
  - 日志行写入后不再改，这些 VER 起不到保护作用；
  - 能用订阅在客户端消费的，优先客户端消费，不占 master；
  - 是否另做"不登记版本的读"见 §14.4。
- **不新增的东西**：
  - keyspace 通知（计数器是字符串，条件见 §5.1）；
  - PUBLISH；
  - 事务路径上的 master 读。
- **对比"事务里读改写计数器行"**：后者同组的并发写入全部在那一行上 VER 冲突、重试。
- **内存**：每个计数器约 98 字节（§5.1）。

## 11. 测试

以两个后端（redis、sqlite）共跑的用例为主，valkey、redis_cluster 也要跑。

**定义**：

- 以下都报 ValueError：
  - dtype 不是 int64；
  - default 不等于 0；
  - 同时写了 unique；
  - 有两个 `commit_seq` 字段；
  - `commit_seq` 字段被别的索引当成分组列；
  - `commit_seq` 字段是 `rls_compare` 的列，或者是 OWNER 组件的 owner。
- `index=False` 警告。
- `point_sub` 警告并忽略，包括 `hidden=True, point_sub=True` 的组合。
- default 写成 0.0 / False 时，json 里是 0，schema 版本和写 0 时相同。
- schema json 只在开启时带这个键；没用它的组件，版本号不变。

**发号**：

- 单行、同组多行（按 insert 顺序连续）、多组、不带 groupby；
- 提交检查失败（VER、NX、UNIQ、CNT）不消耗号；重试后的号仍然连续；本事务 insert 又删掉的行不占号；
- 全成或全不发：同一次提交里，第二个计数器 WRONGTYPE 或越界时，第一个计数器被减回原值，行和 member 都没有写入（两个后端）；
- 越界：直接 SET 计数器到 2^53 附近来测，返回 SEQ 错误。

**并发**（核心用例）：

- 多个协程 / 多个 Session 同时往同一组插入，并让其中一部分事务因为别的冲突重试或者失败；
- 断言：最终每组的号恰好是 1..N，不重不漏；
- 断言：每个串行的生产者自己拿到的号是递增的。从 Python 侧观察不到"号的顺序等于提交成功的顺序"，所以只断言到这一步。

**前缀可见**：订阅推送和 range 读到的号总是连续前缀（显式配置 servants 的主从）。

**禁写**：

- 以下被拒：insert 时给了非 0 的号；update 改号；库里已有号的行改分组列；payload 兜底（绕过 repo 的 update 改到这个索引）；
- 本事务 insert 的行、库里号 ≤ 0 的行可以改分组列，号按最终的组发；
- upsert：用别的 unique 字段锚定、新建的行能发号；命中已有行之后改分组列报错；在块里给号赋值报错；以 `commit_seq` 列为锚报 ValueError，`-O` 下也一样。

**事务内**：

- 本事务 insert 的行不出现在这个索引的 range / get 结果里；按 id、按别的索引能读到，值是 0；
- 开着 phantom_check 时，insert 之后的 `range(desc=True, limit=N)` 不包含这一行，提交照样成功；
- 删掉的库里行照常从结果里拿掉；
- 读己之写的性质测试如果纳入 `commit_seq`，预期结果要单独定义。

**编码与提交脚本**：

- 提交脚本拼出的 member，和 `index_member_` / `to_sortable_bytes` 算出的逐字节一致；行里存的串能正确读回。边界值覆盖 1、255、256、2^31、2^32、2^48、1e14、2^53-1；
- HSETSEQ 最后一对的号，能覆盖 kvs 中间残留的 0；
- 长字符串分组值（P 超过 32 字节）的计数器 key 用完整 hex，并且带分组列名；
- `test_backend_client.py` 的检查码用例补上 HSETSEQ 和 `seq_allocs`，两个后端共跑；
- SQLite 不给计数器发通知，只给行和组 key 发。

**维护**：

- 加列迁移（走默认迁移脚本）之后，组删空了的、组内最大号被删了的组，都接着原来的号发（两个后端）；
- 换簇迁移带上计数器，改名到临时表不带（两个后端）；
- 给已有序号字段打开 `commit_seq` 后，计数器等于每组最大号，之后接着发；包括 `--no-rebuild-index` 的路径；
- 已有的号 ≥ 2^53 时 upgrade 报错，并且旧索引、计数器都没动；
- 重复检查与校准跳过 ≤ 0 的号；组内重复时 upgrade 报错，并且旧索引、计数器都没动；
- 校准只升不降，并且在换上索引之前完成；
- 改 groupby 之后用的是新的计数器 key；
- 易失表 flush 之后，号从 1 开始；
- SQLite 的 drop / rename 带上计数器。

**消费方写法**（把 §9 的写法本身做成用例）：

- 多个生产者并发写，两个消费方共用一个游标；
- 模拟取行落到不同副本：取行时丢掉中间一行；
- 删掉中间的一行，制造永久缺口，消费方确认之后跳过；
- 以上都断言：没有行被越过，也没有行被处理两次。

**headless**：`_schema_diff` 能发现 `commit_seq` 不一致，也能拦住不认识的列属性键。

**CLI**：`--list` 输出 `commit_seq`。

**架构守门**：`test_arch_publish.py`、`test_arch_master_reads.py` 的规则不改，照样要通过。

## 12. 文档

zh 和 en 在同一个提交里手工同步。不要跑 `translate_new_content.py`。

- **`docs/{zh,en}/concepts.md`**，索引一节加 `commit_seq`：
  - §4 的保证和例外，包括"按节点的前缀可见"；
  - 事务内是 0，未分配的行在这个索引上查不到；库里的 0 表示历史行没有号；
  - 这一列不能写，有号的行不能改分组列；
  - 新旧版本不能混跑，headless 客户端也要一起升级。
- **`docs/{zh,en}/advanced.md`**：
  - binlog 式同步的推荐写法（§9）：确认缺口、`phantom_check=False`、VER 的代价、一个游标只给一个消费方、外部副作用要幂等、第三方消费方核对雪花 id 并报警；
  - headless 一节的比对项（zh:631）加上 `commit_seq`。
- **`docs/{zh,en}/unity-client.md`**：
  - 客户端消费：订阅、换游标重订并退订旧的、按号排序、RLS 可见性的要求、不能确认缺口；
  - 用订阅加 `CallSystemAwaitPush` 拿到刚插入那一行的号。
- **`docs/{zh,en}/operations.md`**：
  - 迁移：打开 `commit_seq`、新加列、改 groupby、关掉之后计数器的残留；
  - 自定义迁移脚本照模板改名到临时表，不要删原表；改组件名、字段名会让计数器变成残留；
  - 只导入了行、删了计数器之后，跑 `hetu upgrade` 校准；
  - 易失表的号会从 1 重来；
  - 不要用 `allkeys-*` 淘汰策略；
  - 用完即弃的分组值会让计数器一直增长。
- **docstring**（`docs/api/` 由 `uv run scripts/gen_api_docs.py` 生成）：
  - `property_field`；
  - `define_component` 的 Notes（`component.py:570-575`）；
  - `repo.get` / `repo.range`：`commit_seq` 索引上看不到本事务 insert 的行（`repo.py:332`、`:463-466`）；
  - `LocalApp.get` / `range`（`local.py:381-425`）。
- **其他**：`hetu/llms.txt`、`hetu/skills/building-on-hetu/SKILL.md`。

已知残留：关掉 `commit_seq`、删掉字段、改了 groupby、改了组件名或字段名之后，旧的计数器 key 不会被清理（§8.1）。

## 13. 定下的做法

1. 字段级 `commit_seq=True`，groupby 可选。
   - 只收 int64，default 必须是 0，号从 1 开始；
   - 不能同时 unique，每个组件最多一个；
   - 不能被别的索引当分组列，不能用作 RLS 判定列。
2. 计数器是每组一个 Redis 字符串：
   - key 用 `:seq:` 开头，带上分组列名和 P 的完整 hex；
   - 不用"组内最大号 + 1"；
   - 换簇时搬走，改名成别的组件名时不搬。
3. payload 加第 6 段 `seq_allocs`，另加一种 push 类型 HSETSEQ：号放在 HSET 的最后一对。Phase 2 开头用 pcall 把所有 INCRBY 做完，失败就全部减回，然后才写入。
4. 未分配号的行不出现在这个索引上，而不是排到末尾。
5. 库里已有号（> 0）的行不能改分组列。换组时不重新发号。
6. 迁移：
   - 比较元组加上 `commit_seq_`；
   - 重建时先校准、再换上索引，只升不降；
   - ≤ 0 的号视为没有号；
   - 最大号 ≥ 2^53、组内重复号时报错。
7. 第一版不返回号。
8. 消费方的推荐写法：`phantom_check=False`，遇到缺号时在同一个事务里用一次闭区间 `range` 确认这一段后再跳过（实现审核时从逐号 `get` 改来，§18）。

## 14. 用户的决定（2026-11-09）

1. **System 里不拿刚发的号**，第一版不做。
   - 以后要做的话：提交脚本把分配结果带回来，`ctx.session_commit()` 返回 `{(组件名, 行 id): 号}`。SystemCaller 自动提交时拿不到，只有提前提交之后才能用。
   - 客户端想拿到号，可以用订阅加 `rpcs`（§9）。
2. **重建时发现组内重复号：报错**（原定只警告，审核发现新的后果后，用户改定）。
   - 后果（审核实测）：同一组依次是 1、2、3、3、4 号，按 §9 的写法消费，第二个 3 号的行永远不会被处理，也不报错。
     原因是游标按号推进，越过 3 之后就不会再回头读 3。
   - 做法：报错并列出重复的组，要求先写迁移脚本重新编号。所有旧索引、计数器原样保留（§8.2）。
   - 重复检查跳过 ≤ 0 的号，所以"新加 `commit_seq` 列"这条迁移路径不会被挡住。
3. **游戏项目的消费方目前在服务端**。文档里另外写明：第三方消费方仍可能遇到号被复用，要同时核对雪花 id（§9）。
   - 2026-11-09 更正：这个前提估错了。命令表的消费方在库外：用 headless 的 `servant_range` 读，不在事务里，游标存在快照文件里，
     是第三方消费方。读计数器、确认缺口、核对回退的接口和写法见第三方稿。
4. **"不登记版本的读"以后单独做**（用户定），第一版只把代价写进文档。下面是当时的分析：
   - 背景：服务端消费方每读一行，master 多一条 VER，约 1µs（§10）。
   - 第一版只把代价写进文档，并推荐能在客户端消费的就用订阅。
   - 要做的话，是通用功能，get 和 range 一起做，与 `commit_seq` 无关，建议单独一个 PR：
     - 读不变的行（配置、日志）时，get 也是每读一行一条 VER，只给 range 做，API 就不一致。
     - 本设计的消费方只需要 range 这一半：§9 里的 `get` 是用来确认缺口的，它读空时登记的 CNT 正是跳过缺口的依据，
       必须照常登记；游标行的 upsert 也必须有 VER。
     - 不登记版本的 get 命中时没有 VER；读空时不登记 negative observation，也没有 CNT。所以"get 为 None 就 insert"
       撞上并发插入时，会从 RaceCondition（重试）变成 UniqueViolation，不能用在这类写法上。
     - 不登记版本的 range 同时没有 CNT，也不做读取一致性核对。
     - 读到的行可以放进缓存，但要标记成不产生纯读 VER。之后 update / delete 它，照常按读到的版本做 VER；之后被普通读
       再读到，就改成登记版本。
     - 参数名要避开 kwargs 语法糖：用下划线开头的参数（同 `_group`），或者单独一个访问入口，不新增和字段同名的保留名。
     - 现有的 `Table.servant_get` / `servant_range` 是完全不进事务的读（看不到本事务的写入、只走 servant），System 里拿不到；
       这里说的是事务内、照常读己之写、只是不登记版本的读，两者不同。

## 15. 实施顺序（每一步都带测试）

1. 定义：`Property.commit_seq`、定义校验（`_normalize_prop` 与 `warp` 两处）、schema json、`commit_seq_`、headless 比对（含未知键）、`--list`。
2. 提交：
   - payload：`seq_allocs`、HSETSEQ、update 兜底；
   - `commit_v2.lua`：pcall、减回、`%.0f`、`seq_bytes`；
   - `sqlite/commit.py` 与 store 原语；
   - `raise_for_commit_response_`。
3. 事务：禁写、分组列锁定（放行 ≤ 0）、upsert 锚定检查改成 raise、未分配的行不进索引结果。
4. 维护：
   - 迁移比较；
   - 两个后端改名时的计数器规则；
   - 两个后端的重建：检查放在建的阶段，先校准、后换上；
   - SQLite 的 drop / rename；
   - 默认迁移的警告。
5. 并发、前缀可见、消费方写法的用例；valkey、redis_cluster。
6. 文档、docstring、`gen_api_docs`。

## 16. 审核记录

2026-11-09 做了两路独立审核：一路看存储与提交（§4–§6、§8、§10），一路看定义、事务语义、消费方写法（§3、§7、§9、§11–§14）。审核员只拿到本文与代码，都写了临时用例实测，没有改仓库。

**核实无误的部分**：

- 本文引用的行号、函数行为基本都对。只有一处偏差：换簇改名搬 key 的循环在 `redis/maint.py:161-179`，已更正。
- 编码与数值：
  - 纯 Lua 拼出的 enc(seq) 与 `to_sortable_bytes` 逐字节一致；
  - `tostring` 在 ≥ 1e14 时变成科学计数法；
  - 2^53 的越界判断在边界上正确。
- 性能：多一条 INCRBY 加拼号，每次提交约多 1.1µs；计数器每个 key 约 98 字节。
- 计数器的生命周期：
  - 换簇时跟着走，drop 时删掉；
  - `_table_keys` 不会误删；
  - 副本组件各自计数；
  - `Kghz` 不含 `$`。
- 不加 `commit_seq_` 时，`_finish_schema_migration` 不会重建索引。
- payload 加第 6 段不影响现有调用方：现有测试按下标取各段；Lua 读到 nil，SQLite 会补齐。
- 读的路径：
  - `_range_read` 查索引和取行用的是同一个 client；
  - `phantom_check=False` 时，读不到的行会被静默跳过（实测）；
  - 现在本事务 insert 的行按 0 排在组的最前面（实测）。
- §7 的"其余不用改"成立；游标行的 VER 能防止两个消费方同时推进。
- 没有新增 PUBLISH，事务路径上没有新增 master 读。

**按审核意见修改的部分**：

1. 【高，两路都报】改 dtype 的迁移会把计数器删掉：改名到临时表时计数器跟着搬走，又和临时表一起被删，之后只能按剩下的行校准。
   - 改为：改名时只在组件名不变（换簇）时才搬计数器（§5.1、§8.1）。
   - 另一路建议在 `MigrationScript.upgrade` 前后快照、写回计数器。没有采纳：那样计数器在迁移中途有一段时间不存在，进程被杀就丢了；而改名时不搬，从头到尾都不碰计数器。
2. 【高】`commit_seq` 字段被别的复合索引当分组列，会写坏那个索引：定义时报错（§3.1）。
3. 【中高】发号阶段不是全成或全不发：
   - 后面的计数器出错或越界时，前面的 INCRBY 不会回滚；
   - 校准还可能把计数器设到 2^53 以上。
   - 改为：用 pcall，失败时减回（§6.2）；最大号 ≥ 2^53 时 upgrade 报错（§8.1、§8.2）。
4. 【中，两路都报】"缺口对消费方无害"不成立，消费方会永久停在缺口前。
   - 改为：遇到缺号时，在同一个事务里 `get` 确认（CNT 加 VER）之后再跳过（§9）。
   - 另一路建议"洞持续超过复制延迟上限就跳过并报警"。没有采纳：那依赖时间假设，而 `get` 确认不依赖。
5. 【中】"HeTu 自己的读，索引和取行在同一个节点"在原生 cluster 多副本下不成立。§4.4 改写成按节点的保证，并说明临时洞从哪里来。
6. 【中】消费方每读一行，master 多一条 VER：写进 §10，问题交给用户（§14.4）。
7. 【中】客户端消费时，RLS 不可见的行就是缺口：文档要求组内每一行都对该客户端可见（§9）。
8. 【中】`repo.get` / `range` 的 docstring 和实际行为相反：文档清单加上 docstring（§3.3、§12）。
9. 【中低】校准原来放在换上索引之后，进程中途被杀会留下偏低的计数器：改成先校准、再换上（§8.2）。
10. 【中低】重复检查要排除 0，否则"新加 `commit_seq` 列"的历史行全是重复：≤ 0 视为没有号（§8.1、§8.2）；有号的行才锁分组列（§3.2）。
11. 【中低】备份恢复、主库重启、故障切换的说法不准确：§4.5 分开写，并说明校准修的是"计数器落后于数据"。
12. 【低】Phase 2 出错时两个后端行为不同：写进 §6.3，测试不在 SQLite 上断言出现缺口。
13. 【低】"INCRBY 不产生通知"要看节点原有的配置：§5.1 补上条件。
14. 【低，两路都报】`%d` 依赖 64 位的 long：改用 `%.0f`（§6.2）。
15. 【低，两路都报】旧版 headless 能通过 schema 比对：
    - 文档点明 headless 客户端要一起升级；
    - `_schema_diff` 遇到不认识的列属性键判为不一致（§8.3）。
16. 【低】hidden + point_sub 的检查顺序：忽略 point_sub、强制 index 这两条挪进 `_normalize_prop`，排在报错之前（§3.1）。
17. 【低】upsert 锚定检查靠 assert：改成 raise（§3.2）。
18. 【低】用完即弃的分组值会让计数器一直增长：写进文档，内存按实测更正（§5.1）。
19. 存疑项里采纳的：
    - 计数器 key 带上分组列名，改 groupby 时不会继承旧计数器（§5.1）；
    - HSETSEQ 用 `seq_allocs` 的下标，号放在 HSET 最后一对（§6.1）；
    - `commit_seq` 不能是 RLS 判定列（§3.1）；
    - 淘汰策略写进运维文档（§4.5）；
    - §8.1 关于 `--no-rebuild-index` 的背景更正；
    - "0"的两种含义写进文档（§3.3）。
20. 测试计划、文档清单按两路审核补全（§11、§12）。

**交给用户决定的**（2026-11-09 已定）：

- §14.2：组内重复号改为报错。审核实测重复号会让消费方静默漏行。
- §14.4："不登记版本的读"（get 和 range）以后单独做。

## 17. 实现记录（2026-11-09）

按 §15 的顺序在 `feat/commit-seq` 上实现，每步一个提交、带测试：

1. 定义：`615f98d5`
2. 提交：`94918e2a`
3. 事务：`7e11c348`
4. 维护：`49c430a2`
5. 并发、前缀可见与消费方的用例：`39ff99e0`、`81e00a5f`
6. 文档：`b8ce24ec`

用例在 `tests/test_commit_seq.py`。sqlite、redis、valkey、redis_cluster 四个后端共 142 个通过；全量 2866 个通过，4 个跳过。

和本文写法不同、或者本文没写到的地方：

- **代码里的名字**：
  - 计数器 key 由 `RedisModelClient.seq_counter_key_` 算；
  - 重建时的统计、检查放在 `CommitSeqScan`（`redis_model.py`），两个后端共用；
  - 上限是 `base.COMMIT_SEQ_LIMIT`；
  - Redis 校准计数器用 `_RAISE_COUNTERS_LUA`，一批一条脚本。
- **SQLite 的 drop / rename 提前到第 2 步**：测试之间靠 `create_ref` 的 flush 隔离，drop 不删计数器的话，号会串到下一个用例。
- **两个后端提交脚本一致的用例**：没有加进 `test_backend_client.py`，放在 `test_commit_seq.py`、两个后端共跑。覆盖的是发号的全成或全不发、越界、HSETSEQ 最后一对覆盖残留的 0。
- **upsert 锚定非 unique 字段从 assert 改成 raise ValueError**：`test_backend_session_basic.py::test_upsert_limit` 的预期跟着从 AssertionError 改成了 ValueError。
- **C# 文档**：示例用 `long.MaxValue` 作整数列的上界。
- **LocalApp 的 docstring 没改**：`LocalApp.get` 写的是"语义同 `repo.get`"，`range` 写的是"签名与 `repo.range` 对齐"。
  LocalApp 每次调用都是只读的新事务，不会有本事务 insert 的行，例外不适用。

## 18. 实现审核记录（2026-11-09）

实现完成后做了两路独立代码审核：一路看提交脚本、存储与维护，一路看定义、事务语义、文档。审核员只拿到本文与代码，
都写了临时用例实测，没有改仓库。

**核实无误的部分**：

- 发号的时机、全成或全不发（包括跨组件、副本组件、一次多个号跨过 2^53），两个后端的返回串逐字节一致；
- 号的编码、`%.0f` 与 Python 侧一致，HSETSEQ 的号放在最后一对、下标与 `seq_allocs` 的顺序一致；
- 计数器 key 的 hash tag、在换簇 / 改名到临时表 / drop / 重建 / 副本组件时的去留；
- 定义校验完整；全仓只有 `repo.insert` / `repo.update` 写 idmap，upsert、LocalApp、headless、Sandbox、
  `ctx.session_commit()` 都走同一条路；
- "未分配号的行不在这个索引上"不破坏读己之写、limit 截断、防幻读、InconsistentRangeRead、negative cache、订阅；
- zh / en 文档逐条一致，`docs/api` 与 docstring 同步；没有新增 PUBLISH 和事务路径上的 master 读。

**按审核意见修改的部分**（每条一个提交，带测试）：

1. 【中】改 dtype 的迁移，迁移脚本搬完数据后重建失败（组内重复号、号到 2^53，unique 冲突也一样）：meta 已经是新版本，
   服务器照常启动，而索引没建成、计数器没校准，会发出重号。改成重建失败时把 meta 的版本号写成
   `REBUILD_PENDING_VERSION`，服务器拒绝启动；修好数据后再跑 `hetu upgrade`，按当前定义直接重建（§8.2）。
   审核员另提的"迁移前按目标定义预检一遍"没有做：分不清默认脚本和改过的脚本，改过的脚本可能本来就要重新编号。
   自查时又发现：卡在待重建之后又改了定义，拿共用的哨兵版本号找迁移脚本会捡到别的组件的脚本，改成按数据实际的版本号找。
2. 【中】消费方逐号 `get` 确认缺口，代价随缺口长度线性增长。改成每段缺口一次闭区间 range（§9），补了"确认时副本落后
   要判竞态"和"大缺口只读一次"两条用例。
3. 【中低】不用提交序号列的提交，在 master 上也多了 0.3–0.4µs（单行 update 多 15%）：每次都建发号用的闭包和表、payload
   总带空的第 6 段。改成只有要发号时才带、才建（§6.2）。
4. 【中】客户端文档"处理完把游标推到最后一个号"会永久越过临时缺号：改成只沿连续的号推进。另外写明拿新行的号要求新行
   落在订阅窗口里。
5. 【中低】文档说旧版 headless "读不懂"：传组件类的旧版 headless 其实能连上，并写入没有号的行。改成先升级 headless 再打开。
6. 【低】pcall 把 INCRBY 的所有错误都改写成 SEQ：改成原样返回 Redis 的错误，只有号越界才走 SEQ；计数器被改成负数时
   也不发（号会 ≤ 0）。
7. 【低】两个后端在计数器异常时不一致：SQLite 改按 Redis string2ll 的规则认整数、溢出 int64 的报错对齐；重建校准不用
   `tonumber`，被写坏的值（含不是字符串的）直接改写。
8. 【低】第三方消费方"读不到即号被复用"和裁剪冲突：分三种结果（§9）。
9. 【低】RLS 判定列的检查只在 OWNER / RLS 权限下生效，`rls_compare` 格式不对时不再先抛 IndexError；`property_field` 的
   `index` 参数说明补上 groupby、commit_seq；headless 比对组件级不认识的键也判不一致。
10. 测试缺口：NX / CNT 失败不耗号、绕过 repo 改分组列、本事务 insert 的行改号、一次提交两张提交序号表、headless 写入、
    号为 0 / 负数的历史行、WRONGTYPE、计数器的严格解析、全成或全不发时失败的行不存在、换上索引出错时计数器已校准、
    改 groupby 的端到端迁移。
11. 文档：游标对应逻辑上的消费方、缺口来源补上提交脚本中途出错、第三方的复用清单补上 AOF / RDB 重启；
    `sqlite/commit.py` 的模块说明、本文 §3.1 / §3.2 / §6.3 / §17 的出入。

**没有采纳 / 记为已知问题**：

- 订阅推送是连续前缀没有单独的用例：底层性质由"副本读到的总是连续前缀"的用例覆盖，审核员按组订阅实测过（1..15 都带真号）；
- 不用提交序号列的路径没有性能守门用例；
- `test_servant_reads_are_contiguous_prefixes` 只在有独立副本的 redis / valkey 上有意义，sqlite、cluster 上只是照跑。
