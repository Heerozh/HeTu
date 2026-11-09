# 提交序号列的库外消费方（`servant_seq_*`）— 设计稿

- 日期：2026-11-09
- 状态：已实现（§14），设计过了一轮两路独立审核（§13），已按审核意见改写。
  - 用户已定（2026-11-09）：读计数器和封装好的增量读都做。审核之后改了的命名和返回值见 §13。
- 分支：`feat/commit-seq-third-party`（基于 dev `fce72f5b`）
- 前置：提交序号列（PR #215，`2026-11-09-commit-seq-design.md`，下称"原稿"）
- 影响范围：
  - `hetu/data/backend/base.py`：`BackendClient` 加读方法 `seq_head` / `seq_head_many` / `seq_read`，`SeqReadResult`，异常 `CommitSeqDiverged`
  - `hetu/data/backend/redis_model.py`：三个读方法的共用逻辑、整数解析
  - `hetu/data/backend/redis/client.py`、`sqlite/client.py`、`sqlite/store.py`：读计数器、钉在一个节点上的读
  - `hetu/data/backend/table.py`：`Table.servant_seq_head` / `servant_seq_head_many` / `servant_seq_read`
  - `hetu/data/backend/__init__.py`：导出
  - `hetu/headless.py`：`HeadlessClient` docstring 里的接口列表
  - 守门：`tests/test_arch_master_reads.py`、`tools/master_read_audit.py`、`tests/fixtures/read_counts.py`
  - 文档生成：`scripts/api_extras.py`
  - 测试、文档
- **不改**：
  - 存储格式、提交脚本、payload；
  - `servant_range` / `servant_get_many` 的语义；
  - 服务端消费方的写法（原稿 §9）；
  - 不新增 master 读，不新增 PUBLISH。

## 1. 背景

原稿 §14.3 的前提估错了：游戏项目里命令表的消费方在库外。它用 headless 的 `servant_range` 读命令，不在事务里，游标存在自己的快照文件里，按原稿的分法是第三方消费方。原稿给第三方消费方的写法用不上：

1. **读不到发到了第几号**。原来写入方自己发号，自然知道；改用 `commit_seq` 之后，这个数只在 HeTu 内部的计数器里，没有读它的接口。
2. **没法确认缺口**。原稿 §9 靠提交时的 CNT 在 master 上确认缺口。headless 的读不在事务里；开事务也没用：`Session.commit` 只在有写入（`is_dirty`）时才提交，只读事务不跑任何检查。
3. **核对表用不了**。原稿 §9 给第三方的核对表要看"组内最大号"，也就是计数器。用现存的最大行代替时，尾部被删（比如已消费的行全裁剪掉了）就会误判成数据回退。

另外，现在用 `servant_range` 的结果判断缺号本身就不可靠（§3.2）。

## 2. 目标与非目标

目标：库外的消费方不进事务、不读 master，就能

- 读到每组发到了第几号；
- 确认缺口并安全跳过，不漏掉还在的行；
- 发现序列和自己记下的对不上（号被复用、组内重号，原稿 §4.5）。

非目标：

- 服务端消费方不变，仍按原稿 §9；
- 不提供跨节点一致的"计数器 + 索引"快照（§7）；
- 序列对不上之后不自动修复，只负责发现；
- 不改 `servant_range` 现有的语义（读不到的行跳过）；
- CLI（`hetu get`）不加读计数器的选项。

## 3. 依据

### 3.1 规则：一个节点上的一条 ZRANGE 就是证据

原稿 §4.4：在任何一个节点上，能读到 G 的 n 号，1..n-1 号（被删的除外）就一定也可见。一条 ZRANGE 是它落到的那个节点上的原子快照，所以：

- **快照里有 n 号、没有 j 号（after < j < n）**：j 号所在的提交早就应用了，那一行不在索引里了，可以跳过。原稿 §4.6 列了缺口的来源：删了中间的行、Redis 上提交脚本写到一半出运行时错误、数据损坏。
- **快照里没有任何大于 after 的号**：什么也确认不了。可能是没有新号，也可能是新行都被删了，或者这个节点落后。
- 证据只来自这一条 ZRANGE：不需要 master，也不需要事务。

两点要注意：

- **规则分不清一行是"处理过了才删的"还是"还没处理就被删了"**：只要有更大的号，没有索引项的号一律当缺口跳过，不报错。所以裁剪必须按消费方落盘的游标来（§6 的注意）。
- **孤儿行**：提交脚本插入时先 HSET 行、再 ZADD 索引，删除时先 ZREM 索引、再 DEL 行（`build_commit_payload_` 的 pushes）。中途出错只会留下"有行、没有索引项"的孤儿行，按索引读不到，按缺口跳过。但之后重建索引（`hetu upgrade`）会把它放回原来的号上，那时已经越过它的消费方不会再处理它（§8）。"有索引项、没有行"的残留只来自外部操作，比如维护脚本只删了行。

例外是原稿 §4.5 的回退（故障切换丢了最后几次提交、AOF / RDB 重启、整库恢复、易失表清空）和计数器落后于数据（手工删了计数器、淘汰、只导入了行）：节点的历史本身倒退了，或者号重复了。靠锚点和重号检查发现（§4.2）。

### 3.2 一次读必须钉在一个节点上

`servant_range` 是两步：一条 ZRANGE 拿 id，再用 pipeline 逐行 HGETALL，读不到的行直接跳过（`redis_model.py` 的 `range`）。这两步不一定落在同一个节点上：

- **显式配置了 servants**：`Backend.servant` 每次挑一个 client，每个 client 只连一个节点，两步在同一个节点上。
- **原生 cluster**：servant 用 `ROUND_ROBIN_REPLICAS`（`redis/client.py`）。它不是随机选副本，是每个分片一个确定的轮询计数器，每条读命令（pipeline 里的也一条一条算）加一。
- **读写分离代理**：按命令分流。
- **没有副本**：都在 master 上。SQLite 是同一个库文件。

两步落在复制进度不同的节点上会有两个后果：

1. **取行落后，读不到的行被当成缺口**：`servant_range` 的结果里出现的缺号，可能只是取行那条命令落到的副本还没同步到这一行。拿它当缺口跳过，就永久漏掉一行。
2. **"读不到就停"在 cluster 下也会永久卡住**（审核实测，§13 A1）：某个副本断了复制、照常回答读请求（Redis 默认 `replica-serve-stale-data yes`，比如 masterauth 配错、全量同步反复被输出缓冲上限踢掉）时，每一轮读的命令数对副本数取模是固定的：ZRANGE 总落在新副本、第一行总去旧副本取，或者 ZRANGE 总落在旧副本。1 主 2 从、1 主 3 从都实测锁死，每轮 0 行。

所以 `servant_seq_read` 的 ZRANGE 和取行**钉在同一个节点上**：

- 原生 cluster：每次调用用 `nodes_manager.get_node_from_slot(slot, load_balancing_strategy=LoadBalancingStrategy.RANDOM_REPLICA)` 随机选这个 slot 的一个副本（客户端不认得副本时是 primary），ZRANGE 和取行的 pipeline 都带 `target_nodes=` 发给它。组 zset、行 key 都在 `{CLU<id>}` 下，同一个 slot。
  - 用随机，不用轮询：消费方的其他读（比如读计数器的 MGET）也会推进轮询计数器，用轮询的话，每轮选到的节点又是固定的。随机选时，断了复制的副本只让一部分轮次没有进展。
- 显式 servants、没有副本、SQLite：本来就在一个节点上，不用做什么。
- 钉住之后，一行在 ZRANGE 里有、取的时候读不到，只可能是读完索引之后被删了（或者索引残留）。下次的 ZRANGE 里就没有它了，这时才确认缺口，不会卡住。
- 读写分离代理没法指定节点：断了复制的只读节点仍可能挡住消费方（不会让它跳过行）。代理要把复制断开的只读节点摘掉；被挡住时，返回值里会带出挡住的那一号（§4.2），监控能看到。

### 3.3 为什么计数器不能扩大确认范围

计数器和索引是两个 key，读计数器也不钉节点。计数器来自进度快的副本、ZRANGE 来自进度慢的副本时，(最大的行号, 计数器] 里的行其实还在，只是 ZRANGE 那个副本还没有。所以计数器只能用来知道有新号、监控积压、报警，不能拿来把游标推过没见到行的号。为什么不原子地一起读两者，见 §7。

## 4. 接口

都挂在 `Table` 上，和 `servant_get` / `servant_range` 一样走 servant，不进事务。headless 的 `client.table(Comp)` 返回的就是 `Table`，直接可用。

- 只能用在声明了 `commit_seq` 的组件上，否则 ValueError。
- `group` 的规则同 `range`：复合索引必须正好给出全部分组列，单列索引不能给（`group_prefix_of_`）。
- 整数参数收 int 和 `np.integer`（比如直接传 `rows[-1].seq`），不收 bool / `np.bool_`，进来先 `int()`。类型不对抛 TypeError，取值不对抛 ValueError。

### 4.1 计数器：`servant_seq_head` / `servant_seq_head_many`

```python
head = await tbl.servant_seq_head(group={"owner": 7})                    # -> int
heads = await tbl.servant_seq_head_many([{"owner": o} for o in owners])  # -> list[int]，顺序同参数
```

- 返回这一组最近发出的号；还没发过号是 0（易失表维护清空之后也是 0）。
- 读的是某个副本上的值：可能落后，也不和任何一次读在同一个快照里（§3.3）。
- 计数器的值不是 Redis 认的整数，或者是负数（被手工写坏了）：整批抛 RuntimeError，带上 key。跑 `hetu upgrade` 重建时会把它校准回来（原稿 §8.2）。解析规则同 Redis string2ll。
- Redis 上计数器 key 被写成了别的类型（比如 hash）：MGET 对它返回 nil，读成 0，不报错。这时提交本身会报 WRONGTYPE（原稿 §6.2），在文档里写明。
- `servant_seq_head_many([])` 返回 `[]`，不访问数据库。
- 不带 groupby 的组件用 `servant_seq_head()`；`servant_seq_head_many` 的每一项按 `group` 的规则校验，可以是 None。

### 4.2 增量读：`servant_seq_read`

```python
rows, blocked = await tbl.servant_seq_read(
    after, limit=100, *, group=None, after_id=None, row_format=RowFormat.STRUCT
)
```

参数：

- `after`：游标，也就是最后处理过的那一号；从头读给 0。0 ≤ after < 2^53。
- `after_id`：游标那一号的行的雪花 id（锚点）。给了就核对，这时 `after` 必须 ≥ 1；不给就不核对。任何 int64 都可以（headless 显式给的 id 可以是负数）。
- `limit`：最多返回的行数，正整数。不支持"不限"：积压可能很大，一次读完会长时间占住副本。上限不设，文档里写明一次读太多的代价。
- `row_format`：同 `servant_range`，不支持 `ID_LIST`（raise ValueError，不用 assert）。

返回 `SeqReadResult`，一个 NamedTuple：

- `rows`：号大于 `after` 的行，按号升序。
- `blocked`：`(号, 行 id)` 或 None。索引里有、这次却没取到行的第一个 member，它挡住了后面的行；None 表示没被挡住。

**保证**：设 n 是返回的最后一行的号（没有返回行时 n = after），(after, n] 里没出现在结果里的号都已确认不存在（§3.1），游标推进到 n。`rows` 为空且 `blocked` 为 None：读的那个节点上 after 之后没有任何 member（没有新号、尾部被删，或者这个节点落后）。

执行步骤：

1. 选一个节点，下面的读都发给它（§3.2）。
2. 一条 ZRANGE BYLEX：给了 `after_id` 时从 `after` 读起（含），否则从 `after + 1` 读起，读到 +inf；最多 `limit + 1` 个 member（给了锚点才多读一个）。
3. 从 member 解出号（值段末尾 8 字节，`from_sortable_bytes`）和行 id（`member_row_id_`）。同号的 member 按 id 排序，号是不降的。
4. 核对序列，只看 member，不取锚点那一行。下面任何一条不满足，就抛 `CommitSeqDiverged`（§4.3）：
   - 给了 `after_id`：
     - 号等于 `after` 的 member 只能是锚点自己（id 等于 `after_id`），否则是 `reused`；
     - 没有号等于 `after` 的 member、却有更大的号：锚点行没了，是 `anchor_missing`。按 §6 的裁剪规则，锚点行不会被正常删掉，它没了说明数据回退或者被误删；
     - 一个 member 都没有：什么都确认不了，返回空。
   - 去掉锚点之后，号必须严格递增；出现同号是 `duplicate`（计数器落后于数据时会发出重号）。
5. 剩下的最多 `limit` 个 member，用一次 `hgetall_many_` 取行（行多时按 `RANGE_PIPELINE_CHUNK` 分几次 pipeline 依次往返，只有一行时是一条 HGETALL）。
6. 从前往后，遇到第一个"读不到的行"就停，只返回它之前的行，它记为 `blocked`。"读不到"包括：
   - 行不存在；
   - 行在索引上的值段（`index_value_bytes_(comp, 字段, 行)`，带组前缀）和 member 的值段（`member_value_`）对不上。和 `_range_checks` 一样比整个值段，只比号会放过被挪进本组同一号的别组的行。
7. 按 `row_format` 解码，返回 `SeqReadResult(rows, blocked)`。

代价：一条 ZRANGE、一到几次 pipeline，都在副本上。

### 4.3 `CommitSeqDiverged`

- 含义：组里的序列和消费方记下的对不上了。重试不会好，所以不继承 RaceCondition。
- `reason`：
  - `reused`：游标那一号现在是另一行，号被复用了。原因可能是数据回退（故障切换、AOF / RDB 重启、整库恢复、易失表清空），也可能是计数器落后于数据之后又发了一遍（手工删了计数器、淘汰、只导入了行、改了组件名或字段名之后重新校准，原稿 §4.5、§8.1）；
  - `anchor_missing`：游标那一行没了，后面却有号。数据回退了，或者游标那一行被误删了；
  - `duplicate`：组里有重号。
- 属性：`comp_name`、`group`（调用方给的原值）、`seq`（出问题的那一号）、`expected_id`、`found_id`（`anchor_missing` 时为 None）、`reason`。
- 照 `InconsistentRangeRead` 的写法：构造参数全部传给 `super().__init__`（能 pickle），写 `__str__`。
- 定义在 `base.py`，从 `hetu.data.backend` 导出，加进 `scripts/api_extras.py`。

## 5. 实现

### 5.1 共用部分（`redis_model.py`）

三个读方法的逻辑都写在 `RedisModelClient`，后端只提供 I/O 原语：

- 原语：
  - `zrange_bylex_`、`hgetall_many_`（已有）加一个可选的 `node` 参数：钉在哪个节点上读，None 是照旧；
  - `seq_read_node_(key)`：给 `seq_read` 选节点，cluster 返回随机副本，其他返回 None；
  - `kv_get_many_(keys) -> list[bytes | None]`：新增，读计数器。
- 整数解析（Redis string2ll 的规则，现在是 `sqlite/store.py` 的 `_REDIS_INT`）挪到这里共用，SQLite 的 `kv_get_int` 改用它。
- 计数器 key 用 `seq_counter_key_(cluster_prefix, comp_cls, group_prefix_of_(...))` 算，和提交时用的是同一个函数。
- ZRANGE 的两端用 `zrange_args_` 算（`left=after` 或 `after + 1`，`right=math.inf`），member 的解码同 `range_index_`。

### 5.2 Redis（`redis/client.py`）

- `kv_get_many_`：只有一块时直接一条 MGET；多块（同 `RANGE_PIPELINE_CHUNK`）时放进一个 pipeline，每块用 `pipe.execute_command("MGET", *chunk)`。
  - 不能用 `pipe.mget`：redis-py 的 cluster pipeline 禁用它（`PIPELINE_BLOCKED_COMMANDS`）。
  - 不能用 `mget_nonatomic`：它按 `read_from_replicas` 选节点，HeTu 的 servant 没设这个参数，会读到 master。
  - 同一张表、同一个簇的计数器 key 都在 `{CLU<id>}` 下，属于同一个 slot；MGET 是读命令，cluster 下走副本。
- `seq_read_node_`：cluster 下用 `aio.keyslot(key)` 和 `aio.nodes_manager.get_node_from_slot(..., load_balancing_strategy=LoadBalancingStrategy.RANDOM_REPLICA)`；非 cluster 返回 None。
- `zrange_bylex_` / `hgetall_many_` 带 `node` 时，用 `execute_command(..., target_nodes=node)`（pipeline 里的命令同样带上）。

### 5.3 SQLite（`sqlite/store.py`、`sqlite/client.py`）

- `store.py` 加 `kv_get_many(keys) -> list[bytes | None]`，一次读完；client 的 `kv_get_many_` 用 `run_` 调它。
- 只有一个库文件，`seq_read_node_` 返回 None，`node` 参数忽略。

### 5.4 `Table`、文档生成、导出

- `table.py`：三个属性照 `servant_get` 的写法，函数体只有一句 `return bind_first_arg_with_typehint(self.backend.servant.seq_xxx, self)`，**不写 docstring**。`scripts/gen_api_docs.py` 只把这种写法认成方法，带 docstring 的属性会被当成 Attribute，没有签名（`servant_get_many` 就是这样）。
- docstring 和签名取自 `BackendClient`（`base.py`）的抽象方法：
  - numpy 格式，中英双语（AGENTS.md）；
  - `table_ref` 排第一个（文档生成时当成绑定参数删掉）；
  - 写全默认值；`seq_read` 按 `row_format` 写 overload，和实现一致。
- `scripts/api_extras.py` 的 EXTRAS 加上 `CommitSeqDiverged`（exceptions），`SeqReadResult` 放在 gen_api_docs 收类型的那一页。
- `hetu/data/backend/__init__.py` 导出 `CommitSeqDiverged`、`SeqReadResult`。
- `headless.py`：`HeadlessClient` 的 docstring 列出三个方法。

### 5.5 守门

- `READ_METHODS` 加上 `seq_head`、`seq_head_many`、`seq_read`，三处都要加：`tests/test_arch_master_reads.py`、`tools/master_read_audit.py`、`tests/fixtures/read_counts.py`。
- `tests/test_arch_master_reads.py` 的 `PATTERN` 现在只认调用形式 `master.xxx(`，认不出 `Table` 属性的绑定写法 `bind_first_arg_with_typehint(self.backend.master.xxx, self)`。改成后面跟 `(`、`,`、`)` 都算（审核实测：hetu/ 里不会多出误报）。
- `tests/test_master_read_budget.py` 加一条：有副本时，按 §6 的写法轮询（读计数器 + 增量读），master 读 0 次。

## 6. 消费方的写法（写进文档）

```python
import time

from hetu.data.backend import CommitSeqDiverged

tbl = client.table(ChangeLog)
cursors = load_snapshot()             # {owner: (seq, row_id)}，和消费方自己的状态存在同一个快照里
stopped: set[int] = set()             # 序列对不上、已经停下的组
behind_since: dict[int, float] = {}   # 计数器开始小于游标的时刻


async def poll(owner: int) -> None:
    seq, row_id = cursors.get(owner, (0, None))
    rows, blocked = await tbl.servant_seq_read(
        seq, limit=1000, group={"owner": owner}, after_id=row_id
    )
    for row in rows:
        handle(row)  # 中间没出现的号都已确认不存在
    if len(rows):
        cursors[owner] = (int(rows[-1].seq), int(rows[-1].id))
    watch_blocked(owner, blocked)  # 同一个 (号, id) 挡了很久才报警，见下文


async def poll_all(owners: list[int]) -> None:
    heads = await tbl.servant_seq_head_many([{"owner": o} for o in owners])
    now = time.monotonic()
    for owner, head in zip(owners, heads):
        if owner in stopped:
            continue
        seq = cursors.get(owner, (0, None))[0]
        if head < seq:  # 这个副本落后、在全量同步，或者数据回退了
            if now - behind_since.setdefault(owner, now) > 30:
                await alarm_behind(owner, head, seq)  # 报警前先 client.check_schema()，见"换簇"
            continue
        behind_since.pop(owner, None)
        if head == seq:
            continue  # 没有新号
        try:
            await poll(owner)
        except CommitSeqDiverged as exc:
            stopped.add(owner)  # 只停这一组，别的组照常消费
            await alarm_diverged(owner, exc)
```

**注意：裁剪只删号严格小于所有消费方落盘游标的行，删之前先核对。**

- `servant_seq_read` 遇到"这一号没有行、后面有更大的号"，就认定这一号已经不在了，直接跳过，不报错（§3.1）。它分不清"处理过了所以删掉"和"还没处理就被删了"。
- 消费方从快照文件恢复。如果裁剪按消费方运行时的进度来删，而快照比这个进度旧：崩溃重启后，快照到运行时进度之间的命令已经被删了，会被当成缺口跳过，消费方的状态就悄悄对不上了。所以裁剪的依据必须是**已经落盘的**游标。
- 裁剪方（服务端的 System 或运维脚本）读不到消费方本地的快照：消费方每次快照落盘成功之后，把快照里的 `(号, 行 id)` 写回库里的一张游标表（headless session 里 upsert，一次快照一次提交）。裁剪方按这张表取最小值。
- 只删号**严格小于**最小游标的行，保留每个消费方的锚点行。锚点行不会被正常删掉，所以它没了一定是异常，`servant_seq_read` 会抛 `anchor_missing`。
- 删之前先核对：最小游标那一号还是游标表里记的那一行（System 里用 `range(seq=(s, s), ...)`，headless 里用 `servant_range` 的 `ID_LIST`）。对不上就不删、报警。数据回退之后，复用了号的新行全都不大于旧的游标；不核对就删的话，这些从没被消费过的行会被裁掉，回退也发现不了（审核实测，§13 A2）。
- 给了 `after_id` 时不能"消费完就删"：至少留下最后处理的那一行。要消费完就删，就不能传 `after_id`，也就发现不了序列对不上。

核对结果：

| 情况 | 说明 | 处理 |
|---|---|---|
| 游标那一号还是那一行 | 正常 | 接着往后读（`servant_seq_read` 自动处理） |
| 游标那一号换了一行、组里有重号、游标那一行没了而后面有号 | 序列对不上了（§4.3） | 抛 `CommitSeqDiverged`：停下这一组，报警 |
| 什么都没读到 | 没有新命令；或者这个副本落后；或者数据回退到了游标之前、号还没被复用 | 等。计数器持续小于游标才报警（见下） |

其他：

- **计数器持续小于游标**：一个时间窗（比如 30 秒，要大于副本延迟）里每次读到的计数器都小于游标才报警。报警要写明可能的原因：副本严重落后或在全量同步、换簇了、数据回退了。
- **被挡住**：`blocked` 不为 None 只说明这一轮被它挡住了，取行落到落后的副本、读完索引后行被删都会这样，下一轮通常就好了。同一个 `(号, id)` 挡了很久才报警：
  - `servant_get` 一直读不到这一行：索引残留（比如维护脚本只删了行），停服跑 `hetu upgrade` 重建索引；
  - 读得到：多半是读写分离代理把请求发到了断了复制的只读节点，修副本。
- **追上没有**：看 `rows` 为空且 `blocked` 为 None，不看游标是否等于计数器。最新的几行在消费之前就被删了的话，游标会停在最后见到的那一行，直到更新的一行出现；这期间计数器减游标大于 0，但没有可消费的东西。
- **快照**：游标（号和雪花 id）要和消费方自己的状态存在同一个快照里。崩溃后从快照恢复，会重放快照之后的命令：`handle` 要么幂等，要么状态和游标一起恢复。
- **换簇**：计数器和数据跟着换簇搬走。没重连的消费方手里的 `Table` 还指着旧的簇：计数器全是 0，什么都读不到，正好落进"计数器持续小于游标"。报警前先 `client.check_schema()`，遇到 `ClusterChanged` 就重连，游标照常用。
- **组很多时**：每组每轮算一次 key（约 3µs），10 万组每轮约 0.3 秒 CPU，副本上每轮也要查 10 万个 key。组多就降低轮询频率，或者只轮询活跃的组。组的清单要消费方自己维护（比如从游戏逻辑的房间表来）。
- **一次读太多**：`limit` 越大，ZRANGE 和取行占住副本的时间越长，行数超过 `RANGE_PIPELINE_CHUNK` 时要分几次往返。一般几百到几千行。
- **易失表**：每次维护后号从 1 重来，游标要跟着清；没清的话，会按上表"什么都没读到"或 `CommitSeqDiverged` 处理。

## 7. 不采用的做法

- **用只读 Lua 或 MULTI，把计数器和索引放进同一个快照**：好处是游标能直接推进到计数器，尾部被删时也能确认。但读写分离代理一般默认把 Lua 和事务发给 master（比如阿里云要开 `readonly_lua_route_ronode_enable` 才把只读 Lua 转给只读节点），每次轮询都占 master，换个代理也不一定能用。§3.1 的规则不需要原子地读两个 key，就能确认 n 号之前的缺口，代价只是尾部被删时游标停在最后见到的那一行。
- **headless 开事务，借提交时的 CNT 确认**：只读事务不提交，要假写一笔，每次确认都是一次 master 提交。
- **用计数器把游标推到 head**：见 §3.3。
- **改 `servant_range`，读不到的行不跳过**：会改变现有用法的结果（比如命令队列按 `created_at` 轮询），而且 `servant_range` 不知道号，还是没法核对锚点。
- **把计数器存进组 zset，当一个特殊的 member**：一条 ZRANGE 就能同时拿到，但每次提交多两条命令，而且所有读这个索引的路径（range、订阅、CNT 计数）都要过滤它。
- **读写分离代理下，第一行读不到时单独再取几次**（审核 A1 的备选）：轮询型代理下每次会换一个节点，能绕过断了复制的副本，但这是猜代理的行为。不做，改为要求代理摘掉断了复制的节点，并用 `blocked` 让卡住能被看到。
- **单独的探测方法**（审核 B4 的备选）：`blocked` 已经带出了要区分的信息。

## 8. 已知限制

- **被删的命令一律当缺口跳过**：分不清处理过了才删、还是没处理就被删了。裁剪按 §6 的规则来；游戏逻辑里不要删还没消费的命令，要撤销就另写一条"撤销"命令。
- **孤儿行**：提交脚本中途出错留下的"有行、没有索引项"的行被当成缺口跳过；之后重建索引会把它放回原来的号上，已经越过它的消费方不会再处理它。重建时报告这类行，这次不做（§13）。
- **号被复用、但复用的那一号没有行**（被删了，或者是提交中途出错留下的缺口）：锚点和重号检查都发现不了，只能靠"计数器持续小于游标"，有时间窗。
- 尾部被删时，游标停在最后见到的那一行（§6）。
- 读写分离代理下不能钉节点，断了复制的只读节点会挡住消费方，要靠代理摘除和 `blocked` 报警。
- 计数器来自落后的副本时会短暂小于游标，所以报警要有时间窗。
- Redis 上计数器被写成别的类型时读成 0（§4.1）。
- 顺带发现的既有问题（不只影响本稿，这次不修）：redis-py 的 cluster 客户端只在初始化和遇到 MOVED 时读 CLUSTER SLOTS，而 Redis 不列复制偏移还是 0 的副本。副本还没同步完时启动的客户端认不出副本，所有 servant 读都会落到 primary，守门测试拦不住。

## 9. 测试

新文件 `tests/test_commit_seq_servant.py`，四个后端都跑。

- 计数器：
  - 没发过号是 0；insert 之后是最新的号；
  - 多组批量，顺序和参数一致；不分组的组件；字符串 / bytes 分组列、P 超过 32 字节、值带结尾 `\x00` 时 key 和提交时一致；
  - **redis_cluster 上走到多块的分支**（把块大小 monkeypatch 小）；
  - 易失表 flush 之后为 0；
  - 分组值少给、多给，或者给了不分组的组件：ValueError；组件没有 `commit_seq`：ValueError；
  - 计数器被写成非整数、负数：RuntimeError；Redis 上被写成 hash：读成 0；
  - 空列表不访问数据库。
- 增量读：
  - 从 0 读起、limit、升序、只读本组；不分组的组件；分组值校验；组件没有 `commit_seq`；号 ≤ 0 的历史行不返回、不挡住后面的行；
  - 删了中间的行：跳过；删了尾部的行：返回空，之后再 insert 一行，返回新行、跳过被删的号；
  - **取行读不到**（替换 `hgetall_many_`，模拟读完索引之后行被删）：停在它前面，`blocked` 是它，不跳过；下次读时它不在索引里了，跳过；出现在第一行、中间、最后都测；
  - 值段对不上（行被挪进了别的组、号相同）：按读不到处理；
  - limit 和锚点的边界：锚点在、后面正好 limit 个 member；锚点不在、有 limit + 1 个 member（只返回 limit 行）；`limit=1`；
  - 锚点：对得上时不返回它；`reused`（把计数器拨回去、删掉行再 insert）；`anchor_missing`（删掉锚点行，后面有行）；`duplicate`（把计数器写成 0 再 insert，审核 A3 的场景）；异常的属性、pickle、`__str__`；
  - 参数：`after_id` 配 `after=0`、负数 after、≥ 2^53、bool、np.int64 的 after 和 after_id、负数的 after_id、`limit` ≤ 0、`ID_LIST`；
  - `row_format` 各种格式；
  - **钉节点**：用桩确认 cluster 下一次调用的 ZRANGE 和所有取行都发给同一个节点，节点用 `RANDOM_REPLICA` 选。
- headless 端到端：经 `hetu.headless.connect` 按组件类连接、按组件名连接（`load_json` 生成的类）各跑一次。
- §6 的写法做成用例（同原稿 §11 对服务端写法的做法）：并发写入、取行读不到、按落盘游标裁剪（含删之前的核对）、从快照恢复后重放、一组 `CommitSeqDiverged` 时别的组照常消费。
- 守门：`READ_METHODS` 加上新方法后，审计插件包住了它们；`PATTERN` 能认出绑定写法；master 读预算（§5.5）。
- 有独立副本的 redis / valkey：在真实副本上读，结果和 master 一致。
- 测试用的 cluster 是 `--cluster-replicas 0`，"cluster 下多个副本、某个副本断了复制"测不到。实现时按审核 A1 的做法手工验证一次（1 主 2 从，一个副本改错 masterauth 断开复制），结果记进实现记录。

## 10. 文档

- `docs/zh/advanced.md`、`docs/en/advanced.md`：
  - "binlog 式同步"里第三方消费方那一条改成一小节：§6 的写法、核对表、裁剪的注意（含核对和游标表）、被挡住、追上、快照、换簇、组很多时、易失表；
  - headless 一节"读：`Table.servant_*`"的方法清单里列出三个新方法，轮询命令队列的地方加一句：要连号、不漏的命令表，用 `commit_seq` 加 `servant_seq_read`。
- `docs/zh/operations.md`、`docs/en/operations.md`：游标存在库外的消费方要核对雪花 id 那一句，指向新小节和 `servant_seq_read`。
- `hetu/skills/building-on-hetu/SKILL.md`：Commit sequence 一条补上库外的写法，headless 一节补上三个方法。
- `hetu/llms.txt`：Advanced 一行、Exceptions 一行（`CommitSeqDiverged`）。
- `docs/api`：重新生成（三个新方法、`SeqReadResult`、`CommitSeqDiverged`）。

## 11. 原稿要改的地方

- §4.4 的结论：库外的消费方不在事务里，改用一条 ZRANGE 的快照作证据，见本稿 §3。
- §4.5 第三方消费方那一条："发现组内最大号小于游标时要报警"改成"计数器持续小于游标时报警"，写法见本稿 §6。
- §9 第三方消费方的核对表：作废，换成指向本稿 §6。原来那张表依赖读不到的计数器，而且会把临时漏读当成删除。
- §14.3：前提更正，命令表的消费方在库外，是 headless 第三方消费方。

## 12. 实施顺序（每一步一个提交，带测试）

1. 读计数器：整数解析挪到 `redis_model.py`，`kv_get_many_` 原语，`seq_head` / `seq_head_many`，`Table` 绑定。
2. 增量读：钉节点的读原语，`seq_read`，`SeqReadResult`，`CommitSeqDiverged`。
3. 守门：三处 `READ_METHODS`、`PATTERN`、master 读预算。
4. §6 的写法做成用例，headless 端到端。
5. 文档、`scripts/api_extras.py`、`docs/api` 重新生成。
6. 手工验证多副本 cluster，写实现记录。

## 13. 审核记录（2026-11-09）

两路独立审核：A 看正确性（会不会漏行、误跳、卡住、漏报回退），B 看接口、实现方案、测试和文档计划。两路都按 §4.2 写了原型，在四个后端上实测，A 还自建了 1 主 2 从、1 主 3 从的原生 cluster；临时文件都已删除。

**核实无误的部分**：

- §3.1 的规则在单个节点上成立：提交脚本里插入先 HSET 后 ZADD、删除先 ZREM 后 DEL；显式 servants 下 ZRANGE 和取行在同一个节点；SQLite 是自动提交，ZRANGE 是一条 SELECT；没有副本时同样成立。
- 锚点只看 member 就够了（加上下面 A3 的检查之后）：member 在，就说明那一号现在就是这一行；取锚点那一行不会多知道什么。
- 计数器 key 的 slot 假设成立，cluster 上多 key 的 MGET 不报 CROSSSLOT，MGET 按负载策略走副本；计数器 key 和提交时算的一致（长 U 列、S 列、结尾 `\x00`）。
- 计数器的解析：写进去的都是十进制整数串，DECRBY 回滚留下的 "0" 等于没发过号。
- §3.3 成立；`Session.commit` 只在 `is_dirty` 时提交；`servant_range` 确实静默跳过读不到的行；审计插件能包住新方法。
- §5 引用的函数（`_zrange_members`、`hgetall_many_`、`seq_counter_key_`、`group_prefix_of_`、`from_sortable_bytes`、`member_row_id_`、`member_value_`、`run_`、`kv_get_int`）都在，签名和行为与设计稿一致。

**按审核意见修改的部分**：

- **A1【高】cluster 多副本下永久卡住**：`ROUND_ROBIN_REPLICAS` 是确定性轮询，某个副本断了复制时，"读不到就停"每轮 0 行。改成一次读钉在一个随机选的副本上（§3.2），读写分离代理下写明限制（§8）。
- **A2【高】按落盘游标裁剪遇上回退，没消费的新行被裁掉、回退也发现不了**：改成只删严格小于最小游标的行、保留锚点行，删之前核对；锚点行没了改为抛异常（§6、§4.2）。
- **A3【中】计数器丢失造成的重号发现不了**：锚点之后号不大于 after、一批里同号，都抛 `CommitSeqDiverged`（§4.2 第 4 步）。
- **A4 / B4【中】"没进展、计数器大于游标"分不出原因，§6 的处理会错**：返回值改成 `SeqReadResult(rows, blocked)`，带出挡住的那一号；改写 §6 的"被挡住""追上"。
- **A5 / B5【低】取行只比号**：改成比整个值段（§4.2 第 6 步）。
- **A6【低】孤儿行**：写进 §3.1 和 §8。重建时报告这类行，这次不做：只在提交脚本中途出错时出现，服务端消费方同样受影响，单独处理。
- **A7 / B9【低】WRONGTYPE 读成 0**：写进 §4.1，加用例。顺带发现的 cluster 客户端认不出新副本，记进 §8，这次不修。
- **A8 / B3【中】示例里一组出错拖垮所有组、回退报警没有落点**：§6 的示例按组捕获、加了"计数器持续小于游标"的计时。
- **B1【高】cluster 下 pipeline 里不能 `mget`，`mget_nonatomic` 会读 master**：§5.2 写明用 `execute_command("MGET")`，测试要在 redis_cluster 上走到多块的分支。
- **B2【中】gen_api_docs 的格式要求**：§5.4 写明属性不写 docstring、docstring 和签名写在 base 的抽象方法上、异常加进 `scripts/api_extras.py`。
- **B6【中】裁剪方拿不到快照里的游标**：消费方落盘后把游标写回库里的游标表（§6）。
- **B7【低】参数类型**：收 `np.integer`、`after_id` 可以是负数、写明 TypeError / ValueError、拒绝 `ID_LIST` 用 raise（§4）。
- **B8【低】守门的 PATTERN 认不出绑定写法**：改 PATTERN，加 master 读预算用例，第三份 `READ_METHODS` 也加上（§5.5）。
- **B9【低】实现结构**：逻辑都放 `RedisModelClient`，后端只提供原语，整数解析共用（§5.1）。
- **B10【低】命名**：`servant_seq_heads` 改成 `servant_seq_head_many`，和现有的 `_many` 一致。异常不叫 `CommitSeqRewound`：触发它的不只是回退，还有计数器落后于数据、锚点行被误删、重号。B 建议 `CommitSeqReused`，改成覆盖面更全的 `CommitSeqDiverged` 并带 `reason`；能 pickle，有 `__str__`。
- **B11【低】换簇**、**B12【低】组很多时的代价**：写进 §6。
- **B13【低】文档计划漏项**、**B14【低】原稿 §4.5 的措辞**、**B15【低】测试计划缺口**、**B16【低】措辞**：§10、§11、§9、§4.2 / §5.1 已补。

**没有采纳的**：

- A1 的"读写分离代理下第一行读不到时单独重取几次"、B4 的"单独的探测方法"：见 §7。
- B7 的 `limit` 上限：不设，文档写明代价。
- B12 的缓存组 key：先不做，每组约 3µs，组多时先降轮询频率。

## 14. 实现记录（2026-11-09）

按 §12 的顺序实现，每步一个提交、带测试：

1. 读计数器：`6f2979a1`
2. 增量读：`18fc35f9`
3. 守门：`90db0f7b`
4. §6 写法的用例、headless 端到端：`a4390bdb`
5. 文档、`scripts/api_extras.py`、`docs/api`，以及本节：`3b241130`
6. 补上裁剪规则漏改的两处旧说法（原稿 §9 的补注、binlog 一节的"裁剪"条目：游标那一行要留着）：`a6fc407e`

和本稿写法不同的地方：

- **整数解析放在 `base.py`**（`parse_redis_int_`），不在 `redis_model.py`：SQLite 的 `store.py` 不依赖模型层，从 `base` 导入不会循环。
- **钉节点不给原语加 `node` 参数**：改成 `pinned_reader_(key)` 返回一个带 `zrange_bylex_`、`hgetall_many_` 的对象（`SeqReader` 协议）。非 cluster 返回自己；原生 cluster 返回 `_PinnedClusterReader`，两条原语都带 `target_nodes` 发给选定的节点。热路径上的原语一个字没改。§5.1 里的 `seq_read_node_` 就是它。
- **cluster 下的 master 连接**选 primary（不带负载策略），servant 连接才用 `RANDOM_REPLICA`。
- **`reason` 的区分**：号等于 `after` 的 member 里，锚点还在、另有别的行时是 `duplicate`；锚点不在、这一号是别的行时是 `reused`。
- `SeqReadResult` 用 PEP 695 的泛型 NamedTuple（`class SeqReadResult[RowsT](NamedTuple)`）。

**手工验证多副本 cluster**（§9 最后一条，按审核 A1 的做法）：3 个节点配成 1 主 2 从，写入 1..10 号并同步之后，把其中一个副本的 masterauth 改错、断开复制，它停在 10 号；再写入 11..20 号。从游标 (10, id) 轮询 30 轮：

| 读法 | 只读 | 先读计数器再读 |
|---|---|---|
| `servant_seq_read`（钉在随机选的副本上） | 第一轮就推进到 20 | 第一轮就推进到 20 |
| 对照：不钉节点，ZRANGE 和取行各自按命令轮询副本 | 30 轮都停在 10 | 30 轮都停在 10 |

验证时顺带撞上了 §8 记的既有问题：副本在主库还没有任何写入时同步，复制偏移是 0，CLUSTER SLOTS 不列它们，HeTu 的 servant 客户端只认得 primary，所有 servant 读都落到了 master。先往主库写一笔、等副本的偏移大于 0 再建客户端，才认出两个副本。

测试：`tests/test_commit_seq_servant.py` 在 sqlite、redis、valkey、redis_cluster 上全部通过（cluster 钉节点的用例只在 redis_cluster 上跑）；守门与读预算用例通过；四个后端全量 3100 个通过、6 个跳过（`pytest==9.1.1 -n 8`）。

没有做的：重建索引时报告孤儿行（§8）、`limit` 上限、缓存组 key、§8 记的 cluster 客户端认不出新副本的既有问题。
