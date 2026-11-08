# 提交序号列的库外消费方（`servant_seq_*`）— 设计稿

- 日期：2026-11-09
- 状态：设计稿，待审核
  - 用户已定（2026-11-09）：读计数器和封装好的增量读都做，接口按本稿 §4 的草案。
- 分支：`feat/commit-seq-third-party`（基于 dev `fce72f5b`）
- 前置：提交序号列（PR #215，`2026-11-09-commit-seq-design.md`，下称"原稿"）
- 影响范围：
  - `hetu/data/backend/base.py`：`BackendClient` 加读方法 `seq_head` / `seq_heads` / `seq_read`，异常 `CommitSeqRewound`
  - `hetu/data/backend/redis_model.py`：`seq_read`（两个后端共用）
  - `hetu/data/backend/redis/client.py`、`sqlite/client.py`、`sqlite/store.py`：`seq_heads`
  - `hetu/data/backend/table.py`：`Table.servant_seq_head` / `servant_seq_heads` / `servant_seq_read`
  - `hetu/data/backend/__init__.py`：导出 `CommitSeqRewound`
  - `hetu/headless.py`：`HeadlessClient` docstring 里的接口列表
  - 守门：`tests/test_arch_master_reads.py`、`tools/master_read_audit.py` 的 `READ_METHODS`
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
- 发现号被复用（数据回退，原稿 §4.5）。

非目标：

- 服务端消费方不变，仍按原稿 §9；
- 不提供跨节点一致的"计数器 + 索引"快照（§7）；
- 回退之后不自动修复，只负责发现；
- 不改 `servant_range` 现有的语义（读不到的行跳过）；
- CLI（`hetu get`）不加读计数器的选项。

## 3. 依据：一条 ZRANGE 就是证据

### 3.1 规则

原稿 §4.4：在任何一个节点上，能读到 G 的 n 号，1..n-1 号（被删的除外）就一定也可见。一条 ZRANGE 是它落到的那个节点上的原子快照，所以：

- **快照里有 n 号、没有 j 号（after < j < n）**：j 号所在的提交早就应用了，那一行已经不在了，以后也不会再出现，可以跳过。原稿 §4.6 列了缺口的来源：删了中间的行、Redis 上提交脚本写到一半出运行时错误、数据损坏。
  - 提交脚本插入时先 HSET 行、再 ZADD 索引，删除时先 ZREM 索引、再 DEL 行（`build_commit_payload_` 的 pushes）。中途出错只会留下"有行、没有索引项"的行，按索引读不到，等同于缺口；不会留下"有索引项、没有行"的残留。后者只来自外部操作，比如维护脚本只删了行（§6、§8）。
- **快照里没有任何大于 after 的号**：什么也确认不了。可能是没有新号，也可能是新行都被删了，或者这个节点落后。
- 证据只来自这一条 ZRANGE：不需要 master，也不需要事务。

例外是原稿 §4.5 的回退（故障切换丢了最后几次提交、AOF / RDB 重启、整库恢复、易失表清空）：节点的历史本身倒退了。用锚点发现（§4.2）。

**这条规则分不清一行是"处理过了才删的"还是"还没处理就被删了"**：只要有更大的号，没有行的号一律当缺口跳过，不报错。所以裁剪必须按消费方存下来的游标来（§6 的注意）。

### 3.2 为什么 `servant_range` 的结果不能当证据

`servant_range` 是两步：一条 ZRANGE 拿 id，再用 pipeline 逐行 HGETALL，读不到的行直接跳过（`redis_model.py` 的 `range`）。原稿 §4.4 已经列出，取行会被拆到复制进度不同的副本上：原生 cluster 的 servant 用 `ROUND_ROBIN_REPLICAS`，每条命令单独选副本；读写分离代理按命令分流。结果里的缺号，可能只是取行那条命令落到的副本还没同步到这一行。

- 拿它当缺口跳过：永久漏掉一行；
- 一直等它补上：遇到真被删的行，永远卡住。

用 `ID_LIST` 只拿那一条 ZRANGE 的结果，再 `servant_get_many` 取行、读不到就停，能写对。但细节容易错，所以封装成 `servant_seq_read`（§4.2）。封装之后还能直接从 member 里解出号，核对锚点不用取行。

### 3.3 为什么计数器不能扩大确认范围

计数器和索引是两个 key。读写分离下，读计数器和 ZRANGE 可能落到不同的副本：计数器来自进度快的副本、ZRANGE 来自进度慢的副本时，(最大的行号, 计数器] 里的行其实还在，只是 ZRANGE 那个副本还没有。所以计数器只能用来知道有新号、监控积压、报警，不能拿来把游标推过没见到行的号。为什么不原子地一起读两者，见 §7。

## 4. 接口

都挂在 `Table` 上，和 `servant_get` / `servant_range` 一样走 servant（随机选一个；没配副本就是 master），不进事务。headless 的 `client.table(Comp)` 返回的就是 `Table`，直接可用。

- 只能用在声明了 `commit_seq` 的组件上，否则 ValueError。
- `group` 的规则同 `range`：复合索引必须正好给出全部分组列，单列索引不能给（`group_prefix_of_`）。

### 4.1 计数器：`servant_seq_head` / `servant_seq_heads`

```python
head = await tbl.servant_seq_head(group={"owner": 7})                # -> int
heads = await tbl.servant_seq_heads([{"owner": o} for o in owners])  # -> list[int]，顺序同参数
```

- 返回这一组最近发出的号；还没发过号是 0（易失表维护清空之后也是 0）。
- 读的是某个副本上的值：可能落后，也不和任何一次 range 读在同一个快照里（§3.3）。
- 计数器的值不是 Redis 认的整数（被手工写坏了）：抛 RuntimeError，带上 key。解析规则同 SQLite 的 `kv_get_int`（Redis string2ll）。
- `servant_seq_heads([])` 返回 `[]`，不访问数据库。
- 不带 groupby 的组件用 `servant_seq_head()`；`servant_seq_heads` 的每一项按 `group` 的规则校验，可以是 None。

### 4.2 增量读：`servant_seq_read`

```python
rows = await tbl.servant_seq_read(
    after, limit=100, *, group=None, after_id=None, row_format=RowFormat.STRUCT
)
```

- `after`：游标，也就是最后处理过的号；从头读给 0。必须是 int（不能是 bool），0 ≤ after < 2^53。
- `after_id`：游标那一号的行的雪花 id（锚点）。给了就核对，这时 `after` 必须 ≥ 1；不给就不核对。
- `limit`：最多返回的行数，正整数。不支持"不限"：积压可能很大，一次读完会长时间占住副本。
- `row_format`：同 `servant_range`，不支持 `ID_LIST`。

**返回**号大于 `after` 的行，按号升序。**保证**：设返回的最后一行是 n 号，(after, n] 里没出现在结果里的号都已确认不存在（§3.1），游标可以推进到 n。没有返回行时什么都确认不了，游标不动。

执行步骤：

1. 一条 ZRANGE BYLEX（`_zrange_members`）。给了 `after_id` 时从 `after` 读起（含），否则从 `after + 1` 读起，读到 +inf；最多 `limit + 1` 个 member（给了锚点才多读一个）。
2. 从 member 解出号（值段末尾 8 字节，`from_sortable_bytes`）和行 id（`member_row_id_`）。
3. 核对锚点，只看 member，不取锚点那一行：
   - 第一个 member 正好是 `after` 号、行 id 等于 `after_id`：去掉它，接着处理；
   - 第一个 member 正好是 `after` 号、行 id 不一样：抛 `CommitSeqRewound`（§4.3）；
   - 第一个 member 的号大于 `after`：游标那一行被删了（比如裁剪），没东西可核对。
4. 剩下的最多 `limit` 个 member，用一次 `hgetall_many_` 取行。
5. 从前往后，遇到第一个读不到的行就停，只返回它之前的行。读到的行的号和 member 里的号对不上，也按读不到处理（索引残留）。
   - 读不到可能是取行落到了更落后的副本，也可能是读完索引之后行被删了。两种都不跳过，下次再读：前一种下次能读到；后一种下次的 ZRANGE 里就没有它了，这时才算确认了缺口。
6. 按 `row_format` 解码返回。

代价：一条 ZRANGE、一次 pipeline，都在副本上。

### 4.3 `CommitSeqRewound`

- 含义：号被复用了，游标那一号现在是另一行，也就是数据回退了（原稿 §4.5）。重试不会好，所以不继承 RaceCondition。
- 属性：`comp_name`、`group`、`seq`（即 `after`）、`expected_id`（即 `after_id`）、`found_id`。
- 定义在 `base.py`，从 `hetu.data.backend` 导出。

## 5. 实现

### 5.1 共用部分

- `seq_read` 写在 `RedisModelClient`（`redis_model.py`），两个后端共用：只用到 `_zrange_members` 和 `hgetall_many_` 两个原语，SQLite 早就实现了它们。
- 计数器 key 用 `seq_counter_key_(cluster_prefix, comp_cls, group_prefix_of_(...))` 算，和提交时用的是同一个函数，不会算偏。
- `BackendClient`（`base.py`）加抽象方法 `seq_heads(table_ref, groups)`、`seq_read(table_ref, after, limit, *, group, after_id, row_format)`；`seq_head` 是 `seq_heads` 的单个版本，写在基类。

### 5.2 Redis（`redis/client.py`）

- `seq_heads` 用 MGET。同一张表、同一个簇的计数器 key 都在 `{CLU<id>}` 下，属于同一个 slot，cluster 下一条 MGET 就能读完；MGET 是读命令，cluster 下按 `ROUND_ROBIN_REPLICAS` 走副本。
- 组很多时按块（同 `RANGE_PIPELINE_CHUNK`）拆成几条 MGET，放在一个 pipeline 里，一次往返。

### 5.3 SQLite（`sqlite/store.py`、`sqlite/client.py`）

- `store.py` 加 `kv_get_int_many(keys) -> list[int | None]`，一次读完；`seq_heads` 用 `run_` 调它，结果里有 None 就抛错。

### 5.4 其他

- `table.py`：三个属性，照 `servant_get` 的写法，用 `bind_first_arg_with_typehint` 绑到 `self.backend.servant` 的方法上。
- 守门：`READ_METHODS`（`tests/test_arch_master_reads.py`、`tools/master_read_audit.py`）加上 `seq_head`、`seq_heads`、`seq_read`。以后有人在 master 上调它们会被拦住，摸底时也能数到。
- `headless.py`：`HeadlessClient` 的 docstring 列出这三个方法。

## 6. 消费方的写法（写进文档）

```python
from hetu.data.backend import CommitSeqRewound

tbl = client.table(ChangeLog)
cursors = load_snapshot()  # {owner: (seq, row_id)}，和消费方自己的状态存在同一个快照里


async def poll(owner: int) -> None:
    seq, row_id = cursors.get(owner, (0, None))
    rows = await tbl.servant_seq_read(
        seq, limit=1000, group={"owner": owner}, after_id=row_id
    )
    for row in rows:
        handle(row)  # 中间没出现的号都已确认不存在
    if len(rows):
        cursors[owner] = (int(rows[-1].seq), int(rows[-1].id))


# 组多时先读计数器，只读有新号的组：一条 MGET
heads = await tbl.servant_seq_heads([{"owner": o} for o in owners])
for owner, head in zip(owners, heads):
    if head > cursors.get(owner, (0, None))[0]:
        await poll(owner)
```

**注意：裁剪只能删到快照里存的游标为止，取所有消费方的最小值。**

- `servant_seq_read` 遇到"这一号没有行、后面有更大的号"，就认定这一号已经不在了，直接跳过，不报错（§3.1）。它分不清"处理过了所以删掉"和"还没处理就被删了"。
- 消费方从快照文件恢复。如果裁剪按消费方运行时的进度来删，而快照比这个进度旧：崩溃重启后，快照到运行时进度之间的命令已经被删了，会被当成缺口跳过，消费方的状态就悄悄对不上了。
- 所以裁剪的上界是**所有消费方快照里存下的游标的最小值**，不是内存里的进度。裁剪掉锚点那一行本身不影响核对：雪花 id 记在消费方那边。

核对结果：

| 情况 | 说明 | 处理 |
|---|---|---|
| 游标那一号还是那一行 | 正常 | 接着往后读（`servant_seq_read` 自动处理） |
| 游标那一号换了一行 | 号被复用，数据回退 | 抛 `CommitSeqRewound`：停下这一组，报警 |
| 游标那一号没了，后面有行 | 游标那一行被删了（比如裁剪） | 照常往后读 |
| 什么都没读到 | 没有新命令；或者这个副本落后；或者数据回退到了游标之前、号还没被复用 | 等。计数器持续小于游标、超过副本延迟（比如 30 秒）才报警 |

其他：

- **追上没有**：看这次读有没有返回行，不看游标是否等于计数器。最新的几行在消费之前就被删了的话，游标会停在最后见到的那一行，直到更新的一行出现；这期间计数器减游标大于 0，但没有可消费的东西。
- **快照**：游标（号和雪花 id）要和消费方自己的状态存在同一个快照里。崩溃后从快照恢复，会重放快照之后的命令：`handle` 要么幂等，要么状态和游标一起恢复。
- **长时间停在同一号**：同一组每次都没有进展、计数器又大于游标，多半是索引残留（索引里有、行没有，比如维护脚本只删了行），跑 `hetu upgrade` 重建索引。
- **易失表**：每次维护后号从 1 重来，游标要跟着清；没清的话，会按上表"什么都没读到"或 `CommitSeqRewound` 处理。

## 7. 不采用的做法

- **用只读 Lua 或 MULTI，把计数器和索引放进同一个快照**：好处是游标能直接推进到计数器，尾部被删时也能确认。但读写分离代理一般默认把 Lua 和事务发给 master（比如阿里云要开 `readonly_lua_route_ronode_enable` 才把只读 Lua 转给只读节点），每次轮询都占 master，换个代理也不一定能用。§3.1 的规则不需要原子地读两个 key，就能确认 n 号之前的缺口，代价只是尾部被删时游标停在最后见到的那一行。
- **headless 开事务，借提交时的 CNT 确认**：只读事务不提交，要假写一笔，每次确认都是一次 master 提交。
- **用计数器把游标推到 head**：见 §3.3。
- **改 `servant_range`，读不到的行不跳过**：会改变现有用法的结果（比如命令队列按 `created_at` 轮询），而且 `servant_range` 不知道号，还是没法核对锚点。
- **把计数器存进组 zset，当一个特殊的 member**：一条 ZRANGE 就能同时拿到，但每次提交多两条命令，而且所有读这个索引的路径（range、订阅、CNT 计数）都要过滤它。

## 8. 已知限制

- **被删的命令一律当缺口跳过**：分不清处理过了才删、还是没处理就被删了。裁剪按快照里的游标来（§6 的注意）；游戏逻辑里不要删还没消费的命令，要撤销就另写一条"撤销"命令。
- 尾部被删时，游标停在最后见到的那一行（§6）。
- 回退了、号还没被复用时，锚点发现不了，只能靠"计数器持续小于游标"报警，有时间窗。
- 计数器来自落后的副本时会短暂小于游标，所以报警要有时间窗。
- 索引残留会让消费方停在那一号，靠"长时间没有进展"发现。

## 9. 测试

新文件 `tests/test_commit_seq_servant.py`，四个后端都跑。

- 计数器：
  - 没发过号是 0；insert 之后是最新的号；
  - 多组批量，顺序和参数一致；不分组的组件；
  - 易失表 flush 之后为 0；
  - 分组值少给、多给，或者给了不分组的组件：ValueError；组件没有 `commit_seq`：ValueError；
  - 计数器被写成非整数：RuntimeError；
  - 空列表不访问数据库。
- 增量读：
  - 从 0 读起、limit、升序、只读本组；
  - 删了中间的行：跳过；删了尾部的行：返回空，之后再 insert 一行，返回新行、跳过被删的号；
  - **取行读不到**（替换 servant 的 `hgetall_many_`，模拟落后的副本）：停在它前面，不跳过；恢复之后读到它。这是本设计的核心用例；
  - 读完索引、取行之前行被删：这次停住，下次跳过；
  - 锚点：对得上时不返回它；锚点被删了，照常往后；号被复用（把计数器拨回去、删掉行再 insert）抛 `CommitSeqRewound`，属性正确；
  - 读到的行的号和 member 对不上：停住；
  - 参数：`after_id` 配 `after=0`、负数、≥ 2^53、bool、`limit` ≤ 0、`ID_LIST`；
  - `row_format` 各种格式。
- 守门：`READ_METHODS` 加上新方法后，审计插件包住了它们（`test_audit_plugin_wraps_every_backend_read_method`）。
- 有独立副本的 redis / valkey：在真实副本上读，结果和 master 一致。

## 10. 文档

- `docs/zh/advanced.md`、`docs/en/advanced.md`：
  - "binlog 式同步"里第三方消费方那一条改成一小节：写法、核对表、追上 / 快照 / 长时间停住 / 易失表；
  - **"裁剪只能删到快照里存的游标为止"作为注意写在这一小节里**，内容同 §6；
  - headless 一节轮询命令队列的地方加一句：要连号、不漏的命令表，用 `commit_seq` 加 `servant_seq_read`。
- `hetu/llms.txt`、`hetu/skills/building-on-hetu/SKILL.md`：同步。
- `docs/api`：重新生成（`Table` 的三个新方法、`CommitSeqRewound`）。

## 11. 原稿要改的地方

- §4.4 的结论：库外的消费方不在事务里，改用一条 ZRANGE 的快照作证据，见本稿 §3。
- §4.5 第三方消费方那一条：接口和写法见本稿。
- §9 第三方消费方的核对表：作废，换成指向本稿 §6。原来那张表依赖读不到的计数器，而且会把临时漏读当成删除。
- §14.3：前提更正，命令表的消费方在库外，是 headless 第三方消费方。

## 12. 审核时请重点看

- §3.1 的规则在各种部署下是否都成立：显式 servants、原生 cluster 多副本、读写分离代理、SQLite。
- §4.2 第 5 步"读不到就停"，除了索引残留，会不会让消费方永久卡住。
- 锚点只看 member 是否足够（不取锚点那一行）。
- 批量 MGET 在 cluster 下的 slot 假设。
