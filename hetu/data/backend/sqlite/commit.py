"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com

`redis/commit_v2.lua` 的 Python 版：在一个 `BEGIN IMMEDIATE` 写事务里依次跑 checks → 发号
（提交序号列）→ pushes → 通知，返回与 Lua 字节相同的串。

和 Redis 不同的一处：Phase 2 写到一半出错时，SQLite 的写事务整个回滚，Redis 会留下已经执行的
写入（提交序号列在 Redis 上因此可能留下缺口，SQLite 不会）。

**逐段对应 `commit_v2.lua`，改一边必须改另一边。** `tests/test_backend_client.py` 里的检查码用例、
`tests/test_commit_seq.py` 里的发号用例两个后端共跑，守着两边一致。

通知按 Redis 产生通知的规则写进通知表（和数据同一个事务）：HSET 过的行、删掉的已有行发行频道
（keyspace）；ZADD 真加进了、ZREM 真删掉了 member 的索引发整个索引的频道；表频道、值频道照 Lua 的
PUBLISH。一个提交里同一频道只记一条（MQ 按频道合并，订阅方分不出来）。
"""

import time
from typing import Any

import msgpack
import numpy as np

from ....i18n import _
from ..base import COMMIT_SEQ_LIMIT, ROW_ID_TOKEN_LEN, to_sortable_bytes
from .store import KEYSPACE_PREFIX, SQLiteStore


def _lua_str(value: Any) -> bytes:
    """Lua 的 tostring：HGET 读不到是 false"""
    if value is None or value is False:
        return b"false"
    if isinstance(value, bytes):
        return value
    return str(value).encode()


def _run_checks(
    store: SQLiteStore, checks: list, deleted: dict, pushes: list
) -> bytes | None:
    """
    Phase 1: Checks。顺序即优先级：Python 把竞态类（VER、带 RACE 标记的 NX/UNIQ、区间的 CNT）排在
    确定性类前面，这里首个失败即返回，所以同时存在两类冲突时先报 RACE。
    """
    # 同一 payload 内两条 UNIQ 指向同一 (索引, 值) 的兜底（正常由本地 IdentityMap 拦住）
    seen_uniq: set[bytes] = set()
    # 本次 pushes 要 ZREM 的 (索引 key, member)，UNIQ 查到已有的 member 时才建（常见路径用不到）。
    # 键带索引 key：同一行两个 unique 列同值时 member 字节一样，只按 member 会把另一列一起豁免
    removed: set[bytes] | None = None
    for check in checks:
        op = check[0]
        if op == b"VER":
            # 检查版本号 (乐观锁) ["VER", key, expected_version]
            key, expected = check[1], check[2]
            current = store.hget(key.decode(), "_version")
            if current != expected:
                return (
                    b"RACE: Version mismatch "
                    + key
                    + b" exp:"
                    + _lua_str(expected)
                    + b" got:"
                    + _lua_str(current)
                )
        elif op == b"NX":
            # 检查 Key 不存在 (用于 Insert 主键，以及本事务按 id 读空过的行)
            # ["NX", key, code, label]
            if store.exists(check[1].decode()):
                return check[2] + b": Key already exists " + check[3]
        elif op == b"EX":
            # 检查 Key 存在 (用于 Update/Delete) ["EX", key]
            key = check[1]
            if not store.exists(key.decode()):
                return b"RACE: Key does not exist " + key
        elif op == b"UNIQ":
            # 检查唯一索引 ["UNIQ", index_key, start_val, end_val, code, label]
            idx_key, start_val, end_val, code, label = check[1:6]
            dup = idx_key + b"\x00" + start_val
            if dup in seen_uniq:
                return b"UNIQUE: Duplicate unique value within transaction " + label
            seen_uniq.add(dup)
            res = store.zrange_bylex(idx_key.decode(), start_val, end_val, False, 0, 1)
            if res:
                # member 是 值段\x00id 段，id 段定宽（ROW_ID_TOKEN_LEN），deleted 的键也是 id 段
                row_token = res[0][-ROW_ID_TOKEN_LEN:]
                # 唯一索引指向的行在本次事务中被删除了，则不算冲突
                if not deleted.get(row_token):
                    # 本次提交会把这个 member 移走（同一事务里把值挪到了别的行）也不算：被移走的
                    # 行都有 VER 钉住，库里它的 member 就是本次要 ZREM 的那个
                    if removed is None:
                        removed = {
                            cmd[1] + b"\x00" + member
                            for cmd in pushes
                            if cmd[0] == b"ZREM"
                            for member in cmd[2:]
                        }
                    if idx_key + b"\x00" + res[0] not in removed:
                        return code + b": Unique violation " + label
        elif op == b"CNT":
            # 检查区间行数 (range 读的防幻读校验) ["CNT", index_key, min, max, count, label]
            if store.zlexcount(check[1].decode(), check[2], check[3]) != check[4]:
                return b"RACE: Range changed " + check[5]
    return None


def _alloc_seqs(store: SQLiteStore, seq_allocs: list) -> list[tuple[int, int]] | bytes:
    """
    Phase 2 开头：提交序号列这次要发的号，返回每个计数器的 (第一个号, 发完之后的值)，顺序同
    seq_allocs。全成或全不发，同 Lua 的减回：先读出各计数器、检查完，全部通过才写；有一个不是
    整数、加了会溢出 int64，或者发出的号不在 [1, 2^53) 里，就返回失败串，什么都不写。

    不是整数、溢出 int64 时 Redis 是 INCRBY 报错，提交脚本原样返回它的错误（redis-py 抛
    ResponseError）；这里返回同样内容的 SEQ 失败串（抛 RuntimeError），报错信息对得上
    """
    pending: list[tuple[str, int, int]] = []  # (计数器 key, 发完之后的值, 第一个号)
    for counter_key, count in seq_allocs:
        current = store.kv_get_int(counter_key.decode())
        if current is None:
            return b"SEQ: ERR value is not an integer or out of range " + counter_key
        last = current + count
        if last >= 1 << 63:
            return b"SEQ: ERR increment or decrement would overflow " + counter_key
        if last >= COMMIT_SEQ_LIMIT or last < count:
            return b"SEQ: Counter out of range " + counter_key
        pending.append((counter_key.decode(), last, current + 1))
    for key, last, _first in pending:
        store.kv_set(key, b"%d" % last)
    return [(first, last) for _key, last, first in pending]


def _run_pushes(
    store: SQLiteStore, pushes: list, seq_next: dict[int, int]
) -> dict[str, bytes | None]:
    """Phase 2: 批量写入，返回按 Redis keyspace 规则要发的频道（有序去重）"""
    channels: dict[str, bytes | None] = {}

    def notify(key: str) -> None:
        channels.setdefault(KEYSPACE_PREFIX + key, None)

    for cmd in pushes:
        op = cmd[0]
        if op == b"HSETSEQ":
            # ["HSETSEQ", 下标, 组 key, P, id 段, 行 key, field, val, ..., 号的字段名, 占位]：
            # 号替换最后的占位写进行（同名字段取最后一个值，同 Redis），再把 P + 号的编码 +
            # 0x00 + id 段加进组 key。计数器不发通知：HeTu 给 Redis 开的 Kghz 不含 $
            slot, zset_key, prefix, row_token = cmd[1], cmd[2], cmd[3], cmd[4]
            n = seq_next[slot]
            seq_next[slot] = n + 1
            args = cmd[5:]
            args[-1] = b"%d" % n
            key = args[0].decode()
            mapping = {args[i].decode(): args[i + 1] for i in range(1, len(args), 2)}
            store.hset(key, mapping)
            notify(key)
            member = prefix + to_sortable_bytes(np.int64(n)) + b"\x00" + row_token
            if store.zadd(zset_key.decode(), member):
                notify(zset_key.decode())
        elif op == b"HSET":
            # ["HSET", key, field, val, ...]：HSET 总会产生 keyspace 事件
            key = cmd[1].decode()
            mapping = {cmd[i].decode(): cmd[i + 1] for i in range(2, len(cmd), 2)}
            store.hset(key, mapping)
            notify(key)
        elif op == b"ZADD":
            # ["ZADD", key, score, member, ...]：只有真加进了新 member 才有事件
            key = cmd[1].decode()
            added = [store.zadd(key, cmd[i + 1]) for i in range(2, len(cmd), 2)]
            if any(added):
                notify(key)
        elif op == b"ZREM":
            # ["ZREM", key, member, ...]：只有真删掉了 member 才有事件
            key = cmd[1].decode()
            removed = [store.zrem(key, member) for member in cmd[2:]]
            if any(removed):
                notify(key)
        elif op == b"DEL":
            # ["DEL", key]：key 原来存在才有事件
            key = cmd[1].decode()
            if store.delete_row(key):
                notify(key)
        else:
            raise ValueError(_("未知的提交命令：{op}").format(op=repr(op)))
    return channels


def run_commit(
    store: SQLiteStore, packed: bytes, cleanup_before: float | None = None
) -> bytes | list:
    """
    执行一次提交。packed 是 `RedisModelClient.build_commit_payload_` 的 payload 经 msgpack 打包，
    结构: [ [checks...], [pushes...], {deleted...}, [table_pubs...], [value_chans...],
    [seq_allocs...] ]；按 `raw=True` 解开，和 Lua 里一样全是字节串，末尾缺的元素按空处理（同 Lua
    的 nil）。cleanup_before 不为 None 时，顺手清掉这之前的通知（和提交同一个事务；校验不过时照样清）。

    返回值同 Lua：成功时没有号要发是 ``b"committed"``，有号要发是 ``[b"committed", 各计数器发完
    之后的值, ...]``（顺序同 seq_allocs）；失败时是失败串。
    """
    payload: list = list(msgpack.unpackb(packed, raw=True))
    payload += [None] * (6 - len(payload))
    checks: list = payload[0] or []
    pushes: list = payload[1] or []
    deleted: dict = payload[2] or {}
    table_pubs: list = payload[3] or []
    value_chans: list = payload[4] or []
    seq_allocs: list = payload[5] or []
    with store.write_txn():
        # 校验只读：没通过时什么都还没写，不用回滚
        failure = _run_checks(store, checks, deleted, pushes)
        seq_next: dict[int, int] = {}  # seq_allocs 的下标（从 1 起）→ 下一个要发的号
        seq_lasts: list[int] = []
        if failure is None and seq_allocs:
            allocated = _alloc_seqs(store, seq_allocs)
            if isinstance(allocated, bytes):
                failure = allocated
            else:
                seq_next = {
                    slot: first for slot, (first, _last) in enumerate(allocated, 1)
                }
                seq_lasts = [last for _first, last in allocated]
        if failure is None:
            channels = _run_pushes(store, pushes, seq_next)
            # Phase 3: 表频道（payload 是 msgpack 的 row_id 列表）/ 索引值频道（无 payload）
            for channel, message in table_pubs:
                channels.setdefault(channel.decode(), bytes(message))
            for channel in value_chans:
                channels.setdefault(channel.decode(), None)
            if channels:
                store.notify_insert(channels.items(), time.time())
        if cleanup_before is not None:
            store.notify_cleanup(cleanup_before)
    if failure is not None:
        return failure
    return [b"committed", *seq_lasts] if seq_allocs else b"committed"
