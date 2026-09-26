"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com

`redis/commit_v2.lua` 的 Python 版：在一个 `BEGIN IMMEDIATE` 写事务里依次跑 checks → pushes →
通知，返回与 Lua 字节相同的串。

**逐段对应 `commit_v2.lua`，改一边必须改另一边。** `tests/test_backend_client.py` 里的检查码用例
两个后端共跑，守着两边一致。

通知按 Redis 产生通知的规则写进通知表（和数据同一个事务）：HSET 过的行、删掉的已有行发行频道
（keyspace）；ZADD 真加进了、ZREM 真删掉了 member 的索引发整个索引的频道；表频道、值频道照 Lua 的
PUBLISH。一个提交里同一频道只记一条（MQ 按频道合并，订阅方分不出来）。
"""

import time
from typing import Any

import msgpack

from .store import KEYSPACE_PREFIX, SQLiteStore


class _CheckFailed(Exception):
    """某条 check 没通过：回滚写事务，返回 Lua 同款的串"""

    def __init__(self, resp: bytes):
        super().__init__(resp)
        self.resp = resp


def _lua_str(value: Any) -> bytes:
    """Lua 的 tostring：HGET 读不到是 false"""
    if value is None or value is False:
        return b"false"
    if isinstance(value, bytes):
        return value
    return str(value).encode()


def _run_checks(store: SQLiteStore, checks: list, deleted: dict) -> bytes | None:
    """
    Phase 1: Checks。顺序即优先级：Python 把竞态类（VER、带 RACE 标记的 NX/UNIQ、区间的 CNT）排在
    确定性类前面，这里首个失败即返回，所以同时存在两类冲突时先报 RACE。
    """
    # 同一 payload 内两条 UNIQ 指向同一 (索引, 值) 的兜底（正常由本地 IdentityMap 拦住）
    seen_uniq: set[bytes] = set()
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
            # 检查 Key 不存在 (用于 Insert 主键) ["NX", key, code, label]
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
                # member 是 value\x00row_id，row_id 不含 0x00，故最后一个 0x00 即终止符
                row_id = res[0].rsplit(b"\x00", 1)[-1]
                # 唯一索引指向的行在本次事务中被删除了，则不算冲突
                if not deleted.get(row_id):
                    return code + b": Unique violation " + label
        elif op == b"CNT":
            # 检查区间行数 (range 读的防幻读校验) ["CNT", index_key, min, max, count, label]
            if store.zlexcount(check[1].decode(), check[2], check[3]) != check[4]:
                return b"RACE: Range changed " + check[5]
    return None


def _run_pushes(store: SQLiteStore, pushes: list) -> dict[str, bytes | None]:
    """Phase 2: 批量写入，返回按 Redis keyspace 规则要发的频道（有序去重）"""
    channels: dict[str, bytes | None] = {}

    def notify(key: str) -> None:
        channels.setdefault(KEYSPACE_PREFIX + key, None)

    for cmd in pushes:
        op = cmd[0]
        if op == b"HSET":
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
            raise ValueError(f"未知的提交命令：{op!r}")
    return channels


def run_commit(
    store: SQLiteStore, packed: bytes, cleanup_before: float | None = None
) -> bytes:
    """
    执行一次提交。packed 是 `RedisModelClient.build_commit_payload_` 的 payload 经 msgpack 打包，
    结构: [ [checks...], [pushes...], {deleted...}, [table_pubs...], [value_chans...] ]；按
    `raw=True` 解开，和 Lua 里一样全是字节串，末尾缺的元素按空处理（同 Lua 的 nil）。
    cleanup_before 不为 None 时，顺手清掉这之前的通知（和提交同一个事务）。
    """
    payload: list = list(msgpack.unpackb(packed, raw=True))
    payload += [None] * (5 - len(payload))
    checks: list = payload[0] or []
    pushes: list = payload[1] or []
    deleted: dict = payload[2] or {}
    table_pubs: list = payload[3] or []
    value_chans: list = payload[4] or []
    try:
        with store.write_txn():
            if (failure := _run_checks(store, checks, deleted)) is not None:
                raise _CheckFailed(failure)
            channels = _run_pushes(store, pushes)
            # Phase 3: 表频道（payload 是 msgpack 的 row_id 列表）/ 索引值频道（无 payload）
            for channel, message in table_pubs:
                channels.setdefault(channel.decode(), bytes(message))
            for channel in value_chans:
                channels.setdefault(channel.decode(), None)
            now = time.time()
            if channels:
                store.notify_insert(channels.items(), now)
            if cleanup_before is not None:
                store.notify_cleanup(cleanup_before)
    except _CheckFailed as failed:
        return failed.resp
    return b"committed"
