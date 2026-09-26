"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com

SQLite 上模拟 HeTu 用到的那一小撮 Redis 存储原语（设计见
docs/superpowers/specs/2026-09-26-sqlite-backend-design.md §4.3）：

- hash（行）：每个 `cluster_prefix` 一张行表，表名就是它，`_hetu_key` 是行 key 里的 id，每个字段一列。
  列不声明类型，值照 Redis 存字节：合法 UTF-8 存 TEXT（DB 工具里直接可读），否则存 BLOB；NULL 表示
  hash 里没有这个字段。
- lex zset（索引）：全局一张 `__hetu_zset(key, member)`，member 与 Redis 字节相同；SQLite 的 BLOB 比较是
  memcmp 再比长度，与 zset 的 lex 比较一致。
- 带过期的字符串（维护锁）：`__hetu_kv`。meta：`__hetu_meta`。通知：`__hetu_notify`。

本类的方法只在客户端的专用线程里调用（sqlite3 连接不跨线程），由调用方保证串行。写事务用
`write_txn()`，事务不跨调用。
"""

import logging
import sqlite3
import time
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from itertools import batched
from pathlib import Path
from typing import Any

from ....i18n import _

logger = logging.getLogger("HeTu.root")

# 库文件标识：application_id 是 HeTu 的魔数（ASCII "HeTu"），user_version 是格式版本
APPLICATION_ID = 0x48655475
FORMAT_VERSION = 1
# 旧 SQL 后端（SQLAlchemy）建的 meta 表，认出来就提示删库重建
LEGACY_META_TABLE = "_HeTu_Component_Meta"

# 内部表一律 __hetu_ 前缀：SQLite 表名不分大小写，单下划线会和旧后端的 _Hetu_Notify 撞名
ZSET_TABLE = "__hetu_zset"
META_TABLE = "__hetu_meta"
KV_TABLE = "__hetu_kv"
NOTIFY_TABLE = "__hetu_notify"
# 行表的主键列：行 key（…:id:<id>）里的 id
ROW_KEY = "_hetu_key"
# keyspace 通知频道的前缀，SQLite 后端的 db 号固定 0（见 RedisModelClient.dbi）
KEYSPACE_PREFIX = "__keyspace@0__:"
# 单条 IN 查询的参数上限
IN_CHUNK = 500


def quote(name: str) -> str:
    """SQL 标识符加引号（表名里有 `:`、`{` 这类字符）"""
    return '"' + name.replace('"', '""') + '"'


def split_row_key(key: str) -> tuple[str, int]:
    """行 key（`{cluster_prefix}:id:<id>`）→ (行表名, id)"""
    table, sep, row_id = key.rpartition(":id:")
    if not sep:
        raise ValueError(_("不是行 key：{key}").format(key=key))
    return table, int(row_id)


def encode_value(value: Any) -> bytes:
    """值 → 存进 Redis 的字节，规则同 redis-py 的 Encoder：int / float 用 repr，str 用 UTF-8"""
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    if isinstance(value, bool):
        raise TypeError(_("不能直接写入 bool，请先转成 int / str"))
    if isinstance(value, (int, float)):
        return repr(value).encode()
    if isinstance(value, str):
        return value.encode("utf-8")
    raise TypeError(
        _("不能写入类型为 {type_name} 的值").format(type_name=type(value).__name__)
    )


def to_column(value: bytes) -> str | bytes:
    """存进行表的列值：合法 UTF-8 存成 TEXT（方便看），否则原样存 BLOB。读回时一律还原成字节"""
    try:
        return value.decode("utf-8")
    except UnicodeDecodeError:
        return value


def from_column(value: Any) -> bytes:
    """行表里读出的列值 → Redis 的字节（`to_column` 的逆；数字是外部工具写的，按 repr）"""
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf-8")
    return repr(value).encode()


def _is_missing_table(exc: sqlite3.OperationalError) -> bool:
    return "no such table" in str(exc)


def _is_missing_column(exc: sqlite3.OperationalError) -> bool:
    return "no such column" in str(exc)


def _lex_where(lo: bytes, hi: bytes) -> tuple[str, list[bytes]]:
    """
    ZRANGE BYLEX 的两端 → member 上的条件（照 Redis 解析：`[` 闭、`(` 开、`-` / `+` 无界）。
    lo 是下界、hi 是上界（REV 时调用方已经换过来）。
    """
    clauses: list[str] = []
    params: list[bytes] = []
    for bound, is_min in ((lo, True), (hi, False)):
        if bound == b"-":
            if not is_min:
                return " AND 0", []  # 上界是 -inf：空
            continue
        if bound == b"+":
            if is_min:
                return " AND 0", []  # 下界是 +inf：空
            continue
        head, value = bound[:1], bound[1:]
        if head == b"[":
            op = ">=" if is_min else "<="
        elif head == b"(":
            op = ">" if is_min else "<"
        else:
            raise ValueError("min or max not valid string range item")
        clauses.append(f" AND member {op} ?")
        params.append(value)
    return "".join(clauses), params


class SQLiteStore:
    """一条 sqlite3 连接上的存储原语，见模块说明"""

    def __init__(self, path: str, busy_timeout_ms: int = 5000):
        self.path = path
        # 已确认存在（名字精确匹配）的行表，只作加速：别的进程删了表时读写会报 no such table
        self._known_tables: set[str] = set()
        # 自动提交模式：事务都由我们显式 BEGIN（驱动不会在写语句前偷偷开事务）
        self.conn = sqlite3.connect(path, isolation_level=None)
        try:
            self.conn.execute(f"PRAGMA busy_timeout={int(busy_timeout_ms)}")
            mode = self.conn.execute("PRAGMA journal_mode=WAL").fetchone()[0]
            if str(mode).lower() != "wal":
                logger.warning(
                    _(
                        "⚠️ [💾SQLite] {path} 开不了 WAL（当前 {mode}），读会被写挡住。"
                        "库文件别放在网络文件系统上"
                    ).format(path=path, mode=mode)
                )
            self.conn.execute("PRAGMA synchronous=NORMAL")
            self._check_format()
        except BaseException:
            self.conn.close()
            raise

    def close(self) -> None:
        self.conn.close()

    # ============ 库文件 ============

    def _check_format(self) -> None:
        conn = self.conn
        app_id = conn.execute("PRAGMA application_id").fetchone()[0]
        if app_id == APPLICATION_ID:
            version = conn.execute("PRAGMA user_version").fetchone()[0]
            if version != FORMAT_VERSION:
                raise ValueError(
                    _(
                        "{path} 的格式版本是 {version}，本版 HeTu 用的是 {expected}："
                        "开发数据请删掉重建，或换个文件名"
                    ).format(path=self.path, version=version, expected=FORMAT_VERSION)
                )
            return
        tables = {
            str(row[0]).lower()
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        if LEGACY_META_TABLE.lower() in tables:
            raise ValueError(
                _(
                    "{path} 是旧 SQL 后端建的库文件，新的 SQLite 后端不兼容："
                    "开发数据请删掉重建，或换个文件名"
                ).format(path=self.path)
            )
        if app_id != 0 or tables:
            raise ValueError(
                _("{path} 不是 HeTu 的库文件，不会往里写：请换个文件名").format(
                    path=self.path
                )
            )
        self._init_format()

    def _init_format(self) -> None:
        """空库：建内部表、写标识。多个进程同时初始化时由写锁排队，后到的发现已经建好就跳过"""
        with self.write_txn():
            if self.conn.execute("PRAGMA application_id").fetchone()[0]:
                return
            self.conn.execute(
                f"CREATE TABLE IF NOT EXISTS {quote(ZSET_TABLE)} ("
                "key TEXT NOT NULL, member BLOB NOT NULL, PRIMARY KEY (key, member)"
                ") WITHOUT ROWID"
            )
            self.conn.execute(
                f"CREATE TABLE IF NOT EXISTS {quote(META_TABLE)} ("
                "instance TEXT NOT NULL, comp TEXT NOT NULL, json TEXT NOT NULL, "
                "version TEXT NOT NULL, cluster_id INTEGER NOT NULL, "
                "PRIMARY KEY (instance, comp))"
            )
            self.conn.execute(
                f"CREATE TABLE IF NOT EXISTS {quote(KV_TABLE)} ("
                "key TEXT PRIMARY KEY, value BLOB NOT NULL, expire_at REAL)"
            )
            # AUTOINCREMENT：清理之后 id 也不回退，游标与水位才不会错乱
            self.conn.execute(
                f"CREATE TABLE IF NOT EXISTS {quote(NOTIFY_TABLE)} ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, channel TEXT NOT NULL, "
                "payload BLOB, created_at REAL NOT NULL)"
            )
            self.conn.execute(
                f"CREATE INDEX IF NOT EXISTS {quote(NOTIFY_TABLE + '_created')} "
                f"ON {quote(NOTIFY_TABLE)} (created_at)"
            )
            self.conn.execute(f"PRAGMA application_id={APPLICATION_ID}")
            self.conn.execute(f"PRAGMA user_version={FORMAT_VERSION}")

    # ============ 事务 ============

    @contextmanager
    def write_txn(self) -> Iterator[None]:
        """
        写事务：`BEGIN IMMEDIATE` 一开始就拿写锁（别的进程的写者按 busy_timeout 等），先拿锁再读，
        读到的就是最新提交。异常时回滚。
        """
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield
            conn.execute("COMMIT")
        except BaseException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise

    @contextmanager
    def read_txn(self) -> Iterator[None]:
        """读事务：几条查询看到同一个快照（WAL 下不挡写）"""
        conn = self.conn
        conn.execute("BEGIN")
        try:
            yield
        finally:
            if conn.in_transaction:
                conn.execute("COMMIT")

    # ============ 行表（hash） ============

    def _table_exists(self, table: str, trust_cache: bool = True) -> bool:
        """
        行表在不在（按名字精确匹配）。SQLite 表名不分大小写、Redis key 分：已有只差大小写的另一张
        表时报错，而不是静默共用一张表。写入前传 trust_cache=False，别的进程可能刚删了表。
        """
        if trust_cache and table in self._known_tables:
            return True
        row = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name = ? COLLATE NOCASE",
            (table,),
        ).fetchone()
        if row is None:
            self._known_tables.discard(table)
            return False
        if row[0] != table:
            raise ValueError(
                _(
                    "SQLite 表名不区分大小写：{table} 和已有的 {existing} 只差大小写，"
                    "请给组件（或实例）换个名字"
                ).format(table=table, existing=row[0])
            )
        self._known_tables.add(table)
        return True

    def _columns(self, table: str) -> list[str]:
        return [
            str(row[1])
            for row in self.conn.execute(f"PRAGMA table_info({quote(table)})")
        ]

    @staticmethod
    def _column_order(fields: Iterable[str]) -> list[str]:
        """建表时的列序：id 在前、_version 在后，其余照给的顺序"""
        fields = list(dict.fromkeys(fields))
        rest = [f for f in fields if f not in ("id", "_version")]
        head = ["id"] if "id" in fields else []
        tail = ["_version"] if "_version" in fields else []
        return head + rest + tail

    def _ddl(self, sql: str, table: str) -> None:
        try:
            self.conn.execute(sql)
        except sqlite3.OperationalError as exc:
            if "duplicate column" in str(exc):
                raise ValueError(
                    _(
                        "SQLite 列名不区分大小写：{table} 里有只差大小写的字段名（{err}），"
                        "请改名"
                    ).format(table=table, err=exc)
                ) from exc
            raise

    def ensure_row_table(self, table: str, fields: Iterable[str]) -> None:
        """
        保证行表存在、并且有这些列（Redis 的 hash 字段不受限）。只能在写事务里调用：建表、加列要和
        写入在同一个事务里。
        """
        fields = list(fields)
        if not self._table_exists(table, trust_cache=False):
            cols = ", ".join(quote(c) for c in self._column_order(fields))
            self._ddl(
                f"CREATE TABLE {quote(table)} ({quote(ROW_KEY)} INTEGER PRIMARY KEY, {cols})",
                table,
            )
            self._known_tables.add(table)
            return
        existing = set(self._columns(table))
        for field in fields:
            if field not in existing:
                self._ddl(
                    f"ALTER TABLE {quote(table)} ADD COLUMN {quote(field)}", table
                )
                existing.add(field)

    def hset(
        self, key: str, mapping: dict[str, Any], schema: Iterable[str] | None = None
    ) -> None:
        """
        HSET：只写给出的字段，行不存在就建（和 Redis 一样，缺行时写出的是只有这几个字段的残缺行）。
        schema 是建表时的列（组件的全部字段），不给就按 mapping。只能在写事务里调用。
        """
        table, row_id = split_row_key(key)
        self.ensure_row_table(table, [*(schema or ()), *mapping])
        cols = list(mapping)
        values = [to_column(encode_value(mapping[c])) for c in cols]
        col_sql = ", ".join(quote(c) for c in cols)
        holders = ", ".join("?" for _c in cols)
        updates = ", ".join(f"{quote(c)}=excluded.{quote(c)}" for c in cols)
        self.conn.execute(
            f"INSERT INTO {quote(table)} ({quote(ROW_KEY)}, {col_sql}) "
            f"VALUES (?, {holders}) "
            f"ON CONFLICT({quote(ROW_KEY)}) DO UPDATE SET {updates}",
            (row_id, *values),
        )

    def hset_txn(
        self, key: str, mapping: dict[str, Any], schema: Iterable[str] | None = None
    ) -> None:
        """HSET 自带写事务（direct_set、维护接口用）"""
        with self.write_txn():
            self.hset(key, mapping, schema)

    def select_rows(
        self, table: str, row_ids: Iterable[int]
    ) -> dict[int, dict[bytes, bytes]]:
        """按 id 读行，返回 {id: HGETALL 形状的行}；读不到的 id 不在里面，表不存在当空"""
        found: dict[int, dict[bytes, bytes]] = {}
        ids = list(dict.fromkeys(int(i) for i in row_ids))
        if not ids or not self._table_exists(table):
            return found
        for chunk in batched(ids, IN_CHUNK):
            holders = ", ".join("?" for _i in chunk)
            try:
                cur = self.conn.execute(
                    f"SELECT * FROM {quote(table)} WHERE {quote(ROW_KEY)} IN ({holders})",
                    chunk,
                )
            except sqlite3.OperationalError as exc:
                if _is_missing_table(exc):  # 别的进程刚删了表
                    self._known_tables.discard(table)
                    return found
                raise
            names = [str(d[0]) for d in cur.description]
            key_pos = names.index(ROW_KEY)
            fields = [
                (i, name.encode()) for i, name in enumerate(names) if name != ROW_KEY
            ]
            for row in cur:
                data = {
                    field: from_column(row[i])
                    for i, field in fields
                    if row[i] is not None
                }
                if data:  # 没有任何字段的 hash 在 Redis 里就是不存在
                    found[int(row[key_pos])] = data
        return found

    def hgetall(self, key: str) -> dict[bytes, bytes]:
        """HGETALL：行不存在返回空 dict"""
        table, row_id = split_row_key(key)
        return self.select_rows(table, (row_id,)).get(row_id, {})

    def hgetall_many(self, table: str, row_ids: list[int]) -> list[dict[bytes, bytes]]:
        """按 row_ids 的顺序批量 HGETALL，不存在的为空 dict"""
        found = self.select_rows(table, row_ids)
        return [found.get(int(i), {}) for i in row_ids]

    def hget(self, key: str, field: str) -> bytes | None:
        table, row_id = split_row_key(key)
        if not self._table_exists(table):
            return None
        try:
            row = self.conn.execute(
                f"SELECT {quote(field)} FROM {quote(table)} WHERE {quote(ROW_KEY)} = ?",
                (row_id,),
            ).fetchone()
        except sqlite3.OperationalError as exc:
            if _is_missing_column(exc):
                return None
            if _is_missing_table(exc):
                self._known_tables.discard(table)
                return None
            raise
        if row is None or row[0] is None:
            return None
        return from_column(row[0])

    def exists(self, key: str) -> bool:
        return bool(self.hgetall(key))

    def delete_row(self, key: str) -> bool:
        """DEL 行 key，返回行原来在不在"""
        table, row_id = split_row_key(key)
        if not self._table_exists(table, trust_cache=False):
            return False
        cur = self.conn.execute(
            f"DELETE FROM {quote(table)} WHERE {quote(ROW_KEY)} = ?", (row_id,)
        )
        return cur.rowcount > 0

    def delete_row_txn(self, key: str) -> bool:
        with self.write_txn():
            return self.delete_row(key)

    def row_ids(self, table: str) -> list[int]:
        """表里所有行的 id（Redis 的 KEYS …:id:*）"""
        if not self._table_exists(table):
            return []
        try:
            rows = self.conn.execute(
                f"SELECT {quote(ROW_KEY)} FROM {quote(table)}"
            ).fetchall()
        except sqlite3.OperationalError as exc:
            if _is_missing_table(exc):
                self._known_tables.discard(table)
                return []
            raise
        return [int(row[0]) for row in rows]

    def row_field_values(
        self, table: str, field: str
    ) -> list[tuple[int, bytes | None]]:
        """每一行的 id 与某个字段的原始字节（没有这个字段为 None），重建索引用"""
        if not self._table_exists(table):
            return []
        try:
            rows = self.conn.execute(
                f"SELECT {quote(ROW_KEY)}, {quote(field)} FROM {quote(table)}"
            ).fetchall()
        except sqlite3.OperationalError as exc:
            if _is_missing_column(exc):
                return [(row_id, None) for row_id in self.row_ids(table)]
            raise
        return [
            (int(row_id), None if value is None else from_column(value))
            for row_id, value in rows
        ]

    def row_tables_with_prefix(self, prefix: str) -> list[str]:
        """名字以 prefix 开头（大小写精确匹配）的行表"""
        names = [
            str(row[0])
            for row in self.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        ]
        return [name for name in names if name.startswith(prefix)]

    def count_rows(self, table: str) -> int:
        if not self._table_exists(table, trust_cache=False):
            return 0
        return int(
            self.conn.execute(f"SELECT count(*) FROM {quote(table)}").fetchone()[0]
        )

    def drop_row_table(self, table: str) -> None:
        """只能在写事务里调用"""
        self.conn.execute(f"DROP TABLE IF EXISTS {quote(table)}")
        self._known_tables.discard(table)

    def rename_row_table(self, old: str, new: str) -> None:
        """改行表名，目标表已存在就先删（同 Redis RESTORE 的 REPLACE）。只能在写事务里调用"""
        if not self._table_exists(old, trust_cache=False):
            return
        if self._table_exists(new, trust_cache=False):
            self.drop_row_table(new)
        self.conn.execute(f"ALTER TABLE {quote(old)} RENAME TO {quote(new)}")
        self._known_tables.discard(old)
        self._known_tables.add(new)

    # ============ lex zset（索引） ============

    def zadd(self, key: str, member: bytes) -> bool:
        """ZADD（score 恒 0），返回 member 是不是新加的（Redis 只在 zset 真变了时发通知）"""
        cur = self.conn.execute(
            f"INSERT OR IGNORE INTO {quote(ZSET_TABLE)} (key, member) VALUES (?, ?)",
            (key, bytes(member)),
        )
        return cur.rowcount > 0

    def zrem(self, key: str, member: bytes) -> bool:
        """ZREM，返回 member 原来在不在"""
        cur = self.conn.execute(
            f"DELETE FROM {quote(ZSET_TABLE)} WHERE key = ? AND member = ?",
            (key, bytes(member)),
        )
        return cur.rowcount > 0

    def zrange_bylex(
        self,
        key: str,
        start: bytes,
        end: bytes,
        desc: bool = False,
        offset: int = 0,
        num: int = -1,
    ) -> list[bytes]:
        """
        `ZRANGE key start end BYLEX [REV] LIMIT offset num`：REV 时 start 是上界、end 是下界（同
        Redis）；num 为负数表示不限。
        """
        lo, hi = (end, start) if desc else (start, end)
        where, params = _lex_where(bytes(lo), bytes(hi))
        order = "DESC" if desc else "ASC"
        rows = self.conn.execute(
            f"SELECT member FROM {quote(ZSET_TABLE)} WHERE key = ?{where} "
            f"ORDER BY member {order} LIMIT ? OFFSET ?",
            (key, *params, int(num), int(offset)),
        ).fetchall()
        return [bytes(row[0]) for row in rows]

    def zlexcount(self, key: str, lo: bytes, hi: bytes) -> int:
        """`ZLEXCOUNT key min max`"""
        where, params = _lex_where(bytes(lo), bytes(hi))
        row = self.conn.execute(
            f"SELECT count(*) FROM {quote(ZSET_TABLE)} WHERE key = ?{where}",
            (key, *params),
        ).fetchone()
        return int(row[0])

    def zmembers(self, key: str) -> list[bytes]:
        """zset 的全部 member，按顺序"""
        return self.zrange_bylex(key, b"-", b"+")

    def zreplace(self, key: str, members: Iterable[bytes]) -> None:
        """整个替换一个 zset（重建索引用）。只能在写事务里调用"""
        self.conn.execute(f"DELETE FROM {quote(ZSET_TABLE)} WHERE key = ?", (key,))
        self.conn.executemany(
            f"INSERT OR IGNORE INTO {quote(ZSET_TABLE)} (key, member) VALUES (?, ?)",
            [(key, bytes(m)) for m in members],
        )

    def zkeys_with_prefix(self, prefix: str) -> list[str]:
        rows = self.conn.execute(
            f"SELECT DISTINCT key FROM {quote(ZSET_TABLE)} WHERE substr(key, 1, ?) = ?",
            (len(prefix), prefix),
        ).fetchall()
        return [str(row[0]) for row in rows]

    def zdelete_prefix(self, prefix: str) -> int:
        """删掉 key 以 prefix 开头的所有 zset，返回删掉的 key 数。只能在写事务里调用"""
        count = len(self.zkeys_with_prefix(prefix))
        self.conn.execute(
            f"DELETE FROM {quote(ZSET_TABLE)} WHERE substr(key, 1, ?) = ?",
            (len(prefix), prefix),
        )
        return count

    def zrename_prefix(self, old: str, new: str) -> None:
        """key 的前缀 old 换成 new，目标 key 已存在的先删掉（同 RESTORE REPLACE）。只能在写事务里调用"""
        for key in self.zkeys_with_prefix(old):
            target = new + key[len(old) :]
            self.conn.execute(
                f"DELETE FROM {quote(ZSET_TABLE)} WHERE key = ?", (target,)
            )
            self.conn.execute(
                f"UPDATE {quote(ZSET_TABLE)} SET key = ? WHERE key = ?", (target, key)
            )

    # ============ 带过期的字符串（维护锁） ============

    def kv_set_nx(self, key: str, value: bytes, ttl: float | None) -> bool:
        """`SET key value NX [EX ttl]`，返回设没设上"""
        now = time.time()
        with self.write_txn():
            self.conn.execute(
                f"DELETE FROM {quote(KV_TABLE)} WHERE key = ? AND expire_at <= ?",
                (key, now),
            )
            cur = self.conn.execute(
                f"INSERT OR IGNORE INTO {quote(KV_TABLE)} (key, value, expire_at) "
                "VALUES (?, ?, ?)",
                (key, value, None if ttl is None else now + ttl),
            )
            return cur.rowcount > 0

    def kv_get(self, key: str) -> bytes | None:
        row = self.conn.execute(
            f"SELECT value, expire_at FROM {quote(KV_TABLE)} WHERE key = ?", (key,)
        ).fetchone()
        if row is None or (row[1] is not None and row[1] <= time.time()):
            return None
        return bytes(row[0])

    def kv_delete_if(self, key: str, value: bytes) -> bool:
        """值还是 value（且没过期）才删，返回删没删（同 redis-py Lock 的释放）"""
        with self.write_txn():
            if self.kv_get(key) != value:
                return False
            self.conn.execute(f"DELETE FROM {quote(KV_TABLE)} WHERE key = ?", (key,))
            return True

    # ============ meta ============

    def meta_get(self, instance: str, comp: str) -> tuple[str, str, int] | None:
        """(json, version, cluster_id)"""
        row = self.conn.execute(
            f"SELECT json, version, cluster_id FROM {quote(META_TABLE)} "
            "WHERE instance = ? AND comp = ?",
            (instance, comp),
        ).fetchone()
        return None if row is None else (str(row[0]), str(row[1]), int(row[2]))

    def meta_set(
        self, instance: str, comp: str, json: str, version: str, cluster_id: int
    ) -> None:
        self.conn.execute(
            f"INSERT OR REPLACE INTO {quote(META_TABLE)} "
            "(instance, comp, json, version, cluster_id) VALUES (?, ?, ?, ?, ?)",
            (instance, comp, json, version, int(cluster_id)),
        )

    def meta_delete(self, instance: str, comp: str) -> int:
        cur = self.conn.execute(
            f"DELETE FROM {quote(META_TABLE)} WHERE instance = ? AND comp = ?",
            (instance, comp),
        )
        return cur.rowcount

    # ============ 通知 ============

    def notify_insert(
        self, rows: Iterable[tuple[str, bytes | None]], now: float
    ) -> None:
        """写通知（和数据同一个写事务）"""
        self.conn.executemany(
            f"INSERT INTO {quote(NOTIFY_TABLE)} (channel, payload, created_at) "
            "VALUES (?, ?, ?)",
            [(channel, payload, now) for channel, payload in rows],
        )

    def notify_tail(self) -> int:
        """发过的最后一条通知的 id（清理后也不回退），订阅的水位"""
        row = self.conn.execute(
            "SELECT seq FROM sqlite_sequence WHERE name = ?", (NOTIFY_TABLE,)
        ).fetchone()
        return 0 if row is None else int(row[0])

    def notify_fetch(
        self, after_id: int, channels: list[str] | None, limit: int
    ) -> tuple[list[tuple[int, str, bytes | None]], int | None, int]:
        """
        取游标之后的一批通知（channels 为 None 时不按频道过滤），连同表里现存最小的 id 与发过的最后
        一个 id。三者在同一个快照里读。
        """
        sql = f"SELECT id, channel, payload FROM {quote(NOTIFY_TABLE)} WHERE id > ?"
        params: list[Any] = [after_id]
        if channels is not None:
            sql += f" AND channel IN ({', '.join('?' for _c in channels)})"
            params += channels
        sql += " ORDER BY id LIMIT ?"
        params.append(limit)
        with self.read_txn():
            rows = [
                (int(r[0]), str(r[1]), None if r[2] is None else bytes(r[2]))
                for r in self.conn.execute(sql, params)
            ]
            min_id = self.conn.execute(
                f"SELECT min(id) FROM {quote(NOTIFY_TABLE)}"
            ).fetchone()[0]
            tail = self.notify_tail()
        return rows, (None if min_id is None else int(min_id)), tail

    def notify_cleanup(self, before: float) -> int:
        """
        删掉 before 之前的通知。按 id 删一个前缀（时间只用来找到那个 id），剩下的 id 才是连续的，
        轮询端据此判断自己有没有漏掉被清理的通知。只能在写事务里调用
        """
        cur = self.conn.execute(
            f"DELETE FROM {quote(NOTIFY_TABLE)} WHERE id <= ("
            f"SELECT max(id) FROM {quote(NOTIFY_TABLE)} WHERE created_at < ?)",
            (before,),
        )
        return cur.rowcount


def open_store(path: str, busy_timeout_ms: int) -> SQLiteStore:
    """打开库文件（没有就建，所在目录也一起建）"""
    parent = Path(path).parent
    if str(parent) not in ("", "."):
        parent.mkdir(parents=True, exist_ok=True)
    return SQLiteStore(path, busy_timeout_ms)
