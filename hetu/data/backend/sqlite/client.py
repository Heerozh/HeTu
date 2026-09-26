"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import asyncio
import logging
import random
import sqlite3
import time
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any, Literal, cast, final, overload, override

import numpy as np

from ....i18n import _
from ..base import RowFormat, detach_rows_
from ..redis_model import RedisModelClient
from .commit import run_commit
from .store import KEYSPACE_PREFIX, SQLiteStore, open_store

if TYPE_CHECKING:
    from ...component import BaseComponent
    from ..idmap import RangeObservation
    from ..table import TableReference
    from .maint import SQLiteTableMaintenance
    from .mq import SQLiteMQClient, SQLiteNotifyHub

logger = logging.getLogger("HeTu.root")


@final
class SQLiteBackendClient(RedisModelClient, alias="sqlite"):
    """
    SQLite 开发后端：在 SQLite 上模拟 Redis 的数据模型（行表 / lex zset / commit 脚本 / keyspace
    通知），行为尽可能和 Redis 后端一致，好在开发期暴露问题；不考虑性能。key 布局、索引编码、
    commit payload 等纯逻辑与 Redis 共用（`RedisModelClient`），这里只有 I/O。

    每个客户端一条专用线程和一条 sqlite3 连接，所有操作在这条线程里串行执行；每个方法是一个 job，
    事务不跨 job，更不跨 await。每次调用都是一次真正的 await，协程的交错点和 Redis 一样。
    """

    # 写锁的等待上限（毫秒）：别的进程占着写锁时等这么久
    BUSY_TIMEOUT_MS = 5000
    # 通知保留多久、多久顺手清一次（带抖动，免得多个 worker 一起清）
    NOTIFY_TTL_SECONDS = 60 * 60
    NOTIFY_CLEANUP_INTERVAL = 60 * 15
    NOTIFY_CLEANUP_JITTER = 90.0
    # Redis 专用的配置项：`hetu init` 生成的配置里 SQLite 那一段也带着，忽略
    IGNORED_OPTIONS = frozenset({"raw_clustering", "max_connections", "pool_timeout"})

    @staticmethod
    def parse_dsn(dsn: Any) -> str:
        """
        `sqlite:///<路径>` → 路径：`sqlite:///./hetu.db` 相对当前目录，`sqlite:////abs/hetu.db`
        是绝对路径，Windows 可写 `sqlite:///C:/…`。不支持 `:memory:`（跨客户端、跨进程看不到同一份
        数据）。
        """
        prefix = "sqlite:///"
        if not isinstance(dsn, str) or not dsn.lower().startswith(prefix):
            raise ValueError(
                _(
                    "SQLite 后端的地址要写成 sqlite:///<库文件路径>，比如 "
                    "sqlite:///./hetu.db，收到：{dsn}"
                ).format(dsn=dsn)
            )
        path = dsn[len(prefix) :]
        if not path or path == ":memory:":
            raise ValueError(
                _("SQLite 后端不支持内存库，请给一个库文件路径：{dsn}").format(dsn=dsn)
            )
        return path

    @classmethod
    def check_config_(cls, config: dict) -> None:
        """SQLite 没有只读副本：servants 必须留空（留空时 Backend 会拿 master 的地址当 servant）"""
        if config.get("servants"):
            raise ValueError(
                _("SQLite 后端没有只读副本，servants 请留空：{servants}").format(
                    servants=config["servants"]
                )
            )

    def __init__(self, endpoint: str, is_servant, **kwargs):
        super().__init__(endpoint, is_servant)
        unknown = set(kwargs) - self.IGNORED_OPTIONS
        if unknown:
            raise TypeError(
                _("SQLite 后端不认识的配置项：{names}").format(names=sorted(unknown))
            )
        self.path = self.parse_dsn(endpoint)
        assert KEYSPACE_PREFIX == f"__keyspace@{self.dbi}__:"

        # 专用线程与连接：连接在这条线程里打开，之后的操作也都在这条线程里
        self._executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="hetu-sqlite"
        )
        try:
            self._store: SQLiteStore | None = self._executor.submit(
                open_store, self.path, self.BUSY_TIMEOUT_MS
            ).result()
        except sqlite3.Error as exc:
            self._executor.shutdown(wait=False)
            raise ConnectionError(
                _("无法打开SQLite库文件：{path}（{err}）").format(
                    path=self.path, err=exc
                )
            ) from exc
        except BaseException:
            self._executor.shutdown(wait=False)
            raise

        # 本进程共享的通知表轮询器，首次 get_mq_client 时在事件循环里懒建
        self._hub: SQLiteNotifyHub | None = None
        self._next_notify_cleanup_at = self._next_cleanup_time(time.time())

        # 同 Redis：客户端只能在一个事件循环里用（开发期就暴露跨 loop 使用）
        try:
            self.loop_id = hash(asyncio.get_running_loop())
        except RuntimeError:
            self.loop_id = 0

    def _next_cleanup_time(self, now: float) -> float:
        return (
            now
            + self.NOTIFY_CLEANUP_INTERVAL
            + random.uniform(0.0, self.NOTIFY_CLEANUP_JITTER)
        )

    def _ensure_open(self) -> SQLiteStore:
        if self._store is None:
            raise ConnectionError(_("连接已关闭，已调用过close"))
        return self._store

    async def run_(self, fn: Callable[..., Any], *args: Any) -> Any:
        """内部方法：在专用线程里执行 fn(store, *args)"""
        store = self._ensure_open()
        loop = asyncio.get_running_loop()
        if self.loop_id == 0:
            self.loop_id = hash(loop)
        assert hash(loop) == self.loop_id, _(
            "Backend只能在同一个coroutine中使用。检测到调用此函数的协程发生了变化"
        )
        return await loop.run_in_executor(self._executor, fn, store, *args)

    def run_sync_(self, fn: Callable[..., Any], *args: Any) -> Any:
        """内部方法：同步接口（维护）用，在专用线程里执行 fn(store, *args) 并等它完成"""
        store = self._ensure_open()
        return self._executor.submit(fn, store, *args).result()

    @override
    def post_configure(
        self, components: Iterable[type[BaseComponent]] | None = None
    ) -> None:
        """
        对数据库做的配置工作放在这，可以做些减少运维压力的工作，或是需要项目加载完成后才能做的初始化工作。
        此项在服务器完全加载完毕后才会执行，在测试环境中，也是最后调用。
        """
        self._ensure_open()
        if not self.is_servant:
            # 同 Redis：索引字段的类型要能编码成可排序的字节
            self._schema_checking(components)

    @override
    async def is_synced(self, checkpoint: Any = None) -> tuple[bool, Any]:
        """没有副本，读的就是最新的"""
        self._ensure_open()
        return True, 0 if checkpoint is None else checkpoint

    @override
    async def close(self):
        if self._store is None:
            return
        store, self._store = self._store, None
        if self._hub is not None:
            hub, self._hub = self._hub, None
            await hub.close()
        # 排在已提交的 job 之后关连接：不打断正在跑的事务
        await asyncio.get_running_loop().run_in_executor(self._executor, store.close)
        self._executor.shutdown(wait=False)

    # ============ 读 ============

    @overload
    async def get(
        self,
        table_ref: TableReference,
        row_id: int,
        row_format: Literal[RowFormat.STRUCT] = RowFormat.STRUCT,
    ) -> np.record | None: ...
    @overload
    async def get(
        self,
        table_ref: TableReference,
        row_id: int,
        row_format: Literal[RowFormat.RAW] = ...,
    ) -> dict[str, str] | None: ...
    @overload
    async def get(
        self,
        table_ref: TableReference,
        row_id: int,
        row_format: Literal[RowFormat.TYPED_DICT] = ...,
    ) -> dict[str, Any] | None: ...
    @overload
    async def get(
        self,
        table_ref: TableReference,
        row_id: int,
        row_format: RowFormat = ...,
    ) -> np.record | dict[str, str] | dict[str, Any] | None: ...
    @override
    async def get(
        self, table_ref: TableReference, row_id: int, row_format=RowFormat.STRUCT
    ) -> np.record | dict[str, Any] | None:
        """从数据库直接获取单行数据，见基类"""
        row = await self.run_(SQLiteStore.hgetall, self.row_key(table_ref, row_id))
        if row:
            return self.row_decode_(table_ref.comp_cls, row, row_format)
        return None

    @override
    async def get_many(
        self,
        table_ref: TableReference,
        row_ids: Iterable[int],
        row_format: RowFormat = RowFormat.STRUCT,
    ) -> list[np.record | dict[str, str] | dict[str, Any] | None]:
        assert row_format != RowFormat.ID_LIST, "get_many不支持ID_LIST格式"
        comp_cls = table_ref.comp_cls
        raw_rows = await self.run_(
            SQLiteStore.hgetall_many, self.cluster_prefix(table_ref), list(row_ids)
        )
        if row_format is RowFormat.STRUCT:
            batch = self.rows_decode_(comp_cls, [row for row in raw_rows if row])
            records = detach_rows_(batch)
            return [next(records) if row else None for row in raw_rows]
        return [
            self.row_decode_(comp_cls, row, row_format) if row else None
            for row in raw_rows
        ]

    @override
    async def get_many_array_(
        self, table_ref: TableReference, row_ids: list[int]
    ) -> tuple[np.recarray, list[int]]:
        raw_rows = await self.run_(
            SQLiteStore.hgetall_many, self.cluster_prefix(table_ref), list(row_ids)
        )
        rows = self.rows_decode_(table_ref.comp_cls, [row for row in raw_rows if row])
        return rows, [row_id for row_id, row in zip(row_ids, raw_rows) if not row]

    async def _zrange_members(
        self,
        table_ref: TableReference,
        index_name: str,
        left: float | str | bytes | bool,
        right: float | str | bytes | bool | None,
        limit: int,
        desc: bool,
    ) -> tuple[list[bytes], bytes, bytes]:
        """按索引区间 ZRANGE，返回原样 member（value\\x00id）与规范化后的两个边界"""
        idx_key, b_left, b_right, empty = self.zrange_args_(
            table_ref, index_name, left, right, desc
        )
        if empty:
            return [], b_left, b_right
        members = await self.run_(
            SQLiteStore.zrange_bylex, idx_key, b_left, b_right, desc, 0, limit
        )
        return members, b_left, b_right

    @overload
    async def range(
        self,
        table_ref: TableReference,
        index_name: str,
        left: float | str | bytes | bool,
        right: float | str | bytes | bool | None = None,
        limit: int = 100,
        desc: bool = False,
        row_format: Literal[RowFormat.STRUCT] = RowFormat.STRUCT,
    ) -> np.recarray: ...
    @overload
    async def range(
        self,
        table_ref: TableReference,
        index_name: str,
        left: float | str | bytes | bool,
        right: float | str | bytes | bool | None = None,
        limit: int = 100,
        desc: bool = False,
        row_format: Literal[RowFormat.RAW] = ...,
    ) -> list[dict[str, str]]: ...
    @overload
    async def range(
        self,
        table_ref: TableReference,
        index_name: str,
        left: float | str | bytes | bool,
        right: float | str | bytes | bool | None = None,
        limit: int = 100,
        desc: bool = False,
        row_format: Literal[RowFormat.TYPED_DICT] = ...,
    ) -> list[dict[str, Any]]: ...
    @overload
    async def range(
        self,
        table_ref: TableReference,
        index_name: str,
        left: float | str | bytes | bool,
        right: float | str | bytes | bool | None = None,
        limit: int = 100,
        desc: bool = False,
        row_format: Literal[RowFormat.ID_LIST] = ...,
    ) -> list[int]: ...
    @overload
    async def range(
        self,
        table_ref: TableReference,
        index_name: str,
        left: float | str | bytes | bool,
        right: float | str | bytes | bool | None = None,
        limit: int = 100,
        desc: bool = False,
        row_format: RowFormat = ...,
    ) -> np.recarray | list[dict[str, str]] | list[dict[str, Any]] | list[int]: ...
    @override
    async def range(
        self,
        table_ref: TableReference,
        index_name: str,
        left: float | str | bytes | bool,
        right: float | str | bytes | bool | None = None,
        limit: int = 100,
        desc: bool = False,
        row_format=RowFormat.STRUCT,
    ) -> list[int] | list[dict[str, Any]] | np.recarray:
        """
        按索引区间查询，见基类。和 Redis 一样分两次调用：先查索引拿 id，再批量读行，两次之间
        行可能被删改（读不到的行跳过）。
        """
        members, _b_left, _b_right = await self._zrange_members(
            table_ref, index_name, left, right, limit, desc
        )
        row_ids = [int(vk.rsplit(b"\x00", 1)[-1]) for vk in members]

        if row_format == RowFormat.ID_LIST:
            return row_ids

        comp_cls = table_ref.comp_cls
        raw_rows = [
            row
            for row in await self.run_(
                SQLiteStore.hgetall_many, self.cluster_prefix(table_ref), row_ids
            )
            if row
        ]
        if row_format == RowFormat.STRUCT:
            return self.rows_decode_(comp_cls, raw_rows)
        return [
            cast(dict[str, Any], self.row_decode_(comp_cls, row, row_format))
            for row in raw_rows
        ]

    @override
    async def range_read_(
        self,
        table_ref: TableReference,
        index_name: str,
        left: float | str | bytes | bool,
        right: float | str | bytes | bool | None,
        limit: int,
        desc: bool,
    ) -> tuple[list[int], RangeObservation]:
        members, b_left, b_right = await self._zrange_members(
            table_ref, index_name, left, right, limit, desc
        )
        return self.range_observation_(
            index_name, members, b_left, b_right, limit, desc
        )

    # ============ 写 ============

    @override
    async def commit_script_(self, keys: list[str], args: list[bytes]) -> bytes:
        """commit_v2.lua 的 Python 版（`sqlite/commit.py`），在一个写事务里原子执行，见基类"""
        now = time.time()
        cleanup_before = None
        if now >= self._next_notify_cleanup_at:
            cleanup_before = now - self.NOTIFY_TTL_SECONDS
            self._next_notify_cleanup_at = self._next_cleanup_time(now)
        return await self.run_(run_commit, args[0], cleanup_before)

    @override
    async def direct_set(
        self, table_ref: TableReference, id_: int, **kwargs: str
    ) -> None:
        """
        UNSAFE! 只用于易失数据! 不会做类型检查!

        直接写入属性到数据库，避免session必须要执行get+事务2条指令。
        仅支持非索引字段，索引字段更新是非原子性的，必须使用事务。
        注意此方法可能导致写入数据到已删除的行，请确保逻辑。

        一些系统级别的临时数据，使用直接写入的方式效率会更高，但不保证数据一致性。
        SQLite 后端不为它发订阅通知（契约是不保证通知，见基类）。
        """
        self.check_direct_set_(table_ref, kwargs)
        schema = [name for name, _prop in table_ref.comp_cls.properties_]
        await self.run_(
            SQLiteStore.hset_txn, self.row_key(table_ref, id_), kwargs, schema
        )

    # ============ 维护 / 订阅 ============

    @override
    def get_table_maintenance(self) -> SQLiteTableMaintenance:
        """获取表维护对象"""
        self._ensure_open()
        from .maint import SQLiteTableMaintenance

        return SQLiteTableMaintenance(self)

    @override
    def get_mq_client(self) -> SQLiteMQClient:
        """
        获取消息队列连接（每个用户连接一个）。本进程对本库只有一个 `SQLiteNotifyHub`（一个通知表
        轮询任务）在首次调用时懒建，之后每次返回一个挂在它上面的轻量 MQClient。
        """
        self._ensure_open()
        from .mq import SQLiteMQClient, SQLiteNotifyHub

        if self._hub is None:
            self._hub = SQLiteNotifyHub(self)
        return SQLiteMQClient(self._hub)
