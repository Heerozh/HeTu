"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import hashlib
import logging
import random
import time
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, Literal, Never, cast, final, overload, override

import numpy as np
import sqlalchemy as sa
from sqlalchemy import exc as sa_exc
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from ....i18n import _
from ..base import BackendClient, RaceCondition, RowFormat

if TYPE_CHECKING:
    from ...component import BaseComponent
    from ..idmap import IdentityMap
    from ..table import TableReference
    from .maint import SQLTableMaintenance
    from .mq import SQLMQClient

logger = logging.getLogger("HeTu.root")


def _numpy_to_sqla_type(dtype: np.dtype) -> sa.types.TypeEngine[Any]:
    dtype_type = dtype.type

    if np.issubdtype(dtype_type, np.bool_):
        return sa.Boolean()
    if np.issubdtype(dtype_type, np.signedinteger):
        bits = dtype.itemsize * 8
        if bits <= 16:
            return sa.SmallInteger()
        if bits <= 32:
            return sa.Integer()
        return sa.BigInteger()
    if np.issubdtype(dtype_type, np.unsignedinteger):
        # 各方言对unsigned支持不统一，统一放到BigInteger，保证兼容性。
        return sa.BigInteger()
    if np.issubdtype(dtype_type, np.floating):
        return sa.Float(precision=24 if dtype.itemsize <= 4 else 53)
    if np.issubdtype(dtype_type, np.str_):
        char_len = max(1, dtype.itemsize // 4)
        return sa.String(length=char_len)
    if np.issubdtype(dtype_type, np.bytes_):
        return sa.LargeBinary(length=max(1, dtype.itemsize))

    raise TypeError(_("SQLBackend不支持的数据类型: {dtype}").format(dtype=dtype))


@final
class SQLBackendClient(BackendClient, alias="sql"):
    """SQL后端连接与读写实现（SQLAlchemy Core）。"""

    META_TABLE_NAME = "_HeTu_Component_Meta"
    NOTIFY_TABLE_NAME = "_Hetu_Notify"
    MAINTENANCE_LOCK_TABLE_NAME = "_Hetu_Maintenance_Lock"
    NOTIFY_TTL_SECONDS = 60 * 60
    NOTIFY_CLEANUP_INTERVAL = 60 * 15
    NOTIFY_CLEANUP_JITTER = 90.0

    @staticmethod
    def _get_referred_components() -> list[type[BaseComponent]]:
        from ....system.definer import SystemClusters

        return [comp_cls for comp_cls in SystemClusters().get_components().keys()]

    def _schema_checking_for_sql(self):
        """检查Component的schema定义，确保符合sql系列的要求"""
        for comp_cls in self._get_referred_components():
            for field, _ in comp_cls.indexes_.items():
                dtype = comp_cls.dtype_map_[field]
                # 如果有不支持的dtype，在这raise
                del dtype
                pass

    @classmethod
    def parse_engine_urls(cls, dsn: str) -> tuple[str, str]:
        """
        解析用户dsn，自动补齐sync/async driver。

        要求用户传入的dsn不能包含driver（dialect+driver），否则报错。
        """
        url = sa.engine.make_url(dsn)
        driver_name = url.drivername.lower()
        if "+" in driver_name:
            raise ValueError(
                _(
                    "SQL后端不允许dsn显式指定driver: `{dsn}`，"
                    "请只传 `postgresql://` / `mysql://` / `mariadb://` / `sqlite://`"
                ).format(dsn=dsn)
            )

        if driver_name == "postgres":
            driver_name = "postgresql"
        elif driver_name == "mariadb":
            driver_name = "mysql"

        if driver_name == "postgresql":
            sync_url = url.set(drivername="postgresql+psycopg")
            async_url = url.set(drivername="postgresql+asyncpg")
        elif driver_name == "mysql":
            sync_url = url.set(drivername="mysql+pymysql")
            async_url = url.set(drivername="mysql+aiomysql")
        elif driver_name == "sqlite":
            sync_url = url.set(drivername="sqlite")
            async_url = url.set(drivername="sqlite+aiosqlite")
        else:
            raise ValueError(
                _(
                    "SQL后端目前只支持postgresql/mysql(mariadb)/sqlite，收到: `{dsn}`"
                ).format(dsn=dsn)
            )

        return sync_url.render_as_string(
            hide_password=False
        ), async_url.render_as_string(hide_password=False)

    @staticmethod
    def table_prefix(table_ref: TableReference) -> str:
        return f"{table_ref.instance_name}:{table_ref.comp_cls.name_}"

    @staticmethod
    def cluster_prefix(table_ref: TableReference) -> str:
        return (
            f"{table_ref.instance_name}:{table_ref.comp_cls.name_}:"
            f"{{CLU{table_ref.cluster_id}}}"
        )

    @classmethod
    def row_key(cls, table_ref: TableReference, row_id: int | str) -> str:
        return f"{cls.cluster_prefix(table_ref)}:id:{str(row_id)}"

    @classmethod
    def index_key(cls, table_ref: TableReference, index_name: str) -> str:
        return f"{cls.cluster_prefix(table_ref)}:index:{index_name}"

    @classmethod
    def component_table_name(cls, table_ref: TableReference) -> str:
        raw = (
            f"{table_ref.instance_name}:{table_ref.comp_cls.namespace_}:"
            f"{table_ref.comp_cls.name_}:{table_ref.cluster_id}"
        )
        digest = hashlib.md5(raw.encode("utf-8")).hexdigest()[:10]
        base = raw
        # 如果超过各sql最小长度限制，截断base部分，保留hash摘要
        # - PostgreSQL: 默认最多 63 bytes（NAMEDATALEN=64，取 -1）
        # - MySQL: 表名最多 64 characters
        # - MariaDB: 表名最多 64 characters
        # - SQL Server: 标识符（含表名）1~128 characters
        # - Oracle:
        #     - COMPATIBLE >= 12.2：1~128 bytes
        #     - COMPATIBLE < 12.2：1~30 bytes
        # - SQLite: 无
        encoded = base.encode("utf-8")
        if len(encoded) > 63:
            max_prefix_bytes = 63 - 1 - len(digest)  # 1 for underscore
            prefix = encoded[:max_prefix_bytes].decode("utf-8", "ignore")
            base = f"{prefix}_{digest}"
        return base

    @classmethod
    def component_table(
        cls, table_ref: TableReference, metadata: sa.MetaData | None = None
    ):
        if metadata is None:
            metadata = sa.MetaData()
        table_name = cls.component_table_name(table_ref)
        if table_name in metadata.tables:
            return metadata.tables[table_name]

        columns: list[sa.Column[Any]] = []
        for name, prop in table_ref.comp_cls.properties_:
            dtype = table_ref.comp_cls.dtype_map_[name]
            col_type = _numpy_to_sqla_type(dtype)
            is_primary = name == "id"
            is_unique = bool(prop.unique and not is_primary)
            is_index = bool(prop.index and not prop.unique and not is_primary)
            columns.append(
                sa.Column(
                    name,
                    col_type,
                    primary_key=is_primary,
                    nullable=False,
                    unique=is_unique,
                    index=is_index,
                    autoincrement=False if is_primary else "auto",
                )
            )
        return sa.Table(table_name, metadata, *columns)

    @classmethod
    def meta_table(cls, metadata: sa.MetaData | None = None):
        if metadata is None:
            metadata = sa.MetaData()
        if cls.META_TABLE_NAME in metadata.tables:
            return metadata.tables[cls.META_TABLE_NAME]
        return sa.Table(
            cls.META_TABLE_NAME,
            metadata,
            sa.Column("instance_name", sa.String(length=128), primary_key=True),
            sa.Column("comp_name", sa.String(length=128), primary_key=True),
            sa.Column("version", sa.String(length=64), nullable=False),
            sa.Column("json", sa.Text(), nullable=False),
            sa.Column("cluster_id", sa.Integer(), nullable=False),
            sa.Column("extra_json", sa.Text(), nullable=False, server_default="{}"),
        )

    @classmethod
    def notify_table(cls, metadata: sa.MetaData | None = None):
        if metadata is None:
            metadata = sa.MetaData()
        if cls.NOTIFY_TABLE_NAME in metadata.tables:
            return metadata.tables[cls.NOTIFY_TABLE_NAME]
        return sa.Table(
            cls.NOTIFY_TABLE_NAME,
            metadata,
            sa.Column(
                "id",
                sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
                primary_key=True,
                autoincrement=True,
            ),
            sa.Column("channel", sa.String(length=256), nullable=False, index=True),
            sa.Column("created_at", sa.TIMESTAMP(), nullable=False, index=True),
        )

    @classmethod
    def maintenance_lock_table(cls, metadata: sa.MetaData | None = None):
        if metadata is None:
            metadata = sa.MetaData()
        if cls.MAINTENANCE_LOCK_TABLE_NAME in metadata.tables:
            return metadata.tables[cls.MAINTENANCE_LOCK_TABLE_NAME]
        return sa.Table(
            cls.MAINTENANCE_LOCK_TABLE_NAME,
            metadata,
            sa.Column("lock_name", sa.String(length=64), primary_key=True),
            sa.Column(
                "updated_at",
                sa.TIMESTAMP(),
                nullable=False,
                server_default=sa.text("CURRENT_TIMESTAMP"),
            ),
        )

    @override
    def index_channel(self, table_ref: TableReference, index_name: str):
        return self.index_key(table_ref, index_name)

    @override
    def row_channel(self, table_ref: TableReference, row_id: int):
        return self.row_key(table_ref, row_id)

    def __init__(self, endpoint: str | list[str], is_servant, **kwargs):
        super().__init__(endpoint, is_servant, **kwargs)
        self.urls = [endpoint] if isinstance(endpoint, str) else endpoint
        assert len(self.urls) > 0, _("必须至少指定一个数据库连接URL")

        self._ios: list[sa.Engine] = []
        self._async_ios: list[AsyncEngine] = []

        for dsn in self.urls:
            sync_dsn, async_dsn = self.parse_engine_urls(dsn)
            io = sa.create_engine(
                sync_dsn,
                future=True,
                pool_pre_ping=True,
            )
            aio = create_async_engine(
                async_dsn,
                future=True,
                pool_pre_ping=True,
            )
            self._ios.append(io)
            self._async_ios.append(aio)

        for i, io in enumerate(self._ios):
            try:
                with io.connect() as conn:
                    conn.execute(sa.text("SELECT 1"))
            except Exception as exc:
                raise ConnectionError(
                    _("无法连接到SQL数据库：{url}").format(url=self.urls[i])
                ) from exc

        self._next_notify_cleanup_at = (
            time.time()
            + self.NOTIFY_CLEANUP_INTERVAL
            + random.uniform(0.0, self.NOTIFY_CLEANUP_JITTER)
        )

    @property
    def io(self) -> sa.Engine:
        return random.choice(self._ios)

    @property
    def aio(self) -> AsyncEngine:
        return random.choice(self._async_ios)

    def _ensure_open(self):
        if not self._ios:
            raise ConnectionError(_("连接已关闭，已调用过close"))

    def ensure_support_tables_sync(self):
        meta = sa.MetaData()
        self.meta_table(meta)
        self.notify_table(meta)
        self.maintenance_lock_table(meta)
        try:
            meta.create_all(self.io, checkfirst=True)
        except sa_exc.DBAPIError as exc:
            if "already exists" in str(exc).lower():
                # 可能是并发创建导致的，忽略
                return
            raise

    @override
    def post_configure(self) -> None:
        self._ensure_open()
        if not self.is_servant:
            self.ensure_support_tables_sync()
        # 提示用户schema定义是否符合sql要求
        self._schema_checking_for_sql()

    @override
    async def is_synced(self, checkpoint: Any = None) -> tuple[bool, Any]:
        self._ensure_open()
        if checkpoint is None:
            checkpoint = int(time.time() * 1000)
        return True, checkpoint

    @override
    async def close(self):
        if not self._ios:
            return

        for io in self._ios:
            io.dispose()
        self._ios = []

        for aio in self._async_ios:
            await aio.dispose()
        self._async_ios = []

    @staticmethod
    def _coerce_bool(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, np.integer)):
            return bool(value)
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "t", "yes", "y"}
        return bool(value)

    @classmethod
    def _coerce_scalar(cls, dtype: np.dtype, value: Any) -> Any:
        if isinstance(value, np.generic):
            return value.item()
        if isinstance(value, memoryview):
            value = value.tobytes()

        dtype_type = dtype.type
        if np.issubdtype(dtype_type, np.bool_):
            return cls._coerce_bool(value)
        if np.issubdtype(dtype_type, np.integer):
            if isinstance(value, bytes):
                value = value.decode("utf-8", "ignore")
            return int(value)
        if np.issubdtype(dtype_type, np.floating):
            if isinstance(value, bytes):
                value = value.decode("utf-8", "ignore")
            return float(value)
        if np.issubdtype(dtype_type, np.str_):
            if isinstance(value, bytes):
                return value.decode("utf-8", "ignore")
            return str(value)
        if np.issubdtype(dtype_type, np.bytes_):
            if isinstance(value, bytes):
                return value
            if isinstance(value, str):
                return value.encode("utf-8")
            return bytes(value)
        return value

    @classmethod
    def _row_to_typed_dict(
        cls, comp_cls: type[BaseComponent], row: dict[str, Any]
    ) -> dict[str, Any]:
        typed: dict[str, Any] = {}
        for name in comp_cls.prop_idx_map_:
            typed[name] = cls._coerce_scalar(comp_cls.dtype_map_[name], row[name])
        return typed

    @staticmethod
    def _row_to_raw_dict(row: dict[str, Any]) -> dict[str, str]:
        ret: dict[str, str] = {}
        for key, value in row.items():
            if isinstance(value, memoryview):
                value = value.tobytes()
            if isinstance(value, bytes):
                ret[key] = value.decode("utf-8", "ignore")
            else:
                ret[key] = str(value)
        return ret

    @overload
    @staticmethod
    def row_decode_(
        comp_cls: type[BaseComponent],
        row: dict[str, Any],
        fmt: Literal[RowFormat.STRUCT],
    ) -> np.record: ...
    @overload
    @staticmethod
    def row_decode_(
        comp_cls: type[BaseComponent],
        row: dict[str, Any],
        fmt: Literal[RowFormat.RAW, RowFormat.TYPED_DICT],
    ) -> dict[str, Any]: ...
    @overload
    @staticmethod
    def row_decode_(
        comp_cls: type[BaseComponent],
        row: dict[str, Any],
        fmt: Literal[RowFormat.ID_LIST],
    ) -> Never: ...
    @staticmethod
    def row_decode_(
        comp_cls: type[BaseComponent], row: dict[str, Any], fmt: RowFormat
    ) -> np.record | dict[str, Any]:
        match fmt:
            case RowFormat.RAW:
                return SQLBackendClient._row_to_raw_dict(row)
            case RowFormat.STRUCT:
                typed = SQLBackendClient._row_to_typed_dict(comp_cls, row)
                return comp_cls.dict_to_struct(typed)
            case RowFormat.TYPED_DICT:
                typed = SQLBackendClient._row_to_typed_dict(comp_cls, row)
                struct_row = comp_cls.dict_to_struct(typed)
                return comp_cls.struct_to_dict(struct_row)
            case _:
                raise ValueError(_("不可用的行格式: {fmt}").format(fmt=fmt))

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
        self._ensure_open()
        table = self.component_table(table_ref)
        stmt = sa.select(table).where(table.c.id == int(row_id)).limit(1)
        async with self.aio.connect() as conn:
            try:
                row = (await conn.execute(stmt)).mappings().first()
            except sa_exc.DBAPIError as exc:
                if self._is_table_missing_error(exc):
                    return None
                raise
        if row is None:
            return None
        return self.row_decode_(table_ref.comp_cls, dict(row), row_format)

    @classmethod
    def _normalize_range_bound(
        cls, dtype: np.dtype, value: int | float | str | bytes | bool
    ) -> Any:
        dtype_type = dtype.type
        if np.issubdtype(dtype_type, np.str_):
            if isinstance(value, bytes):
                return value.decode("utf-8", "ignore")
            return str(value)
        if np.issubdtype(dtype_type, np.bytes_):
            if isinstance(value, (bytes, bytearray, memoryview)):
                return bytes(value)
            if isinstance(value, str):
                return value.encode("utf-8")
            raise TypeError(
                _("bytes索引类型不支持该查询值: {value_type}").format(
                    value_type=type(value)
                )
            )
        if np.issubdtype(dtype_type, np.bool_):
            return cls._coerce_bool(value)
        if np.issubdtype(dtype_type, np.integer):
            return int(value)
        if np.issubdtype(dtype_type, np.floating):
            return float(value)
        return value

    @classmethod
    def range_normalize_(
        cls,
        dtype: np.dtype,
        left: int | float | str | bytes | bool,
        right: int | float | str | bytes | bool | None,
        desc: bool,
    ) -> tuple[Any, Any, bool, bool]:
        """规范化范围查询边界（保留Redis版对inf与类型检查的逻辑）。"""
        if right is None:
            right = left
        if desc:
            left, right = right, left

        if issubclass(dtype.type, np.character):
            assert type(left) in (str, bytes) and type(right) in (str, bytes), (
                f"字符串类型的查询变量类型必须是str/bytes，你的：left={type(left)}({left}), "
                f"right={type(right)}({right})"
            )
        else:
            if issubclass(dtype.type, np.integer):
                type_info = np.iinfo(dtype)

                def clamp_inf(x):
                    if type(x) is float and np.isinf(x):
                        return type_info.max if x > 0 else type_info.min
                    return x

                left = clamp_inf(left)
                right = clamp_inf(right)

        def peel(x, _inclusive):
            if type(x) in (str, bytes) and len(x) >= 1:
                ch = x[0:1]
                if ch in ("(", "[") or ch in (b"(", b"["):
                    _inclusive = ch == "[" or ch == b"["
                    x = x[1:]
            return x, _inclusive

        left, li = peel(left, True)
        right, ri = peel(right, True)
        if desc:
            li, ri = ri, li

        left = cls._normalize_range_bound(dtype, left)
        right = cls._normalize_range_bound(dtype, right)
        return left, right, li, ri

    def _is_unique_violation(self, exc: sa_exc.IntegrityError) -> bool:
        message = str(exc).lower()
        markers = (
            "unique",
            "duplicate",
            "constraint failed",
            "duplicate entry",
        )
        return any(marker in message for marker in markers)

    @staticmethod
    def _is_table_missing_error(exc: BaseException) -> bool:
        if not isinstance(exc, sa_exc.DBAPIError):
            return False

        message = str(exc).lower()
        if any(
            marker in message
            for marker in (
                "no such table",
                "does not exist",
                "doesn't exist",
                "undefined table",
                "unknown table",
            )
        ):
            return True

        orig = getattr(exc, "orig", None)
        sqlstate = getattr(orig, "sqlstate", None) or getattr(orig, "pgcode", None)
        return sqlstate == "42P01"

    def _create_related_tables_sync(self, refs: list[TableReference]) -> None:
        """
        使用同步IO创建commit事务相关表（组件表 + 支撑表）。
        """
        for io in self._ios:
            meta = sa.MetaData()
            self.meta_table(meta)
            self.notify_table(meta)
            self.maintenance_lock_table(meta)
            for ref in refs:
                self.component_table(ref, meta)
            meta.create_all(io, checkfirst=True)

    @overload
    async def range(
        self,
        table_ref: TableReference,
        index_name: str,
        left: int | float | str | bytes | bool,
        right: int | float | str | bytes | bool | None = None,
        limit: int = 100,
        desc: bool = False,
        row_format: Literal[RowFormat.STRUCT] = RowFormat.STRUCT,
    ) -> np.recarray: ...
    @overload
    async def range(
        self,
        table_ref: TableReference,
        index_name: str,
        left: int | float | str | bytes | bool,
        right: int | float | str | bytes | bool | None = None,
        limit: int = 100,
        desc: bool = False,
        row_format: Literal[RowFormat.RAW] = ...,
    ) -> list[dict[str, str]]: ...
    @overload
    async def range(
        self,
        table_ref: TableReference,
        index_name: str,
        left: int | float | str | bytes | bool,
        right: int | float | str | bytes | bool | None = None,
        limit: int = 100,
        desc: bool = False,
        row_format: Literal[RowFormat.TYPED_DICT] = ...,
    ) -> list[dict[str, Any]]: ...
    @overload
    async def range(
        self,
        table_ref: TableReference,
        index_name: str,
        left: int | float | str | bytes | bool,
        right: int | float | str | bytes | bool | None = None,
        limit: int = 100,
        desc: bool = False,
        row_format: Literal[RowFormat.ID_LIST] = ...,
    ) -> list[int]: ...
    @overload
    async def range(
        self,
        table_ref: TableReference,
        index_name: str,
        left: int | float | str | bytes | bool,
        right: int | float | str | bytes | bool | None = None,
        limit: int = 100,
        desc: bool = False,
        row_format: RowFormat = ...,
    ) -> np.recarray | list[dict[str, str]] | list[dict[str, Any]] | list[int]: ...
    @override
    async def range(
        self,
        table_ref: TableReference,
        index_name: str,
        left: int | float | str | bytes | bool,
        right: int | float | str | bytes | bool | None = None,
        limit: int = 100,
        desc: bool = False,
        row_format=RowFormat.STRUCT,
    ) -> list[int] | list[dict[str, Any]] | np.recarray:
        self._ensure_open()

        comp_cls = table_ref.comp_cls
        assert index_name in comp_cls.indexes_, (
            f"Component `{comp_cls.name_}` 没有索引 `{index_name}`"
        )

        dtype = comp_cls.dtype_map_[index_name]
        left, right, li, ri = self.range_normalize_(dtype, left, right, desc)
        assert (
            (cast(Any, left) >= cast(Any, right))
            if desc
            else (cast(Any, right) >= cast(Any, left))
        ), f"left必须大于等于right，你的:right={right}, left={left}"

        table = self.component_table(table_ref)
        col = table.c[index_name]
        if desc:
            cond_left = col <= left if li else col < left
            cond_right = col >= right if ri else col > right
            order_by = (col.desc(), table.c.id.desc())
        else:
            cond_left = col >= left if li else col > left
            cond_right = col <= right if ri else col < right
            order_by = (col.asc(), table.c.id.asc())

        if row_format == RowFormat.ID_LIST:
            stmt = (
                sa.select(table.c.id).where(cond_left, cond_right).order_by(*order_by)
            )
            if limit >= 0:
                stmt = stmt.limit(limit)
            async with self.aio.connect() as conn:
                try:
                    rows = (await conn.execute(stmt)).scalars().all()
                except sa_exc.DBAPIError as exc:
                    if self._is_table_missing_error(exc):
                        return []
                    raise
            return [int(x) for x in rows]

        stmt = sa.select(table).where(cond_left, cond_right).order_by(*order_by)
        if limit >= 0:
            stmt = stmt.limit(limit)
        async with self.aio.connect() as conn:
            try:
                rows = (await conn.execute(stmt)).mappings().all()
            except sa_exc.DBAPIError as exc:
                if self._is_table_missing_error(exc):
                    rows = []
                else:
                    raise

        if row_format == RowFormat.RAW or row_format == RowFormat.TYPED_DICT:
            return [
                cast(dict[str, Any], self.row_decode_(comp_cls, dict(row), row_format))
                for row in rows
            ]

        if len(rows) == 0:
            return np.rec.array(np.empty(0, dtype=comp_cls.dtypes))
        records = [
            cast(np.record, self.row_decode_(comp_cls, dict(row), RowFormat.STRUCT))
            for row in rows
        ]
        return np.rec.array(np.stack(records, dtype=comp_cls.dtypes))

    def _dirty_to_typed_update(
        self, comp_cls: type[BaseComponent], dirty: dict[str, str]
    ) -> dict[str, Any]:
        ret: dict[str, Any] = {}
        for key, value in dirty.items():
            if key in {"id", "_version"}:
                continue
            ret[key] = self._coerce_scalar(comp_cls.dtype_map_[key], value)
        return ret

    def _dirty_to_typed_insert(
        self, comp_cls: type[BaseComponent], dirty: dict[str, str]
    ) -> dict[str, Any]:
        ret: dict[str, Any] = {}
        for key in comp_cls.prop_idx_map_:
            if key == "_version":
                continue
            ret[key] = self._coerce_scalar(comp_cls.dtype_map_[key], dirty[key])
        ret["_version"] = 1
        return ret

    @override
    async def commit(self, idmap: IdentityMap) -> None:
        self._ensure_open()
        assert not self.is_servant, _("从节点不允许提交事务")

        dirties = idmap.get_dirty_rows()
        if not dirties:
            raise ValueError(_("没有脏数据需要提交"))

        notify_table = self.notify_table()
        now_ts = time.time()
        now_dt = datetime.now(UTC).replace(tzinfo=None)
        cleanup_due = now_ts >= self._next_notify_cleanup_at
        refs = list(dirties.keys())
        for attempt in range(2):
            channels: set[str] = set()
            try:
                async with self.aio.begin() as conn:
                    # 先删除，避免insert/update遇到本事务中将被删除数据导致unique冲突。
                    for ref, (
                        _inserts,
                        (_old_rows, _new_rows),
                        deletes,
                    ) in dirties.items():
                        table = self.component_table(ref)
                        for old_row in deletes:
                            row_id = int(old_row["id"])
                            old_version = int(old_row["_version"])
                            stmt = sa.delete(table).where(
                                table.c.id == row_id, table.c._version == old_version
                            )
                            result = await conn.execute(stmt)
                            if result.rowcount != 1:
                                raise RaceCondition(
                                    f"Version mismatch when deleting row id={row_id}"
                                )
                            channels.add(self.row_channel(ref, row_id))
                            for index_name in ref.comp_cls.indexes_:
                                channels.add(self.index_channel(ref, index_name))

                    for ref, (
                        _inserts,
                        (old_rows, new_rows),
                        _deletes,
                    ) in dirties.items():
                        table = self.component_table(ref)
                        indexes = ref.comp_cls.indexes_
                        for old_row, changed_row in zip(old_rows, new_rows):
                            row_id = int(old_row["id"])
                            old_version = int(old_row["_version"])
                            updates = self._dirty_to_typed_update(
                                ref.comp_cls, changed_row
                            )
                            if len(updates) == 0:
                                continue
                            updates["_version"] = old_version + 1
                            stmt = (
                                sa.update(table)
                                .where(
                                    table.c.id == row_id,
                                    table.c._version == old_version,
                                )
                                .values(**updates)
                            )
                            try:
                                result = await conn.execute(stmt)
                            except sa_exc.IntegrityError as exc:
                                if self._is_unique_violation(exc):
                                    raise RaceCondition(
                                        f"UNIQUE violation: {exc}"
                                    ) from exc
                                raise
                            if result.rowcount != 1:
                                raise RaceCondition(
                                    f"Version mismatch when updating row id={row_id}"
                                )
                            channels.add(self.row_channel(ref, row_id))
                            for index_name in updates:
                                if index_name in indexes:
                                    channels.add(self.index_channel(ref, index_name))

                    for ref, (
                        inserts,
                        (_old_rows, _new_rows),
                        _deletes,
                    ) in dirties.items():
                        table = self.component_table(ref)
                        for row in inserts:
                            typed_row = self._dirty_to_typed_insert(ref.comp_cls, row)
                            row_id = int(typed_row["id"])
                            try:
                                await conn.execute(sa.insert(table).values(**typed_row))
                            except sa_exc.IntegrityError as exc:
                                if self._is_unique_violation(exc):
                                    raise RaceCondition(
                                        f"UNIQUE violation: {exc}"
                                    ) from exc
                                raise
                            channels.add(self.row_channel(ref, row_id))
                            for index_name in ref.comp_cls.indexes_:
                                channels.add(self.index_channel(ref, index_name))

                    if channels:
                        await conn.execute(
                            sa.insert(notify_table),
                            [
                                {"channel": channel, "created_at": now_dt}
                                for channel in sorted(channels)
                            ],
                        )

                    if cleanup_due:
                        expire_at = now_dt - timedelta(seconds=self.NOTIFY_TTL_SECONDS)
                        await conn.execute(
                            sa.delete(notify_table).where(
                                notify_table.c.created_at < expire_at
                            )
                        )
                        self._next_notify_cleanup_at = (
                            now_ts
                            + self.NOTIFY_CLEANUP_INTERVAL
                            + random.uniform(0.0, self.NOTIFY_CLEANUP_JITTER)
                        )
                return
            except sa_exc.DBAPIError as exc:
                # 目前缺表创建逻辑主要用于tests，因为hetu正式启动时会创建所有引用的表
                # 但是tests很多表是单元中动态创建，每次都写create又不清晰，暂时放这里创建，以后再想想
                if attempt == 0 and self._is_table_missing_error(exc):
                    logger.warning(
                        _(
                            "⚠️ [💾SQL] commit时发现缺失表，正在用同步IO创建相关表并重试事务..."
                        )
                    )
                    self._create_related_tables_sync(refs)
                    continue
                raise

    @override
    async def direct_set(
        self, table_ref: TableReference, id_: int, **kwargs: str
    ) -> None:
        self._ensure_open()
        assert "id" not in kwargs, "id不允许修改"
        assert table_ref.comp_cls.volatile_, "direct_set只能用于易失数据的Component"

        for prop in kwargs:
            if prop in table_ref.comp_cls.indexes_:
                raise ValueError(f"索引字段`{prop}`不允许用direct_set修改")
            if prop not in table_ref.comp_cls.prop_idx_map_:
                raise ValueError(f"Component `{table_ref.comp_name}` 没有字段`{prop}`")

        values = {
            key: self._coerce_scalar(table_ref.comp_cls.dtype_map_[key], value)
            for key, value in kwargs.items()
        }
        table = self.component_table(table_ref)
        async with self.aio.begin() as conn:
            await conn.execute(
                sa.update(table).where(table.c.id == int(id_)).values(**values)
            )

    @override
    def get_table_maintenance(self) -> SQLTableMaintenance:
        self._ensure_open()
        from .maint import SQLTableMaintenance

        return SQLTableMaintenance(self)

    @override
    def get_mq_client(self) -> SQLMQClient:
        self._ensure_open()
        from .mq import SQLMQClient

        return SQLMQClient(self)
