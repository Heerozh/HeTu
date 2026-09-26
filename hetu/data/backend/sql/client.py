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
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, Literal, Never, cast, final, overload, override

import msgpack
import numpy as np
import sqlalchemy as sa
from sqlalchemy import event
from sqlalchemy import exc as sa_exc
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from ....i18n import _
from ..base import (
    BackendClient,
    InconsistentRangeRead,
    RaceCondition,
    RowFormat,
    UniqueViolation,
    detach_rows_,
    inverted_bounds_error_,
    peel_bound_,
    sortable_token,
    to_sortable_bytes,
)
from ..idmap import RangeObservation

if TYPE_CHECKING:
    from ...component import BaseComponent
    from ..idmap import IdentityMap
    from ..table import TableReference
    from .maint import SQLTableMaintenance
    from .mq import SQLMQClient, SQLNotifyHub

logger = logging.getLogger("HeTu.root")

# 无符号整型都放在 BIGINT（有符号 64 位）列里，uint64 超过它的值存不下：写入时明确拒绝
# （_reject_uint64_overflow），查询边界收回这个范围（clamp_uint64_bounds_）
_BIGINT_MAX = 2**63 - 1


def _numpy_to_sqla_type(dtype: np.dtype) -> sa.types.TypeEngine[Any]:
    dtype_type = dtype.type

    # define_component 已把 bool 字段强制转成 int8，组件 dtype 里不会出现 bool，走不到
    # if np.issubdtype(dtype_type, np.bool_):
    #     return sa.Boolean()
    if np.issubdtype(dtype_type, np.signedinteger):
        bits = dtype.itemsize * 8
        if bits <= 16:
            return sa.SmallInteger()
        if bits <= 32:
            return sa.Integer()
        return sa.BigInteger()
    if np.issubdtype(dtype_type, np.unsignedinteger):
        # 各方言对unsigned支持不统一，统一放到BigInteger，保证兼容性（见 _BIGINT_MAX）。
        return sa.BigInteger()
    if np.issubdtype(dtype_type, np.floating):
        return sa.Float(precision=24 if dtype.itemsize <= 4 else 53)
    if np.issubdtype(dtype_type, np.str_):
        char_len = max(1, dtype.itemsize // 4)
        return sa.String(length=char_len)
    if np.issubdtype(dtype_type, np.bytes_):
        return sa.LargeBinary(length=max(1, dtype.itemsize))

    raise TypeError(_("SQLBackend不支持的数据类型: {dtype}").format(dtype=dtype))


def _apply_sqlite_pragmas(dbapi_conn: Any, _rec: Any) -> None:
    """每条SQLite连接建立时设置PRAGMA，降低磁盘写入压力。

    默认的 DELETE 回滚日志 + synchronous=FULL 下，每次事务提交都要建删 `-journal`
    边车文件并 fsync 多次（哪怕只改一个字段也写整页）。HeTu 空闲时也在持续提交
    （循环 FutureCall、Worker 租约续期、每次改动往通知表写行），会造成持续的、被
    放大的磁盘写入。改用 WAL + synchronous=NORMAL 后，提交变成往单个 `-wal` 文件
    追加、仅在 checkpoint 时 fsync，写入量数量级下降。

    - journal_mode=WAL：持久化在库头，一次生效（重复设置无副作用）。
    - synchronous=NORMAL：每连接生效，WAL 下不会丢已提交事务（只在断电+checkpoint
      边界有极小丢失窗口，对游戏服可接受）。
    - busy_timeout：多进程（instance×worker）共用一个库文件时，抢写锁优雅退避，
      减少 "database is locked"。
    """
    cur = dbapi_conn.cursor()
    try:
        cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA synchronous=NORMAL")
        cur.execute("PRAGMA busy_timeout=5000")
    finally:
        cur.close()


def _escape_bytes_hex(value: bytes) -> str:
    return f"_binary X'{value.hex()}'"


def _patch_aiomysql_escape_bytes() -> None:
    """让 aiomysql 0.3.2 在 PyMySQL >= 1.2.3 下也能转义 bytes 参数。

    aiomysql 0.3.2 转义 bytes 参数用的是 PyMySQL 的内部函数 `escape_bytes_prefixed`；
    PyMySQL 1.2.1 的安全修复（GHSA-x4f8-9hx9-hpp9）删掉了它，1.2.3 为了让 aiomysql 能
    import 又放回一个字符串占位，结果一有 bytes 参数就 `'str' object is not callable`。
    这里照 aiomysql 自己的修复（aio-libs/aiomysql#1081，0.3.3）换成 16 进制字面量，
    也就是 PyMySQL 1.2.1 修复后的做法。

    只在认出那个字符串占位时才替换：PyMySQL 还是老版本（它仍是函数），或 aiomysql 已经
    不再用这个名字（>= 0.3.3）时什么都不做。aiomysql 0.3.3 发布后可以删掉本函数。
    """
    import aiomysql.connection

    if isinstance(getattr(aiomysql.connection, "escape_bytes_prefixed", None), str):
        aiomysql.connection.escape_bytes_prefixed = _escape_bytes_hex  # pyright: ignore[reportAttributeAccessIssue]


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

    def _schema_checking_for_sql(
        self, components: Iterable[type[BaseComponent]] | None = None
    ):
        """检查Component的schema定义，确保符合sql系列的要求"""
        if components is None:
            components = self._get_referred_components()
        for comp_cls in components:
            for field, _is_str in comp_cls.indexes_.items():
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
            # 表级频道的payload：msgpack的row_id列表；行/索引频道为NULL
            sa.Column("payload", sa.LargeBinary(), nullable=True),
        )

    @classmethod
    def ensure_notify_payload_column_sync(cls, io: sa.Engine) -> None:
        """
        旧版本的通知表没有payload列，create_all(checkfirst)不会给已有表加列，这里补上。
        """
        inspector = sa.inspect(io)
        if not inspector.has_table(cls.NOTIFY_TABLE_NAME):
            return
        columns = {c["name"] for c in inspector.get_columns(cls.NOTIFY_TABLE_NAME)}
        if "payload" in columns:
            return
        col_type = sa.LargeBinary().compile(dialect=io.dialect)
        # 表名含大写，建表时被SQLAlchemy加了引号，这里也必须按方言引用，
        # 否则PostgreSQL会把未引用的标识符折叠成小写而找不到表
        table_name = io.dialect.identifier_preparer.quote(cls.NOTIFY_TABLE_NAME)
        with io.begin() as conn:
            conn.execute(
                sa.text(f"ALTER TABLE {table_name} ADD COLUMN payload {col_type}")
            )
        logger.info(
            _("[💾SQL] 通知表 {table} 已补充 payload 列").format(
                table=cls.NOTIFY_TABLE_NAME
            )
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
    def index_value_channel(
        self, table_ref: TableReference, index_name: str, value: Any
    ) -> str:
        """
        只有声明了 point_sub 的索引才有值频道，否则抛 ValueError（见基类）。

        Only indexes declared with `point_sub` have value channels; raises `ValueError`
        otherwise (see the base class).
        """
        self.require_point_sub_(table_ref, index_name)
        return self.value_channel_(table_ref, index_name, value)

    def value_channel_(
        self, table_ref: TableReference, index_name: str, value: Any
    ) -> str:
        """`index_value_channel` 的内部形式，不检查声明（commit 用）。与 Redis 后端同一串
        名字；通知表 channel 列是 VARCHAR(256)，token 最长 64 字符"""
        dtype = table_ref.comp_cls.dtype_map_[index_name]
        token = sortable_token(to_sortable_bytes(dtype.type(value)))
        return f"{self.index_key(table_ref, index_name)}:{token}"

    @override
    def row_channel(self, table_ref: TableReference, row_id: int):
        return self.row_key(table_ref, row_id)

    @override
    def table_channel(self, table_ref: TableReference):
        return f"{self.cluster_prefix(table_ref)}{self.TABLE_CHANNEL_SUFFIX}"

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
            # SQLite后端默认日志模式(DELETE+FULL)对频繁小事务磁盘写入极重，
            # 挂connect监听切到WAL；异步engine要监听其底层sync_engine。
            # 其他方言(postgres/mysql)无需此PRAGMA。
            if io.dialect.name == "sqlite":
                event.listen(io, "connect", _apply_sqlite_pragmas)
                event.listen(aio.sync_engine, "connect", _apply_sqlite_pragmas)
            if aio.dialect.driver == "aiomysql":
                _patch_aiomysql_escape_bytes()
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
        # 本进程共享的通知表轮询器，首次 get_mq_client 时在事件循环里懒建
        self._hub: SQLNotifyHub | None = None

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
            self.ensure_notify_payload_column_sync(self.io)
        except sa_exc.DBAPIError as exc:
            if "already exists" in str(exc).lower():
                # 可能是并发创建导致的，忽略
                return
            raise

    @override
    def post_configure(
        self, components: Iterable[type[BaseComponent]] | None = None
    ) -> None:
        self._ensure_open()
        if not self.is_servant:
            self.ensure_support_tables_sync()
        # 提示用户schema定义是否符合sql要求
        self._schema_checking_for_sql(components)

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

        if self._hub is not None:
            hub, self._hub = self._hub, None
            await hub.close()
        for aio in self._async_ios:
            await aio.dispose()
        self._async_ios = []

    # define_component 已把 bool 字段强制转成 int8，组件 dtype 里不会出现 bool，用不到
    # @staticmethod
    # def _coerce_bool(value: Any) -> bool:
    #     if isinstance(value, bool):
    #         return value
    #     if isinstance(value, (int, np.integer)):
    #         return bool(value)
    #     if isinstance(value, str):
    #         return value.strip().lower() in {"1", "true", "t", "yes", "y"}
    #     return bool(value)

    @classmethod
    def _coerce_scalar(cls, dtype: np.dtype, value: Any) -> Any:
        if isinstance(value, np.generic):
            return value.item()
        if isinstance(value, memoryview):
            value = value.tobytes()

        dtype_type = dtype.type
        # bool 字段已被 define_component 转成 int8，此分支走不到
        # if np.issubdtype(dtype_type, np.bool_):
        #     return cls._coerce_bool(value)
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
            case RowFormat.STRUCT:
                return SQLBackendClient.rows_decode_(comp_cls, (row,))[0]
            case RowFormat.TYPED_DICT:
                struct_row = SQLBackendClient.rows_decode_(comp_cls, (row,))[0]
                return comp_cls.struct_to_dict(struct_row)
            case RowFormat.RAW:
                return SQLBackendClient._row_to_raw_dict(row)
            case _:
                raise ValueError(_("不可用的行格式: {fmt}").format(fmt=fmt))

    @classmethod
    def rows_decode_(
        cls, comp_cls: type[BaseComponent], rows: Iterable[dict[str, Any]]
    ) -> np.recarray:
        """
        把数据库读回的多行一次解码成 recarray，顺序与传入一致。`row_decode_` 的 STRUCT
        格式就是它的单行特例，解码规则只写在这里。
        """
        coerce = cls._coerce_scalar
        fields = list(comp_cls.dtype_map_.items())
        values = [
            tuple([coerce(dtype, row[name]) for name, dtype in fields]) for row in rows
        ]
        return np.array(values, dtype=comp_cls.dtypes).view(np.recarray)

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

    # 单条 IN 查询的参数上限，避免SQLite等数据库的参数数量限制
    GET_MANY_CHUNK = 500

    async def _select_many(
        self, table_ref: TableReference, ids: list[int]
    ) -> dict[int, dict[str, Any]]:
        """按 id 分块 IN 查询，返回 {id: 行}，读不到的 id 不在里面"""
        self._ensure_open()
        found: dict[int, dict[str, Any]] = {}
        if not ids:
            return found
        table = self.component_table(table_ref)
        async with self.aio.connect() as conn:
            for i in range(0, len(ids), self.GET_MANY_CHUNK):
                chunk = ids[i : i + self.GET_MANY_CHUNK]
                stmt = sa.select(table).where(table.c.id.in_(chunk))
                try:
                    rows = (await conn.execute(stmt)).mappings().all()
                except sa_exc.DBAPIError as exc:
                    if self._is_table_missing_error(exc):
                        break
                    raise
                for row in rows:
                    found[int(row["id"])] = dict(row)
        return found

    @override
    async def get_many(
        self,
        table_ref: TableReference,
        row_ids: Iterable[int],
        row_format: RowFormat = RowFormat.STRUCT,
    ) -> list[np.record | dict[str, str] | dict[str, Any] | None]:
        assert row_format != RowFormat.ID_LIST, "get_many不支持ID_LIST格式"
        ids = [int(i) for i in row_ids]
        found = await self._select_many(table_ref, ids)
        comp_cls = table_ref.comp_cls
        if row_format is RowFormat.STRUCT:
            batch = self.rows_decode_(comp_cls, [found[i] for i in ids if i in found])
            records = detach_rows_(batch)
            return [next(records) if i in found else None for i in ids]
        return [
            self.row_decode_(comp_cls, found[i], row_format) if i in found else None
            for i in ids
        ]

    @override
    async def get_many_array_(
        self, table_ref: TableReference, row_ids: list[int]
    ) -> tuple[np.recarray, list[int]]:
        ids = [int(i) for i in row_ids]
        found = await self._select_many(table_ref, ids)
        rows = self.rows_decode_(
            table_ref.comp_cls, [found[i] for i in ids if i in found]
        )
        return rows, [i for i in ids if i not in found]

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
        # bool 字段已被 define_component 转成 int8，此分支走不到
        # if np.issubdtype(dtype_type, np.bool_):
        #     return cls._coerce_bool(value)
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
            if type(left) not in (str, bytes) or type(right) not in (str, bytes):
                raise ValueError(
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

        # 边界值开头的 "(" / "[" 指定开/闭，默认闭区间。desc 时上面已把值换过来，li / ri
        # 跟着各自的值走（left 是上界、right 是下界），不能再交换
        left, li = peel_bound_(left)
        right, ri = peel_bound_(right)
        li = True if li is None else li
        ri = True if ri is None else ri

        left = cls._normalize_range_bound(dtype, left)
        right = cls._normalize_range_bound(dtype, right)
        return left, right, li, ri

    def _clamp_float_inf(
        self, dtype: np.dtype, left: Any, right: Any
    ) -> tuple[Any, Any]:
        """MySQL/MariaDB 不接受 ±inf 绑定参数（也存不下 inf），float 列的无穷边界钳到
        dtype 极值，语义不变；其他方言原样支持 inf，不动。"""
        if self.io.dialect.name != "mysql" or not np.issubdtype(
            dtype.type, np.floating
        ):
            return left, right
        limit = float(np.finfo(dtype).max)

        def clamp(x):
            if isinstance(x, float) and np.isinf(x):
                return limit if x > 0 else -limit
            return x

        return clamp(left), clamp(right)

    @staticmethod
    def clamp_uint64_bounds_(
        dtype: np.dtype, left: Any, right: Any, li: bool, ri: bool, desc: bool
    ) -> tuple[Any, Any, bool, bool]:
        """
        uint64 的区间边界收回 BIGINT 范围：超过 2**63-1 的值绑不进参数（inf 也会
        被 range_normalize_ 钳到 uint64 的最大值），库里也没有这么大的值，写入时
        就拒绝了。上界超了等于到头（闭区间），下界超了什么都查不到（开区间）。
        desc 时 left 是上界、right 是下界。
        """
        if dtype.kind != "u" or dtype.itemsize != 8:
            return left, right, li, ri
        lower, upper = ((right, ri), (left, li)) if desc else ((left, li), (right, ri))
        if upper[0] > _BIGINT_MAX:
            upper = (_BIGINT_MAX, True)
        if lower[0] > _BIGINT_MAX:
            lower = (_BIGINT_MAX, False)
        if desc:
            return upper[0], lower[0], upper[1], lower[1]
        return lower[0], upper[0], lower[1], upper[1]

    def _is_unique_violation(self, exc: sa_exc.IntegrityError) -> bool:
        message = str(exc).lower()
        markers = (
            "unique",
            "duplicate",
            "constraint failed",
            "duplicate entry",
        )
        return any(marker in message for marker in markers)

    async def _check_unique_conflicts(
        self,
        conn: AsyncConnection,
        dirties: dict[TableReference, Any],
        absent_by_ref: dict[TableReference, dict[int, set[str]]],
    ) -> None:
        """
        commit 事务内的显式唯一性检查，在 delete 之后、update / insert 之前执行（与 Redis Lua
        "checks 先于 pushes、本事务删除的行不算冲突"对齐：delete 已先执行，SELECT 自然看不到）。

        insert 行查全部 unique 列（含 id），update 行只查变更的 unique 列，按 (ref, field)
        分组各 SELECT 一次。update 行若查到的是自身（并发把本行改成了同值）则忽略，交给后面
        UPDATE 的版本条件报 RaceCondition。汇总全部冲突：任一"本事务曾 get 观察其不存在"的列
        → RaceCondition（RACE 优先，重试可解）；否则 → UniqueViolation（确定性，不重试）。
        SELECT 与写入之间被并发抢先的窗口仍由 IntegrityError → RaceCondition 兜底。
        """
        race: list[str] = []
        strict: list[str] = []
        for ref, (inserts, (old_rows, new_rows), _deletes) in dirties.items():
            comp_cls = ref.comp_cls
            dtype_map = comp_cls.dtype_map_
            absent_rows = absent_by_ref.get(ref, {})

            def _norm(field: str, value: Any, _dtype_map=dtype_map) -> Any:
                # 两侧都过一遍 dtype（float32 精度、np/py 标量）保证查回的值能对上本地 key
                dtype = _dtype_map[field]
                return dtype.type(self._coerce_scalar(dtype, value)).item()

            # {field: {normalized_value: (row_id, is_race, op)}}
            wanted: dict[str, dict[Any, tuple[int, bool, str]]] = {}
            for row in inserts:
                row_id = int(row["id"])
                absent = absent_rows.get(row_id, set())
                for field in sorted(comp_cls.uniques_):
                    wanted.setdefault(field, {})[_norm(field, row[field])] = (
                        row_id,
                        field in absent,
                        "insert",
                    )
            for old_row, changed in zip(old_rows, new_rows):
                row_id = int(old_row["id"])
                absent = absent_rows.get(row_id, set())
                for field in sorted(comp_cls.uniques_):
                    if field in changed:
                        wanted.setdefault(field, {})[_norm(field, changed[field])] = (
                            row_id,
                            field in absent,
                            "update",
                        )
            if not wanted:
                continue
            table = self.component_table(ref)
            for field, by_value in wanted.items():
                col = table.c[field]

                def _classify(
                    hit: tuple[int, bool, str],
                    found_id: Any,
                    _name: str = f"{comp_cls.name_}.{field}",
                ) -> None:
                    row_id, is_race, op = hit
                    if op == "update" and int(found_id) == row_id:
                        return  # 自身行：由 UPDATE 的版本条件报 Race
                    msg = f"Unique violation {_name} id={row_id} {op}"
                    (race if is_race else strict).append(msg)

                stmt = sa.select(table.c.id, col).where(col.in_(list(by_value)))
                unattributed = False
                for found_id, found_value in (await conn.execute(stmt)).all():
                    hit = by_value.get(_norm(field, found_value))
                    if hit is None:
                        unattributed = True
                        continue
                    _classify(hit, found_id)
                if unattributed:
                    # 数据库按自身相等语义（如 MariaDB 默认的大小写不敏感 collation）命中了，
                    # 但查回的值和本地哪个候选都对不上：逐个候选再问数据库，让它自己判定撞的
                    # 是谁。不能放过去交给 UNIQUE 约束兜底——约束报的 IntegrityError 会被当成
                    # RaceCondition 无限重试，而重试每次结果都一样
                    for value, hit in by_value.items():
                        probe = sa.select(table.c.id).where(col == value).limit(1)
                        found = (await conn.execute(probe)).first()
                        if found is not None:
                            _classify(hit, found[0])
        if race:
            raise RaceCondition("RACE: " + race[0])
        if strict:
            # 与 Redis 一致：同时存在两类冲突时竞态优先。前置版本 SELECT 只覆盖纯读行，
            # update 行的版本条件要到后面的 UPDATE 语句才检查，这里先核一遍：本事务读到的
            # 行已经被别人改过的话报 RaceCondition 让上层重试（重跑事务体可能就不写那个值了），
            # 而不是把确定性的 UniqueViolation 交给客户端
            await self._raise_if_updates_stale(conn, dirties)
            raise UniqueViolation("UNIQUE: " + strict[0])

    async def _raise_if_updates_stale(
        self, conn: AsyncConnection, dirties: dict[TableReference, Any]
    ) -> None:
        """update 态的行有版本对不上（被并发改过/删掉）的就抛 RaceCondition"""
        for ref, (_inserts, (old_rows, _new_rows), _deletes) in dirties.items():
            if not old_rows:
                continue
            table = self.component_table(ref)
            expected = {int(row["id"]): int(row["_version"]) for row in old_rows}
            stmt = sa.select(table.c.id, table.c._version).where(
                table.c.id.in_(list(expected))
            )
            found = {
                int(_id): int(_ver) for _id, _ver in (await conn.execute(stmt)).all()
            }
            for row_id, version in expected.items():
                actual = found.get(row_id)
                if actual is None or actual != version:
                    raise RaceCondition(
                        f"Version mismatch on updated row id={row_id} "
                        f"exp:{version} got:{actual}"
                    )

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
            self.ensure_notify_payload_column_sync(io)

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
        if row_format == RowFormat.ID_LIST:
            stmt = self._range_stmt(
                table_ref, index_name, left, right, limit, desc, True
            )
            async with self.aio.connect() as conn:
                try:
                    rows = (await conn.execute(stmt)).scalars().all()
                except sa_exc.DBAPIError as exc:
                    if self._is_table_missing_error(exc):
                        return []
                    raise
            return [int(x) for x in rows]

        stmt = self._range_stmt(table_ref, index_name, left, right, limit, desc, False)
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

    def _range_stmt(
        self,
        table_ref: TableReference,
        index_name: str,
        left: int | float | str | bytes | bool,
        right: int | float | str | bytes | bool | None,
        limit: int,
        desc: bool,
        id_only: bool,
    ) -> sa.Select:
        """range 的查询语句。commit 校验区间时用同样的参数重建，保证与读取时是同一条查询"""
        comp_cls = table_ref.comp_cls
        if index_name not in comp_cls.indexes_:
            raise ValueError(f"Component `{comp_cls.name_}` 没有索引 `{index_name}`")

        dtype = comp_cls.dtype_map_[index_name]
        left, right, li, ri = self.range_normalize_(dtype, left, right, desc)
        if (
            (cast(Any, left) < cast(Any, right))
            if desc
            else (cast(Any, right) < cast(Any, left))
        ):
            raise inverted_bounds_error_(*((right, left) if desc else (left, right)))
        left, right = self._clamp_float_inf(dtype, left, right)
        left, right, li, ri = self.clamp_uint64_bounds_(
            dtype, left, right, li, ri, desc
        )

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

        stmt = sa.select(table.c.id if id_only else table)
        stmt = stmt.where(cond_left, cond_right).order_by(*order_by)
        if limit >= 0:
            stmt = stmt.limit(limit)
        return stmt

    @override
    async def range_read_(
        self,
        table_ref: TableReference,
        index_name: str,
        left: int | float | str | bytes | bool,
        right: int | float | str | bytes | bool | None,
        limit: int,
        desc: bool,
    ) -> tuple[list[int], RangeObservation]:
        row_ids = cast(
            list[int],
            await self.range(
                table_ref, index_name, left, right, limit, desc, RowFormat.ID_LIST
            ),
        )
        # commit 时用同样的参数重跑这条查询，比对 id 集合（见 _check_range_observations）
        bounds = (left, right, limit, desc)
        return row_ids, RangeObservation(index_name, row_ids, bounds)

    async def _check_range_observations(
        self, conn: AsyncConnection, idmap: IdentityMap
    ) -> None:
        """
        commit 事务内的区间校验（防幻读）：本事务每次 range 读，用同样的参数重跑同一条
        查询，id 集合必须没变，否则 RaceCondition。要在任何写入之前执行，看到的才是本事务
        写入前的状态（与 Redis 的 checks 先于 pushes 对齐）。

        与 Redis 只比行数不同，这里比精确的 id 集合：截断读若按"(值, id) <= 最后一行"计数，
        得拿读回的列值做等值比较，MariaDB 的单精度 FLOAT 对不上，会变成永远失败的重试；
        精确集合也顺带覆盖了读取中途行被改走的情况，不用再核对读取一致性。

        只保护返回行的观察（get 命中）不重跑查询，只核对读回的行仍满足查询（取行前没被
        改走），与 Redis 核对 member 对齐。先在本地比较，比不上再以数据库的相等语义为准：
        行是数据库按它自己的规则命中的（如 MariaDB 默认大小写不敏感的 collation），Python
        的 != 不能直接判竞态，否则重试每次读到的都一样，会一直重试到上限。
        """
        for ref, observations in idmap.range_observations().items():
            for obs in observations:
                if obs.rows_only:
                    for row_id in obs.ids:
                        row = idmap.db_row(ref, row_id)
                        if (
                            row is None
                            or obs.point is None
                            or row[obs.index_name] == obs.point
                        ):
                            continue
                        # 用读取时的同一条查询加上 id 问数据库。库里这一行若已不是读到的
                        # 那一版，版本校验照样会判竞态，所以问现在的库就行
                        left, right, _limit, desc = obs.bounds
                        stmt = self._range_stmt(
                            ref, obs.index_name, left, right, 1, desc, True
                        )
                        probe = stmt.where(stmt.selected_columns.id == row_id)
                        if (await conn.execute(probe)).first() is None:
                            raise InconsistentRangeRead(
                                ref.comp_cls.name_, obs.index_name, row_id
                            )
                    continue
                left, right, limit, desc = obs.bounds
                stmt = self._range_stmt(
                    ref, obs.index_name, left, right, limit, desc, True
                )
                found = {int(x) for x in (await conn.execute(stmt)).scalars().all()}
                if found != set(obs.ids):
                    raise RaceCondition(
                        f"RACE: Range changed {ref.comp_cls.name_}.{obs.index_name}"
                    )

    def _dirty_to_typed_update(
        self, comp_cls: type[BaseComponent], dirty: dict[str, str | bytes]
    ) -> dict[str, Any]:
        ret: dict[str, Any] = {}
        for key, value in dirty.items():
            if key in {"id", "_version"}:
                continue
            ret[key] = self._coerce_scalar(comp_cls.dtype_map_[key], value)
        return ret

    def _dirty_to_typed_insert(
        self, comp_cls: type[BaseComponent], dirty: dict[str, str | bytes]
    ) -> dict[str, Any]:
        ret: dict[str, Any] = {}
        for key in comp_cls.prop_idx_map_:
            if key == "_version":
                continue
            ret[key] = self._coerce_scalar(comp_cls.dtype_map_[key], dirty[key])
        ret["_version"] = 1
        return ret

    @staticmethod
    def _reject_uint64_overflow(dirties: dict[TableReference, Any]) -> None:
        """
        uint64 超过 2**63-1 的值存不进 BIGINT 列：在执行任何语句之前明确拒绝，而不是让
        各数据库驱动报各自的溢出错误（整个事务什么都不写）。
        """
        for ref, (inserts, (_old_rows, new_rows), _deletes) in dirties.items():
            wide = [
                name
                for name, dtype in ref.comp_cls.dtype_map_.items()
                if dtype.kind == "u" and dtype.itemsize == 8
            ]
            if not wide:
                continue
            for row in (*inserts, *new_rows):
                for name in wide:
                    if name in row and int(row[name]) > _BIGINT_MAX:
                        raise ValueError(
                            _(
                                "{comp_name}.{field} 的值 {value} 超过了 SQL 后端"
                                "能存的上限 2**63-1：无符号整型存在 BIGINT 列里，"
                                "更大的 uint64 请用 Redis 后端"
                            ).format(
                                comp_name=ref.comp_name, field=name, value=row[name]
                            )
                        )

    @override
    async def commit(self, idmap: IdentityMap) -> None:
        self._ensure_open()
        assert not self.is_servant, _("从节点不允许提交事务")

        dirties = idmap.get_dirty_rows()
        if not dirties:
            raise ValueError(_("没有脏数据需要提交"))
        self._reject_uint64_overflow(dirties)
        # 本事务曾 get 观察"不存在"的 unique 列：{ref: {row_id: {field}}}，决定冲突判 RACE 还是 UNIQUE
        absent_by_ref = idmap.get_absent_unique_fields()
        # range 读时就发现行已被删，读到的不是任何一刻的区间，不用去数据库就能判竞态
        if located := idmap.inconsistent_range():
            raise InconsistentRangeRead(*located)

        notify_table = self.notify_table()
        now_ts = time.time()
        now_dt = datetime.now(UTC).replace(tzinfo=None)
        cleanup_due = now_ts >= self._next_notify_cleanup_at
        # 缺表时要建的表：写入的表，加上 range 读过的表（区间校验要查它）
        refs = list(dict.fromkeys([*dirties, *idmap.range_observations()]))

        def _enter_value(chans: set[str], ref: TableReference, index_name: str, value):
            """记一条索引值频道通知：有行"进入"了该 (索引, 值)（insert、或字段改成该值）。
            只给声明了 point_sub 的索引记（与 Redis 后端同语义）：离开由订阅者订着的行频道
            发现，不用记；没声明的索引没人订它的值频道"""
            if index_name in ref.comp_cls.point_subs_:
                chans.add(self.value_channel_(ref, index_name, value))

        for attempt in range(2):
            channels: set[str] = set()
            # 表级变更通知：ref -> 本事务变动的row_id列表。只有声明了 table_sub 的组件要用，
            # 别的组件不收集
            touched_ids: dict[TableReference, list[str]] = {}
            # 索引值频道通知（不带 payload），一个事务每个 (索引, 值) 一条
            value_chans: set[str] = set()
            try:
                async with self.aio.begin() as conn:
                    # 对纯读行加版本检查，防止事务依赖的陈旧读：
                    # 事务读到的某行，在提交前若被其他事务修改/删除，本事务应失败重试。
                    for ref, row_versions in idmap.get_clean_rows().items():
                        clean_table = self.component_table(ref)
                        clean_ids = [int(rid) for rid in row_versions.keys()]
                        clean_stmt = sa.select(
                            clean_table.c.id, clean_table.c._version
                        ).where(clean_table.c.id.in_(clean_ids))
                        clean_result = await conn.execute(clean_stmt)
                        found = {
                            int(_id): int(_ver) for _id, _ver in clean_result.all()
                        }
                        for row_id, expected_version in row_versions.items():
                            actual = found.get(int(row_id))
                            if actual is None or actual != int(expected_version):
                                raise RaceCondition(
                                    f"Version mismatch on read row id={int(row_id)} "
                                    f"exp:{expected_version} got:{actual}"
                                )

                    # range 读过的区间没变（防幻读），必须在任何写入之前
                    await self._check_range_observations(conn, idmap)

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
                            if ref.comp_cls.table_sub_:
                                touched_ids.setdefault(ref, []).append(str(row_id))

                    # 显式唯一性检查：delete 之后、update / insert 之前（见 _check_unique_conflicts）
                    await self._check_unique_conflicts(conn, dirties, absent_by_ref)

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
                                # 只有 _check_unique_conflicts 的 SELECT 与写入之间被并发
                                # 抢先才会到这里；重试后 SELECT 会给出确定判定，不会无限重试
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
                                    _enter_value(
                                        value_chans,
                                        ref,
                                        index_name,
                                        updates[index_name],
                                    )
                            if ref.comp_cls.table_sub_:
                                touched_ids.setdefault(ref, []).append(str(row_id))

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
                                # 只有 _check_unique_conflicts 的 SELECT 与写入之间被并发
                                # 抢先才会到这里；重试后 SELECT 会给出确定判定，不会无限重试
                                if self._is_unique_violation(exc):
                                    raise RaceCondition(
                                        f"UNIQUE violation: {exc}"
                                    ) from exc
                                raise
                            channels.add(self.row_channel(ref, row_id))
                            for index_name in ref.comp_cls.indexes_:
                                channels.add(self.index_channel(ref, index_name))
                                _enter_value(
                                    value_chans, ref, index_name, typed_row[index_name]
                                )
                            if ref.comp_cls.table_sub_:
                                touched_ids.setdefault(ref, []).append(str(row_id))

                    if channels:
                        notify_rows: list[dict[str, Any]] = [
                            {"channel": channel, "created_at": now_dt, "payload": None}
                            for channel in sorted(channels)
                        ]
                        # 表级频道：只给声明了 table_sub 的组件（touched_ids 只收集了它们），
                        # 一个事务一张表一条，payload为变动row_id列表
                        for ref, ids in touched_ids.items():
                            notify_rows.append(
                                {
                                    "channel": self.table_channel(ref),
                                    "created_at": now_dt,
                                    "payload": msgpack.packb(ids),
                                }
                            )
                        # 索引值频道：一个事务每个 (索引, 值) 一条，点查询订阅用，不带 payload
                        for channel in sorted(value_chans):
                            notify_rows.append(
                                {
                                    "channel": channel,
                                    "created_at": now_dt,
                                    "payload": None,
                                }
                            )
                        await conn.execute(sa.insert(notify_table), notify_rows)

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
                raise ValueError(
                    _("索引字段`{prop}`不允许用direct_set修改").format(prop=prop)
                )
            if prop not in table_ref.comp_cls.prop_idx_map_:
                raise ValueError(
                    _("Component `{comp_name}` 没有字段`{prop}`").format(
                        comp_name=table_ref.comp_name, prop=prop
                    )
                )

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
        """
        获取消息队列连接（每个用户连接一个）。本进程只有一个 `SQLNotifyHub`（一个通知表
        轮询任务）在首次调用时懒建，之后每次返回一个挂在它上面的轻量 MQClient。
        """
        self._ensure_open()
        from .mq import SQLMQClient, SQLNotifyHub

        if self._hub is None:
            self._hub = SQLNotifyHub(self)
        return SQLMQClient(self._hub)
