"""
无服务器进程的表直读写客户端（headless client）。

给**不是** HeTu 应用的可信内部进程用（如独立的战斗模拟进程）：不跑 Sanic、不定义 System、
不建簇、不发雪花号，只用 `Backend` + `Session` 直接读写由 HeTu 服务器建好的组件表。

写入走与 System 完全相同的 `Session.commit()` 提交路径，因此表级 PUBLISH、keyspace 通知、
乐观锁版本检查、unique 校验与服务器逐字节一致——客户端的订阅推送不区分行是 System 还是
headless 进程写的。**不做权限 / RLS 检查。**

Headless table client for trusted non-server processes: read / write component tables
created by a HeTu server through the exact same `Session.commit()` path Systems use, so
subscriptions fire identically. No Sanic, no Systems, no cluster building, no snowflake
ids, no permission / RLS checks.

用法 / Usage::

    import hetu.headless

    client = await hetu.headless.connect(
        backend_config,                      # config.yml 里 BACKENDS[x] 的 dict
        instance="my-region",
        components=[BattleCommand, "BattleReport"],   # 组件类或组件名，可混用
    )
    cmd_tbl = client.table(BattleCommand)
    rows = await cmd_tbl.servant_range("created_at", since, float("inf"), limit=4096)

    async with client.session(BattleReport) as s:
        async with s[BattleReport].upsert(id=-report_key) as row:   # 显式 id，不发号
            row.kind = 1
    await client.close()

@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0
"""

import copy
import json
import logging
from collections.abc import Iterable, Mapping, Sequence
from types import MappingProxyType
from typing import TYPE_CHECKING, Self

from .data.backend import Backend, Session, Table
from .data.component import BaseComponent
from .i18n import _

if TYPE_CHECKING:
    from .data.backend import SessionRepository

__all__ = [
    "ClusterChanged",
    "HeadlessClient",
    "HeadlessError",
    "HeadlessSession",
    "SchemaMismatch",
    "TableNotFound",
    "connect",
]

logger = logging.getLogger("HeTu.root")

ComponentSpec = type[BaseComponent] | str
"""组件的两种指定方式：本地组件类，或只给组件名（schema 取自服务器写入的表 meta）"""


class HeadlessError(Exception):
    """headless client 的异常基类 / Base class of headless client errors."""


class TableNotFound(HeadlessError):
    """组件表在后端不存在（没有 meta）。建表 / 迁移权归服务器，headless 不建表。

    The component table does not exist on the backend; headless never creates tables.
    """

    def __init__(self, instance: str, comp_name: str) -> None:
        self.instance = instance
        self.comp_name = comp_name
        super().__init__(
            _(
                "实例 {instance} 里不存在组件表 {comp_name}：请先启动 HeTu 服务器建表"
                "（或检查 instance / 组件名 / 后端配置是否指向同一个数据库）"
            ).format(instance=instance, comp_name=comp_name)
        )


class SchemaMismatch(HeadlessError):
    """本地组件类的数据布局（namespace、列名、dtype、unique、index）与服务器 meta 不一致。

    `diff` 逐条列出差异，方向为 ``服务器 -> 本地``。

    Local component class layout differs from the server-side table meta.
    """

    def __init__(self, comp_name: str, diff: list[str]) -> None:
        self.comp_name = comp_name
        self.diff = diff
        super().__init__(
            _(
                "组件 {comp_name} 的本地定义与服务器表结构不一致（服务器 -> 本地）：{diff}"
            ).format(comp_name=comp_name, diff="; ".join(diff))
        )


class ClusterChanged(HeadlessError):
    """`check_schema()` 发现表的 cluster_id 变了（服务器已迁簇），请重新 `connect`。

    Raised by `check_schema()` when the table's cluster id changed on the server.
    """

    def __init__(self, comp_name: str, old_id: int, new_id: int) -> None:
        self.comp_name = comp_name
        self.old_id = old_id
        self.new_id = new_id
        super().__init__(
            _(
                "组件 {comp_name} 的 cluster_id 已由 {old_id} 变为 {new_id}"
                "（服务器已迁簇），请重新 connect"
            ).format(comp_name=comp_name, old_id=old_id, new_id=new_id)
        )


def _schema_diff(
    local: type[BaseComponent], meta_json: str
) -> tuple[list[str], list[str]]:
    """比对本地组件类与服务器 meta 的定义，返回 ``(必须一致的差异, 只告警的差异)``。

    只比数据布局：``namespace``（SQL 表名含它）和 ``properties`` 的列名 / dtype / unique /
    index；``default`` 差异只告警；``permission / rls_compare / volatile / readonly /
    backend`` 与 headless 无关，忽略。差异行方向为 ``服务器 -> 本地``。
    """
    local_d = json.loads(local.json_)
    remote_d = json.loads(meta_json)
    errors: list[str] = []
    warns: list[str] = []
    if local_d["namespace"] != remote_d["namespace"]:
        errors.append(f"namespace: {remote_d['namespace']} -> {local_d['namespace']}")
    local_p = local_d["properties"]
    remote_p = remote_d["properties"]
    for col in sorted(set(local_p) | set(remote_p)):
        if col not in remote_p:
            errors.append(f"+ col {col} ({local_p[col]['dtype']})")
            continue
        if col not in local_p:
            errors.append(f"- col {col} ({remote_p[col]['dtype']})")
            continue
        for key in ("dtype", "unique", "index"):
            if local_p[col][key] != remote_p[col][key]:
                errors.append(
                    f"~ col {col}.{key}: {remote_p[col][key]} -> {local_p[col][key]}"
                )
        if local_p[col]["default"] != remote_p[col]["default"]:
            warns.append(
                f"~ col {col}.default: {remote_p[col]['default']!r} -> "
                f"{local_p[col]['default']!r}"
            )
    return errors, warns


def _comp_name(comp: ComponentSpec) -> str:
    return comp if isinstance(comp, str) else comp.name_


class HeadlessSession(Session):
    """headless 事务：`Session` 的子类，只多一个 ``s[Comp]`` 取 `SessionRepository`。

    组件白名单在 `HeadlessClient.session(*comps)` 时给定（并已校验同簇），``s[Comp]`` 只
    放行声明过的组件，其余一律 `KeyError`；同一组件返回同一个 repo，跨 `retry()` 复用。
    `retry()` / `commit()` / `discard()` 等全部继承自 `Session`。

    Session subclass adding ``s[Comp]`` → `SessionRepository`, restricted to the
    components declared in `HeadlessClient.session(...)`.
    """

    def __init__(
        self,
        backend: Backend,
        instance: str,
        cluster_id: int,
        comps: Iterable[type[BaseComponent]],
    ) -> None:
        super().__init__(backend, instance, cluster_id)
        self._comps: dict[str, type[BaseComponent]] = {c.name_: c for c in comps}
        self._repos: dict[str, SessionRepository] = {}

    def __getitem__(self, comp: ComponentSpec) -> SessionRepository:
        name = _comp_name(comp)
        comp_cls = self._comps.get(name)
        if comp_cls is None or (not isinstance(comp, str) and comp_cls is not comp):
            raise KeyError(
                _(
                    "组件 {name} 未在本事务声明（或不是 connect 时认下的那个类），"
                    "请在 client.session(...) 里加上；已声明：{declared}"
                ).format(name=name, declared=list(self._comps))
            )
        repo = self._repos.get(name)
        if repo is None:
            repo = self._repos[name] = self.using(comp_cls)
        return repo


class HeadlessClient:
    """无服务器进程的表直读写客户端，用 `connect()` 创建。

    - `table(comp)` → 现有 `Table`：非事务读走 `servant_get` / `servant_range` /
      `servant_get_many`（`direct_set` 绕过事务、也不发通知，请勿使用）。
    - `session(*comps)` → `HeadlessSession`：``s[Comp]`` 即现有 `SessionRepository`，
      退出 ``async with`` 即 commit，提交路径与 System 完全相同。
    - 不发雪花号：`insert` 的行 id 必须非零，`upsert` 只在锚定 ``id=<显式值>`` 时允许新建。
    - `check_schema()`：重读 meta，簇 / schema 变了就抛异常，由调用方重连或退出。

    Table client for trusted non-server processes; create with `connect()`.
    """

    def __init__(
        self,
        backend: Backend,
        instance: str,
        tables: Iterable[Table],
        *,
        explicit_ids_only: bool = True,
        owns_backend: bool = True,
    ) -> None:
        """底层构造：`tables` 已解析好（含 cluster_id）。一般用 `connect()` /
        `from_backend()`；`hetu.testing.Sandbox` 用本构造复用自己建好的表。

        explicit_ids_only: 事务是否禁止自动发雪花号（headless 恒 True；Sandbox False）。
        owns_backend: `close()` 是否一并关闭 backend。
        """
        self.backend = backend
        self.instance = instance
        self.explicit_ids_only = explicit_ids_only
        self._owns_backend = owns_backend
        self._tables: dict[str, Table] = {}
        for tbl in tables:
            if tbl.comp_name in self._tables:
                raise ValueError(_("组件 {name} 重复声明").format(name=tbl.comp_name))
            self._tables[tbl.comp_name] = tbl

    @staticmethod
    def resolve_tables_(
        backend: Backend, instance: str, components: Sequence[ComponentSpec]
    ) -> list[Table]:
        """内部方法：按服务器 meta 把组件解析成 `Table`（cluster_id 来自 meta，不本地重算）。

        传类 → 与 meta 比对数据布局，不一致抛 `SchemaMismatch`，通过则用本地类；
        传名字 → 用 meta 里的 schema 生成类（不注册进 `ComponentDefines`）。
        表不存在抛 `TableNotFound`，不建表。
        """
        if not components:
            raise ValueError(_("components 不能为空：至少声明一个组件"))
        maint = backend.get_table_maintenance()
        tables: list[Table] = []
        seen: set[str] = set()
        for comp in components:
            name = _comp_name(comp)
            if name in seen:
                raise ValueError(_("组件 {name} 重复声明").format(name=name))
            seen.add(name)
            meta = maint.read_meta(instance, name)
            if meta is None:
                raise TableNotFound(instance, name)
            if isinstance(comp, str):
                comp_cls = BaseComponent.load_json(meta.json)
            else:
                errors, warns = _schema_diff(comp, meta.json)
                if errors:
                    raise SchemaMismatch(name, errors)
                for warn in warns:
                    logger.warning(
                        _(
                            "⚠️ [🧩Headless] 组件 {name} 的默认值与服务器不同"
                            "（只影响本进程 new_row 的填充值）：{diff}"
                        ).format(name=name, diff=warn)
                    )
                comp_cls = comp
            tables.append(Table(comp_cls, instance, meta.cluster_id, backend))
            logger.info(
                _("[🧩Headless] 实例 {instance} 组件表 {name} → cluster {cid}").format(
                    instance=instance, name=name, cid=meta.cluster_id
                )
            )
        return tables

    @classmethod
    async def from_backend(
        cls, backend: Backend, instance: str, components: Sequence[ComponentSpec]
    ) -> Self:
        """用一个已有的 `Backend` 创建 client（不接管其生命周期，`close()` 不会关它）。

        Create a client on an existing `Backend`; the backend is not owned.
        """
        tables = cls.resolve_tables_(backend, instance, components)
        backend.post_configure([t.comp_cls for t in tables])
        return cls(backend, instance, tables, owns_backend=False)

    @property
    def tables(self) -> Mapping[str, Table]:
        """按组件名索引的全部已认表 / All resolved tables keyed by component name."""
        return MappingProxyType(self._tables)

    def table(self, comp: ComponentSpec) -> Table:
        """按组件类或组件名取 `Table`（非事务读用）。未声明的组件抛 `KeyError`。

        Get the `Table` of a declared component by class or name.
        """
        name = _comp_name(comp)
        tbl = self._tables.get(name)
        if tbl is None:
            raise KeyError(
                _(
                    "组件 {name} 未在 connect(components=...) 里声明；已声明：{declared}"
                ).format(name=name, declared=list(self._tables))
            )
        if not isinstance(comp, str) and tbl.comp_cls is not comp:
            raise KeyError(
                _(
                    "组件 {name} 是按名字声明的，其类来自服务器 meta，请用 "
                    "client.table('{name}').comp_cls 而不是本地类"
                ).format(name=name)
            )
        return tbl

    def session(
        self, *comps: ComponentSpec, only_master: bool = True
    ) -> HeadlessSession:
        """开一个事务，声明本事务会碰的组件（必须同簇，否则 `ValueError`）。

        only_master: 事务内读取（get / range）是否只走 master，默认 True——
        headless 写量小，读 replica 省不了什么，却会把复制延迟变成 `RaceCondition` 空转。
        轮询用的 `servant_*` 不受此影响。

        Open a transaction over the given (same-cluster) components.
        """
        if not comps:
            raise ValueError(_("session() 至少要声明一个组件"))
        tables = [self.table(c) for c in comps]
        cluster_ids = {t.cluster_id for t in tables}
        if len(cluster_ids) != 1:
            detail = ", ".join(f"{t.comp_name}->{t.cluster_id}" for t in tables)
            raise ValueError(
                _(
                    "同一事务的组件必须同簇（簇由服务器 System 的引用关系决定，"
                    "跨簇请拆成两个事务）：{detail}"
                ).format(detail=detail)
            )
        session = HeadlessSession(
            self.backend, self.instance, cluster_ids.pop(), [t.comp_cls for t in tables]
        )
        session.only_master = only_master
        session.explicit_ids_only = self.explicit_ids_only
        return session

    async def check_schema(self) -> None:
        """重读所有已认表的 meta，簇或数据布局变了就抛异常（`TableNotFound` /
        `ClusterChanged` / `SchemaMismatch`），不自动刷新——由调用方重新 `connect` 或退出。
        建议在租约 / 心跳循环里周期调用。

        Re-read table metas; raise if the cluster id or layout changed on the server.
        """
        maint = self.backend.get_table_maintenance()
        for name, tbl in self._tables.items():
            meta = maint.read_meta(self.instance, name)
            if meta is None:
                raise TableNotFound(self.instance, name)
            if meta.cluster_id != tbl.cluster_id:
                raise ClusterChanged(name, tbl.cluster_id, meta.cluster_id)
            errors, _warns = _schema_diff(tbl.comp_cls, meta.json)
            if errors:
                raise SchemaMismatch(name, errors)

    async def close(self) -> None:
        """关闭连接（仅当 backend 由本 client 创建时才关它）/ Close owned backend."""
        if self._owns_backend:
            await self.backend.close()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()


async def connect(
    backend_config: dict,
    instance: str,
    components: Sequence[ComponentSpec],
) -> HeadlessClient:
    """连接后端，按服务器写入的表 meta 认下 `components`，返回 `HeadlessClient`。

    不 import sanic、不需要 app 文件、不建簇、不建表、不初始化 `SnowflakeID`。
    `connect` / 使用 / `close` 必须在同一个 event loop 上（Redis 异步连接绑定 loop）。

    Parameters
    ----------
    backend_config: dict
        config.yml 里 ``BACKENDS[x]`` 那个 dict（``type`` / ``master`` / ``servants`` ...），
        Redis 与 SQL 都支持。
    instance: str
        服务器实例名（``INSTANCES`` 里的一个），表按实例隔离。
    components:
        组件类列表（本地定义须与服务器数据布局一致，否则 `SchemaMismatch`），或组件名
        （schema 取自服务器 meta，本地零定义，用 ``client.table(name).comp_cls`` 拿类），
        可混用。表不存在抛 `TableNotFound`。

    Connect to the backend and resolve the given components from server-written
    table metas. Never imports sanic, builds clusters, creates tables or mints ids.
    """
    backend = Backend(copy.deepcopy(backend_config))
    try:
        tables = HeadlessClient.resolve_tables_(backend, instance, components)
        backend.post_configure([t.comp_cls for t in tables])
    except BaseException:
        await backend.close()
        raise
    return HeadlessClient(backend, instance, tables)
