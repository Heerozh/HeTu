"""
进程内的 HeTu 应用运行时：不跑 Sanic、不收连接，直接连配置好的后端跑 System、读写组件表。

`hetu call` / `hetu shell` 用它调试运行中的游戏；`hetu.testing.Sandbox` 是它的子类（临时
SQLite + 建表），所以单测和线上调试是同一套 API：`call_system` / `get` / `must_get` /
`range` / `insert` / `upsert`。

写入走与 System 完全相同的 `Session.commit()`，在线客户端的订阅推送与正常调用没有区别。
**不做权限 / RLS 检查**——能用它的进程手里本来就有数据库地址和口令。

In-process HeTu app runtime: runs Systems and reads / writes component tables against the
configured backends without Sanic or connections. Used by `hetu call` / `hetu shell`;
`hetu.testing.Sandbox` subclasses it, so unit tests and live debugging share one API.

用法 / Usage::

    from hetu.local import open_local_app

    app = await open_local_app(config)            # config.yml 读出来的 dict
    try:
        await app.call_system("add_gold", 1001, 500)
        row = await app.get("Player", owner=1001)
    finally:
        await app.aclose()

@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2025, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import importlib.util
import os
import sys
import time
from contextlib import asynccontextmanager
from types import ModuleType
from typing import TYPE_CHECKING, Any, Self

import msgspec

from .common.permission import Permission
from .data.backend import Backend
from .data.backend.snowflake_lease import SnowflakeLease
from .data.backend.worker_keeper import WorkerLease, create_worker_keeper
from .endpoint.response import RejectResponse, ResponseToClient
from .headless import HeadlessClient
from .i18n import _
from .manager import ComponentTableManager
from .system import SystemClusters, SystemContext
from .system.caller import SystemCaller

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable

    import numpy as np

    from .data.backend import Table
    from .data.component import BaseComponent
    from .system.definer import SystemDefine

__all__ = [
    "BackendNotReady",
    "IdentityRequired",
    "LocalApp",
    "TableNotReady",
    "load_app_module",
    "open_local_app",
    "resolve_identity",
]


class TableNotReady(Exception):
    """组件表在库里不存在，或与本地代码的表结构 / 簇不一致。不建表、不迁移，交给服务器和
    `hetu upgrade`。

    The component table is missing or its schema / cluster differs from the local code.
    """

    def __init__(self, comp_name: str, status: str) -> None:
        self.comp_name = comp_name
        self.status = status
        if status == "not_exists":
            hint = _("库里没有这张表：先用当前代码启动一次服务器（会建新表）")
        else:
            hint = _(
                "本地代码与库里的表结构 / 簇不一致：先 hetu upgrade，"
                "或用与线上一致的代码版本运行"
            )
        super().__init__(
            _("组件 {comp_name} 的表状态为 {status}，{hint}").format(
                comp_name=comp_name, status=status, hint=hint
            )
        )


class BackendNotReady(Exception):
    """后端还不能用（如 SQLite 库文件不存在）。不新建库文件。

    The backend is not usable yet (e.g. the SQLite database file does not exist).
    """


class IdentityRequired(ValueError):
    """USER 权限的 System 必须显式指定玩家身份（caller）。

    生产里 USER 端点不放行未登录（caller=0）的连接；默认用 caller=0 跑，会让
    ``upsert(owner=ctx.caller)`` 这类写法在库里悄悄建出 owner=0 的行。
    """

    def __init__(self, system: str) -> None:
        self.system = system
        super().__init__(
            _(
                "System {system} 是 USER 权限：生产里只有已登录连接能调，请指定玩家 id"
                "（hetu call 用 --as <uid>，代码里传 caller=）"
            ).format(system=system)
        )


def load_app_module(app_file: str) -> ModuleType:
    """按路径加载 app 文件（注册其中的 `@define_*`），与服务器相同：模块名 ``HeTuApp``。"""
    spec = importlib.util.spec_from_file_location("HeTuApp", app_file)
    if spec is None or spec.loader is None:
        raise ImportError(
            _("无法加载app文件 {app_file}").format(app_file=app_file), path=app_file
        )
    module = importlib.util.module_from_spec(spec)
    sys.modules["HeTuApp"] = module
    spec.loader.exec_module(module)
    return module


def resolve_identity(
    sys_def: SystemDefine | None, caller: int | None, group: str | None
) -> tuple[int, str]:
    """
    没给全身份时按 System 的 permission 推默认值（严格规则，`LocalApp` 用）：

    - None（内部 System）/ ADMIN / GM：caller=0，group="admin"（GM 端点也放行 admin）
    - EVERYBODY：caller=0，group="guest"（等同匿名连接）
    - USER：必须给 caller，否则 `IdentityRequired`
    - 给了 caller 没给 group：group="guest"（模拟真实玩家）

    Resolve the default identity from the System's permission (strict rules).
    """
    permission = sys_def.permission if sys_def is not None else None
    if caller is None:
        if permission == Permission.USER and sys_def is not None:
            raise IdentityRequired(sys_def.func.__name__)
        if group is None:
            if permission == Permission.EVERYBODY:
                group = "guest"
            else:
                group = "admin"
        return 0, group
    return caller, "guest" if group is None else group


class _AppTableClient(HeadlessClient):
    """`LocalApp.client`：取表时先过 `LocalApp` 的按需表结构校验"""

    def __init__(self, app: LocalApp, backend: Backend, tables: Iterable[Table]):
        super().__init__(
            backend,
            app.instance_name,
            tables,
            explicit_ids_only=False,
            owns_backend=False,
        )
        self._app = app

    def table(self, comp: type[BaseComponent] | str) -> Table:
        tbl = super().table(comp)
        self._app.verify_table(tbl)
        return tbl


class _AppSystemCaller(SystemCaller):
    """跑 System 前先校验它引用的全部组件表（嵌套的 ctx.systems.call 用的也是它）"""

    def __init__(self, app: LocalApp, context: SystemContext):
        super().__init__(app.namespace, app.tbl_mgr, context)
        self._app = app

    async def call_(self, sys: SystemDefine, *args, uuid: str = "") -> Any:
        self._app.verify_components(sys.full_components)
        return await super().call_(sys, *args, uuid=uuid)


class LocalApp:
    """
    进程内的 HeTu 应用运行时：簇已建好、后端已连好，提供与 `Sandbox` 相同的调用 / 读写 API。

    生产 / 开发库请用 `open_local_app(config)` 打开；直接构造用于已自备 backends / tbl_mgr
    的场景（`Sandbox` 即是）。

    In-process app runtime; open it with `open_local_app(config)`.
    """

    namespace: str
    instance_name: str
    backends: dict[str, Backend]
    tbl_mgr: ComponentTableManager
    client: HeadlessClient
    """表直读写客户端：`get`/`range`/`insert`/`upsert` 都经它；多表事务用 `client.session`"""
    address: str
    """作为 `ctx.address`：hetu call / shell 为 "cli"，Sandbox 为 "sandbox\""""
    lease: SnowflakeLease | None
    """发号租约（`open_local_app(mint_ids=True)` 时持有），`aclose()` 时释放"""

    def __init__(
        self,
        namespace: str,
        instance_name: str,
        backends: dict[str, Backend],
        tbl_mgr: ComponentTableManager,
        *,
        verify_tables: bool,
        address: str = "local",
    ) -> None:
        """
        verify_tables: 是否在每张表第一次用到时核对它的表结构与簇（`check_table`），不一致抛
            `TableNotReady`。用户的库要开；自己刚建好表的（Sandbox）不用。
        """
        self.namespace = namespace
        self.instance_name = instance_name
        self.backends = backends
        self.tbl_mgr = tbl_mgr
        self.address = address
        self.lease = None
        self._verify_tables = verify_tables
        self._verified: set[str] = set()
        default = backends.get("default") or next(iter(backends.values()))
        self.client = _AppTableClient(
            self, default, [tbl for _comp, tbl in tbl_mgr.items()]
        )
        # 与 server pipeline 同款 msgpack codec（见 hetu/server/pipeline/jsonb.py），
        # 用于模拟真实 wire 序列化往返（见 _wire_roundtrip）。
        self._msg_encoder = msgspec.msgpack.Encoder()
        self._msg_decoder = msgspec.msgpack.Decoder()

    # ============ 表结构校验 ============

    def verify_table(self, tbl: Table) -> None:
        """第一次用到这张表时核对表结构与簇（结果缓存），不一致抛 `TableNotReady`。

        按需而不是开局全量：只为碰到的表读 meta；正在改的无关组件不挡路。本地簇编号整体
        漂移时，碰到的表自己的 cluster_id 也对不上，照样拦得住。
        """
        if not self._verify_tables or tbl.comp_name in self._verified:
            return
        status, _meta = tbl.backend.get_table_maintenance().check_table(tbl)
        if status != "ok":
            raise TableNotReady(tbl.comp_name, status)
        self._verified.add(tbl.comp_name)

    def verify_components(self, comps: Iterable[type[BaseComponent]]) -> None:
        """校验一组组件的表（System 调用前对 full_components 调用）"""
        if not self._verify_tables:
            return
        for comp in comps:
            tbl = self.tbl_mgr.get_table(comp)
            if tbl is not None:
                self.verify_table(tbl)

    # ============ System 调用 ============

    def resolve_identity(
        self, system: str, caller: int | None, group: str | None
    ) -> tuple[int, str]:
        """没给全身份时的默认值，见模块函数 `resolve_identity`（严格规则）。"""
        return resolve_identity(SystemClusters().get_system(system), caller, group)

    def new_context(
        self, caller: int, group: str, user_data: dict | None = None
    ) -> SystemContext:
        """建一个直接跑 System 用的上下文：没有连接（connection_id=0、request=None）。

        user_data 传入则作为 `ctx.user_data`（同一 dict 对调用方可见）。
        """
        ctx = SystemContext(
            caller=caller,
            connection_id=0,
            address=self.address,
            group=group,
            user_data=user_data if user_data is not None else {},
            # 绕过了Endpoint层，所以这里代替它打上请求时间戳，与生产的ctx.timestamp一致
            timestamp=time.time(),
            request=None,  # type: ignore[arg-type]
            systems=None,  # type: ignore[arg-type]
        )
        ctx.systems = _AppSystemCaller(self, ctx)
        return ctx

    async def call_system(
        self,
        system: str,
        *args: Any,
        caller: int | None = None,
        group: str | None = None,
        user_data: dict | None = None,
        uuid: str = "",
        raw: bool = False,
    ) -> Any:
        """绕过 Endpoint 层、以 `caller` / `group` 身份直接跑一个 System，默认返回 client SDK
        实际收到的 payload。

        没给身份时按 `resolve_identity` 推默认值（`Sandbox` 覆盖为宽松规则：caller=0、
        group="guest"）。

        默认（`raw=False`）按 server `receiver.rpc()` 的 framing 处理 System 返回值，
        并过一遍与生产同款的 msgpack 序列化往返（见 `to_client_payload`），以暴露在
        真实 wire 上才会出现的问题：

        - System 返回 `ResponseToClient(msg)` → 返回 msgpack 往返后的 `msg`；
          不可序列化的 payload（如 numpy 标量、自定义对象）会在此抛 `TypeError`，
          `tuple`/`set` 会如实变成 `list`，与 client 实际收到的一致。
        - System 返回 `None` 或任意普通值 → 返回字符串 `"ok"`（普通返回值在 wire 上
          被无视，仅用于 System 间嵌套调用）。
        - System 返回 `RejectResponse` → 原样返回该对象。

        `raw=True` 时跳过上述处理，原样返回 System 的返回值。

        内部会开事务、自动在 `RaceCondition` 时重试。

        Run a System as `caller`/`group`; by default returns what the client SDK actually
        receives. Pass `raw=True` for the System's untouched return value.
        """
        caller, group = self.resolve_identity(system, caller, group)
        ctx = self.new_context(caller, group, user_data)
        rtn = await ctx.systems.call(system, *args, uuid=uuid)
        if raw:
            return rtn
        return self.to_client_payload(rtn)

    def _wire_roundtrip(self, message: Any) -> Any:
        """把一条 message 按 server `receiver.rpc()` 的 ``["rsp", message]`` framing 过一遍
        与生产同款的 msgpack codec（见 `hetu/server/pipeline/jsonb.py`），返回 client SDK
        实际收到的裸 message。

        不可序列化的 payload（如 numpy 标量、自定义对象）会在此抛 `TypeError`（与生产
        wire 一致），`tuple`/`set` 会如实变成 `list`。
        """
        decoded = self._msg_decoder.decode(self._msg_encoder.encode(["rsp", message]))
        return decoded[1]

    def to_client_payload(self, rtn: Any) -> Any:
        """把 System 返回值按 server `receiver.rpc()` 的 framing + 真实 msgpack 往返，
        返回 client SDK 实际收到的 payload。不可序列化的 payload 会在此抛 `TypeError`。
        """
        if isinstance(rtn, RejectResponse):
            # 软拒绝在 wire 上是 ["rej", name, code]，由 Endpoint guard 产生；原样返回该对象
            return rtn
        # framing 与 receiver.rpc() 对齐：ResponseToClient → message，其余（含 None /
        # 普通返回值）→ "ok"。
        message = rtn.message if isinstance(rtn, ResponseToClient) else "ok"
        return self._wire_roundtrip(message)

    # ============ 组件表直读写 ============

    def _resolve_table(self, comp: Any) -> Table:
        """把 Component 类或名字字符串解析为本应用的 `Table`。"""
        try:
            return self.client.table(comp)
        except KeyError as e:
            raise ValueError(
                _("找不到 Component：{comp}（是否在 components 中引用过？）").format(
                    comp=repr(comp)
                )
            ) from e

    async def get(self, comp: Any, **query: Any) -> np.record | None:
        """按 unique/index 字段读一行；无则返回 None（语义同 `repo.get`）。

        `comp` 可传 Component 类或其名字字符串；`query` 只允许一个带索引的字段，
        如 `get("Player", owner=1234)`。已知行必然存在时，改用 `must_get` 可免去判空。
        """
        table = self._resolve_table(comp)
        async with self.client.session(table.comp_cls) as session:
            return await session[table.comp_cls].get(**query)

    async def must_get(self, comp: Any, **query: Any) -> np.record:
        """同 `get`，但断言该行存在：命中返回该行，未命中抛 `LookupError`。"""
        row = await self.get(comp, **query)
        if row is None:
            raise LookupError(
                _("{comp} 中不存在匹配 {query} 的行").format(comp=comp, query=query)
            )
        return row

    async def range(
        self,
        comp: Any,
        index_name: str | None = None,
        _left: Any = None,
        _right: Any = None,
        limit: int = 10,
        desc: bool = False,
        **kwargs: Any,
    ) -> np.recarray:
        """按索引区间读多行。默认闭区间 `[left, right]`。

        签名与 `repo.range` 对齐，两种形态都支持：位置参数
        `range(comp, "value", 1.0, 2.0)`，或 kwarg 区间 `range(comp, value=(1.0, 2.0))`。
        `comp` 可传 Component 类或其名字字符串；索引字段必须带 index/unique。
        `limit` 默认 10（与 `repo.range` 一致，注意超出会静默截断），负数表示不限制；
        `desc=True` 降序。返回 `numpy.recarray`（c-struct array），无数据时为空数组。
        """
        table = self._resolve_table(comp)
        async with self.client.session(table.comp_cls) as session:
            return await session[table.comp_cls].range(
                index_name, _left, _right, limit=limit, desc=desc, **kwargs
            )

    async def insert(self, comp: Any, **fields: Any) -> int:
        """插入一行并返回其 `id`，省去手写 `new_row()` + `repo.insert(row)` 的样板。

        只需给关心的字段，其余字段保留组件默认值。传 `id=` 可指定主键，否则自动生成
        雪花 id（返回值即该 id）。`_version` 由引擎管理，不能设置。

        `comp` 可传 Component 类或其名字字符串；重复 unique 会抛 `UniqueViolation`。
        直接落库（绕过 System / 不做权限检查）。
        """
        table = self._resolve_table(comp)
        comp_cls = table.comp_cls
        valid = set(comp_cls.prop_idx_map_)  # 全部字段名（含 id/_version）
        row = comp_cls.new_row(id_=fields.pop("id", None))
        for name, value in fields.items():
            if name not in valid:
                raise ValueError(
                    _("{comp} 组件没有叫 {name} 的字段").format(
                        comp=comp_cls.name_, name=name
                    )
                )
            if name == "_version":
                raise ValueError(_("_version 由引擎管理，insert 时不能设置"))
            row[name] = value
        async with self.client.session(comp_cls) as session:
            await session[comp_cls].insert(row)
        return int(row.id)

    @asynccontextmanager
    async def upsert(self, comp: Any, **anchor: Any) -> AsyncIterator[np.record]:
        """以 `async with` 语法 upsert 一行，镜像 `repo.upsert`：按 unique 字段锚定
        查询，块内修改字段，退出块时自动 update/insert 并 commit。

        用法 / Usage::

            async with app.upsert(RLSComp, owner=1001) as row:
                row.value = 50

        `anchor` 只能给一个 **unique** 字段（如 `owner=...`/`id=...`）。直接落库。
        """
        comp_cls = self._resolve_table(comp).comp_cls
        async with (
            self.client.session(comp_cls) as session,
            session[comp_cls].upsert(**anchor) as row,
        ):
            yield row

    # ============ 生命周期 ============

    async def aclose(self) -> None:
        """释放发号租约（写精确水位），再关闭全部后端连接。"""
        lease, self.lease = self.lease, None
        try:
            if lease is not None:
                await lease.__aexit__(None, None, None)
        finally:
            for backend in {id(b): b for b in self.backends.values()}.values():
                await backend.close()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()


def _sqlite_path(db_cfg: dict) -> str | None:
    """SQLite 后端配置的库文件路径；不是 SQLite 返回 None"""
    if str(db_cfg.get("type", "")).lower() != "sqlite":
        return None
    from .data.backend.sqlite.client import SQLiteBackendClient

    return SQLiteBackendClient.parse_dsn(db_cfg["master"])


async def open_local_app(
    config: dict,
    *,
    instance: str | None = None,
    mint_ids: bool = True,
    address: str = "local",
) -> LocalApp:
    """
    按配置打开一个进程内应用：加载 APP_FILE、建簇、连上全部后端，`mint_ids` 时租 worker id。

    **不建表、不迁移**：每张表第一次用到时核对表结构与簇，不一致抛 `TableNotReady`。
    不新建 SQLite 库文件（不存在抛 `BackendNotReady`）。

    Parameters
    ----------
    config: dict
        config.yml 读出来的 dict（`APP_FILE` 已解析成可用路径），至少要有 ``APP_FILE`` /
        ``NAMESPACE`` / ``INSTANCES`` / ``BACKENDS``。
    instance: str
        实例名，默认 ``INSTANCES[0]``。
    mint_ids: bool
        是否初始化雪花 ID（要跑会 insert 的 System / 写入就要）。会租一个 worker id（工具进程
        模式：Redis 从 1023 往下，SQLite 在预留段里），接上 ``INSTANCES[0]`` 的时间戳高水位，
        起续约 / 预留水位两个后台任务；`aclose()` 时写精确水位并释放。
    address: str
        作为 `ctx.address`。

    Open an in-process app from a config dict. Never creates or migrates tables.
    """
    namespace = config["NAMESPACE"]
    instances: list[str] = list(config["INSTANCES"])
    instance = instance or instances[0]
    if instance not in instances:
        raise ValueError(
            _("实例 {instance} 不在配置的 INSTANCES 里：{instances}").format(
                instance=instance, instances=instances
            )
        )

    # 1. 加载 app、建簇（进程内只建一次）。WorkerLease 已随本模块 import 注册：服务器进程
    #    也注册了它，两边算出的簇编号才一致
    clusters = SystemClusters()
    if clusters.get_clusters(namespace) is None:
        load_app_module(config["APP_FILE"])
        clusters.build_clusters(namespace)
    elif clusters.main_namespace != namespace:
        clusters.switch_main(namespace)

    # 2. 后端，第一个为 default（同 start_backends）。不新建 SQLite 库文件
    for db_cfg in config["BACKENDS"].values():
        path = _sqlite_path(db_cfg)
        if path is not None and not os.path.exists(path):
            raise BackendNotReady(
                _(
                    "库文件不存在：{path}（服务器还没在这个库上启动过，或配置路径不对）"
                ).format(path=os.path.abspath(path))
            )
    backends: dict[str, Backend] = {}
    try:
        for name, db_cfg in config["BACKENDS"].items():
            backends[name] = Backend(db_cfg)
            backends.setdefault("default", backends[name])
        unique_backends = {id(b): b for b in backends.values()}.values()

        # 3. 表管理器（不建表）；只配置 master 一侧：不订阅，不需要 servant 的 keyspace 配置
        tbl_mgr = ComponentTableManager(namespace, instance, backends)
        for backend in unique_backends:
            backend.post_configure(servants=False)

        app = LocalApp(
            namespace, instance, backends, tbl_mgr, verify_tables=True, address=address
        )

        # 4. 发号租约：水位表固定用 INSTANCES[0] 的 WorkerLease，和服务器同一张
        if mint_ids:
            lease_mgr = (
                tbl_mgr
                if instance == instances[0]
                else ComponentTableManager(namespace, instances[0], backends)
            )
            lease_tbl = lease_mgr.get_table(WorkerLease)
            assert lease_tbl is not None
            status, _meta = lease_tbl.backend.get_table_maintenance().check_table(
                lease_tbl
            )
            if status != "ok":
                raise TableNotReady(lease_tbl.comp_name, status)
            lease = SnowflakeLease(
                create_worker_keeper(lease_tbl.backend, os.getpid(), tool=True),
                lease_tbl,
            )
            await lease.__aenter__()
            app.lease = lease
        return app
    except BaseException:
        for backend in {id(b): b for b in backends.values()}.values():
            await backend.close()
        raise
