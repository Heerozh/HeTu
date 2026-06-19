"""
进程内 HeTu 应用测试沙盒（SQLite 临时文件），用于游戏包对自己的 `@define_system`
与 `@define_endpoint` 做单元测试 / TDD。

In-process HeTu application test sandbox (SQLite temp file), for game packages to
unit-test their own `@define_system` / `@define_endpoint` logic.

`call` 走 Endpoint 正常路径（权限/guard/elevate 全过），`call_system` 绕过 Endpoint 层
直接跑 System。只覆盖"进程内 SQLite"的单测场景；多后端参数化（Redis/Valkey/Postgres）
与 docker 服务编排保留在 HeTu 内部 fixture，不在此模块范围。

@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0
"""

import importlib
import warnings
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, Literal

import msgspec

from ..common.snowflake_id import SnowflakeID
from ..data.backend import Backend
from ..data.component import ComponentDefines
from ..endpoint.connection import elevate
from ..endpoint.definer import EndpointDefines
from ..endpoint.executor import EndpointExecutor
from ..endpoint.response import RejectResponse, ResponseToClient
from ..manager import ComponentTableManager
from ..system import SystemClusters, SystemContext
from ..system.caller import SystemCaller

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    import numpy as np

    from ..data.backend import Table

__all__ = ["CallRejected", "ConnectionClosed", "Sandbox", "sandbox_fixture"]


class CallRejected(Exception):
    """`Sandbox.call` 软拒绝：endpoint 的 guard `raise ClientReject` → 服务器回 rej 帧。

    `code` 即客户端收到的 rej code（如 ``"RATE_LIMITED"``/自定义），用 `pytest.raises`
    断言；连接不断开（软拒绝）。`reason` 仅服务端诊断用，不发给客户端。

    Soft reject raised by `Sandbox.call` when an endpoint guard raises `ClientReject`.
    """

    def __init__(self, code: str, reason: str | None = None) -> None:
        self.code = code
        self.reason = reason
        super().__init__(reason or code)


class ConnectionClosed(Exception):
    """`Sandbox.call` 非法调用：服务器会关闭连接，对应客户端实际被断开。

    触发原因：endpoint 不存在、权限不符、参数个数不对、连接被踢，或 endpoint 内部抛异常。
    注意 executor 会吞掉 endpoint 内部异常只回失败——想看底层 System 的真实异常、或断言
    内部计算结果，请改用 `call_system`（不走 Endpoint 层、不吞异常）。

    Raised by `Sandbox.call` when the gateway rejects the call and would drop the
    connection (bad permission/args, unknown endpoint, kicked, or endpoint raised).
    """


class Sandbox:
    """进程内 HeTu 应用沙盒（SQLite 临时文件），用于单测 System / Endpoint。

    用法 / Usage::

        import my_game_pkg
        from hetu.testing import Sandbox

        async with await Sandbox.create("my_game", my_game_pkg, db_path=str(tmp)) as sb:
            await sb.insert(PlayerInfo, owner=1001, name="Alice")  # 直接喂初始行
            ret = await sb.call("store_player", "Bob", caller=1002)  # 走 Endpoint 正常路径
            row = await sb.get("PlayerInfo", owner=1002)
            assert row.name == "Bob"

    两种调用入口 / Two call entries:

    - `call`：像客户端那样走 **Endpoint 正常路径**（分配连接 → 权限/参数校验 → 调用前
      guard → 执行 → `receiver.rpc()` framing）。能调纯 `@define_endpoint`，也能调 System
      自动生成的 endpoint；`caller` 非 0 时先 `elevate` 模拟已登录。成功返回 client 实际
      收到的 payload；guard 软拒绝抛 `CallRejected(code)`；权限不符/参数错/endpoint 内部
      抛异常等非法调用抛 `ConnectionClosed`。
    - `call_system`：**绕过 Endpoint 层**直接运行 System（等同可信内部调用，不做权限/
      guard/登录检查）。默认返回 client payload，`raw=True` 拿 System 原始返回值；适合只
      想断言 System 内部逻辑/计算结果。

    两者的成功返回都过一遍与生产同款的 msgpack 往返（不可序列化的返回会在此抛错，与生产
    wire 一致）。`insert`/`upsert` 直接喂初始行（绕过 System，便于 seeding），`get`/`range`
    直接读回组件表；二者都不做 RLS 检查。

    注意 / Notes
    -----
    - 同一测试进程只支持一个 app/namespace（注册表为全局单例）。
    - `call_system` 调用的 System 必须引用至少 1 个 Component（引擎限制）。
    - `call` 每次都是独立的新连接、调用结束即断开，不跨调用累积 guard/限流状态；要验证
      `@rate_limit` 的跨调用计数请走集成测试（与不测 slowapi 同理，限流本身是 HeTu 的事）。
    """

    namespace: str
    instance_name: str
    backend: Backend
    tbl_mgr: ComponentTableManager

    def __init__(
        self,
        namespace: str,
        instance_name: str,
        backend: Backend,
        tbl_mgr: ComponentTableManager,
    ) -> None:
        """一般不直接调用，请用 `Sandbox.create(...)`。直接构造用于已自备 backend/tbl_mgr
        的高级场景。"""
        self.namespace = namespace
        self.instance_name = instance_name
        self.backend = backend
        self.tbl_mgr = tbl_mgr
        # 与 server pipeline 同款 msgpack codec（见 hetu/server/pipeline/jsonb.py），
        # 用于 call/call_system 模拟真实 wire 序列化往返（见 _wire_roundtrip）。
        self._msg_encoder = msgspec.msgpack.Encoder()
        self._msg_decoder = msgspec.msgpack.Decoder()

    @classmethod
    async def create(
        cls,
        namespace: str,
        app_module: Any,
        *,
        db_path: str,
        instance_name: str = "test",
        worker_id: int = 1,
        reload_app: bool = False,
    ) -> "Sandbox":
        """拉起一个 SQLite 后端的应用沙盒，注册 namespace 的 Component/System，建表。

        Parameters
        ----------
        namespace: str
            要测试的 app namespace。
        app_module
            含 `@define_*` 装饰器的已 import 模块/包（如 `import my_game_pkg`）。
        db_path: str
            SQLite 文件路径（务必是文件，不能是 `:memory:`，否则跨 session 读不到数据）。
        instance_name: str
            实例名，默认 "test"。
        worker_id: int
            SnowflakeID 的 worker id，默认 1。
        reload_app: bool
            默认 False（build-once）：仅当该 namespace 还没构建过时才构建注册表。在干净
            进程中不会 `importlib.reload`，适用于多模块游戏包，且无类身份错位问题。
            （若检测到注册表被其他 namespace 污染——如 HeTu 自身共享测试套件——会自动
            清空并 reload 以恢复，此恢复路径同样仅对单文件 app 完全可靠。）
            设 True 时总是清空注册表并 `importlib.reload(app_module)` 强制重载——仅适用
            于单文件 app（如 HeTu 自带 tests/app.py），对子模块里定义装饰器的包无效。
        """
        # 1. SnowflakeID：进程内只需一次，幂等保护
        if SnowflakeID().worker_id < 0:
            SnowflakeID().init(worker_id, 0)

        # 2. 注册表：默认 build-once，仅在需要构建时才动注册表。
        if reload_app or SystemClusters().get_clusters(namespace) is None:
            # build_clusters 要求全局 _clusters 为空。若注册表里已有其他 namespace
            # （共享进程被污染，如 HeTu 自身测试套件），必须先重置再重载才能构建。
            # 单文件 app 重载即可重新注册；多模块游戏包在干净进程中不会触发此重置/重载
            # （registry 为空 → 直接 build-once），因此无 reload 的子模块注册/类身份问题。
            dirty = SystemClusters()._clusters != {}
            if reload_app or dirty:
                ComponentDefines().clear_()
                EndpointDefines()._clear()
                SystemClusters()._clear()
                importlib.reload(app_module)
            else:
                # 确保 app 已 import（其 @define_* 已注册），通常调用方已 import
                importlib.import_module(app_module.__name__)
            SystemClusters().build_clusters(namespace)
            SystemClusters().build_endpoints()

        # 3. SQLite backend
        config = {"type": "sql", "master": f"sqlite:///{db_path}", "servants": []}
        backend = Backend(config)
        # SQL backend 的 schema 检查/支持表会用到 referred components，与内部 fixture 对齐
        backend.master._get_referred_components = (  # type: ignore[method-assign]
            lambda: ComponentDefines().get_all()
        )
        backend.post_configure()

        # 4. 建表（与服务器启动同一调用，创建所有不存在的表，含 unique/index）
        tbl_mgr = ComponentTableManager(namespace, instance_name, {"default": backend})
        tbl_mgr.check_and_create_new_tables()

        return cls(namespace, instance_name, backend, tbl_mgr)

    async def call(
        self,
        endpoint: str,
        *args: Any,
        caller: int = 0,
        user_data: dict | None = None,
    ) -> Any:
        """像客户端那样经 Endpoint 正常路径调用，返回 client SDK 实际收到的 payload。

        走 `EndpointExecutor.execute` 完整路径：分配连接 → 权限/参数校验 → 调用前 guard
        → 执行 → 按 `receiver.rpc()` framing + 真实 msgpack 往返。可调用纯
        `@define_endpoint`（`call_system` 调不到），也可调用 System 自动生成的 endpoint。

        `caller` 非 0 时，会先建连接并 `elevate(caller)` 模拟已登录，再执行（等价"已登录
        客户端调用"）；`caller=0` 为匿名连接。每次调用都是独立的新连接，调用结束即断开。
        """
        # 与生产 server 一致：统一用 SystemContext（is-a Context），这样 EndpointExecutor
        # 与 SystemCaller 都能收，且 endpoint 内 ctx.systems.call 需要的 repo/depend 字段齐备。
        ctx = SystemContext(
            caller=0,
            connection_id=0,
            address="sandbox",
            group="",
            user_data=user_data if user_data is not None else {},
            timestamp=0,
            request=None,  # type: ignore[arg-type]
            systems=None,  # type: ignore[arg-type]
        )
        ctx.systems = SystemCaller(self.namespace, self.tbl_mgr, ctx)
        executor = EndpointExecutor(self.namespace, self.tbl_mgr, ctx)
        await executor.initialize("sandbox")
        try:
            if caller:
                ok_elev, reason = await elevate(ctx, caller)
                if not ok_elev:
                    raise RuntimeError(
                        f"sandbox: elevate(caller={caller}) 失败: {reason}"
                    )
            ok, res = await executor.execute(endpoint, *args)
        finally:
            await executor.terminate()
        if not ok:
            raise ConnectionClosed(
                f"endpoint {endpoint!r} 被服务器拒绝（权限/参数/不存在/内部异常），"
                f"连接已断开；如需排查 System 内部异常请改用 call_system"
            )
        if isinstance(res, RejectResponse):
            raise CallRejected(res.code, res.reason)
        message = res.message if isinstance(res, ResponseToClient) else "ok"
        return self._wire_roundtrip(message)

    async def call_system(
        self,
        system: str,
        *args: Any,
        caller: int = 0,
        uuid: str = "",
        user_data: dict | None = None,
        raw: bool = False,
    ) -> Any:
        """绕过 Endpoint 层、以 `caller` 身份直接跑一个 System，默认返回 client SDK 实际
        收到的 payload。要测权限/guard/登录等 Endpoint 行为请改用 `call`。

        默认（`raw=False`）按 server `receiver.rpc()` 的 framing 处理 System 返回值，
        并过一遍与生产同款的 msgpack 序列化往返（见 `_to_client_payload`），以暴露在
        真实 wire 上才会出现的问题：

        - System 返回 `ResponseToClient(msg)` → 返回 msgpack 往返后的 `msg`；
          不可序列化的 payload（如 numpy 标量、自定义对象）会在此抛 `TypeError`，
          `tuple`/`set` 会如实变成 `list`，与 client 实际收到的一致。
        - System 返回 `None` 或任意普通值 → 返回字符串 `"ok"`（普通返回值在 wire 上
          被无视，仅用于 System 间嵌套调用）。
        - System 返回 `RejectResponse` → 原样返回该对象（边角情况；软拒绝由 Endpoint
          guard 产生，Sandbox 不模拟 Endpoint 层）。

        `raw=True` 时跳过上述处理，原样返回 System 的返回值（等价 `ctx.systems.call`
        的嵌套调用语义，便于断言 System 内部计算/返回值，不做序列化校验）。

        内部会开事务、自动在 `RaceCondition` 时重试。

        Run a System as `caller`; by default returns what the client SDK actually
        receives (after `receiver.rpc()` framing + the production msgpack round-trip),
        so unit tests catch wire-only failures. Pass `raw=True` to get the System's
        untouched return value instead.

        user_data: 传入则作为 `ctx.user_data`（同一 dict 对调用方可见，便于断言
        System 对其的写入）；不传则用空 dict。
        """
        ctx = SystemContext(
            caller=caller,
            connection_id=0,
            address="sandbox",
            group="",
            user_data=user_data if user_data is not None else {},
            timestamp=0,
            request=None,  # type: ignore[arg-type]
            systems=None,  # type: ignore[arg-type]
        )
        ctx.systems = SystemCaller(self.namespace, self.tbl_mgr, ctx)
        rtn = await ctx.systems.call(system, *args, uuid=uuid)
        if raw:
            return rtn
        return self._to_client_payload(rtn)

    def _wire_roundtrip(self, message: Any) -> Any:
        """把一条 message 按 server `receiver.rpc()` 的 ``["rsp", message]`` framing 过一遍
        与生产同款的 msgpack codec（见 `hetu/server/pipeline/jsonb.py`），返回 client SDK
        实际收到的裸 message。

        不可序列化的 payload（如 numpy 标量、自定义对象）会在此抛 `TypeError`（与生产
        wire 一致），`tuple`/`set` 会如实变成 `list`。`call` 与 `call_system` 共用此往返。
        """
        decoded = self._msg_decoder.decode(self._msg_encoder.encode(["rsp", message]))
        return decoded[1]

    def _to_client_payload(self, rtn: Any) -> Any:
        """把 System 返回值按 server `receiver.rpc()` 的 framing + 真实 msgpack 往返，
        返回 client SDK 实际收到的 payload。

        序列化采用与 `hetu/server/pipeline/jsonb.py` 同款的 `msgspec.msgpack`，因此
        不可序列化的 payload 会在此抛 `TypeError`（与生产 wire 一致）。
        """
        if isinstance(rtn, RejectResponse):
            # 软拒绝在 wire 上是 ["rej", name, code]，由 Endpoint guard 产生；call_system
            # 不模拟 Endpoint 层，原样返回该对象（code/reason 是字符串，无序列化问题）。
            return rtn
        # framing 与 receiver.rpc() 对齐：ResponseToClient → message，其余（含 None /
        # 普通返回值）→ "ok"。
        message = rtn.message if isinstance(rtn, ResponseToClient) else "ok"
        return self._wire_roundtrip(message)

    def _resolve_table(self, comp: Any) -> "Table":
        """把 Component 类或名字字符串解析为本沙盒的 `Table`。"""
        table = self.tbl_mgr.get_table(comp)
        if table is None:
            raise ValueError(
                f"找不到 Component：{comp!r}（是否在 components 中引用过？）"
            )
        return table

    async def get(self, comp: Any, **query: Any) -> "np.record | None":
        """按 unique/index 字段读一行；无则返回 None。

        `comp` 可传 Component 类或其名字字符串；`query` 只允许一个带索引的字段，
        如 `get("Player", owner=1234)`。
        """
        table = self._resolve_table(comp)
        async with table.session() as session:
            repo = session.using(table.comp_cls)
            return await repo.get(**query)

    async def range(
        self,
        comp: Any,
        index_name: str | None = None,
        _left: Any = None,
        _right: Any = None,
        limit: int = 10,
        desc: bool = False,
        **kwargs: Any,
    ) -> "np.recarray":
        """按索引区间读多行，便于断言列表场景。默认闭区间 `[left, right]`。

        签名与 `repo.range` 对齐，两种形态都支持：位置参数
        `range(comp, "value", 1.0, 2.0)`，或 kwarg 区间 `range(comp, value=(1.0, 2.0))`。
        `comp` 可传 Component 类或其名字字符串；索引字段必须带 index/unique。
        `limit` 默认 10（与 `repo.range` 一致，注意超出会静默截断），负数表示不限制；
        `desc=True` 降序。返回 `numpy.recarray`（c-struct array），无数据时为空数组。
        """
        table = self._resolve_table(comp)
        async with table.session() as session:
            repo = session.using(table.comp_cls)
            return await repo.range(
                index_name, _left, _right, limit=limit, desc=desc, **kwargs
            )

    async def insert(self, comp: Any, **fields: Any) -> int:
        """插入一行并返回其 `id`，省去手写 `new_row()` + `repo.insert(row)` 的样板。

        只需给关心的字段，其余字段保留组件默认值。传 `id=` 可指定主键，否则自动生成
        雪花 id（返回值即该 id）。`_version` 由引擎管理，不能设置。

        用法 / Usage::

            rid = await sb.insert(PlayerInfo, owner=1001, name="Alice")
            await sb.insert(PlayerInfo, owner=1002, id=12345)  # 指定 id

        `comp` 可传 Component 类或其名字字符串；重复 unique 会抛 `UniqueViolation`。
        直接落库（绕过 System / 不做权限检查），便于测试喂初始行。
        """
        table = self._resolve_table(comp)
        comp_cls = table.comp_cls
        valid = set(comp_cls.prop_idx_map_)  # 全部字段名（含 id/_version）
        row = comp_cls.new_row(id_=fields.pop("id", None))
        for name, value in fields.items():
            if name not in valid:
                raise ValueError(f"{comp_cls.name_} 组件没有叫 {name} 的字段")
            if name == "_version":
                raise ValueError("_version 由引擎管理，insert 时不能设置")
            row[name] = value
        async with table.session() as session:
            await session.using(comp_cls).insert(row)
        return int(row.id)

    @asynccontextmanager
    async def upsert(self, comp: Any, **anchor: Any) -> "AsyncIterator[np.record]":
        """以 `async with` 语法 upsert 一行，镜像 `repo.upsert`：按 unique 字段锚定
        查询，块内修改字段，退出块时自动 update/insert 并 commit。

        用法 / Usage::

            async with sb.upsert(RLSComp, owner=1001) as row:
                row.value = 50
            # 退出：owner=1001 存在则更新其 value，否则插入新行（owner=1001），并 commit

        `anchor` 只能给一个 **unique** 字段（如 `owner=...`/`id=...`），等同
        `repo.upsert(**anchor)` 的锚定语义。`comp` 可传 Component 类或其名字字符串。
        直接落库（绕过 System / 不做权限检查），便于测试喂初始行。
        """
        table = self._resolve_table(comp)
        async with table.session() as session:
            repo = session.using(table.comp_cls)
            async with repo.upsert(**anchor) as row:
                yield row

    async def flush(self) -> None:
        """清空本沙盒所有组件表的数据（测试间复用同一 backend 时用）。"""
        # force flush 是本助手的预期操作，抑制引擎"强制删除"的劝阻性警告
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            self.tbl_mgr._flush_all(force=True)

    async def aclose(self) -> None:
        """关闭 backend 连接。"""
        await self.backend.close()

    async def __aenter__(self) -> "Sandbox":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()


def sandbox_fixture(
    namespace: str,
    app_module: Any,
    *,
    scope: Literal["function", "class", "module", "package", "session"] = "function",
) -> Any:
    """返回一个 pytest fixture，每个测试产出一个建好表的 `Sandbox`。

    用法（游戏包 conftest.py）::

        import my_game_pkg
        from hetu.testing import sandbox_fixture
        sandbox = sandbox_fixture("my_game", my_game_pkg)

    然后测试里直接用 `sandbox` 参数::

        async def test_login(sandbox):
            await sandbox.call("store_player", "Alice", caller=1001)
            assert (await sandbox.get("PlayerInfo", owner=1001)).name == "Alice"

    pytest 在本函数内惰性 import，故仅在调用本工厂时才需要 pytest（HeTu 运行期不依赖
    pytest）。function scope 下每个测试一套干净库；若用更大 scope 复用 backend，请在每个
    测试开头 `await sb.flush()` 保证隔离。
    """
    import pytest

    @pytest.fixture(scope=scope)
    async def _sandbox(tmp_path_factory: Any) -> Any:
        db = tmp_path_factory.mktemp("hetu_sandbox") / "sandbox.sqlite3"
        sb = await Sandbox.create(namespace, app_module, db_path=str(db))
        try:
            yield sb
        finally:
            await sb.aclose()

    return _sandbox
