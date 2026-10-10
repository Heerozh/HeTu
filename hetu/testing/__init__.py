"""
进程内 HeTu 应用测试沙盒（SQLite 临时文件），用于游戏包对自己的 `@define_system`
与 `@define_endpoint` 做单元测试 / TDD。

In-process HeTu application test sandbox (SQLite temp file), for game packages to
unit-test their own `@define_system` / `@define_endpoint` logic.

`call` 走 Endpoint 正常路径（权限/guard/elevate 全过），`call_system` 绕过 Endpoint 层
直接跑 System。只覆盖"进程内 SQLite"的单测场景；多后端参数化（Redis/Valkey/SQLite）
与 docker 服务编排保留在 HeTu 内部 fixture，不在此模块范围。

@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0
"""

import importlib
import warnings
from typing import Any, Literal

from ..common.snowflake_id import SnowflakeID
from ..data.backend import Backend
from ..data.component import ComponentDefines
from ..endpoint.connection import elevate
from ..endpoint.definer import EndpointDefines
from ..endpoint.executor import EndpointExecutor
from ..endpoint.response import RejectResponse, ResponseToClient
from ..local import LocalApp
from ..manager import ComponentTableManager
from ..system import SystemClusters, SystemContext
from ..system.caller import SystemCaller

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


class Sandbox(LocalApp):
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
    直接读回组件表；二者都不做 RLS 检查。它们都是对 `client`（一个
    `hetu.headless.HeadlessClient`）的薄包装：需要多表事务或 `Table` 时直接用
    `sb.client.session(A, B)` / `sb.client.table(A)`。Sandbox 与 headless 的区别只在
    它自己建簇、建表、初始化雪花 id、并能跑 System。

    Sandbox 是 `hetu.local.LocalApp` 的子类：`call_system` / `get` / `must_get` / `range` /
    `insert` / `upsert` 与 `hetu shell` 里同名函数是同一套实现，只多了临时库、建表、`call`
    和 `flush`，身份默认值更宽松（没给 caller 就是 0、group 为 "guest"）。

    注意 / Notes
    -----
    - 注册表为全局单例；多 app/namespace 共享测试进程时（如 uv workspace 全仓
      pytest，各包 conftest 收集期均已 import），首建者会把全部 namespace 的簇
      一次建齐，其后 create 其他 namespace 仅重指 main 快速表（switch_main），
      包间交错创建可双向切换。
    - `call_system` 调用的 System 必须引用至少 1 个 Component（引擎限制）。
    - `call` 每次都是独立的新连接、调用结束即断开，不跨调用累积 guard/限流状态；要验证
      `@rate_limit` 的跨调用计数请走集成测试（与不测 slowapi 同理，限流本身是 HeTu 的事）。
    """

    backend: Backend
    """本沙盒的 SQLite 后端（即 `backends["default"]`）"""

    def __init__(
        self,
        namespace: str,
        instance_name: str,
        backend: Backend,
        tbl_mgr: ComponentTableManager,
    ) -> None:
        """一般不直接调用，请用 `Sandbox.create(...)`。直接构造用于已自备 backend/tbl_mgr
        的高级场景。"""
        # 表是自己刚建的，不用再核对表结构；数据读写全部复用 LocalApp（headless client）
        super().__init__(
            namespace,
            instance_name,
            {"default": backend},
            tbl_mgr,
            verify_tables=False,
            address="sandbox",
        )
        self.backend = backend

    def resolve_identity(
        self, system: str, caller: int | None, group: str | None
    ) -> tuple[int, str]:
        """单测用宽松默认：没给 caller 就是 0、没给 group 就是 "guest"（同线上的普通连接），
        不按 permission 推断，不要求 USER System 必须给 caller。"""
        return (0 if caller is None else caller), ("guest" if group is None else group)

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
    ) -> Sandbox:
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
        elif SystemClusters().main_namespace != namespace:
            # 多游戏包共享测试进程：本 namespace 的簇已随首建者顺带建齐
            # （build_clusters 会构建 _system_map 里的**所有** namespace，而各包
            # conftest 在收集期就已 import 注册），只是 main 快速表仍指向首建者
            # → 仅重指 main（get_system 单参查询走 main 表），不清表不重建。
            SystemClusters().switch_main(namespace)

        # 3. SQLite backend；schema 检查直接给全部已定义组件，不依赖 SystemClusters
        config = {"type": "sqlite", "master": f"sqlite:///{db_path}", "servants": []}
        backend = Backend(config)
        backend.post_configure(ComponentDefines().get_all())

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

    async def flush(self) -> None:
        """清空本沙盒所有组件表的数据（测试间复用同一 backend 时用）。"""
        # force flush 是本助手的预期操作，抑制引擎"强制删除"的劝阻性警告
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            self.tbl_mgr._flush_all(force=True)


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
