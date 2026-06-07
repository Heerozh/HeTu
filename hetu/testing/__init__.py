"""
进程内 HeTu 应用测试沙盒（SQLite 临时文件），用于游戏包对自己的 `@define_system`
做单元测试 / TDD。

In-process HeTu application test sandbox (SQLite temp file), for game packages to
unit-test their own `@define_system` logic.

只覆盖"进程内 SQLite 跑 System"的单测场景；多后端参数化（Redis/Valkey/Postgres）
与 docker 服务编排保留在 HeTu 内部 fixture，不在此模块范围。

@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0
"""

import importlib
import warnings
from typing import TYPE_CHECKING, Any, Literal

from ..common.snowflake_id import SnowflakeID
from ..data.backend import Backend
from ..data.component import ComponentDefines
from ..endpoint.definer import EndpointDefines
from ..manager import ComponentTableManager
from ..system import SystemClusters, SystemContext
from ..system.caller import SystemCaller

if TYPE_CHECKING:
    import numpy as np

    from ..data.backend import Table

__all__ = ["Sandbox", "sandbox_fixture"]


class Sandbox:
    """进程内 HeTu 应用沙盒（SQLite 临时文件），用于单测 System。

    用法 / Usage::

        import my_game_pkg
        from hetu.testing import Sandbox

        async with await Sandbox.create("my_game", my_game_pkg, db_path=str(tmp)) as sb:
            ret = await sb.call("store_player", "Alice", caller=1001)
            row = await sb.get("PlayerInfo", owner=1001)
            assert row.name == "Alice"

    `call` 以指定 `caller` 身份直接运行 System（绕过 Endpoint 权限校验，等同可信内部
    调用）；`get`/`range` 直接读回组件表，不做 RLS 检查。

    注意 / Notes
    -----
    - 同一测试进程只支持一个 app/namespace（注册表为全局单例）。
    - 被调用的 System 必须引用至少 1 个 Component（引擎限制）。
    - v1 不模拟连接/登录(`elevate`)，也不测 Endpoint 权限。
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
        self, system: str, *args: Any, caller: int = 0, uuid: str = "",
        user_data: dict | None = None,
    ) -> Any:
        """以 `caller` 身份跑一个 System，返回该 System 的返回值。

        System 若返回 `ResponseToClient`，则原样返回该对象（其载荷在 `.message`）。
        内部会开事务、自动在 `RaceCondition` 时重试。

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
        return await ctx.systems.call(system, *args, uuid=uuid)

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
        index: str,
        left: Any,
        right: Any,
        limit: int = 100,
    ) -> "np.recarray":
        """按索引区间读多行，便于断言列表场景。默认闭区间 `[left, right]`。

        `comp` 可传 Component 类或其名字字符串；`index` 必须是带索引的字段。
        返回 `numpy.recarray`（c-struct array），无数据时为空数组。
        """
        table = self._resolve_table(comp)
        async with table.session() as session:
            repo = session.using(table.comp_cls)
            return await repo.range(index, left, right, limit=limit)

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
