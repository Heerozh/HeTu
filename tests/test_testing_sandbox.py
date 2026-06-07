"""hetu.testing.Sandbox 自测（dogfood）。

用现成的 tests/app.py（namespace="pytest"）验证 Sandbox 与既有 fixture 等价：
进程内 SQLite 临时文件跑 System、读回组件行。
"""

import app

from hetu.testing import Sandbox, sandbox_fixture

# sandbox_fixture 工厂：每个测试产出一个建好表的 Sandbox
sandbox = sandbox_fixture("pytest", app)


async def test_sandbox_call_and_get(tmp_path):
    db = tmp_path / "t.sqlite3"
    async with await Sandbox.create("pytest", app, db_path=str(db)) as sb:
        # add_rls_comp_value：默认 value=100，+9 => 109，返回普通值
        ret = await sb.call("add_rls_comp_value", 9, caller=1234)
        assert ret == 109

        # 用字符串名读回组件行
        row = await sb.get("RLSComp", owner=1234)
        assert row is not None
        assert row.value == 109


async def test_sandbox_range(tmp_path):
    db = tmp_path / "t.sqlite3"
    async with await Sandbox.create("pytest", app, db_path=str(db)) as sb:
        # create_row(owner, v1=value, v2=name后缀)；IndexComp1.value 是索引字段
        await sb.call("create_row", 10, 1.0, "a", caller=10)
        await sb.call("create_row", 11, 2.0, "b", caller=11)
        await sb.call("create_row", 12, 3.0, "c", caller=12)

        # 闭区间 [1.0, 2.0] 应命中 owner 10、11 两行
        rows = await sb.range("IndexComp1", "value", 1.0, 2.0)
        assert len(rows) == 2
        assert set(rows.owner.tolist()) == {10, 11}


async def test_sandbox_flush(tmp_path):
    db = tmp_path / "t.sqlite3"
    async with await Sandbox.create("pytest", app, db_path=str(db)) as sb:
        await sb.call("add_rls_comp_value", 5, caller=777)
        assert (await sb.get("RLSComp", owner=777)).value == 105

        await sb.flush()
        # flush 后所有组件表数据被清空
        assert await sb.get("RLSComp", owner=777) is None


async def test_sandbox_response_to_client_passthrough(tmp_path):
    import hetu

    db = tmp_path / "t.sqlite3"
    async with await Sandbox.create("pytest", app, db_path=str(db)) as sb:
        # on_disconnect(permission=None) 给 DisconnectRecord.count +1
        await sb.call("on_disconnect", caller=2001)
        # get_disconnect_count 返回 ResponseToClient，Sandbox 原样透传（不拆包）
        resp = await sb.call("get_disconnect_count", 2001, caller=0)
        assert isinstance(resp, hetu.ResponseToClient)
        assert resp.message == 1


async def test_sandbox_fixture_factory(sandbox):
    # 直接使用工厂产出的 fixture（已建好表）
    await sandbox.call("add_rls_comp_value", 3, caller=555)
    assert (await sandbox.get("RLSComp", owner=555)).value == 103


async def test_sandbox_reload_app(tmp_path):
    # reload_app=True：清空注册表 + importlib.reload(app) + 重建，从任意状态都能跑
    db = tmp_path / "t.sqlite3"
    async with await Sandbox.create(
        "pytest", app, db_path=str(db), reload_app=True
    ) as sb:
        await sb.call("add_rls_comp_value", 7, caller=99)
        assert (await sb.get("RLSComp", owner=99)).value == 107
