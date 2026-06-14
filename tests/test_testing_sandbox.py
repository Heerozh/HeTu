"""hetu.testing.Sandbox 自测（dogfood）。

用现成的 tests/app.py（namespace="pytest"）验证 Sandbox 与既有 fixture 等价：
进程内 SQLite 临时文件跑 System、读回组件行。
"""

import app
import msgspec
import pytest

import hetu
from hetu.testing import Sandbox, sandbox_fixture

# sandbox_fixture 工厂：每个测试产出一个建好表的 Sandbox
sandbox = sandbox_fixture("pytest", app)


@pytest.fixture(autouse=True)
def _restore_app_clusters(test_app):
    """每个 sandbox 测试前重建干净的 app.py "pytest" 注册表，保证顺序无关。

    Sandbox 默认走 build-once 路径（reload_app=False），仅靠
    `get_clusters("pytest") is None` 判断是否需要构建，无法察觉同名 namespace 已被
    换成另一套 System。HeTu 自身测试套件共享进程级单件，前序测试（如
    test_system_startup 用 new_clusters_env 清空后以自己的 inline System 重建同名
    "pytest"）会留下被污染的构建，于是 Sandbox 复用它，导致
    `add_rls_comp_value` 等找不到。复用 test_app（清空+reload app+build "pytest"）在
    每个 sandbox 测试前恢复正确环境。
    """
    return None


async def test_sandbox_call_and_get(tmp_path):
    db = tmp_path / "t.sqlite3"
    async with await Sandbox.create("pytest", app, db_path=str(db)) as sb:
        # add_rls_comp_value 返回普通值 row.value（未包 ResponseToClient），
        # 在 wire 上被无视为 "ok"
        ret = await sb.call("add_rls_comp_value", 9, caller=1234)
        assert ret == "ok"

        # raw=True 拿到 System 原始返回值（numpy 标量），便于断言内部计算结果
        ret_raw = await sb.call("add_rls_comp_value", 1, caller=1234, raw=True)
        assert ret_raw == 110

        # 用字符串名读回组件行
        row = await sb.get("RLSComp", owner=1234)
        assert row is not None
        assert row.value == 110


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


async def test_sandbox_response_to_client_roundtrip(tmp_path):
    db = tmp_path / "t.sqlite3"
    async with await Sandbox.create("pytest", app, db_path=str(db)) as sb:
        # on_disconnect(permission=None) 给 DisconnectRecord.count +1
        await sb.call("on_disconnect", caller=2001)
        # get_disconnect_count 返回 ResponseToClient(int)，默认走 framing+msgpack
        # 往返，返回客户端实际收到的裸 message
        resp = await sb.call("get_disconnect_count", 2001, caller=0)
        assert resp == 1

        # raw=True 仍拿到原始 ResponseToClient 对象
        resp_raw = await sb.call("get_disconnect_count", 2001, caller=0, raw=True)
        assert isinstance(resp_raw, hetu.ResponseToClient)
        assert resp_raw.message == 1


async def test_sandbox_call_rejects_unserializable_payload(tmp_path):
    db = tmp_path / "t.sqlite3"
    async with await Sandbox.create("pytest", app, db_path=str(db)) as sb:
        await sb.call("add_rls_comp_value", 5, caller=42)
        # get_rls_value_unsafe 直接返回 numpy 组件字段，msgpack 编不了，默认路径应在
        # call 处抛 TypeError（与生产 wire 一致），而不是悄悄通过
        with pytest.raises(TypeError):
            await sb.call("get_rls_value_unsafe", caller=42)
        # raw=True 跳过序列化校验，仍能拿到裸 ResponseToClient
        resp = await sb.call("get_rls_value_unsafe", caller=42, raw=True)
        assert isinstance(resp, hetu.ResponseToClient)


async def test_sandbox_call_roundtrips_payload_shape(tmp_path):
    db = tmp_path / "t.sqlite3"
    async with await Sandbox.create("pytest", app, db_path=str(db)) as sb:
        # tuple 经 msgpack 往返变成 list（与 client 实际收到的一致）
        assert await sb.call("echo_response", (1, 2, 3), caller=0) == [1, 2, 3]
        # dict payload 原样往返
        assert await sb.call("echo_response", {"ok": True, "v": 7}, caller=0) == {
            "ok": True,
            "v": 7,
        }
        # raw=True 保留原始 tuple，不做往返
        resp = await sb.call("echo_response", (1, 2, 3), caller=0, raw=True)
        assert resp.message == (1, 2, 3)


async def test_sandbox_fixture_factory(sandbox):
    # 直接使用工厂产出的 fixture（已建好表）
    await sandbox.call("add_rls_comp_value", 3, caller=555)
    assert (await sandbox.get("RLSComp", owner=555)).value == 103


def test_sandbox_codec_parity_with_server_pipeline():
    # 守卫：Sandbox 的序列化 codec 必须与 server pipeline 的 jsonb 层零漂移。
    # 若哪天 jsonb.py 改了 codec 配置（如加 enc_hook），本测试会失败提醒同步。
    from hetu.server.pipeline.jsonb import JSONBinaryLayer

    layer = JSONBinaryLayer()
    sandbox_encoder = msgspec.msgpack.Encoder()
    for frame in (["rsp", "ok"], ["rsp", {"a": 1, "b": [1, 2, 3]}], ["rsp", 109]):
        assert sandbox_encoder.encode(frame) == layer.encode(None, frame)


async def test_sandbox_reload_app(tmp_path):
    # reload_app=True：清空注册表 + importlib.reload(app) + 重建，从任意状态都能跑
    db = tmp_path / "t.sqlite3"
    async with await Sandbox.create(
        "pytest", app, db_path=str(db), reload_app=True
    ) as sb:
        await sb.call("add_rls_comp_value", 7, caller=99)
        assert (await sb.get("RLSComp", owner=99)).value == 107
