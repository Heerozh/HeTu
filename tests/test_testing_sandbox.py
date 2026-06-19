"""hetu.testing.Sandbox 自测（dogfood）。

用现成的 tests/app.py（namespace="pytest"）验证 Sandbox 与既有 fixture 等价：
进程内 SQLite 临时文件跑 System、读回组件行。
"""

import app
import msgspec
import pytest

import hetu
from hetu.testing import CallRejected, ConnectionClosed, Sandbox, sandbox_fixture

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
        ret = await sb.call_system("add_rls_comp_value", 9, caller=1234)
        assert ret == "ok"

        # raw=True 拿到 System 原始返回值（numpy 标量），便于断言内部计算结果
        ret_raw = await sb.call_system("add_rls_comp_value", 1, caller=1234, raw=True)
        assert ret_raw == 110

        # 用字符串名读回组件行
        row = await sb.get("RLSComp", owner=1234)
        assert row is not None
        assert row.value == 110


async def test_sandbox_range(tmp_path):
    db = tmp_path / "t.sqlite3"
    async with await Sandbox.create("pytest", app, db_path=str(db)) as sb:
        # create_row(owner, v1=value, v2=name后缀)；IndexComp1.value 是索引字段
        await sb.call_system("create_row", 10, 1.0, "a", caller=10)
        await sb.call_system("create_row", 11, 2.0, "b", caller=11)
        await sb.call_system("create_row", 12, 3.0, "c", caller=12)

        # 闭区间 [1.0, 2.0] 应命中 owner 10、11 两行
        rows = await sb.range("IndexComp1", "value", 1.0, 2.0)
        assert len(rows) == 2
        assert set(rows.owner.tolist()) == {10, 11}


async def test_sandbox_flush(tmp_path):
    db = tmp_path / "t.sqlite3"
    async with await Sandbox.create("pytest", app, db_path=str(db)) as sb:
        await sb.call_system("add_rls_comp_value", 5, caller=777)
        assert (await sb.get("RLSComp", owner=777)).value == 105

        await sb.flush()
        # flush 后所有组件表数据被清空
        assert await sb.get("RLSComp", owner=777) is None


async def test_sandbox_response_to_client_roundtrip(tmp_path):
    db = tmp_path / "t.sqlite3"
    async with await Sandbox.create("pytest", app, db_path=str(db)) as sb:
        # on_disconnect(permission=None) 给 DisconnectRecord.count +1
        await sb.call_system("on_disconnect", caller=2001)
        # get_disconnect_count 返回 ResponseToClient(int)，默认走 framing+msgpack
        # 往返，返回客户端实际收到的裸 message
        resp = await sb.call_system("get_disconnect_count", 2001, caller=0)
        assert resp == 1

        # raw=True 仍拿到原始 ResponseToClient 对象
        resp_raw = await sb.call_system(
            "get_disconnect_count", 2001, caller=0, raw=True
        )
        assert isinstance(resp_raw, hetu.ResponseToClient)
        assert resp_raw.message == 1


async def test_sandbox_call_rejects_unserializable_payload(tmp_path):
    db = tmp_path / "t.sqlite3"
    async with await Sandbox.create("pytest", app, db_path=str(db)) as sb:
        await sb.call_system("add_rls_comp_value", 5, caller=42)
        # get_rls_value_unsafe 直接返回 numpy 组件字段，msgpack 编不了，默认路径应在
        # call 处抛 TypeError（与生产 wire 一致），而不是悄悄通过
        with pytest.raises(TypeError):
            await sb.call_system("get_rls_value_unsafe", caller=42)
        # raw=True 跳过序列化校验，仍能拿到裸 ResponseToClient
        resp = await sb.call_system("get_rls_value_unsafe", caller=42, raw=True)
        assert isinstance(resp, hetu.ResponseToClient)


async def test_sandbox_call_roundtrips_payload_shape(tmp_path):
    db = tmp_path / "t.sqlite3"
    async with await Sandbox.create("pytest", app, db_path=str(db)) as sb:
        # tuple 经 msgpack 往返变成 list（与 client 实际收到的一致）
        assert await sb.call_system("echo_response", (1, 2, 3), caller=0) == [1, 2, 3]
        # dict payload 原样往返
        assert await sb.call_system(
            "echo_response", {"ok": True, "v": 7}, caller=0
        ) == {
            "ok": True,
            "v": 7,
        }
        # raw=True 保留原始 tuple，不做往返
        resp = await sb.call_system("echo_response", (1, 2, 3), caller=0, raw=True)
        assert resp.message == (1, 2, 3)


async def test_sandbox_fixture_factory(sandbox):
    # 直接使用工厂产出的 fixture（已建好表）
    await sandbox.call_system("add_rls_comp_value", 3, caller=555)
    assert (await sandbox.get("RLSComp", owner=555)).value == 103


def test_sandbox_codec_parity_with_server_pipeline():
    # 守卫：Sandbox 的序列化 codec 必须与 server pipeline 的 jsonb 层零漂移。
    # 若哪天 jsonb.py 改了 codec 配置（如加 enc_hook），本测试会失败提醒同步。
    from hetu.server.pipeline.jsonb import JSONBinaryLayer

    layer = JSONBinaryLayer()
    sandbox_encoder = msgspec.msgpack.Encoder()
    for frame in (["rsp", "ok"], ["rsp", {"a": 1, "b": [1, 2, 3]}], ["rsp", 109]):
        assert sandbox_encoder.encode(frame) == layer.encode(None, frame)


async def test_sandbox_insert_and_get(tmp_path):
    db = tmp_path / "t.sqlite3"
    async with await Sandbox.create("pytest", app, db_path=str(db)) as sb:
        # 只给关心的字段，其余走组件默认；返回自动生成的雪花 id
        rid = await sb.insert(app.RLSComp, owner=1001, value=42)
        assert rid > 0
        row = await sb.get("RLSComp", owner=1001)
        assert row is not None
        assert row.value == 42
        assert int(row.id) == rid

        # 未指定的字段保留组件默认值（RLSComp.value 默认 100）
        await sb.insert(app.RLSComp, owner=1002)
        assert (await sb.get("RLSComp", owner=1002)).value == 100


async def test_sandbox_insert_explicit_id_and_bad_field(tmp_path):
    db = tmp_path / "t.sqlite3"
    async with await Sandbox.create("pytest", app, db_path=str(db)) as sb:
        # 指定 id，返回值即该 id，可按 owner 读回核对
        rid = await sb.insert(app.RLSComp, owner=1003, value=7, id=12345)
        assert rid == 12345
        assert int((await sb.get("RLSComp", owner=1003)).id) == 12345

        # 未知字段报错
        with pytest.raises(ValueError):
            await sb.insert(app.RLSComp, owner=1004, nope=1)
        # _version 由引擎管理，不能设置
        with pytest.raises(ValueError):
            await sb.insert(app.RLSComp, owner=1005, _version=3)


async def test_sandbox_upsert_insert_then_update(tmp_path):
    db = tmp_path / "t.sqlite3"
    async with await Sandbox.create("pytest", app, db_path=str(db)) as sb:
        # 不存在 → 插入（锚点 owner 自动落到新行），退出 with 自动 commit
        async with sb.upsert(app.RLSComp, owner=2001) as row:
            row.value = 55
        seeded = await sb.get("RLSComp", owner=2001)
        assert seeded is not None
        assert seeded.value == 55
        assert seeded.owner == 2001

        # 已存在 → 更新同一行
        async with sb.upsert(app.RLSComp, owner=2001) as row:
            row.value = 99
        assert (await sb.get("RLSComp", owner=2001)).value == 99


async def test_sandbox_range_kwarg_matches_positional(tmp_path):
    db = tmp_path / "t.sqlite3"
    async with await Sandbox.create("pytest", app, db_path=str(db)) as sb:
        await sb.insert(app.IndexComp1, owner=10, value=1.0)
        await sb.insert(app.IndexComp1, owner=11, value=2.0)
        await sb.insert(app.IndexComp1, owner=12, value=3.0)

        # 位置参数形式（现状）与 kwarg 区间形式（对齐 repo.range）等价
        pos = await sb.range("IndexComp1", "value", 1.0, 2.0)
        kw = await sb.range("IndexComp1", value=(1.0, 2.0))
        assert set(pos.owner.tolist()) == set(kw.owner.tolist()) == {10, 11}


async def test_sandbox_call_reaches_plain_endpoint(tmp_path):
    # call 走 endpoint 正常路径：能调到纯 @define_endpoint（login），这是 call_system
    # 调不到的（login 不是 System）。login 是 EVERYBODY endpoint，内部 elevate(user_id)
    # 后返回 ResponseToClient({"id": caller})，应原样（经 msgpack 往返）返回。
    db = tmp_path / "t.sqlite3"
    async with await Sandbox.create("pytest", app, db_path=str(db)) as sb:
        assert await sb.call("login", 1234) == {"id": 1234}


async def test_sandbox_call_user_endpoint_as_caller(tmp_path):
    # call(caller=1234) 先建连接 + elevate 模拟已登录，再走 USER 权限的 system-endpoint，
    # 且 alive_checker 能在 SQLite 上查到 conn.owner==caller 放行。
    db = tmp_path / "t.sqlite3"
    async with await Sandbox.create("pytest", app, db_path=str(db)) as sb:
        # add_rls_comp_value 返回普通 int（非 ResponseToClient）→ wire 上是 "ok"
        assert await sb.call("add_rls_comp_value", 9, caller=1234) == "ok"
        # 系统确实以 caller=1234 执行了：RLSComp.value 默认 100 + 9 = 109
        assert (await sb.get("RLSComp", owner=1234)).value == 109


async def test_sandbox_call_soft_reject_raises(tmp_path):
    # guarded_add 带 @guard：value<0 时 guard raise ClientReject("NEGATIVE")，走 endpoint
    # 路径应转成 rej 帧 → call 抛 CallRejected，code 与客户端收到的一致。
    db = tmp_path / "t.sqlite3"
    async with await Sandbox.create("pytest", app, db_path=str(db)) as sb:
        with pytest.raises(CallRejected) as exc_info:
            await sb.call("guarded_add", -5, caller=1234)
        assert exc_info.value.code == "NEGATIVE"
        # 软拒绝不落库：RLSComp 未被创建/修改
        assert await sb.get("RLSComp", owner=1234) is None


async def test_sandbox_call_denied_raises_connection_closed(tmp_path):
    # 匿名连接（caller=0）调 USER 权限 endpoint → 服务器判非法、会关连接 → call 抛
    # ConnectionClosed（对应客户端实际被断开）。
    db = tmp_path / "t.sqlite3"
    async with await Sandbox.create("pytest", app, db_path=str(db)) as sb:
        with pytest.raises(ConnectionClosed):
            await sb.call("add_rls_comp_value", 9)  # 无 caller → 匿名，USER 不放行


async def test_sandbox_call_unknown_endpoint_raises_connection_closed(tmp_path):
    # permission=None 的 System（on_disconnect）不会生成 endpoint → 走 call 时 endpoint
    # 不存在 → 同样判非法、关连接。这与权限不符是 execute_check 里的不同分支。
    db = tmp_path / "t.sqlite3"
    async with await Sandbox.create("pytest", app, db_path=str(db)) as sb:
        with pytest.raises(ConnectionClosed):
            await sb.call("on_disconnect", caller=2001)


async def test_sandbox_reload_app(tmp_path):
    # reload_app=True：清空注册表 + importlib.reload(app) + 重建，从任意状态都能跑
    db = tmp_path / "t.sqlite3"
    async with await Sandbox.create(
        "pytest", app, db_path=str(db), reload_app=True
    ) as sb:
        await sb.call_system("add_rls_comp_value", 7, caller=99)
        assert (await sb.get("RLSComp", owner=99)).value == 107
