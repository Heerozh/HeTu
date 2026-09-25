import asyncio
import contextlib
import logging
import os
from typing import Callable, cast

import pytest
import sanic_testing
import sanic_testing.testing
from nacl.public import PrivateKey
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK

import hetu
from hetu import webext
from hetu.data import sub as sub_module
from hetu.endpoint.definer import EndpointDefines
from hetu.safelogging.default import DEFAULT_LOGGING_CONFIG
from hetu.server import pipeline, worker_main
from hetu.server import websocket as websocket_server
from hetu.system import SystemClusters

logger = logging.getLogger("HeTu.root")
logger.setLevel(logging.DEBUG)
assert logging.lastResort
logging.lastResort.setLevel(logging.DEBUG)


@pytest.fixture
def setup_websocket_proxy():
    # 设置ws测试routine方法

    async def websocket_proxy(url, *args, **kwargs):
        mimic = kwargs.pop("mimic", None)
        from websockets.legacy.client import connect

        class ProxyForWebsocketProxy:
            def __init__(self):
                self.wss = []

            async def close(self):
                [await ws.close() for ws in self.wss]

        ws_proxy = sanic_testing.websocket.WebsocketProxy(ProxyForWebsocketProxy())  # type: ignore

        async def new_connection():
            ws = await connect(url, *args, **kwargs)
            ws_proxy.ws.wss.append(ws)

            do_send = ws.send
            do_recv = ws.recv

            client_pipe = pipeline.MessagePipeline()
            client_pipe.add_layer(pipeline.JSONBinaryLayer())
            client_pipe.add_layer(pipeline.ZlibLayer())
            crypto_layer = pipeline.CryptoLayer()
            client_pipe.add_layer(crypto_layer)
            pipe_ctx = None
            logger.debug(f"客户端pipe {id(crypto_layer)}")

            async def handshake():
                nonlocal pipe_ctx
                # 生成密钥对
                private_key = PrivateKey.generate()
                public_key = private_key.public_key
                handshake_msg = [b""] * client_pipe.num_handshake_layers
                handshake_msg[-1] = public_key.encode()
                # 握手
                await do_send(client_pipe.encode(None, handshake_msg))
                data = cast(bytes, await do_recv())
                message = client_pipe.decode(None, data)
                assert type(message) is list
                ctx, msg = client_pipe.handshake(message)
                ctx[-1] = crypto_layer.client_handshake(
                    private_key.encode(), message[-1]
                )
                pipe_ctx = ctx

            async def send(data, *, fragment_size=None):
                logger.debug(
                    f"[{id(crypto_layer)}] > Sent: {data} [{len(repr(data))} bytes]"
                )
                ws_proxy.client_sent.append(data)
                encoded = client_pipe.encode(pipe_ctx, data)
                if fragment_size is None:
                    await do_send(encoded)
                else:

                    async def fragments():
                        for offset in range(0, len(encoded), fragment_size):
                            yield encoded[offset : offset + fragment_size]
                            await asyncio.sleep(0)

                    await do_send(fragments())

            async def recv():
                data = cast(bytes, await do_recv())
                message = client_pipe.decode(pipe_ctx, data)
                logger.debug(
                    f"[{id(crypto_layer)}] < Received: {message} [{len(repr(message))} bytes]"
                )
                ws_proxy.client_received.append(message)
                return message

            def clear_recv():
                ws_proxy.client_received.clear()

            await handshake()

            ws.send = send  # type: ignore
            ws.recv = recv  # type: ignore
            ws.clear_recv = clear_recv  # type: ignore

            return ws

        if mimic:
            mimic: Callable
            try:
                await mimic(new_connection)
            except ConnectionClosedOK:
                pass

        return ws_proxy

    sanic_testing.testing.websocket_proxy = websocket_proxy


@pytest.fixture
def test_server(setup_websocket_proxy, ses_redis_service):
    SystemClusters()._clear()
    EndpointDefines()._clear()
    # webext 注册表按 模块名.函数名 记路由：别的测试 `import app` 过之后再由 worker_main
    # 以 HeTuApp 之名 exec 同一个文件，同一 uri 就会注册两次撞 RouteExists，先清掉
    webext.clear()
    import re

    match = re.match(r"redis://127\.0\.0\.1:(\d+)/0", ses_redis_service[0])
    assert match
    port = match.group(1)

    app_file = os.path.join(os.path.dirname(__file__), "app.py")
    logging_cfg = DEFAULT_LOGGING_CONFIG
    logging_cfg["loggers"]["HeTu.replay"]["level"] = logging.DEBUG
    server = worker_main(
        "Hetu-test",
        {
            "APP_FILE": app_file,
            "NAMESPACE": "pytest",
            "INSTANCES": ["pytest_1"],
            "LISTEN": "0.0.0.0:874",
            "PACKET_LAYERS": [
                {"type": "jsonb"},
                {"type": "zlib"},
                {"type": "crypto"},
            ],
            "BACKENDS": {
                "Redis": {
                    "type": "Redis",
                    "master": f"redis://127.0.0.1:{port}/0",
                }
            },
            "CLIENT_SEND_LIMITS": [[10, 1], [27, 5], [100, 50], [300, 300]],
            "MAX_TABLE_SUBSCRIPTION": 1,
            "LOGGING": logging_cfg,
            "DEBUG": False,
            "WORKER_NUM": 4,
            "ACCESS_LOG": False,
        },
    )

    yield server

    server.stop()
    webext.clear()


def test_websocket_started(test_server):
    # 测试服务器是否正常启动
    # 这行future出现异常是正常的，因为下面的请求很快就关闭了
    request, response = test_server.test_client.get("/")
    assert request.method.lower() == "get"
    assert "Powered by HeTu" in response.body.decode()
    assert response.status == 200
    # 因为上面get("/")会启动future线程，也因此启动了redis，所以要切换下connection_pool
    # app.ctx.default_backend.reset_connection_pool() 优化了process，不再需要


def test_custom_http_route(test_server):
    # app.py里用@hetu.define_route注册的普通HTTP端点应该能正常提供服务
    _request, response = test_server.test_client.get("/webext-test/abc.zip")
    assert response.status == 200
    assert response.body.decode() == "downloaded abc.zip"


@pytest.mark.timeout(20)
def test_websocket_call_system(test_server):
    # 测试call和结果
    async def normal_routine(connect):
        client1 = await connect()
        await client1.send(["rpc", "login", 1])
        await client1.recv()

        await client1.send(["sub", "RLSComp", "range", "owner", 1, 999])
        await client1.send(["sub", "IndexComp1", "range", "owner", 1, 999])
        await client1.recv()
        await client1.recv()

        await client1.send(["rpc", "add_rls_comp_value", 1])
        await client1.recv()  # 首次sub这里会卡至少0.5s等待连接
        await client1.recv()

        await client1.send(["rpc", "login", 2])  # 测试重复登录应该无效
        await client1.recv()

        # 正式开始接受sub消息
        await client1.send(["rpc", "add_rls_comp_value", 1])
        await client1.recv()
        await client1.recv()

        # 模拟其他用户修改了用户1订阅的数据
        client2 = await connect()
        await client2.send(["rpc", "login", 2])
        # 这个是rls数据，client1不会收到
        await client2.send(["rpc", "add_rls_comp_value", 9])
        # 这个client1应该收到
        await client2.send(["rpc", "create_row", 2, 9.1, "1"])
        await asyncio.sleep(0.1)

        await client1.recv()  # 因为客户端2并没订阅，测试用户1是否收到

    _, response1 = test_server.test_client.websocket(
        "/hetu/pytest_1", mimic=normal_routine
    )
    # print(response1.client_received)
    # 测试add_rls_comp_value调用了2次
    id1 = next(iter(response1.client_received[4][2].keys()))
    assert response1.client_received[4][2][id1] == {
        "id": int(id1),
        "owner": 1,
        "value": 101,
    }
    assert response1.client_received[7][2][id1] == {
        "id": int(id1),
        "owner": 1,
        "value": 102,
    }

    # 测试收到连接2的+9.1
    id2 = next(iter(response1.client_received[8][2].keys()))
    assert response1.client_received[8][2][id2] == {
        "id": int(id2),
        "owner": 2,
        "value": 9.1,
    }


def test_websocket_kick_connect(test_server):
    # 测试踢掉别人的连接
    async def kick_routine(connect):
        client1 = await connect()
        await client1.send(["rpc", "login", 1])
        await client1.recv()
        await client1.send(["rpc", "add_rls_comp_value", 1])
        await client1.recv()

        client2 = await connect()
        await client2.send(["rpc", "login", 1])
        await client2.recv()
        await client2.send(["rpc", "add_rls_comp_value", 2])
        await client2.recv()

        # client2 顶掉了 client1：服务器收到 client1 那行 Connection 的变更通知后会
        # 主动断开它，不用等 client1 再调一次 system。留点时间给通知（SQL hub 轮询 0.1s）
        await asyncio.sleep(0.5)

        # 测试踢出成功
        with pytest.raises(ConnectionClosedError):
            await client1.send(["rpc", "add_rls_comp_value", 4])

    _, response1 = test_server.test_client.websocket(
        "/hetu/pytest_1", mimic=kick_routine
    )
    # 用来确定最后一行执行到了，不然在中途报错会被webserver catch跳过，导致test通过
    assert response1.client_sent[-1] == [
        "rpc",
        "add_rls_comp_value",
        4,
    ], "最后一行没执行到"


def test_websocket_kick_without_rpc_after_login(test_server):
    """登录后一次 RPC 都不再调（只挂着订阅）的连接被顶号，也要靠通知主动断开"""

    async def kick_routine(connect):
        client1 = await connect()
        await client1.send(["rpc", "login", 1])
        await client1.recv()

        client2 = await connect()
        await client2.send(["rpc", "login", 1])
        await client2.recv()

        # client1 没有任何后续调用，只能靠 owner 索引值频道的通知把它断开
        with pytest.raises((ConnectionClosedError, ConnectionClosedOK)):
            async with asyncio.timeout(3):
                await client1.recv()
        await client2.send(["rpc", "add_rls_comp_value", 2])
        await client2.recv()

    _, response = test_server.test_client.websocket(
        "/hetu/pytest_1", mimic=kick_routine
    )
    assert response.client_sent[-1] == ["rpc", "add_rls_comp_value", 2], (
        "最后一行没执行到"
    )


@pytest.mark.timeout(60)
def test_websocket_normal_logout_no_spurious_kick_log(test_server, caplog):
    """已登录连接正常断开：拆连接时自己删了 Connection 行，删行的通知不能被当成"被顶号"记日志"""
    user_id = 199993
    caplog.set_level(logging.INFO, logger="HeTu.root")

    async def routine(connect):
        for _ in range(3):
            client1 = await connect()
            await client1.send(["rpc", "login", user_id])
            await client1.recv()
            await client1.send(["rpc", "add_rls_comp_value", 1])
            await client1.recv()
            await client1.close()

            client2 = await connect()
            for _ in range(
                40
            ):  # 等服务端把这条连接拆完（断线 System 跑过就说明拆到那一步了）
                await client2.send(["rpc", "get_disconnect_count", user_id])
                message = await client2.recv()
                if message[1] >= 1:
                    break
                await asyncio.sleep(0.05)
            await client2.close()
            await asyncio.sleep(0.2)  # 留时间给删行之后可能冒出来的假顶号核查

    test_server.test_client.websocket("/hetu/pytest_1", mimic=routine)
    kicked = [r.getMessage() for r in caplog.records if "已被顶号" in r.getMessage()]
    assert kicked == [], f"正常断开被记成了顶号：{kicked}"


@pytest.mark.timeout(20)
def test_websocket_disconnect_system_called(test_server):
    user_id = 199991

    async def disconnect_routine(connect):
        client1 = await connect()
        await client1.send(["rpc", "login", user_id])
        await client1.recv()
        await client1.close()

        client2 = await connect()
        count = 0
        for _ in range(20):
            await client2.send(["rpc", "get_disconnect_count", user_id])
            message = await client2.recv()
            count = message[1]
            if count == 1:
                break
            await asyncio.sleep(0.05)
        assert count == 1

    test_server.test_client.websocket("/hetu/pytest_1", mimic=disconnect_routine)


@pytest.mark.timeout(20)
def test_websocket_disconnect_system_missing_skip(monkeypatch, test_server):
    user_id = 199992
    monkeypatch.setattr(websocket_server, "DISCONNECT_SYSTEM", "__not_exists__")

    async def disconnect_routine(connect):
        client1 = await connect()
        await client1.send(["rpc", "login", user_id])
        await client1.recv()
        await client1.close()
        await asyncio.sleep(0.2)

        client2 = await connect()
        await client2.send(["rpc", "get_disconnect_count", user_id])
        message = await client2.recv()
        assert message[1] == 0

    test_server.test_client.websocket("/hetu/pytest_1", mimic=disconnect_routine)


def test_call_flooding_lv1_normal(test_server):
    # 测试CLIENT_SEND_LIMITS配置
    # CLIENT_SEND_LIMITS:
    # - [ 10, 1 ]  <---测试该层
    # - [ 27, 5 ]
    # 登录后默认CLIENT_SEND_LIMITS值乘10,所以是100次/秒
    async def normal_routine(connect):
        client1 = await connect()
        for i in range(100):
            await client1.send(["rpc", "login", 1])
            await client1.recv()

    test_server.test_client.websocket("/hetu/pytest_1", mimic=normal_routine)


def test_call_flooding_lv1_flooding(test_server):
    # 因为同时启动2个websocket会报
    # sanic.exceptions.ServerError: Sanic server could not start: [Errno 98] Address already in use.
    # 所以分2个测试，或者可以尝试启动时用随机的port
    async def flooding_routine(connect):
        client1 = await connect()
        with pytest.raises(ConnectionClosedError):
            for i in range(101):
                await client1.send(["rpc", "login", 1])
                await client1.recv()

    test_server.test_client.websocket("/hetu/pytest_1", mimic=flooding_routine)


def test_call_flooding_lv2_normal(test_server):
    # 测试CLIENT_SEND_LIMITS配置
    # CLIENT_SEND_LIMITS:
    # - [ 10, 1 ]
    # - [ 27, 5 ]  <---测试该层
    # 登录后默认CLIENT_SEND_LIMITS值乘10,所以是270次/秒
    async def normal_routine_lv2(connect):
        client1 = await connect()
        for i in range(270):
            await client1.send(["rpc", "login", 1])
            await client1.recv()
            if i == 99:
                await asyncio.sleep(1)

    test_server.test_client.websocket("/hetu/pytest_1", mimic=normal_routine_lv2)


def test_call_flooding_lv2_flooding(test_server):
    async def flooding_routine_lv2(connect):
        client1 = await connect()
        with pytest.raises(ConnectionClosedError):
            for i in range(271):
                await client1.send(["rpc", "login", 1])
                await client1.recv()
                if i == 99:
                    await asyncio.sleep(1)

    test_server.test_client.websocket("/hetu/pytest_1", mimic=flooding_routine_lv2)


@pytest.mark.timeout(20)
def test_websocket_table_subscribe(test_server):
    # 整表订阅：["sub", comp, "table"]，回包格式与range一致，之后按行收增量
    async def routine(connect):
        client1 = await connect()
        # 先造两行
        await client1.send(["rpc", "set_public_name", 1, "Alice"])
        await client1.recv()
        await client1.send(["rpc", "set_public_name", 2, "Bob"])
        await client1.recv()

        await client1.send(["sub", "PublicNames", "table"])
        await client1.recv()  # ["sub", "PublicNames.table", [rows...]]

        # 自己改名 -> 收到updt
        await client1.send(["rpc", "set_public_name", 1, "Alice2"])
        await client1.recv()  # rsp
        await client1.recv()  # updt

        # 别人改名/新增/删除 -> 也收到
        client2 = await connect()
        await client2.send(["rpc", "set_public_name", 3, "Carol"])
        await client2.recv()
        await asyncio.sleep(0.3)
        await client1.recv()  # updt insert
        await client2.send(["rpc", "set_public_name", 2, ""])
        await client2.recv()
        await asyncio.sleep(0.3)
        await client1.recv()  # updt delete

    _, response = test_server.test_client.websocket("/hetu/pytest_1", mimic=routine)
    # client_received 混有两个连接的rsp，按帧类型筛
    subs = [m for m in response.client_received if m[0] == "sub"]
    updts = [
        {int(k): v for k, v in m[2].items()}
        for m in response.client_received
        if m[0] == "updt" and m[1] == "PublicNames.table"
    ]
    assert len(subs) == 1 and len(updts) == 3
    sub_reply = subs[0]
    assert sub_reply[1] == "PublicNames.table"
    names = {row["owner"]: row["name"] for row in sub_reply[2]}
    assert names == {1: "Alice", 2: "Bob"}
    ids = {row["owner"]: row["id"] for row in sub_reply[2]}

    # 自己改名
    assert updts[0][ids[1]]["name"] == "Alice2"
    # 别人新增
    (row,) = updts[1].values()
    assert row["owner"] == 3 and row["name"] == "Carol"
    # 别人删除
    assert updts[2] == {ids[2]: None}


@pytest.mark.timeout(20)
def test_websocket_table_subscribe_limit(test_server):
    # MAX_TABLE_SUBSCRIPTION=1：未登录连接订第二张表时被断开
    closed_detected = False

    async def routine(connect):
        nonlocal closed_detected
        client1 = await connect()
        await client1.send(["sub", "PublicNames", "table"])
        await client1.recv()
        await client1.send(["sub", "PublicConfig", "table"])
        # 服务器可能先发回包再关闭，也可能直接关闭，之后任何收发都应失败
        with pytest.raises(ConnectionClosedError):
            await client1.recv()
            await asyncio.sleep(0.3)
            await client1.send(["sub", "PublicNames", "table"])
            await client1.recv()
        closed_detected = True

    _, response = test_server.test_client.websocket("/hetu/pytest_1", mimic=routine)
    assert response.client_received[0][1] == "PublicNames.table"
    assert closed_detected, "连接没有被服务器关闭"


# ==== 整表订阅要先等一个 interval 再全量读（复制延迟预算），等待不能堵住接收协程 ====


@pytest.mark.timeout(30)
def test_websocket_table_subscribe_does_not_block_following_messages(
    monkeypatch, test_server
):
    """整表订阅在等全量读的时候，后面的消息照常执行（另一个连接能先看到后面那个 RPC 的
    写入）；回复没有请求 id、SDK 按顺序对应，所以回复仍按请求顺序送达"""
    real_read = sub_module.read_whole_table_
    gate: dict[str, asyncio.Event] = {}

    async def gated_read(*args, **kwargs):
        gate["reading"].set()
        await gate["release"].wait()
        return await real_read(*args, **kwargs)

    monkeypatch.setattr(sub_module, "read_whole_table_", gated_read)
    result = {}

    async def routine(connect):
        gate["reading"], gate["release"] = asyncio.Event(), asyncio.Event()
        watcher = await connect()
        await watcher.send(["sub", "PublicNames", "range", "owner", 8001, 8002])
        watch_id = (await watcher.recv())[1]
        client = await connect()
        try:
            await client.send(["sub", "PublicNames", "table"])
            async with asyncio.timeout(5):
                await gate["reading"].wait()  # 整表订阅卡在全量读上
            await client.send(["rpc", "set_public_name", 8001, "Early"])
            try:
                async with asyncio.timeout(5):
                    result["early"] = await watcher.recv()
            except TimeoutError:
                result["early"] = None
        finally:
            gate["release"].set()
        order = []
        async with asyncio.timeout(5):
            while len(order) < 2:
                msg = await client.recv()
                if msg[0] in ("sub", "rsp"):
                    order.append(msg[0])
        result["order"] = order
        result["watch_id"] = watch_id

    test_server.test_client.websocket("/hetu/pytest_1", mimic=routine)
    assert result["early"], "整表订阅在等全量读，后面的 RPC 没被执行"
    assert result["early"][:2] == ["updt", result["watch_id"]]
    assert result["order"] == ["sub", "rsp"], "回复没按请求顺序送达"


@pytest.mark.timeout(30)
def test_websocket_table_subscribes_wait_concurrently(monkeypatch, test_server):
    """连着发两个整表订阅：第二个不用排在第一个的等待和全量读后面（登录时常一次订好几张
    表，串行就是每张一个 interval）；回复仍按请求顺序"""
    real_read = sub_module.read_whole_table_
    release: dict[str, asyncio.Event] = {}
    reads: list[str] = []

    async def gated_read(servant, table_ref, max_rows):
        reads.append(table_ref.comp_name)
        if len(reads) == 1:  # 先读的那张表卡住
            await release["event"].wait()
        return await real_read(servant, table_ref, max_rows)

    monkeypatch.setattr(sub_module, "read_whole_table_", gated_read)
    result = {}

    async def routine(connect):
        release["event"] = asyncio.Event()
        client = await connect()
        await client.send(["rpc", "login", 7101])  # 登录后整表订阅数上限才够两张
        await client.recv()
        try:
            await client.send(["sub", "PublicNames", "table"])
            await client.send(["sub", "PublicConfig", "table"])
            try:
                async with asyncio.timeout(5):
                    while len(reads) < 2:
                        await asyncio.sleep(0.01)
                result["concurrent"] = True
            except TimeoutError:
                result["concurrent"] = False
        finally:
            release["event"].set()
        replies = []
        async with asyncio.timeout(5):
            while len(replies) < 2:
                msg = await client.recv()
                if msg[0] == "sub":
                    replies.append(msg[1])
        result["replies"] = replies

    test_server.test_client.websocket("/hetu/pytest_1", mimic=routine)
    assert result["concurrent"], "第二个整表订阅排在了第一个的等待后面"
    assert result["replies"] == ["PublicNames.table", "PublicConfig.table"]


@pytest.mark.timeout(30)
def test_websocket_disconnect_while_table_subscribe_waits(monkeypatch, test_server):
    """整表订阅等全量读期间客户端断开：等待中的后半段要跟着连接一起取消，不能挂在后台"""
    real_read = sub_module.read_whole_table_
    state: dict[str, asyncio.Event] = {}

    async def gated_read(*args, **kwargs):
        state["reading"].set()
        try:
            await state["release"].wait()
        except asyncio.CancelledError:
            state["cancelled"].set()
            raise
        return await real_read(*args, **kwargs)

    monkeypatch.setattr(sub_module, "read_whole_table_", gated_read)
    result = {}

    async def routine(connect):
        for name in ("reading", "release", "cancelled"):
            state[name] = asyncio.Event()
        client = await connect()
        try:
            await client.send(["sub", "PublicNames", "table"])
            async with asyncio.timeout(5):
                await state["reading"].wait()
            await client.close()
            try:
                async with asyncio.timeout(5):
                    await state["cancelled"].wait()
                result["cancelled"] = True
            except TimeoutError:
                result["cancelled"] = False
        finally:
            state["release"].set()

    test_server.test_client.websocket("/hetu/pytest_1", mimic=routine)
    assert result["cancelled"], "断线后整表订阅的后半段还挂在后台"


@pytest.mark.timeout(30)
def test_websocket_duplicate_table_subscribe_while_waiting(monkeypatch, test_server):
    """整表订阅等全量读期间又订同一张表：认得出是重复订阅、不重复登记（未登录上限 1 张，
    重复登记会超限断线）；两个回复按顺序送达，连接照常可用"""
    real_read = sub_module.read_whole_table_
    state: dict[str, asyncio.Event] = {}
    reads: list[None] = []

    async def gated_read(*args, **kwargs):
        reads.append(None)
        await state["release"].wait()
        return await real_read(*args, **kwargs)

    monkeypatch.setattr(sub_module, "read_whole_table_", gated_read)
    result = {}

    async def routine(connect):
        state["release"] = asyncio.Event()
        client = await connect()
        try:
            await client.send(["sub", "PublicNames", "table"])
            await client.send(["sub", "PublicNames", "table"])
            # 等两次全量读都卡上（第一个的后半段 + 重复订阅的那次读）；串行处理时第二条
            # 要等第一条读完才轮到，等不到也照样放行，只看最终结果
            with contextlib.suppress(TimeoutError):
                async with asyncio.timeout(3):
                    while len(reads) < 2:
                        await asyncio.sleep(0.01)
        finally:
            state["release"].set()
        replies = []
        async with asyncio.timeout(5):
            while len(replies) < 2:
                msg = await client.recv()
                if msg[0] == "sub":
                    replies.append(msg[1])
            await client.send(["rpc", "set_public_name", 7201, "Dup"])
            while (await client.recv())[0] != "rsp":
                pass
        result["replies"] = replies

    test_server.test_client.websocket("/hetu/pytest_1", mimic=routine)
    assert result.get("replies") == ["PublicNames.table", "PublicNames.table"]


def test_check_length():
    from hetu.server.receiver import check_length

    # 闭区间内不报错
    check_length("x", [1, 2, 3], 3, 3)
    check_length("x", [1, 2, 3], 2, 5)
    check_length("x", list(range(9)), 5, 9)  # range 订阅带 desc/force 共 9 项
    # 区间外报错
    with pytest.raises(ValueError, match="Invalid x message"):
        check_length("x", [1, 2], 3, 5)
    with pytest.raises(ValueError, match="got 6"):
        check_length("x", [1, 2, 3, 4, 5, 6], 3, 5)


@pytest.mark.timeout(20)
def test_websocket_invalid_sub_length_disconnects(test_server):
    # 长度不合法的 sub 消息应被拒绝并断开连接
    closed = False

    async def routine(connect):
        nonlocal closed
        client1 = await connect()
        with pytest.raises(ConnectionClosedError):
            await client1.send(["sub", "PublicNames"])  # 缺少查询类型
            await client1.recv()
            await asyncio.sleep(0.3)
            await client1.send(["sub", "PublicNames", "table"])
            await client1.recv()
        closed = True

    test_server.test_client.websocket("/hetu/pytest_1", mimic=routine)
    assert closed, "连接没有被服务器关闭"


@pytest.mark.timeout(20)
def test_websocket_bad_message_closes_only_that_connection(test_server, caplog):
    """各种非法消息（release 模式）：发的那条连接被断开并记下原因，别的连接照常可用"""
    caplog.set_level(logging.INFO, logger="HeTu.root")

    async def send_raw(ws, data):
        # 绕过客户端 pipeline，直接发原始帧
        await type(ws).send(ws, data)

    # 用例名 → (发送动作, 服务端记录的断开原因)
    cases = {
        "empty_frame": (lambda ws: send_raw(ws, b""), "收到空帧"),
        "text_frame": (lambda ws: send_raw(ws, "hello"), "收到非二进制帧：str"),
        "not_list": (lambda ws: ws.send({"rpc": "login"}), "Invalid message format"),
        "unknown_msg_type": (lambda ws: ws.send(["bogus"]), "未知消息类型：bogus"),
        "unknown_component": (
            lambda ws: ws.send(["sub", "NoSuchComp", "table"]),
            "订阅请求非法",
        ),
        "unknown_sub_op": (
            lambda ws: ws.send(["sub", "PublicNames", "bogus"]),
            "未知订阅操作：bogus",
        ),
        # 未登录调 USER 权限的 System：执行失败，release 模式直接断开、不回原因
        "rpc_rejected": (
            lambda ws: ws.send(["rpc", "add_rls_comp_value", 1]),
            "rpc 调用失败",
        ),
    }
    closed = []
    healthy_ok = False

    async def routine(connect):
        nonlocal healthy_ok
        healthy = await connect()
        for name, (send_bad, _reason) in cases.items():
            client = await connect()
            with pytest.raises(ConnectionClosedError):
                await send_bad(client)
                await client.recv()
            closed.append(name)
        # 别的连接不受影响
        await healthy.send(["rpc", "login", 1])
        await healthy.recv()
        healthy_ok = True

    test_server.test_client.websocket("/hetu/pytest_1", mimic=routine)
    assert closed == list(cases), "有非法消息没让连接断开"
    assert healthy_ok, "正常连接被连累了"
    close_logs = [
        r.getMessage() for r in caplog.records if "接收协程结束" in r.getMessage()
    ]
    for name, (_send_bad, reason) in cases.items():
        assert any(reason in msg for msg in close_logs), f"{name} 没按预期原因断开"
    assert any(
        "不存在的Component名" in r.getMessage() and "NoSuchComp" in r.getMessage()
        for r in caplog.records
    )


@pytest.mark.timeout(20)
def test_websocket_unsub_stops_updates(test_server):
    """unsub 之后该订阅不再推 updt；同连接的其他订阅照常推"""
    owner_a, owner_b = 7001, 7002
    sub_ids = {}

    async def routine(connect):
        client1 = await connect()
        await client1.send(["rpc", "set_public_name", owner_a, "A"])
        await client1.recv()
        await client1.send(["rpc", "set_public_name", owner_b, "B"])
        await client1.recv()
        for owner in (owner_a, owner_b):
            await client1.send(["sub", "PublicNames", "get", "owner", owner])
            sub_ids[owner] = (await client1.recv())[1]

        await client1.send(["unsub", sub_ids[owner_a]])
        # A 先改、B 后改：A 的订阅要是还在，它的 updt 会先于或随 B 的一起到
        await client1.send(["rpc", "set_public_name", owner_a, "A2"])
        await client1.send(["rpc", "set_public_name", owner_b, "B2"])
        while True:
            msg = await client1.recv()
            if msg[0] == "updt" and msg[1] == sub_ids[owner_b]:
                break
        # 再多等一会儿，迟到的推送也要算上
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(0.3):
                await client1.recv()

    _, response = test_server.test_client.websocket("/hetu/pytest_1", mimic=routine)
    assert sub_ids[owner_a] and sub_ids[owner_b]
    updts = [m for m in response.client_received if m[0] == "updt"]
    assert [m[1] for m in updts] == [sub_ids[owner_b]], "退订后仍收到了推送"
    (row,) = updts[0][2].values()
    assert row["name"] == "B2"


@pytest.mark.timeout(20)
def test_websocket_motd(test_server):
    """motd 回一条明文文本帧欢迎语（不走 pipeline），之后连接照常可用"""
    motd = None

    async def routine(connect):
        nonlocal motd
        client1 = await connect()
        await client1.send(["motd"])
        # 欢迎语没经过 pipeline 编码，绕过客户端的解码直接收原始帧
        motd = await type(client1).recv(client1)
        await client1.send(["rpc", "login", 1])
        await client1.recv()

    _, response = test_server.test_client.websocket("/hetu/pytest_1", mimic=routine)
    assert motd == f"👋 Welcome to HeTu Database! v{hetu.__version__}"
    assert response.client_received[-1] == ["rsp", {"id": 1}]


@pytest.mark.timeout(20)
def test_websocket_setup_failure_deletes_connection_row(
    monkeypatch, test_server, ses_redis_service
):
    """initialize() 落库之后初始化再出错（这里让订阅管理器构造失败）：连接断开，
    且本连接那行 Connection 必须被删掉，不能永远留在库里（匿名连接数按 IP 计数）"""
    import redis

    class BrokenBroker:
        def __init__(self, *args, **kwargs):
            raise RuntimeError("broker boom")

    monkeypatch.setattr(websocket_server, "SubscriptionBroker", BrokenBroker)
    r = redis.Redis.from_url(ses_redis_service[0])
    pattern = "pytest_1:Connection:*:id:*"
    before = len(r.keys(pattern))

    async def routine(connect):
        client1 = (
            await connect()
        )  # 握手在 initialize 之前，能成功；之后服务端初始化失败
        with pytest.raises((ConnectionClosedError, ConnectionClosedOK)):
            await client1.recv()
        for _ in range(50):  # 等 finally 里的 terminate() 跑完
            if len(r.keys(pattern)) == before:
                break
            await asyncio.sleep(0.05)
        assert len(r.keys(pattern)) == before, (
            "初始化失败的连接把 Connection 行留在库里了"
        )

    test_server.test_client.websocket("/hetu/pytest_1", mimic=routine)


@pytest.mark.timeout(30)
def test_shutdown_waits_for_connection_cleanup(
    monkeypatch, test_server, ses_redis_service
):
    """关服要等连接拆完（断线 System、删 Connection 行）再关后端。清理任务不在 Sanic 的任务表
    里，不等的话 loop 一关它就永远挂住：Connection 行留在库里；SQLite 后端还会被它没提交的
    事务一直占着写锁，同进程之后的测试全部 database is locked"""
    import redis

    cleanup = websocket_server._cleanup_connection

    async def slow_cleanup(*args, **kwargs):
        await asyncio.sleep(0.5)  # 负载高时清理比停服慢
        await cleanup(*args, **kwargs)

    monkeypatch.setattr(websocket_server, "_cleanup_connection", slow_cleanup)
    r = redis.Redis.from_url(ses_redis_service[0])
    pattern = "pytest_1:Connection:*:id:*"
    before = len(r.keys(pattern))

    async def routine(connect):
        client1 = await connect()
        await client1.send(["rpc", "login", 199994])
        await client1.recv()
        # 返回后测试客户端关掉连接就停服，服务端的清理和停服同时进行

    test_server.test_client.websocket("/hetu/pytest_1", mimic=routine)
    # 返回时服务器已经停了
    assert len(r.keys(pattern)) == before, "关服没等连接清理，Connection 行留在库里了"


@pytest.mark.timeout(20)
def test_websocket_fragmented_rpc_and_following_message(test_server):
    """加密压缩包跨多帧（含事件循环切换），必须完整重组且不打乱后续 nonce。"""

    async def routine(connect):
        client = await connect()
        payload = {"message": "分片消息", "values": list(range(50))}
        await client.send(["rpc", "echo_response", payload], fragment_size=1)
        assert await client.recv() == ["rsp", payload]
        await client.send(["rpc", "echo_response", ["next"]])
        assert await client.recv() == ["rsp", ["next"]]

    test_server.test_client.websocket("/hetu/pytest_1", mimic=routine)


@pytest.mark.timeout(20)
def test_websocket_streaming_close_mid_fragment(test_server):
    """未收到 FIN 就断线，也必须取消接收器并删除 Connection 行。"""
    from websockets.frames import Opcode

    from hetu.endpoint.connection import Connection

    async def routine(connect):
        client = await connect()
        # 确认初始化完成，然后停在下一条消息的分片中间。
        await client.send(["rpc", "echo_response", ["ready"]])
        assert await client.recv() == ["rsp", ["ready"]]
        table = test_server.ctx.table_managers["pytest_1"].get_table(Connection)
        assert table is not None
        before = await table.servant_range("id", 0, float("inf"), limit=100)
        assert len(before) == 1
        await client.write_frame(False, Opcode.BINARY, b"incomplete")
        await client.close()
        for _ in range(100):
            remaining = await table.servant_range("id", 0, float("inf"), limit=100)
            if not len(remaining):
                return
            await asyncio.sleep(0.02)
        pytest.fail("Connection 行未随分片中的断线清理")

    test_server.test_client.websocket("/hetu/pytest_1", mimic=routine)


@pytest.mark.timeout(20)
def test_websocket_fragmented_message_size_limit(test_server):
    """流式接收也必须遵守消息总长上限，而非仅检查单个分片。"""
    test_server.config.WEBSOCKET_MAX_SIZE = 128

    async def routine(connect):
        client = await connect()
        await client.send(["rpc", "echo_response", ["ready"]])
        assert await client.recv() == ["rsp", ["ready"]]
        # 每帧都小于上限，整条消息超过；必须由 WebSocket 层在解密前拒绝。
        with pytest.raises(ConnectionClosedError) as exc_info:
            await type(client).send(client, [b"x" * 40] * 4)
            await client.recv()
        assert exc_info.value.rcvd is not None
        assert exc_info.value.rcvd.code == 1009

    test_server.test_client.websocket("/hetu/pytest_1", mimic=routine)
