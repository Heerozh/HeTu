import asyncio
import os
import zlib
from typing import Callable

import pytest
import logging

import sanic_testing
from websockets.exceptions import ConnectionClosedOK, ConnectionClosedError

from hetu.server.message import encode_message, decode_message
from hetu.server import start_webserver
from hetu.system import SystemClusters
from hetu.safelogging.default import DEFAULT_LOGGING_CONFIG

logger = logging.getLogger("HeTu.root")
logger.setLevel(logging.DEBUG)
logging.lastResort.setLevel(logging.DEBUG)


@pytest.fixture
def setup_websocket_proxy():
    # 设置ws测试routine方法
    protocol = dict(compress=zlib, crypto=None)

    async def websocket_proxy(url, *args, **kwargs):
        mimic = kwargs.pop("mimic", None)
        from websockets.legacy.client import connect

        class ProxyForWebsocketProxy:
            def __init__(self):
                self.wss = []

            async def close(self):
                [await ws.close() for ws in self.wss]

        ws_proxy = sanic_testing.websocket.WebsocketProxy(ProxyForWebsocketProxy())

        async def new_connection():
            ws = await connect(url, *args, **kwargs)
            ws_proxy.ws.wss.append(ws)

            do_send = ws.send
            do_recv = ws.recv

            async def send(data):
                logger.debug(f"> Sent: {data} [{len(repr(data))} bytes]")
                ws_proxy.client_sent.append(data)
                await do_send(encode_message(data, protocol))

            async def recv():
                message = decode_message(await do_recv(), protocol)
                logger.debug(f"< Received: {message} [{len(repr(message))} bytes]")
                ws_proxy.client_received.append(message)
                return message

            def clear_recv():
                ws_proxy.client_received.clear()

            ws.send = send  # type: ignore
            ws.recv = recv  # type: ignore
            ws.clear_recv = clear_recv

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
def test_server(setup_websocket_proxy, mod_redis_service):
    SystemClusters()._clear()
    port = 23318
    mod_redis_service(port)
    app_file = os.path.join(os.path.dirname(__file__), "app.py")
    logging_cfg = DEFAULT_LOGGING_CONFIG
    logging_cfg["loggers"]["HeTu.replay"]["level"] = logging.DEBUG
    server = start_webserver(
        "Hetu-test",
        {
            "APP_FILE": app_file,
            "NAMESPACE": "pytest",
            "INSTANCE_NAME": "pytest_1",
            "LISTEN": f"0.0.0.0:874",
            "PACKET_COMPRESSION_CLASS": "zlib",
            "BACKENDS": {
                "Redis": {
                    "type": "Redis",
                    "master": f"redis://127.0.0.1:{port}/0",
                }
            },
            "CLIENT_SEND_LIMITS": [[10, 1], [27, 5], [100, 50], [300, 300]],
            "LOGGING": logging_cfg,
            "DEBUG": True,
            "WORKER_NUM": 4,
            "ACCESS_LOG": False,
        },
        os.getpid(),
        True,
    )

    yield server

    server.stop()


def test_websocket_started(test_server):
    # 测试服务器是否正常启动
    # 这行future出现异常是正常的，因为下面的请求很快就关闭了
    request, response = test_server.test_client.get("/")
    assert request.method.lower() == "get"
    assert "Powered by HeTu" in response.body.decode()
    assert response.status == 200
    # 因为上面get("/")会启动future线程，也因此启动了redis，所以要切换下connection_pool
    # app.ctx.default_backend.reset_connection_pool() 优化了process，不再需要


@pytest.mark.timeout(20)
def test_websocket_call_system(test_server):
    # 测试call和结果
    async def normal_routine(connect):
        client1 = await connect()
        await client1.send(["sys", "login", 1])
        await client1.recv()

        await client1.send(["sub", "RLSComp", "query", "owner", 1, 999])
        await client1.send(["sub", "IndexComp1", "query", "owner", 1, 999])
        await client1.recv()
        await client1.recv()

        await client1.send(["sys", "add_rls_comp_value", 1])
        await client1.recv()  # 首次sub这里会卡至少0.5s等待连接

        await client1.send(["sys", "login", 2])  # 测试重复登录应该无效
        await client1.recv()

        # 正式开始接受sub消息
        await client1.send(["sys", "add_rls_comp_value", 1])
        await client1.recv()

        # 模拟其他用户修改了用户1订阅的数据
        client2 = await connect()
        await client2.send(["sys", "login", 2])
        # 这个是rls数据，client1不会收到
        await client2.send(["sys", "add_rls_comp_value", 9])
        # 这个client1应该收到
        await client2.send(["sys", "create_row", 2, 9, "1"])
        await asyncio.sleep(0.1)

        await client1.recv()  # 因为客户端2并没订阅，测试用户1是否收到

    _, response1 = test_server.test_client.websocket("/hetu", mimic=normal_routine)
    # print(response1.client_received)
    # 测试add_rls_comp_value调用了2次
    assert response1.client_received[3][2]["1"] == {"id": 1, "owner": 1, "value": 101}
    assert response1.client_received[5][2]["1"] == {"id": 1, "owner": 1, "value": 102}

    # 测试收到连接2的+9
    assert response1.client_received[6][2]["1"] == {"id": 1, "owner": 2, "value": 9}


def test_websocket_kick_connect(test_server):
    # 测试踢掉别人的连接
    async def kick_routine(connect):
        client1 = await connect()
        await client1.send(["sys", "login", 1])
        await client1.send(["sys", "add_rls_comp_value", 1])
        await asyncio.sleep(0.1)

        client2 = await connect()
        await client2.send(["sys", "login", 1])
        await client2.send(["sys", "add_rls_comp_value", 2])
        await asyncio.sleep(0.1)

        # 虽然上面的client2踢掉了client1，但是client1并不会主动断开连接，
        # 需要调用一次system才能发现自己被踢掉了
        await client1.send(["sys", "add_rls_comp_value", 3])

        # 测试踢出成功
        await asyncio.sleep(0.1)
        with pytest.raises(ConnectionClosedError):
            await client1.send(["sys", "add_rls_comp_value", 4])

    _, response1 = test_server.test_client.websocket("/hetu", mimic=kick_routine)
    # 用来确定最后一行执行到了，不然在中途报错会被webserver catch跳过，导致test通过
    assert response1.client_sent[-1] == [
        "sys",
        "add_rls_comp_value",
        4,
    ], "最后一行没执行到"


def test_call_flooding_lv1_normal(test_server):
    # 测试CLIENT_SEND_LIMITS配置
    # CLIENT_SEND_LIMITS:
    # - [ 10, 1 ]  <---测试该层
    # - [ 27, 5 ]
    # 登录后默认CLIENT_SEND_LIMITS值乘10,所以是100次/秒
    async def normal_routine(connect):
        client1 = await connect()
        for i in range(100):
            await client1.send(["sys", "login", 1])
            await client1.recv()

    test_server.test_client.websocket("/hetu", mimic=normal_routine)


def test_call_flooding_lv1_flooding(test_server):
    # 因为同时启动2个websocket会报
    # sanic.exceptions.ServerError: Sanic server could not start: [Errno 98] Address already in use.
    # 所以分2个测试，或者可以尝试启动时用随机的port
    async def flooding_routine(connect):
        client1 = await connect()
        with pytest.raises(ConnectionClosedError):
            for i in range(101):
                await client1.send(["sys", "login", 1])
                await client1.recv()

    test_server.test_client.websocket("/hetu", mimic=flooding_routine)


def test_call_flooding_lv2_normal(test_server):
    # 测试CLIENT_SEND_LIMITS配置
    # CLIENT_SEND_LIMITS:
    # - [ 10, 1 ]
    # - [ 27, 5 ]  <---测试该层
    # 登录后默认CLIENT_SEND_LIMITS值乘10,所以是270次/秒
    async def normal_routine_lv2(connect):
        client1 = await connect()
        for i in range(270):
            await client1.send(["sys", "login", 1])
            await client1.recv()
            if i == 99:
                await asyncio.sleep(1)

    test_server.test_client.websocket("/hetu", mimic=normal_routine_lv2)


def test_call_flooding_lv2_flooding(test_server):
    async def flooding_routine_lv2(connect):
        client1 = await connect()
        with pytest.raises(ConnectionClosedError):
            for i in range(271):
                await client1.send(["sys", "login", 1])
                await client1.recv()
                if i == 99:
                    await asyncio.sleep(1)

    test_server.test_client.websocket("/hetu", mimic=flooding_routine_lv2)
