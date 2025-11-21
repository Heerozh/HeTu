import asyncio
import os
import zlib
from typing import Callable

import pytest

import sanic_testing
from websockets.exceptions import ConnectionClosedOK, ConnectionClosedError

from hetu.server.message import encode_message, decode_message
from hetu.server import start_webserver
from hetu.system import SystemClusters
from hetu.safelogging.default import DEFAULT_LOGGING_CONFIG


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
                ws_proxy.client_sent.append(data)
                await do_send(encode_message(data, protocol))

            async def recv():
                message = decode_message(await do_recv(), protocol)
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
    return start_webserver(
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
            "LOGGING": DEFAULT_LOGGING_CONFIG,
            "DEBUG": True,
            "WORKER_NUM": 4,
            "ACCESS_LOG": False,
        },
        os.getpid(),
        True,
    )


def test_websocket_started(test_server):
    # 测试服务器是否正常启动
    # 这行future出现异常是正常的，因为下面的请求很快就关闭了
    request, response = test_server.test_client.get("/")
    assert request.method.lower() == "get"
    assert "Powered by HeTu" in response.body.decode()
    assert response.status == 200
    # 因为上面get("/")会启动future线程，也因此启动了redis，所以要切换下connection_pool
    # app.ctx.default_backend.reset_connection_pool() 优化了process，不再需要


def test_websocket_call_system(test_server):
    # 测试call和结果
    async def normal_routine(connect):
        client1 = await connect()
        await client1.send(["sys", "login", 1])
        await client1.recv()
        await client1.send(["sub", "HP", "query", "owner", 1, 999])
        await client1.recv()
        await client1.send(["sub", "MP", "query", "owner", 1, 999])
        await client1.recv()
        await client1.send(["sys", "use_hp", 1])
        await client1.send(["sys", "login", 2])  # 测试重复登录
        await client1.recv()
        client1.clear_recv()

        # 正式开始接受sub消息
        await client1.send(["sys", "use_hp", 1])
        await client1.recv()

        client2 = await connect()
        await client2.send(["sys", "login", 2])
        await client2.send(["sys", "use_mp", 1])
        await asyncio.sleep(0.1)

        await client1.recv()

    _, response1 = test_server.test_client.websocket("/hetu", mimic=normal_routine)
    # print(response1.client_received)
    # 测试hp减2
    assert response1.client_received[0][2]["1"] == {"id": 1, "owner": 1, "value": 98}

    # 测试收到连接2减的mp
    assert response1.client_received[1][2]["1"] == {"id": 1, "owner": 2, "value": 99}

    # 准备下一轮测试，重置redis connection_pool，因为切换线程了
    # app.ctx.default_backend.reset_connection_pool() 优化了process，不再需要

    # 测试踢掉别人的连接
    async def kick_routine(connect):
        client1 = await connect()
        await client1.send(["sys", "login", 1])
        await client1.send(["sys", "use_hp", 1])
        await asyncio.sleep(0.1)

        client2 = await connect()
        await client2.send(["sys", "login", 1])
        await client2.send(["sys", "use_hp", 1])
        await asyncio.sleep(0.1)

        await client1.send(["sys", "use_hp", 1])
        await asyncio.sleep(0.1)
        with self.assertRaisesRegex(ConnectionClosedError, ".*"):
            await client1.send(["sys", "use_hp", 2])

    _, response1 = app.test_client.websocket("/hetu", mimic=kick_routine)
    # 用来确定最后一行执行到了，不然在中途报错会被webserver catch跳过，导致test通过
    assert response1.client_sent[-1] == ["sys", "use_hp", 2], "最后一行没执行到"

    # app.ctx.default_backend.reset_connection_pool() 优化了process，不再需要

    app.stop()


def test_flooding(test_server):
    async def normal_routine(connect):
        client1 = await connect()
        for i in range(100):
            await client1.send(["sys", "login", 1])
            await client1.recv()

    async def flooding_routine(connect):
        client1 = await connect()
        for i in range(101):
            await client1.send(["sys", "login", 1])
            await client1.recv()

    test_server.test_client.websocket("/hetu", mimic=normal_routine)

    # 准备下一轮测试，重置redis connection_pool，因为切换线程了
    # app.ctx.default_backend.reset_connection_pool() 优化了process，不再需要
    with pytest.raises(Exception):
        test_server.test_client.websocket("/hetu", mimic=flooding_routine)

    async def normal_routine_lv2(connect):
        client1 = await connect()
        for i in range(270):
            await client1.send(["sys", "login", 1])
            await client1.recv()
            if i == 99:
                await asyncio.sleep(1)

    async def flooding_routine_lv2(connect):
        client1 = await connect()
        for i in range(271):
            await client1.send(["sys", "login", 1])
            await client1.recv()
            if i == 99:
                await asyncio.sleep(1)

    test_server.stop()
    app = self.create_app_under_current_coroutine()
    app.test_client.websocket("/hetu", mimic=normal_routine_lv2)

    # app.ctx.default_backend.reset_connection_pool()
    with pytest.raises(Exception):
        app.test_client.websocket("/hetu", mimic=flooding_routine_lv2)

    app.stop()
