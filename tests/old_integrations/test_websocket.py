import asyncio
import os
import unittest
import zlib

import sanic_testing
from websockets.exceptions import ConnectionClosedOK, ConnectionClosedError

from backend_mgr import UnitTestBackends
from hetu.server.message import encode_message, decode_message
from hetu.server import start_webserver
from hetu.system import SystemClusters
from hetu.safelogging.default import DEFAULT_LOGGING_CONFIG


class TestWebsocket(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.backend_mgr = UnitTestBackends()
        cls.backend_mgr.start_redis_server()

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
                mimic: callable
                try:
                    await mimic(new_connection)
                except ConnectionClosedOK:
                    pass

            return ws_proxy

        sanic_testing.testing.websocket_proxy = websocket_proxy


    @classmethod
    def tearDownClass(cls):
        cls.backend_mgr.teardown()

    @classmethod
    def create_app_under_current_coroutine(cls):
        SystemClusters()._clear()
        app_file = os.path.join(os.path.dirname(__file__), 'app.py')
        return start_webserver("Hetu-test", {
            'APP_FILE': app_file,
            'NAMESPACE': 'ssw',
            'INSTANCE_NAME': 'unittest1',
            'LISTEN': f"0.0.0.0:874",
            'PACKET_COMPRESSION_CLASS': 'zlib',
            'BACKENDS': {
                'Redis': {
                    "type": "Redis",
                    "master": 'redis://127.0.0.1:23318/0',
                }
            },
            'CLIENT_SEND_LIMITS': [[10, 1], [27, 5], [100, 50], [300, 300]],
            'LOGGING': DEFAULT_LOGGING_CONFIG,
            'DEBUG': True,
            'WORKER_NUM': 4,
            'ACCESS_LOG': False,
        }, os.getpid(), True)

    def test_websocket(self):
        # 测试服务器是否正常启动
        app = self.create_app_under_current_coroutine()
        # 这行future出现异常是正常的，因为下面的请求很快就关闭了
        request, response = app.test_client.get("/")
        self.assertEqual(request.method.lower(), "get")
        self.assertIn("Powered by HeTu", response.body.decode())
        self.assertEqual(response.status, 200)
        # 因为上面get("/")会启动future线程，也因此启动了redis，所以要切换下connection_pool
        app.ctx.default_backend.reset_connection_pool()

        # 测试call和结果
        async def normal_routine(connect):
            client1 = await connect()
            await client1.send(['sys', 'login', 1])
            await client1.recv()
            await client1.send(['sub', 'HP', 'query', 'owner', 1, 999])
            await client1.recv()
            await client1.send(['sub', 'MP', 'query', 'owner', 1, 999])
            await client1.recv()
            await client1.send(['sys', 'use_hp', 1])
            await client1.send(['sys', 'login', 2])  # 测试重复登录
            await client1.recv()
            client1.clear_recv()

            # 正式开始接受sub消息
            await client1.send(['sys', 'use_hp', 1])
            await client1.recv()

            client2 = await connect()
            await client2.send(['sys', 'login', 2])
            await client2.send(['sys', 'use_mp', 1])
            await asyncio.sleep(0.1)

            await client1.recv()

        _, response1 = app.test_client.websocket("/hetu", mimic=normal_routine)
        # print(response1.client_received)
        # 测试hp减2
        self.assertEqual(response1.client_received[0][2]['1'],
                         {'id': 1, 'owner': 1, 'value': 98})
        # 测试收到连接2减的mp
        self.assertEqual(response1.client_received[1][2]['1'],
                         {'id': 1, 'owner': 2, 'value': 99})

        # 准备下一轮测试，重置redis connection_pool，因为切换线程了
        app.ctx.default_backend.reset_connection_pool()

        # 测试踢掉别人的连接
        async def kick_routine(connect):
            client1 = await connect()
            await client1.send(['sys', 'login', 1])
            await client1.send(['sys', 'use_hp', 1])
            await asyncio.sleep(0.1)

            client2 = await connect()
            await client2.send(['sys', 'login', 1])
            await client2.send(['sys', 'use_hp', 1])
            await asyncio.sleep(0.1)

            await client1.send(['sys', 'use_hp', 1])
            await asyncio.sleep(0.1)
            with self.assertRaisesRegex(ConnectionClosedError, '.*'):
                await client1.send(['sys', 'use_hp', 2])

        _, response1 = app.test_client.websocket("/hetu", mimic=kick_routine)
        # 用来确定最后一行执行到了，不然在中途报错会被webserver catch跳过，导致test通过
        self.assertEqual(response1.client_sent[-1], ['sys', 'use_hp', 2], "最后一行没执行到")
        app.ctx.default_backend.reset_connection_pool()

        app.stop()

    def test_flooding(self):
        async def normal_routine(connect):
            client1 = await connect()
            for i in range(100):
                await client1.send(['sys', 'login', 1])
                await client1.recv()

        async def flooding_routine(connect):
            client1 = await connect()
            for i in range(101):
                await client1.send(['sys', 'login', 1])
                await client1.recv()

        app = self.create_app_under_current_coroutine()
        app.test_client.websocket("/hetu", mimic=normal_routine)

        # 准备下一轮测试，重置redis connection_pool，因为切换线程了
        # todo 改进process后应该不需要该调用
        # app.ctx.default_backend.reset_connection_pool()
        with self.assertRaises(Exception):
            app.test_client.websocket("/hetu", mimic=flooding_routine)

        async def normal_routine_lv2(connect):
            client1 = await connect()
            for i in range(270):
                await client1.send(['sys', 'login', 1])
                await client1.recv()
                if i == 99:
                    await asyncio.sleep(1)

        async def flooding_routine_lv2(connect):
            client1 = await connect()
            for i in range(271):
                await client1.send(['sys', 'login', 1])
                await client1.recv()
                if i == 99:
                    await asyncio.sleep(1)

        app.stop()
        app = self.create_app_under_current_coroutine()
        app.test_client.websocket("/hetu", mimic=normal_routine_lv2)

        # app.ctx.default_backend.reset_connection_pool()
        with self.assertRaises(Exception):
            app.test_client.websocket("/hetu", mimic=flooding_routine_lv2)

        app.stop()

if __name__ == '__main__':
    unittest.main()
