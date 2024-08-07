import asyncio
import os
import unittest
import zlib

import sanic_testing

from backend_mgr import UnitTestBackends
from hetu.server import encode_message, decode_message
from hetu.server import start_webserver
from hetu.system import SystemClusters


class TestWebsocket(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        SystemClusters()._clear()
        cls.backend_mgr = UnitTestBackends()
        cls.backend_mgr.start_redis_server()
        cls.app = start_webserver("Hetu-test", {
            'APP_FILE': 'app.py',
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
            'DEBUG': True,
            'WORKER_NUM': 4,
            'ACCESS_LOG': False,
        }, os.getpid(), True)

    @classmethod
    def tearDownClass(cls):
        cls.backend_mgr.teardown()

    def test_websocket(self):
        from websockets.exceptions import ConnectionClosedOK, ConnectionClosedError

        # 测试服务器是否正常启动
        request, response = self.app.test_client.get("/")
        self.assertEqual(request.method.lower(), "get")
        self.assertIn("Powered by HeTu", response.body.decode())
        self.assertEqual(response.status, 200)

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

        _, response1 = self.app.test_client.websocket("/hetu", mimic=normal_routine)
        # print(response1.client_received)
        # 测试hp减2
        self.assertEqual(response1.client_received[0][2]['1'],
                         {'id': '1', 'owner': '1', 'value': '98'})
        # 测试收到连接2减的mp
        self.assertEqual(response1.client_received[1][2]['1'],
                         {'id': '1', 'owner': '2', 'value': '99'})

        # 准备下一轮测试，重置redis connection_pool，因为切换线程了
        self.app.ctx.default_backend.reset_connection_pool()

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

        _, response1 = self.app.test_client.websocket("/hetu", mimic=kick_routine)
        # 用来确定最后一行执行到了，不然在中途报错会被webserver catch跳过，导致test通过
        self.assertEqual(response1.client_sent[-1], ['sys', 'use_hp', 2], "最后一行没执行到")
        self.app.ctx.default_backend.reset_connection_pool()


if __name__ == '__main__':
    unittest.main()
