import asyncio
import unittest
import docker
import zlib
import os
import sanic_testing

from hetu.server import start_webserver
from hetu.server import encode_message, decode_message


class TestWebsocket(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # docker启动对应的backend
        self.containers = []
        try:
            client = docker.from_env()
        except docker.errors.DockerException:
            raise unittest.SkipTest("请启动DockerDesktop或者Docker服务后再运行测试")
        # 先删除已启动的
        try:
            client.containers.get('hetu_test_redis').kill()
            client.containers.get('hetu_test_redis').remove()
        except (docker.errors.NotFound, docker.errors.APIError):
            pass
        # 启动redis服务器
        self.containers.append(
            client.containers.run("redis:latest", detach=True, ports={'6379/tcp': 23318},
                                  name='hetu_test_redis', auto_remove=True)
        )
        # 启动hetu服务器
        self.app = start_webserver("Hetu-test", {
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

    def __del__(self):
        for container in self.containers:
            try:
                container.kill()
            except (docker.errors.NotFound, ImportError):
                pass

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

                ws.send = send  # type: ignore
                ws.recv = recv  # type: ignore

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
            await client1.send(['sub', 'HP', 'query', 'owner', 1, 999])
            await client1.send(['sub', 'MP', 'query', 'owner', 1, 999])
            await client1.send(['sys', 'use_hp', 1])
            await client1.send(['sys', 'login', 2])  # 测试重复登录
            await client1.send(['sys', 'use_hp', 1])
            await client1.recv()
            await client1.recv()
            await client1.recv()

            client2 = await connect()
            await client2.send(['sys', 'login', 2])
            await client2.send(['sys', 'use_mp', 1])
            await asyncio.sleep(1)

            await client1.recv()

        _, response1 = self.app.test_client.websocket("/hetu", mimic=normal_routine)
        # print(response1.client_received)
        # 测试hp减2
        self.assertEqual(response1.client_received[2][2]['1'],
                         {'id': '1', 'owner': '1', 'value': '98'})
        # 测试收到连接2减的mp
        self.assertEqual(response1.client_received[3][2]['1'],
                         {'id': '1', 'owner': '2', 'value': '99'})

        # 测试踢掉别人的连接
        async def kick_routine(connect):
            client1 = await connect()
            await client1.send(['sys', 'login', 1])
            await client1.send(['sys', 'use_hp', 1])
            await asyncio.sleep(1)

            client2 = await connect()
            await client2.send(['sys', 'login', 1])
            await client2.send(['sys', 'use_hp', 1])
            await asyncio.sleep(1)

            await client1.send(['sys', 'use_hp', 1])
            await asyncio.sleep(1)
            with self.assertRaisesRegex(ConnectionClosedError, '.*'):
                await client1.send(['sys', 'use_hp', 2])

        _, response1 = self.app.test_client.websocket("/hetu", mimic=kick_routine)


if __name__ == '__main__':
    unittest.main()
