import asyncio
import unittest
import docker
import zlib
import sys
import os
import websockets
import ssl
import subprocess
import aiohttp
from threading import Thread
import sanic_testing.testing

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

        # self.server = subprocess.Popen(
        #     ["python", "-m", "hetu", "start", "--app-file=app.py", "--namespace=ssw",
        #      "--instance=unittest1", "--port=23874", "--db=redis://127.0.0.1:23318/0"],
        #     stdout=subprocess.PIPE)
        # # 等待服务器启动完成
        # self.server_out = asyncio.Queue()
        #
        # def enqueue_output(out, queue):
        #     for _line in iter(out.readline, b''):
        #         sys.stdout.write(_line.decode())
        #         queue.put_nowait(_line.decode())
        #     out.close()
        # t = Thread(target=enqueue_output, args=(self.server.stdout, self.server_out))
        # t.daemon = True
        # t.start()
        #
        # while True:
        #     try:
        #         line = self.server_out.get_nowait()
        #         if 'Starting worker' in line:
        #             break
        #     except asyncio.queues.QueueEmpty:
        #         pass

    def __del__(self):
        for container in self.containers:
            try:
                container.kill()
            except (docker.errors.NotFound, ImportError):
                pass
        # self.server.terminate()
        # self.server.kill()

    def test_websocket(self):
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
            from websockets.exceptions import ConnectionClosedOK

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
        print(response1.client_received)
        # 测试hp减2
        self.assertEqual(response1.client_received[2][2]['1'],
                         {'id': '1', 'owner': '1', 'value': '98'})
        # 测试收到连接2减的mp
        self.assertEqual(response1.client_received[3][2]['1'],
                         {'id': '1', 'owner': '2', 'value': '99'})
        #
        #
        #
        # ssl_context = ssl.create_default_context()
        # ssl_context.check_hostname = False
        # ssl_context.verify_mode = ssl.CERT_NONE  # 应该使用ssl_context.load_cert_chain(cert_file)更安全
        #
        # # 测试服务器是否正常启动
        # conn = aiohttp.TCPConnector(ssl=ssl_context)
        # async with aiohttp.ClientSession(connector=conn) as session:
        #     async with session.get('https://127.0.0.1:23874/') as response:
        #         html = await response.text()
        #         self.assertIn("Powered by HeTu", html)
        #         self.assertEqual(response.status, 200)
        #
        # protocol = dict(compress=zlib, crypto=None)
        # client1_received = []
        #
        # # 测试websocket能正常
        # async with websockets.connect(f'ws://127.0.0.1:23874/hetu') as ws:
        #     await ws.send(encode_message(['motd'], protocol))
        #     print(await ws.recv())
        #
        # # 测试call和结果
        # async def client1_routine():
        #     async with websockets.connect(f'wss://127.0.0.1:23874/hetu', ssl=ssl_context) as ws:
        #         await ws.send(encode_message(['sys', 'login', 1], protocol))
        #         await ws.send(encode_message(['sub', 'HP', 'query', 'owner', 1, 999], protocol))
        #         await ws.send(encode_message(['sub', 'MP', 'query', 'owner', 1, 999], protocol))
        #         await ws.send(encode_message(['sys', 'use_hp', 1], protocol))
        #         await ws.send(encode_message(['sys', 'login', 2], protocol))
        #         await ws.send(encode_message(['sys', 'use_hp', 1], protocol))
        #         client1_received.append(decode_message(await ws.recv(), protocol))
        #         client1_received.append(decode_message(await ws.recv(), protocol))
        #         await asyncio.sleep(5)
        #         client1_received.append(decode_message(await ws.recv(), protocol))
        #
        # async def client2_routine():
        #     async with websockets.connect(f'wss://127.0.0.1:23874/hetu', ssl=ssl_context) as ws:
        #         await asyncio.sleep(1)
        #         await ws.send(encode_message(['sys', 'login', 2], protocol))
        #         await ws.send(encode_message(['sys', 'use_mp', 1], protocol))
        #         await asyncio.sleep(1)
        #
        # # task1 = asyncio.create_task(client1_routine())
        # # task2 = asyncio.create_task(client2_routine())
        # # await asyncio.gather(task2)
        # # await task1
        # await asyncio.sleep(5)
        # # client_received1 = [decode_message(msg, protocol) for msg in response1.client_received]
        # print(client1_received)
        # self.assertEqual(client1_received[2][2]['1'], {'id': '1', 'owner': '1', 'value': '98'})
        # self.assertEqual(client1_received[3][2]['1'], {'id': '1', 'owner': '2', 'value': '99'})
        #
        # # response.client_received
        # # 测试踢掉别人的连接


if __name__ == '__main__':
    unittest.main()
