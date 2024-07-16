import unittest
import docker
import os
from hetu.server import start_webserver


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
        # 启动服务器
        self.containers.append(
            client.containers.run("redis:latest", detach=True, ports={'6379/tcp': 23318},
                                  name='hetu_test_redis', auto_remove=True)
        )
        # 启动websocket
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
        pass

    def test_websocket(self):
        # 测试服务器是否正常启动
        request, response = self.app.test_client.get("/")
        self.assertEqual(request.method.lower(), "get")
        self.assertIn("Powered by HeTu", response.body.decode())
        self.assertEqual(response.status, 200)

        # 测试call和结果

        # 测试踢掉别人的连接




if __name__ == '__main__':
    unittest.main()
