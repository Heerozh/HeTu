import asyncio
import websockets


# --- aiohttp (Port 18001) ---
async def client_aiohttp():
    async with websockets.connect("ws://127.0.0.1:18001") as ws:
        yield ws


async def benchmark_aiohttp_ping(client_aiohttp):
    pong_waiter = await client_aiohttp.ping()
    await pong_waiter


async def benchmark_aiohttp_echo(client_aiohttp):
    await client_aiohttp.send("hello")
    await client_aiohttp.recv()


# --- websockets (Port 18002) ---
async def client_websockets():
    async with websockets.connect("ws://127.0.0.1:18002") as ws:
        yield ws


async def benchmark_websockets_ping(client_websockets):
    pong_waiter = await client_websockets.ping()
    await pong_waiter


async def benchmark_websockets_echo(client_websockets):
    await client_websockets.send("hello")
    await client_websockets.recv()


# --- sanic (Port 18003) ---
async def client_sanic():
    async with websockets.connect("ws://127.0.0.1:18003") as ws:
        yield ws


async def benchmark_sanic_ping(client_sanic):
    pong_waiter = await client_sanic.ping()
    await pong_waiter


async def benchmark_sanic_echo(client_sanic):
    await client_sanic.send("hello")
    await client_sanic.recv()


# --- socketify (Port 18004) ---
async def client_socketify():
    async with websockets.connect("ws://127.0.0.1:18004") as ws:
        yield ws


async def benchmark_socketify_ping(client_socketify):
    pong_waiter = await client_socketify.ping()
    await pong_waiter


async def benchmark_socketify_echo(client_socketify):
    await client_socketify.send("hello")
    await client_socketify.recv()


"""
请按如下步骤在终端中运行压测：

打开第一个终端，启动所有的 WebSocket 服务器：
bash
cd benchmark
uv run python hypothesis/ws_bench_servers.py
这会同时在后台启动 aiohttp, websockets, sanic, socketify 四个服务进程。

打开第二个终端，使用 ya 工具跑压测客户端：
bash
cd benchmark
uv run ya hypothesis/ya_ws_ping.py
ya_ws_ping.py 这个脚本里我们分别测试了：

纯协议层 Ping/Pong 吞吐 (benchmark_xxx_ping)
应用层 Text Echo 收发吞吐 (benchmark_xxx_echo)

Windows:
|                           |      CPS |
|:--------------------------|---------:|
| benchmark_aiohttp_echo    | 23680    |
| benchmark_aiohttp_ping    | 26620.1  |
| benchmark_sanic_echo      | 19334.9  |
| benchmark_sanic_ping      | 29979    |
| benchmark_socketify_echo  | 40090.6  |
| benchmark_socketify_ping  |  8373.83 |
| benchmark_websockets_echo | 18546.3  |
| benchmark_websockets_ping | 25666.3  |
"""
