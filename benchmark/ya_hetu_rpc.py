# 测试河图的性能

import os
import websockets
import msgspec
import random
import string

msg_encoder = msgspec.msgpack.Encoder()
msg_decoder = msgspec.msgpack.Decoder()
buffer = bytearray()

# Configuration
# 可以通过环境变量配置Redis连接
HETU_URL = os.getenv("HETU_URL", "ws://localhost:2466/hetu")


# Data Scale
# 预设数据规模，例如10000个用户
BENCH_ID_RANGE = 30000


# === 工具函数 ===


def decode_message(message: bytes) -> list:
    parsed = msg_decoder.decode(message)
    return parsed


def encode_message(message: list | dict) -> bytes:
    msg_encoder.encode_into(message, buffer)
    return bytes(buffer)


# === 夹具 ===


async def websocket():
    ws = await websockets.connect(HETU_URL)
    yield ws
    await ws.close()


async def rpc(websocket, message):
    # 为了测试准确的性能，采用call-response模式
    await websocket.send(encode_message(message))
    # 统计事务冲突率
    received = await websocket.recv()
    # received = zlib.decompress(received)
    received = decode_message(received)
    return received[1]


# === 基准测试 ===


async def benchmark_hello_world(websocket: websockets.connect):
    received = await rpc(websocket, ["rpc", "hello_world"])
    return received[0]


async def benchmark_ge(websocket: websockets.connect):
    row_id = random.randint(1, BENCH_ID_RANGE)
    received = await rpc(websocket, ["rpc", "just_get", row_id])
    return received[0]


async def benchmark_get_then_update(websocket: websockets.connect):
    row_id = random.randint(1, BENCH_ID_RANGE)
    received = await rpc(websocket, ["rpc", "upsert", row_id])
    return received[0]


async def benchmark_get2_update2(websocket: websockets.connect):
    rnd_str = "".join(random.choices(string.ascii_uppercase + string.digits, k=3))
    row_id = random.randint(1, BENCH_ID_RANGE)
    received = await rpc(websocket, ["rpc", "exchange_data", rnd_str, row_id])
    return received[0]


# bash
"""
cd benchmark/

export REDIS_URL=redis://:@localhost:6379/0
hetu start --app-file=./server/app.py --db=${REDIS_URL} --namespace=bench --instance=bench --workers=76

export HETU_HOST=ws://localhost:2466/hetu

# 启动 200 个并发用户

ya ya_hetu_rpc.py -n 200 -t 5


"""
