# 测试河图的性能

import os
import websockets
import msgspec

msg_encoder = msgspec.msgpack.Encoder()
msg_decoder = msgspec.msgpack.Decoder()
buffer = bytearray()

# Configuration
# 可以通过环境变量配置Redis连接
HETU_URL = os.getenv("HETU_URL", "ws://localhost:2466")


# Data Scale
# 预设数据规模，例如10000个用户
ACC_ID_RANGE = 30000


# === 工具函数 ===


def decode_message(message: bytes) -> list:
    parsed = msg_decoder.decode(message)
    return parsed


def encode_message(message: list | dict) -> bytes:
    msg_encoder.encode_into(message, buffer)
    return bytes(buffer)


# === 夹具 ===


async def websocket():
    ws = websockets.connect(HETU_URL)
    yield ws
    await ws.connection.close()


async def rpc(websocket, message):
    # 为了测试准确的性能，采用call-response模式
    await websocket.send(encode_message(message))
    # 统计事务冲突率
    received = await websocket.recv()
    # received = zlib.decompress(received)
    received = decode_message(received)
    return received


# === 基准测试 ===


async def benchmark_hello_world(websocket: websockets.connect):
    received = rpc(websocket, ["rpc", "hello_world"])
    return received


"""

export HETU_HOST=ws://localhost:2466

# 启动 200 个并发用户
cd benchmark/
ya ya_hetu.py -n 200 -t 5


"""
