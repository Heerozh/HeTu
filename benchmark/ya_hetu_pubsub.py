# 测试河图的订阅性能

import os
import websockets
import msgspec

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


async def rpc(websocket, message):
    # 为了测试准确的性能，采用call-response模式
    await websocket.send(encode_message(message))
    # 统计事务冲突率
    received = await websocket.recv()
    received = decode_message(received)
    return received[1]


async def websocket():
    ws = await websockets.connect(HETU_URL)
    # 订阅数据
    await rpc(ws, ["sub", "IntTable", "query", "number", 0, 100, 100])
    yield ws
    await ws.close()


# === 基准测试 ===


async def bench_pubsub_routine(websocket):
    # 压测其实就是获取订阅消息，最后统计每秒能接受多少个
    _ = await websocket.recv()


# bash
"""
cd benchmark/

export REDIS_URL=redis://:@localhost:6379/0
hetu start --app-file=./server/app.py --db=${REDIS_URL} --namespace=bench --instance=bench --workers=76

export HETU_HOST=ws://localhost:2466/hetu

# 首先启动一个更新器，不停的更新数据
ya ya_hetu_pubsub_updater.py

# 然后启动压测
ya ya_hetu_pubsub.py 


"""
