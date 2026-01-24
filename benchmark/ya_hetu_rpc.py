# 测试河图的性能

from typing import cast
import os
import websockets
import msgspec
import random
import string
from nacl.public import PrivateKey
from hetu.server import pipeline

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


async def connection():
    ws = await websockets.connect(HETU_URL)

    # 设置管道
    client_pipe = pipeline.MessagePipeline()
    client_pipe.add_layer(pipeline.LimitCheckerLayer())
    client_pipe.add_layer(pipeline.JSONBinaryLayer())
    client_pipe.add_layer(pipeline.ZstdLayer())
    crypto_layer = pipeline.CryptoLayer()
    client_pipe.add_layer(crypto_layer)
    pipe_ctx = None
    # 生成密钥对
    private_key = PrivateKey.generate()
    public_key = private_key.public_key
    handshake_msg = [b""] * 4
    handshake_msg[-1] = public_key.encode()
    # 握手
    await ws.send(client_pipe.encode(None, handshake_msg))
    data = cast(bytes, await ws.recv())
    peer_handshake = client_pipe.decode(None, data)
    assert type(peer_handshake) is list
    ctx, _ = client_pipe.handshake(peer_handshake)
    ctx[-1] = crypto_layer.client_handshake(private_key.encode(), peer_handshake[-1])

    pipe_ctx = ctx

    yield ws, client_pipe, pipe_ctx
    await ws.close()


async def rpc(connection, message):
    websocket, client_pipe, pipe_ctx = connection
    # 为了测试准确的性能，采用call-response模式
    await websocket.send(client_pipe.encode(pipe_ctx, message))
    # 统计事务冲突率
    received = await websocket.recv()
    received = client_pipe.decode(pipe_ctx, received)
    return received[1]


# === 基准测试 ===


async def benchmark_hello_world(connection):
    received = await rpc(connection, ["rpc", "hello_world"])
    return received[0]


async def benchmark_get(connection):
    row_id = random.randint(1, BENCH_ID_RANGE)
    received = await rpc(connection, ["rpc", "just_get", row_id])
    return received[0]


async def benchmark_get_then_update(connection):
    row_id = random.randint(1, BENCH_ID_RANGE)
    received = await rpc(connection, ["rpc", "upsert", row_id])
    return received[0]


async def benchmark_get2_update2(connection):
    rnd_str = "".join(random.choices(string.ascii_uppercase + string.digits, k=3))
    row_id = random.randint(1, BENCH_ID_RANGE)
    received = await rpc(connection, ["rpc", "exchange_data", rnd_str, row_id])
    return received[0]


# bash
"""
cd benchmark/

export REDIS_URL='redis://:@localhost:6379/0'
uv run hetu start --app-file=./server/app.py --db=${REDIS_URL} --namespace=bench --instance=bench --workers=76

export HETU_HOST=ws://localhost:2466/hetu

# 启动 200 个并发用户

uv run ya ya_hetu_rpc.py -n 1800 -t 2

# 测试ttl

uv run ya ya_hetu_rpc.py -n 1 -p 1 -t 2

"""
