# 测试河图的性能

import os
import random
import string

import msgspec
import websockets
from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey

from hetu.server import pipeline

msg_encoder = msgspec.msgpack.Encoder()
msg_decoder = msgspec.msgpack.Decoder()
buffer = bytearray()

# Configuration
# 可以通过环境变量配置 WebSocket 连接
HETU_URL = os.getenv("HETU_URL", "ws://localhost:2466/hetu/bench")


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


def _bytes(data: str | bytes) -> bytes:
    return data.encode() if isinstance(data, str) else data


async def connection():
    ws = await websockets.connect(HETU_URL)

    # 设置管道
    client_pipe = pipeline.MessagePipeline()
    client_pipe.add_layer(pipeline.JSONBinaryLayer())
    client_pipe.add_layer(pipeline.ZlibLayer())
    crypto_layer = pipeline.CryptoLayer()
    client_pipe.add_layer(crypto_layer)
    # 握手（与 tests/test_websocket.py 一致）
    private_key = X25519PrivateKey.generate()
    handshake_msg = [b""] * client_pipe.num_handshake_layers
    handshake_msg[-1] = private_key.public_key().public_bytes_raw()
    await ws.send(client_pipe.encode(None, handshake_msg))
    peer_handshake = client_pipe.decode(None, _bytes(await ws.recv()))
    assert isinstance(peer_handshake, list)
    ctx, _ = client_pipe.handshake(peer_handshake)
    ctx[-1] = crypto_layer.client_handshake(
        private_key.private_bytes_raw(), peer_handshake[-1]
    )

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


async def benchmark_exchange(connection):
    rnd_str = "".join(random.choices(string.ascii_uppercase + string.digits, k=3))
    row_id = random.randint(1, BENCH_ID_RANGE)
    received = await rpc(connection, ["rpc", "exchange_data", rnd_str, row_id])
    return received[0]


async def benchmark_range50_update2(connection):
    # 连续的 number 索引中保证至少能读到 50 行。
    left = random.randint(1, BENCH_ID_RANGE - 49)
    right = random.randint(left + 49, BENCH_ID_RANGE)
    new1 = "".join(random.choices(string.ascii_uppercase + string.digits, k=3))
    new2 = "".join(random.choices(string.ascii_uppercase + string.digits, k=3))
    received = await rpc(
        connection, ["rpc", "range50_update2", left, right, new1, new2]
    )
    return received[0]


# bash
"""
cd benchmark/

export REDIS_URL='redis://:@localhost:6379/0?protocol=2'
uv run hetu start --app-file=./server/app.py --db=${REDIS_URL} --namespace=bench --instance=bench --workers=76

# benchmark_get 按 id 读 IntTable，服务端启动后先填充（读空会直接报错）
uv run python seed_get_rows.py --redis ${REDIS_URL}
# range50_update2 要填充 number 索引；只对隔离压测 Redis 执行。
uv run python seed_range50_rows.py --redis ${REDIS_URL}

export HETU_URL=ws://localhost:2466/hetu/bench

# 启动 1200 个并发用户

uv run ya ya_hetu_rpc.py -n 1200 -t 0.5

# 测试ttl

uv run ya ya_hetu_rpc.py -n 1 -p 1 -t 2

Windows:
redis-server.exe
redis-cli.exe config set protected-mode no
uv run hetu start --app-file=./server/app.py --db="redis://:@172.29.0.1:6379/0" --namespace=bench --instance=bench --workers=40
uv run python seed_get_rows.py --redis "redis://:@172.29.0.1:6379/0"

"""
