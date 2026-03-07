# 假设： Score 查询更快

import os

import redis
import struct

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)


# LUA_SCRIPT = """
# -- Simple PING script
# return redis.call('PING')
# """


async def redis_client():
    client = redis.asyncio.Redis(
        host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, password=REDIS_PASSWORD
    )
    yield client
    await client.close()


# async def lua_sha(redis_client):
#     sha = await redis_client.script_load(LUA_SCRIPT)
#     return sha


async def benchmark_ping(redis_client):
    await redis_client.ping()


# async def benchmark_lua_ping(redis_client, lua_sha):
#     await redis_client.evalsha(lua_sha, 0)

key_score = "test:zset:score"
key_lex = "test:zset:lex"


async def benchmark_score(redis_client):
    # 验证 Score 模式
    pipe = redis_client.pipeline()
    for i in range(100):
        pipe.zrange(key_score, 5000, 5010, withscores=True, byscore=True)
    resp = await pipe.execute()
    return str(resp[-1][-1])


async def benchmark_lex(redis_client):
    # 验证 Lex 模式
    # 注意：打印出来的是 bytes
    pipe = redis_client.pipeline()
    for i in range(100):
        pipe.zrange(
            key_lex,
            b"[" + struct.pack(">I", 5000),
            b"[" + struct.pack(">I", 5010) + b";",
            withscores=False,
            bylex=True,
        )
    resp = await pipe.execute()
    return str(resp[-1][-1])


# |                 | CPS       |
# |:----------------|:----------|
# | benchmark_lex   | 4,436.10  |
# | benchmark_ping  | 42,298.89 |
# | benchmark_score | 4,088.08  |
# 没啥区别
