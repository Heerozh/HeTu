# 测试河图的订阅性能的更新器

import os
import random
import string
import redis
import redis.asyncio


# Configuration
# 可以通过环境变量配置Redis连接

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)

if REDIS_PASSWORD:
    url = f"redis://:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"
else:
    url = f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"
instance_name = "bench"

io = redis.from_url(url)

pattern: str = f"{instance_name}:IntTable:{{CLU0}}:id:*"
_keys = io.keys(pattern)
keys: list[str] = [key.decode() for key in _keys]  # type: ignore
if len(keys) == 0:
    raise Exception("没有数据，请先运行call bench生成数据")

# === 工具函数 ===


# === 夹具 ===


async def redis_client():
    aio = redis.asyncio.from_url(url)

    yield aio

    await aio.close()


# === 基准测试 ===


async def benchmark_pubsub_update(redis_client):
    rnd_str = "".join(random.choices(string.ascii_uppercase + string.digits, k=3))
    key = random.choice(keys)

    await redis_client.hset(key, key="name", value=rnd_str)
