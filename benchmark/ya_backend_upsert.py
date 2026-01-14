# 测试Hetu backend的upsert性能
# 目前只有redis


import os
import random
import string

import numpy as np

import hetu
from hetu.common.snowflake_id import SnowflakeID
from hetu.data.backend import Backend, RaceCondition
from hetu.data.backend.idmap import IdentityMap
from hetu.data.backend.table import TableReference

# Configuration
# 可以通过环境变量配置Redis连接
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)

# Data Scale
# 预设数据规模，例如10000个用户
ACC_ID_RANGE = 30000

# === 组件定义 ===


@hetu.define_component(
    namespace="bench", volatile=True, permission=hetu.Permission.EVERYBODY
)
class IntTable(hetu.BaseComponent):
    number: np.int32 = hetu.property_field(0, unique=True)
    name: "<U16" = hetu.property_field("Unnamed")


# 需要定义System以确保Component被注册，不然Component schema不会加入到lua脚本中
@hetu.define_system(namespace="bench", components=(IntTable,))
async def ref_components(ctx):
    pass


# 初始化instance & clusters
hetu.system.SystemClusters().build_clusters("bench")
item_ref = TableReference(IntTable, "bench", 1)

# === 夹具 ===


async def redis_backend():
    if REDIS_PASSWORD:
        url = f"redis://:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"
    else:
        url = f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"
    config = {
        "type": "redis",
        "master": url,
    }

    SnowflakeID().init(hash(os.getpid()) % 1024, 0)

    _backend = Backend(config)
    _backend.post_configure()
    yield _backend
    await _backend.close()


# === 基准测试 ===


async def benchmark_redis_upsert(redis_backend: Backend):
    client = redis_backend.master
    count = 0
    while True:
        id_map = IdentityMap()

        rand_number = random.randint(0, ACC_ID_RANGE)
        rows = await client.range(item_ref, "number", rand_number, limit=1)
        if rows.size > 0:
            row = rows[0]
            id_map.add_clean(item_ref, row)
            row.name = "".join(
                random.choices(string.ascii_uppercase + string.digits, k=3)
            )
            id_map.update(item_ref, row)
        else:
            row = IntTable.new_row()
            row.number = rand_number
            row.name = "".join(
                random.choices(string.ascii_uppercase + string.digits, k=3)
            )
            id_map.add_insert(item_ref, row)
        try:
            count += 1
            await client.commit(id_map)
            return count
        except RaceCondition:
            # 如果发生了竞争条件，重新尝试
            continue


"""

export REDIS_HOST=...
export REDIS_PASSWORD=...

# 启动 200 个并发用户
cd benchmark/
ya ya_backend_upsert.py -n 200 -t 1


"""
