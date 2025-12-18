# 测试Hetu backend的upsert性能
# 目前只有redis


import os
import hetu
from hetu.data.backend import Backend
from hetu.data.backend.idmap import IdentityMap
from hetu.data.backend.table import TableReference
import numpy as np
import random
import string
from hetu.common.snowflake_id import SnowflakeID
from hetu.data.backend import RaceCondition
import uuid


# Configuration
# 可以通过环境变量配置Redis连接
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)

# Data Scale
# 预设数据规模，例如10000个用户
ACC_ID_RANGE = 30000


@hetu.data.define_component(
    namespace="bench", persist=False, permission=hetu.data.Permission.EVERYBODY
)
class IntTable(hetu.data.BaseComponent):
    number: np.int32 = hetu.data.property_field(0, unique=True)
    name: "<U16" = hetu.data.property_field("Unnamed")


# 需要定义System以确保Component被注册，不然Component schema不会加入到lua脚本中
@hetu.system.define_system(namespace="bench", components=(IntTable,))
async def ref_components(ctx):
    pass


# 初始化instance & clusters
hetu.system.SystemClusters().build_clusters("bench")
item_ref = TableReference(IntTable, "bench", 1)


async def redis_backend():
    if REDIS_PASSWORD:
        url = f"redis://:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"
    else:
        url = f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"
    config = {
        "type": "redis",
        "master": url,
    }

    SnowflakeID().init(hash(uuid.getnode()) % 1024, 0)

    _backend = Backend(config)
    _backend.configure()
    yield _backend
    await _backend.close()


async def benchmark_redis_upsert(redis_backend):
    client = redis_backend.master
    count = 0
    while True:
        id_map = IdentityMap()

        rand_number = random.randint(0, ACC_ID_RANGE)
        row = await client.range(item_ref, "number", rand_number, limit=1)
        if row.size > 0:
            row = row[0]
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
