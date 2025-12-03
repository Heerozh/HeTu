# 对redis进行Read-Modify-Write事务基准测试
# 验证以下假设：
# 使用版本号+lua的事务，比watch+multi的事务性能更好
#
# 事务内容(get-or-create + 修改 + update)：
# 1. get or create, by some unique index
# 2. modify some fields by previous value + random delta
# 3. update back
#
# 不初始化数据集，但预设一个数据规模
# 数据模型：User(id, acc_id, name, age, email, version)
# 索引：acc_id为唯一索引
#
# 使用一种locust User, 两个不同的task，来分别测试两种事务实现方式的性能
# 任务1: 使用watch(index)->zrange(acc_id)->watch(key)->hgetall(不存在就create)->multi->hset->exec的方式，修改随机一行数据
#       注意事务会有其他locust User客户端竞态，所以第一步就要watch index
# 任务2: 使用zrange(acc_id)->hgetall（不存在就create）->lua(检测version，如果version是0表示create则检测index是否不存在该acc_id)的方式，修改随机一行数据

import os
import random
import time
import uuid
import msgspec

import redis
from locust import User, constant, events, task, tag

# Configuration
# 可以通过环境变量配置Redis连接
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))

# Data Scale
# 预设数据规模，例如10000个用户
ACC_ID_RANGE = 10000

# ================= Lua 脚本 =================
# 核心逻辑：
# 1. 检查 Index 是否存在 acc_id。
# 2. 如果存在，检查对应的 user_key 版本号是否匹配。
# 3. 如果不存在，检查传入的 expect_version 是否为 0（Create模式）。
# 4. 如果一切匹配，执行 update/create。
LUA_SCRIPT = """
local index_key = KEYS[1]
local acc_id = ARGV[1]
local key_id = ARGV[2]  -- key ID，仅在Create时使用
local data = cmsgpack.unpack(ARGV[3])     -- 修改后的数据
local expect_ver = tonumber(ARGV[4])

-- 1. 查询 Index
local existing_keys = redis.call('zrangebyscore', index_key, acc_id, acc_id)
local is_create = false

if not existing_keys then
    -- Create 场景
    if expect_ver ~= 0 then
        return -1 -- 错误：索引不存在，但客户端以为是 Update
    end
    is_create = true
else
    key_id = existing_keys[1]  -- 使用查询出来的 key_id, 因为可能被其他事务篡改
    -- Update 场景
    if expect_ver == 0 then
        return -2 -- 错误：索引已存在，但客户端以为是 Create
    end
end

local user_key = "user:" .. key_id

-- 2. 乐观锁版本检查 (如果不是新建)
if not is_create then
    local current_ver = redis.call('HGET', user_key, "version")
    if not current_ver then current_ver = 0 end

    if tonumber(current_ver) ~= expect_ver then
        return 0 -- 冲突：版本号不匹配
    end
end

-- 3. 执行写入
-- 解析 JSON (简单起见，这里直接存 JSON 字符串到 data 字段，实际可用 HMSET 展开)
local flat_data = {}
for k, v in pairs(data) do
    table.insert(flat_data, k)
    table.insert(flat_data, tostring(v))
end
redis.call('HMSET', user_key, unpack(flat_data))


if is_create then
    redis.call('ZSET', index_key, acc_id, key_id)
end

return 1 -- 成功
"""


class RedisClient:
    _client = None

    @classmethod
    def get_client(cls):
        if cls._client is None:
            cls._client = redis.Redis(
                host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True
            )
        return cls._client


class BaseRedisUser(User):
    wait_time = constant(0)  # 尽可能快地发送请求
    abstract = True

    def on_start(self):
        self.client = RedisClient.get_client()
        # 注册Lua脚本
        # self.update_script = self.client.register_script(LUA_SCRIPT)
        self.sha = self.client.script_load(LUA_SCRIPT)

    @classmethod
    def generate_user_data(cls, acc_id=None, version=1):
        if acc_id is None:
            acc_id = random.randint(1, ACC_ID_RANGE)
        return {
            "acc_id": str(acc_id),
            "name": f"user_{acc_id}",
            "age": str(random.randint(18, 80)),
            "email": f"user_{acc_id}@example.com",
            "version": str(version),
        }


class RedisBenchUser(BaseRedisUser):
    """
    任务1: WATCH + MULTI 实现
    流程: WATCH Index -> Check Index -> (WATCH User -> Get User) -> MULTI -> EXEC
    """

    @task
    @tag("watch")
    def task_watch_multi(self):
        acc_id = random.randint(1, ACC_ID_RANGE)
        start_time = time.time()
        try:
            # 乐观锁重试循环
            max_retries = 20
            for _ in range(max_retries):
                try:
                    with self.client.pipeline() as pipe:
                        # 1. ZRANGE (Lookup ID by acc_id)
                        pipe.watch("users:index:acc_id")
                        key_ids = pipe.zrangebyscore(
                            "users:index:acc_id", acc_id, acc_id
                        )
                        if not key_ids:
                            key_id = str(uuid.uuid4())
                        else:
                            key_id = key_ids[0]

                        # 2. hgetall
                        user_key = f"user:{key_id}"
                        pipe.watch(user_key)
                        if not key_ids:
                            new_ver = 1
                        else:
                            current_data = pipe.hgetall(user_key)
                            current_ver = int(current_data.get("version", 0))
                            new_ver = current_ver + 1
                        new_data = self.generate_user_data(
                            acc_id=acc_id, version=new_ver
                        )
                        new_data["id"] = key_id

                        # 4. update
                        pipe.multi()
                        pipe.hset(user_key, mapping=new_data)
                        if not key_ids:
                            pipe.zadd("users:index:acc_id", {key_id: acc_id})
                        pipe.execute()
                        break  # 成功则跳出循环
                except redis.WatchError:
                    continue  # 冲突，重试

            events.request.fire(
                request_type="WATCH",
                name="Tx_Success",
                response_time=(time.time() - start_time) * 1000,
                response_length=0,
            )
        except Exception as e:
            events.request.fire(
                request_type="WATCH",
                name="Tx_Error",
                response_time=(time.time() - start_time) * 1000,
                exception=e,
            )

    @task
    @tag("lua")
    def task_lua_version(self):
        """
        任务2: Version + Lua 实现
        流程: Get Index -> Get User (No Lock) -> Python Calc -> Lua (Check Ver + Write)
        """
        acc_id = random.randint(1, ACC_ID_RANGE)
        start_time = time.time()
        try:
            # 乐观锁重试循环
            max_retries = 20
            for _ in range(max_retries):
                # 1. 读阶段 (无锁)
                key_ids = self.client.zrangebyscore(
                    "users:index:acc_id", acc_id, acc_id
                )

                if not key_ids:
                    key_id = str(uuid.uuid4())
                else:
                    key_id = key_ids[0]

                user_key = f"user:{key_id}"

                # 2. 计算期望版本号
                if not key_ids:
                    old_ver = 0
                else:
                    current_data = self.client.hgetall(user_key)
                    old_ver = int(current_data.get("version", 0))
                new_ver = old_ver + 1
                new_data = self.generate_user_data(acc_id=acc_id, version=new_ver)
                new_data["id"] = key_id

                # 3. Lua 提交 (CAS)
                try:
                    # keys: [index_key, user_key] (实际上user_key在lua里拼可能更好，但为了传参方便这里不拼，只传index)
                    # 修正：为了符合 Cluster hash tag 规范，通常 index 和 user 很难在同一 slot。
                    # 这里假设是单机 Redis。
                    res = self.client.evalsha(
                        self.sha,
                        1,  # numkeys
                        "users:index:acc_id",
                        acc_id,
                        key_id,
                        msgspec.msgpack.encode(new_data),
                        old_ver,
                    )

                    if res == 1:
                        events.request.fire(
                            request_type="LUA",
                            name="Tx_Success",
                            response_time=(time.time() - start_time) * 1000,
                            response_length=0,
                        )
                        break
                    elif res == 0:
                        # Version 冲突 (被人改了数据)
                        continue
                    elif res == -1 or res == -2:
                        # Index 状态变化 (本来以为是Create结果被人Create了，或者反之)
                        continue
                    else:
                        events.request.fire(
                            request_type="LUA",
                            name="Tx_Unknown_Error",
                            response_time=(time.time() - start_time) * 1000,
                            exception=Exception(f"Lua ret {res}"),
                        )
                        break

                except Exception as e:
                    events.request.fire(
                        request_type="LUA",
                        name="Tx_Sys_Error",
                        response_time=(time.time() - start_time) * 1000,
                        exception=e,
                    )
        except Exception as e:
            events.request.fire(
                request_type="LUA",
                name="Tx_Sys_Error",
                response_time=(time.time() - start_time) * 1000,
                exception=e,
            )


"""
# 启动 50 个并发用户，只运行打标为 watch 的任务
cd benchmark/hypothesis/
locust -f locust_redis_upsert.py --tags watch --users 50 --spawn-rate 10 --headless -t 1m

# 启动 50 个并发用户，只运行打标为 lua 的任务
locust -f locust_redis_upsert --tags lua --users 50 --spawn-rate 10 --headless -t 1m
"""

