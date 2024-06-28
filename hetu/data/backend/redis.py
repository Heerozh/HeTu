"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""
import random
import hashlib
import redis
import itertools
import asyncio
from datetime import datetime, timedelta
from ..component import BaseComponent, Property
from .base import ComponentTransaction, ComponentBackend, BackendClientPool, RaceCondition
import logging
logger = logging.getLogger('HeTu')


class RedisBackendClientPool(BackendClientPool):
    """储存到Redis后端的客户端连接，服务器启动时由server.py根据Config初始化，并传入RedisComponentBackend。"""
    def __init__(self, config: dict):
        super().__init__(config)
        # 同步io连接, 异步io连接, 只读io连接
        self.io = redis.from_url(config['master'], decode_responses=True)
        self.aio = redis.asyncio.from_url(config['master'], decode_responses=True)
        # 连接只读数据库
        servants = config.get('servants', [])
        self.replicas = [redis.asyncio.from_url(url, decode_responses=True) for url in servants]
        if not servants:
            servants.append(config['master'])
            self.replicas.append(self.aio)

        # 配置keyspace通知
        for url in servants:
            redis.from_url(url).config_set('notify-keyspace-events', 'Kghz')

    async def close(self):
        self.io.close()
        await self.aio.aclose()
        for replica in self.replicas:
            await replica.aclose()

    def rnd_replica(self):
        """每个websocket连接获得一个随机的replica连接，用于读取订阅"""
        i = random.randint(0, len(self.replicas))
        return i, self.replicas[i]


class RedisComponentBackend(ComponentBackend):
    """
    在redis种初始化/管理Component数据表，提供事务指令。

    参考：
    redis-py吞吐量基准：
    sync调用：单进程：1200/s，10进程理论上12 Kops，符合hiredis基准测试
    async调用：单进程+Semaphore限制100协程：6000/s， 参考QPS 100,000

    使用以下keys：
    instance_name:component_name.{CLU0}:id:1~n
    instance_name:component_name.{CLU0}:index:key~
    instance_name:component_name:meta
    """

    def __init__(self, component_cls: type[BaseComponent], instance_name, cluster_id,
                 conn_pool: RedisBackendClientPool):
        super().__init__(component_cls, instance_name, cluster_id, conn_pool)
        self._conn_pool = conn_pool  # 为了让代码提示知道类型是RedisBackendClientPool
        component_cls.hosted_ = self
        # redis key名
        hash_tag = f'{{CLU{cluster_id}}}'
        self._name = component_cls.component_name_
        self._root_prefix = f'{instance_name}:{self._name}:'
        self._key_prefix = f'{self._root_prefix}{hash_tag}:id:'
        self._idx_prefix = f'{self._root_prefix}{hash_tag}:index:'
        self._lock_key = f'{self._root_prefix}:init_lock'
        self._meta_key = f'{self._root_prefix}:meta'
        self._trans_pipe = None
        self._autoinc = None
        # 检测meta信息，然后做对应处理
        self.check_meta()

    def check_meta(self):
        """
        检查meta信息，然后做对应处理
        meta格式:
        json: 组件的结构信息
        version: json的hash
        cluster_id: 所属簇id
        last_index_rebuild: 上次重建索引时间
        """
        io = self._conn_pool.io
        lock = io.lock(self._lock_key, timeout=60*5)
        logger.info(f"⌚ [💾Redis][{self._name}组件] 准备锁定检查meta信息...")
        lock.acquire(blocking=True)

        # 获取redis已存的组件信息
        meta = io.hgetall(self._meta_key)
        if not meta:
            meta = self._create_emtpy()
        else:
            version = hashlib.md5(self._component_cls.json_.encode("utf-8")).hexdigest()
            # 如果cluster_id改变，则迁移改key名
            if int(meta['cluster_id']) != self._cluster_id:
                self._migration_cluster_id(old=int(meta['cluster_id']))

            # 如果版本不一致，组件结构可能有变化，也可能只是改权限，总之调用迁移代码
            if meta['version'] != version:
                self._migration_schema(old=meta['json'])
                # 因为迁移了，强制rebuild_index
                meta['last_index_rebuild'] = '2024-06-19T03:41:18.682529+08:00'

        # 重建数据，每次启动间隔超过1小时就重建，主要是为了防止多个node同时启动执行了多次
        last_index_rebuild = datetime.fromisoformat(meta.get('last_index_rebuild'))
        now = datetime.now().astimezone()
        if last_index_rebuild <= now - timedelta(hours=1):
            # 如果非持久化组件，则每次启动清空
            if not self._component_cls.persist_:
                logger.info(f"⌚ [💾Redis][{self._name}组件] 本组件无需持久化，清空已存数据中...")
                del_keys = io.keys(self._root_prefix + '*')
                map(io.delete, del_keys)
                logger.info(f"✅ [💾Redis][{self._name}组件] 已删除{len(del_keys)}个键值")

            # 重建索引，如果已处理过了就不处理
            self._rebuild_index()
            # 写入meta信息
            io.hset(self._meta_key, 'last_index_rebuild', now.isoformat())

        lock.release()
        logger.info(f"✅ [💾Redis][{self._name}组件] 检查完成，解锁组件")

    def _create_emtpy(self):
        logger.info(f"⌚ [💾Redis][{self._name}组件] 组件无meta信息，数据不存在，正在创建空表...")

        # 只需要写入meta，其他的_rebuild_index会创建
        meta = {
            'json': self._component_cls.json_,
            'version': hashlib.md5(self._component_cls.json_.encode("utf-8")).hexdigest(),
            'cluster_id': self._cluster_id,
            'last_index_rebuild': '2024-06-19T03:41:18.682529+08:00'
        }
        self._conn_pool.io.hset(self._meta_key, mapping=meta)
        logger.info(f"✅ [💾Redis][{self._name}组件] 空表创建完成")
        return meta

    def _rebuild_index(self):
        logger.info(f"⌚ [💾Redis][{self._name}组件] 正在重建索引...")
        io = self._conn_pool.io
        rows = io.keys(self._key_prefix + '*')
        if len(rows) == 0:
            logger.info(f"✅ [💾Redis][{self._name}组件] 无数据，无需重建索引。")
            return

        for idx_name, str_type in self._component_cls.indexes_.items():
            idx_key = self._idx_prefix + idx_name
            # 先删除所有_idx_key开头的索引
            io.delete(idx_key)
            # 重建所有索引，不管unique还是index都是sset
            pipe = io.pipeline()
            row_ids = []
            for row in rows:
                row_id = row.split(':')[-1]
                row_ids.append(row_id)
                pipe.hget(row, idx_name)
            values = pipe.execute()
            # 把values按dtype转换下
            struct = self._component_cls.new_row()
            for i, v in enumerate(values):
                struct[idx_name] = v
                values[i] = struct[idx_name].item()
            # 建立redis索引
            if str_type:
                # 字符串类型要特殊处理，score=0, member='name:1'形式
                io.zadd(idx_key, {f'{value}:{rid}': 0 for rid, value in zip(row_ids, values)})
            else:
                # zadd 会替换掉member相同的值，等于是set
                io.zadd(idx_key, dict(zip(row_ids, values)))
        logger.info(f"✅ [💾Redis][{self._name}组件] 索引重建完成, "
                    f"{len(rows)}行 * {len(self._component_cls.indexes_)}个索引。")

    def _migration_cluster_id(self, old):
        logger.warning(f"⚠️ [💾Redis][{self._name}组件] "
                       f"cluster_id 由 {old} 变更为 {self._cluster_id}，"
                       f"将尝试迁移cluster数据...")
        # 重命名key
        old_hash_tag = f'{{CLU{old}}}'
        new_hash_tag = f'{{CLU{self._cluster_id}}}'
        old_prefix = f'{self._root_prefix}{old_hash_tag}:'
        old_prefix_len = len(old_prefix)
        new_prefix = f'{self._root_prefix}{new_hash_tag}:'

        io = self._conn_pool.io
        old_keys = io.keys(old_prefix + '*')
        for old_key in old_keys:
            new_key = new_prefix + old_key[old_prefix_len:]
            io.rename(old_key, new_key)
        # 更新meta
        io.hset(self._meta_key, 'cluster_id', self._cluster_id)
        logger.warning(f"✅ [💾Redis][{self._name}组件] cluster 迁移完成，共迁移{len(old_keys)}个键值。")

    def _migration_schema(self, old):
        """如果数据库中的属性和定义不一致，尝试进行简单迁移，可以处理属性更名以外的情况。"""
        # 加载老的组件
        old_comp_cls = BaseComponent.load_json(old)

        # 只有properties名字和类型变更才迁移
        dtypes_in_db = old_comp_cls.dtypes
        new_dtypes = self._component_cls.dtypes
        if dtypes_in_db == new_dtypes:
            return

        logger.warning(f"⚠️ [💾Redis][{self._name}组件] 代码定义的Schema与已存的不一致，"
                       f"数据库中：\n"
                       f"{dtypes_in_db}\n"
                       f"代码定义的：\n"
                       f"{new_dtypes}\n "
                       f"将尝试数据迁移（只处理新属性，不处理类型变更，改名等等情况）：")

        # todo 调用自定义版本迁移代码（define_migration）

        # 检查是否有属性被删除
        for prop_name in dtypes_in_db.fields:
            if prop_name not in new_dtypes.fields:
                logger.warning(f"⚠️ [💾Redis][{self._name}组件] "
                               f"数据库中的属性 {prop_name} 在新的组件定义中不存在，如果改名了需要手动迁移，"
                               f"默认丢弃该属性数据。")

        # 多出来的列再次报警告，然后忽略
        io = self._conn_pool.io
        rows = io.keys(self._key_prefix + '*')
        props = dict(self._component_cls.properties_)  # type: dict[str, Property]
        added = 0
        for prop_name in new_dtypes.fields:
            if prop_name not in dtypes_in_db.fields:
                logger.warning(f"⚠️ [💾Redis][{self._name}组件] "
                               f"新的代码定义中多出属性 {prop_name}，将使用默认值填充。")
                default = props[prop_name].default
                if default is None:
                    logger.error(f"⚠️ [💾Redis][{self._name}组件] "
                                 f"迁移时尝试新增 {prop_name} 属性失败，该属性没有默认值，无法新增。")
                    raise ValueError("迁移失败")
                pipe = io.pipeline()
                for row in rows:
                    pipe.hset(row, prop_name, default)
                pipe.execute()
                added += 1
        logger.warning(f"✅ [💾Redis][{self._name}组件] 新属性增加完成，共处理{len(rows)}行 * "
                       f"{added}个属性。")

    def transaction(self):
        return RedisComponentTransaction(self._component_cls, self._conn_pool,
                                         self._key_prefix, self._idx_prefix)


class RedisComponentTransaction(ComponentTransaction):

    # key: 1:结果保存到哪个key, 2-n:要检查的keys， args： 要检查的keys的value，按顺序
    LUA_CHECK_UNIQUE_SCRIPT = """
    local result_key = KEYS[1]

    for i = 2, #KEYS, 1 do
        local start = ARGV[i-1]
        local stop = start
        local by = 'BYSCORE'
        if start:sub(1, 1) == '[' then
            start = start .. ':'
            stop = start .. ';'
            by = 'BYLEX'
        end
        local rows = redis.call('zrange', KEYS[i], start, stop, by, 'LIMIT', 0, 1)
        if #rows > 0 then
            redis.call('set', result_key, 0, 'PX', 100)
            return 'FAIL'
        end
    end
    redis.call('set', result_key, 1, 'PX', 100)
    return 'OK'
    """
    # key: 1:是否执行的标记key, 2-n:不使用，仅供客户端判断hash slot用, args: stacked的命令
    LUA_IF_RUN_STACK_SCRIPT = """
    local result_key = KEYS[1]
    local unique_check_ok = redis.call('get',  result_key)
    if tonumber(unique_check_ok) <= 0 then
        return 'FAIL'
    end

    local cur = 1
    local last_row_id = nil
    while cur <= #ARGV do
        local len = tonumber(ARGV[cur])
        local cmds = {unpack(ARGV, cur+1, cur+len)}
        cur = cur + len + 1
        if cmds[1] == 'AUTO_INCR' then
            local idx_key = cmds[2]
            local ids = redis.call('zrange', idx_key, 0, 0, 'REV', 'WITHSCORES')
            if #ids == 0 then 
                last_row_id = 1
            else
                last_row_id = tonumber(ids[2]) + 1
            end
        elseif cmds[1] == 'END_INCR' then
            last_row_id = nil
        else
            if last_row_id ~= nil then
                local _
                for i = 2, #cmds, 1 do
                    cmds[i], _ = string.gsub(cmds[i], '{rowid}', last_row_id)
                end
            end
            -- redis.log(2, table.concat(cmds, ','))
            redis.call(unpack(cmds))
        end
    end
    return 'OK'
    """
    lua_check_unique = None
    lua_run_stack = None

    def __init__(self, component_cls: type[BaseComponent], conn_pool: RedisBackendClientPool,
                 key_prefix: str, index_prefix: str):
        super().__init__(component_cls, conn_pool)
        self._conn_pool = conn_pool  # 为了让代码提示知道类型是RedisBackendClientPool

        self._key_prefix = key_prefix
        self._idx_prefix = index_prefix
        self._trans_pipe = self._conn_pool.aio.pipeline(transaction=True)
        # 强制pipeline进入立即模式，不然当我们需要读取未锁定的index时，会不返回结果
        self._trans_pipe.watching = True

        cls = self.__class__
        if cls.lua_check_unique is None:
            cls.lua_check_unique = self._conn_pool.aio.register_script(cls.LUA_CHECK_UNIQUE_SCRIPT)
        if cls.lua_run_stack is None:
            cls.lua_run_stack = self._conn_pool.aio.register_script(cls.LUA_IF_RUN_STACK_SCRIPT)

    async def end_transaction(self, discard):
        # 并实现事务提交的操作，将_updates中的命令写入事务
        if discard:
            await self._trans_pipe.reset()
            self._trans_pipe = None
            return True

        pipe = self._trans_pipe

        # 准备对unique index进行最终检查，之前虽然检查过，但没有lock index，值可能变更
        # 这里用lua在事务中检查，可以减少watch的key数量
        lua_unique_keys = [self._key_prefix + 'unique_check_is_ok', ]
        lua_unique_argv = []
        for idx in self._component_cls.uniques_:
            if idx == 'id':  # insert不需要检查id, update也不需要因为基类里会确认id一样
                continue
            for cmd, _, old_row, new_row in self._updates:
                if (cmd == 'update' and old_row[idx] != new_row[idx]) or cmd == 'insert':
                    lua_unique_keys.append(self._idx_prefix + idx)
                    str_type = self._component_cls.indexes_[idx]
                    if str_type:
                        lua_unique_argv.append('[' + new_row[idx].item())
                    else:
                        lua_unique_argv.append(new_row[idx].item())

        # 生成事务stack，让lua来判断unique检查通过的情况下，才执行。减少冲突概率。
        lua_run_keys = [self._key_prefix + 'unique_check_is_ok', ]
        lua_run_argv = []

        def stack_cmd(*args):
            if len(args) > 1:
                lua_run_keys.append(args[1])
            lua_run_argv.extend([len(args), ] + list(args))

        for cmd, row_id, old_row, new_row in self._updates:
            if cmd == 'delete':
                row_key = self._key_prefix + str(row_id)
                stack_cmd('del', row_key)
                for idx_name, str_type in self._component_cls.indexes_.items():
                    idx_key = self._idx_prefix + idx_name
                    if str_type:
                        stack_cmd('zrem', idx_key, f'{old_row[idx_name]}:{row_id}')
                    else:
                        stack_cmd('zrem', idx_key, row_id)
            else:
                # 插入/更新数据
                if row_id is None:
                    stack_cmd('AUTO_INCR', self._idx_prefix + 'id')
                    row_id = '{rowid}'
                row_key = self._key_prefix + str(row_id)
                kvs = itertools.chain.from_iterable(zip(new_row.dtype.names, new_row.tolist()))
                stack_cmd('hset', row_key, *kvs)
                for idx_name, str_type in self._component_cls.indexes_.items():
                    idx_key = self._idx_prefix + idx_name
                    if str_type:
                        if cmd == 'update':   # 删除老数据
                            stack_cmd('zrem', idx_key, f'{old_row[idx_name]}:{row_id}')
                        stack_cmd('zadd', idx_key, 0, f'{new_row[idx_name]}:{row_id}')
                    elif idx_name == 'id':
                        stack_cmd('zadd', idx_key, row_id, row_id)
                    else:
                        stack_cmd('zadd', idx_key, new_row[idx_name].item(), row_id)
                if row_id == '{rowid}':
                    stack_cmd('hset', row_key, 'id', row_id)
                    stack_cmd('END_INCR')

        pipe.multi()
        await self.lua_check_unique(args=lua_unique_argv, keys=lua_unique_keys, client=pipe)
        await self.lua_run_stack(args=lua_run_argv, keys=lua_run_keys, client=pipe)

        try:
            result = await pipe.execute()
            if result[-1] == 'FAIL':
                raise RaceCondition(f"unique index在事务中变动，被其他事务添加了相同值")
        except redis.WatchError:
            raise RaceCondition(f"watched key被其他事务修改")
        else:
            return True
        finally:
            # 无论是else里的return还是except里的raise，finally都会在他们之前执行
            await pipe.reset()
            self._trans_pipe = None

    async def _backend_get(self, row_id: int):
        # 获取行数据的操作
        key = self._key_prefix + str(row_id)
        pipe = self._trans_pipe

        # 同时要让乐观锁锁定该行
        await pipe.watch(key)
        # 返回值要通过dict_to_row包裹下
        row = await pipe.hgetall(key)
        if row:
            return self._component_cls.dict_to_row(row)
        else:
            return None

    async def _backend_query(self, index_name: str, left, right=None, limit=10, desc=False):
        # 范围查询的操作，返回List[int] of row_id。如果你的数据库同时返回了数据，可以存到_cache中
        idx_key = self._idx_prefix + index_name
        pipe = self._trans_pipe

        if right is None:
            right = left
        if desc:
            left, right = right, left

        # 对于str类型查询，要用[开始
        str_type = self._component_cls.indexes_[index_name]
        by_lex = False
        if str_type:
            assert type(left) is str and type(right) is str, \
                f"字符串类型索引`{index_name}`的查询(left={left}, {type(left)})变量类型必须是str"
            if not left.startswith(('(', '[')):
                left = f'[{left}'
            if not right.startswith(('(', '[')):
                right = f'[{right}'

            if left == right:  # 如果是精确查询
                left = f'{left}:'  # name:id 形式，所以:作为结尾标识符
                right = f'{right};'  # ';' = 3B, ':' = 3A

            by_lex = True

        row_ids = await pipe.zrange(idx_key, left, right, desc=desc, offset=0, num=limit,
                                    byscore=not by_lex, bylex=by_lex)

        if str_type:
            row_ids = [vk.split(':')[-1] for vk in row_ids]

        # 未查询到数据时返回[]
        return list(map(int, row_ids))
