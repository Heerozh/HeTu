"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""
import asyncio
import hashlib
import itertools
import logging
import random
import time
import uuid
import warnings

import numpy as np
import redis

from .base import ComponentTransaction, ComponentTable, Backend, BackendTransaction, MQClient
from .base import RaceCondition, HeadLockFailed
from ..component import BaseComponent, Property
from ...common.helper import batched
from ...common.multimap import MultiMap

logger = logging.getLogger('HeTu')
MAX_SUBSCRIBED = 1000


class RedisBackend(Backend):
    """储存到Redis后端的连接，服务器启动时由server.py根据Config初始化，并传入RedisComponentBackend。"""

    def __init__(self, config: dict):
        super().__init__(config)
        # 同步io连接, 异步io连接, 只读io连接
        self.master_url = config['master']
        self.servant_urls = config.get('servants', [])
        self.io = redis.from_url(self.master_url, decode_responses=True)
        self._aio = redis.asyncio.from_url(self.master_url, decode_responses=True)
        self.dbi = self.io.connection_pool.connection_kwargs['db']
        # 连接只读数据库
        self.replicas = [redis.asyncio.from_url(url, decode_responses=True)
                         for url in self.servant_urls]
        if not self.servant_urls:
            self.servant_urls.append(self.master_url)
            self.replicas.append(self._aio)
        # 限制aio运行的coroutine
        try:
            self.loop_id = hash(asyncio.get_running_loop())
        except RuntimeError:
            self.loop_id = None

    def configure(self):
        # 检测redis版本
        version = self.io.info('server')['redis_version']
        assert tuple(map(int, version.split("."))) >= (7, 0), "Redis版本过低，至少需要7.0版本"
        for url in self.servant_urls:
            version = redis.from_url(url).info('server')['redis_version']
            assert tuple(map(int, version.split("."))) >= (7, 0), "Redis版本过低，至少需要7.0版本"
        # 检查servants设置
        target_keyspace = 'Kghz'
        for servant_url in self.servant_urls:
            r = redis.from_url(servant_url)
            try:
                # 设置keyspace通知
                db_keyspace = r.config_get('notify-keyspace-events')['notify-keyspace-events']
                db_keyspace = db_keyspace.replace('A', 'g$lshztxed')
                db_keyspace_new = db_keyspace
                for flag in list(target_keyspace):
                    if flag not in db_keyspace:
                        db_keyspace_new += flag
                if db_keyspace_new != db_keyspace:
                    r.config_set('notify-keyspace-events', db_keyspace_new)
            except (redis.exceptions.NoPermissionError, redis.exceptions.ResponseError):
                logger.warning(
                    f"⚠️ [💾Redis] 无权限调用数据库{servant_url}的config_set命令，数据订阅将"
                    f"不起效。可手动设置配置文件：notify-keyspace-events={target_keyspace}")
            # 检查是否是replica模式
            if servant_url != self.master_url:
                db_replica = r.config_get('replica-read-only')
                if db_replica.get('replica-read-only') != 'yes':
                    logger.warning("⚠️ [💾Redis] servant必须是Read Only Replica模式。"
                                   f"{servant_url} 未设置replica-read-only=yes")
                # 不检查replicaof master地址，因为replicaof的可能是其他replica地址
            # 考虑可以检查pubsub client buff设置，看看能否redis崩了提醒下
            # pubsub值建议为$剩余内存/预估在线数$

    @property
    def aio(self):
        if self.loop_id is None:
            self.loop_id = hash(asyncio.get_running_loop())
        # redis-py的async connection用的python的steam.connect，绑定到当前协程
        # 而aio是一个connection pool，断开的连接会放回pool中，所以aio不能跨协程传递
        assert hash(asyncio.get_running_loop()) == self.loop_id, \
            "Backend只能在同一个coroutine中使用。检测到调用此函数的协程发生了变化"

        return self._aio

    def reset_connection_pool(self):
        self.loop_id = None
        self._aio.connection_pool.reset()
        for replica in self.replicas:
            replica.connection_pool.reset()

    async def close(self):
        if self.io.get('head_lock') == str(id(self)):
            self.io.delete('head_lock')

        self.io.close()
        await self._aio.aclose()
        for replica in self.replicas:
            await replica.aclose()

    def requires_head_lock(self) -> bool:
        self.io.set('head_lock', id(self), nx=True)
        # 不在set中get，兼容一些redis变种
        locked = self.io.get('head_lock')
        if locked is None:
            locked = str(id(self))
        return locked == str(id(self))

    def random_replica(self) -> redis.Redis:
        """随机返回一个只读连接"""
        if self.loop_id is None:
            self.loop_id = hash(asyncio.get_running_loop())
        assert hash(asyncio.get_running_loop()) == self.loop_id, \
            "Backend只能在同一个coroutine中使用。检测到调用此函数的协程发生了变化"

        return random.choice(self.replicas)

    def get_mq_client(self) -> 'RedisMQClient':
        """每个websocket连接获得一个随机的replica连接，用于读取订阅"""
        return RedisMQClient(self.random_replica())

    def transaction(self, cluster_id: int) -> 'RedisTransaction':
        """进入db的事务模式，返回事务连接"""
        return RedisTransaction(self, cluster_id)


class RedisTransaction(BackendTransaction):
    """数据库事务类，负责开始事务，并提交事务"""
    # key: 1:结果保存到哪个key, 2-n:要检查的keys， args： 要检查的keys的value，按顺序
    LUA_CHECK_UNIQUE_SCRIPT = """
    local result_key = KEYS[1]
    local redis = redis

    for i = 1, #ARGV, 4 do
        local rows = redis.call('zrange', ARGV[i], ARGV[i+1], ARGV[i+2], ARGV[i+3], 'LIMIT', 0, 1)
        if #rows > 0 then
            redis.call('set', result_key, 0, 'PX', 100)
            return 'FAIL'
        end
    end
    redis.call('set', result_key, 1, 'PX', 100)
    return 'OK'
    """
    # key: 1:是否执行的标记key, 2-n:不使用，仅供客户端判断hash slot用, args: stacked的命令
    LUA_IF_RUN_STACKED_SCRIPT = """
    local result_key = KEYS[1]
    local redis = redis
    local tonumber = tonumber
    local unpack = unpack
    local gsub = string.gsub
    local insert = table.insert

    local unique_check_ok = redis.call('get',  result_key)
    if tonumber(unique_check_ok) <= 0 then
        return 'FAIL'
    end

    local cur = 1
    local last_row_id = nil
    local rtn = {}
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
            insert(rtn, last_row_id)
        elseif cmds[1] == 'END_INCR' then
            last_row_id = nil
        else
            if last_row_id ~= nil then
                local _
                for i = 2, #cmds, 1 do
                    cmds[i], _ = gsub(cmds[i], '{rowid}', last_row_id)
                end
            end
            -- redis.log(2, table.concat(cmds, ','))
            redis.call(unpack(cmds))
        end
    end
    return rtn
    """
    lua_check_unique = None
    lua_run_stacked = None

    def __init__(self, backend: RedisBackend, cluster_id: int):
        super().__init__(backend, cluster_id)

        cls = self.__class__
        if cls.lua_check_unique is None:
            cls.lua_check_unique = backend.aio.register_script(cls.LUA_CHECK_UNIQUE_SCRIPT)
            backend.io.script_load(cls.LUA_CHECK_UNIQUE_SCRIPT)
        if cls.lua_run_stacked is None:
            cls.lua_run_stacked = backend.aio.register_script(cls.LUA_IF_RUN_STACKED_SCRIPT)
            backend.io.script_load(cls.LUA_IF_RUN_STACKED_SCRIPT)
            from redis.asyncio.client import Pipeline
            # 不要让pipeline每次执行lua脚本运行script exist命令，这个命令会占用Redis 20%CPU
            async def _pipeline_lua_speedup(_):
                return
            Pipeline.load_scripts = _pipeline_lua_speedup

        self._uuid = uuid.uuid4().hex
        self._checks = []  # 事务中的unique检查
        self._stack = []  # 事务中的更新操作
        self._request_auto_incr = False

        self._trx_pipe = backend.aio.pipeline()
        # 强制pipeline进入立即模式，不然当我们需要读取未锁定的index时，会不返回结果
        self._trx_pipe.watching = True

    @property
    def pipe(self):
        return self._trx_pipe

    def stack_unique_check(self, index_key: str, start, stop, byscore) -> None:
        """加入需要在end_transaction时进行unique检查的index和value"""
        self._checks.extend([index_key, start, stop, 'BYSCORE' if byscore else 'BYLEX'])

    def stack_cmd(self, *args):
        if args[0] == 'AUTO_INCR':
            self._request_auto_incr = True
        self._stack.extend([len(args), ] + list(args))

    async def end_transaction(self, discard) -> list[int] | None:
        if self._trx_pipe is None:
            return
        # 并实现事务提交的操作，将_stack中的命令写入事务
        if discard or len(self._stack) == 0:
            await self._trx_pipe.reset()
            self._trx_pipe = None
            return

        pipe = self._trx_pipe

        # 2种模式，如果有unique要检查，或者有auto incr要执行，就用lua脚本执行所有命令
        if len(self._checks) > 0 or self._request_auto_incr:
            # 在提交前最后检查一遍unique
            # 之前的insert和update时也有unique检查，但为了降低事务冲突并不锁定index，因此可能有变化
            # 这里在lua中检查unique，不用锁定index
            unique_check_key = f'unique_check:{{CLU{self.cluster_id}}}:' + self._uuid
            lua_unique_keys = [unique_check_key, ]
            lua_unique_argv = self._checks

            # 生成事务stack，让lua来判断unique检查通过的情况下，才执行。减少冲突概率。
            lua_run_keys = [unique_check_key, ]
            lua_run_argv = self._stack

            pipe.multi()
            await self.lua_check_unique(args=lua_unique_argv, keys=lua_unique_keys, client=pipe)
            await self.lua_run_stacked(args=lua_run_argv, keys=lua_run_keys, client=pipe)

            try:
                result = await pipe.execute()
                if result[-1] == 'FAIL':
                    raise RaceCondition(f"unique index在事务中变动，被其他事务添加了相同值")
                result = result[-1]
            except redis.WatchError:
                raise RaceCondition(f"watched key被其他事务修改")
            else:
                return result
            finally:
                # 无论是else里的return还是except里的raise，finally都会在他们之前执行
                await pipe.reset()
                self._trx_pipe = None

        else:
            # 执行事务
            pipe.multi()

            cur = 0
            while cur < len(self._stack):
                arg_len = int(self._stack[cur])
                cmds = self._stack[cur + 1:cur + arg_len + 1]
                cur = cur + arg_len + 1
                await pipe.execute_command(*cmds)

            try:
                await pipe.execute()
            except redis.WatchError:
                raise RaceCondition(f"watched key被其他事务修改")
            else:
                return []
            finally:
                await pipe.reset()
                self._trx_pipe = None


class RedisComponentTable(ComponentTable):
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

    def __init__(
            self,
            component_cls: type[BaseComponent],
            instance_name: str,
            cluster_id: int,
            backend: RedisBackend
    ):
        super().__init__(component_cls, instance_name, cluster_id, backend)
        self._backend = backend  # 为了让代码提示知道类型是RedisBackend
        component_cls.hosted_ = self
        # redis key名
        hash_tag = f'{{CLU{cluster_id}}}:'
        self._name = component_cls.component_name_
        self._root_prefix = f'{instance_name}:{self._name}:'
        self._key_prefix = f'{self._root_prefix}{hash_tag}id:'
        self._idx_prefix = f'{self._root_prefix}{hash_tag}index:'
        self._init_lock_key = f'{self._root_prefix}init_lock'
        self._meta_key = f'{self._root_prefix}meta'
        self._trx_pipe = None

    def create_or_migrate(self, cluster_only=False):
        """
        检查表结构是否正确，不正确则尝试进行迁移。此方法同时会强制重建表的索引。
        meta格式:
        json: 组件的结构信息
        version: json的hash
        cluster_id: 所属簇id
        """
        if not self._backend.requires_head_lock():
            raise HeadLockFailed("redis中head_lock键")

        io = self._backend.io
        logger.info(f"⌚ [💾Redis][{self._name}组件] 准备锁定检查meta信息...")
        if cluster_only:
            logger.info(f"⌚ [💾Redis][{self._name}组件] 对该表只检查cluster id迁移信息...")
        with io.lock(self._init_lock_key, timeout=60 * 5):
            # 获取redis已存的组件信息
            meta = io.hgetall(self._meta_key)
            if not meta:
                self._create_emtpy()
            else:
                version = hashlib.md5(self._component_cls.json_.encode("utf-8")).hexdigest()
                # 如果cluster_id改变，则迁移改key名
                if int(meta['cluster_id']) != self._cluster_id:
                    self._migration_cluster_id(old=int(meta['cluster_id']))

                # 如果版本不一致，组件结构可能有变化，也可能只是改权限，总之调用迁移代码
                if meta['version'] != version and not cluster_only:
                    self._migration_schema(old=meta['json'])

            # 重建索引数据
            if not cluster_only:
                self._rebuild_index()
            logger.info(f"✅ [💾Redis][{self._name}组件] 检查完成，解锁组件")

    def flush(self, force=False):
        if not self._backend.requires_head_lock():
            raise HeadLockFailed("redis中head_lock键")

        if force:
            warnings.warn("flush正在强制删除所有数据，此方式只建议维护代码调用。")

        # 如果非持久化组件，则允许调用flush主动清空数据
        if not self._component_cls.persist_ or force:

            io = self._backend.io
            logger.info(f"⌚ [💾Redis][{self._name}组件] 对非持久化组件flush清空数据中...")

            with io.lock(self._init_lock_key, timeout=60 * 5):
                del_keys = io.keys(self._root_prefix + '*')
                print('准备删除', len(del_keys), '个键')
                del_keys.remove(self._init_lock_key)
                for batch in batched(del_keys, 1000):
                    with io.pipeline() as pipe:
                        list(map(pipe.delete, batch))
                        pipe.execute()

            self.create_or_migrate()

            logger.info(f"✅ [💾Redis][{self._name}组件] 已删除{len(del_keys)}个键值")
        else:
            raise ValueError(f"{self._name}是持久化组件，不允许flush操作")

    def _create_emtpy(self):
        logger.info(f"⌚ [💾Redis][{self._name}组件] 组件无meta信息，数据不存在，正在创建空表...")

        # 只需要写入meta，其他的_rebuild_index会创建
        meta = {
            'json': self._component_cls.json_,
            'version': hashlib.md5(self._component_cls.json_.encode("utf-8")).hexdigest(),
            'cluster_id': self._cluster_id,
        }
        self._backend.io.hset(self._meta_key, mapping=meta)
        logger.info(f"✅ [💾Redis][{self._name}组件] 空表创建完成")
        return meta

    def _rebuild_index(self):
        logger.info(f"⌚ [💾Redis][{self._name}组件] 正在重建索引...")
        io = self._backend.io
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
        old_hash_tag = f'{{CLU{old}}}:'
        new_hash_tag = f'{{CLU{self._cluster_id}}}:'
        old_prefix = f'{self._root_prefix}{old_hash_tag}'
        old_prefix_len = len(old_prefix)
        new_prefix = f'{self._root_prefix}{new_hash_tag}'

        io = self._backend.io
        old_keys = io.keys(old_prefix + '*')
        for old_key in old_keys:
            new_key = new_prefix + old_key[old_prefix_len:]
            io.rename(old_key, new_key)
        # 更新meta
        io.hset(self._meta_key, 'cluster_id', self._cluster_id)
        logger.warning(
            f"✅ [💾Redis][{self._name}组件] cluster 迁移完成，共迁移{len(old_keys)}个键值。")

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
        io = self._backend.io
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

    @classmethod
    def make_query_cmd(
            cls, component_cls, index_name: str, left, right, limit, desc
    ) -> dict[str, str] | list[int]:
        """生成数据库查询命令，在direct_query和_db_query中复用此类。"""
        if right is None:
            right = left
            if index_name == 'id':
                return [int(left)]
        if desc:
            left, right = right, left

        # 对于str类型查询，要用[开始
        str_type = component_cls.indexes_[index_name]
        by_lex = False
        if str_type:
            assert type(left) is str and type(right) is str, \
                f"字符串类型索引`{index_name}`的查询(left={left}, {type(left)})变量类型必须是str"
            if not left.startswith(('(', '[')):
                left = f'[{left}'
            if not right.startswith(('(', '[')):
                right = f'[{right}'

            if not left.endswith((':', ';')):
                left = f'{left}:'  # name:id 形式，所以:作为结尾标识符
            if not right.endswith((':', ';')):
                right = f'{right};'  # ';' = 3B, ':' = 3A

            by_lex = True

        return {'start': left, 'end': right, 'desc': desc, 'offset': 0,
                'num': limit, 'bylex': by_lex, 'byscore': not by_lex}

    async def direct_query(
            self,
            index_name: str,
            left,
            right=None,
            limit=10,
            desc=False,
            row_format='struct',
    ) -> np.recarray | list[dict | int]:
        replica = self._backend.random_replica()
        idx_key = self._idx_prefix + index_name

        cmds = RedisComponentTable.make_query_cmd(
            self._component_cls, index_name, left, right, limit, desc)

        if type(cmds) is list:  # 如果是list说明不需要查询直接返回id
            row_ids = cmds
        else:
            row_ids = await replica.zrange(name=idx_key, **cmds)
            str_type = self._component_cls.indexes_[index_name]
            if str_type:
                row_ids = [vk.split(':')[-1] for vk in row_ids]

        if row_format == 'id':
            return list(map(int, row_ids))
        raw = row_format == 'raw'

        key_prefix = self._key_prefix
        rows = []
        for _id in row_ids:
            row = await replica.hgetall(key_prefix + str(_id))
            if raw:
                rows.append(row)
            else:
                rows.append(self._component_cls.dict_to_row(row))

        if raw:
            return rows
        else:
            if len(rows) == 0:
                return np.rec.array(np.empty(0, dtype=self._component_cls.dtypes))
            else:
                return np.rec.array(np.stack(rows, dtype=self._component_cls.dtypes))

    async def direct_get(self, row_id: int) -> None | np.record:
        replica = self._backend.random_replica()
        key = self._key_prefix + str(row_id)
        row = await replica.hgetall(key)
        if row:
            return self._component_cls.dict_to_row(row)
        else:
            return None

    async def direct_set(self, row_id: int, **kwargs):
        aio = self._backend.aio
        key = self._key_prefix + str(row_id)

        for prop in kwargs:
            if prop in self._component_cls.indexes_:
                raise ValueError(f"索引字段`{prop}`不允许用direct_set修改")
        await aio.hset(key, mapping=kwargs)

    def attach(self, backend_trx: RedisTransaction) -> 'RedisComponentTransaction':
        # 这里不用检查cluster_id，因为ComponentTransaction会检查
        # assert backend_trx.cluster_id == self._cluster_id
        return RedisComponentTransaction(
            self, backend_trx, self._key_prefix, self._idx_prefix)

    def channel_name(self, index_name: str = None, row_id: int = None):
        dbi = self._backend.dbi
        if index_name:
            return f'__keyspace@{dbi}__:{self._idx_prefix + index_name}'
        elif row_id is not None:
            return f'__keyspace@{dbi}__:{self._key_prefix + str(row_id)}'
        raise ValueError("index_name和row_id必须有一个")


class RedisComponentTransaction(ComponentTransaction):
    def __init__(
            self,
            comp_tbl: RedisComponentTable,
            trx_conn: RedisTransaction,
            key_prefix: str,
            index_prefix: str
    ):
        super().__init__(comp_tbl, trx_conn)
        self._trx_conn = trx_conn  # 为了让代码提示知道类型是RedisTransaction

        self._key_prefix = key_prefix
        self._idx_prefix = index_prefix

    async def _db_get(self, row_id: int) -> None | np.record:
        # 获取行数据的操作
        key = self._key_prefix + str(row_id)
        pipe = self._trx_conn.pipe

        # 同时要让乐观锁锁定该行
        await pipe.watch(key)
        # 返回值要通过dict_to_row包裹下
        if row := await pipe.hgetall(key):
            return self._component_cls.dict_to_row(row)
        else:
            return None

    async def _db_query(
            self,
            index_name: str,
            left,
            right=None,
            limit=10,
            desc=False,
            lock_index=True
    ) -> list[int]:
        idx_key = self._idx_prefix + index_name
        pipe = self._trx_conn.pipe

        cmds = RedisComponentTable.make_query_cmd(
            self._component_cls, index_name, left, right, limit, desc)

        if type(cmds) is list:  # 如果是list说明不需要查询直接返回id
            return cmds

        if lock_index:
            await pipe.watch(idx_key)
        row_ids = await pipe.zrange(name=idx_key, **cmds)

        str_type = self._component_cls.indexes_[index_name]
        if str_type:
            row_ids = [vk.split(':')[-1] for vk in row_ids]

        # 未查询到数据时返回[]
        return list(map(int, row_ids))

    def _trx_check_unique(self, old_row, new_row: np.record) -> None:
        trx = self._trx_conn
        component_cls = self._component_cls
        idx_prefix = self._idx_prefix

        for idx in component_cls.uniques_:
            if idx == 'id':  # insert不需要检查id, update也不需要因为基类里会确认id一样
                continue
            if old_row is None or old_row[idx] != new_row[idx]:
                key = idx_prefix + idx
                str_type = component_cls.indexes_[idx]
                qv = new_row[idx].item()
                if str_type:
                    trx.stack_unique_check(key, f'[{qv}:', f'[{qv};', False)
                else:
                    trx.stack_unique_check(key, qv, qv, True)

    def _trx_insert(self, row: np.record) -> None:
        trx = self._trx_conn
        component_cls = self._component_cls
        idx_prefix = self._idx_prefix

        self._trx_check_unique(None, row)
        # 开始自增id模式, 并用placeholder {rowid}替换id
        trx.stack_cmd('AUTO_INCR', idx_prefix + 'id')
        row_id = '{rowid}'
        row_key = self._key_prefix + str(row_id)
        # 设置row数据
        kvs = itertools.chain.from_iterable(zip(row.dtype.names, row.tolist()))
        trx.stack_cmd('hset', row_key, *kvs)
        # 更新索引
        for idx_name, str_type in component_cls.indexes_.items():
            idx_key = idx_prefix + idx_name
            if str_type:
                trx.stack_cmd('zadd', idx_key, 0, f'{row[idx_name]}:{row_id}')
            elif idx_name == 'id':
                trx.stack_cmd('zadd', idx_key, row_id, row_id)
            else:
                trx.stack_cmd('zadd', idx_key, row[idx_name].item(), row_id)
        # 结束自增id模式
        trx.stack_cmd('hset', row_key, 'id', row_id)
        trx.stack_cmd('END_INCR')

    def _trx_update(self, row_id: int, old_row: np.record, new_row: np.record) -> None:
        trx = self._trx_conn
        component_cls = self._component_cls
        idx_prefix = self._idx_prefix

        self._trx_check_unique(old_row, new_row)
        # 更新row数据
        row_key = self._key_prefix + str(row_id)
        kvs = []
        for key_name, new_value in zip(new_row.dtype.names, new_row.tolist()):
            if old_row[key_name] != new_value:
                kvs.extend([key_name, new_value])
        # 如果没任何数据变动，跳出
        if len(kvs) == 0:
            return
        # 更新数据
        trx.stack_cmd('hset', row_key, *kvs)
        # 更新索引
        for idx_name, str_type in component_cls.indexes_.items():
            if old_row[idx_name] == new_row[idx_name]:
                continue
            idx_key = idx_prefix + idx_name
            if str_type:
                trx.stack_cmd('zadd', idx_key, 0, f'{new_row[idx_name]}:{row_id}')
                trx.stack_cmd('zrem', idx_key, f'{old_row[idx_name]}:{row_id}')
            elif idx_name == 'id':
                trx.stack_cmd('zadd', idx_key, row_id, row_id)
            else:
                trx.stack_cmd('zadd', idx_key, new_row[idx_name].item(), row_id)

    def _trx_delete(self, row_id: int, old_row: np.record) -> None:
        trx = self._trx_conn
        component_cls = self._component_cls
        idx_prefix = self._idx_prefix

        row_key = self._key_prefix + str(row_id)
        trx.stack_cmd('del', row_key)

        for idx_name, str_type in component_cls.indexes_.items():
            idx_key = idx_prefix + idx_name
            if str_type:
                trx.stack_cmd('zrem', idx_key, f'{old_row[idx_name]}:{row_id}')
            else:
                trx.stack_cmd('zrem', idx_key, row_id)


##############################


class RedisMQClient(MQClient):
    """连接到消息队列的客户端，每个用户连接一个实例。"""

    def __init__(self, redis_conn: redis.asyncio.Redis | redis.asyncio.RedisCluster):
        # todo 要测试redis cluster是否能正常pub sub
        # 2种模式：
        # a. 每个ws连接一个pubsub连接，分发交给servants，结构清晰，目前的模式，但网络占用高
        # b. 每个worker一个pubsub连接，分发交给worker来做，这样连接数较少，但等于2套分发系统结构复杂
        self._mq = redis_conn.pubsub()
        self.subscribed = set()
        self.pulled_deque = MultiMap()
        self.pulled_set = set()

    async def close(self):
        return await self._mq.aclose()

    async def subscribe(self, channel_name) -> None:
        await self._mq.subscribe(channel_name)
        self.subscribed.add(channel_name)
        if len(self.subscribed) > MAX_SUBSCRIBED:
            # 抑制此警告可通过修改hetu.backend.redis.MAX_SUBSCRIBED参数
            logger.warning(f"⚠️ [💾Redis] 订阅数据数超过1000行，可能导致网络和CPU消耗过大，"
                           f"当前订阅数：{len(self.subscribed)}。")

    async def unsubscribe(self, channel_name) -> None:
        await self._mq.unsubscribe(channel_name)
        self.subscribed.remove(channel_name)

    async def pull(self) -> None:
        mq = self._mq

        # 如果没订阅过内容，那么redis mq的connection是None，无需get_message
        if mq.connection is None:
            await asyncio.sleep(0.5)  # 不写协程就死锁了
            return

        # 获得更新得频道名，如果不在pulled列表中，才添加，列表按添加时间排序
        msg = await mq.get_message(ignore_subscribe_messages=True, timeout=None)
        if msg is not None:
            channel_name = msg['channel']
            # 判断是否已在deque中了，get_message会自动去重，这里判断是为了防止pop的时间正好夹断2条相同的消息
            if channel_name not in self.pulled_set:
                self.pulled_deque.add(time.time(), channel_name)
                self.pulled_set.add(channel_name)
                # pop 2分钟前的消息，防止队列溢出
                dropped = set(self.pulled_deque.pop(0, time.time() - 120))
                self.pulled_set -= dropped

    async def get_message(self) -> set[str]:
        pulled_deque = self.pulled_deque

        interval = 1 / self.UPDATE_FREQUENCY
        # 如果没数据，等待直到有数据
        while not pulled_deque:
            await asyncio.sleep(interval)

        while True:
            # 只取超过interval的数据，这样可以减少频繁更新。set一下可以合并相同消息
            rtn = set(pulled_deque.pop(0, time.time() - interval))
            if rtn:
                self.pulled_set -= rtn
                return rtn
            await asyncio.sleep(interval)

    @property
    def subscribed_channels(self) -> set[str]:
        return set(self._mq.channels) - set(self._mq.pending_unsubscribe_channels)
