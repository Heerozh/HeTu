"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""
import numpy as np
import random
import hashlib
import redis
import asyncio
from datetime import datetime, timedelta
from sanic.log import logger
from ..component import BaseComponent, Property
from .base import ComponentTable, RaceCondition


def get_coroutine_executor():
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run
    else:
        return asyncio.create_task


class RedisBackend:
    def __init__(self, config: dict):
        # 同步io连接, 异步io连接, 只读io连接
        self.io = redis.from_url(config['master'], decode_responses=True)
        self.aio = redis.asyncio.from_url(config['master'], decode_responses=True)
        servants = config.get('servants', [])
        if servants:
            self.replicas = [redis.asyncio.from_url(url, decode_responses=True)
                             for url in config['servants']]
        else:
            self.replicas = [self.aio]

        # 配置keyspace通知
        run_async = get_coroutine_executor()
        for replica in self.replicas:
            run_async(replica.config_set('notify-keyspace-events', 'Kghz'))

    def rnd_replica(self):
        """每个websocket连接获得一个随机的replica连接，用于读取订阅"""
        i = random.randint(0, len(self.replicas))
        return i, self.replicas[i]


class RedisComponentTable(ComponentTable):
    """
    使用redis实现的Component后端。

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
                 backend: RedisBackend):
        super().__init__(component_cls, instance_name, cluster_id, backend)
        component_cls.hosted_ = self
        # redis key名
        hash_tag = f'{{CLU{cluster_id}}}'
        # 不能用component_cls.__name__ 可能是json加载的名字不对
        self._name = component_cls.components_name_
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
        json: 表的结构信息
        version: json的hash
        cluster_id: 所属簇id
        last_index_rebuild: 上次重建索引时间
        """
        io = self._backend.io
        lock = io.lock(self._lock_key)
        lock.acquire(blocking=True)

        # 获取redis已存的表信息
        meta = io.hgetall(self._meta_key)
        if not meta:
            meta = self._create_emtpy()
        else:
            version = hashlib.md5(self._component_cls.json_.encode("utf-8")).hexdigest()
            # 如果cluster_id改变，则迁移改key名
            if int(meta['cluster_id']) != self._cluster_id:
                self._migration_cluster_id(old=int(meta['cluster_id']))

            # 如果版本不一致，表结构可能有变化，也可能只是改权限，总之调用迁移代码
            if meta['version'] != version:
                self._migration_schema(old=meta['json'])

        # 重建数据，每次启动间隔超过1小时就重建
        last_index_rebuild = datetime.fromisoformat(meta.get('last_index_rebuild'))
        now = datetime.now().astimezone()
        if last_index_rebuild <= now - timedelta(hours=1):
            # 如果非持久化表，则每次启动清空
            if not self._component_cls.persist_:
                logger.info(f"⌚ [💾Redis] {self._name}表 无需持久化，清空中...")
                del_keys = io.keys(self._root_prefix + '*')
                map(io.delete, del_keys)

            # 重建索引，如果已处理过了就不处理
            self._rebuild_index()
            # 写入meta信息
            io.hset(self._meta_key, 'last_index_rebuild', now.isoformat())

        lock.release()

    def _create_emtpy(self):
        logger.info(f"ℹ️ [💾Redis] {self._name}表无meta信息，正在重新创建...")

        # 只需要写入meta，其他的_rebuild_index会创建
        meta = {
            'json': self._component_cls.json_,
            'version': hashlib.md5(self._component_cls.json_.encode("utf-8")).hexdigest(),
            'cluster_id': self._cluster_id,
            'last_index_rebuild': '2024-06-19T03:41:18.682529+08:00'
        }
        self._backend.io.hset(self._meta_key, mapping=meta)
        return meta

    def _rebuild_index(self):
        logger.info(f"⌚ [💾Redis] {self._name}表 正在重建索引...")
        io = self._backend.io
        rows = io.keys(self._key_prefix + '*')
        if len(rows) == 0:
            logger.info(f"✅ [💾Redis] {self._name}表 无数据，无需重建索引。")
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
                io.hget(row, idx_name)
            values = pipe.execute()
            if str_type:
                # 字符串类型要特殊处理，score=0, member='name:1'形式
                io.zadd(idx_key, {f'{value}:{rid}': 0 for rid, value in zip(row_ids, values)})
            else:
                # zadd 会替换掉member相同的值，等于是set
                io.zadd(idx_key, dict(zip(row_ids, values)))

    def _migration_cluster_id(self, old):
        logger.warning(f"⚠️ [💾Redis] {self._name}表 cluster_id 由 {old} 变更为 {self._cluster_id}，"
                       f"将尝试迁移cluster数据...")
        # 重命名key
        old_hash_tag = f'{{CLU{old}}}'
        new_hash_tag = f'{{CLU{self._cluster_id}}}'
        old_prefix = f'{self._root_prefix}{old_hash_tag}:'
        old_prefix_len = len(old_prefix)
        new_prefix = f'{self._root_prefix}{new_hash_tag}:'

        io = self._backend.io
        old_keys = io.keys(old_prefix + '*')
        for old_key in old_keys:
            new_key = new_prefix + old_key[old_prefix_len:]
            io.rename(new_key, new_key)
        # 更新meta
        io.hset(self._meta_key, 'cluster_id', self._cluster_id)

    def _migration_schema(self, old):
        """如果数据库中的属性和定义不一致，尝试进行简单迁移，可以处理属性更名以外的情况。"""
        # 加载老的表
        old_comp_cls = BaseComponent.load_json(old)

        # 只有properties名字和类型变更才迁移
        dtypes_in_db = old_comp_cls.dtypes
        new_dtypes = self._component_cls.dtypes
        if dtypes_in_db == new_dtypes:
            return

        logger.warning(f"⚠️ [💾Redis] {self._name}表 代码定义与存档不一致，"
                       f"存档：\n"
                       f"{dtypes_in_db}\n"
                       f"代码定义的：\n"
                       f"{new_dtypes}\n "
                       f"将尝试迁移数据：")

        # todo 调用自定义版本迁移代码（define_migration）

        # 检查是否有属性被删除
        for prop_name in dtypes_in_db.fields:
            if prop_name not in new_dtypes.fields:
                logger.warning(f"⚠️ [💾Redis] {self._name}表 "
                               f"新的代码定义中缺少属性 {prop_name}，如果改名了需要手动迁移，"
                               f"默认丢弃该属性数据。")

        # 多出来的列再次报警告，然后忽略
        io = self._backend.io
        props = dict(self._component_cls.properties_)  # type: dict[str, Property]
        for prop_name in new_dtypes.fields:
            if prop_name not in dtypes_in_db.fields:
                logger.warning(f"⚠️ [💾Redis] {self._name}表 "
                               f"新的代码定义中多出属性 {prop_name}，将使用默认值填充。")
                default = props[prop_name].default
                if default is None:
                    logger.error(f"⚠️ [💾Redis] {self._name}表 "
                                 f"迁移时尝试新增 {prop_name} 属性失败，该属性没有默认值，无法新增。")
                    raise ValueError("迁移失败")
                pipe = io.pipeline()
                rows = io.keys(self._key_prefix + '*')
                for row in rows:
                    pipe.hset(row, prop_name, default)
                pipe.execute()

    def begin_transaction(self):
        super().begin_transaction()
        self._autoinc = -1
        self._trans_pipe = self._backend.aio.pipeline(transaction=True)
        # 强制pipeline进入立即模式，不然当我们需要读取未锁定的index时，会不返回结果
        self._trans_pipe.watching = True

    async def end_transaction(self, discard):
        # 并实现事务提交的操作，将_updates中的命令写入事务
        if discard:
            self._trans_pipe.discard()
            self._trans_pipe = None
            return True

        pipe = self._trans_pipe

        # 对unique index进行最终检查，之前虽然检查过，但没有lock index，
        # 此次检查会锁定index，在最后才锁定index可以降低事务冲突概率
        locked_indexes = set()
        for idx in self._component_cls.uniques_:
            for cmd, _, old_row, new_row in self._updates:
                if (cmd == 'update' and old_row[idx] != new_row[idx]) or cmd == 'insert':
                    if idx not in locked_indexes:
                        await pipe.watch(self._idx_prefix + idx)
                        locked_indexes.add(idx)
                    if len(await self._backend_query(idx, new_row[idx].item(), limit=1)) > 0:
                        raise RaceCondition()

        # 执行事务
        pipe.multi()
        for cmd, row_id, old_row, new_row in self._updates:
            if cmd == 'delete':
                row_key = self._key_prefix + str(row_id)
                pipe.delete(row_key)
                for idx_name, str_type in self._component_cls.indexes_.items():
                    idx_key = self._idx_prefix + idx_name
                    if str_type:
                        pipe.zrem(idx_key, f'{old_row[idx_name]}:{row_id}')
                    else:
                        pipe.zrem(idx_key, row_id)
            else:
                # 插入/更新
                row_key = self._key_prefix + str(row_id)
                dict_row = dict(zip(new_row.dtype.names, new_row.tolist()))
                pipe.hset(row_key, mapping=dict_row)
                for idx_name, str_type in self._component_cls.indexes_.items():
                    idx_key = self._idx_prefix + idx_name
                    if str_type:
                        # 先删除老数据
                        if cmd == 'update':
                            pipe.zrem(idx_key, f'{old_row[idx_name]}:{row_id}')
                        pipe.zadd(idx_key, {f'{dict_row[idx_name]}:{row_id}': 0})
                    else:
                        pipe.zadd(idx_key, {str(row_id): dict_row[idx_name]})

        try:
            await pipe.execute()
        except redis.WatchError:
            self._trans_pipe = None
            return False
        self._trans_pipe = None
        return True

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

    async def _backend_get_max_id(self):
        if self._autoinc >= 0:
            return self._autoinc + sum([1 for cmd, _, _, _ in self._updates if cmd == 'insert'])

        # 获取最大id的操作
        idx_key = self._idx_prefix + 'id'
        pipe = self._trans_pipe

        max_score = await pipe.zrange(idx_key, 0, 0, desc=True, withscores=True)
        max_score = max_score[0][1] if max_score else 0
        self._autoinc = max_score
        return max_score

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
            left = type(left) is np.str_ and left.item() or left
            right = type(right) is np.str_ and right.item() or right
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

        # 同时锁定所有读取的行
        # row_keys = [self._key_prefix + row_id for row_id in row_ids]
        # await asyncio.gather(*[pipe.watch(row_key) for row_key in row_keys])
        # rtn = await asyncio.gather(*[pipe.hgetall(row_key) for row_key in row_keys])

        # 未查询到数据时返回[]
        return list(map(int, row_ids))




