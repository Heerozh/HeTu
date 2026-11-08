"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import logging
from collections.abc import Mapping
from contextlib import AbstractContextManager
from typing import TYPE_CHECKING, Any, cast, final, override

import numpy as np
from redis.cluster import RedisCluster

from ....common.helper import batched
from ...component import BaseComponent
from ..base import (
    RowFormat,
    TableMaintenance,
    member_row_id_,
    member_value_,
    schema_version_,
)
from ..redis_model import CommitSeqScan
from ..table import TableReference

if TYPE_CHECKING:
    import redis
    import redis.lock
    from redis.typing import EncodableT, FieldT

    from .client import RedisBackendClient

logger = logging.getLogger("HeTu.root")


@final
class RedisTableMaintenance(TableMaintenance):
    """
    表维护类，服务器启动时会调用此类检查组件表状态，并创建不存在的表。
    如果发现表的cluster_id或schema不匹配，则显示警告，要求管理员手动运行cli迁移命令。

    继承此类实现具体的维护逻辑，此类除了check_table/create_table，其他方法仅在CLI相关命令时才会启用。
    """

    _lock_key = "maintenance:lock"
    client: RedisBackendClient

    @staticmethod
    def meta_key(table_ref: TableReference) -> str:
        """获取redis表元数据的key名"""
        from .client import RedisBackendClient

        return f"{RedisBackendClient.table_prefix(table_ref)}:meta"

    def get(self, ref: TableReference, row_id: int) -> np.record | None:
        """获取指定表的指定行数据"""
        key = self.client.row_key(ref, row_id)
        io = self.client.io
        if row := io.hgetall(key):
            row = cast(dict, row)
            return self.client.row_decode_(ref.comp_cls, row, RowFormat.STRUCT)
        return None

    def range(
        self,
        ref: TableReference,
        index_name: str,
        left: Any,
        right: Any = None,
        limit: int = 10,
        *,
        group: Mapping[str, Any] | None = None,
    ) -> list[int]:
        """按索引范围查询指定表的数据。复合索引要用 group 给出全部分组列的值"""
        idx_key, b_left, b_right, empty = self.client.zrange_args_(
            ref, index_name, left, right, False, group
        )
        if empty:
            return []
        row_ids = self.client.io.zrange(
            name=idx_key, **self.client.make_zrange_cmd_(b_left, b_right, False, limit)
        )
        return [member_row_id_(vk) for vk in cast(list[bytes], row_ids)]

    @override
    def get_all_row_id(self, ref: TableReference) -> list[int]:
        # 获取所有row id
        io = self.client.io
        keys = io.keys(
            self.client.cluster_prefix(ref) + ":id:*",
            target_nodes=RedisCluster.PRIMARIES,
        )
        # 执行迁移脚本函数
        keys = cast(list[bytes], keys)
        return [int(key.split(b":")[-1]) for key in keys]

    @override
    def delete_row(self, ref: TableReference, row_id: int):
        """删除指定表的指定行数据"""
        key = self.client.row_key(ref, row_id)
        self.client.io.delete(key)

    @override
    def upsert_row(self, ref: TableReference, row_data: np.record):
        """更新指定表的一行数据，如果不存在就插入"""
        io = self.client.io
        key = self.client.row_key(ref, row_data.id)
        io.delete(key)
        mapping = ref.comp_cls.struct_to_dict(row_data)
        io.hset(key, mapping=mapping)

    @override
    def read_meta(
        self, instance_name: str, comp: type[BaseComponent] | str
    ) -> TableMaintenance.TableMeta | None:
        """读取组件表的meta信息"""
        # meta key 只由 instance 与组件名组成（见 meta_key / table_prefix），与 cluster_id 无关
        key = f"{instance_name}:{self.comp_name_of_(comp)}:meta"

        io = self.client.io
        meta = cast(dict, io.hgetall(key))
        if not meta:
            return None
        return TableMaintenance.TableMeta(
            version=meta[b"version"].decode(),
            json=meta[b"json"].decode(),
            cluster_id=int(meta[b"cluster_id"]),
            extra={},
        )

    @override
    def get_lock(self) -> AbstractContextManager:
        """获得一个可以锁整个数据库的with锁"""
        return self.lock

    def __init__(self, master: RedisBackendClient):
        super().__init__(master)
        self.lock: redis.lock.Lock = self.client.io.lock(self._lock_key, timeout=60 * 5)

    @override
    def do_create_table_(self, table_ref: TableReference) -> TableMaintenance.TableMeta:
        """创建组件表。如果已存在，会抛出RaceCondition异常"""
        # 只需要写入meta，其他的_rebuild_index会创建
        meta = {
            "json": table_ref.comp_cls.json_,
            "version": schema_version_(table_ref.comp_cls.json_),
            "cluster_id": table_ref.cluster_id,
        }
        assert not self.client.io.exists(self.meta_key(table_ref))
        self.client.io.hset(self.meta_key(table_ref), mapping=meta)
        meta_recon = self.read_meta(table_ref.instance_name, table_ref.comp_cls)
        assert meta_recon
        return meta_recon

    # 无需drop_table, 此类操作适合人工删除

    def do_rename_table_(self, from_: TableReference, to_: TableReference):
        """
        重命名组件表。

        提交序号列的计数器（`{cluster_prefix}:seq:…`）只在组件名不变（换簇）时跟着搬：默认迁移
        脚本先把原表改名成临时表、拷完再删掉临时表，计数器跟着搬就会被一起删掉，之后只能按剩下
        的行校准，删掉的尾部号会被复用。留在原名下，新表的 cluster_prefix 和原表相同，接着用
        """
        # 重命名key
        from_prefix = f"{self.client.cluster_prefix(from_)}:"
        from_prefix_len = len(from_prefix)
        to_prefix = f"{self.client.cluster_prefix(to_)}:"

        io = self.client.io
        from_keys = io.keys(
            from_prefix + "*",
            target_nodes=RedisCluster.PRIMARIES,
        )
        from_keys = cast(list[bytes], from_keys)
        if from_.comp_name != to_.comp_name:
            seq_head = (from_prefix + "seq:").encode()
            from_keys = [key for key in from_keys if not key.startswith(seq_head)]
        for b_from_key in from_keys:
            from_key = b_from_key.decode()
            to_key = to_prefix + from_key[from_prefix_len:]
            dump_data = cast(bytes, io.dump(from_key))
            ttl = cast(float, io.pttl(from_key))
            if ttl is None or ttl < 0:
                ttl = 0  # 0 代表永不过期
            io.restore(to_key, ttl, dump_data, replace=True)
            io.delete(from_key)  # cluster 不能跨节点rename，必须create+delete

        # 更新meta：json 换成 to_ 的（改名到临时组件时组件名变了）。版本号照搬：改名不动数据，
        # 索引还是原来的格式，按当前格式重算会把旧格式的表标成新的，迁移就不重建了
        from_meta_key = self.meta_key(from_)
        to_meta_key = self.meta_key(to_)
        version = cast(bytes, io.hget(from_meta_key, "version"))
        io.delete(from_meta_key)
        meta = {
            "json": to_.comp_cls.json_,
            "version": version,
            "cluster_id": to_.cluster_id,
        }
        self.client.io.hset(to_meta_key, mapping=meta)

        logger.warning(
            f"  ✔️ [💾Redis][{to_.comp_name}组件] rename完成，共改名{len(from_keys)}个键值。"
        )

    @override
    def do_update_meta_(self, table_ref: TableReference) -> None:
        """把组件表的meta改写成table_ref的定义，不动表数据"""
        json_ = table_ref.comp_cls.json_
        meta: dict[FieldT, EncodableT] = {
            "json": json_,
            "version": schema_version_(json_),
            "cluster_id": table_ref.cluster_id,
        }
        self.client.io.hset(self.meta_key(table_ref), mapping=meta)

    @override
    def do_drop_table_(self, table_ref: TableReference) -> int:
        """
        清空易失性组件表数据，force为True时强制清空任意组件表。
        注意：此操作会删除所有数据！
        """
        io = self.client.io
        # 删除数据（所有簇的行与索引，不碰名字以本组件名开头的副本组件）
        del_keys = io.keys(
            self.client.all_clusters_prefix(table_ref) + "*",
            target_nodes=RedisCluster.PRIMARIES,
        )
        del_keys = cast(list[bytes], del_keys)
        del_keys = [key.decode() for key in del_keys]
        for batch in batched(del_keys, 1000):
            with io.pipeline() as pipe:
                list(map(pipe.delete, batch))
                pipe.execute()
        # 删除meta
        io.delete(self.meta_key(table_ref))
        return len(del_keys)

    # 重建索引时每批读多少行、写多少个 member：几百万行塞进一条命令，客户端和 Redis 都要占
    # 一大块内存
    REBUILD_BATCH = 10000

    def _delete_keys(self, keys: list[str]) -> None:
        """
        分批 UNLINK：旧的单列索引可能很大，DEL 会阻塞 Redis；复合索引的组 key 可能很多，一条命令
        塞不下。空列表什么也不做（不带参数的命令会报错，盖住调用方要抛的异常）
        """
        for batch in batched(keys, 1000):
            self.client.io.unlink(*batch)

    def _table_keys(
        self, table_ref: TableReference
    ) -> tuple[list[str], list[str], dict[str, list[str]]]:
        """
        这张表（这个簇）名下的 key，一次 KEYS 拿到再按种类分开：(行 key, 上次重建中断留下的临时
        key, {索引名: 这个索引名下的 zset key})。KEYS 每次都要扫整个库，hetu upgrade 默认重建
        所有表，按种类、按索引各扫一次会把停服时间拉长好几倍
        """
        cluster_prefix = self.client.cluster_prefix(table_ref)
        found = self.client.io.keys(
            cluster_prefix + ":*", target_nodes=RedisCluster.PRIMARIES
        )
        row_head = cluster_prefix + ":id:"
        idx_head = cluster_prefix + ":index:"
        tmp_head = cluster_prefix + ":rebuilding:"
        rows: list[str] = []
        leftover: list[str] = []
        indexes: dict[str, list[str]] = {}
        for raw in cast(list[bytes], found):
            key = raw.decode()
            if key.startswith(row_head):
                rows.append(key)
            elif key.startswith(idx_head):
                # 索引名（字段名）和组 key 的 token 里都没有 ":"：{idx_key} 或 {idx_key}:{token}
                name = key[len(idx_head) :].split(":", 1)[0]
                indexes.setdefault(name, []).append(key)
            elif key.startswith(tmp_head):
                leftover.append(key)
        return rows, leftover, indexes

    @override
    def do_rebuild_index_(self, table_ref: TableReference) -> int:
        """
        按行数据重建组件表的索引，返回行数。分两阶段：先把每个索引都建到临时 key、通过 unique
        检查，全部建好之后再统一 RENAME 覆盖旧索引。建的阶段失败（中断、超时、unique 冲突）所有
        旧索引原样保留：逐个建、逐个换的话，后面的索引失败时前面的已经换成新定义的格式（比如复合
        索引），而 meta 还是旧定义，回退代码就读不懂了。换上阶段很短，在那里出错时，已换上的索引
        是新的，出错的索引原来的 key 还在（先换上、后删残留），重跑一次即可恢复（设计稿 §14.6）。

        复合索引每组一个 zset，按组建临时 key；换上之后，删掉这个索引名下不在新结果里的 key：
        已经空了的旧组、改成复合之前的单个 key、改回单列之前的组 key。表里一行都没有时，索引里
        剩下的都是残留，直接删掉。上次中断留下的临时 key 在开头清掉。

        提交序号列：建的阶段统计每组最大的号，组内有重复、有号到了上限时报错（见
        `CommitSeqScan`）；检查通过后先把计数器校准到每组最大的号（只升不降），再换上索引。
        """
        from .client import RedisBackendClient

        io = self.client.io
        cluster_prefix = self.client.cluster_prefix(table_ref)
        keys, leftover, existing = self._table_keys(table_ref)
        comp_cls = table_ref.comp_cls
        # 临时 key 带同一个 {CLU} hash tag（cluster 模式下同 slot 才能 RENAME），又不在
        # `{idx_key}:` 下，不会被当成组 key。先删掉上次中断留下的
        idx_head = cluster_prefix + ":index:"
        tmp_head = cluster_prefix + ":rebuilding:"
        self._delete_keys(leftover)

        if not keys:
            for idx_name in comp_cls.indexes_:
                self._delete_keys(existing.get(idx_name, []))
            return 0

        # 只拿来按 dtype 转值，给定 id 不发号：hetu upgrade 进程没初始化 SnowflakeID
        struct = comp_cls.new_row(id_=0)
        # {索引名: {zset key: 临时 key}}
        renames: dict[str, dict[str, str]] = {}
        # 提交序号列：建的时候统计每组最大的号，建完先检查、再校准计数器，最后才换上
        scan = (
            CommitSeqScan(comp_cls, table_ref.comp_name)
            if comp_cls.commit_seq_
            else None
        )
        try:
            for idx_name in comp_cls.indexes_:
                idx_key = self.client.index_key(table_ref, idx_name)
                targets = renames.setdefault(idx_name, {})
                # 复合索引要读分组列和本字段
                columns = (*comp_cls.index_groups_[idx_name], idx_name)
                is_unique = idx_name in comp_cls.uniques_
                seen: set[bytes] = set()
                for chunk in batched(keys, self.REBUILD_BATCH):
                    pipe = io.pipeline()
                    for key in chunk:
                        pipe.hmget(key, columns)
                    rows: list[list[bytes | None]] = pipe.execute()
                    # 不管 unique 还是 index 都是 zset，member 是 值段\x00id 段
                    by_key: dict[str, dict[bytes, int]] = {}
                    for key, raws in zip(chunk, rows):
                        if any(raw is None for raw in raws):
                            raise RuntimeError(
                                f"组件{table_ref.comp_name}的行{key}缺少索引"
                                f"`{idx_name}`要用的字段{columns}，重建索引失败。"
                            )
                        # 与 commit 写索引时同一个编码（SQLite 后端的重建也用它）
                        zset_key, member = RedisBackendClient.rebuild_entry_(
                            comp_cls,
                            idx_key,
                            idx_name,
                            struct,
                            dict(zip(columns, cast(list[bytes], raws))),
                            int(key.rsplit(":", 1)[-1]),
                        )
                        if is_unique:
                            # 按值段（复合索引带组前缀）查重，同组同值才算冲突
                            value = member_value_(member)
                            if value in seen:
                                raise RuntimeError(
                                    f"组件{table_ref.comp_name}的unique索引`{idx_name}`"
                                    f"（列 {columns}）在重建时发现违反unique约束，可能是迁移时"
                                    f"缩短了值类型、新增了Unique标记、或去掉了groupby（组内"
                                    f"唯一变成了全局唯一）导致。"
                                )
                            seen.add(value)
                        if scan is not None and idx_name == scan.field:
                            scan.add(struct, member)
                        by_key.setdefault(zset_key, {})[member] = 0
                    pipe = io.pipeline()
                    for zset_key, members in by_key.items():
                        if (tmp_key := targets.get(zset_key)) is None:
                            tmp_key = tmp_head + zset_key[len(idx_head) :]
                            targets[zset_key] = tmp_key
                        pipe.zadd(tmp_key, members)
                    pipe.execute()
            if scan is not None:
                # 组内重复、到了上限：报错，旧索引、计数器都不动。先校准、再换上：只往上调计数器
                # 不会产生重号，换上之后才校准的话，中途被杀会留下偏低的计数器
                scan.raise_for_problems()
                self._raise_counters(scan.counters(cluster_prefix))
        except BaseException:
            self._delete_keys(
                [tmp for targets in renames.values() for tmp in targets.values()]
            )
            raise
        for idx_name, targets in renames.items():
            # 先换上、后删残留：换到一半出错（超时、中断）时，这个索引原来的 key 还在，回退到旧
            # 定义照样能读。一批一条脚本：少些往返，一批之内是原子的。cluster 模式的管道不让发
            # RENAME，脚本的 key 都带同一个 {CLU} hash tag，在同一个 slot
            for batch in batched(targets.items(), self.REBUILD_BATCH):
                names = [name for zset_key, tmp in batch for name in (tmp, zset_key)]
                io.eval(self._RENAME_LUA, len(names), *names)
            # 这个索引名下（开头列出的）不在新结果里的：空了的旧组、另一种格式的 key
            stale = [key for key in existing.get(idx_name, []) if key not in targets]
            self._delete_keys(stale)
        return len(keys)

    # KEYS 依次是 (临时 key, 目标 key) 对
    _RENAME_LUA = """
for i = 1, #KEYS, 2 do
    redis.call("RENAME", KEYS[i], KEYS[i + 1])
end
return #KEYS / 2
"""

    # KEYS 是计数器，ARGV 是对应的最大号：只升不降。被写坏了的（不是字符串、不是整数串）直接
    # 改写：规则同 INCRBY 认的整数，和 SQLite 的 kv_get_int 一致，不用 tonumber（它还认
    # "0x10"、"5.5"、"1e3"）
    _RAISE_COUNTERS_LUA = """
for i = 1, #KEYS do
    local raw = redis.pcall("GET", KEYS[i])
    local current = nil
    if type(raw) == "string" and (raw == "0" or string.match(raw, "^%-?[1-9]%d*$")) then
        current = tonumber(raw)
    end
    if current == nil or current < tonumber(ARGV[i]) then
        redis.call("SET", KEYS[i], ARGV[i])
    end
end
return #KEYS
"""

    def _raise_counters(self, counters: list[tuple[str, int]]) -> None:
        """
        提交序号列的计数器校准到每组最大的号，只升不降。一批一条脚本：比较和写入之间不会插进别的
        提交。计数器都带同一个 {CLU} hash tag，cluster 模式下在同一个 slot
        """
        for batch in batched(counters, 1000):
            keys = [key for key, _seq in batch]
            values = [str(seq) for _key, seq in batch]
            self.client.io.eval(self._RAISE_COUNTERS_LUA, len(keys), *keys, *values)
