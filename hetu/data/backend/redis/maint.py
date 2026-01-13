"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import hashlib
import logging
import warnings
from typing import TYPE_CHECKING, cast, final, override, Any
import numpy as np

from ....common.helper import batched
from ...component import BaseComponent
from .. import RaceCondition
from ..base import TableMaintenance

from redis.cluster import RedisCluster

if TYPE_CHECKING:
    import redis
    import redis.lock
    from ..table import TableReference

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

    def __init__(self, master: RedisBackendClient):
        super().__init__(master)
        self.lock: redis.lock.Lock = self.client.io.lock(self._lock_key, timeout=60 * 5)

    @override
    def check_table(self, table_ref: TableReference) -> tuple[str, Any]:
        """
        检查组件表在数据库中的状态。
        此方法检查各个组件表的meta键值。

        Returns
        -------
        status: str
            "not_exists" - 表不存在
            "ok" - 表存在且状态正常
            "cluster_mismatch" - 表存在但cluster_id不匹配
            "schema_mismatch" - 表存在但schema不匹配
        meta: dict[bytes, Any]
            组件表的meta信息，一般含有：
                - b"version": 组件结构的md5值
                - b"json": 组件结构的json字符串
                - b"cluster_id": 组件所属的cluster id
        """
        io = self.client.io

        # 获取redis已存的组件信息
        key = self.meta_key(table_ref)
        meta = cast(dict, io.hgetall(key))
        if not meta:
            return "not_exists", None
        else:
            version = hashlib.md5(table_ref.comp_cls.json_.encode("utf-8")).hexdigest()
            # 如果cluster_id改变，则迁移改key名，必须先检查cluster_id
            if int(meta[b"cluster_id"]) != table_ref.cluster_id:
                return "cluster_mismatch", meta

            # 如果版本不一致，组件结构可能有变化，也可能只是改权限，总之调用迁移代码
            if meta[b"version"].decode() != version:
                return "schema_mismatch", meta

        return "ok", meta

    @override
    def create_table(self, table_ref: TableReference) -> Any:
        """创建组件表。如果已存在，会抛出异常"""
        with self.lock:
            if self.check_table(table_ref)[0] != "not_exists":
                raise RaceCondition(
                    f"[💾Redis][{table_ref.comp_name}组件] 组件表已存在，无法创建。"
                )
            logger.info(
                f"  ➖ [💾Redis][{table_ref.comp_name}组件] 组件无meta信息，数据不存在，正在创建空表..."
            )
            # 只需要写入meta，其他的_rebuild_index会创建
            meta = {
                "json": table_ref.comp_cls.json_,
                "version": hashlib.md5(
                    table_ref.comp_cls.json_.encode("utf-8")
                ).hexdigest(),
                "cluster_id": table_ref.cluster_id,
            }
            self.client.io.hset(self.meta_key(table_ref), mapping=meta)
            logger.info(f"  ✔️ [💾Redis][{table_ref.comp_name}组件] 空表创建完成")
            return meta

    # 无需drop_table, 此类操作适合人工删除

    @override
    def migration_cluster_id(self, table_ref: TableReference, old_meta: Any) -> None:
        """迁移组件表的cluster_id"""
        old_cluster_id = int(old_meta[b"cluster_id"])
        logger.warning(
            f"  ⚠️ [💾Redis][{table_ref.comp_name}组件] "
            f"cluster_id 由 {old_cluster_id} 变更为 {table_ref.cluster_id}，"
            f"将尝试迁移cluster数据..."
        )
        with self.lock:
            if self.check_table(table_ref)[0] != "cluster_mismatch":
                raise RaceCondition(
                    f"[💾Redis][{table_ref.comp_name}组件] 组件表已迁移过簇id。"
                )
            # 重命名key
            old_hash_tag = f"{{CLU{old_cluster_id}}}"
            new_hash_tag = f"{{CLU{table_ref.cluster_id}}}"
            old_prefix = f"{self.client.table_prefix(table_ref)}:{old_hash_tag}"
            old_prefix_len = len(old_prefix)
            new_prefix = f"{self.client.table_prefix(table_ref)}:{new_hash_tag}"

            io = self.client.io
            old_keys = io.keys(
                old_prefix + ":*",
                target_nodes=RedisCluster.PRIMARIES,
            )
            old_keys = cast(list[bytes], old_keys)
            for old_key in old_keys:
                old_key = old_key.decode()
                new_key = new_prefix + old_key[old_prefix_len:]
                dump_data = cast(bytes, io.dump(old_key))
                ttl = cast(float, io.pttl(old_key))
                if ttl is None or ttl < 0:
                    ttl = 0  # 0 代表永不过期
                io.restore(new_key, ttl, dump_data, replace=True)
                io.delete(old_key)  # cluster 不能跨节点rename，必须create+delete
            # 更新meta
            io.hset(self.meta_key(table_ref), "cluster_id", str(table_ref.cluster_id))
            logger.warning(
                f"  ✔️ [💾Redis][{table_ref.comp_name}组件] cluster 迁移完成，共迁移{len(old_keys)}个键值。"
            )

    @override
    def migration_schema(
        self, table_ref: TableReference, old_meta: Any, force=False
    ) -> bool:
        """
        迁移组件表的schema，本方法必须在migration_cluster_id之后执行。
        此方法调用后需要rebuild_index

        本方法将先寻找是否有迁移脚本，如果有则调用脚本进行迁移，否则使用默认迁移逻辑。

        默认迁移逻辑无法处理数据被删除的情况，以及类型转换失败的情况，
        force参数指定是否强制迁移，也就是遇到上述情况直接丢弃数据。
        """
        old_json = old_meta[b"json"].decode()
        old_version = old_meta[b"version"].decode()

        # todo 首先调用手动迁移，完成后再调用自动迁移
        # migration_script = self._load_migration_schema_script(table_ref, old_version)

        # 加载老的组件
        old_comp_cls = BaseComponent.load_json(old_json)

        # 只有properties名字和类型变更才迁移
        dtypes_in_db = old_comp_cls.dtypes
        new_dtypes = table_ref.comp_cls.dtypes
        if dtypes_in_db == new_dtypes:
            return True

        logger.warning(
            f"  ⚠️ [💾Redis][{table_ref.comp_name}组件] 代码定义的Schema与已存的不一致，"
            f"数据库中：\n"
            f"{dtypes_in_db}\n"
            f"代码定义的：\n"
            f"{new_dtypes}\n "
            f"将尝试数据迁移（只处理新属性，不处理类型变更，改名等等情况）："
        )

        # 检查是否有属性被删除
        assert dtypes_in_db.fields and new_dtypes.fields  # for type checker
        for prop_name in dtypes_in_db.fields:
            if prop_name not in new_dtypes.fields:
                msg = (
                    f"  ⚠️ [💾Redis][{table_ref.comp_name}组件] "
                    f"数据库中的属性 {prop_name} 在新的组件定义中不存在，如果改名了需要手动迁移，"
                    f"强制执行将丢弃该属性数据。"
                )
                logger.warning(msg)
                if not force:
                    return False

        # 检查是否有属性类型变更且无法自动转换
        for prop_name in new_dtypes.fields:
            if prop_name in dtypes_in_db.fields:
                old_type = dtypes_in_db.fields[prop_name]
                new_type = new_dtypes.fields[prop_name]
                if not np.can_cast(old_type[0], new_type[0]):
                    msg = (
                        f"  ⚠️ [💾Redis][{table_ref.comp_name}组件] "
                        f"属性 {prop_name} 的类型由 {old_type} 变更为 {new_type}，"
                        f"无法自动转换类型，需要手动迁移，强制执行将截断/丢弃该属性数据。"
                    )
                    logger.warning(msg)
                    if not force:
                        return False

        with self.lock:
            if self.check_table(table_ref)[0] != "schema_mismatch":
                raise RaceCondition(
                    f"[💾Redis][{table_ref.comp_name}组件] 组件表已迁移过schema。"
                )

            # 多出来的列再次报警告，然后忽略
            io = self.client.io
            keys = io.keys(
                self.client.cluster_prefix(table_ref) + ":id:*",
                target_nodes=RedisCluster.PRIMARIES,
            )
            keys = cast(list[bytes], keys)
            props = dict(table_ref.comp_cls.properties_)
            added = 0
            converted = 0
            convert_failed = 0
            for prop_name in new_dtypes.fields:
                if prop_name not in dtypes_in_db.fields:
                    logger.warning(
                        f"  ⚠️ [💾Redis][{table_ref.comp_name}组件] "
                        f"新的代码定义中多出属性 {prop_name}，将使用默认值填充。"
                    )
                    default = props[prop_name].default
                    if default is None:
                        logger.error(
                            f"  ⚠️ [💾Redis][{table_ref.comp_name}组件] "
                            f"迁移时尝试新增 {prop_name} 属性失败，该属性没有默认值，无法新增。"
                        )
                        raise ValueError("迁移失败")
                    pipe = io.pipeline()
                    for key in keys:
                        pipe.hset(key.decode(), prop_name, default)
                    pipe.execute()
                    added += 1
                elif force:  # 类型转换
                    old_type = dtypes_in_db.fields[prop_name][0]
                    new_type = new_dtypes.fields[prop_name][0]
                    if old_type == new_type:
                        continue
                    default = props[prop_name].default
                    pipe = io.pipeline()
                    for key in keys:
                        val = io.hget(key.decode(), prop_name)
                        if val is None:
                            continue
                        try:
                            val = cast(bytes, cast(object, val))
                            casted_val = new_type.type(old_type.type(val.decode()))

                            if np.issubdtype(new_type, np.character):
                                # 字符串类型需要特殊截断处理，不然np会自动延长
                                def fixed_str_len(dt: np.dtype) -> int:
                                    dt = np.dtype(dt)
                                    if dt.kind == "U":
                                        return dt.itemsize // 4
                                    if dt.kind == "S":
                                        return dt.itemsize
                                    raise TypeError(
                                        f"not a fixed-length string dtype: {dt!r}"
                                    )

                                casted_val = casted_val[: fixed_str_len(new_type)]

                            pipe.hset(key.decode(), prop_name, str(casted_val))
                            converted += 1
                        except ValueError as _:
                            # 强制模式下丢弃该属性
                            pipe.hset(key.decode(), prop_name, default)
                            convert_failed += 1
                    pipe.execute()

            # 更新meta
            version = hashlib.md5(table_ref.comp_cls.json_.encode("utf-8")).hexdigest()
            io.hset(self.meta_key(table_ref), "version", version)
            io.hset(self.meta_key(table_ref), "json", table_ref.comp_cls.json_)

            logger.warning(
                f"  ✔️ [💾Redis][{table_ref.comp_name}组件] 新属性增加完成，共处理{len(keys)}行 * "
                f"{added}个属性。 转换类型成功{converted}次，失败{convert_failed}次。"
            )
            return True

    @override
    def flush(self, table_ref: TableReference, force=False) -> None:
        """
        清空易失性组件表数据，force为True时强制清空任意组件表。
        注意：此操作会删除所有数据！
        """
        if force:
            warnings.warn("flush正在强制删除所有数据，此方式只建议维护代码调用。")

        # 如果非持久化组件，则允许调用flush主动清空数据
        if table_ref.comp_cls.volatile_ or force:
            io = self.client.io
            logger.info(
                f"⌚ [💾Redis][{table_ref.comp_name}组件] 对非持久化组件flush清空数据中..."
            )

            with self.lock:
                del_keys = io.keys(
                    self.client.table_prefix(table_ref) + ":*",
                    target_nodes=RedisCluster.PRIMARIES,
                )
                del_keys = cast(list[bytes], del_keys)
                del_keys = [key.decode() for key in del_keys]
                for batch in batched(del_keys, 1000):
                    with io.pipeline() as pipe:
                        list(map(pipe.delete, batch))
                        pipe.execute()

            logger.info(
                f"✅ [💾Redis][{table_ref.comp_name}组件] 已删除{len(del_keys)}个键值"
            )
            self.create_table(table_ref)
        else:
            raise ValueError(f"{table_ref.comp_name}是持久化组件，不允许flush操作")

    @override
    def rebuild_index(self, table_ref: TableReference) -> None:
        """重建组件表的索引数据"""
        from .client import RedisBackendClient

        logger.info(f"  ➖ [💾Redis][{table_ref.comp_name}组件] 正在重建索引...")
        with self.lock:
            io = self.client.io
            keys = io.keys(
                self.client.cluster_prefix(table_ref) + ":id:*",
                target_nodes=RedisCluster.PRIMARIES,
            )
            keys = cast(list[bytes], keys)
            if len(keys) == 0:
                logger.info(
                    f"  ✔️ [💾Redis][{table_ref.comp_name}组件] 无数据，无需重建索引。"
                )
                return

            for idx_name, _ in table_ref.comp_cls.indexes_.items():
                idx_key = self.client.index_key(table_ref, idx_name)
                # 先删除所有_idx_key开头的索引
                io.delete(idx_key)
                # 重建所有索引，不管unique还是index都是sset
                pipe = io.pipeline()
                b_row_ids: list[bytes] = []
                for key in keys:
                    row_id = key.split(b":")[-1]
                    b_row_ids.append(row_id)
                    pipe.hget(key.decode(), idx_name)
                values: list[bytes] = pipe.execute()
                # 把values按dtype转换下
                struct = table_ref.comp_cls.new_row()
                scalers: list[np.generic] = [np.str_()] * len(values)
                for i, v in enumerate(values):
                    struct[idx_name] = v.decode()
                    scalers[i] = struct[idx_name]

                # 建立redis索引
                def get_member(_value: np.generic, _b_row_id) -> bytes:
                    _sortable_value = RedisBackendClient.to_sortable_bytes(_value)
                    return _sortable_value + b":" + _b_row_id

                io.zadd(
                    idx_key,
                    {
                        get_member(scaler, b_row_id): 0
                        for b_row_id, scaler in zip(b_row_ids, scalers)
                    },
                )

                # 检测是否有unique违反
                if idx_name in table_ref.comp_cls.uniques_:
                    if len(values) != len(set(values)):
                        raise RuntimeError(
                            f"组件{table_ref.comp_name}的unique索引`{idx_name}`在重建时发现违反unique约束，"
                            f"可能是迁移时缩短了值类型、或新增了Unique标记导致。"
                        )

            logger.info(
                f"  ✔️ [💾Redis][{table_ref.comp_name}组件] 索引重建完成, "
                f"{len(keys)}行 * {len(table_ref.comp_cls.indexes_)}个索引。"
            )
