"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import hashlib
import logging
import warnings
from typing import TYPE_CHECKING

from ....common.helper import batched
from ...component import BaseComponent
from .. import (
    RaceCondition,
)
from ..base import CLITableMaintenance
from ..table import TableReference

if TYPE_CHECKING:
    from .client import RedisBackendClient

logger = logging.getLogger("HeTu.root")


class RedisCLITableMaintenance(CLITableMaintenance):
    _lock_key = "maintenance:lock"
    client: RedisBackendClient

    @staticmethod
    def meta_key(table_ref: TableReference) -> str:
        """获取redis表元数据的key名"""
        return f"{table_ref.instance_name}:{table_ref.comp_cls.component_name_}:meta"

    def __init__(self, client: RedisBackendClient):
        super().__init__(client)
        self.lock = self.client.io.lock(self._lock_key, timeout=60 * 5)

    async def lock(self):
        await self.lock.acquire()

    def check_table(self, table_ref: TableReference):
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
        """
        io = self.client.io

        # 获取redis已存的组件信息
        key = self.meta_key(table_ref)
        meta = io.hgetall(key)
        if not meta:
            return "not_exists"
        else:
            version = hashlib.md5(table_ref.comp_cls.json_.encode("utf-8")).hexdigest()
            # 如果cluster_id改变，则迁移改key名
            if int(meta["cluster_id"]) != table_ref.cluster_id:
                return "cluster_mismatch"

            # 如果版本不一致，组件结构可能有变化，也可能只是改权限，总之调用迁移代码
            if meta["version"] != version:
                return "schema_mismatch"

        return "ok"

    def create_table(self, table_ref: TableReference) -> dict:
        """创建组件表。如果已存在，会抛出异常"""
        with self.lock:
            if self.check_table(table_ref) != "not_exists":
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
            self._backend.io.hset(self.meta_key(table_ref), mapping=meta)
            logger.info(f"  ✔️ [💾Redis][{table_ref.comp_name}组件] 空表创建完成")
            return meta

    # 无需drop_table, 此类操作适合人工删除

    def migration_cluster_id(
        self, table_ref: TableReference, old_cluster_id: int
    ) -> None:
        """迁移组件表的cluster_id"""
        raise NotImplementedError

    def migration_schema(self, table_ref: TableReference, old_json: str) -> None:
        """迁移组件表的schema"""
        raise NotImplementedError

    def flush(self, table_ref: TableReference, force=False) -> None:
        """
        清空易失性组件表数据，force为True时强制清空任意组件表。
        注意：此操作会删除所有数据！
        """
        raise NotImplementedError

    def rebuild_index(self, table_ref: TableReference) -> None:
        """重建组件表的索引数据"""
        raise NotImplementedError

    def create_or_migrate(self, cluster_only=False):
        """
        检查表结构是否正确，不正确则尝试进行迁移。此方法同时会强制重建表的索引。
        meta格式:
        json: 组件的结构信息
        version: json的hash
        cluster_id: 所属簇id

        Parameters
        ----------
        cluster_only : bool
            如果为True，则只处理cluster_id的变更，其他结构迁移和重建索引等不处理。
        """
        # todo 考虑取消head_lock，通过记录版本号来实现，只要版本号不一致，就停止服务要求迁移
        #       同时把迁移移动到专门的cli命令中，不是自动执行而是手动让ci/cd发布流程调用
        if not self._backend.requires_head_lock():
            raise HeadLockFailed("redis中head_lock键")

        io = self._backend.io
        logger.info(f"⌚ [💾Redis][{self._name}组件] 准备锁定检查meta信息...")
        if cluster_only:
            logger.info(
                f"  ℹ️ [💾Redis][{self._name}组件] 此表仅cluster id迁移模式开启。"
            )
        with io.lock(self._init_lock_key, timeout=60 * 5):
            # 获取redis已存的组件信息
            meta = io.hgetall(self._meta_key)
            if not meta:
                self._create_emtpy()
            else:
                version = hashlib.md5(
                    self._component_cls.json_.encode("utf-8")
                ).hexdigest()
                # 如果cluster_id改变，则迁移改key名
                if int(meta["cluster_id"]) != self._cluster_id:
                    self._migration_cluster_id(old=int(meta["cluster_id"]))

                # 如果版本不一致，组件结构可能有变化，也可能只是改权限，总之调用迁移代码
                if meta["version"] != version and not cluster_only:
                    self._migration_schema(old=meta["json"])

            # 重建索引数据
            if not cluster_only:
                self._rebuild_index()
            logger.info(f"✅ [💾Redis][{self._name}组件] 检查完成，解锁组件")

    def flush(self, force=False):
        # todo 考虑取消head_lock，建议易失数据全部加上component行级timeout信息，过期后由
        #       hetu自己启动事务删除row,包括Index.
        #       flush命令则是交给ci/cd来执行，因为需要重启服务器必然牵涉到app版本提升，不然停机干嘛？
        if not self._backend.requires_head_lock():
            raise HeadLockFailed("redis中head_lock键")

        if force:
            warnings.warn("flush正在强制删除所有数据，此方式只建议维护代码调用。")

        # 如果非持久化组件，则允许调用flush主动清空数据
        if not self._component_cls.persist_ or force:
            io = self._backend.io
            logger.info(
                f"⌚ [💾Redis][{self._name}组件] 对非持久化组件flush清空数据中..."
            )

            with io.lock(self._init_lock_key, timeout=60 * 5):
                del_keys = io.keys(self._root_prefix + "*")
                del_keys.remove(self._init_lock_key)
                for batch in batched(del_keys, 1000):
                    with io.pipeline() as pipe:
                        list(map(pipe.delete, batch))
                        pipe.execute()
            logger.info(f"✅ [💾Redis][{self._name}组件] 已删除{len(del_keys)}个键值")

            self.create_or_migrate()
        else:
            raise ValueError(f"{self._name}是持久化组件，不允许flush操作")

    def _create_emtpy(self):
        logger.info(
            f"  ➖ [💾Redis][{self._name}组件] 组件无meta信息，数据不存在，正在创建空表..."
        )

        # 只需要写入meta，其他的_rebuild_index会创建
        meta = {
            "json": self._component_cls.json_,
            "version": hashlib.md5(
                self._component_cls.json_.encode("utf-8")
            ).hexdigest(),
            "cluster_id": self._cluster_id,
        }
        self._backend.io.hset(self._meta_key, mapping=meta)
        logger.info(f"  ✔️ [💾Redis][{self._name}组件] 空表创建完成")
        return meta

    def _rebuild_index(self):
        logger.info(f"  ➖ [💾Redis][{self._name}组件] 正在重建索引...")
        io = self._backend.io
        rows = io.keys(self._key_prefix + "*")
        if len(rows) == 0:
            logger.info(f"  ✔️ [💾Redis][{self._name}组件] 无数据，无需重建索引。")
            return

        for idx_name, str_type in self._component_cls.indexes_.items():
            idx_key = self._idx_prefix + idx_name
            # 先删除所有_idx_key开头的索引
            io.delete(idx_key)
            # 重建所有索引，不管unique还是index都是sset
            pipe = io.pipeline()
            row_ids = []
            for row in rows:
                row_id = row.split(":")[-1]
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
                io.zadd(
                    idx_key,
                    {f"{value}:{rid}": 0 for rid, value in zip(row_ids, values)},
                )
            else:
                # zadd 会替换掉member相同的值，等于是set
                io.zadd(idx_key, dict(zip(row_ids, values)))
            # 检测是否有unique违反
            if idx_name in self._component_cls.uniques_:
                if len(values) != len(set(values)):
                    raise RuntimeError(
                        f"组件{self._name}的unique索引`{idx_name}`在重建时发现违反unique约束，"
                        f"可能是迁移时缩短了值类型、或新增了Unique标记导致。"
                    )

        logger.info(
            f"  ✔️ [💾Redis][{self._name}组件] 索引重建完成, "
            f"{len(rows)}行 * {len(self._component_cls.indexes_)}个索引。"
        )

    def _migration_cluster_id(self, old):
        logger.warning(
            f"  ⚠️ [💾Redis][{self._name}组件] "
            f"cluster_id 由 {old} 变更为 {self._cluster_id}，"
            f"将尝试迁移cluster数据..."
        )
        # 重命名key
        old_hash_tag = f"{{CLU{old}}}:"
        new_hash_tag = f"{{CLU{self._cluster_id}}}:"
        old_prefix = f"{self._root_prefix}{old_hash_tag}"
        old_prefix_len = len(old_prefix)
        new_prefix = f"{self._root_prefix}{new_hash_tag}"

        io = self._backend.io
        old_keys = io.keys(old_prefix + "*")
        for old_key in old_keys:
            new_key = new_prefix + old_key[old_prefix_len:]
            io.rename(old_key, new_key)
        # 更新meta
        io.hset(self._meta_key, "cluster_id", self._cluster_id)
        logger.warning(
            f"  ✔️ [💾Redis][{self._name}组件] cluster 迁移完成，共迁移{len(old_keys)}个键值。"
        )

    def _migration_schema(self, old):
        """如果数据库中的属性和定义不一致，尝试进行简单迁移，可以处理属性更名以外的情况。"""
        # 加载老的组件
        old_comp_cls = BaseComponent.load_json(old)

        # 只有properties名字和类型变更才迁移
        dtypes_in_db = old_comp_cls.dtypes
        new_dtypes = self._component_cls.dtypes
        if dtypes_in_db == new_dtypes:
            return

        logger.warning(
            f"  ⚠️ [💾Redis][{self._name}组件] 代码定义的Schema与已存的不一致，"
            f"数据库中：\n"
            f"{dtypes_in_db}\n"
            f"代码定义的：\n"
            f"{new_dtypes}\n "
            f"将尝试数据迁移（只处理新属性，不处理类型变更，改名等等情况）："
        )

        # todo 调用自定义版本迁移代码（define_migration）

        # 检查是否有属性被删除
        for prop_name in dtypes_in_db.fields:
            if prop_name not in new_dtypes.fields:
                logger.warning(
                    f"  ⚠️ [💾Redis][{self._name}组件] "
                    f"数据库中的属性 {prop_name} 在新的组件定义中不存在，如果改名了需要手动迁移，"
                    f"默认丢弃该属性数据。"
                )

        # 多出来的列再次报警告，然后忽略
        io = self._backend.io
        rows = io.keys(self._key_prefix + "*")
        props = dict(self._component_cls.properties_)
        added = 0
        for prop_name in new_dtypes.fields:
            if prop_name not in dtypes_in_db.fields:
                logger.warning(
                    f"  ⚠️ [💾Redis][{self._name}组件] "
                    f"新的代码定义中多出属性 {prop_name}，将使用默认值填充。"
                )
                default = props[prop_name].default
                if default is None:
                    logger.error(
                        f"  ⚠️ [💾Redis][{self._name}组件] "
                        f"迁移时尝试新增 {prop_name} 属性失败，该属性没有默认值，无法新增。"
                    )
                    raise ValueError("迁移失败")
                pipe = io.pipeline()
                for row in rows:
                    pipe.hset(row, prop_name, default)
                pipe.execute()
                added += 1

        # 更新meta
        version = hashlib.md5(self._component_cls.json_.encode("utf-8")).hexdigest()
        io.hset(self._meta_key, "version", version)
        io.hset(self._meta_key, "json", self._component_cls.json_)

        logger.warning(
            f"  ✔️ [💾Redis][{self._name}组件] 新属性增加完成，共处理{len(rows)}行 * "
            f"{added}个属性。"
        )
