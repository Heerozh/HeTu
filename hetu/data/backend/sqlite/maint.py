"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import logging
import time
import uuid
from collections.abc import Mapping
from contextlib import AbstractContextManager
from typing import TYPE_CHECKING, Any, cast, final, override

import numpy as np

from ....i18n import _
from ...component import BaseComponent
from ..base import (
    RowFormat,
    TableMaintenance,
    member_row_id_,
    member_value_,
    schema_version_,
)
from ..table import TableReference
from .store import SQLiteStore

if TYPE_CHECKING:
    from .client import SQLiteBackendClient

logger = logging.getLogger("HeTu.root")


class _MaintenanceLock(AbstractContextManager):
    """
    锁整个库的维护锁，语义同 redis-py 的 `Lock`（Redis 后端用的那把）：带过期时间，获取时阻塞轮询，
    只释放自己拿到的；过期后被别人拿走了，释放时报错。
    """

    def __init__(self, client: SQLiteBackendClient, key: str, timeout: float):
        self.client = client
        self.key = key
        self.timeout = timeout
        self.sleep = 0.1
        self.token: bytes | None = None

    def __enter__(self):
        token = uuid.uuid4().hex.encode()
        while not self.client.run_sync_(
            SQLiteStore.kv_set_nx, self.key, token, self.timeout
        ):
            time.sleep(self.sleep)
        self.token = token
        return self

    def __exit__(self, *exc_info) -> None:
        token, self.token = self.token, None
        assert token is not None
        if not self.client.run_sync_(SQLiteStore.kv_delete_if, self.key, token):
            raise RuntimeError(
                _("维护锁已经过期并被别人拿走，无法释放：{key}").format(key=self.key)
            )


@final
class SQLiteTableMaintenance(TableMaintenance):
    """
    SQLite 后端的表维护，逐项对应 `RedisTableMaintenance`（在 SQLite 上模拟同一套 key）：
    `delete_row` 只删行、不动索引（残留由 `rebuild_index` 清），`upsert_row` 先删再整行写，
    `rebuild_index` 按行数据重算、原子替换。维护写入不发订阅通知。
    """

    _lock_key = "maintenance:lock"
    _lock_timeout = 60 * 5
    client: SQLiteBackendClient

    def __init__(self, master: SQLiteBackendClient):
        super().__init__(master)
        self.lock = _MaintenanceLock(self.client, self._lock_key, self._lock_timeout)

    def _run(self, fn, *args) -> Any:
        return self.client.run_sync_(fn, *args)

    @override
    def get(self, ref: TableReference, row_id: int) -> np.record | None:
        """获取指定表的指定行数据"""
        row = self._run(SQLiteStore.hgetall, self.client.row_key(ref, row_id))
        if not row:
            return None
        return self.client.row_decode_(ref.comp_cls, row, RowFormat.STRUCT)

    @override
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
        members = self._run(
            SQLiteStore.zrange_bylex, idx_key, b_left, b_right, False, 0, limit
        )
        return [member_row_id_(vk) for vk in members]

    @override
    def get_all_row_id(self, ref: TableReference) -> list[int]:
        return self._run(SQLiteStore.row_ids, self.client.cluster_prefix(ref))

    @override
    def delete_row(self, ref: TableReference, row_id: int):
        """删除指定表的指定行数据（只删行，同 Redis 不动索引）"""
        self._run(SQLiteStore.delete_row_txn, self.client.row_key(ref, row_id))

    @override
    def upsert_row(self, ref: TableReference, row_data: np.record):
        """更新指定表的一行数据，如果不存在就插入（同 Redis：先删再整行写）"""
        key = self.client.row_key(ref, int(row_data.id))
        mapping = ref.comp_cls.struct_to_dict(row_data)
        schema = [name for name, _prop in ref.comp_cls.properties_]

        def _upsert(store: SQLiteStore):
            with store.write_txn():
                store.delete_row(key)
                store.hset(key, mapping, schema)

        self._run(_upsert)

    @override
    def read_meta(
        self, instance_name: str, comp: type[BaseComponent] | str
    ) -> TableMaintenance.TableMeta | None:
        """读取组件表的meta信息"""
        meta = self._run(SQLiteStore.meta_get, instance_name, self.comp_name_of_(comp))
        if meta is None:
            return None
        json_, version, cluster_id = meta
        return TableMaintenance.TableMeta(
            version=version, json=json_, cluster_id=cluster_id, extra={}
        )

    @override
    def get_lock(self) -> AbstractContextManager:
        """获得一个可以锁整个数据库的with锁"""
        return self.lock

    @staticmethod
    def _meta_of(table_ref: TableReference) -> tuple[str, str, str, str, int]:
        json_ = table_ref.comp_cls.json_
        return (
            table_ref.instance_name,
            table_ref.comp_name,
            json_,
            schema_version_(json_),
            table_ref.cluster_id,
        )

    @override
    def do_create_table_(self, table_ref: TableReference) -> TableMaintenance.TableMeta:
        """创建组件表：写 meta，并建好空的行表（开服后在 DB 工具里就能看到）"""
        table = self.client.cluster_prefix(table_ref)
        schema = [name for name, _prop in table_ref.comp_cls.properties_]
        meta = self._meta_of(table_ref)

        def _create(store: SQLiteStore):
            with store.write_txn():
                assert store.meta_get(meta[0], meta[1]) is None
                store.meta_set(*meta)
                store.ensure_row_table(table, schema)

        self._run(_create)
        meta_recon = self.read_meta(table_ref.instance_name, table_ref.comp_cls)
        assert meta_recon
        return meta_recon

    # 无需drop_table, 此类操作适合人工删除

    @override
    def do_rename_table_(self, from_: TableReference, to_: TableReference):
        """
        重命名组件表：行表改名、索引 key 改前缀、meta 换成 to_ 的定义。版本号照搬（同 Redis 后端）：
        改名不动数据，索引还是原来的格式。

        提交序号列的计数器只在组件名不变（换簇）时跟着搬，规则同 Redis 后端：默认迁移脚本先把
        原表改名成临时表、拷完再删掉临时表，计数器跟着搬就会被一起删掉；留在原名下，新表接着用
        """
        from_table = self.client.cluster_prefix(from_)
        to_table = self.client.cluster_prefix(to_)
        from_meta = (from_.instance_name, from_.comp_name)
        to_instance, to_comp, to_json, _version, to_cluster = self._meta_of(to_)
        move_counters = from_.comp_name == to_.comp_name

        def _rename(store: SQLiteStore) -> int:
            with store.write_txn():
                old = store.meta_get(*from_meta)
                assert old is not None
                count = store.count_rows(from_table) + len(
                    store.zkeys_with_prefix(from_table + ":")
                )
                store.rename_row_table(from_table, to_table)
                store.zrename_prefix(from_table + ":", to_table + ":")
                if move_counters:
                    seq_head = ":seq:"
                    count += len(store.kv_items_with_prefix(from_table + seq_head))
                    store.kv_rename_prefix(from_table + seq_head, to_table + seq_head)
                store.meta_delete(*from_meta)
                store.meta_set(to_instance, to_comp, to_json, old[1], to_cluster)
                return count

        count = self._run(_rename)
        logger.warning(
            _(
                "  ✔️ [💾SQLite][{comp_name}组件] rename完成，共改名{count}个键值。"
            ).format(comp_name=to_.comp_name, count=count)
        )

    @override
    def do_update_meta_(self, table_ref: TableReference) -> None:
        """把组件表的meta改写成table_ref的定义，不动表数据"""
        meta = self._meta_of(table_ref)

        def _update(store: SQLiteStore):
            with store.write_txn():
                store.meta_set(*meta)

        self._run(_update)

    @override
    def do_drop_table_(self, table_ref: TableReference) -> int:
        """
        清空组件表的全部数据（所有簇的行表、索引、提交序号列的计数器）和 meta。返回删掉的键数：
        行数 + 索引 key 数 + 计数器数 + meta，对应 Redis 删掉的 key 数。名字以本组件名开头的副本
        组件不碰
        """
        prefix = self.client.all_clusters_prefix(table_ref)
        instance, comp = table_ref.instance_name, table_ref.comp_name

        def _drop(store: SQLiteStore) -> int:
            with store.write_txn():
                count = 0
                for table in store.row_tables_with_prefix(prefix):
                    count += store.count_rows(table)
                    store.drop_row_table(table)
                count += store.zdelete_prefix(prefix)
                count += store.kv_delete_prefix(prefix)
                count += store.meta_delete(instance, comp)
                return count

        return self._run(_drop)

    @override
    def do_rebuild_index_(self, table_ref: TableReference) -> int:
        """
        按行数据重建组件表的索引，返回行数。member 与 zset key 的算法与 Redis 后端、与 commit 写
        索引时相同（`rebuild_entry_`）；先全部算好、unique 检查通过后才在一个事务里整体替换：中途
        失败（unique 冲突）旧索引原样保留。复合索引每组一个 zset，替换时这个索引名下的旧 key 全部
        删掉（单列的 key 与复合的组 key），不留另一种格式或空了的组。表里一行都没有时，索引里
        剩下的都是残留，直接删掉。
        """
        comp_cls = table_ref.comp_cls
        table = self.client.cluster_prefix(table_ref)
        row_count = len(self._run(SQLiteStore.row_ids, table))
        # {索引 key: {zset key: members}}
        new_indexes: dict[str, dict[str, list[bytes]]] = {}
        # 只拿来按 dtype 转值，给定 id 不发号：hetu upgrade 进程没初始化 SnowflakeID
        struct = comp_cls.new_row(id_=0)
        for idx_name in comp_cls.indexes_:
            idx_key = self.client.index_key(table_ref, idx_name)
            by_key: dict[str, list[bytes]] = {}
            if row_count:
                # 复合索引要读分组列和本字段
                columns = (*comp_cls.index_groups_[idx_name], idx_name)
                is_unique = idx_name in comp_cls.uniques_
                seen: set[bytes] = set()
                for row_id, raws in self._run(
                    SQLiteStore.row_field_values, table, columns
                ):
                    for col, raw in zip(columns, raws):
                        if raw is None:
                            raise RuntimeError(
                                _(
                                    "组件{comp_name}的行 id={row_id} 缺字段`{idx_name}`，"
                                    "无法重建索引"
                                ).format(
                                    comp_name=table_ref.comp_name,
                                    row_id=row_id,
                                    idx_name=col,
                                )
                            )
                    zset_key, member = self.client.rebuild_entry_(
                        comp_cls,
                        idx_key,
                        idx_name,
                        struct,
                        dict(zip(columns, cast(tuple[bytes, ...], raws))),
                        row_id,
                    )
                    if is_unique:
                        # 按值段（复合索引带组前缀）查重，同组同值才算冲突
                        value = member_value_(member)
                        if value in seen:
                            raise RuntimeError(
                                _(
                                    "组件{comp_name}的unique索引`{idx_name}`（列 {columns}）"
                                    "在重建时发现违反unique约束，可能是迁移时缩短了值类型、"
                                    "新增了Unique标记、或去掉了groupby（组内唯一变成了全局"
                                    "唯一）导致。"
                                ).format(
                                    comp_name=table_ref.comp_name,
                                    idx_name=idx_name,
                                    columns=columns,
                                )
                            )
                        seen.add(value)
                    by_key.setdefault(zset_key, []).append(member)
            new_indexes[idx_key] = by_key

        def _replace(store: SQLiteStore):
            with store.write_txn():
                for idx_key, by_key in new_indexes.items():
                    # 前缀带 ":"：不会删到名字以本索引名开头的别的索引（level 与 level2）
                    store.zreplace(idx_key, ())
                    store.zdelete_prefix(idx_key + ":")
                    for zset_key, members in by_key.items():
                        store.zreplace(zset_key, members)

        self._run(_replace)
        return row_count
