"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import hashlib
import logging
import time
import uuid
from contextlib import AbstractContextManager
from typing import TYPE_CHECKING, Any, final, override

import numpy as np

from ....i18n import _
from ...component import BaseComponent
from ..base import RowFormat, TableMaintenance
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
    ) -> list[int]:
        """按索引范围查询指定表的数据"""
        comp_cls = ref.comp_cls
        assert index_name in comp_cls.indexes_
        b_left, b_right = self.client.range_normalize_(
            comp_cls.dtype_map_[index_name], left, right, False
        )
        members = self._run(
            SQLiteStore.zrange_bylex,
            self.client.index_key(ref, index_name),
            b_left,
            b_right,
            False,
            0,
            limit,
        )
        return [int(vk.rsplit(b"\x00", 1)[-1]) for vk in members]

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
            hashlib.md5(json_.encode("utf-8")).hexdigest(),
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
        """重命名组件表：行表改名、索引 key 改前缀、meta 换成 to_ 的定义"""
        from_table = self.client.cluster_prefix(from_)
        to_table = self.client.cluster_prefix(to_)
        from_meta = (from_.instance_name, from_.comp_name)
        to_meta = self._meta_of(to_)

        def _rename(store: SQLiteStore) -> int:
            with store.write_txn():
                count = store.count_rows(from_table) + len(
                    store.zkeys_with_prefix(from_table + ":")
                )
                store.rename_row_table(from_table, to_table)
                store.zrename_prefix(from_table + ":", to_table + ":")
                store.meta_delete(*from_meta)
                store.meta_set(*to_meta)
                return count

        count = self._run(_rename)
        logger.warning(
            f"  ✔️ [💾SQLite][{to_.comp_name}组件] rename完成，共改名{count}个键值。"
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
        清空组件表的全部数据（所有簇的行表、索引）和 meta。返回删掉的键数：行数 + 索引 key 数 +
        meta，对应 Redis 删掉的 key 数。
        """
        prefix = self.client.table_prefix(table_ref) + ":"
        instance, comp = table_ref.instance_name, table_ref.comp_name

        def _drop(store: SQLiteStore) -> int:
            with store.write_txn():
                count = 0
                for table in store.row_tables_with_prefix(prefix):
                    count += store.count_rows(table)
                    store.drop_row_table(table)
                count += store.zdelete_prefix(prefix)
                count += store.meta_delete(instance, comp)
                return count

        return self._run(_drop)

    @override
    def do_rebuild_index_(self, table_ref: TableReference) -> int:
        """
        按行数据重建组件表的索引，返回行数。member 的算法与 Redis 后端、与 commit 写索引时相同
        （`rebuild_member_`）；先全部算好、unique 检查通过后才在一个事务里整体替换：中途失败
        （unique 冲突）旧索引原样保留。表里一行都没有时，索引里剩下的都是残留，直接删掉。
        """
        comp_cls = table_ref.comp_cls
        table = self.client.cluster_prefix(table_ref)
        row_count = len(self._run(SQLiteStore.row_ids, table))
        new_indexes: dict[str, list[bytes]] = {}
        # 只拿来按 dtype 转值，给定 id 不发号：hetu upgrade 进程没初始化 SnowflakeID
        struct = comp_cls.new_row(id_=0)
        for idx_name in comp_cls.indexes_:
            members: list[bytes] = []
            if row_count:
                is_unique = idx_name in comp_cls.uniques_
                is_bytes = idx_name in comp_cls.bytes_fields_
                seen: set[bytes | None] = set()
                for row_id, value in self._run(
                    SQLiteStore.row_field_values, table, idx_name
                ):
                    if is_unique:
                        if value in seen:
                            raise RuntimeError(
                                f"组件{table_ref.comp_name}的unique索引`{idx_name}`在重建时"
                                f"发现违反unique约束，可能是迁移时缩短了值类型、或新增了"
                                f"Unique标记导致。"
                            )
                        seen.add(value)
                    if value is None:
                        raise RuntimeError(
                            f"组件{table_ref.comp_name}的行 id={row_id} 缺字段`{idx_name}`，"
                            f"无法重建索引"
                        )
                    members.append(
                        self.client.rebuild_member_(
                            struct, idx_name, value, is_bytes, str(row_id).encode()
                        )
                    )
            new_indexes[self.client.index_key(table_ref, idx_name)] = members

        def _replace(store: SQLiteStore):
            with store.write_txn():
                for idx_key, idx_members in new_indexes.items():
                    store.zreplace(idx_key, idx_members)

        self._run(_replace)
        return row_count
