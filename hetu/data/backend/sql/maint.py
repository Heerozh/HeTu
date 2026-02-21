"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import hashlib
import logging
import threading
from contextlib import AbstractContextManager
from typing import TYPE_CHECKING, Any, cast, final, override

import numpy as np
import sqlalchemy as sa
from sqlalchemy import exc as sa_exc

from ...component import BaseComponent
from ..base import RowFormat, TableMaintenance
from ..table import TableReference

if TYPE_CHECKING:
    from .client import SQLBackendClient

logger = logging.getLogger("HeTu.root")


@final
class SQLTableMaintenance(TableMaintenance):
    """
    SQL后端的组件表维护实现。
    """

    client: SQLBackendClient

    def __init__(self, master: SQLBackendClient):
        super().__init__(master)
        self._lock = threading.RLock()
        self.client._ensure_support_tables_sync()

    def _table_exists(self, conn: sa.Connection, table_name: str) -> bool:
        return sa.inspect(conn).has_table(table_name)

    def _safe_get_table(self, ref: TableReference):
        return self.client.component_table(ref)

    def _safe_get_meta(self):
        return self.client.meta_table()

    def get(self, ref: TableReference, row_id: int) -> np.record | None:
        table = self._safe_get_table(ref)
        with self.client.io.connect() as conn:
            if not self._table_exists(conn, table.name):
                return None
            row = (
                conn.execute(sa.select(table).where(table.c.id == int(row_id)).limit(1))
                .mappings()
                .first()
            )
        if row is None:
            return None
        return cast(
            np.record,
            self.client.row_decode_(ref.comp_cls, dict(row), RowFormat.STRUCT),
        )

    def range(
        self,
        ref: TableReference,
        index_name: str,
        left: Any,
        right: Any = None,
        limit: int = 10,
    ) -> list[int]:
        comp_cls = ref.comp_cls
        assert index_name in comp_cls.indexes_, (
            f"Component `{comp_cls.name_}` 没有索引 `{index_name}`"
        )

        table = self._safe_get_table(ref)
        dtype = comp_cls.dtype_map_[index_name]
        left, right, li, ri = self.client.range_normalize_(dtype, left, right, False)
        col = table.c[index_name]
        cond_left = col >= left if li else col > left
        cond_right = col <= right if ri else col < right
        stmt = sa.select(table.c.id).where(cond_left, cond_right).order_by(
            col.asc(), table.c.id.asc()
        )
        if limit >= 0:
            stmt = stmt.limit(limit)

        with self.client.io.connect() as conn:
            if not self._table_exists(conn, table.name):
                return []
            rows = conn.execute(stmt).scalars().all()
        return [int(x) for x in rows]

    @override
    def get_all_row_id(self, ref: TableReference) -> list[int]:
        table = self._safe_get_table(ref)
        with self.client.io.connect() as conn:
            if not self._table_exists(conn, table.name):
                return []
            rows = conn.execute(sa.select(table.c.id)).scalars().all()
        return [int(x) for x in rows]

    @override
    def delete_row(self, ref: TableReference, row_id: int):
        table = self._safe_get_table(ref)
        with self.client.io.begin() as conn:
            if not self._table_exists(conn, table.name):
                return
            conn.execute(sa.delete(table).where(table.c.id == int(row_id)))

    @override
    def upsert_row(self, ref: TableReference, row_data: np.record):
        table = self._safe_get_table(ref)
        row_dict = ref.comp_cls.struct_to_dict(row_data)

        with self.client.io.begin() as conn:
            if not self._table_exists(conn, table.name):
                table.create(conn)
            stmt = sa.update(table).where(table.c.id == int(row_data.id)).values(**row_dict)
            updated = conn.execute(stmt)
            if updated.rowcount == 0:
                try:
                    conn.execute(sa.insert(table).values(**row_dict))
                except sa_exc.IntegrityError as exc:
                    raise RuntimeError(
                        f"组件{ref.comp_name}在迁移写入时触发unique冲突：{exc}"
                    ) from exc

    @override
    def read_meta(
        self, instance_name: str, comp_cls: type[BaseComponent]
    ) -> TableMaintenance.TableMeta | None:
        meta = self._safe_get_meta()
        with self.client.io.connect() as conn:
            row = (
                conn.execute(
                    sa.select(meta).where(
                        meta.c.instance_name == instance_name,
                        meta.c.comp_name == comp_cls.name_,
                    )
                )
                .mappings()
                .first()
            )
        if row is None:
            return None
        return TableMaintenance.TableMeta(
            version=str(row["version"]),
            json=str(row["json"]),
            cluster_id=int(row["cluster_id"]),
            extra={},
        )

    @override
    def get_lock(self) -> AbstractContextManager:
        return self._lock

    @override
    def do_create_table_(self, table_ref: TableReference) -> TableMaintenance.TableMeta:
        table = self._safe_get_table(table_ref)
        meta = self._safe_get_meta()

        meta_row = {
            "instance_name": table_ref.instance_name,
            "comp_name": table_ref.comp_name,
            "json": table_ref.comp_cls.json_,
            "version": hashlib.md5(
                table_ref.comp_cls.json_.encode("utf-8")
            ).hexdigest(),
            "cluster_id": table_ref.cluster_id,
            "extra_json": "{}",
        }

        with self.client.io.begin() as conn:
            table.create(conn, checkfirst=True)
            conn.execute(sa.delete(meta).where(
                meta.c.instance_name == table_ref.instance_name,
                meta.c.comp_name == table_ref.comp_name,
            ))
            conn.execute(sa.insert(meta).values(**meta_row))

        meta_recon = self.read_meta(table_ref.instance_name, table_ref.comp_cls)
        assert meta_recon
        return meta_recon

    @override
    def do_rename_table_(self, from_: TableReference, to_: TableReference):
        from_table = self._safe_get_table(from_)
        to_table = self._safe_get_table(to_)
        meta = self._safe_get_meta()

        with self.client.io.begin() as conn:
            if not self._table_exists(conn, from_table.name):
                return

            if self._table_exists(conn, to_table.name):
                to_table.drop(conn)
            to_table.create(conn)

            rows = conn.execute(sa.select(from_table)).mappings().all()
            if rows:
                conn.execute(sa.insert(to_table), [dict(row) for row in rows])

            from_table.drop(conn)

            conn.execute(
                sa.delete(meta).where(
                    meta.c.instance_name == from_.instance_name,
                    meta.c.comp_name.in_([from_.comp_name, to_.comp_name]),
                )
            )
            conn.execute(
                sa.insert(meta).values(
                    instance_name=to_.instance_name,
                    comp_name=to_.comp_name,
                    json=to_.comp_cls.json_,
                    version=hashlib.md5(to_.comp_cls.json_.encode("utf-8")).hexdigest(),
                    cluster_id=to_.cluster_id,
                    extra_json="{}",
                )
            )

    @override
    def do_drop_table_(self, table_ref: TableReference) -> int:
        table = self._safe_get_table(table_ref)
        meta = self._safe_get_meta()
        count = 0
        with self.client.io.begin() as conn:
            if self._table_exists(conn, table.name):
                count = int(
                    conn.execute(sa.select(sa.func.count()).select_from(table)).scalar()
                    or 0
                )
                table.drop(conn)
            conn.execute(sa.delete(meta).where(
                meta.c.instance_name == table_ref.instance_name,
                meta.c.comp_name == table_ref.comp_name,
            ))
        return count

    @override
    def do_rebuild_index_(self, table_ref: TableReference) -> int:
        # SQL索引由数据库自动维护。这里保留unique一致性检查，以兼容迁移流程。
        table = self._safe_get_table(table_ref)
        with self.client.io.connect() as conn:
            if not self._table_exists(conn, table.name):
                return 0

            row_count = int(
                conn.execute(sa.select(sa.func.count()).select_from(table)).scalar() or 0
            )

            for unique_name in table_ref.comp_cls.uniques_:
                col = table.c[unique_name]
                duplicated = conn.execute(
                    sa.select(col)
                    .group_by(col)
                    .having(sa.func.count() > 1)
                    .limit(1)
                ).first()
                if duplicated is not None:
                    raise RuntimeError(
                        f"组件{table_ref.comp_name}的unique索引`{unique_name}`"
                        "在重建时发现违反unique约束，"
                        "可能是迁移时缩短了值类型、或新增了Unique标记导致。"
                    )
        return row_count
