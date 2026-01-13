"""
Component Table管理类，通过System定义的Component来管理他们所属的数据库位置。

@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

from typing import ItemsView, TYPE_CHECKING
from .data.backend import Table

from .system import SystemClusters

if TYPE_CHECKING:
    from hetu.data.backend import Backend
    from hetu.data.component import BaseComponent


class ComponentTableManager:
    """
    Component Table管理类，负责储存每个Component的Backend和TableReference。
    此类只能在SystemCluster.build_clusters()后初始化。
    """

    @property
    def namespace(self) -> str:
        return self._namespace

    @property
    def backends(self) -> dict[str, Backend]:
        return self._backends

    def __init__(
        self,
        namespace: str,
        instance_name: str,
        backends: dict[str, Backend],
    ):
        self._tables: dict[type[BaseComponent], Table] = {}
        self._tables_by_name: dict[str, Table] = {}
        self._namespace: str = namespace
        self._backends: dict[str, Backend] = backends

        clusters = SystemClusters().get_clusters(namespace)
        assert clusters
        for cluster in clusters:
            for comp in cluster.components:
                backend = backends.get(comp.backend_)
                if backend is None:
                    raise ValueError(f"Backend {comp.backend_} not found")
                table = Table(comp, instance_name, cluster.id, backend)
                self._tables[comp] = table
                self._tables_by_name[comp.component_name_] = table
                comp.hosted_ = table

    def create_or_migrate_all(self, force=False) -> bool:
        """
        创建或安全的迁移所有表，如果schema有无法安全迁移的变更，则raise异常。
        此时要么写迁移脚本，要么用cli强制迁移。
        """
        for _, tbl in self._tables.items():
            maint = tbl.backend.get_table_maintenance()
            tbl_status, old_meta = maint.check_table(tbl)
            match tbl_status:
                case "not_exists":
                    maint.create_table(tbl)
                case "schema_mismatch":
                    if not maint.migration_schema(tbl, old_meta, force=force):
                        return False
                case "cluster_mismatch":
                    # 非持久化的Component也需要cluster迁移，不然数据就永远的留在了数据库中
                    maint.migration_cluster_id(tbl, old_meta)
        return True

    def check_and_create_new_tables(self) -> bool:
        """
        只在数据库创建所有新表，已存在的表不会调用。
        返回true表示所有表的状态正常，false表示有表需要迁移。
        """
        ret = True
        for _, tbl in self._tables.items():
            maint = tbl.backend.get_table_maintenance()
            tbl_status, old_meta = maint.check_table(tbl)
            match tbl_status:
                case "not_exists":
                    maint.create_table(tbl)
                case "schema_mismatch":
                    ret = False
                case "cluster_mismatch":
                    ret = False
        return ret

    def flush_volatile(self):
        """清空所有非持久化数据"""
        for comp, tbl in self._tables.items():
            if comp.volatile_:
                maint = tbl.backend.get_table_maintenance()
                maint.flush(tbl)

    def _flush_all(self, force=False):
        """测试用，清空所有数据"""
        for tbl in self._tables.values():
            maint = tbl.backend.get_table_maintenance()
            maint.flush(tbl, force)

    def get_table(self, component_cls: type[BaseComponent] | str) -> Table | None:
        if isinstance(component_cls, str):
            return self._tables_by_name.get(component_cls)
        else:
            return self._tables.get(component_cls)

    def items(self) -> ItemsView[type[BaseComponent], Table]:
        return self._tables.items()
