"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

from typing import ItemsView

from hetu.data.backend import Backend, TableReference
from hetu.data.component import BaseComponent
from hetu.system import SystemClusters


# todo rename ComponentBackendManager
class ComponentTableManager:
    """
    ComponentTable管理类，负责对每个ComponentTable的初始化和获取。
    此类只能在SystemCluster.build_clusters()后初始化
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
        table_constructors: dict[str, type[RawComponentTable]],
    ):
        self._tables = {}
        self._tables_by_name = {}
        self._namespace = namespace
        self._backends = backends

        clusters = SystemClusters().get_clusters(namespace)
        for cluster in clusters:
            for comp in cluster.components:
                backend = backends.get(comp.backend_)
                table_constructor = table_constructors.get(comp.backend_)
                if backend is None or table_constructor is None:
                    raise ValueError(f"Backend {comp.backend_} not found")
                table = table_constructor(comp, instance_name, cluster.id, backend)
                self._tables[comp] = table
                self._tables_by_name[comp.component_name_] = table

    def create_or_migrate_all(self):
        for comp, tbl in self._tables.items():
            # 非持久化的Component需要cluster迁移，不然数据就永远的留在了数据库中
            tbl.create_or_migrate(cluster_only=comp.volatile_)

    def flush_volatile(self):
        """清空所有非持久化数据"""
        for comp, tbl in self._tables.items():
            if comp.volatile_:
                tbl.flush()

    def _flush_all(self, force=False):
        """测试用，清空所有数据"""
        for comp, tbl in self._tables.items():
            tbl.flush(force)

    def get_table(
        self, component_cls: type[BaseComponent] | str
    ) -> RawComponentTable | None:
        if type(component_cls) is str:
            return self._tables_by_name.get(component_cls)
        else:
            return self._tables.get(component_cls)

    def items(self) -> ItemsView[type[BaseComponent], RawComponentTable]:
        return self._tables.items()
