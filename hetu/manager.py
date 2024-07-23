"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""
from typing import ItemsView

from hetu.data.backend.base import Backend, ComponentTable
from hetu.data.component import BaseComponent
from hetu.system import SystemClusters


class ComponentTableManager:
    """
    ComponentTable管理类，负责对每个ComponentTable的初始化和获取。
    此类只能在SystemCluster.build_clusters()后初始化
    """

    def __init__(
            self,
            namespace: str,
            instance_name: str,
            backends: dict[str, Backend],
            table_constructors: dict[str, type[ComponentTable]]
    ):
        self.tables = {}
        self.tables_by_name = {}

        clusters = SystemClusters().get_clusters(namespace)
        for cluster in clusters:
            for comp in cluster.components:
                backend = backends.get(comp.backend_)
                table_constructor = table_constructors.get(comp.backend_)
                if backend is None or table_constructor is None:
                    raise ValueError(f"Backend {comp.backend_} not found")
                table = table_constructor(comp, instance_name, cluster.id, backend)
                self.tables[comp] = table
                self.tables_by_name[comp.component_name_] = table

    def create_or_migrate_all(self):
        for comp, tbl in self.tables.items():
            if comp.persist_:
                tbl.create_or_migrate()

    def flush_volatile(self):
        for comp, tbl in self.tables.items():
            if not comp.persist_:
                tbl.flush()

    def get_table(self, component_cls: type[BaseComponent] | str) -> ComponentTable | None:
        if type(component_cls) is str:
            return self.tables_by_name.get(component_cls)
        else:
            return self.tables.get(component_cls)

    def items(self) -> ItemsView[type[BaseComponent], ComponentTable]:
        return self.tables.items()
