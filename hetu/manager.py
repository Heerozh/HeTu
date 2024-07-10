"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""
from hetu.data.component import BaseComponent
from hetu.data.backend.base import Backend, ComponentTable
from hetu.common import Singleton
from hetu.system import SystemClusters
from typing import ItemsView


class ComponentTableManager(metaclass=Singleton):
    """
    ComponentTable管理类，负责对每个ComponentTable的初始化和获取。
    此类是单件，每个进程只有一个实例。
    """
    def __init__(self):
        self.tables = {}
        self.subscriptions = {}

    def build(
            self,
            namespace: str,
            instance_name: str,
            backends: dict[str, Backend],
            table_classes: dict[str, type[ComponentTable]]
    ):
        """初始化所有ComponentTable的实例，此方法只能在SystemCluster.build_clusters()后调用。"""
        clusters = SystemClusters().get_clusters(namespace)
        for cluster in clusters:
            for comp in cluster.components:
                backend = backends.get(comp.backend_)
                table_cls = table_classes.get(comp.backend_)
                if backend is None or table_cls is None:
                    raise ValueError(f"Backend {comp.backend_} not found")
                table = table_cls(comp, instance_name, cluster.id, backend)
                self.tables[comp] = table
                # self.subscriptions[comp] = backend.subscribe(comp)

    def get_table(self, component_cls: type[BaseComponent]) -> ComponentTable | None:
        return self.tables.get(component_cls)

    def items(self) -> ItemsView[type[BaseComponent], ComponentTable]:
        return self.tables.items()
