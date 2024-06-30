from component import BaseComponent
from ..system import SystemClusters
from backend.base import Backend, ComponentTable
from ..common import Singleton


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

    def get_table(self, component_cls: type[BaseComponent]):
        return self.tables.get(component_cls)

