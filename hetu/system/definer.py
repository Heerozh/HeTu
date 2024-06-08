"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""
from inspect import signature
from typing import Type
from dataclasses import dataclass
import asyncio
import warnings
import traceback
from ..common import Singleton
from ..data import BaseComponent, Permission


@dataclass
class SystemDefine:
    func: callable
    components: tuple[Type[BaseComponent]]     # 引用的Components
    full_components: tuple[Type[BaseComponent]] | None   # 完整的引用Components，包括继承自父System的
    inherits: tuple[str]
    full_inherits: tuple[str] | None
    permission: Permission
    arg_count: int         # 全部参数个数（含默认参数）
    defaults_count: int    # 默认参数个数
    cluster_id: int


class SystemClusters(metaclass=Singleton):
    """
    储存所有System，并分类成簇。可以通过它查询System的定义信息，和所属簇id(每次服务器启动时按簇大小排序分配），
    每个namespace下的簇id从0重新分配。
    簇是按照components的交集来划分的，表示这些System互相之间可能有事务冲突，簇和簇之间绝不会冲突。目前并无具体作用，
    只用来观察ECS结构拆的是否足够细。

    此类只负责查询，调度器通过此类查询System信息。
    """
    @dataclass
    class Cluster:
        id: int
        components: set[Type[BaseComponent]]
        namespace: str
        systems: set[str]

    def __init__(self):
        self._system_map = {}  # type: dict[str, dict[str, SystemDefine]]
        self._clusters = {}    # type: dict[str, list[SystemClusters.Cluster]]

    def get_system(self, namespace: str, system_name: str) -> SystemDefine:
        return self._system_map[namespace][system_name]

    def get_systems(self, cluster: Cluster) -> dict[str, SystemDefine]:
        return {name: self.get_system(cluster.namespace, name) for name in cluster.systems}

    def get_clusters(self, namespace: str):
        return self._clusters[namespace]

    def get_cluster(self, namespace: str, cluster_id: int):
        return self._clusters[namespace][cluster_id]

    def build_clusters(self):
        assert self._clusters == {}, "簇已经生成过了"
        # 按Component交集生成簇，只有启动时运行，不用考虑性能

        def merge_cluster(clusters_: list[SystemClusters.Cluster]):
            """合并2个冲突的簇"""
            for x in range(len(clusters_)):
                for y in range(x + 1, len(clusters_)):
                    if (clusters_[x].namespace == clusters_[y].namespace and
                            clusters_[x].components.intersection(clusters_[y].components)):
                        clusters_[x].components.update(clusters_[y].components)
                        clusters_[x].systems.update(clusters_[y].systems)
                        del clusters_[y]
                        return True
            return False

        def inherit_components(namespace_, inherits, req: set, inh: set):
            for base_system in inherits:
                base_def = self._system_map[namespace_][base_system]
                req.update(base_def.components)
                inh.update(base_def.inherits)
                inherit_components(namespace_, base_def.inherits, req, inh)

        for namespace in self._system_map:
            clusters = []
            # 首先把所有系统变成独立的簇/并生成完整的请求表
            for sys_name, sys_def in self._system_map[namespace].items():
                sys_def.full_components = set(sys_def.components)
                sys_def.full_inherits = set(sys_def.inherits)
                inherit_components(namespace, sys_def.inherits, sys_def.full_components,
                                   sys_def.full_inherits)
                clusters.append(SystemClusters.Cluster(
                    -1, sys_def.full_components.copy(),
                    namespace, {sys_name}))

            self._clusters[namespace] = clusters

            # 然后反复调用merge_cluster直到不再缩小
            while merge_cluster(clusters):
                pass

            # 先按system数排序，然后按第一个system的alphabet排序，让簇id尽量不变
            sorted(clusters, key=lambda x: f"{len(x.systems):02}_{next(iter(x.systems))}")
            for i in range(len(clusters)):
                clusters[i].id = i

            # 把簇的id重新分配个系统定义
            for cluster in clusters:
                for sys_name in cluster.systems:
                    self._system_map[cluster.namespace][sys_name].cluster_id = cluster.id

    def add(self, namespace, func, components, force, permission, inherits):
        sub_map = self._system_map.setdefault(namespace, dict())

        if not force:
            assert func.__name__ not in sub_map, "System重复定义"
        if components is None:
            components = tuple()

        # 获取函数参数个数，存下来，要求客户端调用严格匹配
        arg_count = func.__code__.co_argcount
        defaults_count = len(func.__defaults__) if func.__defaults__ else 0

        sub_map[func.__name__] = SystemDefine(
            func=func, components=components, inherits=inherits,
            arg_count=arg_count, defaults_count=defaults_count, cluster_id=-1,
            permission=permission, full_components=None, full_inherits=None)


def define_system(components: tuple[Type[BaseComponent], ...] = None,
                  namespace: str = "default", force: bool = False, permission=Permission.USER,
                  inherits: tuple[str] = tuple()):
    """
    定义系统
    :param namespace: 系统命名空间
    :param components: 引用读/写的表
    :param force: 遇到重复定义是否强制覆盖前一个, 单元测试用
    :param permission: 系统权限
    :param inherits: 继承其他系统, 让系统可以在内部调用它，并不破坏事务一致性。但是簇会和其他系统绑定。

    格式：
    @define_system(
        namespace="ssw",
        components=(Position, Hp),
    )
    def system_dash(ctx, entity_self, entity_target, vec):
        pos_self = ctx[Position].select(entity_self)
        pos_self.x += vec.x
        ctx[Position].update(entity_self, pos_self)
        items = ctx[Inventory].query("owner", entity_self)
        ...

    """
    def warp(func):
        # warp只是在系统里记录下有这么个东西，实际不改变function

        # 严格要求第一个参数命名
        func_args = signature(func).parameters
        func_arg_names = list(func_args.keys())[:1]
        assert func_arg_names == ["ctx"], \
            (f"System参数名定义错误，第一个参数必须为：ctx。"
             f"你的：{func_arg_names}")

        assert permission != permission.OWNER, "System的权限不支持OWNER"

        if components is not None and len(components) > 0:
            if not asyncio.iscoroutinefunction(func):
                formatted_lines = traceback.format_stack()
                warnings.warn(f"System {func.__name__} 不是异步函数，数据库请求可能会阻塞整个Worker: \n" +
                              formatted_lines[-2])

        SystemClusters().add(namespace, func, components, force,
                             permission, inherits)

        # 返回假的func，因为不允许直接调用。
        def warp_system_call(*_, **__):
            raise RuntimeError("系统函数不允许直接调用")
            # return call_system(namespace, func.__name__, *args, **kwargs)
        return warp_system_call

    return warp