"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import copy
import functools
import inspect
from dataclasses import dataclass
from inspect import signature
from types import FunctionType
from typing import TYPE_CHECKING, Any

from ..endpoint.definer import EndpointDefine, ENDPOINT_NAME_MAX_LEN

from ..common import Singleton
from ..common.permission import Permission
from .lock import SystemLock

if TYPE_CHECKING:
    from ..data import BaseComponent


# 定义一个空System，占位用
def func_alias(ctx):
    pass


@dataclass
class SystemDefine(EndpointDefine):
    components: set[type[BaseComponent]]  # 引用的Components
    full_components: set[type[BaseComponent]]  # 完整的引用，包括继承自父System的
    depends: set[str]
    full_depends: set[str]
    permission: Permission | None
    max_retry: int
    cluster_id: int


class SystemClusters(metaclass=Singleton):
    """
    储存所有System，并分类成共置簇。可以通过它查询System的定义信息，和所属簇id。簇id每次服务器
    启动时按簇大小排序分配，每个namespace下的簇id从0重新分配。
    System之间components有交集的，表示这些System互相之间可能有事务冲突，形成一个共置簇，
    簇和簇之间无交集，绝不会有事务冲突。

    此类只负责储存定义，调度器通过此类查询System信息。
    """

    @dataclass
    class Cluster:
        id: int
        components: set[type[BaseComponent]]
        namespace: str
        systems: set[str]

    @property
    def main_namespace(self):
        return self._main_namespace

    def __init__(self):
        # ==== @define_system(namespace=xxx) 定义的所有System ====
        # 所有system定义表，按namespace分类
        self._system_map: dict[str, dict[str, SystemDefine]] = {}
        # 所有Component所属的cluster id表，只包含被system引用过的Component
        self._component_map: dict[str, dict[type[BaseComponent], int]] = {}
        # 所有namespace下的clusters信息列表
        self._clusters: dict[str, list[SystemClusters.Cluster]] = {}
        # @define_system(namespace="global") 定义的所有System
        self._global_system_map: dict[str, SystemDefine] = {}
        # 方便快速访问主namespace的System定义
        self._main_namespace: str = ""
        self._main_system_map: dict[str, SystemDefine] = {}

    def _clear(self):
        self._clusters = {}
        self._component_map = {}
        self._system_map = {}

    def get_system(
        self, system_name: str, namespace: str | None = None
    ) -> SystemDefine | None:
        if namespace:
            return self._system_map[namespace].get(system_name, None)
        else:
            return self._main_system_map.get(system_name, None)

    def get_systems(self, cluster: Cluster) -> dict[str, SystemDefine]:
        return {
            name: self.get_system(cluster.namespace, name) for name in cluster.systems
        }  # type: ignore

    def get_component_cluster_id(
        self, namespace: str, comp: type[BaseComponent]
    ) -> int | None:
        return self.get_components(namespace).get(comp, None)

    def get_components(
        self, namespace: str | None = None
    ) -> dict[type[BaseComponent], int]:
        """返回所有被System引用过的Component及其所属簇id"""
        if not namespace:
            return self._component_map.get(self._main_namespace, {})
        return self._component_map.get(namespace, {})

    def get_clusters(self, namespace: str):
        return self._clusters.get(namespace, None)

    def get_cluster(self, namespace: str, cluster_id: int):
        return self._clusters[namespace][cluster_id]

    def build_clusters(self, main_namespace: str):
        assert self._clusters == {}, "簇已经生成过了"
        assert main_namespace in self._system_map, (
            f"没有找到namespace={main_namespace}的System定义"
        )
        self._main_namespace = main_namespace

        # 首先添加个空的system定义，引用namespace为core的所有component
        from ..data.component import ComponentDefines

        core_comps = ComponentDefines().get_all("core")
        for comp in core_comps:
            func_alias.__name__ = f"__core_pin_system_{comp.__name__}__"
            SystemClusters().add(
                "global",
                func=func_alias,
                components=(comp,),
                force=True,
                permission=None,
                depends=tuple(),
                max_retry=0,
            )

        # 按Component交集生成簇，只有启动时运行，不用考虑性能
        def merge_cluster(clusters_: list[SystemClusters.Cluster]):
            """合并2个冲突的簇"""
            for x in range(len(clusters_)):
                for y in range(x + 1, len(clusters_)):
                    same_namespace = clusters_[x].namespace == clusters_[y].namespace
                    have_intersection = clusters_[x].components.intersection(
                        clusters_[y].components
                    )
                    if same_namespace and have_intersection:
                        clusters_[x].components.update(clusters_[y].components)
                        clusters_[x].systems.update(clusters_[y].systems)
                        del clusters_[y]
                        return True
            return False

        def inherit_components(namespace_, depends, req: set, inh: set):
            for dep_sys in depends:
                base_name, sys_suffix = (
                    dep_sys.split(":") if ":" in dep_sys else (dep_sys, "")
                )
                if base_name not in self._system_map[namespace_]:
                    raise RuntimeError(
                        f"{sys_name} 的 `depends` 引用的System {base_name} 不存在"
                    )
                base_def = self._system_map[namespace_][base_name]
                if sys_suffix:
                    # 复制Component
                    req.update(
                        [
                            _comp.duplicate(namespace_, sys_suffix)
                            for _comp in base_def.components
                        ]
                    )
                else:
                    req.update(base_def.components)
                inh.update(base_def.depends)
                inherit_components(namespace_, base_def.depends, req, inh)

        for namespace in self._system_map:
            # 把global的System迁移到当前namespace
            self._system_map[namespace].update(copy.deepcopy(self._global_system_map))

            clusters = []
            # 首先把所有系统变成独立的簇/并生成完整的请求表
            for sys_name, sys_def in self._system_map[namespace].items():
                sys_def.full_components = set(sys_def.components)
                sys_def.full_depends = set(sys_def.depends)
                inherit_components(
                    namespace,
                    sys_def.depends,
                    sys_def.full_components,
                    sys_def.full_depends,
                )
                # 检查所有System引用的Component和继承的也是同一个backend
                backend_names = [comp.backend_ for comp in sys_def.full_components]
                if len(set(backend_names)) > 1:
                    refs = [
                        f"{comp.component_name_}:{comp.backend_}"
                        for comp in sys_def.full_components
                    ]
                    raise AssertionError(
                        f"System {sys_name} 引用的Component必须都是同一种backend，"
                        f"现在有：{refs}"
                    )
                # 添加到clusters
                clusters.append(
                    SystemClusters.Cluster(
                        -1, sys_def.full_components.copy(), namespace, {sys_name}
                    )
                )

            self._clusters[namespace] = clusters

            # 然后反复调用merge_cluster直到不再缩小
            while merge_cluster(clusters):
                pass

            # 先按system数排序，然后按第一个system的alphabet排序，让簇id尽量不变
            # todo 需要改成取这个组中最hub的Component作为名字来排序，在实际生产环境中测试下是不是比较稳定
            sorted(
                clusters, key=lambda x: f"{len(x.systems):02}_{next(iter(x.systems))}"
            )
            for i in range(len(clusters)):
                clusters[i].id = i

            # 把簇的id重新分配个系统定义
            for cluster in clusters:
                sys_map = self._system_map[cluster.namespace]
                for sys_name in cluster.systems:
                    sys_map[sys_name].cluster_id = cluster.id
                for comp in cluster.components:
                    if cluster.namespace not in self._component_map:
                        self._component_map[cluster.namespace] = {}
                    assert comp not in self._component_map[cluster.namespace]
                    self._component_map[cluster.namespace][comp] = cluster.id

        self._main_system_map = self._system_map[main_namespace]

    def build_endpoints(self):
        """把System定义复制到EndpointDefines中，作为Endpoint使用"""
        from ..endpoint.definer import EndpointDefines
        from .enpoint import create_system_endpoint

        for namespace, sys_map in self._system_map.items():
            for sys_name, sys_def in sys_map.items():
                if sys_def.permission is None:
                    continue
                func = create_system_endpoint(sys_name, sys_def.permission)
                EndpointDefines().add(
                    namespace,
                    func,
                    force=True,
                    arg_count=sys_def.arg_count,
                    defaults_count=sys_def.defaults_count,
                )

    def add(self, namespace, func, components, force, permission, depends, max_retry):
        sub_map = self._system_map.setdefault(namespace, dict())

        if not force:
            assert func.__name__ not in sub_map, "System重复定义：" + func.__name__
        if components is None:
            components = set()

        # 获取函数参数个数，存下来，要求客户端调用严格匹配
        arg_count = func.__code__.co_argcount
        defaults_count = len(func.__defaults__) if func.__defaults__ else 0

        sub_map[func.__name__] = SystemDefine(
            func=func,
            components=components,
            depends=depends,
            max_retry=max_retry,
            arg_count=arg_count,
            defaults_count=defaults_count,
            cluster_id=-1,
            permission=permission,
            full_components=set(),
            full_depends=set(),
        )

        if namespace == "global":
            self._global_system_map[func.__name__] = sub_map[func.__name__]


# todo 改成 @hetu.system()，统一hetu入口
def define_system(
    components: tuple[type[BaseComponent], ...],
    namespace: str = "default",
    force: bool = False,
    permission: Permission | None = Permission.USER,
    retry: int = 9999,
    depends: tuple[str | FunctionType, ...] = tuple(),
    call_lock=False,
):
    """
    定义System，System类似数据库的储存过程，主要用于数据CRUD。
    如果permission不为None，则会自动创建一个Endpoint，让客户端可以直接调用System，
    并自动判断permission是否符合。

    如果需要更多的控制，可以自己写@endpoint()调用System。

    Examples
    --------
    >>> from hetu.data import BaseComponent, define_component, property_field
    >>> # 定义Component
    >>> @define_component
    ... class Stock(BaseComponent):
    ...     owner: int = property_field(0)
    ...     value: int = property_field(0)
    >>> @define_component
    ... class Order(BaseComponent):
    ...     owner: int = property_field(0)
    ...     paid: bool = property_field(False)
    ...     qty: int = property_field(0)
    >>>
    >>> # 定义System
    >>> from hetu.system import define_system, SystemContext
    >>> from hetu.endpoint import ResponseToClient
    >>> @define_system(namespace="example", components=(Stock, Order))
    ... async def pay(ctx: SystemContext, order_id, paid):
    ...     async with ctx[Order].upsert(id=order_id) as order:
    ...        order.paid = paid
    ...     async with ctx[Order].upsert(owner=order.owner) as stock:
    ...        stock.value += order.qty
    ...     # ctx.commit()  # 可以省略，也可以提前提交
    ...     return ResponseToClient(['anything', 'blah blah'])

    Parameters
    ----------
    namespace: str
        是你的项目名，一个网络地址只能启动一个namespace下的System们。
        定义为"global"的namespace可以在所有项目下通用。
    components: list of BaseComponent class
        引用Component，引用的Component可以在`ctx`中进行相关的事务操作，保证数据一致性。
        所有引用的Components会加入共置簇(Colocation Cluster)中，指放在同一个物理数据库中，
        具体见Notes。
    force: bool
        遇到重复定义是否强制覆盖前一个, 单元测试用
    permission: Permission
        设为None时表示客户端SDK不可调用，设置任意权限会创建一个供客户端调用的Endpoint，并检查对应权限。
        - everybody: 任何客户端连接都可以调用执行。（不安全）
        - user: 只有已登录客户端连接可以调用
        - owner: **不可用** OWNER权限这里不可使用，需要自行做安全检查
        - admin: 只有管理员权限客户端连接可以调用
        - rls: **不可用** RLS权限这里不可使用，需要自行做安全检查
    retry: int
        如果System遇到事务冲突，会重复执行直到成功。设为0关闭
    depends: tuple of (str | FunctionType)
        定义要事务依赖的其他System，调用时会在同一个事务会话中执行。
        可传入System函数本身，或字符串，如("system1", system2)。
        可通过`ctx.depend["system1"](ctx, ...)`或直接`system2(ctx, ...)`方式调用定义的函数。
        如果希望使用System副本，可以使用字符串式定义，加':副本名后缀'。具体见Notes。
        注意: 所有depends，将被合并进同一个共置簇中。
    call_lock: bool
        是否对此System启用调用锁，启用后在调用时可以通过传入调用UUID来防止System重复执行。
        如果此System需要给未来调用使用，则此项必须为True。

    Notes
    -----
    **System函数：** ::

        async def pay(ctx: SystemContext, order_id, paid)

    async:
        System必须是异步函数。
    ctx: SystemContext
        System上下文，具体见下述SystemContext部分
    其他参数:
        为hetu client SDK调用时传入的参数。
    System返回值:
        如果调用方是父System（通过`depends`调用），则返回值会原样传给调用方；

        如果调用方是hetu client SDK：
            - 返回值是 hetu.system.ResponseToClient(data)时，则把data发送给调用方sdk。
            - 其他返回值丢弃

    **SystemContext部分：**
        ctx.caller: int
            调用者id，由你在登录System中调用 `elevate` 函数赋值，`None` 或 0 表示未登录用户
        ctx.retry_count: int
            当前因为事务冲突的重试次数，0表示首次执行。
        ctx.repo[Component Class]: SessionRepository
            获取Component事务访问仓库，如 `ctx.repo[Position]`，只能获取定义时在 `components`
            中引用的实例。可以进行数据库操作，并自动包装为事务在System结束后执行。
            具体参考 :py:func:`hetu.data.backend.SessionRepository` 的文档。
        ctx.depend['SystemName']: func
            获取定义时在 `depends` 中传入的System函数。
        await ctx.session_commit():
            提前显式提交当前System事务，如果遇到事务冲突，则此行下面的代码不会执行。
            注意：调用完 `session_commit`，`ctx` 将不再能够获取 `repo`/`depend` 实列
        ctx.session_discard():
            提前显式放弃当前System事务，放弃所有写入操作。
            注意：调用完 `session_discard`，`ctx` 将不再能够获取 `repo`/`depend` 实列

    **Component 共置簇**

    System之间引用的Components有交集的，表示这些System之间可能有事务冲突，这些
    Components将加入同一个共置簇，共置簇中的Components数据会放在同一个物理数据库节点中。

    大量System都引用的Component称为Hub Component，会导致簇过大，从而数据库无法通过
    Cluster分区提升性能，影响未来的扩展性。正常建议通过拆分Component属性来降低簇聚集。

    **System副本继承：**
    代码示例：
    >>> @define_system(namespace="global", components=(Order, ), )
    ... async def remove(ctx: Context, order_id):
    ...     ctx.repo[Order].delete(id=order_id)
    >>>
    >>> @define_system(namespace="example", depends=('remove:ItemOrder', ))
    ... async def remove_item(ctx: Context, order_id):
    ...     return await ctx.depend['remove:ItemOrder'](order_id)

    `depends=('remove:ItemOrder', )`等同创建一个新的`remove` System，但是使用
    `components=(Order.duplicate(namespace, suffix='ItemOrder'), )` 参数。

    正常调用`remove`(不使用System副本)的话，数据是保存在名为 `Order` 的表中。
    在这个例子中，`remove_item` 调用的 `remove` 函数中的数据会储存在名为
    `Order:ItemOrder`的表中，从而实现同一套System逻辑在不同数据集上的操作。

    这么做的意义是不同数据集不会事务冲突，可以拆分成不同的Cluster，从而放到不同的数据库节点上，
    提升性能和扩展性。

    比如create_future_call这个内置System，它引用了FutureCalls队列组件，如果不使用副本继承，那么
    所有用到未来调用的System都将在同一个数据库节点上运行，形成一个大簇，影响扩展性。
    因此通过副本继承此方法，可以拆解出一部分System到不同的数据库节点上运行。
    不用担心，未来调用的后台任务在检查调用队列时，会循环检查所有FutureCalls副本组件队列。

    See Also
    --------
    hetu.system.SystemContext : SystemContext类定义
    hetu.endpoint.endpoint : endpoint装饰器定义Endpoint
    """
    from .context import SystemContext

    def warp(func):
        # warp只是在系统里记录下有这么个东西，实际不改变function

        # 严格要求第一个参数命名
        func_args = signature(func).parameters
        func_arg_names = list(func_args.keys())[:1]
        assert func_arg_names == ["ctx"], (
            f"System参数名定义错误，第一个参数必须为：ctx。你的：{func_arg_names}"
        )

        if permission:
            assert permission not in (permission.OWNER, permission.RLS), (
                "System的权限不支持OWNER/RLS"
            )

        assert len(func.__name__) <= ENDPOINT_NAME_MAX_LEN, (
            f"System函数名过长，最大长度为{ENDPOINT_NAME_MAX_LEN}个字符"
        )

        assert inspect.iscoroutinefunction(func), (
            f"System {func.__name__} 必须是异步函数(`async def ...`)"
        )

        assert len(components) > 0, "System必须引用至少一个Component"

        # 检查components是否都是同一个backend
        backend_names = [comp.backend_ for comp in components]
        assert len(set(backend_names)) <= 1, (
            f"System {func.__name__} 引用的Component必须都是同一种backend"
        )

        _components = components

        # 把call lock的表添加到components中
        if call_lock:
            lock_table = SystemLock.duplicate(namespace, func.__name__)
            lock_table.backend_ = components[0].backend_
            _components = list(components) + [lock_table]

        depend_names = [
            dep if isinstance(dep, str) else dep.__name__ for dep in depends
        ]

        SystemClusters().add(
            namespace, func, _components, force, permission, depend_names, retry
        )

        # 返回包装的func，因为不允许直接调用，需要检查。
        @functools.wraps(func)  # 传入原函数meta信息
        def warp_direct_system_call(*_args, **__kwargs) -> Any:
            # 检查ctx
            ctx = _args[0]
            assert isinstance(ctx, SystemContext), "第一个参数必须是SystemContext实例"
            # 检查当前ctx[]
            try:
                assert ctx.depend[func.__name__] == func, (
                    f"错误，ctx[{func.__name__}] 返回值不是本函数"
                )
            except KeyError:
                raise RuntimeError(
                    "要调用其他System必须在define_system时通过depends参数定义"
                )
            return func(*_args, **__kwargs)

        return warp_direct_system_call

    return warp
