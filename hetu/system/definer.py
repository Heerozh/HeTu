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

from ..common import Singleton
from ..common.permission import Permission
from .execution import ExecutionLock

if TYPE_CHECKING:
    from ..data import BaseComponent

SYSTEM_NAME_MAX_LEN = 32


@dataclass
class SystemDefine:
    func: FunctionType
    components: set[type[BaseComponent]]  # 引用的Components
    full_components: set[type[BaseComponent]]  # 完整的引用，包括继承自父System的
    non_transactions: set[type[BaseComponent]]  # 直接获取的Components，不走事务
    full_non_trx: set[type[BaseComponent]]  # 完整的无事务引用，包括继承自父System的
    subsystems: set[str]
    full_subsystems: set[str]
    permission: Permission
    max_retry: int
    arg_count: int  # 全部参数个数（含默认参数）
    defaults_count: int  # 默认参数个数
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

    def __init__(self):
        # ==== @define_system(namespace=xxx) 定义的所有System ====
        # 所有system定义表，按namespace分类
        self._system_map: dict[str, dict[str, SystemDefine]] = {}
        # 所有Component所属的cluster id表，只包含被system引用过的Component
        self._component_map: dict[type[BaseComponent], int] = {}
        # 所有namespace下的clusters信息列表
        self._clusters: dict[str, list[SystemClusters.Cluster]] = {}
        # @define_system(namespace="global") 定义的所有System
        self._global_system_map: dict[str, SystemDefine] = {}
        # 方便快速访问主namespace的System定义
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

    def get_component_cluster_id(self, comp: type[BaseComponent]) -> int | None:
        return self._component_map.get(comp, None)

    def get_components(self) -> dict[type[BaseComponent], int]:
        """返回所有被System引用过的Component及其所属簇id"""
        return self._component_map

    def get_clusters(self, namespace: str):
        return self._clusters.get(namespace, None)

    def get_cluster(self, namespace: str, cluster_id: int):
        return self._clusters[namespace][cluster_id]

    def build_clusters(self, main_namespace: str):
        assert self._clusters == {}, "簇已经生成过了"
        assert main_namespace in self._system_map, (
            f"没有找到namespace={main_namespace}的System定义"
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

        def inherit_components(namespace_, subsystems, req: set, n_trx: set, inh: set):
            for base_sys in subsystems:
                base_name, sys_suffix = (
                    base_sys.split(":") if ":" in base_sys else (base_sys, "")
                )
                if base_name not in self._system_map[namespace_]:
                    raise RuntimeError(
                        f"{sys_name} 的 `subsystems` 引用的System {base_name} 不存在"
                    )
                base_def = self._system_map[namespace_][base_name]
                if sys_suffix:
                    # 复制Component
                    req.update(
                        [
                            comp.duplicate(namespace_, sys_suffix)
                            for comp in base_def.components
                        ]
                    )
                    n_trx.update(
                        [
                            comp.duplicate(namespace_, sys_suffix)
                            for comp in base_def.non_transactions
                        ]
                    )
                else:
                    req.update(base_def.components)
                    n_trx.update(base_def.non_transactions)
                inh.update(base_def.subsystems)
                inherit_components(namespace_, base_def.subsystems, req, n_trx, inh)

        non_trx = set()

        for namespace in self._system_map:
            # 把global的System迁移到当前namespace
            self._system_map[namespace].update(copy.deepcopy(self._global_system_map))

            clusters = []
            # 首先把所有系统变成独立的簇/并生成完整的请求表
            for sys_name, sys_def in self._system_map[namespace].items():
                sys_def.full_components = set(sys_def.components)
                sys_def.full_subsystems = set(sys_def.subsystems)
                sys_def.full_non_trx = set(sys_def.non_transactions)
                inherit_components(
                    namespace,
                    sys_def.subsystems,
                    sys_def.full_components,
                    sys_def.full_non_trx,
                    sys_def.full_subsystems,
                )
                non_trx.update(sys_def.full_non_trx)
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
                    self._component_map[comp] = cluster.id

        self._main_system_map = self._system_map[main_namespace]

        # 检查是否有component被non_transactions引用，但没有任何正常方式引用了它，必须至少有一个标准引用
        # 加入这个限制的原因是，不被事务引用下，本方法就无法确定该Component应该储存在哪个Cluster中
        for comp in non_trx:
            if comp not in self._component_map:
                raise RuntimeError(
                    f"non_transactions 方式为非事务引用(弱引用)，但需要保证该"
                    f"Component {comp.__name__} 至少有一个正常引用。"
                    + (
                        "该Component是副本，每个副本也同样要保证至少有一个正常引用。"
                        if ":" in comp.__name__
                        else ""
                    )
                )

    def add(
        self,
        namespace,
        func,
        components,
        non_trx,
        force,
        permission,
        subsystems,
        max_retry,
    ):
        sub_map = self._system_map.setdefault(namespace, dict())

        if not force:
            assert func.__name__ not in sub_map, "System重复定义：" + func.__name__
        if components is None:
            components = set()
        if non_trx is None:
            non_trx = set()

        # 获取函数参数个数，存下来，要求客户端调用严格匹配
        arg_count = func.__code__.co_argcount
        defaults_count = len(func.__defaults__) if func.__defaults__ else 0

        sub_map[func.__name__] = SystemDefine(
            func=func,
            components=components,
            non_transactions=non_trx,
            subsystems=subsystems,
            max_retry=max_retry,
            arg_count=arg_count,
            defaults_count=defaults_count,
            cluster_id=-1,
            permission=permission,
            full_components=set(),
            full_non_trx=set(),
            full_subsystems=set(),
        )

        if namespace == "global":
            self._global_system_map[func.__name__] = sub_map[func.__name__]


def define_system(
    components: tuple[type[BaseComponent], ...] | None = None,
    non_transactions: tuple[type[BaseComponent], ...] | None = None,
    namespace: str = "default",
    force: bool = False,
    permission=Permission.USER,
    retry: int = 9999,
    subsystems: tuple[str | FunctionType] = tuple(),
    call_lock=False,
):
    """
    定义System(函数)

    Examples
    --------
    >>> from hetu.data import BaseComponent, define_component, property_field
    >>> @define_component
    ... class Position(BaseComponent):
    ...     x: int = property_field(0)
    >>> @define_component
    ... class HP(BaseComponent):
    ...     hp: int = property_field(0)
    ...
    >>> from hetu.system import define_system, Context, ResponseToClient
    >>> @define_system(
    ...     namespace="ssw",
    ...     components=(Position, HP),
    ... )
    ... async def system_dash(ctx: Context, entity_self, entity_target, vec):
    ...     pos_self = await ctx[Position].select(entity_self)
    ...     pos_self.x += vec.x
    ...     await ctx[Position].update(entity_self, pos_self)
    ...     enemy_hp = ctx[HP].query("owner", entity_target)
    ...     enemy_hp -= 10
    ...     await ctx[HP].update(entity_target, enemy_hp)
    ...     return ResponseToClient(['client cmd', 'blah blah'])

    Parameters
    ----------
    namespace: str
        是你的项目名，一个网络地址只能启动一个namespace下的System们。
        定义为"global"的namespace可以在所有项目下通用。
    components: list of BaseComponent class
        引用Component，引用的Component可以在`ctx`中进行相关的事务操作，保证数据一致性。
        所有引用的Components会加入共置簇(Colocation Cluster)中，指放在同一个物理数据库中，
        具体见Notes。
    non_transactions: list of BaseComponent class
        直接获得该Component底层类，因此可以绕过事务直接写入，注意不保证数据一致性，请只做原子操作。
        写入操作不支持打开索引的字符串属性，因为字符串索引无法实现原子操作。

        该引用不会加入共置簇，一般用在Hub Component上，让事务解耦，代价是没有数据一致性保证。
    force: bool
        遇到重复定义是否强制覆盖前一个, 单元测试用
    permission: Permission
        System执行权限，只对hetu client sdk连接起作用，服务器端代码不受限制。

        - everybody: 任何客户端连接都可以调用执行。（不安全）
        - user: 只有已登录客户端连接可以调用
        - owner: **不可用** OWNER权限这里不可使用，需要自行做安全检查
        - admin: 只有管理员权限客户端连接可以调用

    retry: int
        如果System遇到事务冲突，会重复执行直到成功。设为0关闭
    subsystems: tuple of (str | FunctionType)
        定义要调用的其他System，调用时会在同一个事务会话中执行。
        可传入System函数本身，或字符串，如("system1", system2)。
        可通过`ctx["system1"](ctx, ...)`或直接`system2(ctx, ...)`方式调用定义的函数。
        如果希望使用System副本，可以使用字符串式定义，加':副本名后缀'。具体见Notes。
        注意: 所有subsystems，将被合并进同一个共置簇中。
    call_lock: bool
        是否对此System启用调用锁，启用后在调用时可以通过传入调用UUID来防止System重复执行。
        如果此System需要给未来调用使用，则此项必须为True。

    Notes
    -----
    **System函数：**

    >>> async def system_dash(ctx: Context, entity_self, entity_target, vec)

    async:
        System必须是异步函数。
    ctx: Context
        上下文，具体见下述Context部分
    其他参数:
        为hetu client SDK调用时传入的参数。
    System返回值:
        如果调用方是父System（通过`subsystems`调用），则返回值会原样传给调用方；

        如果调用方是hetu client SDK：
            - 返回值是 hetu.system.ResponseToClient(data)时，则把data发送给调用方sdk。
            - 其他返回值丢弃

    **Context部分：**
        ctx.caller: int
            调用者id，由你在登录System中调用 `elevate` 函数赋值，`None` 或 0 表示未登录用户
        ctx.retry_count: int
            当前因为事务冲突的重试次数，0表示首次执行。
        ctx[Component Class]: ComponentTransaction
            获取Component事务实例，如 `ctx[Position]`，只能获取定义时在 `components` 中引用的实例。
            类型为 `ComponentTransaction`，可以进行数据库操作，并自动包装为事务在System结束后执行。
            具体参考 :py:func:`hetu.data.backend.ComponentTransaction` 的文档。
        ctx['SystemName']: func
            获取定义时在 `subsystems` 中传入的System函数。
        ctx.nontrxs[Component Class]: RawComponentTable
            获取non_transactions中引用的Component实例，类型为 `RawComponentTable`，可以direct_get/set数据，
            而不通过事务，因此危险不保证数据一致性，请只做原子操作。
        await ctx.end_transaction(discard=False):
            提前显式结束事务，如果遇到事务冲突，则此行下面的代码不会执行。
            注意：调用完 `end_transaction`，`ctx` 将不再能够获取 `components` 实列

    **Component 共置簇**

    System之间引用的Components有交集的，表示这些System之间可能有事务冲突，这些
    Components将加入同一个共置簇，共置簇中的Components数据会放在同一个物理数据库节点中。

    大量System都引用的Component称为Hub Component，会导致簇过大，从而数据库无法通过
    Cluster分区提升性能，影响未来的扩展性。正常建议通过拆分Component属性来降低簇聚集。

    **System副本继承：**

    代码示例：
    >>> @define_system(namespace="global", components=(Position, ), )
    ... async def move(ctx: Context, new_pos):
    ...     async with ctx[Position].update_or_insert(ctx.caller, 'owner') as pos:
    ...         pos.x = new_pos.x
    ...         pos.y = new_pos.y
    >>> @define_system(namespace="game", subsystems=('move:Map1', ))
    ... async def move_in_map1(ctx: Context, new_pos):
    ...     return await ctx['move:Map1'](new_pos)

    `subsystems=('move:Map1', )`等同创建一个新的`move` System，但是使用
    `components=(Position.duplicate(namespace, suffix='Map1'), )` 参数。

    正常调用`move`(不使用System副本)的话，数据是保存在名为 `Position` 的表中。
    在这个例子中，`move_in_map1` 调用的 `move` 函数中的数据会储存在名为
    `Position:Map1`的表中，从而实现同一套System逻辑在不同数据集上的操作。

    这么做的意义是不同数据集不会事务冲突，可以拆分成不同的Cluster，从而放到不同的数据库节点上，
    提升性能和扩展性。

    比如create_future_call这个内置System就是，它引用了FutureCalls队列组件，如果不使用副本继承，那么
    所有用到未来调用的System都将在同一个数据库节点上运行，形成一个大簇，影响扩展性。
    因此通过副本继承此方法，可以拆解出一部分System到不同的数据库节点上运行。
    不用担心，未来调用的后台任务在检查调用队列时，会循环检查所有FutureCalls副本组件队列。

    """
    from .context import Context

    # todo non_transactions名字还是不够好，考虑改名为direct_refs
    # todo 不用non_transactions，而是允许分割事务，可以传入多组事务，分别执行。
    #      不然每次客户端的一个命令，会强制都在一个事务，而future_call概念太复杂。
    def warp(func):
        # warp只是在系统里记录下有这么个东西，实际不改变function

        # 严格要求第一个参数命名
        func_args = signature(func).parameters
        func_arg_names = list(func_args.keys())[:1]
        assert func_arg_names == ["ctx"], (
            f"System参数名定义错误，第一个参数必须为：ctx。你的：{func_arg_names}"
        )

        assert permission not in (
            permission.OWNER,
            permission.RLS,
        ), "System的权限不支持OWNER/RLS"

        assert len(func.__name__) <= SYSTEM_NAME_MAX_LEN, (
            f"System函数名过长，最大长度为{SYSTEM_NAME_MAX_LEN}个字符"
        )

        assert inspect.iscoroutinefunction(func), (
            f"System {func.__name__} 必须是异步函数(`async def ...`)"
        )

        if components is not None and len(components) > 0:
            # 检查components是否都是同一个backend
            backend_names = [comp.backend_ for comp in components]
            assert len(set(backend_names)) <= 1, (
                f"System {func.__name__} 引用的Component必须都是同一种backend"
            )

        _components = components

        # 把call lock的表添加到components中
        if call_lock:
            lock_table = ExecutionLock.duplicate(namespace, func.__name__)
            if components is not None and len(components) > 0:
                lock_table.backend_ = components[0].backend_
                _components = list(components) + [lock_table]
            else:
                _components = [lock_table]

        subsystem_names = [
            subsystem if type(subsystem) is str else subsystem.__name__
            for subsystem in subsystems
        ]

        SystemClusters().add(
            namespace,
            func,
            _components,
            non_transactions,
            force,
            permission,
            subsystem_names,
            retry,
        )

        # 返回包装的func，因为不允许直接调用，需要检查。
        @functools.wraps(func)  # 传入原函数meta信息
        def warp_direct_system_call(*_args, **__kwargs) -> Any:
            # 检查ctx
            ctx = _args[0]
            assert isinstance(ctx, Context), "第一个参数必须是Context实例"
            # 检查当前ctx[]
            try:
                assert ctx[func.__name__] == func, (
                    f"错误，ctx[{func.__name__}] 返回值不是本函数"
                )
            except KeyError:
                raise RuntimeError(
                    "要调用其他System必须在define_system时通过subsystems参数定义"
                )
            return func(*_args, **__kwargs)

        return warp_direct_system_call

    return warp
