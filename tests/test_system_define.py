#  """
#  @author: Heerozh (Zhang Jianhao)
#  @copyright: Copyright 2024, Heerozh. All rights reserved.
#  @license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
#  @email: heeroz@gmail.com
#  """

import pytest

from hetu.data import define_component, property_field, BaseComponent, Permission
from hetu.system import SystemClusters, define_system, Context


@pytest.fixture
def test_component(new_component_env, new_clusters_env):
    @define_component(namespace="pytest", force=True)
    class Comp1(BaseComponent):
        index1: float = property_field(0, True)
        index2: float = property_field(0, True)

    @define_component(namespace="pytest", force=True)
    class Comp2(BaseComponent):
        value1: float = property_field(0)
        value2: float = property_field(0)

    @define_component(namespace="pytest", force=True)
    class Comp3(BaseComponent):
        owner: int = property_field(0, True)

    @define_component(namespace="pytest", force=True)
    class Comp4(BaseComponent):
        value1: float = property_field(0)

    return Comp1, Comp2, Comp3, Comp4


@pytest.fixture
def test_system1(test_component):
    # 定义测试系统
    @define_system(
        namespace="pytest",
        components=test_component[:3],
    )
    async def system1(ctx, vec, hit=1):
        pass

    return system1


def test_define(test_component, test_system1):
    # 要能取到定义
    sys1_def = SystemClusters().get_system("system1", namespace="pytest")

    assert sys1_def.func != test_system1
    assert sys1_def.components == test_component[:3]
    assert sys1_def.arg_count == 3
    assert sys1_def.defaults_count == 1


def test_direct_func_call_forbid(test_system1):
    # 直接调用要禁止
    with pytest.raises(AssertionError, match="Context"):
        test_system1(1, 2, 3)

    ctx = Context(
        caller=1,
        connection_id=1,
        address="127.0.0.1",
        group="admin",
        timestamp=0,
        user_data={},
        retry_count=0,
        transactions={},
        inherited={},
    )
    with pytest.raises(RuntimeError, match="subsystems"):
        test_system1(ctx, 2, 3)


def test_duplicate_define(test_system1):
    # 重复定义
    with pytest.raises(AssertionError, match="System重复定义"):

        @define_system(
            namespace="pytest",
            components=(),
        )
        async def system1(ctx):
            pass

    # 测试换个namespace能定义成功
    @define_system(
        namespace="ns2",
        components=(),
    )
    async def system1(ctx):
        pass


def test_system_first_param(test_component):
    # 测试参数不对，第一个必须是ctx
    with pytest.raises(AssertionError, match="参数名定义错误"):

        @define_system(
            namespace="pytest",
            components=test_component,
        )
        def system_error(vec, param1):
            pass

    @define_system(
        namespace="pytest",
        components=test_component,
    )
    async def system_error(ctx, param1, param2):
        pass


def test_system_permission(test_component):
    # 测试权限不对，system没有rls权限
    with pytest.raises(AssertionError, match="权限"):

        @define_system(
            namespace="pytest",
            components=test_component,
            force=True,
            permission=Permission.OWNER,
        )
        def system1(ctx, param1, param2):
            pass

    with pytest.raises(AssertionError, match="权限"):

        @define_system(
            namespace="pytest",
            components=test_component,
            force=True,
            permission=Permission.RLS,
        )
        def system1(ctx, param1):
            pass


def test_system_inheritance(test_component):
    comp1, comp2, comp3, comp4 = test_component

    # 测试继承的结果是否正确
    @define_system(
        namespace="pytest",
        components=(comp1,),
    )
    async def system_base(ctx, param1, param2):
        pass

    @define_system(
        namespace="pytest", components=(comp2, comp3), subsystems=("system_base",)
    )
    async def system_inherit1(ctx, param1, param2):
        pass

    @define_system(
        namespace="pytest", components=(comp4,), subsystems=(system_inherit1,)
    )
    async def system_inherit2(ctx, param1):
        pass

    SystemClusters().build_clusters("pytest")

    sys_def = SystemClusters().get_system("system_inherit2", namespace="pytest")
    clu = SystemClusters().get_cluster("pytest", 0)
    assert sys_def.full_components == set(test_component)
    assert clu.components == set(test_component)


def test_system_must_be_async(test_component):
    # 检测sync是否有警告
    with pytest.raises(AssertionError, match="async"):

        @define_system(
            namespace="ssw",
            components=test_component,
        )
        def system_sync(ctx, param1, param2):
            pass


def test_system_backend_consistent(test_component):
    # 检测不同backend是否有警告
    with pytest.raises(AssertionError, match="backend"):

        @define_component(namespace="pytest", force=True, backend="PostgreSQL")
        class PostgreSQLComp(BaseComponent):
            x: float = property_field(0, True)
            y: float = property_field(0, True)

        @define_system(
            namespace="ssw",
            components=(test_component[0], PostgreSQLComp),
        )
        async def system_diff_backend(ctx, vec, hit):
            pass


def test_system_inh_backend_consistent(test_component):
    # 检测继承的backend也要一致
    @define_component(namespace="pytest", force=True, backend="PostgreSQL")
    class PostgreSQLComp(BaseComponent):
        x: float = property_field(0, True)
        y: float = property_field(0, True)

    @define_system(
        namespace="pytest",
        components=(PostgreSQLComp,),
    )
    async def system_postgresql(ctx, vec, hit):
        pass

    @define_system(
        namespace="pytest", components=test_component, subsystems=("system_postgresql",)
    )
    async def system_diff_inh_backend(ctx, vec, hit):
        pass

    with pytest.raises(AssertionError, match="backend"):
        SystemClusters().build_clusters("pytest")


def test_system_clusters(test_component):
    comp1, comp2, comp3, comp4 = test_component

    # 定义测试系统
    @define_system(
        namespace="pytest",
        components=(
            comp2,
            comp1,
        ),
    )
    async def system1(
        ctx,
    ):
        pass

    @define_system(
        namespace="pytest",
        components=(comp1,),
    )
    async def system2(
        ctx,
    ):
        pass

    @define_system(
        namespace="pytest",
        components=(comp2,),
    )
    async def system3(
        ctx,
    ):
        pass

    @define_system(namespace="pytest", components=(comp3,), non_transactions=(comp2,))
    async def system4(
        ctx,
    ):
        pass

    @define_system(
        namespace="ns2",
        components=(comp3,),
    )
    async def system4(
        ctx,
    ):
        pass

    @define_system(
        namespace="global",
        components=(comp3, comp4),
    )
    async def system5(
        ctx,
    ):
        pass

    # 测试cluster
    clusters = SystemClusters()
    clusters.build_clusters("pytest")
    global_clusters = len(clusters.get_clusters("global")) - 1
    assert len(clusters.get_clusters("pytest")) == 2 + global_clusters
    assert len(clusters.get_clusters("pytest")[0].systems) == 3
    assert len(clusters.get_clusters("pytest")[1].systems) == 2
    assert clusters.get_clusters("pytest")[0].id == 0
    assert clusters.get_clusters("ns2")[0].id == 0

    assert clusters.get_system("system1", namespace="pytest").cluster_id == 0
    assert clusters.get_system("system4", namespace="pytest").cluster_id == 1
    assert clusters.get_system("system4", namespace="ns2").cluster_id == 0
    assert clusters.get_system("system5", namespace="pytest").cluster_id == 1
    assert clusters.get_system("system4", namespace="pytest").full_non_trx == {
        comp2,
    }

    # bug 测试clusters.append是忘记sys_def.full_components.copy()的bug
    assert clusters.get_system("system4", namespace="pytest").full_components == {comp3}


def test_system_copy(test_component):
    comp1, comp2, comp3, comp4 = test_component

    # 定义测试系统
    @define_system(
        namespace="pytest",
        components=(comp1,),
    )
    async def __not_used__(
        ctx,
    ):
        pass

    @define_system(
        namespace="pytest",
        components=(comp1.duplicate("pytest", "copy"),),
    )
    async def __not_used2__(
        ctx,
    ):
        pass

    @define_system(
        namespace="pytest",
        components=(
            comp2,
            comp3,
        ),
        non_transactions=(comp1,),
    )
    async def system1(
        ctx,
    ):
        pass

    @define_system(
        namespace="pytest",
        subsystems=("system1:copy",),
    )
    async def system_copy1(
        ctx,
    ):
        pass

    # 检测组件为副本
    clusters = SystemClusters()
    clusters.build_clusters("pytest")

    system1_def = clusters.get_system("system1", namespace="pytest")
    system_copy1_def = clusters.get_system("system_copy1", namespace="pytest")
    assert system_copy1_def.full_components == {
        comp2.duplicate("pytest", "copy"),
        comp3.duplicate("pytest", "copy"),
    }
    assert system_copy1_def.full_non_trx == {comp1.duplicate("pytest", "copy")}

    # 检测cluster不相关
    assert system1_def.cluster_id != system_copy1_def.cluster_id
