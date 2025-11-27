#  """
#  @author: Heerozh (Zhang Jianhao)
#  @copyright: Copyright 2024, Heerozh. All rights reserved.
#  @license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
#  @email: heeroz@gmail.com
#  """

import unittest

from hetu.data import (
    define_component,
    property_field,
    BaseComponent,
    ComponentDefines,
    Permission,
)
from hetu.system import SystemClusters, define_system


class TestSystemDefine(unittest.TestCase):
    @classmethod
    def build_test_component(cls):
        global GalaxyPosition, Hp, Inventory, Map, Time, World

        @define_component(namespace="ssw", force=True)
        class GalaxyPosition(BaseComponent):
            x: float = property_field(0, True)
            y: float = property_field(0, True)

        @define_component(namespace="ssw", force=True)
        class Hp(BaseComponent):
            hp: float = property_field(0, True)
            max_hp: float = property_field(0, True)

        @define_component(namespace="ssw", force=True)
        class Inventory(BaseComponent):
            owner: int = property_field(0, True)

        @define_component(namespace="ssw", force=True)
        class Map(BaseComponent):
            owner: int = property_field(0, True)

        @define_component(namespace="ssw", force=True, persist=False)
        class Time(BaseComponent):
            clock: int = property_field(0, False)

        @define_component(namespace="ssw", force=True, persist=False, readonly=True)
        class World(BaseComponent):
            some: int = property_field(0, False)

    @classmethod
    def setUpClass(cls):
        ComponentDefines().clear_()
        cls.build_test_component()

    def test_normal_define(self):
        # 先卸载SystemClusters单件防止重定义
        SystemClusters()._clear()

        # 定义测试系统
        @define_system(
            namespace="ssw",
            components=(GalaxyPosition, Hp, Inventory),
        )
        async def system1(ctx, vec, hit=1):
            pass

        # 要能取到定义
        sys1_def = SystemClusters().get_system("system1", namespace="ssw")
        self.assertNotEqual(sys1_def.func, system1)
        self.assertEqual(sys1_def.components, (GalaxyPosition, Hp, Inventory))
        self.assertEqual(sys1_def.arg_count, 3)
        self.assertEqual(sys1_def.defaults_count, 1)

        # 直接调用要禁止
        with self.assertRaisesRegex(AssertionError, "Context"):
            system1(1, 2, 3)

        # 重复定义
        with self.assertRaisesRegex(AssertionError, "System重复定义"):

            @define_system(
                namespace="ssw",
                components=(GalaxyPosition, Hp, Inventory),
            )
            async def system1(ctx, vec, hit):
                pass

        @define_system(
            namespace="ssw", components=(GalaxyPosition, Hp, Inventory), force=True
        )
        async def system1(ctx, vec, hit):
            pass

        # 测试参数不对
        with self.assertRaisesRegex(AssertionError, "参数名定义错误"):

            @define_system(
                namespace="ssw",
                components=(GalaxyPosition, Hp, Inventory),
            )
            def system_error(vec, hit):
                pass

        # 测试权限不对
        with self.assertRaisesRegex(AssertionError, "权限"):

            @define_system(
                namespace="ssw",
                components=(GalaxyPosition, Hp, Inventory),
                force=True,
                permission=Permission.OWNER,
            )
            def system1(ctx, vec, hit):
                pass

        # 测试继承的结果是否正确
        @define_system(
            namespace="ssw",
            components=(GalaxyPosition,),
        )
        async def system_base(ctx, vec, hit):
            pass

        @define_system(
            namespace="ssw", components=(Hp, Inventory), subsystems=("system_base",)
        )
        async def system_inherit1(ctx, vec, hit):
            pass

        @define_system(
            namespace="ssw", components=(World,), subsystems=("system_inherit1",)
        )
        async def system_inherit2(ctx, vec, hit):
            pass

        SystemClusters().build_clusters("ssw")

        sys_def = SystemClusters().get_system("system_inherit2", namespace="ssw")
        clu = SystemClusters().get_cluster("ssw", 0)
        self.assertEqual(
            sys_def.full_components, {GalaxyPosition, Hp, Inventory, World}
        )
        self.assertEqual(clu.components, {GalaxyPosition, Hp, Inventory, World})

        # 检测sync是否有警告
        with self.assertRaisesRegex(AssertionError, "async"):

            @define_system(
                namespace="ssw",
                components=(GalaxyPosition, Hp, Inventory),
            )
            def system_sync(ctx, vec, hit):
                pass

        # 检测不同backend是否有警告
        with self.assertRaisesRegex(AssertionError, "backend"):

            @define_component(namespace="ssw", force=True, backend="Mysql")
            class GalaxyPositionMysql(BaseComponent):
                x: float = property_field(0, True)
                y: float = property_field(0, True)

            @define_system(
                namespace="ssw",
                components=(GalaxyPositionMysql, Hp, Inventory, Map),
            )
            async def system_diff_backend(ctx, vec, hit):
                pass

        # 检测继承的backend也要一致
        SystemClusters()._clear()

        @define_system(
            namespace="ssw",
            components=(GalaxyPositionMysql,),
        )
        async def system_mysql(ctx, vec, hit):
            pass

        @define_system(
            namespace="ssw",
            components=(Hp, Inventory, Map),
            subsystems=("system_mysql",),
        )
        async def system_diff_inh_backend(ctx, vec, hit):
            pass

        with self.assertRaisesRegex(AssertionError, "backend"):
            SystemClusters().build_clusters("ssw")

    def test_system_clusters(self):
        # 先卸载SystemClusters单件防止重定义
        SystemClusters()._clear()

        # 定义测试系统
        @define_system(
            namespace="ssw",
            components=(
                Map,
                Hp,
            ),
        )
        async def system1(
            ctx,
        ):
            pass

        @define_system(
            namespace="ssw",
            components=(Hp,),
        )
        async def system2(
            ctx,
        ):
            pass

        @define_system(
            namespace="ssw",
            components=(Map,),
        )
        async def system3(
            ctx,
        ):
            pass

        @define_system(
            namespace="ssw", components=(GalaxyPosition,), non_transactions=(Map,)
        )
        async def system4(
            ctx,
        ):
            pass

        @define_system(
            namespace="ssw2",
            components=(GalaxyPosition,),
        )
        async def system4(
            ctx,
        ):
            pass

        @define_system(
            namespace="global",
            components=(GalaxyPosition, Inventory),
        )
        async def system5(
            ctx,
        ):
            pass

        # 测试cluster
        clusters = SystemClusters()
        clusters.build_clusters("ssw")
        global_clusters = len(clusters.get_clusters("global")) - 1
        self.assertEqual(len(clusters.get_clusters("ssw")), 2 + global_clusters)
        self.assertEqual(len(clusters.get_clusters("ssw")[0].systems), 3)
        self.assertEqual(len(clusters.get_clusters("ssw")[1].systems), 2)
        self.assertEqual(clusters.get_clusters("ssw")[0].id, 0)
        self.assertEqual(clusters.get_clusters("ssw2")[0].id, 0)

        self.assertEqual(clusters.get_system("system1", namespace="ssw").cluster_id, 0)
        self.assertEqual(clusters.get_system("system4", namespace="ssw").cluster_id, 1)
        self.assertEqual(clusters.get_system("system4", namespace="ssw2").cluster_id, 0)
        self.assertEqual(clusters.get_system("system5", namespace="ssw").cluster_id, 1)
        self.assertEqual(
            clusters.get_system("system4", namespace="ssw").full_non_trx,
            {
                Map,
            },
        )

        # bug 测试clusters.append是忘记sys_def.full_components.copy()的bug
        self.assertEqual(
            clusters.get_system("system4", namespace="ssw").full_components,
            {GalaxyPosition},
        )

    def test_system_copy(self):
        # 先卸载SystemClusters单件防止重定义
        SystemClusters()._clear()

        # 定义测试系统
        @define_system(
            namespace="ssw",
            components=(GalaxyPosition,),
        )
        async def __not_used__(
            ctx,
        ):
            pass

        @define_system(
            namespace="ssw",
            components=(GalaxyPosition.duplicate("ssw", "copy"),),
        )
        async def __not_used2__(
            ctx,
        ):
            pass

        @define_system(
            namespace="ssw",
            components=(
                Map,
                Hp,
            ),
            non_transactions=(GalaxyPosition,),
        )
        async def system1(
            ctx,
        ):
            pass

        @define_system(
            namespace="ssw",
            subsystems=("system1:copy",),
        )
        async def system_copy1(
            ctx,
        ):
            pass

        # 检测组件为副本
        clusters = SystemClusters()
        clusters.build_clusters("ssw")

        self.assertEqual(
            clusters.get_system("system_copy1", namespace="ssw").full_components,
            {Map.duplicate("ssw", "copy"), Hp.duplicate("ssw", "copy")},
        )
        self.assertEqual(
            clusters.get_system("system_copy1", namespace="ssw").full_non_trx,
            {GalaxyPosition.duplicate("ssw", "copy")},
        )

        # 检测cluster不相关
        self.assertNotEqual(
            clusters.get_system("system1", namespace="ssw").cluster_id,
            clusters.get_system("system_copy1", namespace="ssw").cluster_id,
        )


if __name__ == "__main__":
    unittest.main()
