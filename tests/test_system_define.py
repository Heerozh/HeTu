import unittest
from hetu.data import (define_component, Property, BaseComponent,
                       ComponentDefines, Permission)
from hetu.system import (SystemClusters, define_system)


class TestSystemDefine(unittest.TestCase):
    @classmethod
    def build_test_component(cls):
        global GalaxyPosition, Hp, Inventory, Map, Time, World

        @define_component(namespace="ssw", force=True)
        class GalaxyPosition(BaseComponent):
            x: float = Property(0, True)
            y: float = Property(0, True)

        @define_component(namespace="ssw", force=True)
        class Hp(BaseComponent):
            hp: float = Property(0, True)
            max_hp: float = Property(0, True)

        @define_component(namespace="ssw", force=True)
        class Inventory(BaseComponent):
            owner: int = Property(0, True)

        @define_component(namespace="ssw", force=True)
        class Map(BaseComponent):
            owner: int = Property(0, True)

        @define_component(namespace="ssw", force=True, persist=False)
        class Time(BaseComponent):
            clock: int = Property(0, False)

        @define_component(namespace="ssw", force=True, persist=False, readonly=True)
        class World(BaseComponent):
            some: int = Property(0, False)

    def test_normal_define(self):
        # 先卸载SystemClusters单件防止重定义
        SystemClusters._instances.pop(SystemClusters, None)
        self.build_test_component()

        # 定义测试系统
        @define_system(
            namespace="ssw",
            components=(GalaxyPosition, Hp, Inventory),
        )
        async def system1(ctx, vec, hit=1):
            pass

        # 要能取到定义
        sys1_def = SystemClusters().get_system('ssw', 'system1')
        self.assertNotEqual(sys1_def.func, system1)
        self.assertEqual(sys1_def.components, (GalaxyPosition, Hp, Inventory))
        self.assertEqual(sys1_def.arg_count, 3)
        self.assertEqual(sys1_def.defaults_count, 1)

        # 直接调用要禁止
        with self.assertRaisesRegex(RuntimeError, "调用"):
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
            namespace="ssw",
            components=(GalaxyPosition, Hp, Inventory),
            force=True
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
                permission=Permission.OWNER
            )
            def system1(ctx, vec, hit):
                pass

        # 测试继承的结果是否正确
        @define_system(
            namespace="ssw",
            components=(GalaxyPosition, ),
        )
        async def system_base(ctx, vec, hit):
            pass

        @define_system(
            namespace="ssw",
            components=(Hp, Inventory),
            inherits=('system_base',)
        )
        async def system_inherit1(ctx, vec, hit):
            pass

        @define_system(
            namespace="ssw",
            components=(World, ),
            inherits=('system_inherit1',)
        )
        async def system_inherit2(ctx, vec, hit):
            pass

        SystemClusters().build_clusters()

        sys_def = SystemClusters().get_system(
            'ssw', 'system_inherit2')
        clu = SystemClusters().get_cluster('ssw', 0)
        self.assertEqual(sys_def.full_components, {GalaxyPosition, Hp, Inventory, World})
        self.assertEqual(clu.components, {GalaxyPosition, Hp, Inventory, World})

        # 检测sync是否有警告
        with self.assertWarns(UserWarning):
            @define_system(
                namespace="ssw",
                components=(GalaxyPosition, Hp, Inventory),
            )
            def system_sync(ctx, vec, hit):
                pass

    def test_system_clusters(self):
        # 先卸载SystemClusters单件防止重定义
        SystemClusters._instances.pop(SystemClusters, None)
        self.build_test_component()

        # 定义测试系统
        @define_system(
            namespace="ssw",
            components=(Map, Hp,),
        )
        async def system1(ctx, ):
            pass

        @define_system(
            namespace="ssw",
            components=(Hp, ),
        )
        async def system2(ctx, ):
            pass

        @define_system(
            namespace="ssw",
            components=(Map, ),
        )
        async def system3(ctx, ):
            pass

        @define_system(
            namespace="ssw",
            components=(GalaxyPosition,),
        )
        async def system4(ctx, ):
            pass

        @define_system(
            namespace="ssw2",
            components=(GalaxyPosition,),
        )
        async def system4(ctx, ):
            pass

        @define_system(
            namespace="ssw",
            components=(GalaxyPosition, Inventory),
        )
        async def system5(ctx, ):
            pass

        # 测试cluster
        clusters = SystemClusters()
        clusters.build_clusters()
        self.assertEqual(len(clusters.get_clusters('ssw')), 2)
        self.assertEqual(len(clusters.get_clusters('ssw')[0].systems), 3)
        self.assertEqual(len(clusters.get_clusters('ssw')[1].systems), 2)
        self.assertEqual(clusters.get_clusters('ssw')[0].id, 0)
        self.assertEqual(clusters.get_clusters('ssw2')[0].id, 0)

        self.assertEqual(
            clusters.get_system('ssw', 'system1').cluster_id, 0)
        self.assertEqual(
            clusters.get_system('ssw', 'system4').cluster_id, 1)
        self.assertEqual(
            clusters.get_system('ssw2', 'system4').cluster_id, 0)
        self.assertEqual(
            clusters.get_system('ssw', 'system5').cluster_id, 1)

        # bug 测试clusters.append是忘记sys_def.full_components.copy()的bug
        self.assertEqual(
            clusters.get_system('ssw', 'system4').full_components,
            {GalaxyPosition})


if __name__ == '__main__':
    unittest.main()
