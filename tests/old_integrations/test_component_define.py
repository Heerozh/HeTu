import unittest
import numpy as np
from hetu.data import (
    define_component, Property, BaseComponent, ComponentDefines
    )
from hetu.system.executor import SystemClusters


class TestComponentDefine(unittest.TestCase):
    def test_normal_define(self):
        ComponentDefines().clear_()

        @define_component(namespace="ssw")
        class GalaxyPosition(BaseComponent):
            x: int = Property(0, True)
            y: float = Property(0, False)
            aaa: np.int8 = Property(0, False)

        # 测试是否删除了定义的属性
        with self.assertRaises(AttributeError):
            print(GalaxyPosition.x)

        # 测试属性是否正确放入了_properties
        self.assertEqual(
            GalaxyPosition.properties_,
            [
                ('aaa', Property(0, False, False, np.dtype(np.int8).str)),
                ('id', Property(0, True, True, np.dtype(np.int64).str)),
                ('x', Property(0, True, True, np.dtype(int).str)),
                ('y', Property(0, False, False, np.dtype(float).str))
            ])

        # 测试是否会提示无Property错误
        with self.assertRaisesRegex(AssertionError, "Property"):
            @define_component(namespace="ssw")
            class GalaxyPosition(BaseComponent):
                x: int = 0
                y: float = 0

        # 测试是否会提示【继承】错误
        with self.assertRaisesRegex(AssertionError, "BaseComponent"):
            @define_component(namespace="ssw")
            class GalaxyPosition:
                x: int = Property(0, True)
                y: float = Property(0, False)

        # 测试是否会提示重复定义错误
        with self.assertRaises(AssertionError):
            @define_component(namespace="ssw")
            class GalaxyPosition(BaseComponent):
                x: int = Property(0, True)
                y: float = Property(0, False)

        # 强制重定义
        @define_component(namespace="ssw", force=True)
        class GalaxyPosition(BaseComponent):
            x: int = Property(0, True)
            y: float = Property(0, False)

        # 测试重定义id
        with self.assertRaisesRegex(AssertionError, "id"):
            @define_component(namespace="ssw", force=True)
            class GalaxyPosition(BaseComponent):
                id: int = Property(0, True)

        # 测试默认值和dtype冲突
        with self.assertRaisesRegex(AssertionError, "default值"):
            @define_component(namespace="ssw", force=True)
            class GalaxyPosition(BaseComponent):
                name: np.int8 = Property('0')

        with self.assertRaisesRegex(AssertionError, "default值"):
            @define_component(namespace="ssw", force=True)
            class GalaxyPosition(BaseComponent):
                name: 'U8' = Property(99999999999)

        with self.assertRaisesRegex(AssertionError, "None"):
            @define_component(namespace="ssw", force=True)
            class GalaxyPosition(BaseComponent):
                name: float = Property(None)

        # 测试默认值
        @define_component(namespace="ssw", force=True)
        class GalaxyPosition(BaseComponent):
            x: int = Property(88, True)
            y: float = Property(44, False)

        row = GalaxyPosition.new_row()
        self.assertEqual(row.x, 88)

        row = GalaxyPosition.new_row(2)
        self.assertEqual(row.x[1], 88)

        # 测试布尔值强制更换
        @define_component(namespace="ssw", force=True)
        class TestBool(BaseComponent):
            a: bool = Property(True, True)
            b: '?' = Property(True, False)
            c: np.bool_ = Property(True, False)

        np.testing.assert_array_equal(
            np.array(list(TestBool.dtypes.fields.values()))[:, 0],
            [np.int8, np.int8, np.int8, np.int64])

        # 测试版本信息, git hash 长度40
        self.assertEqual(len(TestBool.git_hash_), 40)

        # 测试字符串byte类型
        @define_component(namespace="ssw", force=True)
        class TestString(BaseComponent):
            a: 'U8' = Property(b'123', True, True)
            b: 'S8' = Property(b'123', True, False)
            c: 'b'  = Property(1, True, False)

        self.assertEqual(TestString.indexes_['a'], True)
        self.assertEqual(TestString.indexes_['b'], True)
        self.assertEqual(TestString.indexes_['c'], False)

    def test_instance_define(self):
        @define_component(namespace="ssw")
        class Health(BaseComponent):
            value: np.int8 = Property(0, False)

        from hetu.system import define_system, SystemClusters

        @define_system(components=(Health.duplicate("ssw", "copy"), ), namespace="ssw")
        async def test_hp(ctx):
            pass

        # 测试system和instance是否正确定义
        sys_def = SystemClusters().get_system("test_hp", "ssw")
        self.assertEqual(
            Health.get_duplicates("ssw")["copy"],
            next(iter(sys_def.components))
        )
        self.assertEqual(
            Health.get_duplicates("ssw")["copy"].component_name_,
            "Health:copy"
        )
        self.assertEqual(
            Health.get_duplicates("ssw")["copy"].properties_,
            Health.properties_
        )
        # 测试instance的instances属性应该为空
        self.assertEqual(
            Health.get_duplicates("ssw")["copy"].instances_,
            {}
        )

    def test_keyword_define(self):
        with self.assertRaisesRegex(ValueError, "关键字"):
            @define_component(namespace='HeTu', persist=False)
            class TestKeywordComponent(BaseComponent):
                bool: bool = Property(False)

        with self.assertRaisesRegex(ValueError, "C#"):
            @define_component(namespace='HeTu', persist=False)
            class TestKeywordComponent(BaseComponent):
                sbyte: bool = Property(False)

        with self.assertRaisesRegex(ValueError, "C#"):
            @define_component(namespace='HeTu', persist=False)
            class sbyte(BaseComponent):
                _ok: bool = Property(False)


if __name__ == '__main__':
    unittest.main()
