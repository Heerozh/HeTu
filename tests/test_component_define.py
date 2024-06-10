import unittest
import numpy as np
from hetu.data import (
    define_component, Property, BaseComponent, ComponentDefines
    )


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
                ('aaa', Property(0, False, False, np.int8)),
                ('id', Property(0, True, True, np.int64)),
                ('x', Property(0, True, True, int)),
                ('y', Property(0, False, False, float))
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
        with self.assertRaisesRegex(AssertionError, "默认值"):
            @define_component(namespace="ssw", force=True)
            class GalaxyPosition(BaseComponent):
                name: np.int8 = Property('0')

        with self.assertRaisesRegex(AssertionError, "默认值"):
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
        self.assertEqual(len(TestBool.version_), 40)


if __name__ == '__main__':
    unittest.main()
