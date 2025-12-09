#  """
#  @author: Heerozh (Zhang Jianhao)
#  @copyright: Copyright 2024, Heerozh. All rights reserved.
#  @license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
#  @email: heeroz@gmail.com
#  """

import numpy as np
import pytest

from hetu.data import define_component, property_field, BaseComponent


def test_normal_define(new_component_env):
    @define_component(namespace="pytest")
    class GalaxyPosition(BaseComponent):
        x: int = property_field(0, True)
        y: float = property_field(0, False)
        aaa: np.int8 = property_field(0, False)

    # 测试是否删除了定义的属性
    with pytest.raises(AttributeError):
        print(GalaxyPosition.x)

    # 测试属性是否正确放入了_properties
    assert GalaxyPosition.properties_ == [
        ("aaa", property_field(0, False, False, np.dtype(np.int8).str)),
        ("id", property_field(0, True, True, np.dtype(np.int64).str)),
        ("x", property_field(0, True, True, np.dtype(int).str)),
        ("y", property_field(0, False, False, np.dtype(float).str)),
    ]

    # 测试是否会提示无Property错误
    with pytest.raises(AssertionError, match="Property"):

        @define_component(namespace="pytest")
        class GalaxyPosition(BaseComponent):
            x: int = 0
            y: float = 0

    # 测试是否会提示【继承】错误
    with pytest.raises(AssertionError, match="BaseComponent"):

        @define_component(namespace="pytest")
        class GalaxyPosition:
            x: int = property_field(0, True)
            y: float = property_field(0, False)

    # 测试是否会提示重复定义错误
    with pytest.raises(AssertionError):

        @define_component(namespace="pytest")
        class GalaxyPosition(BaseComponent):
            x: int = property_field(0, True)
            y: float = property_field(0, False)

    # 强制重定义
    @define_component(namespace="pytest", force=True)
    class GalaxyPosition(BaseComponent):
        x: int = property_field(0, True)
        y: float = property_field(0, False)

    # 测试重定义id
    with pytest.raises(AssertionError, match="id"):

        @define_component(namespace="pytest", force=True)
        class GalaxyPosition(BaseComponent):
            id: int = property_field(0, True)

    # 测试默认值和dtype冲突
    with pytest.raises(AssertionError, match="default值"):

        @define_component(namespace="pytest", force=True)
        class GalaxyPosition(BaseComponent):
            name: np.int8 = property_field("0")

    with pytest.raises(AssertionError, match="default值"):

        @define_component(namespace="pytest", force=True)
        class GalaxyPosition(BaseComponent):
            name: "U8" = property_field(99999999999)

    with pytest.raises(AssertionError, match="None"):

        @define_component(namespace="pytest", force=True)
        class GalaxyPosition(BaseComponent):
            name: float = property_field(None)

    # 测试默认值
    @define_component(namespace="pytest", force=True)
    class GalaxyPosition(BaseComponent):
        x: int = property_field(88, True)
        y: float = property_field(44, False)

    row = GalaxyPosition.new_row()
    assert row.x == 88

    row = GalaxyPosition.new_rows(2)
    assert row.x[1] == 88

    # 测试布尔值强制更换
    @define_component(namespace="pytest", force=True)
    class TestBool(BaseComponent):
        a: bool = property_field(True, True)
        b: "?" = property_field(True, False)
        c: np.bool_ = property_field(True, False)

    np.testing.assert_array_equal(
        np.array(list(TestBool.dtypes.fields.values()))[:, 0],
        [np.int8, np.int8, np.int8, np.int64],
    )

    # 测试字符串byte类型
    @define_component(namespace="pytest", force=True)
    class TestString(BaseComponent):
        a: "U8" = property_field(b"123", True, True)
        b: "S8" = property_field(b"123", True, False)
        c: "b" = property_field(1, True, False)

    assert TestString.indexes_["a"] == True
    assert TestString.indexes_["b"] == True
    assert TestString.indexes_["c"] == False


def test_instance_define(new_component_env, new_clusters_env):
    @define_component(namespace="pytest")
    class Health(BaseComponent):
        value: np.int8 = property_field(0, False)

    from hetu.system import define_system, SystemClusters

    @define_system(components=(Health.duplicate("pytest", "copy"),), namespace="pytest")
    async def test_hp(ctx):
        pass

    # 测试system和instance是否正确定义
    sys_def = SystemClusters().get_system("test_hp", "pytest")
    assert Health.get_duplicates("pytest")["copy"] == next(iter(sys_def.components))
    assert Health.get_duplicates("pytest")["copy"].component_name_ == "Health:copy"
    assert Health.get_duplicates("pytest")["copy"].properties_ == Health.properties_

    # 测试instance的instances属性应该为空
    assert Health.get_duplicates("pytest")["copy"].instances_ == {}


def test_keyword_define(new_component_env):
    with pytest.raises(ValueError, match="关键字"):

        @define_component(namespace="HeTu", persist=False)
        class TestKeywordComponent(BaseComponent):
            bool: bool = property_field(False)

    with pytest.raises(ValueError, match="C#"):

        @define_component(namespace="HeTu", persist=False)
        class TestKeywordComponent(BaseComponent):
            sbyte: bool = property_field(False)

    with pytest.raises(ValueError, match="C#"):

        @define_component(namespace="HeTu", persist=False)
        class sbyte(BaseComponent):
            _ok: bool = property_field(False)


def test_unique_index_false(new_component_env, caplog):
    @define_component(namespace="pytest", force=True)
    class TestComp(BaseComponent):
        a: np.int64 = property_field(0, unique=True, index=False)

    assert "index" in caplog.text
    caplog.clear()

    @define_component(namespace="pytest", force=True)
    class TestComp(BaseComponent):
        a: np.int64 = property_field(0, unique=True)

    assert "index" not in caplog.text
