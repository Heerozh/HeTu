#  """
#  @author: Heerozh (Zhang Jianhao)
#  @copyright: Copyright 2024, Heerozh. All rights reserved.
#  @license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
#  @email: heeroz@gmail.com
#  """

import numpy as np
import pytest

from hetu.data import define_component, property_field, BaseComponent

from hetu.common.snowflake_id import SnowflakeID

SnowflakeID().init(1, 0)


def test_normal_define(new_component_env):
    @define_component(namespace="pytest")
    class MyPosition(BaseComponent):
        x: int = property_field(0, True)
        y: float = property_field(0, False)
        aaa: np.int8 = property_field(0, False)

    # 测试是否删除了定义的属性
    with pytest.raises(AttributeError):
        print(MyPosition.x)

    # 测试属性是否正确放入了_properties
    assert MyPosition.properties_ == [
        ("_version", property_field(0, False, False, np.dtype(np.int32).str)),
        ("aaa", property_field(0, False, False, np.dtype(np.int8).str)),
        ("id", property_field(0, True, True, np.dtype(np.int64).str)),
        ("x", property_field(0, True, True, np.dtype(int).str)),
        ("y", property_field(0, False, False, np.dtype(float).str)),
    ]

    # 测试是否会提示无Property错误
    with pytest.raises(AssertionError, match="Property"):

        @define_component(namespace="pytest")
        class MyPosition(BaseComponent):
            x: int = 0
            y: float = 0

    # 测试是否会提示【继承】错误
    with pytest.raises(AssertionError, match="BaseComponent"):

        @define_component(namespace="pytest")
        class MyPosition:
            x: int = property_field(0, True)
            y: float = property_field(0, False)

    # 测试是否会提示重复定义错误
    with pytest.raises(AssertionError):

        @define_component(namespace="pytest")
        class MyPosition(BaseComponent):
            x: int = property_field(0, True)
            y: float = property_field(0, False)

    # 强制重定义
    @define_component(namespace="pytest", force=True)
    class MyPosition(BaseComponent):
        x: int = property_field(0, True)
        y: float = property_field(0, False)

    # 测试重定义id
    with pytest.raises(AssertionError, match="id"):

        @define_component(namespace="pytest", force=True)
        class MyPosition(BaseComponent):
            id: int = property_field(0, True)

    # 测试默认值和dtype冲突
    with pytest.raises(AssertionError, match="default值"):

        @define_component(namespace="pytest", force=True)
        class MyPosition(BaseComponent):
            name: np.int8 = property_field("0")

    with pytest.raises(AssertionError, match="default值"):

        @define_component(namespace="pytest", force=True)
        class MyPosition(BaseComponent):
            name: "U8" = property_field(99999999999)

    with pytest.raises(AssertionError, match="None"):

        @define_component(namespace="pytest", force=True)
        class MyPosition(BaseComponent):
            name: float = property_field(None)

    # 测试默认值
    @define_component(namespace="pytest", force=True)
    class MyPosition(BaseComponent):
        x: int = property_field(88, True)
        y: float = property_field(44, False)

    row = MyPosition.new_row()
    assert row.x == 88

    row = MyPosition.new_rows(2)
    assert row.x[1] == 88

    # 测试布尔值强制更换（含字符串拼写的 bool dtype，应同样转为 int8）
    @define_component(namespace="pytest", force=True)
    class TestBool(BaseComponent):
        a: bool = property_field(True, True)
        b: "?" = property_field(True, False)
        c: np.bool_ = property_field(True, False)
        d: bool = property_field(True, False, dtype="bool")
        e: bool = property_field(True, False, dtype="|b1")
        f: bool = property_field(True, False, dtype="<b1")

    np.testing.assert_array_equal(
        np.array(list(TestBool.dtypes.fields.values()))[:, 0],
        [np.int32, np.int8, np.int8, np.int8, np.int8, np.int8, np.int8, np.int64],
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

    # 整行转换保留 Unicode 字符串和原始字节字段
    converted = TestString.dict_to_struct(
        {"_version": "2", "id": "42", "a": "汉字", "b": b"\xff\x00", "c": "1"}
    )
    assert converted.id == 42
    assert converted._version == 2
    assert converted.a == "汉字"
    assert converted.b == b"\xff"
    assert converted.c == 1


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
    assert Health.get_duplicates("pytest")["copy"].name_ == "Health:copy"
    assert Health.get_duplicates("pytest")["copy"].properties_ == Health.properties_

    # 测试instance的instances属性应该为空
    assert Health.get_duplicates("pytest")["copy"].instances_ == {}


def test_str_max_len(new_component_env):
    @define_component(namespace="pytest", force=True)
    class TestStrLen(BaseComponent):
        uni: "U32" = property_field("", False)
        code: "U8" = property_field("", False)
        raw: "S16" = property_field(b"", False)
        num: np.int32 = property_field(0, False)

    # 字符串(U)/字节(S)列：返回最大字符数（不是字节数）
    assert TestStrLen.str_max_len("uni") == 32
    assert TestStrLen.str_max_len("code") == 8
    assert TestStrLen.str_max_len("raw") == 16
    # 非字符串列 → ValueError
    with pytest.raises(ValueError, match="字符串"):
        TestStrLen.str_max_len("num")


def test_keyword_define(new_component_env):
    with pytest.raises(ValueError, match="关键字"):

        @define_component(namespace="HeTu", volatile=True)
        class TestKeywordComponent(BaseComponent):
            bool: bool = property_field(False)

    with pytest.raises(ValueError, match="C#"):

        @define_component(namespace="HeTu", volatile=True)
        class TestKeywordComponent(BaseComponent):
            sbyte: bool = property_field(False)

    with pytest.raises(ValueError, match="C#"):

        @define_component(namespace="HeTu", volatile=True)
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


def test_notify_declarations_default_off(new_component_env):
    """不声明时：不允许整表订阅、没有支持点订阅的索引"""

    @define_component(namespace="pytest", force=True)
    class PlainComp(BaseComponent):
        owner: np.int64 = property_field(0, index=True)

    assert PlainComp.table_sub_ is False
    assert PlainComp.point_subs_ == frozenset()


def test_point_sub_define(new_component_env, caplog):
    """point_sub 强制打开 index（同 unique）；显式 index=False 时警告并修正"""

    @define_component(namespace="pytest", force=True)
    class PointComp(BaseComponent):
        owner: np.int64 = property_field(0, point_sub=True)
        zone: np.int32 = property_field(0, index=True, point_sub=True)
        name: str = property_field("", unique=True, dtype="U8", point_sub=True)
        level: np.int32 = property_field(0, index=True)

    assert PointComp.point_subs_ == frozenset({"owner", "zone", "name"})
    assert {"owner", "zone", "name", "level"} <= set(PointComp.indexes_)
    assert "point_sub" not in caplog.text

    @define_component(namespace="pytest", force=True)
    class PointComp(BaseComponent):
        owner: np.int64 = property_field(0, index=False, point_sub=True)

    assert "point_sub" in caplog.text
    assert "owner" in PointComp.indexes_
    assert PointComp.point_subs_ == frozenset({"owner"})


def test_table_sub_define(new_component_env):
    @define_component(namespace="pytest", force=True, table_sub=True)
    class TableComp(BaseComponent):
        name: str = property_field("", dtype="U8")

    assert TableComp.table_sub_ is True


def test_notify_declarations_in_schema(new_component_env):
    """两个声明写进组件 schema：load_json、duplicate 还原；headless 按名字从 meta 还原也靠它"""
    import json

    @define_component(namespace="pytest", force=True, table_sub=True)
    class DeclComp(BaseComponent):
        owner: np.int64 = property_field(0, point_sub=True)
        level: np.int32 = property_field(0, index=True)

    data = json.loads(DeclComp.json_)
    assert data["table_sub"] is True
    assert data["properties"]["owner"]["point_sub"] is True
    assert data["properties"]["level"]["point_sub"] is False

    loaded = BaseComponent.load_json(DeclComp.json_)
    assert loaded.table_sub_ is True
    assert loaded.point_subs_ == frozenset({"owner"})

    copy = DeclComp.duplicate("pytest", "copy")
    assert copy.table_sub_ is True
    assert copy.point_subs_ == frozenset({"owner"})


def test_load_json_without_notify_keys(new_component_env):
    """迁移路径会读旧 meta：没有这两个键时按未声明处理"""
    import json

    @define_component(namespace="pytest", force=True, table_sub=True)
    class OldComp(BaseComponent):
        owner: np.int64 = property_field(0, point_sub=True)

    data = json.loads(OldComp.json_)
    del data["table_sub"]
    for prop in data["properties"].values():
        del prop["point_sub"]

    loaded = BaseComponent.load_json(json.dumps(data))
    assert loaded.table_sub_ is False
    assert loaded.point_subs_ == frozenset()
    assert "owner" in loaded.indexes_
