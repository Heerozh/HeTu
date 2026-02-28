#  """
#  @author: Heerozh (Zhang Jianhao)
#  @copyright: Copyright 2024, Heerozh. All rights reserved.
#  @license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
#  @email: heeroz@gmail.com
#  """

import numpy as np
from types import SimpleNamespace

from hetu.data import BaseComponent, Permission, define_component, property_field
from hetu.sourcegen.csharp import generate_component, generate_all_components
from hetu.system import SystemClusters


def test_component_csharp_gen():
    @define_component(namespace="HeTu", volatile=True, permission=Permission.ADMIN)
    class TestComponent(BaseComponent):
        int64b: ">i8" = property_field(0)
        int64l: "<i8" = property_field(0)
        float64: np.float64 = property_field(0.0)
        str7: "U7" = property_field("")
        int16: np.int16 = property_field(0)
        uint8: np.uint8 = property_field(0)
        uint64: np.uint64 = property_field(0)
        float16: np.float16 = property_field(0.0)
        bool_: bool = property_field(False)

    code = "\n".join(generate_component(TestComponent))

    expect = """
[MessagePackObject]
public class TestComponent : IBaseComponent
{
    [Key("id")] public long ID { get; set; }
    [Key("bool_")] public sbyte bool_;
    [Key("float16")] public float float16;
    [Key("float64")] public double float64;
    [Key("int16")] public short int16;
    [Key("int64b")] public long int64b;
    [Key("int64l")] public long int64l;
    [Key("str7")] public string str7;
    [Key("uint64")] public ulong uint64;
    [Key("uint8")] public byte uint8;
}
"""
    assert code == expect


def test_generate_all_components_latest_template(tmp_path, monkeypatch):
    @define_component(namespace="HeTu", volatile=True, force=True)
    class TestA(BaseComponent):
        owner: np.int64 = property_field(0)

    @define_component(namespace="HeTu", volatile=True, force=True)
    class TestB(BaseComponent):
        value: np.float32 = property_field(0)

    monkeypatch.setattr(
        SystemClusters(),
        "get_clusters",
        lambda _namespace: [
            SimpleNamespace(components=[TestA]),
            SimpleNamespace(components=[TestA, TestB]),
        ],
    )

    output = tmp_path / "Components.cs"
    generate_all_components("demo_ns", str(output))
    code = output.read_text(encoding="utf-8")

    assert "using MessagePack;" in code
    assert "namespace demo_ns" in code
    assert code.count("public class TestA : IBaseComponent") == 1
    assert code.count("public class TestB : IBaseComponent") == 1
    assert '[Key("id")] public long ID { get; set; }' in code
