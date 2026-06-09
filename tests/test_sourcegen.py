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
    [Key("bool_")] public sbyte Bool;
    [Key("float16")] public float Float16;
    [Key("float64")] public double Float64;
    [Key("int16")] public short Int16;
    [Key("int64b")] public long Int64b;
    [Key("int64l")] public long Int64l;
    [Key("str7")] public string Str7;
    [Key("uint64")] public ulong Uint64;
    [Key("uint8")] public byte Uint8;
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


def test_generate_all_components_order_independent(tmp_path, monkeypatch):
    # 真实路径里 Cluster.components 是 set，其对类对象的迭代顺序按 id() 哈希，
    # 每个进程可能不同。生成内容必须与输入顺序无关，否则每次 build 内容都变，
    # 触发客户端（Unity）无谓重编译。
    @define_component(namespace="HeTu", volatile=True, force=True)
    class Alpha(BaseComponent):
        value: np.int64 = property_field(0)

    @define_component(namespace="HeTu", volatile=True, force=True)
    class Bravo(BaseComponent):
        value: np.int64 = property_field(0)

    out1 = tmp_path / "Order1.cs"
    monkeypatch.setattr(
        SystemClusters(),
        "get_clusters",
        lambda _namespace: [SimpleNamespace(components=[Alpha, Bravo])],
    )
    generate_all_components("demo_ns", str(out1))

    out2 = tmp_path / "Order2.cs"
    monkeypatch.setattr(
        SystemClusters(),
        "get_clusters",
        lambda _namespace: [SimpleNamespace(components=[Bravo, Alpha])],
    )
    generate_all_components("demo_ns", str(out2))

    assert out1.read_text(encoding="utf-8") == out2.read_text(encoding="utf-8")


def test_generate_all_components_skips_write_when_unchanged(
    tmp_path, monkeypatch, capsys
):
    # 内容不变时不应重写文件（保持 mtime），避免触发客户端重编译。
    @define_component(namespace="HeTu", volatile=True, force=True)
    class Skippy(BaseComponent):
        value: np.int64 = property_field(0)

    monkeypatch.setattr(
        SystemClusters(),
        "get_clusters",
        lambda _namespace: [SimpleNamespace(components=[Skippy])],
    )

    output = tmp_path / "Components.cs"
    generate_all_components("demo_ns", str(output))
    first = output.read_text(encoding="utf-8")
    mtime_before = output.stat().st_mtime_ns
    capsys.readouterr()  # 清空首次输出

    generate_all_components("demo_ns", str(output))

    assert "跳过写入" in capsys.readouterr().out
    assert output.read_text(encoding="utf-8") == first
    assert output.stat().st_mtime_ns == mtime_before  # 未重写


def test_generate_all_components_dedup_duplicates(tmp_path, monkeypatch):
    # 副本(duplicate)的 name_ 带冒号(如 ChatMessage:Universe)，是非法 C# 类名。
    # 生成时应统一映射到 master 去重，只输出一个干净的 master 类。
    @define_component(namespace="HeTu", volatile=True, force=True)
    class ChatMessage(BaseComponent):
        owner: np.int64 = property_field(0)

    dup = ChatMessage.duplicate("HeTu", "Universe")
    assert dup.name_ == "ChatMessage:Universe"

    monkeypatch.setattr(
        SystemClusters(),
        "get_clusters",
        lambda _namespace: [
            SimpleNamespace(components=[ChatMessage]),
            SimpleNamespace(components=[dup]),
        ],
    )

    output = tmp_path / "Components.cs"
    generate_all_components("demo_ns", str(output))
    code = output.read_text(encoding="utf-8")

    # 副本不能生成带冒号的非法 C# 类
    assert "ChatMessage:Universe" not in code
    # 只输出一个干净的 master 类
    assert code.count("public class ChatMessage : IBaseComponent") == 1
