from typing import Any
import pytest
from hetu.server import pipeline
from hetu.data import define_component, property_field, BaseComponent, Permission
from hetu.system import SystemClusters, define_system, SystemContext


@pytest.fixture()
def base_pipeline(mod_item_model, mod_rls_test_model, new_clusters_env):
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

    @define_system(
        namespace="pytest",
        components=(Comp1, Comp2, Comp3, Comp4, mod_item_model, mod_rls_test_model),
        force=True,
    )
    async def do_nothing(ctx):
        pass

    SystemClusters().build_clusters("pytest")

    pipe = pipeline.MessagePipeline()
    pipe.add_layer(pipeline.LimitCheckerLayer())
    pipe.add_layer(pipeline.JSONBinaryLayer())
    return pipe


def test_handshake_returns_dict_message(base_pipeline):
    zstd_layer = pipeline.ZstdLayer(level=3)
    base_pipeline.add_layer(zstd_layer)
    # 测试握手返回字典消息
    ctx, msg = zstd_layer.handshake(b"")
    assert isinstance(msg, (bytes, bytearray))
    assert msg == zstd_layer.dict_message
    assert len(zstd_layer.dict_message) == zstd_layer.dict_size


def test_zstd_encode_decode_roundtrip(base_pipeline, mod_item_model):
    zstd_layer = pipeline.ZstdLayer(level=3)
    base_pipeline.add_layer(zstd_layer)

    pipeline.ZstdLayer(level=3)
    ctx, msg = base_pipeline.handshake([None, None, b""])

    # 只能接受dict
    row = mod_item_model.new_row(id_=123)
    with pytest.raises(AssertionError, match="dict"):
        base_pipeline.encode(ctx, row)

    # 压缩
    payload = BaseComponent.struct_to_dict(row)
    encoded = base_pipeline.encode(ctx, payload)
    assert isinstance(encoded, (bytes, bytearray))

    # 解压
    decoded = base_pipeline.decode(ctx, encoded)
    assert decoded == payload

    # 初始压缩比也就那样0.8~1.3之间
    assert 1.3 > zstd_layer.encode_ratio > 0.5
    print(f"Initial zstd encode ratio: {zstd_layer.encode_ratio}")

    for i in range(50):
        row = mod_item_model.new_row(id_=123 + i)
        payload = BaseComponent.struct_to_dict(row)
        encoded = base_pipeline.encode(ctx, payload)
        print(f"Zstd encode ratio after {i + 1} messages: {zstd_layer.encode_ratio}")

    # 流式压缩应该会随着滑动窗口的建立，压缩比越来越好
    assert 0.5 > zstd_layer.encode_ratio > 0.1


def test_zlib_encode_decode_roundtrip(base_pipeline, mod_item_model):
    zlib_layer = pipeline.ZlibLayer(level=6)
    base_pipeline.add_layer(zlib_layer)

    ctx, msg = base_pipeline.handshake([None, None, b""])
    row = mod_item_model.new_row(id_=123)

    # 压缩
    payload = BaseComponent.struct_to_dict(row)
    encoded = base_pipeline.encode(ctx, payload)
    assert isinstance(encoded, (bytes, bytearray))

    # 解压
    decoded = base_pipeline.decode(ctx, encoded)
    assert decoded == payload

    # 初始压缩比也就那样0.8~1.3之间
    assert 1.3 > zlib_layer.encode_ratio > 0.5
    print(f"Initial zstd encode ratio: {zlib_layer.encode_ratio}")

    for i in range(50):
        row = mod_item_model.new_row(id_=123 + i)
        payload: dict[str, Any] = BaseComponent.struct_to_dict(row)
        encoded = base_pipeline.encode(ctx, payload)
        print(f"Zstd encode ratio after {i + 1} messages: {zlib_layer.encode_ratio}")

    # 流式压缩应该会随着滑动窗口的建立，压缩比越来越好
    assert 0.5 > zlib_layer.encode_ratio > 0.1


def test_passthrough_without_ctx(base_pipeline):
    zstd_layer = pipeline.ZstdLayer(level=3)

    payload = ["not", "bytes"]
    assert zstd_layer.encode(None, payload) is payload
    assert zstd_layer.decode(None, payload) is payload


def test_encode_requires_bytes(base_pipeline):
    zstd_layer = pipeline.ZstdLayer(level=3)

    with pytest.raises(AssertionError):
        zstd_layer.encode(object(), [1, 2, 3])


def test_decode_requires_bytes(base_pipeline):
    zstd_layer = pipeline.ZstdLayer(level=3)

    with pytest.raises(AssertionError):
        zstd_layer.decode(object(), [1, 2, 3])
