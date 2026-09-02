import hashlib
import hmac
import logging
from typing import Any

import pytest
from nacl.public import PrivateKey

from hetu.data import BaseComponent, Permission, define_component, property_field
from hetu.server import pipeline
from hetu.server.pipeline.brotli import BrotliLayer
from hetu.system import SystemClusters, SystemContext, define_system


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
    ctx, msg = base_pipeline.handshake([b""])

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

    ctx, msg = base_pipeline.handshake([b""])
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
    print(f"Initial zlib encode ratio: {zlib_layer.encode_ratio}")

    for i in range(50):
        row = mod_item_model.new_row(id_=123 + i)
        payload: dict[str, Any] = BaseComponent.struct_to_dict(row)
        encoded = base_pipeline.encode(ctx, payload)
        print(f"zlib encode ratio after {i + 1} messages: {zlib_layer.encode_ratio}")

    # 流式压缩应该会随着滑动窗口的建立，压缩比越来越好
    assert 0.5 > zlib_layer.encode_ratio > 0.1


def test_brotli_encode_decode_roundtrip(base_pipeline, mod_item_model):
    brotli_layer: BrotliLayer = pipeline.BrotliLayer(quality=4)
    base_pipeline.add_layer(brotli_layer)

    ctx, msg = base_pipeline.handshake([b""])
    row = mod_item_model.new_row(id_=123)

    # 压缩
    payload = BaseComponent.struct_to_dict(row)
    encoded = base_pipeline.encode(ctx, payload)
    assert isinstance(encoded, (bytes, bytearray))

    # 解压
    decoded = base_pipeline.decode(ctx, encoded)
    assert decoded == payload

    # 初始压缩比也就那样0.8~1.3之间
    assert 1.3 > brotli_layer.encode_ratio > 0.5
    print(f"Initial zstd encode ratio: {brotli_layer.encode_ratio}")

    for i in range(50):
        row = mod_item_model.new_row(id_=123 + i)
        payload: dict[str, Any] = BaseComponent.struct_to_dict(row)
        encoded = base_pipeline.encode(ctx, payload)
        print(
            f"Brotli encode ratio after {i + 1} messages: {brotli_layer.encode_ratio}"
        )

    # 流式压缩应该会随着滑动窗口的建立，压缩比越来越好
    assert 0.5 > brotli_layer.encode_ratio > 0.1


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


def _build_signed_hello(public_key: bytes, auth_key: bytes) -> bytes:
    magic = b"H2A1"
    timestamp = (0).to_bytes(8, byteorder="big")
    nonce = b"\x01" * 16
    payload = magic + public_key + timestamp + nonce
    signature = hmac.new(auth_key, payload, hashlib.sha256).digest()
    return payload + signature


def test_crypto_handshake_accepts_signed_when_auth_key_matches():
    layer = pipeline.CryptoLayer(auth_key="secret")
    client_private = PrivateKey.generate()
    client_public = client_private.public_key.encode()
    hello = _build_signed_hello(client_public, b"secret")

    ctx, server_pub = layer.handshake(hello)
    assert isinstance(ctx, pipeline.CryptoLayer.CryptoContext)
    assert isinstance(server_pub, bytes)
    assert len(server_pub) == 32


def test_crypto_handshake_rejects_signed_when_auth_key_mismatch():
    layer = pipeline.CryptoLayer(auth_key="secret")
    client_private = PrivateKey.generate()
    client_public = client_private.public_key.encode()
    hello = _build_signed_hello(client_public, b"wrong-secret")

    with pytest.raises(ValueError, match="unknown protocol"):
        layer.handshake(hello)


def test_crypto_handshake_accepts_legacy_when_auth_key_not_configured():
    layer = pipeline.CryptoLayer()
    client_private = PrivateKey.generate()
    client_public = client_private.public_key.encode()

    ctx, server_pub = layer.handshake(client_public)
    assert isinstance(ctx, pipeline.CryptoLayer.CryptoContext)
    assert len(server_pub) == 32


def test_crypto_handshake_ignores_signed_key_when_server_has_no_auth_key():
    layer = pipeline.CryptoLayer()
    client_private = PrivateKey.generate()
    client_public = client_private.public_key.encode()
    hello = _build_signed_hello(client_public, b"not-used")

    ctx, server_pub = layer.handshake(hello)
    assert isinstance(ctx, pipeline.CryptoLayer.CryptoContext)
    assert len(server_pub) == 32


# ---- auth_key 脱敏展示（mask_auth_key） ----


def test_mask_auth_key_middle_truncation():
    # 首尾各 4 位真实字符，中间打码（Stripe 式）
    assert pipeline.CryptoLayer.mask_auth_key("1a2bXXXXXXX3c4d") == "1a2b*******3c4d"


def test_mask_auth_key_short_key_fully_masked():
    # 太短（<8）的 key 全部打码，避免露头尾就暴露大半内容
    assert pipeline.CryptoLayer.mask_auth_key("secret") == "******"
    masked = pipeline.CryptoLayer.mask_auth_key("password")  # 正好 8 位
    assert masked != "password"
    assert set(masked.strip("*")) and "*" in masked  # 露一点、留星号


def test_mask_auth_key_str_and_bytes_match():
    assert pipeline.CryptoLayer.mask_auth_key("samevalue123") == (
        pipeline.CryptoLayer.mask_auth_key(b"samevalue123")
    )


def test_mask_auth_key_distinguishes_different_keys():
    # 不同 key 的脱敏结果不同，便于跨服务器辨识
    assert pipeline.CryptoLayer.mask_auth_key(
        "alpha-key-1234"
    ) != pipeline.CryptoLayer.mask_auth_key("bravo-key-5678")


def test_mask_auth_key_empty_returns_empty():
    assert pipeline.CryptoLayer.mask_auth_key(None) == ""
    assert pipeline.CryptoLayer.mask_auth_key("") == ""


def test_mask_auth_key_never_leaks_full_key():
    key = "server-secret-key-1234"
    masked = pipeline.CryptoLayer.mask_auth_key(key)
    assert key not in masked
    assert "*" in masked


# ---- 握手失败诊断（HandshakeError.diagnostic） ----


def test_handshake_error_is_value_error_with_vague_str():
    err = pipeline.CryptoLayer.HandshakeError("详细原因")
    assert isinstance(err, ValueError)
    assert str(err) == "unknown protocol"  # 对外含糊
    assert err.diagnostic == "详细原因"


def test_crypto_handshake_mismatch_diagnostic_mentions_auth_key():
    layer = pipeline.CryptoLayer(auth_key="server-secret-key")
    client_public = PrivateKey.generate().public_key.encode()
    hello = _build_signed_hello(client_public, b"client-other-key")

    with pytest.raises(pipeline.CryptoLayer.HandshakeError) as exc_info:
        layer.handshake(hello)
    assert str(exc_info.value) == "unknown protocol"
    assert "auth_key" in exc_info.value.diagnostic


def test_crypto_handshake_unsigned_client_with_server_authkey_diagnostic():
    layer = pipeline.CryptoLayer(auth_key="server-secret-key")
    client_public = PrivateKey.generate().public_key.encode()  # 32 字节，未签名

    with pytest.raises(pipeline.CryptoLayer.HandshakeError) as exc_info:
        layer.handshake(client_public)
    assert "签名" in exc_info.value.diagnostic


def test_crypto_handshake_malformed_diagnostic():
    layer = pipeline.CryptoLayer(auth_key="server-secret-key")

    with pytest.raises(pipeline.CryptoLayer.HandshakeError) as exc_info:
        layer.handshake(b"\x00\x01\x02garbage")
    assert "格式" in exc_info.value.diagnostic


def test_crypto_handshake_failure_logs_masked_key_not_plaintext(caplog):
    key = "server-secret-key-1234"
    layer = pipeline.CryptoLayer(auth_key=key)
    client_public = PrivateKey.generate().public_key.encode()
    hello = _build_signed_hello(client_public, b"client-other-key-9999")

    with caplog.at_level(logging.WARNING, logger="HeTu.root"):
        with pytest.raises(ValueError):
            layer.handshake(hello)

    assert pipeline.CryptoLayer.mask_auth_key(key) in caplog.text
    assert key not in caplog.text  # 完整明文绝不进日志
