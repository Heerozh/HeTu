import hashlib
import hmac
import logging
import zlib
from typing import Any

import msgspec
import nacl.bindings
import nacl.exceptions
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


def test_zlib_dict_negotiation(base_pipeline):
    """客户端进程没注册组件时字典和服务端不同，必须采纳握手时下发的服务端字典。"""
    server_layer = pipeline.ZlibLayer(level=6)
    client_layer = pipeline.ZlibLayer(level=6)
    # 模拟客户端进程（如 benchmark 脚本）没加载 app，字典退化成只有默认词
    client_layer.dict_message = b"updt"
    assert client_layer.dict_message != server_layer.dict_message

    server_ctx, reply = server_layer.handshake(b"")
    assert reply == server_layer.dict_message
    client_ctx, _ = client_layer.handshake(reply)

    msg = b'["rpc", "just_get", 123]' * 4
    assert server_layer.decode(server_ctx, client_layer.encode(client_ctx, msg)) == msg
    assert client_layer.decode(client_ctx, server_layer.encode(server_ctx, msg)) == msg

    # 反例：各用各的字典时，解压会在 DICTID 校验上失败
    unnegotiated_ctx, _ = client_layer.handshake(b"")
    fresh_server_ctx, _ = server_layer.handshake(b"")
    with pytest.raises(zlib.error, match="zdict"):
        server_layer.decode(
            fresh_server_ctx, client_layer.encode(unnegotiated_ctx, msg)
        )


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


# ---- Crypto 层对异常帧的拒绝 ----


def _crypto_pair():
    """握手好的一对加密上下文：(layer, 服务端ctx, 客户端ctx)"""
    layer = pipeline.CryptoLayer()
    client_private = PrivateKey.generate()
    server_ctx, server_pub = layer.handshake(client_private.public_key.encode())
    client_ctx = layer.client_handshake(client_private.encode(), server_pub)
    return layer, server_ctx, client_ctx


def test_crypto_handshake_rejects_empty_message(caplog):
    layer = pipeline.CryptoLayer()
    with (
        caplog.at_level(logging.WARNING, logger="HeTu.root"),
        pytest.raises(ValueError, match="unknown protocol"),
    ):
        layer.handshake(b"")
    assert "握手异常" in caplog.text


def test_crypto_decode_rejects_truncated_frame():
    """不够一个 Poly1305 Tag（16 字节）的帧直接拒绝，且不消耗 nonce：后面的正常帧照常解开"""
    layer, server_ctx, client_ctx = _crypto_pair()
    with pytest.raises(ValueError, match="数据长度不足"):
        layer.decode(server_ctx, b"\x00" * 15)
    assert layer.decode(server_ctx, layer.encode(client_ctx, b"hello")) == b"hello"


def test_crypto_decode_rejects_tampered_frame(caplog):
    layer, server_ctx, client_ctx = _crypto_pair()
    frame = bytearray(layer.encode(client_ctx, b"hello"))
    frame[0] ^= 0xFF
    with (
        caplog.at_level(logging.ERROR, logger="HeTu.root"),
        pytest.raises(nacl.exceptions.CryptoError),
    ):
        layer.decode(server_ctx, bytes(frame))
    assert "解密验证失败" in caplog.text


def test_crypto_decode_rejects_replayed_frame():
    """nonce 按帧递增：截获的帧原样重放解不开"""
    layer, server_ctx, client_ctx = _crypto_pair()
    frame = layer.encode(client_ctx, b"hello")
    assert layer.decode(server_ctx, frame) == b"hello"
    with pytest.raises(nacl.exceptions.CryptoError):
        layer.decode(server_ctx, frame)


# ---- JsonB 层的编解码失败 ----


def test_jsonb_decode_rejects_malformed_bytes(caplog):
    layer = pipeline.JSONBinaryLayer()
    with (
        caplog.at_level(logging.ERROR, logger="HeTu.root"),
        pytest.raises(msgspec.DecodeError),
    ):
        layer.decode(None, b"\xc1")  # msgpack 里从不使用的字节
    assert "JSONB反序列化失败" in caplog.text


def test_jsonb_encode_unsupported_type_raises(caplog):
    layer = pipeline.JSONBinaryLayer()
    with caplog.at_level(logging.ERROR, logger="HeTu.root"), pytest.raises(TypeError):
        layer.encode(None, ["rsp", object()])
    assert "JSONB序列化失败" in caplog.text


# ---- 禁用层 ----


def _jsonb_zlib_pipe():
    """jsonb + zlib 两层的管道和握手好的 pipe_ctx"""
    pipe = pipeline.MessagePipeline()
    pipe.add_layer(pipeline.JSONBinaryLayer())
    zlib_layer = pipeline.ZlibLayer()
    pipe.add_layer(zlib_layer)
    zlib_ctx, _ = zlib_layer.handshake(b"")
    return pipe, [None, zlib_ctx]


def test_disabled_layer_skipped_in_encode():
    """禁用压缩层后 encode 的输出就是 jsonb 那层的结果"""
    pipe, ctx = _jsonb_zlib_pipe()
    msg = ["rpc", "login", 1]
    assert pipe.encode(ctx, msg) != msgspec.msgpack.encode(msg)  # 没禁用时压缩了
    pipe.disable_layer(1)
    assert pipe.encode(ctx, msg) == msgspec.msgpack.encode(msg)


def test_disabled_layer_skipped_in_decode():
    """decode 倒序遍历各层时按层原本的下标判断禁用，跳过的是被禁用的那层本身"""
    pipe, ctx = _jsonb_zlib_pipe()
    msg = ["rpc", "login", 1]
    pipe.disable_layer(1)
    assert pipe.decode(ctx, msgspec.msgpack.encode(msg)) == msg


def test_disabled_layer_after_handshake():
    """禁用中间的压缩层：禁用层不参与握手（客户端也不发它的握手消息，与 C# SDK 一致），
    pipe_ctx 仍按层下标对齐，之后照常收发"""
    server = pipeline.MessagePipeline()
    server.add_layer(pipeline.JSONBinaryLayer())
    server.add_layer(pipeline.ZlibLayer())
    crypto = pipeline.CryptoLayer()
    server.add_layer(crypto)
    server.disable_layer(1)
    # 服务端按它校验客户端握手消息的条数：只剩加密层
    assert server.num_handshake_layers == 1

    client_private = PrivateKey.generate()
    pipe_ctx, reply = server.handshake([client_private.public_key.encode()])
    (server_pub,) = msgspec.msgpack.decode(reply)
    client_ctx = crypto.client_handshake(client_private.encode(), server_pub)

    msg = ["rsp", "ok"]
    # 客户端 → 服务端
    frame = crypto.encode(client_ctx, msgspec.msgpack.encode(msg))
    assert server.decode(pipe_ctx, frame) == msg
    # 服务端 → 客户端
    frame = server.encode(pipe_ctx, msg)
    assert msgspec.msgpack.decode(crypto.decode(client_ctx, frame)) == msg


def test_clean_resets_disabled_layers():
    """clean 后重新加的层不继承之前的禁用标记（服务端的管道是单件，每次 worker_main
    都会 clean 再重新加层）"""
    pipe, _ctx = _jsonb_zlib_pipe()
    pipe.disable_layer(1)
    pipe.clean()
    assert pipe.num_layers == 0 and pipe.num_handshake_layers == 0

    pipe.add_layer(pipeline.JSONBinaryLayer())
    zlib_layer = pipeline.ZlibLayer()
    pipe.add_layer(zlib_layer)
    zlib_ctx, _ = zlib_layer.handshake(b"")
    msg = ["rpc", "login", 1]
    assert pipe.num_handshake_layers == 1
    # 压缩层照常生效，没被当成禁用
    assert pipe.encode([None, zlib_ctx], msg) != msgspec.msgpack.encode(msg)


@pytest.mark.parametrize("server_side", [False, True])
@pytest.mark.parametrize("size", [0, 1, 25, 1024, 65536])
def test_crypto_wire_compatible_with_pynacl(server_side, size):
    """逐字节兼容旧实现：双方向、多包计数、空包和大包，不仅是自身 roundtrip。"""
    layer = pipeline.CryptoLayer()
    key = bytes(range(32))
    ctx = layer.CryptoContext(key, server_side, 0, 0)
    payload = (bytes(range(256)) * (size // 256 + 1))[:size]
    for counter in (1, 2, 3):
        send_nonce = (b"\x00" if server_side else b"\xff") + counter.to_bytes(11)
        recv_nonce = (b"\xff" if server_side else b"\x00") + counter.to_bytes(11)
        expected = nacl.bindings.crypto_aead_chacha20poly1305_ietf_encrypt(
            payload, None, send_nonce, key
        )
        assert layer.encode(ctx, payload) == expected
        incoming = nacl.bindings.crypto_aead_chacha20poly1305_ietf_encrypt(
            payload, None, recv_nonce, key
        )
        assert layer.decode(ctx, incoming) == payload


def test_crypto_rejects_wrong_key_and_direction():
    layer = pipeline.CryptoLayer()
    key = b"a" * 32
    for peer_key, peer_side in ((b"b" * 32, False), (key, True)):
        receiver = layer.CryptoContext(key, True, 0, 0)
        sender = layer.CryptoContext(peer_key, peer_side, 0, 0)
        frame = layer.encode(sender, b"hello")
        with pytest.raises(nacl.exceptions.CryptoError):
            layer.decode(receiver, frame)


def test_crypto_auth_failure_logs_reason(caplog):
    """认证失败的错误日志要带原因：cryptography 的 InvalidTag 没有消息，不能打出空原因"""
    layer = pipeline.CryptoLayer()
    receiver = layer.CryptoContext(b"a" * 32, True, 0, 0)
    sender = layer.CryptoContext(b"b" * 32, False, 0, 0)
    frame = layer.encode(sender, b"hello")
    with (
        caplog.at_level(logging.ERROR, logger="HeTu.root"),
        pytest.raises(nacl.exceptions.CryptoError),
    ):
        layer.decode(receiver, frame)
    errors = [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR]
    assert len(errors) == 1
    assert "Decryption failed." in errors[0]


def test_crypto_nonce_overflow_does_not_wrap():
    layer = pipeline.CryptoLayer()
    ctx = layer.CryptoContext(b"a" * 32, True, (1 << 88) - 1, (1 << 88) - 1)
    with pytest.raises(OverflowError):
        layer.encode(ctx, b"hello")
    with pytest.raises(OverflowError):
        layer.decode(ctx, b"x" * 16)
