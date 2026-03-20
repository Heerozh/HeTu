"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import hashlib
import hmac
import logging
from dataclasses import dataclass
from typing import Any, override

import nacl.bindings
import nacl.encoding
import nacl.hash
import nacl.utils
from nacl.public import PrivateKey, PublicKey

from ...i18n import _
from .pipeline import JSONType, MessageProcessLayer

logger = logging.getLogger("HeTu.root")
replay = logging.getLogger("HeTu.replay")


class CryptoLayer(MessageProcessLayer, alias="crypto"):
    """
    加密层
    使用 ECDH (Curve25519) 协商密钥，使用 ChaCha20-Poly1305-IETF 进行加密通讯。
    """

    # ChaCha20-Poly1305-IETF 用 96-bit (12 bytes) nonce
    NONCE_SIZE = 12
    LEGACY_HELLO_SIZE = 32
    SIGNED_HELLO_MAGIC = b"H2A1"
    SIGNED_HELLO_SIZE = 92

    @dataclass
    class CryptoContext:
        session_key: bytes
        server_side: bool
        send_nonce: int
        recv_nonce: int

        def __repr__(self) -> str:
            return f"CryptoContext('{self.session_key.hex()[:8]}...')"

    def __init__(self, auth_key: str | bytes | None = None):
        super().__init__()
        if isinstance(auth_key, str):
            auth_key = auth_key.encode("utf-8")
        self._auth_key = auth_key or None

    def _parse_client_public_key(self, message: bytes) -> bytes:
        if len(message) == self.LEGACY_HELLO_SIZE:
            if self._auth_key is not None:
                raise ValueError("unknown protocol")
            return message

        if (
            len(message) == self.SIGNED_HELLO_SIZE
            and message[:4] == self.SIGNED_HELLO_MAGIC
        ):
            payload = message[:-32]
            signature = message[-32:]
            client_public_key = message[4:36]
            if self._auth_key is not None:
                expected = hmac.new(self._auth_key, payload, hashlib.sha256).digest()
                if not hmac.compare_digest(signature, expected):
                    raise ValueError("unknown protocol")
            return client_public_key

        raise ValueError("unknown protocol")

    def client_handshake(self, client_pvt: bytes, server_pub: bytes) -> CryptoContext:
        """
        客户端握手辅助函数。
        """
        # 1. 解析双方密钥
        peer_public_key = PublicKey(server_pub)
        my_private_key = PrivateKey(client_pvt)

        # 2. ECDH: 计算共享点 (Shared Point)
        # 在数学上，ECDH 本质上就是标量乘法 (Scalar Multiplication)
        shared_point = nacl.bindings.crypto_scalarmult(
            my_private_key.encode(),  # 转为 bytes
            peer_public_key.encode(),  # 转为 bytes
        )

        # 4. KDF: 派生会话密钥 (Session Key)
        # 直接使用共享点作为密钥并不总是安全的（虽然Curve25519通常可以），
        # 推荐使用 Hash 函数通过共享点派生出会话密钥。这里使用 Blake2b。
        session_key = nacl.hash.blake2b(
            shared_point, digest_size=32, encoder=nacl.encoding.RawEncoder
        )

        # 返回 Session Key 作为 Context，以及服务端的公钥给客户端
        ctx = self.CryptoContext(session_key, False, 0, 0)
        return ctx

    @override
    def handshake(self, message: bytes) -> tuple[Any, bytes]:
        """
        连接前握手工作。
        预期 message 为客户端的 Public Key (32 bytes)。
        返回的第一个值(SessionKey)会保存在连接中，贯穿之后的encode/decode调用。
        返回的第二个值(ServerPublicKey)会发送给客户端。
        """
        try:
            if not message:
                raise ValueError("unknown protocol")

            client_public_key = self._parse_client_public_key(message)

            # 1. 解析客户端公钥
            peer_public_key = PublicKey(client_public_key)

            # 2. 生成服务端临时密钥对 (Ephemeral Key Pair)
            private_key = PrivateKey.generate()
            public_key = private_key.public_key

            # 3. ECDH: 计算共享点 (Shared Point)
            # 在数学上，ECDH 本质上就是标量乘法 (Scalar Multiplication)
            shared_point = nacl.bindings.crypto_scalarmult(
                private_key.encode(),  # 转为 bytes
                peer_public_key.encode(),  # 转为 bytes
            )

            # 4. KDF: 派生会话密钥 (Session Key)
            # 直接使用共享点作为密钥并不总是安全的（虽然Curve25519通常可以），
            # 推荐使用 Hash 函数通过共享点派生出会话密钥。这里使用 Blake2b。
            session_key = nacl.hash.blake2b(
                shared_point, digest_size=32, encoder=nacl.encoding.RawEncoder
            )

            # 返回 Session Key 作为 Context，以及服务端的公钥给客户端
            ctx = self.CryptoContext(session_key, True, 0, 0)
            return ctx, public_key.encode()

        except Exception as e:
            logger.warning(_("⚠️ [📡Pipeline] [Crypto层] 握手异常: {err}").format(err=e))
            raise

    @override
    def encode(
        self, layer_ctx: CryptoContext | None, message: JSONType | bytes
    ) -> JSONType | bytes:
        """
        发送消息时调用：加密
        输入: 明文 bytes (通常是 zstd 压缩后的数据)
        输出: [Nonce(12)] + [Ciphertext + Tag]
        """
        # 如果没有握手成功或者不需要加密，layer_ctx 为空
        if not layer_ctx:
            return message

        assert isinstance(message, bytes), "CryptoLayer只能加密bytes类型数据"

        # 1. 生成随机 Nonce
        # 对于 ChaCha20-Poly1305，Nonce 必须对每个 key 唯一。
        # 这里使用随机 Nonce。对于12字节Nonce，随机碰撞概率极低，足以应付长连接。
        # nonce = nacl.utils.random(self.NONCE_SIZE)
        # 这里用简单的递增 Nonce，避免随机碰撞风险
        layer_ctx.send_nonce += 1
        sign = b"\x00" if layer_ctx.server_side else b"\xff"
        nonce = sign + layer_ctx.send_nonce.to_bytes(
            self.NONCE_SIZE - 1, byteorder="big"
        )
        # print(id(self), f"encode 使用的nonce: {sign} + {layer_ctx.send_nonce}")
        # 2. 加密 (ChaCha20-Poly1305-IETF)
        # 结果包含 Ciphertext 和 Poly1305 MAC Tag
        encrypted = nacl.bindings.crypto_aead_chacha20poly1305_ietf_encrypt(
            message,
            None,  # Additional Authenticated Data (AAD)，这里不用
            nonce,
            layer_ctx.session_key,
        )

        # 3. 拼接: Nonce放头部发送给对方用于解密
        # return nonce + encrypted
        # 直接返回无Nonce版本
        return encrypted

    @override
    def decode(
        self, layer_ctx: CryptoContext | None, message: JSONType | bytes
    ) -> JSONType | bytes:
        """
        接收消息时调用：解密
        输入: [Nonce(12)] + [Ciphertext + Tag]
        输出: 明文 bytes
        """
        if not layer_ctx:
            return message

        assert isinstance(message, bytes), "CryptoLayer只能解密bytes类型数据"

        # 检查最小长度: Nonce(12) + Tag(16) = 28 bytes
        # 实际上空消息加密后也有 Tag，所以长度至少是 NONCE_SIZE + 16
        # min_len = self.NONCE_SIZE + 16
        # 去掉NONCE SIZE
        min_len = 16
        if len(message) < min_len:
            err_msg = _(
                "解密失败：数据长度不足 (len={length})，可能非加密数据或截断"
            ).format(length=len(message))
            logger.warning(
                _("⚠️ [📡Pipeline] [Crypto层] {err_msg}").format(err_msg=err_msg)
            )
            raise ValueError(err_msg)

        # 1. 提取 Nonce
        # nonce = message[: self.NONCE_SIZE]
        # ciphertext = message[self.NONCE_SIZE :]
        # 这里用简单的递增 Nonce，避免随机碰撞风险
        layer_ctx.recv_nonce += 1
        sign = b"\xff" if layer_ctx.server_side else b"\x00"
        nonce = sign + layer_ctx.recv_nonce.to_bytes(
            self.NONCE_SIZE - 1, byteorder="big"
        )
        # print(id(self), f"decode 使用的nonce: {sign} + {layer_ctx.recv_nonce}")
        try:
            # 2. 解密 & 验证
            # 如果 Tag 验证失败，这里会抛出 nacl.exceptions.CryptoError
            decrypted = nacl.bindings.crypto_aead_chacha20poly1305_ietf_decrypt(
                message,
                None,  # AAD
                nonce,
                layer_ctx.session_key,
            )
            return decrypted

        except Exception as e:
            # 严重安全警告：解密/验证失败意味着数据可能被篡改或密钥不匹配
            logger.error(
                _(
                    "❌ [📡Pipeline] [Crypto层] 解密验证失败，断开连接。原因: {err}"
                ).format(err=e)
            )
            raise
