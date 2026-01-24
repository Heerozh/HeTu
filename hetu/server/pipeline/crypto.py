"""
Python 端使用 nacl (PyNaCl) 库，Unity 端使用 Sodium 的 C# 绑定（libsodium）。

握手： 连接时 ECDH 协商出 Session Key。
发送： Python 生成 JSON -> 去掉 Key 转 Array -> zstd 压缩 -> ChaCha20-Poly1305 加密 -> 发送。
接收： Unity 接收 -> 解密 (Poly1305 验证失败直接断开) -> zstd 解压 -> 还原数据。
构建： Unity 必须开启 IL2CPP。
混淆： 购买或使用开源的 C# 代码混淆器，重点混淆网络解密部分的类名和方法名。
        Metadata 混淆：
            虽然 IL2CPP 很难读，但此时函数名、类名还在 global-metadata.dat 里。
            使用工具（如 Il2CppDumper 的对抗工具，或者商业混淆插件如 BeeByte）混淆代码结构，把 DecryptData() 这种函数名变成 A() 或者乱码。
"""

import logging
from dataclasses import dataclass
from typing import Any, override

import nacl.bindings
import nacl.encoding
import nacl.hash
import nacl.utils
from nacl.public import PrivateKey, PublicKey

from .pipeline import JSONType, MessageProcessLayer

logger = logging.getLogger("HeTu.root")
replay = logging.getLogger("HeTu.replay")


class CryptoLayer(MessageProcessLayer):
    """
    加密层
    使用 ECDH (Curve25519) 协商密钥，使用 ChaCha20-Poly1305-IETF 进行加密通讯。
    依赖库: PyNaCl (pip install pynacl)
    """

    # ChaCha20-Poly1305-IETF 用 96-bit (12 bytes) nonce
    NONCE_SIZE = 12

    @dataclass
    class CryptoContext:
        session_key: bytes
        server_side: bool
        send_nonce: int
        recv_nonce: int

    def __init__(self):
        super().__init__()

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
            # 客户端连接后，应当首先发送它的 Curve25519 Link Public Key
            if not message or len(message) != 32:
                # 长度不对，或者为空，视为非法握手
                # 注意：如果 message 为空且您希望支持服务端先发送 PubKey 模式，需修改此处逻辑。
                # 但根据通常 ECDH 流程及 "协商出 Session Key" 描述，假设 Client 先发。
                raise ValueError(
                    f"握手失败：客户端公钥长度错误，预期32字节，实际收到 {len(message) if message else 0} 字节"
                )

            # 1. 解析客户端公钥
            peer_public_key = PublicKey(message)

            # 生成服务端临时密钥对 (Ephemeral Key Pair)
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
            logger.warning(f"⚠️ [📡Pipeline] [Crypto层] 握手异常: {e}")
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
            err_msg = (
                f"解密失败：数据长度不足 (len={len(message)})，可能非加密数据或截断"
            )
            logger.warning(f"⚠️ [📡Pipeline] [Crypto层] {err_msg}")
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
                f"❌ [📡Pipeline] [Crypto层] 解密验证失败，断开连接。原因: {e}"
            )
            raise
