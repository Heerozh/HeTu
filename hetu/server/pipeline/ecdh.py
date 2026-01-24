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

# todo 实现标准河图加密解密
import logging
from typing import Any, override


from .pipeline import MessageProcessLayer, MsgType

logger = logging.getLogger("HeTu.root")
replay = logging.getLogger("HeTu.replay")


class CryptoLayer(MessageProcessLayer):
    """
    检查消息合法性的类
    """

    def __init__(self):
        super().__init__()

    @override
    def handshake(self, message: MsgType) -> tuple[Any, MsgType]:
        """
        连接前握手工作，例如协商参数等。
        返回的第一个值会保存在连接中，贯穿之后的encode/decode调用。
        返回的第二个值会发送给对端。
        """
        return None, b""

    @override
    def encode(self, layer_ctx: Any, message: MsgType) -> MsgType:
        """
        对消息进行正向处理
        """
        return message

    @override
    def decode(self, layer_ctx: Any, message: MsgType) -> MsgType:
        """
        对消息进行逆向处理
        """
        return message
