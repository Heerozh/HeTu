"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import logging
from dataclasses import dataclass
from typing import Any, override

import zlib

from .pipeline import JSONType, MessageProcessLayer

logger = logging.getLogger("HeTu.root")
replay = logging.getLogger("HeTu.replay")


class ZlibLayer(MessageProcessLayer):
    """
    使用 zlib 进行消息的流式压缩和解压缩。

    注意：zlib 的字典功能依赖预共享字典，当前实现不做字典训练/协商。
    """

    @dataclass
    class ZlibContext:
        compressor: zlib._Compress
        decompressor: zlib._Decompress

    def __init__(self, level: int = 3, wbits: int = zlib.MAX_WBITS):
        """
        Parameters
        ----------
        level
                zlib 压缩级别，范围 0-9，zlib标准6, 但较慢。
        wbits
                窗口大小/数据格式控制，默认 zlib.MAX_WBITS。
        """
        super().__init__()
        self.level = level
        self.wbits = wbits
        self.encode_count = 0
        self.encode_ratio = 0.0
        self.dict_message: bytes = self._build_dict_from_keys()

    @staticmethod
    def _build_dict_from_keys() -> bytes:
        from ...common import Permission
        from ...system import SystemClusters

        keys: set[str] = set()
        for comp, _ in SystemClusters().get_components().items():
            if comp.permission_ == Permission.ADMIN:
                continue
            keys.update(comp.dtype_map_.keys())

        if not keys:
            return b""

        # 用分隔符拼接形成字典内容，重复模式更易被 zlib 利用
        joined = "\n".join(sorted(keys))
        return joined.encode("utf-8")

    @override
    def handshake(self, message: bytes) -> tuple[Any, bytes]:
        """
        连接前握手工作。
        zlib 不做字典协商，忽略 message 并返回空字节。
        """
        zdict = self.dict_message

        ctx = self.ZlibContext(
            compressor=zlib.compressobj(
                self.level, zlib.DEFLATED, self.wbits, zdict=zdict
            ),
            decompressor=zlib.decompressobj(self.wbits, zdict=zdict),
        )
        return ctx, self.dict_message or b""

    @override
    def encode(self, layer_ctx: Any, message: JSONType | bytes) -> JSONType | bytes:
        """
        对消息进行正向处理（流式压缩）
        """
        if not layer_ctx:
            return message

        assert type(message) is bytes, "ZlibCompressor 只能压缩 bytes 类型的消息"

        # Z_SYNC_FLUSH 保持流式语义，确保对端及时解压
        chunk = layer_ctx.compressor.compress(message)
        chunk += layer_ctx.compressor.flush(zlib.Z_SYNC_FLUSH)

        ratio = len(chunk) / len(message) if len(message) > 0 else 1.0
        self.encode_count += 1
        self.encode_ratio += (ratio - self.encode_ratio) / self.encode_count
        return chunk

    @override
    def decode(self, layer_ctx: Any, message: JSONType | bytes) -> JSONType | bytes:
        """
        对消息进行逆向处理（流式解压）
        """
        if not layer_ctx:
            return message

        assert type(message) is bytes, "ZlibDecompressor 只能解压 bytes 类型的消息"

        try:
            return layer_ctx.decompressor.decompress(message)
        except Exception as e:
            logger.exception(
                f"❌ [📡Pipeline] [Zlib层] 解压失败，异常：{type(e).__name__}:{e}"
            )
            raise
