"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import logging
from dataclasses import dataclass
from typing import Any, override

import brotli

from .pipeline import JSONType, MessageProcessLayer

logger = logging.getLogger("HeTu.root")
replay = logging.getLogger("HeTu.replay")


class BrotliLayer(MessageProcessLayer):
    """
    使用 Brotli 进行消息的流式压缩和解压缩。

    注意：Brotli 的 Python 包当前不支持自定义字典训练/协商，
    这里仅在握手时附带所有 key 字典，供对端感知或未来扩展。
    """

    @dataclass
    class BrotliContext:
        compressor: Any
        decompressor: Any

    def __init__(
        self,
        quality: int = 4,
        lgwin: int = 22,
        lgblock: int = 0,
        mode: int = brotli.MODE_GENERIC,
    ):
        """
        Parameters
        ----------
        quality
                Brotli 压缩级别，范围 0-11。
        lgwin
                滑动窗口大小（log2），范围 10-24。
        lgblock
                最大输入块大小（log2），0 表示自动。
        mode
                压缩模式：MODE_GENERIC / MODE_TEXT / MODE_FONT。
        """
        super().__init__()
        self.quality = quality
        self.lgwin = lgwin
        self.lgblock = lgblock
        self.mode = mode
        self.encode_count = 0
        self.encode_ratio = 0.0

    @override
    def handshake(self, message: bytes) -> tuple[Any, bytes]:
        """
        连接前握手工作。
        Brotli 不做字典协商。
        """

        ctx = self.BrotliContext(
            compressor=brotli.Compressor(
                mode=self.mode,
                quality=self.quality,
                lgwin=self.lgwin,
                lgblock=self.lgblock,
            ),
            decompressor=brotli.Decompressor(),
        )
        return ctx, b""

    @override
    def encode(self, layer_ctx: Any, message: JSONType | bytes) -> JSONType | bytes:
        """
        对消息进行正向处理（流式压缩）
        """
        if not layer_ctx:
            return message

        assert type(message) is bytes, "BrotliCompressor 只能压缩 bytes 类型的消息"

        chunk = layer_ctx.compressor.process(message)
        chunk += layer_ctx.compressor.flush()

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

        assert type(message) is bytes, "BrotliDecompressor 只能解压 bytes 类型的消息"

        try:
            return layer_ctx.decompressor.process(message)
        except Exception as e:
            logger.exception(
                f"❌ [📡Pipeline] [Brotli层] 解压失败，异常：{type(e).__name__}:{e}"
            )
            raise
