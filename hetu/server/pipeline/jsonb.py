"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import logging
from typing import Any, override

import msgspec

from .pipeline import MessageProcessLayer, MsgType

logger = logging.getLogger("HeTu.root")
replay = logging.getLogger("HeTu.replay")


class JSONBinaryLayer(MessageProcessLayer):
    """
    把list/dict转换为byte的类
    """

    def __init__(self):
        super().__init__()
        self.msg_encoder = msgspec.msgpack.Encoder()
        self.msg_decoder = msgspec.msgpack.Decoder()
        self.buffer = bytearray()

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
        assert type(message) in (list, dict), (
            "jsonb正向处理的message必须是list或dict类型"
        )

        try:
            self.msg_encoder.encode_into(message, self.buffer)
            return bytes(self.buffer)
        except Exception as e:
            logger.exception(
                f"❌ [📡Pipeline] [JsonB层]  JSONB序列化失败，消息：{message}，异常：{type(e).__name__}:{e}"
            )
            raise

    @override
    def decode(self, layer_ctx: Any, message: MsgType) -> MsgType:
        """
        对消息进行逆向处理
        """
        assert type(message) is bytes, "jsonb逆向处理的message必须是bytes类型"

        try:
            return self.msg_decoder.decode(message)
        except Exception as e:
            logger.exception(
                f"❌ [📡Pipeline] [JsonB层]  JSONB反序列化失败，消息：{message}，异常：{type(e).__name__}:{e}"
            )
            raise
