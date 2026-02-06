"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import logging
from typing import Any, override

from .pipeline import JSONType, MessageProcessLayer

logger = logging.getLogger("HeTu.root")
replay = logging.getLogger("HeTu.replay")


class LimitCheckerLayer(MessageProcessLayer):
    """
    检查消息合法性的类
    """

    def __init__(self):
        super().__init__()

    def is_handshake_required(self) -> bool:
        """
        是否需要在连接时进行握手
        """
        return False

    @override
    def handshake(self, message: bytes) -> tuple[Any, bytes]:
        """
        连接前握手工作，例如协商参数等。
        返回的第一个值会保存在连接中，贯穿之后的encode/decode调用。
        返回的第二个值会发送给对端。
        """
        return None, b""

    @override
    def encode(self, layer_ctx: Any, message: JSONType | bytes) -> JSONType | bytes:
        """
        对消息进行正向处理
        """
        # 服务器正向处理的都是自己的数据，采取绝对信任策略
        return message

    @override
    def decode(self, layer_ctx: Any, message: JSONType | bytes) -> JSONType | bytes:
        """
        对消息进行逆向处理
        """
        if len(message) > 10240:
            raise ValueError("Message too long，为了防止性能攻击限制长度")
        return message
