"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import logging
from typing import Any

from ...common.singleton import Singleton

logger = logging.getLogger("HeTu.root")
replay = logging.getLogger("HeTu.replay")

JSONType = dict[str, Any] | list[Any]
PipeContext = list[Any]


class MessageProcessLayer:
    def __init__(self):
        self._parent: MessagePipeline = None  # type: ignore
        self._layer_idx = -1

    def on_attach(self, parent: MessagePipeline, layer_idx: int):
        self._parent = parent
        self._layer_idx = layer_idx

    def is_handshake_required(self) -> bool:
        """
        是否需要在连接时进行握手
        """
        return True

    def handshake(self, message: bytes) -> tuple[Any, bytes]:
        """
        连接前握手工作，例如协商参数等。
        返回的第一个值会保存在连接中，贯穿之后的encode/decode调用。
        返回的第二个值会发送给对端。
        """
        raise NotImplementedError()

    def encode(self, layer_ctx: Any, message: JSONType | bytes) -> JSONType | bytes:
        """
        对消息进行正向处理
        """
        raise NotImplementedError()

    def decode(self, layer_ctx: Any, message: JSONType | bytes) -> JSONType | bytes:
        """
        对消息进行逆向处理
        """
        raise NotImplementedError()


class MessagePipeline:
    """
    消息流层叠处理类。

    这里的layer有些需要全局初始化，有些需要连接初始化
    比如zstd的训练就是全局字典
    ecdh的密钥交换是连接初始化
    """

    def __init__(self) -> None:
        self._layers: list[MessageProcessLayer] = []
        self._disabled: list[bool] = []
        self._handshake_layers_count = 0

    def add_layer(self, layer: MessageProcessLayer):
        """
        添加一层流处理组件，例如压缩或加密组件。
        """
        self._layers.append(layer)
        self._disabled.append(False)
        layer.on_attach(self, len(self._layers) - 1)
        if layer.is_handshake_required():
            self._handshake_layers_count += 1

    def disable_layer(self, idx: int):
        """
        禁用指定索引的层
        """
        self._disabled[idx] = True

    def clean(self):
        """
        清除所有层，重置管道
        """
        self._layers.clear()
        self._handshake_layers_count = 0

    @property
    def num_layers(self) -> int:
        return len(self._layers)

    @property
    def num_handshake_layers(self) -> int:
        return self._handshake_layers_count

    def handshake(self, client_messages: list[bytes]) -> tuple[PipeContext, bytes]:
        """
        通过客户端发来的握手消息，完成所有层的握手工作。
        返回握手后的上下文；以及要发送给客户端的握手消息。
        """
        # logger.info(f"🔧 [📡Pipeline] 握手开始 {client_messages} ")
        pipe_ctx: PipeContext = []
        reply_messages = []
        handshake_index = 0
        for i, layer in enumerate(self._layers):
            if self._disabled[i]:
                continue
            if layer.is_handshake_required():
                ctx, reply = layer.handshake(client_messages[handshake_index])
                pipe_ctx.append(ctx)
                reply_messages.append(reply)
                handshake_index += 1
            else:
                pipe_ctx.append(None)

        logger.info(f"🔧 [📡Pipeline] 握手完成 {pipe_ctx}")
        return pipe_ctx, self.encode(None, reply_messages)

    def encode(
        self, pipe_ctx: PipeContext | None, message: JSONType, until=-1
    ) -> bytes:
        """
        对消息进行正向处理，可以传入until参数表示只处理到哪层
        """
        ctx = None
        encoded: JSONType | bytes = message
        for i, layer in enumerate(self._layers):
            if self._disabled[i]:
                continue
            if 0 < until < i:
                break
            if pipe_ctx is not None:
                ctx = pipe_ctx[i]
            encoded = layer.encode(ctx, encoded)
        assert type(encoded) is bytes
        return encoded

    def decode(self, pipe_ctx: PipeContext | None, message: bytes) -> JSONType:
        """
        对消息进行逆向处理
        """
        ctx = None
        decoded: JSONType | bytes = message
        for i, layer in enumerate(reversed(self._layers)):
            if self._disabled[i]:
                continue
            if pipe_ctx is not None:
                original_index = len(pipe_ctx) - 1 - i
                ctx = pipe_ctx[original_index]
            decoded = layer.decode(ctx, decoded)
        assert isinstance(decoded, (dict, list))
        return decoded


class ServerMessagePipeline(MessagePipeline, metaclass=Singleton):
    """
    服务器端的消息流层叠处理类，单例模式。
    """

    pass
