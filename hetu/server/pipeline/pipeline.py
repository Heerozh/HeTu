"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

from typing import Any

MsgType = list | dict | bytes
PipeContext = list[Any]


class MessageProcessLayer:
    def __init__(self):
        self._parent: MessagePipeline = None  # type: ignore
        self._layer_idx = -1

    def on_attach(self, parent: MessagePipeline, layer_idx: int):
        self._parent = parent
        self._layer_idx = layer_idx

    def handshake(self, message: MsgType) -> tuple[Any, MsgType]:
        """
        连接前握手工作，例如协商参数等。
        返回的第一个值会保存在连接中，贯穿之后的encode/decode调用。
        返回的第二个值会发送给对端。
        """
        raise NotImplementedError()

    def encode(self, layer_ctx: Any, message: MsgType) -> MsgType:
        """
        对消息进行正向处理
        """
        raise NotImplementedError()

    def decode(self, layer_ctx: Any, message: MsgType) -> MsgType:
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

    def __init__(self):
        self._layers: list[MessageProcessLayer] = []

    def add_layer(self, layer: MessageProcessLayer):
        """
        添加一层流处理组件，例如压缩或加密组件。
        """
        self._layers.append(layer)
        layer.on_attach(self, len(self._layers) - 1)

    def handshake(
        self, client_messages: list[MsgType]
    ) -> tuple[PipeContext, list[MsgType]]:
        """
        通过客户端发来的握手消息，完成所有层的握手工作。
        返回握手后的上下文；以及要发送给客户端的握手消息。
        """
        pipe_ctx = []
        reply_messages = []
        for i, layer in enumerate(self._layers):
            ctx, reply = layer.handshake(client_messages[i])
            pipe_ctx.append(ctx)
            reply_messages.append(reply)
        return pipe_ctx, reply_messages

    def encode(self, pipe_ctx: PipeContext, message: MsgType, until=-1) -> MsgType:
        """
        对消息进行正向处理，可以传入until参数表示只处理到哪层
        """
        for i, layer in enumerate(self._layers):
            if 0 < until < i:
                break
            message = layer.encode(pipe_ctx[i], message)
        return message

    def decode(self, pipe_ctx: PipeContext, message: MsgType) -> MsgType:
        """
        对消息进行逆向处理
        """
        for i, layer in enumerate(reversed(self._layers)):
            original_index = len(pipe_ctx) - 1 - i
            message = layer.decode(pipe_ctx[original_index], message)
        return message
