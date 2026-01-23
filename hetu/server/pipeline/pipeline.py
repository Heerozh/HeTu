"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

from typing import Any

MsgType = list | dict | bytes


class MessageProcessLayer:
    def __init__(self):
        self._parent: MessagePipeline = None  # type: ignore
        self._layer_idx = -1

    def on_attach(self, parent: MessagePipeline, layer_idx: int):
        self._parent = parent
        self._layer_idx = layer_idx

    def prepare(self) -> Any:
        """
        连接前准备工作，例如协商参数等。返回需要发送给对端的准备消息（如果有的话）。
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
        此方法是全局的。
        """
        self._layers.append(layer)
        layer.on_attach(self, len(self._layers) - 1)

    def prepare(self) -> list[Any]:
        """
        准备所有层，例如初始化密钥等。
        返回每一层的准备上下文。
        """
        layer_ctxs = []
        for layer in self._layers:
            ctx = layer.prepare()
            layer_ctxs.append(ctx)
        return layer_ctxs

    def encode(self, layer_ctxs: list[Any], message: MsgType, until=-1) -> MsgType:
        """
        对消息进行正向处理，可以传入until参数表示只处理到哪层
        """
        for i, layer in enumerate(self._layers):
            if 0 < until < i:
                break
            message = layer.encode(layer_ctxs[i], message)
        return message

    def decode(self, layer_ctxs: list[Any], message: MsgType) -> MsgType:
        """
        对消息进行逆向处理
        """
        for i, layer in enumerate(reversed(self._layers)):
            original_index = len(layer_ctxs) - 1 - i
            message = layer.decode(layer_ctxs[original_index], message)
        return message
