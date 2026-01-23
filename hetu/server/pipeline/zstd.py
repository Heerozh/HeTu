"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

from compression import zstd

from .pipeline import MessageProcessLayer, MsgType
from typing import Any, override


class ZstdCompressor(MessageProcessLayer):
    """
    使用python 3.14内置的 compression/zstd 模块进行消息的压缩和解压缩。
    """

    def __init__(self, level: int = 3):
        self.level = level

    def train_dict(self):
        """
        训练Zstd字典以提高压缩效率。
        使用所有非Admin的组件，创建一行默认值数据，然后用pipeline在本层之前进行预处理，
        然后用它们作为样本。
        """
        from ...system import SystemClusters
        from ...common import Permission
        from ...data.sub import Subscriptions

        samples = []
        for comp, _ in SystemClusters().get_components().items():
            if comp.permission_ == Permission.ADMIN:
                continue
            default_row = comp.new_row(id_=0)
            default_row = comp.struct_to_dict(default_row)
            sub_id = Subscriptions._make_query_str(comp.name_, "row", [0], {})
            sub_message = ["updt", sub_id, default_row]
            samples.append(default_row)

    @override
    def prepare(self) -> Any:
        """
        连接前准备工作，例如协商参数等。返回需要发送给对端的准备消息（如果有的话）。
        """
        return None

    @override
    def encode(self, layer_ctx: Any, message: MsgType) -> MsgType:
        """
        对消息进行正向处理
        """
        raise NotImplementedError()

    @override
    def decode(self, layer_ctx: Any, message: MsgType) -> MsgType:
        """
        对消息进行逆向处理
        """
        raise NotImplementedError()
