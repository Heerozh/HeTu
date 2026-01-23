"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import compression.zstd as zstd  # 仅在 Python 3.14+ 可用
import random

import numpy as np

from hetu import BaseComponent


from .pipeline import MessageProcessLayer, MsgType
from typing import Any, override


class ZstdCompressor(MessageProcessLayer):
    """
    使用python 3.14内置的 compression/zstd 模块进行消息的压缩和解压缩。
    """

    def __init__(self, level: int = 3):
        super().__init__()
        self.level = level
        self.zstd_dict = self.train_dict()
        self.dict_message = self.zstd_dict.dict_content
        self.compressor = zstd.ZstdCompressor(
            level=self.level, zstd_dict=self.zstd_dict
        )
        self.decompressor = zstd.ZstdDecompressor(zstd_dict=self.zstd_dict)

    def train_dict(self) -> zstd.ZstdDict:
        """
        训练Zstd字典以提高压缩效率。
        使用所有非Admin的组件，创建一行默认值数据，然后用pipeline在本层之前进行预处理，
        然后用它们作为样本。
        """
        from ...system import SystemClusters
        from ...common import Permission
        from ...data.sub import Subscriptions
        from ...data.backend import TableReference

        rng = np.random.default_rng()

        def make_rand_sub_message(comp: type[BaseComponent]):
            """生成一个随机的订阅更新消息用于样本数据"""
            default_row: np.record = comp.new_row(id_=0)

            # 对随机属性进行随机填充，这是为了只保留key特征。我们这里放弃值重复特征。
            dt = default_row.dtype
            raw = bytearray(default_row.tobytes())  # 拷贝为可变 bytes
            raw[:] = rng.integers(0, 256, size=len(raw), dtype=np.uint8).tobytes()
            default_row = np.frombuffer(raw, dtype=dt, count=1)[0]  # 结构化标量
            row_dict = comp.struct_to_dict(default_row)

            # 对订阅id随机填充，这是为了只保留key特征。我们这里放弃值重复特征。
            ref = TableReference(comp, "", 0)
            sub_id = Subscriptions.make_query_id_(
                ref,
                "id",
                rng.integers(0, np.iinfo(np.int64).max),
                rng.integers(0, np.iinfo(np.int64).max),
                rng.integers(1, 100),
                rng.choice([True, False]),
            )

            return ["updt", sub_id, row_dict]

        samples = []
        # cherry pick样本
        for comp, _ in SystemClusters().get_components().items():
            if comp.permission_ == Permission.ADMIN:
                continue

            # 倍增样本数量以获得更好的字典
            for _ in range(50):
                sub_message = make_rand_sub_message(comp)
                # 把订阅更新消息编码到压缩前
                encoded_message = self._parent.encode(
                    [None] * (self._layer_idx + 1), sub_message, self._layer_idx
                )
                samples.append(encoded_message)

        # 训练Zstd字典
        dict_size = sum(len(s) for s in samples) // 10  # 目标字典大小为样本总大小的10%
        return zstd.train_dict(samples, dict_size)

    @override
    def handshake(self, message: MsgType) -> tuple[Any, MsgType]:
        """
        连接前握手工作，例如协商参数等。
        返回之后的encode/decode的context，以及需要发送给对端的准备消息（如果有的话）。
        """
        return None, self.dict_message

    @override
    def encode(self, layer_ctx: Any, message: MsgType) -> MsgType:
        """
        对消息进行正向处理（流式压缩）
        """
        # 如果没有 ctx (握手未完成/未协商字典)，直接返回原始消息
        if not layer_ctx:
            return message

        assert type(message) is bytes, "ZstdCompressor只能压缩bytes类型的消息"

        # 使用预训练的字典进行压缩
        # 1. 写入数据到流
        # 2. FLUSH_BLOCK:
        #    这会强制输出当前块的数据，确保接收端能立即收到并解压。
        #    同时不会结束当前帧 (Frame)，保留了历史参考信息（流式压缩的核心优势）。
        compressor = self.compressor
        chunk = compressor.compress(message, mode=ZstdCompressor.FLUSH_BLOCK)
        chunk += compressor.flush(zstd.FLUSH_BLOCK) FLUSH_FRAME的区别是啥？

        return compressed_data

    @override
    def decode(self, layer_ctx: Any, message: MsgType) -> MsgType:
        """
        对消息进行逆向处理（流式解压）
        """
        # 如果没有 ctx，假设消息是未压缩的原始格式
        if not layer_ctx:
            return message

        assert type(message) is bytes, "ZstdDecompressor只能解压bytes类型的消息"

        # 反之，使用字典解压
        if not isinstance(message, bytes):
            # 防御性编程：如果传入的不是bytes，可能已经是解压后的或者是错误类型
            return message

        try:
            decompressed_bytes = zstd.decompress(message, zstd_dict=self.zstd_dict)
            # 解压后尝试恢复为结构化对象 (dict/list)
            return self._from_bytes(decompressed_bytes)
        except Exception as e:
            # 解压失败处理，视业务逻辑而定，这里打印错误并返回原始值
            print(f"Decompression error: {e}")
            return message
