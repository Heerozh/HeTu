"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import logging
import time
from dataclasses import dataclass
from typing import Any, override

import compression.zstd as zstd  # 仅在 Python 3.14+ 可用
import numpy as np

from .pipeline import JSONType, MessageProcessLayer

logger = logging.getLogger("HeTu.root")
replay = logging.getLogger("HeTu.replay")


class ZstdLayer(MessageProcessLayer):
    """
    使用python 3.14内置的 compression/zstd 模块进行消息的压缩和解压缩。
    """

    @dataclass
    class ZstdContext:
        compressor: zstd.ZstdCompressor
        decompressor: zstd.ZstdDecompressor

    def __init__(self, level: int = 3, dict_size: int = 1024):
        """

        Parameters
        ----------
        level
            Zstd压缩级别，范围从1（最快，压缩率最低）到22（最慢，压缩率最高）。
            一般推荐使用3，之后的速度会非常慢，但压缩率提升有限。
        dict_size
            Zstd字典的大小，单位为字节。字典保存常用字符串，比如Component的属性名，极大增加压缩率。
            较大的字典会增加连接时的网络开销，一般推荐使用1024字节（1KB）。
        """
        super().__init__()
        self.level = level
        self.dict_size = dict_size
        self.samples: list[Any] = []
        self.zstd_dict: zstd.ZstdDict | None = None
        self.dict_message: bytes = b""
        self.last_trained_at: float = 0.0
        self.encode_count = 0
        self.encode_ratio = 0.0

    def initial_samples(self) -> list[bytes]:
        """
        返回用于训练Zstd字典的随机生成的样本数据。在没有真实数据的情况下使用这个。
        """
        from ...common import Permission
        from ...data import BaseComponent
        from ...data.backend import TableReference
        from ...data.sub import Subscriptions
        from ...system import SystemClusters

        rng = np.random.default_rng()

        def make_rand_sub_message(_comp: type[BaseComponent]):
            """生成一个随机的订阅更新消息用于样本数据"""
            default_row: np.record = _comp.new_row(id_=0)

            # 对随机属性进行随机填充，这是为了只保留key特征。我们这里放弃值重复特征。
            dt = default_row.dtype
            raw = bytearray(default_row.tobytes())  # 拷贝为可变 bytes
            raw[:] = rng.integers(0, 256, size=len(raw), dtype=np.uint8).tobytes()
            default_row = np.frombuffer(raw, dtype=dt, count=1)[0]  # 结构化标量
            row_dict = _comp.struct_to_dict(default_row)
            del row_dict["_version"]  # 删除版本字段

            # 对订阅id随机填充，这是为了只保留key特征。我们这里放弃值重复特征。
            ref = TableReference(_comp, "", 0)
            sub_id = Subscriptions.make_query_id_(
                ref,
                rng.choice(["id"] + list(row_dict.keys())),
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

            # zstd算法(COVER)需要统计显著性，需要识别重复模式而非记住完整内容
            for _ in range(200):
                sub_message = make_rand_sub_message(comp)
                # 把订阅更新消息编码到压缩前
                if self._parent:
                    encoded_message = self._parent.encode(
                        [None] * (self._layer_idx + 1), sub_message, self._layer_idx
                    )
                else:
                    encoded_message = str(sub_message).encode("utf-8")
                samples.append(encoded_message)

        if len(samples) == 0:
            # 兜底，防止没有样本
            for _ in range(1000):
                samples.append(b"updt FutureCall id")

        return samples

    def train_dict(self) -> zstd.ZstdDict:
        """
        训练Zstd字典以提高压缩效率。
        使用所有非Admin的组件，创建一行默认值数据，然后用pipeline在本层之前进行预处理，
        然后用它们作为样本。
        """
        # 如果有运行期间收集的数据，用它们训练
        if len(self.samples) > 1000:
            samples = self.samples
        else:
            # 否则使用初始样本
            samples = self.initial_samples()
        # 训练Zstd字典
        return zstd.train_dict(samples, self.dict_size)

    @override
    def handshake(self, message: bytes) -> tuple[Any, bytes]:
        """
        连接前握手工作，例如协商参数等。
        返回的第一个值会保存在连接中，贯穿之后的encode/decode调用。
        返回的第二个值会发送给对端。
        """
        if len(message) == 0:
            # 如果没有训练过字典，用初始样本训练
            if self.zstd_dict is None:
                self.zstd_dict = self.train_dict()
                self.last_trained_at = time.time()
                self.dict_message = self.zstd_dict.dict_content
            else:
                # 反之定期的更新字典
                # todo 如果上次训练时间超过24小时，重新训练字典
                pass
        else:
            # 如果对端发送了字典数据，使用对端的字典
            self.zstd_dict = zstd.ZstdDict(message)
            self.dict_message = self.zstd_dict.dict_content

        assert self.dict_message

        ctx = self.ZstdContext(
            compressor=zstd.ZstdCompressor(
                level=self.level,
                # as_digested_dict会在self.zstd_dict内部建立已消化字典的cache，让下次加载更快
                # 但是部分压缩参数会有被字典的参数覆盖，这里没用到那些参数所以无妨
                zstd_dict=self.zstd_dict.as_digested_dict,
            ),
            decompressor=zstd.ZstdDecompressor(zstd_dict=self.zstd_dict),
        )
        return ctx, self.dict_message

    @override
    def encode(self, layer_ctx: Any, message: JSONType | bytes) -> JSONType | bytes:
        """
        对消息进行正向处理（流式压缩）
        """
        # 如果没有 ctx (握手未完成/未协商字典)，直接返回原始消息
        if not layer_ctx:
            return message

        assert type(message) is bytes, "ZstdCompressor只能压缩bytes类型的消息"

        # todo 如果self.samples长度不足，说明被清空了，收集样本数据以便后续训练字典
        # todo， 记录实际压缩比率，和压缩耗时，对应level和dict_size参数，以便后续调优

        # 使用预训练的字典进行压缩
        # 1. 写入数据到流
        # 2. FLUSH_BLOCK:
        #    这会强制输出当前块的数据，确保接收端能立即收到并解压。
        #    同时不会结束当前帧 (Frame)，保留了历史参考信息（流式压缩的核心优势）。
        chunk = layer_ctx.compressor.compress(
            message, mode=zstd.ZstdCompressor.FLUSH_BLOCK
        )
        ratio = len(chunk) / len(message)
        self.encode_count += 1
        self.encode_ratio += (ratio - self.encode_ratio) / self.encode_count
        return chunk

    @override
    def decode(self, layer_ctx: Any, message: JSONType | bytes) -> JSONType | bytes:
        """
        对消息进行逆向处理（流式解压）
        """
        # 如果没有 ctx，假设消息是未压缩的原始格式
        if not layer_ctx:
            return message

        assert type(message) is bytes, "ZstdDecompressor只能解压bytes类型的消息"

        # 反之，使用字典流式解压
        # zstd 模块会自动处理跨包的数据缓冲
        try:
            return layer_ctx.decompressor.decompress(message)
        except Exception as e:
            # 解压失败处理
            logger.exception(
                f"❌ [📡Pipeline] [Zstd层] 解压失败，异常：{type(e).__name__}:{e}"
            )
            raise
