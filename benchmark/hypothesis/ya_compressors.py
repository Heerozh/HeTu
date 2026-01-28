import numpy as np
import random

from hetu import (
    BaseComponent,
    Permission,
    define_component,
    define_system,
    property_field,
)
from hetu.data.backend import TableReference
from hetu.data.sub import Subscriptions
from hetu.system import SystemClusters
from hetu.server import pipeline

rng = np.random.default_rng()


@define_component(namespace="pytest", permission=Permission.OWNER)
class Item(BaseComponent):
    owner: np.int64 = property_field(0, unique=False, index=True)
    model: np.float32 = property_field(0, unique=False, index=True)
    qty: np.int16 = property_field(1, unique=False, index=False)
    level: np.int8 = property_field(1, unique=False, index=False)
    time: np.int64 = property_field(0, unique=True, index=True)
    name: "U8" = property_field("", unique=True, index=True)  # type: ignore  # noqa
    used: bool = property_field(False, unique=False, index=True)


@define_system(
    namespace="pytest",
    components=(Item,),
    force=True,
)
async def do_nothing(ctx):
    pass


SystemClusters().build_clusters("pytest")


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

    return str(["updt", sub_id, row_dict]).encode("utf-8")


data = [make_rand_sub_message(Item) for _ in range(1000)]


layers = {}


async def zstd():
    layer = pipeline.ZstdLayer(level=3)
    ctx, _ = layer.handshake(b"")
    layers["zstd"] = layer
    return layer, ctx


async def zlib():
    layer = pipeline.ZlibLayer(level=3)
    ctx, _ = layer.handshake(b"")
    layers["zlib"] = layer
    return layer, ctx


async def brotli():
    layer = pipeline.BrotliLayer(quality=4)
    ctx, _ = layer.handshake(b"")
    layers["brotli"] = layer
    return layer, ctx


async def benchmark_zstd_level3(zstd):
    layer, ctx = zstd
    for message in random.sample(data, k=100):
        payload = layer.encode(ctx, message)
        _ = layer.decode(ctx, payload)


async def benchmark_zlib_level3(zlib):
    layer, ctx = zlib
    for message in random.sample(data, k=100):
        payload = layer.encode(ctx, message)
        _ = layer.decode(ctx, payload)


async def benchmark_brotli_level4(brotli):
    layer, ctx = brotli
    for message in random.sample(data, k=100):
        payload = layer.encode(ctx, message)
        _ = layer.decode(ctx, payload)


async def task_teardown():
    for name, layer in layers.items():
        print(f"{name} encode ratio: {layer.encode_ratio}")


"""
cd hypothesis
uv run ya ya_compressors.py -t 0.1

|                         | CPS      |
|:------------------------|:---------|
| benchmark_brotli_level4 | 677.02   |
| benchmark_zlib_level3   | 513.25   |
| benchmark_zstd_level3   | 1,575.95 |

brotli encode ratio: 0.06424025265774627
zlib encode ratio: 0.5043165878223276
zstd encode ratio: 0.47444256218709674
"""
