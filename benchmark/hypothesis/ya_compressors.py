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

jsonb = pipeline.JSONBinaryLayer()


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

    return jsonb.encode(None, ["updt", sub_id, row_dict])


data = [make_rand_sub_message(Item) for _ in range(10000)]


layers = {}


async def zstd():
    def _create_layer(lv):
        key = f"zstd_lv{lv}"
        if key in layers:
            return layers[key]
        layer = pipeline.ZstdLayer(level=lv)
        ctx, _ = layer.handshake(b"")
        layers[key] = (layer, ctx)
        return layer, ctx

    return _create_layer


async def zlib():
    def _create_layer(lv):
        key = f"zlib_lv{lv}"
        if key in layers:
            return layers[key]
        layer = pipeline.ZlibLayer(level=lv)
        ctx, _ = layer.handshake(b"")
        layers[key] = (layer, ctx)
        return layer, ctx

    return _create_layer


async def brotli():
    def _create_layer(lv):
        key = f"brotli_lv{lv}"
        if key in layers:
            return layers[key]
        layer = pipeline.BrotliLayer(quality=lv)
        ctx, _ = layer.handshake(b"")
        layers[key] = (layer, ctx)
        return layer, ctx

    return _create_layer


def bench(layer, ctx):
    for message in random.sample(data, k=1000):
        payload = layer.encode(ctx, message)
        _ = layer.decode(ctx, payload)


async def benchmark_zstd_level3(zstd):
    bench(*zstd(3))


async def benchmark_zstd_level12(zstd):
    bench(*zstd(12))


async def benchmark_zlib_level3(zlib):
    bench(*zlib(3))


async def benchmark_zlib_level6(zlib):
    bench(*zlib(6))


async def benchmark_brotli_level4(brotli):
    bench(*brotli(4))


async def benchmark_brotli_level3(brotli):
    bench(*brotli(3))


async def benchmark_brotli_level12(brotli):
    bench(*brotli(12))


async def task_teardown():
    for name, layer in layers.items():
        print(f"{name} encode ratio: {layer[0].encode_ratio}")


"""
cd hypothesis
uv run ya ya_compressors.py -t 0.5 -n 1 -p 1

9950x3d

|                         | CPS(k) | 
|:------------------------|-------:|
| benchmark_brotli_level3 |  92.05 |
| benchmark_brotli_level4 |  87.04 |
| benchmark_zlib_level3   | 116.18 |
| benchmark_zlib_level6   | 105.41 |
| benchmark_zstd_level12  |  73.68 |
| benchmark_zstd_level3   | 442.76 |


brotli_lv3 encode ratio: 0.7534961938307967
brotli_lv4 encode ratio: 0.5799906913090168
zlib_lv3 encode ratio: 0.7207111787807501
zlib_lv6 encode ratio: 0.7021223569930083
zstd_lv12 encode ratio: 0.7088246839633067
zstd_lv3 encode ratio: 0.7751032480679005
"""
