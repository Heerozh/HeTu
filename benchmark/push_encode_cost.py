"""
每连接推送编码的成本：一帧聊天更新 ["updt", sub_id, {新行 id: 行, 最旧行 id: None}] 走服务端 pipeline
（jsonb → zlib(预共享字典，level 1) → ChaCha20-Poly1305）要多久、多大。每条连接一个 zlib 流，同一条
消息给每条连接各编码一次，所以这是"同查询共享订阅"之后聊天每条消息、每个连接剩下的主要固定成本之一
（另外还有交付、ws 分帧和 send 系统调用）。

用法：uv run python benchmark/push_encode_cost.py [--cpu 0]
"""

import argparse
import os
import time

import sub_scenarios_app as app
from cryptography.hazmat.primitives.asymmetric.x25519 import (
    X25519PrivateKey,
)

from hetu.server import pipeline
from hetu.system import SystemClusters

CONNS = 200
MSGS = 100


def frame_of(i: int) -> list:
    """第 i 条聊天消息进窗口、最旧一条离开：每条内容不同，zlib 流里不会整帧重复"""
    row = {
        "id": 7384738473847384738 + i,
        "owner": 100 + i % 900,
        "name": f"user{100 + i % 900}",
        "text": f"hello world message number {123456 + i} " * 3,
        "kind": "chat",
        "created_at_ms": 1790608000000 + i * 37,
        "ts": 1790608000.123 + i * 0.037,
    }
    return [
        "updt",
        "ChatMessage.id[0:9223372036854775807:-1][:1024]",
        {str(row["id"]): row, str(7384738473847380000 + i): None},
    ]


def main(args) -> None:
    if hasattr(os, "sched_setaffinity"):
        os.sched_setaffinity(0, {args.cpu})
    if SystemClusters().get_clusters(app.NAMESPACE) is None:
        SystemClusters().build_clusters(app.NAMESPACE)
        SystemClusters().build_endpoints()
    server = pipeline.ServerMessagePipeline()
    server.clean()
    server.add_layer(pipeline.JSONBinaryLayer())
    server.add_layer(pipeline.ZlibLayer(level=1))
    server.add_layer(pipeline.CryptoLayer())
    client = pipeline.MessagePipeline()
    client.add_layer(pipeline.JSONBinaryLayer())
    client.add_layer(pipeline.ZlibLayer())
    client.add_layer(pipeline.CryptoLayer())

    ctxs = []
    for _ in range(CONNS):
        hs = [b""] * client.num_handshake_layers
        hs[-1] = X25519PrivateKey.generate().public_key().public_bytes_raw()
        msg = client.decode(None, client.encode(None, hs))
        assert isinstance(msg, list)
        ctx, _reply = server.handshake(msg)
        ctxs.append(ctx)

    for i in range(5):  # 预热
        frame = frame_of(i)
        for ctx in ctxs:
            server.encode(ctx, frame)
    n = size = 0
    cpu0 = time.process_time()
    for i in range(5, 5 + MSGS):
        frame = frame_of(i)
        for ctx in ctxs:
            size += len(server.encode(ctx, frame))
            n += 1
    cpu = time.process_time() - cpu0
    print(
        f"每帧编码 {cpu / n * 1e6:.1f}us CPU，平均 {size / n:.0f} 字节"
        f"（原始 JSON 约 {len(str(frame_of(1)))} 字符）"
    )


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])  # type: ignore[union-attr]
    ap.add_argument("--cpu", type=int, default=0, help="绑哪个核（P 核）")
    main(ap.parse_args())
