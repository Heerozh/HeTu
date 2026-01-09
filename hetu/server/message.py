"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import logging

import msgspec

logger = logging.getLogger("HeTu.root")
replay = logging.getLogger("HeTu.replay")

msg_encoder = msgspec.msgpack.Encoder()
msg_decoder = msgspec.msgpack.Decoder()
buffer = bytearray()


def decode_message(message: bytes, protocol: dict) -> dict:
    if len(message) > 10240:
        raise ValueError("Message too long，为了防止性能攻击限制长度")
    if crypto := protocol["crypto"]:
        message = crypto.decrypt(message)
    if compress := protocol["compress"]:
        message = compress.decompress(message)
    parsed = msg_decoder.decode(message)
    return parsed


def encode_message(message: list | dict, protocol: dict) -> bytes:
    try:
        msg_encoder.encode_into(message, buffer)
        ret = bytes(buffer)
    except Exception as e:
        logger.exception(
            f"❌ [📡WSSender] JSON序列化失败，消息：{message}，异常：{type(e).__name__}:{e}"
        )
        raise
    if compress := protocol["compress"]:
        ret = compress.compress(ret)
    if crypto := protocol["crypto"]:
        ret = crypto.encrypt(ret)
    return ret
