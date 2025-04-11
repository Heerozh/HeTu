"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import logging

import orjson

logger = logging.getLogger('HeTu.root')
replay = logging.getLogger('HeTu.replay')


def decode_message(message: bytes, protocol: dict):
    if len(message) > 10240:
        raise ValueError("Message too long，为了防止性能攻击限制长度")
    if crypto := protocol['crypto']:
        message = crypto.decrypt(message)
    if compress := protocol['compress']:
        message = compress.decompress(message)
    json_parsed = orjson.loads(message)
    return json_parsed


def encode_message(message: list | dict, protocol: dict):
    try:
        message = orjson.dumps(message)
    except Exception as e:
        logger.exception(f"❌ [📡WSSender] JSON序列化失败，消息：{message}，异常：{e}")
        raise
    if compress := protocol['compress']:
        message = compress.compress(message)
    if crypto := protocol['crypto']:
        message = crypto.encrypt(message)
    return message
