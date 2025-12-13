#  """
#  @author: Heerozh (Zhang Jianhao)
#  @copyright: Copyright 2024, Heerozh. All rights reserved.
#  @license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
#  @email: heeroz@gmail.com
#  """
import numpy as np
import pytest

from hetu.data.backend import UniqueViolation, RedisBackendClient, Backend
from hetu.data.backend import random


async def test_table(mod_item_model, mod_auto_backend):
    backend: Backend = mod_auto_backend()
    client = backend.master_or_servant

    from hetu.data.
    # 测试插入数据

    tabref =
    client.get()
