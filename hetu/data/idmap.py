"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import logging

logger = logging.getLogger("HeTu.root")


class IdentityMap:
    """
    用于缓存和管理事务中的对象。
    SessionComponentTable会经由本类来查询和缓存对象。
    BackendSession在提交时可以通过本类，获得脏对象列表，然后想办法合并成事务指令。
    """
