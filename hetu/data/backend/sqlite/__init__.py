"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com

SQLite 开发后端：在 SQLite 上模拟 Redis 的数据模型，行为尽可能和 Redis 后端一致，给开发和随手测试用。
设计见 docs/superpowers/specs/2026-09-26-sqlite-backend-design.md。
"""

from .client import SQLiteBackendClient
from .maint import SQLiteTableMaintenance

__all__ = ["SQLiteBackendClient", "SQLiteTableMaintenance"]
