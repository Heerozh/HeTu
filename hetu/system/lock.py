"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import datetime
import logging
import time
from typing import TYPE_CHECKING

import numpy as np

from ..common.permission import Permission
from ..data import BaseComponent, define_component, property_field
from ..i18n import _

if TYPE_CHECKING:
    from hetu.manager import ComponentTableManager

logger = logging.getLogger("HeTu.root")
replay = logging.getLogger("HeTu.replay")


@define_component(namespace="HeTu", volatile=True, permission=Permission.ADMIN)
class SystemLock(BaseComponent):
    """带有UUID的SystemCall执行记录，用于锁住防止相同uuid的调用重复执行。调用方用完后要记得删除自己的记录。"""

    uuid: str = property_field("", dtype="<U32", unique=True)  # 唯一标识
    name: str = property_field("", dtype="<U32")  # 系统名
    caller: np.int64 = property_field(0)
    called: np.double = property_field(0, index=True)  # 执行时间


async def clean_expired_call_locks(tbl_mgr: ComponentTableManager):
    """清空超过7天的call_lock的已执行uuid数据，只有服务器非正常关闭才可能遗留这些数据，因此只需服务器启动时调用。"""
    timestamp_7d_ago = time.time() - datetime.timedelta(days=7).total_seconds()
    duplicates = SystemLock.get_duplicates(tbl_mgr.namespace).values()
    for comp in [SystemLock] + list(duplicates):
        tbl = tbl_mgr.get_table(comp)
        if tbl is None:  # 说明项目没任何地方引用此Component
            continue
        deleted = 0
        while True:
            async with tbl.session() as session:
                repo = session.using(comp)
                rows = await repo.range(called=(0, timestamp_7d_ago), limit=1000)
                # 循环每行数据，删除
                for row in rows:
                    repo.delete(row.id)
                deleted += len(rows)
                if len(rows) == 0:
                    break 
        logger.info(_("🔗 [⚙️Future] 释放了 {comp_name} 的 {deleted} 条过期数据").format(comp_name=comp.name_, deleted=deleted))
