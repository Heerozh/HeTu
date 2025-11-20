"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import datetime
import logging
import time

import numpy as np

from ..data import BaseComponent, define_component, Property, Permission

logger = logging.getLogger("HeTu.root")
replay = logging.getLogger("HeTu.replay")


@define_component(namespace="HeTu", persist=False, permission=Permission.ADMIN)
class ExecutionLock(BaseComponent):
    """带有UUID的SystemCall执行记录，用于锁住防止相同uuid的调用重复执行。调用方用完后要记得删除自己的记录。"""

    uuid: str = Property("", dtype="<U32", unique=True)  # 唯一标识
    name: str = Property("", dtype="<U32")  # 系统名
    caller: np.int64 = Property(0)
    called: np.double = Property(0, index=True)  # 执行时间


async def clean_expired_call_locks(comp_mgr):
    """清空超过7天的call_lock的已执行uuid数据，只有服务器非正常关闭才可能遗留这些数据，因此只需服务器启动时调用。"""
    duplicates = ExecutionLock.get_duplicates(comp_mgr.namespace).values()
    for comp in [ExecutionLock] + list(duplicates):
        tbl = comp_mgr.get_table(comp)
        if tbl is None:  # 说明项目没任何地方引用此Component
            continue
        backend = tbl.backend
        deleted = 0
        while True:
            async with backend.transaction(tbl.cluster_id) as session:
                tbl_trx = tbl.attach(session)
                rows = await tbl_trx.query(
                    "called",
                    left=0,
                    right=time.time() - datetime.timedelta(days=7).total_seconds(),
                    limit=1000,
                )
                # 循环每行数据，删除
                for row in rows:
                    await tbl_trx.delete(row.id)
                deleted += len(rows)
                if len(rows) == 0:
                    break
        logger.info(
            f"🔗 [⚙️Future] 释放了 {comp.component_name_} 的 {deleted} 条过期数据"
        )
