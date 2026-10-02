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
from ..data.backend import RaceCondition
from ..i18n import _

if TYPE_CHECKING:
    from hetu.manager import ComponentTableManager

logger = logging.getLogger("HeTu.root")
replay = logging.getLogger("HeTu.replay")


# call lock 的保留期（秒），由配置 CALL_LOCK_RETENTION 在 worker 启动时设置。uuid 去重只在
# 保留期内有效：过期的锁由 future_call_task 定期清理（on_start 的锁除外）。一次性未来调用的
# timeout 不能超过它的一半，见 future._build_future_row
CALL_LOCK_RETENTION: float = 30 * 60
# worker 启动时兜底清理的保留期：on_start 的锁不参与定期清理，只在这里清
STARTUP_LOCK_RETENTION = datetime.timedelta(days=7).total_seconds()
# 清理时一个事务最多删这么多行。提交的 Lua 执行期间 master 不处理别的命令：每行约 4µs，
# 1000 行一次要占 4ms，200 行不到 1ms，总开销一样
CLEAN_BATCH = 200


@define_component(namespace="HeTu", volatile=True, permission=Permission.ADMIN)
class SystemLock(BaseComponent):
    """
    带有UUID的SystemCall执行记录，用于锁住防止相同uuid的调用重复执行。保留 CALL_LOCK_RETENTION
    秒后由 future_call_task 定期清理；on_start 的锁只在 worker 启动时清 7 天前的。
    """

    uuid: str = property_field("", dtype="<U32", unique=True)  # 唯一标识
    name: str = property_field("", dtype="<U32")  # 系统名
    caller: np.int64 = property_field(0)
    called: np.double = property_field(0, index=True)  # 执行时间


async def clean_expired_call_locks(
    tbl_mgr: ComponentTableManager,
    older_than: float = STARTUP_LOCK_RETENTION,
    *,
    skip_on_start: bool = False,
) -> int:
    """
    删掉 called 早于 older_than 秒前的 call lock，返回删掉的行数。

    skip_on_start 为 True 时跳过 on_start System 的锁表："每次开服只跑一次"靠这把锁，worker 在
    保留期之后崩溃重启时不能再跑一遍。这些锁只由 worker 启动时的兜底清理（7 天）清。
    多个 worker 同时清同一张表会撞竞态，撞上就把这张表让给别人清。
    """
    from .definer import SystemClusters

    cutoff = time.time() - older_than
    skip = set()
    if skip_on_start:
        skip = set(SystemClusters().get_startup_systems(tbl_mgr.namespace))
    tables = [("", SystemLock), *SystemLock.get_duplicates(tbl_mgr.namespace).items()]
    total = 0
    for suffix, comp in tables:
        tbl = tbl_mgr.get_table(comp)
        if tbl is None or suffix in skip:  # 没被任何 System 引用，或是 on_start 的锁表
            continue
        deleted = 0
        while True:
            try:
                async with tbl.session() as session:
                    repo = session.using(comp)
                    rows = await repo.range(
                        called=(0, cutoff), limit=CLEAN_BATCH, phantom_check=False
                    )
                    for row in rows:
                        repo.delete(row.id)
            except RaceCondition:
                break
            deleted += len(rows)
            if len(rows) < CLEAN_BATCH:
                break
        if deleted:
            logger.debug(
                _("🔗 [⚙️Future] 释放了 {comp_name} 的 {deleted} 条过期数据").format(
                    comp_name=comp.name_, deleted=deleted
                )
            )
        total += deleted
    return total
