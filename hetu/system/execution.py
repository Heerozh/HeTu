"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""
import numpy as np

from ..data import BaseComponent, define_component, Property, Permission


@define_component(namespace='HeTu', persist=False, permission=Permission.ADMIN)
class ExecutionLock(BaseComponent):
    """带有UUID的SystemCall执行记录，用于锁住防止相同uuid的调用重复执行。调用方用完后要记得删除自己的记录。"""
    uuid: str = Property('', dtype='<U32', unique=True)  # 唯一标识
    name: str = Property('', dtype='<U32')  # 系统名
    caller: np.int64 = Property(0)
    called: np.double = Property(0, index=True)  # 执行时间
