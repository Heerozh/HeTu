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

    # 帮我实现IdentityMap，要有如下功能：
    # add_clean(row)方法，添加一个查询到的对象到row缓存中，可以通过row类型判断对应comp表，row缓存是个np表
    #    该np表可以用np.rec.array(np.empty(0, dtype=self._component_cls.dtypes))初始化
    #    用np.rec.array(np.stack(rows, dtype=self._component_cls.dtypes))堆叠
    #    如果数据行已存在，则更新该行。
    #    除了row的np表，还有维护一个row id对应的状态表，标记row是insert, update, delete等
    # get(comp_cls，id)方法，如果该comp的row缓存中有则返回缓存中的对象，否则返回None表示要调用方去数据库查询
    # -----
    # add_range(index_name=(left, right), rows)方法，index_name是动态参数，表示索引名称，left和right表示范围
    #    表示调用方把range查询结果储存到范围缓存中，rows是查询结果列表，储存到row缓存，可以通过row类型判断对应comp表
    # range(comp_cls，index_name=(left, right))方法，首先看范围是否在范围缓存中，如果在，因为row缓存是np表，可以直接np[条件]方式返回。
    #    如果不在，则返回None表示要调用方去数据库查询
    # -----
    # add_insert(row)方法，添加一个新插入的对象到row缓存中，并标记为insert状态，row的id为累加负数
    # update(row)方法，更新一个对象到row缓存中，并标记为update状态
    # mark_deleted(id)方法，标记id对应的对象为删除状态
    # get_dirty_rows()方法，返回所有脏对象的列表，按insert, update, delete状态分开
