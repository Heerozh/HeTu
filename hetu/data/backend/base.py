import numpy as np
from ..component import BaseComponent


class TransactionConflict(Exception):
    pass


class ComponentTable:
    """
    Component的数据表操作接口，和数据库通讯并处理事务的抽象接口。
    继承此类，完善所有NotImplementedError的方法。有些数据库可能要重写更多方法。
    """
    def __init__(self, component_cls: type[BaseComponent], instance_name, cluster_id, backend):
        self._component_cls = component_cls
        self._instance_name = instance_name
        self._backend = backend
        self._cluster_id = cluster_id

        self._cache = {}  # 事务中缓存数据，key为row_id，value为row
        self._updates = []  # 事务中写入队列，在end_transaction时作为事务一起写入

    def begin_transaction(self):
        self._cache = {}
        self._updates = []

    async def end_transaction(self):
        # 继承，并实现事务提交的操作，将_updates中的命令写入事务
        # updates是一个List[(row_id, row)]，row_id为None表示插入，否则为更新，row为None表示删除
        # 如果index是独立分离的，写入时要同时更新index
        raise NotImplementedError

    async def _backend_get(self, row_id: int):
        # 继承，并实现获取行数据的操作，返回值要通过dict_to_row包裹下
        # 如果不存在该行数据，返回None
        # 同时要让乐观锁锁定该行。sql是记录该行的version，用于后续的update条件
        raise NotImplementedError

    async def _backend_get_max_id(self):
        # 继承，并实现获取最大id的操作
        # 如果自增id是数据库负责的，这个可以返回-1，只要你end_transaction时处理即可
        # 如果最大id是单独储存的，要用乐观锁锁定该行数据，或者锁定id的索引也可以
        raise NotImplementedError

    async def _backend_query(self, index_name: str, left, right=None, limit=10, desc=False):
        # 继承，并实现范围查询的操作，返回List[int] of row_id。如果你的数据库同时返回了数据，可以存到_cache中
        # 未查询到数据时返回[]
        # 如果index数据是独立分离的，要用乐观锁锁定该index
        raise NotImplementedError

    async def select(self, value, where: str = 'id'):
        """
        获取`where`==`value`的单行数据，返回c-struct like。
        `where`不是unique索引时，返回升序排序的第一条数据。
        如果没有查询到数据，返回None。
        """
        assert np.issctype(type(value)), \
            f"value必须为标量类型(数字，字符串等), 你的:{type(value)}, {value}"
        assert where in self._component_cls.indexes_, \
            f"{self._component_cls.components_name_} 组件没有叫 {where} 的索引"

        # 查询并lock index
        if len(slicer := await self._backend_query(where, value, limit=1)) == 0:
            return None

        row_id = slicer[0]

        if (row := self._cache.get(row_id)) is not None:
            return row

        # 获得行数据并lock row
        if (row := await self._backend_get(row_id)) is None:
            # 如果index是独立的，且乐观锁锁定，由于乐观锁只有最后写入时才会冲突，这里get依然有可能获得空值（被删除了）
            # 如果不写这句，最后写入时也会提示事务冲突，但间隔太远，而且可能导致用户代码因为返回值是None出错
            raise TransactionConflict()
        self._cache[row_id] = row

        return row

    async def query(self, index_name: str, left, right=None, limit=10, desc=False):
        """
        查询`index_name`在`left`和`right`之间的数据，限制`limit`条，是否降序`desc`。
        如果right为None，则查询等于left的数据。
        返回Numpy.Array[row]，如果没有查询到数据，返回空列表[]。
        """
        assert np.issctype(type(left)), \
            f"left必须为标量类型(数字，字符串等), 你的:{type(left)}, {left}"
        assert index_name in self._component_cls.indexes_, \
            f"{self._component_cls.components_name_} 组件没有叫 {index_name} 的索引"

        # 查询并lock index
        slicer = await self._backend_query(index_name, left, right, limit, desc)

        # 获得所有行数据并lock row
        rtn = [] # todo 这里要改成返回numpy array
        for row_id in slicer:
            if (row := self._cache.get(row_id)) is not None:
                rtn.append(row)
            elif (row := await self._backend_get(row_id)) is not None:
                self._cache[row_id] = row
                rtn.append(row)
            else:
                raise TransactionConflict()
        return rtn

    async def is_exist(self, value, where: str = 'id'):
        """查询索引是否存在该键值，并返回索引位置，返回值：(bool, i)"""
        assert np.issctype(type(value)), \
            f"value必须为标量类型(数字，字符串等), 你的:{type(value)}, {value}"
        assert where in self._component_cls.indexes_, \
            f"{self._component_cls.components_name_} 组件没有叫 {where} 的索引"

        slicer = await self._backend_query(where, value, limit=1)
        return len(slicer) > 0, slicer[0]

    async def select_or_create(self, value, where: str = None):
        uniques = self._component_cls.uniques_ - {'id', where}
        assert len(uniques) == 0, "有多个Unique属性的Component不能使用select_or_create"

        rtn = await self.select(value, where)
        if rtn is None:
            rtn = self._component_cls.new_row()
            rtn[where] = value
            await self.insert(rtn)
        return rtn

    async def _check_uniques(self, old_row: [np.void, None], new_row: np.void):
        """检查新行所有unique索引是否满足条件"""
        is_update = old_row is not None
        is_insert = old_row is None

        # 先检查是否可以添加，循环所有unique index
        for prop in self._component_cls.uniques_:
            # 如果值变动了/或是插入，先检查目标值是否已存在
            if (is_update and old_row[prop] != new_row[prop]) or is_insert:
                if len(await self._backend_query(prop, new_row[prop], limit=1)) > 0:
                    raise ValueError(f"Unique索引{self._component_cls.components_name_}.{prop}，"
                                     f"已经存在值为({new_row[prop]})的行，无法Update/Insert")

    async def update(self, row_id: int, row):
        assert type(row) is np.void, "update数据必须是单行数据"
        if row.id != row_id:
            raise ValueError(f"更新的row.id {row.id} 与传入的row_id {row_id} 不一致")

        # 先查询旧数据是否存在，一般update调用时，旧数据都在_cache里，不然你哪里获得的row数据
        old_row = self._cache.get(row_id)  # or await self._backend_get(row_id)
        if old_row is None:
            raise KeyError(f"{self._component_cls.components_name_} 组件没有id为 {row_id} 的行")

        # 检查先决条件
        await self._check_uniques(old_row, row)
        # 更新cache数据
        self._cache[row_id] = row
        # 加入到更新队列
        self._updates.append((row_id, row))

    async def update_rows(self, rows: np.rec.array):
        assert type(rows) is np.recarray and rows.shape[0] > 1, "update_rows数据必须是多行数据"
        for i, id_ in enumerate(rows.id):
            await self.update(id_, rows[i])

    async def insert(self, row):
        assert type(row) is np.void, "插入数据必须是单行数据"
        assert row.id == 0, "插入数据要求 row.id == 0"

        # 检查先决条件
        row.id = await self._backend_get_max_id() + 1
        await self._check_uniques(None, row)

        # 尝试获取数据，由于上面先决条件检查了，肯定获取不到，主要目的是lock row
        old_row = await self._backend_get(row.id)
        if old_row is not None:
            raise TransactionConflict()

        # 加入到更新队列
        self._updates.append((None, row))

    async def delete(self, row_id: int):
        # 先查询旧数据是否存在，顺便lock row
        old_row = self._cache.get(row_id) or await self._backend_get(row_id)
        if old_row is None:
            raise KeyError(f"{self._component_cls.components_name_} 组件没有id为 {row_id} 的行")

        # 标记删除
        self._updates.append((row_id, None))


