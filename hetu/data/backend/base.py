import numpy as np
from ..component import BaseComponent


class RaceCondition(Exception):
    pass


class UniqueViolation(IndexError):
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

    async def __aenter__(self):
        self.begin_transaction()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.end_transaction(discard=False)

    async def end_transaction(self, discard: bool):
        # 继承，并实现事务提交的操作，将_updates中的命令写入事务
        # updates是一个List[(cmd, row_id, row)]
        # 如果index是独立分离的，写入前要锁定index，写入时要同时更新index
        raise NotImplementedError

    async def _backend_get(self, row_id: int):
        # 继承，并实现获取行数据的操作，返回值要通过dict_to_row包裹下
        # 如果不存在该行数据，返回None
        # 同时要让乐观锁锁定该行。sql是记录该行的version，用于后续的update条件
        raise NotImplementedError

    async def _backend_get_max_id(self):
        # 继承，并实现获取最大id的操作
        # 如果自增id是数据库负责的，这个可以返回-1，只要你end_transaction时处理即可
        # 如果是自己负责，则注意返回值应该是max_id + 当前事务pending的insert数
        raise NotImplementedError

    async def _backend_query(self, index_name: str, left, right=None, limit=10, desc=False):
        # 继承，并实现范围查询的操作，返回List[int] of row_id。如果你的数据库同时返回了数据，可以存到_cache中
        # 未查询到数据时返回[]
        # 如果index数据是独立分离的，不要锁index，太容易事务冲突，只在end_transaction里锁定
        raise NotImplementedError

    async def select(self, value, where: str = 'id'):
        """
        获取`where`==`value`的单行数据，返回c-struct like。
        `where`不是unique索引时，返回升序排序的第一条数据。
        如果没有查询到数据，返回None。
        """
        assert np.isscalar(value), f"value必须为标量类型(数字，字符串等), 你的:{type(value)}, {value}"
        assert where in self._component_cls.indexes_, \
            f"{self._component_cls.components_name_} 组件没有叫 {where} 的索引"

        if issubclass(type(value), np.generic):
            value = value.item()

        # 查询
        if where == 'id':
            row_id = value
        else:
            if len(row_ids := await self._backend_query(where, value, limit=1)) == 0:
                return None
            row_id = int(row_ids[0])

        if (row := self._cache.get(row_id)) is not None:
            return row

        # 获得行数据并lock row，我们不锁index, 所以index的查询结果和get到的可能不一样（就算锁了也要写入时才知道）
        # 所以我们这里判断下如果值不同就抛出冲突异常
        if (row := await self._backend_get(row_id)) is None:
            raise RaceCondition()
        if row[where] != value:
            raise RaceCondition()

        self._cache[row_id] = row

        return row

    async def query(self, index_name: str, left, right=None, limit=10, desc=False):
        """
        查询`index_name`在`left`和`right`之间的数据，限制`limit`条，是否降序`desc`。
        如果right为None，则查询等于left的数据。
        返回Numpy.Array[row]，如果没有查询到数据，返回空Numpy.Array。
        """
        assert np.isscalar(left), f"left必须为标量类型(数字，字符串等), 你的:{type(left)}, {left}"
        assert index_name in self._component_cls.indexes_, \
            f"{self._component_cls.components_name_} 组件没有叫 {index_name} 的索引"

        left = np.issubdtype(type(left), np.bool_) and int(left) or left
        left = issubclass(type(left), np.generic) and left.item() or left
        right = issubclass(type(right), np.generic) and right.item() or right

        if right is None:
            right = left
        assert right >= left, f"right必须大于等于left，你的:{right}, {left}"

        # 查询
        row_ids = await self._backend_query(index_name, left, right, limit, desc)

        # 获得所有行数据并lock row
        rtn = []
        for row_id in row_ids:
            row_id = int(row_id)
            if (row := self._cache.get(row_id)) is not None:
                rtn.append(row)
            elif (row := await self._backend_get(row_id)) is not None:
                # 由于没lock index，如果发现get结果不在query范围内，就抛出冲突
                if not (left <= row[index_name] <= right):
                    raise RaceCondition()
                self._cache[row_id] = row
                rtn.append(row)
            else:
                raise RaceCondition()

        # 返回numpy array
        if len(rtn) == 0:
            return np.rec.array(np.empty(0, dtype=self._component_cls.dtypes))
        else:
            return np.rec.array(np.stack(rtn, dtype=self._component_cls.dtypes))

    async def is_exist(self, value, where: str = 'id'):
        """查询索引是否存在该键值，并返回row_id，返回值：(bool, int)"""
        assert np.isscalar(value), f"value必须为标量类型(数字，字符串等), 你的:{type(value)}, {value}"
        assert where in self._component_cls.indexes_, \
            f"{self._component_cls.components_name_} 组件没有叫 {where} 的索引"

        if issubclass(type(value), np.generic):
            value = value.item()

        row_ids = await self._backend_query(where, value, limit=1)
        found = len(row_ids) > 0
        return found, found and int(row_ids[0]) or None

    async def select_or_create(self, value, where: str = None):
        uniques = self._component_cls.uniques_ - {'id', where}
        assert len(uniques) == 0, "有多个Unique属性的Component不能使用select_or_create"

        rtn = await self.select(value, where)
        if rtn is None:
            rtn = self._component_cls.new_row()
            rtn[where] = value
            await self.insert(rtn)
        return rtn

    async def _check_uniques(self, old_row: [np.record, None], new_row: np.record):
        """检查新行所有unique索引是否满足条件"""
        is_update = old_row is not None
        is_insert = old_row is None

        # 先检查是否可以添加，循环所有unique index
        for idx_name in self._component_cls.uniques_:
            # 如果值变动了/或是插入，先检查目标值是否已存在
            if (is_update and old_row[idx_name] != new_row[idx_name]) or is_insert:
                if len(await self._backend_query(idx_name, new_row[idx_name].item(), limit=1)) > 0:
                    raise UniqueViolation(
                        f"Unique索引{self._component_cls.components_name_}.{idx_name}，"
                        f"已经存在值为({new_row[idx_name]})的行，无法Update/Insert")

    async def update(self, row_id: int, row):
        """修改row_id行的数据"""
        assert type(row) is np.record, "update数据必须是单行数据"
        row_id = int(row_id)

        if row.id != row_id:
            raise ValueError(f"更新的row.id {row.id} 与传入的row_id {row_id} 不一致")

        # 先查询旧数据是否存在，一般update调用时，旧数据都在_cache里，不然你哪里获得的row数据
        old_row = self._cache.get(row_id)  # or await self._backend_get(row_id)
        if old_row is None:
            raise KeyError(f"{self._component_cls.components_name_} 组件没有id为 {row_id} 的行")

        # 检查先决条件
        await self._check_uniques(old_row, row)
        # 更新cache数据
        row = row.copy()
        self._cache[row_id] = row
        # 加入到更新队列
        self._updates.append(('update', row_id, old_row, row))

    async def update_rows(self, rows: np.rec.array):
        assert type(rows) is np.recarray and rows.shape[0] > 1, "update_rows数据必须是多行数据"
        for i, id_ in enumerate(rows.id):
            await self.update(id_, rows[i])

    async def insert(self, row):
        """插入单行数据"""
        assert type(row) is np.record, "插入数据必须是单行数据"
        assert row.id == 0, "插入数据要求 row.id == 0"

        # 检查先决条件
        row.id = await self._backend_get_max_id() + 1
        await self._check_uniques(None, row)

        # 尝试获取数据，由于上面先决条件检查了，肯定获取不到，主要目的是lock row
        old_row = await self._backend_get(row.id)
        if old_row is not None:
            raise RaceCondition()

        # 加入到更新队列
        row = row.copy()
        self._cache[row.id.item()] = row
        self._updates.append(('insert', row.id, None, row))

    async def delete(self, row_id: int | np.integer):
        """删除row_id行"""
        row_id = int(row_id)
        # 先查询旧数据是否存在，顺便lock row
        old_row = self._cache.get(row_id) or await self._backend_get(row_id)
        if old_row is None or (type(old_row) is str and old_row == 'deleted'):
            raise KeyError(f"{self._component_cls.components_name_} 组件没有id为 {row_id} 的行")

        # 标记删除
        self._cache[row_id] = 'deleted'
        self._updates.append(('delete', row_id, old_row, None))


class ComponentPublisher:
    """
    Component的数据订阅和查询接口
    """
    def __init__(self, component_cls: type[BaseComponent], instance_name, cluster_id, backend):
        self._component_cls = component_cls
        self._instance_name = instance_name
        self._backend = backend
        self._cluster_id = cluster_id

    async def subscribe_select(self, value, where: str = 'id'):
        """订阅单行数据"""
        # 需要获取backend db数字
        # 频道： '__keyspace@0__:keyname' 事件：hset, del
        raise NotImplementedError

    async def subscribe_query(self, index_name: str, left, right=None, limit=10, desc=False):
        """订阅多行数据"""
        # 频道： '__keyspace@0__:indexname' 事件：zadd，zrem
        # 然后再对所有查询的结果分别订阅select
        raise NotImplementedError

    async def unsubscribe(self, sub_id):
        """取消订阅数据"""
        raise NotImplementedError

    async def _backend_get_changes(self):
        # 继承，并获取数据row和index发生变动的通知
        # 根据通知，组合成row数据，返回List，
        # 返回示例：[('update', row), ('insert', row), ('delete', row_id)]
        raise NotImplementedError

    async def get_message(self):
        """处理一次订阅事件"""
        # 调用get_message()，没值返回None
        rows = await self._backend_get_changes()
        # 组合成消息
        msg = []
        row_names = rows.dtype.names
        for row in rows:
            dict_row = dict(zip(row_names, row))
            msg.append({'cmd': 'update', 'row': dict_row})
        # 返回消息
        raise msg
