"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com

                      事务相关结构
    ┌────────────────┐           ┌─────────────────┐
    │  DBClientPool  ├──────────►│  DBTransaction  │
    │数据库直连池（单件)│           │    事务模式连接    │
    └────────────────┘           └─────────────────┘
            ▲                             ▲
            │初始化数据                     │ 写入数据
  ┌─────────┴──────────┐      ┌───────────┴────────────┐
  │  ComponentBackend  │      │  ComponentTransaction  │
  │   件数据管理（单件)   │      │      组件相关事务操作     │
  └────────────────────┘      └────────────────────────┘


        数据订阅结构
    ┌─────────────────┐
    │   MQClientPool  │
    │消息队列直连池（单件)│
    └─────────────────┘
            ▲
            │
  ┌─────────┴──────────┐
  │  BackendPublisher  │
  │ 接受消息队列消息并分发 │
  └────────────────────┘
            ▲
            │
  ┌─────────┴──────────┐
  │ComponentSubscriber │
  │   组件相关订阅操作    │
  └────────────────────┘
"""
import numpy as np
from ...common import Singleton
from ..component import BaseComponent


class RaceCondition(Exception):
    pass


class UniqueViolation(IndexError):
    pass


class DBClientPool:
    """
    存放数据库连接的池，并负责开始事务。
    继承此类，完善所有NotImplementedError的方法。
    """

    def __init__(self, config: dict):
        _ = config  # 压制未使用的变量警告
        pass

    async def close(self):
        raise NotImplementedError

    def transaction(self, cluster_id: int) -> 'DBTransaction':
        """进入db的事务模式，返回事务连接，事务只能在对应的cluster_id中执行，不能跨cluster"""
        raise NotImplementedError


class DBTransaction:
    """数据库事务类，负责开始事务，并提交事务"""
    def __init__(self, conn_pool: DBClientPool, cluster_id: int):
        self._conn_pool = conn_pool
        self._cluster_id = cluster_id

    @property
    def cluster_id(self):
        return self._cluster_id

    async def end_transaction(self, discard: bool) -> None:
        """事务结束，提交或放弃事务"""
        # 继承，并实现事务提交的操作，将stacked的命令写入事务
        # stacked的命令由你继承的_trans_insert等方法负责写入
        # 如果你用乐观锁，要考虑清楚何时检查
        raise NotImplementedError

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.end_transaction(discard=False)


class ComponentBackend:
    """
    Component后端主类，负责对每个Component数据的初始化操作，并可以启动Component相关的事务操作。
    继承此类，完善所有NotImplementedError的方法。
    """
    def __init__(
            self, component_cls: type[BaseComponent],
            instance_name: str,
            cluster_id: int,
            conn_pool: DBClientPool
    ):
        self._component_cls = component_cls
        self._instance_name = instance_name
        self._conn_pool = conn_pool
        self._cluster_id = cluster_id

    @property
    def cluster_id(self) -> int:
        return self._cluster_id

    @property
    def component_cls(self) -> type[BaseComponent]:
        return self._component_cls

    def attach(self, db_trans: DBTransaction) -> 'ComponentTransaction':
        """进入Component的事务模式，返回事务操作类"""
        # 继承，并执行：
        # return YourComponentTransaction(self._component_cls, self.db_trans)
        raise NotImplementedError


class ComponentTransaction:
    """
    Component的数据表操作接口，和数据库通讯并处理事务的抽象接口。
    继承此类，完善所有NotImplementedError的方法。
    已写的方法可能不能完全适用所有情况，有些数据库可能要重写这些方法。
    """
    def __init__(self, backend: ComponentBackend, trans_conn: DBTransaction):
        assert trans_conn.cluster_id == backend.cluster_id, \
            "事务只能在对应的cluster_id中执行，不能跨cluster"
        self._component_cls = backend.component_cls  # type: type[BaseComponent]
        self._trans_conn = trans_conn
        self._cache = {}  # 事务中缓存数据，key为row_id，value为row

    async def _db_get(self, row_id: int) -> None | np.record:
        # 继承，并实现获取行数据的操作，返回值要通过dict_to_row包裹下
        # 如果不存在该行数据，返回None
        # 如果用乐观锁，这里同时要让乐观锁锁定该行。sql是记录该行的version，事务提交时判断
        raise NotImplementedError

    async def _db_query(
            self,
            index_name: str,
            left,
            right=None,
            limit=10,
            desc=False
    ) -> list[int]:
        # 继承，并实现范围查询的操作，返回List[int] of row_id。如果你的数据库同时返回了数据，可以存到_cache中
        # 未查询到数据时返回[]
        # 如果你用乐观锁，要考虑清楚何时检查
        raise NotImplementedError

    def _trans_insert(self, row: np.record) -> None:
        # 继承，并实现往transaction里stack插入数据的操作
        raise NotImplementedError

    def _trans_update(self, row_id: int, old_row: np.record, new_row: np.record) -> None:
        # 继承，并实现往transaction里stack更新数据的操作
        raise NotImplementedError

    def _trans_delete(self, row_id: int, old_row: np.record) -> None:
        # 继承，并实现往transaction里stack删除数据的操作
        raise NotImplementedError

    async def select(self, value, where: str = 'id') -> None | np.record:
        """
        获取`where`==`value`的单行数据，返回c-struct like。
        `where`不是unique索引时，返回升序排序的第一条数据。
        如果没有查询到数据，返回None。
        """
        assert np.isscalar(value), f"value必须为标量类型(数字，字符串等), 你的:{type(value)}, {value}"
        assert where in self._component_cls.indexes_, \
            f"{self._component_cls.component_name_} 组件没有叫 {where} 的索引"

        if issubclass(type(value), np.generic):
            value = value.item()

        # 查询
        if where == 'id':
            row_id = value
        else:
            if len(row_ids := await self._db_query(where, value, limit=1)) == 0:
                return None
            row_id = int(row_ids[0])

        if (row := self._cache.get(row_id)) is not None:
            if type(row) is str and row == 'deleted':
                return None
            else:
                return row.copy()

        # 如果cache里没有row，说明query时后端没有返回行数据，说明后端架构index和行数据是分离的，
        # 由于index是分离的，且不能锁定index(不然事务冲突率很高, 而且乐观锁也要写入时才知道冲突），
        # 所以检测get结果是否在查询范围内，不在就抛出冲突
        if (row := await self._db_get(row_id)) is None:
            if where == 'id':
                return None  # 如果不是从index查询到的id，而是直接传入，那就不需要判断race了
            else:
                raise RaceCondition('select: row中途被删除了')
        if row[where] != value:
            raise RaceCondition(f'select: row.{where}值变动了')

        self._cache[row_id] = row

        return row.copy()

    async def query(self, index_name: str, left, right=None, limit=10, desc=False) -> np.recarray:
        """
        查询`index_name`在`left`和`right`之间的数据，限制`limit`条，是否降序`desc`。
        如果right为None，则查询等于left的数据。
        返回Numpy.Array[row]，如果没有查询到数据，返回空Numpy.Array。
        """
        assert np.isscalar(left), f"left必须为标量类型(数字，字符串等), 你的:{type(left)}, {left}"
        assert index_name in self._component_cls.indexes_, \
            f"{self._component_cls.component_name_} 组件没有叫 {index_name} 的索引"

        left = np.issubdtype(type(left), np.bool_) and int(left) or left
        left = issubclass(type(left), np.generic) and left.item() or left
        right = issubclass(type(right), np.generic) and right.item() or right

        if right is None:
            right = left
        assert right >= left, f"right必须大于等于left，你的:{right}, {left}"

        # 查询
        row_ids = await self._db_query(index_name, left, right, limit, desc)

        # 获得所有行数据并lock row
        rtn = []
        for row_id in row_ids:
            row_id = int(row_id)
            if (row := self._cache.get(row_id)) is not None:
                rtn.append(row)
            elif (row := await self._db_get(row_id)) is not None:
                # 如果cache里没有row，说明query时后端没有返回行数据，说明后端架构index和行数据是分离的，
                # 由于index是分离的，且不能锁定index(不然事务冲突率很高），所以检测get结果是否在查询范围内，
                # 不在就抛出冲突
                if not (left <= row[index_name] <= right):
                    raise RaceCondition(f'select: row.{index_name}值变动了')
                self._cache[row_id] = row
                rtn.append(row)
            else:
                raise RaceCondition('select: row中途被删除了')

        # 返回numpy array
        if len(rtn) == 0:
            return np.rec.array(np.empty(0, dtype=self._component_cls.dtypes))
        else:
            return np.rec.array(np.stack(rtn, dtype=self._component_cls.dtypes))

    async def is_exist(self, value, where: str = 'id') -> tuple[bool, int | None]:
        """查询索引是否存在该键值，并返回row_id，返回值：(bool, int)"""
        assert np.isscalar(value), f"value必须为标量类型(数字，字符串等), 你的:{type(value)}, {value}"
        assert where in self._component_cls.indexes_, \
            f"{self._component_cls.component_name_} 组件没有叫 {where} 的索引"

        if issubclass(type(value), np.generic):
            value = value.item()

        row_ids = await self._db_query(where, value, limit=1)
        found = len(row_ids) > 0
        return found, found and int(row_ids[0]) or None

    async def select_or_create(self, value, where: str = None) -> np.record:
        uniques = self._component_cls.uniques_ - {'id', where}
        assert len(uniques) == 0, "有多个Unique属性的Component不能使用select_or_create"

        rtn = await self.select(value, where)
        if rtn is None:
            rtn = self._component_cls.new_row()
            rtn[where] = value
            await self.insert(rtn)
        return rtn

    async def _check_uniques(
            self,
            old_row: [np.record, None],
            new_row: np.record,
            ignores=None
    ) -> None:
        """检查新行所有unique索引是否满足条件"""
        is_update = old_row is not None
        is_insert = old_row is None

        # 循环所有unique index, 检查是否可以添加/更新行
        for idx_name in self._component_cls.uniques_:
            if ignores and idx_name in ignores:
                continue
            # 如果值变动了，或是插入新行
            if (is_update and old_row[idx_name] != new_row[idx_name]) or is_insert:
                if len(await self._db_query(idx_name, new_row[idx_name].item(), limit=1)) > 0:
                    raise UniqueViolation(
                        f"Unique索引{self._component_cls.component_name_}.{idx_name}，"
                        f"已经存在值为({new_row[idx_name]})的行，无法Update/Insert")

    async def update(self, row_id: int, row) -> None:
        """修改row_id行的数据"""
        assert type(row) is np.record, "update数据必须是单行数据"
        row_id = int(row_id)

        if row.id != row_id:
            raise ValueError(f"更新的row.id {row.id} 与传入的row_id {row_id} 不一致")

        # 先查询旧数据是否存在，一般update调用时，旧数据都在_cache里，不然你哪里获得的row数据
        old_row = self._cache.get(row_id)  # or await self._db_get(row_id)
        if old_row is None:
            raise KeyError(f"{self._component_cls.component_name_} 组件没有id为 {row_id} 的行")

        # 检查先决条件
        await self._check_uniques(old_row, row)
        # 更新cache数据
        row = row.copy()
        old_row = old_row.copy()  # 因为要放入_updates，从cache获取的，得copy防止修改
        self._cache[row_id] = row
        # 加入到更新队列
        self._trans_update(row_id, old_row, row)

    async def update_rows(self, rows: np.recarray) -> None:
        assert type(rows) is np.recarray and rows.shape[0] > 1, "update_rows数据必须是多行数据"
        for i, id_ in enumerate(rows.id):
            await self.update(id_, rows[i])

    async def insert(self, row) -> None:
        """插入单行数据"""
        assert type(row) is np.record, "插入数据必须是单行数据"
        assert row.id == 0, "插入数据要求 row.id == 0"

        # 提交到事务前先检查无unique冲突
        await self._check_uniques(None, row, ignores={'id'})

        # 加入到更新队列
        row = row.copy()
        self._trans_insert(row)

    async def delete(self, row_id: int | np.integer) -> None:
        """删除row_id行"""
        row_id = int(row_id)
        # 先查询旧数据是否存在
        old_row = self._cache.get(row_id) or await self._db_get(row_id)
        if old_row is None or (type(old_row) is str and old_row == 'deleted'):
            raise KeyError(f"{self._component_cls.component_name_} 组件没有id为 {row_id} 的行")
        old_row = old_row.copy()  # 因为要放入_updates，从cache获取的，得copy防止修改

        # 标记删除
        self._cache[row_id] = 'deleted'
        self._trans_delete(row_id, old_row)


##################################################

class MQClientPool:
    pass


class BackendPublisher:
    """
    Component的数据订阅和查询接口
    """
    def __init__(self, instance_name, mq: MQClientPool):
        self._instance_name = instance_name
        self._mq = mq


class ComponentSubscriber:

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


class ComponentBackendPool(metaclass=Singleton):
    """
    ComponentBackend的池，负责对每个ComponentBackend的初始化操作。
    此类是单件，每个进程只有一个实例。
    """
    def __init__(self):
        self.backends = {}
        self.publishers = {}

    def create_backend(
            self,
            component_cls: type[BaseComponent],
            instance_name: str,
            cluster_id: int,
            conn_pool: DBClientPool
    ):
        assert component_cls not in self.backends, f"{component_cls.component_name_} Backend已经存在"
        self.backends[component_cls] = ComponentBackend(
            component_cls, instance_name, cluster_id, conn_pool)
        # self.publishers[component_cls] = ComponentPublisher(
        #     component_cls, instance_name, cluster_id, self.backends[component_cls])

    def backend(self, item: type[BaseComponent]) -> ComponentBackend:
        return self.backends[item]

    # def publisher(self, item: type[BaseComponent]) -> ComponentPublisher:
    #     return self.publishers[item]
