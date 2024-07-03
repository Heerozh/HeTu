"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com

                      事务相关结构
    ┌────────────────┐           ┌──────────────────┐
    │    Backend     ├──────────►│BackendTransaction│
    │数据库直连池（单件)│           │    事务模式连接     │
    └────────────────┘           └──────────────────┘
            ▲                             ▲
            │初始化数据                     │ 写入数据
  ┌─────────┴──────────┐      ┌───────────┴────────────┐
  │   ComponentTable   │      │  ComponentTransaction  │
  │  组件数据管理（单件)  │      │      组件相关事务操作     │
  └────────────────────┘      └────────────────────────┘


        数据订阅结构
    ┌─────────────────┐
    │   MQClientPool  │
    │消息队列直连池（单件)│
    └─────────────────┘
            ▲
            │
  ┌─────────┴──────────┐
  │    Subscriptions   │
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
from ..component import BaseComponent


class RaceCondition(Exception):
    pass


class UniqueViolation(IndexError):
    pass


class Backend:
    """
    存放数据库连接的池，并负责开始事务。
    继承此类，完善所有NotImplementedError的方法。
    """

    def __init__(self, config: dict):
        _ = config  # 压制未使用的变量警告
        pass

    async def close(self):
        raise NotImplementedError

    def transaction(self, cluster_id: int) -> 'BackendTransaction':
        """进入db的事务模式，返回事务连接，事务只能在对应的cluster_id中执行，不能跨cluster"""
        raise NotImplementedError


class BackendTransaction:
    """数据库事务类，负责开始事务，并提交事务"""
    def __init__(self, backend: Backend, cluster_id: int):
        self._backend = backend
        self._cluster_id = cluster_id

    @property
    def cluster_id(self):
        return self._cluster_id

    async def end_transaction(self, discard: bool) -> list[int] | None:
        """事务结束，提交或放弃事务。返回insert的row.id列表，按调用顺序"""
        # 继承，并实现事务提交的操作，将stacked的命令写入事务
        # stacked的命令由你继承的_trx_insert等方法负责写入
        # 如果你用乐观锁，要考虑清楚何时检查
        raise NotImplementedError

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.end_transaction(discard=False)


class ComponentTable:
    """
    Component数据主类，负责对每个Component数据的初始化操作，并可以启动Component相关的事务操作。
    继承此类，完善所有NotImplementedError的方法。
    """
    def __init__(
            self, component_cls: type[BaseComponent],
            instance_name: str,
            cluster_id: int,
            backend: Backend
    ):
        self._component_cls = component_cls
        self._instance_name = instance_name
        self._backend = backend
        self._cluster_id = cluster_id

    @property
    def cluster_id(self) -> int:
        return self._cluster_id

    @property
    def backend(self) -> Backend:
        return self._backend

    @property
    def component_cls(self) -> type[BaseComponent]:
        return self._component_cls

    def flush(self):
        """如果非持久化组件，则允许调用flush主动清空数据"""
        raise NotImplementedError

    async def direct_query(
            self,
            index_name: str,
            left,
            right=None,
            limit=10,
            desc=False) -> np.recarray:
        """直接获取数据库的值，而不通过事务，一般用在维护时。注意，获取的值可能被其他进程变动，不可在System中使用。"""
        raise NotImplementedError

    def attach(self, backend_trx: BackendTransaction) -> 'ComponentTransaction':
        """返回当前组件的事务操作类，并附加到现有的后端事务连接"""
        # 继承，并执行：
        # return YourComponentTransaction(self, backend_trx)
        raise NotImplementedError

    def new_transaction(self) -> tuple[BackendTransaction, 'ComponentTransaction']:
        """返回当前组件的事务操作类，并新建一个后端事务连接"""
        conn = self._backend.transaction(self._cluster_id)
        return conn, self.attach(conn)


class ComponentTransaction:
    """
    Component的数据表操作接口，和数据库通讯并处理事务的抽象接口。
    继承此类，完善所有NotImplementedError的方法。
    已写的方法可能不能完全适用所有情况，有些数据库可能要重写这些方法。
    """
    def __init__(self, comp_tbl: ComponentTable, trx_conn: BackendTransaction):
        assert trx_conn.cluster_id == comp_tbl.cluster_id, \
            "事务只能在对应的cluster_id中执行，不能跨cluster"
        self._component_cls = comp_tbl.component_cls  # type: type[BaseComponent]
        self._trx_conn = trx_conn
        self._cache = {}  # 事务中缓存数据，key为row_id，value为row

    @property
    def attached(self) -> BackendTransaction:
        return self._trx_conn

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
            desc=False,
            lock_index=True
    ) -> list[int]:
        # 继承，并实现范围查询的操作，返回List[int] of row_id。如果你的数据库同时返回了数据，可以存到_cache中
        # 未查询到数据时返回[]
        # 如果你用乐观锁，要考虑清楚何时检查
        raise NotImplementedError

    def _trx_insert(self, row: np.record) -> None:
        # 继承，并实现往transaction里stack插入数据的操作
        raise NotImplementedError

    def _trx_update(self, row_id: int, old_row: np.record, new_row: np.record) -> None:
        # 继承，并实现往transaction里stack更新数据的操作
        raise NotImplementedError

    def _trx_delete(self, row_id: int, old_row: np.record) -> None:
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
            if len(row_ids := await self._db_query(where, value, limit=1, lock_index=False)) == 0:
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

    async def query(
            self, index_name: str, left, right=None, limit=10, desc=False, lock_index=True
    ) -> np.recarray:
        """
        查询`index_name`在`left`和`right`之间的数据，限制`limit`条，是否降序`desc`。
        如果right为None，则查询等于left的数据。
        返回Numpy.Array[row]，如果没有查询到数据，返回空Numpy.Array。

        如何多条件查询？
        请利用python的特性，举例：
        items = ctx[Item].query('owner', ctx.caller, limit=100)  # 先在数据库上筛选出最少量的数据
        swords = items[items.model == 'sword']                   # 然后本地二次筛选
        或者:
        few_items = items[items.amount < 10]

        `lock_index`: 表示是否锁定`index_name`索引，安全起见默认锁定，但因为存在行锁定，
        其实大部分情况锁定index是不必要的。

        锁定分2种：
        * 行锁定：任何其他协程/进程对查询结果所含行的修改会引发事务冲突，但无关行不会。此锁定是强制的，不可关闭。
        * Index锁定：任何其他协程/进程修改了该index(插入新行/update本列/删除任意行)都会引起事务冲突。
          如果慢日志回报了大量的事务冲突，再考虑设为False。

        所以一般情况下：
        * 如果你只对query返回的行操作，因为有行锁定，所以可以不锁index。
        * 如果你对query结果本身有要求，比如需要判断结果数量/是否已存在，你需要保持锁定index。
            - 建议使用`unique`索引在底层限制唯一性

        举个删除背包所有道具的例子：1.查询背包，2.删除查询到的行。
        此需求可以不锁定index，只是1和2之间可能有新的道具进入背包，删除可能不彻底。
        由于存在行锁定，即使不锁定index，2也可以保证道具不会被其他进程删除。

        """
        assert np.isscalar(left), f"left必须为标量类型(数字，字符串等), 你的:{type(left)}, {left}"
        assert index_name in self._component_cls.indexes_, \
            f"{self._component_cls.component_name_} 组件没有叫 {index_name} 的索引"

        left = int(left) if np.issubdtype(type(left), np.bool_) else left
        left = left.item() if issubclass(type(left), np.generic) else left
        right = right.item() if issubclass(type(right), np.generic) else right

        if right is None:
            right = left
        assert right >= left, f"right必须大于等于left，你的:{right}, {left}"

        # 查询
        row_ids = await self._db_query(index_name, left, right, limit, desc, lock_index)

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

        row_ids = await self._db_query(where, value, limit=1, lock_index=True)
        found = len(row_ids) > 0
        return found, found and int(row_ids[0]) or None

    async def select_or_create(self, value, where: str = None) -> 'UpdateOrInsert':
        """
        同:func:`~hetu.data.ComponentTransaction.select`，
        如果没有查询到值时，会返回空数据（Component.new_row()）。

        返回值是`UpdateOrInsert`类型，可通过`UpdateOrInsert.row`获取row数据，
        `UpdateOrInsert.commit()`提交更新。
        或者用With语句，可以自动提交，如下：
        async with ctx[Component].select_or_create(...) as row:
            row.value = 100
        """
        rtn = await self.select(value, where)
        if rtn is None:
            rtn = self._component_cls.new_row()
            rtn[where] = value
            return UpdateOrInsert(rtn, self, 0)
        else:
            return UpdateOrInsert(rtn, self, rtn.id)

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
                row_ids = await self._db_query(
                    idx_name, new_row[idx_name].item(), limit=1, lock_index=False)
                if len(row_ids) > 0:
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
        self._trx_update(row_id, old_row, row)

    async def update_rows(self, rows: np.recarray) -> None:
        assert type(rows) is np.recarray and rows.shape[0] > 1, "update_rows数据必须是多行数据"
        for i, id_ in enumerate(rows.id):
            await self.update(id_, rows[i])

    async def insert(self, row: np.record) -> None:
        """
        插入单行数据。
        如果想获得插入后的row id，只能在事务结束后获得。

        或者可在System最后调用end_transaction获得，如下：
        @define_system(...)
        async some_system(ctx, ...):
            ctx[Table].insert(...)
            inserted_ids = await ctx.trx.end_transaction(discard=False)
            ctx.user_data['my_id'] = inserted_ids[0]
            return ...
        注意：调用完end_transaction，ctx将不再能够获取Components
        """
        assert type(row) is np.record, "插入数据必须是单行数据"
        assert row.id == 0, "插入数据要求 row.id == 0"

        # 提交到事务前先检查无unique冲突
        await self._check_uniques(None, row, ignores={'id'})

        # 加入到更新队列
        row = row.copy()
        self._trx_insert(row)

    async def update_or_insert(self, row: np.record) -> None:
        """
        如果row.id == 0, 则插入该row，反之更新该id的row。
        一般不要使用，请使用明确的update或insert语句。此方法仅在select_or_create后使用。
        """
        if row.id == 0:
            await self.insert(row)
        else:
            await self.update(row.id, row)

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
        self._trx_delete(row_id, old_row)


class UpdateOrInsert:
    def __init__(self, row: np.record, comp_trx: ComponentTransaction, row_id: int):
        self.row = row
        self.comp_trx = comp_trx
        self.row_id = row_id

    async def commit(self):
        if self.row_id == 0:
            await self.comp_trx.insert(self.row)
        else:
            await self.comp_trx.update(self.row_id, self.row)

    async def __aenter__(self):
        return self.row

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            await self.commit()


##################################################


class MQClientPool:
    pass


class Subscriptions:
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
