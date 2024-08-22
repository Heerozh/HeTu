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
    │     MQClient    │
    │消息队列连接(每用户）│
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
  │ 用户连接(Websocket) │
  │   等待Subs返回消息   │
  └────────────────────┘
"""
import asyncio
import logging

import numpy as np

from ..component import BaseComponent, Permission

logger = logging.getLogger('HeTu.root')


class RaceCondition(Exception):
    pass


class UniqueViolation(IndexError):
    pass


class HeadLockFailed(RuntimeError):
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

    def configure(self):
        """
        启动时检查并配置数据库，减少运维压力的帮助方法，非必须。
        """
        raise NotImplementedError

    def requires_head_lock(self) -> bool:
        """
        要求持有head锁，防止启动2台有head标记的服务器。
        所有ComponentTable的create_or_migrate或flush调用时都会调用此方法。
        返回True表示锁定成功，或已持有该锁。
        返回False表示已有别人持有了锁，程序退出。
        """
        raise NotImplementedError

    def transaction(self, cluster_id: int) -> 'BackendTransaction':
        """进入db的事务模式，返回事务连接，事务只能在对应的cluster_id中执行，不能跨cluster"""
        raise NotImplementedError

    def get_mq_client(self) -> 'MQClient':
        """获取消息队列连接"""
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
        # 继承，并实现事务提交的操作，将_trx_insert等方法堆叠的命令写入数据库
        # 如果你用乐观锁，要考虑清楚何时检查
        # 如果数据库不具备写入通知功能，要在此手动往MQ推送数据变动消息。
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
            backend: Backend,
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

    def create_or_migrate(self, cluster_only=False):
        """进行表的初始化操作，每次服务器启动时都会进行。"""
        raise NotImplementedError

    def flush(self, force=False):
        """如果非持久化组件，则允许调用flush主动清空数据"""
        raise NotImplementedError

    async def direct_query(
            self,
            index_name: str,
            left,
            right=None,
            limit=10,
            desc=False,
            row_format='struct',
    ) -> np.recarray | list[dict | int]:
        """
        不通过事务直接从servant数据库查询值，不影响Master性能，但没有数据一致性保证。

        .. warning:: ⚠️ 警告：从servant读取值存在更新延迟，且脱离事务，在System中使用要确保逻辑能接受数据不一致。

        Parameters
        ----------
        index_name: str
            查询Component中的哪条索引
        left, right: str or number
            查询范围，闭区间。字符串查询时，可以在开头指定是[闭区间，还是(开区间
        limit: int
            限制返回的行数，越低越快
        desc: bool
            是否降序排列
        row_format:
            'struct': 包装成component struct返回
            'raw': 直接返回数据库中的值，由dict包装，可能包含多余数据，也不会进行类型转换。
            'id': 只返回row_id列表
        """
        # 请使用servant数据库来操作
        raise NotImplementedError

    async def direct_get(self, row_id: int) -> None | np.record:
        """
        不通过事务，从servant数据库直接读取某行的值。

        .. warning:: ⚠️ 警告：从servant读取值存在更新延迟，且脱离事务，在System中使用要确保逻辑能接受数据不一致。
        """
        raise NotImplementedError

    async def direct_set(self, row_id: int, **kwargs):
        """
        不通过事务，直接设置数据库某行的值。此方法不检查任何正确性，比如row_id不存在也会设置。

        .. warning:: ⚠️ 警告：由于不在事务中，值随时可能被其他进程修改/删除，不保证数据一致性。
        请勿在System中使用，除非原子操作。
        """
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

    def channel_name(self, index_name: str = None, row_id: int = None):
        """返回当前组件表，在消息队列中的频道名。表如果有数据变动，会发送到对应频道"""
        raise NotImplementedError


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
        self._del_flags = set()  # 事务中的删除操作标记
        self._updt_flags = set()  # 事务中的更新操作标记

    @property
    def component_cls(self) -> type[BaseComponent]:
        return self._component_cls

    @property
    def attached(self) -> BackendTransaction:
        return self._trx_conn

    async def _db_get(self, row_id: int, lock_row=True) -> None | np.record:
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
        # 继承，并实现往BackendTransaction里stack插入数据的操作
        raise NotImplementedError

    def _trx_update(self, row_id: int, old_row: np.record, new_row: np.record) -> None:
        # 继承，并实现往BackendTransaction里stack更新数据的操作
        raise NotImplementedError

    def _trx_delete(self, row_id: int, old_row: np.record) -> None:
        # 继承，并实现往BackendTransaction里stack删除数据的操作
        raise NotImplementedError

    async def get_by_ids(self, row_ids: list[int] | np.ndarray) -> np.recarray:
        """
        通过row_id，批量获取行数据，返回numpy array。一般用在query获得row_ids后。

        假设我们有个Slot组件，每个Slot有一个item_id指向道具
        >>> @define_component
        ... class Slot(BaseComponent):
        ...     owner: np.int64 = Property(0, index=True)
        ...     item_id: np.int64 = Property(0)
        取出所有slot.owner == caller的道具数据：
        >>> @define_system(components=(Slot, Item))
        ... async def get_all_items(ctx):
        ...     slots = await ctx[Slot].query('owner', ctx.caller, limit=100, lock_index=False)
        ...     items = await ctx[Item].get_by_ids(slots.item_id)
        """
        rtn = []
        for row_id in row_ids:
            if (row := self._cache.get(row_id)) is not None:
                if type(row) is str and row == 'deleted':
                    raise RaceCondition('gets: row已经被你自己删除了')
                rtn.append(row)
            else:
                if (row := await self._db_get(row_id)) is None:
                    raise RaceCondition('gets: row中途被删除了')
                self._cache[row_id] = row
                rtn.append(row)

        return np.rec.array(np.stack(rtn, dtype=self._component_cls.dtypes))

    async def select(self, value, where: str = 'id', lock_row=True) -> None | np.record:
        """
        获取 `where` == `value` 的单行数据，返回c-struct like。
        `where` 不是unique索引时，返回升序排序的第一条数据。
        本方法等于 `query(where, value, limit=1, lock_index=False,lock_row=lock_row)`，但速度更快一些。

        Parameters
        ----------
        value: str or number
            查询的值
        where: str
            查询的索引名，如 'id', 'owner', 'name' 等
        lock_row: bool
            是否锁定查询到的行，默认锁定。如果不锁定，该数据只能做只读操作，不然会有数据写入冲突。
            一般不需要关闭锁定，除非慢日志回报了大量的事务冲突，考虑清楚后再做调整。

        Returns
        -------
        row: np.record or None
            返回c-struct like的单行数据。如果没有查询到数据，返回None。

        Examples
        --------
        >>> from hetu.system import define_system
        >>> from hetu.data import define_component, Property
        >>> @define_component
        ... class Item(BaseComponent):
        ...     owner: np.int64 = Property(0, index=True)
        >>> @define_system(components=(Item, ))
        ... async def some_system(ctx):
        ...     item_row = await ctx[Item].select(ctx.caller, 'owner')
        ...     print(item_row.name)
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
        if (row := await self._db_get(row_id, lock_row=lock_row)) is None:
            if where == 'id':
                return None  # 如果不是从index查询到的id，而是直接传入，那就不需要判断race了
            else:
                raise RaceCondition('select: row中途被删除了')
        if row[where] != value:
            raise RaceCondition(f'select: row.{where}值变动了')

        self._cache[row_id] = row

        return row.copy()

    async def query(
            self, index_name: str, left, right=None, limit=10, desc=False, lock_index=True,
            index_only=False, lock_rows=True
    ) -> np.recarray | list[int]:
        """
        查询 索引`index_name` 在 `left` 和 `right` 之间的数据，限制 `limit` 条，是否降序 `desc`。
        如果 `right` 为 `None`，则查询等于 `left` 的数据。

        Parameters
        ----------
        index_name: str
            查询Component中的哪条索引
        left, right: str or number
            查询范围，闭区间。字符串查询时，可以在开头指定是[闭区间，还是(开区间
        limit: int
            限制返回的行数，越低越快
        desc: bool
            是否降序排列
        lock_index: bool
            表示是否锁定 `index_name` 索引，安全起见默认锁定，但因为存在行锁定，
            其实大部分情况锁定index是不必要的。

            锁定分2种：

            * 行锁定：任何其他协程/进程对查询结果所含行的修改会引发事务冲突，但无关行不会。
            * Index锁定：任何其他协程/进程修改了该index(插入新行/update本列/删除任意行)都会引起事务冲突。
              如果慢日志回报了大量的事务冲突，再考虑设为 `False`。

            所以一般情况下：

            * 如果你只对 `query` 返回的行操作(如`rows[0].value = 1`)，因为有行锁定，所以可以不锁index。
            * 如果你对 `query` 结果本身有要求(如要求`len(rows) == 0`)，你需要保持锁定index，
              不然提交事务时index可能已变。
                - 建议使用 `unique` 索引在底层限制唯一性，事务冲突率低

            举个删除背包所有道具的例子：1.查询背包，2.删除查询到的行。

            由于1在查询完后，已经对所有查询到的行进行了锁定，即使不锁定index，2也可以保证道具不会被其他进程修改。
            所以如果不锁定index，只会导致1和2之间，有新道具进入背包，删除可能不彻底，没有其他害处。
        lock_rows: bool
            是否锁定查询到的行，默认锁定。如果不锁定，该数据只能做只读操作，不然会有数据写入冲突。
            一般不需要关闭锁定，除非慢日志回报了大量的事务冲突，考虑清楚后再做调整。
        index_only: bool
            如果只需要获取Index的查询结果，不需要行数据，可以选择index_only=True。
            返回的是List[int] of row_id。

        Returns
        -------
        rows: np.recarray
            返回 `numpy.array`，如果没有查询到数据，返回空 `numpy.array`。
            如果 `index_only=True`，返回的是 `List[int]`。

        Notes
        -----
        如何多条件查询？
        请利用python的特性，举例：

        >>> items = ctx[Item].query('owner', ctx.caller, limit=100)  # noqa
        先在数据库上筛选出最少量的数据
        >>> swords = items[items.model == 'sword']
        然后本地二次筛选，也可以用范围判断：
        >>> few_items = items[items.amount < 10]

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

        if index_only:
            return row_ids

        # 获得所有行数据并lock row
        rtn = []
        for row_id in row_ids:
            row_id = int(row_id)
            if (row := self._cache.get(row_id)) is not None:
                rtn.append(row)
            elif (row := await self._db_get(row_id, lock_row=lock_rows)) is not None:
                # 如果cache里没有row，说明query时后端没有返回行数据，说明后端架构index和行数据是分离的，
                # 由于index是分离的，且不能锁定index(不然事务冲突率很高），所以检测get结果是否在查询范围内，
                # 不在就抛出冲突
                if not (left <= row[index_name] <= right):
                    raise RaceCondition(f'select: row.{index_name}值变动了')
                if lock_rows:
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

    def select_or_create(self, value, where: str = None) -> 'UpdateOrInsert':
        """
        同 :py:func:`hetu.data.ComponentTransaction.select`，只是返回的是一个自动更新的上下文。

        Returns
        -------
        expression: UpdateOrInsert
            返回的是一个UpdateOrInsert对象，可以在with语句中使用，离开with时自动update或insert。
            如果没有查询到值时，上下文内是空数据（`Component.new_row()`），并在离开with时自动insert。

        Examples
        --------
        使用方法如下：
        >>> from hetu.system import define_system
        >>> from hetu.data import define_component, Property
        >>> @define_component
        ... class Portfolio(BaseComponent):
        ...     owner: np.int64 = Property(0, index=True)
        ...     cash: np.int64 = Property(0)
        >>> @define_system(components=(Portfolio, ))
        ... async def deposit_franklin(ctx):
        ...     async with ctx[].select_or_create(ctx.caller, 'owner') as row:
        ...         row.cash += 100
        """
        return UpdateOrInsert(self, value, where)

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
        row_id = int(row_id)

        if row_id in self._updt_flags:
            raise KeyError(f"{self._component_cls.component_name_}行（id:{row_id}）"
                           f"已经在事务中更新过了，不允许重复更新。")
        if row_id in self._del_flags:
            raise KeyError(f"{self._component_cls.component_name_}行（id:{row_id}）"
                           f"已经在事务中删除了，不允许再次更新。")

        assert type(row) is np.record, "update数据必须是单行数据"

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
        self._updt_flags.add(row_id)
        # 加入到更新队列
        self._trx_update(row_id, old_row, row)

    async def update_rows(self, rows: np.recarray) -> None:
        assert type(rows) is np.recarray and rows.shape[0] > 1, "update_rows数据必须是多行数据"
        for i, id_ in enumerate(rows.id):
            await self.update(id_, rows[i])

    async def insert(self, row: np.record, unique_violation_as_race=False) -> None:
        """
        插入单行数据。unique_violation_as_race表示是否把
        UniqueViolation(插入时遇到Unique值被占用)当作RaceCondition(事务冲突)抛出。

        Examples
        --------
        >>> from hetu.system import define_system
        >>> from hetu.data import define_component, Property
        >>> @define_component
        ... class Item(BaseComponent):
        ...     owner: np.int64 = Property(0, index=True)
        ...     model: str = Property("", dtype='<U8')
        >>> @define_system(components=(Item, ))
        ... async def create_item(ctx):
        ...     new_item = Item.new_row()
        ...     new_item.model = 'SWORD_1'
        ...     ctx[Item].insert(new_item)

        Notes
        -----
        如果想获得插入后的row id，或者想知道是否事务执行成功，可通过显式结束事务获得。

        调用 `end_transaction` 方法，如果事务冲突，后面的代码不会执行，如下：

        >>> @define_system(components=(Item, ))
        ... async def create_item(ctx):
        ...     ctx[Item].insert(...)
        ...     inserted_ids = await ctx.trx.end_transaction(discard=False)
        ...     ctx.user_data['my_id'] = inserted_ids[0]  # 如果事务冲突，这句不会执行

        ⚠️ 注意：调用完end_transaction，ctx将不再能够获取Components
        """
        assert type(row) is np.record, "插入数据必须是单行数据"
        assert row.id == 0, "插入数据要求 row.id == 0"

        # 提交到事务前先检查无unique冲突
        try:
            await self._check_uniques(None, row, ignores={'id'})
        except UniqueViolation:
            if unique_violation_as_race:
                raise RaceCondition("插入数据时，unique冲突")
            else:
                raise

        # 加入到更新队列
        row = row.copy()
        self._trx_insert(row)

    async def delete(self, row_id: int | np.integer) -> None:
        """删除row_id行"""
        row_id = int(row_id)

        if row_id in self._updt_flags:
            raise KeyError(f"{self._component_cls.component_name_} 行（id:{row_id}）"
                           f"在事务中已有update命令，不允许再次删除。")
        if row_id in self._del_flags:
            raise KeyError(f"{self._component_cls.component_name_} 行（id:{row_id}）"
                           f"已经在事务中删除了，不允许重复删除。")

        # 先查询旧数据是否存在
        old_row = self._cache.get(row_id) or await self._db_get(row_id)
        if old_row is None:
            raise KeyError(f"{self._component_cls.component_name_} 组件没有id为 {row_id} 的行")

        old_row = old_row.copy()  # 因为要放入_updates，从cache获取的，得copy防止修改

        # 标记删除
        self._cache[row_id] = 'deleted'
        self._del_flags.add(row_id)
        self._trx_delete(row_id, old_row)

    async def delete_rows(self, row_ids: list[int] | np.ndarray) -> None:
        assert type(row_ids) is np.ndarray and row_ids.shape[0] > 1, "deletes数据必须是多行数据"
        for row_id in row_ids:
            await self.delete(row_id)


class UpdateOrInsert:
    def __init__(self, comp_trx: ComponentTransaction, value, where):
        self.comp_trx = comp_trx
        self.value = value
        self.where = where
        self.row = None
        self.row_id = None

    async def commit(self):
        if self.row_id == 0:
            await self.comp_trx.insert(self.row, unique_violation_as_race=True)
        else:
            await self.comp_trx.update(self.row_id, self.row)

    async def __aenter__(self):
        row = await self.comp_trx.select(self.value, self.where)
        if row is None:
            row = self.comp_trx.component_cls.new_row()
            row[self.where] = self.value
            self.row = row
            self.row_id = 0
        else:
            self.row = row
            self.row_id = row.id
        return self.row

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            await self.commit()


# === === === === === === 数据订阅 === === === === === ===


class MQClient:
    """连接到消息队列的客户端，每个用户连接一个实例。订阅后端只需要继承此类。"""
    UPDATE_FREQUENCY = 10   # 控制客户端所有订阅的数据（如果有变动），每秒更新几次

    async def close(self):
        raise NotImplementedError

    async def pull(self) -> None:
        """
        从消息队列接收一条消息到本地队列，消息内容为channel名，每行数据，每个Index，都是一个channel。
        该channel收到了任何消息都说明有数据更新，所以只需要保存channel名。
        每条消息带一个首次接受时间，重复的消息忽略。
        此方法需要单独的协程反复调用，防止消息堆积。
        """
        # 必须合并消息，因为index更新时大都是2条一起的
        raise NotImplementedError

    async def get_message(self) -> set[str]:
        """
        pop并返回之前pull()到本地的消息，只pop收到时间大于1/UPDATE_FREQUENCY的消息。
        之后Subscriptions会对该消息进行分析，并重新读取数据库获数据。
        如果没有消息，则堵塞到永远。
        """
        raise NotImplementedError

    async def subscribe(self, channel_name: str) -> None:
        """订阅频道"""
        raise NotImplementedError

    async def unsubscribe(self, channel_name: str) -> None:
        """取消订阅频道"""
        raise NotImplementedError

    @property
    def subscribed_channels(self) -> set[str]:
        """返回当前订阅的频道名"""
        raise NotImplementedError


class BaseSubscription:
    async def get_updated(self, channel) -> tuple[set[str], set[str], dict[str, dict | None]]:
        raise NotImplementedError

    @property
    def channels(self) -> set[str]:
        raise NotImplementedError


class RowSubscription(BaseSubscription):
    __cache = {}

    def __init__(self, table: ComponentTable, caller: int | str, channel: str, row_id: int):
        self.table = table
        if table.component_cls.permission_ == Permission.OWNER and caller != 'admin':
            self.req_owner = caller
        else:
            self.req_owner = None
        self.channel = channel
        self.row_id = row_id

    @classmethod
    def clear_cache(cls, channel):
        cls.__cache.pop(channel, None)

    async def get_updated(self, channel) -> tuple[set[str], set[str], dict[str, dict | None]]:
        # 如果订阅有交叉，这里会重复被调用，需要一个class级别的cache，但外部每次收到channel消息时要清空该cache
        if (cache := RowSubscription.__cache.get(channel, None)) is not None:
            return set(), set(), cache

        rows = await self.table.direct_query('id', self.row_id, limit=1, row_format='raw')
        if len(rows) == 0:
            # get_updated主要发给客户端，需要json，所以key直接用str
            rtn = {str(self.row_id): None}
        else:
            if self.req_owner is None or int(rows[0].get('owner', 0)) == self.req_owner:
                rtn = {str(self.row_id): rows[0]}
            else:
                rtn = {str(self.row_id): None}
        RowSubscription.__cache[channel] = rtn
        return set(), set(), rtn

    @property
    def channels(self) -> set[str]:
        return {self.channel}


class IndexSubscription(BaseSubscription):
    def __init__(
            self, table: ComponentTable, caller: int | str,
            index_channel: str, last_query, query_param: dict
    ):
        self.table = table
        if table.component_cls.permission_ == Permission.OWNER and caller != 'admin':
            self.req_owner = caller
        else:
            self.req_owner = None
        self.index_channel = index_channel
        self.query_param = query_param
        self.row_subs: dict[str, RowSubscription] = {}
        self.last_query = last_query

    def add_row_subscriber(self, channel, row_id):
        self.row_subs[channel] = RowSubscription(self.table, self.req_owner, channel, row_id)

    async def get_updated(self, channel) -> tuple[set[str], set[str], dict[str, dict | None]]:
        if channel == self.index_channel:
            # 查询index更新，比较row_id是否有变化
            row_ids = await self.table.direct_query(**self.query_param, row_format='id')
            row_ids = set(row_ids)
            inserts = row_ids - self.last_query
            deletes = self.last_query - row_ids
            self.last_query = row_ids
            new_chans = set()
            rem_chans = set()
            rtn = {}
            for row_id in inserts:
                rows = await self.table.direct_query(
                    'id', row_id, limit=1, row_format='raw')
                if len(rows) == 0:
                    self.last_query.remove(row_id)
                    continue  # 可能是刚添加就删了
                else:
                    if self.req_owner is None or int(rows[0].get('owner', 0)) == self.req_owner:
                        rtn[str(row_id)] = rows[0]
                    new_chan_name = self.table.channel_name(row_id=row_id)
                    new_chans.add(new_chan_name)
                    self.row_subs[new_chan_name] = RowSubscription(
                        self.table, self.req_owner, new_chan_name, row_id)
            for row_id in deletes:
                rtn[str(row_id)] = None
                rem_chan_name = self.table.channel_name(row_id=row_id)
                rem_chans.add(rem_chan_name)
                self.row_subs.pop(rem_chan_name)

            return new_chans, rem_chans, rtn
        elif channel in self.row_subs:
            return await self.row_subs[channel].get_updated(channel)

    @property
    def channels(self) -> set[str]:
        return {self.index_channel, *self.row_subs.keys()}


class Subscriptions:
    """
    Component的数据订阅和查询接口
    """

    def __init__(self, backend: Backend):
        self._backend = backend
        self._mq_client = backend.get_mq_client()

        self._subs: dict[str, BaseSubscription] = {}  # key是sub_id
        self._channel_subs: dict[str, set[str]] = {}  # key是频道名， value是set[sub_id]
        self._index_sub_count = 0

    async def close(self):
        return await self._mq_client.close()

    async def mq_pull(self):
        """从MQ获得消息，并存放到本地内存。需要单独的协程反复调用，防止MQ消息堆积。"""
        return await self._mq_client.pull()

    def count(self):
        """获取订阅数，返回row订阅数，index订阅数"""
        return len(self._subs) - self._index_sub_count, self._index_sub_count

    @classmethod
    def _make_query_str(cls, table: ComponentTable, index_name: str, left, right, limit, desc):
        return (f"{table.component_cls.component_name_}.{index_name}"
                f"[{left}:{right}:{desc and -1 or 1}][:{limit}]")

    @classmethod
    def _has_table_permission(cls, table: ComponentTable, caller: int | str) -> bool:
        """判断caller是否对整个表有权限"""
        comp_permission = table.component_cls.permission_
        # admin和EVERYBODY权限永远返回True
        if caller == 'admin' or comp_permission == Permission.EVERYBODY:
            return True
        else:
            # 其他权限要求至少登陆过
            if comp_permission == Permission.ADMIN:
                return False
            if caller and caller > 0:
                return True
            return False

    @classmethod
    def _has_row_permission(cls, table: ComponentTable, caller: int | str, row: dict) -> bool:
        """判断是否对行有权限，首先你要调用_has_table_permission判断是否有表权限"""
        comp_permission = table.component_cls.permission_
        # 非owner权限在_has_table_permission里判断
        if comp_permission != Permission.OWNER:
            return True
        # admin永远返回true
        if caller == 'admin':
            return True
        else:
            if int(row.get('owner', 0)) == caller:
                return True
            else:
                return False

    async def subscribe_select(
            self, table: ComponentTable, caller: int | str, value: any, where: str = 'id'
    ) -> tuple[str | None, np.record | None]:
        """
        获取并订阅单行数据，返回订阅id(sub_id: str)和单行数据(row: dict)。
        如果未查询到数据，或owner不符，返回None, None。
        如果是重复订阅，会返回上一次订阅的sub_id。客户端应该写代码防止重复订阅。
        """
        # 首先caller要对整个表有权限
        if not self._has_table_permission(table, caller):
            return None, None

        if len(rows := await table.direct_query(where, value, limit=1, row_format='raw')) == 0:
            return None, None
        row = rows[0]
        row['id'] = int(row['id'])

        # 再次caller要对该row有权限
        if not self._has_row_permission(table, caller, row):
            return None, None

        # 开始订阅
        sub_id = self._make_query_str(
            table, 'id', row['id'], None, 1, False)
        if sub_id in self._subs:
            logger.warning(f"⚠️ [💾Subscription] {sub_id} 数据重复订阅，检查客户端代码")
            return sub_id, row

        channel_name = table.channel_name(row_id=row['id'])
        await self._mq_client.subscribe(channel_name)

        self._subs[sub_id] = RowSubscription(table, caller, channel_name, row['id'])
        self._channel_subs.setdefault(channel_name, set()).add(sub_id)
        return sub_id, row

    async def subscribe_query(
            self,
            table: ComponentTable,
            caller: int | str,
            index_name: str,
            left,
            right=None,
            limit=10,
            desc=False,
            force=True,
    ) -> tuple[str | None, list[dict]]:
        """
        获取并订阅多行数据，返回订阅id(sub_id: str)，和多行数据(rows: list[dict])。
        如果未查询到数据，返回None, []。
        但force参数可以强制未查询到数据时也订阅，返回订阅id(sub_id: str)，和[]。
        如果是重复订阅，会返回上一次订阅的sub_id。客户端应该写代码防止重复订阅。

        时间复杂度是O(log(N)+M)，N是index的条目数；M是查询到的行数。
        Component权限是OWNER时，查询到的行在最后再根据owner值筛选，M为筛选前的行数。
        """
        # 首先caller要对整个表有权限，不然就算force也不给订阅
        if not self._has_table_permission(table, caller):
            logger.warning(f"⚠️ [💾Subscription] {table.component_cls.component_name_}无调用权限，"
                           f"检查是否非法调用，caller：{caller}")
            return None, []

        rows = await table.direct_query(
            index_name, left, right, limit, desc, row_format='raw')

        # 如果是owner权限，只取owner相同的
        if table.component_cls.permission_ == Permission.OWNER:
            rows = [row for row in rows if self._has_row_permission(table, caller, row)]

        if not force and len(rows) == 0:
            return None, rows

        sub_id = self._make_query_str(table, index_name, left, right, limit, desc)
        if sub_id in self._subs:
            logger.warning(f"⚠️ [💾Subscription] {sub_id} 数据重复订阅，检查客户端代码")
            return sub_id, rows

        index_channel = table.channel_name(index_name=index_name)
        await self._mq_client.subscribe(index_channel)

        row_ids = {int(row['id']) for row in rows}
        idx_sub = IndexSubscription(
            table, caller, index_channel, row_ids,
            dict(index_name=index_name, left=left, right=right, limit=limit, desc=desc))
        self._subs[sub_id] = idx_sub
        self._channel_subs.setdefault(index_channel, set()).add(sub_id)
        self._index_sub_count = list(map(type, self._subs.values())).count(IndexSubscription)

        # 还要订阅每行的信息，这样每行数据变更时才能收到消息
        for row_id in row_ids:
            row_channel = table.channel_name(row_id=row_id)
            await self._mq_client.subscribe(row_channel)
            idx_sub.add_row_subscriber(row_channel, row_id)
            self._channel_subs.setdefault(row_channel, set()).add(sub_id)

        return sub_id, rows

    async def unsubscribe(self, sub_id) -> None:
        """取消订阅数据"""
        if sub_id not in self._subs:
            return

        for channel in self._subs[sub_id].channels:
            self._channel_subs[channel].remove(sub_id)
            if len(self._channel_subs[channel]) == 0:
                await self._mq_client.unsubscribe(channel)
                del self._channel_subs[channel]
        self._subs.pop(sub_id)
        self._index_sub_count = list(map(type, self._subs.values())).count(IndexSubscription)


    async def get_updates(self, timeout=None) -> dict[str, dict[str, dict]]:
        """
        pop之前Subscriptions.mq_pull()到的数据更新通知，然后通过查询数据库取出最新的值，并返回。
        返回值为dict: key是sub_id；value是更新的行数据，格式为dict：key是row_id，value是数据库raw值。
        timeout参数主要给单元测试用，None时堵塞到有消息，否则等待timeout秒。
        """
        mq = self._mq_client
        channel_subs = self._channel_subs

        rtn = {}
        if timeout is not None:
            try:
                async with asyncio.timeout(timeout):
                    updated_channels = await mq.get_message()
            except TimeoutError:
                return rtn
        else:
            updated_channels = await mq.get_message()
        for channel in updated_channels:
            RowSubscription.clear_cache(channel)
            sub_ids = channel_subs.get(channel, [])
            for sub_id in sub_ids:
                sub = self._subs[sub_id]
                # 获取sub更新的行数据
                new_chans, rem_chans, sub_updates = await sub.get_updated(channel)
                # 如果有行添加或删除，订阅或取消订阅
                for new_chan in new_chans:
                    await mq.subscribe(new_chan)
                    channel_subs.setdefault(new_chan, set()).add(sub_id)
                for rem_chan in rem_chans:
                    channel_subs[rem_chan].remove(sub_id)
                    if len(channel_subs[rem_chan]) == 0:
                        await mq.unsubscribe(rem_chan)
                        del channel_subs[rem_chan]
                # 添加行数据到返回值
                if len(sub_updates) > 0:
                    rtn.setdefault(sub_id, dict()).update(sub_updates)
        return rtn
