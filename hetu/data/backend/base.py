"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com

                      事务相关结构
    ┌────────────────┐           ┌──────────────────┐
    │    Backend     ├──────────►│BackendTransaction│     todo 首先要先改名BackendSession, 包含idmap
    │数据库直连池（单件)│           │    事务模式连接     │
    └────────────────┘           └──────────────────┘
            ▲                             ▲
            │初始化数据                     │ 写入数据
  ┌─────────┴──────────┐      ┌───────────┴────────────┐
  │   ComponentTable   │      │  ComponentTransaction  │
  │  组件数据管理（单件)  │      │      组件相关事务操作     │  # todo 改成SessionComponentTable，读写其实是传给idmap，提交也是idmap
  └────────────────────┘      └────────────────────────┘
  todo 直接select出来的就是此类

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

from ..component import BaseComponent

logger = logging.getLogger("HeTu.root")


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

    async def is_synced(self) -> bool:
        """
        检查各个slave数据库和master数据库的数据是否已完成同步。
        主要用于test用例。
        """
        raise NotImplementedError

    async def wait_for_synced(self) -> None:
        """
        等待各个slave数据库和master数据库的数据完成同步。
        主要用于test用例。
        """
        while not await self.is_synced():
            await asyncio.sleep(0.1)

    def requires_head_lock(self) -> bool:
        """
        要求持有head锁，防止启动2台有head标记的服务器。
        所有ComponentTable的create_or_migrate或flush调用时都会调用此方法。
        返回True表示锁定成功，或已持有该锁。
        返回False表示已有别人持有了锁，程序退出。
        """
        raise NotImplementedError

    def transaction(self, cluster_id: int) -> "BackendTransaction":
        """进入db的事务模式，返回事务连接，事务只能在对应的cluster_id中执行，不能跨cluster"""
        raise NotImplementedError

    def get_mq_client(self) -> "MQClient":
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
        if exc_type is None:
            await self.end_transaction(discard=False)
        else:
            await self.end_transaction(discard=True)


class ComponentTable:
    """
    Component数据主类，负责对每个Component数据的初始化操作，并可以启动Component相关的事务操作。
    继承此类，完善所有NotImplementedError的方法。
    """

    def __init__(
        self,
        component_cls: type[BaseComponent],
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
        row_format="struct",
    ) -> np.recarray | list[dict | int]:
        """
        不通过事务直接从servant数据库查询值，不影响Master性能，但没有数据一致性保证。

        .. warning:: ⚠️ 警告：从servant读取值存在更新延迟，且脱离事务，值随时可能被其他进程修改/删除，
        在System中使用要确保逻辑能接受数据不一致。

        Parameters
        ----------
        index_name: str
            查询Component中的哪条索引
        left, right: str or number
            查询范围，闭区间。字符串查询时，可以在开头指定是[闭区间，还是(开区间。
            如果right不填写，则精确查询等于left的数据
        limit: int
            限制返回的行数，越低越快
        desc: bool
            是否降序排列
        row_format:
            'struct': 包装成component struct返回，类型np.record
            'raw': 直接返回数据库中的值，由dict包装，可能包含多余数据，也不会进行类型转换。
            'typed_dict’: 直接返回dict，但是进行类型转换，删除多余数据。
            'id': 只返回row_id列表
        """
        # 请使用servant数据库来操作
        raise NotImplementedError

    async def direct_get(
        self, row_id: int, row_format="struct"
    ) -> None | np.record | dict:
        """
        不通过事务，从servant数据库直接读取某行的值。

        Parameters
        ----------
        row_id: int
            需要读取的行id
        row_format:
            'struct': 包装成component struct返回，类型np.record
            'raw': 直接返回数据库中的值，由dict包装，可能包含多余数据，也不会进行类型转换。
            'typed_dict’: 直接返回dict，但是进行类型转换，删除多余数据。

        .. warning:: ⚠️ 警告：从servant读取值存在更新延迟，且脱离事务，值随时可能被其他进程修改/删除，
        使用时要确保逻辑能接受数据不一致。
        """
        raise NotImplementedError

    async def direct_set(self, row_id: int, **kwargs):
        """
        不通过System事务，直接设置数据库某行的值。

        .. warning:: ⚠️ 警告：由于不在System事务中，如果`direct_set`的逻辑基于`direct_get/query`等的返回值，
        则不保证数据一致性。使用时要确保逻辑能接受数据不一致。
        """
        raise NotImplementedError

    async def direct_insert(self, **kwargs) -> list[int] | None:
        """
        不通过System事务，直接数据库插入行。

        .. warning:: ⚠️ 警告：由于不在System事务中，如果`direct_insert`的逻辑基于`direct_get/query`等的返回值，
        则不保证数据一致性。使用时要确保逻辑能接受数据不一致。

        Returns
        -------
        row_ids: list
        按插入顺序的row id
        """
        raise NotImplementedError

    async def direct_delete(self, row_id: int):
        """
        不通过System事务，直接对数据库删除行。

        .. warning:: ⚠️ 警告：由于不在System事务中，如果`direct_delete`的逻辑基于`direct_get/query`等的返回值，
        则不保证数据一致性。使用时要确保逻辑能接受数据不一致。
        """
        raise NotImplementedError

    def attach(self, backend_trx: BackendTransaction) -> "ComponentTransaction":
        """返回当前组件的事务操作类，并附加到现有的后端事务连接"""
        # 继承，并执行：
        # return YourComponentTransaction(self, backend_trx)
        raise NotImplementedError

    def new_transaction(self) -> tuple[BackendTransaction, "ComponentTransaction"]:
        """返回当前组件的事务操作类，并新建一个后端事务连接"""
        conn = self._backend.transaction(self._cluster_id)
        return conn, self.attach(conn)

    def channel_name(self, index_name: str | None = None, row_id: int | None = None):
        """返回当前组件表，在消息队列中的频道名。表如果有数据变动，会发送到对应频道"""
        raise NotImplementedError


class ComponentTransaction:
    """
    Component的数据表操作接口，和数据库通讯并处理事务的抽象接口。
    继承此类，完善所有NotImplementedError的方法。
    已写的方法可能不能完全适用所有情况，有些数据库可能要重写这些方法。
    """

    def __init__(self, comp_tbl: ComponentTable, trx_conn: BackendTransaction):
        assert trx_conn.cluster_id == comp_tbl.cluster_id, (
            "事务只能在对应的cluster_id中执行，不能跨cluster"
        )
        self._component_cls = comp_tbl.component_cls  # type: type[BaseComponent]
        self._trx_conn = trx_conn  # todo 改成_session_conn
        self._cache = {}  # 事务中缓存数据，key为row_id，value为row
        # insert缓存数据，因为没有id，所以用list存储
        self._insert_caches = np.rec.array(
            np.empty(0, dtype=self._component_cls.dtypes)
        )
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
        self, index_name: str, left, right=None, limit=10, desc=False, lock_index=True
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
        ...     owner: np.int64 = property_field(0, index=True)
        ...     item_id: np.int64 = property_field(0)
        取出所有slot.owner == caller的道具数据：
        >>> @define_system(components=(Slot, Item))
        ... async def get_all_items(ctx):
        ...     slots = await ctx[Slot].query('owner', ctx.caller, limit=100, lock_index=False)
        ...     items = await ctx[Item].get_by_ids(slots.item_id)
        """
        rtn = []
        for row_id in row_ids:
            if (row := self._cache.get(row_id)) is not None:
                if type(row) is str and row == "deleted":
                    raise RaceCondition("gets: row已经被你自己删除了")
                rtn.append(row)
            else:
                if (row := await self._db_get(row_id)) is None:
                    raise RaceCondition("gets: row中途被删除了")
                self._cache[row_id] = row
                rtn.append(row)

        return np.rec.array(np.stack(rtn, dtype=self._component_cls.dtypes))

    async def select(self, value, where: str = "id", lock_row=True) -> None | np.record:
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
        >>> from hetu.data import define_component, property_field
        >>> @define_component
        ... class Item(BaseComponent):
        ...     owner: np.int64 = property_field(0, index=True)
        >>> @define_system(components=(Item, ))
        ... async def some_system(ctx):
        ...     item_row = await ctx[Item].select(ctx.caller, 'owner')
        ...     print(item_row.name)
        """
        assert np.isscalar(value), (
            f"value必须为标量类型(数字，字符串等), 你的:{type(value)}, {value}"
        )
        assert where in self._component_cls.indexes_, (
            f"{self._component_cls.component_name_} 组件没有叫 {where} 的索引"
        )

        if issubclass(type(value), np.generic):
            value = value.item()

        # 查询
        if where == "id":
            row_id = value
        else:
            row_ids = await self._db_query(where, value, limit=1, lock_index=False)
            if len(row_ids) == 0:
                return None
            row_id = int(row_ids[0])

        if (row := self._cache.get(row_id)) is not None:
            if type(row) is str and row == "deleted":
                return None
            else:
                return row.copy()

        # 如果cache里没有row，说明query时后端没有返回行数据，说明后端架构index和行数据是分离的，
        # 由于index是分离的，且不能锁定index(不然事务冲突率很高, 而且乐观锁也要写入时才知道冲突），
        # 所以检测get结果是否在查询范围内，不在就抛出冲突
        if (row := await self._db_get(row_id, lock_row=lock_row)) is None:
            if where == "id":
                return None  # 如果不是从index查询到的id，而是直接传入，那就不需要判断race了
            else:
                raise RaceCondition("select: row中途被删除了")
        if row[where] != value:
            raise RaceCondition(f"select: row.{where}值变动了")

        self._cache[row_id] = row

        return row.copy()

    async def query(
        self,
        index_name: str,
        left,
        right=None,
        limit=10,
        desc=False,
        lock_index=True,
        index_only=False,
        lock_rows=True,
    ) -> np.recarray | list[int]:
        """
        查询 索引`index_name` 在 `left` 和 `right` 之间的数据，限制 `limit` 条，是否降序 `desc`。
        如果 `right` 为 `None`，则查询等于 `left` 的数据。

        Parameters
        ----------
        index_name: str
            查询Component中的哪条索引
        left, right: str or number
            查询范围，闭区间。字符串查询时，可以在开头指定是[闭区间，还是(开区间。
            如果right不填写，则精确查询等于left的数据。
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
        assert np.isscalar(left), (
            f"left必须为标量类型(数字，字符串等), 你的:{type(left)}, {left}"
        )
        assert index_name in self._component_cls.indexes_, (
            f"{self._component_cls.component_name_} 组件没有叫 {index_name} 的索引"
        )

        left = int(left) if np.issubdtype(type(left), np.bool_) else left
        left = left.item() if issubclass(type(left), np.generic) else left
        right = int(right) if np.issubdtype(type(right), np.bool_) else right
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
                    raise RaceCondition(f"select: row.{index_name}值变动了")
                if lock_rows:
                    self._cache[row_id] = row
                rtn.append(row)
            else:
                raise RaceCondition("select: row中途被删除了")

        # 返回numpy array
        if len(rtn) == 0:
            return np.rec.array(np.empty(0, dtype=self._component_cls.dtypes))
        else:
            return np.rec.array(np.stack(rtn, dtype=self._component_cls.dtypes))

    async def is_exist(self, value, where: str = "id") -> tuple[bool, int | None]:
        """查询索引是否存在该键值，并返回row_id，返回值：(bool, int)"""
        assert np.isscalar(value), (
            f"value必须为标量类型(数字，字符串等), 你的:{type(value)}, {value}"
        )
        assert where in self._component_cls.indexes_, (
            f"{self._component_cls.component_name_} 组件没有叫 {where} 的索引"
        )

        if issubclass(type(value), np.generic):
            value = value.item()

        row_ids = await self._db_query(where, value, limit=1, lock_index=True)
        found = len(row_ids) > 0
        return found, found and int(row_ids[0]) or None

    def update_or_insert(self, value, where: str = None) -> UpdateOrInsert:
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
        >>> from hetu.data import define_component, property_field
        >>> @define_component
        ... class Portfolio(BaseComponent):
        ...     owner: np.int64 = property_field(0, index=True)
        ...     cash: np.int64 = property_field(0)
        >>> @define_system(components=(Portfolio, ))
        ... async def deposit_franklin(ctx):
        ...     async with ctx[].update_or_insert(ctx.caller, 'owner') as row:
        ...         row.cash += 100
        """
        return UpdateOrInsert(self, value, where)

    def upsert(self, value, where: str = None) -> UpdateOrInsert:
        return self.update_or_insert(value, where)

    def insert_cache_exists(self, value, where: str) -> bool:
        """检查是否已插入过该值"""
        return (self._insert_caches[where] == value).any()

    async def unique_value_exists(self, value, index_name: str) -> bool:
        """检查单个unique索引是否满足条件"""
        row_ids = await self._db_query(index_name, value, limit=1, lock_index=False)
        if len(row_ids) > 0:
            return True
        return self.insert_cache_exists(value, index_name)

    async def _check_uniques(
        self, old_row: np.record | None, new_row: np.record, ignores=None
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
                if await self.unique_value_exists(new_row[idx_name].item(), idx_name):
                    raise UniqueViolation(
                        f"Unique索引{self._component_cls.component_name_}.{idx_name}，"
                        f"已经存在值为({new_row[idx_name]})的行，无法Update/Insert"
                    )

    async def update(self, row_id: int, row) -> None:
        """修改row_id行的数据"""
        row_id = int(row_id)

        if row_id in self._updt_flags:
            raise KeyError(
                f"{self._component_cls.component_name_}行（id:{row_id}）"
                f"已经在事务中更新过了，不允许重复更新。"
            )
        if row_id in self._del_flags:
            raise KeyError(
                f"{self._component_cls.component_name_}行（id:{row_id}）"
                f"已经在事务中删除了，不允许再次更新。"
            )

        assert type(row) is np.record, "update数据必须是单行数据"

        if row.id != row_id:
            raise ValueError(f"更新的row.id {row.id} 与传入的row_id {row_id} 不一致")

        # 先查询旧数据是否存在，一般update调用时，旧数据都在_cache里，不然你哪里获得的row数据
        old_row = self._cache.get(row_id)  # or await self._db_get(row_id)
        if old_row is None:
            raise KeyError(
                f"{self._component_cls.component_name_} 组件没有id为 {row_id} 的行"
            )

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
        assert type(rows) is np.recarray and rows.shape[0] > 1, (
            "update_rows数据必须是多行数据"
        )
        for i, id_ in enumerate(rows.id):
            await self.update(id_, rows[i])

    async def insert(self, row: np.record) -> None:
        """
        插入单行数据。

        Examples
        --------
        >>> from hetu.system import define_system
        >>> from hetu.data import define_component, property_field
        >>> @define_component
        ... class Item(BaseComponent):
        ...     owner: np.int64 = property_field(0, index=True)
        ...     model: str = property_field("", dtype='<U8')
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
        ...     inserted_ids = await ctx.end_transaction(discard=False)
        ...     ctx.user_data['my_id'] = inserted_ids[0]  # 如果事务冲突，这句不会执行

        ⚠️ 注意：调用完end_transaction，ctx将不再能够获取Components
        """
        assert type(row) is np.record, "插入数据必须是单行数据"
        assert row.id == 0, "插入数据要求 row.id == 0"

        # 提交到事务前先检查无unique冲突
        await self._check_uniques(None, row, ignores={"id"})

        # 加入到更新队列
        row = row.copy()
        self._trx_insert(row)
        self._insert_caches = np.append(self._insert_caches, row)

    async def delete(self, row_id: int | np.integer) -> None:
        """删除row_id行"""
        row_id = int(row_id)

        if row_id in self._updt_flags:
            raise KeyError(
                f"{self._component_cls.component_name_} 行（id:{row_id}）"
                f"在事务中已有update命令，不允许再次删除。"
            )
        if row_id in self._del_flags:
            raise KeyError(
                f"{self._component_cls.component_name_} 行（id:{row_id}）"
                f"已经在事务中删除了，不允许重复删除。"
            )

        # 先查询旧数据是否存在
        old_row = self._cache.get(row_id) or await self._db_get(row_id)
        if old_row is None:
            raise KeyError(
                f"{self._component_cls.component_name_} 组件没有id为 {row_id} 的行"
            )

        old_row = old_row.copy()  # 因为要放入_updates，从cache获取的，得copy防止修改

        # 标记删除
        self._cache[row_id] = "deleted"
        self._del_flags.add(row_id)
        self._trx_delete(row_id, old_row)

    async def delete_rows(self, row_ids: list[int] | np.ndarray) -> None:
        assert type(row_ids) is np.ndarray and row_ids.shape[0] > 1, (
            "deletes数据必须是多行数据"
        )
        for row_id in row_ids:
            await self.delete(row_id)


class UpdateOrInsert:
    def __init__(self, comp_trx: ComponentTransaction, value, where):
        if where not in comp_trx.component_cls.uniques_:
            raise ValueError(
                "UpdateOrInsert只能用于unique索引，"
                f"{comp_trx.component_cls.component_name_}组件的{where}不是unique索引"
            )
        self.comp_trx = comp_trx
        self.value = value
        self.where = where
        self.row = None
        self.row_id = None

    async def commit(self):
        if self.row_id == 0:
            # 如果是insert，但是where依据却存在，说明违反unique约束，重试即可
            if await self.comp_trx.unique_value_exists(self.value, self.where):
                raise RaceCondition("upsert决定插入数据时，发现unique冲突")
            await self.comp_trx.insert(self.row)
        else:
            await self.comp_trx.update(self.row_id, self.row)

    async def __aenter__(self):
        if self.comp_trx.insert_cache_exists(self.value, self.where):
            # todo: 更好的实现，应该撤销上一次insert，然后获取上一次的row值作为select结果返回
            #       目前redis insert是stack的，无法索引撤销
            raise UniqueViolation(
                f"upsert: 事务中已经插入过该值 ({self.where}: {self.value})，"
                f"违反unique约束"
            )

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

    # todo 加入到config中去，设置服务器的通知tick
    UPDATE_FREQUENCY = 10  # 控制客户端所有订阅的数据（如果有变动），每秒更新几次

    async def close(self):
        raise NotImplementedError

    async def pull(self) -> None:
        """
        从消息队列接收一条消息到本地队列，消息内容为channel名，每行数据，每个Index，都是一个channel。
        该channel收到了任何消息都说明有数据更新，所以只需要保存channel名。

        消息存放本地时，需要用时间作为索引，并且忽略重复的消息。存放前先把2分钟前的消息丢弃，防止堆积。
        此方法需要单独的协程反复调用，防止服务器也消息堆积。
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
    async def get_updated(
        self, channel
    ) -> tuple[set[str], set[str], dict[str, dict | None]]:
        raise NotImplementedError

    @property
    def channels(self) -> set[str]:
        raise NotImplementedError
