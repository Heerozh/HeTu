"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

from typing import TYPE_CHECKING, cast

import numpy as np

from ...i18n import _
from .base import BackendClient, RowFormat, UniqueViolation
from .idmap import RangeObservation, RowState
from .table import TableReference

if TYPE_CHECKING:
    from hetu.data.component import BaseComponent

    from .session import Session

IndexScalar = (
    np.integer
    | np.floating
    | np.str_
    | np.bytes_
    | np.bool_
    | float
    | str
    | bytes
    | bool
)
Int64 = np.int64 | int


class SessionRepository:
    """从数据库查询数据并放入Session缓存，`System`的`ctx.repo[...]`返回的便是此类。"""

    def __init__(self, session: Session, comp_cls: type[BaseComponent]) -> None:
        self._session = session
        self.ref: TableReference = TableReference(
            comp_cls, session.instance_name, session.cluster_id
        )
        """`TableReference`对象，表示当前repo关联的数据库`Table`信息"""

    @property
    def session(self) -> Session:
        """获取所属的内部`Session`对象。"""
        return self._session

    def _local_has_unique_conflicts(self, row: np.record, fields: set) -> str | None:
        """
        在Session本地缓存中检查Unique索引冲突。
        """
        idmap = self._session.idmap
        ref = self.ref
        for unique_index in fields:
            value = row[unique_index]
            rows = idmap.filter(ref, **{unique_index: value})
            if len(rows) > 0:
                return unique_index
        return None

    async def remote_has_unique_conflicts_(
        self, row: np.record, fields: set
    ) -> str | None:
        """
        内部方法，在远程数据库中检查Unique索引冲突。
        """
        session = self._session
        client = session.master_or_servant
        ref = self.ref
        for unique_index in fields:
            value: np.generic = row[unique_index]
            existing_row = await client.range(
                ref,
                unique_index,
                value.item(),
                value.item(),
                1,
                False,
                RowFormat.ID_LIST,
            )
            if len(existing_row) > 0:
                # 如果existing_row的id存在于mark_deleted中，则不算冲突
                if session.idmap.is_deleted(ref, existing_row[0]):
                    continue
                return unique_index
        return None

    def _get_changed_fields(self, row: np.record):
        """
        根据row.id对比缓存，获取修改的字段列表。
        如果id在修改字段中，表示没有找到旧数据。
        """
        idmap = self._session.idmap
        old_row, row_stat = idmap.get(self.ref, row["id"])
        assert row.dtype.names  # for type checker, could be removed by python -O
        if old_row is None or row_stat == RowState.DELETE:
            return set(row.dtype.names)
        else:
            return {key for key in row.dtype.names if old_row[key] != row[key]}

    async def is_unique_conflicts(
        self, row: np.record, insert=False
    ) -> tuple[str | None, bool]:
        """
        检查一行数据的Unique索引在本地和远程数据库中是否有冲突。

        这是**可选的提前检查**（每个 unique 字段 1 次往返）：`insert` / `update` 默认不再
        调用本方法，等价的判定在 `commit()` 时由后端原子执行（见
        `IdentityMap.get_absent_unique_fields`）。想在事务体内提前失败、或据此分支时可
        显式调用。

        Parameters
        ----------
        row : np.record
            待检查的行数据，必须是 `c-struct` 格式。
        insert : bool
            如果为True，表示这是一个插入操作，否则是更新操作，要求之前已获取过旧数据。

        Returns
        -------
        (field, is_race)
            `field` 为冲突的unique列名，无冲突时为 None。`is_race` 为 True 表示该冲突
            应作为 `RaceCondition` 处理：远程冲突命中了一个「本事务曾观察其不存在」的列
            （基于过期快照的乐观并发失败，重试可解）；False 则是确定性冲突，应
            `UniqueViolation`。

            注意优先级：只要有任一 observed-absent 列发生远程冲突就判为竞态，即便另有
            非 observed-absent 列也冲突——否则像 `upsert` 在锚定列与其他unique列同时撞车
            时会错误退化为 UniqueViolation，破坏「重试后转为 update」的语义。
        """
        changed_fields = self._get_changed_fields(row)
        if not insert:
            # 如果有id字段，表示没有找到旧数据
            assert "id" not in changed_fields, _(
                "row id({row_id})在session中未寻到，更新操作必须有旧数据。"
            ).format(row_id=row.id)
        else:
            assert "id" in changed_fields, _(
                "session中已存在该row id({row_id})，插入操作必须没有旧数据。"
            ).format(row_id=row.id)

        changed_fields = changed_fields & self.ref.comp_cls.uniques_

        # 本地（同事务）冲突：确定性，非竞态
        if field := self._local_has_unique_conflicts(row, changed_fields):
            return field, False

        # 远程冲突：优先判定「本事务曾观察其不存在」的列 → 竞态
        idmap = self._session.idmap
        absent_fields = {
            f for f in changed_fields if idmap.observed_absent(self.ref, f, row[f])
        }
        if field := await self.remote_has_unique_conflicts_(row, absent_fields):
            return field, True
        if field := await self.remote_has_unique_conflicts_(
            row, changed_fields - absent_fields
        ):
            return field, False

        return None, False

    async def get_by_id(self, row_id: Int64) -> np.record | None:
        """
        从数据库获取单行数据，并放入`Session`缓存。
        本指令如果命中缓存，不会去数据库查询。
        """
        # 主键查询，先查缓存
        row_id = cast(int, row_id)
        row, row_stat = self._session.idmap.get(self.ref, row_id)
        if row_stat is not None:
            return None if row_stat == RowState.DELETE else row
        return await self._fetch_by_id(row_id)

    async def _fetch_by_id(self, row_id: int) -> np.record | None:
        """缓存未命中：查数据库，读到就放进 Session 缓存"""
        row = await self._session.master_or_servant.get(
            self.ref, row_id, RowFormat.STRUCT
        )
        if row is not None:
            self._session.idmap.add_clean(self.ref, row)
        return row

    async def get(
        self,
        index_name: str | None = None,
        query_value: IndexScalar | None = None,
        **kwargs: IndexScalar,
    ) -> np.record | None:
        """
        从数据库获取单行数据，并放入Session缓存。
        推荐通过"id"主键查询，这样无须查询索引，如果缓存命中，不会去数据库查询；否则会执行1-2次查询。
        主键或 unique 列读空会登记"本事务观察到该值不存在"：同一事务内再次 `get` 同一值直接返回
        None（不再查询数据库），commit 时若该值已被并发写入则判为 `RaceCondition` 重试。
        结果按本事务眼里的数据：本事务新 insert 的行能查到，删掉的、已改走这个值的行不算匹配。

        非 unique 列同样会在提交时校验：读空而提交前已有匹配的行（被并发插入，或读到了滞后的
        副本）判 `RaceCondition`，所以"get 为 None 就 insert"的写法是安全的；命中时只保证返回
        的这一行没被改过，之后别的事务再插入同值的行不算冲突。

        Parameters
        ----------
        index_name: str | None
            辅助参数，如果不便使用kwargs参数时使用。
        query_value: IndexScalar | None
            辅助参数，如果不便使用kwargs参数时使用。
        kwargs: IndexScalar
            查询字段和值，例如 `id=1234567890`。只能查询一个字段，且该字段必须有索引。

        Examples
        --------
        ::

            item = await session.using(Item).get(id=1234567890)

        Returns
        -------
        row: np.record or None
            如果未查询到匹配数据，则返回 None。如果查询到数据，则返回查询到的第一行数据。
            返回 np.record (c-struct) 格式。
        """
        if index_name is None or query_value is None:
            # 判断kwargs有且只有一个键值对
            assert len(kwargs) == 1, "Only one field can be queried."
            index_name, query_value = next(iter(kwargs.items()))

        comp_cls = self.ref.comp_cls

        if index_name not in comp_cls.indexes_:
            raise ValueError(
                _("{comp_name} 组件没有叫 {index_name} 的索引").format(
                    comp_name=comp_cls.name_, index_name=index_name
                )
            )

        idmap = self._session.idmap
        # 如果不是主键，直接用range方法
        if index_name != "id":
            # 去cache查询（含本事务新 insert 的行，所以要先于 negative cache）
            rows = idmap.filter(self.ref, **{index_name: query_value})
            if len(rows) > 0:
                return rows[0]

            # negative cache：本事务已观察过该值不存在，事务内可重复读，不再打远程
            # （upsert 内部会再 get 一次锚定值，SystemLock 等流程因此省一次往返）
            is_unique = index_name in comp_cls.uniques_
            if is_unique and idmap.observed_absent(self.ref, index_name, query_value):
                return None

            # cache未命中，去数据库查询。本事务删掉的、改走了这个索引值的行，提交前还在数据库
            # 索引的原值上，会占掉读到的名额：多读这么多行，剩下的里面才一定有真匹配的（如果
            # 有）；读空时也就读全了这个值，校验的是整个值上没有别的行
            moved = idmap.moved_away(self.ref, index_name)
            rows, obs = await self._range_rows(
                index_name, query_value, None, 1 + len(moved), False, True
            )
            if moved:
                # 删掉的 _range_rows 已经排除了，改走的也去掉：它们已经不匹配这个值了
                rows = rows[~np.isin(rows.id, list(moved))]
            if obs is not None:
                # 命中：get 的约定是"返回一行匹配的"，只保护这一行（VER），不校验有没有同值
                # 新行排到它前面，省一次区间校验；读空：要校验这个值上仍然没有行
                obs.rows_only = rows.shape[0] > 0
                idmap.add_range_observation(self.ref, obs)
            if rows.shape[0] > 0:
                return rows[0]
            # 等值查询unique列读空：登记negative observation，供commit判定竞态。
            # （区间range查询不登记negative observation，区间无穷且本就不保证事务内可见性。）
            if is_unique:
                idmap.mark_absent(self.ref, index_name, query_value)
            return None
        else:
            row_id = int(query_value)
            # 先查cache（含本事务新 insert 的行），再看 negative cache，最后才去数据库
            row, row_stat = idmap.get(self.ref, row_id)
            if row_stat is not None:
                return None if row_stat == RowState.DELETE else row
            if idmap.observed_absent(self.ref, "id", row_id):
                return None
            row = await self._fetch_by_id(row_id)
            if row is None:
                # 主键id恒为unique，登记“本事务观察到该id不存在”
                idmap.mark_absent(self.ref, "id", row_id)
            return row

    async def range(
        self,
        index_name: str | None = None,
        _left: IndexScalar | None = None,  # 参数前加_防止和用户字段冲突
        _right: IndexScalar | None = None,
        limit: int = 10,
        desc: bool = False,
        phantom_check: bool = True,
        **kwargs: tuple[IndexScalar, IndexScalar],
    ) -> np.recarray:
        """
        从数据库查询索引，返回区间内数据，限制 `limit` 条。
        本指令会去数据库执行 1～2 次往返：先查索引拿 id 列表，缓存未命中的行再一次批量读回。

        与 `get` 不同，本方法的区间匹配只读取**已提交**的数据，不会读取当前事务中未提交
        的修改：当前事务内新 `insert` 的行、或索引字段被改动的行，不会反映在返回结果里
        （但已 `delete` 的行仍会被正确排除）。如需读取事务内新插入的行，请改用 `get`。

        读到的区间会在提交时校验（防幻读）：若同样的查询届时会返回不同的行——别的事务往
        区间里插了一行、删改了返回的行，或者这次读到的是滞后的副本——提交时抛
        `RaceCondition`，`System` 会自动重试。所以"range 查不到就 insert、查到就 update"
        的写法是安全的。只读事务不提交，不受影响。

        截断读（数据库返回了 `limit` 行）只保护看到的前 `limit` 行：区间里排在最后一个返回行
        之后的行本来就没读到，它们的增减不算冲突。**用 range 判断"有没有"时必须读全**
        （`limit=-1`），否则没读到的行会被当成不存在。本事务删掉的行不在返回结果里、却占着
        `limit` 的名额，所以返回行数少于 `limit` 不代表读全了。

        Parameters
        ----------
        index_name: str | None
            辅助参数，如果不便使用kwargs参数时使用。
        _left: IndexScalar | None
            辅助参数，如果不便使用kwargs参数时使用。
        _right: IndexScalar | None
            辅助参数，如果不便使用kwargs参数时使用。
        kwargs: IndexScalar
            查询字段和区间，例如 `level=(1, 10)`。只能查询一个字段，且该字段必须有索引。
            默认闭区间，如果要自定义区间，请转换为字符串并开头指定 `(` 或 `[`。
            * 如果要查询的字段和参数冲突，请使用辅助参数方式。
        limit: int
            限制返回的行数，越少越快。负数表示不限制行数。
        desc: bool
            是否降序排列
        phantom_check: bool
            提交时是否校验区间，默认 True。读写频繁的区间（如"读最新 N 条消息再插一条"），
            且逻辑不依赖"区间里没有别的行"时可关掉，避免无谓的冲突重试。关掉后返回的行仍然
            参与版本校验，只是不管区间里新增的行。

        Returns
        -------
        row: np.recarray
            返回 `numpy.recarray`，如果没有查询到数据，返回空 `numpy.recarray`。
            `numpy.recarray` 是一种 c-struct array。

        Notes
        -----
        如何复合条件查询？
        请利用python的特性，先在数据库上筛选出最少量的数据，然后本地二次筛选::

            items = await session.using(Item).range(level=(10, 20), limit=100)
            few_items = items[items.amount < 10]

        由于python numpy支持SIMD，比直接在数据库复合查询快。
        """
        if index_name is None and _left is None:
            # 判断kwargs有且只有一个键值对
            assert len(kwargs) == 1, "Only one field can be queried."
            index_name, (_left, _right) = next(iter(kwargs.items()))
        else:
            assert index_name, _("不使用kwargs形式时，index_name不能为空")
            assert _left is not None, _("不使用kwargs形式时，left不能为空")

        # assert np.isscalar(left), (
        #     f"left必须为标量类型(数字，字符串等), 你的:{type(left)}, {left}"
        # )
        # assert np.isscalar(right), (
        #     f"right必须为标量类型(数字，字符串等), 你的:{type(right)}, {right}"
        # )

        comp_cls = self.ref.comp_cls

        # 判断index_name存在
        if index_name not in comp_cls.indexes_:
            raise ValueError(
                _("{comp_name} 组件没有叫 {index_name} 的索引").format(
                    comp_name=comp_cls.name_, index_name=index_name
                )
            )

        rows, obs = await self._range_rows(
            index_name, _left, _right, limit, desc, phantom_check
        )
        if obs is not None:
            self._session.idmap.add_range_observation(self.ref, obs)
        return rows

    async def _range_rows(
        self,
        index_name: str,
        left: IndexScalar,
        right: IndexScalar | None,
        limit: int,
        desc: bool,
        phantom_check: bool,
    ) -> tuple[np.recarray, RangeObservation | None]:
        """
        range 的主体：查索引、取行、放入缓存，返回行和这次读取的观察（不校验区间时为
        None）。观察由调用方登记：range 原样登记，get 命中时改成只保护返回的行。
        """
        comp_cls = self.ref.comp_cls
        if isinstance(left, np.generic):
            left = left.item()
        if isinstance(right, np.generic):
            right = right.item()

        # 先查询 id 列表；要校验区间的，顺便拿回这次读取的观察，commit 时由后端校验
        client = self._session.master_or_servant
        obs: RangeObservation | None = None
        if phantom_check and limit != 0:
            row_ids, obs = await client.range_read_(
                self.ref, index_name, left, right, limit, desc
            )
        else:
            row_ids = await client.range(
                self.ref, index_name, left, right, limit, desc, RowFormat.ID_LIST
            )

        idmap = self._session.idmap
        # 等值点查的值（判定规则同订阅侧 point_query_value_），不是点查为 None
        point = BackendClient.point_query_value_(
            comp_cls.dtype_map_[index_name], left, right
        )
        # unique 列读空时与 get 一样登记 negative observation，让"先 range 确认不存在再写"
        # 的写法撞车时判竞态而非 UniqueViolation。区间查询不登记：区间无穷且本就不保证事务内
        # 可见性。
        if not row_ids and point is not None and index_name in comp_cls.uniques_:
            idmap.mark_absent(self.ref, index_name, point)

        # 再按 id 取行：命中 Session 缓存的直接用（含本事务的修改，已删除的排除），
        # 未命中的 id 一次 get_many 批量读回并放入缓存（N 行 1 次往返，而非逐行 get）。
        # 取行和查 id 用同一个节点：各自随机选的话，节点间的复制进度不同，读取一致性核对
        # 会把这种滞后误判成竞态
        rows: list[np.record | None] = []
        miss_slots: list[int] = []
        miss_ids: list[int] = []
        for _id in row_ids:
            row, row_stat = idmap.get(self.ref, _id)
            if row_stat is None:
                miss_slots.append(len(rows))
                miss_ids.append(_id)
                rows.append(None)  # 占位，保持索引顺序
            elif row_stat != RowState.DELETE:
                rows.append(row)
        missing: list[int] = []
        if miss_ids:
            fetched = cast(
                list[np.record | None],
                await client.get_many(self.ref, miss_ids, RowFormat.STRUCT),
            )
            # 读不到的行是 ZRANGE 与读行之间刚被删除的，跳过（区间观察里记下，见下）
            found = [r for r in fetched if r is not None]
            if found:
                idmap.add_clean(
                    self.ref, np.rec.array(np.stack(found, dtype=comp_cls.dtypes))
                )
            for slot, _id, r in zip(miss_slots, miss_ids, fetched):
                rows[slot] = r
                if r is None:
                    missing.append(_id)
        result = [r for r in rows if r is not None]

        if obs is not None:
            # 取行时有行已被删，读到的就不是任何一刻的区间，commit 会直接判竞态
            obs.point = point
            obs.missing = missing

        # 转换成 np.recarray 返回
        if len(result) == 0:
            return np.rec.array(np.empty(0, dtype=comp_cls.dtypes)), obs
        else:
            return np.rec.array(np.stack(result, dtype=comp_cls.dtypes)), obs

    async def insert(self, row: np.record) -> None:
        """
        向Session中添加一行待插入数据。

        只在本地 IdentityMap 检查 unique：同一事务内已有同值行 → 立即抛 `UniqueViolation`。
        与数据库既有数据的主键 / unique 冲突不在此检查（0 往返），由 `commit()` 原子判定：
        本事务曾 `get` 观察该值不存在 → `RaceCondition`（自动重试），否则 → `UniqueViolation`。
        要提前确认可调用 `is_unique_conflicts`。

        Parameters
        ----------
        row: np.record
            待插入的行数据，必须是 `c-struct` 格式。
        """
        assert row["_version"] == 0, "Insert row's _version must be 0."
        if self._session.explicit_ids_only and int(row["id"]) == 0:
            raise ValueError(
                _(
                    "{comp_name} 的 insert 行 id 为 0：本 Session 不发雪花号，"
                    "请用 new_row(id_=...) 显式给出非零 id"
                ).format(comp_name=self.ref.comp_cls.name_)
            )

        changed_fields = self._get_changed_fields(row)
        assert "id" in changed_fields, _(
            "session中已存在该row id({row_id})，插入操作必须没有旧数据。"
        ).format(row_id=row.id)

        # 本地（同事务）unique 检查，0 往返；与库中既有数据的冲突由 commit 判定
        if field := self._local_has_unique_conflicts(
            row, changed_fields & self.ref.comp_cls.uniques_
        ):
            raise UniqueViolation(
                f"Insert failed: row.{field} violates a unique index "
                "(duplicate within transaction)"
            )

        self._session.idmap.add_insert(self.ref, row)

    async def update(self, row: np.record) -> None:
        """
        向Session中添加一行待更新数据。

        只在本地 IdentityMap 检查 unique（同事务内重复 → 立即抛 `UniqueViolation`）；
        与数据库既有数据的冲突由 `commit()` 判定，规则同 `insert`。

        Parameters
        ----------
        row : np.record
            待更新的行数据，必须是 `c-struct` 格式。
        """
        changed_fields = self._get_changed_fields(row)
        # 检查row.id在cache中存在
        if "id" in changed_fields:
            raise LookupError("Cannot update: row id not found in cache.")

        # 检查和cache中的_version一致
        if "_version" in changed_fields:
            raise ValueError("Cannot update _version field.")

        # 检查有修改的列
        if len(changed_fields) == 0:
            raise ValueError("No fields changed, cannot update.")

        # 本地（同事务）unique 检查，0 往返；与库中既有数据的冲突由 commit 判定
        if field := self._local_has_unique_conflicts(
            row, changed_fields & self.ref.comp_cls.uniques_
        ):
            raise UniqueViolation(
                f"Update failed: row.{field} violates a unique index "
                "(duplicate within transaction)"
            )

        self._session.idmap.update(self.ref, row)

    def upsert(self, **kwargs: IndexScalar) -> UpsertContext:
        """
        使用async with语法，根据Unique索引，查询并返回一行数据，如果不存在则返回新行数据。
        在退出上下文时，自动插入新行，或是更新已有行。

        Examples
        --------
        ::

            async with session.using(Order).upsert(id=1234567890) as order:
                order.status = "completed"

        Parameters
        ----------
        kwargs: IndexScalar
            查询字段和值，例如 `id=1234567890`。只能查询一个字段，且该字段必须为unique索引。
        """
        # 判断kwargs有且只有一个键值对
        assert len(kwargs) == 1, "Only one field can be queried."
        index_name, query_value = next(iter(kwargs.items()))

        # 检查index_name存在且为unique索引
        comp_cls = self.ref.comp_cls
        assert index_name in comp_cls.uniques_, _(
            "upsert只能用于unique索引，{comp_name}组件的{index_name}不是unique索引"
        ).format(comp_name=comp_cls.name_, index_name=index_name)

        return UpsertContext(self, index_name, query_value)

    def delete(self, row_id: int) -> None:
        """
        向Session中添加一行待删除数据。

        Parameters
        ----------
        row_id : int
            待删除行的主键ID。
        """
        old_row, row_stat = self._session.idmap.get(self.ref, row_id)
        if old_row is None or row_stat == RowState.DELETE:
            raise LookupError("Row not existing: not in cache or already deleted.")

        self._session.idmap.mark_deleted(self.ref, row_id)


class UpsertContext:
    """用于在事务中执行UpdateOrInsert操作的上下文管理器。"""

    def __init__(
        self, repo: SessionRepository, index_name: str, query_value: IndexScalar
    ) -> None:
        self.clean_data = None
        self.row_data = None
        self.insert = None
        self.repo = repo
        self.index_name = index_name
        self.query_value = query_value

    async def __aenter__(self) -> np.record:
        existing_row = await self.repo.get(self.index_name, self.query_value)
        if existing_row is not None:
            self.row_data = existing_row
            self.clean_data = existing_row.copy()
            self.insert = False
        elif self.index_name == "id":
            # 锚定主键：新行直接用锚定值做 id，不发雪花号（也就不依赖 SnowflakeID 初始化）
            self.row_data = self.repo.ref.comp_cls.new_row(id_=int(self.query_value))
            self.insert = True
        elif self.repo.session.explicit_ids_only:
            raise LookupError(
                _(
                    "{comp_name}.{index_name}={value} 不存在：本 Session 不发雪花号，"
                    "upsert 只允许命中已有行，或改用 upsert(id=...) 显式给出 id"
                ).format(
                    comp_name=self.repo.ref.comp_cls.name_,
                    index_name=self.index_name,
                    value=self.query_value,
                )
            )
        else:
            self.row_data = self.repo.ref.comp_cls.new_row()
            self.row_data[self.index_name] = self.query_value
            self.insert = True
        return self.row_data

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type is None:
            assert self.row_data is not None
            if self.insert:
                # 锚定字段在 __aenter__ 已 get 读空、并被登记为“观察不存在”。若提交前被
                # 并发插入，commit 会据此把锚定字段的冲突判为 RaceCondition（重试后 get
                # 命中转 update）；非锚定的其他unique列冲突仍是确定性 UniqueViolation。
                # 见 IdentityMap.get_absent_unique_fields。
                await self.repo.insert(self.row_data)
            else:
                if self.row_data == self.clean_data:
                    # 无修改不更新
                    return
                await self.repo.update(self.row_data)
