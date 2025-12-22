"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import asyncio
import logging
import random
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast, final, overload, override

from msgspec import msgpack
import numpy as np
import redis

from ..base import BackendClient, RaceCondition, RowFormat

if TYPE_CHECKING:
    import redis.asyncio
    import redis.asyncio.cluster
    import redis.cluster
    import redis.exceptions

    from ...component import BaseComponent
    from ..idmap import IdentityMap
    from ..table import TableReference
    from .maint import RedisTableMaintenance
    from .worker_keeper import RedisWorkerKeeper

logger = logging.getLogger("HeTu.root")


@final
class RedisBackendClient(BackendClient, alias="redis"):
    """和Redis后端的操作的类，服务器启动时由server.py根据Config初始化"""

    @staticmethod
    def _get_referred_components() -> list[type["BaseComponent"]]:
        """获取当前app用到的Component列表"""
        from ....system.definer import SystemClusters

        return [comp_cls for comp_cls in SystemClusters().get_components().keys()]

    def _lua_schema_definitions(self) -> str:
        """生成lua脚本里用到的schema定义部分"""
        # todo 不该在这耦合system的东西， lua改成直接stack cmd

        # 读取namespace下的所有schema定义，然后替换lua脚本里的schema定义
        # ["User:{CLU1}"] = {
        #     unique = { ["email"] = true, ["phone"] = true },
        #     indexes = { ["email"] = false, ["age"] = true, ["phone"] = true }
        # }
        lua_schema_def = ["{"]
        for comp_cls in self._get_referred_components():
            lua_schema_def.append(f'["{comp_cls.component_name_}"] = {{')
            # unique
            lua_schema_def.append("unique = {")
            for field in comp_cls.uniques_:
                lua_schema_def.append(f'["{field}"] = true,')
            lua_schema_def.append("},")
            # indexes
            lua_schema_def.append("indexes = {")
            for field, is_str in comp_cls.indexes_.items():
                str_flag = "true" if is_str else "false"
                lua_schema_def.append(f'["{field}"] = {str_flag},')
            lua_schema_def.append("},")
            lua_schema_def.append("},")
        lua_schema_def.append("}")
        return "\n".join(lua_schema_def)

    def load_commit_scripts(self, file: str | Path):
        assert self._async_ios, "连接已关闭，已调用过close"
        assert self.is_servant is False, (
            "Servant不允许加载Lua事务脚本，Lua事务脚本只能在Master上加载"
        )
        assert len(self._async_ios) == 1, (
            "Lua事务脚本只能在Master上加载，但当前连接池中有多个服务器"
        )
        # read file to text
        with open(file, "r", encoding="utf-8") as f:
            script_text = f.read()

        # replace PLACEHOLDER_SCHEMA_DEFINITIONS in script_text
        script_text = script_text.replace(
            "PLACEHOLDER_SCHEMA", self._lua_schema_definitions()
        )

        with open(str(file) + ".debug.lua", "w", encoding="utf-8") as f:
            _ = f.write(script_text)

        # 上传脚本到服务器使用同步io
        self._ios[0].script_load(script_text)
        # 注册脚本到异步io，因为master只能有一个连接，直接[0]就行了
        return self._async_ios[0].register_script(script_text)  # pyright: ignore[reportAttributeAccessIssue]

    @property
    def io(self) -> redis.Redis | redis.cluster.RedisCluster:
        """随机返回一个同步连接"""
        return random.choice(self._ios)

    @property
    def aio(self):
        """随机返回一个异步连接"""
        if self.loop_id == 0:
            self.loop_id = hash(asyncio.get_running_loop())
        # redis-py的async connection用的python的steam.connect，绑定到当前协程
        # 而aio是一个connection pool，断开的连接会放回pool中，所以aio不能跨协程传递
        assert hash(asyncio.get_running_loop()) == self.loop_id, (
            "Backend只能在同一个coroutine中使用。检测到调用此函数的协程发生了变化"
        )

        return random.choice(self._async_ios)

    @staticmethod
    def table_prefix(table_ref: TableReference) -> str:
        """获取redis表名前缀"""
        return f"{table_ref.instance_name}:{table_ref.comp_cls.component_name_}"

    @staticmethod
    def cluster_prefix(table_ref: TableReference) -> str:
        """获取redis表名前缀"""
        return (
            f"{table_ref.instance_name}:{table_ref.comp_cls.component_name_}:"
            f"{{CLU{table_ref.cluster_id}}}"
        )

    @classmethod
    def row_key(cls, table_ref: TableReference, row_id: str | int) -> str:
        """获取redis表行的key名"""
        return f"{cls.cluster_prefix(table_ref)}:id:{str(row_id)}"

    @classmethod
    def index_key(cls, table_ref: TableReference, index_name: str) -> str:
        """获取redis表索引的key名"""
        return f"{cls.cluster_prefix(table_ref)}:index:{index_name}"

    async def reset_async_connection_pool(self):
        """重置异步连接池，用于协程切换后，解决aio不能跨协程传递的问题"""
        self.loop_id = 0
        for aio in self._async_ios:
            if isinstance(aio, redis.asyncio.cluster.RedisCluster):
                await aio.aclose()  # 未测试
            else:
                aio.connection_pool.reset()

    # ============ 继承自BackendClient的方法 ============

    def __init__(self, endpoint: str | list[str], clustering: bool, is_servant=False):
        super().__init__(endpoint, clustering, is_servant)
        # redis的endpoint配置为url, 或list of url
        self.urls = [endpoint] if type(endpoint) is str else endpoint
        assert len(self.urls) > 0, "必须至少指定一个数据库连接URL"

        # 创建连接
        self._ios: list[redis.Redis | redis.cluster.RedisCluster] = []
        self._async_ios: list[
            redis.asyncio.Redis | redis.asyncio.cluster.RedisCluster
        ] = []
        for url in self.urls:
            if self.clustering:
                self._ios.append(redis.cluster.RedisCluster.from_url(url))
                self._async_ios.append(redis.asyncio.cluster.RedisCluster.from_url(url))
            else:
                self._ios.append(redis.Redis.from_url(url))
                self._async_ios.append(redis.asyncio.Redis.from_url(url))

        # 测试连接是否正常
        for i, io in enumerate(self._ios):
            try:
                io.ping()
            except redis.exceptions.ConnectionError as e:
                raise ConnectionError(f"无法连接到Redis数据库：{self.urls[i]}") from e

        # 获得db index
        if self.clustering:
            self.dbi = 0  # 集群模式没有db的概念，默认0
        else:
            io = self._ios[0]
            assert isinstance(io, redis.Redis)  # for type checking
            self.dbi = io.connection_pool.connection_kwargs["db"]

        self.lua_commit = None

        # 限制aio运行的coroutine
        try:
            self.loop_id = hash(asyncio.get_running_loop())
        except RuntimeError:
            self.loop_id = 0

    @override
    def post_configure(self) -> None:
        """
        对数据库做的配置工作放在这，可以做些减少运维压力的工作，或是需要项目加载完成后才能做的初始化工作。
        此项在服务器完全加载完毕后才会执行，在测试环境中，也是最后调用。
        """
        if self.is_servant:
            self.configure_servant()
        else:
            self.configure_master()

    def configure_master(self) -> None:
        if not self._ios:
            raise ConnectionError("连接已关闭，已调用过close")

        # 检测redis版本
        def parse_version(x):
            return tuple(map(int, x.split(".")))

        for i, io in enumerate(self._ios):
            info: dict = cast(dict, io.info("server"))  # 防止Awaitable类型检查报错
            redis_ver = parse_version(info["redis_version"])
            assert redis_ver >= (7, 0), "Redis/Valkey 版本过低，至少需要7.0版本"

        # 加载lua脚本，注意redis-py的pipeline里不能用lua，会反复检测script exists性能极低
        self.lua_commit = self.load_commit_scripts(
            Path(__file__).parent.resolve() / "commit.lua"
        )

    def configure_servant(self) -> None:
        if not self._ios:
            raise ConnectionError("连接已关闭，已调用过close")
            # 检查servants设置

        target_keyspace = "Kghz"
        for i, io in enumerate(self._ios):
            try:
                # 设置keyspace通知，先cast防止Awaitable类型检查报错
                notify_config = cast(dict, io.config_get("notify-keyspace-events"))
                db_keyspace = notify_config["notify-keyspace-events"]
                db_keyspace = db_keyspace.replace("A", "g$lshztxed")
                db_keyspace_new = db_keyspace
                for flag in list(target_keyspace):
                    if flag not in db_keyspace:
                        db_keyspace_new += flag
                if db_keyspace_new != db_keyspace:
                    io.config_set("notify-keyspace-events", db_keyspace_new)
            except (
                redis.exceptions.NoPermissionError,
                redis.exceptions.ResponseError,
            ):
                msg = (
                    f"⚠️ [💾Redis] 无权限调用数据库{self.urls[i]}的config_set命令，数据订阅将"
                    f"不起效。可手动设置配置文件：notify-keyspace-events={target_keyspace}"
                )
                logger.warning(msg)
            # 检查是否是replica模式
            db_replica = cast(dict, io.config_get("replica-read-only"))
            if db_replica.get("replica-read-only") != "yes":
                msg = (
                    "⚠️ [💾Redis] servant必须是Read Only Replica模式。"
                    f"{self.urls[i]} 未设置replica-read-only=yes"
                )
                logger.warning(msg)
                # 不检查replicaof master地址，因为replicaof的可能是其他replica地址
            # 考虑可以检查pubsub client buff设置，看看能否redis崩了提醒下
            # pubsub值建议为$剩余内存/预估在线数$

    @override
    async def is_synced(self) -> bool:
        if not self._ios:
            raise ConnectionError("连接已关闭，已调用过close")

        assert not self.is_servant, "is_synced只能在master上调用"

        info = await self.aio.info("replication")
        master_offset = int(info.get("master_repl_offset", 0))
        for key, value in info.items():
            # 兼容 Redis 新旧版本（slave/replica 字段）
            if key.startswith("slave") or key.startswith("replica"):
                lag_of_offset = master_offset - int(value.get("offset", 0))
                if lag_of_offset > 0:
                    return False
        return True

    @override
    def get_worker_keeper(self, sequence_id: int) -> RedisWorkerKeeper:
        """
        获取RedisWorkerKeeper实例，用于雪花ID的worker id管理。

        Parameters
        ----------
        sequence_id: int
            启动进程的顺序ID，从0开始。
        """
        assert not self.is_servant, "get_worker_keeper"
        from .worker_keeper import RedisWorkerKeeper

        return RedisWorkerKeeper(sequence_id, self.io, self.aio)

    @override
    async def close(self):
        if not self._ios:
            return

        for io in self._ios:
            io.close()
        self._ios = []

        for aio in self._async_ios:
            await aio.aclose()
        self._async_ios = []

    @staticmethod
    def _row_decode(
        comp_cls: type[BaseComponent], row: dict[bytes, bytes], fmt: RowFormat
    ) -> np.record | dict[str, Any]:
        """将redis获取的行byte数据解码为指定格式"""
        row_decoded = {
            k.decode("utf-8", "ignore"): v.decode("utf-8", "ignore")
            for k, v in row.items()
        }
        match fmt:
            case RowFormat.RAW:
                return row_decoded
            case RowFormat.STRUCT:
                return comp_cls.dict_to_row(row_decoded)
            case RowFormat.TYPED_DICT:
                struct_row = comp_cls.dict_to_row(row_decoded)
                return comp_cls.row_to_dict(struct_row)
            case _:
                raise ValueError(f"不可用的行格式: {fmt}")

    @overload
    async def get(
        self,
        table_ref: TableReference,
        row_id: int,
        row_format: Literal[RowFormat.STRUCT] = RowFormat.STRUCT,
    ) -> np.record | None: ...
    @overload
    async def get(
        self,
        table_ref: TableReference,
        row_id: int,
        row_format: Literal[RowFormat.RAW] = ...,
    ) -> dict[str, str] | None: ...
    @overload
    async def get(
        self,
        table_ref: TableReference,
        row_id: int,
        row_format: Literal[RowFormat.TYPED_DICT] = ...,
    ) -> dict[str, Any] | None: ...
    @overload
    async def get(
        self,
        table_ref: TableReference,
        row_id: int,
        row_format: RowFormat = ...,
    ) -> np.record | dict[str, str] | dict[str, Any] | None: ...
    @override
    async def get(
        self, table_ref: TableReference, row_id: int, row_format=RowFormat.STRUCT
    ) -> np.record | dict[str, Any] | None:
        """
        从数据库直接获取单行数据。

        Parameters
        ----------
        table_ref: TableReference
            表信息，指定Component、实例名、分片簇id。
        row_id: int
            row id主键
        row_format
            返回数据解码格式，见 "Returns"

        Returns
        -------
        row: np.record or dict[str, any] or None
            如果未查询到匹配数据，则返回 None。
            否则根据 `row_format` 参数返回以下格式之一：

            - RowFormat.STRUCT - **默认值**
                返回 np.record (c-struct) 的单行数据
            - RowFormat.RAW
                返回无类型的原始数据 (dict[str, str])
            - RowFormat.TYPED_DICT
                返回符合Component定义的，有格式的dict类型。
        """
        # todo 所有get query要合批
        key = self.row_key(table_ref, row_id)
        if row := await self.aio.hgetall(key):  # pyright: ignore[reportGeneralTypeIssues]
            return self._row_decode(table_ref.comp_cls, row, row_format)
        else:
            return None

    @staticmethod
    def _range_normalize(
        is_str_index: bool,
        left: int | float | str | bool,
        right: int | float | str | bool | None,
        desc: bool,
    ) -> tuple[int | float | str, int | float | str]:
        """规范化范围查询的左边界和右边界"""
        if right is None:
            right = left
        if desc:
            left, right = right, left

        # 对于str类型查询，要用[开始
        if is_str_index:
            left = str(left)
            right = str(right)
            # 判断type效率太低了，特别是isinstance，取消掉
            # assert (
            #     isinstance(left, (str, np.str_)) and isinstance(right, (str, np.str_))
            # ), f"字符串类型索引`{index_name}`的查询(left={left}, {type(left)})变量类型必须是str"
            if not left.startswith(("(", "[")):
                left = f"[{left}"
            if not right.startswith(("(", "[")):
                right = f"[{right}"

            if not left.endswith((":", ";")):
                left = f"{left}:"  # name:id 形式，所以:作为结尾标识符
            if not right.endswith((":", ";")):
                right = f"{right};"  # ';' = 3B, ':' = 3A

        return left, right

    @overload
    async def range(
        self,
        table_ref: TableReference,
        index_name: str,
        left: int | float | str | bool,
        right: int | float | str | bool | None = None,
        limit: int = 100,
        desc: bool = False,
        row_format: Literal[RowFormat.STRUCT] = RowFormat.STRUCT,
    ) -> np.recarray: ...
    @overload
    async def range(
        self,
        table_ref: TableReference,
        index_name: str,
        left: int | float | str | bool,
        right: int | float | str | bool | None = None,
        limit: int = 100,
        desc: bool = False,
        row_format: Literal[RowFormat.RAW] = ...,
    ) -> list[dict[str, str]]: ...
    @overload
    async def range(
        self,
        table_ref: TableReference,
        index_name: str,
        left: int | float | str | bool,
        right: int | float | str | bool | None = None,
        limit: int = 100,
        desc: bool = False,
        row_format: Literal[RowFormat.TYPED_DICT] = ...,
    ) -> list[dict[str, Any]]: ...
    @overload
    async def range(
        self,
        table_ref: TableReference,
        index_name: str,
        left: int | float | str | bool,
        right: int | float | str | bool | None = None,
        limit: int = 100,
        desc: bool = False,
        row_format: Literal[RowFormat.ID_LIST] = ...,
    ) -> list[int]: ...
    @overload
    async def range(
        self,
        table_ref: TableReference,
        index_name: str,
        left: int | float | str | bool,
        right: int | float | str | bool | None = None,
        limit: int = 100,
        desc: bool = False,
        row_format: RowFormat = ...,
    ) -> np.recarray | list[dict[str, str]] | list[dict[str, Any]] | list[int]: ...
    @override
    async def range(
        self,
        table_ref: TableReference,
        index_name: str,
        left: int | float | str | bool,
        right: int | float | str | bool | None = None,
        limit: int = 100,
        desc: bool = False,
        row_format=RowFormat.STRUCT,
    ) -> list[int] | list[dict[str, Any]] | np.recarray:
        """
        从数据库直接查询索引 `index_name`，返回在 [`left`, `right`] 闭区间内数据。
        如果 `right` 为 `None`，则查询等于 `left` 的数据，限制 `limit` 条。

        Parameters
        ----------
        table_ref: TableReference
            表信息，指定Component、实例名、分片簇id。
        index_name: str
            查询Component中的哪条索引
        left, right: str or number
            查询范围，闭区间。字符串查询时，可以在开头指定是[闭区间，还是(开区间。
            如果right不填写，则精确查询等于left的数据。
        limit: int
            限制返回的行数，越少越快
        desc: bool
            是否降序排列
        row_format
            返回数据解码格式，见 "Returns"

        Returns
        -------
        row: np.recarray or list[id] or list[dict]
            根据 `row_format` 参数返回以下格式之一：

            - RowFormat.STRUCT - **默认值**
                返回 `numpy.recarray`，如果没有查询到数据，返回空 `numpy.recarray`。
                `numpy.recarray` 是一种 c-struct array。
            - RowFormat.RAW
                返回无类型的原始数据 (dict[str, str]) 列表，如果没有查询到数据，返回空list
            - RowFormat.TYPED_DICT
                返回符合Component定义的，有格式的dict类型列表，如果没有查询到数据，返回空list
            - RowFormat.ID_LIST
                返回查询到的 row id 列表，如果没有查询到数据，返回空list

        Notes
        -----
        如何复合条件查询？
        请利用python的特性，先在数据库上筛选出最少量的数据，然后本地二次筛选::

            items = client.range(ref, "owner", player_id, limit=100)
            few_items = items[items.amount < 10]

        由于python numpy支持SIMD，比直接在数据库复合查询快。
        """
        # todo 所有get query要合批
        idx_key = self.index_key(table_ref, index_name)
        aio = self.aio  # 保存随机选中的aio连接

        # 生成zrange命令
        comp_cls = table_ref.comp_cls
        is_str_index = comp_cls.indexes_[index_name]
        left, right = self._range_normalize(
            is_str_index,
            left,
            right,
            desc,
        )

        # 对于str类型查询，要用bylex
        by_lex = True if is_str_index else False
        cmds = {
            "start": left,
            "end": right,
            "desc": desc,
            "offset": 0,
            "num": limit,
            "bylex": by_lex,
            "byscore": not by_lex,
        }

        row_ids = await aio.zrange(name=idx_key, **cmds)
        if is_str_index:
            row_ids = [
                int(vk.decode("utf-8", "ignore").split(":")[-1]) for vk in row_ids
            ]
        else:
            row_ids = list(map(int, row_ids))

        if row_format == RowFormat.ID_LIST:
            return row_ids

        key_prefix = self.cluster_prefix(table_ref) + ":id:"  # 存下前缀组合key快1倍
        rows = []
        for _id in row_ids:
            # todo 要么用合批的请求方法，要么用pipeline
            if row := await aio.hgetall(key_prefix + str(_id)):  # pyright: ignore[reportGeneralTypeIssues]
                rows.append(self._row_decode(comp_cls, row, row_format))

        if row_format == RowFormat.RAW or row_format == RowFormat.TYPED_DICT:
            return cast(list[dict[str, Any]], rows)
        else:
            if len(rows) == 0:
                return np.rec.array(np.empty(0, dtype=comp_cls.dtypes))
            else:
                record_list = cast(list[np.record], rows)
                return np.rec.array(np.stack(record_list, dtype=comp_cls.dtypes))

    @override
    async def commit(self, idmap: IdentityMap) -> None:
        """
        使用事务，向数据库提交IdentityMap中的所有数据修改

        Exceptions
        --------
        RaceCondition
            当提交数据时，发现数据已被其他事务修改，抛出此异常

        """
        assert not self.is_servant, "从节点不允许提交事务"

        dirty_rows = idmap.get_dirty_rows()
        assert len(dirty_rows) > 0, "没有脏数据需要提交"

        ref = idmap.first_reference()
        assert ref is not None, "不该走到这里，仅用于typing检查"

        # 转换dirty_rows为纯lua可用的信息格式：
        # payload={"insert": {"instance:TableName:{CLU1}": [row_dict, ...]}...}
        # todo 尝试组合成checks/sets命令表，减少lua脚本的复杂度
        #      checks有exists/unique/version
        #      sets有hmset/zadd/zrem/del
        payload = {
            commit_type: {
                self.cluster_prefix(ref): rows for ref, rows in commit_data.items()
            }
            for commit_type, commit_data in dirty_rows.items()
        }
        payload_json = msgpack.encode(payload)
        # 添加一个带cluster id的key，指明lua脚本执行的集群
        keys = [self.row_key(ref, 1)]

        # 这里不需要判断redis.exceptions.NoScriptError，因为里面会处理
        assert self.lua_commit is not None, "typing检查, 可忽略"
        resp = await self.lua_commit(keys, [payload_json])
        resp = resp.decode("utf-8")  # pyright: ignore[reportAttributeAccessIssue]

        if resp != "committed":
            if resp.startswith("RACE"):
                raise RaceCondition(resp)
            elif resp.startswith("UNIQUE"):
                # unique违反就是index的竞态原因
                raise RaceCondition(resp)
            else:
                raise RuntimeError(f"未知的提交错误：{resp}")

    def get_table_maintenance(self) -> RedisTableMaintenance:
        """
        获取表维护对象。
        """
        from .maint import RedisTableMaintenance

        return RedisTableMaintenance(self)
