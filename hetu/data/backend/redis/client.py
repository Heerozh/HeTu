"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import asyncio
import itertools
import logging
import random
import struct
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Never, cast, final, overload, override

# from msgspec import msgpack  # 不支持关闭bin type，lua 的msgpack库7年没更新了
import msgpack
import numpy as np
import redis
from redis.cluster import LoadBalancingStrategy

from ....i18n import _
from ..base import BackendClient, RaceCondition, RowFormat

# from .batch import RedisBatchedClient

if TYPE_CHECKING:
    import redis.asyncio
    import redis.asyncio.cluster
    import redis.cluster
    import redis.exceptions

    from ...component import BaseComponent
    from ..idmap import IdentityMap
    from ..table import TableReference
    from .maint import RedisTableMaintenance
    from .mq import RedisMQClient
    from .worker_keeper import RedisWorkerKeeper

logger = logging.getLogger("HeTu.root")
msg_packer = msgpack.Packer(use_bin_type=False)


@final
class RedisBackendClient(BackendClient, alias="redis"):
    """和Redis后端的操作的类，服务器启动时由server.py根据Config初始化"""

    @staticmethod
    def _get_referred_components() -> list[type[BaseComponent]]:
        """获取当前app用到的Component列表"""
        from ....system.definer import SystemClusters

        return [comp_cls for comp_cls in SystemClusters().get_components().keys()]

    def _schema_checking_for_redis(self):
        """检查Component的schema定义，确保符合Redis的要求"""
        for comp_cls in self._get_referred_components():
            for field, _is_str in comp_cls.indexes_.items():
                dtype = comp_cls.dtype_map_[field]
                # 索引不支持复数
                if np.issubdtype(dtype, np.complexfloating):
                    raise ValueError(
                        _(
                            "Component `{comp_name}` 的索引字段`{field}`"
                            "使用了复数，Redis后端不支持此类型作为索引字段"
                        ).format(comp_name=comp_cls.name_, field=field)
                    )
                # 其他类型不支持索引
                elif np.issubdtype(dtype, np.object_):
                    raise ValueError(
                        _(
                            "Component `{comp_name}` 的索引字段`{field}`"
                            "使用了不可用的类型 `{dtype}`，此类型不支持索引"
                        ).format(comp_name=comp_cls.name_, field=field, dtype=dtype)
                    )

    def load_commit_scripts(self, file: str | Path):
        assert self._async_ios, _("连接已关闭，已调用过close")
        assert self.is_servant is False, _(
            "Servant不允许加载Lua事务脚本，Lua事务脚本只能在Master上加载"
        )
        assert len(self._async_ios) == 1, _(
            "Lua事务脚本只能在Master上加载，但当前连接池中有多个服务器"
        )
        # read file to text
        with open(file, "r", encoding="utf-8") as f:
            script_text = f.read()

        # 上传脚本到服务器使用同步io
        self._ios[0].script_load(script_text)
        # 注册脚本到异步io，因为master只能有一个连接，直接[0]就行了
        return self._async_ios[0].register_script(script_text)  # type: ignore

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
        assert hash(asyncio.get_running_loop()) == self.loop_id, _(
            "Backend只能在同一个coroutine中使用。检测到调用此函数的协程发生了变化"
        )

        return random.choice(self._async_ios)

    @staticmethod
    def table_prefix(table_ref: TableReference) -> str:
        """获取redis表名前缀"""
        return f"{table_ref.instance_name}:{table_ref.comp_cls.name_}"

    @staticmethod
    def cluster_prefix(table_ref: TableReference) -> str:
        """获取redis表名前缀"""
        return (
            f"{table_ref.instance_name}:{table_ref.comp_cls.name_}:"
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

    @override
    def index_channel(self, table_ref: TableReference, index_name: str):
        """返回索引的频道名。如果索引有数据变动，会通知到该频道"""
        return f"__keyspace@{self.dbi}__:{self.index_key(table_ref, index_name)}"

    @override
    def row_channel(self, table_ref: TableReference, row_id: int):
        """返回行数据的频道名。如果行有变动，会通知到该频道"""
        return f"__keyspace@{self.dbi}__:{self.row_key(table_ref, row_id)}"

    async def reset_async_connection_pool(self):
        """重置异步连接池，用于协程切换后，解决aio不能跨协程传递的问题"""
        self.loop_id = 0
        for aio in self._async_ios:
            if isinstance(aio, redis.asyncio.cluster.RedisCluster):
                await aio.aclose()  # 未测试
            else:
                aio.connection_pool.reset()

    @staticmethod
    def to_sortable_bytes(value: np.generic) -> bytes:
        """将np类型的值转换为可排序的bytes，用于索引"""
        dtype = value.dtype
        if np.issubdtype(dtype, np.signedinteger):
            data = value.item() + (1 << 63)
            return struct.pack(">Q", data)
        elif np.issubdtype(dtype, np.unsignedinteger):
            return struct.pack(">Q", value)
        elif np.issubdtype(dtype, np.floating):
            double = value.item()
            packed = struct.pack(">d", value)
            [u64] = struct.unpack(">Q", packed)
            # IEEE 754 浮点数排序调整
            if double >= 0:
                # 正数让符号位变1
                u64 = u64 | (1 << 63)
            else:
                # 负数要全部取反，因为浮点负数是绝对值，变成int那种从0xFF递减
                u64 = ~u64 & 0xFFFFFFFFFFFFFFFF
            return struct.pack(">Q", u64)
        elif np.issubdtype(dtype, np.str_):
            encoded = value.item().encode("utf-8")
            return encoded
        elif np.issubdtype(dtype, np.bytes_):
            return value.item()
        elif np.issubdtype(dtype, np.bool_):
            return b"\x01" if value else b"\x00"
        assert False, _("不可排序的索引类型: {dtype}").format(dtype=dtype)

    # ============ 主要方法 ============

    def __init__(
        self, endpoint: str | list[str], is_servant, raw_clustering: bool = False
    ):
        super().__init__(endpoint, is_servant)
        self.raw_clustering = raw_clustering
        # redis的endpoint配置为url, 或list of url
        self.urls = [endpoint] if type(endpoint) is str else endpoint
        assert len(self.urls) > 0, _("必须至少指定一个数据库连接URL")

        # 创建连接
        self._ios: list[redis.Redis | redis.cluster.RedisCluster] = []
        self._async_ios: list[
            redis.asyncio.Redis | redis.asyncio.cluster.RedisCluster
        ] = []
        for url in self.urls:
            if self.raw_clustering:
                load_balancing_strategy = None  # 不从任何replica读取
                if is_servant:  # 只从replica读取
                    load_balancing_strategy = LoadBalancingStrategy.ROUND_ROBIN_REPLICAS
                io = redis.cluster.RedisCluster.from_url(
                    url, load_balancing_strategy=load_balancing_strategy
                )
                aio = redis.asyncio.cluster.RedisCluster.from_url(
                    url, load_balancing_strategy=load_balancing_strategy
                )
                self._ios.append(io)
                self._async_ios.append(aio)
            else:
                self._ios.append(redis.Redis.from_url(url))
                self._async_ios.append(redis.asyncio.Redis.from_url(url))

        # 取消，只在单机模式下有所增长
        # self._batched_aio = RedisBatchedClient(self._async_ios)

        # 测试连接是否正常
        for i, io in enumerate(self._ios):
            try:
                io.ping()
            except redis.exceptions.ConnectionError as e:
                raise ConnectionError(
                    _("无法连接到Redis数据库：{url}").format(url=self.urls[i])
                ) from e

        # 获得db index
        if self.raw_clustering:
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
            raise ConnectionError(_("连接已关闭，已调用过close"))

        # 检测redis版本
        def parse_version(x):
            return tuple(map(int, x.split(".")))

        for i, io in enumerate(self._ios):
            info: dict = cast(dict, io.info("server"))  # 防止Awaitable类型检查报错
            redis_ver = parse_version(info["redis_version"])
            assert redis_ver >= (7, 0), "Redis/Valkey 版本过低，至少需要7.0版本"

        # 加载lua脚本，注意redis-py的pipeline里不能用lua，会反复检测script exists性能极低
        self.lua_commit = self.load_commit_scripts(
            Path(__file__).parent.resolve() / "commit_v2.lua"
        )
        # 提示用户schema定义是否符合redis要求，比如索引类型不能有复数等
        self._schema_checking_for_redis()

    def configure_servant(self) -> None:
        if not self._ios:
            raise ConnectionError(_("连接已关闭，已调用过close"))
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
                msg = _(
                    "⚠️ [💾Redis] 无权限调用数据库{url}的config_set命令，数据订阅将"
                    "不起效。可手动设置配置文件：notify-keyspace-events={keyspace}"
                ).format(url=self.urls[i], keyspace=target_keyspace)
                logger.warning(msg)
            # 检查是否是replica模式(目前是把master也当servent的，这个检查不行，对只有master的配置会报错）
            # db_replica = cast(dict, io.config_get("replica-read-only"))
            # if db_replica.get("replica-read-only") != "yes":
            #     msg = (
            #         "⚠️ [💾Redis] servant必须是Read Only Replica模式。"
            #         f"{self.urls[i]} 未设置replica-read-only=yes"
            #     )
            #     logger.warning(msg)
            # 不检查replicaof master地址，因为replicaof的可能是其他replica地址
            # 考虑可以检查pubsub client buff设置，看看能否redis崩了提醒下
            # pubsub值建议为$剩余内存/预估在线数$

    @override
    async def is_synced(self, checkpoint: Any = None) -> tuple[bool, Any]:
        """
        在master库上查询待各个savants数据库同步状态，防止后续事务获取不到数据。
        主要用于关键节点，比如创建新用户连接。
        checkpoint指数据检查点，如写入日志的行数，检查该点之前的数据是否已同步完成。

        返回是否已完成同步，以及master最新checkpoint（可以用来下一次查询）。
        """
        if not self._ios:
            raise ConnectionError(_("连接已关闭，已调用过close"))

        assert not self.is_servant, _("is_synced只能在master上调用")

        info = await self.aio.info("replication")
        master_offset = int(info.get("master_repl_offset", 0))
        if checkpoint is None:
            checkpoint = master_offset
        for key, value in info.items():
            # 兼容 Redis 新旧版本（slave/replica 字段）
            if key.startswith("slave") or key.startswith("replica"):
                if type(value) is not dict:  # 可能是 replicas_waiting_psync:0
                    continue
                lag_of_offset = checkpoint - int(value.get("offset", 0))
                if lag_of_offset > 0:
                    return False, master_offset
        return True, master_offset

    @override
    async def close(self):
        if not self._ios:
            return

        # print("Batch量:次数 统计：")
        # print(sorted(self._batched_aio._log.items()))
        # ya_backend_upsert结果：
        # [(1, 844), (2, 781), (3, 796), (4, 720), (5, 616), (6, 468), (7, 291), (8, 185), (9, 1)]

        for io in self._ios:
            io.close()
        self._ios = []

        for aio in self._async_ios:
            await aio.aclose()
        self._async_ios = []

    @overload
    @staticmethod
    def row_decode_(
        comp_cls: type[BaseComponent],
        row: dict[bytes, bytes],
        fmt: Literal[RowFormat.STRUCT],
    ) -> np.record: ...
    @overload
    @staticmethod
    def row_decode_(
        comp_cls: type[BaseComponent],
        row: dict[bytes, bytes],
        fmt: Literal[RowFormat.RAW, RowFormat.TYPED_DICT],
    ) -> dict[str, Any]: ...
    @overload
    @staticmethod
    def row_decode_(
        comp_cls: type[BaseComponent],
        row: dict[bytes, bytes],
        fmt: Literal[RowFormat.ID_LIST],
    ) -> Never: ...
    @staticmethod
    def row_decode_(
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
                return comp_cls.dict_to_struct(row_decoded)
            case RowFormat.TYPED_DICT:
                struct_row = comp_cls.dict_to_struct(row_decoded)
                return comp_cls.struct_to_dict(struct_row)
            case _:
                raise ValueError(_("不可用的行格式: {fmt}").format(fmt=fmt))

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
                此方法性能低于 `RowFormat.STRUCT` ，主要用于json后传递给客户端。
        """
        if not self._ios:
            raise ConnectionError(_("连接已关闭，已调用过close"))
        key = self.row_key(table_ref, row_id)
        aio = self.aio  # self._batched_aio
        if row := await aio.hgetall(key):  # type: ignore
            return self.row_decode_(table_ref.comp_cls, row, row_format)
        else:
            return None

    @classmethod
    def range_normalize_(
        cls,
        dtype: np.dtype,
        left: int | float | str | bytes | bool,
        right: int | float | str | bytes | bool | None,
        desc: bool,
    ) -> tuple[bytes, bytes]:
        """规范化范围查询的左边界和右边界"""
        # 处理right none, 顺序问题
        if right is None:
            right = left
        if desc:
            left, right = right, left

        if issubclass(dtype.type, np.character):
            # component字段如果是str/bytes类型的索引，不能查询数字
            assert type(left) in (str, bytes) and type(right) in (str, bytes), (
                f"字符串类型的查询变量类型必须是str/bytes，你的：left={type(left)}({left}), "
                f"right={type(right)}({right})"
            )
        else:
            # component字段如果是int数字，则处理inf
            # 浮点不用处理，因为浮点的inf是高位的Exponent全FF，大于2**1023最大值，自然永远最大
            if issubclass(dtype.type, np.integer):
                type_info = np.iinfo(dtype)

                def clamp_inf(x):
                    if type(x) is float and np.isinf(x):
                        return type_info.max if x > 0 else type_info.min
                    return x

                left = clamp_inf(left)
                right = clamp_inf(right)

        # 处理范围区间
        def peel(x, _inclusive):
            if type(x) in (str, bytes) and len(x) >= 1:
                ch = x[0:1]  # bytes必须用范围切片
                if ch in ("(", "[") or ch in (b"(", b"["):
                    _inclusive = ch == "[" or ch == b"["
                    x = x[1:]

            return x, _inclusive

        left, li = peel(left, True)
        right, ri = peel(right, True)
        # 因为member是value:id，所以left="value;"为排除，right="value:"为排除
        ls = b":" if li else b";"
        rs = b";" if ri else b":"
        if desc:
            ls, rs = rs, ls

        # 二进制化。
        b_left = b"[" + cls.to_sortable_bytes(dtype.type(left)) + ls
        b_right = b"[" + cls.to_sortable_bytes(dtype.type(right)) + rs
        return b_left, b_right

    @staticmethod
    def make_zrange_cmd_(b_left, b_right, desc, limit):
        return {
            "start": b_left,
            "end": b_right,
            "desc": desc,
            "offset": 0,
            "num": limit,
            "bylex": True,
            "byscore": False,
        }

    @overload
    async def range(
        self,
        table_ref: TableReference,
        index_name: str,
        left: int | float | str | bytes | bool,
        right: int | float | str | bytes | bool | None = None,
        limit: int = 100,
        desc: bool = False,
        row_format: Literal[RowFormat.STRUCT] = RowFormat.STRUCT,
    ) -> np.recarray: ...
    @overload
    async def range(
        self,
        table_ref: TableReference,
        index_name: str,
        left: int | float | str | bytes | bool,
        right: int | float | str | bytes | bool | None = None,
        limit: int = 100,
        desc: bool = False,
        row_format: Literal[RowFormat.RAW] = ...,
    ) -> list[dict[str, str]]: ...
    @overload
    async def range(
        self,
        table_ref: TableReference,
        index_name: str,
        left: int | float | str | bytes | bool,
        right: int | float | str | bytes | bool | None = None,
        limit: int = 100,
        desc: bool = False,
        row_format: Literal[RowFormat.TYPED_DICT] = ...,
    ) -> list[dict[str, Any]]: ...
    @overload
    async def range(
        self,
        table_ref: TableReference,
        index_name: str,
        left: int | float | str | bytes | bool,
        right: int | float | str | bytes | bool | None = None,
        limit: int = 100,
        desc: bool = False,
        row_format: Literal[RowFormat.ID_LIST] = ...,
    ) -> list[int]: ...
    @overload
    async def range(
        self,
        table_ref: TableReference,
        index_name: str,
        left: int | float | str | bytes | bool,
        right: int | float | str | bytes | bool | None = None,
        limit: int = 100,
        desc: bool = False,
        row_format: RowFormat = ...,
    ) -> np.recarray | list[dict[str, str]] | list[dict[str, Any]] | list[int]: ...
    @override
    async def range(
        self,
        table_ref: TableReference,
        index_name: str,
        left: int | float | str | bytes | bool,
        right: int | float | str | bytes | bool | None = None,
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
            查询范围，闭区间。可以在开头加上"["指定闭区间，还是"("开区间。
            如果right不填写，则精确查询等于left的数据。
        limit: int
            限制返回的行数，本方法至少请求数据库 `1 + limit` 次。
            负数表示不限制行数。
        desc: bool
            是否降序排列
        row_format
            返回数据解码格式，见 "Returns"

        Returns
        -------
        row: np.recarray or list[int] or list[dict]
            根据 `row_format` 参数返回以下格式之一：

            - RowFormat.STRUCT - **默认值**
                返回 `numpy.recarray`，如果没有查询到数据，返回空 `numpy.recarray`。
                `numpy.recarray` 是一种 c-struct array。
            - RowFormat.RAW
                返回无类型的原始数据 (dict[str, str]) 的列表，如果没有查询到数据，返回空list
            - RowFormat.TYPED_DICT
                返回符合Component定义的，有格式的dict类型列表，如果没有查询到数据，返回空list
                此方法性能低于 `RowFormat.STRUCT` ，主要用于json后传递给客户端。
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
        if not self._ios:
            raise ConnectionError(_("连接已关闭，已调用过close"))

        idx_key = self.index_key(table_ref, index_name)
        aio = self.aio  # self._batched_aio

        # 生成zrange命令
        comp_cls = table_ref.comp_cls
        assert index_name in comp_cls.indexes_, (
            f"Component `{comp_cls.name_}` 没有索引 `{index_name}`"
        )
        b_left, b_right = self.range_normalize_(
            comp_cls.dtype_map_[index_name], left, right, desc
        )
        assert (b_left >= b_right) if desc else (b_right >= b_left), (
            f"left必须大于等于right，你的:right={right}, left={left}"
        )

        row_ids = await aio.zrange(
            name=idx_key, **self.make_zrange_cmd_(b_left, b_right, desc, limit)
        )
        row_ids = [int(vk.rsplit(b":", 1)[-1]) for vk in row_ids]

        if row_format == RowFormat.ID_LIST:
            return row_ids

        key_prefix = self.cluster_prefix(table_ref) + ":id:"  # 存下前缀组合key快1倍
        rows = []
        for _id in row_ids:
            if row := await aio.hgetall(key_prefix + str(_id)):  # type: ignore
                rows.append(self.row_decode_(comp_cls, row, row_format))

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

        def _key_must_not_exist(_key: str):
            """添加key must not exist的检查"""
            checks.append(["NX", _key])

        def _version_must_match(_key: str, _old_version):
            """添加version match的检查"""
            checks.append(["VER", _key, _old_version])

        def _unique_meet(_unique_fields, _dtype_map, _idx_prefix, _row: dict[str, str]):
            """添加unique索引检查"""
            for _field, _value in _row.items():
                if _field in _unique_fields:
                    _idx_key = _idx_prefix + _field
                    _sortable_value = self.to_sortable_bytes(
                        _dtype_map[_field].type(_value)
                    )
                    _start_val = b"[" + _sortable_value + b":"
                    _end_val = b"[" + _sortable_value + b";"
                    checks.append(["UNIQ", _idx_key, _start_val, _end_val])

        def _hset_key(_key, _old_version, _update: dict[str, str]):
            """添加hset的push命令"""
            # 版本+1
            _ver = int(_old_version) + 1
            _update.pop("_version", None)  # 无视用户传入的_version字段
            # 组合hset, 别忘记写_version
            _kvs = itertools.chain.from_iterable(_update.items())
            pushes.append(["HSET", _key, "_version", str(_ver), *_kvs])

        def _exc_index(_indexes, _dtype_map, _idx_prefix, _old, _new, _add):
            """exchange index(zadd/zrem)的push命令"""
            _b_row_id = _old["id"].encode("ascii")
            _values = _new if _add else _old
            for _field in _new.keys():
                if _field in _indexes:
                    _idx_key = _idx_prefix + _field
                    # 索引全部转换为bytes索引，测试下来lex和score排序性能是一样的
                    _sortable_value = self.to_sortable_bytes(
                        _dtype_map[_field].type(_values[_field])
                    )
                    _member = _sortable_value + b":" + _b_row_id
                    if _add:
                        # score统一用0，因为我们不需要score排序功能
                        pushes.append(["ZADD", _idx_key, "0", _member])
                    else:
                        pushes.append(["ZREM", _idx_key, _member])

        def _del_key(_key):
            """添加del的push命令"""
            pushes.append(["DEL", _key])

        assert not self.is_servant, _("从节点不允许提交事务")

        dirties = idmap.get_dirty_rows()
        if not dirties:
            raise ValueError(_("没有脏数据需要提交"))

        first_ref = idmap.first_reference()
        assert first_ref is not None, "typing检查"

        # 组合成checks/pushes命令表，减少lua脚本的复杂度
        # checks有exists/unique/version
        # pushes有hset/zadd/zrem/del
        checks: list[list[str | bytes]] = []
        pushes: list[list[str | bytes]] = []
        deleted: dict[str, bool] = {}

        for ref, (inserts, (old_rows, new_rows), deletes) in dirties.items():
            id_prefix = self.cluster_prefix(ref) + ":id:"
            idx_prefix = self.cluster_prefix(ref) + ":index:"
            comp_cls = ref.comp_cls
            unique_fields = comp_cls.uniques_
            indexes = comp_cls.indexes_
            dtype_map = comp_cls.dtype_map_
            # insert
            for insert in inserts:
                row_id = insert["id"]
                key = id_prefix + row_id
                _key_must_not_exist(key)
                _unique_meet(unique_fields, dtype_map, idx_prefix, insert)
                _hset_key(key, 0, insert)
                _exc_index(indexes, dtype_map, idx_prefix, insert, insert, _add=True)
            # update
            for old_row, new_row in zip(old_rows, new_rows):
                row_id = old_row["id"]
                key = id_prefix + row_id
                old_version = old_row["_version"]
                _version_must_match(key, old_version)
                _unique_meet(unique_fields, dtype_map, idx_prefix, new_row)
                _hset_key(key, old_version, new_row)
                _exc_index(indexes, dtype_map, idx_prefix, old_row, new_row, _add=False)
                _exc_index(indexes, dtype_map, idx_prefix, old_row, new_row, _add=True)
            # delete
            for delete in deletes:
                # 传入deleted ids，如果之后的unique冲突查到的id在deleted里，就返回false
                deleted[str(delete["id"])] = True
                key = id_prefix + str(delete["id"])
                old_version = delete["_version"]
                _version_must_match(key, old_version)
                _exc_index(indexes, dtype_map, idx_prefix, delete, delete, _add=False)
                _del_key(key)

        payload_json: bytes = msg_packer.pack([checks, pushes, deleted])  # type: ignore
        # 添加一个带cluster id的key，指明lua脚本执行的集群
        keys = [self.row_key(first_ref, 1)]

        # 这里不需要判断redis.exceptions.NoScriptError，因为里面会处理
        assert self.lua_commit is not None, _(
            "lua_commit脚本没有初始化，请先调用 post_configure"
        )
        resp = await self.lua_commit(keys, [payload_json])
        resp = resp.decode("utf-8")  # type: ignore

        if resp != "committed":
            # 把事务相关的key满门抄斩
            # self._batched_aio.invalidate_cache(idmap.get_clean_row_keys())
            if resp.startswith("RACE"):
                raise RaceCondition(resp)
            elif resp.startswith("UNIQUE"):
                # unique违反就是index的竞态原因
                raise RaceCondition(resp)
            else:
                raise RuntimeError(_("未知的提交错误：{resp}").format(resp=resp))

    async def direct_set(
        self, table_ref: TableReference, id_: int, **kwargs: str
    ) -> None:
        """
        UNSAFE! 只用于易失数据! 不会做类型检查!

        直接写入属性到数据库，避免session必须要执行get+事务2条指令。
        仅支持非索引字段，索引字段更新是非原子性的，必须使用事务。
        注意此方法可能导致写入数据到已删除的行，请确保逻辑。

        一些系统级别的临时数据，使用直接写入的方式效率会更高，但不保证数据一致性。
        """
        assert "id" not in kwargs, "id不允许修改"
        assert table_ref.comp_cls.volatile_, "direct_set只能用于易失数据的Component"

        aio = self.aio
        key = self.row_key(table_ref, id_)

        for prop in kwargs:
            if prop in table_ref.comp_cls.indexes_:
                raise ValueError(
                    _("索引字段`{prop}`不允许用direct_set修改").format(prop=prop)
                )
            if prop not in table_ref.comp_cls.prop_idx_map_:
                raise ValueError(
                    _("Component `{comp_name}` 没有字段`{prop}`").format(
                        comp_name=table_ref.comp_name, prop=prop
                    )
                )
        await aio.hset(key, mapping=kwargs)  # type: ignore

    def get_table_maintenance(self) -> RedisTableMaintenance:
        """
        获取表维护对象。
        """
        if not self._ios:
            raise ConnectionError(_("连接已关闭，已调用过close"))

        from .maint import RedisTableMaintenance

        return RedisTableMaintenance(self)

    def get_mq_client(self) -> RedisMQClient:
        """获取消息队列连接"""
        if not self._ios:
            raise ConnectionError(_("连接已关闭，已调用过close"))
        from .mq import RedisMQClient

        return RedisMQClient(self)
