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
from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Never, cast, final, overload, override

# from msgspec import msgpack  # 不支持关闭bin type，lua 的msgpack库7年没更新了
import msgpack
import numpy as np
import redis
from redis.cluster import LoadBalancingStrategy

from ....i18n import _
from ..base import (
    BackendClient,
    InconsistentRangeRead,
    RaceCondition,
    RowFormat,
    UniqueViolation,
    detach_rows_,
    exact_number_,
    inverted_bounds_error_,
    normalize_int_bounds_,
    peel_bound_,
    sortable_token,
    to_sortable_bytes,
)
from ..idmap import RangeObservation
from .pool import HeTuConnectionPool

if TYPE_CHECKING:
    import redis.asyncio
    import redis.asyncio.cluster
    import redis.cluster
    import redis.exceptions

    from ...component import BaseComponent
    from ..idmap import IdentityMap
    from ..table import TableReference
    from .maint import RedisTableMaintenance
    from .mq import PubSubHub, RedisMQClient

logger = logging.getLogger("HeTu.root")
msg_packer = msgpack.Packer(use_bin_type=False)


@final
class RedisBackendClient(BackendClient, alias="redis"):
    """和Redis后端的操作的类，服务器启动时由server.py根据Config初始化"""

    # range/get_many 批量读行时，每个pipeline最多打包的HGETALL条数
    RANGE_PIPELINE_CHUNK = 1000

    @staticmethod
    def _get_referred_components() -> list[type[BaseComponent]]:
        """获取当前app用到的Component列表"""
        from ....system.definer import SystemClusters

        return [comp_cls for comp_cls in SystemClusters().get_components().keys()]

    def _schema_checking_for_redis(
        self, components: Iterable[type[BaseComponent]] | None = None
    ):
        """检查Component的schema定义，确保符合Redis的要求"""
        if components is None:
            components = self._get_referred_components()
        for comp_cls in components:
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
        """返回整个索引的频道名（keyspace 通知）。该索引 zset 任何 ZADD/ZREM 都会通知到该频道"""
        return f"__keyspace@{self.dbi}__:{self.index_key(table_ref, index_name)}"

    @classmethod
    def value_channel_(cls, idx_key: str, sortable: bytes) -> str:
        """`index_value_channel` 的内部形式：commit 里已经算好 sortable bytes 时直接拼，不重复编码"""
        return f"{idx_key}:{sortable_token(sortable)}"

    @override
    def index_value_channel(
        self, table_ref: TableReference, index_name: str, value: Any
    ) -> str:
        """
        返回索引某一个值的频道名（只有声明了 point_sub 的索引才有，否则抛 ValueError）。
        这是 commit lua 脚本主动 PUBLISH 的普通频道（非 keyspace 通知）；名字带 {CLU}
        hash tag，cluster 模式下按 slot 路由。

        Channel of one index value (only for indexes declared with `point_sub`, raises
        `ValueError` otherwise). A plain channel PUBLISHed by the commit Lua script, not
        a keyspace notification; the name carries the {CLU} hash tag, so cluster mode
        routes it by slot.
        """
        self.require_point_sub_(table_ref, index_name)
        dtype = table_ref.comp_cls.dtype_map_[index_name]
        return self.value_channel_(
            self.index_key(table_ref, index_name), to_sortable_bytes(dtype.type(value))
        )

    @override
    def row_channel(self, table_ref: TableReference, row_id: int):
        """返回行数据的频道名。如果行有变动，会通知到该频道"""
        return f"__keyspace@{self.dbi}__:{self.row_key(table_ref, row_id)}"

    @override
    def table_channel(self, table_ref: TableReference):
        """
        返回表级变更频道名。这是commit lua脚本主动PUBLISH的普通频道（非keyspace通知），
        只给声明了 table_sub 的组件发；名字带{CLU}hash tag，cluster模式下
        AsyncKeyspacePubSub按slot路由订阅。

        Channel of table-level changes: a plain channel PUBLISHed by the commit Lua
        script (not a keyspace notification), only for components declared with
        `table_sub`. The name carries the {CLU} hash tag, so in cluster mode
        AsyncKeyspacePubSub routes the subscription by slot.
        """
        return f"{self.cluster_prefix(table_ref)}{self.TABLE_CHANNEL_SUFFIX}"

    async def reset_async_connection_pool(self):
        """重置异步连接池，用于协程切换后，解决aio不能跨协程传递的问题"""
        self.loop_id = 0
        await self._close_hub()
        for aio in self._async_ios:
            if isinstance(aio, redis.asyncio.cluster.RedisCluster):
                await aio.aclose()  # 未测试
            else:
                aio.connection_pool.reset()

    # 索引 member 的值编码搬到了 base.py（两个后端共用来给索引值频道命名），这里保留同名别名
    to_sortable_bytes = staticmethod(to_sortable_bytes)

    # ============ 主要方法 ============

    def __init__(
        self,
        endpoint: str | list[str],
        is_servant,
        raw_clustering: bool = False,
        max_connections: int = 64,
        pool_timeout: float | None = 5.0,
    ):
        """
        Parameters
        ----------
        endpoint
            redis url 或 url 列表，见 CONFIG_TEMPLATE.yml 的 BACKENDS 段。
        is_servant
            是否为只读副本连接。
        raw_clustering
            是否为 Redis 原生集群模式。
        max_connections
            本进程对该地址的异步连接池上限。订阅的 pubsub 走每 worker 一条的独立连接
            （见 PubSubHub），池里只有短命的读写命令，所以不需要很大。redis-py 8 起默认
            只有 100，这里显式给出。standalone 模式下池满会排队（见 HeTuConnectionPool，
            没满时取/还连接不付排队的代价）；原生集群模式下 redis-py 每个节点的池只能设
            上限、满了直接抛 MaxConnectionsError，需要时请调大。
        pool_timeout
            standalone 模式下池满时排队等待的秒数，None 为一直等，超时抛 ConnectionError。
        """
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
                    url,
                    load_balancing_strategy=load_balancing_strategy,
                    max_connections=max_connections,
                )
                self._ios.append(io)
                self._async_ios.append(aio)
            else:
                self._ios.append(redis.Redis.from_url(url))
                # 池满排队而不是抛 MaxConnectionsError；from_pool 让 client 接管池的关闭
                pool = HeTuConnectionPool.from_url(
                    url, max_connections=max_connections, timeout=pool_timeout
                )
                self._async_ios.append(redis.asyncio.Redis.from_pool(pool))

        # 测试连接是否正常
        for i, io in enumerate(self._ios):
            try:
                io.ping()
            except redis.exceptions.ConnectionError as e:
                raise ConnectionError(
                    _("无法连接到Redis数据库：{url}").format(url=self.urls[i])
                ) from e
            except redis.exceptions.ResponseError as e:
                # redis-py 8 默认 RESP3，每条连接建立后先发 HELLO 握手，所以 PING 还没
                # 发出就可能在这里失败。部分代理层（如云厂商的 Redis 代理版）不实现
                # HELLO，回 unknown command，报错里看不出是哪个后端，这里补上提示。
                if "unknown command" in str(e).lower():
                    raise ConnectionError(
                        _(
                            "Redis数据库 {url} 不支持 HELLO 命令（RESP3 握手）。通常是前面"
                            "挂了不支持 RESP3 的代理层，请在 url 后追加 ?protocol=2 降级到 "
                            "RESP2，master 和 servants 都要加。"
                        ).format(url=self.urls[i])
                    ) from e
                raise ConnectionError(
                    _("连接Redis数据库 {url} 时服务端返回错误：{error}").format(
                        url=self.urls[i], error=e
                    )
                ) from e

        # 获得db index
        if self.raw_clustering:
            self.dbi = 0  # 集群模式没有db的概念，默认0
        else:
            io = self._ios[0]
            assert isinstance(io, redis.Redis)  # for type checking
            self.dbi = io.connection_pool.connection_kwargs["db"]

        self.lua_commit = None
        # 本进程共享的 pubsub 分发器，首次 get_mq_client 时在事件循环里懒建
        self._hub: PubSubHub | None = None

        # 限制aio运行的coroutine
        try:
            self.loop_id = hash(asyncio.get_running_loop())
        except RuntimeError:
            self.loop_id = 0

    @override
    def post_configure(
        self, components: Iterable[type[BaseComponent]] | None = None
    ) -> None:
        """
        对数据库做的配置工作放在这，可以做些减少运维压力的工作，或是需要项目加载完成后才能做的初始化工作。
        此项在服务器完全加载完毕后才会执行，在测试环境中，也是最后调用。
        """
        if self.is_servant:
            self.configure_servant()
        else:
            self.configure_master(components)

    def configure_master(
        self, components: Iterable[type[BaseComponent]] | None = None
    ) -> None:
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
        self._schema_checking_for_redis(components)

    def configure_servant(self) -> None:
        if not self._ios:
            raise ConnectionError(_("连接已关闭，已调用过close"))
            # 检查servants设置

        target_keyspace = "Kghz"
        for i, io in enumerate(self._ios):
            # keyspace 通知只在 key 所在的节点本地产生、只推给连在该节点上的订阅者。集群客户端
            # 的 CONFIG GET/SET 只会发到默认节点，所以要逐个节点设置（主从都要，pubsub 可能
            # 订在从节点上），否则落在别的分片上的行 / 索引永远收不到通知
            if isinstance(io, redis.cluster.RedisCluster):
                node_ios = [
                    (f"{self.urls[i]} ({node.name})", io.get_redis_connection(node))
                    for node in io.get_nodes()
                ]
            else:
                node_ios = [(self.urls[i], io)]
            for url, node_io in node_ios:
                try:
                    # 设置keyspace通知，先cast防止Awaitable类型检查报错
                    notify_config = cast(
                        dict, node_io.config_get("notify-keyspace-events")
                    )
                    db_keyspace = notify_config["notify-keyspace-events"]
                    db_keyspace = db_keyspace.replace("A", "g$lshztxed")
                    db_keyspace_new = db_keyspace
                    for flag in list(target_keyspace):
                        if flag not in db_keyspace:
                            db_keyspace_new += flag
                    if db_keyspace_new != db_keyspace:
                        node_io.config_set("notify-keyspace-events", db_keyspace_new)
                except (
                    redis.exceptions.NoPermissionError,
                    redis.exceptions.ResponseError,
                ):
                    msg = _(
                        "⚠️ [💾Redis] 无权限调用数据库{url}的config_set命令，数据订阅将"
                        "不起效。可手动设置配置文件：notify-keyspace-events={keyspace}"
                    ).format(url=url, keyspace=target_keyspace)
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

        for io in self._ios:
            io.close()
        self._ios = []

        await self._close_hub()
        for aio in self._async_ios:
            await aio.aclose()
        self._async_ios = []

    async def _close_hub(self):
        if self._hub is not None:
            hub, self._hub = self._hub, None
            await hub.close()

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
        match fmt:
            case RowFormat.STRUCT:
                return RedisBackendClient.rows_decode_(comp_cls, (row,))[0]
            case RowFormat.TYPED_DICT:
                struct_row = RedisBackendClient.rows_decode_(comp_cls, (row,))[0]
                return comp_cls.struct_to_dict(struct_row)
            case RowFormat.RAW:
                # RAW 一律是 str，bytes 字段也按 utf-8 容错解码
                return {
                    k.decode("utf-8", "ignore"): v.decode("utf-8", "ignore")
                    for k, v in row.items()
                }
            case _:
                raise ValueError(_("不可用的行格式: {fmt}").format(fmt=fmt))

    @staticmethod
    def rows_decode_(
        comp_cls: type[BaseComponent], rows: Iterable[dict[bytes, bytes]]
    ) -> np.recarray:
        """
        把 HGETALL 读回的多行一次解码成 recarray，顺序与传入一致。`row_decode_` 的 STRUCT
        格式就是它的单行特例，解码规则只写在这里。
        """
        # bytes 字段用原始字节：utf-8 解码会丢掉不合法的字节，非 ASCII 的 str 也存不进
        # S 类型。其余字段按 utf-8 容错解码成 str，交给 numpy 按 dtype 转换
        fields = [
            (name.encode(), name in comp_cls.bytes_fields_)
            for name, _prop in comp_cls.properties_
        ]
        values = [
            tuple(
                [
                    row[key] if raw else row[key].decode("utf-8", "ignore")
                    for key, raw in fields
                ]
            )
            for row in rows
        ]
        return np.array(values, dtype=comp_cls.dtypes).view(np.recarray)

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
        aio = self.aio
        if row := await aio.hgetall(key):  # type: ignore
            return self.row_decode_(table_ref.comp_cls, row, row_format)
        else:
            return None

    async def _hgetall_many(
        self, key_prefix: str, row_ids: Iterable[int | str]
    ) -> list[dict]:
        """
        按块pipeline批量HGETALL，返回与row_ids顺序一致的raw dict列表，不存在的为空dict。

        注意这不是跨请求的自动合批（已否决）：只是把**同一个逻辑操作**内部
        的N次读取合并成 ceil(N/CHUNK) 次往返，不会让不相关的请求互相等待。
        同一张表的所有行key都带同一个 {CLU} hash tag，cluster模式下同slot，pipeline可直接用。
        """
        aio = self.aio
        if not isinstance(row_ids, (list, tuple)):
            row_ids = list(row_ids)
        if len(row_ids) == 1:
            # 单行直接 HGETALL：redis-py 8 的 pipeline 有固定开销（建对象、HIMPORT 预处理、
            # asyncio.shield 还连接），比单条命令贵一截，而 upsert、get(unique=) 每次都走这里
            return [await aio.hgetall(key_prefix + str(row_ids[0]))]
        rows: list[dict] = []
        for chunk in itertools.batched(row_ids, self.RANGE_PIPELINE_CHUNK):
            async with aio.pipeline(transaction=False) as pipe:
                for _id in chunk:
                    pipe.hgetall(key_prefix + str(_id))
                rows.extend(await pipe.execute())
        return rows

    @override
    async def get_many(
        self,
        table_ref: TableReference,
        row_ids: Iterable[int],
        row_format: RowFormat = RowFormat.STRUCT,
    ) -> list[np.record | dict[str, str] | dict[str, Any] | None]:
        if not self._ios:
            raise ConnectionError(_("连接已关闭，已调用过close"))
        assert row_format != RowFormat.ID_LIST, "get_many不支持ID_LIST格式"
        key_prefix = self.cluster_prefix(table_ref) + ":id:"
        comp_cls = table_ref.comp_cls
        raw_rows = await self._hgetall_many(key_prefix, row_ids)
        if row_format is RowFormat.STRUCT:
            batch = self.rows_decode_(comp_cls, [row for row in raw_rows if row])
            records = detach_rows_(batch)
            return [next(records) if row else None for row in raw_rows]
        return [
            self.row_decode_(comp_cls, row, row_format) if row else None
            for row in raw_rows
        ]

    @override
    async def get_many_array_(
        self, table_ref: TableReference, row_ids: list[int]
    ) -> tuple[np.recarray, list[int]]:
        if not self._ios:
            raise ConnectionError(_("连接已关闭，已调用过close"))
        key_prefix = self.cluster_prefix(table_ref) + ":id:"
        raw_rows = await self._hgetall_many(key_prefix, row_ids)
        rows = self.rows_decode_(table_ref.comp_cls, [row for row in raw_rows if row])
        return rows, [row_id for row_id, row in zip(row_ids, raw_rows) if not row]

    @classmethod
    def range_normalize_(
        cls,
        dtype: np.dtype,
        left: int | float | str | bytes | bool,
        right: int | float | str | bytes | bool | None,
        desc: bool,
    ) -> tuple[bytes, bytes]:
        """
        规范化范围查询的边界，返回 ZRANGE BYLEX 的两端，按扫描顺序排（desc 时上界在前）。

        left / right 是 (下界, 上界)，desc 时也一样；下界大于上界多半是传反了，报 ValueError。
        整数索引按区间的数学含义收成闭区间（见 `normalize_int_bounds_`），越界、小数照常查。
        区间是空的（两端开区间同值、整数列里没有整数……）时，返回的两端交叉或相等：ZRANGE /
        ZLEXCOUNT 对这样的两端自然得到空 / 0，`_zrange_members` 也就不去查了。
        """
        if right is None:
            right = left

        # component字段如果是str/bytes类型的索引，不能查询数字
        if issubclass(dtype.type, np.character) and (
            type(left) not in (str, bytes) or type(right) not in (str, bytes)
        ):
            raise ValueError(
                f"字符串类型的查询变量类型必须是str/bytes，你的：left={type(left)}({left}), "
                f"right={type(right)}({right})"
            )

        # 边界值开头的 "(" / "[" 指定开/闭，默认闭区间
        lower, li = peel_bound_(left)
        upper, ui = peel_bound_(right)
        li = True if li is None else li
        ui = True if ui is None else ui

        if issubclass(dtype.type, np.integer):
            # 不按 dtype 转换（会溢出、会截断小数）：先用精确的数判定传反，再收成范围内的闭区间
            lower_num, upper_num = exact_number_(lower), exact_number_(upper)
            if lower_num > upper_num:
                raise inverted_bounds_error_(lower, upper)
            bounds = normalize_int_bounds_(dtype, lower_num, li, upper_num, ui)
            if bounds is None:
                # 区间里没有整数：给交叉的两端 [最大值, 最小值]
                info = np.iinfo(dtype)
                bounds = (info.max, info.min)
            lower_value = to_sortable_bytes(dtype.type(bounds[0]))
            upper_value = to_sortable_bytes(dtype.type(bounds[1]))
            li = ui = True
        else:
            lower_value = to_sortable_bytes(dtype.type(lower))
            upper_value = to_sortable_bytes(dtype.type(upper))
            # 按值判定传反（编码是保序的）。同值时开区间让两端交叉，那是空区间，不是传反
            if upper_value < lower_value:
                raise inverted_bounds_error_(lower, upper)

        # member 是 value\x00id（value 段已对 0x00 转义，见 to_sortable_bytes）。
        # 终止符 b"\x00" = 该 value 的下边界(含最小 id)，b"\x00\xff" = 上边界(含所有 id)
        b_lower = b"[" + lower_value + (b"\x00" if li else b"\x00\xff")
        b_upper = b"[" + upper_value + (b"\x00\xff" if ui else b"\x00")
        return (b_upper, b_lower) if desc else (b_lower, b_upper)

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
        members, _b_left, _b_right = await self._zrange_members(
            table_ref, index_name, left, right, limit, desc
        )
        row_ids = [int(vk.rsplit(b"\x00", 1)[-1]) for vk in members]

        if row_format == RowFormat.ID_LIST:
            return row_ids

        comp_cls = table_ref.comp_cls
        key_prefix = self.cluster_prefix(table_ref) + ":id:"  # 存下前缀组合key快1倍
        # pipeline批量读行，N行只需 ceil(N/RANGE_PIPELINE_CHUNK) 次往返
        rows = [
            self.row_decode_(comp_cls, row, row_format)
            for row in await self._hgetall_many(key_prefix, row_ids)
            if row
        ]

        if row_format == RowFormat.RAW or row_format == RowFormat.TYPED_DICT:
            return cast(list[dict[str, Any]], rows)
        else:
            if len(rows) == 0:
                return np.rec.array(np.empty(0, dtype=comp_cls.dtypes))
            else:
                record_list = cast(list[np.record], rows)
                return np.rec.array(np.stack(record_list, dtype=comp_cls.dtypes))

    async def _zrange_members(
        self,
        table_ref: TableReference,
        index_name: str,
        left: int | float | str | bytes | bool,
        right: int | float | str | bytes | bool | None,
        limit: int,
        desc: bool,
    ) -> tuple[list[bytes], bytes, bytes]:
        """按索引区间 ZRANGE，返回原样 member（value\\x00id）与规范化后的两个边界"""
        if not self._ios:
            raise ConnectionError(_("连接已关闭，已调用过close"))

        idx_key = self.index_key(table_ref, index_name)
        comp_cls = table_ref.comp_cls
        if index_name not in comp_cls.indexes_:
            raise ValueError(f"Component `{comp_cls.name_}` 没有索引 `{index_name}`")
        b_left, b_right = self.range_normalize_(
            comp_cls.dtype_map_[index_name], left, right, desc
        )
        # 两端交叉或相等就是空区间（传反的已在 range_normalize_ 里报错），不用去查
        if (b_left <= b_right) if desc else (b_right <= b_left):
            return [], b_left, b_right

        members = await self.aio.zrange(
            name=idx_key, **self.make_zrange_cmd_(b_left, b_right, desc, limit)
        )
        return cast(list[bytes], members), b_left, b_right

    @override
    async def range_read_(
        self,
        table_ref: TableReference,
        index_name: str,
        left: int | float | str | bytes | bool,
        right: int | float | str | bytes | bool | None,
        limit: int,
        desc: bool,
    ) -> tuple[list[int], RangeObservation]:
        members, b_left, b_right = await self._zrange_members(
            table_ref, index_name, left, right, limit, desc
        )
        row_ids = [int(vk.rsplit(b"\x00", 1)[-1]) for vk in members]
        # 观察区间给 ZLEXCOUNT 用，要按 min, max 排；desc 时 b_left 是上界
        lo, hi = (b_right, b_left) if desc else (b_left, b_right)
        if 0 < limit == len(members):
            # 截断读只看到了前 limit 行，观察区间收到最后一个返回的 member 为止
            if desc:
                lo = b"[" + members[-1]
            else:
                hi = b"[" + members[-1]
        return row_ids, RangeObservation(index_name, row_ids, (lo, hi), members)

    def _range_checks(self, idmap: IdentityMap) -> list[list[str | bytes | int]]:
        """
        把本事务的 range 观察变成 commit 的 CNT 检查（ZLEXCOUNT 观察区间 == 读到的行数）。

        行数不变 + 读到的行 VER 不变，就说明区间里还是这些行；前提是读到的每一行，本事务
        手里的数据（VER 钉住的那个版本）与读取时索引里的 member 一致。ZRANGE 与随后取行
        不是原子的、还可能打到不同节点，中间有行被改走又有行插进来时行数可能不变，所以
        这里先在 worker 上核对，对不上直接判竞态，不去 master。本事务新 insert 的行不核对，
        主键冲突交给 NX 判定（否则盲插已存在的 id 会从 UniqueViolation 变成无限重试）。
        unique 列点查已由 VER / UNIQ 保证不变的，不发 CNT（见 range_observations_to_check）。
        """
        if located := idmap.inconsistent_range():
            raise InconsistentRangeRead(*located)
        for ref, observations in idmap.range_observations().items():
            comp_cls = ref.comp_cls
            for obs in observations:
                dtype = comp_cls.dtype_map_[obs.index_name]
                for row_id, member in zip(obs.ids, obs.members or ()):
                    row = idmap.db_row(ref, row_id)
                    if row is None:
                        continue
                    value = to_sortable_bytes(dtype.type(row[obs.index_name]))
                    if value != member.rsplit(b"\x00", 1)[0]:
                        raise InconsistentRangeRead(
                            comp_cls.name_, obs.index_name, row_id
                        )
        checks: list[list[str | bytes | int]] = []
        for ref, observations in idmap.range_observations_to_check().items():
            for obs in observations:
                lo, hi = obs.bounds
                checks.append(
                    [
                        "CNT",
                        self.index_key(ref, obs.index_name),
                        lo,
                        hi,
                        len(obs.ids),
                        f"{ref.comp_cls.name_}.{obs.index_name}",
                    ]
                )
        return checks

    @override
    async def commit(self, idmap: IdentityMap) -> None:
        """
        使用事务，向数据库提交IdentityMap中的所有数据修改

        Exceptions
        --------
        RaceCondition
            数据已被其他事务修改（版本不符）；或主键 / unique 冲突命中了本事务曾 `get`
            观察其不存在的值（基于过期快照）；或本事务 range 读过的区间变了，可重试
        UniqueViolation
            主键 / unique 值已被占用，且本事务从未观察其不存在：确定性冲突，不重试

        """

        def _key_must_not_exist(_key: str, _race: bool, _label: str):
            """添加key must not exist的检查（insert 主键）；_race 表示本事务曾 get 观察其不存在"""
            (race_checks if _race else strict_checks).append(
                ["NX", _key, "RACE" if _race else "UNIQUE", _label]
            )

        def _version_must_match(_key: str, _old_version):
            """添加version match的检查，恒为竞态类"""
            race_checks.append(["VER", _key, _old_version])

        def _unique_meet(
            _unique_fields,
            _dtype_map,
            _idx_prefix,
            _row: dict[str, str | bytes],
            _absent: set[str],
            _comp_name: str,
            _row_id: str,
            _op: str,
        ):
            """添加unique索引检查；_absent 内的列冲突判竞态(RACE)，其余判确定性冲突(UNIQUE)"""
            for _field, _value in _row.items():
                if _field in _unique_fields:
                    _idx_key = _idx_prefix + _field
                    _sortable_value = to_sortable_bytes(_dtype_map[_field].type(_value))
                    _start_val = b"[" + _sortable_value + b"\x00"
                    _end_val = b"[" + _sortable_value + b"\x00\xff"
                    _race = _field in _absent
                    (race_checks if _race else strict_checks).append(
                        [
                            "UNIQ",
                            _idx_key,
                            _start_val,
                            _end_val,
                            "RACE" if _race else "UNIQUE",
                            f"{_comp_name}.{_field} id={_row_id} {_op}",
                        ]
                    )

        def _hset_key(_key, _old_version, _update: dict[str, str | bytes]):
            """添加hset的push命令"""
            # 版本+1
            _ver = int(_old_version) + 1
            _update.pop("_version", None)  # 无视用户传入的_version字段
            # 组合hset, 别忘记写_version
            _kvs = itertools.chain.from_iterable(_update.items())
            pushes.append(["HSET", _key, "_version", str(_ver), *_kvs])

        def _exc_index(
            _indexes, _point_subs, _dtype_map, _idx_prefix, _old, _new, _add
        ):
            """exchange index(zadd/zrem)的push命令"""
            _b_row_id = _old["id"].encode("ascii")
            _values = _new if _add else _old
            for _field in _new.keys():
                if _field in _indexes:
                    _idx_key = _idx_prefix + _field
                    # 索引全部转换为bytes索引，测试下来lex和score排序性能是一样的
                    _sortable_value = to_sortable_bytes(
                        _dtype_map[_field].type(_values[_field])
                    )
                    # 值频道只给声明了 point_sub 的索引、只记"进入"（insert 的值、update 的
                    # 新值）：离开（delete、改走）由订阅者订着的行频道发现，不用发。
                    # 同一 (索引, 值) 一个事务只发一条
                    if _add and _field in _point_subs:
                        value_chans[self.value_channel_(_idx_key, _sortable_value)] = (
                            None
                        )
                    _member = _sortable_value + b"\x00" + _b_row_id
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
        # 本事务曾 get 观察"不存在"的 unique 列：{ref: {row_id: {field}}}，决定冲突判 RACE 还是 UNIQUE
        absent_by_ref = idmap.get_absent_unique_fields()
        # range 读的区间校验（防幻读）；读取本身就不一致的，这里直接抛 RaceCondition
        range_checks = self._range_checks(idmap)

        # 组合成checks/pushes命令表，减少lua脚本的复杂度
        # checks有exists/unique/version/区间行数，分两组：竞态类在前（VER、带 RACE 标记的
        # NX/UNIQ，最后是区间的 CNT），确定性类在后。Lua 首个失败即返回 → 同时存在两类冲突
        # 时 RACE 优先（保住 upsert 锚定列与其他 unique 列同时撞车时"重试后转 update"的
        # 语义；基于过时区间做的决定撞上 unique 也该重试）。CNT 排在其他竞态检查之后，
        # 同时冲突时报出的仍是原来的信息
        # pushes有hset/zadd/zrem/del
        race_checks: list[list[str | bytes]] = []
        strict_checks: list[list[str | bytes]] = []
        pushes: list[list[str | bytes]] = []
        deleted: dict[str, bool] = {}
        # 主动 PUBLISH 的通知只有两种，都只给声明了的组件/索引发（PUBLISH 很贵，见
        # benchmark/redis_publish_cost_result.md；tests/test_arch_publish.py 守门，别往这里
        # 加新通知、也别往消息里塞内容）：
        # - 表频道 [channel, msgpack(row_id列表)]：table_sub 组件，一个事务一张表一条
        # - 值频道 channel（消息为空串）：point_sub 索引的"进入"，一个事务每个 (索引, 值) 一条
        table_pubs: list[list[str | bytes]] = []
        value_chans: dict[str, None] = {}  # 有序去重

        for ref, (inserts, (old_rows, new_rows), deletes) in dirties.items():
            id_prefix = self.cluster_prefix(ref) + ":id:"
            idx_prefix = self.cluster_prefix(ref) + ":index:"
            comp_cls = ref.comp_cls
            unique_fields = comp_cls.uniques_
            indexes = comp_cls.indexes_
            point_subs = comp_cls.point_subs_
            dtype_map = comp_cls.dtype_map_
            comp_name = comp_cls.name_
            absent_rows = absent_by_ref.get(ref, {})
            # insert
            for insert in inserts:
                row_id = str(insert["id"])
                key = id_prefix + row_id
                absent = absent_rows.get(int(row_id), set())
                _key_must_not_exist(
                    key, "id" in absent, f"{comp_name}.id id={row_id} insert"
                )
                _unique_meet(
                    unique_fields,
                    dtype_map,
                    idx_prefix,
                    insert,
                    absent,
                    comp_name,
                    row_id,
                    "insert",
                )
                _hset_key(key, 0, insert)
                _exc_index(
                    indexes, point_subs, dtype_map, idx_prefix, insert, insert, True
                )
            # update
            for old_row, new_row in zip(old_rows, new_rows):
                row_id = str(old_row["id"])
                key = id_prefix + row_id
                old_version = old_row["_version"]
                _version_must_match(key, old_version)
                _unique_meet(
                    unique_fields,
                    dtype_map,
                    idx_prefix,
                    new_row,
                    absent_rows.get(int(row_id), set()),
                    comp_name,
                    row_id,
                    "update",
                )
                _hset_key(key, old_version, new_row)
                _exc_index(
                    indexes, point_subs, dtype_map, idx_prefix, old_row, new_row, False
                )
                _exc_index(
                    indexes, point_subs, dtype_map, idx_prefix, old_row, new_row, True
                )
            # delete
            for delete in deletes:
                # 传入deleted ids，如果之后的unique冲突查到的id在deleted里，就返回false
                deleted[str(delete["id"])] = True
                key = id_prefix + str(delete["id"])
                old_version = delete["_version"]
                _version_must_match(key, old_version)
                _exc_index(
                    indexes, point_subs, dtype_map, idx_prefix, delete, delete, False
                )
                _del_key(key)
            # 变动的 row_id 只有表频道要用：没声明 table_sub 的组件（绝大多数）不收集
            if comp_cls.table_sub_:
                touched_ids = [
                    *(row["id"] for row in inserts),
                    *(row["id"] for row in old_rows),
                    *(str(row["id"]) for row in deletes),
                ]
                if touched_ids:
                    ids_msg: bytes = msg_packer.pack(touched_ids)  # type: ignore
                    table_pubs.append([self.table_channel(ref), ids_msg])

        # 对纯读行加版本检查，防止事务依赖的陈旧读：
        # 事务读到的某行，在提交前若被其他事务修改，本事务应失败重试。
        for ref, row_versions in idmap.get_clean_rows().items():
            clean_id_prefix = self.cluster_prefix(ref) + ":id:"
            for row_id, old_version in row_versions.items():
                _version_must_match(clean_id_prefix + str(row_id), old_version)

        checks = race_checks + range_checks + strict_checks
        payload_json: bytes = msg_packer.pack(  # type: ignore
            [checks, pushes, deleted, table_pubs, list(value_chans)]
        )
        # 添加一个带cluster id的key，指明lua脚本执行的集群
        keys = [self.row_key(first_ref, 1)]

        # 这里不需要判断redis.exceptions.NoScriptError，因为里面会处理
        assert self.lua_commit is not None, _(
            "lua_commit脚本没有初始化，请先调用 post_configure"
        )
        resp = await self.lua_commit(keys, [payload_json])
        resp = resp.decode("utf-8")  # type: ignore

        if resp != "committed":
            if resp.startswith("RACE"):
                raise RaceCondition(resp)
            elif resp.startswith("UNIQUE"):
                # 确定性冲突：本事务从未 get 观察该值不存在，重试无意义
                raise UniqueViolation(resp)
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
        """
        获取消息队列连接（每个用户连接一个）。本进程对本地址只有一个 `PubSubHub`
        （一条 pubsub 连接）在首次调用时懒建，之后每次返回一个挂在它上面的轻量 MQClient。
        """
        if not self._ios:
            raise ConnectionError(_("连接已关闭，已调用过close"))
        from .mq import PubSubHub, RedisMQClient

        if self._hub is None:
            self._hub = PubSubHub(self.aio)  # aio 会断言事件循环一致
        return RedisMQClient(self._hub)
