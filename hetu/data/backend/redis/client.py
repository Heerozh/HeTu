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
from typing import TYPE_CHECKING, Any, Literal, cast, final, overload, override

import numpy as np
import redis
from redis.cluster import LoadBalancingStrategy

from ....i18n import _
from ..base import RowFormat, detach_rows_
from ..redis_model import RedisModelClient
from .pool import HeTuConnectionPool

if TYPE_CHECKING:
    import redis.asyncio
    import redis.asyncio.cluster
    import redis.cluster
    import redis.exceptions

    from ...component import BaseComponent
    from ..idmap import RangeObservation
    from ..table import TableReference
    from .maint import RedisTableMaintenance
    from .mq import PubSubHub, RedisMQClient

logger = logging.getLogger("HeTu.root")


@final
class RedisBackendClient(RedisModelClient, alias="redis"):
    """
    和Redis后端的操作的类，服务器启动时由server.py根据Config初始化。
    key 布局、索引编码、commit payload 等纯逻辑在 `RedisModelClient`（与 SQLite 后端共用），
    这里只有 redis-py 的 I/O。
    """

    # range/get_many 批量读行时，每个pipeline最多打包的HGETALL条数
    RANGE_PIPELINE_CHUNK = 1000

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

    async def reset_async_connection_pool(self):
        """重置异步连接池，用于协程切换后，解决aio不能跨协程传递的问题"""
        self.loop_id = 0
        await self._close_hub()
        for aio in self._async_ios:
            if isinstance(aio, redis.asyncio.cluster.RedisCluster):
                await aio.aclose()  # 未测试
            else:
                aio.connection_pool.reset()

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
        # 提示用户schema定义是否符合要求，比如索引类型不能有复数等
        self._schema_checking(components)

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
        raw_rows = [row for row in await self._hgetall_many(key_prefix, row_ids) if row]
        if row_format == RowFormat.STRUCT:
            return self.rows_decode_(comp_cls, raw_rows)
        return [
            cast(dict[str, Any], self.row_decode_(comp_cls, row, row_format))
            for row in raw_rows
        ]

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

        idx_key, b_left, b_right, empty = self.zrange_args_(
            table_ref, index_name, left, right, desc
        )
        # 两端交叉或相等就是空区间（传反的已在 range_normalize_ 里报错），不用去查
        if empty:
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
        return self.range_observation_(
            index_name, members, b_left, b_right, limit, desc
        )

    @override
    async def commit_script_(self, keys: list[str], args: list[bytes]) -> bytes:
        """执行 commit_v2.lua（`configure_master` 里加载），见基类"""
        # 这里不需要判断redis.exceptions.NoScriptError，因为里面会处理
        assert self.lua_commit is not None, _(
            "lua_commit脚本没有初始化，请先调用 post_configure"
        )
        return await self.lua_commit(keys, args)

    @override
    async def direct_set(
        self, table_ref: TableReference, id_: int, **kwargs: str
    ) -> None:
        """
        UNSAFE! 只用于易失数据! 不会做类型检查! 契约见基类。

        HSET：缺行时建出只有这几个字段的残缺行；行频道是行 key 的 keyspace 通知，会顺带触发
        （契约不保证通知）。
        """
        self.check_direct_set_(table_ref, kwargs)
        aio = self.aio
        key = self.row_key(table_ref, id_)
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
