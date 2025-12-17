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
from typing import TYPE_CHECKING, Any, Callable, cast

import msgspec
import numpy as np
import redis

from ....common.snowflake_id import RedisWorkerKeeper
from ..base import BackendClient, RowFormat

if TYPE_CHECKING:
    import redis.asyncio
    import redis.asyncio.cluster
    import redis.cluster
    import redis.exceptions

    from ...component import BaseComponent
    from ..idmap import IdentityMap
    from ..table import TableReference

logger = logging.getLogger("HeTu.root")


class RedisBackendClient(BackendClient, alias="redis"):
    """和Redis后端的操作的类，服务器启动时由server.py根据Config初始化"""

    def load_commit_scripts(self, file: str | Path) -> Callable:
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
        # 读取namespace下的所有schema定义，然后替换lua脚本里的schema定义
        # ["User:{CLU1}"] = {
        #     unique = { ["email"] = true, ["phone"] = true },
        #     indexes = { ["email"] = false, ["age"] = true, ["phone"] = true }
        # }
        from ....system.definer import SystemClusters

        lua_schema_def = ["{"]
        for comp_cls in SystemClusters().get_components().keys():
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
        lua_schema_text = "\n".join(lua_schema_def)
        # replace PLACEHOLDER_SCHEMA_DEFINITIONS in script_text
        script_text = script_text.replace("PLACEHOLDER_SCHEMA", lua_schema_text)

        # 上传脚本到服务器使用同步io
        self._ios[0].script_load(script_text)
        # 注册脚本到异步io，因为master只能有一个连接，直接[0]就行了
        return self._async_ios[0].register_script(script_text)

    @property
    def io(self):
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
        return (
            f"{table_ref.instance_name}:{table_ref.comp_cls.component_name_}:"
            f"{{CLU{table_ref.cluster_id}}}"
        )

    @classmethod
    def row_key(cls, table_ref: TableReference, row_id: str | int) -> str:
        """获取redis表行的key名"""
        return f"{cls.table_prefix(table_ref)}:id:{str(row_id)}"

    @classmethod
    def index_key(cls, table_ref: TableReference, index_name: str) -> str:
        """获取redis表索引的key名"""
        return f"{cls.table_prefix(table_ref)}:index:{index_name}"

    @staticmethod
    def meta_key(table_ref: TableReference) -> str:
        """获取redis表元数据的key名"""
        return f"{table_ref.instance_name}:{table_ref.comp_cls.component_name_}:meta"

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
                # todo: 测试byte数据是否能正确的储存和读取
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
            io = cast(redis.Redis, self._ios[0])  # 转换类型，为了通过类型检查
            self.dbi = io.connection_pool.connection_kwargs["db"]

        # 加载lua脚本，注意pipeline里不能用lua，会反复检测script exists性能极低
        if not self.is_servant:
            self.lua_commit = self.load_commit_scripts(
                Path(__file__).parent.resolve() / "commit.lua"
            )

        # 限制aio运行的coroutine
        try:
            self.loop_id = hash(asyncio.get_running_loop())
        except RuntimeError:
            self.loop_id = 0

    def configure(self) -> None:
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
            assert redis_ver >= (7, 0), "Redis版本过低，至少需要7.0版本"

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
                logger.warning(
                    f"⚠️ [💾Redis] 无权限调用数据库{self.urls[i]}的config_set命令，数据订阅将"
                    f"不起效。可手动设置配置文件：notify-keyspace-events={target_keyspace}"
                )
            # 检查是否是replica模式
            db_replica = cast(dict, io.config_get("replica-read-only"))
            if db_replica.get("replica-read-only") != "yes":
                logger.warning(
                    "⚠️ [💾Redis] servant必须是Read Only Replica模式。"
                    f"{self.urls[i]} 未设置replica-read-only=yes"
                )
                # 不检查replicaof master地址，因为replicaof的可能是其他replica地址
            # 考虑可以检查pubsub client buff设置，看看能否redis崩了提醒下
            # pubsub值建议为$剩余内存/预估在线数$

    async def is_synced(self) -> bool:
        if not self._ios:
            raise ConnectionError("连接已关闭，已调用过close")

        assert not self.is_servant, "is_synced只能在master上调用"

        info = await self.aio.info("replication")
        master_offset = info.get("master_repl_offset", 0)
        for key, value in info.items():
            # 兼容 Redis 新旧版本（slave/replica 字段）
            if key.startswith("slave") or key.startswith("replica"):
                lag_of_offset = master_offset - int(value.get("offset", 0))
                if lag_of_offset > 0:
                    return False
        return True

    def get_worker_keeper(self) -> RedisWorkerKeeper:
        """
        获取RedisWorkerKeeper实例，用于雪花ID的worker id管理。
        """
        assert not self.is_servant, "get_worker_keeper"
        return RedisWorkerKeeper(self.io, self.aio)

    async def close(self):
        if not self._ios:
            return

        for io in self._ios:
            io.close()
        self._ios = []

        for aio in self._async_ios:
            await aio.aclose()
        self._async_ios = []

    # def get_mq_client(self) -> RedisMQClient:
    #     """每个websocket连接获得一个随机的replica连接，用于读取订阅"""
    #     if not self.io:
    #         raise ConnectionError("连接已关闭，已调用过close")
    #     return RedisMQClient(self.random_replica())

    @staticmethod
    def _row_decode(
        comp_cls: type[BaseComponent], row: dict[str, str], fmt: RowFormat
    ) -> np.record | dict[str, Any]:
        """将redis获取的行byte数据解码为指定格式"""
        match fmt:
            case RowFormat.RAW:
                # todo encode byte
                return row
            case RowFormat.STRUCT:
                return comp_cls.dict_to_row(row)
            case RowFormat.TYPED_DICT:
                struct_row = comp_cls.dict_to_row(row)
                return comp_cls.row_to_dict(struct_row)
            case _:
                raise ValueError(f"不可用的行格式: {fmt}")

    async def get(
        self, table_ref: TableReference, row_id: int, row_format=RowFormat.STRUCT
    ) -> np.record | dict[str, Any] | None:
        """获取行数据"""
        # todo 所有get query要合批
        key = self.row_key(table_ref, row_id)
        if row := await self.aio.hgetall(key):
            # todo 此时的row数据都是byte
            return self._row_decode(table_ref.comp_cls, row, row_format)
        else:
            return None

    @staticmethod
    def _range_normalize(
        is_str_index: bool,
        left: int | float | str,
        right: int | float | str | None,
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

    async def range(
        self,
        table_ref: TableReference,
        index_name: str,
        left: int | float | str,
        right: int | float | str | None,
        limit: int = 100,
        desc: bool = False,
        row_format=RowFormat.STRUCT,
    ) -> list[int] | list[dict[str, Any]] | np.recarray:
        """查询index数据"""
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
            row_ids = [vk.split(":")[-1] for vk in row_ids]

        if row_format == RowFormat.ID_LIST:
            return list(map(int, row_ids))

        key_prefix = self.table_prefix(table_ref) + ":id:"  # 存下前缀组合key快1倍
        rows = []
        for _id in row_ids:
            if row := await aio.hgetall(key_prefix + str(_id)):
                rows.append(self._row_decode(comp_cls, row, row_format))

        if row_format == RowFormat.RAW or row_format == RowFormat.TYPED_DICT:
            return cast(list[dict[str, Any]], rows)
        else:
            if len(rows) == 0:
                return np.rec.array(np.empty(0, dtype=comp_cls.dtypes))
            else:
                record_list = cast(list[np.record], rows)
                return np.rec.array(np.stack(record_list, dtype=comp_cls.dtypes))

    async def commit(self, idmap: IdentityMap) -> None:
        """
        提交修改事务，使用从IdentityMap中获取的脏数据
        Returns
        -------
        new_ids: list[int]
            返回新插入行的ID列表，顺序和插入顺序一致。
        """
        # todo 在事务的insert方法需要判断：unique，version为0
        #      update要判断 有列修改 已修改列的unique id不允许修改

        assert not self.is_servant, "从节点不允许提交事务"

        dirty_rows = idmap.get_dirty_rows()
        assert len(dirty_rows) > 0, "没有脏数据需要提交"

        ref = idmap.first_reference()
        assert ref is not None, "不该走到这里，仅用于typing检查"

        # 转换dirty_rows为纯lua可用的信息格式：
        # payload={"insert": {"instance:TableName:{CLU1}": [row_dict, ...]}...}
        payload: dict[str, dict[str, list[dict[str, Any]]]] = {
            "insert": {
                self.table_prefix(ref): [ref.comp_cls.row_to_dict(row) for row in rows]
                for ref, rows in dirty_rows["insert"].items()
            },
            "update": {
                self.table_prefix(ref): [ref.comp_cls.row_to_dict(row) for row in rows]
                for ref, rows in dirty_rows["update"].items()
            },
            "delete": {
                self.table_prefix(ref): [
                    # 只需要id和_version字段
                    {"id": row["id"], "_version": row["_version"]}
                    for row in rows
                ]
                for ref, rows in dirty_rows["delete"].items()
            },
        }
        payload_json = msgspec.msgpack.encode(payload)
        # 添加一个带cluster id的key，指明lua脚本执行的集群
        keys = [self.row_key(ref, 1)]
        return await self.lua_commit(keys, payload_json)

    # 还需要
    # create table
    # migration table schema
    # migration cluster id
    # flush table
    # rebuild table index
    # 可以考虑一个table_maintenance类专门做这个
    # 这个类只需要启动时运行一次，然后就可以丢掉了。
    # 每启动一次namespace应该都需要启动一次table_maintenance

    # def flush(self, comp_cls: type[BaseComponent], force=False):
    #     if force:
    #         warnings.warn("flush正在强制删除所有数据，此方式只建议维护代码调用。")

    #     # 如果非持久化组件，则允许调用flush主动清空数据
    #     if not comp_cls.persist_ or force:
    #         io = self.io
    #         logger.info(
    #             f"⌚ [💾Redis][{self._name}组件] 对非持久化组件flush清空数据中..."
    #         )

    #         # 这部分要想办法
    #         with io.lock(self._init_lock_key, timeout=60 * 5):
    #             del_keys = io.keys(self._root_prefix + "*")
    #             del_keys.remove(self._init_lock_key)
    #             for batch in batched(del_keys, 1000):
    #                 with io.pipeline() as pipe:
    #                     list(map(pipe.delete, batch))
    #                     pipe.execute()
    #         logger.info(f"✅ [💾Redis][{self._name}组件] 已删除{len(del_keys)}个键值")

    #         self.create_or_migrate()
    #     else:
    #         raise ValueError(f"{self._name}是持久化组件，不允许flush操作")
