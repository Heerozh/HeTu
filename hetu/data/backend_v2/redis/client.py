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
from typing import Any, Callable

import msgspec
import numpy as np
import redis
import redis.asyncio
import redis.asyncio.cluster
import redis.cluster
import redis.exceptions

from ...component import BaseComponent
from ...idmap import IdentityMap
from ..base import BackendClient, RowFormat

logger = logging.getLogger("HeTu.root")


class RedisBackendClient(BackendClient, alias="redis"):
    """和Redis后端的操作的类，服务器启动时由server.py根据Config初始化"""

    def load_lua_scripts(self, file: str | Path) -> Callable:
        assert self._async_ios
        # read file to text
        with open(file, "r", encoding="utf-8") as f:
            script_text = f.read()
        # 上传脚本到服务器使用同步io
        self.io.script_load(script_text)
        # 注册脚本到异步io
        return self.aio.register_script(script_text)

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

    def __init__(self, endpoint: str | list[str], clustering: bool, is_servant=False):
        super().__init__(endpoint, clustering, is_servant)
        # redis的endpoint配置为url, 或list of url
        self.urls = [endpoint] if type(endpoint) is str else endpoint
        assert len(self.urls) > 0, "必须至少指定一个数据库连接URL"

        # 创建连接
        self._ios = []
        self._async_ios = []
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
        self.dbi = self._ios[0].connection_pool.connection_kwargs["db"]

        # 加载lua脚本，注意pipeline里不能用lua，会反复检测script exists性能极低
        self.lua_commit = self.load_lua_scripts(
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
            version = io.info("server")["redis_version"]
            assert parse_version(version) >= (7, 0), "Redis版本过低，至少需要7.0版本"

    def configure_servant(self) -> None:
        if not self._ios:
            raise ConnectionError("连接已关闭，已调用过close")
            # 检查servants设置

        target_keyspace = "Kghz"
        for i, io in enumerate(self._ios):
            try:
                # 设置keyspace通知
                db_keyspace = io.config_get("notify-keyspace-events")[
                    "notify-keyspace-events"
                ]
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
            db_replica = io.config_get("replica-read-only")
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

    def reset_async_connection_pool(self):
        """重置异步连接池，用于协程切换后，解决aio不能跨协程传递的问题"""
        self.loop_id = 0
        for aio in self._async_ios:
            aio.connection_pool.reset()

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

    async def get(
        self,
        comp_cls: type[BaseComponent],
        row_id: int,
        row_format=RowFormat.STRUCT,
    ) -> np.record | dict[str, Any] | None:
        """获取行数据"""
        # todo 所有get query要合批
        key = self._key_prefix + str(row_id)
        row = await self.aio.hgetall(key)
        if row:
            # todo 此时的row数据都是byte

            match row_format:
                case RowFormat.RAW:
                    return row
                case RowFormat.STRUCT:
                    return comp_cls.dict_to_row(row)
                case RowFormat.TYPED_DICT:
                    struct_row = comp_cls.dict_to_row(row)
                    return comp_cls.row_to_dict(struct_row)
                case _:
                    raise ValueError(f"不可用的行格式: {row_format}")
        else:
            return None

    def _range_normalize(
        self,
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
        comp_cls: type[BaseComponent],
        index_name: str,
        left: int | float | str,
        right: int | float | str | None,
        limit: int = 100,
        desc: bool = False,
        row_format=RowFormat.STRUCT,
    ) -> list[int] | np.recarray:
        """查询index数据"""
        # todo 所有get query要合批
        # todo 想一下这几个keyprefix如何处理，可以先把session做完了再考虑？
        idx_key = self._idx_prefix + index_name
        aio = self.aio  # 保存随机选中的aio连接

        # 生成zrange命令
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

        typed = row_format == RowFormat.TYPED_DICT or row_format == RowFormat.STRUCT
        dict_fmt = row_format == RowFormat.RAW or row_format == RowFormat.TYPED_DICT

        key_prefix = self._key_prefix
        rows = []
        for _id in row_ids:
            if row := await aio.hgetall(key_prefix + str(_id)):
                if typed:
                    row = comp_cls.dict_to_row(row)
                if dict_fmt:
                    row = comp_cls.row_to_dict(row)
                rows.append(row)

        if dict_fmt:
            return rows
        else:
            if len(rows) == 0:
                return np.rec.array(np.empty(0, dtype=comp_cls.dtypes))
            else:
                return np.rec.array(np.stack(rows, dtype=comp_cls.dtypes))

    async def commit(self, idmap: IdentityMap) -> None:
        """提交修改事务，使用从IdentityMap中获取的脏数据"""
        assert not self.is_servant, "从节点不允许提交事务"

        dirty_rows = idmap.get_dirty_rows()
        assert len(dirty_rows) > 0, "没有脏数据需要提交"

        payload = msgspec.msgpack.encode(dirty_rows)
        keys = []  # todo 需要添加一个表示cluster的key
        return await self.lua_commit(keys, payload)

    # 还需要
    # create table
    # migration table schema
    # migration cluster id
    # flush table
    # rebuild table index
    # 可以考虑一个table_maintenance类专门做这个

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
