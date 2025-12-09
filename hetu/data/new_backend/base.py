"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com

                      后端相关结构
                  ┌────────────────┐
                  │  BackendClient │ 继承此类实现各种BackendClient
                  │数据库连接/事务处理│
                  └────────────────┘
                           ▲
                  ┌────────┴─────────┐
                  │     Backend      │
                  │数据库连接管理（单件)│
                  └──────────────────┘
                           ▲
            ┌──────────────┴────────────┐
  ┌─────────┴──────────┐      ┌─────────┴────────┐
  │   ComponentTable   │      │      Session     │     todo 包含idmap
  │  组件数据访问（单件)  │      │     事务处理类    │
  └────────────────────┘      └──────────────────┘
                                       ▲
                           ┌───────────┴────────────┐
                           │         Select         │    todo 直接select出来的就是此类
                           │      组件相关事务操作     │  # todo 改成SessionComponentTable，读写其实是传给idmap，提交也是idmap
                           └────────────────────────┘

        数据订阅结构
    ┌─────────────────┐
    │     MQClient    │
    │消息队列连接(每用户）│  继承此类实现各种backend
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

import numpy as np
from enum import Enum
from typing import Any

from ..component import BaseComponent
from ..idmap import IdentityMap


class RaceCondition(Exception):
    pass


class UniqueViolation(IndexError):
    pass


class RowFormat(Enum):
    """行格式枚举"""

    RAW = 0  # 未经类型转换的dict格式，具体类型由数据库决定
    STRUCT = 1  # 默认值：按Component定义严格转换的np.record（c-struct like）类型
    TYPED_DICT = 2  # 先转换成STRUCT，再转换成dict的类型。
    ID_LIST = 3  # 只返回list of row id，只能用于range查询


class BackendClient:
    """
    数据库后端的连接类，Backend会用此类创建master, servant连接。继承方法：
    class PostgresClient(BackendClient, alias="postgres")
    服务器启动时，server.py会根据Config中backend type配置，寻找对应alias初始化Client。
    继承此类，完善所有NotImplementedError的方法。
    """

    _registry: dict[str, type["BackendClient"]] = {}

    def __init_subclass__(cls, **kwargs):
        """让继承子类自动注册alias"""
        super().__init_subclass__(**kwargs)
        BackendClient._registry[kwargs["alias"]] = cls

    def __init__(self, config: dict, is_servant=False):
        """通过config初始化连接，is_servant指定本连接是否为从节点"""
        self.is_servant = is_servant

    async def close(self):
        raise NotImplementedError

    def configure(self) -> None:
        """启动时检查并配置数据库，减少运维压力的帮助方法，非必须。"""
        raise NotImplementedError

    # def get_mq_client(self) -> "MQClient":
    #     raise NotImplementedError

    async def get(
        self,
        comp_cls: type[BaseComponent],
        row_id: int,
        row_format=RowFormat.STRUCT,
    ) -> np.record | dict[str, Any] | None:
        """获取行数据"""
        raise NotImplementedError

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
        raise NotImplementedError

    async def commit(self, idmap: IdentityMap) -> None:
        """提交修改事务，使用从IdentityMap中获取的脏数据"""
        raise NotImplementedError

    # def flush(self, comp_cls: type[BaseComponent], force=False):
    #     raise NotImplementedError


# class Backend:
#     """
#     存放数据库连接的池，并负责开始事务。
#     继承此类，完善所有NotImplementedError的方法。
#     """

#     def __init__(self, config: dict):
#         self.master =
#         pass

#     async def close(self):
#         raise NotImplementedError

#     def configure(self):
#         """
#         启动时检查并配置数据库，减少运维压力的帮助方法，非必须。
#         """
#         raise NotImplementedError

#     async def is_synced(self) -> bool:
#         """
#         检查各个slave数据库和master数据库的数据是否已完成同步。
#         主要用于test用例。
#         """
#         raise NotImplementedError

#     async def wait_for_synced(self) -> None:
#         """
#         等待各个slave数据库和master数据库的数据完成同步。
#         主要用于test用例。
#         """
#         while not await self.is_synced():
#             await asyncio.sleep(0.1)

#     def get_mq_client(self) -> MQClient:
#         """获取消息队列连接"""
#         raise NotImplementedError

#     # 这几个是通过BackendClient来抽象，还是通过自己的private方法来抽象？
#     # 用BackendClient可以分别创建master和slave的连接池，而连接不用管自己是啥。不过master和slave的config方式不一样，还是无法逻辑分开
#     async def master_get(
#         self,
#         comp_cls: type[BaseComponent],
#         row_id: int,
#         row_format="struct",
#     ) -> np.record | None:
#         """从主节点获取行数据"""
#         raise NotImplementedError

#     async def master_query(
#         self,
#         comp_cls: type[BaseComponent],
#         index_name: str,
#         left,
#         right,
#         limit: int = 100,
#         desc: bool = False,
#         row_format="struct",
#     ) -> np.recarray:
#         """从主节点查询index数据"""
#         raise NotImplementedError

#     async def master_commit(self, dirty_rows) -> None:
#         """提交修改事务到主节点，dirty_rows为从IdentityMap中获取的脏数据"""
#         raise NotImplementedError

#     async def servant_get(
#         self,
#         comp_cls: type[BaseComponent],
#         row_id: int,
#         row_format="struct",
#     ) -> np.record | None:
#         """从从节点获取行数据"""
#         raise NotImplementedError

#     async def servant_query(
#         self,
#         comp_cls: type[BaseComponent],
#         index_name: str,
#         left,
#         right,
#         limit: int = 100,
#         desc: bool = False,
#         row_format="struct",
#     ) -> np.recarray:
#         """从从节点查询index数据"""
#         raise NotImplementedError


# # === === === === === === 数据订阅 === === === === === ===


# class MQClient:
#     """连接到消息队列的客户端，每个用户连接一个实例。订阅后端只需要继承此类。"""

#     # todo 加入到config中去，设置服务器的通知tick
#     UPDATE_FREQUENCY = 10  # 控制客户端所有订阅的数据（如果有变动），每秒更新几次

#     async def close(self):
#         raise NotImplementedError

#     async def pull(self) -> None:
#         """
#         从消息队列接收一条消息到本地队列，消息内容为channel名，每行数据，每个Index，都是一个channel。
#         该channel收到了任何消息都说明有数据更新，所以只需要保存channel名。

#         消息存放本地时，需要用时间作为索引，并且忽略重复的消息。存放前先把2分钟前的消息丢弃，防止堆积。
#         此方法需要单独的协程反复调用，防止服务器也消息堆积。
#         """
#         # 必须合并消息，因为index更新时大都是2条一起的
#         raise NotImplementedError

#     async def get_message(self) -> set[str]:
#         """
#         pop并返回之前pull()到本地的消息，只pop收到时间大于1/UPDATE_FREQUENCY的消息。
#         之后Subscriptions会对该消息进行分析，并重新读取数据库获数据。
#         如果没有消息，则堵塞到永远。
#         """
#         raise NotImplementedError

#     async def subscribe(self, channel_name: str) -> None:
#         """订阅频道"""
#         raise NotImplementedError

#     async def unsubscribe(self, channel_name: str) -> None:
#         """取消订阅频道"""
#         raise NotImplementedError

#     @property
#     def subscribed_channels(self) -> set[str]:
#         """返回当前订阅的频道名"""
#         raise NotImplementedError


# class BaseSubscription:
#     async def get_updated(
#         self, channel
#     ) -> tuple[set[str], set[str], dict[str, dict | None]]:
#         raise NotImplementedError

#     @property
#     def channels(self) -> set[str]:
#         raise NotImplementedError
