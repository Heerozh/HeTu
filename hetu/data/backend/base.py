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
                           ├───────────────────────────┐
                 ┌─────────┴──────────┐      ┌─────────┴─────────┐
                 │      Backend       │      │  TableMaintenance │ 继承此类实现数据维护
                 │   数据库连接管理器    │      │     组件表维护类    │
                 └────────────────────┘      └───────────────────┘
                           ▲
            ┌──────────────┴────────────┐
  ┌─────────┴──────────┐      ┌─────────┴────────┐
  │   ComponentTable   │      │      Session     │
  │    组件数据访问      │      │     事务处理类     │
  └────────────────────┘      └──────────────────┘
            ▲                          ▲
 ┌──────────┴──────────┐   ┌───────────┴────────────┐
 │ComponentTableManager│   │         Select         │    todo 直接select出来的就是此类
 │   组件数据访问管理器   │   │      组件相关事务操作     │  # todo 改成SessionComponentTable，读写其实是传给idmap，提交也是idmap
 └─────────────────────┘   └────────────────────────┘




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

from enum import Enum
from typing import TYPE_CHECKING, Any

import numpy as np

if TYPE_CHECKING:
    from ...common.snowflake_id import WorkerKeeper
    from .idmap import IdentityMap
    from .table import TableReference


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
    数据库后端的连接类，Backend会用此类创建master, servant连接。

    继承写法：
    class PostgresClient(BackendClient, alias="postgres")

    服务器启动时，Backend会根据Config中type配置，寻找对应alias初始化Client。
    继承此类，完善所有NotImplementedError的方法。
    """

    def __init_subclass__(cls, **kwargs):
        """让继承子类自动注册alias"""
        super().__init_subclass__()
        BackendClientFactory.register(kwargs["alias"], cls)

    def __init__(self, endpoint: Any, clustering: bool, is_servant=False):
        """
        建立数据库连接。
        endpoint为config中master，或者servants的内容。
        clustering表示数据库是一个垂直分片（按Component分片）的集群，每个Component的
        所属集群cluster_id可以通过SystemClusters获得，发生变更时也要Client负责迁移。
        is_servant指定endpoint是否为从节点，从节点只读。
        """
        self.endpoint = endpoint
        self.clustering = clustering
        self.is_servant = is_servant

    async def close(self):
        """关闭数据库连接，释放资源。"""
        raise NotImplementedError

    def configure(self) -> None:
        """启动时检查并配置数据库，减少运维压力的帮助方法，非必须。"""
        raise NotImplementedError

    async def is_synced(self) -> bool:
        """
        在主库上查询同步状态，返回是否已完成同步。
        主要用于test用例。
        """
        # assert not self.is_servant, "is_synced只能在master上调用"
        raise NotImplementedError

    def get_worker_keeper(self) -> WorkerKeeper | None:
        """
        获取WorkerKeeper实例，用于雪花ID的worker id管理。
        如果不支持worker id管理，可以返回None
        """
        raise NotImplementedError

    # def get_mq_client(self) -> "MQClient":
    #     raise NotImplementedError

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
        raise NotImplementedError

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
        请利用python的特性，先在数据库上筛选出最少量的数据，然后本地二次筛选：

        >>> items = client.range(ref, "owner", player_id, limit=100)  # noqa
        >>> few_items = items[items.amount < 10]

        由于python numpy支持SIMD，比直接在数据库复合查询快。
        """
        raise NotImplementedError

    async def commit(self, idmap: IdentityMap) -> None:
        """
        使用事务，向数据库提交IdentityMap中的所有数据修改

        Exceptions
        --------
        RaceCondition
            当提交数据时，发现数据已被其他事务修改，抛出此异常

        """
        raise NotImplementedError


#     def get_mq_client(self) -> MQClient:
#         """获取消息队列连接"""
#         raise NotImplementedError


class BackendClientFactory:
    _registry: dict[str, type[BackendClient]] = {}

    @staticmethod
    def register(alias: str, client_cls: type[BackendClient]) -> None:
        BackendClientFactory._registry[alias] = client_cls

    @staticmethod
    def create(
        alias: str, endpoint: Any, clustering: bool, is_servant=False
    ) -> BackendClient:
        return BackendClientFactory._registry[alias](endpoint, clustering, is_servant)


class TableMaintenance:
    """
    提供给CLI命令使用的组件表维护类。当有新表，或需要迁移时使用。
    继承此类实现具体的维护逻辑，此类仅在CLI相关命令时才会启用。
    """

    def __init__(self, client: BackendClient):
        """传入master连接的BackendClient实例"""
        self.client = client

    # 检测是否需要维护的方法
    def check_table(self, table_ref: TableReference):
        """
        检查组件表在数据库中的状态。

        Returns
        -------
        status: str
            "not_exists" - 表不存在
            "ok" - 表存在且状态正常
            "cluster_mismatch" - 表存在但cluster_id不匹配
            "schema_mismatch" - 表存在但schema不匹配
        """
        raise NotImplementedError

    def create_table(self, table_ref: TableReference) -> dict:
        """创建组件表。如果已存在，会抛出异常"""
        raise NotImplementedError

    # 无需drop_table, 此类操作适合人工删除

    def migration_cluster_id(
        self, table_ref: TableReference, old_cluster_id: int
    ) -> None:
        """迁移组件表的cluster_id"""
        raise NotImplementedError

    def migration_schema(self, table_ref: TableReference, old_json: str) -> None:
        """迁移组件表的schema"""
        raise NotImplementedError

    def flush(self, table_ref: TableReference, force=False) -> None:
        """
        清空易失性组件表数据，force为True时强制清空任意组件表。
        注意：此操作会删除所有数据！
        """
        raise NotImplementedError

    def rebuild_index(self, table_ref: TableReference) -> None:
        """重建组件表的索引数据"""
        raise NotImplementedError


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
