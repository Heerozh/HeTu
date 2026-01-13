"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com


                               Backend相关结构
    ┌─────────────────┐      ┌────────────────┐       ┌───────────────────┐
    │     MQClient    │      │  BackendClient │       │  TableMaintenance │
    │消息队列连接(每连接)│─────►│  数据库连接/操作 │◄──────┤    组件表维护类     │
    └─────────────────┘      └────────────────┘       └───────────────────┘
    继承此类实现各种通知队列      继承此类实现各种数据库         继承此类实现表维护
            ▲                        ▲                         ▲
            │                        └───────────┬─────────────┘
 数据订阅结构 │                                    │ 数据事务结构
  ┌─────────┴──────────┐               ┌─────────┴──────────┐
  │    Subscriptions   │               │      Backend       │
  │ 每连接一个的消息管理器 │               │  数据库连接管理器    │ 每个进程一个Backend
  └────────────────────┘               └────────────────────┘
            ▲                                    ▲
  ┌─────────┴──────────┐                ┌────────┴─────────┐
  │ 用户连接(Websocket) │                │      Session     │
  │   等待Subs返回消息   │                │     事务处理类     │
  └────────────────────┘                └──────────────────┘
                                                 ▲
                                       ┌─────────┴──────────┐
                                       │  SessionRepository │
                                       │   组件相关事务操作    │
                                       └────────────────────┘

"""

import logging
from enum import Enum
from typing import TYPE_CHECKING, Any, Callable, Literal, overload

import numpy as np

if TYPE_CHECKING:
    from ...common.snowflake_id import WorkerKeeper
    from .idmap import IdentityMap
    from .table import TableReference

logger = logging.getLogger("HeTu.root")


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

    def index_channel(self, table_ref: TableReference, index_name: str):
        """返回索引的频道名。如果索引有数据变动，会通知到该频道"""
        raise NotImplementedError

    def row_channel(self, table_ref: TableReference, row_id: int):
        """返回行数据的频道名。如果行有变动，会通知到该频道"""
        raise NotImplementedError

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

    def post_configure(self) -> None:
        """
        对数据库做的配置工作放在这，可以做些减少运维压力的工作，或是需要项目加载完成后才能做的初始化工作。
        此项在服务器完全加载完毕后才会执行，在测试环境中，也是最后调用。
        """
        raise NotImplementedError

    async def is_synced(self, checkpoint: Any = None) -> tuple[bool, Any]:
        """
        在master库上查询待各个savants数据库同步状态，防止后续事务获取不到数据。
        主要用于关键节点，比如创建新用户连接。
        checkpoint指数据检查点，如写入日志的行数，检查该点之前的数据是否已同步完成。

        返回是否已完成同步，以及master最新checkpoint（可以用来下一次查询）。
        """
        # assert not self.is_servant, "is_synced只能在master上调用"
        raise NotImplementedError

    def get_worker_keeper(self, pid: int) -> WorkerKeeper | None:
        """
        获取WorkerKeeper实例，用于雪花ID的worker id管理。
        如果不支持worker id管理，可以返回None

        Parameters
        ----------
        pid: int
            worker的pid。
        """
        raise NotImplementedError

    # 类型注解部分
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

    @overload
    async def range(
        self,
        table_ref: TableReference,
        index_name: str,
        left: int | float | str | bytes | bool,
        right: int | float | str | bytes | bool | None = None,
        limit: int = 10,
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
        limit: int = 10,
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
        limit: int = 10,
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
        limit: int = 10,
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
        limit: int = 10,
        desc: bool = False,
        row_format: RowFormat = ...,
    ) -> np.recarray | list[dict[str, str]] | list[dict[str, Any]] | list[int]: ...
    async def range(
        self,
        table_ref: TableReference,
        index_name: str,
        left: int | float | str | bytes | bool,
        right: int | float | str | bytes | bool | None = None,
        limit: int = 10,
        desc: bool = False,
        row_format=RowFormat.STRUCT,
    ):
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
        assert table_ref.comp_cls.volatile_, "direct_set只能用于易失数据的Component"
        raise NotImplementedError

    def get_table_maintenance(self) -> TableMaintenance:
        """
        获取表维护对象，根据不同后端类型返回不同的实现。
        """
        raise NotImplementedError

    def get_mq_client(self) -> MQClient:
        """获取消息队列连接"""
        raise NotImplementedError


class BackendClientFactory:
    _registry: dict[str, type[BackendClient]] = {}

    @staticmethod
    def register(alias: str, client_cls: type[BackendClient]) -> None:
        BackendClientFactory._registry[alias] = client_cls

    @staticmethod
    def create(
        alias: str, endpoint: Any, clustering: bool, is_servant=False
    ) -> BackendClient:
        alias = alias.lower()
        if alias not in BackendClientFactory._registry:
            raise NotImplementedError(f"{alias} 后端未实现")
        return BackendClientFactory._registry[alias](endpoint, clustering, is_servant)


class TableMaintenance:
    """
    提供给CLI命令使用的组件表维护类。当有新表，或需要迁移时使用。
    继承此类实现具体的维护逻辑，此类仅在CLI相关命令时才会启用。
    """

    @staticmethod
    def _load_migration_schema_script(
        table_ref: TableReference, old_version: str
    ) -> Callable | None:
        """加载组件模型的的用户迁移脚本"""
        # todo test
        import hashlib
        import importlib.util
        import sys
        from pathlib import Path

        new_version = hashlib.md5(table_ref.comp_cls.json_.encode("utf-8")).hexdigest()
        migration_file = f"{table_ref.comp_name}_{old_version}_to_{new_version}.py"
        # 组合当前目录 + maint/migration/目录 + 迁移文件名
        script_path = Path.cwd() / "maint" / "migration" / migration_file
        script_path = script_path.absolute()
        if script_path.exists():
            logger.warning(
                f"  ➖ [💾Redis][{table_ref.comp_name}组件] "
                f"发现自定义迁移脚本 {script_path}，将调用脚本进行迁移..."
            )
            module_name = (
                f"Migration_{table_ref.comp_name}_{old_version}_to_{new_version}"
            )
            spec = importlib.util.spec_from_file_location(module_name, script_path)
            assert spec and spec.loader, "Could not load script:" + str(script_path)
            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module
            spec.loader.exec_module(module)

            migration_func = getattr(module, "do_migration", None)
            assert migration_func, "Migration script must define do_migration function"

            # todo 这个方法应该是，首先用老的comp_cls，把所有rows读取
            #      然后传给do_migration，返回新的rows，然后再用hmset写回去
            #      或者直接用commit，都不用写专门代码了
            return migration_func
        logger.warning(
            f"  ➖ [💾Redis][{table_ref.comp_name}组件] "
            f"未发现自定义迁移脚本 {script_path}，将使用默认迁移逻辑..."
        )
        return None

    def __init__(self, master: BackendClient):
        """传入master连接的BackendClient实例"""
        self.client = master

    # 检测是否需要维护的方法
    def check_table(self, table_ref: TableReference) -> tuple[str, Any]:
        """
        检查组件表在数据库中的状态。
        此方法检查各个组件表的meta键值。

        Returns
        -------
        status: str
            "not_exists" - 表不存在
            "ok" - 表存在且状态正常
            "cluster_mismatch" - 表存在但cluster_id不匹配
            "schema_mismatch" - 表存在但schema不匹配
        meta: Any
            组件表的meta信息。由各个后端自行定义。直接传给migration_cluster_id和migration_schema
        """
        raise NotImplementedError

    def create_table(self, table_ref: TableReference) -> Any:
        """
        创建组件表。如果已存在，会抛出异常。
        组件表的meta信息。
        """
        raise NotImplementedError

    # 无需drop_table, 此类操作适合人工删除

    def migration_cluster_id(self, table_ref: TableReference, old_meta: Any) -> None:
        """迁移组件表的cluster_id"""
        raise NotImplementedError

    def migration_schema(
        self, table_ref: TableReference, old_meta: Any, force=False
    ) -> None:
        """
        迁移组件表的schema，本方法必须在migration_cluster_id之后执行。
        此方法调用后需要rebuild_index

        本方法将先寻找是否有迁移脚本，如果有则调用脚本进行迁移，否则使用默认迁移逻辑。

        默认迁移逻辑无法处理数据被删除的情况，以及类型转换失败的情况，
        force参数指定是否强制迁移，也就是遇到上述情况直接丢弃数据。
        """
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


# === === === === === === 数据订阅 === === === === === ===


class MQClient:
    """
    连接到消息队列的客户端，每个用户连接一个实例。
    继承此类实现数据库写入通知和消息队列的结合。
    """

    # todo 加入到config中去，设置服务器的通知tick
    UPDATE_FREQUENCY = 10  # 控制客户端所有订阅的数据（如果有变动），每秒更新几次

    async def close(self):
        raise NotImplementedError

    async def pull(self) -> None:
        """
        从消息队列接收一条消息到本地队列，消息内容为channel名。每行数据，每个Index，都是一个channel。
        该channel收到了任何消息都说明有数据更新，所以只需要保存channel名。

        消息存放本地时，需要用时间作为索引，并且忽略重复的消息。存放前先把2分钟前的消息丢弃，防止堆积。
        此方法需要单独的协程反复调用，防止服务器也消息堆积。如果没有消息，则堵塞到永远。
        """
        # 必须合并消息，因为index更新时大都是2条一起的(remove/add)
        raise NotImplementedError

    async def get_message(self) -> set[str]:
        """
        pop并返回之前pull()到本地的消息，只pop收到时间大于1/UPDATE_FREQUENCY的消息。
        留1/UPDATE_FREQUENCY时间是为了消息的合批。

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
