"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import inspect
import json
import keyword
import logging
import operator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, cast, overload

import numpy as np

if TYPE_CHECKING:
    from .backend.table import Table

from ..common import Singleton, csharp_keyword
from ..common.permission import Permission
from ..common.snowflake_id import SnowflakeID

logger = logging.getLogger("HeTu.root")
SNOWFLAKE_ID = SnowflakeID()


@dataclass
class Property:
    default: Any  # 属性的默认值
    unique: bool = False  # 是否是字典索引 (此项优先级高于index，查询速度高)
    index: bool = False  # 是否是排序索引
    dtype: str | type = ""  # 数据类型，最好用np的明确定义
    # nullable说明：
    # Component表现为c-struct like的数据，因此值不能为null，也无法判断值是否有被设置过
    # 因此nullable是无法实现的。hetu本身的理念是细化Component，所以如果有nullable需求的
    # 列，可以单独拆分成一个Component来存储，然后用owner来关联。


# 辅助函数，过滤类型检查器报错
def property_field(
    default: Any,
    unique: bool = False,
    index: bool | None = None,
    dtype: str | type = "",
) -> Any:
    if index is None:
        index = unique
    return Property(default=default, unique=unique, index=index, dtype=dtype)


class BaseComponent:
    # -------------------------------定义部分-------------------------------
    properties_: list[tuple[str, Property]] = []  # Ordered属性列表
    name_: str
    namespace_: str
    permission_: Permission = Permission.USER
    rls_compare_: tuple[Callable[[Any, Any], bool], str, str] | None = None
    volatile_: bool = False  # 易失标记，此标记的Component每次维护会清空数据
    readonly_: bool = False  # 只读标记，暂无作用
    backend_: str  # 自定义该Component由哪个后端(数据库)负责储存和查询
    # ------------------------------内部变量-------------------------------
    dtypes: np.dtype  # np structured dtype
    default_row: np.recarray  # 默认空数据行
    hosted_: Table | None = None  # 启动后comp_mgr分配的数据库todo 不建议使用去掉
    prop_idx_map_: dict[str, int]  # 属性名->第几个属性（矩阵下标）的映射
    dtype_map_: dict[str, np.dtype]  # 属性名->dtype的映射
    uniques_: set[str]  # 唯一索引的属性名集合
    indexes_: dict[str, bool]  # 索引名->是否是字符串类型 的映射
    json_: str  # Component定义的json字符串
    instances_: dict[str, dict[str, type[BaseComponent]]] = {}  # 所有副本实例
    master_: type[BaseComponent] | None = None  # 该Component的主实例

    @staticmethod
    def make_json(
        properties,
        namespace,
        name,
        permission,
        volatile,
        readonly,
        backend,
        rls_compare,
    ):
        return json.dumps(
            {
                "namespace": str(namespace),
                "name": str(name),
                "permission": permission.name,
                "rls_compare": rls_compare,
                "volatile": bool(volatile),
                "readonly": bool(readonly),
                "backend": str(backend),
                "properties": {
                    name: {
                        "default": (
                            prop.default.decode("utf8")
                            if type(prop.default) is bytes
                            else prop.default
                        ),
                        "unique": bool(prop.unique),
                        "index": bool(prop.index),
                        "dtype": np.dtype(prop.dtype).str,
                    }
                    for name, prop in properties.items()
                },
            }
        )

    @classmethod
    def load_json(cls, json_str: str, suffix: str = "") -> type[BaseComponent]:
        data = json.loads(json_str)
        if suffix:
            data["name"] += ":" + suffix
        # 如果是直接调用的BaseComponent.load_json，则创建一个新的类
        if cls is BaseComponent:
            comp: type[BaseComponent] = type(data["name"], (BaseComponent,), {})
        else:
            comp = cls
        comp.namespace_ = str(data["namespace"])
        comp.name_ = str(data["name"])
        comp.permission_ = Permission[data["permission"]]
        comp.volatile_ = bool(data["volatile"])
        comp.readonly_ = bool(data["readonly"])
        comp.backend_ = str(data["backend"])
        comp.properties_ = [
            (name, Property(**prop)) for name, prop in data["properties"].items()
        ]
        comp.properties_ = sorted(comp.properties_, key=lambda x: x[0])
        comp.json_ = json.dumps(data)  # 重新序列化，保持一致
        comp.instances_ = {}
        # dump rls
        if rls := data["rls_compare"]:
            rls = (getattr(operator, rls[0]), *rls[1:])
        comp.rls_compare_ = rls
        # 成员变量初始化
        # 从properties生成np structured dtype，align为True更慢，arm服务器会好些
        comp.dtypes = np.dtype(
            [(name, prop.dtype) for name, prop in comp.properties_], align=False
        )
        comp.default_row = np.rec.array(
            [tuple([prop.default for name, prop in comp.properties_])],
            dtype=comp.dtypes,
        )
        comp.uniques_ = {name for name, prop in comp.properties_ if prop.unique}
        comp.indexes_ = {
            name: np.dtype(prop.dtype).type in (np.str_, np.bytes_)
            for name, prop in comp.properties_
            if prop.unique or prop.index
        }

        comp.prop_idx_map_ = {}
        comp.dtype_map_ = {}
        for name, prop in comp.properties_:
            comp.prop_idx_map_[name] = len(comp.prop_idx_map_)
            comp.dtype_map_[name] = np.dtype(prop.dtype)

        return comp

    @classmethod
    def new_row(cls, id_=None) -> np.record:
        """返回空数据行，id生成uuid，用于insert"""
        row = cast(np.record, cls.default_row[0].copy())
        if id_ is not None:
            row.id = id_
        else:
            row.id = SNOWFLAKE_ID.next_id()
        return row

    @classmethod
    def new_rows(cls, size) -> np.recarray:
        """返回空数据行，id生成uuid，用于insert"""
        rows = cls.default_row.copy() if size == 1 else cls.default_row.repeat(size, 0)
        for i in range(size):
            rows[i].id = SNOWFLAKE_ID.next_id()
        return cast(np.recarray, rows)

    @classmethod
    def dict_to_struct(cls, data: dict) -> np.record:
        """从dict转换为c-struct like的，可直接传给数据库的，行数据"""
        row = cls.new_row()
        for i, (name, _) in enumerate(cls.properties_):
            row[i] = data[name]
        return row

    @classmethod
    def struct_to_dict(cls, data: np.record) -> dict[str, Any]:
        """从c-struct like的行数据转换为typed dict"""
        assert data.dtype.names
        return dict(zip(data.dtype.names, data.item()))

    @classmethod
    def duplicate(cls, namespace: str, suffix: str) -> type[BaseComponent]:
        """
        复制一个新的副本组件。拥有相同的定义，但使用suffix结尾的新的名字。
        注意：只能在define阶段使用
        """
        if namespace == cls.namespace_ and not suffix:
            return cls

        instances = cls.instances_.setdefault(namespace, {})
        if suffix in instances:
            return instances[suffix]

        new_cls = BaseComponent.load_json(cls.json_, suffix)
        instances[suffix] = new_cls
        new_cls.master_ = cls
        return new_cls

    @classmethod
    def get_duplicates(cls, namespace: str) -> dict[str, type[BaseComponent]]:
        """获取此Component在指定namespace下的所有副本实例"""
        return cls.instances_.get(namespace, {})

    @classmethod
    def is_rls(cls) -> bool:
        """此Component是否是RLS权限"""
        return cls.permission_ in (Permission.OWNER, Permission.RLS)


class ComponentDefines(metaclass=Singleton):
    """
    储存所有定义了的Component
    """

    def __init__(self):
        self._components: dict[str, dict[str, type[BaseComponent]]] = {}

    def clear_(self):
        cores = self._components.get("core", {})
        self._components.clear()
        # 不清除core组件，因为这些都是系统组件
        self._components["core"] = cores

    def get_all(self, namespace: str | None = None) -> list[type[BaseComponent]]:
        """返回所有Component类，但一般不使用此方法，而是用SystemClusters().get_clusters()获取用到的表"""
        if namespace:
            return list(self._components.get(namespace, {}).values())
        else:
            return [
                comp for comps in self._components.values() for comp in comps.values()
            ]

    def get_component(self, namespace: str, component_name: str) -> type[BaseComponent]:
        return self._components[namespace][component_name]

    def add_component(
        self, namespace: str, component_cls: type[BaseComponent], force: bool = False
    ):
        comp_map = self._components.setdefault(namespace, dict())
        if not force:
            assert component_cls.name_ not in comp_map, "Component重复定义"
        comp_map[component_cls.name_] = component_cls


@overload
def define_component(
    cls: type[BaseComponent],
    /,
    *,
    namespace: str = "default",
    force: bool = False,
    permission: Permission = Permission.USER,
    volatile: bool = False,
    # readonly: bool = False,
    backend: str = "default",
    rls_compare: tuple[str, str, str] | None = None,
) -> type[BaseComponent]: ...
@overload
def define_component(
    cls: None = None,
    /,
    *,
    namespace: str = "default",
    force: bool = False,
    permission: Permission = Permission.USER,
    volatile: bool = False,
    # readonly: bool = False,
    backend: str = "default",
    rls_compare: tuple[str, str, str] | None = None,
) -> Callable[[type[BaseComponent]], type[BaseComponent]]: ...
def define_component(
    _cls=None,
    /,
    *,
    namespace: str = "default",
    force: bool = False,
    permission=Permission.USER,
    volatile=False,
    # readonly=False,
    backend: str = "default",
    rls_compare: tuple[str, str, str] | None = None,
) -> Callable[[type[BaseComponent]], type[BaseComponent]] | type[BaseComponent]:
    """
    定义Component组件的schema模型

    Examples
    --------
    >>> import hetu
    >>> import numpy as np
    >>> @hetu.define_component(namespace="ssw")
    ... class Position(hetu.BaseComponent):
    ...     x: np.float32 = hetu.property_field(default=0)
    ...     y: np.float32 = hetu.property_field(default=0)
    ...     owner: np.int64 = hetu.property_field(default=0, unique=True)
    ...     name: str = hetu.property_field(default="12345678", dtype="U8")

    Parameters
    ----------
    namespace: str
        你的项目名，主要为了区分不同项目的同名Component。
        不同于System，Component的Namespace可以随意填写，只要被System引用了都会加载。
        如果为"core"，则此Component即使没被任何System引用，也会被加载。
    volatile: bool
        是否是易失表，设为True时，每次维护你的数据会被清除，请小心。
    backend: str
        指定Component后端，对应配置文件中BACKENDS的字典key。默认为default，对应BACKENDS配置中第一个
    permission: Permission
        设置读取权限，只对hetu client sdk连接起作用，服务器端代码不受限制。

        - everybody: 任何客户端连接都可以读，适合读一些服务器状态类的数据，如在线人数
        - user: 只有已登录的客户端都连接可以读
        - admin: 只有管理员权限客户端连接可以读
        - owner: 只能读取到owner属性值==登录的用户id（`ctx.caller`）的行，未登录的客户端无法读取。
                 此权限等同rls权限，且`rls_compare=('eq', 'owner', 'caller')`
        - rls: 行级权限，需要配合`rls_compare`参数使用，定义具体的行级权限逻辑

    rls_compare: tuple[str, str, str] | None
        当permission设置为RLS(行级权限)时，定义行级安全的比较函数和属性名。

        - rls_compare[0]: operator比较方法字符串，如"lt", "gt"等。参考python operator标准运算符函数模块
        - rls_compare[1]: 组件属性名字符串
        - rls_compare[2]: Context属性名字符串，或Context.user_data的key名

        只有operator比较后返回True时允许读取此行。如果属性不存在，按nan处理（无法和任何值比较）。
    force: bool
        强制覆盖同名Component，单元测试用。
    _cls: class
        当所有参数使用默认值时，可以直接无参数使用，如::

            @define_component
            class Position(BaseComponent):
                ...

    Notes
    -----
    `property_field(default, unique, index, dtype)` 是Component的属性定义，可定义默认值和数据类型。
        - `index` 表示此属性开启索引；
        - `unique` 表示属性值必须唯一，启动此项默认会同时打开index。

    .. warning:: ⚠️ 警告：索引会降低全表性能，请控制数量。其中unique索引降低的更多。

    属性值的类型由type hint决定（如 `: np.float32`），请使用长度明确的np类型。
    字符串类型格式为"<U8"，U是Unicode，8表示长度，<表示little-endian。
    不想看到"<U8"在IDE里标红语法错误的话，可用 `name: str = property_field(dtype='<U8')` 方式。

    每个Component表都有个默认的主键`id: np.int64 = property_field(default=雪花uuid, unique=True)`，
    是一个uuid，无法修改。
    """

    def _normalize_prop(cname: str, fname: str, anno_type, prop: Property):
        # 如果未设置dtype，则用type hint
        if prop.dtype == "":
            prop.dtype = anno_type
        # 判断名称合法性
        if keyword.iskeyword(fname) or fname in ["bool", "int", "float", "str"]:
            raise ValueError(f"{cname}.{fname}属性定义出错，属性名不能是Python关键字。")
        if csharp_keyword.iskeyword(fname):
            raise ValueError(f"{cname}.{fname}属性定义出错，属性名不能是C#关键字。")
        # 判断类型，以及长度合法性
        assert np.dtype(prop.dtype).itemsize > 0, (
            f"{cname}.{fname}属性的dtype不能为0长度。str类型请用'<U8'方式定义"
        )
        assert np.dtype(prop.dtype).type is not np.void, (
            f"{cname}.{fname}属性的dtype不支持void类型"
        )
        # bool类型在一些后端数据库中不支持，强制转换为int8
        if prop.dtype is bool or prop.dtype is np.bool_ or prop.dtype == "?":
            prop.dtype = np.int8
        # 开启unique时，强制index为True
        if prop.unique:
            if not prop.index:
                logger.warning(
                    f"⚠️ [🛠️Define] {cname}.{fname}属性设置为unique时，"
                    f"index不能设置为False。"
                )
            prop.index = True
        # 判断default值必须设置
        assert prop.default is not None, (
            f"{cname}.{fname}默认值不能为None。所有属性都要有默认值，"
            f"因为数据接口统一用c like struct实现，强类型struct不接受NULL/None值。"
        )
        # 判断default值和dtype匹配，包括长度能安全转换
        can_cast = np.can_cast(np.min_scalar_type(prop.default), prop.dtype)
        non_numeric = (str, bytes)
        if not can_cast and type(prop.default) not in non_numeric:
            # min_scalar_type(1)会判断为uint8, prop.dtype为int8时判断会失败,所以要转为负数再判断一次
            default_value = -prop.default if prop.default != 0 else -1
            can_cast = np.can_cast(np.min_scalar_type(default_value), prop.dtype)
        assert can_cast, (
            f"{cname}.{fname}的default值："
            f"{type(prop.default).__name__}({prop.default})"
            f"和属性dtype({prop.dtype})不匹配"
        )

    def _rls_define_check(cname, properties):
        if permission == Permission.OWNER:
            assert rls_compare is None, f"{cname}权限为OWNER时，不能设置rls_compare参数"
            assert "owner" in properties, f"{cname}权限为OWNER时，必须有owner属性"
            # 取消, owner有很多地方需要不是唯一，比如每行一个道具的情况
            # if not properties['owner'].unique:
            #     logger.warning(f"⚠️ [🛠️Define] {cls.__name__}.owner属性不是unique唯一，"
            #                    f"你确定正确么？")
            assert np.issubdtype(properties["owner"].dtype, np.number), (
                f"{cname}的owner属性必需是numeric数字(int, np.int64, ...)类型"
            )

        # 检查RLS定义
        if permission == Permission.RLS:
            assert rls_compare is not None, (
                f"{cname}权限为RLS时，必须通过rls_compare参数定义行级权限逻辑"
            )
            assert all(type(e) is str for e in rls_compare), (
                f"{cname}.rls_compare参数必须全部是字符串类型"
            )
            assert len(rls_compare) == 3, f"{cname}.rls_compare参数必须只有3个元素)"

            assert hasattr(operator, rls_compare[0]), (
                f"{cname}权限为RLS: {rls_compare}，但operator模块没有{rls_compare[0]}方法"
            )

            assert rls_compare[1] in properties, (
                f"{cname}权限为RLS: {rls_compare}，但表没有定义{rls_compare[1]}属性"
            )

    def warp(cls):
        # class名合法性检测
        if csharp_keyword.iskeyword(cls.__name__):
            raise ValueError(f"组件名({cls.__name__})是C#关键字，请refactor。")
        # 获取class的property成员列表
        cls_annotations = inspect.get_annotations(cls)
        properties = {}
        # 从class读取并删除该成员
        for _name, anno_type in cls_annotations.items():
            prop = getattr(cls, _name, None)
            if isinstance(prop, Property):
                _normalize_prop(cls.__name__, _name, anno_type, prop)
                properties[_name] = prop
            else:
                raise AssertionError(f"{cls.__name__}.{_name}不是Property类型")
            delattr(cls, _name)
        # Property类型强制要求定义type hint
        for name, value in cls.__dict__.items():
            if isinstance(value, Property) and name not in properties:
                raise ValueError(
                    f"{cls.__name__}.{name}属性未定义type hint。请使用以下形式，"
                    f"{name}: type = property_field(...)"
                )

        assert properties, f"{cls.__name__}至少要有1个Property成员"

        # 添加保留键，如果冲突，报错
        assert "id" not in properties, (
            f"{cls.__name__}.id是保留的内置主键，外部不能重定义"
        )
        assert "_version" not in properties, (
            f"{cls.__name__}._version是保留的内置主键，外部不能重定义"
        )
        # 必备索引，调用new_row时会用雪花算法生成uuid，该属性无法修改。加unique索引防止意外
        properties["id"] = Property(0, True, True, np.int64)
        # 增加version属性，该属性只读（只能lua修改）
        properties["_version"] = Property(0, False, False, np.int32)

        # 检查class必须继承于BaseComponent
        assert issubclass(cls, BaseComponent), f"{cls.__name__}必须继承于BaseComponent"

        # 检查RLS权限各种定义符合要求
        _rls_define_check(cls.__name__, properties)
        nonlocal rls_compare
        if permission == Permission.OWNER:
            # 修改闭包外的变量rls_compare
            rls_compare = ("eq", "owner", "caller")

        # 生成json格式，并通过json加载到class中
        json_str = BaseComponent.make_json(
            properties,
            namespace,
            cls.__name__,
            permission,
            volatile,
            False,
            backend,
            rls_compare,
        )
        cls.load_json(json_str)

        # 把class加入到总集中
        ComponentDefines().add_component(namespace, cls, force)
        return cls

    if _cls is None:
        return warp
    else:
        return warp(_cls)
