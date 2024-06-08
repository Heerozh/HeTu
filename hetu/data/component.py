"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""
from ..common import Singleton
from dataclasses import dataclass
from enum import Enum
import numpy as np
import logging
logger = logging.getLogger('HeTu')


class Permission(Enum):
    EVERYBODY = 1
    USER = 2
    OWNER = 3
    ADMIN = 999


@dataclass
class Property:
    default: any                # 属性的默认值
    unique: bool = False        # 是否是字典索引 (此项优先级高于index，查询速度高)
    index: bool = False         # 是否是排序索引
    dtype: type = None          # 数据类型，最好用np的明确定义


class BaseComponent:
    # 表的属性值
    properties_ = []
    dtypes = None
    components_name_ = None
    namespace_ = None
    permission_ = Permission.USER
    persist_ = True
    default_row = None      # type: np.ndarray
    readonly_ = False
    backend_ = None         # type: str
    hosted_ = None          # type: ComponentTable
    prop_idx_map_ = None    # type: dict[str, int]
    dtype_map_ = None       # type: dict[str, np.dtype]
    uniques_ = None         # type: set[str]

    @classmethod
    def new_row(cls, size=1):
        """返回空数据行， id为0时，insert会自动赋予id"""
        row = cls.default_row[0].copy() if size == 1 else np.repeat(cls.default_row, size, 0)
        return row


class ComponentTable:
    """
    Component的数据表操作接口，和后端通讯并处理事务。
    """

    def __init__(self, component_cls: type[BaseComponent], config: dict):
        self.component_cls = component_cls
        self.config = config

    def select(self, value, where: str = None):
        raise NotImplementedError

    def select_or_create(self, value, where: str = None):
        uniques = self.component_cls.uniques_ - {'id', where}
        assert len(uniques) == 0, "有多个Unique属性的Component不能使用select_or_create"

        rtn = self.select(value, where)
        if rtn is None:
            rtn = self.component_cls.new_row()
            rtn[where] = value
            self.insert(rtn)
        return rtn

    def query(self, index_name: str, left, right=None, limit=10, desc=False):
        raise NotImplementedError

    def update(self, row_id: int, row):
        raise NotImplementedError

    def insert(self, row):
        raise NotImplementedError

    def delete(self, row_id: int):
        raise NotImplementedError

    def is_exist(self, value, where: str = None):
        raise NotImplementedError


class ComponentDefines(metaclass=Singleton):
    """
    储存所有定义了的Component
    """

    def __init__(self):
        self._components = {}

    def clear_(self):
        self._components.clear()

    def get_component(self, namespace: str, component_name: str) -> type[BaseComponent]:
        return self._components[namespace][component_name]

    def add_component(self, namespace: str, component_cls: type[BaseComponent],
                      force: bool = False):
        comp_map = self._components.setdefault(namespace, dict())
        if not force:
            assert component_cls.components_name_ not in comp_map, "Component重复定义"
        comp_map[component_cls.components_name_] = component_cls


def define_component(_cls=None,  /, *, namespace: str = "default", force: bool = False,
                     permission=Permission.USER, persist=True, readonly=False,
                     backend: str = 'Redis'):
    """
    定义组件（表）的数据结构
    格式：
    @define_component(namespace="ssw")
    class Position(BaseComponent):
        x: np.float32 = Property(default=0)
        y: np.float32 = Property(default=0)
        owner: np.int64 = Property(default=0, unique=True)

    :param namespace: 是你的项目名，一个网络地址只能启动一个namespace。
    :param persist: 表示是否持久化。
    :param readonly: 只读表，只读表不会被加事务保护，增加并行性。
    :param backend: 指定Component后端，对应配置文件中的db_name。默认为Redis
    :param permission: 设置读取权限，只对游戏客户端的读取查询调用起作用。
        - everybody: 任何人都可以读，适合读一些服务器状态类的数据，如在线人数
        - user: 登录用户都可以读
        - admin: 只有管理员可以读
        - owner: 只有表的owner属性值==登录的用户id（`ctx.caller`）可以读，如果无owner值则认为不可读
    :param force: 强制覆盖同名Component，否则会报错。
    :param _cls: 按@define_component()方式调用时，不需要传入_cls参数。

    `Property(default, unique, index, dtype)` 是Component的属性定义，可定义默认值和数据类型。
        `index`表示此属性开启索引；
        `unique`表示属性值必须唯一，索引性能更高，启动此项默认会同时打开index。

    * ⚠️ 警告：索引会降低全表性能，请控制数量。

    属性值的类型由type hint决定（如`: np.float32`），请使用长度明确的np类型。
    字符串类型格式为"<U8"，U是Unicode，8表示长度。
    不想看到"<U8"在IDE里标红语法错误的话，可用`name = Property(dtype='<U8')`方式。

    每个Component表都有个默认的主键`id: np.int64 = Property(default=0, unique=True)`，
    会自行自增无法修改。
    """
    def warp(cls):
        # 获取class的property成员列表
        cls_annotations = cls.__dict__.get('__annotations__', {})
        properties = {}

        # 从class读取并删除该成员
        for name, dtype in cls_annotations.items():
            prop = getattr(cls, name, None)
            if isinstance(prop, Property):
                if prop.dtype is None:
                    prop.dtype = dtype
                if np.dtype(prop.dtype).itemsize == 0:
                    raise AssertionError(f"{cls.__name__}.{name}属性的dtype不能为0长度。"
                                         f"str类型请用'<U8'方式定义")
                # bool类型在一些后端数据库中不支持，强制转换为int8
                if prop.dtype is bool or prop.dtype is np.bool_ or prop.dtype == '?':
                    prop.dtype = np.int8
                if prop.unique:
                    prop.index = True
                assert prop.default is not None, \
                    (f"{cls.__name__}.{name}默认值不能为None。所有属性都要有默认值，"
                     f"因为数据接口统一用c like struct实现，强类型struct不接受NULL/None值。")
                if type(prop.default) is str:
                    can_cast = np.can_cast(np.min_scalar_type(prop.default), prop.dtype)
                else:
                    can_cast = np.can_cast(prop.default, prop.dtype)
                assert can_cast, (f"{cls.__name__}.{name}属性的默认值({prop.default})"
                                  f"类型和dtype({prop.dtype})不匹配")
                properties[name] = prop
            delattr(cls, name)

        assert properties, f"{cls.__name__}至少要有1个Property成员"

        # 添加id主键，如果冲突，报错
        assert 'id' not in properties, f"{cls.__name__}.id是保留的内置主键，外部不能重定义"
        properties['id'] = Property(0, True, True, np.int64)  # 必备索引，只进行unique索引为了基础性能

        # 检查class必须继承于BaseComponent
        assert issubclass(cls, BaseComponent), f"{cls.__name__}必须继承于BaseComponent"

        # 成员变量初始化
        cls.properties_ = sorted(list(properties.items()), key=lambda x: x[0])
        cls.components_name_ = cls.__name__
        cls.permission_ = permission
        cls.namespace_ = namespace
        cls.persist_ = persist
        cls.readonly_ = readonly
        cls.backend_ = backend
        # 从properties生成np structured dtype，align为True更慢，arm服务器会好些
        cls.dtypes = np.dtype([(name, prop.dtype) for name, prop in cls.properties_], align=False)
        cls.default_row = np.rec.array(
            [tuple([prop.default for name, prop in cls.properties_])],
            dtype=cls.dtypes)  # or np.object_
        cls.uniques_ = {name for name, prop in cls.properties_ if prop.unique}

        cls.prop_idx_map_ = {}
        cls.dtype_map_ = {}
        for name, prop in cls.properties_:
            cls.prop_idx_map_[name] = len(cls.prop_idx_map_)
            cls.dtype_map_[name] = prop.dtype

        # 把class加入到总集中
        ComponentDefines().add_component(namespace, cls, force)
        return cls

    if _cls is None:
        return warp
    else:
        return warp(_cls)
