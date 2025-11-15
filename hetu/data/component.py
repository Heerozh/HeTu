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
import os
from collections.abc import Callable
from dataclasses import dataclass
from enum import IntEnum
from typing import Any

import git
import numpy as np

from ..common import Singleton
from ..common import csharp_keyword

logger = logging.getLogger('HeTu.root')


class Permission(IntEnum):
    EVERYBODY = 1
    USER = 2
    OWNER = 3  # 同RLS权限，只是预设了rls_compare为('eq', 'owner', 'caller')
    RLS = 4  # 由rls_compare参数(operator_function, component_property_name, context_property_name)决定具体的rls逻辑
    ADMIN = 999


@dataclass
class Property:
    default: Any                # 属性的默认值
    unique: bool = False        # 是否是字典索引 (此项优先级高于index，查询速度高)
    index: bool | None = None         # 是否是排序索引
    dtype: str | type = None          # 数据类型，最好用np的明确定义


class BaseComponent:
    # -------------------------------定义部分-------------------------------
    properties_ = []        # type:list[tuple[str, Property]] # Ordered属性列表
    component_name_ = None
    namespace_ = None
    permission_ = Permission.USER
    rls_compare_ = None     # type: tuple[Callable[[Any, Any], bool], str, str] | None
    persist_ = True                                     # 只是标记，每次启动时会清空此标记的数据
    readonly_ = False                                   # 只是标记，调用写入会警告
    backend_ = None         # type: str                 # 该Component由哪个后端(数据库)负责储存和查询
    # ------------------------------内部变量-------------------------------
    dtypes = None           # type: np.dtype            # np structured dtype
    default_row = None      # type: np.ndarray          # 默认空数据行
    hosted_ = None          # type: "ComponentTable"    # 该Component运行时被托管的实例
    prop_idx_map_ = None    # type: dict[str, int]      # 属性名->第几个属性 的映射
    dtype_map_ = None       # type: dict[str, np.dtype] # 属性名->dtype的映射
    uniques_ = None         # type: set[str]            # 唯一索引的属性名集合
    indexes_ = None         # type: dict[str, bool]     # 索引名->是否是字符串类型 的映射
    json_ = None            # type: str                 # Component定义的json字符串
    git_hash_ = None        # type: str                 # Component定义的app文件版本
    instances_ = {}         # type: dict[str, dict[str, type[BaseComponent]]] # 该Component的所有副本实例
    master_ = None          # type: type[BaseComponent]  # 该Component的主实例

    @staticmethod
    def make_json(properties, namespace, component_name, permission, persist, readonly,
                  backend, rls_compare):
        return json.dumps({
            'namespace': str(namespace),
            'component_name': str(component_name),
            'permission': permission.name,
            'rls_compare': rls_compare,
            'persist': bool(persist),
            'readonly': bool(readonly),
            'backend': str(backend),
            'properties': {name: {
                'default': prop.default.decode('utf8')
                if type(prop.default) is bytes else prop.default,
                'unique': bool(prop.unique),
                'index': bool(prop.index),
                'dtype': np.dtype(prop.dtype).str,
            } for name, prop in properties.items()},
        })

    @classmethod
    def load_json(cls, json_str: str, suffix: str = "") -> type['BaseComponent']:
        data = json.loads(json_str)
        if suffix:
            data['component_name'] += ":" + suffix
        # 如果是直接调用的BaseComponent.load_json，则创建一个新的类
        if cls is BaseComponent:
            comp = type(data['component_name'], (BaseComponent,), {})
        else:
            comp = cls
        comp.namespace_ = data['namespace']
        comp.component_name_ = data['component_name']
        comp.permission_ = Permission[data['permission']]
        comp.persist_ = data['persist']
        comp.readonly_ = data['readonly']
        comp.backend_ = data['backend']
        comp.properties_ = [(name, Property(**prop)) for name, prop in data['properties'].items()]
        comp.properties_ = sorted(comp.properties_, key=lambda x: x[0])
        comp.json_ = json.dumps(data)  # 重新序列化，保持一致
        comp.instances_ = {}
        # dump rls
        if rls := data['rls_compare']:
            rls = (getattr(operator, rls[0]), *rls[1:])
        comp.rls_compare_ = rls
        # 成员变量初始化
        # 从properties生成np structured dtype，align为True更慢，arm服务器会好些
        comp.dtypes = np.dtype([(name, prop.dtype) for name, prop in comp.properties_], align=False)
        comp.default_row = np.rec.array(
            [tuple([prop.default for name, prop in comp.properties_])],
            dtype=comp.dtypes)
        comp.uniques_ = {name for name, prop in comp.properties_ if prop.unique}
        comp.indexes_ = {name: np.dtype(prop.dtype).type in (np.str_, np.bytes_)
                         for name, prop in comp.properties_ if prop.unique or prop.index}

        comp.prop_idx_map_ = {}
        comp.dtype_map_ = {}
        for name, prop in comp.properties_:
            comp.prop_idx_map_[name] = len(comp.prop_idx_map_)
            comp.dtype_map_[name] = prop.dtype

        # 从json生成的Component没有git版本信息
        comp.git_hash_ = ""
        return comp

    @classmethod
    def new_row(cls, size=1) -> np.record | np.ndarray | np.recarray:
        """返回空数据行，id为0，用于insert"""
        row = cls.default_row[0].copy() if size == 1 else cls.default_row.repeat(size, 0)
        return row

    @classmethod
    def dict_to_row(cls, data: dict):
        """从dict转换为c-struct like的，可直接传给数据库的，行数据"""
        row = cls.new_row()
        for i, (name, _) in enumerate(cls.properties_):
            row[i] = data[name]
        return row

    @classmethod
    def row_to_dict(cls, data: np.record | np.ndarray | np.recarray | dict):
        """从c-struct like的行数据转换为typed dict"""
        if type(data) is dict:
            return data
        return dict(zip(data.dtype.names, data.item()))

    @classmethod
    def duplicate(cls, namespace: str, suffix: str) -> type['BaseComponent']:
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
    def get_duplicates(cls, namespace: str) -> dict[str, type['BaseComponent']]:
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
        self._components = {}

    def clear_(self):
        self._components.clear()

    def get_all(self) -> list[type[BaseComponent]]:
        """返回所有Component类，但一般不使用此方法，而是用SystemClusters().get_clusters()获取用到的表"""
        return [comp for comps in self._components.values() for comp in comps.values()]

    def get_component(self, namespace: str, component_name: str) -> type[BaseComponent]:
        return self._components[namespace][component_name]

    def add_component(self, namespace: str, component_cls: type[BaseComponent],
                      force: bool = False):
        comp_map = self._components.setdefault(namespace, dict())
        if not force:
            assert component_cls.component_name_ not in comp_map, "Component重复定义"
        comp_map[component_cls.component_name_] = component_cls


def define_component(
    _cls=None,
    /,
    *,
    namespace: str = "default",
    force: bool = False,
    permission=Permission.USER,
    persist=True,
    readonly=False,
    backend: str = "default",
    rls_compare: tuple[str, str, str] = None,
):
    """
    定义Component组件（表）的数据结构

    Examples
    --------
    >>> from hetu.data import BaseComponent, Property, define_component, Permission
    >>> @define_component(namespace="ssw")
    ... class Position(BaseComponent):
    ...     x: np.float32 = Property(default=0)
    ...     y: np.float32 = Property(default=0)
    ...     owner: np.int64 = Property(default=0, unique=True)
    ...     name: '<U8' = Property(default="12345678")

    Parameters
    ----------
    namespace: str
        你的项目名。不同于System，Component的Namespace主要用在数据库表名，可以任意起名
    persist: bool
        表示是否持久化，设为False时，每次启动你的数据会被清除，请小心。
    readonly: bool
        是否只读Component，只读Component不会被加事务保护，增加并行性。
    backend: str
        指定Component后端，对应配置文件中的backend_name。默认为default，对应配置文件中第一个
    permission: Permission
        设置读取权限，只对hetu client sdk连接起作用，服务器端代码不受限制。

        - everybody: 任何客户端连接都可以读，适合读一些服务器状态类的数据，如在线人数
        - user: 只有已登录的客户端都连接可以读
        - admin: 只有管理员权限客户端连接可以读
        - owner: 只能读取到owner属性值==登录的用户id（`ctx.caller`）的行，未登录的客户端无法读取。
                 此权限等同rls权限，且`rls_compare=('eq', 'owner', 'caller')`
        - rls: 行级权限，需要配合rls_compare参数使用，定义具体的行级权限逻辑
    rls_compare:
        当permission设置为RLS(行级权限)时，定义行级安全的比较函数和属性名，格式为(operator方法名, 表属性名, context属性名)，
        只有operator.operator方法名(row.表属性名, ctx.context属性名)返回True时允许读取此行。如果属性不存在，按nan处理（无法和任何值比较）。
    force: bool
        强制覆盖同名Component，单元测试用。
    _cls: class
        当所有参数使用默认值时，可以直接无参数使用，如：

        >>> @define_component
        ... class Position(BaseComponent):
        ...    ...

    Notes
    -----
    `Property(default, unique, index, dtype)` 是Component的属性定义，可定义默认值和数据类型。
        - `index` 表示此属性开启索引；
        - `unique` 表示属性值必须唯一，启动此项默认会同时打开index。

    .. warning:: ⚠️ 警告：索引会降低全表性能，请控制数量。其中unique索引降低的更多。

    属性值的类型由type hint决定（如 `: np.float32`），请使用长度明确的np类型。
    字符串类型格式为"<U8"，U是Unicode，8表示长度，<表示little-endian。
    不想看到"<U8"在IDE里标红语法错误的话，可用 `name = Property(dtype='<U8')` 方式。

    每个Component表都有个默认的主键`id: np.int64 = Property(default=0, unique=True)`，
    会自行自增无法修改。
    """

    def warp(cls):
        # class名合法性检测
        if csharp_keyword.iskeyword(cls.__name__):
            raise ValueError(f"组件名({cls.__name__})是C#关键字，请refactor。")
        # 获取class的property成员列表
        cls_annotations = cls.__dict__.get('__annotations__', {})
        properties = {}
        # 从class读取并删除该成员
        for _name, dtype in cls_annotations.items():
            prop = getattr(cls, _name, None)
            if isinstance(prop, Property):
                if prop.dtype is None:
                    prop.dtype = dtype
                if keyword.iskeyword(_name) or _name in ['bool', 'int', 'float', 'str']:
                    raise ValueError(f"{cls.__name__}.{_name}属性定义出错，属性名不能是Python关键字。")
                if csharp_keyword.iskeyword(_name):
                    raise ValueError(f"{cls.__name__}.{_name}属性定义出错，属性名不能是C#关键字。")
                assert np.dtype(prop.dtype).itemsize > 0, \
                    f"{cls.__name__}.{_name}属性的dtype不能为0长度。str类型请用'<U8'方式定义"
                assert np.dtype(prop.dtype).type is not np.void, \
                    f"{cls.__name__}.{_name}属性的dtype不支持void类型"
                # bool类型在一些后端数据库中不支持，强制转换为int8
                if prop.dtype is bool or prop.dtype is np.bool_ or prop.dtype == '?':
                    prop.dtype = np.int8
                if prop.unique:
                    if prop.index is False:
                        logger.warning(f"⚠️ [🛠️Define] "
                                       f"{cls.__name__}.{_name}属性设置为unique时，"
                                       f"index不能设置为False。")
                    prop.index = True
                if prop.index is None:
                    prop.index = False
                assert prop.default is not None, \
                    (f"{cls.__name__}.{_name}默认值不能为None。所有属性都要有默认值，"
                     f"因为数据接口统一用c like struct实现，强类型struct不接受NULL/None值。")
                can_cast = np.can_cast(np.min_scalar_type(prop.default), prop.dtype)
                if not can_cast and not (type(prop.default) is str or type(prop.default) is bytes):
                    # min_scalar_type(1)会判断为uint8, prop.dtype为int8时判断会失败,所以要转为负数再判断一次
                    default_value = -prop.default if prop.default != 0 else -1
                    can_cast = np.can_cast(np.min_scalar_type(default_value), prop.dtype)
                assert can_cast, (f"{cls.__name__}.{_name}的default值："
                                  f"{type(prop.default).__name__}({prop.default})"
                                  f"和属性dtype({prop.dtype})不匹配")
                properties[_name] = prop
            else:
                raise AssertionError(f"{cls.__name__}.{_name}不是Property类型")
            delattr(cls, _name)
        # Property类型强制要求定义type hint
        for name, value in cls.__dict__.items():
            if isinstance(value, Property) and name not in properties:
                raise ValueError(f"{cls.__name__}.{name}属性未定义type hint。请使用以下形式，"
                                 f"{name}: type = Property(...)")

        assert properties, f"{cls.__name__}至少要有1个Property成员"

        # 添加id主键，如果冲突，报错
        assert 'id' not in properties, f"{cls.__name__}.id是保留的内置主键，外部不能重定义"
        properties['id'] = Property(0, True, True, np.int64)  # 必备索引，只进行unique索引为了基础性能

        # 检查class必须继承于BaseComponent
        assert issubclass(cls, BaseComponent), f"{cls.__name__}必须继承于BaseComponent"

        # 检查OWNER权限必须有owner属性
        nonlocal rls_compare
        if permission == Permission.OWNER:
            assert rls_compare is None, \
                f"{cls.__name__}权限设置为OWNER时，不能再设置rls_compare参数"
            # 修改闭包外的变量rls_compare
            rls_compare = ('eq', 'owner', 'caller')
            assert 'owner' in properties, \
                f"{cls.__name__}权限设置为OWNER时，必须有owner属性，该属性表明此条数据属于哪个用户"
            # if not properties['owner'].unique:
            #     logger.warning(f"⚠️ [🛠️Define] {cls.__name__}.owner属性不是unique唯一，"
            #                    f"你确定正确么？")
            assert np.issubdtype(properties['owner'].dtype, np.number), \
                f"{cls.__name__}的owner属性必需是numeric数字(int, np.int64, ...)类型"

        # 检查RLS定义
        if permission == Permission.RLS:
            assert rls_compare is not None, \
                f"{cls.__name__}权限设置为RLS时，必须设置rls_compare参数，定义行级权限逻辑"
            assert all(type(e) is str for e in rls_compare), \
                f"{cls.__name__}.rls_compare参数必须全部是字符串类型"
            assert len(rls_compare) == 3, \
                f"{cls.__name__}.rls_compare参数必须只有3个元素)"

            assert hasattr(operator, rls_compare[0]), \
                f"{cls.__name__}权限设置为RLS: {rls_compare}，但operator模块没有{rls_compare[0]}方法"

            assert rls_compare[1] in properties, \
                f"{cls.__name__}权限设置为RLS: {rls_compare}，但表没有定义{rls_compare[1]}属性"

        # 生成json格式，并通过json加载到class中
        json_str = BaseComponent.make_json(properties, namespace, cls.__name__, permission,
                                           persist, readonly, backend, rls_compare)
        cls.load_json(json_str)

        # 保存app文件的git版本信息，目前无作用，主要用于以后支持自动迁移
        caller = inspect.stack()[1]
        try:
            repo = git.Repo(caller.filename, search_parent_directories=True)
            tree = repo.head.commit.tree
            relpath = os.path.relpath(caller.filename, repo.working_dir).replace(os.sep, '/')
            blob = tree[relpath]
            sha = blob.hexsha
            cls.git_hash_ = sha
        except (KeyError, git.exc.InvalidGitRepositoryError, ValueError):
            lib_path = os.path.abspath(__file__ + '/../../')
            # if lib_path not in caller.filename: 等自动迁移完成后再打开
            #     warnings.warn(f"⚠️ [🛠️Define] {caller.filename}文件不在git版本控制中，"
            #                   f"将无法用于根据文件版本变化自动迁移{cls.__name__}组件的功能。")
            cls.git_hash_ = 'untracked'

        # 把class加入到总集中
        ComponentDefines().add_component(namespace, cls, force)
        return cls

    if _cls is None:
        return warp
    else:
        return warp(_cls)
