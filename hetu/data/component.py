"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""
from ..common import Singleton
from dataclasses import dataclass
from enum import Enum
import json
import warnings
import inspect
import git
import os
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
    dtype: str | type = None          # 数据类型，最好用np的明确定义


class BaseComponent:
    # -------------------------------定义部分-------------------------------
    properties_ = []                                    # Component的属性们
    component_name_ = None
    namespace_ = None
    permission_ = Permission.USER
    persist_ = True                                     # 只是标记，每次启动时会清空此标记的数据
    readonly_ = False                                   # 只是标记，调用写入会警告
    backend_ = None         # type: str                 # 该Component由哪个后端(数据库)负责储存和查询
    # ------------------------------内部变量-------------------------------
    dtypes = None           # type: np.dtype            # np structured dtype
    default_row = None      # type: np.ndarray          # 默认空数据行
    hosted_ = None          # type: "ComponentBackend"  # 该Component运行时被托管的后端实例
    prop_idx_map_ = None    # type: dict[str, int]      # 属性名->第几个属性 的映射
    dtype_map_ = None       # type: dict[str, np.dtype] # 属性名->dtype的映射
    uniques_ = None         # type: set[str]            # 唯一索引的属性名集合
    indexes_ = None         # type: dict[str, bool]     # 索引名->是否是字符串类型 的映射
    json_ = None            # type: str                 # Component定义的json字符串
    git_hash_ = None        # type: str                 # Component定义的app文件版本

    @staticmethod
    def make_json(properties, namespace, component_name, permission, persist, readonly,
                  backend):
        return json.dumps({
            'namespace': str(namespace),
            'component_name': str(component_name),
            'permission': permission.name,
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
    def load_json(cls, json_str: str):
        data = json.loads(json_str)
        # 如果是直接调用的BaseComponent.load_json，则创建一个新的类
        if cls is BaseComponent:
            comp = type(data['component_name'], (BaseComponent, ), {})
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
        """返回空数据行， id为0时，insert会自动赋予id"""
        row = cls.default_row[0].copy() if size == 1 else cls.default_row.repeat(size, 0)
        return row

    @classmethod
    def dict_to_row(cls, data: dict):
        """从dict生成一个数据行"""
        row = cls.new_row()
        for name, _ in cls.dtype_map_.items():
            row[name] = data[name]
        return row


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
            assert component_cls.component_name_ not in comp_map, "Component重复定义"
        comp_map[component_cls.component_name_] = component_cls


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
    :param readonly: 是否只读Component，只读Component不会被加事务保护，增加并行性。
    :param backend: 指定Component后端，对应配置文件中的db_name。默认为Redis
    :param permission: 设置读取权限，只对游戏客户端的读取查询调用起作用。
        - everybody: 任何客户端都可以读，适合读一些服务器状态类的数据，如在线人数
        - user: 已登录客户端都可以读
        - admin: 只有管理员可以读
        - owner: 只有owner属性值==登录的用户id（`ctx.caller`）时可以读，如果无owner值则认为该行不可读
    :param force: 强制覆盖同名Component，否则会报错。
    :param _cls: 按@define_component()方式调用时，不需要传入_cls参数。

    `Property(default, unique, index, dtype)` 是Component的属性定义，可定义默认值和数据类型。
        `index`表示此属性开启索引；
        `unique`表示属性值必须唯一，启动此项默认会同时打开index。

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
                assert np.dtype(prop.dtype).itemsize > 0, \
                    f"{cls.__name__}.{name}属性的dtype不能为0长度。str类型请用'<U8'方式定义"
                assert np.dtype(prop.dtype).type is not np.void, \
                    f"{cls.__name__}.{name}属性的dtype不支持void类型"
                # bool类型在一些后端数据库中不支持，强制转换为int8
                if prop.dtype is bool or prop.dtype is np.bool_ or prop.dtype == '?':
                    prop.dtype = np.int8
                if prop.unique:
                    prop.index = True
                assert prop.default is not None, \
                    (f"{cls.__name__}.{name}默认值不能为None。所有属性都要有默认值，"
                     f"因为数据接口统一用c like struct实现，强类型struct不接受NULL/None值。")
                can_cast = np.can_cast(np.min_scalar_type(prop.default), prop.dtype)
                if not can_cast and not (type(prop.default) is str or type(prop.default) is bytes):
                    # min_scalar_type(1)会判断为uint8, prop.dtype为int8时判断会失败,所以要转为负数再判断一次
                    default_value = -prop.default if prop.default != 0 else -1
                    can_cast = np.can_cast(np.min_scalar_type(default_value), prop.dtype)
                assert can_cast, (f"{cls.__name__}.{name}的default值："
                                  f"{type(prop.default).__name__}({prop.default})"
                                  f"和属性dtype({prop.dtype})不匹配")
                properties[name] = prop
            delattr(cls, name)

        assert properties, f"{cls.__name__}至少要有1个Property成员"

        # 添加id主键，如果冲突，报错
        assert 'id' not in properties, f"{cls.__name__}.id是保留的内置主键，外部不能重定义"
        properties['id'] = Property(0, True, True, np.int64)  # 必备索引，只进行unique索引为了基础性能

        # 检查class必须继承于BaseComponent
        assert issubclass(cls, BaseComponent), f"{cls.__name__}必须继承于BaseComponent"

        # 生成json格式，并通过json加载到class中
        json_str = BaseComponent.make_json(properties, namespace, cls.__name__, permission,
                                           persist, readonly, backend)
        cls.load_json(json_str)

        # 保存app文件的版本信息
        caller = inspect.stack()[1]
        repo = git.Repo(caller.filename, search_parent_directories=True)
        tree = repo.head.commit.tree
        relpath = os.path.relpath(caller.filename, repo.working_dir).replace(os.sep, '/')
        try:
            blob = tree[relpath]
            sha = blob.hexsha
            cls.git_hash_ = sha
        except KeyError:
            warnings.warn(f"⚠️ [🛠️Define] {caller.filename}文件不在git版本控制中，"
                          f"将无法检测组件{cls.__name__}的版本。")
            cls.git_hash_ = 'untracked'

        # 把class加入到总集中
        ComponentDefines().add_component(namespace, cls, force)
        return cls

    if _cls is None:
        return warp
    else:
        return warp(_cls)
