"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING
from typing import Callable, Concatenate, ParamSpec, TypeVar


if TYPE_CHECKING:
    from ..component import BaseComponent
    from . import Backend
    from .session import Session


# 定义泛型变量
P = ParamSpec("P")
R = TypeVar("R")
T = TypeVar("T")


# 这是一个类型安全的 partial 辅助函数
def bind_first_arg_with_typehint(
    func: Callable[Concatenate[T, P], R], first_arg: T
) -> Callable[P, R]:
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        return func(first_arg, *args, **kwargs)

    return wrapper


@dataclass(frozen=True, eq=True)  # 定义为不可变，且可以作为按内容hash的dict键
class TableReference:
    """
    Table表的地址信息，在后端，组件持久化的目标称为表。
    组件实际储存在数据库中时，需要实例名和cluster id等信息，此类封装了这些信息。
    """

    comp_cls: type[BaseComponent]
    instance_name: str
    cluster_id: int

    def is_same_txn_group(self, other: TableReference) -> bool:
        return (
            self.instance_name == other.instance_name
            and self.cluster_id == other.cluster_id
        )

    @property
    def comp_name(self) -> str:
        return self.comp_cls.name_


@dataclass(frozen=True, eq=True)
class Table(TableReference):
    """
    Table表的地址信息加上所属的Backend。
    """

    backend: Backend

    def session(self) -> Session:
        return self.backend.session(self.instance_name, self.cluster_id)

    @property
    def servant_get(self):
        return bind_first_arg_with_typehint(self.backend.servant.get, self)

    @property
    def servant_range(self):
        return bind_first_arg_with_typehint(self.backend.servant.range, self)

    @property
    def direct_set(self):
        return bind_first_arg_with_typehint(self.backend.master.direct_set, self)
