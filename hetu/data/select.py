"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from .component import BaseComponent
    from .session import Session


# todo 语法改成：
#   row = mod_item_component.new_row()
#   session.component(component).insert(row)
#   session.component(component).get(id=id)
#   session.component(component).query(index=(left, right)).desc().top(10)
#   session.component(component).update(id, row)
#   session.component(component).delete(id)
#   session.component(component).upsert(index=row.index, row)
class Select:
    def __init__(self, comp_cls: type[BaseComponent]) -> None:
        pass

    def attach(self, session: Session) -> None:
        """
        将查询附加到会话中。
        """

        pass
