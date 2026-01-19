"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2025, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""


class MigrationScript:
    """
    迁移脚本类。

    首先是migration_schema时初始化此类，如果加载到了脚本，则执行脚本中的迁移方法。

    否则执行类中的自动迁移方法。

    方法会给脚本传递old row，要求返回new row。
    同时会传递给你所有已知的old版本的Table的引用，方便你读取。所以此方法必须在cluster id变更后进行，
    不然会找不到key，不对meta里有old cluster id
    首先协议化meta内容到base里，然后规范化create table流程


    对于删除的component怎么办？可以返回所有meta内容

    """

    def __init__(self, system):
        self.system = system

    def up(self):
        """执行迁移操作"""
        raise NotImplementedError("请在子类中实现迁移逻辑")

    def down(self):
        """回滚迁移操作"""
        raise NotImplementedError("请在子类中实现回滚逻辑")
