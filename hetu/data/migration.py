"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2025, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import logging
import hashlib
from pathlib import Path
from typing import TYPE_CHECKING, Callable

import numpy as np

if TYPE_CHECKING:
    from .backend.table import TableReference
    from .backend.base import TableMaintenance

logger = logging.getLogger("HeTu.root")


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

    @staticmethod
    def _load_script(file) -> Callable:
        logger.warning(
            f"  ➖ [💾Redis][{table_ref.comp_name}组件] "
            f"发现自定义迁移脚本 {script_path}，将调用脚本进行迁移..."
        )
        module_name = f"Migration_{table_ref.comp_name}_{old_version}_to_{new_version}"
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
        # todo 层叠升级检测
        return migration_func

    @staticmethod
    def _load_schema_migration_script(
        table_ref: TableReference, old_version: str
    ) -> Callable:
        """加载组件模型的的用户迁移脚本"""
        # todo test
        import hashlib
        from pathlib import Path

        migration_file = f"{table_ref.comp_name}_{old_version}_to_{new_version}.py"
        # 组合app目录 + maint/migration/目录 + 迁移文件名
        script_path = Path.cwd() / "maint" / "migration" / migration_file
        script_path = script_path.absolute()
        if not script_path.exists():
            logger.warning(
                f"  ➖ [💾Redis][{table_ref.comp_name}组件] "
                f"未发现自定义迁移脚本 {script_path}，将使用默认迁移逻辑..."
            )
            # 读取当前目录下的默认迁移脚本
            script_path = Path(__file__).parent / "default_migration.py"

        return MigrationScript._load_script(script_path)

    def __init__(
        self,
        app_file: str,
        table_ref: TableReference,
        old_meta: TableMaintenance.TableMeta,
    ):
        self.upgrade_stack = []
        # 读取table迁移脚本
        # 首先看old_meta里的版本号，然后搜索该版本号开头的文件
        old_version = old_meta.version
        new_version = hashlib.md5(table_ref.comp_cls.json_.encode("utf-8")).hexdigest()
        # 然后看该文件的目标版本号，继续搜索，直到目标版本号和当前版本号相同为止
        app_root = Path(app_file).parent
        target_version = ""
        while target_version != new_version:
            script_file, target_version = self._find_script(app_root, old_version)
            self.upgrade_stack.append(script_file)
            old_version = target_version
        # 如果最后一个版本号没找到，则尝试生成默认迁移脚本，如果是有损的，则看force
        if target_version != new_version:
            script_file = self._generate_default_migration_script(
                table_ref, old_version, new_version
            )
            self.upgrade_stack.append(script_file)

    def up(self, row: np.record) -> np.record:
        """执行迁移操作"""
        # todo 先执行自动迁移逻辑，然后再执行迁移脚本
        #      或者执行层叠迁移，自动迁移永远叠在每层脚本之上
        return migration_func(row)
