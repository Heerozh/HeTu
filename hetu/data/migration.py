"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2025, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import hashlib
import importlib.util
import logging
import sys
from pathlib import Path
from types import ModuleType
from typing import TYPE_CHECKING

from hetu.data import BaseComponent
from hetu.i18n import _

from .backend.table import TableReference

if TYPE_CHECKING:
    from .backend.base import TableMaintenance

logger = logging.getLogger("HeTu.root")


class MigrationScript:
    """
    迁移脚本版本管理类，自动读取Schema升级脚本，从低版本一层层升级上来。
    如果未找到迁移脚本，则会生成默认迁移脚本，管理员可以根据需要修改脚本内容后再执行迁移操作。

    """

    @staticmethod
    def _load_schema_migration_script(table_ref, file: Path):
        """import script.py"""
        logger.warning(
            _(
                "  ➖ [💾Redis][{comp_name}组件] "
                "发现自定义迁移脚本 {file}，将调用脚本进行迁移..."
            ).format(comp_name=table_ref.comp_name, file=file)
        )
        module_name = str(file)
        spec = importlib.util.spec_from_file_location(module_name, file)
        assert spec and spec.loader, "Could not load script:" + str(file)
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)

        return module

    @staticmethod
    def _find_script(app_root: Path, old_version):
        # 文件名格式是 任意前缀_oldversion_to_newversion.py
        # 查找oldversion符合的文件，并返回newversion
        migration_dir = app_root / "maint" / "migration"
        for file in migration_dir.glob(f"*_v{old_version}_to_v*.py"):
            parts = file.stem.split("_to_v")
            if len(parts) != 2:
                continue
            target_version = parts[1]
            return file, target_version
        return None, None

    @staticmethod
    def _generate_default_migration_script(
        app_root: Path, target_model, down_model_json, old_version, new_version
    ):
        # 在app_root/maint/migration/目录下，把默认迁移脚本写进去
        migration_dir = app_root / "maint" / "migration"
        migration_dir.mkdir(parents=True, exist_ok=True)
        migration_file = f"{target_model.name_}_v{old_version}_to_v{new_version}.py"
        script_path = migration_dir / migration_file
        if script_path.exists():
            return script_path
        # 读取__file__.parent / default_migration.py模板
        template_path = Path(__file__).parent / "default_migration.py"
        with open(template_path, "r", encoding="utf-8") as f:
            template = f.read()
        # 替换模板中的占位符
        template = template.replace('<"TARGET_JSON">', f"{target_model.json_}")
        template = template.replace('<"DOWN_JSON">', down_model_json)
        # 写入新脚本
        with open(script_path, "w", encoding="utf-8") as f:
            f.write(template)
        logger.warning(
            _(
                "  ➖ [💾Redis][{comp_name}组件] "
                "缺少迁移脚本，生成默认迁移脚本 {script_path}，请根据需要修改脚本内容后再执行迁移操作..."
            ).format(comp_name=target_model.name_, script_path=script_path)
        )
        return script_path

    def _load_scripts(self) -> list[ModuleType]:
        return [
            self._load_schema_migration_script(self.ref, file)
            for file in self.upgrade_stack
        ]

    def __init__(
        self,
        app_file: str,
        table_ref: TableReference,
        old_meta: TableMaintenance.TableMeta,
    ):
        self.upgrade_stack = []
        self.ref = table_ref
        self.loaded_upgrade_stack: list[ModuleType] = []
        # 读取table迁移脚本
        # 首先看old_meta里的版本号，然后搜索该版本号开头的文件
        old_version = old_meta.version
        new_version = hashlib.md5(table_ref.comp_cls.json_.encode("utf-8")).hexdigest()
        # 然后看该文件的目标版本号，继续搜索，直到目标版本号和当前版本号相同为止
        app_root = Path(app_file).parent
        target_version = ""
        while target_version != new_version:
            script_file, target_version = self._find_script(app_root, old_version)
            if not script_file:
                break
            self.upgrade_stack.append(script_file)
            old_version = target_version
        # 如果最后一个版本号没找到，则尝试生成默认迁移脚本，如果是有损的，则看force
        if target_version != new_version:
            script_file = self._generate_default_migration_script(
                app_root, table_ref.comp_cls, old_meta.json, old_version, new_version
            )
            self.upgrade_stack.append(script_file)

    def prepare(self):
        self.loaded_upgrade_stack = self._load_scripts()
        ret = "skip"
        for module in self.loaded_upgrade_stack:
            prepare_func = getattr(module, "prepare", None)
            assert prepare_func, (
                f"Migration script {module} must define prepare function"
            )
            status = prepare_func()
            if status == "skip":
                pass
            elif status == "ok":
                ret = "ok"
            elif status == "unsafe":
                return "unsafe"
            else:
                raise RuntimeError(
                    f"Migration script {module} prepare function returned invalid status {status}"
                )
        return ret

    def upgrade(self, row_ids: list[int], maint: TableMaintenance):
        """执行迁移操作，注意执行前需要锁定整个数据库，防止多个worker同时执行。"""
        from ..system import SystemClusters

        # 加载所有component在数据库中的版本
        down_tables = {}
        for comp, cluster_id in SystemClusters().get_components().items():
            # 从数据库读取老版本
            down_meta = maint.read_meta(self.ref.instance_name, comp)
            if not down_meta:
                # 说明该组件是新加的，还没create table
                continue

            down_comp = BaseComponent.load_json(down_meta.json)
            if comp == self.ref.comp_name:
                continue
            down_tables[down_comp.name_] = TableReference(
                down_comp, self.ref.instance_name, cluster_id
            )

        # 运行所有升级stack
        for module in self.loaded_upgrade_stack:
            # 前置检查
            prepare_func = getattr(module, "prepare", None)
            upgrade_func = getattr(module, "upgrade", None)
            assert prepare_func and upgrade_func, (
                f"Migration script {module} must define prepare/upgrade function"
            )
            target_model = getattr(module, "TARGET_COMPONENT_MODEL", None)
            down_model = getattr(module, "DOWN_COMPONENT_MODEL", None)
            assert target_model and down_model, (
                f"Migration script {module} must define "
                f"TARGET_COMPONENT_MODEL/DOWN_COMPONENT_MODEL"
            )

            # 切换model到脚本中指定的版本，因为每个stack model都会升一级
            down_tables[down_model.name_] = TableReference(
                down_model, self.ref.instance_name, self.ref.cluster_id
            )
            target_table = TableReference(
                target_model, self.ref.instance_name, self.ref.cluster_id
            )

            # 再次执行检查看是否跳过
            status = prepare_func()
            if status == "skip":
                continue

            # 开始迁移
            logger.info(
                _("  ➖ [💾Redis][{comp_name}组件] 执行upgrade迁移：{module}").format(
                    comp_name=self.ref.comp_name, module=module
                )
            )
            upgrade_func(row_ids, down_tables, target_table, maint)
            logger.warning(
                _(
                    "  ✔️ [💾Redis][{comp_name}组件] Schema升级迁移完成，共处理{count}行"
                ).format(comp_name=self.ref.comp_name, count=len(row_ids))
            )
            maint.do_rebuild_index_(target_table)
            logger.warning(
                _("  ✔️ [💾Redis][{comp_name}组件] 已重建Index").format(
                    comp_name=self.ref.comp_name
                )
            )
