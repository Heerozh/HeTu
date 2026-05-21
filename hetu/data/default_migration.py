import logging

import numpy as np

from hetu import BaseComponent
from hetu.data.backend import TableReference
from hetu.data.backend.base import TableMaintenance

logger = logging.getLogger("HeTu.root")

down_component_json = r'<"DOWN_JSON">'
target_component_json = r'<"TARGET_JSON">'
# 设置导出模块变量，表示迁移的源和目标模型
TARGET_COMPONENT_MODEL = BaseComponent.load_json(target_component_json)
DOWN_COMPONENT_MODEL = BaseComponent.load_json(down_component_json)


# 默认迁移脚本用变量
remove_columns = []
add_columns = []
unsafe_convert_columns = []
type_convert_columns = []


def prepare() -> str:
    """
    迁移前的预检查，如果不能迁移在这报错。
    此方法会在upgrade前多次调用，必须幂等。

    Returns
    -------
    str
        - "skip": 组件表结构无变更，无需迁移。
        - "unsafe": 本迁移代码是有损迁移，需要用force指令手动迁移。
        - "ok": 可以安全迁移。
    """
    name = TARGET_COMPONENT_MODEL.name_
    # 检查是否无变更
    down_dtypes = DOWN_COMPONENT_MODEL.dtypes
    target_dtypes = TARGET_COMPONENT_MODEL.dtypes
    if down_dtypes == target_dtypes:
        return "skip"

    logger.warning(
        f"  ⚠️ [💾MIGRATION][{name}组件] 代码定义的Schema与已存的不一致，"
        f"数据库中：\n"
        f"{down_dtypes}\n"
        f"代码定义的：\n"
        f"{target_dtypes}\n "
        f"将尝试数据迁移（只处理新属性，不处理类型变更，改名等等情况）："
    )

    # 准备列检查
    assert down_dtypes.fields and target_dtypes.fields  # for type checker
    down_columns = down_dtypes.fields
    target_columns = target_dtypes.fields

    # 检查是否有属性被删除
    for down_column in down_columns:
        if down_column not in target_columns:
            msg = (
                f"  ⚠️ [💾MIGRATION][{name}组件] "
                f"数据库中的属性 {down_column} 在新的组件定义中不存在，如果改名了需要手动迁移，"
                f"强制执行将丢弃该属性数据。"
            )
            logger.warning(msg)
            remove_columns.append(down_column)

    # 检查是否有属性类型变更且无法自动转换
    for target_column in target_columns:
        if target_column in down_columns:
            old_type = down_dtypes.fields[target_column]
            new_type = target_dtypes.fields[target_column]
            if old_type != new_type:
                type_convert_columns.append(target_column)
                if not np.can_cast(old_type[0], new_type[0]):
                    msg = (
                        f"  ⚠️ [💾MIGRATION][{name}组件] "
                        f"属性 {target_column} 的类型由 {old_type} 变更为 {new_type}，"
                        f"无法自动转换类型，需要手动迁移，强制执行将截断/丢弃该属性数据。"
                    )
                    logger.warning(msg)
                    unsafe_convert_columns.append(target_column)

    # 检查新增的属性是否有默认值
    target_props = dict(TARGET_COMPONENT_MODEL.properties_)
    for target_column in target_columns:
        if target_column not in down_columns:
            add_columns.append(target_column)
            logger.warning(
                f"  ⚠️ [💾MIGRATION][{name}组件] "
                f"新的代码定义中多出属性 {target_column}，将使用默认值填充。"
            )
            default = target_props[target_column].default
            if default is None:
                msg = (
                    f"  ⚠️ [💾MIGRATION][{name}组件] "
                    f"迁移时尝试新增 {target_column} 属性失败，该属性没有默认值，无法新增。"
                )
                logger.error(msg)
                raise ValueError(msg)

    if remove_columns or unsafe_convert_columns:
        return "unsafe"

    return "ok"


def upgrade(
    row_ids: list[int],
    down_tables: dict[str, TableReference],
    target_table: TableReference,
    client: TableMaintenance,  # 负责直接写入数据的，专供迁移使用的客户端
) -> None:
    """实际执行升级迁移的操作，本操作不可失败。"""
    # 一些属性信息
    assert DOWN_COMPONENT_MODEL.name_ == TARGET_COMPONENT_MODEL.name_
    table_name = DOWN_COMPONENT_MODEL.name_
    target_columns = dict(TARGET_COMPONENT_MODEL.properties_)
    down_columns = dict(DOWN_COMPONENT_MODEL.properties_)
    down_table = down_tables[table_name]

    # 修改老的table名, 老的表读完后就删除
    renamed_down_component = DOWN_COMPONENT_MODEL.duplicate(
        DOWN_COMPONENT_MODEL.namespace_, "__temp__"
    )
    renamed_down_tbl = TableReference(
        renamed_down_component, down_table.instance_name, down_table.cluster_id
    )
    client.do_rename_table_(down_table, renamed_down_tbl)
    # 创建表，开始schema迁移
    client.do_create_table_(target_table)

    for row_id in row_ids:
        down_row = client.get(renamed_down_tbl, row_id)
        assert down_row

        up_row = TARGET_COMPONENT_MODEL.new_row(down_row.id)

        # 复制共有列
        for col in target_columns:
            if col in down_columns:
                up_row[col] = down_row[col]

        # 如果有新增列，不用管，new_row已经自动填充了默认值
        # 如果有删除列，不用管，up_row已经不包含了
        # 如果有类型变更，也不用管，前面在复制共有列时自动完成了

        client.upsert_row(target_table, up_row)

    # 删除老的表
    client.do_drop_table_(renamed_down_tbl)
