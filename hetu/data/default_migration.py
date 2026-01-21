import numpy as np
from hetu import BaseComponent
import logging

from hetu.data.backend import TableReference
from hetu.data.backend.base import TableMaintenance

logger = logging.getLogger("HeTu.root")

down_model_json = r"<DOWN_JSON>"
target_model_json = r"<TARGET_JSON>"
# 设置导出模块变量，表示迁移的源和目标模型
TARGET_MODEL = BaseComponent.load_json(target_model_json)
DOWN_MODEL = BaseComponent.load_json(down_model_json)


# 默认迁移脚本用变量
remove_columns = []
add_columns = []
unsafe_convert_columns = []
type_convert_columns = []


def prepare() -> str:
    """
    迁移前的预检查，此方法会在upgrade前多次调用，必须幂等。

    Returns
    -------
    str
        - "skip": 组件表结构无变更，无需迁移。
        - "unsafe": 本迁移代码是有损迁移，需要用force指令手动迁移。
        - "ok": 可以安全迁移。
    """
    name = TARGET_MODEL.component_name_
    # 检查是否无变更
    down_dtypes = DOWN_MODEL.dtypes
    target_dtypes = TARGET_MODEL.dtypes
    if down_dtypes == target_dtypes:
        return "skip"

    logger.warning(
        f"  ⚠️ [💾Redis][{name}组件] 代码定义的Schema与已存的不一致，"
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
                f"  ⚠️ [💾Redis][{name}组件] "
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
                        f"  ⚠️ [💾Redis][{name}组件] "
                        f"属性 {target_column} 的类型由 {old_type} 变更为 {new_type}，"
                        f"无法自动转换类型，需要手动迁移，强制执行将截断/丢弃该属性数据。"
                    )
                    logger.warning(msg)
                    unsafe_convert_columns.append(target_column)

    # 检查新增的属性是否有默认值
    # todo nullable属性的处理
    target_props = dict(TARGET_MODEL.properties_)
    for target_column in target_columns:
        if target_column not in down_columns:
            add_columns.append(target_column)
            logger.warning(
                f"  ⚠️ [💾Redis][{name}组件] "
                f"新的代码定义中多出属性 {target_column}，将使用默认值填充。"
            )
            default = target_props[target_column].default
            if default is None:
                msg = (
                    f"  ⚠️ [💾Redis][{name}组件] "
                    f"迁移时尝试新增 {target_column} 属性失败，该属性没有默认值，无法新增。"
                )
                logger.error(msg)
                raise ValueError(msg)

    if remove_columns or unsafe_convert_columns:
        return "unsafe"

    return "ok"


def upgrade(
    row_ids: list[int],
    table_refs: dict[type[BaseComponent], TableReference],
    client: TableMaintenance.MaintenanceClient,  # 负责直接写入数据的，专供迁移使用的客户端
) -> None:
    # 一些属性信息
    from_dtypes = DOWN_MODEL.dtypes
    target_dtypes = TARGET_MODEL.dtypes
    target_columns = dict(TARGET_MODEL.properties_)
    assert from_dtypes.fields and target_dtypes.fields  # for type checker

    # 开始schema迁移
    # 迁移删除的列
    for col in remove_columns:
        client.remove_column(table_refs[DOWN_MODEL], col)
    # 迁移增加的列
    for col in add_columns:
        client.add_column(table_refs[DOWN_MODEL], col, target_columns[col].dtype)
    # 迁移类型变更的列
    for col in type_convert_columns:
        client.add_column(
            table_refs[DOWN_MODEL], col + "__temp__", target_columns[col].dtype
        )

    added = 0
    converted = 0
    convert_failed = 0
    for row_id in row_ids:
        up_row = client.get(table_refs[DOWN_MODEL], row_id)
        assert up_row

        # 如果有新增列，需要update整个row(comp自动带默认值)
        # 如果有删除列，也update整个row(client自动del then update)
        # 如果有类型变更，也update整个row(client在get时已自动类型更新了)
        client.update(table_refs[DOWN_MODEL], up_row)
        # 迁移删除的列

    # 删除类型变更的临时列
    for col in type_convert_columns:
        client.remove_column(table_refs[DOWN_MODEL], col + "__temp__")
    ###########################################

    for prop_name in target_dtypes.fields:
        # todo 删除的属性目前会遗留在redis中
        if prop_name not in from_dtypes.fields:
            default = target_props[prop_name].default
            pipe = io.pipeline()
            for key in keys:
                pipe.hset(key.decode(), prop_name, default)
            pipe.execute()
            added += 1
        elif force:  # 类型转换
            old_type = from_dtypes.fields[prop_name][0]
            new_type = target_dtypes.fields[prop_name][0]
            if old_type == new_type:
                continue
            default = props[prop_name].default
            pipe = io.pipeline()
            for key in keys:
                val = io.hget(key.decode(), prop_name)
                if val is None:
                    continue
                try:
                    val = cast(bytes, cast(object, val))
                    casted_val = new_type.type(old_type.type(val.decode()))

                    if np.issubdtype(new_type, np.character):
                        # 字符串类型需要特殊截断处理，不然np会自动延长
                        def fixed_str_len(dt: np.dtype) -> int:
                            dt = np.dtype(dt)
                            if dt.kind == "U":
                                return dt.itemsize // 4
                            if dt.kind == "S":
                                return dt.itemsize
                            raise TypeError(f"not a fixed-length string dtype: {dt!r}")

                        casted_val = casted_val[: fixed_str_len(new_type)]

                    pipe.hset(key.decode(), prop_name, str(casted_val))
                    converted += 1
                except ValueError as _:
                    # 强制模式下丢弃该属性
                    pipe.hset(key.decode(), prop_name, default)
                    convert_failed += 1
            pipe.execute()

    # 更新meta
    version = hashlib.md5(table_ref.comp_cls.json_.encode("utf-8")).hexdigest()
    io.hset(self.meta_key(table_ref), "version", version)
    io.hset(self.meta_key(table_ref), "json", table_ref.comp_cls.json_)

    logger.warning(
        f"  ✔️ [💾Redis][{table_ref.comp_name}组件] 新属性增加完成，共处理{len(keys)}行 * "
        f"{added}个属性。 转换类型成功{converted}次，失败{convert_failed}次。"
    )
    return True
