"""
`hetu get` / `hetu range`：直接看组件数据（不做 RLS），stdout 输出一行 JSON。

**不 import app**：按组件名从服务器写入的表 meta 解析 schema（`hetu.headless`），app 代码
import 报错、本地定义改到一半时照样能看数据。默认读 servant，`--master` 读 master。
`hetu get --list` 列出组件、字段与索引（这个要加载 app，不连数据库）。

@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2025, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import argparse
import copy
from typing import TYPE_CHECKING, Any

from ..i18n import _
from .base import (
    CommandInterface,
    UsageError,
    add_config_arguments,
    load_command_config,
    pick_instance,
)
from .console import describe_target, run_json_command, to_jsonable

if TYPE_CHECKING:
    import numpy as np

    from ..data.backend import Backend, Table
    from ..data.component import BaseComponent


# ============ 值转换 ============


def convert_value(dtype: np.dtype, raw: str, *, point: bool) -> Any:
    """
    命令行给的值按列 dtype 转换。数值列接受 "1001"，也接受 inf / -inf（区间边界），以及表示
    闭 / 开区间的 [ / ( 前缀——带前缀的校验过是个数后原样交给后端（见 base.peel_bound_）。
    布尔列只认 true/false/1/0。字符串（含 bytes）列：point（get 的点查）时以 [ 或 ( 开头的值
    前面补一个 [，按字面值查。
    """
    from ..data.backend.base import exact_number_

    kind = dtype.kind
    try:
        match kind:
            case "i" | "u" | "f":
                prefixed = raw[:1] in ("[", "(")
                number = exact_number_(raw[1:] if prefixed else raw)
                if kind == "f":
                    number = float(number)
                return raw if prefixed else number
            case "b":
                lowered = raw.strip().lower()
                if lowered in ("true", "1"):
                    return True
                if lowered in ("false", "0"):
                    return False
                raise ValueError(raw)
    except (ValueError, OverflowError) as e:
        raise UsageError(
            _("值 {raw} 不能转换成列类型 {dtype}").format(raw=raw, dtype=dtype)
        ) from e
    if point and raw[:1] in ("[", "("):
        raw = "[" + raw
    return raw.encode("utf-8") if kind == "S" else raw


def convert_group_value(dtype: np.dtype, raw: str) -> Any:
    """
    复合索引的分组值按字面值转换：不处理 [ / ( 前缀（分组值是等值，没有开闭区间），整数列要是
    整数。之后交给 resolve_group_ 按列类型校验
    """
    if dtype.kind in "iu":
        try:
            return int(raw)
        except ValueError as e:
            raise UsageError(
                _("分组值 {raw} 不能转换成列类型 {dtype}").format(raw=raw, dtype=dtype)
            ) from e
    return raw.encode("utf-8") if dtype.kind == "S" else raw


def parse_pairs(items: list[str]) -> dict[str, str]:
    """命令行的 FIELD=VALUE 们 → {FIELD: VALUE}，同一个 FIELD 只能给一次"""
    pairs: dict[str, str] = {}
    for item in items:
        if "=" not in item:
            raise UsageError(_("查询要写成 FIELD=VALUE，如 owner=1001"))
        field, raw = item.split("=", 1)
        field = field.strip()
        if field in pairs:
            raise UsageError(_("{field} 给了不止一次").format(field=field))
        pairs[field] = raw
    return pairs


def queryable_fields(comp_cls: type[BaseComponent]) -> list[str]:
    """可查的索引，复合索引带上分组列，如 level(zone)"""
    groups = comp_cls.index_groups_
    return sorted(
        f"{name}({','.join(groups[name])})" if groups[name] else name
        for name in comp_cls.indexes_
    )


def check_index(comp_cls: type[BaseComponent], field: str) -> None:
    if field not in comp_cls.indexes_:
        raise UsageError(
            _(
                "组件 {comp} 的 {field} 不是 id 或带索引的字段，不能查；可查的有：{fields}"
                "（括号里是复合索引的分组列，查询时要一并用 FIELD=VALUE 给出）"
            ).format(
                comp=comp_cls.name_, field=field, fields=queryable_fields(comp_cls)
            )
        )


def match_index(
    comp_cls: type[BaseComponent], pairs: dict[str, str], explicit: str | None
) -> str:
    """
    get 的 FIELD=VALUE 们对应哪个索引：--index 指定的，或者只给了一个字段，或者键集合正好是
    某个复合索引的本字段加全部分组列
    """
    if explicit is not None:
        if explicit not in pairs:
            raise UsageError(
                _("--index {index} 要同时用 {index}=VALUE 给出查询值").format(
                    index=explicit
                )
            )
        return explicit
    if len(pairs) == 1:
        return next(iter(pairs))
    groups = comp_cls.index_groups_
    keys = set(pairs)
    matched = [
        name for name in pairs if name in groups and keys == {name, *groups[name]}
    ]
    if len(matched) == 1:
        return matched[0]
    if not matched:
        raise UsageError(
            _(
                "查询条件 {keys} 对不上组件 {comp} 的任何索引：要么只给一个带索引的字段，"
                "要么正好给出复合索引的本字段和全部分组列；可查的有：{fields}"
            ).format(
                keys=sorted(keys),
                comp=comp_cls.name_,
                fields=queryable_fields(comp_cls),
            )
        )
    raise UsageError(
        _("查询条件 {keys} 同时匹配了 {matched} 几个索引，请用 --index 指定").format(
            keys=sorted(keys), matched=matched
        )
    )


def build_group(
    comp_cls: type[BaseComponent], index: str, given: dict[str, str]
) -> dict[str, Any] | None:
    """index 的分组值：given 要正好是它的分组列（单列索引就是空），各值按字面值转换"""
    groups = comp_cls.index_groups_[index]
    if set(given) != set(groups):
        if groups:
            raise UsageError(
                _(
                    "{index} 是按 {columns} 分组的复合索引，要用 FIELD=VALUE 正好给出这些分组列，"
                    "收到：{keys}"
                ).format(index=index, columns=", ".join(groups), keys=sorted(given))
            )
        raise UsageError(
            _("{index} 不是复合索引，多给了 {keys}").format(
                index=index, keys=sorted(given)
            )
        )
    dtype_map = comp_cls.dtype_map_
    return {
        col: convert_group_value(dtype_map[col], given[col]) for col in groups
    } or None


def project(rows: list[dict], fields: str | None) -> list[dict]:
    if not fields:
        return rows
    wanted = [f.strip() for f in fields.split(",") if f.strip()]
    return [{k: row[k] for k in wanted if k in row} for row in rows]


# ============ 打开表 ============


async def resolve_table(
    config: dict, instance: str, comp_name: str
) -> tuple[Table, list[Backend]]:
    """按组件名在各后端里找表 meta（配置顺序，第一个找到的为准），返回 Table 与打开的后端"""
    from ..data.backend import Backend
    from ..headless import HeadlessClient, TableNotFound
    from ..local import check_backend_files

    check_backend_files(config)
    backends: list[Backend] = []
    try:
        for db_cfg in config["BACKENDS"].values():
            backend = Backend(copy.deepcopy(db_cfg))
            backends.append(backend)
            try:
                (table,) = HeadlessClient.resolve_tables_(
                    backend, instance, [comp_name]
                )
            except TableNotFound:
                continue
            return table, backends
    except BaseException:
        await _close(backends)
        raise
    await _close(backends)
    raise TableNotFound(instance, comp_name)


async def _close(backends: list[Backend]) -> None:
    for backend in backends:
        await backend.close()


async def _read(
    table: Table,
    master: bool,
    index: str,
    left: Any,
    right: Any,
    limit: int,
    desc: bool,
    group: dict[str, Any] | None = None,
) -> list[dict]:
    """读行：默认 servant（非事务），master 时在只读 master 的事务里读。group 是复合索引的分组值"""
    from ..data.backend.base import BackendClient, RowFormat
    from ..headless import HeadlessClient

    if master:
        client = HeadlessClient(table.backend, table.instance_name, [table])
        async with client.session(table.comp_cls) as session:
            rows = await session[table.comp_cls].range(
                index, left, right, limit=limit, desc=desc, _group=group
            )
        return to_jsonable(rows)
    if index == "id":
        # 按 id 的点查直接取行。小数、越界、开区间不是点（区间里没有这个 id），走区间查询
        point = BackendClient.point_query_value_(
            table.comp_cls.dtype_map_["id"], left, right
        )
        if point is not None:
            row = await table.servant_get(int(point), RowFormat.STRUCT)
            return [] if row is None else [to_jsonable(row)]
    rows = await table.servant_range(
        index, left, right, limit, desc, RowFormat.STRUCT, group=group
    )
    return to_jsonable(rows)


# ============ 命令主体 ============


async def _get_main(args: argparse.Namespace, report: dict) -> dict:
    config, config_file = load_command_config(args, need_app=False, need_db=True)
    instance = pick_instance(config, args.instance)
    report["target"] = describe_target(config, config_file, instance)
    if not args.query:
        raise UsageError(_("查询要写成 FIELD=VALUE，如 owner=1001"))
    pairs = parse_pairs(args.query)
    table, backends = await resolve_table(config, instance, args.component)
    try:
        comp_cls = table.comp_cls
        field = match_index(comp_cls, pairs, args.index)
        check_index(comp_cls, field)
        group = build_group(
            comp_cls, field, {k: v for k, v in pairs.items() if k != field}
        )
        value = convert_value(comp_cls.dtype_map_[field], pairs[field], point=True)
        rows = await _read(table, args.master, field, value, value, 1, False, group)
    finally:
        await _close(backends)
    rows = project(rows, args.fields)
    return {
        "component": comp_cls.name_,
        "read_from": "master" if args.master else "servant",
        "row": rows[0] if rows else None,
    }


async def _range_main(args: argparse.Namespace, report: dict) -> dict:
    config, config_file = load_command_config(args, need_app=False, need_db=True)
    instance = pick_instance(config, args.instance)
    report["target"] = describe_target(config, config_file, instance)
    table, backends = await resolve_table(config, instance, args.component)
    limit = args.limit
    try:
        comp_cls = table.comp_cls
        check_index(comp_cls, args.index)
        group = build_group(comp_cls, args.index, parse_pairs(args.group))
        dtype = comp_cls.dtype_map_[args.index]
        left = convert_value(dtype, args.left, point=False)
        right = (
            None
            if args.right is None
            else convert_value(dtype, args.right, point=False)
        )
        # 多取一行，准确判断有没有被截断
        fetch = -1 if limit < 0 else limit + 1
        rows = await _read(
            table, args.master, args.index, left, right, fetch, args.desc, group
        )
    finally:
        await _close(backends)
    truncated = 0 <= limit < len(rows)
    if truncated:
        rows = rows[:limit]
    return {
        "component": comp_cls.name_,
        "read_from": "master" if args.master else "servant",
        "rows": project(rows, args.fields),
        "count": len(rows),
        "truncated": truncated,
    }


def list_components(namespace: str) -> list[dict]:
    from ..system import SystemClusters

    out = []
    comps = SystemClusters().get_components(namespace)
    for comp, cluster_id in sorted(comps.items(), key=lambda kv: kv[0].name_):
        fields = []
        for name, prop in comp.properties_:
            fields.append(
                {
                    "name": name,
                    "dtype": str(comp.dtype_map_[name]),
                    "default": to_jsonable(prop.default),
                    "unique": bool(prop.unique),
                    "index": name in comp.indexes_,
                    # 复合索引的分组列：查询时要一并给出
                    "groupby": list(prop.groupby),
                    "hidden": bool(prop.hidden),
                }
            )
        out.append(
            {
                "name": comp.name_,
                "namespace": comp.namespace_,
                "permission": getattr(comp.permission_, "name", str(comp.permission_)),
                "volatile": bool(comp.volatile_),
                "backend": comp.backend_,
                "cluster_id": cluster_id,
                "fields": fields,
            }
        )
    return out


async def _list_main(args: argparse.Namespace, report: dict) -> dict:
    from ..local import build_app_registry

    config, config_file = load_command_config(args, need_app=True, need_db=False)
    report["target"] = describe_target(config, config_file, None)
    build_app_registry(config)
    return {"components": list_components(config["NAMESPACE"])}


def _common_read_arguments(parser: argparse.ArgumentParser) -> None:
    add_config_arguments(parser)
    parser.add_argument(
        "--master",
        action="store_true",
        help=_("读 master（刚写完马上读时用）；默认读 servant"),
    )
    parser.add_argument("--fields", metavar="F1,F2", help=_("只输出这些列，逗号分隔"))
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help=_("stderr 上多打日志：-v 为 INFO，-vv 为 DEBUG"),
    )


class GetCommand(CommandInterface):
    json_output = True

    @classmethod
    def name(cls):
        return "get"

    @classmethod
    def register(cls, subparsers):
        parser = subparsers.add_parser(
            "get",
            help=_(
                "按 id 或带索引的字段读一行组件数据（不做 RLS），stdout 输出一行 JSON；"
                "不加载 app，schema 取自库里的表 meta"
            ),
        )
        _common_read_arguments(parser)
        parser.add_argument(
            "--list",
            action="store_true",
            help=_("列出组件、字段与索引（加载 app，不连数据库）"),
        )
        parser.add_argument(
            "--index",
            help=_(
                "FIELD=VALUE 同时匹配了几个复合索引时，指定按哪个索引查（其余 FIELD=VALUE "
                "是它的分组值）"
            ),
        )
        parser.add_argument("component", nargs="?", metavar="COMPONENT")
        parser.add_argument(
            "query",
            nargs="*",
            metavar="FIELD=VALUE",
            help=_(
                "一个带索引的字段，如 owner=1001；复合索引要同时给出全部分组列，"
                "如 name=abc zone=3"
            ),
        )

    @classmethod
    def execute(cls, args):
        if args.list:
            run_json_command(args.verbose, lambda report: _list_main(args, report))
        if not args.component:

            async def missing(_report: dict) -> dict:
                raise UsageError(
                    _("要给组件名和 FIELD=VALUE（hetu get --list 查看组件）")
                )

            run_json_command(args.verbose, missing)
        run_json_command(args.verbose, lambda report: _get_main(args, report))


class RangeCommand(CommandInterface):
    json_output = True

    @classmethod
    def name(cls):
        return "range"

    @classmethod
    def register(cls, subparsers):
        parser = subparsers.add_parser(
            "range",
            help=_(
                "按带索引的字段读一个区间的组件数据（不做 RLS），stdout 输出一行 JSON；"
                "省略 RIGHT 即精确匹配 LEFT，( / [ 前缀表示开 / 闭区间"
            ),
        )
        _common_read_arguments(parser)
        parser.add_argument(
            "--limit",
            type=int,
            default=10,
            help=_("最多返回多少行，默认 10，-1 为不限"),
        )
        parser.add_argument("--desc", action="store_true", help=_("降序"))
        parser.add_argument(
            "--group",
            action="append",
            default=[],
            metavar="FIELD=VALUE",
            help=_("复合索引的分组值，每个分组列给一次，如 --group zone=3"),
        )
        parser.add_argument("component", metavar="COMPONENT")
        parser.add_argument("index", metavar="INDEX")
        parser.add_argument("left", metavar="LEFT")
        parser.add_argument("right", nargs="?", metavar="RIGHT")

    @classmethod
    def execute(cls, args):
        run_json_command(args.verbose, lambda report: _range_main(args, report))
