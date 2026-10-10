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


def queryable_fields(comp_cls: type[BaseComponent]) -> list[str]:
    return sorted(comp_cls.indexes_)


def check_index(comp_cls: type[BaseComponent], field: str) -> None:
    if field not in comp_cls.indexes_:
        raise UsageError(
            _(
                "组件 {comp} 的 {field} 不是 id 或带索引的字段，不能查；可查的有：{fields}"
            ).format(
                comp=comp_cls.name_, field=field, fields=queryable_fields(comp_cls)
            )
        )


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
) -> list[dict]:
    """读行：默认 servant（非事务），master 时在只读 master 的事务里读"""
    from ..data.backend.base import BackendClient, RowFormat
    from ..headless import HeadlessClient

    if master:
        client = HeadlessClient(table.backend, table.instance_name, [table])
        async with client.session(table.comp_cls) as session:
            rows = await session[table.comp_cls].range(
                index, left, right, limit=limit, desc=desc
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
    rows = await table.servant_range(index, left, right, limit, desc, RowFormat.STRUCT)
    return to_jsonable(rows)


# ============ 命令主体 ============


async def _get_main(args: argparse.Namespace, report: dict) -> dict:
    config, config_file = load_command_config(args, need_app=False, need_db=True)
    instance = pick_instance(config, args.instance)
    report["target"] = describe_target(config, config_file, instance)
    if not args.query or "=" not in args.query:
        raise UsageError(_("查询要写成 FIELD=VALUE，如 owner=1001"))
    field, raw = args.query.split("=", 1)
    field = field.strip()
    table, backends = await resolve_table(config, instance, args.component)
    try:
        comp_cls = table.comp_cls
        check_index(comp_cls, field)
        value = convert_value(comp_cls.dtype_map_[field], raw, point=True)
        rows = await _read(table, args.master, field, value, value, 1, False)
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
            table, args.master, args.index, left, right, fetch, args.desc
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
        parser.add_argument("component", nargs="?", metavar="COMPONENT")
        parser.add_argument("query", nargs="?", metavar="FIELD=VALUE")

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
        parser.add_argument("component", metavar="COMPONENT")
        parser.add_argument("index", metavar="INDEX")
        parser.add_argument("left", metavar="LEFT")
        parser.add_argument("right", nargs="?", metavar="RIGHT")

    @classmethod
    def execute(cls, args):
        run_json_command(args.verbose, lambda report: _range_main(args, report))
