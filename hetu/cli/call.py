"""
`hetu call`：在本进程里直连配置好的后端，以 admin 或指定玩家身份调用一个 System，输出一行
JSON（原始返回值、客户端实际收到的内容、写集、traceback）。`hetu call --list` 列出全部 System。

@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2025, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import argparse
import asyncio
import difflib
import inspect
import json
import sys
import time
import types
from annotationlib import Format
from typing import TYPE_CHECKING, Any

from ..i18n import _
from .base import (
    CommandInterface,
    UsageError,
    add_config_arguments,
    load_command_config,
    pick_instance,
)
from .console import (
    MAX_AUDIT_ARGS,
    AuditLog,
    CallTimeout,
    LeaseLost,
    WriteRecorder,
    client_payload_of,
    clip,
    describe_target,
    is_production,
    run_json_command,
    to_jsonable,
    write_mode_for,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from ..local import LocalApp
    from ..system.definer import SystemDefine

CORE_PIN_PREFIX = "__core_pin_system_"


# ============ 参数 ============


def _str_annotated(func: Callable) -> list[bool]:
    """System 每个参数（ctx 之后）的注解是不是 str。按字符串读注解，不对前向引用求值"""
    try:
        params = list(
            inspect.signature(func, annotation_format=Format.STRING).parameters.values()
        )[1:]
    except TypeError, ValueError:
        return []
    return [p.annotation in ("str", "builtins.str") for p in params]


def parse_call_args(func: Callable, raw_args: list[str]) -> list[Any]:
    """
    位置参数：形参注解为 str 的原样传字符串；其余先按 JSON 解析，解析不了就当字符串。
    看起来像 JSON（以 { [ " 开头）却解析失败的直接报错：PowerShell 常把引号吃掉。
    """
    str_flags = _str_annotated(func)
    values: list[Any] = []
    for i, raw in enumerate(raw_args):
        if i < len(str_flags) and str_flags[i]:
            values.append(raw)
            continue
        try:
            values.append(json.loads(raw))
        except ValueError:
            if raw.lstrip()[:1] in ("{", "[", '"'):
                raise UsageError(
                    _(
                        "第 {n} 个参数看起来是 JSON 但解析失败（PowerShell 可能吃掉了引号），"
                        "改用 --args-file 或 stdin：{raw}"
                    ).format(n=i + 1, raw=raw)
                ) from None
            values.append(raw)
    return values


def read_args_file(path: str) -> list[Any]:
    """--args-file：一个 JSON 数组，`-` 为 stdin。按 utf-8-sig 读，容忍 PowerShell 写出的 BOM"""
    try:
        if path == "-":
            text = sys.stdin.buffer.read().decode("utf-8-sig")
        else:
            with open(path, "r", encoding="utf-8-sig") as f:
                text = f.read()
        values = json.loads(text)
    except (OSError, ValueError) as e:
        raise UsageError(
            _("--args-file 读不了或不是合法 JSON：{err}").format(err=e)
        ) from e
    if not isinstance(values, list):
        raise UsageError(_("--args-file 的内容必须是 JSON 数组"))
    return values


def read_user_data(raw: str | None) -> dict | None:
    """--user-data：JSON 对象，或 @文件"""
    if raw is None:
        return None
    try:
        if raw.startswith("@"):
            with open(raw[1:], "r", encoding="utf-8-sig") as f:
                raw = f.read()
        data = json.loads(raw)
    except (OSError, ValueError) as e:
        raise UsageError(
            _("--user-data 读不了或不是合法 JSON：{err}").format(err=e)
        ) from e
    if not isinstance(data, dict):
        raise UsageError(_("--user-data 必须是 JSON 对象"))
    return data


def check_arg_count(name: str, sys_def: SystemDefine, args: list) -> None:
    """参数个数，规则同 EndpointExecutor.execute_check"""
    low = sys_def.arg_count - sys_def.defaults_count - 1
    high = sys_def.arg_count - 1
    if not low <= len(args) <= high:
        raise UsageError(
            _("System {name} 要 {low}-{high} 个参数，给了 {n} 个").format(
                name=name, low=low, high=high, n=len(args)
            )
        )


def reads_ctx_caller(func: Callable) -> bool:
    """启发式：System 函数（及其嵌套函数）的代码里有没有读 `.caller`"""

    def walk(code: types.CodeType) -> bool:
        if "caller" in code.co_names:
            return True
        return any(walk(c) for c in code.co_consts if isinstance(c, types.CodeType))

    code = getattr(func, "__code__", None)
    return isinstance(code, types.CodeType) and walk(code)


def find_system(name: str) -> SystemDefine:
    from ..system import SystemClusters

    clusters = SystemClusters()
    sys_def = clusters.get_system(name)
    if sys_def is None or name.startswith(CORE_PIN_PREFIX):
        names = [n for n in clusters.systems_of() if not n.startswith(CORE_PIN_PREFIX)]
        close = difflib.get_close_matches(name, names, n=5)
        hint = _("，相近的有：{close}").format(close=close) if close else ""
        raise UsageError(
            _("不存在的 System：{name}{hint}（hetu call --list 查看全部）").format(
                name=name, hint=hint
            )
        )
    return sys_def


# ============ --list ============


def describe_params(func: Callable) -> list[dict]:
    try:
        sig = inspect.signature(func, annotation_format=Format.STRING)
    except TypeError, ValueError:
        return []
    out = []
    for param in list(sig.parameters.values())[1:]:
        if param.kind is inspect.Parameter.VAR_POSITIONAL:
            out.append({"name": "*" + param.name, "required": False})
            continue
        item: dict[str, Any] = {"name": param.name}
        if param.annotation is not inspect.Parameter.empty:
            item["annotation"] = str(param.annotation)
        item["required"] = param.default is inspect.Parameter.empty
        if not item["required"]:
            item["default"] = to_jsonable(param.default)
        out.append(item)
    return out


def first_doc_line(func: Callable) -> str | None:
    doc = inspect.getdoc(func)
    if not doc:
        return None
    return next((line.strip() for line in doc.splitlines() if line.strip()), None)


def permission_name(permission: Any) -> str | None:
    return None if permission is None else getattr(permission, "name", str(permission))


def list_systems_and_endpoints(namespace: str) -> dict:
    from ..endpoint.definer import EndpointDefines
    from ..system import SystemClusters
    from ..system.lock import SystemLock

    systems = []
    sys_map = SystemClusters().systems_of(namespace)
    for name, sys_def in sorted(sys_map.items()):
        if name.startswith(CORE_PIN_PREFIX):
            continue
        func = sys_def.func
        comps = sorted(
            c.name_ for c in sys_def.full_components if c.master_ is not SystemLock
        )
        systems.append(
            {
                "name": name,
                "permission": permission_name(sys_def.permission),
                "params": describe_params(func),
                "components": comps,
                "depends": sorted(sys_def.depends),
                "call_lock": any(
                    c.master_ is SystemLock for c in sys_def.full_components
                ),
                "on_start": sys_def.on_start,
                "builtin": getattr(func, "__module__", "").startswith("hetu."),
                "doc": first_doc_line(func),
            }
        )
    endpoints = []
    try:
        ep_map = EndpointDefines().get_endpoints(namespace)
    except KeyError:
        ep_map = {}
    for name, ep in sorted(ep_map.items()):
        if name in sys_map:
            continue
        endpoints.append(
            {
                "name": name,
                "permission": permission_name(ep.permission),
                "params": describe_params(ep.func),
                "doc": first_doc_line(ep.func),
                "callable": False,
            }
        )
    return {"systems": systems, "endpoints": endpoints}


async def _list_main(args: argparse.Namespace, report: dict) -> dict:
    from ..local import build_app_registry

    config, config_file = load_command_config(args, need_app=True, need_db=False)
    report["target"] = describe_target(config, config_file, None)
    build_app_registry(config)
    return list_systems_and_endpoints(config["NAMESPACE"])


# ============ 调用 ============


def _guard_lease(app: LocalApp) -> list[bool]:
    """租约丢失时取消当前任务；返回的列表非空表示是因为租约丢失被取消的"""
    lost: list[bool] = []
    task = asyncio.current_task()
    if app.lease is not None and task is not None:

        def on_lost() -> None:
            lost.append(True)
            task.cancel()

        app.lease.on_lost = on_lost
    return lost


async def run_system(
    app: LocalApp,
    recorder: WriteRecorder,
    name: str,
    call_args: list,
    *,
    caller: int,
    group: str,
    user_data: dict | None,
    uuid: str,
    timeout: float,
    warnings: list[str],
) -> dict:
    """在提交观察钩子下跑一个 System，返回输出字段"""
    lost = _guard_lease(app)
    ctx = app.new_context(caller, group, user_data)
    before = asyncio.all_tasks()
    started = time.perf_counter()
    with recorder.active():
        try:
            async with asyncio.timeout(timeout if timeout > 0 else None):
                rtn = await ctx.systems.call(name, *call_args, uuid=uuid)
        except TimeoutError as e:
            raise CallTimeout(
                _(
                    "System {name} 超过 {timeout} 秒没结束，已中止。若超时发生在提交途中，"
                    "提交可能已经生效：请看输出的 writes、审计日志或直接查数据"
                ).format(name=name, timeout=timeout)
            ) from e
        except asyncio.CancelledError:
            if not lost:
                raise
            current = asyncio.current_task()
            if current is not None:
                current.uncancel()
            raise LeaseLost(
                _("Worker ID 租约丢失（本进程卡住太久被别人接手），命令已中止")
            ) from None
    elapsed_ms = (time.perf_counter() - started) * 1000

    current = asyncio.current_task()
    pending = [
        t for t in asyncio.all_tasks() - before if not t.done() and t is not current
    ]
    if pending:
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        warnings.append(
            _(
                "System 留下 {n} 个后台任务，CLI 退出时已取消（在服务器里它们会继续跑）"
            ).format(n=len(pending))
        )
    client, wire_error = client_payload_of(rtn, app.to_client_payload)
    return {
        "result": to_jsonable(rtn),
        "client": client,
        "wire_error": wire_error,
        "retries": ctx.race_count,
        "elapsed_ms": round(elapsed_ms, 3),
    }


async def _call_main(args: argparse.Namespace, report: dict) -> dict:
    from ..endpoint.executor import permission_allows
    from ..local import build_app_registry, open_local_app, resolve_identity

    config, config_file = load_command_config(args, need_app=True, need_db=True)
    instance = pick_instance(config, args.instance)
    report["target"] = describe_target(config, config_file, instance)
    warnings: list[str] = []
    report["warnings"] = warnings

    # 用法错误先报，再连库、租 id
    build_app_registry(config)
    name = args.system
    sys_def = find_system(name)
    if args.args_file is not None:
        if args.args:
            raise UsageError(_("--args-file 与位置参数不能同时给"))
        call_args = read_args_file(args.args_file)
    else:
        call_args = parse_call_args(sys_def.func, args.args)
    check_arg_count(name, sys_def, call_args)
    user_data = read_user_data(args.user_data)
    caller, group = resolve_identity(sys_def, args.caller, args.group)
    derived = args.caller is None and args.group is None
    if (
        not derived
        and sys_def.permission is not None
        and not permission_allows(sys_def.permission, caller, group)
    ):
        warnings.append(
            _(
                "生产端点不会放行这个身份（caller={caller}，group={group}）调用 "
                "{permission} 权限的 System"
            ).format(
                caller=caller,
                group=group,
                permission=permission_name(sys_def.permission),
            )
        )
    if args.caller is None and reads_ctx_caller(sys_def.func):
        warnings.append(
            _(
                "System {name} 读了 ctx.caller，而当前 caller 是推断出来的 0；"
                "要以玩家身份跑请用 --as <uid>"
            ).format(name=name)
        )

    mode = write_mode_for(config, args.dry_run)
    audit = AuditLog.from_config(
        config,
        config_file,
        {
            "config": report["target"]["config"],
            "namespace": config["NAMESPACE"],
            "instance": instance,
        },
    )
    identity = {"caller": caller, "group": group, "derived": derived}
    if audit is not None:
        audit.start(
            command="call",
            system=name,
            args=clip(repr(call_args), MAX_AUDIT_ARGS),
            identity=identity,
            write_mode=mode,
        )
    recorder = WriteRecorder(mode, audit=audit, production=is_production(config))
    recorder.warnings = warnings
    report["writes"] = recorder.writes

    started = time.perf_counter()
    ok = False
    error_type = None
    app = None
    try:
        app = await open_local_app(config, instance=instance, address="cli")
        if audit is not None and app.lease is not None:
            audit.base["worker_id"] = app.lease.worker_id
        fields = await run_system(
            app,
            recorder,
            name,
            call_args,
            caller=caller,
            group=group,
            user_data=user_data,
            uuid=args.uuid,
            timeout=args.timeout,
            warnings=warnings,
        )
        ok = True
    except BaseException as e:
        error_type = type(e).__name__
        raise
    finally:
        if app is not None:
            await app.aclose()
        if audit is not None:
            audit.write_quietly(
                "end",
                ok=ok,
                error_type=error_type,
                elapsed_ms=round((time.perf_counter() - started) * 1000, 3),
            )
    return {
        "system": name,
        "identity": identity,
        **fields,
        "dry_run": mode == "dry_run",
    }


class CallCommand(CommandInterface):
    @classmethod
    def name(cls):
        return "call"

    @classmethod
    def register(cls, subparsers):
        parser = subparsers.add_parser(
            "call",
            help=_(
                "在本进程里直连后端调用一个 System（不经服务器），stdout 输出一行 JSON。"
                "调试用：写入与 System 走同一条提交路径，在线客户端照常收到推送"
            ),
        )
        add_config_arguments(parser)
        parser.add_argument(
            "--list",
            action="store_true",
            help=_("列出全部 System（参数、权限、引用的组件、文档第一行），不连数据库"),
        )
        ident = parser.add_argument_group(_("身份"))
        ident.add_argument(
            "--as",
            dest="caller",
            type=int,
            metavar="UID",
            help=_(
                "以这个玩家 id 调用（ctx.caller）。USER 权限的 System 必须给；"
                "不给时按权限推断：ADMIN/GM/内部 System 用 admin，EVERYBODY 用 guest"
            ),
        )
        ident.add_argument(
            "--group",
            metavar="NAME",
            help=_("ctx.group，如 gm / admin；给了 --as 时默认 guest"),
        )
        ident.add_argument(
            "--user-data",
            metavar="JSON|@FILE",
            help=_("ctx.user_data（登录时写进去的数据 CLI 不知道，需要时手动给）"),
        )
        parser.add_argument(
            "--args-file",
            metavar="PATH|-",
            help=_(
                "从文件读参数（一个 JSON 数组），- 为 stdin；避开 PowerShell 的引号问题"
            ),
        )
        parser.add_argument(
            "--uuid",
            default="",
            help=_("调用锁 uuid（call_lock 的 System），同一 uuid 只执行一次"),
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help=_("照常执行但不提交，输出它会写什么（写集）"),
        )
        parser.add_argument(
            "--timeout",
            type=float,
            default=30.0,
            metavar="SEC",
            help=_("System 调用的超时秒数，默认 30，0 为不限"),
        )
        parser.add_argument(
            "-v",
            "--verbose",
            action="count",
            default=0,
            help=_("stderr 上多打日志：-v 为 INFO，-vv 为 DEBUG"),
        )
        parser.add_argument("system", nargs="?", metavar="SYSTEM", help=_("System 名"))
        parser.add_argument(
            "args",
            nargs="*",
            metavar="ARG",
            help=_("参数：按 JSON 解析，解析不了当字符串；- 开头的放在 -- 后面"),
        )

    @classmethod
    def execute(cls, args):
        if args.list:
            run_json_command(args.verbose, lambda report: _list_main(args, report))
        if not args.system:

            async def missing(_report: dict) -> dict:
                raise UsageError(_("要给 System 名（hetu call --list 查看全部）"))

            run_json_command(args.verbose, missing)
        run_json_command(args.verbose, lambda report: _call_main(args, report))
