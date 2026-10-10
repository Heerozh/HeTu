"""
`hetu shell`：进程内连上配置好的后端，预置与 `Sandbox` 同名的 API（`call_system` / `get` /
`must_get` / `insert` / `upsert`，`app.range`），支持顶层 await。代码来自 ``-c``、脚本文件或
stdin；都没给且 stdin 是终端时进交互模式。

@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2025, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import argparse
import ast
import asyncio
import code
import concurrent.futures
import contextvars
import inspect
import json
import sys
import threading
import time
import traceback
import types
from typing import TYPE_CHECKING, Any, NoReturn, TextIO

import numpy as np

from ..i18n import _
from .base import (
    CommandInterface,
    add_config_arguments,
    load_command_config,
    pick_instance,
)
from .console import (
    AuditLog,
    WriteRecorder,
    cancel_new_tasks,
    close_app_quietly,
    code_for_audit,
    describe_target,
    exit_code_for,
    is_production,
    lease_guard,
    logger,
    setup_process,
    to_jsonable,
    write_mode_for,
)

if TYPE_CHECKING:
    from ..local import LocalApp


def make_show(out: TextIO):
    def show(obj: Any) -> None:
        """把任意值（numpy 行 / 数组、返回值……）按 JSON 打出来，numpy 行带字段名"""
        out.write(json.dumps(to_jsonable(obj), ensure_ascii=False, indent=2) + "\n")
        out.flush()

    return show


def _no_call(*_args: Any, **_kwargs: Any) -> NoReturn:
    raise NotImplementedError(
        _(
            "call 留给以后的 Endpoint 路径（与 Sandbox.call 同义），"
            "跑 System 请用 call_system"
        )
    )


def build_namespace(app: LocalApp, out: TextIO) -> dict[str, Any]:
    """shell 预置的名字。range 不预置成裸名字（会遮住内置的 range），用 app.range"""
    import hetu

    return {
        "__name__": "__hetu_shell__",
        "__builtins__": __builtins__,
        "app": app,
        "client": app.client,
        "call_system": app.call_system,
        "get": app.get,
        "must_get": app.must_get,
        "insert": app.insert,
        "upsert": app.upsert,
        "show": make_show(out),
        "call": _no_call,
        "np": np,
        "hetu": hetu,
        "asyncio": asyncio,
    }


async def run_source(source: str, filename: str, ns: dict, show) -> None:
    """整段执行，支持顶层 await；最后一条语句是表达式时 show 它的值"""
    flags = ast.PyCF_ALLOW_TOP_LEVEL_AWAIT
    tree = ast.parse(source, filename, "exec")
    last = None
    if tree.body and isinstance(tail := tree.body[-1], ast.Expr):
        tree.body.pop()
        last = ast.Expression(tail.value)
    if tree.body:
        result = eval(compile(tree, filename, "exec", flags=flags), ns)
        if inspect.iscoroutine(result):
            await result
    if last is not None:
        value = eval(compile(last, filename, "eval", flags=flags), ns)
        if inspect.iscoroutine(value):
            value = await value
        if value is not None:
            show(value)


class _AsyncConsole(code.InteractiveConsole):
    """交互模式：REPL 在单独线程读输入，代码交给主线程的事件循环跑（同 python -m asyncio），
    空闲时租约循环照常运行"""

    def __init__(
        self,
        ns: dict,
        loop: asyncio.AbstractEventLoop,
        context: contextvars.Context,
    ):
        super().__init__(ns)
        self.compile.compiler.flags |= ast.PyCF_ALLOW_TOP_LEVEL_AWAIT
        self.loop = loop
        self.context = context

    def runcode(self, code: types.CodeType) -> None:
        future: concurrent.futures.Future = concurrent.futures.Future()

        def callback() -> None:
            try:
                func = types.FunctionType(code, self.locals)  # type: ignore[arg-type]
                coro = func()
            except BaseException as e:  # noqa: BLE001
                future.set_exception(e)
                return
            if not inspect.iscoroutine(coro):
                future.set_result(coro)
                return
            # 在设置了提交观察钩子的上下文里跑，dry-run / 写保护 / 审计对交互代码同样生效
            task = self.loop.create_task(coro, context=self.context)

            def done(t: asyncio.Task) -> None:
                if t.cancelled():
                    future.cancel()
                elif (exc := t.exception()) is not None:
                    future.set_exception(exc)
                else:
                    future.set_result(t.result())

            task.add_done_callback(done)

        self.loop.call_soon_threadsafe(callback, context=self.context)
        try:
            future.result()
        except SystemExit:
            raise
        except BaseException:  # noqa: BLE001
            self.showtraceback()


def _displayhook(show):
    def hook(value: Any) -> None:
        if value is None:
            return
        if isinstance(value, (np.void, np.ndarray)) and getattr(
            value.dtype, "names", None
        ):
            show(value)
        else:
            sys.__displayhook__(value)

    return hook


async def interact(ns: dict, show) -> None:
    loop = asyncio.get_running_loop()
    context = contextvars.copy_context()
    done = loop.create_future()
    console = _AsyncConsole(ns, loop, context)
    sys.displayhook = _displayhook(show)

    def repl() -> None:
        try:
            console.interact(
                banner=_(
                    "HeTu shell：预置 app / client / call_system / get / must_get / insert"
                    " / upsert / show，range 用 app.range；支持顶层 await，Ctrl+D 退出"
                ),
                exitmsg="",
            )
        except SystemExit:
            pass
        finally:
            loop.call_soon_threadsafe(lambda: done.done() or done.set_result(None))

    threading.Thread(target=repl, name="hetu-shell-repl", daemon=True).start()
    await done


def read_source(args: argparse.Namespace) -> tuple[str | None, str]:
    """代码来源：-c、脚本文件、stdin（- 或 stdin 不是终端）；都没有返回 None（交互模式）"""
    if args.code is not None:
        return args.code, "<-c>"
    if args.file not in (None, "-"):
        with open(args.file, "r", encoding="utf-8-sig") as f:
            return f.read(), args.file
    if args.file == "-" or not sys.stdin.isatty():
        return sys.stdin.buffer.read().decode("utf-8-sig"), "<stdin>"
    return None, "<interactive>"


async def _shell_main(args: argparse.Namespace, out: TextIO) -> None:
    from ..local import open_local_app

    config, config_file = load_command_config(args, need_app=True, need_db=True)
    instance = pick_instance(config, args.instance)
    target = describe_target(config, config_file, instance)

    source, filename = read_source(args)
    if source is not None and not source.strip():
        print(
            _(
                "没有要执行的代码：用 -c CODE、脚本文件，或从 stdin 传入；"
                "在终端里直接运行 hetu shell 进交互模式"
            ),
            file=sys.stderr,
        )

    mode = write_mode_for(config, args.dry_run)
    audit = AuditLog.from_config(
        config,
        config_file,
        {
            "config": target["config"],
            "namespace": config["NAMESPACE"],
            "instance": instance,
        },
    )
    if audit is not None:
        audit.start(
            command="shell",
            write_mode=mode,
            **(
                code_for_audit(source, None if filename.startswith("<") else filename)
                if source is not None
                else {"code": "(interactive)"}
            ),
        )
    recorder = WriteRecorder(mode, audit=audit, production=is_production(config))
    started = time.perf_counter()
    ok = False
    error_type = None
    app = None
    try:
        app = await open_local_app(config, instance=instance, address="cli")
        if audit is not None and app.lease is not None:
            audit.base["worker_id"] = app.lease.worker_id
        ns = build_namespace(app, out)
        before = asyncio.all_tasks()
        with lease_guard(app), recorder.active():
            try:
                if source is None:
                    await interact(ns, ns["show"])
                else:
                    await run_source(source, filename, ns, ns["show"])
            finally:
                # 代码留下的后台任务赶在关闭后端之前取消：它们的 finally 可能还要读写数据库
                leftover = await cancel_new_tasks(before)
                if leftover:
                    logger.warning(
                        _("⚠️ shell 退出时取消了 {n} 个还在跑的后台任务").format(
                            n=leftover
                        )
                    )
        ok = True
    except BaseException as e:
        error_type = type(e).__name__
        raise
    finally:
        if app is not None:
            await close_app_quietly(app)
        if audit is not None:
            audit.write_quietly(
                "end",
                ok=ok,
                error_type=error_type,
                elapsed_ms=round((time.perf_counter() - started) * 1000, 3),
            )


class ShellCommand(CommandInterface):
    @classmethod
    def name(cls):
        return "shell"

    @classmethod
    def register(cls, subparsers):
        parser = subparsers.add_parser(
            "shell",
            help=_(
                "进程内连上后端的 Python shell：预置 call_system / get / must_get / insert / "
                "upsert / show 与 app.range，支持顶层 await"
            ),
        )
        add_config_arguments(parser)
        parser.add_argument("-c", dest="code", metavar="CODE", help=_("执行这段代码"))
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help=_("执行但不提交任何写入"),
        )
        parser.add_argument(
            "-v",
            "--verbose",
            action="count",
            default=0,
            help=_("stderr 上多打日志：-v 为 INFO，-vv 为 DEBUG"),
        )
        parser.add_argument(
            "file",
            nargs="?",
            metavar="FILE",
            help=_("要执行的脚本，- 为 stdin；都不给且 stdin 是终端时进交互模式"),
        )

    @classmethod
    def execute(cls, args):
        out = setup_process(args.verbose, redirect_stdout=False)
        try:
            asyncio.run(_shell_main(args, out))
        except KeyboardInterrupt:
            sys.exit(130)
        except SystemExit:  # 脚本里 exit() / sys.exit(n)：照它的退出码
            raise
        except BaseException as e:  # noqa: BLE001 traceback 写 stderr，退出码同 hetu call
            traceback.print_exception(e, file=sys.stderr)
            sys.exit(exit_code_for(e))
        sys.exit(0)
