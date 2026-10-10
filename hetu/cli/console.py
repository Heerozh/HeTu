"""
hetu call / get / range / shell 共用的进程环境、输出格式、写保护与审计。

- stdout 只有一行 JSON（shell 除外），日志与一切 print 都进 stderr，强制 UTF-8；
- 退出码：0 成功，1 代码或调用失败，2 用法错误，3 环境未就绪；
- 写模式（commit / dry_run / forbid）、写集记录、审计都挂在提交观察钩子
  （`hetu.data.backend.session.commit_observer`）上。

@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2025, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import asyncio
import base64
import getpass
import hashlib
import json
import logging
import math
import os
import socket
import sys
import traceback
import uuid
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from datetime import datetime
from typing import TYPE_CHECKING, Any, Literal, NoReturn, TextIO
from urllib.parse import urlparse

import numpy as np

from ..endpoint.response import RejectResponse, ResponseToClient
from ..i18n import _
from .base import UsageError

if TYPE_CHECKING:
    from ..data.backend.session import CommitFn, Session
    from ..local import LocalApp

logger = logging.getLogger("HeTu.root")

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_USAGE = 2
EXIT_NOT_READY = 3

# 写集里每张表每种操作最多列多少行
MAX_WRITE_ROWS = 20
# 审计里参数 / 代码的最大长度
MAX_AUDIT_ARGS = 1024
MAX_AUDIT_CODE = 4096
DEFAULT_AUDIT_LOG = "logs/hetu_cli_audit.jsonl"

WriteMode = Literal["commit", "dry_run", "forbid"]


class CliWriteForbidden(Exception):
    """配置禁止 CLI 写入（CLI_ALLOW_WRITE: false），且没有 --dry-run（退出码 3）"""

    def __init__(self) -> None:
        super().__init__(
            _(
                "本配置禁止 hetu call / shell 写入（CLI_ALLOW_WRITE: false）："
                "加 --dry-run 看它会写什么，或在配置里设 CLI_ALLOW_WRITE: true"
            )
        )


class LeaseLost(Exception):
    """Worker ID 租约丢失（进程卡住太久被别人接手），命令中止（退出码 3）"""


class AuditUnavailable(Exception):
    """审计日志写不进去，不在没有审计的情况下操作（退出码 3）"""


class CallTimeout(Exception):
    """System 调用超过 --timeout（退出码 1）"""


# ============ 进程环境 ============


def setup_process(verbosity: int, *, redirect_stdout: bool = True) -> TextIO:
    """
    设置进程环境，返回真正的 stdout（最后那行 JSON 写到这里）。

    - stdout / stderr 强制 UTF-8：日志和报错里有 emoji 与中文，Windows 的 GBK 管道会让进程
      在打印自己的错误时抛 UnicodeEncodeError；
    - redirect_stdout：之后的一切 print（app import、System 里的）都改写到 stderr；
    - 日志由这里配置，**不套用配置文件的 LOGGING**（其中 console handler 写 stdout）：
      HeTu 日志写 stderr，默认 WARNING，-v 为 INFO，-vv 为 DEBUG。
    """
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is not None:
            try:
                reconfigure(encoding="utf-8", errors="backslashreplace")
            except ValueError, OSError:
                pass
    real_stdout = sys.stdout
    if redirect_stdout:
        sys.stdout = sys.stderr

    level = {0: logging.WARNING, 1: logging.INFO}.get(verbosity, logging.DEBUG)
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(levelname)s %(name)s: %(message)s"))
    root = logging.getLogger()
    for old in list(root.handlers):
        root.removeHandler(old)
    root.addHandler(handler)
    root.setLevel(level)
    logging.getLogger("HeTu.root").setLevel(level)
    replay = logging.getLogger("HeTu.replay")
    replay.propagate = verbosity >= 2
    replay.setLevel(logging.DEBUG if verbosity >= 2 else logging.CRITICAL)
    return real_stdout


def emit(real_stdout: TextIO, payload: dict) -> None:
    """把结果写成恰好一行 JSON"""
    line = json.dumps(to_jsonable(payload), ensure_ascii=False, allow_nan=False)
    real_stdout.write(line + "\n")
    real_stdout.flush()


def run_json_command(
    verbosity: int, main: Callable[[dict], Awaitable[dict]]
) -> NoReturn:
    """
    跑一个输出一行 JSON 的命令并退出。``main(report)`` 返回成功时的字段；``report`` 是失败
    时也要带上的字段（target、writes、warnings），由命令边跑边填。
    """
    real_stdout = setup_process(verbosity)
    report: dict = {}
    try:
        fields = asyncio.run(main(report))
        payload = {"ok": True, **fields, **report}
        code = EXIT_OK
    except KeyboardInterrupt:
        raise
    except BaseException as e:  # noqa: BLE001 一律转成 JSON 与退出码
        code = exit_code_for(e)
        payload = {**error_fields(e, with_traceback=code == EXIT_FAILED), **report}
    emit(real_stdout, payload)
    sys.exit(code)


# ============ JSON 转换 ============


def _jsonable_float(value: float) -> float | str:
    if math.isnan(value):
        return "NaN"
    if math.isinf(value):
        return "Infinity" if value > 0 else "-Infinity"
    return value


def to_jsonable(obj: Any) -> Any:
    """
    把任意返回值转成 JSON 能表示的样子：带字段名的 numpy 行 → dict；结构化数组 → dict 列表；
    numpy 标量 → Python 标量；NaN / ±inf → 字符串；bytes 能按 UTF-8 解码就转 str，否则
    ``{"__bytes__": base64}``；tuple / set → list；其他对象 → ``{"__repr__": repr(x)}``。
    """
    if obj is None or isinstance(obj, (bool, int, str)):
        return obj
    if isinstance(obj, float):
        return _jsonable_float(obj)
    if isinstance(obj, ResponseToClient):
        return {"ResponseToClient": to_jsonable(obj.message)}
    if isinstance(obj, RejectResponse):
        return {"RejectResponse": {"code": obj.code, "reason": obj.reason}}
    if isinstance(obj, np.void) and obj.dtype.names:
        return {name: to_jsonable(obj[name]) for name in obj.dtype.names}
    if isinstance(obj, np.ndarray):
        if obj.dtype.names:
            return [to_jsonable(row) for row in obj]
        return [to_jsonable(x) for x in obj.tolist()]
    if isinstance(obj, np.generic):
        return to_jsonable(obj.item())
    if isinstance(obj, (bytes, bytearray, memoryview)):
        raw = bytes(obj)
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError:
            return {"__bytes__": base64.b64encode(raw).decode("ascii")}
    if isinstance(obj, dict):
        return {
            (k if isinstance(k, str) else str(k)): to_jsonable(v)
            for k, v in obj.items()
        }
    if isinstance(obj, (list, tuple)):
        return [to_jsonable(x) for x in obj]
    if isinstance(obj, (set, frozenset)):
        items = [to_jsonable(x) for x in obj]
        try:
            return sorted(items)
        except TypeError:
            return items
    return {"__repr__": repr(obj)}


def client_payload_of(rtn: Any, to_client_payload) -> tuple[Any, str | None]:
    """客户端实际收到的内容（按 receiver.rpc() 包装 + 线上同款 msgpack 往返）与序列化错误"""
    if isinstance(rtn, RejectResponse):
        return {"rej": rtn.code}, None
    try:
        return to_client_payload(rtn), None
    except Exception as e:  # noqa: BLE001 线上这条回复发不出去，原样报给调用方
        return None, f"{type(e).__name__}: {e}"


# ============ 错误与退出码 ============


def exit_code_for(exc: BaseException) -> int:
    """按异常类型映射退出码，与异常在哪里抛出无关"""
    from ..common.snowflake_id import WorkerLeaseExpired
    from ..headless import HeadlessError
    from ..local import BackendNotReady, IdentityRequired, TableNotReady
    from .base import ConfigError

    if isinstance(exc, (UsageError, IdentityRequired)):
        return EXIT_USAGE
    not_ready: tuple[type[BaseException], ...] = (
        ConfigError,
        TableNotReady,
        BackendNotReady,
        HeadlessError,
        CliWriteForbidden,
        LeaseLost,
        AuditUnavailable,
        WorkerLeaseExpired,
    )
    if isinstance(exc, not_ready):
        return EXIT_NOT_READY
    try:
        from redis.exceptions import ConnectionError as RedisConnectionError
        from redis.exceptions import TimeoutError as RedisTimeoutError

        if isinstance(exc, (RedisConnectionError, RedisTimeoutError)):
            return EXIT_NOT_READY
    except ImportError:
        pass
    return EXIT_FAILED


def error_fields(exc: BaseException, *, with_traceback: bool = True) -> dict:
    """失败时输出的 error_type / error / traceback。用法错误、环境未就绪只给消息，
    traceback 只对代码或调用失败（退出码 1）有用"""
    error_type = "Timeout" if isinstance(exc, CallTimeout) else type(exc).__name__
    fields = {"ok": False, "error_type": error_type, "error": str(exc) or error_type}
    if with_traceback:
        fields["traceback"] = "".join(traceback.format_exception(exc))
    return fields


# ============ 配置描述 ============


def mask_url(url: str) -> str:
    """数据库地址打码口令；SQLite 给库文件绝对路径"""
    if url.startswith("sqlite:///"):
        from ..data.backend.sqlite.client import SQLiteBackendClient

        try:
            path = SQLiteBackendClient.parse_dsn(url)
        except Exception:  # noqa: BLE001
            return url
        # 与 HeTu 的写法一致：POSIX 绝对路径是 sqlite:////abs，Windows 是 sqlite:///C:/…
        return "sqlite:///" + os.path.abspath(path).replace(os.sep, "/")
    parsed = urlparse(url)
    if parsed.password is None:
        return url
    user = parsed.username or ""
    host = parsed.hostname or ""
    port = f":{parsed.port}" if parsed.port else ""
    return parsed._replace(netloc=f"{user}:***@{host}{port}").geturl()


def describe_target(
    config: dict, config_file: str | None, instance: str | None
) -> dict:
    """输出里的 target：配置文件、namespace、instance、各后端地址（口令打码）"""
    return {
        "config": os.path.abspath(config_file) if config_file else None,
        "namespace": config.get("NAMESPACE"),
        "instance": instance,
        "backends": {
            name: mask_url(str(cfg.get("master", "")))
            for name, cfg in (config.get("BACKENDS") or {}).items()
            if isinstance(cfg, dict)
        },
    }


# ============ 审计 ============


def mask_argv(argv: list[str]) -> list[str]:
    """审计里记的命令行：数据库地址（``--db URL`` / ``--db=URL``）打码口令，每项最多 1 KB"""
    out = []
    for arg in argv:
        head, sep, value = arg.partition("=") if arg.startswith("--") else ("", "", arg)
        if "://" in value:
            value = mask_url(value)
        out.append(clip(head + sep + value, MAX_AUDIT_ARGS))
    return out


class AuditLog:
    """
    CLI 审计：只追加的 JSONL，每行一次 ``os.write``（O_APPEND），不轮转。记录 start（命令行、
    参数、身份、写模式）、每次真实提交的写集 id（commit）、end（结果）。不用 replay 日志：
    它默认关闭、要套用配置的 LOGGING（console handler 写 stdout），且文件 handler 不是进程安全的。
    """

    def __init__(self, path: str, base: dict) -> None:
        self.path = path
        self.base = {
            "run": uuid.uuid4().hex,
            "user": _current_user(),
            "host": socket.gethostname(),
            "pid": os.getpid(),
            "cwd": os.getcwd(),
            **base,
        }

    @classmethod
    def from_config(
        cls, config: dict, config_file: str | None, base: dict
    ) -> AuditLog | None:
        """按配置 CLI_AUDIT_LOG 建审计日志（相对配置目录；参数模式相对当前目录），"" 关闭"""
        path = config.get("CLI_AUDIT_LOG", DEFAULT_AUDIT_LOG)
        if not path:
            return None
        path = str(path)
        if not os.path.isabs(path):
            root = os.path.dirname(os.path.abspath(config_file)) if config_file else "."
            path = os.path.join(root, path)
        return cls(os.path.abspath(path), base)

    def write(self, event: str, **fields: Any) -> None:
        record = {
            "ts": datetime.now().astimezone().isoformat(timespec="milliseconds"),
            "event": event,
            **self.base,
            **fields,
        }
        line = json.dumps(to_jsonable(record), ensure_ascii=False, allow_nan=False)
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        fd = os.open(self.path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)
        try:
            os.write(fd, (line + "\n").encode("utf-8"))
        finally:
            os.close(fd)

    def start(self, **fields: Any) -> None:
        """start 记录（带打码后的命令行）写不进去就中止命令（AuditUnavailable）"""
        try:
            self.write("start", argv=mask_argv(sys.argv), **fields)
        except OSError as e:
            raise AuditUnavailable(
                _(
                    "审计日志写不进去：{path}（{err}）。可用配置 CLI_AUDIT_LOG 改路径"
                ).format(path=self.path, err=e)
            ) from e

    def write_quietly(self, event: str, **fields: Any) -> None:
        """之后的记录写不进去只告警：提交已经发生，撤不回"""
        try:
            self.write(event, **fields)
        except OSError as e:
            logger.warning(
                _("⚠️ 审计日志写不进去：{path}（{err}）").format(path=self.path, err=e)
            )


def _current_user() -> str:
    try:
        return getpass.getuser()
    except Exception:  # noqa: BLE001 没有用户名的环境（容器里随意的 uid）
        return str(os.getuid()) if hasattr(os, "getuid") else "?"


def clip(text: str, limit: int) -> str:
    return text if len(text) <= limit else text[:limit] + "…"


def code_for_audit(code: str, path: str | None) -> dict:
    """审计里记 shell 执行的代码：不超过 4 KB 记原文，否则记 sha256 与文件路径"""
    if len(code) <= MAX_AUDIT_CODE:
        return {"code": code, "file": path}
    return {"code_sha256": hashlib.sha256(code.encode()).hexdigest(), "file": path}


# ============ 写模式与写集 ============


def _decode_field(comp_cls: Any, field: str, value: Any) -> Any:
    """get_dirty_rows() 给的是提交用的字符串，按组件 dtype 还原成 JSON 类型"""
    dtype = comp_cls.dtype_map_.get(field)
    if dtype is None or isinstance(value, (bytes, bytearray)):
        return to_jsonable(value)
    try:
        match dtype.kind:
            case "i" | "u":
                return int(value)
            case "f":
                return _jsonable_float(float(value))
            case "b":
                return value == "True"
    except TypeError, ValueError:
        pass
    return to_jsonable(value)


def _decode_row(comp_cls: Any, row: dict) -> dict:
    return {k: _decode_field(comp_cls, k, v) for k, v in row.items()}


def _clipped(items: list) -> tuple[list, int]:
    return items[:MAX_WRITE_ROWS], max(0, len(items) - MAX_WRITE_ROWS)


def summarize_writes(session: Session, dirty: dict, committed: bool) -> dict | None:
    """一次提交的写集（每张表按 insert / update / delete 分组，每种最多列 MAX_WRITE_ROWS 行），
    没有实际改动返回 None。dirty 是 ``session.idmap.get_dirty_rows()``"""
    tables: dict[str, dict] = {}
    for ref, (inserts, (olds, news), deletes) in dirty.items():
        if not (inserts or news or deletes):
            continue
        comp_cls = ref.comp_cls
        entry: dict[str, Any] = {}
        if inserts:
            rows, omitted = _clipped(inserts)
            entry["insert"] = [_decode_row(comp_cls, r) for r in rows]
            if omitted:
                entry["insert_omitted"] = omitted
        if news:
            pairs, omitted = _clipped(list(zip(olds, news, strict=True)))
            entry["update"] = [
                {
                    "id": _decode_field(comp_cls, "id", old["id"]),
                    "changes": {
                        k: [
                            _decode_field(comp_cls, k, old.get(k)),
                            _decode_field(comp_cls, k, v),
                        ]
                        for k, v in new.items()
                    },
                }
                for old, new in pairs
            ]
            if omitted:
                entry["update_omitted"] = omitted
        if deletes:
            rows, omitted = _clipped(deletes)
            entry["delete"] = [_decode_row(comp_cls, r) for r in rows]
            if omitted:
                entry["delete_omitted"] = omitted
        tables[comp_cls.name_] = entry
    if not tables:
        return None
    return {
        "instance": session.instance_name,
        "cluster": session.cluster_id,
        "committed": committed,
        "tables": tables,
    }


def _ids_by_op(dirty: dict) -> dict:
    """审计里只记各表按操作分组的 id，全部记（写集摘要每种操作只列 MAX_WRITE_ROWS 行）"""
    out: dict[str, dict[str, list]] = {}
    for ref, (inserts, (olds, news), deletes) in dirty.items():
        comp_cls = ref.comp_cls
        ops = {
            op: [_decode_field(comp_cls, "id", r["id"]) for r in rows]
            for op, rows in (("insert", inserts), ("update", olds), ("delete", deletes))
            if rows and (op != "update" or news)
        }
        if ops:
            out[comp_cls.name_] = ops
    return out


class WriteRecorder:
    """
    提交观察者：按写模式决定提交与否，记录写集，写审计，DEBUG 关闭时给出真写入警告。

    用 `active()` 只在执行用户工作（System 调用、shell 代码）期间装上。
    """

    def __init__(
        self,
        mode: WriteMode,
        *,
        audit: AuditLog | None = None,
        production: bool = False,
    ) -> None:
        self.mode = mode
        self.audit = audit
        self.production = production
        self.writes: list[dict] = []
        self.warnings: list[str] = []
        self._production_warned = False

    async def __call__(self, session: Session, commit_fn: CommitFn) -> None:
        from ..data.backend.base import RaceCondition, UniqueViolation

        dirty = session.idmap.get_dirty_rows()
        entry = summarize_writes(session, dirty, committed=self.mode == "commit")
        if self.mode == "forbid" and entry is not None:
            raise CliWriteForbidden()
        if self.mode == "dry_run":
            if entry is not None:
                self.writes.append(entry)
            return
        if entry is None:
            await commit_fn(session.idmap)
            return
        try:
            await commit_fn(session.idmap)
        except RaceCondition, UniqueViolation:
            raise  # 后端拒绝了这次提交，什么都没写（RaceCondition 时事务会重试）
        except BaseException:
            # 提交途中被取消（--timeout、租约丢失）或连接出错：可能已经生效，照样记下来
            entry["committed"] = "unknown"
            self._record(entry, dirty)
            raise
        self._record(entry, dirty)
        if self.production and not self._production_warned:
            self._production_warned = True
            msg = _(
                "DEBUG 关闭的配置（按生产库对待）上 CLI 刚刚真写入了数据；"
                "要禁止，在配置里设 CLI_ALLOW_WRITE: false"
            )
            self.warnings.append(msg)
            logger.warning("⚠️ " + msg)

    def _record(self, entry: dict, dirty: dict) -> None:
        self.writes.append(entry)
        if self.audit is not None:
            self.audit.write_quietly(
                "commit",
                cluster=entry["cluster"],
                instance=entry["instance"],
                committed=entry["committed"],
                tables=_ids_by_op(dirty),
            )

    @contextmanager
    def active(self) -> Iterator[None]:
        """在当前任务（及之后创建的子任务）里装上本观察者"""
        from ..data.backend.session import commit_observer

        token = commit_observer.set(self)
        try:
            yield
        finally:
            commit_observer.reset(token)


def write_mode_for(config: dict, dry_run: bool) -> WriteMode:
    """--dry-run 优先；否则看配置 CLI_ALLOW_WRITE（默认 true）"""
    if dry_run:
        return "dry_run"
    allow = config.get("CLI_ALLOW_WRITE", True)
    if isinstance(allow, str):
        allow = allow.strip().lower() not in ("false", "0", "no", "off", "")
    return "commit" if allow else "forbid"


def is_production(config: dict) -> bool:
    """配置的 DEBUG 关闭（或没有）就按生产库对待"""
    debug = config.get("DEBUG", 0)
    if isinstance(debug, str):
        return debug.strip().lower() in ("", "0", "false", "no", "off")
    return not debug


# ============ 执行用户工作与收尾 ============

# 收尾时等被取消的后台任务结束的最长秒数（吞掉取消、不肯退出的任务不能卡住命令）
TASK_CLEANUP_TIMEOUT = 5.0


@contextmanager
def lease_guard(app: LocalApp) -> Iterator[None]:
    """
    执行用户工作期间：租约丢失（本进程卡住太久、worker id 被别人接手）就取消当前任务，
    并把那次取消转成 `LeaseLost`。其他来源的取消（Ctrl+C 等）原样传出。
    """
    task = asyncio.current_task()
    lease = app.lease
    if lease is None or task is None:
        yield
        return
    lost = False

    def on_lost() -> None:
        nonlocal lost
        lost = True
        task.cancel()

    lease.on_lost = on_lost
    try:
        yield
    except asyncio.CancelledError:
        if not lost:
            raise
        task.uncancel()
        raise LeaseLost(
            _("Worker ID 租约丢失（本进程卡住太久被别人接手），命令已中止")
        ) from None
    finally:
        lease.on_lost = None


async def cancel_new_tasks(before: set[asyncio.Task]) -> int:
    """
    取消并等待 ``before`` 之后新建、还没结束的任务（System 或 shell 代码留下的后台任务），
    返回个数。要在 `LocalApp.aclose` 之前调用：它们的 finally 可能还要读写数据库、发号。
    """
    current = asyncio.current_task()
    pending = [
        t for t in asyncio.all_tasks() - before if not t.done() and t is not current
    ]
    if not pending:
        return 0
    for task in pending:
        task.cancel()
    done, still = await asyncio.wait(pending, timeout=TASK_CLEANUP_TIMEOUT)
    for task in done:
        if not task.cancelled() and (exc := task.exception()) is not None:
            logger.warning(
                _("⚠️ 后台任务 {task} 退出时出错：{err}").format(
                    task=task.get_name(), err=f"{type(exc).__name__}: {exc}"
                )
            )
    if still:
        logger.warning(
            _("⚠️ {n} 个后台任务 {timeout} 秒内没有响应取消，不再等待").format(
                n=len(still), timeout=TASK_CLEANUP_TIMEOUT
            )
        )
    return len(pending)


async def close_app_quietly(app: LocalApp, warnings: list[str] | None = None) -> None:
    """命令收尾关闭 app（写水位、释放租约、关连接）。出错只告警：用户工作已经做完（提交已经
    发生），不能因为收尾失败报成失败，也不能盖掉命令本身的异常"""
    try:
        await app.aclose()
    except Exception as e:  # noqa: BLE001
        msg = _("收尾释放租约 / 关闭连接时出错（不影响已经完成的操作）：{err}").format(
            err=f"{type(e).__name__}: {e}"
        )
        logger.warning("⚠️ " + msg)
        if warnings is not None:
            warnings.append(msg)
