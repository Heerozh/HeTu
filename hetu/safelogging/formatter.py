"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com

控制台用的彩色 formatter。格式移植自 Sanic 25.12 的 AutoFormatter（MIT License，
Copyright (c) 2016-present Sanic Community），换掉 Sanic 后输出保持不变。
"""

import logging
import os
import sys
from typing import Any, ClassVar

from ..common.helper import worker_ident

_END = "\033[0m"
_BOLD = "\033[1m"
_BLUE = "\033[34m"
_RED = "\033[31m"
_YELLOW = "\033[33m"
_GREY = "\033[38;5;240m"
LEVEL_COLORS = {
    logging.DEBUG: _BLUE,
    logging.WARNING: _YELLOW,
    logging.ERROR: _RED,
    logging.CRITICAL: _RED + _BOLD,
}
# 光标控制：把 ident 截到固定宽度、让正文从固定列开始，日志在终端里对齐
CONTROL_LIMIT_IDENT = "\033[1000D\033[{limit}C"
CONTROL_LIMIT_START = "\033[1000D\033[{start}C\033[K"
# LogRecord 自带的字段，加上格式里用到的，都不算 extra
DEFAULT_FIELDS = set(logging.LogRecord("", 0, "", 0, "", (), None).__dict__) | {
    "ident",
    "message",
    "asctime",
    "right",
}
_MISSING = object()


def _use_color() -> bool:
    """终端里才上颜色；设了 NO_COLOR 环境变量（https://no-color.org）就不上"""
    return bool(sys.stdout and sys.stdout.isatty()) and not os.environ.get("NO_COLOR")


class ContextAutoFormatter(logging.Formatter):
    """
    控制台用的彩色 formatter：终端里带颜色和对齐，否则是纯文本 ``ident 时间 级别: 正文``。

    进程标识（ident）优先用 record 上 ContextFilter 打好的：推荐配置里 console handler 挂在管理
    进程的 QueueListener 上，各 worker 的日志都在管理进程里格式化，不能用格式化所在进程的编号。
    record 上没有（没经过 ContextFilter）时才用本进程的。

    格式化不改 record：QueueListener 把同一个 record 依次交给各 handler，排在后面的文件 handler
    要读原样的 ident、levelname，不能被染上颜色码。

    `LOG_EXTRA` 为真时，把 record 上非标准的字段（如 ctx）逐行附在后面。深度 Debug（DEBUG > 1）
    时由 setup_logging 打开。

    Colored console formatter that prefers the ``ident`` put on the record by
    ContextFilter and never mutates the record.
    """

    IDENT: ClassVar[str] = worker_ident()
    LOG_EXTRA: ClassVar[bool] = False
    DATE_FORMAT: ClassVar[str] = "%Y-%m-%d %H:%M:%S %z"
    IDENT_LIMIT: ClassVar[int] = 5
    MESSAGE_START: ClassVar[int] = 42

    def __init__(
        self,
        fmt: str | None = None,
        datefmt: str | None = None,
        style: Any = "%",
        validate: bool = True,
        *,
        defaults: dict[str, Any] | None = None,
    ) -> None:
        self.color = _use_color()
        super().__init__(
            fmt or self._make_format(),
            datefmt or self.DATE_FORMAT,
            style,
            validate,
            defaults=defaults,
        )

    def _make_format(self) -> str:
        if not self.color:
            return "%(ident)s %(asctime)s %(levelname)s: %(message)s"
        limit = CONTROL_LIMIT_IDENT.format(limit=self.IDENT_LIMIT)
        start = CONTROL_LIMIT_START.format(start=self.MESSAGE_START)
        return (
            f"{_GREY}%(ident)s{limit} %(asctime)s {_END}%(levelname)s: {start}"
            "%(message)s"
        )

    def format(self, record: logging.LogRecord) -> str:
        # ident 不是 LogRecord 的标准字段，直接操作 __dict__
        attrs = record.__dict__
        saved_ident = attrs.get("ident", _MISSING)
        saved_levelname = record.levelname
        attrs["ident"] = attrs.get("ident") or self.IDENT
        if self.color and (color := LEVEL_COLORS.get(record.levelno)):
            record.levelname = f"{color}{record.levelname}{_END}"
        try:
            output = super().format(record)
        finally:
            record.levelname = saved_levelname
            if saved_ident is _MISSING:
                attrs.pop("ident", None)
            else:
                attrs["ident"] = saved_ident
        if self.LOG_EXTRA:
            output += self._log_extra(record)
        return output

    def _log_extra(self, record: logging.LogRecord, indent: int = 0) -> str:
        lines = [""]
        for key, value in record.__dict__.items():
            if key not in DEFAULT_FIELDS:
                lines.append(self._format_key_value(key, value, indent))
        return "\n".join(lines)

    def _format_key_value(self, key: str, value: Any, indent: int) -> str:
        indentation = " " * indent
        if self.color:
            template = f"{indentation}  {_YELLOW}{{key}}{_END}={{value}}"
        else:
            template = f"{indentation}{{key}}={{value}}"
        if isinstance(value, dict):
            nested = [template.format(key=key, value="")]
            for nested_key, nested_value in value.items():
                nested.append(
                    self._format_key_value(nested_key, nested_value, indent + 2)
                )
            return "\n".join(nested)
        return template.format(key=key, value=value)
