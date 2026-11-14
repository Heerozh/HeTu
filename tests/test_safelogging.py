"""hetu.safelogging 测试。"""

import contextvars
import logging
import threading
from pathlib import Path

import yaml

import hetu
from hetu.common import yamlloader
from hetu.safelogging import formatter as formatter_mod
from hetu.safelogging.filter import ContextFilter
from hetu.safelogging.formatter import ContextAutoFormatter


def _make_record(level: int = logging.DEBUG) -> logging.LogRecord:
    return logging.LogRecord(
        name="hetu.test",
        level=level,
        pathname=__file__,
        lineno=1,
        msg="test message",
        args=None,
        exc_info=None,
    )


def test_context_filter_runs_in_worker_thread():
    """ContextFilter 会在未设置过 ContextVar 的 worker thread 里运行
    （例如 SQLite 后端的专用线程，ContextVar 不跨线程继承），此时不应抛
    LookupError。"""
    record = _make_record()
    outcome: dict[str, object] = {}

    def run():
        try:
            outcome["result"] = ContextFilter().filter(record)
        except Exception as exc:
            outcome["error"] = exc

    thread = threading.Thread(target=run)
    thread.start()
    thread.join()

    assert "error" not in outcome, (
        f"filter 在 worker thread 抛了异常: {outcome.get('error')!r}"
    )
    assert outcome["result"] is True
    assert getattr(record, "ctx", None)  # ctx 字段应被回落值填充


def test_context_filter_uses_set_log_context():
    """正常路径：set_log_context 设置后，同 context 内 filter 应取到该值。"""
    record = _make_record()

    def run():
        ContextFilter.set_log_context("[uid|conn|MySystem]")
        return ContextFilter().filter(record)

    # 在独立 context 内运行，避免污染其它测试
    result = contextvars.Context().run(run)

    assert result is True
    assert getattr(record, "ctx", None) == "[uid|conn|MySystem]"


def test_context_auto_formatter_keeps_record_ident():
    """彩色 formatter 应使用 record 上由 ContextFilter 打好的 ident。

    这个 formatter 跑在管理进程的 QueueListener 里，要是用"本进程"的编号，每个 worker 的日志
    都会显示成 Main（原来 Sanic 的 AutoFormatter 就是这样）。"""
    record = _make_record()
    record.ident = "Srv 3"

    output = ContextAutoFormatter().format(record)

    assert "Srv 3" in output
    # 不能被就地改写，后面用 %(ident)s 的文本 formatter 还要读它
    assert getattr(record, "ident", None) == "Srv 3"
    # 只允许临时遮蔽，格式化完必须还原，不能污染下一条日志
    assert "IDENT" not in ContextAutoFormatter().__dict__


def test_context_auto_formatter_falls_back_to_process_ident():
    """没经过 ContextFilter 的 record（如进程内直连的 handler）用本进程的标识。"""
    record = _make_record()

    output = ContextAutoFormatter().format(record)

    assert ContextAutoFormatter.IDENT in output


def test_config_template_console_formatter_is_context_aware():
    """模板里 console 挂在 QueueListener 上，就必须配 ident 安全的 formatter。"""
    template = Path(hetu.__file__).parent / "CONFIG_TEMPLATE.yml"
    config = yaml.load(template.read_text(encoding="utf-8"), yamlloader.Loader)
    logging_config = config["LOGGING"]

    # console 由队列的 listener 消费 = 在管理进程里格式化
    assert "console" in logging_config["handlers"]["log_queue"]["handlers"]
    console_formatter = logging_config["handlers"]["console"]["formatter"]
    assert logging_config["formatters"][console_formatter]["class"] == (
        "hetu.safelogging.formatter.ContextAutoFormatter"
    )


def _pair_of_records(level: int = logging.INFO):
    """两条一模一样的 record（同一个时间戳），分别给两个 formatter 用"""
    first, second = _make_record(level), _make_record(level)
    second.created, second.msecs = first.created, first.msecs
    return first, second


def test_context_auto_formatter_plain_output_matches_sanic(monkeypatch):
    """
    不在终端里（不上颜色）时，输出和原来 Sanic 25.12 的 AutoFormatter 逐字一致，带不带 extra
    字段都一样。第 6 步删 Sanic 时，把这里的对照换成固定的字符串。
    """
    from sanic.logging.formatter import AutoFormatter

    monkeypatch.setattr(formatter_mod, "_use_color", lambda: False)
    monkeypatch.setattr(AutoFormatter, "ATTY", False)
    for log_extra in (False, True):
        monkeypatch.setattr(AutoFormatter, "LOG_EXTRA", log_extra)
        monkeypatch.setattr(ContextAutoFormatter, "LOG_EXTRA", log_extra)
        ours, theirs = _pair_of_records(logging.WARNING)
        for record in (ours, theirs):
            record.ctx = "[uid|conn|MySystem]"
        assert ContextAutoFormatter().format(ours) == AutoFormatter().format(theirs)


def test_context_auto_formatter_color_output(monkeypatch):
    """终端里：ident 灰色并截到 5 列，正文从第 42 列开始，级别按颜色。格式化完 record 原样不动，
    排在后面的文件 handler 读到的 levelname 不能带颜色码"""
    monkeypatch.setattr(formatter_mod, "_use_color", lambda: True)
    fmt = ContextAutoFormatter()
    record = _make_record(logging.WARNING)
    record.ident = "Srv 3"

    output = fmt.format(record)

    asctime = fmt.formatTime(record, ContextAutoFormatter.DATE_FORMAT)
    assert output == (
        f"\033[38;5;240mSrv 3\033[1000D\033[5C {asctime} \033[0m"
        "\033[33mWARNING\033[0m: \033[1000D\033[42C\033[Ktest message"
    )
    assert record.levelname == "WARNING"
    assert getattr(record, "ident", None) == "Srv 3"


def test_context_auto_formatter_does_not_leave_ident_on_record(monkeypatch):
    """没经过 ContextFilter 的 record，格式化时临时借用本进程的 ident，格式化完不留在 record 上"""
    monkeypatch.setattr(formatter_mod, "_use_color", lambda: False)
    record = _make_record()

    ContextAutoFormatter().format(record)

    assert not hasattr(record, "ident")
