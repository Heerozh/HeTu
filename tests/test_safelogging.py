"""hetu.safelogging 测试。"""

import contextvars
import logging
import threading
from pathlib import Path

import yaml

import hetu
from hetu.common import yamlloader
from hetu.safelogging.filter import ContextFilter


def _make_record() -> logging.LogRecord:
    return logging.LogRecord(
        name="aiosqlite",
        level=logging.DEBUG,
        pathname=__file__,
        lineno=1,
        msg="test message",
        args=None,
        exc_info=None,
    )


def test_context_filter_runs_in_worker_thread():
    """ContextFilter 会在未设置过 ContextVar 的 worker thread 里运行
    （例如 aiosqlite 的 DB 线程，ContextVar 不跨线程继承），此时不应抛
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


def test_config_template_quiets_aiosqlite_logger():
    """CONFIG_TEMPLATE.yml 应把 aiosqlite 等第三方 logger 调到 DEBUG 之上。

    SQLite 后端用的 aiosqlite 会为每次数据库游标操作打 DEBUG 日志，root=DEBUG
    时会被它继承导致刷屏。调高它的级别即可屏蔽第三方噪音，同时保留 HeTu 和
    用户自己代码的 DEBUG 日志。"""
    template = Path(hetu.__file__).parent / "CONFIG_TEMPLATE.yml"
    config = yaml.load(template.read_text(encoding="utf-8"), yamlloader.Loader)
    loggers = config["LOGGING"]["loggers"]
    levels = logging.getLevelNamesMapping()

    # 第三方噪音 logger 被调高到 DEBUG 之上
    assert "aiosqlite" in loggers, "模板缺少 aiosqlite logger 条目"
    assert levels[loggers["aiosqlite"]["level"]] > levels["DEBUG"]

    # 但 root 与 HeTu 自己的 logger 仍保持 DEBUG（不能误伤自己的日志）
    assert levels[loggers["root"]["level"]] == levels["DEBUG"]
    assert levels[loggers["HeTu.root"]["level"]] == levels["DEBUG"]
