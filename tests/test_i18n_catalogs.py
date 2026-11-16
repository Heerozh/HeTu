"""
提交进仓库的 .po 译文的静态检查（不依赖编译出的 .mo）。

.po 由 `autolang sync` 从源码提取、`autolang translate` 交给模型翻译，译文每次发布都可能
重新生成，这里兜住模型容易犯、后果又重的错。
"""

import string
from pathlib import Path

from babel.messages.pofile import read_po

I18N_DIR = Path(__file__).resolve().parent.parent / "hetu" / "i18n"


def _brace_fields(text: str) -> set[str]:
    """str.format 会用到的字段名（{name} / {0} / {name!r:>8} 都只取 name）"""
    return {
        field for _, field, _, _ in string.Formatter().parse(text) if field is not None
    }


def _bare_percents(text: str) -> int:
    """去掉转义的 %% 之后还剩几个 %"""
    return text.replace("%%", "").count("%")


def test_percent_format_translations_keep_escaping():
    """原文走 % 格式化的条目（python-format，比如 argparse 的 help），去掉 %% 之后译文里的
    % 必须和原文一样多。多出一个没转义的 %（比如把"90%%"译成"90 %"），Python 3.14 的
    argparse 在 add_argument 时就报 badly formed help string，这个语言下 hetu 命令直接起不来"""
    catalogs = sorted(I18N_DIR.glob("*/LC_MESSAGES/messages.po"))
    assert catalogs
    bad = []
    for po in catalogs:
        with open(po, "rb") as f:
            catalog = read_po(f)
        for message in catalog:
            msgid, msgstr = message.id, message.string
            if not isinstance(msgid, str) or not isinstance(msgstr, str):
                continue  # 复数条目
            if not msgid or not msgstr or message.fuzzy:
                continue  # 表头、未翻译、待重译
            if "python-format" not in message.flags:
                continue
            if _bare_percents(msgstr) != _bare_percents(msgid):
                bad.append(f"{po.parts[-3]}: {msgstr!r}")
    assert not bad, "译文里的 % 没按原文转义：\n" + "\n".join(bad)


def test_brace_format_translations_keep_fields():
    """原文走 str.format 的条目（python-brace-format），译文的字段名必须和原文一致。
    多出或改名的字段（比如把 {comp_name} 译成 {nombre}）在 .format() 时抛 KeyError，
    这个语言下报错 / 打日志的地方反而自己崩了；漏掉的字段不崩，但日志里少了关键信息
    （v0.4.0 的西语译文就漏过一个 {comp_name}）。译文写坏、解析不了也算"""
    catalogs = sorted(I18N_DIR.glob("*/LC_MESSAGES/messages.po"))
    assert catalogs
    bad = []
    for po in catalogs:
        with open(po, "rb") as f:
            catalog = read_po(f)
        for message in catalog:
            msgid, msgstr = message.id, message.string
            if not isinstance(msgid, str) or not isinstance(msgstr, str):
                continue  # 复数条目
            if not msgid or not msgstr or message.fuzzy:
                continue  # 表头、未翻译、待重译
            if "python-brace-format" not in message.flags:
                continue
            try:
                got = _brace_fields(msgstr)
            except ValueError as e:
                bad.append(f"{po.parts[-3]}:{message.lineno}: 解析失败 {e}")
                continue
            want = _brace_fields(msgid)
            if got != want:
                bad.append(
                    f"{po.parts[-3]}:{message.lineno}: "
                    f"{sorted(want)} -> {sorted(got)}  {msgstr[:60]!r}"
                )
    assert not bad, "译文的 {} 字段和原文对不上：\n" + "\n".join(bad)
