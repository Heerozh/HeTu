"""
scripts/translate_new_content.py：大文档切段翻译、译文检查。不连真的翻译接口，用假的 urlopen：把请求里
包着的那段原文照原样（或者故意改坏）当成译文返回。
"""

import json

import pytest

from scripts import translate_new_content as tnc

FRONT_MATTER = "---\ntitle: 'Doc'\nweight: 3\n---\n\n"


def make_doc(sections: int, body_bytes: int) -> str:
    """front matter + sections 个二级标题章节，每个约 body_bytes 字节，带一个里面有 `#` 行的代码块"""
    words = ("word " * (body_bytes // 5)).strip()
    body = "".join(
        f"## Section {i}\n\n{words}\n\n```bash\n# not a heading\necho {i}\n```\n\n"
        for i in range(sections)
    )
    return FRONT_MATTER + "Intro.\n\n" + body


class _Reply:
    def __init__(self, body: bytes):
        self._body = body

    def read(self) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def fake_api(monkeypatch, translate=lambda source: source, finish_reason="stop"):
    """换掉 urlopen：返回 translate(那段原文)。记下每次请求的 (payload, 原文)"""
    requests = []

    def urlopen(req, timeout):
        payload = json.loads(req.data)
        prompt = payload["messages"][1]["content"]
        source = prompt.split("---BEGIN DOCUMENT---\n", 1)[1]
        source = source.rsplit("\n---END DOCUMENT---", 1)[0]
        requests.append((payload, source))
        choice = {
            "message": {"content": translate(source)},
            "finish_reason": finish_reason,
        }
        return _Reply(json.dumps({"choices": [choice]}).encode())

    monkeypatch.setattr(tnc, "urlopen", urlopen)
    return requests


def translate(source: str) -> str:
    return tnc.translate_text(
        "http://api", "token", "model", "en", "zh_Hans", "Chinese", source
    )


def test_split_round_trips_and_cuts_only_before_headings_outside_code():
    """切开再拼回来和原文逐字相同；只在代码块外的标题前切（代码块里的 # 行不算）；front matter 在第一段"""
    doc = make_doc(sections=8, body_bytes=5000)
    chunks = tnc.split_into_chunks(doc, target_bytes=12 * 1024)

    assert len(chunks) > 1
    assert "".join(chunks) == doc
    assert chunks[0].startswith(FRONT_MATTER)
    assert all(chunk.startswith("## Section ") for chunk in chunks[1:])
    assert all(len(chunk.encode()) <= 12 * 1024 for chunk in chunks)


def test_small_doc_is_one_chunk_and_big_section_stays_whole():
    """小文档不切；单个章节本身比目标大的，自己成一段，不从中间切"""
    small = make_doc(sections=2, body_bytes=500)
    assert tnc.split_into_chunks(small) == [small]

    big = make_doc(sections=3, body_bytes=20_000)
    chunks = tnc.split_into_chunks(big, target_bytes=12 * 1024)
    assert "".join(chunks) == big
    assert [chunk.count("## Section ") for chunk in chunks[1:]] == [1, 1, 1]


def test_large_doc_is_translated_part_by_part(monkeypatch):
    """
    大文档分几次请求翻译，每次都带输出上限、告诉模型这是第几段；译文去掉了首尾的空行也照样拼回原来的样子
    """
    requests = fake_api(monkeypatch, translate=lambda source: source.strip("\n"))
    doc = make_doc(sections=8, body_bytes=5000)

    assert translate(doc) == doc
    assert len(requests) == len(tnc.split_into_chunks(doc))
    assert len(requests) > 1
    for index, (payload, _source) in enumerate(requests, start=1):
        assert payload["max_tokens"] == tnc.MAX_OUTPUT_TOKENS
        assert f"第 {index}/{len(requests)} 段" in payload["messages"][1]["content"]


def test_small_doc_is_one_request_without_part_note(monkeypatch):
    requests = fake_api(monkeypatch)
    doc = make_doc(sections=2, body_bytes=500)

    assert translate(doc) == doc
    assert len(requests) == 1
    assert "段，只翻译这一段" not in requests[0][0]["messages"][1]["content"]


def test_truncated_reply_is_rejected(monkeypatch):
    """输出达到上限被截断（finish_reason=length）：报错，不当成译文。原来只要不是空的就照写、照提交"""
    fake_api(
        monkeypatch,
        translate=lambda source: source[: len(source) // 2],
        finish_reason="length",
    )
    with pytest.raises(RuntimeError, match="截断"):
        translate(make_doc(sections=2, body_bytes=500))


@pytest.mark.parametrize(
    ("broken", "translate_badly", "match"),
    [
        ("dropped heading", lambda s: s.replace("## Section 1\n", "", 1), "标题"),
        (
            "heading level changed",
            lambda s: s.replace("## Section 1", "### Section 1", 1),
            "标题",
        ),
        ("unclosed code block", lambda s: s[: s.rindex("```")], "代码块"),
        ("whole part repeated", lambda s: s + s, "标题"),
        (
            "summarised",
            lambda s: s.split("\n\n")[0] + "\n\n" + s[-200:],
            "标题|代码块|倍",
        ),
    ],
)
def test_broken_translation_is_rejected(monkeypatch, broken, translate_badly, match):
    """漏了标题、标题级别变了、代码块没闭合、整段重复、只"总结"了几句：都报错，不写进文件"""
    fake_api(monkeypatch, translate=translate_badly)
    with pytest.raises(RuntimeError, match=match):
        translate(make_doc(sections=3, body_bytes=3000))


def test_size_ratio_is_checked_for_documents_big_enough():
    """长度之比只查不小于 SIZE_CHECK_MIN_BYTES 的原文：太小的比例不稳定"""
    prose = "word " * 1000  # 5000 字节，没有标题、代码块
    tnc.check_translation(prose, "字" * 1300)  # 3900 字节，0.78 倍
    with pytest.raises(RuntimeError, match="倍"):
        tnc.check_translation(prose, "字" * 500)  # 1500 字节，0.3 倍
    tnc.check_translation("short text", "短")


def test_failed_part_is_named(monkeypatch):
    """分段翻译时哪一段失败，报错里写明第几段"""

    def drop_heading_in_third_part(source):
        if source.startswith("## Section 4"):
            return source.replace("## Section 4\n", "", 1)
        return source

    fake_api(monkeypatch, translate=drop_heading_in_third_part)
    doc = make_doc(sections=8, body_bytes=5000)
    chunks = tnc.split_into_chunks(doc)
    index = next(
        i for i, chunk in enumerate(chunks, 1) if chunk.startswith("## Section 4")
    )
    with pytest.raises(RuntimeError, match=f"第 {index}/{len(chunks)} 段"):
        translate(doc)
