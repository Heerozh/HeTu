import pytest

from scripts.gen_api_docs import (
    _build_symbol_link_map,
    build_record,
    collect_public_symbols,
    group_by_topic,
    render_topic_page,
)


def _build_index(symbols):
    return {id(s.obj): s for s in symbols}


def test_collect_public_symbols_includes_all_and_extras():
    symbols = collect_public_symbols()
    qualnames = {s.qualname for s in symbols}

    assert any(q.endswith("define_component") for q in qualnames)
    assert any(q.endswith("BaseComponent") for q in qualnames)
    assert any(q.endswith("RaceCondition") for q in qualnames)

    assert not any(q == "hetu.data" for q in qualnames)


def test_build_record_extracts_signature_and_source():
    symbols = collect_public_symbols()
    symbol_index = _build_index(symbols)
    define_component = next(s for s in symbols if s.short_name == "define_component")

    record = build_record(define_component, symbol_index)

    assert "define_component" in record["signature"]
    assert record["source_path"].endswith("hetu/data/component.py")
    assert isinstance(record["source_line"], int)
    assert record["source_line"] > 0


def test_build_record_handles_missing_docstring():
    symbols = collect_public_symbols()
    symbol_index = _build_index(symbols)
    for s in symbols:
        record = build_record(s, symbol_index)
        if record["summary"] is None:
            return
    pytest.skip("All current symbols have docstrings; nothing to verify")


def test_group_by_topic_buckets_correctly():
    symbols = collect_public_symbols()
    grouped = group_by_topic(symbols)

    assert "decorators" in grouped
    assert any(s.short_name == "define_component" for s in grouped["decorators"])
    assert any(s.short_name == "RaceCondition" for s in grouped["exceptions"])


def test_render_topic_page_returns_markdown_with_frontmatter():
    symbols = collect_public_symbols()
    grouped = group_by_topic(symbols)
    symbol_index = _build_index(symbols)
    link_map = _build_symbol_link_map(symbols)

    md = render_topic_page(
        "decorators", grouped["decorators"], symbol_index, link_map
    )

    assert md.startswith("---\ntitle: ")
    assert "<!-- AUTO-GENERATED" in md
    assert "## `define_component" in md
