"""
Generate Markdown API reference under docs/api/ from source-code docstrings.

Usage:
    uv run python scripts/gen_api_docs.py

The script overwrites every file under docs/api/. Manual edits are lost.
"""

from __future__ import annotations

import importlib
import inspect
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

import griffe
from jinja2 import Environment, FileSystemLoader

from scripts.api_extras import EXTRAS, SKIP, TOPIC_MAP

REPO_ROOT = Path(__file__).resolve().parent.parent
DOCS_API_DIR = REPO_ROOT / "docs" / "api"
TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"

TOPIC_META: dict[str, tuple[str, str, int]] = {
    "decorators": (
        "Decorators",
        "Decorators that register components, systems, and endpoints.",
        10,
    ),
    "components": (
        "Components",
        "Component base class, fields, and permission constants.",
        20,
    ),
    "system": (
        "System",
        "System execution context and cluster grouping.",
        30,
    ),
    "endpoint": (
        "Endpoint",
        "Endpoint context and helpers for low-level RPC handlers.",
        40,
    ),
    "exceptions": (
        "Exceptions",
        "Exceptions you may catch in your application code.",
        50,
    ),
}


@dataclass
class Symbol:
    qualname: str
    short_name: str
    topic: str
    obj: object = field(repr=False)


def collect_public_symbols() -> list[Symbol]:
    """Walk hetu.__all__ plus the EXTRAS whitelist; skip submodules."""
    hetu = importlib.import_module("hetu")
    out: list[Symbol] = []

    for name in hetu.__all__:
        if name in SKIP:
            continue
        if name not in TOPIC_MAP:
            print(
                f"WARN: {name} is in __all__ but has no topic mapping",
                file=sys.stderr,
            )
            continue
        obj = getattr(hetu, name)
        out.append(
            Symbol(
                qualname=f"hetu.{name}",
                short_name=name,
                topic=TOPIC_MAP[name],
                obj=obj,
            )
        )

    for dotted, topic in EXTRAS.items():
        module_path, _, attr = dotted.rpartition(".")
        module = importlib.import_module(module_path)
        obj = getattr(module, attr)
        out.append(
            Symbol(
                qualname=dotted,
                short_name=attr,
                topic=topic,
                obj=obj,
            )
        )

    return out


SIGNATURE_WRAP_THRESHOLD = 88


def _signature_string(obj: object, name: str) -> str:
    try:
        sig = inspect.signature(obj)
    except (TypeError, ValueError, NameError):
        return name

    one_line = f"{name}{sig}"
    if len(one_line) <= SIGNATURE_WRAP_THRESHOLD:
        return one_line

    PO = inspect.Parameter.POSITIONAL_ONLY
    VP = inspect.Parameter.VAR_POSITIONAL
    KO = inspect.Parameter.KEYWORD_ONLY

    parts: list[str] = []
    prev_kind = None
    for p in sig.parameters.values():
        if prev_kind == PO and p.kind != PO:
            parts.append("/")
        if p.kind == KO and prev_kind not in (KO, VP):
            parts.append("*")
        parts.append(str(p))
        prev_kind = p.kind
    if prev_kind == PO:
        parts.append("/")

    body = ",\n    ".join(parts)
    out = f"{name}(\n    {body},\n)"
    if sig.return_annotation is not inspect.Signature.empty:
        out += f" -> {inspect.formatannotation(sig.return_annotation)}"
    return out


def _source_location(obj: object) -> tuple[str, int]:
    try:
        path = inspect.getsourcefile(obj) or "<unknown>"
        _, lineno = inspect.getsourcelines(obj)
    except (OSError, TypeError):
        return ("<unknown>", 0)

    abs_path = Path(path).resolve()
    try:
        rel = abs_path.relative_to(REPO_ROOT)
        return (str(rel).replace("\\", "/"), lineno)
    except ValueError:
        return (str(abs_path), lineno)


def _example_to_string(value: object) -> str:
    """griffe's Examples section returns list[tuple[kind, str]] in 2.x."""
    if isinstance(value, tuple) and len(value) == 2:
        return str(value[1])
    return str(value)


def _parse_docstring(obj: object) -> dict:
    raw = inspect.getdoc(obj)
    if not raw:
        return {
            "summary": None,
            "parameters": [],
            "returns": None,
            "examples": [],
            "notes": None,
        }

    docstring = griffe.Docstring(raw, parser="numpy")
    sections = docstring.parse()

    out: dict = {
        "summary": None,
        "parameters": [],
        "returns": None,
        "examples": [],
        "notes": None,
    }
    for section in sections:
        kind = section.kind.value
        if kind == "text" and out["summary"] is None:
            out["summary"] = section.value
        elif kind == "parameters":
            for p in section.value:
                out["parameters"].append(
                    {
                        "name": p.name,
                        "annotation": str(p.annotation) if p.annotation else "Any",
                        "default": p.default if p.default is not None else None,
                        "description": p.description or None,
                    }
                )
        elif kind == "returns":
            out["returns"] = "\n".join(r.description for r in section.value)
        elif kind == "examples":
            out["examples"] = [_example_to_string(v) for v in section.value]
        elif kind in ("notes", "admonition"):
            value = section.value
            if isinstance(value, str):
                out["notes"] = value
            elif hasattr(value, "description"):
                out["notes"] = value.description
            else:
                out["notes"] = None
    return out


def build_record(symbol: Symbol) -> dict:
    src_path, src_line = _source_location(symbol.obj)
    parsed = _parse_docstring(symbol.obj)
    return {
        "qualname": symbol.qualname,
        "short_name": symbol.short_name,
        "signature": _signature_string(symbol.obj, symbol.short_name),
        "source_path": src_path,
        "source_line": src_line,
        "deprecated": None,
        **parsed,
    }


def group_by_topic(symbols: list[Symbol]) -> dict[str, list[Symbol]]:
    grouped: dict[str, list[Symbol]] = defaultdict(list)
    for s in symbols:
        grouped[s.topic].append(s)
    for topic in grouped:
        grouped[topic].sort(key=lambda s: s.short_name)
    return dict(grouped)


def _env() -> Environment:
    return Environment(
        loader=FileSystemLoader(str(TEMPLATES_DIR)),
        keep_trailing_newline=True,
    )


def render_topic_page(topic: str, symbols: list[Symbol]) -> str:
    title, description, weight = TOPIC_META[topic]
    records = [build_record(s) for s in symbols]
    return _env().get_template("api_page.md.j2").render(
        topic_title=title,
        topic_description=description,
        weight=weight,
        apis=records,
    )


def render_index_page(grouped: dict[str, list[Symbol]]) -> str:
    topics = []
    for topic, symbols in sorted(
        grouped.items(), key=lambda kv: TOPIC_META[kv[0]][2]
    ):
        title, _, _ = TOPIC_META[topic]
        topics.append(
            {
                "title": title,
                "slug": topic,
                "symbols": [s.short_name for s in symbols],
            }
        )
    return _env().get_template("api_index.md.j2").render(topics=topics)


def render_coverage_page(symbols: list[Symbol]) -> str:
    missing = []
    for s in symbols:
        if not inspect.getdoc(s.obj):
            path, line = _source_location(s.obj)
            missing.append(
                {"qualname": s.qualname, "source_path": path, "source_line": line}
            )
    return _env().get_template("api_coverage.md.j2").render(
        total=len(symbols),
        missing=missing,
    )


def main() -> None:
    DOCS_API_DIR.mkdir(parents=True, exist_ok=True)
    symbols = collect_public_symbols()
    grouped = group_by_topic(symbols)

    (DOCS_API_DIR / "_index.md").write_text(
        render_index_page(grouped), encoding="utf-8"
    )
    for topic, group_symbols in grouped.items():
        (DOCS_API_DIR / f"{topic}.md").write_text(
            render_topic_page(topic, group_symbols), encoding="utf-8"
        )
    (DOCS_API_DIR / "_coverage.md").write_text(
        render_coverage_page(symbols), encoding="utf-8"
    )

    print(f"Wrote {len(grouped) + 2} files to {DOCS_API_DIR}")


if __name__ == "__main__":
    main()
