"""
Generate Markdown API reference under docs/api/ from source-code docstrings.

Usage:
    uv run python scripts/gen_api_docs.py

The script overwrites every file under docs/api/. Manual edits are lost.
"""

from __future__ import annotations

import annotationlib
import ast
import dataclasses
import importlib
import inspect
import re
import sys
import textwrap
import typing as _typing
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

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

# inspect.formatannotation prints a top-level ForwardRef nicely as "Name", but
# nested inside a generic (e.g. dict[type[ForwardRef('X')], ...]) it falls back
# to repr. Strip the verbose form to just the bare name.
_FORWARDREF_REPR_RE = re.compile(r"ForwardRef\('([^']+)'(?:,[^)]*)?\)")


def _clean_forwardref(s: str) -> str:
    return _FORWARDREF_REPR_RE.sub(r"\1", s)


def _signature_string(
    obj: object,
    name: str,
    skip_self: bool = False,
    drop_first_n: int = 0,
) -> str:
    try:
        # FORWARDREF lets PEP 649 lazy annotations resolve when possible and
        # fall back to ForwardRef objects for TYPE_CHECKING-only names —
        # avoids NameError on otherwise valid signatures.
        sig = inspect.signature(
            obj, annotation_format=annotationlib.Format.FORWARDREF
        )
    except (TypeError, ValueError, NameError):
        return name

    if skip_self:
        params = list(sig.parameters.values())
        if params and params[0].name in ("self", "cls"):
            sig = sig.replace(parameters=params[1:])

    if drop_first_n > 0:
        params = list(sig.parameters.values())
        sig = sig.replace(parameters=params[drop_first_n:])

    one_line = _clean_forwardref(f"{name}{sig}")
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
    return _clean_forwardref(out)


_griffe_pkg_cache: dict[str, object] = {}


def _griffe_load_pkg(pkg_name: str):
    if pkg_name not in _griffe_pkg_cache:
        try:
            _griffe_pkg_cache[pkg_name] = griffe.load(
                pkg_name, search_paths=[str(REPO_ROOT)]
            )
        except Exception as e:
            print(f"WARN: griffe load failed for {pkg_name}: {e}", file=sys.stderr)
            _griffe_pkg_cache[pkg_name] = None
    return _griffe_pkg_cache[pkg_name]


def _griffe_class(cls: type):
    """Locate the griffe Class node corresponding to a runtime Python class."""
    module_path = cls.__module__
    pkg_name = module_path.split(".")[0]
    pkg = _griffe_load_pkg(pkg_name)
    if pkg is None:
        return None
    try:
        node = pkg
        for part in module_path.split(".")[1:]:
            node = node[part]
        return node[cls.__name__]
    except (KeyError, AttributeError):
        return None


def _griffe_attributes(cls: type) -> list[dict]:
    """Pull per-attribute docstrings from griffe (style A: string after field)."""
    g_cls = _griffe_class(cls)
    if g_cls is None:
        return []
    out: list[dict] = []
    for name, member in g_cls.members.items():
        if member.kind.value != "attribute":
            continue
        if name.startswith("_") or name.endswith("_"):
            continue
        doc = member.docstring
        description = inspect.cleandoc(doc.value) if doc else None
        annotation = str(member.annotation) if member.annotation else "Any"
        out.append(
            {
                "name": name,
                "annotation": annotation,
                "description": description,
            }
        )
    return out


_BIND_HELPER_NAMES = {"bind_first_arg_with_typehint"}
_MISSING: Any = object()


def _resolve_one_annotation(obj: Any, name: str) -> Any:
    """Resolve a single annotation on `obj`, walking parent-package namespaces
    so TYPE_CHECKING-only imports (e.g. table.py's `Backend`) still resolve.
    Returns the live type, or None if the annotation is missing/unresolvable.
    """
    try:
        ann = annotationlib.get_annotations(
            obj, format=annotationlib.Format.FORWARDREF
        )
    except Exception:
        return None
    val = ann.get(name)
    if val is None:
        return None
    if not isinstance(val, annotationlib.ForwardRef):
        return val
    module_name = getattr(obj, "__module__", "")
    if not module_name:
        return None
    parts = module_name.split(".")
    for i in range(len(parts), 0, -1):
        mod = sys.modules.get(".".join(parts[:i]))
        if mod is None:
            continue
        try:
            return val.evaluate(locals=vars(mod))
        except Exception:
            continue
    return None


def _flatten_attr_chain(node: ast.AST) -> list[str] | None:
    """Convert AST `self.a.b.c` into ['self', 'a', 'b', 'c']; None if shape differs."""
    parts: list[str] = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
        return list(reversed(parts))
    return None


def _walk_attr_chain(start_cls: type, parts: list[str]) -> object | None:
    """Statically walk a dotted attribute chain on `start_cls`. Properties are
    traversed via their fget return annotation; other names via the host class's
    annotations (covers dataclass fields and plain `name: T` declarations).
    Returns the leaf attribute (typically an unbound function), or None if any
    hop is unresolvable.
    """
    current_cls: type | None = start_cls
    for i, part in enumerate(parts):
        if current_cls is None or not inspect.isclass(current_cls):
            return None
        attr = inspect.getattr_static(current_cls, part, _MISSING)
        is_leaf = i == len(parts) - 1
        if is_leaf:
            return None if attr is _MISSING else attr
        if attr is not _MISSING and isinstance(attr, property):
            if attr.fget is None:
                return None
            current_cls = _resolve_one_annotation(attr.fget, "return")
        else:
            current_cls = _resolve_one_annotation(current_cls, part)
    return None


def _bind_first_arg_target(cls: type, prop: property) -> object | None:
    """If a @property's body is exactly
        return bind_first_arg_with_typehint(<self.x.y.z>, self)
    statically resolve <self.x.y.z> to the wrapped callable. Returns None if
    the property's shape doesn't match this pattern.
    """
    if prop.fget is None:
        return None
    try:
        src = textwrap.dedent(inspect.getsource(prop.fget))
    except (OSError, TypeError):
        return None
    try:
        tree = ast.parse(src)
    except SyntaxError:
        return None
    if not tree.body:
        return None
    func_node = tree.body[0]
    if not isinstance(func_node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        return None
    if len(func_node.body) != 1:
        return None
    stmt = func_node.body[0]
    if not isinstance(stmt, ast.Return) or stmt.value is None:
        return None
    call = stmt.value
    if not (
        isinstance(call, ast.Call)
        and isinstance(call.func, ast.Name)
        and call.func.id in _BIND_HELPER_NAMES
        and call.args
    ):
        return None
    parts = _flatten_attr_chain(call.args[0])
    if parts is None or parts[0] != "self":
        return None
    return _walk_attr_chain(cls, parts[1:])


def _collect_methods(cls: type) -> list[dict]:
    """Build sub-records for public methods defined directly on this class."""
    out: list[dict] = []
    for name, raw in vars(cls).items():
        if name.startswith("_"):
            continue

        # Special case: @property whose body is just
        #   return bind_first_arg_with_typehint(self.x.y.z, self)
        # Borrow the wrapped target's docstring and signature, dropping `self`
        # plus the bound first argument.
        if isinstance(raw, property):
            target = _bind_first_arg_target(cls, raw)
            if target is None:
                continue
            if not (inspect.isfunction(target) or inspect.iscoroutinefunction(target)):
                continue
            if not inspect.getdoc(target):
                continue
            path, line = _source_location(target)
            parsed = _parse_docstring(target)
            # Match the dropped signature args: drop the first docstring
            # parameter (the bound first_arg, e.g. table_ref).
            if parsed["parameters"]:
                parsed["parameters"] = parsed["parameters"][1:]
            out.append(
                {
                    "qualname": f"{cls.__qualname__}.{name}",
                    "short_name": name,
                    "signature": _signature_string(
                        target, name, skip_self=True, drop_first_n=1
                    ),
                    "source_path": path,
                    "source_line": line,
                    "deprecated": None,
                    **parsed,
                    "methods": [],
                }
            )
            continue

        # Unwrap classmethod / staticmethod descriptors.
        if isinstance(raw, (classmethod, staticmethod)):
            attr = raw.__func__
        else:
            attr = raw
        if not (inspect.isfunction(attr) or inspect.iscoroutinefunction(attr)):
            continue
        if not inspect.getdoc(attr):
            continue
        path, line = _source_location(attr)
        parsed = _parse_docstring(attr)
        out.append(
            {
                "qualname": f"{cls.__qualname__}.{name}",
                "short_name": name,
                "signature": _signature_string(attr, name, skip_self=True),
                "source_path": path,
                "source_line": line,
                "deprecated": None,
                **parsed,
                "methods": [],
            }
        )
    return out


def _source_location(obj: object) -> tuple[str, int]:
    target = inspect.unwrap(obj) if callable(obj) else obj
    try:
        path = inspect.getsourcefile(target) or "<unknown>"
        _, lineno = inspect.getsourcelines(target)
    except (OSError, TypeError):
        return ("<unknown>", 0)

    abs_path = Path(path).resolve()
    try:
        rel = abs_path.relative_to(REPO_ROOT)
        return (str(rel).replace("\\", "/"), lineno)
    except ValueError:
        return (str(abs_path), lineno)


def _example_item(value: object) -> dict:
    """
    griffe's Examples section returns list[tuple[kind, str]] in 2.x, where kind
    is `examples` (doctest code) or `text` (prose between code blocks).
    """
    if isinstance(value, tuple) and len(value) == 2:
        kind = value[0]
        kind_str = kind.value if hasattr(kind, "value") else str(kind)
        return {"kind": kind_str, "content": str(value[1])}
    return {"kind": "examples", "content": str(value)}


def _is_auto_dataclass_doc(obj: object, raw: str) -> bool:
    """
    Python's @dataclass synthesizes __doc__ as 'ClassName(field: T, ...)' when
    the class lacks a real docstring. Treat that as no documentation.
    """
    if not (inspect.isclass(obj) and dataclasses.is_dataclass(obj)):
        return False
    return raw.startswith(f"{obj.__name__}(")


def _parse_docstring(obj: object) -> dict:
    raw = inspect.getdoc(obj)
    if not raw or _is_auto_dataclass_doc(obj, raw):
        return {
            "summary": None,
            "parameters": [],
            "attributes": [],
            "returns": None,
            "examples": [],
            "notes": None,
        }

    docstring = griffe.Docstring(raw, parser="numpy")
    sections = docstring.parse()

    out: dict = {
        "summary": None,
        "parameters": [],
        "attributes": [],
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
        elif kind == "attributes":
            for a in section.value:
                out["attributes"].append(
                    {
                        "name": a.name,
                        "annotation": str(a.annotation) if a.annotation else "Any",
                        "description": a.description or None,
                    }
                )
        elif kind == "returns":
            parts = []
            for r in section.value:
                # numpy parser puts free-form prose in `annotation` when there
                # is no `name : type` header — fall back to it.
                text = r.description or (str(r.annotation) if r.annotation else "")
                if text:
                    parts.append(text)
            out["returns"] = "\n".join(parts) if parts else None
        elif kind == "examples":
            out["examples"] = [_example_item(v) for v in section.value]
        elif kind in ("notes", "admonition"):
            value = section.value
            if isinstance(value, str):
                out["notes"] = value
            elif hasattr(value, "description"):
                out["notes"] = value.description
            else:
                out["notes"] = None

    # For classes, prefer style-A per-attribute docstrings from source
    # (only visible via griffe — runtime introspection cannot see them).
    if inspect.isclass(obj):
        griffe_attrs = _griffe_attributes(obj)
        if griffe_attrs:
            out["attributes"] = griffe_attrs

    return out


def _resolve_bases(cls: type, symbol_index: dict[int, Symbol]) -> list[dict]:
    """
    Map cls.__bases__ to a list of {name, link?} dicts. If a base is itself a
    documented symbol (possibly under a different public name, e.g. Context →
    EndpointContext), resolve to its public name and anchor.
    """
    out: list[dict] = []
    for base in cls.__bases__:
        if base is object:
            continue
        sym = symbol_index.get(id(base))
        if sym is not None:
            anchor = sym.short_name.lower()
            out.append(
                {"name": sym.short_name, "link": f"{sym.topic}.md#{anchor}"}
            )
        else:
            out.append({"name": base.__name__, "link": None})
    return out


def build_record(symbol: Symbol, symbol_index: dict[int, Symbol]) -> dict:
    src_path, src_line = _source_location(symbol.obj)
    parsed = _parse_docstring(symbol.obj)
    is_cls = inspect.isclass(symbol.obj)
    methods = _collect_methods(symbol.obj) if is_cls else []
    # Names rendered as Methods (e.g. bind-first-arg properties) shouldn't
    # also appear as bare Attributes — griffe picks up the property name
    # without seeing the borrowed docstring underneath.
    method_names = {m["short_name"] for m in methods}
    if parsed["attributes"] and method_names:
        parsed["attributes"] = [
            a for a in parsed["attributes"] if a["name"] not in method_names
        ]
    return {
        "qualname": symbol.qualname,
        "short_name": symbol.short_name,
        "signature": _signature_string(symbol.obj, symbol.short_name),
        "source_path": src_path,
        "source_line": src_line,
        "deprecated": None,
        **parsed,
        "bases": _resolve_bases(symbol.obj, symbol_index) if is_cls else [],
        "methods": methods,
    }


_INLINE_CODE_RE = re.compile(r"`([A-Za-z_][A-Za-z0-9_]*)`")
_BARE_IDENT_RE = re.compile(r"\b[A-Za-z_][A-Za-z0-9_]*\b")


def _build_symbol_link_map(symbols: list[Symbol]) -> dict[str, str]:
    """Map each documented symbol's short name to its `topic.md#anchor` URL."""
    out: dict[str, str] = {}
    for s in symbols:
        anchor = s.short_name.lower()
        out[s.short_name] = f"{s.topic}.md#{anchor}"
    return out


def _link_inline_code(text: str | None, link_map: dict[str, str]) -> str | None:
    """Turn `Symbol` (inline code) into [`Symbol`](link) when Symbol is documented."""
    if not text:
        return text

    def repl(m: re.Match[str]) -> str:
        name = m.group(1)
        link = link_map.get(name)
        return f"[`{name}`]({link})" if link else m.group(0)

    return _INLINE_CODE_RE.sub(repl, text)


def _link_annotation(text: str | None, link_map: dict[str, str]) -> str | None:
    """Turn bare Symbol identifiers in a type annotation into [`Symbol`](link)."""
    if not text:
        return text

    def repl(m: re.Match[str]) -> str:
        name = m.group(0)
        link = link_map.get(name)
        return f"[`{name}`]({link})" if link else name

    return _BARE_IDENT_RE.sub(repl, text)


def _apply_links(rec: dict, link_map: dict[str, str]) -> None:
    """Walk a record (and its nested method records) replacing cross-refs."""
    rec["summary"] = _link_inline_code(rec.get("summary"), link_map)
    rec["returns"] = _link_inline_code(rec.get("returns"), link_map)
    rec["notes"] = _link_inline_code(rec.get("notes"), link_map)
    for p in rec.get("parameters") or []:
        p["annotation"] = _link_annotation(p.get("annotation"), link_map)
        p["description"] = _link_inline_code(p.get("description"), link_map)
    for a in rec.get("attributes") or []:
        a["annotation"] = _link_annotation(a.get("annotation"), link_map)
        a["description"] = _link_inline_code(a.get("description"), link_map)
    for m in rec.get("methods") or []:
        _apply_links(m, link_map)


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


def render_topic_page(
    topic: str,
    symbols: list[Symbol],
    symbol_index: dict[int, Symbol],
    link_map: dict[str, str],
) -> str:
    title, description, weight = TOPIC_META[topic]
    records = [build_record(s, symbol_index) for s in symbols]
    for rec in records:
        _apply_links(rec, link_map)
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
        raw = inspect.getdoc(s.obj)
        if not raw or _is_auto_dataclass_doc(s.obj, raw):
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
    symbol_index = {id(s.obj): s for s in symbols}
    link_map = _build_symbol_link_map(symbols)
    grouped = group_by_topic(symbols)

    (DOCS_API_DIR / "_index.md").write_text(
        render_index_page(grouped), encoding="utf-8"
    )
    for topic, group_symbols in grouped.items():
        (DOCS_API_DIR / f"{topic}.md").write_text(
            render_topic_page(topic, group_symbols, symbol_index, link_map),
            encoding="utf-8",
        )
    (DOCS_API_DIR / "_coverage.md").write_text(
        render_coverage_page(symbols), encoding="utf-8"
    )

    print(f"Wrote {len(grouped) + 2} files to {DOCS_API_DIR}")


if __name__ == "__main__":
    main()
