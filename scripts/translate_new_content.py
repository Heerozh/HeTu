#!/usr/bin/env python3
"""
自动翻译默认语言内容文件，并自动提交与推送。

功能：
1) 全量扫描默认语言 contentDir 下的 Markdown 文件（排除各语言目录）。
2) 对每个源文件比较“源文件最近提交时间”与“译文最近提交时间”（第一个目标语言的）。
3) 若源文件更新更晚，则并发翻译到所有目标语言目录。大文档在标题前切成几段，一段一段翻译再拼起来
   （split_into_chunks）：一次请求的译文太长会超出模型的输出上限、被截断，长输出也更容易出错。
4) 每段译文都要过检查（check_translation）：没被截断、代码块外的标题和代码块围栏与原文一致、长度合理。
   有一篇没过就不提交，没过的不写文件。
5) 自动 git add 翻译结果，并执行 git commit + git push。

环境变量（OpenAI 兼容接口）：
- TRANSLATE_API_URL / OPENAI_BASE_URL / OPENAI_API_BASE
- TRANSLATE_API_TOKEN / OPENAI_API_KEY
- TRANSLATE_API_MODEL / OPENAI_MODEL（必填）
- TRANSLATE_MAX_WORKERS（可选，并发数）
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

REPO_ROOT = Path(__file__).resolve().parents[1]
MARKDOWN_EXTENSIONS = {".md", ".markdown"}
DEFAULT_MAX_WORKERS = 8
# 每次请求的输出上限（token），deepseek-chat 最多 8K。原来只给大于 10KB 的原文设，而且整篇一次翻：
# 现在的 advanced.md 有 70KB，译文远超这个上限，被截断后只要不是空的就照写、照提交
MAX_OUTPUT_TOKENS = 8 * 1024
# 大文档在标题前切开，每段原文尽量不超过这么多字节，译文远小于输出上限
CHUNK_TARGET_BYTES = 12 * 1024
# 译文和原文的字节数之比要在这个范围里，不然多半是漏翻了或者重复了一段。原文太小的不查（比例不稳定）
SIZE_RATIO_RANGE = (0.6, 1.6)
SIZE_CHECK_MIN_BYTES = 2 * 1024
FENCE_RE = re.compile(r"^\s*(```|~~~)")
HEADING_RE = re.compile(r"^#{1,6}\s")

# 不参与翻译的源目录：api 由 scripts/gen_api_docs.py 自动生成（重译会被覆盖），
# plans/superpowers 是开发内部产物。
EXCLUDED_SOURCE_DIRS = ("docs/en/api",)

SYSTEM_PROMPT = """你是一名专业技术文档翻译助手。
请将输入的 Hugo/Markdown 文档翻译为目标语言。
必须严格遵守：
1) 保持原始格式与结构不变：front matter 分隔符、键顺序、标题层级、列表缩进、空行、表格、引用、HTML、Hugo shortcode、代码块围栏、行内代码、链接 URL、图片路径。
2) front matter 的键名、shortcode 名称、代码、URL、路径、变量名、占位符不得翻译； `tags` 和 `glyph` 的值可以翻译成目标语言。
3) 翻译文件头部title时注意，Hugo 解析 Front Matter 时用的是 YAML, 转义是无法使用的，比如：（❌错误：title: 'de l\'imprimante '），请用双''代替（✅正确：title: 'de l''imprimante'）。
4) 仅翻译自然语言文本。
5) 仅输出翻译后的完整文档，不要解释，不要添加代码围栏。
6) 不要复述输入中的分隔标记（例如 ---BEGIN DOCUMENT--- / ---END DOCUMENT---），它们只是包裹源文档用的，不属于文档内容。"""


@dataclass
class LanguageConfig:
    key: str
    content_dir: str
    language_name: str


@dataclass
class TranslationTask:
    src_path: str
    source_text: str
    rel_path: str
    target: LanguageConfig


def eprint(message: str) -> None:
    print(message, file=sys.stderr)


def run_git(args: list[str], text: bool = True) -> subprocess.CompletedProcess[Any]:
    return subprocess.run(
        ["git", *args],
        cwd=REPO_ROOT,
        text=text,
        capture_output=True,
        check=False,
    )


def normalize_rel_path(path: str) -> str:
    return path.replace("\\", "/").lstrip("./").strip("/")


def is_subpath(path: str, base: str) -> bool:
    path = normalize_rel_path(path)
    base = normalize_rel_path(base)
    return path == base or path.startswith(base + "/")


def get_relative_subpath(path: str, base: str) -> str:
    path = normalize_rel_path(path)
    base = normalize_rel_path(base)
    if not is_subpath(path, base) or path == base:
        return ""
    return path[len(base) + 1 :]


def list_all_repo_files() -> list[str]:
    proc = run_git(["ls-files"])
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or "git ls-files 执行失败")

    files = [
        normalize_rel_path(line) for line in proc.stdout.splitlines() if line.strip()
    ]
    return files


def collect_default_content_files(
    all_repo_files: list[str], default_content_dir: str, target_content_dirs: list[str]
) -> list[str]:
    sources: list[str] = []

    for rel_path in all_repo_files:
        suffix = Path(rel_path).suffix.lower()
        if suffix not in MARKDOWN_EXTENSIONS:
            continue

        if not is_subpath(rel_path, default_content_dir):
            continue

        if any(is_subpath(rel_path, d) for d in target_content_dirs):
            continue

        if any(is_subpath(rel_path, d) for d in EXCLUDED_SOURCE_DIRS):
            continue

        rel = get_relative_subpath(rel_path, default_content_dir)
        if not rel:
            continue

        sources.append(rel_path)

    return sources


def read_repo_file(path: str) -> str:
    abs_path = REPO_ROOT / normalize_rel_path(path)
    with abs_path.open("r", encoding="utf-8") as f:
        return f.read()


def get_last_commit_timestamp(path: str) -> int:
    rel = normalize_rel_path(path)
    proc = run_git(["log", "-1", "--format=%ct", "--", rel])
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or f"获取提交时间失败: {rel}")

    value = (proc.stdout or "").strip()
    if not value:
        return 0

    try:
        return int(value)
    except ValueError as exc:
        raise RuntimeError(f"提交时间格式异常: {rel} -> {value}") from exc


def resolve_api_endpoint(base_url: str) -> str:
    base_url = base_url.strip().rstrip("/")
    if not base_url:
        return ""
    if base_url.endswith("/chat/completions"):
        return base_url
    if base_url.endswith("/v1"):
        return base_url + "/chat/completions"
    return base_url + "/v1/chat/completions"


def resolve_api_env() -> tuple[str, str, str]:
    api_url = (
        os.getenv("TRANSLATE_API_URL")
        or os.getenv("OPENAI_BASE_URL")
        or os.getenv("OPENAI_API_BASE")
        or ""
    )
    api_token = os.getenv("TRANSLATE_API_TOKEN") or os.getenv("OPENAI_API_KEY") or ""
    model = os.getenv("TRANSLATE_API_MODEL") or os.getenv("OPENAI_MODEL") or ""

    endpoint = resolve_api_endpoint(api_url)

    if not endpoint:
        raise RuntimeError(
            "缺少 API URL。请设置 TRANSLATE_API_URL（或 OPENAI_BASE_URL / OPENAI_API_BASE），带v1"
        )
    if not api_token:
        raise RuntimeError(
            "缺少 API Token。请设置 TRANSLATE_API_TOKEN（或 OPENAI_API_KEY）"
        )
    if not model:
        raise RuntimeError(
            "缺少 API Model。请设置 TRANSLATE_API_MODEL（或 OPENAI_MODEL），例如 deepseek-chat"
        )

    return endpoint, api_token, model


def unwrap_code_fence_if_needed(text: str) -> str:
    stripped = text.strip()
    if stripped.startswith("```") and stripped.endswith("```"):
        lines = stripped.splitlines()
        if len(lines) >= 2:
            return "\n".join(lines[1:-1])
    return text


DOCUMENT_BEGIN_MARKER = "---BEGIN DOCUMENT---"
DOCUMENT_END_MARKER = "---END DOCUMENT---"


def strip_document_markers(text: str) -> str:
    rstripped = text.rstrip()
    if rstripped.endswith(DOCUMENT_END_MARKER):
        text = rstripped[: -len(DOCUMENT_END_MARKER)]

    lstripped = text.lstrip()
    if lstripped.startswith(DOCUMENT_BEGIN_MARKER):
        text = lstripped[len(DOCUMENT_BEGIN_MARKER) :]

    return text


def sanitize_translation(text: str) -> str:
    while True:
        prev = text
        text = unwrap_code_fence_if_needed(text)
        text = strip_document_markers(text)
        if text == prev:
            return text


def _front_matter_end(lines: list[str]) -> int:
    """front matter（--- 或 +++ 包着的）之后第一行的行号，没有 front matter 返回 0"""
    if not lines or lines[0].strip() not in ("---", "+++"):
        return 0
    delimiter = lines[0].strip()
    for i in range(1, len(lines)):
        if lines[i].strip() == delimiter:
            return i + 1
    return 0


def document_outline(text: str) -> tuple[list[int], int]:
    """代码块外每个标题的级别（按顺序），和代码块围栏的行数。front matter 不算"""
    levels: list[int] = []
    fences = 0
    in_fence = False
    lines = text.splitlines()
    for line in lines[_front_matter_end(lines) :]:
        if FENCE_RE.match(line):
            fences += 1
            in_fence = not in_fence
        elif not in_fence and HEADING_RE.match(line):
            levels.append(len(line) - len(line.lstrip("#")))
    return levels, fences


def check_translation(source: str, translated: str) -> None:
    """
    译文和原文对一下结构，对不上就抛 RuntimeError：代码块外的标题（个数、级别、顺序）和代码块围栏的行数
    要一样；原文不小于 SIZE_CHECK_MIN_BYTES 时，长度之比要在 SIZE_RATIO_RANGE 里。

    能抓到的是漏了段、多了段、整段重复、代码块没闭合、"总结"了而不是逐句翻。在段落里多出、少了几句抓不到。
    """
    src_levels, src_fences = document_outline(source)
    dst_levels, dst_fences = document_outline(translated)
    if dst_levels != src_levels:
        raise RuntimeError(
            f"译文的标题和原文对不上：原文 {len(src_levels)} 个，译文 {len(dst_levels)} 个"
            "（个数一样就是级别或顺序不同）"
        )
    if dst_fences != src_fences:
        raise RuntimeError(
            f"译文的代码块围栏和原文对不上：原文 {src_fences} 行，译文 {dst_fences} 行"
        )
    src_bytes = len(source.encode("utf-8"))
    if src_bytes >= SIZE_CHECK_MIN_BYTES:
        ratio = len(translated.encode("utf-8")) / src_bytes
        low, high = SIZE_RATIO_RANGE
        if not low <= ratio <= high:
            raise RuntimeError(
                f"译文长度是原文的 {ratio:.2f} 倍，不在 {low}~{high} 之间，多半漏翻了或者重复了一段"
            )


def split_into_chunks(text: str, target_bytes: int = CHUNK_TARGET_BYTES) -> list[str]:
    """
    把文档在标题前切成几段，每段尽量不超过 target_bytes 字节；单个章节本身就更大的，自己成一段。只在代码块外
    的标题行前切，front matter 留在第一段。几段直接拼起来和原文逐字相同。
    """
    lines = text.splitlines(keepends=True)
    starts = [0]
    in_fence = False
    for i in range(_front_matter_end(lines), len(lines)):
        line = lines[i]
        if FENCE_RE.match(line):
            in_fence = not in_fence
        elif not in_fence and i > 0 and HEADING_RE.match(line):
            starts.append(i)
    ends = [*starts[1:], len(lines)]
    chunks: list[str] = []
    for start, end in zip(starts, ends, strict=True):
        section = "".join(lines[start:end])
        if chunks and len((chunks[-1] + section).encode("utf-8")) <= target_bytes:
            chunks[-1] += section
        else:
            chunks.append(section)
    return chunks


def match_edge_newlines(source: str, translated: str) -> str:
    """译文开头、结尾的换行和原文一样多：几段译文直接拼起来，段和段之间的空行不多不少"""
    lead = len(source) - len(source.lstrip("\n"))
    trail = len(source) - len(source.rstrip("\n"))
    return "\n" * lead + translated.strip("\n") + "\n" * trail


def request_translation(
    endpoint: str,
    token: str,
    model: str,
    source_lang: str,
    target_lang_key: str,
    target_lang_name: str,
    source_text: str,
    part: tuple[int, int] | None = None,
) -> str:
    """翻译一段（或者一整篇小文档）：一次请求，没被截断、过了 check_translation 才返回"""
    part_note = ""
    if part is not None:
        index, total = part
        part_note = (
            f"（这是一篇长文档在标题前切开的第 {index}/{total} 段，只翻译这一段，"
            "不要补出前后的内容，也不要加 front matter）"
        )
    user_prompt = (
        f"源语言：{source_lang}\n"
        f"目标语言：{target_lang_name} ({target_lang_key})\n\n"
        f"请翻译下面的文档{part_note}，严格保持原始格式：\n"
        "---BEGIN DOCUMENT---\n"
        f"{source_text}\n"
        "---END DOCUMENT---"
    )

    payload = {
        "model": model,
        "temperature": 1,
        "max_tokens": MAX_OUTPUT_TOKENS,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
    }

    req = Request(
        endpoint,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
        method="POST",
    )

    try:
        with urlopen(req, timeout=600) as resp:
            body = resp.read().decode("utf-8")
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"翻译接口 HTTP {exc.code}: {detail}") from exc
    except URLError as exc:
        raise RuntimeError(f"翻译接口连接失败: {exc}") from exc

    try:
        parsed = json.loads(body)
        choice = parsed["choices"][0]
        translated = choice["message"]["content"]
        finish_reason = choice.get("finish_reason")
    except (
        KeyError,
        IndexError,
        TypeError,
        AttributeError,
        json.JSONDecodeError,
    ) as exc:
        raise RuntimeError(f"翻译接口返回格式异常: {body}") from exc

    if finish_reason == "length":
        raise RuntimeError(f"译文被截断：输出达到上限 max_tokens={MAX_OUTPUT_TOKENS}")
    if finish_reason == "content_filter":
        raise RuntimeError("译文被内容过滤截掉了")
    if not isinstance(translated, str) or not translated.strip():
        raise RuntimeError("翻译接口返回空内容")

    translated = match_edge_newlines(source_text, sanitize_translation(translated))
    check_translation(source_text, translated)
    return translated


def translate_text(
    endpoint: str,
    token: str,
    model: str,
    source_lang: str,
    target_lang_key: str,
    target_lang_name: str,
    source_text: str,
) -> str:
    """翻译一篇文档：大的在标题前切成几段，一段一段翻再拼起来。任何一段没过检查，整篇失败"""
    if not source_text.strip():
        return source_text
    chunks = split_into_chunks(source_text)
    total = len(chunks)
    parts: list[str] = []
    for index, chunk in enumerate(chunks, start=1):
        try:
            parts.append(
                request_translation(
                    endpoint,
                    token,
                    model,
                    source_lang,
                    target_lang_key,
                    target_lang_name,
                    chunk,
                    part=(index, total) if total > 1 else None,
                )
            )
        except RuntimeError as exc:
            if total == 1:
                raise
            raise RuntimeError(f"第 {index}/{total} 段: {exc}") from exc
    translated = "".join(parts)
    check_translation(source_text, translated)
    return translated


def read_text_exact(path: Path) -> str:
    with path.open("r", encoding="utf-8", newline="") as f:
        return f.read()


def write_text_exact(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        f.write(content)


def stage_files(paths: list[str]) -> None:
    if not paths:
        return
    proc = run_git(["add", "--", *paths])
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or "git add 执行失败")


def commit_and_push(commit_message: str) -> None:
    proc_commit = run_git(["commit", "-m", commit_message])
    if proc_commit.returncode != 0:
        stderr = (proc_commit.stderr or "").strip()
        stdout = (proc_commit.stdout or "").strip()
        merged = "\n".join([s for s in [stderr, stdout] if s]).strip()
        if "nothing to commit" in merged.lower() or "没有要提交的内容" in merged:
            return
        raise RuntimeError(merged or "git commit 执行失败")

    proc_push = run_git(["push"])
    if proc_push.returncode != 0:
        raise RuntimeError(proc_push.stderr.strip() or "git push 执行失败")


def resolve_max_workers(total_tasks: int) -> int:
    if total_tasks <= 0:
        return 1

    default_workers = min(DEFAULT_MAX_WORKERS, total_tasks)
    raw = (os.getenv("TRANSLATE_MAX_WORKERS") or "").strip()
    if not raw:
        return default_workers

    try:
        configured = int(raw)
    except ValueError as exc:
        raise RuntimeError("TRANSLATE_MAX_WORKERS 必须是正整数，例如 4 或 8") from exc

    if configured <= 0:
        raise RuntimeError("TRANSLATE_MAX_WORKERS 必须大于 0")

    return min(configured, total_tasks)


def process_translation_task(
    endpoint: str,
    token: str,
    model: str,
    source_lang: str,
    task: TranslationTask,
) -> tuple[str, bool]:
    target_path = normalize_rel_path(f"{task.target.content_dir}/{task.rel_path}")
    target_abs = REPO_ROOT / target_path

    try:
        translated = translate_text(
            endpoint=endpoint,
            token=token,
            model=model,
            source_lang=source_lang,
            target_lang_key=task.target.key,
            target_lang_name=task.target.language_name,
            source_text=task.source_text,
        )
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(
            f"翻译失败 {task.src_path} -> {task.target.key}: {exc}"
        ) from exc

    existing = ""
    if target_abs.exists():
        try:
            existing = read_text_exact(target_abs)
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"读取目标文件失败 {target_path}: {exc}") from exc

    if existing == translated:
        return target_path, False

    try:
        write_text_exact(target_abs, translated)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"写入目标文件失败 {target_path}: {exc}") from exc

    return target_path, True


def main() -> int:
    force = "--force" in sys.argv[1:]

    default_lang = "en"
    default_content_dir = "docs/en/"
    targets = [LanguageConfig("zh_Hans", "docs/zh/", "Chinese")]

    if not targets:
        print("[translate-hook] 未检测到目标语言，跳过")
        return 0

    # 选第一个目标作为"新鲜度基准"：源文件比该目标译文更新，则触发翻译。
    # 单目标时这就是该目标本身；多目标时这只是触发条件，实际写入仍按各目标独立判断。
    freshness_target = targets[0]

    try:
        all_repo_files = list_all_repo_files()
    except Exception as exc:  # noqa: BLE001
        eprint(f"[translate-hook] 扫描仓库文件失败: {exc}")
        return 1

    target_content_dirs = [t.content_dir for t in targets]
    source_files = collect_default_content_files(
        all_repo_files, default_content_dir, target_content_dirs
    )

    if not source_files:
        print("[translate-hook] 未发现默认语言 content 源文件，跳过")
        return 0

    source_need_translate: list[str] = []
    if force:
        source_need_translate = list(source_files)
        print(
            f"[translate-hook] --force 模式：强制重译全部 {len(source_need_translate)} 个源文件"
        )
    else:
        for src_path in source_files:
            rel = get_relative_subpath(src_path, default_content_dir)
            if not rel:
                continue
            target_path = normalize_rel_path(f"{freshness_target.content_dir}/{rel}")

            try:
                src_ts = get_last_commit_timestamp(src_path)
                target_ts = get_last_commit_timestamp(target_path)
            except Exception as exc:  # noqa: BLE001
                eprint(f"[translate-hook] 读取提交时间失败 {src_path}: {exc}")
                return 1

            if src_ts > target_ts:
                source_need_translate.append(src_path)

    if not source_need_translate:
        print("[translate-hook] 所有译文均是最新，无需翻译")
        return 0

    try:
        endpoint, token, model = resolve_api_env()
    except Exception as exc:  # noqa: BLE001
        eprint(f"[translate-hook] 环境变量错误: {exc}")
        return 1

    print(
        "[translate-hook] 待翻译源文件: "
        + f"{len(source_need_translate)}，目标语言: {', '.join(t.key for t in targets)}"
    )

    generated_or_updated: list[str] = []
    translation_tasks: list[TranslationTask] = []

    for src_path in source_need_translate:
        try:
            source_text = read_repo_file(src_path)
        except Exception as exc:  # noqa: BLE001
            eprint(f"[translate-hook] 读取源文件失败 {src_path}: {exc}")
            return 1

        rel = get_relative_subpath(src_path, default_content_dir)
        if not rel:
            eprint(f"[translate-hook] 源路径异常，无法计算相对路径: {src_path}")
            return 1

        for target in targets:
            target_path = normalize_rel_path(f"{target.content_dir}/{rel}")
            print(
                f"[translate-hook] 翻译 {src_path} -> {target_path} ({target.language_name})"
            )
            translation_tasks.append(
                TranslationTask(
                    src_path=src_path,
                    source_text=source_text,
                    rel_path=rel,
                    target=target,
                )
            )

    if not translation_tasks:
        print("[translate-hook] 无翻译任务，跳过")
        return 0

    try:
        max_workers = resolve_max_workers(len(translation_tasks))
    except Exception as exc:  # noqa: BLE001
        eprint(f"[translate-hook] 并发配置错误: {exc}")
        return 1

    print(
        f"[translate-hook] 并发执行翻译任务: {len(translation_tasks)}，"
        + f"max_workers={max_workers}"
    )

    future_map: dict[Future[tuple[str, bool]], TranslationTask] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for task in translation_tasks:
            future = executor.submit(
                process_translation_task,
                endpoint,
                token,
                model,
                default_lang,
                task,
            )
            future_map[future] = task

        failures = 0
        for future in as_completed(future_map):
            try:
                target_path, changed = future.result()
            except Exception as exc:  # noqa: BLE001
                failures += 1
                eprint(f"[translate-hook] 并发任务失败: {exc}")
                continue

            if changed:
                generated_or_updated.append(target_path)

    if failures:
        eprint(
            f"[translate-hook] {failures} 个翻译任务失败，不提交。没过检查的没有写文件，"
            "其余写好的译文留在工作区"
        )
        return 1

    changed_unique = sorted(set(generated_or_updated))
    if not changed_unique:
        print("[translate-hook] 翻译结果无变更")
        return 0

    try:
        stage_files(changed_unique)
    except Exception as exc:  # noqa: BLE001
        eprint(f"[translate-hook] git add 失败: {exc}")
        return 1

    source_names = [Path(p).name for p in source_need_translate]
    commit_message = "DOC: AI translated " + " ".join(source_names)
    try:
        commit_and_push(commit_message)
    except Exception as exc:  # noqa: BLE001
        eprint(f"[translate-hook] 提交或推送失败: {exc}")
        return 1

    print(
        f"[translate-hook] 已提交并推送翻译结果: {len(changed_unique)} 个文件，"
        + f"commit='{commit_message}'"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
