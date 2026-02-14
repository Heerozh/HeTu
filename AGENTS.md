# Repository Guidelines

## Project Structure & Module Organization
- `hetu/`: core Python package (`data/`, `server/`, `system/`, `endpoint/`, `cli/`, `sourcegen/`).
- `tests/`: pytest suite and shared fixtures, including backend service fixtures in `tests/fixtures/`.
- `examples/server/first_game/`: runnable sample server app.
- `ClientSDK/`: client SDKs for `csharp/`, `unity/`, and `typescript/`.
- `benchmark/`: performance scripts and notes.
- `.github/workflows/`: CI/CD workflows. Runtime templates live in `CONFIG_TEMPLATE.yml` and `docker-compose.yaml`.

## Build, Test, and Development Commands
Use Python `3.14` (matches CI).

```bash
uv sync --group dev
uv run ruff check .
uv run ruff format .
uv run basedpyright
uv run pytest tests/
uv run pytest --cov-config=.coveragerc --cov=hetu tests/
```

- `uv sync --group dev`: install project and dev dependencies.
- `ruff check/format`: lint and format code.
- `basedpyright`: static type checking.
- `pytest`: run tests; use coverage command before PRs.

## Coding Style & Naming Conventions
- 4-space indentation, max line length `88`.
- Prefer `snake_case` for functions/modules, `PascalCase` for classes, `UPPER_SNAKE_CASE` for constants.
- Keep public API docstrings concise; bilingual Chinese/English docstrings are preferred for public interfaces.
- Keep imports and formatting tool-clean with Ruff.

## Testing Guidelines
- Frameworks: `pytest`, `pytest-asyncio`, `pytest-cov`, `pytest-timeout`.
- Name tests as `test_*.py`; place reusable fixtures under `tests/fixtures/`.
- For backend parity with CI, set `HETU_TEST_BACKENDS=redis`.
- Integration tests may require Docker services and should skip gracefully when unavailable.

## Commit & Pull Request Guidelines
- Follow commit prefixes used in history: `ENH:`, `BUG:`, `MAINT:` plus a short imperative summary.
- Keep commits focused; avoid mixing refactors with behavior changes.
- PRs should include: problem statement, key changes, test evidence (pytest/coverage output), and linked issues.
- Include usage snippets or screenshots for SDK/UI-facing changes (especially Unity/TypeScript surfaces).

## Configuration & Runtime Tips
- Local Redis example:

```bash
docker run -d --rm --name hetu-redis -p 6379:6379 redis:latest
uv run hetu start --app-file=./examples/server/first_game/src/app.py --db=redis://127.0.0.1:6379/0 --namespace=ssw --instance=dev
```
