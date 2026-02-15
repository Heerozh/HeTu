# Suggested development commands (Windows/PowerShell)

## Environment setup
- `uv sync --group dev`

## Quality checks
- `uv run ruff check .`
- `uv run ruff format .`
- `uv run basedpyright`

## Tests
- `uv run pytest tests/`
- `uv run pytest tests/test_backend_basic.py`
- `uv run pytest tests/test_backend_basic.py::test_name`
- `uv run pytest --cov-config=.coveragerc --cov=hetu tests/`

## Test backend parity
- Set env var for CI parity: `HETU_TEST_BACKENDS=redis`

## Common Windows shell utilities
- list files: `Get-ChildItem`
- change dir: `Set-Location <path>`
- find text: `Select-String -Path <glob> -Pattern <regex>`
- git status: `git status`
- git diff: `git diff`