# What to do when task is completed

1. Run formatting/lint/type checks as applicable:
   - `uv run ruff format .`
   - `uv run ruff check .`
   - `uv run basedpyright`
2. Run focused tests first, then broader test suite if needed:
   - `uv run pytest <targeted tests>`
   - `uv run pytest tests/`
3. Ensure Docker/Redis-dependent tests have required environment (`HETU_TEST_BACKENDS=redis`).
4. Review changed files and ensure docstrings/comments follow project conventions.
5. Summarize behavior changes and any migration/compatibility implications.