# Style and conventions

- Indentation: 4 spaces.
- Max line length: 88 (Ruff).
- Naming: `snake_case` functions, `PascalCase` classes, `UPPER_SNAKE_CASE` constants.
- Public APIs should use bilingual (Chinese/English) docstrings.
- Tests are `test_*.py`; fixtures in `tests/fixtures/`.
- Prefer type hints where appropriate; project uses basedpyright for type checking.
- Commit prefixes: `ENH:`, `BUG:`, `MAINT:`.