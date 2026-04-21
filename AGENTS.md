# Repository Instructions

## Testing

- Use `uv run pytest --basetemp=.pytest_tmp` when running pytest.
- For focused tests, keep the same temp override, for example:
  `uv run pytest tests/test_run_log.py -q --basetemp=.pytest_tmp`
- The default pytest temp root under `%TEMP%` may be inaccessible in this environment, so do not run pytest without an explicit repo-local `--basetemp`.
