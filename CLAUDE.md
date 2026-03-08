# CLAUDE.md

Project conventions for contributors (human and AI).

---

## Environment

| Setting     | Value                       |
| ----------- | --------------------------- |
| Environment | Pixi (`~/.pixi/bin/pixi`)   |
| Formatting  | Ruff                        |
| Testing     | Pytest                      |

IMPORTANT: Always use the full pixi path (`~/.pixi/bin/pixi`) when running
commands. The short `pixi` form is for user-facing docs only.

---

## Commands

```bash
pixi install                              # Install dependencies
pixi run -e dev test                      # Run tests
pixi run -e dev test-slurm                # Run SLURM integration tests
pixi run -e dev fmt                       # Format and lint
pixi run prefect-start                    # Start Prefect server (background)
pixi run prefect-stop                     # Stop Prefect server
pixi run install-kernel                   # Register Jupyter kernel
pixi run python script.py                 # Run scripts
```

---

## Code Style

- **DRY, YAGNI, KISS** — no premature abstractions
- **No backwards-compat shims** — when removing/renaming, delete completely
- **Fail fast** — validate inputs early, raise clear exceptions
- **Type hints** on all function signatures
- **Comments for "why"**, not "what"
- **Functions < 30 lines** ideally
- **Specific exceptions** — no bare `except:`
- **Google-style docstrings:**

```python
def example(param1: str, param2: int = 0) -> bool:
    """Short description.

    Args:
        param1: Description.
        param2: Description. Defaults to 0.

    Returns:
        Description.

    Raises:
        ValueError: When param1 is empty.
    """
```

---

## Testing

Tests mirror source structure: `tests/`

- Files: `test_<module>.py`
- Functions: `test_<function>_<scenario>`
- Cover: happy path, edge cases, error conditions
- `@pytest.mark.slurm` for tests that submit real SLURM jobs
- `@pytest.mark.slurm_gpu` for tests requiring a GPU partition
- Integration tests in `tests/integration/`

---

## Architecture

```
src/prefect_submitit/          # Prefect TaskRunner for SLURM via submitit
├── __init__.py                # Public API exports
├── runner.py                  # SlurmTaskRunner (main entry point)
├── submission.py              # SLURM job submission logic
├── executors.py               # Submitit executor wrappers
├── constants.py               # Default values and env var names
├── utils.py                   # Utility functions
└── futures/                   # Prefect future implementations
    ├── base.py                # Base future class
    ├── array.py               # Job array futures
    └── batched.py             # Batched execution futures

scripts/prefect/               # Prefect server management
├── init_postgres.sh           # One-time PostgreSQL setup
├── start_prefect_server.sh    # Start server (PostgreSQL + Prefect)
└── stop_prefect_server.sh     # Stop server

examples/                      # Demo notebooks
tests/                         # Test suite
```

---

## Git Conventions

### Branch Naming

```
feat/[name]      fix/[name]       refactor/[name]
docs/[name]      test/[name]      chore/[name]
```

### Commit Format

```
type: Brief description

Co-Authored-By: Claude <noreply@anthropic.com>
```

Types: `feat`, `fix`, `refactor`, `docs`, `test`, `chore`, `perf`, `style`

### PR Validation Order

1. `pixi run -e dev fmt`
2. `pixi run -e dev test`

---

## Pre-PR Checklist

- [ ] Code: no debug prints, no commented-out code
- [ ] Tests pass (`pixi run -e dev test`), new code has tests
- [ ] Formatted and linted (`pixi run -e dev fmt`)
- [ ] Commits are atomic with proper messages
- [ ] Self-reviewed all changes

---

## Personal Overrides

Create a `CLAUDE.local.md` file (auto-gitignored) for personal workflow
preferences such as base branch, push behavior, pixi paths, and MCP tool
configuration.
