# Contributing to prefect-submitit

Thank you for your interest in contributing! This guide will help you get
started.

## Getting Started

### Prerequisites

- Python 3.12+
- [Pixi](https://pixi.sh) package manager
- Git

### Development Setup

1. Fork and clone the repository:

   ```bash
   git clone https://github.com/<your-username>/prefect-submitit.git
   cd prefect-submitit
   ```

2. Install dependencies:

   ```bash
   pixi install
   ```

3. Install pre-commit hooks:

   ```bash
   pixi run -e dev pre-commit install
   ```

4. Verify everything works:

   ```bash
   pixi run -e dev test
   ```

## Making Changes

### Branch Naming

Create a branch from `main` with a descriptive prefix:

```
feat/description    # New features
fix/description     # Bug fixes
docs/description    # Documentation changes
test/description    # Test additions or fixes
chore/description   # Maintenance tasks
```

### Code Style

- **Formatting and linting** are handled by
  [Ruff](https://docs.astral.sh/ruff/). Run `pixi run -e dev fmt` before
  committing.
- **Type checking** uses [mypy](https://mypy.readthedocs.io/) in strict mode.
- All function signatures should have **type hints**.
- Use **Google-style docstrings**.
- Pre-commit hooks will catch most style issues automatically.

### Commit Messages

Use [conventional commits](https://www.conventionalcommits.org/):

```
type: brief description
```

Types: `feat`, `fix`, `docs`, `test`, `chore`, `refactor`, `perf`, `style`

### Tests

- Tests live in `tests/` and mirror the source structure.
- Run the full suite with `pixi run -e dev test`.
- New features and bug fixes should include tests.
- Use descriptive test names: `test_<function>_<scenario>`.

## Submitting a Pull Request

1. Ensure all checks pass locally:

   ```bash
   pixi run -e dev fmt     # Lint and format
   pixi run -e dev test    # Run tests
   ```

2. Push your branch and open a pull request against `main`.

3. Fill out the PR template — describe what changed and why.

4. A maintainer will review your PR. Please be patient and responsive to
   feedback.

## Reporting Issues

- Use the
  [issue tracker](https://github.com/dexterity-systems/prefect-submitit/issues)
  to report bugs or request features.
- Check existing issues before opening a new one.
- For bugs, include: Python version, OS, steps to reproduce, and the full error
  traceback.

## Code of Conduct

This project follows the
[Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md). By participating,
you agree to uphold this standard.

## License

By contributing, you agree that your contributions will be licensed under the
[BSD 3-Clause License](LICENSE).
