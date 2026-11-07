# Contributing to prefect-submitit

Thank you for your interest in contributing! This guide will help you get
started.

Follow the [committed-content policy](AGENTS.md#committed-content) for source,
documentation, examples, and generated artifacts.

## Before You Start (for larger changes)

Small fixes and docs improvements are always welcome via PR.

If you’re planning a larger change (new features, behavior changes, refactors, or anything that touches multiple areas), please open an issue or start a discussion with the maintainers first. For substantial work, we may ask for a short design doc so we can align on approach before you invest a lot of time.
## Getting Started

### Prerequisites

- Python 3.12+
- [Pixi](https://pixi.sh) 0.83.0 or newer
- Git

### Development Setup

1. Fork and clone the repository:

   ```bash
   git clone https://github.com/<your-username>/prefect-submitit.git
   cd prefect-submitit
   ```

2. Install dependencies:

   ```bash
   pixi install -e dev
   ```

3. Install pre-commit hooks:

   ```bash
   pixi run -e dev install-hooks
   ```

4. Verify everything works:

   ```bash
   pixi run -e dev test
   ```

### Lockfiles and worktrees

Commit `pixi.lock`. Use ordinary `pixi install` and `pixi run` locally; use
`--locked` in CI and agent work that does not change dependencies. Refresh with
`pixi lock` only for intentional dependency changes, review the package changes,
and check with `pixi lock --check`. Source edits, commits, comments, tasks, and
version-only edits do not require a lock refresh.

Each worktree needs its own installed environment. Run `pixi install -e dev`
from that worktree, and use its `.pixi/envs/dev/bin/python` in editors and
notebooks. Shared package caches are fine; never copy or link `.pixi` between
worktrees. Docker intentionally uses detached environments in `/pixi-env`; use a
separate Compose project and volume per checkout, and verify its editable source
points to that checkout. See [Docker isolation](docker/README.md).

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

## Versions and releases

`[project].version` in `pyproject.toml` is the package version. Feature commits
leave it unchanged; it identifies a release series, not a source revision. Read
the installed version with `importlib.metadata.version("prefect-submitit")`.

1. Set the intended final or prerelease version and update `CHANGELOG.md`.
2. Synchronize with `pixi install -e dev`, run formatting and tests, then build
   fresh artifacts with `pixi run -e dev build-dist` and `pixi run -e dev check-dist`.
3. Run `pixi run -e dev check-release v<version>`. It verifies the wheel and
   sdist metadata before the existing GitHub release workflow can publish.
4. Commit the version and release notes, plus any required dependency lock change.
   When publication is authorized, create the matching tag and GitHub release.
5. Advance to the next planned development version after release, synchronize the
   environment, and commit that version change.

The Conda recipe builds the published PyPI sdist for the same manifest version.
After that version is published, update its verified SHA-256 in `recipe/meta.yaml`
and render/build the recipe from the repository checkout. An unpublished version
or stale checksum must fail; the recipe must not substitute local source.

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
