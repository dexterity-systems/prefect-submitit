# Your Package

A short description of what this project does.

---

## Setup Checklist

After creating a new repo from this template, update the following:

- [ ] **Rename `src/your_package/`** directory to your actual package name (Python-importable, use underscores)
- [ ] **`pyproject.toml`** — update these fields:
  - [ ] `project.name` — your PyPI package name (e.g. `"dexterity-mypackage"`)
  - [ ] `project.description`
  - [ ] `project.maintainers` — name and email
  - [ ] `tool.hatch.build.targets.wheel.packages` — point to `["src/your_package_name"]`
  - [ ] `tool.pixi.pypi-dependencies` — match the key to `project.name`
- [ ] **`src/your_package/__init__.py`** — update the docstring
- [ ] **`LICENSE`** — update the copyright holder name/org
- [ ] **`README.md`** — replace this checklist with your project's documentation
- [ ] **Delete `pixi.lock`** and run `pixi install` to generate a fresh lockfile

---

## Quick Start

**Prerequisites:** Python 3.12+, [Pixi](https://pixi.sh)

```bash
# Install Pixi (if needed)
curl -fsSL https://pixi.sh/install.sh | bash

# Clone and install
git clone https://github.com/your-org/your-repo.git
cd your-repo
pixi install

# Verify
pixi run python -c "import your_package; print('Installed successfully')"
```

---

## Development

```bash
pixi run -e dev test    # Run tests
pixi run -e dev fmt     # Format and lint
```

## Publishing to PyPI

```bash
pixi run -e dev build-dist    # Build sdist + wheel
pixi run -e dev check-dist    # Validate with twine
pixi run -e dev upload-pypi   # Upload to PyPI
```

---

## License

BSD 3-Clause. See [LICENSE](LICENSE) for details.
