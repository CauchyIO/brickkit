# BrickKit Project Guidelines

## Development Environment

This project uses `uv` for dependency management and running tools.

### Running Commands

**ALWAYS use `uv run` prefix for all Python tools:**

```bash
# Linting, formatting, type checking
uv run pre-commit run --all-files

# Running Python scripts
uv run python script.py

# Running tests
uv run pytest
```

## Project Structure

```
src/
├── brickkit/           # Main package - public API exports
├── models/             # Pydantic models for Unity Catalog objects
│   ├── base.py         # BaseGovernanceModel, BaseSecurable, Tag
│   ├── genie.py        # Genie Space models
│   ├── vector_search.py # Vector Search models
│   └── ...             # Other securables (tables, schemas, etc.)
├── executors/          # SDK execution layer
│   ├── base.py         # BaseExecutor with retry/rollback
│   ├── genie_executor.py
│   ├── vector_search_executor.py
│   └── ...
└── genie/              # Documentation only
```

## Code Style

- Follow existing patterns in the codebase
- Use Pydantic models with proper type hints
- Executors should inherit from `BaseExecutor[T]`
- Models should inherit from `BaseSecurable` or `BaseGovernanceModel`

## Import Conventions

Models are imported from `brickkit.models.*`:
```python
from brickkit.models.genie import GenieSpace
from brickkit.models.vector_search import VectorSearchEndpoint, VectorSearchIndex
from brickkit.models.tables import Table, Column
from brickkit.models.catalogs import Catalog
from brickkit.models.schemas import Schema
from brickkit.models.grants import AccessPolicy, Principal
from brickkit.models.base import Tag, get_current_environment
from brickkit.models.enums import Environment, IsolationMode, SecurableType
```

Executors are imported from `brickkit.executors`:
```python
from brickkit.executors import GenieSpaceExecutor, VectorSearchEndpointExecutor
```

### Bad Practice: Non-brickkit Imports

**NEVER** use bare `models.*` or `executors.*` imports without the `brickkit.` prefix, anywhere (including `examples/`):

```python
# WRONG - missing brickkit prefix
from models.genie import GenieSpace
from models.base import Tag
from models.enums import IsolationMode
from models.securables import Catalog, Schema
from models.access import AccessPolicy, Principal
from executors import CatalogExecutor

# CORRECT - full absolute imports
from brickkit.models.genie import GenieSpace
from brickkit.models.base import Tag
from brickkit.models.enums import IsolationMode
from brickkit.models.catalogs import Catalog
from brickkit.models.schemas import Schema
from brickkit.models.grants import AccessPolicy, Principal
from brickkit.executors import CatalogExecutor
```

### Bad Practice: Relative Imports

**NEVER** use relative imports anywhere in `src/`. Always use full absolute imports:

```python
# WRONG - relative imports
from .enums import SecurableType
from .base import BaseExecutor
from .loader import YamlConvention

# CORRECT - absolute imports
from brickkit.models.enums import SecurableType
from brickkit.executors.base import BaseExecutor
from brickkit.yaml_convention.loader import YamlConvention
```

This applies everywhere in `src/`, including `__init__.py` files, `TYPE_CHECKING` blocks, and lazy imports inside functions.

### Bad Practice: sys.path Manipulation

**NEVER** use `sys.path.append()`, `sys.path.insert()`, or any other `sys.path` manipulation anywhere (including `examples/`). Package resolution is handled by `uv` and the project's `pyproject.toml`.

## Dependency Management

### Pinning Rules

**Regular dependencies** (`[project] dependencies`): pin to exact version.
```toml
"pydantic==2.12.5"
"databricks-sdk[notebook]==0.96.0"
```

**Optional / dev dependencies**: use `>=X.Y.Z,<NEXT_MAJOR`.
```toml
"pytest>=9.0.2,<10"
"pre-commit>=4.5.1,<5"
```

### Packages That Must Always Be Optional

Never put these in `[project] dependencies`:
- `databricks-connect` → group: `notebook`
- `ipykernel` → group: `notebook`
- `pytest`, `pytest-cov`, `pre-commit`, `ruff`, `ty` → group: `dev`

Keep `[project.optional-dependencies]` and `[dependency-groups]` in sync.

### Updating Dependencies

Use the `/fix-deps` skill to look up the latest PyPI versions and update `pyproject.toml` automatically.

After any dependency changes, validate the environment resolves:
```bash
uv sync --extra dev
```

## Skills

Custom slash commands are defined in `.claude/commands/`. Use them to automate common workflows:

| Skill | Command | Description |
|-------|---------|-------------|
| Fix dependencies | `/fix-deps` | Look up latest PyPI versions and update `pyproject.toml` |
| Run notebook | `/run-notebook <path>` | Deploy and run a notebook on Databricks via Asset Bundles |

### `/run-notebook`

Deploys the brickkit wheel and runs a notebook as a Databricks job.

```bash
/run-notebook examples/01_quickstart/basic_catalog.py
```

What it does:
1. Derives a job resource key from the notebook filename (e.g. `basic_catalog_job`)
2. Ensures `resources/` exists and is included in `databricks.yml`
3. Creates `resources/<key>.yml` if it doesn't exist, with `env`, `git_sha`, and `run_id` base parameters and the built `.whl` as a dependency
4. Runs `databricks bundle deploy` then `databricks bundle run <key>`

## Example File Format

All Python files in `examples/` must be formatted as Databricks notebooks:

- **First line**: `# Databricks notebook source`
- **Cell separator**: `# COMMAND ----------` between logical sections (imports, configuration, each major code block)

This enables running examples interactively in both VS Code (via the Jupyter extension) and Databricks notebooks.

```python
# Databricks notebook source
"""
Example description.
"""

from brickkit.models.catalogs import Catalog

# COMMAND ----------

catalog = Catalog(name="my_catalog")

# COMMAND ----------

print(catalog)
```

**NEVER** use `#!/usr/bin/env python` shebangs in example files — they are notebooks, not standalone scripts.
