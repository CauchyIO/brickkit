# BrickKit Project Guidelines

## Development Environment

This project uses `uv` for dependency management and running tools.

### Running Commands

**ALWAYS use `uv run` prefix for all Python tools:**

```bash
# Linting
uv run ruff check src/
uv run ruff check src/ --fix

# Type checking
uv run ty check src/

# Running Python scripts
uv run python script.py

# Running tests (if applicable)
uv run pytest
```

**DO NOT run tools directly without `uv run`:**
```bash
# WRONG
ruff check src/
ty check src/
python script.py

# CORRECT
uv run ruff check src/
uv run ty check src/
uv run python script.py
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

Models are imported from `models.*`:
```python
from models.genie import GenieSpace
from models.vector_search import VectorSearchEndpoint, VectorSearchIndex
from models.tables import Table, Column, GoverningTable
```

Executors are imported from `executors`:
```python
from executors import GenieSpaceExecutor, VectorSearchEndpointExecutor
```

## Testing Changes

Before committing, always run:
```bash
uv run ruff check src/
uv run ty check src/
```
