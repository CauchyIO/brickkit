# Brickkit

Building blocks for Databricks governance—bring order to your lakehouse with unified, declarative asset management.

Brickkit is a governance framework for Databricks that provides unified, declarative infrastructure-as-code (IaC) for managing Unity Catalog objects, Genie AI Spaces, Vector Search assets, and ML models. It combines Pydantic-based type safety with the Databricks SDK to enable teams to define, validate, and deploy governed data assets at scale.

## Features

- **Unified Governance Model**: Single abstraction layer for all Databricks assets (catalogs, schemas, tables, volumes, functions, Genie Spaces, Vector Search indexes)
- **Declarative Infrastructure**: Define governance policies as Python code using Pydantic models
- **Tag-Based Access Control (ABAC)**: Implement attribute-based governance using tags on securables
- **Environment-Aware Naming**: Automatic DEV/ACC/PRD suffixes and environment-specific values
- **Proactive ML Governance**: Shift-left approach with MLflow decorators for embedding governance checks during experimentation
- **SDK-Native Execution**: Idempotent, rollback-safe operations with comprehensive error handling

## Installation

```bash
# Clone repository
git clone <repo-url>
cd brickkit

# Install with uv (recommended)
uv venv
source .venv/bin/activate
uv sync

# Or with pip
pip install -e .
```

**Requirements**: Python 3.10+

## Quick Start

```python
from brickkit import (
    Catalog, Schema, Table,
    Principal, AccessPolicy,
    GovernanceDefaults, TagDefault, RequiredTag
)
from executors import CatalogExecutor
from databricks.sdk import WorkspaceClient

# Define organization defaults
class MyOrgDefaults(GovernanceDefaults):
    @property
    def default_tags(self):
        return [
            TagDefault(key="managed_by", value="brickkit"),
            TagDefault(key="environment", value="dev"),
        ]

    @property
    def required_tags(self):
        return [RequiredTag(key="data_owner")]

# Create governed assets
defaults = MyOrgDefaults()
catalog = Catalog(name="analytics").with_defaults(defaults)
schema = Schema(name="sales", parent=catalog)
table = Table(name="transactions", parent=schema)

# Grant access
table.grant(
    Principal(name="data_analysts"),
    AccessPolicy.READER()
)

# Deploy
client = WorkspaceClient()
executor = CatalogExecutor(client, dry_run=False)
result = executor.execute(catalog)
```

## Project Structure

```
brickkit/
├── src/
│   ├── models/           # Pydantic models for Databricks assets
│   │   ├── base.py       # BaseSecurable, BaseGovernanceModel
│   │   ├── securables.py # Catalog, Schema, Volume, Function, etc.
│   │   ├── access.py     # Principal, Privilege, AccessPolicy
│   │   └── ml_models.py  # ModelVersion, ModelServingEndpoint
│   │
│   ├── executors/        # CRUD operation executors for Unity Catalog
│   │   ├── base.py       # BaseExecutor, ExecutionResult
│   │   ├── catalog_executor.py
│   │   ├── schema_executor.py
│   │   └── ...
│   │
│   ├── genie/            # Genie Space management
│   │   ├── models.py     # GenieSpace, DataSources
│   │   └── deploy_genie_spaces.py
│   │
│   ├── vector_search/    # Vector Search asset management
│   │   └── models.py     # VectorSearchEndpoint, VectorSearchIndex
│   │
│   ├── brickkit/         # Main package exports
│   │   └── defaults.py   # GovernanceDefaults, TagDefault
│   │
│   └── ml_governance.py  # Proactive ML governance with MLflow
│
└── docs/governance/      # Governance framework documentation
```

## Genie Space Deployment

```bash
# Deploy all Genie Spaces
uv run python src/genie/deploy_genie_spaces.py -p <profile>

# Dry-run to preview changes
uv run python src/genie/deploy_genie_spaces.py -p <profile> --dry-run
```

## ML Governance

Enforce governance policies during model training:

```python
from ml_governance import enforce_governance, GovernancePolicy, ModelTier

@enforce_governance(policy=GovernancePolicy(
    tier=ModelTier.PRODUCTION,
    require_signature=True,
    min_accuracy=0.95,
    require_peer_review=True
))
def train_model(X, y):
    ...
```

## Documentation

See `/docs/governance/` for comprehensive governance framework documentation:

- **DATA_GOVERNANCE_PRINCIPLES.md** - Tool-agnostic governance philosophy
- **OPERATIONAL_GOVERNANCE_PATTERNS.md** - Operational patterns and team structures
- **GOVERNANCE_ABSTRACTION_STRATEGY.md** - Tool boundaries and ecosystem definition
- **GOVERNANCE_STRATEGIES.md** - Databricks/Unity Catalog implementation details

## License

MIT License - Copyright 2026 Cauchy
