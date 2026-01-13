# Brickkit Examples

Runnable examples demonstrating brickkit governance patterns.

## Structure

```
examples/
├── 01_quickstart/           # Basic concepts
│   ├── basic_catalog.py     # Single catalog with tags
│   ├── schema_hierarchy.py  # Catalog → Schema structure
│   ├── simple_grants.py     # Principal + AccessPolicy
│   └── table_with_tags.py   # Table/Column tags, SCD2, SQL generation
│
├── 02_governance_defaults/  # Custom policies
│   ├── enterprise_defaults.py    # Org-wide defaults
│   ├── financial_services.py     # Strict regulatory compliance
│   └── minimal_defaults.py       # Lightweight governance
│
├── 03_team_governance/      # Team-based patterns
│   ├── team_with_workspaces.py   # Team, Workspace, bindings
│   ├── cross_env_access.py       # DEV reads PRD pattern
│   └── access_manager_usage.py   # Bulk grants, audit trail
│
└── 04_patterns/             # Operational patterns
    ├── physical_segregation.py   # Tiered catalogs by sensitivity
    └── zone_progression.py       # Bronze/Silver/Gold
```

## Running Examples

Set the environment before running:

```bash
export DATABRICKS_ENV=dev  # or acc, prd
```

Examples can be run directly:

```bash
python examples/01_quickstart/basic_catalog.py
```

## Prerequisites

- Python 3.10+
- `databricks-sdk` installed
- Valid Databricks workspace credentials (for execution examples)
