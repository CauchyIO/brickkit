# Vector Search Demo with BrickKit

This demo shows end-to-end usage of BrickKit for deploying governed Vector Search.

## Files

| File | Purpose |
|------|---------|
| `vector_search_demo.ipynb` | **Main demo** - Complete end-to-end notebook |
| `conventions/financial_services.yml` | Governance convention (tags, rules, naming) |
| `demo_convention.py` | Quick CLI test for convention loading |
| `config/manifest.yml` | Project manifest with organization metadata |

## Running the Demo

1. Open `vector_search_demo.ipynb` in Databricks
2. Set the widgets (catalog, schema, environment, dry_run)
3. Run all cells

The notebook demonstrates:
- Loading governance conventions from YAML
- Creating sample data (or optionally fetching from World Bank API)
- Defining governed resources with BrickKit models
- Deploying with BrickKit executors (idempotent, with wait logic)
- Testing vector search
- Viewing what governance BrickKit applied automatically

## Quick Start

```python
from brickkit import load_convention, Catalog, SecurableType

# Load convention from YAML
convention = load_convention("conventions/financial_services.yml")

# Generate a compliant name
name = convention.generate_name(SecurableType.CATALOG)
# → "dev_quant_risk_analytics"

# Get the configured owner
owner = convention.get_catalog_owner()
# → Principal(name="spn_trading_platform", type=SERVICE_PRINCIPAL)

# Create catalog with convention
catalog = Catalog(name=name, owner=owner)

# Apply tags from convention
convention.apply_to(catalog)
# Adds: cost_center, team, managed_by, compliance, environment, data_classification

# Validate against rules
errors = convention.get_validation_errors(catalog)
```

## Convention File Structure

See `conventions/financial_services.yml`:

```yaml
version: "1.0"
convention: financial_services

# Name generation pattern
naming:
  pattern: "{env}_{team}_{product}"
  team: quant
  product: risk_analytics

# Ownership by securable type
ownership:
  catalog:
    type: SERVICE_PRINCIPAL
    name: spn_trading_platform
  default:
    type: GROUP
    name: grp_quant_team

# Governance rules
rules:
  - rule: catalog_must_have_sp_owner
    mode: enforced
  - rule: require_tags
    tags: [cost_center, team]
    mode: advisory

# Default tags
tags:
  cost_center: CC-TRD-4521
  team: quant

# Environment-specific overrides
tag_overrides:
  prd:
    environment: production
    data_classification: confidential
```

## Available Placeholders

| Placeholder | Description | Example |
|-------------|-------------|---------|
| `{env}` | Current environment | `dev`, `acc`, `prd` |
| `{team}` | Team from naming config | `quant` |
| `{product}` | Product from naming config | `risk_analytics` |
| `{acronym}` | Securable type acronym | `cat`, `sch`, `tbl` |
| `{name}` | Custom name passed at runtime | any string |

## Built-in Rules

| Rule | Description | Parameters |
|------|-------------|------------|
| `catalog_must_have_sp_owner` | Catalogs must have SERVICE_PRINCIPAL owner | - |
| `owner_must_be_sp_or_group` | No individual USER owners | - |
| `require_tags` | Specified tags must exist | `tags: [list]` |
| `naming_pattern` | Names must match regex | `pattern: "regex"` |

## Rule Modes

- **enforced**: Violations cause `get_validation_errors()` to return errors
- **advisory**: Violations are logged as warnings, not returned as errors by default

## Environment-Aware Features

The convention system respects `DATABRICKS_ENV`:

```bash
export DATABRICKS_ENV=PRD
```

Or set programmatically:

```python
from brickkit.models.base import set_current_environment
from brickkit.models.enums import Environment

set_current_environment(Environment.PRD)
```

This affects:
- Name generation (`{env}` placeholder)
- Owner name suffixes (if `add_environment_suffix: true`)
- Tag overrides (merges `tag_overrides.prd` into base tags)

## Loading Multiple Conventions

```python
from brickkit import load_conventions_dir

# Load all .yml/.yaml files from directory
conventions = load_conventions_dir("conventions/")

# Access by convention name
finance = conventions["financial_services"]
```

## Custom Rules

Register custom rules:

```python
from brickkit.yaml_convention import RuleDefinition, get_default_registry

def my_validator_factory(**params):
    def validator(securable, context):
        # validation logic
        return RuleValidationResult(passed=True, rule_name="my_rule")
    return validator

registry = get_default_registry()
registry.register(RuleDefinition(
    name="my_custom_rule",
    description="My custom validation",
    validator_factory=my_validator_factory,
))
```

Then use in YAML:

```yaml
rules:
  - rule: my_custom_rule
    mode: enforced
```
