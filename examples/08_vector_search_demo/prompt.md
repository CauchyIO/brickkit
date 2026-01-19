# BrickKit Vector Search Governance Demo

This demo demonstrates key brickkit governance features using a Databricks Asset Bundle (DAB) deployment pattern.

## Features Demonstrated

### 1. Databricks Asset Bundle (DAB)
- Full DAB structure with `databricks.yml`
- Environment-specific variable substitution (dev/acc/prd)
- Job workflow definition in `resources/`
- Notebook parameters sourced from DAB variables

### 2. YAML Convention Configuration
- Declarative governance rules in `conventions/financial_services.yml`
- No Python code needed to define conventions
- Supports naming patterns, ownership rules, tags, and validation

### 3. Naming Conventions
- Pattern: `{env}_{team}_{product}`
- Catalog: `dev_quant_risk_analytics`, `acc_quant_risk_analytics`, `prd_quant_risk_analytics`
- Endpoint names follow same pattern

### 4. Ownership Rules (Convention-Based)
- **Enforced**: Catalogs must be owned by Service Principals
- **Advisory**: All securables should be owned by Service Principals OR Groups (not individual users)

### 5. Principal Types
- `Principal.service_principal("spn_trading_platform")` - for SP ownership
- `Principal.group("grp_quant_team")` - for Group ownership
- `Principal.user("john.smith@acme.com")` - for User references

### 6. Tag Auto-Application
- Tags from convention YAML are automatically applied to all securables
- Environment-specific tag overrides (e.g., `data_classification: confidential` for PRD)

## Project Structure

```
08_vector_search_demo/
├── databricks.yml                # DAB bundle definition
├── conventions/
│   └── financial_services.yml    # YAML convention with governance rules
├── resources/
│   └── vector_search_workflow.yml # Job definition for ETL + VS setup
├── notebooks/
│   ├── 01_load_worldbank_metadata.ipynb  # ETL to load World Bank data
│   └── 02_vector_search_setup.ipynb      # Create VS endpoint and index
├── src/
│   └── demo.py                   # Main demo script showing brickkit usage
├── README.md                     # Quick start guide
└── prompt.md                     # This file
```

## Running the Demo

### Option 1: Deploy with DAB (Recommended)

```bash
cd examples/08_vector_search_demo

# Validate the bundle
databricks bundle validate

# Deploy to dev
databricks bundle deploy -t dev

# Run the workflow
databricks bundle run vector_search_demo -t dev
```

### Option 2: Dry Run (Local Validation)

```bash
cd examples/08_vector_search_demo
PYTHONPATH=../../src uv run python src/demo.py --dry-run
```

### Option 3: Deploy via Python Script

```bash
# Deploy to dev environment
PYTHONPATH=../../src uv run python src/demo.py --environment dev

# Deploy to production
PYTHONPATH=../../src uv run python src/demo.py --environment prd
```

## DAB Variables

The `databricks.yml` defines these variables that flow through to notebooks:

| Variable | Description | Dev Value | Prd Value |
|----------|-------------|-----------|-----------|
| `catalog` | Unity Catalog name | `dev_quant_risk_analytics` | `prd_quant_risk_analytics` |
| `schema` | Schema name | `indicators` | `indicators` |
| `vs_endpoint_name` | Vector Search endpoint | `dev_quant_risk_analytics` | `prd_quant_risk_analytics` |
| `cost_center` | Cost center tag | `CC-TRD-4521` | `CC-TRD-4521` |

## What Gets Created

| Resource | Name Pattern | Owner |
|----------|-------------|-------|
| Catalog | `{env}_quant_risk_analytics` | Service Principal |
| Schema | `indicators` | Group |
| Table | `worldbank_indicators` | Group |
| VS Endpoint | `{env}_quant_risk_analytics` | (auto) |
| VS Index | `worldbank_indicators_index` | (auto) |
| SQL Function | `search_worldbank_indicators` | (auto) |

## Convention Rules Applied

| Rule | Mode | Description |
|------|------|-------------|
| `catalog_must_have_sp_owner` | ENFORCED | Catalogs must be owned by service principals |
| `owner_must_be_sp_or_group` | ENFORCED | Securables must not be owned by individual users |
| `require_tags` | ADVISORY | Cost center and team tags should be present |

## Data Pipeline

1. **Load Metadata** (`01_load_worldbank_metadata.ipynb`)
   - Fetches ~20k indicators from World Bank API
   - Creates Delta table with Change Data Feed enabled
   - Includes `embedding_text` field for vector search
   - Supports incremental loading (resume from checkpoint)

2. **Setup Vector Search** (`02_vector_search_setup.ipynb`)
   - Creates Vector Search endpoint (if not exists)
   - Creates managed embedding index with Delta Sync
   - Creates SQL search function for easy querying
   - Tests search with example queries

## Key BrickKit Patterns Shown

### Loading Convention from YAML
```python
from brickkit import load_convention

convention = load_convention("conventions/financial_services.yml")
```

### Generating Names from Convention
```python
from brickkit import SecurableType
from brickkit.models.enums import Environment

name = convention.generate_name(SecurableType.CATALOG, Environment.DEV)
# Returns "dev_quant_risk_analytics"
```

### Getting Owners from Convention
```python
catalog_owner = convention.get_catalog_owner()
schema_owner = convention.get_owner(SecurableType.SCHEMA)
```

### Applying Convention (Adds Tags)
```python
catalog = Catalog(name=name, owner=catalog_owner)
convention.apply_to(catalog)  # Adds tags from YAML
```

### Validating Against Rules
```python
errors = convention.get_validation_errors(catalog)
if errors:
    raise ValueError(f"Validation failed: {errors}")
```

### Setting Environment
```python
from brickkit.models.base import set_current_environment
from brickkit.models.enums import Environment

set_current_environment(Environment.PRD)
```

## Usage Examples

### Search for indicators via SQL
```sql
SELECT * FROM dev_quant_risk_analytics.indicators.search_worldbank_indicators('poverty inequality')
```

### Search via Python API
```python
from databricks.vector_search.client import VectorSearchClient

client = VectorSearchClient()
index = client.get_index(
    endpoint_name="dev_quant_risk_analytics",
    index_name="dev_quant_risk_analytics.indicators.worldbank_indicators_index"
)
results = index.similarity_search(
    query_text="economic growth GDP",
    columns=["indicator_id", "indicator_name"],
    num_results=10
)
```
