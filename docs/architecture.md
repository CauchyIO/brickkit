# Brickkit Architecture

Brickkit provides building blocks for Databricks governance. It is not an end-user library. Companies wrap it, configure it with their standards, and distribute their own governance library internally.

## The Three Tiers

Most organizations have three groups interacting with Unity Catalog:

**Platform Team** manages infrastructure. Metastores, catalogs, storage credentials. They may use Terraform, the Databricks UI, or their own tooling. Brickkit supports this tier but does not require it.

**Architects** define standards. Naming conventions, required tags, default owners. They create the company's governance library by wrapping brickkit with pre-configured conventions.

**Engineering and ML Teams** build data products. Schemas, tables, volumes, functions. They use the company library. They should not think about governance mechanics. It should just work.

```
Engineering Teams
    use →  company-governance (your internal library)
              wraps →  brickkit (this library)
```

## What Brickkit Provides

The models handle complexity that downstream users should not see:

- **Environment suffixes.** Names resolve to `catalog_dev`, `catalog_prd` based on `DATABRICKS_ENV`. No manual string concatenation.

- **Inheritance.** Owner flows from Catalog to Schema to Table. External location flows down. If not explicitly set, it inherits.

- **Grant propagation.** Call `schema.grant(principal, policy)`. Grants propagate to all tables, volumes, functions in that schema.

- **Tag management.** Conventions apply default tags. Required tags are validated before deployment.

- **Validation.** Pydantic catches invalid inputs early. Storage URLs are validated. Credential configurations are checked.

- **SDK translation.** Each model has `to_sdk_create_params()`. The executor handles Databricks API details.

## Conventions

A Convention bundles governance rules: default tags, required tags, naming patterns, default owner.

```python
from brickkit import Convention, TagDefault, RequiredTag

CONVENTION = Convention(
    name="acme_standards",
    default_tags=[
        TagDefault(key="managed_by", value="acme-platform"),
        TagDefault(key="cost_center", value="shared", applies_to={"SCHEMA", "TABLE"}),
    ],
    required_tags=[
        RequiredTag(key="data_owner", applies_to={"TABLE"}),
    ],
)
```

Conventions separate concerns:

- **Default tags** are applied automatically. Technical metadata like `managed_by` or `environment`.
- **Required tags** must be provided by the user. Business context like `data_owner` or `cost_center`.

The convention applies defaults. Validation checks requirements. These are distinct operations.

## Building a Company Library

Architects wrap brickkit into a company-specific library. The wrapper applies conventions at object creation.

```python
# acme_governance/__init__.py

from brickkit import Convention, Schema, Table, Tag, TagDefault, RequiredTag
from brickkit.convention import Convention
from models.base import get_current_environment

CONVENTION = Convention(
    name="acme_standards",
    default_tags=[
        TagDefault(key="managed_by", value="acme-platform"),
    ],
    required_tags=[
        RequiredTag(key="data_owner", applies_to={"TABLE"}),
    ],
)

def schema(name: str, catalog: str, **kwargs) -> Schema:
    """Create a schema with ACME governance applied."""
    s = Schema(name=name, catalog_name=catalog, **kwargs)
    CONVENTION.apply_to(s, get_current_environment())
    return s

def table(name: str, **kwargs) -> Table:
    """Create a table with ACME governance applied."""
    t = Table(name=name, **kwargs)
    CONVENTION.apply_to(t, get_current_environment())
    return t
```

Engineering teams import from `acme_governance`, not from `brickkit`:

```python
from acme_governance import schema, table, Tag

# Catalog "ml_platform" exists. Platform team manages it.
s = schema("recommendations", catalog="ml_platform")

t = table(
    "user_embeddings",
    columns=[...],
    tags=[Tag(key="data_owner", value="ml-team")],  # Required tag
)
s.add_table(t)
```

The engineering team writes five lines. Behind those lines:

- Environment suffix applied to catalog name
- Default tags added by convention
- Owner inherited from schema
- Table registered with parent schema
- Ready for executor to deploy

## Convention Propagation

For Platform teams managing full hierarchies, conventions can propagate through the tree:

```python
m = Metastore(name="main")
c = Catalog(name="analytics")
m.add_catalog(c)

m.with_convention(CONVENTION)  # Applies to catalog and all descendants
```

New children inherit the convention automatically:

```python
s = Schema(name="reports")
c.add_schema(s)  # Convention applied to schema
```

This is useful when one team controls the full hierarchy. For most organizations, the factory function pattern above is simpler.

## Validation

Validation checks required tags and naming conventions. It runs on demand, not automatically.

```python
errors = CONVENTION.validate(table)
if errors:
    raise ValueError(f"Governance validation failed: {errors}")
```

Integrate validation into your deployment pipeline. The executor can check before creating resources.

## Reference Models

Most teams use Databricks Asset Bundles or pipelines to create schemas and tables. Brickkit does not need to recreate them. Reference models govern existing resources.

```python
from acme_governance import schema_ref, table_ref, Tag

# Schema created by DABs
s = schema_ref("recommendations", catalog="ml_platform")

# Table created by a pipeline
t = table_ref(
    "user_embeddings",
    schema=s,
    tags=[Tag(key="data_owner", value="ml-team")],
)
```

Reference models are lightweight. They store name, location, tags, grants. They do not store structural details like columns or partitions. The resource exists. Brickkit adds governance.

Two model types:
- **Full models** create resources: `Catalog`, `Schema`, `Table`
- **Reference models** govern existing resources: `SchemaReference`, `TableReference`

Use full models when brickkit owns the resource lifecycle. Use reference models when DABs or pipelines own it.

## What This Is Not

Brickkit is not a deployment tool. It defines models. Executors translate models to SDK calls. But orchestration, state management, and rollback are out of scope. Use your existing deployment pipeline.

Brickkit is not a replacement for Terraform or Databricks Asset Bundles. These tools have different strengths. Terraform excels at infrastructure that changes rarely. DABs excels at project-based deployments. Brickkit fills gaps neither can cover: tags on Unity Catalog resources, cross-resource conventions, validation logic.

See [integration.md](integration.md) for how Terraform, DABs, and Brickkit work together.

Brickkit is not prescriptive about organization structure. The three-tier model described here is common. Your organization may differ. The library adapts.

## Summary

1. Brickkit provides building blocks, not an end-user API
2. Architects wrap it with company conventions
3. Engineering teams use the wrapped library
4. Models handle environment suffixes, inheritance, propagation, validation
5. Conventions separate automatic defaults from user-provided requirements
