# Terraform, DABs, and Brickkit

Three tools. Different strengths. Overlapping scope. This document explains how they fit together.

## The Landscape

**Terraform with Databricks Provider**
Infrastructure as code. Declarative. State-managed. Best for resources that change rarely: metastores, catalogs, storage credentials, external locations, cluster policies. The provider covers most Databricks resources but YAML/HCL configuration has limits for complex governance logic.

**Databricks Asset Bundles (DABs)**
Project-based deployments. YAML-defined. Git-integrated. Best for resources tied to a project lifecycle: schemas, volumes, jobs, pipelines, ML models. DABs handles permissions and grants on Unity Catalog resources. It does not handle tags on UC resources or table-level governance.

**Brickkit (SDK wrapper)**
Governance models. Code-defined. Convention-driven. Best for what the other tools cannot express: tags on Unity Catalog resources, ownership changes, cross-resource conventions, validation logic. Brickkit references resources created by other tools and applies governance via SDK.

## What Each Tool Covers

The table shows capability and recommendation. **Bold** indicates the preferred tool.

| Resource | Terraform | DABs | Brickkit | Notes |
|----------|-----------|------|----------|-------|
| Metastore | **Yes** | No | Possible | Rarely changes. Terraform owns it. |
| Catalog | **Yes** | No | Possible | Shared infrastructure. Terraform preferred. |
| Schema | Possible | **Yes** | Ref only | Project-scoped. DABs owns lifecycle. |
| Table | Possible | No | **Ref only** | Created by pipelines. Brickkit governs. |
| Volume | Possible | **Yes** | Ref only | Project-scoped. DABs owns lifecycle. |
| Registered Model | Possible | **Yes** | Ref only | ML project asset. DABs owns lifecycle. |
| Storage Credential | **Yes** | No | Possible | Infrastructure. Terraform preferred. |
| External Location | **Yes** | No | Possible | Infrastructure. Terraform preferred. |
| Jobs | Possible | **Yes** | No | Project-scoped. DABs owns lifecycle. |
| Pipelines | Possible | **Yes** | No | Project-scoped. DABs owns lifecycle. |
| Grants (UC) | Possible | **Yes** | Possible | DABs if it owns the resource. |
| Tags (UC) | Partial | No | **Yes** | DABs cannot. Brickkit fills the gap. |
| Ownership | Possible | No* | **Yes** | DABs cannot change ownership. |

\* Schema ownership in DABs is fixed to the deployment user.

## The Gap

DABs creates schemas. Pipelines create tables. But:

- Tags on schemas and tables require SDK calls
- Ownership cannot be changed via DABs
- Cross-resource conventions need code, not YAML
- Validation logic exceeds what declarative config can express

Brickkit fills this gap. It does not replace DABs. It complements it.

## Two Patterns

### Pattern 1: DABs-First (Recommended for most teams)

DABs manages resource lifecycle. Brickkit applies governance to existing resources.

```
┌─────────────────────────────────────────────────────────┐
│  databricks.yml                                         │
│  - Creates schemas, volumes, models                     │
│  - Sets grants and permissions                          │
│  - Deploys jobs and pipelines                           │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│  Pipelines / Notebooks                                  │
│  - Create tables                                        │
│  - Transform data                                       │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│  Brickkit (governance layer)                            │
│  - Apply tags to schemas, tables, volumes               │
│  - Validate against conventions                         │
│  - Set ownership where DABs cannot                      │
└─────────────────────────────────────────────────────────┘
```

DABs YAML:

```yaml
# databricks.yml
resources:
  schemas:
    recommendations:
      catalog_name: ml_platform
      grants:
        - principal: ml_team
          privileges:
            - USE_SCHEMA
            - CREATE_TABLE
```

Brickkit governance (runs after DABs deploy):

```python
from acme_governance import schema_ref, table_ref, Tag

# Reference the DAB-created schema
s = schema_ref("recommendations", catalog="ml_platform")

# Reference tables created by pipelines
t = table_ref(
    "user_embeddings",
    schema=s,
    tags=[
        Tag(key="data_owner", value="ml-team"),
        Tag(key="pii", value="false"),
    ],
)

# Executor applies tags via SDK
executor.apply(s)
executor.apply(t)
```

### Pattern 2: SDK-First (Platform teams, infrastructure)

Brickkit manages the full resource lifecycle via SDK. No DABs involvement.

```
┌─────────────────────────────────────────────────────────┐
│  Brickkit models                                        │
│  - Define catalogs, schemas, tables                     │
│  - Apply conventions                                    │
│  - Execute via SDK                                      │
└─────────────────────────────────────────────────────────┘
```

```python
from acme_governance import catalog, schema, table

c = catalog("ml_platform", tags=[Tag(key="cost_center", value="ml-001")])
s = schema("recommendations", catalog=c)
t = table("user_embeddings", schema=s, columns=[...])

# Executor creates resources via SDK
executor.create(c)
executor.create(s)
executor.create(t)
```

Use this pattern for:
- Platform team managing shared infrastructure
- Resources not supported by DABs
- Complex creation logic that YAML cannot express

## Reference Models

Brickkit provides two model types:

**Full models** create resources: `Catalog`, `Schema`, `Table`

**Reference models** govern existing resources: `SchemaReference`, `TableReference`, `VolumeReference`

Reference models are lightweight. They store name, location, tags, and grants. They do not store columns, partitions, or other structural details. The resource already exists. Brickkit adds governance.

```python
# Full model: creates the table
t = Table(
    name="users",
    columns=[ColumnInfo(name="id", type_name="BIGINT"), ...],
    table_type=TableType.MANAGED,
)

# Reference model: governs existing table
t = TableReference(
    name="users",
    schema_name="core",
    catalog_name="analytics",
    tags=[Tag(key="pii", value="true")],
)
```

## Recommended Boundaries

**Use Terraform for:**
- Metastores (one per region, rarely changes)
- Catalogs (shared infrastructure)
- Storage credentials and external locations
- Cluster policies
- Workspace configuration

**Use DABs for:**
- Schemas tied to a project
- Volumes for project artifacts
- Jobs and pipelines
- ML model deployments
- Grants on DAB-managed resources

**Use Brickkit for:**
- Tags on Unity Catalog resources
- Table governance (tables come from pipelines)
- Ownership changes
- Cross-resource conventions
- Validation before deployment
- Resources DABs does not support

## Integration in CI/CD

A typical pipeline:

```
1. Terraform apply     → Infrastructure exists
2. DABs deploy         → Schemas, jobs, pipelines deployed
3. Pipeline runs       → Tables created
4. Brickkit apply      → Tags and governance applied
5. Brickkit validate   → Check conventions before merge
```

Brickkit can run as a post-deployment step or as a validation gate in CI.

## When Tools Overlap

Both DABs and Brickkit can set grants on schemas. Both Terraform and Brickkit can create catalogs. Overlap is intentional. Choose based on your workflow:

- If DABs owns the schema lifecycle, let DABs set grants
- If you need tag-based governance, use Brickkit
- If Terraform owns the catalog, don't recreate it with Brickkit

The tools compose. They do not compete. Pick the right tool for each resource based on who owns it and what governance it needs.

## Summary

| Question | Answer |
|----------|--------|
| Who creates schemas? | DABs (most teams) or Brickkit (platform teams) |
| Who creates tables? | Pipelines, notebooks, SQL |
| Who applies tags? | Brickkit (DABs cannot) |
| Who sets grants? | DABs for DAB-managed resources, Brickkit for the rest |
| Who validates conventions? | Brickkit |
| Who manages metastores/catalogs? | Terraform or Brickkit (platform teams) |
