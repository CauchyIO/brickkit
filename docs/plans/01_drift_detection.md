# Plan: Diff-Based Apply and Drift Detection

## Overview

Transform brickkit from "apply and hope" to a state-aware governance engine. The SDK wrapper should:

1. Collect actual state from Databricks
2. Compare against declared state (models + conventions)
3. Detect drift (actual differs from expected)
4. Apply only changes (idempotent)
5. Report what changed, what drifted, what's missing

## Current State

Executors have `create()`, `update()`, `delete()`, `exists()`. They apply changes but don't compare states. No drift detection. No reconciliation.

## Design Principles

- **Read before write.** Always fetch current state before applying.
- **Diff, don't replace.** Apply only what changed.
- **Report everything.** Make state visible.
- **Convention-aware.** Drift includes missing convention tags.

---

## SDK vs SQL Operations

Not all operations use the Databricks SDK. Some require SQL execution via a warehouse connection.

### SDK-Based Operations (StateReader can use SDK)
| Operation | SDK Method |
|-----------|------------|
| Catalog state | `client.catalogs.get()` |
| Schema state | `client.schemas.get()` |
| Table metadata | `client.tables.get()` |
| Volume state | `client.volumes.read()` |
| Grants | `client.grants.get()` |
| Tags | `client.tags.list()` |
| Storage credentials | `client.storage_credentials.get()` |
| External locations | `client.external_locations.get()` |

### SQL-Based Operations (StateReader needs SQLExecutor)
| Operation | SQL Required | Reason |
|-----------|--------------|--------|
| Row filter state | `DESCRIBE TABLE EXTENDED` | SDK doesn't expose row filters |
| Column mask state | `DESCRIBE TABLE EXTENDED` | SDK doesn't expose column masks |
| Function definition | `DESCRIBE FUNCTION EXTENDED` | SDK only returns metadata |
| Table creation | `CREATE TABLE` DDL | SDK limited for full control |
| Row filter apply | `ALTER TABLE SET ROW FILTER` | No SDK support |
| Column mask apply | `ALTER TABLE ALTER COLUMN SET MASK` | No SDK support |

### SQLExecutor Dependency

The `StateReader` and `Reconciler` need a `SQLExecutor` for SQL-based operations:

```python
class SQLExecutor:
    """Executes SQL statements via warehouse connection."""

    def __init__(self, client: WorkspaceClient, warehouse_id: str):
        self.client = client
        self.warehouse_id = warehouse_id

    def execute(self, sql: str) -> List[Dict[str, Any]]:
        """Execute SQL and return results."""

    def describe_table_extended(self, full_name: str) -> TableExtendedInfo:
        """Get extended table info including row filter and column masks."""

    def describe_function(self, full_name: str) -> FunctionInfo:
        """Get function definition."""
```

This means drift detection requires both:
1. `WorkspaceClient` for SDK operations
2. `warehouse_id` for SQL operations

---

## Component 1: State Reader

New module: `src/brickkit/state/reader.py`

Reads actual state from Databricks for each resource type.

```python
class StateReader:
    """Reads actual state from Databricks."""

    def __init__(self, client: WorkspaceClient, sql_executor: Optional[SQLExecutor] = None):
        self.client = client
        self.sql_executor = sql_executor  # Required for row filters, column masks, functions

    def read_catalog(self, name: str) -> Optional[CatalogState]:
        """Read catalog state including tags, grants, owner."""

    def read_schema(self, catalog: str, name: str) -> Optional[SchemaState]:
        """Read schema state including tags, grants, owner."""

    def read_table(self, catalog: str, schema: str, name: str) -> Optional[TableState]:
        """Read table state including tags, grants, row_filter, column_masks."""

    def read_volume(self, catalog: str, schema: str, name: str) -> Optional[VolumeState]:
        """Read volume state."""

    def read_function(self, catalog: str, schema: str, name: str) -> Optional[FunctionState]:
        """Read function state."""

    def read_storage_credential(self, name: str) -> Optional[StorageCredentialState]:
        """Read storage credential state."""

    def read_external_location(self, name: str) -> Optional[ExternalLocationState]:
        """Read external location state."""

    # Bulk operations for efficiency
    def read_all_schemas(self, catalog: str) -> List[SchemaState]:
        """Read all schemas in a catalog."""

    def read_all_tables(self, catalog: str, schema: str) -> List[TableState]:
        """Read all tables in a schema."""
```

State classes capture actual state:

```python
@dataclass
class TableState:
    """Actual state of a table in Databricks."""
    catalog_name: str
    schema_name: str
    name: str
    table_type: str
    owner: str
    tags: Dict[str, str]
    grants: List[GrantState]
    row_filter: Optional[str]  # Function name
    column_masks: Dict[str, str]  # column -> function name
    columns: List[ColumnState]
    created_at: datetime
    updated_at: datetime

@dataclass
class GrantState:
    principal: str
    privileges: Set[str]
```

---

## Component 2: State Differ

New module: `src/brickkit/state/differ.py`

Compares declared state (models) against actual state.

```python
@dataclass
class Diff:
    """Difference between declared and actual state."""
    resource_type: str
    resource_name: str
    changes: List[Change]

@dataclass
class Change:
    field: str
    declared: Any
    actual: Any
    action: Literal["add", "remove", "modify"]

class StateDiffer:
    """Compares declared models against actual state."""

    def diff_table(self, declared: Table, actual: Optional[TableState]) -> Diff:
        """Compare declared table against actual state."""

    def diff_schema(self, declared: Schema, actual: Optional[SchemaState]) -> Diff:
        """Compare declared schema against actual state."""

    def diff_catalog(self, declared: Catalog, actual: Optional[CatalogState]) -> Diff:
        """Compare declared catalog against actual state."""

    # Convention-aware diffing
    def diff_with_convention(
        self,
        declared: BaseSecurable,
        actual: Optional[Any],
        convention: Convention
    ) -> Diff:
        """Diff including convention requirements."""
```

Example diff output:

```python
Diff(
    resource_type="TABLE",
    resource_name="ml_platform.recommendations.user_embeddings",
    changes=[
        Change(field="tags.data_owner", declared="ml-team", actual=None, action="add"),
        Change(field="tags.pii", declared="false", actual="true", action="modify"),
        Change(field="grants.analysts", declared={"SELECT"}, actual=set(), action="add"),
        Change(field="row_filter", declared="pii_filter", actual=None, action="add"),
    ]
)
```

---

## Component 3: Drift Detector

New module: `src/brickkit/state/drift.py`

Detects drift: actual state differs from declared + convention.

```python
@dataclass
class DriftReport:
    """Report of all drift detected."""
    timestamp: datetime
    environment: str
    drifted: List[Drift]
    missing: List[str]  # Declared but doesn't exist
    unmanaged: List[str]  # Exists but not declared
    compliant: List[str]  # No drift

@dataclass
class Drift:
    resource_type: str
    resource_name: str
    field: str
    expected: Any
    actual: Any
    severity: Literal["critical", "warning", "info"]

class DriftDetector:
    """Detects drift between declared and actual state."""

    def __init__(self, reader: StateReader, convention: Convention):
        self.reader = reader
        self.convention = convention

    def detect_catalog(self, declared: Catalog) -> List[Drift]:
        """Detect drift for a catalog and all descendants."""

    def detect_schema(self, declared: Schema) -> List[Drift]:
        """Detect drift for a schema and all children."""

    def detect_table(self, declared: TableReference) -> List[Drift]:
        """Detect drift for a table reference."""

    def detect_all(self, declared: List[BaseSecurable]) -> DriftReport:
        """Detect drift across all declared resources."""
```

Severity levels:

- **critical**: Security-related (grants, row filters, PII tags)
- **warning**: Governance-related (missing required tags)
- **info**: Metadata (description changed, non-required tags)

---

## Component 4: Reconciler

New module: `src/brickkit/state/reconciler.py`

Applies only the diff to reach desired state.

```python
@dataclass
class ReconcileResult:
    resource_name: str
    changes_applied: List[Change]
    changes_skipped: List[Change]
    errors: List[str]
    duration_ms: int

class Reconciler:
    """Applies diffs to reconcile state."""

    def __init__(
        self,
        client: WorkspaceClient,
        sql_executor: Optional[SQLExecutor] = None,
        dry_run: bool = False
    ):
        self.client = client
        self.sql_executor = sql_executor  # Required for row filters, column masks
        self.dry_run = dry_run
        self.tag_executor = TagExecutor(client)
        self.grant_executor = GrantExecutor(client)
        # ... other executors

    def reconcile(self, diff: Diff) -> ReconcileResult:
        """Apply diff to reach declared state."""

    def reconcile_tags(self, resource: BaseSecurable, changes: List[Change]) -> List[Change]:
        """Reconcile tag changes."""

    def reconcile_grants(self, resource: BaseSecurable, changes: List[Change]) -> List[Change]:
        """Reconcile grant changes (add missing, revoke extra)."""

    def reconcile_row_filter(self, table: Table, change: Change) -> Change:
        """Apply or remove row filter."""

    def reconcile_column_masks(self, table: Table, changes: List[Change]) -> List[Change]:
        """Apply or remove column masks."""
```

---

## Component 5: CLI Commands

New module: `src/brickkit/cli/drift.py`

```bash
# Detect drift for all declared resources
brickkit drift detect ./governance/

# Output:
# DRIFT REPORT - 2024-01-16 14:30:00 - Environment: dev
#
# CRITICAL (2):
#   ml_platform.recommendations.user_embeddings
#     - grants.analysts: expected {SELECT}, actual {}
#     - row_filter: expected pii_filter, actual None
#
# WARNING (3):
#   ml_platform.recommendations.scores
#     - tags.data_owner: expected ml-team, actual None
#   ...
#
# COMPLIANT (15):
#   ml_platform.core.users
#   ml_platform.core.events
#   ...

# Show what would be applied
brickkit drift plan ./governance/

# Apply changes to fix drift
brickkit drift apply ./governance/

# Validate conventions without checking actual state
brickkit validate ./governance/
```

---

## Component 6: Collector

New module: `src/brickkit/state/collector.py`

Discovers all resources in Databricks (not just declared ones).

```python
class ResourceCollector:
    """Collects all resources from Databricks."""

    def __init__(self, client: WorkspaceClient):
        self.client = client

    def collect_catalog(self, name: str) -> CollectedCatalog:
        """Collect catalog and all descendants."""

    def collect_all_catalogs(self) -> List[CollectedCatalog]:
        """Collect all catalogs in metastore."""

    def collect_unmanaged(
        self,
        declared: List[BaseSecurable]
    ) -> List[CollectedResource]:
        """Find resources that exist but aren't declared."""
```

Use case: Find tables created by pipelines that need governance.

```python
collector = ResourceCollector(client)
all_tables = collector.collect_all_tables("ml_platform", "recommendations")

declared_names = {t.name for t in my_declared_tables}
unmanaged = [t for t in all_tables if t.name not in declared_names]

# Generate references for unmanaged tables
for table in unmanaged:
    print(f"table_ref('{table.name}', schema=s, tags=[...])")
```

---

## Component 7: Executor Updates

Update all executors with diff-aware methods:

```python
class BaseExecutor(ABC, Generic[T]):
    # Existing
    def create(self, resource: T) -> ExecutionResult: ...
    def update(self, resource: T) -> ExecutionResult: ...
    def delete(self, resource: T) -> ExecutionResult: ...
    def exists(self, resource: T) -> bool: ...

    # New
    def read_state(self, resource: T) -> Optional[StateT]:
        """Read current state from Databricks."""

    def diff(self, resource: T) -> Diff:
        """Compare declared resource against actual state."""

    def reconcile(self, resource: T) -> ReconcileResult:
        """Apply only changes needed to reach declared state."""

    def detect_drift(self, resource: T, convention: Convention) -> List[Drift]:
        """Check if actual state drifted from declared + convention."""
```

---

## Implementation Order

### Phase 1: Foundation
1. `StateReader` - Read actual state for all resource types
2. State dataclasses (`TableState`, `SchemaState`, etc.)
3. Basic `StateDiffer` for tags and grants

### Phase 2: Drift Detection
4. `DriftDetector` with convention awareness
5. `DriftReport` generation
6. CLI `brickkit drift detect`

### Phase 3: Reconciliation
7. `Reconciler` for tags
8. `Reconciler` for grants
9. CLI `brickkit drift apply`

### Phase 4: Collection
10. `ResourceCollector` for discovery
11. Unmanaged resource detection
12. Reference generation helpers

### Phase 5: Executor Integration
13. Add `read_state()` to all executors
14. Add `diff()` to all executors
15. Add `reconcile()` to all executors

---

## Resource Coverage Matrix

| Resource | Read State | Diff | Drift Detect | Reconcile |
|----------|------------|------|--------------|-----------|
| Catalog | Phase 1 | Phase 1 | Phase 2 | Phase 3 |
| Schema | Phase 1 | Phase 1 | Phase 2 | Phase 3 |
| Table | Phase 1 | Phase 1 | Phase 2 | Phase 3 |
| Volume | Phase 1 | Phase 1 | Phase 2 | Phase 3 |
| Function | Phase 1 | Phase 1 | Phase 2 | Phase 3 |
| Storage Credential | Phase 1 | Phase 1 | Phase 2 | Phase 3 |
| External Location | Phase 1 | Phase 1 | Phase 2 | Phase 3 |
| Connection | Phase 1 | Phase 1 | Phase 2 | Phase 3 |
| Tags | Phase 1 | Phase 1 | Phase 2 | Phase 3 |
| Grants | Phase 1 | Phase 1 | Phase 2 | Phase 3 |
| Row Filter | Phase 1 | Phase 1 | Phase 2 | Phase 3 |
| Column Mask | Phase 1 | Phase 1 | Phase 2 | Phase 3 |

---

## Success Criteria

1. `brickkit drift detect` returns accurate drift report
2. `brickkit drift apply` fixes drift without side effects
3. Apply is idempotent (running twice changes nothing)
4. Convention violations detected as drift
5. Unmanaged resources discoverable
6. CI integration works (exit code 1 if drift detected)
