# Executor Refactoring Plan

## Overview

The executors folder contains 4,334 lines of code implementing declarative CRUD operations for Databricks Unity Catalog resources. While the declarative pattern is good, there are critical issues with exception handling, code duplication, and missing integrations with the recently refactored governance models.

## Current State

| Metric | Value |
|--------|-------|
| Total files | 13 |
| Total lines | 4,334 |
| Blanket `except Exception` violations | 8+ |
| Silent `pass` failures | 6 |
| Code duplication instances | 3 files |

## Critical Issues

### 1. Silent Exception Handling (CLAUDE.md Violation)

**Files affected:**
- `grant_executor.py:561,570,580` - `except Exception: pass`
- `grant_executor.py:582-585` - assumes existence on API failure
- `catalog_executor.py:125-128,260-262,392-393` - broad catches with silent recovery
- `tag_executor.py:78,89` - multiple bare except blocks

### 2. Code Duplication

Workspace binding logic (~50 lines) duplicated in:
- `catalog_executor.py:100-161`
- `external_location_executor.py:285-333`
- `storage_credential_executor.py:249-296`

### 3. Missing Integrations

| New Model | Executor Status |
|-----------|-----------------|
| `GovernanceDefaults` | Not integrated |
| `GenieSpace` | No executor exists |
| `VectorSearchEndpoint` | No executor exists |
| `VectorSearchIndex` | No executor exists |

---

## Refactoring Phases

### Phase 1: Fix Exception Handling (CRITICAL)

**Goal:** Replace all silent exception catches with specific exception types.

**Files to modify:**
1. `grant_executor.py` - Fix principal validation (lines 554-589)
2. `catalog_executor.py` - Fix workspace binding error handling
3. `tag_executor.py` - Fix tag application error handling

**Pattern to apply:**
```python
# BEFORE (forbidden)
except Exception:
    pass

# AFTER (correct)
from databricks.sdk.errors import NotFound, ResourceDoesNotExist, PermissionDenied

try:
    result = self.client.some_api.method()
except (NotFound, ResourceDoesNotExist):
    # Expected case - resource doesn't exist
    logger.debug(f"Resource not found: {name}")
except PermissionDenied as e:
    logger.error(f"Permission denied: {e}")
    raise
```

### Phase 2: Extract Workspace Binding Mixin

**Goal:** Eliminate duplication of workspace binding logic.

**New file:** `executors/mixins.py`

```python
class WorkspaceBindingMixin:
    """Shared workspace binding logic."""

    def apply_workspace_bindings(
        self,
        resource_name: str,
        workspace_ids: List[int],
        binding_type: BindingType = BindingType.BINDING_TYPE_READ_WRITE
    ) -> ExecutionResult:
        """Apply workspace bindings with proper error handling."""
        ...
```

**Files to update:**
- `catalog_executor.py` - use mixin
- `external_location_executor.py` - use mixin
- `storage_credential_executor.py` - use mixin

### Phase 3: Add Missing Executors

**New files:**
1. `genie_executor.py` - GenieSpaceExecutor
2. `vector_search_executor.py` - VectorSearchExecutor

**Template:**
```python
class GenieSpaceExecutor(BaseExecutor[GenieSpace]):
    """Executor for Genie Space CRUD operations."""

    def create(self, space: GenieSpace) -> ExecutionResult:
        resource_name = space.resolved_name

        if self.dry_run:
            return ExecutionResult(
                success=True,
                operation=OperationType.CREATE,
                resource_type="GenieSpace",
                resource_name=resource_name,
                message=f"[DRY RUN] Would create Genie Space {resource_name}"
            )

        result = space.create(self.client)
        return ExecutionResult(
            success=True,
            operation=OperationType.CREATE,
            resource_type="GenieSpace",
            resource_name=resource_name,
            message=f"Created Genie Space {resource_name}"
        )
```

### Phase 4: Integrate GovernanceDefaults

**Goal:** Add governance validation before resource creation.

**Modify:** `executors/base.py`

```python
class BaseExecutor(ABC, Generic[T]):
    def __init__(
        self,
        client: WorkspaceClient,
        dry_run: bool = False,
        max_retries: int = 3,
        continue_on_error: bool = False,
        governance_defaults: Optional[GovernanceDefaults] = None  # NEW
    ):
        self.governance_defaults = governance_defaults

    def _validate_governance(self, resource: T) -> List[str]:
        """Validate resource against governance rules before execution."""
        if self.governance_defaults and hasattr(resource, 'validate_governance'):
            return resource.validate_governance(self.governance_defaults)
        return []
```

### Phase 5: Reduce Verbosity

**Goal:** 30% reduction in logging statements.

**Pattern to apply:**
```python
# BEFORE (6 lines)
logger.info(f"Step 1: Applying workspace bindings BEFORE setting ISOLATED mode")
logger.info(f"  Catalog: {resource_name}")
logger.info(f"  Target workspace IDs: {workspace_ids_to_bind}")
logger.info(f"Calling workspace_bindings.update API")
logger.info(f"  catalog name: {resource_name}")
logger.info(f"  assign_workspaces: {workspace_ids_as_ints}")

# AFTER (2 lines)
logger.info(f"Applying workspace bindings to {resource_name}: {workspace_ids_to_bind}")
logger.debug(f"API call: workspace_bindings.update(name={resource_name}, assign_workspaces={workspace_ids_as_ints})")
```

---

## Verification

After each phase:
1. Run existing tests (if any)
2. Test with dry_run=True to verify logging
3. Test actual execution in dev environment
4. Verify error messages are informative

---

## Timeline

| Phase | Scope | Priority |
|-------|-------|----------|
| Phase 1 | Exception handling | CRITICAL - Do first |
| Phase 2 | Code duplication | HIGH |
| Phase 3 | New executors | HIGH |
| Phase 4 | Governance integration | MEDIUM |
| Phase 5 | Verbosity reduction | LOW |
