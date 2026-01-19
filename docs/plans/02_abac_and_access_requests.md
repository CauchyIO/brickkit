# Plan: ABAC Enforcement and Access Requests

## Overview

Move brickkit from "governance theater" (tags as metadata) to actual enforcement:

1. Row filters and column masks applied via SQL
2. Tag-based automatic policy application
3. Access request workflows for self-service
4. Time-bound and reviewable access grants

## Current State

**Implemented:**
- Grant propagation through hierarchy (works well)
- Row filter and column mask models on Table
- Function model with `is_row_filter` and `is_column_mask` flags
- Tag infrastructure on all securables

**Gaps:**
- Row filter/column mask SQL execution not implemented
- Tags don't trigger policies automatically
- No access request workflow
- No time-bound grants
- No access review mechanism

---

## Part 1: Row Filters and Column Masks

### Two Approaches

| Approach | Method | SDK Support | Recommended |
|----------|--------|-------------|-------------|
| Table-level (direct) | `ALTER TABLE SET ROW FILTER` | No - SQL only | No |
| Policy-level (ABAC) | `CREATE POLICY` with tag matching | Yes - `PoliciesAPI` | **Yes** |

The SDK `tables` API explicitly states: "Column masks are not supported when creating tables through this API." Row filters are similarly not exposed.

**Use ABAC policies instead.** They are:
- Tag-driven (dynamic)
- Scalable (one policy covers many tables)
- SDK-supported (`client.policies`)

### Legacy Table-Level Approach (Not Recommended)

For reference only. Direct table-level filters/masks require SQL:

```sql
-- Direct row filter (legacy)
ALTER TABLE catalog.schema.table
SET ROW FILTER function_catalog.function_schema.filter_function ON (columns);

-- Direct column mask (legacy)
ALTER TABLE catalog.schema.table
ALTER COLUMN sensitive_column
SET MASK function_catalog.function_schema.mask_function;
```

This doesn't scale. Each table needs explicit configuration.

### Recommended: ABAC Policies

Define policies at catalog/schema level. Tag tables/columns. Policies match via tags.

```sql
-- Policy-based row filter (recommended)
CREATE POLICY pii_filter
ON SCHEMA prod.customers
ROW FILTER pii_row_filter_udf
TO `account users`
EXCEPT `pii_authorized`
FOR TABLES
MATCH COLUMNS hasTagValue('pii', 'true') AS pii_cols
USING COLUMNS (pii_cols);
```

Now any table with `pii=true` tag gets the row filter automatically.

### UDF Functions

Both approaches require UDFs for the filter/mask logic:

```python
def create_pii_row_filter() -> Function:
    """Row filter: only show rows user is authorized to see."""
    return Function(
        name="pii_row_filter",
        function_type=FunctionType.SCALAR,
        return_type="BOOLEAN",
        definition="""
        RETURN (
            is_account_group_member('pii_authorized')
            OR current_user() = owner_email
        )
        """,
    )

def create_ssn_mask() -> Function:
    """Column mask: hide SSN except last 4 digits."""
    return Function(
        name="mask_ssn",
        function_type=FunctionType.SCALAR,
        return_type="STRING",
        parameters=[("ssn", "STRING")],
        definition="""
        RETURN CASE
            WHEN is_account_group_member('pii_authorized') THEN ssn
            ELSE CONCAT('***-**-', RIGHT(ssn, 4))
        END
        """,
    )
```

### Current Model State

The Table model has `row_filter` and `column_masks` fields. These can be:

1. **Deprecated** - Remove in favor of ABAC policies
2. **Kept for reference** - Indicate which filter/mask applies (via policy)
3. **Used for legacy support** - SQL-based direct application

Recommendation: Keep for documentation/reference, but primary enforcement via ABAC policies.

---

## Part 2: Unity Catalog ABAC Policies

### Native ABAC in Unity Catalog

Databricks Unity Catalog has native ABAC policy support. Policies are tag-driven and managed via SQL or the SDK `PoliciesAPI`. This is the correct approach - not custom brickkit logic.

**Key concepts:**
- Policies are defined at catalog, schema, or table level
- Policies reference UDFs for row filter / column mask logic
- Policies match on governed tags using `hasTag()` and `hasTagValue()`
- Policies apply to principals (users, groups, service principals)

**Quotas:**
- 10 policies per catalog
- 10 policies per schema
- 5 policies per table

**Constraints:**
- Only one row filter can resolve per table per user
- Only one column mask can resolve per column per user
- Multiple matching policies = error (by design)

### SQL Syntax

Row filter policy:

```sql
CREATE POLICY hide_pii_rows
ON SCHEMA prod.customers
COMMENT 'Hide rows containing PII from non-authorized users'
ROW FILTER pii_row_filter
TO `account users`
EXCEPT `pii_authorized`
FOR TABLES
MATCH COLUMNS hasTagValue('pii', 'true') AS pii_cols
USING COLUMNS (pii_cols);
```

Column mask policy:

```sql
CREATE POLICY mask_ssn
ON SCHEMA prod.customers
COMMENT 'Mask SSN columns'
COLUMN MASK mask_ssn_function
TO `account users`
EXCEPT `pii_authorized`
FOR TABLES
MATCH COLUMNS hasTagValue('pii_type', 'ssn') AS ssn_cols
ON COLUMN ssn_cols;
```

### SDK PoliciesAPI

The SDK provides `client.policies` with methods:

```python
# Create policy
client.policies.create_policy(
    on_securable_type=SecurableType.SCHEMA,
    on_securable_fullname="prod.customers",
    name="hide_pii_rows",
    # ... policy definition
)

# List policies
policies = client.policies.list_policies(
    on_securable_type=SecurableType.CATALOG,
    on_securable_fullname="prod",
    include_inherited=True,
)

# Get specific policy
policy = client.policies.get_policy(
    on_securable_type=SecurableType.SCHEMA,
    on_securable_fullname="prod.customers",
    name="hide_pii_rows",
)

# Update policy
client.policies.update_policy(
    on_securable_type=SecurableType.SCHEMA,
    on_securable_fullname="prod.customers",
    name="hide_pii_rows",
    update_mask="...",
    # ... updated definition
)

# Delete policy
client.policies.delete_policy(
    on_securable_type=SecurableType.SCHEMA,
    on_securable_fullname="prod.customers",
    name="hide_pii_rows",
)
```

### Model: ABACPolicy

New module: `src/brickkit/models/policies.py`

```python
class TagCondition(BaseModel):
    """Tag condition for policy matching."""
    tag_key: str
    tag_value: Optional[str] = None  # None = hasTag(), value = hasTagValue()

    def to_sql(self) -> str:
        if self.tag_value:
            return f"hasTagValue('{self.tag_key}', '{self.tag_value}')"
        return f"hasTag('{self.tag_key}')"

class ABACPolicy(BaseGovernanceModel):
    """Unity Catalog ABAC policy definition."""
    name: str
    comment: Optional[str] = None
    policy_type: Literal["row_filter", "column_mask"]
    function: Function  # UDF implementing the filter/mask logic
    target_principals: List[Principal]  # TO clause
    except_principals: List[Principal] = Field(default_factory=list)  # EXCEPT clause
    match_conditions: List[TagCondition]  # Up to 3 conditions
    target_column: Optional[str] = None  # Required for column_mask

    @field_validator('match_conditions')
    @classmethod
    def max_three_conditions(cls, v):
        if len(v) > 3:
            raise ValueError("Maximum 3 match conditions per policy")
        return v

    def to_sql(self, securable_type: str, securable_name: str) -> str:
        """Generate CREATE POLICY SQL."""
        principals = ", ".join(f"`{p.resolved_name}`" for p in self.target_principals)
        except_clause = ""
        if self.except_principals:
            except_names = ", ".join(f"`{p.resolved_name}`" for p in self.except_principals)
            except_clause = f"EXCEPT {except_names}"

        conditions = ", ".join(c.to_sql() for c in self.match_conditions)
        alias = "matched_cols"

        if self.policy_type == "row_filter":
            return f"""
CREATE OR REPLACE POLICY {self.name}
ON {securable_type.upper()} {securable_name}
COMMENT '{self.comment or ""}'
ROW FILTER {self.function.fqdn}
TO {principals}
{except_clause}
FOR TABLES
MATCH COLUMNS {conditions} AS {alias}
USING COLUMNS ({alias});
"""
        else:  # column_mask
            return f"""
CREATE OR REPLACE POLICY {self.name}
ON {securable_type.upper()} {securable_name}
COMMENT '{self.comment or ""}'
COLUMN MASK {self.function.fqdn}
TO {principals}
{except_clause}
FOR TABLES
MATCH COLUMNS {conditions} AS {alias}
ON COLUMN {alias};
"""
```

### Executor: PolicyExecutor

New module: `src/brickkit/executors/policy_executor.py`

```python
class PolicyExecutor(BaseExecutor[ABACPolicy]):
    """Manages Unity Catalog ABAC policies."""

    def __init__(self, client: WorkspaceClient, sql_executor: SQLExecutor, dry_run: bool = False):
        super().__init__(client, dry_run)
        self.sql_executor = sql_executor
        self.function_executor = FunctionExecutor(client, dry_run)

    def create(self, policy: ABACPolicy, securable_type: str, securable_name: str) -> ExecutionResult:
        """Create or replace an ABAC policy."""
        # Ensure the UDF exists first
        self.function_executor.create_or_update(policy.function)

        sql = policy.to_sql(securable_type, securable_name)

        if self.dry_run:
            logger.info(f"[DRY RUN] Would execute:\n{sql}")
            return ExecutionResult(success=True, operation=OperationType.SKIPPED, ...)

        result = self.sql_executor.execute(sql)
        return ExecutionResult(
            success=result.success,
            operation=OperationType.CREATE,
            message=f"Created policy {policy.name} on {securable_name}",
        )

    def delete(self, policy_name: str, securable_type: str, securable_name: str) -> ExecutionResult:
        """Delete an ABAC policy."""
        sql = f"DROP POLICY IF EXISTS {policy_name} ON {securable_type.upper()} {securable_name}"
        # ...

    def list_policies(self, securable_type: str, securable_name: str) -> List[ABACPolicy]:
        """List policies on a securable."""
        return list(self.client.policies.list_policies(
            on_securable_type=securable_type,
            on_securable_fullname=securable_name,
            include_inherited=True,
        ))

    def exists(self, policy_name: str, securable_type: str, securable_name: str) -> bool:
        """Check if policy exists."""
        try:
            self.client.policies.get_policy(
                on_securable_type=securable_type,
                on_securable_fullname=securable_name,
                name=policy_name,
            )
            return True
        except NotFound:
            return False
```

### Convention with ABAC Policies

Conventions can define standard policies that get applied to matching securables:

```python
CONVENTION = Convention(
    name="acme_standards",
    default_tags=[...],
    required_tags=[
        RequiredTag(key="pii", allowed_values={"true", "false"}, applies_to={"TABLE"}),
        RequiredTag(key="pii_type", applies_to={"COLUMN"}),
    ],
    abac_policies=[
        ABACPolicy(
            name="pii_row_filter",
            comment="Filter rows containing PII for non-authorized users",
            policy_type="row_filter",
            function=create_pii_row_filter(),
            target_principals=[Principal.all_account_users()],
            except_principals=[Principal(name="pii_authorized", add_environment_suffix=False)],
            match_conditions=[TagCondition(tag_key="pii", tag_value="true")],
        ),
        ABACPolicy(
            name="ssn_mask",
            comment="Mask SSN columns",
            policy_type="column_mask",
            function=create_ssn_mask(),
            target_principals=[Principal.all_account_users()],
            except_principals=[Principal(name="pii_authorized", add_environment_suffix=False)],
            match_conditions=[TagCondition(tag_key="pii_type", tag_value="ssn")],
        ),
    ],
)
```

### Workflow

1. **Architect defines convention** with required tags and ABAC policies
2. **Engineering applies tags** to tables and columns (`pii=true`, `pii_type=ssn`)
3. **Brickkit ensures UDFs exist** (creates if missing)
4. **Brickkit creates/updates policies** via SQL or SDK
5. **Unity Catalog enforces** policies at query time

Tags are the bridge: brickkit manages tags, Unity Catalog enforces policies based on tags.

---

## Part 3: Access Request Workflow

### Databricks RequestForAccess API

Databricks has a request-for-access feature (preview). Users request access to securables, owners approve/deny.

SDK: `client.permissions.request_access()` and related endpoints.

### Model: AccessRequest

New module: `src/brickkit/models/access_requests.py`

```python
class AccessRequest(BaseGovernanceModel):
    """Request for access to a securable."""
    id: Optional[str] = None  # Set by Databricks
    securable_type: SecurableType
    securable_name: str  # FQDN
    requester: str  # User or service principal
    requested_privileges: Set[PrivilegeType]
    justification: str
    requested_duration: Optional[timedelta] = None  # None = permanent
    status: Literal["pending", "approved", "denied", "expired"] = "pending"
    reviewed_by: Optional[str] = None
    reviewed_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None

class AccessRequestPolicy(BaseModel):
    """Policy governing access requests."""
    securable_pattern: str  # Glob pattern: "catalog.schema.*"
    allowed_privileges: Set[PrivilegeType]
    max_duration: Optional[timedelta] = None
    require_justification: bool = True
    auto_approve_for: List[Principal] = Field(default_factory=list)
    approvers: List[Principal] = Field(default_factory=list)
    notify_on_request: List[str] = Field(default_factory=list)  # Email addresses
```

### Executor: AccessRequestExecutor

New module: `src/brickkit/executors/access_request_executor.py`

```python
class AccessRequestExecutor:
    """Manages access requests."""

    def __init__(self, client: WorkspaceClient):
        self.client = client
        self.grant_executor = GrantExecutor(client)

    def create_request(self, request: AccessRequest) -> AccessRequest:
        """Submit an access request."""
        # Use SDK to create request
        # Return request with ID populated

    def list_pending(self, securable: Optional[BaseSecurable] = None) -> List[AccessRequest]:
        """List pending access requests."""

    def approve(self, request: AccessRequest, reviewer: str) -> ExecutionResult:
        """Approve an access request and grant access."""
        # Validate request
        # Create grants
        # If time-bound, schedule revocation
        # Update request status

    def deny(self, request: AccessRequest, reviewer: str, reason: str) -> ExecutionResult:
        """Deny an access request."""

    def revoke_expired(self) -> List[ExecutionResult]:
        """Revoke access for expired grants."""
```

### Time-Bound Grants

```python
class TimeBoundGrant(BaseModel):
    """Grant that expires after a duration."""
    privilege: Privilege
    granted_at: datetime
    expires_at: datetime
    request_id: Optional[str] = None  # Link to access request

class GrantScheduler:
    """Manages time-bound grants."""

    def schedule_revocation(self, grant: TimeBoundGrant) -> None:
        """Schedule grant revocation at expiry time."""

    def revoke_expired(self) -> List[ExecutionResult]:
        """Revoke all expired grants. Run periodically."""
```

### Self-Service Access Request

Usage from developer perspective:

```python
from acme_governance import request_access, PrivilegeType

# Developer requests access
request = request_access(
    table="ml_platform.recommendations.user_embeddings",
    privileges={PrivilegeType.SELECT},
    justification="Need to analyze embedding quality for model v2",
    duration=timedelta(days=7),
)

print(f"Request submitted: {request.id}")
print(f"Status: {request.status}")  # pending
```

Owner/approver side:

```python
from acme_governance import list_pending_requests, approve_request

requests = list_pending_requests(catalog="ml_platform")

for req in requests:
    print(f"{req.requester} requests {req.requested_privileges} on {req.securable_name}")
    print(f"Justification: {req.justification}")
    print(f"Duration: {req.requested_duration}")

# Approve
approve_request(requests[0], reviewer="platform-admin@acme.com")
```

### Access Review

Periodic review of who has access to what:

```python
class AccessReview(BaseModel):
    """Periodic access review."""
    id: str
    securable: str
    review_period: timedelta
    last_reviewed: Optional[datetime] = None
    next_review: datetime
    reviewers: List[Principal]

class AccessReviewExecutor:
    """Manages access reviews."""

    def create_review(self, review: AccessReview) -> AccessReview:
        """Schedule an access review."""

    def list_due_reviews(self) -> List[AccessReview]:
        """List reviews that are due."""

    def complete_review(
        self,
        review: AccessReview,
        keep: List[Principal],
        revoke: List[Principal],
        reviewer: str
    ) -> ExecutionResult:
        """Complete a review: keep some grants, revoke others."""
```

---

## Part 4: Integration with Convention

Update Convention to include access request policies and auto-policies:

```python
class Convention(BaseModel):
    name: str
    default_tags: List[TagDefault] = Field(default_factory=list)
    required_tags: List[RequiredTag] = Field(default_factory=list)
    naming_conventions: List[NamingConvention] = Field(default_factory=list)
    default_owner: Optional[str] = None

    # New fields
    auto_policies: List[AutoPolicy] = Field(default_factory=list)
    access_request_policies: List[AccessRequestPolicy] = Field(default_factory=list)
    review_schedule: Optional[timedelta] = None  # e.g., every 90 days
```

---

## Implementation Order

### Phase 1: ABAC Policy Models
1. `ABACPolicy` model with `TagCondition`
2. `Function` model updates for UDF patterns (row filters, column masks)
3. Convention updates with `abac_policies` field

### Phase 2: Policy Executor (SDK-Based)
4. `PolicyExecutor` using `client.policies.create_policy()`
5. `PolicyExecutor.list_policies()` and `exists()`
6. `PolicyExecutor.delete()` and `update()`
7. UDF deployment via `FunctionExecutor`

### Phase 3: Policy Drift Detection
8. Read actual policies via `client.policies.list_policies()`
9. Compare declared policies vs actual
10. Report policy drift (missing, extra, changed)

### Phase 4: Access Requests
11. `AccessRequest` and `AccessRequestPolicy` models
12. `AccessRequestExecutor` basic operations
13. `TimeBoundGrant` and `GrantScheduler`
14. CLI for request/approve/deny

### Phase 5: Access Reviews
15. `AccessReview` model
16. `AccessReviewExecutor`
17. Notification integration
18. CLI for review management

### Phase 6 (Optional): Legacy Table-Level Support
19. `SQLExecutor` for `ALTER TABLE` DDL
20. `TableExecutor` row filter/column mask methods
21. Only if customers need direct table-level assignment

---

## Success Criteria

1. ABAC policy created via `client.policies.create_policy()`
2. UDF for row filter/column mask deployed via `FunctionExecutor`
3. Policy matches tables/columns with governed tags automatically
4. Convention defines policies, brickkit deploys them
5. Policy drift detected (policy changed outside brickkit)
6. Access request created, approved, grant applied
7. Time-bound grant revoked automatically at expiry
8. Access review workflow completes end-to-end
9. All operations idempotent
10. CLI commands: `brickkit policies apply`, `brickkit policies drift`

---

## References

- [Unity Catalog ABAC Overview](https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/)
- [Create and Manage ABAC Policies](https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/policies)
- [Databricks SDK PoliciesAPI](https://databricks-sdk-py.readthedocs.io/en/latest/workspace/catalog/policies.html)
