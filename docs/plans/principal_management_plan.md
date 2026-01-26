# Principal Management System - Design Plan (v2)

## Overview

Extend BrickKit to support full principal lifecycle management in Databricks by wrapping the existing SDK types with governance capabilities.

## Databricks SDK Types (Already Available)

The `databricks.sdk.service.iam` module provides:

### Dataclasses
- `Group` - display_name, entitlements, external_id, groups (nested), members, roles, schemas
- `ServicePrincipal` - active, application_id, display_name, entitlements, external_id, groups, roles, schemas
- `User` - active, display_name, entitlements, external_id, groups, roles, emails, user_name
- `ComplexValue` - used for entitlements, members, roles (has value, display, type, ref, primary)
- `AccessControlRequest` - group_name, user_name, service_principal_name, permission_level
- `ObjectPermissions` - access_control_list, object_id, object_type

### Enums
- `PermissionLevel` - CAN_ATTACH_TO, CAN_BIND, CAN_CREATE, CAN_EDIT, CAN_MANAGE, CAN_READ, CAN_RESTART, CAN_RUN, CAN_USE, CAN_VIEW, IS_OWNER, etc.
- `GroupSchema`, `ServicePrincipalSchema`, `UserSchema` - SCIM schema identifiers

### APIs
- `GroupsAPI` - create, delete, get, list, patch, update
- `ServicePrincipalsAPI` - create, delete, get, list, patch, update
- `UsersAPI` - create, delete, get, list, patch, update
- `PermissionsAPI` - get, set, update (for object ACLs)

**Key insight:** We should NOT recreate these types. Instead, wrap them with governance features.

---

## BrickKit Governance Layer

### Design Principle

BrickKit models wrap SDK types to add:
1. **Environment-aware naming** - `grp_data_engineering` → `grp_data_engineering_dev`
2. **Convention validation** - Naming patterns, required entitlements
3. **External principal detection** - Identify Entra-synced principals
4. **Declarative membership** - Define desired state, executor syncs

### 1. New Enums (`models/enums.py`)

Only add what SDK doesn't provide:

```python
from databricks.sdk.service.iam import PermissionLevel  # Re-export from SDK

class PrincipalSource(str, Enum):
    """Origin of the principal - determines if we can create/modify it."""
    DATABRICKS = "DATABRICKS"  # Native Databricks principal (can create/modify)
    EXTERNAL = "EXTERNAL"       # Synced from external IdP (read-only, validate only)

# Common entitlement values (SDK uses ComplexValue.value strings)
class WorkspaceEntitlement(str, Enum):
    """Well-known entitlement values for convenience."""
    WORKSPACE_ACCESS = "workspace-access"
    DATABRICKS_SQL_ACCESS = "databricks-sql-access"
    CLUSTER_CREATE = "allow-cluster-create"
    INSTANCE_POOL_CREATE = "allow-instance-pool-create"

# Object types for ACLs (SDK PermissionsAPI uses strings)
class AclObjectType(str, Enum):
    """Object types that support ACL permissions."""
    ALERTS = "alerts"
    CLUSTERS = "clusters"
    CLUSTER_POLICIES = "cluster-policies"
    DASHBOARDS = "dashboards"
    DIRECTORIES = "directories"
    EXPERIMENTS = "experiments"
    FILES = "files"
    INSTANCE_POOLS = "instance-pools"
    JOBS = "jobs"
    NOTEBOOKS = "notebooks"
    PIPELINES = "pipelines"
    QUERIES = "queries"
    REGISTERED_MODELS = "registered-models"
    REPOS = "repos"
    SERVING_ENDPOINTS = "serving-endpoints"
    WAREHOUSES = "warehouses"
```

### 2. Governed Principal Models (`models/principals.py`)

Wrapper models that add governance to SDK types:

```python
from databricks.sdk.service.iam import (
    Group as SdkGroup,
    ServicePrincipal as SdkServicePrincipal,
    ComplexValue,
    PermissionLevel,
    AccessControlRequest,
)

class MemberReference(BaseGovernanceModel):
    """
    Reference to a principal that should be a group member.

    Separates the 'what we want' (member definition) from
    'what exists' (SDK ComplexValue).
    """
    name: str
    principal_type: PrincipalType  # USER, GROUP, SERVICE_PRINCIPAL
    add_environment_suffix: bool = True

    @computed_field
    def resolved_name(self) -> str:
        """Get environment-aware name."""
        if not self.add_environment_suffix:
            return self.name
        env = get_current_environment()
        return f"{self.name}_{env.value.lower()}"

    def to_complex_value(self) -> ComplexValue:
        """Convert to SDK ComplexValue for API calls."""
        return ComplexValue(value=self.resolved_name)


class ManagedGroup(BaseGovernanceModel):
    """
    Governance wrapper for Databricks Group.

    Adds:
    - Environment-aware naming
    - Declarative membership (desired state)
    - External principal detection
    - Convention validation
    """
    name: str = Field(..., description="Base group name")
    display_name: Optional[str] = None

    # Source - determines create vs validate behavior
    source: PrincipalSource = PrincipalSource.DATABRICKS
    external_id: Optional[str] = None  # Entra Object ID for external groups

    # Environment configuration
    add_environment_suffix: bool = True
    environment_mapping: Dict[Environment, str] = Field(default_factory=dict)

    # Desired membership (declarative)
    members: List[MemberReference] = Field(default_factory=list)

    # Desired entitlements
    entitlements: List[WorkspaceEntitlement] = Field(default_factory=list)

    # Roles (AWS instance profiles, etc.)
    roles: List[str] = Field(default_factory=list)

    @computed_field
    def resolved_name(self) -> str:
        """Environment-aware name resolution."""
        env = get_current_environment()
        if env in self.environment_mapping:
            return self.environment_mapping[env]
        if not self.add_environment_suffix:
            return self.name
        return f"{self.name}_{env.value.lower()}"

    def to_sdk_group(self) -> SdkGroup:
        """Convert to SDK Group for API calls."""
        return SdkGroup(
            display_name=self.display_name or self.resolved_name,
            external_id=self.external_id,
            members=[m.to_complex_value() for m in self.members],
            entitlements=[ComplexValue(value=e.value) for e in self.entitlements],
            roles=[ComplexValue(value=r) for r in self.roles],
        )

    @classmethod
    def from_sdk_group(cls, sdk_group: SdkGroup, source: PrincipalSource = PrincipalSource.DATABRICKS) -> 'ManagedGroup':
        """Create from SDK Group (e.g., when importing)."""
        # Parse members back to MemberReference
        members = []
        if sdk_group.members:
            for m in sdk_group.members:
                # Determine type from $ref or assume user
                member_type = PrincipalType.USER
                if m.ref and 'Groups' in m.ref:
                    member_type = PrincipalType.GROUP
                elif m.ref and 'ServicePrincipals' in m.ref:
                    member_type = PrincipalType.SERVICE_PRINCIPAL
                members.append(MemberReference(
                    name=m.value or m.display or '',
                    principal_type=member_type,
                    add_environment_suffix=False  # Already resolved
                ))

        # Parse entitlements
        entitlements = []
        if sdk_group.entitlements:
            for e in sdk_group.entitlements:
                try:
                    entitlements.append(WorkspaceEntitlement(e.value))
                except ValueError:
                    pass  # Unknown entitlement

        return cls(
            name=sdk_group.display_name or '',
            display_name=sdk_group.display_name,
            source=source,
            external_id=sdk_group.external_id,
            add_environment_suffix=False,
            members=members,
            entitlements=entitlements,
            roles=[r.value for r in (sdk_group.roles or []) if r.value],
        )

    # Convenience methods for building membership
    def add_user(self, email: str) -> 'ManagedGroup':
        """Add a user member."""
        self.members.append(MemberReference(
            name=email,
            principal_type=PrincipalType.USER,
            add_environment_suffix=False  # Users don't get suffixes
        ))
        return self

    def add_service_principal(self, name: str, add_env_suffix: bool = True) -> 'ManagedGroup':
        """Add a service principal member."""
        self.members.append(MemberReference(
            name=name,
            principal_type=PrincipalType.SERVICE_PRINCIPAL,
            add_environment_suffix=add_env_suffix
        ))
        return self

    def add_nested_group(self, name: str, add_env_suffix: bool = True) -> 'ManagedGroup':
        """Add a nested group member."""
        self.members.append(MemberReference(
            name=name,
            principal_type=PrincipalType.GROUP,
            add_environment_suffix=add_env_suffix
        ))
        return self


class ManagedServicePrincipal(BaseGovernanceModel):
    """
    Governance wrapper for Databricks ServicePrincipal.
    """
    name: str = Field(..., description="Base SPN name")
    display_name: Optional[str] = None
    application_id: Optional[str] = None  # For external SPNs (Entra app registration)

    # Source
    source: PrincipalSource = PrincipalSource.DATABRICKS
    external_id: Optional[str] = None

    # Environment configuration
    add_environment_suffix: bool = True
    environment_mapping: Dict[Environment, str] = Field(default_factory=dict)

    # Desired entitlements
    entitlements: List[WorkspaceEntitlement] = Field(default_factory=list)

    # Groups this SPN should belong to
    group_memberships: List[str] = Field(default_factory=list)

    @computed_field
    def resolved_name(self) -> str:
        """Environment-aware name resolution."""
        env = get_current_environment()
        if env in self.environment_mapping:
            return self.environment_mapping[env]
        if not self.add_environment_suffix:
            return self.name
        return f"{self.name}_{env.value.lower()}"

    def to_sdk_service_principal(self) -> SdkServicePrincipal:
        """Convert to SDK ServicePrincipal for API calls."""
        return SdkServicePrincipal(
            display_name=self.display_name or self.resolved_name,
            application_id=self.application_id,
            external_id=self.external_id,
            entitlements=[ComplexValue(value=e.value) for e in self.entitlements],
            active=True,
        )
```

### 3. ACL Model (`models/acls.py`)

Thin wrapper over SDK types:

```python
from databricks.sdk.service.iam import AccessControlRequest, PermissionLevel

class AclBinding(BaseGovernanceModel):
    """
    Declarative ACL binding for an object.

    Maps principal references to permissions on a specific object.
    """
    object_type: AclObjectType
    object_id: str  # Cluster ID, job ID, notebook path, etc.

    # ACL entries (desired state)
    permissions: List[AclEntry] = Field(default_factory=list)

    # Sync mode
    replace_all: bool = False  # If True, removes permissions not in this list

    def to_access_control_requests(self) -> List[AccessControlRequest]:
        """Convert to SDK AccessControlRequest list."""
        requests = []
        for entry in self.permissions:
            req = AccessControlRequest(permission_level=entry.permission)
            if entry.principal_type == PrincipalType.GROUP:
                req.group_name = entry.resolved_principal_name
            elif entry.principal_type == PrincipalType.SERVICE_PRINCIPAL:
                req.service_principal_name = entry.resolved_principal_name
            else:
                req.user_name = entry.resolved_principal_name
            requests.append(req)
        return requests


class AclEntry(BaseGovernanceModel):
    """Single ACL entry."""
    principal_name: str
    principal_type: PrincipalType
    permission: PermissionLevel  # From SDK
    add_environment_suffix: bool = True

    @computed_field
    def resolved_principal_name(self) -> str:
        if not self.add_environment_suffix or self.principal_type == PrincipalType.USER:
            return self.principal_name
        env = get_current_environment()
        return f"{self.principal_name}_{env.value.lower()}"
```

### 4. External Principal Validator

```python
class ExternalPrincipalValidator:
    """
    Validates external principals against workspace SCIM sync.

    External principals (from Entra ID) are read-only in Databricks:
    - Cannot be created/deleted via API
    - Can have entitlements/ACLs set
    - Must exist (synced) before referencing
    """

    def __init__(self, workspace_client: WorkspaceClient):
        self.client = workspace_client
        self._external_groups: Optional[Set[str]] = None

    def is_external_group(self, group_name: str) -> bool:
        """Check if a group is externally synced (has external_id)."""
        try:
            groups = list(self.client.groups.list(filter=f'displayName eq "{group_name}"'))
            if groups and groups[0].external_id:
                return True
        except Exception:
            pass
        return False

    def validate_external_exists(self, name: str, principal_type: PrincipalType) -> bool:
        """Validate that an external principal exists in the workspace."""
        try:
            if principal_type == PrincipalType.GROUP:
                groups = list(self.client.groups.list(filter=f'displayName eq "{name}"'))
                return len(groups) > 0
            elif principal_type == PrincipalType.SERVICE_PRINCIPAL:
                sps = list(self.client.service_principals.list(filter=f'displayName eq "{name}"'))
                return len(sps) > 0
        except Exception:
            pass
        return False

    def detect_scim_sync_enabled(self) -> bool:
        """Heuristic: check if any principals have external_id set."""
        try:
            for group in self.client.groups.list(count=10):
                if group.external_id:
                    return True
            for sp in self.client.service_principals.list(count=10):
                if sp.external_id:
                    return True
        except Exception:
            pass
        return False
```

### 5. Executors (`executors/principal_executor.py`)

```python
from databricks.sdk.service.iam import (
    Group as SdkGroup,
    ServicePrincipal as SdkServicePrincipal,
    ComplexValue,
    Patch,
    PatchOp,
)

class GroupExecutor(BaseExecutor[ManagedGroup]):
    """
    Executor for managing Databricks groups.

    Handles:
    - Create (DATABRICKS source only)
    - Update members and entitlements
    - Validate (EXTERNAL source - check exists)
    - Sync (full reconciliation)
    """

    def create(self, group: ManagedGroup) -> ExecutionResult:
        """Create or validate a group."""
        if self.dry_run:
            return ExecutionResult(operation=Operation.DRY_RUN, message=f"Would create group {group.resolved_name}")

        # For external groups, just validate existence
        if group.source == PrincipalSource.EXTERNAL:
            return self._validate_external(group)

        # Check if already exists
        existing = self._get_by_name(group.resolved_name)
        if existing:
            return ExecutionResult(operation=Operation.SKIP, message=f"Group {group.resolved_name} already exists")

        # Create via SDK
        sdk_group = group.to_sdk_group()
        result = self.client.groups.create(
            display_name=sdk_group.display_name,
            entitlements=sdk_group.entitlements,
            members=sdk_group.members,
            roles=sdk_group.roles,
        )

        return ExecutionResult(operation=Operation.CREATE, message=f"Created group {group.resolved_name}")

    def sync_members(self, group: ManagedGroup) -> ExecutionResult:
        """Sync group membership to desired state."""
        existing = self._get_by_name(group.resolved_name)
        if not existing:
            raise ValueError(f"Group {group.resolved_name} does not exist")

        # Calculate diff
        desired_members = {m.resolved_name for m in group.members}
        current_members = {m.value for m in (existing.members or []) if m.value}

        to_add = desired_members - current_members
        to_remove = current_members - desired_members

        if not to_add and not to_remove:
            return ExecutionResult(operation=Operation.SKIP, message="Members already in sync")

        # Use PATCH to update
        operations = []
        if to_add:
            operations.append(Patch(
                op=PatchOp.ADD,
                path="members",
                value=[{"value": m} for m in to_add]
            ))
        if to_remove:
            for m in to_remove:
                operations.append(Patch(
                    op=PatchOp.REMOVE,
                    path=f'members[value eq "{m}"]'
                ))

        self.client.groups.patch(id=existing.id, operations=operations)

        return ExecutionResult(
            operation=Operation.UPDATE,
            message=f"Added {len(to_add)}, removed {len(to_remove)} members"
        )

    def sync_entitlements(self, group: ManagedGroup) -> ExecutionResult:
        """Sync group entitlements to desired state."""
        # Similar pattern to sync_members
        ...

    def _get_by_name(self, name: str) -> Optional[SdkGroup]:
        """Find group by display name."""
        try:
            groups = list(self.client.groups.list(filter=f'displayName eq "{name}"'))
            return groups[0] if groups else None
        except Exception:
            return None

    def _validate_external(self, group: ManagedGroup) -> ExecutionResult:
        """Validate external group exists."""
        existing = self._get_by_name(group.resolved_name)
        if existing:
            if existing.external_id:
                return ExecutionResult(operation=Operation.SKIP, message=f"External group {group.resolved_name} exists")
            else:
                return ExecutionResult(operation=Operation.SKIP, message=f"Group {group.resolved_name} exists (not external)")
        return ExecutionResult(operation=Operation.ERROR, message=f"External group {group.resolved_name} not found - check SCIM sync")


class ServicePrincipalExecutor(BaseExecutor[ManagedServicePrincipal]):
    """Executor for managing Databricks service principals."""
    # Similar pattern to GroupExecutor
    ...


class AclExecutor(BaseExecutor[AclBinding]):
    """
    Executor for managing object-level ACLs.

    Uses PermissionsAPI from SDK.
    """

    def set_permissions(self, binding: AclBinding) -> ExecutionResult:
        """Set permissions on an object (replace mode)."""
        if self.dry_run:
            return ExecutionResult(operation=Operation.DRY_RUN, message=f"Would set permissions on {binding.object_type.value}/{binding.object_id}")

        requests = binding.to_access_control_requests()
        self.client.permissions.set(
            request_object_type=binding.object_type.value,
            request_object_id=binding.object_id,
            access_control_list=requests,
        )

        return ExecutionResult(operation=Operation.UPDATE, message=f"Set {len(requests)} permissions")

    def update_permissions(self, binding: AclBinding) -> ExecutionResult:
        """Update permissions on an object (merge mode)."""
        if self.dry_run:
            return ExecutionResult(operation=Operation.DRY_RUN, message=f"Would update permissions on {binding.object_type.value}/{binding.object_id}")

        requests = binding.to_access_control_requests()
        self.client.permissions.update(
            request_object_type=binding.object_type.value,
            request_object_id=binding.object_id,
            access_control_list=requests,
        )

        return ExecutionResult(operation=Operation.UPDATE, message=f"Updated {len(requests)} permissions")
```

---

## Usage Examples

### Example 1: Create Native Group with Members

```python
from brickkit.models.principals import ManagedGroup, WorkspaceEntitlement
from brickkit.executors import GroupExecutor

# Define group
data_team = ManagedGroup(
    name="grp_data_engineering",
    display_name="Data Engineering Team",
    entitlements=[
        WorkspaceEntitlement.WORKSPACE_ACCESS,
        WorkspaceEntitlement.CLUSTER_CREATE,
    ],
)

# Add members fluently
data_team.add_user("alice@company.com")
data_team.add_user("bob@company.com")
data_team.add_service_principal("spn_etl_pipeline")  # → spn_etl_pipeline_dev

# Deploy
executor = GroupExecutor(workspace_client)
executor.create(data_team)
executor.sync_members(data_team)
executor.sync_entitlements(data_team)
```

### Example 2: Reference External (Entra) Group

```python
# External group - managed in Entra, synced via SCIM
entra_admins = ManagedGroup(
    name="Cloud-Databricks-Admins",
    source=PrincipalSource.EXTERNAL,
    external_id="12345678-...",  # Entra Object ID
    add_environment_suffix=False,
)

# Executor validates existence (doesn't create)
result = executor.create(entra_admins)  # SKIP if exists, ERROR if not synced

# Can still set entitlements on external groups
entra_admins.entitlements = [WorkspaceEntitlement.WORKSPACE_ACCESS]
executor.sync_entitlements(entra_admins)
```

### Example 3: Set Cluster ACLs

```python
from brickkit.models.acls import AclBinding, AclEntry, AclObjectType
from databricks.sdk.service.iam import PermissionLevel

cluster_acl = AclBinding(
    object_type=AclObjectType.CLUSTERS,
    object_id="0123-456789-abcdef",
    permissions=[
        AclEntry(
            principal_name="grp_data_engineering",
            principal_type=PrincipalType.GROUP,
            permission=PermissionLevel.CAN_RESTART,
        ),
        AclEntry(
            principal_name="spn_etl_pipeline",
            principal_type=PrincipalType.SERVICE_PRINCIPAL,
            permission=PermissionLevel.CAN_MANAGE,
        ),
    ],
)

acl_executor = AclExecutor(workspace_client)
acl_executor.update_permissions(cluster_acl)
```

---

## File Structure

```
src/brickkit/
├── models/
│   ├── principals.py      # NEW: ManagedGroup, ManagedServicePrincipal, MemberReference
│   ├── acls.py            # NEW: AclBinding, AclEntry
│   └── enums.py           # UPDATE: PrincipalSource, WorkspaceEntitlement, AclObjectType
├── executors/
│   ├── group_executor.py          # NEW
│   ├── service_principal_executor.py  # NEW
│   └── acl_executor.py            # NEW
└── validators/
    └── external_validator.py      # NEW: ExternalPrincipalValidator
```

---

## Key Differences from v1

| Aspect | v1 (Original) | v2 (SDK-aware) |
|--------|---------------|----------------|
| Enums | Recreated PermissionLevel, etc. | Import from `databricks.sdk.service.iam` |
| Data models | Separate from SDK | Wrap SDK types (`to_sdk_*`, `from_sdk_*`) |
| Entitlements | Custom enum | `WorkspaceEntitlement` + use SDK `ComplexValue` |
| ACLs | Custom AclEntry | Thin wrapper over `AccessControlRequest` |
| API calls | Custom implementation | Delegate to SDK APIs |

---

## Implementation Phases

### Phase 1: Core Models
- [ ] Add `PrincipalSource`, `WorkspaceEntitlement`, `AclObjectType` to enums
- [ ] Create `models/principals.py` with `ManagedGroup`, `ManagedServicePrincipal`
- [ ] Create `models/acls.py` with `AclBinding`, `AclEntry`

### Phase 2: Executors
- [ ] Create `GroupExecutor` using SDK `GroupsAPI`
- [ ] Create `ServicePrincipalExecutor` using SDK `ServicePrincipalsAPI`
- [ ] Create `AclExecutor` using SDK `PermissionsAPI`

### Phase 3: External Validation
- [ ] Create `ExternalPrincipalValidator`
- [ ] Integrate validation into executors

### Phase 4: Integration
- [ ] Update existing `Principal` model to link with `ManagedGroup`/`ManagedServicePrincipal`
- [ ] Add principal support to conventions

---

## Open Questions

1. **Account-level principals**: SDK has separate `AccountGroupsAPI`, `AccountServicePrincipalsAPI`. Support account-level management?

2. **Roles (AWS instance profiles)**: Include in scope or defer?

3. **Nested group resolution**: When adding a nested group, should we validate it exists first?
