"""
Executor for managing Databricks Groups.

Handles creating, updating, and syncing groups including membership
and entitlements, with support for external (Entra-synced) groups.
"""

import logging
from typing import Optional, Set

from databricks.sdk.errors import AlreadyExists, NotFound, ResourceConflict, ResourceDoesNotExist
from databricks.sdk.service.iam import (
    Group as SdkGroup,
)
from databricks.sdk.service.iam import (
    Patch,
    PatchOp,
    PatchSchema,
)

from brickkit.models.enums import PrincipalSource
from brickkit.models.principals import ManagedGroup

from .base import BaseExecutor, ExecutionResult, OperationType

logger = logging.getLogger(__name__)


class GroupExecutor(BaseExecutor[ManagedGroup]):
    """
    Executor for managing Databricks groups.

    Handles:
    - Create (DATABRICKS source only)
    - Update members and entitlements
    - Validate (EXTERNAL source - check exists)
    - Sync (full reconciliation)

    Example:
        ```python
        executor = GroupExecutor(workspace_client)

        group = ManagedGroup(name="grp_data_engineering")
        group.add_user("alice@company.com")
        group.add_entitlement("workspace-access")

        result = executor.create(group)
        result = executor.sync_members(group)
        ```
    """

    def get_resource_type(self) -> str:
        """Get the resource type name."""
        return "Group"

    def exists(self, resource: ManagedGroup) -> bool:
        """Check if the group exists."""
        return self._get_by_name(resource.resolved_name) is not None

    def create(self, resource: ManagedGroup) -> ExecutionResult:
        """
        Create or validate a group.

        For DATABRICKS source: Creates the group if it doesn't exist
        For EXTERNAL source: Validates the group exists (synced via SCIM)
        """
        start_time = self._start_timer()
        resource_name = resource.resolved_name

        if self.dry_run:
            return ExecutionResult(
                success=True,
                operation=OperationType.NO_OP,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message=f"[DRY RUN] Would create group {resource_name}",
                duration_seconds=self._elapsed(start_time),
            )

        # For external groups, just validate existence
        if resource.source == PrincipalSource.EXTERNAL:
            return self._validate_external(resource, start_time)

        # Check if already exists
        existing = self._get_by_name(resource_name)
        if existing:
            resource._sdk_id = existing.id
            return ExecutionResult(
                success=True,
                operation=OperationType.SKIPPED,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message=f"Group {resource_name} already exists",
                duration_seconds=self._elapsed(start_time),
            )

        # Create via SDK
        sdk_group = resource.to_sdk_group()
        try:
            result = self.client.groups.create(
                display_name=sdk_group.display_name,
                entitlements=sdk_group.entitlements,
                members=sdk_group.members,
                roles=sdk_group.roles,
            )
            resource._sdk_id = result.id

            return ExecutionResult(
                success=True,
                operation=OperationType.CREATE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message=f"Created group {resource_name}",
                duration_seconds=self._elapsed(start_time),
            )
        except (ResourceConflict, AlreadyExists):
            # Group was created between our check and create - idempotent success
            existing = self._get_by_name(resource_name)
            if existing:
                resource._sdk_id = existing.id
            return ExecutionResult(
                success=True,
                operation=OperationType.SKIPPED,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message=f"Group {resource_name} already exists",
                duration_seconds=self._elapsed(start_time),
            )
        except Exception as e:
            return self._handle_error(OperationType.CREATE, resource_name, e)

    def update(self, resource: ManagedGroup) -> ExecutionResult:
        """Update a group (sync members and entitlements)."""
        # For groups, update means sync members and entitlements
        member_result = self.sync_members(resource)
        if not member_result.success:
            return member_result

        entitlement_result = self.sync_entitlements(resource)
        if not entitlement_result.success:
            return entitlement_result

        return ExecutionResult(
            success=True,
            operation=OperationType.UPDATE,
            resource_type=self.get_resource_type(),
            resource_name=resource.resolved_name,
            message="Synced members and entitlements",
        )

    def delete(self, resource: ManagedGroup) -> ExecutionResult:
        """Delete a group."""
        start_time = self._start_timer()
        resource_name = resource.resolved_name

        if self.dry_run:
            return ExecutionResult(
                success=True,
                operation=OperationType.NO_OP,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message=f"[DRY RUN] Would delete group {resource_name}",
                duration_seconds=self._elapsed(start_time),
            )

        # Cannot delete external groups
        if resource.source == PrincipalSource.EXTERNAL:
            return ExecutionResult(
                success=False,
                operation=OperationType.DELETE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message=f"Cannot delete external group {resource_name} - managed by IdP",
                duration_seconds=self._elapsed(start_time),
            )

        existing = self._get_by_name(resource_name)
        if not existing:
            return ExecutionResult(
                success=True,
                operation=OperationType.SKIPPED,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message=f"Group {resource_name} does not exist",
                duration_seconds=self._elapsed(start_time),
            )

        try:
            if not existing.id:
                raise ValueError(f"Group {resource_name} has no ID")
            self.client.groups.delete(existing.id)
            return ExecutionResult(
                success=True,
                operation=OperationType.DELETE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message=f"Deleted group {resource_name}",
                duration_seconds=self._elapsed(start_time),
            )
        except Exception as e:
            return self._handle_error(OperationType.DELETE, resource_name, e)

    def sync_members(self, resource: ManagedGroup) -> ExecutionResult:
        """
        Sync group membership to desired state.

        Adds missing members and removes members not in the desired state.
        """
        start_time = self._start_timer()
        resource_name = resource.resolved_name

        if self.dry_run:
            return ExecutionResult(
                success=True,
                operation=OperationType.NO_OP,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message=f"[DRY RUN] Would sync members for {resource_name}",
                duration_seconds=self._elapsed(start_time),
            )

        existing = self._get_by_name(resource_name)
        if not existing:
            return ExecutionResult(
                success=False,
                operation=OperationType.UPDATE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message=f"Group {resource_name} does not exist",
                duration_seconds=self._elapsed(start_time),
            )

        # Calculate diff
        desired_members: Set[str] = {m.resolved_name for m in resource.members}
        current_members: Set[str] = {m.value for m in (existing.members or []) if m.value}

        to_add = desired_members - current_members
        to_remove = current_members - desired_members

        if not to_add and not to_remove:
            return ExecutionResult(
                success=True,
                operation=OperationType.SKIPPED,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message="Members already in sync",
                duration_seconds=self._elapsed(start_time),
            )

        # Use PATCH to update
        if not existing.id:
            raise ValueError(f"Group {resource_name} has no ID")

        try:
            operations = []
            if to_add:
                operations.append(Patch(op=PatchOp.ADD, path="members", value=[{"value": m} for m in to_add]))
            if to_remove:
                for m in to_remove:
                    operations.append(Patch(op=PatchOp.REMOVE, path=f'members[value eq "{m}"]'))

            self.client.groups.patch(
                id=existing.id,
                operations=operations,
                schemas=[PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
            )

            return ExecutionResult(
                success=True,
                operation=OperationType.UPDATE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message=f"Added {len(to_add)}, removed {len(to_remove)} members",
                duration_seconds=self._elapsed(start_time),
                changes={"added": list(to_add), "removed": list(to_remove)},
            )
        except Exception as e:
            return self._handle_error(OperationType.UPDATE, resource_name, e)

    def sync_entitlements(self, resource: ManagedGroup) -> ExecutionResult:
        """
        Sync group entitlements to desired state.

        Adds missing entitlements and removes entitlements not in the desired state.
        """
        start_time = self._start_timer()
        resource_name = resource.resolved_name

        if self.dry_run:
            return ExecutionResult(
                success=True,
                operation=OperationType.NO_OP,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message=f"[DRY RUN] Would sync entitlements for {resource_name}",
                duration_seconds=self._elapsed(start_time),
            )

        existing = self._get_by_name(resource_name)
        if not existing:
            return ExecutionResult(
                success=False,
                operation=OperationType.UPDATE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message=f"Group {resource_name} does not exist",
                duration_seconds=self._elapsed(start_time),
            )

        # Calculate diff
        desired_entitlements: Set[str] = set(resource.entitlements)
        current_entitlements: Set[str] = {e.value for e in (existing.entitlements or []) if e.value}

        to_add = desired_entitlements - current_entitlements
        to_remove = current_entitlements - desired_entitlements

        if not to_add and not to_remove:
            return ExecutionResult(
                success=True,
                operation=OperationType.SKIPPED,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message="Entitlements already in sync",
                duration_seconds=self._elapsed(start_time),
            )

        if not existing.id:
            raise ValueError(f"Group {resource_name} has no ID")

        # Use PATCH to update
        try:
            operations = []
            if to_add:
                operations.append(Patch(op=PatchOp.ADD, path="entitlements", value=[{"value": e} for e in to_add]))
            if to_remove:
                for e in to_remove:
                    operations.append(Patch(op=PatchOp.REMOVE, path=f'entitlements[value eq "{e}"]'))

            self.client.groups.patch(
                id=existing.id,
                operations=operations,
                schemas=[PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
            )

            return ExecutionResult(
                success=True,
                operation=OperationType.UPDATE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message=f"Added {len(to_add)}, removed {len(to_remove)} entitlements",
                duration_seconds=self._elapsed(start_time),
                changes={"added": list(to_add), "removed": list(to_remove)},
            )
        except Exception as e:
            return self._handle_error(OperationType.UPDATE, resource_name, e)

    def sync(self, resource: ManagedGroup) -> ExecutionResult:
        """
        Full sync: create if needed, then sync members and entitlements.
        """
        # First ensure group exists
        create_result = self.create(resource)
        if not create_result.success and create_result.operation != OperationType.SKIPPED:
            return create_result

        # Sync members
        member_result = self.sync_members(resource)

        # Sync entitlements
        entitlement_result = self.sync_entitlements(resource)

        # Aggregate results
        if not member_result.success or not entitlement_result.success:
            return ExecutionResult(
                success=False,
                operation=OperationType.UPDATE,
                resource_type=self.get_resource_type(),
                resource_name=resource.resolved_name,
                message="Sync partially failed",
                changes={
                    "create": create_result.message,
                    "members": member_result.message,
                    "entitlements": entitlement_result.message,
                },
            )

        return ExecutionResult(
            success=True,
            operation=OperationType.UPDATE,
            resource_type=self.get_resource_type(),
            resource_name=resource.resolved_name,
            message=f"Synced group {resource.resolved_name}",
            changes={
                "create": create_result.message,
                "members": member_result.message,
                "entitlements": entitlement_result.message,
            },
        )

    def _get_by_name(self, name: str) -> Optional[SdkGroup]:
        """Find group by display name."""
        try:
            groups = list(self.client.groups.list(filter=f'displayName eq "{name}"'))
            return groups[0] if groups else None
        except (NotFound, ResourceDoesNotExist):
            return None
        except Exception as e:
            logger.warning(f"Error looking up group {name}: {e}")
            return None

    def _validate_external(self, resource: ManagedGroup, start_time: float) -> ExecutionResult:
        """Validate external group exists."""
        resource_name = resource.resolved_name
        existing = self._get_by_name(resource_name)

        if existing:
            resource._sdk_id = existing.id
            if existing.external_id:
                return ExecutionResult(
                    success=True,
                    operation=OperationType.SKIPPED,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message=f"External group {resource_name} exists (external_id: {existing.external_id})",
                    duration_seconds=self._elapsed(start_time),
                )
            else:
                return ExecutionResult(
                    success=True,
                    operation=OperationType.SKIPPED,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message=f"Group {resource_name} exists (not externally synced)",
                    duration_seconds=self._elapsed(start_time),
                )

        return ExecutionResult(
            success=False,
            operation=OperationType.SKIPPED,
            resource_type=self.get_resource_type(),
            resource_name=resource_name,
            message=f"External group {resource_name} not found - check SCIM sync configuration",
            duration_seconds=self._elapsed(start_time),
        )

    def _start_timer(self) -> float:
        """Start a timer for duration tracking."""
        import time

        return time.time()

    def _elapsed(self, start_time: float) -> float:
        """Get elapsed time since start."""
        import time

        return time.time() - start_time
