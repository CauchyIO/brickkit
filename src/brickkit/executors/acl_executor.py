"""
Executor for managing object-level ACLs in Databricks.

Handles setting and updating permissions on workspace objects like
clusters, jobs, notebooks, SQL warehouses, etc.
"""

import logging
from typing import Optional

from databricks.sdk.errors import NotFound, ResourceDoesNotExist
from databricks.sdk.service.iam import ObjectPermissions

from brickkit.models.acls import AclBinding

from .base import BaseExecutor, ExecutionResult, OperationType

logger = logging.getLogger(__name__)


class AclExecutor(BaseExecutor[AclBinding]):
    """
    Executor for managing object-level ACLs.

    Uses the Databricks Permissions API to set permissions on workspace objects
    including clusters, jobs, notebooks, SQL warehouses, and more.

    Example:
        ```python
        executor = AclExecutor(workspace_client)

        # Set permissions on a cluster
        binding = AclBinding.for_cluster("0123-456789-abcdef")
        binding.grant_group("grp_data_engineering", PermissionLevel.CAN_RESTART)
        binding.grant_service_principal("spn_etl", PermissionLevel.CAN_MANAGE)

        result = executor.set_permissions(binding)
        ```
    """

    def get_resource_type(self) -> str:
        """Get the resource type name."""
        return "ACL"

    def exists(self, resource: AclBinding) -> bool:
        """
        Check if the object exists and has permissions.

        Note: This checks if we can read permissions, not if specific
        permissions exist.
        """
        try:
            self.client.permissions.get(
                request_object_type=resource.object_type.value, request_object_id=resource.object_id
            )
            return True
        except (NotFound, ResourceDoesNotExist):
            return False

    def create(self, resource: AclBinding) -> ExecutionResult:
        """Create is the same as set_permissions for ACLs."""
        return self.set_permissions(resource)

    def update(self, resource: AclBinding) -> ExecutionResult:
        """Update is the same as update_permissions for ACLs."""
        return self.update_permissions(resource)

    def delete(self, resource: AclBinding) -> ExecutionResult:
        """
        Delete/clear permissions on an object.

        Note: This sets an empty permission list, effectively removing
        all explicitly set permissions.
        """
        start_time = self._start_timer()
        object_ref = f"{resource.object_type.value}/{resource.object_id}"

        if self.dry_run:
            return ExecutionResult(
                success=True,
                operation=OperationType.NO_OP,
                resource_type=self.get_resource_type(),
                resource_name=object_ref,
                message=f"[DRY RUN] Would clear permissions on {object_ref}",
                duration_seconds=self._elapsed(start_time),
            )

        try:
            self.client.permissions.set(
                request_object_type=resource.object_type.value,
                request_object_id=resource.object_id,
                access_control_list=[],
            )

            return ExecutionResult(
                success=True,
                operation=OperationType.DELETE,
                resource_type=self.get_resource_type(),
                resource_name=object_ref,
                message=f"Cleared permissions on {object_ref}",
                duration_seconds=self._elapsed(start_time),
            )
        except Exception as e:
            return self._handle_error(OperationType.DELETE, object_ref, e)

    def set_permissions(self, resource: AclBinding) -> ExecutionResult:
        """
        Set permissions on an object (replace mode).

        This replaces all existing permissions with the specified ones.
        Use update_permissions() to merge instead.
        """
        start_time = self._start_timer()
        object_ref = f"{resource.object_type.value}/{resource.object_id}"

        if self.dry_run:
            principals = [p.resolved_principal_name for p in resource.permissions]
            return ExecutionResult(
                success=True,
                operation=OperationType.NO_OP,
                resource_type=self.get_resource_type(),
                resource_name=object_ref,
                message=f"[DRY RUN] Would set {len(resource.permissions)} permissions on {object_ref}",
                duration_seconds=self._elapsed(start_time),
                changes={"principals": principals},
            )

        requests = resource.to_access_control_requests()

        try:
            self.client.permissions.set(
                request_object_type=resource.object_type.value,
                request_object_id=resource.object_id,
                access_control_list=requests,
            )

            return ExecutionResult(
                success=True,
                operation=OperationType.UPDATE,
                resource_type=self.get_resource_type(),
                resource_name=object_ref,
                message=f"Set {len(requests)} permissions on {object_ref}",
                duration_seconds=self._elapsed(start_time),
                changes={"permissions_count": len(requests)},
            )
        except Exception as e:
            return self._handle_error(OperationType.UPDATE, object_ref, e)

    def update_permissions(self, resource: AclBinding) -> ExecutionResult:
        """
        Update permissions on an object (merge mode).

        This merges the specified permissions with existing ones.
        Use set_permissions() to replace instead.
        """
        start_time = self._start_timer()
        object_ref = f"{resource.object_type.value}/{resource.object_id}"

        if self.dry_run:
            principals = [p.resolved_principal_name for p in resource.permissions]
            return ExecutionResult(
                success=True,
                operation=OperationType.NO_OP,
                resource_type=self.get_resource_type(),
                resource_name=object_ref,
                message=f"[DRY RUN] Would update {len(resource.permissions)} permissions on {object_ref}",
                duration_seconds=self._elapsed(start_time),
                changes={"principals": principals},
            )

        requests = resource.to_access_control_requests()

        try:
            self.client.permissions.update(
                request_object_type=resource.object_type.value,
                request_object_id=resource.object_id,
                access_control_list=requests,
            )

            return ExecutionResult(
                success=True,
                operation=OperationType.UPDATE,
                resource_type=self.get_resource_type(),
                resource_name=object_ref,
                message=f"Updated {len(requests)} permissions on {object_ref}",
                duration_seconds=self._elapsed(start_time),
                changes={"permissions_count": len(requests)},
            )
        except Exception as e:
            return self._handle_error(OperationType.UPDATE, object_ref, e)

    def get_permissions(self, resource: AclBinding) -> Optional[ObjectPermissions]:
        """
        Get current permissions on an object.

        Args:
            resource: The ACL binding (only object_type and object_id are used)

        Returns:
            ObjectPermissions if found, None otherwise
        """
        try:
            return self.client.permissions.get(
                request_object_type=resource.object_type.value, request_object_id=resource.object_id
            )
        except (NotFound, ResourceDoesNotExist):
            return None

    def get_permission_levels(self, resource: AclBinding):
        """
        Get available permission levels for an object type.

        Args:
            resource: The ACL binding (only object_type and object_id are used)

        Returns:
            GetPermissionLevelsResponse with available levels
        """
        return self.client.permissions.get_permission_levels(
            request_object_type=resource.object_type.value, request_object_id=resource.object_id
        )

    def _start_timer(self) -> float:
        """Start a timer for duration tracking."""
        import time

        return time.time()

    def _elapsed(self, start_time: float) -> float:
        """Get elapsed time since start."""
        import time

        return time.time() - start_time
