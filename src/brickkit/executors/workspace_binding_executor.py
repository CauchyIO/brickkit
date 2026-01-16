"""
Executor for managing catalog workspace bindings in Unity Catalog.

Handles the association between catalogs and workspaces for ISOLATED catalogs.
"""

import logging
from typing import List, Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, PermissionDenied, ResourceDoesNotExist

from brickkit.models import Catalog, IsolationMode

from .base import BaseExecutor, ExecutionPlan, ExecutionResult, OperationType

logger = logging.getLogger(__name__)


class WorkspaceBindingExecutor(BaseExecutor[Catalog]):
    """
    Manages workspace bindings for Unity Catalog catalogs.
    
    Only ISOLATED catalogs support workspace bindings. OPEN catalogs are 
    accessible from all workspaces by default.
    """

    def __init__(
        self,
        client: WorkspaceClient,
        dry_run: bool = False,
        force: bool = False
    ):
        """
        Initialize the workspace binding executor.
        
        Args:
            client: Databricks workspace client
            dry_run: If True, only log actions without executing
            force: If True, force updates even if they appear unchanged
        """
        super().__init__(client, dry_run, force)
        self.resource_type = "WORKSPACE_BINDING"

    def update_bindings(
        self,
        catalog: Catalog,
        workspace_ids: Optional[List[int]] = None
    ) -> ExecutionResult:
        """
        Update workspace bindings for a catalog.
        
        Args:
            catalog: The catalog to update bindings for
            workspace_ids: List of workspace IDs to bind. If None, uses catalog.workspace_ids
            
        Returns:
            ExecutionResult with operation status
        """
        import time
        start_time = time.time()

        # Use provided workspace_ids or fall back to catalog's
        target_workspace_ids = workspace_ids if workspace_ids is not None else catalog.workspace_ids

        # Validate catalog mode
        if catalog.isolation_mode != IsolationMode.ISOLATED:
            return ExecutionResult(
                success=False,
                operation=OperationType.NO_OP,
                resource_type=self.resource_type,
                resource_name=catalog.resolved_name,
                message=f"Catalog is {catalog.isolation_mode.value} mode, workspace bindings not applicable",
                duration_seconds=time.time() - start_time
            )

        if not target_workspace_ids:
            return ExecutionResult(
                success=False,
                operation=OperationType.NO_OP,
                resource_type=self.resource_type,
                resource_name=catalog.resolved_name,
                message="No workspace IDs provided for ISOLATED catalog",
                duration_seconds=time.time() - start_time
            )

        try:
            # Get current bindings
            current_bindings = self._get_current_bindings(catalog.resolved_name)
            current_ids = set(current_bindings)
            target_ids = set(target_workspace_ids)

            # Calculate changes
            to_add = target_ids - current_ids
            to_remove = current_ids - target_ids

            if not to_add and not to_remove:
                return ExecutionResult(
                    success=True,
                    operation=OperationType.NO_OP,
                    resource_type=self.resource_type,
                    resource_name=catalog.resolved_name,
                    message="Workspace bindings already up to date",
                    duration_seconds=time.time() - start_time
                )

            if self.dry_run:
                changes = {}
                if to_add:
                    changes["workspaces_to_add"] = list(to_add)
                if to_remove:
                    changes["workspaces_to_remove"] = list(to_remove)

                return ExecutionResult(
                    success=True,
                    operation=OperationType.UPDATE,
                    resource_type=self.resource_type,
                    resource_name=catalog.resolved_name,
                    message="[DRY RUN] Would update workspace bindings",
                    changes=changes,
                    duration_seconds=time.time() - start_time
                )

            # Apply the update
            self.client.workspace_bindings.update(
                name=catalog.resolved_name,
                assign_workspaces=list(to_add),
                unassign_workspaces=list(to_remove)
            )

            return ExecutionResult(
                success=True,
                operation=OperationType.UPDATE,
                resource_type=self.resource_type,
                resource_name=catalog.resolved_name,
                message=f"Updated workspace bindings: +{len(to_add)} -{len(to_remove)} workspaces",
                changes={
                    "added": list(to_add),
                    "removed": list(to_remove)
                },
                duration_seconds=time.time() - start_time
            )

        except PermissionDenied as e:
            logger.error(f"Permission denied updating workspace bindings: {e}")
            raise
        except (ResourceDoesNotExist, NotFound) as e:
            return ExecutionResult(
                success=False,
                operation=OperationType.UPDATE,
                resource_type=self.resource_type,
                resource_name=catalog.resolved_name,
                message=f"Catalog not found: {e}",
                error=e,
                duration_seconds=time.time() - start_time
            )

    def _get_current_bindings(self, catalog_name: str) -> List[int]:
        """Get current workspace bindings for a catalog."""
        try:
            bindings = self.client.workspace_bindings.get_bindings(
                securable_type="catalog",
                securable_name=catalog_name
            )
            return [b.workspace_id for b in bindings.workspaces if b.workspace_id]
        except (ResourceDoesNotExist, NotFound):
            return []
        except PermissionDenied as e:
            logger.error(f"Permission denied getting bindings for {catalog_name}: {e}")
            raise

    def remove_bindings(self, catalog_name: str) -> ExecutionResult:
        """
        Remove all workspace bindings from a catalog by name.
        
        Args:
            catalog_name: The name of the catalog to remove bindings from
            
        Returns:
            ExecutionResult with operation status
        """
        # Create a temporary catalog object for the operation
        from brickkit.models import Catalog, IsolationMode
        temp_catalog = Catalog(name=catalog_name, isolation_mode=IsolationMode.ISOLATED)
        return self.remove_all_bindings(temp_catalog)

    def remove_all_bindings(self, catalog: Catalog) -> ExecutionResult:
        """
        Remove all workspace bindings from a catalog.
        
        Args:
            catalog: The catalog to remove bindings from
            
        Returns:
            ExecutionResult with operation status
        """
        import time
        start_time = time.time()

        if catalog.isolation_mode != IsolationMode.ISOLATED:
            return ExecutionResult(
                success=False,
                operation=OperationType.NO_OP,
                resource_type=self.resource_type,
                resource_name=catalog.resolved_name,
                message=f"Catalog is {catalog.isolation_mode.value} mode, cannot remove bindings",
                duration_seconds=time.time() - start_time
            )

        try:
            current_bindings = self._get_current_bindings(catalog.resolved_name)

            if not current_bindings:
                return ExecutionResult(
                    success=True,
                    operation=OperationType.NO_OP,
                    resource_type=self.resource_type,
                    resource_name=catalog.resolved_name,
                    message="No workspace bindings to remove",
                    duration_seconds=time.time() - start_time
                )

            if self.dry_run:
                return ExecutionResult(
                    success=True,
                    operation=OperationType.DELETE,
                    resource_type=self.resource_type,
                    resource_name=catalog.resolved_name,
                    message=f"[DRY RUN] Would remove {len(current_bindings)} workspace bindings",
                    changes={"workspaces_to_remove": current_bindings},
                    duration_seconds=time.time() - start_time
                )

            # Remove all bindings
            self.client.workspace_bindings.update(
                name=catalog.resolved_name,
                assign_workspaces=[],
                unassign_workspaces=current_bindings
            )

            return ExecutionResult(
                success=True,
                operation=OperationType.DELETE,
                resource_type=self.resource_type,
                resource_name=catalog.resolved_name,
                message=f"Removed {len(current_bindings)} workspace bindings",
                changes={"removed": current_bindings},
                duration_seconds=time.time() - start_time
            )

        except PermissionDenied as e:
            logger.error(f"Permission denied removing workspace bindings: {e}")
            raise
        except (ResourceDoesNotExist, NotFound) as e:
            return ExecutionResult(
                success=False,
                operation=OperationType.DELETE,
                resource_type=self.resource_type,
                resource_name=catalog.resolved_name,
                message=f"Catalog not found: {e}",
                error=e,
                duration_seconds=time.time() - start_time
            )

    def plan(self, catalog: Catalog) -> ExecutionPlan:  # type: ignore[override]
        """
        Create an execution plan for workspace binding updates.
        
        Args:
            catalog: The catalog to plan updates for
            
        Returns:
            ExecutionPlan with planned operations
        """
        plan = ExecutionPlan()

        if catalog.isolation_mode != IsolationMode.ISOLATED:
            # No operations needed for OPEN catalogs
            return plan

        if not catalog.workspace_ids:
            # Warning: ISOLATED catalog without bindings
            logger.warning(
                f"ISOLATED catalog '{catalog.name}' has no workspace bindings defined"
            )
            return plan

        # Get current state
        current_bindings = self._get_current_bindings(catalog.resolved_name)
        current_ids = set(current_bindings)
        target_ids = set(catalog.workspace_ids)

        to_add = target_ids - current_ids
        to_remove = current_ids - target_ids

        if to_add or to_remove:
            changes = {}
            if to_add:
                changes["workspaces_to_add"] = list(to_add)
            if to_remove:
                changes["workspaces_to_remove"] = list(to_remove)

            plan.add_operation(
                operation=OperationType.UPDATE,
                resource_type=self.resource_type,
                resource_name=catalog.resolved_name,
                changes=changes
            )

        return plan
