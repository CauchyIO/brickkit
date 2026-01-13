"""
Metastore assignment executor for Unity Catalog operations.

Handles assigning metastores to workspaces via the Databricks SDK.
"""

import time
from typing import Optional
import logging
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import MetastoreAssignment
from databricks.sdk.errors import ResourceDoesNotExist, NotFound, PermissionDenied
from .base import ExecutionResult, OperationType

logger = logging.getLogger(__name__)


class MetastoreAssignmentExecutor:
    """Executor for metastore-to-workspace assignments."""
    
    def __init__(self, client: WorkspaceClient, dry_run: bool = False):
        """
        Initialize the metastore assignment executor.
        
        Args:
            client: Databricks WorkspaceClient
            dry_run: If True, only simulate operations
        """
        self.client = client
        self.dry_run = dry_run
    
    def get_resource_type(self) -> str:
        """Get the resource type."""
        return "METASTORE_ASSIGNMENT"
    
    def current_assignment(self) -> Optional[MetastoreAssignment]:
        """Get the current metastore assignment for the workspace."""
        try:
            return self.client.metastores.current()
        except (ResourceDoesNotExist, NotFound):
            return None
        except PermissionDenied as e:
            logger.error(f"Permission denied checking metastore assignment: {e}")
            raise
    
    def assign(self, metastore_id: str, workspace_id: int, default_catalog: Optional[str] = None) -> ExecutionResult:
        """
        Assign a metastore to a workspace.
        
        Args:
            metastore_id: The metastore ID to assign
            workspace_id: The workspace ID to assign to
            default_catalog: Optional default catalog for the workspace
            
        Returns:
            ExecutionResult indicating success or failure
        """
        start_time = time.time()
        
        try:
            if self.dry_run:
                logger.info(f"[DRY RUN] Would assign metastore {metastore_id} to workspace {workspace_id}")
                return ExecutionResult(
                    success=True,
                    operation=OperationType.CREATE,
                    resource_type=self.get_resource_type(),
                    resource_name=f"{metastore_id}_{workspace_id}",
                    message="Would assign metastore (dry run)"
                )
            
            # Check current assignment
            current = self.current_assignment()
            if current and current.metastore_id == metastore_id:
                logger.info(f"Metastore {metastore_id} already assigned to workspace {workspace_id}")
                return ExecutionResult(
                    success=True,
                    operation=OperationType.NO_OP,
                    resource_type=self.get_resource_type(),
                    resource_name=f"{metastore_id}_{workspace_id}",
                    message="Already assigned"
                )
            
            logger.info(f"Assigning metastore {metastore_id} to workspace {workspace_id}")
            
            # Perform assignment
            if default_catalog:
                self.client.metastores.assign(
                    metastore_id=metastore_id,
                    workspace_id=workspace_id,
                    default_catalog_name=default_catalog
                )
            else:
                # Try to find an existing catalog to use as default
                default_catalog_name = self._find_default_catalog(metastore_id)
                if not default_catalog_name:
                    return ExecutionResult(
                        success=False,
                        operation=OperationType.CREATE,
                        resource_type=self.get_resource_type(),
                        resource_name=f"{metastore_id}_{workspace_id}",
                        message="No catalogs available in metastore to set as default. Create a catalog first or specify default_catalog parameter.",
                        duration_seconds=time.time() - start_time
                    )
                
                self.client.metastores.assign(
                    metastore_id=metastore_id,
                    workspace_id=workspace_id,
                    default_catalog_name=default_catalog_name
                )
            
            duration = time.time() - start_time
            return ExecutionResult(
                success=True,
                operation=OperationType.CREATE,
                resource_type=self.get_resource_type(),
                resource_name=f"{metastore_id}_{workspace_id}",
                message="Assigned metastore successfully",
                duration_seconds=duration
            )
            
        except PermissionDenied as e:
            logger.error(f"Permission denied assigning metastore: {e}")
            raise
        except Exception as e:
            return ExecutionResult(
                success=False,
                operation=OperationType.CREATE,
                resource_type=self.get_resource_type(),
                resource_name=f"{metastore_id}_{workspace_id}",
                message=str(e),
                duration_seconds=time.time() - start_time,
                error=e
            )

    def unassign(self, metastore_id: str, workspace_id: int) -> ExecutionResult:
        """
        Unassign a metastore from a workspace.
        
        Args:
            metastore_id: The metastore ID to unassign
            workspace_id: The workspace ID to unassign from
            
        Returns:
            ExecutionResult indicating success or failure
        """
        start_time = time.time()
        
        try:
            if self.dry_run:
                logger.info(f"[DRY RUN] Would unassign metastore {metastore_id} from workspace {workspace_id}")
                return ExecutionResult(
                    success=True,
                    operation=OperationType.DELETE,
                    resource_type=self.get_resource_type(),
                    resource_name=f"{metastore_id}_{workspace_id}",
                    message="Would unassign metastore (dry run)"
                )
            
            # Check current assignment
            current = self.current_assignment()
            if not current or current.metastore_id != metastore_id:
                logger.info(f"Metastore {metastore_id} not assigned to workspace {workspace_id}")
                return ExecutionResult(
                    success=True,
                    operation=OperationType.NO_OP,
                    resource_type=self.get_resource_type(),
                    resource_name=f"{metastore_id}_{workspace_id}",
                    message="Not assigned"
                )
            
            logger.info(f"Unassigning metastore {metastore_id} from workspace {workspace_id}")
            
            # Perform unassignment
            self.client.metastores.unassign(
                metastore_id=metastore_id,
                workspace_id=workspace_id
            )
            
            duration = time.time() - start_time
            return ExecutionResult(
                success=True,
                operation=OperationType.DELETE,
                resource_type=self.get_resource_type(),
                resource_name=f"{metastore_id}_{workspace_id}",
                message="Unassigned metastore successfully",
                duration_seconds=duration
            )
            
        except PermissionDenied as e:
            logger.error(f"Permission denied unassigning metastore: {e}")
            raise
        except Exception as e:
            return ExecutionResult(
                success=False,
                operation=OperationType.DELETE,
                resource_type=self.get_resource_type(),
                resource_name=f"{metastore_id}_{workspace_id}",
                message=str(e),
                duration_seconds=time.time() - start_time,
                error=e
            )

    def update_default_catalog(self, metastore_id: str, workspace_id: int, default_catalog: str) -> ExecutionResult:
        """
        Update the default catalog for a metastore assignment.
        
        Args:
            metastore_id: The metastore ID
            workspace_id: The workspace ID
            default_catalog: The new default catalog
            
        Returns:
            ExecutionResult indicating success or failure
        """
        start_time = time.time()
        
        try:
            if self.dry_run:
                logger.info(f"[DRY RUN] Would update default catalog to {default_catalog}")
                return ExecutionResult(
                    success=True,
                    operation=OperationType.UPDATE,
                    resource_type=self.get_resource_type(),
                    resource_name=f"{metastore_id}_{workspace_id}",
                    message="Would update default catalog (dry run)"
                )
            
            # Check current assignment
            current = self.current_assignment()
            if not current or current.metastore_id != metastore_id:
                logger.error(f"Metastore {metastore_id} not assigned to workspace {workspace_id}")
                return ExecutionResult(
                    success=False,
                    operation=OperationType.UPDATE,
                    resource_type=self.get_resource_type(),
                    resource_name=f"{metastore_id}_{workspace_id}",
                    message="Metastore not assigned to workspace"
                )
            
            if current.default_catalog_name == default_catalog:
                logger.info(f"Default catalog already set to {default_catalog}")
                return ExecutionResult(
                    success=True,
                    operation=OperationType.NO_OP,
                    resource_type=self.get_resource_type(),
                    resource_name=f"{metastore_id}_{workspace_id}",
                    message="Default catalog unchanged"
                )
            
            logger.info(f"Updating default catalog from {current.default_catalog_name} to {default_catalog}")
            
            # Update assignment
            self.client.metastores.update_assignment(
                workspace_id=workspace_id,
                metastore_id=metastore_id,
                default_catalog_name=default_catalog
            )
            
            duration = time.time() - start_time
            return ExecutionResult(
                success=True,
                operation=OperationType.UPDATE,
                resource_type=self.get_resource_type(),
                resource_name=f"{metastore_id}_{workspace_id}",
                message=f"Updated default catalog to {default_catalog}",
                duration_seconds=duration,
                changes={"default_catalog": {"from": current.default_catalog_name, "to": default_catalog}}
            )
            
        except PermissionDenied as e:
            logger.error(f"Permission denied updating default catalog: {e}")
            raise
        except Exception as e:
            return ExecutionResult(
                success=False,
                operation=OperationType.UPDATE,
                resource_type=self.get_resource_type(),
                resource_name=f"{metastore_id}_{workspace_id}",
                message=str(e),
                duration_seconds=time.time() - start_time,
                error=e
            )

    def _find_default_catalog(self, metastore_id: str) -> Optional[str]:
        """Find a suitable catalog to use as default."""
        try:
            catalogs = list(self.client.catalogs.list())
            for catalog in catalogs:
                if catalog.name in ["main", "default", "hive_metastore"]:
                    return catalog.name
            return catalogs[0].name if catalogs else None
        except (ResourceDoesNotExist, NotFound):
            return None
        except PermissionDenied as e:
            logger.error(f"Permission denied listing catalogs: {e}")
            raise
