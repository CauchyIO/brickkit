"""
Catalog executor for Unity Catalog operations.

Handles creation, update, and deletion of catalogs via the Databricks SDK.
"""

from typing import Dict, Any, Optional, List
import logging
import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import CatalogInfo, IsolationMode as SDKIsolationMode
from databricks.sdk.errors import (
    ResourceDoesNotExist,
    ResourceAlreadyExists,
    PermissionDenied,
    NotFound,
    BadRequest,
    InvalidParameterValue,
)
from ..models import Catalog
from ..models.enums import IsolationMode
from .base import BaseExecutor, ExecutionResult, OperationType

logger = logging.getLogger(__name__)


class CatalogExecutor(BaseExecutor[Catalog]):
    """Executor for catalog operations."""
    
    def get_resource_type(self) -> str:
        """Get the resource type."""
        return "CATALOG"
    
    def exists(self, catalog: Catalog) -> bool:
        """
        Check if a catalog exists.
        
        Args:
            catalog: The catalog to check
            
        Returns:
            True if catalog exists, False otherwise
        """
        try:
            self.client.catalogs.get(catalog.resolved_name)
            return True
        except (ResourceDoesNotExist, NotFound):
            return False
        except PermissionDenied as e:
            # Caller lacks permission to check - propagate
            logger.error(f"Permission denied checking catalog existence: {e}")
            raise
    
    def create(self, catalog: Catalog) -> ExecutionResult:
        """
        Create a new catalog.
        
        Args:
            catalog: The catalog to create
            
        Returns:
            ExecutionResult indicating success or failure
        """
        start_time = time.time()
        resource_name = catalog.resolved_name
        
        try:
            if self.dry_run:
                logger.info(f"[DRY RUN] Would create catalog {resource_name}")
                return ExecutionResult(
                    success=True,
                    operation=OperationType.CREATE,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message="Would be created (dry run)"
                )
            
            # Determine workspace IDs to bind (if any) BEFORE creating catalog
            workspace_ids_to_bind = []
            if catalog.workspace_ids:
                # Use the workspace IDs that were set by Team.add_catalog()
                workspace_ids_to_bind = [str(ws_id) for ws_id in catalog.workspace_ids]
                logger.info(f"Catalog has workspace bindings from Team configuration: {workspace_ids_to_bind}")
            elif catalog.isolation_mode == IsolationMode.ISOLATED:
                logger.warning(
                    f"ISOLATED catalog {resource_name} has no workspace_ids set! "
                    f"This catalog should have been added to a Team using Team.add_catalog() "
                    f"which would set the appropriate workspace bindings."
                )

            # Convert to SDK parameters
            params = catalog.to_sdk_create_params()

            logger.info(f"Creating catalog {resource_name} with params: {params}")
            self.execute_with_retry(self.client.catalogs.create, **params)

            # Add rollback operation
            self._rollback_stack.append(
                lambda: self.client.catalogs.delete(resource_name, force=True)
            )

            # CRITICAL ORDERING for ISOLATED catalogs with workspace bindings:
            # 1. Create catalog (in default/OPEN mode)
            # 2. Apply workspace bindings FIRST (while catalog is still accessible)
            # 3. THEN set isolation mode to ISOLATED
            # This avoids the catch-22 where an ISOLATED catalog without bindings is inaccessible

            # Apply workspace bindings BEFORE setting isolation mode (if needed)
            if workspace_ids_to_bind and catalog.isolation_mode == IsolationMode.ISOLATED:
                logger.info(f"Step 1: Applying workspace bindings BEFORE setting ISOLATED mode")
                logger.info(f"  Catalog: {resource_name}")
                logger.info(f"  Target workspace IDs: {workspace_ids_to_bind}")

                try:
                    # Try the catalog-specific update() method which is simpler
                    workspace_ids_as_ints = [int(ws_id) for ws_id in workspace_ids_to_bind]
                    logger.info(f"Calling workspace_bindings.update API")
                    logger.info(f"  catalog name: {resource_name}")
                    logger.info(f"  assign_workspaces: {workspace_ids_as_ints}")

                    result = self.client.workspace_bindings.update(
                        name=resource_name,
                        assign_workspaces=workspace_ids_as_ints
                    )

                    logger.info(f"✓ workspace_bindings.update API call succeeded!")
                    if result:
                        logger.info(f"  API Response type: {type(result)}")

                    # Brief wait for propagation
                    time.sleep(2)

                except PermissionDenied as e:
                    # Permission errors should propagate - caller lacks rights
                    logger.error(f"Permission denied applying workspace bindings to {resource_name}: {e}")
                    raise
                except (NotFound, ResourceDoesNotExist) as e:
                    # This can happen if catalog creation is still propagating
                    logger.warning(f"Catalog {resource_name} not found for binding (may be propagating): {e}")
                except (BadRequest, InvalidParameterValue) as e:
                    # Invalid parameters - log error but catalog exists
                    logger.error(f"Invalid workspace binding request for {resource_name}: {e}")

            # NOW set isolation mode AFTER bindings are applied
            if catalog.isolation_mode:
                from databricks.sdk.service.catalog import CatalogIsolationMode

                logger.info(f"Step 2: Setting catalog isolation mode to {catalog.isolation_mode.value}")
                update_params = {
                    "name": resource_name,
                    "isolation_mode": CatalogIsolationMode(catalog.isolation_mode.value)
                }

                self.execute_with_retry(
                    self.client.catalogs.update,
                    **update_params
                )
                logger.info(f"✓ Isolation mode set to {catalog.isolation_mode.value}")

            # Final verification of bindings (if they were applied)
            if workspace_ids_to_bind and catalog.isolation_mode == IsolationMode.ISOLATED:
                logger.info(f"Step 3: Final verification of workspace bindings")
                time.sleep(2)  # Wait for everything to propagate

                try:
                    verification = self.client.workspace_bindings.get(name=resource_name)
                    if verification and hasattr(verification, 'workspaces') and verification.workspaces:
                        applied_ws_ids = [str(ws.workspace_id) for ws in verification.workspaces]
                        logger.info(f"✓ FINAL VERIFICATION: Catalog {resource_name} bound to workspaces: {applied_ws_ids}")
                    else:
                        logger.warning(f"⚠️ FINAL VERIFICATION: No workspace bindings found")
                        logger.warning(f"   Expected: {workspace_ids_to_bind}")
                except (NotFound, ResourceDoesNotExist) as e:
                    logger.warning(f"Could not verify bindings - catalog not found: {e}")
                except PermissionDenied as e:
                    logger.warning(f"Could not verify bindings - permission denied: {e}")

            duration = time.time() - start_time
            return ExecutionResult(
                success=True,
                operation=OperationType.CREATE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message="Created successfully",
                duration_seconds=duration
            )
            
        except ResourceAlreadyExists:
            # This shouldn't happen if exists() works correctly
            logger.warning(f"Catalog {resource_name} already exists")
            return ExecutionResult(
                success=True,
                operation=OperationType.NO_OP,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message="Already exists"
            )
        except Exception as e:
            # Check if it's a "already exists" error from SDK
            error_msg = str(e)
            if "already exists" in error_msg.lower():
                logger.info(f"Catalog {resource_name} already exists - will update bindings if needed")

                # Catalog exists, but we may need to update workspace bindings
                # This is important for fixing catalogs that were created without proper bindings
                if workspace_ids_to_bind and catalog.isolation_mode == IsolationMode.ISOLATED:
                    logger.info(f"Existing catalog needs workspace bindings applied")
                    return self._apply_bindings_to_existing_catalog(
                        catalog, resource_name, workspace_ids_to_bind
                    )

                return ExecutionResult(
                    success=True,
                    operation=OperationType.NO_OP,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message="Already exists"
                )
            return self._handle_error(OperationType.CREATE, resource_name, e)
    
    def update(self, catalog: Catalog) -> ExecutionResult:
        """
        Update an existing catalog.

        Args:
            catalog: The catalog to update

        Returns:
            ExecutionResult indicating success or failure
        """
        start_time = time.time()
        resource_name = catalog.resolved_name

        try:
            # Get current state
            existing = self.client.catalogs.get(resource_name)

            # Check if update needed
            changes = self._get_catalog_changes(existing, catalog)

            # ALSO check workspace bindings for ISOLATED catalogs
            workspace_bindings_updated = False
            if catalog.isolation_mode == IsolationMode.ISOLATED and catalog.workspace_ids:
                logger.info(f"Checking workspace bindings for ISOLATED catalog {resource_name}")

                # Check current bindings
                try:
                    current_bindings = self.client.workspace_bindings.get(name=resource_name)
                    current_ws_ids = []
                    if current_bindings and hasattr(current_bindings, 'workspaces') and current_bindings.workspaces:
                        current_ws_ids = sorted([int(ws.workspace_id) for ws in current_bindings.workspaces])

                    desired_ws_ids = sorted([int(ws_id) for ws_id in catalog.workspace_ids])

                    if current_ws_ids != desired_ws_ids:
                        logger.info(f"Workspace bindings need update:")
                        logger.info(f"  Current: {current_ws_ids}")
                        logger.info(f"  Desired: {desired_ws_ids}")

                        if not self.dry_run:
                            # Update workspace bindings
                            logger.info(f"Updating workspace bindings for {resource_name}")
                            self.client.workspace_bindings.update(
                                name=resource_name,
                                assign_workspaces=desired_ws_ids,
                                unassign_workspaces=list(set(current_ws_ids) - set(desired_ws_ids))
                            )
                            workspace_bindings_updated = True
                            changes['workspace_bindings'] = {
                                'from': current_ws_ids,
                                'to': desired_ws_ids
                            }
                    else:
                        logger.debug(f"Workspace bindings are already correct: {current_ws_ids}")

                except PermissionDenied as e:
                    # Permission errors should propagate
                    logger.error(f"Permission denied updating workspace bindings: {e}")
                    raise
                except (NotFound, ResourceDoesNotExist) as e:
                    logger.warning(f"Could not update workspace bindings - resource not found: {e}")
                except (BadRequest, InvalidParameterValue) as e:
                    logger.warning(f"Invalid workspace binding parameters: {e}")

            if not changes and not workspace_bindings_updated:
                return ExecutionResult(
                    success=True,
                    operation=OperationType.NO_OP,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message="No changes needed"
                )

            if self.dry_run:
                logger.info(f"[DRY RUN] Would update catalog {resource_name}")
                return ExecutionResult(
                    success=True,
                    operation=OperationType.UPDATE,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message=f"Would update: {changes} (dry run)",
                    changes=changes
                )

            # Only update catalog properties if there are non-binding changes
            if any(k != 'workspace_bindings' for k in changes.keys()):
                # Convert to SDK parameters
                params = catalog.to_sdk_update_params()

                # Convert isolation_mode string to SDK enum if present
                if "isolation_mode" in params:
                    from databricks.sdk.service.catalog import CatalogIsolationMode
                    params["isolation_mode"] = CatalogIsolationMode(params["isolation_mode"])

                logger.info(f"Updating catalog {resource_name}: {changes}")
                self.execute_with_retry(self.client.catalogs.update, **params)

            duration = time.time() - start_time
            return ExecutionResult(
                success=True,
                operation=OperationType.UPDATE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message=f"Updated: {changes}",
                duration_seconds=duration,
                changes=changes
            )

        except ResourceDoesNotExist:
            # Catalog was deleted between exists() and update()
            logger.warning(f"Catalog {resource_name} no longer exists")
            return self.create(catalog)

        except Exception as e:
            return self._handle_error(OperationType.UPDATE, resource_name, e)
    
    def delete(self, catalog: Catalog) -> ExecutionResult:
        """
        Delete a catalog.
        
        Args:
            catalog: The catalog to delete
            
        Returns:
            ExecutionResult indicating success or failure
        """
        start_time = time.time()
        resource_name = catalog.resolved_name
        
        try:
            if not self.exists(catalog):
                return ExecutionResult(
                    success=True,
                    operation=OperationType.NO_OP,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message="Does not exist"
                )
            
            if self.dry_run:
                logger.info(f"[DRY RUN] Would delete catalog {resource_name}")
                return ExecutionResult(
                    success=True,
                    operation=OperationType.DELETE,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message="Would be deleted (dry run)"
                )
            
            logger.info(f"Deleting catalog {resource_name}")
            self.execute_with_retry(
                self.client.catalogs.delete,
                resource_name,
                force=True  # Force delete even if not empty
            )
            
            duration = time.time() - start_time
            return ExecutionResult(
                success=True,
                operation=OperationType.DELETE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message="Deleted successfully",
                duration_seconds=duration
            )
            
        except Exception as e:
            return self._handle_error(OperationType.DELETE, resource_name, e)
    
    def _apply_bindings_to_existing_catalog(
        self,
        catalog: Catalog,
        resource_name: str,
        workspace_ids_to_bind: List[str]
    ) -> ExecutionResult:
        """
        Apply workspace bindings to an existing catalog.

        This is needed when a catalog was created without proper bindings
        and needs to be fixed.
        """
        logger.info(f"Applying workspace bindings to existing catalog {resource_name}")

        try:
            # First check current bindings
            try:
                current_bindings = self.client.workspace_bindings.get(name=resource_name)
                if current_bindings and hasattr(current_bindings, 'workspaces') and current_bindings.workspaces:
                    current_ws_ids = [str(ws.workspace_id) for ws in current_bindings.workspaces]
                    logger.info(f"  Current bindings: {current_ws_ids}")
                else:
                    logger.info(f"  No current bindings")
            except (NotFound, ResourceDoesNotExist):
                logger.info(f"  No existing bindings found")
            except PermissionDenied as e:
                logger.warning(f"  Could not check bindings - permission denied: {e}")

            # Apply the workspace bindings
            workspace_ids_as_ints = [int(ws_id) for ws_id in workspace_ids_to_bind]
            logger.info(f"  Applying bindings: {workspace_ids_as_ints}")

            result = self.client.workspace_bindings.update(
                name=resource_name,
                assign_workspaces=workspace_ids_as_ints
            )

            logger.info(f"  ✓ Bindings applied successfully")

            # Also ensure isolation mode is set
            if catalog.isolation_mode:
                from databricks.sdk.service.catalog import CatalogIsolationMode
                logger.info(f"  Setting isolation mode to {catalog.isolation_mode.value}")
                self.client.catalogs.update(
                    name=resource_name,
                    isolation_mode=CatalogIsolationMode(catalog.isolation_mode.value)
                )

            # Verify bindings
            time.sleep(2)
            verification = self.client.workspace_bindings.get(name=resource_name)
            if verification and hasattr(verification, 'workspaces') and verification.workspaces:
                applied_ws_ids = [str(ws.workspace_id) for ws in verification.workspaces]
                logger.info(f"  ✓ Verified bindings: {applied_ws_ids}")

            return ExecutionResult(
                success=True,
                operation=OperationType.UPDATE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message="Applied workspace bindings to existing catalog"
            )

        except Exception as e:
            logger.error(f"Failed to apply bindings to existing catalog: {e}")
            return ExecutionResult(
                success=True,  # Catalog exists at least
                operation=OperationType.NO_OP,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message=f"Exists but could not apply bindings: {e}"
            )

    def _get_catalog_changes(
        self,
        existing: CatalogInfo,
        desired: Catalog
    ) -> Dict[str, Any]:
        """
        Compare existing and desired catalog to find changes.
        
        Args:
            existing: Current catalog from Databricks
            desired: Desired catalog configuration
            
        Returns:
            Dictionary of changes needed
        """
        changes = {}
        
        # Check comment
        if hasattr(existing, 'comment') and existing.comment != desired.comment:
            changes['comment'] = {
                'from': existing.comment,
                'to': desired.comment
            }
        
        # Check owner
        if desired.owner:
            desired_owner = desired.owner.resolved_name
            if hasattr(existing, 'owner') and existing.owner != desired_owner:
                changes['owner'] = {
                    'from': existing.owner,
                    'to': desired_owner
                }
        
        # Check isolation mode
        if desired.isolation_mode:
            desired_mode = desired.isolation_mode.value
            if hasattr(existing, 'isolation_mode'):
                existing_mode = existing.isolation_mode.value if existing.isolation_mode else None
                if existing_mode != desired_mode:
                    changes['isolation_mode'] = {
                        'from': existing_mode,
                        'to': desired_mode
                    }
        
        # Check storage root (can only be set at creation)
        if desired.storage_root:
            if hasattr(existing, 'storage_root'):
                existing_root = existing.storage_root
                if existing_root != desired.storage_root:
                    logger.warning(
                        f"Storage root cannot be changed after creation. "
                        f"Current: {existing_root}, Desired: {desired.storage_root}"
                    )
        
        return changes
    
    def _needs_update(self, catalog: Catalog) -> bool:
        """
        Check if a catalog needs updating.
        
        Args:
            catalog: The catalog to check
            
        Returns:
            True if update needed
        """
        try:
            existing = self.client.catalogs.get(catalog.resolved_name)
            changes = self._get_catalog_changes(existing, catalog)
            return bool(changes)
        except ResourceDoesNotExist:
            return False
        except Exception as e:
            logger.warning(f"Error checking if update needed: {e}")
            return False
    
    def _get_changes(self, catalog: Catalog) -> Dict[str, Any]:
        """
        Get the changes that would be made to a catalog.
        
        Args:
            catalog: The catalog
            
        Returns:
            Dictionary of changes
        """
        try:
            existing = self.client.catalogs.get(catalog.resolved_name)
            return self._get_catalog_changes(existing, catalog)
        except ResourceDoesNotExist:
            return {'action': 'create'}
        except Exception as e:
            logger.warning(f"Error getting changes: {e}")
            return {}


