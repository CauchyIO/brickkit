"""
Catalog executor for Unity Catalog operations.

Handles creation, update, and deletion of catalogs via the Databricks SDK.
"""

import logging
import time
from typing import Any, Dict, List

from databricks.sdk.errors import (
    NotFound,
    PermissionDenied,
    ResourceAlreadyExists,
    ResourceDoesNotExist,
)
from databricks.sdk.service.catalog import CatalogInfo

from brickkit.models import Catalog
from brickkit.models.enums import IsolationMode

from .base import BaseExecutor, ExecutionResult, OperationType
from .mixins import WorkspaceBindingMixin

logger = logging.getLogger(__name__)


class CatalogExecutor(BaseExecutor[Catalog], WorkspaceBindingMixin):
    """Executor for catalog operations."""

    def get_resource_type(self) -> str:
        """Get the resource type."""
        return "CATALOG"

    def exists(self, resource: Catalog) -> bool:
        """
        Check if a catalog exists.
        
        Args:
            catalog: The catalog to check
            
        Returns:
            True if catalog exists, False otherwise
        """
        try:
            self.client.catalogs.get(resource.resolved_name)
            return True
        except (ResourceDoesNotExist, NotFound):
            return False
        except PermissionDenied as e:
            # Caller lacks permission to check - propagate
            logger.error(f"Permission denied checking catalog existence: {e}")
            raise

    def create(self, resource: Catalog) -> ExecutionResult:
        """
        Create a new resource.
        
        Args:
            catalog: The catalog to create
            
        Returns:
            ExecutionResult indicating success or failure
        """
        start_time = time.time()
        resource_name = resource.resolved_name

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
            if resource.workspace_ids:
                workspace_ids_to_bind = [str(ws_id) for ws_id in resource.workspace_ids]
            elif resource.isolation_mode == IsolationMode.ISOLATED:
                logger.warning(f"ISOLATED catalog {resource_name} has no workspace_ids - use Team.add_catalog()")

            params = resource.to_sdk_create_params()
            logger.info(f"Creating catalog {resource_name}")
            logger.debug(f"Catalog params: {params}")
            self.execute_with_retry(self.client.catalogs.create, **params)

            # Add rollback operation
            self._rollback_stack.append(
                lambda: self.client.catalogs.delete(resource_name, force=True)
            )

            # CRITICAL ORDERING for ISOLATED catalogs:
            # 1. Create catalog (in default/OPEN mode)
            # 2. Apply workspace bindings FIRST (while catalog is still accessible)
            # 3. THEN set isolation mode to ISOLATED
            if workspace_ids_to_bind and resource.isolation_mode == IsolationMode.ISOLATED:
                workspace_ids_as_ints = [int(ws_id) for ws_id in workspace_ids_to_bind]
                self.apply_workspace_bindings(
                    resource_name=resource_name,
                    workspace_ids=workspace_ids_as_ints,
                    securable_type="catalog"
                )

            # Set isolation mode AFTER bindings are applied
            if resource.isolation_mode:
                from databricks.sdk.service.catalog import CatalogIsolationMode
                logger.debug(f"Setting isolation mode to {resource.isolation_mode.value}")
                self.execute_with_retry(
                    self.client.catalogs.update,
                    name=resource_name,
                    isolation_mode=CatalogIsolationMode(resource.isolation_mode.value)
                )

            # Verify bindings were applied
            if workspace_ids_to_bind and resource.isolation_mode == IsolationMode.ISOLATED:
                workspace_ids_as_ints = [int(ws_id) for ws_id in workspace_ids_to_bind]
                if not self.verify_workspace_bindings(resource_name, workspace_ids_as_ints, "catalog"):
                    logger.warning(f"Workspace binding verification failed for {resource_name}")

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
            error_msg = str(e)
            if "already exists" in error_msg.lower():
                # Catalog exists - update bindings if needed
                if workspace_ids_to_bind and resource.isolation_mode == IsolationMode.ISOLATED:
                    return self._apply_bindings_to_existing_catalog(
                        resource, resource_name, workspace_ids_to_bind
                    )
                return ExecutionResult(
                    success=True,
                    operation=OperationType.NO_OP,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message="Already exists"
                )
            return self._handle_error(OperationType.CREATE, resource_name, e)

    def update(self, resource: Catalog) -> ExecutionResult:
        """
        Update an existing resource.

        Args:
            catalog: The catalog to update

        Returns:
            ExecutionResult indicating success or failure
        """
        start_time = time.time()
        resource_name = resource.resolved_name

        try:
            # Get current state
            existing = self.client.catalogs.get(resource_name)

            # Check if update needed
            changes = self._get_catalog_changes(existing, resource)

            # Check and update workspace bindings for ISOLATED catalogs
            workspace_bindings_updated = False
            if resource.isolation_mode == IsolationMode.ISOLATED and resource.workspace_ids:
                desired_ws_ids = [int(ws_id) for ws_id in resource.workspace_ids]
                current_ws_ids = self.get_current_workspace_bindings(resource_name, "catalog")

                if set(current_ws_ids) != set(desired_ws_ids):
                    logger.info(f"Workspace bindings need update for {resource_name}: {list(current_ws_ids)} -> {desired_ws_ids}")

                    if not self.dry_run:
                        workspace_bindings_updated = self.update_workspace_bindings(
                            resource_name=resource_name,
                            desired_workspace_ids=desired_ws_ids,
                            securable_type="catalog"
                        )
                        if workspace_bindings_updated:
                            changes['workspace_bindings'] = {
                                'from': list(current_ws_ids),
                                'to': desired_ws_ids
                            }

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
                params = resource.to_sdk_update_params()

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
            return self.create(resource)

        except Exception as e:
            return self._handle_error(OperationType.UPDATE, resource_name, e)

    def delete(self, resource: Catalog) -> ExecutionResult:
        """
        Delete a resource.
        
        Args:
            catalog: The catalog to delete
            
        Returns:
            ExecutionResult indicating success or failure
        """
        start_time = time.time()
        resource_name = resource.resolved_name

        try:
            if not self.exists(resource):
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
        """Apply workspace bindings to an existing resource."""
        workspace_ids_as_ints = [int(ws_id) for ws_id in workspace_ids_to_bind]

        if not self.apply_workspace_bindings(resource_name, workspace_ids_as_ints, "catalog"):
            return ExecutionResult(
                success=True,
                operation=OperationType.NO_OP,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message="Exists but could not apply bindings"
            )

        if catalog.isolation_mode:
            from databricks.sdk.service.catalog import CatalogIsolationMode
            self.client.catalogs.update(
                name=resource_name,
                isolation_mode=CatalogIsolationMode(catalog.isolation_mode.value)
            )

        self.verify_workspace_bindings(resource_name, workspace_ids_as_ints, "catalog")

        return ExecutionResult(
            success=True,
            operation=OperationType.UPDATE,
            resource_type=self.get_resource_type(),
            resource_name=resource_name,
            message="Applied workspace bindings"
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

    def _needs_update(self, resource: Catalog) -> bool:
        """
        Check if a catalog needs updating.

        Args:
            resource: The catalog to check

        Returns:
            True if update needed
        """
        try:
            existing = self.client.catalogs.get(resource.resolved_name)
            changes = self._get_catalog_changes(existing, resource)
            return bool(changes)
        except ResourceDoesNotExist:
            return False
        except Exception as e:
            logger.warning(f"Error checking if update needed: {e}")
            return False

    def _get_changes(self, resource: Catalog) -> Dict[str, Any]:
        """
        Get the changes that would be made to a resource.

        Args:
            resource: The catalog

        Returns:
            Dictionary of changes
        """
        try:
            existing = self.client.catalogs.get(resource.resolved_name)
            return self._get_catalog_changes(existing, resource)
        except ResourceDoesNotExist:
            return {'action': 'create'}
        except Exception as e:
            logger.warning(f"Error getting changes: {e}")
            return {}


