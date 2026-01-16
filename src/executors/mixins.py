"""
Mixin classes for shared executor functionality.

This module contains reusable mixins that provide common functionality
across multiple executors, reducing code duplication.
"""

import logging
import time
from typing import List, Optional, Set

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import (
    ResourceDoesNotExist,
    NotFound,
    PermissionDenied,
    BadRequest,
    InvalidParameterValue,
)

logger = logging.getLogger(__name__)


class WorkspaceBindingMixin:
    """
    Mixin providing workspace binding functionality for isolated securables.

    This mixin can be used by executors that manage securables which support
    workspace bindings (catalogs, storage credentials, external locations).

    Requires:
        - self.client: WorkspaceClient instance

    Usage:
        class CatalogExecutor(BaseExecutor[Catalog], WorkspaceBindingMixin):
            def create(self, catalog: Catalog) -> ExecutionResult:
                ...
                self.apply_workspace_bindings(
                    resource_name=catalog.resolved_name,
                    workspace_ids=workspace_ids,
                    securable_type="catalog"
                )
    """

    # Subclasses must have a client attribute
    client: WorkspaceClient

    def get_current_workspace_bindings(
        self,
        resource_name: str,
        securable_type: Optional[str] = None
    ) -> Set[int]:
        """
        Get current workspace bindings for a resource.

        Args:
            resource_name: Name of the resource
            securable_type: Type of securable (catalog, storage_credential, external_location)
                           Note: Only 'catalog' is fully supported by the current SDK.

        Returns:
            Set of workspace IDs currently bound

        Raises:
            PermissionDenied: If caller lacks permission to read bindings
        """
        try:
            # The SDK workspace_bindings.get() only supports catalog bindings
            # For other securable types, this may not work correctly
            bindings = self.client.workspace_bindings.get(name=resource_name)

            if bindings and hasattr(bindings, 'workspaces') and bindings.workspaces:
                workspace_ids: Set[int] = set()
                for ws in bindings.workspaces:
                    if hasattr(ws, 'workspace_id'):
                        workspace_ids.add(ws.workspace_id)
                    elif isinstance(ws, int):
                        workspace_ids.add(ws)
                return workspace_ids
            return set()

        except (NotFound, ResourceDoesNotExist):
            logger.debug(f"No existing bindings found for {resource_name}")
            return set()
        except PermissionDenied:
            raise

    def apply_workspace_bindings(
        self,
        resource_name: str,
        workspace_ids: List[int],
        securable_type: Optional[str] = None,
        wait_for_propagation: bool = True
    ) -> bool:
        """
        Apply workspace bindings to a resource.

        Args:
            resource_name: Name of the resource
            workspace_ids: List of workspace IDs to bind
            securable_type: Type of securable (for logging only; SDK only supports catalogs)
            wait_for_propagation: If True, wait briefly for bindings to propagate

        Returns:
            True if bindings were applied successfully

        Raises:
            PermissionDenied: If caller lacks permission to modify bindings
        """
        if not workspace_ids:
            logger.debug(f"No workspace IDs to bind for {resource_name}")
            return True

        resource_type_str = securable_type or "resource"
        logger.info(f"Applying workspace bindings to {resource_type_str} {resource_name}: {workspace_ids}")

        try:
            workspace_ids_as_ints = [int(ws_id) for ws_id in workspace_ids]

            # The SDK workspace_bindings.update() only supports catalog bindings
            # and expects List[int] for assign_workspaces
            self.client.workspace_bindings.update(
                name=resource_name,
                assign_workspaces=workspace_ids_as_ints
            )

            logger.info(f"Successfully applied workspace bindings to {resource_name}")

            if wait_for_propagation:
                time.sleep(2)

            return True

        except PermissionDenied as e:
            logger.error(f"Permission denied applying workspace bindings to {resource_name}: {e}")
            raise
        except (NotFound, ResourceDoesNotExist) as e:
            logger.warning(f"Resource {resource_name} not found for binding (may be propagating): {e}")
            return False
        except (BadRequest, InvalidParameterValue) as e:
            logger.error(f"Invalid workspace binding request for {resource_name}: {e}")
            return False

    def update_workspace_bindings(
        self,
        resource_name: str,
        desired_workspace_ids: List[int],
        securable_type: Optional[str] = None
    ) -> bool:
        """
        Update workspace bindings to match desired state.

        Adds missing bindings and removes unwanted bindings.

        Args:
            resource_name: Name of the resource
            desired_workspace_ids: Desired list of workspace IDs
            securable_type: Type of securable (for logging only; SDK only supports catalogs)

        Returns:
            True if bindings were updated successfully

        Raises:
            PermissionDenied: If caller lacks permission to modify bindings
        """
        current = self.get_current_workspace_bindings(resource_name, securable_type)
        desired = set(int(ws_id) for ws_id in desired_workspace_ids)

        if current == desired:
            logger.debug(f"Workspace bindings already correct for {resource_name}")
            return True

        to_add = list(desired - current)
        to_remove = list(current - desired)

        resource_type_str = securable_type or "resource"
        logger.info(f"Updating workspace bindings for {resource_type_str} {resource_name}")
        logger.info(f"  Adding: {to_add}, Removing: {to_remove}")

        try:
            # The SDK workspace_bindings.update() only supports catalog bindings
            # and expects List[int] for assign/unassign_workspaces
            self.client.workspace_bindings.update(
                name=resource_name,
                assign_workspaces=to_add if to_add else None,
                unassign_workspaces=to_remove if to_remove else None
            )

            logger.info(f"Successfully updated workspace bindings for {resource_name}")
            return True

        except PermissionDenied as e:
            logger.error(f"Permission denied updating workspace bindings: {e}")
            raise
        except (NotFound, ResourceDoesNotExist) as e:
            logger.warning(f"Resource not found for binding update: {e}")
            return False
        except (BadRequest, InvalidParameterValue) as e:
            logger.warning(f"Invalid workspace binding parameters: {e}")
            return False

    def verify_workspace_bindings(
        self,
        resource_name: str,
        expected_workspace_ids: List[int],
        securable_type: Optional[str] = None
    ) -> bool:
        """
        Verify that workspace bindings match expected state.

        Args:
            resource_name: Name of the resource
            expected_workspace_ids: Expected workspace IDs
            securable_type: Type of securable

        Returns:
            True if bindings match expected state
        """
        try:
            current = self.get_current_workspace_bindings(resource_name, securable_type)
            expected = set(int(ws_id) for ws_id in expected_workspace_ids)

            if current == expected:
                logger.debug(f"Workspace bindings verified for {resource_name}: {current}")
                return True
            else:
                logger.warning(
                    f"Workspace binding mismatch for {resource_name}: "
                    f"expected {expected}, got {current}"
                )
                return False

        except PermissionDenied:
            logger.warning("Cannot verify bindings - permission denied")
            return False


__all__ = [
    "WorkspaceBindingMixin",
]
