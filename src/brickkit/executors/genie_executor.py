"""
Genie Space executor for AI/ML governance operations.

Handles creation, update, and management of Genie Spaces via the Databricks SDK.
Includes permission management, batch deployment, and export functionality.

Usage:
    from databricks.sdk import WorkspaceClient
    from brickkit.executors import GenieSpaceExecutor
    from brickkit import GenieSpace

    client = WorkspaceClient()
    executor = GenieSpaceExecutor(client)

    # Deploy a single space
    space = GenieSpace(name="analytics", title="Analytics Space", warehouse_id="abc123")
    result = executor.create_or_update(space)

    # Deploy multiple spaces
    results = executor.deploy_all([space1, space2])

    # Export spaces to JSON
    executor.export_to_json([space1, space2], Path("./exports"))
"""

import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import (
    BadRequest,
    NotFound,
    PermissionDenied,
    ResourceDoesNotExist,
)
from databricks.sdk.service.iam import AccessControlRequest, PermissionLevel

from brickkit.models.genie import GenieSpace

from .base import BaseExecutor, ExecutionResult, OperationType

logger = logging.getLogger(__name__)


# =============================================================================
# SERVICE PRINCIPAL FOR GENIE SPACE MANAGEMENT
# =============================================================================

@dataclass
class ServicePrincipal:
    """Service Principal that needs access to deployed Genie Spaces."""
    application_id: str
    name: str


class GenieSpacePermission:
    """Permission levels for Genie Spaces."""
    CAN_VIEW = "CAN_VIEW"
    CAN_EDIT = "CAN_EDIT"
    CAN_MANAGE = "CAN_MANAGE"


# =============================================================================
# GENIE SPACE EXECUTOR
# =============================================================================

class GenieSpaceExecutor(BaseExecutor[GenieSpace]):
    """
    Executor for Genie Space operations.

    Features:
    - Create, update, delete Genie Spaces
    - Batch deployment with progress tracking
    - Permission management for service principals
    - Export to JSON for version control
    - Dry-run mode for testing
    """

    def __init__(
        self,
        client: WorkspaceClient,
        dry_run: bool = False,
        max_retries: int = 3,
        continue_on_error: bool = False,
        governance_defaults: Optional[Any] = None,
        default_warehouse_id: Optional[str] = None,
    ):
        """
        Initialize the Genie Space executor.

        Args:
            client: Databricks SDK client
            dry_run: If True, only show what would be done
            max_retries: Maximum retry attempts for transient failures
            continue_on_error: Continue execution despite errors
            governance_defaults: Optional governance defaults for validation
            default_warehouse_id: Default warehouse to use if not set on space
        """
        super().__init__(client, dry_run, max_retries, continue_on_error, governance_defaults)
        self._default_warehouse_id = default_warehouse_id

    def get_resource_type(self) -> str:
        return "GENIE_SPACE"

    @property
    def default_warehouse_id(self) -> str:
        """Get or lazily resolve the default warehouse ID."""
        if self._default_warehouse_id is None:
            self._default_warehouse_id = self._get_first_warehouse_id()
        return self._default_warehouse_id

    def _get_first_warehouse_id(self) -> str:
        """Get the first available SQL warehouse ID."""
        warehouses = list(self.client.warehouses.list())
        if not warehouses:
            raise RuntimeError("No SQL warehouses found in workspace")
        return warehouses[0].id

    def exists(self, resource: GenieSpace) -> bool:
        """Check if a Genie Space exists by title."""
        try:
            response = self.client.genie.list_spaces()
            spaces = response.spaces if response and response.spaces else []
            return any(s.title == resource.title for s in spaces)
        except (ResourceDoesNotExist, NotFound):
            return False
        except PermissionDenied as e:
            logger.error(f"Permission denied checking Genie Space: {e}")
            raise

    def get_space_id_by_title(self, title: str) -> Optional[str]:
        """Get space ID by title, or None if not found."""
        try:
            response = self.client.genie.list_spaces()
            spaces = response.spaces if response and response.spaces else []
            for space in spaces:
                if space.title == title:
                    return space.space_id
            return None
        except (ResourceDoesNotExist, NotFound):
            return None

    def create(self, resource: GenieSpace) -> ExecutionResult:
        """Create a new Genie Space."""
        start_time = time.time()
        resource_name = resource.title

        # Ensure warehouse_id is set
        if not resource.warehouse_id:
            resource.warehouse_id = self.default_warehouse_id

        if self.dry_run:
            logger.info(f"[DRY RUN] Would create Genie Space {resource_name}")
            return ExecutionResult(
                success=True,
                operation=OperationType.CREATE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message="Would be created (dry run)",
                changes=self._get_space_summary(resource)
            )

        try:
            logger.info(f"Creating Genie Space: {resource_name}")
            result = resource.create(self.client)

            duration = time.time() - start_time
            return ExecutionResult(
                success=True,
                operation=OperationType.CREATE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message=f"Created with ID: {result.space_id}",
                duration_seconds=duration
            )

        except PermissionDenied as e:
            logger.error(f"Permission denied creating Genie Space: {e}")
            raise
        except BadRequest as e:
            return self._handle_error(OperationType.CREATE, resource_name, e)

    def update(self, resource: GenieSpace) -> ExecutionResult:
        """Update an existing Genie Space."""
        start_time = time.time()
        resource_name = resource.title

        # Ensure warehouse_id is set
        if not resource.warehouse_id:
            resource.warehouse_id = self.default_warehouse_id

        if self.dry_run:
            logger.info(f"[DRY RUN] Would update Genie Space {resource_name}")
            return ExecutionResult(
                success=True,
                operation=OperationType.UPDATE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message="Would be updated (dry run)",
                changes=self._get_space_summary(resource)
            )

        try:
            # Find existing space ID if not set
            if not resource.space_id:
                resource.space_id = self.get_space_id_by_title(resource.title)

            if not resource.space_id:
                logger.info(f"Genie Space {resource_name} not found, creating")
                return self.create(resource)

            logger.info(f"Updating Genie Space: {resource_name}")
            result = resource.update(self.client)

            duration = time.time() - start_time
            return ExecutionResult(
                success=True,
                operation=OperationType.UPDATE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message=f"Updated space ID: {result.space_id}",
                duration_seconds=duration
            )

        except (ResourceDoesNotExist, NotFound):
            logger.info(f"Genie Space {resource_name} not found, creating")
            return self.create(resource)
        except PermissionDenied as e:
            logger.error(f"Permission denied updating Genie Space: {e}")
            raise
        except BadRequest as e:
            return self._handle_error(OperationType.UPDATE, resource_name, e)

    def delete(self, resource: GenieSpace) -> ExecutionResult:
        """Delete a Genie Space (not supported via SDK)."""
        return ExecutionResult(
            success=False,
            operation=OperationType.DELETE,
            resource_type=self.get_resource_type(),
            resource_name=resource.title,
            message="Genie Space deletion not supported via SDK"
        )

    def create_or_update(self, resource: GenieSpace) -> ExecutionResult:
        """Create or update a Genie Space (idempotent)."""
        start_time = time.time()
        resource_name = resource.title

        # Ensure warehouse_id is set
        if not resource.warehouse_id:
            resource.warehouse_id = self.default_warehouse_id

        if self.dry_run:
            logger.info(f"[DRY RUN] Would create/update Genie Space {resource_name}")
            return ExecutionResult(
                success=True,
                operation=OperationType.CREATE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message="Would be created/updated (dry run)",
                changes=self._get_space_summary(resource)
            )

        try:
            logger.info(f"Creating or updating Genie Space: {resource_name}")
            result = resource.create_or_update(self.client)

            duration = time.time() - start_time
            return ExecutionResult(
                success=True,
                operation=OperationType.CREATE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message=f"Created/updated with ID: {result.space_id}",
                duration_seconds=duration
            )

        except PermissionDenied as e:
            logger.error(f"Permission denied: {e}")
            raise
        except BadRequest as e:
            return self._handle_error(OperationType.CREATE, resource_name, e)

    # =========================================================================
    # BATCH OPERATIONS
    # =========================================================================

    def deploy_all(
        self,
        spaces: List[GenieSpace],
        warehouse_id: Optional[str] = None,
    ) -> Dict[str, ExecutionResult]:
        """
        Deploy multiple Genie Spaces.

        Args:
            spaces: List of GenieSpace objects to deploy
            warehouse_id: Override warehouse ID for all spaces

        Returns:
            Dict mapping space titles to ExecutionResults
        """
        results = {}
        wh_id = warehouse_id or self.default_warehouse_id

        for space in spaces:
            if not space.warehouse_id:
                space.warehouse_id = wh_id

            logger.info(f"Deploying: {space.title}")
            try:
                result = self.create_or_update(space)
                results[space.title] = result
                self.results.append(result)

                if result.success:
                    logger.info(f"  Success: {result.message}")
                else:
                    logger.error(f"  Failed: {result.message}")

            except Exception as e:
                error_result = ExecutionResult(
                    success=False,
                    operation=OperationType.CREATE,
                    resource_type=self.get_resource_type(),
                    resource_name=space.title,
                    message=str(e),
                    error=e
                )
                results[space.title] = error_result
                self.results.append(error_result)

                if not self.continue_on_error:
                    raise

        return results

    # =========================================================================
    # PERMISSION MANAGEMENT
    # =========================================================================

    def grant_access(
        self,
        space_id: str,
        spn: ServicePrincipal,
        permission: str = GenieSpacePermission.CAN_EDIT,
    ) -> bool:
        """
        Grant a Service Principal access to a Genie Space.

        Tries multiple permission APIs since Genie spaces may use different
        permission systems depending on the Databricks version.

        Args:
            space_id: The Genie Space ID
            spn: ServicePrincipal to grant access to
            permission: Permission level (CAN_VIEW, CAN_EDIT, CAN_MANAGE)

        Returns:
            True if successful, False otherwise

        Raises:
            RuntimeError: If unable to grant permissions via any known method
        """
        if self.dry_run:
            logger.info(f"[DRY RUN] Would grant {permission} to {spn.name} on space {space_id}")
            return True

        acl = [
            AccessControlRequest(
                service_principal_name=spn.application_id,
                permission_level=PermissionLevel(permission),
            )
        ]

        # Try different object types - Genie spaces may be registered differently
        object_types_to_try = [
            "genie-spaces",
            "dashboards",
            "dbsql-dashboards",
        ]

        errors = []
        for obj_type in object_types_to_try:
            try:
                self.client.permissions.update(
                    request_object_type=obj_type,
                    request_object_id=space_id,
                    access_control_list=acl,
                )
                logger.info(f"Granted {permission} to {spn.name} via {obj_type}")
                return True
            except Exception as e:
                errors.append(f"{obj_type}: {e}")

        # All attempts failed
        error_details = "\n  ".join(errors)
        logger.warning(
            f"Could not grant {spn.name} access to space {space_id}. "
            f"Tried:\n  {error_details}\n"
            f"Please grant access manually via the Genie Space UI."
        )
        return False

    # =========================================================================
    # EXPORT FUNCTIONALITY
    # =========================================================================

    def export_to_json(
        self,
        spaces: List[GenieSpace],
        output_dir: Path,
    ) -> List[Path]:
        """
        Export Genie Spaces to JSON files.

        Args:
            spaces: List of GenieSpace objects to export
            output_dir: Directory to write JSON files

        Returns:
            List of created file paths
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        created_files = []

        for space in spaces:
            filename = space.title.lower().replace(" ", "_") + ".json"
            filepath = output_dir / filename
            space.to_json_file(str(filepath))
            logger.info(f"Exported: {filepath}")
            created_files.append(filepath)

        return created_files

    # =========================================================================
    # LISTING AND DISCOVERY
    # =========================================================================

    def list_spaces(self) -> List[Dict[str, Any]]:
        """
        List all Genie Spaces in the workspace.

        Returns:
            List of space info dictionaries
        """
        try:
            response = self.client.genie.list_spaces()
            spaces = response.spaces if response and response.spaces else []
            return [
                {
                    "space_id": s.space_id,
                    "title": s.title,
                    "description": s.description,
                }
                for s in spaces
            ]
        except Exception as e:
            logger.error(f"Failed to list Genie Spaces: {e}")
            raise

    # =========================================================================
    # HELPERS
    # =========================================================================

    def _get_space_summary(self, space: GenieSpace) -> Dict[str, Any]:
        """Get a summary of space configuration for logging."""
        return {
            "title": space.title,
            "tables": len(space.serialized_space.data_sources.tables),
            "functions": len(space.serialized_space.instructions.sql_functions),
            "instructions": len(space.serialized_space.instructions.text_instructions),
        }

    def _get_resource_name(self, resource: GenieSpace) -> str:
        """Get the name of a Genie Space for logging."""
        return resource.title
