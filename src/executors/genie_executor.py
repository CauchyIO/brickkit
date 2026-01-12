"""
Genie Space executor for AI/ML governance operations.

Handles creation and update of Genie Spaces via the Databricks SDK.
"""

import time
import logging
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import (
    ResourceDoesNotExist,
    NotFound,
    PermissionDenied,
    BadRequest,
)

from ..genie.models import GenieSpace
from .base import BaseExecutor, ExecutionResult, OperationType

logger = logging.getLogger(__name__)


class GenieSpaceExecutor(BaseExecutor[GenieSpace]):
    """Executor for Genie Space operations."""

    def get_resource_type(self) -> str:
        return "GENIE_SPACE"

    def exists(self, space: GenieSpace) -> bool:
        """Check if a Genie Space exists by title."""
        try:
            spaces = list(self.client.genie.list_spaces())
            return any(s.title == space.title for s in spaces)
        except (ResourceDoesNotExist, NotFound):
            return False
        except PermissionDenied as e:
            logger.error(f"Permission denied checking Genie Space: {e}")
            raise

    def create(self, space: GenieSpace) -> ExecutionResult:
        """Create a new Genie Space."""
        start_time = time.time()
        resource_name = space.title

        if self.dry_run:
            logger.info(f"[DRY RUN] Would create Genie Space {resource_name}")
            return ExecutionResult(
                success=True,
                operation=OperationType.CREATE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message="Would be created (dry run)"
            )

        try:
            logger.info(f"Creating Genie Space: {resource_name}")
            result = space.create(self.client)

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

    def update(self, space: GenieSpace) -> ExecutionResult:
        """Update an existing Genie Space."""
        start_time = time.time()
        resource_name = space.title

        if self.dry_run:
            logger.info(f"[DRY RUN] Would update Genie Space {resource_name}")
            return ExecutionResult(
                success=True,
                operation=OperationType.UPDATE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message="Would be updated (dry run)"
            )

        try:
            logger.info(f"Updating Genie Space: {resource_name}")
            result = space.update(self.client)

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
            return self.create(space)
        except PermissionDenied as e:
            logger.error(f"Permission denied updating Genie Space: {e}")
            raise
        except BadRequest as e:
            return self._handle_error(OperationType.UPDATE, resource_name, e)

    def delete(self, space: GenieSpace) -> ExecutionResult:
        """Delete a Genie Space (not supported via SDK)."""
        return ExecutionResult(
            success=False,
            operation=OperationType.DELETE,
            resource_type=self.get_resource_type(),
            resource_name=space.title,
            message="Genie Space deletion not supported via SDK"
        )

    def create_or_update(self, space: GenieSpace) -> ExecutionResult:
        """Create or update a Genie Space (idempotent)."""
        start_time = time.time()
        resource_name = space.title

        if self.dry_run:
            logger.info(f"[DRY RUN] Would create/update Genie Space {resource_name}")
            return ExecutionResult(
                success=True,
                operation=OperationType.CREATE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message="Would be created/updated (dry run)"
            )

        try:
            logger.info(f"Creating or updating Genie Space: {resource_name}")
            result = space.create_or_update(self.client)

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
