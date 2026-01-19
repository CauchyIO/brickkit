"""
External Location executor for Unity Catalog operations.

Handles creation, update, and deletion of external locations (cloud storage paths) via the Databricks SDK.
"""

import logging
import time
from typing import Any, Dict

from databricks.sdk.errors import (
    NotFound,
    PermissionDenied,
    ResourceDoesNotExist,
)
from databricks.sdk.service.catalog import ExternalLocationInfo

from brickkit.models import ExternalLocation

from .base import BaseExecutor, ExecutionResult, OperationType
from .mixins import WorkspaceBindingMixin

logger = logging.getLogger(__name__)


class ExternalLocationExecutor(BaseExecutor[ExternalLocation], WorkspaceBindingMixin):
    """Executor for external location operations."""

    def get_resource_type(self) -> str:
        """Get the resource type."""
        return "EXTERNAL_LOCATION"

    def exists(self, resource: ExternalLocation) -> bool:
        """Check if an external location exists."""
        try:
            self.client.external_locations.get(resource.resolved_name)
            return True
        except (ResourceDoesNotExist, NotFound):
            return False
        except PermissionDenied as e:
            logger.error(f"Permission denied checking external location existence: {e}")
            raise

    def create(self, resource: ExternalLocation) -> ExecutionResult:
        """
        Create a new external resource.

        External locations provide a mapping between a storage credential and
        a specific cloud storage path (S3 bucket, Azure container, GCS bucket).
        """
        start_time = time.time()
        resource_name = resource.resolved_name

        try:
            if self.dry_run:
                logger.info(f"[DRY RUN] Would create external location {resource_name}")
                return ExecutionResult(
                    success=True,
                    operation=OperationType.CREATE,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message="Would be created (dry run)"
                )

            params = resource.to_sdk_create_params()
            url = resource.url
            cloud_provider = "S3" if url.startswith("s3://") else "Azure" if url.startswith(("abfss://", "wasbs://")) else "GCS" if url.startswith("gs://") else "Unknown"
            credential_name = resource.storage_credential.resolved_name

            logger.info(f"Creating external location {resource_name} ({cloud_provider})")

            if credential_name:
                try:
                    self.client.storage_credentials.get(credential_name)
                except ResourceDoesNotExist:
                    raise ValueError(f"Storage credential '{credential_name}' does not exist")

            self.execute_with_retry(self.client.external_locations.create, **params)

            self._rollback_stack.append(
                lambda: self.client.external_locations.delete(resource_name)
            )

            # Apply workspace bindings if specified
            if resource.workspace_ids:
                self.apply_workspace_bindings(
                    resource_name=resource_name,
                    workspace_ids=[int(ws_id) for ws_id in resource.workspace_ids],
                    securable_type="external_location"
                )

            duration = time.time() - start_time
            return ExecutionResult(
                success=True,
                operation=OperationType.CREATE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message=f"Created {cloud_provider} location successfully",
                duration_seconds=duration
            )

        except Exception as e:
            return self._handle_error(OperationType.CREATE, resource_name, e)

    def update(self, resource: ExternalLocation) -> ExecutionResult:
        """
        Update an existing external resource.

        Note: The URL cannot be changed after creation. Only metadata and
        credential can be updated.
        """
        start_time = time.time()
        resource_name = resource.resolved_name

        try:
            existing = self.client.external_locations.get(resource_name)
            changes = self._get_location_changes(existing, resource)

            if not changes:
                return ExecutionResult(
                    success=True,
                    operation=OperationType.NO_OP,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message="No changes needed"
                )

            if self.dry_run:
                logger.info(f"[DRY RUN] Would update external location {resource_name}")
                return ExecutionResult(
                    success=True,
                    operation=OperationType.UPDATE,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message=f"Would update: {changes} (dry run)",
                    changes=changes
                )

            if 'url' in changes:
                logger.warning(f"URL is immutable for {resource_name} - skipping URL change")
                changes.pop('url')

            if changes:
                params = resource.to_sdk_update_params()
                logger.info(f"Updating external location {resource_name}")
                if 'credential_name' in changes:
                    logger.warning(f"Credential change for {resource_name} may affect dependent objects")
                self.execute_with_retry(self.client.external_locations.update, **params)

            duration = time.time() - start_time
            return ExecutionResult(
                success=True,
                operation=OperationType.UPDATE if changes else OperationType.NO_OP,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message=f"Updated: {changes}" if changes else "No updatable changes",
                duration_seconds=duration,
                changes=changes
            )

        except Exception as e:
            return self._handle_error(OperationType.UPDATE, resource_name, e)

    def delete(self, resource: ExternalLocation) -> ExecutionResult:
        """
        Delete an external resource.

        Warning: Cannot delete if external tables or volumes are using this resource.
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
                logger.info(f"[DRY RUN] Would delete external location {resource_name}")
                return ExecutionResult(
                    success=True,
                    operation=OperationType.DELETE,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message="Would be deleted (dry run)"
                )

            logger.info(f"Deleting external location {resource_name}")

            # Note: This will fail if tables/volumes depend on this location
            self.execute_with_retry(
                self.client.external_locations.delete,
                resource_name,
                force=False  # Don't force delete if dependencies exist
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

    def _get_location_changes(self, existing: ExternalLocationInfo, desired: ExternalLocation) -> Dict[str, Any]:
        """Compare existing and desired location to find changes."""
        changes = {}

        # Check comment
        if hasattr(existing, 'comment') and existing.comment != desired.comment:
            changes['comment'] = {'from': existing.comment, 'to': desired.comment}

        # Check owner
        if desired.owner:
            desired_owner = desired.owner.resolved_name
            if hasattr(existing, 'owner') and existing.owner != desired_owner:
                changes['owner'] = {'from': existing.owner, 'to': desired_owner}

        # Check URL (immutable)
        if hasattr(existing, 'url') and existing.url != desired.url:
            changes['url'] = {
                'from': existing.url,
                'to': desired.url,
                'note': 'URL is immutable - requires recreate'
            }

        # Check credential
        if hasattr(existing, 'credential_name'):
            existing_cred = existing.credential_name
            desired_cred = desired.storage_credential.resolved_name if desired.storage_credential else None
            if existing_cred != desired_cred:
                changes['credential_name'] = {
                    'from': existing_cred,
                    'to': desired_cred
                }

        # Check read/write permissions
        if hasattr(existing, 'read_only') and hasattr(desired, 'read_only'):
            if existing.read_only != desired.read_only:
                changes['read_only'] = {
                    'from': existing.read_only,
                    'to': desired.read_only
                }

        return changes

