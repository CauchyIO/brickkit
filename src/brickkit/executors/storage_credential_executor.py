"""
Storage Credential executor for Unity Catalog operations.

Handles creation, update, and deletion of storage credentials (cloud authentication) via the Databricks SDK.
"""

import logging
import time
from typing import Any, Dict

from databricks.sdk.errors import (
    NotFound,
    PermissionDenied,
    ResourceDoesNotExist,
)
from databricks.sdk.service.catalog import StorageCredentialInfo

from brickkit.models import StorageCredential

from .base import BaseExecutor, ExecutionResult, OperationType
from .mixins import WorkspaceBindingMixin

logger = logging.getLogger(__name__)


class StorageCredentialExecutor(BaseExecutor[StorageCredential], WorkspaceBindingMixin):
    """Executor for storage credential operations."""

    def get_resource_type(self) -> str:
        """Get the resource type."""
        return "STORAGE_CREDENTIAL"

    def exists(self, resource: StorageCredential) -> bool:
        """Check if a storage credential exists."""
        try:
            self.client.storage_credentials.get(resource.resolved_name)
            return True
        except (ResourceDoesNotExist, NotFound):
            return False
        except PermissionDenied as e:
            logger.error(f"Permission denied checking storage credential existence: {e}")
            raise

    def create(self, resource: StorageCredential) -> ExecutionResult:
        """
        Create a new storage resource.
        
        Storage credentials provide authentication to cloud storage services:
        - AWS: IAM roles
        - Azure: Service principals
        - GCP: Service account keys
        """
        start_time = time.time()
        resource_name = resource.resolved_name

        try:
            if self.dry_run:
                logger.info(f"[DRY RUN] Would create storage credential {resource_name}")
                return ExecutionResult(
                    success=True,
                    operation=OperationType.CREATE,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message="Would be created (dry run)"
                )

            params = resource.to_sdk_create_params()
            cloud_provider = "AWS" if resource.aws_iam_role else "Azure" if resource.azure_service_principal else "GCP" if resource.gcp_service_account_key else "Unknown"

            logger.info(f"Creating storage credential {resource_name} ({cloud_provider})")
            self.execute_with_retry(self.client.storage_credentials.create, **params)

            self._rollback_stack.append(
                lambda: self.client.storage_credentials.delete(resource_name)
            )

            # Apply workspace bindings if specified
            if resource.workspace_ids:
                self.apply_workspace_bindings(
                    resource_name=resource_name,
                    workspace_ids=[int(ws_id) for ws_id in resource.workspace_ids],
                    securable_type="storage_credential"
                )

            duration = time.time() - start_time
            return ExecutionResult(
                success=True,
                operation=OperationType.CREATE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message=f"Created {cloud_provider} credential successfully",
                duration_seconds=duration
            )

        except Exception as e:
            return self._handle_error(OperationType.CREATE, resource_name, e)

    def update(self, resource: StorageCredential) -> ExecutionResult:
        """
        Update an existing storage resource.
        
        Note: Credential details (IAM role, service principal) can typically be updated,
        but this may affect all external locations using this resource.
        """
        start_time = time.time()
        resource_name = resource.resolved_name

        try:
            existing = self.client.storage_credentials.get(resource_name)
            changes = self._get_credential_changes(existing, resource)

            if not changes:
                return ExecutionResult(
                    success=True,
                    operation=OperationType.NO_OP,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message="No changes needed"
                )

            if self.dry_run:
                logger.info(f"[DRY RUN] Would update storage credential {resource_name}")
                return ExecutionResult(
                    success=True,
                    operation=OperationType.UPDATE,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message=f"Would update: {changes} (dry run)",
                    changes=changes
                )

            if 'aws_iam_role' in changes or 'azure_service_principal' in changes:
                logger.warning(f"Updating auth for {resource_name} - affects dependent external locations")

            params = resource.to_sdk_update_params()
            logger.info(f"Updating storage credential {resource_name}")
            self.execute_with_retry(self.client.storage_credentials.update, **params)

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

        except Exception as e:
            return self._handle_error(OperationType.UPDATE, resource_name, e)

    def delete(self, resource: StorageCredential) -> ExecutionResult:
        """
        Delete a storage resource.
        
        Warning: Cannot delete if external locations are using this resource.
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
                logger.info(f"[DRY RUN] Would delete storage credential {resource_name}")
                return ExecutionResult(
                    success=True,
                    operation=OperationType.DELETE,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message="Would be deleted (dry run)"
                )

            logger.info(f"Deleting storage credential {resource_name}")

            # Note: This will fail if external locations depend on this credential
            # The error message from the SDK will indicate which locations are affected
            self.execute_with_retry(
                self.client.storage_credentials.delete,
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

    def _get_credential_changes(self, existing: StorageCredentialInfo, desired: StorageCredential) -> Dict[str, Any]:
        """Compare existing and desired credential to find changes."""
        changes = {}

        # Check comment
        if hasattr(existing, 'comment') and existing.comment != desired.comment:
            changes['comment'] = {'from': existing.comment, 'to': desired.comment}

        # Check owner
        if desired.owner:
            desired_owner = desired.owner.resolved_name
            if hasattr(existing, 'owner') and existing.owner != desired_owner:
                changes['owner'] = {'from': existing.owner, 'to': desired_owner}

        # Check cloud-specific credentials (sensitive - log carefully)
        if desired.aws_iam_role:
            if hasattr(existing, 'aws_iam_role'):
                # Compare role ARN
                existing_role = existing.aws_iam_role.role_arn if existing.aws_iam_role else None
                desired_role = desired.aws_iam_role.role_arn
                if existing_role != desired_role:
                    changes['aws_iam_role'] = {
                        'from': 'existing',
                        'to': 'updated'  # Don't log actual ARNs
                    }

        # Similar checks for Azure and GCP...

        return changes

