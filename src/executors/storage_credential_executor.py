"""
Storage Credential executor for Unity Catalog operations.

Handles creation, update, and deletion of storage credentials (cloud authentication) via the Databricks SDK.
"""

import time
from typing import Dict, Any, List
import logging
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import StorageCredentialInfo
from databricks.sdk.errors import ResourceDoesNotExist, ResourceAlreadyExists
from ..models import StorageCredential
from .base import BaseExecutor, ExecutionResult, OperationType

logger = logging.getLogger(__name__)


class StorageCredentialExecutor(BaseExecutor[StorageCredential]):
    """Executor for storage credential operations."""
    
    def get_resource_type(self) -> str:
        """Get the resource type."""
        return "STORAGE_CREDENTIAL"
    
    def exists(self, credential: StorageCredential) -> bool:
        """Check if a storage credential exists."""
        try:
            self.client.storage_credentials.get(credential.resolved_name)
            return True
        except ResourceDoesNotExist:
            return False
        except Exception as e:
            logger.warning(f"Error checking storage credential existence: {e}")
            return False
    
    def create(self, credential: StorageCredential) -> ExecutionResult:
        """
        Create a new storage credential.
        
        Storage credentials provide authentication to cloud storage services:
        - AWS: IAM roles
        - Azure: Service principals
        - GCP: Service account keys
        """
        start_time = time.time()
        resource_name = credential.resolved_name
        
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
            
            params = credential.to_sdk_create_params()
            
            # Determine cloud provider for logging
            cloud_provider = "Unknown"
            if credential.aws_iam_role:
                cloud_provider = "AWS IAM Role"
            elif credential.azure_service_principal:
                cloud_provider = "Azure Service Principal"
            elif credential.gcp_service_account_key:
                cloud_provider = "GCP Service Account"
            
            logger.info(f"Creating storage credential {resource_name} ({cloud_provider})")
            
            # Security note: The credential details are sensitive
            # The SDK handles secure transmission to Databricks
            self.execute_with_retry(self.client.storage_credentials.create, **params)
            
            logger.info(
                f"Storage credential {resource_name} created. "
                f"This credential can now be used in external locations."
            )
            
            self._rollback_stack.append(
                lambda: self.client.storage_credentials.delete(resource_name)
            )

            # Apply workspace bindings if specified
            if credential.workspace_ids:
                self._apply_workspace_bindings(resource_name, credential.workspace_ids)

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
    
    def update(self, credential: StorageCredential) -> ExecutionResult:
        """
        Update an existing storage credential.
        
        Note: Credential details (IAM role, service principal) can typically be updated,
        but this may affect all external locations using this credential.
        """
        start_time = time.time()
        resource_name = credential.resolved_name
        
        try:
            existing = self.client.storage_credentials.get(resource_name)
            changes = self._get_credential_changes(existing, credential)
            
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
            
            # Warn about impact
            if 'aws_iam_role' in changes or 'azure_service_principal' in changes:
                logger.warning(
                    f"Updating authentication details for {resource_name}. "
                    f"This will affect all external locations using this credential."
                )
            
            params = credential.to_sdk_update_params()
            logger.info(f"Updating storage credential {resource_name}: {changes}")
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
    
    def delete(self, credential: StorageCredential) -> ExecutionResult:
        """
        Delete a storage credential.
        
        Warning: Cannot delete if external locations are using this credential.
        """
        start_time = time.time()
        resource_name = credential.resolved_name
        
        try:
            if not self.exists(credential):
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
            if "is still referenced" in str(e) or "dependencies" in str(e).lower():
                logger.error(
                    f"Cannot delete {resource_name}: External locations still depend on it. "
                    f"Delete the external locations first."
                )
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
                desired_role = desired.aws_iam_role.get('role_arn')
                if existing_role != desired_role:
                    changes['aws_iam_role'] = {
                        'from': 'existing',
                        'to': 'updated'  # Don't log actual ARNs
                    }
        
        # Similar checks for Azure and GCP...

        return changes

    def _apply_workspace_bindings(self, resource_name: str, workspace_ids: List[int]) -> None:
        """
        Apply workspace bindings to the storage credential.

        Args:
            resource_name: Name of the storage credential
            workspace_ids: List of workspace IDs to bind
        """
        if not workspace_ids:
            return

        logger.info(f"Applying workspace bindings for storage credential {resource_name}: {workspace_ids}")

        try:
            from databricks.sdk.service.catalog import WorkspaceBinding, WorkspaceBindingBindingType

            # Create WorkspaceBinding objects
            workspace_bindings = []
            for ws_id in workspace_ids:
                binding = WorkspaceBinding(
                    workspace_id=int(ws_id),
                    binding_type=WorkspaceBindingBindingType.BINDING_TYPE_READ_WRITE
                )
                workspace_bindings.append(binding)

            # Apply bindings via workspace_bindings API
            try:
                # Try with securable_type parameter first (newer SDK versions)
                self.client.workspace_bindings.update(
                    securable_type="storage_credential",
                    securable_name=resource_name,
                    assign_workspaces=workspace_bindings
                )
            except (TypeError, AttributeError):
                # Fall back to name-only parameter (older SDK versions)
                self.client.workspace_bindings.update(
                    name=resource_name,
                    assign_workspaces=workspace_bindings
                )

            logger.info(f"Successfully bound storage credential {resource_name} to workspaces: {[b.workspace_id for b in workspace_bindings]}")

        except Exception as e:
            logger.error(f"Failed to apply workspace bindings for storage credential {resource_name}: {e}")
            logger.error(f"  Storage credential: {resource_name}")
            logger.error(f"  Workspace IDs attempted: {workspace_ids}")
            logger.error(f"  Error type: {type(e).__name__}")
            logger.error(f"  Error details: {str(e)}")
            # Not fatal - storage credential exists but may not be isolated