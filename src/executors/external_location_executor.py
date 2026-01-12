"""
External Location executor for Unity Catalog operations.

Handles creation, update, and deletion of external locations (cloud storage paths) via the Databricks SDK.
"""

import time
from typing import Dict, Any, List
import logging
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import ExternalLocationInfo
from databricks.sdk.errors import ResourceDoesNotExist, ResourceAlreadyExists
from ..models import ExternalLocation
from .base import BaseExecutor, ExecutionResult, OperationType

logger = logging.getLogger(__name__)


class ExternalLocationExecutor(BaseExecutor[ExternalLocation]):
    """Executor for external location operations."""
    
    def get_resource_type(self) -> str:
        """Get the resource type."""
        return "EXTERNAL_LOCATION"
    
    def exists(self, location: ExternalLocation) -> bool:
        """Check if an external location exists."""
        try:
            self.client.external_locations.get(location.resolved_name)
            return True
        except ResourceDoesNotExist:
            return False
        except Exception as e:
            logger.warning(f"Error checking external location existence: {e}")
            return False
    
    def create(self, location: ExternalLocation) -> ExecutionResult:
        """
        Create a new external location.
        
        External locations provide a mapping between a storage credential and
        a specific cloud storage path (S3 bucket, Azure container, GCS bucket).
        """
        start_time = time.time()
        resource_name = location.resolved_name
        
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
            
            params = location.to_sdk_create_params()
            
            # Validate URL format
            url = location.url
            cloud_provider = "Unknown"
            if url.startswith("s3://"):
                cloud_provider = "AWS S3"
            elif url.startswith("abfss://") or url.startswith("wasbs://"):
                cloud_provider = "Azure Storage"
            elif url.startswith("gs://"):
                cloud_provider = "Google Cloud Storage"
            
            # Get the credential name from the storage_credential object
            credential_name = location.storage_credential.resolved_name
            
            logger.info(
                f"Creating external location {resource_name} "
                f"({cloud_provider}: {url}) "
                f"using credential '{credential_name}'"
            )
            
            # Validate storage credential exists
            if credential_name:
                try:
                    self.client.storage_credentials.get(credential_name)
                except ResourceDoesNotExist:
                    raise ValueError(
                        f"Storage credential '{credential_name}' does not exist. "
                        f"Create the credential before creating this external location."
                    )
            
            self.execute_with_retry(self.client.external_locations.create, **params)
            
            logger.info(
                f"External location {resource_name} created. "
                f"This location can now be used for external tables and volumes."
            )
            
            self._rollback_stack.append(
                lambda: self.client.external_locations.delete(resource_name)
            )

            # Apply workspace bindings if specified
            if location.workspace_ids:
                self._apply_workspace_bindings(resource_name, location.workspace_ids)

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
    
    def update(self, location: ExternalLocation) -> ExecutionResult:
        """
        Update an existing external location.
        
        Note: The URL cannot be changed after creation. Only metadata and
        credential can be updated.
        """
        start_time = time.time()
        resource_name = location.resolved_name
        
        try:
            existing = self.client.external_locations.get(resource_name)
            changes = self._get_location_changes(existing, location)
            
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
            
            # Check for immutable changes
            if 'url' in changes:
                logger.warning(
                    f"URL cannot be changed for external location {resource_name}. "
                    f"Current: {existing.url}, Desired: {location.url}. "
                    f"Create a new external location for the new URL."
                )
                changes.pop('url')
            
            if changes:
                params = location.to_sdk_update_params()
                logger.info(f"Updating external location {resource_name}: {changes}")
                
                # Warn about credential changes
                if 'credential_name' in changes:
                    logger.warning(
                        f"Changing storage credential for {resource_name}. "
                        f"This may affect access to existing external tables and volumes."
                    )
                
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
    
    def delete(self, location: ExternalLocation) -> ExecutionResult:
        """
        Delete an external location.
        
        Warning: Cannot delete if external tables or volumes are using this location.
        """
        start_time = time.time()
        resource_name = location.resolved_name
        
        try:
            if not self.exists(location):
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
            if "is still referenced" in str(e) or "dependencies" in str(e).lower():
                logger.error(
                    f"Cannot delete {resource_name}: Tables or volumes still depend on it. "
                    f"Delete the dependent objects first."
                )
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

    def _apply_workspace_bindings(self, resource_name: str, workspace_ids: List[int]) -> None:
        """
        Apply workspace bindings to the external location.

        Args:
            resource_name: Name of the external location
            workspace_ids: List of workspace IDs to bind
        """
        if not workspace_ids:
            return

        logger.info(f"Applying workspace bindings for external location {resource_name}: {workspace_ids}")

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
                    securable_type="external_location",
                    securable_name=resource_name,
                    assign_workspaces=workspace_bindings
                )
            except (TypeError, AttributeError):
                # Fall back to name-only parameter (older SDK versions)
                self.client.workspace_bindings.update(
                    name=resource_name,
                    assign_workspaces=workspace_bindings
                )

            logger.info(f"Successfully bound external location {resource_name} to workspaces: {[b.workspace_id for b in workspace_bindings]}")

        except Exception as e:
            logger.error(f"Failed to apply workspace bindings for external location {resource_name}: {e}")
            logger.error(f"  External location: {resource_name}")
            logger.error(f"  Workspace IDs attempted: {workspace_ids}")
            logger.error(f"  Error type: {type(e).__name__}")
            logger.error(f"  Error details: {str(e)}")
            # Not fatal - external location exists but may not be isolated