"""
Schema executor for Unity Catalog operations.

Handles creation, update, and deletion of schemas via the Databricks SDK.
"""

import time
from typing import Dict, Any
import logging
from databricks.sdk.service.catalog import SchemaInfo
from databricks.sdk.errors import ResourceDoesNotExist, NotFound, PermissionDenied
from models import Schema
from .base import BaseExecutor, ExecutionResult, OperationType

logger = logging.getLogger(__name__)


class SchemaExecutor(BaseExecutor[Schema]):
    """Executor for schema operations."""
    
    def get_resource_type(self) -> str:
        """Get the resource type."""
        return "SCHEMA"
    
    def exists(self, resource: Schema) -> bool:
        """Check if a schema exists."""
        try:
            self.client.schemas.get(resource.fqdn)
            return True
        except (ResourceDoesNotExist, NotFound):
            return False
        except PermissionDenied as e:
            logger.error(f"Permission denied checking schema existence: {e}")
            raise
    
    def create(self, resource: Schema) -> ExecutionResult:
        """Create a new resource."""
        start_time = time.time()
        resource_name = resource.fqdn
        
        try:
            if self.dry_run:
                logger.info(f"[DRY RUN] Would create schema {resource_name}")
                return ExecutionResult(
                    success=True,
                    operation=OperationType.CREATE,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message="Would be created (dry run)"
                )
            
            params = resource.to_sdk_create_params()
            logger.info(f"Creating schema {resource_name}")
            self.execute_with_retry(self.client.schemas.create, **params)
            
            self._rollback_stack.append(
                lambda: self.client.schemas.delete(resource_name, force=True)
            )
            
            duration = time.time() - start_time
            return ExecutionResult(
                success=True,
                operation=OperationType.CREATE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message="Created successfully",
                duration_seconds=duration
            )
            
        except Exception as e:
            return self._handle_error(OperationType.CREATE, resource_name, e)
    
    def update(self, resource: Schema) -> ExecutionResult:
        """Update an existing resource."""
        start_time = time.time()
        resource_name = resource.fqdn
        
        try:
            existing = self.client.schemas.get(resource_name)
            changes = self._get_schema_changes(existing, resource)
            
            if not changes:
                return ExecutionResult(
                    success=True,
                    operation=OperationType.NO_OP,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message="No changes needed"
                )
            
            if self.dry_run:
                logger.info(f"[DRY RUN] Would update schema {resource_name}")
                return ExecutionResult(
                    success=True,
                    operation=OperationType.UPDATE,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message=f"Would update: {changes} (dry run)",
                    changes=changes
                )
            
            params = resource.to_sdk_update_params()
            logger.info(f"Updating schema {resource_name}: {changes}")
            self.execute_with_retry(self.client.schemas.update, **params)
            
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
    
    def delete(self, resource: Schema) -> ExecutionResult:
        """Delete a resource."""
        start_time = time.time()
        resource_name = resource.fqdn
        
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
                logger.info(f"[DRY RUN] Would delete schema {resource_name}")
                return ExecutionResult(
                    success=True,
                    operation=OperationType.DELETE,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message="Would be deleted (dry run)"
                )
            
            logger.info(f"Deleting schema {resource_name}")
            self.execute_with_retry(self.client.schemas.delete, resource_name, force=True)
            
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
    
    def _get_schema_changes(self, existing: SchemaInfo, desired: Schema) -> Dict[str, Any]:
        """Compare existing and desired schema to find changes."""
        changes = {}
        
        if hasattr(existing, 'comment') and existing.comment != desired.comment:
            changes['comment'] = {'from': existing.comment, 'to': desired.comment}
        
        if desired.owner:
            desired_owner = desired.owner.resolved_name
            if hasattr(existing, 'owner') and existing.owner != desired_owner:
                changes['owner'] = {'from': existing.owner, 'to': desired_owner}
        
        return changes