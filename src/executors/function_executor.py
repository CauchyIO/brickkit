"""
Function executor for Unity Catalog operations.

Handles creation, update, and deletion of functions (UDFs, row filters, column masks) via the Databricks SDK.
"""

import time
from typing import Dict, Any
import logging
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import FunctionInfo
from databricks.sdk.errors import ResourceDoesNotExist, ResourceAlreadyExists
from ..models import Function, FunctionType
from .base import BaseExecutor, ExecutionResult, OperationType

logger = logging.getLogger(__name__)


class FunctionExecutor(BaseExecutor[Function]):
    """Executor for function operations including row filters and column masks."""
    
    def get_resource_type(self) -> str:
        """Get the resource type."""
        return "FUNCTION"
    
    def exists(self, function: Function) -> bool:
        """Check if a function exists."""
        try:
            self.client.functions.get(function.fqdn)
            return True
        except ResourceDoesNotExist:
            return False
        except Exception as e:
            logger.warning(f"Error checking function existence: {e}")
            return False
    
    def create(self, function: Function) -> ExecutionResult:
        """
        Create a new function.
        
        Note: In Unity Catalog, row filters and column masks are special functions
        that execute with definer's rights (transparent to users).
        """
        start_time = time.time()
        resource_name = function.fqdn
        
        try:
            if self.dry_run:
                logger.info(f"[DRY RUN] Would create function {resource_name}")
                return ExecutionResult(
                    success=True,
                    operation=OperationType.CREATE,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message="Would be created (dry run)"
                )
            
            params = function.to_sdk_create_params()
            
            # Determine function purpose for logging
            function_purpose = "UDF"
            if function.is_row_filter:
                function_purpose = "row filter"
            elif function.is_column_mask:
                function_purpose = "column mask"
            
            logger.info(f"Creating {function_purpose} function {resource_name}")
            
            # Note: The actual function creation would typically be done via SQL
            # The SDK primarily manages metadata. For full implementation, we'd need:
            # 1. Create the function via SQL execution
            # 2. Register it in Unity Catalog
            # 3. Set up proper permissions (definer's rights for filters/masks)
            
            self.execute_with_retry(self.client.functions.create, **params)
            
            # Log if this is a security function
            if function.is_row_filter or function.is_column_mask:
                logger.info(
                    f"Security function {resource_name} created with definer's rights. "
                    f"Users will not need EXECUTE permission to use this {function_purpose}."
                )
            
            self._rollback_stack.append(
                lambda: self.client.functions.delete(resource_name)
            )
            
            duration = time.time() - start_time
            return ExecutionResult(
                success=True,
                operation=OperationType.CREATE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message=f"Created {function_purpose} successfully",
                duration_seconds=duration
            )
            
        except Exception as e:
            return self._handle_error(OperationType.CREATE, resource_name, e)
    
    def update(self, function: Function) -> ExecutionResult:
        """
        Update an existing function.
        
        Note: Functions typically cannot be updated directly - they must be
        dropped and recreated. This method updates metadata only.
        """
        start_time = time.time()
        resource_name = function.fqdn
        
        try:
            existing = self.client.functions.get(resource_name)
            changes = self._get_function_changes(existing, function)
            
            if not changes:
                return ExecutionResult(
                    success=True,
                    operation=OperationType.NO_OP,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message="No changes needed"
                )
            
            if self.dry_run:
                logger.info(f"[DRY RUN] Would update function {resource_name}")
                return ExecutionResult(
                    success=True,
                    operation=OperationType.UPDATE,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message=f"Would update: {changes} (dry run)",
                    changes=changes
                )
            
            # Note: Most function properties are immutable
            # Only metadata like owner and comment can typically be updated
            if 'definition' in changes:
                logger.warning(
                    f"Function definition cannot be updated. "
                    f"Drop and recreate {resource_name} to change the definition."
                )
                changes.pop('definition')
            
            if changes:
                params = function.to_sdk_update_params()
                logger.info(f"Updating function metadata {resource_name}: {changes}")
                self.execute_with_retry(self.client.functions.update, **params)
            
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
    
    def delete(self, function: Function) -> ExecutionResult:
        """Delete a function."""
        start_time = time.time()
        resource_name = function.fqdn
        
        try:
            if not self.exists(function):
                return ExecutionResult(
                    success=True,
                    operation=OperationType.NO_OP,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message="Does not exist"
                )
            
            if self.dry_run:
                logger.info(f"[DRY RUN] Would delete function {resource_name}")
                return ExecutionResult(
                    success=True,
                    operation=OperationType.DELETE,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message="Would be deleted (dry run)"
                )
            
            # Check if function is used as row filter or column mask
            if function.referencing_tables:
                logger.warning(
                    f"Function {resource_name} is referenced by {len(function.referencing_tables)} tables. "
                    f"Removing it may affect row-level security or column masking."
                )
            
            logger.info(f"Deleting function {resource_name}")
            self.execute_with_retry(self.client.functions.delete, resource_name)
            
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
    
    def _get_function_changes(self, existing: FunctionInfo, desired: Function) -> Dict[str, Any]:
        """Compare existing and desired function to find changes."""
        changes = {}
        
        # Only metadata can typically be updated
        if hasattr(existing, 'comment') and existing.comment != desired.comment:
            changes['comment'] = {'from': existing.comment, 'to': desired.comment}
        
        if desired.owner:
            desired_owner = desired.owner.resolved_name
            if hasattr(existing, 'owner') and existing.owner != desired_owner:
                changes['owner'] = {'from': existing.owner, 'to': desired_owner}
        
        # Function definition changes require drop/recreate
        if hasattr(existing, 'routine_definition') and hasattr(desired, 'definition'):
            if existing.routine_definition != desired.definition:
                changes['definition'] = {
                    'from': 'existing',
                    'to': 'new',
                    'note': 'Requires drop and recreate'
                }
        
        return changes