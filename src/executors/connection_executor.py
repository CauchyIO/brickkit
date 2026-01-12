"""
Connection executor for Unity Catalog operations.

Handles creation, update, and deletion of connections (external database connections) via the Databricks SDK.
"""

import time
from typing import Dict, Any
import logging
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import ConnectionInfo
from databricks.sdk.errors import ResourceDoesNotExist, ResourceAlreadyExists
from ..models import Connection, ConnectionType
from .base import BaseExecutor, ExecutionResult, OperationType

logger = logging.getLogger(__name__)


class ConnectionExecutor(BaseExecutor[Connection]):
    """Executor for external database connection operations."""
    
    def get_resource_type(self) -> str:
        """Get the resource type."""
        return "CONNECTION"
    
    def exists(self, connection: Connection) -> bool:
        """Check if a connection exists."""
        try:
            self.client.connections.get(connection.resolved_name)
            return True
        except ResourceDoesNotExist:
            return False
        except Exception as e:
            logger.warning(f"Error checking connection existence: {e}")
            return False
    
    def create(self, connection: Connection) -> ExecutionResult:
        """
        Create a new external database connection.
        
        Connections allow Unity Catalog to access external databases like:
        - MySQL, PostgreSQL
        - Snowflake, Redshift
        - SQL Server, Azure Synapse
        - Other Databricks workspaces
        """
        start_time = time.time()
        resource_name = connection.resolved_name
        
        try:
            if self.dry_run:
                logger.info(f"[DRY RUN] Would create connection {resource_name}")
                return ExecutionResult(
                    success=True,
                    operation=OperationType.CREATE,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message="Would be created (dry run)"
                )
            
            params = connection.to_sdk_create_params()
            
            # Log connection type and host (but not credentials)
            connection_desc = f"{connection.connection_type.value}"
            if connection.host:
                connection_desc += f" at {connection.host}"
                if connection.port:
                    connection_desc += f":{connection.port}"
            
            logger.info(f"Creating connection {resource_name} ({connection_desc})")
            
            # Security note: Connection credentials are sensitive
            # The SDK handles secure transmission
            if connection.user and connection.password:
                logger.info(f"Using username/password authentication for {resource_name}")
            elif connection.options and 'token' in connection.options:
                logger.info(f"Using token authentication for {resource_name}")
            
            self.execute_with_retry(self.client.connections.create, **params)
            
            logger.info(
                f"Connection {resource_name} created. "
                f"You can now create foreign catalogs using this connection."
            )
            
            self._rollback_stack.append(
                lambda: self.client.connections.delete(resource_name)
            )
            
            duration = time.time() - start_time
            return ExecutionResult(
                success=True,
                operation=OperationType.CREATE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message=f"Created {connection.connection_type.value} connection successfully",
                duration_seconds=duration
            )
            
        except Exception as e:
            return self._handle_error(OperationType.CREATE, resource_name, e)
    
    def update(self, connection: Connection) -> ExecutionResult:
        """
        Update an existing connection.
        
        Note: Connection URL and type cannot be changed. Credentials and
        connection options can be updated.
        """
        start_time = time.time()
        resource_name = connection.resolved_name
        
        try:
            existing = self.client.connections.get(resource_name)
            changes = self._get_connection_changes(existing, connection)
            
            if not changes:
                return ExecutionResult(
                    success=True,
                    operation=OperationType.NO_OP,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message="No changes needed"
                )
            
            if self.dry_run:
                logger.info(f"[DRY RUN] Would update connection {resource_name}")
                return ExecutionResult(
                    success=True,
                    operation=OperationType.UPDATE,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message=f"Would update: {changes} (dry run)",
                    changes=changes
                )
            
            # Check for immutable changes
            if 'connection_type' in changes:
                logger.warning(
                    f"Connection type cannot be changed for {resource_name}. "
                    f"Create a new connection for a different database type."
                )
                changes.pop('connection_type')
            
            if 'host' in changes or 'port' in changes:
                logger.warning(
                    f"Connection endpoint (host/port) changes may affect existing queries. "
                    f"Ensure the new endpoint is compatible."
                )
            
            if changes:
                params = connection.to_sdk_update_params()
                
                # Log credential updates without exposing them
                if 'password' in changes or 'options' in changes:
                    logger.info(f"Updating credentials for connection {resource_name}")
                
                logger.info(f"Updating connection {resource_name}: {list(changes.keys())}")
                self.execute_with_retry(self.client.connections.update, **params)
            
            duration = time.time() - start_time
            return ExecutionResult(
                success=True,
                operation=OperationType.UPDATE if changes else OperationType.NO_OP,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message=f"Updated: {list(changes.keys())}" if changes else "No updatable changes",
                duration_seconds=duration,
                changes=changes
            )
            
        except Exception as e:
            return self._handle_error(OperationType.UPDATE, resource_name, e)
    
    def delete(self, connection: Connection) -> ExecutionResult:
        """
        Delete a connection.
        
        Warning: Cannot delete if foreign catalogs are using this connection.
        """
        start_time = time.time()
        resource_name = connection.resolved_name
        
        try:
            if not self.exists(connection):
                return ExecutionResult(
                    success=True,
                    operation=OperationType.NO_OP,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message="Does not exist"
                )
            
            if self.dry_run:
                logger.info(f"[DRY RUN] Would delete connection {resource_name}")
                return ExecutionResult(
                    success=True,
                    operation=OperationType.DELETE,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message="Would be deleted (dry run)"
                )
            
            logger.info(f"Deleting connection {resource_name}")
            
            # Note: This will fail if foreign catalogs depend on this connection
            self.execute_with_retry(
                self.client.connections.delete,
                resource_name
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
            if "is still referenced" in str(e) or "foreign catalog" in str(e).lower():
                logger.error(
                    f"Cannot delete {resource_name}: Foreign catalogs still depend on it. "
                    f"Delete the foreign catalogs first."
                )
            return self._handle_error(OperationType.DELETE, resource_name, e)
    
    def _get_connection_changes(self, existing: ConnectionInfo, desired: Connection) -> Dict[str, Any]:
        """Compare existing and desired connection to find changes."""
        changes = {}
        
        # Check comment
        if hasattr(existing, 'comment') and existing.comment != desired.comment:
            changes['comment'] = {'from': existing.comment, 'to': desired.comment}
        
        # Check owner
        if desired.owner:
            desired_owner = desired.owner.resolved_name
            if hasattr(existing, 'owner') and existing.owner != desired_owner:
                changes['owner'] = {'from': existing.owner, 'to': desired_owner}
        
        # Check connection type (immutable)
        if hasattr(existing, 'connection_type'):
            existing_type = ConnectionType(existing.connection_type)
            if existing_type != desired.connection_type:
                changes['connection_type'] = {
                    'from': existing_type.value,
                    'to': desired.connection_type.value,
                    'note': 'Connection type is immutable - requires recreate'
                }
        
        # Check host/port
        if hasattr(existing, 'host') and existing.host != desired.host:
            changes['host'] = {'from': existing.host, 'to': desired.host}
        
        if hasattr(existing, 'port') and existing.port != desired.port:
            changes['port'] = {'from': existing.port, 'to': desired.port}
        
        # Check credentials (don't log actual values)
        if hasattr(existing, 'user') and existing.user != desired.user:
            changes['user'] = {'from': 'existing', 'to': 'updated'}
        
        # Password changes can't be detected (not returned by API)
        # but we can note if a new password is provided
        if desired.password:
            changes['password'] = {'note': 'Password will be updated'}
        
        # Check options (may contain sensitive data)
        if desired.options:
            # Just note that options are being updated
            changes['options'] = {'note': 'Connection options will be updated'}
        
        return changes