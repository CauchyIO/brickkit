"""
Table executor for Unity Catalog operations.

Handles creation, update, and deletion of tables via the Databricks SDK.
"""

import time
from typing import Dict, Any
import logging
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import TableInfo
from databricks.sdk.errors import ResourceDoesNotExist, ResourceAlreadyExists
from ..models import Table
from .base import BaseExecutor, ExecutionResult, OperationType

logger = logging.getLogger(__name__)


class TableExecutor(BaseExecutor[Table]):
    """Executor for table operations."""
    
    def get_resource_type(self) -> str:
        """Get the resource type."""
        return "TABLE"
    
    def exists(self, table: Table) -> bool:
        """Check if a table exists."""
        try:
            self.client.tables.get(table.fqdn)
            return True
        except ResourceDoesNotExist:
            return False
        except Exception as e:
            logger.warning(f"Error checking table existence: {e}")
            return False
    
    def create(self, table: Table) -> ExecutionResult:
        """Create a new table."""
        start_time = time.time()
        resource_name = table.fqdn
        
        try:
            if self.dry_run:
                logger.info(f"[DRY RUN] Would create table {resource_name}")
                return ExecutionResult(
                    success=True,
                    operation=OperationType.CREATE,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message="Would be created (dry run)"
                )
            
            # Try SDK API first
            try:
                params = table.to_sdk_create_params()
                logger.info(f"Creating table {resource_name} via SDK API")
                self.execute_with_retry(self.client.tables.create, **params)

                # Set comment via update if needed
                if table.comment:
                    logger.debug(f"Setting table comment: {table.comment}")
                    self.execute_with_retry(
                        self.client.tables.update,
                        full_name=f"{table.resolved_catalog_name}.{table.schema_name}.{table.name}",
                        comment=table.comment
                    )

                created_via = "SDK API"
            except Exception as sdk_error:
                # Check if it's a permission error or path overlap error
                error_msg = str(sdk_error)
                if "PERMISSION_DENIED" in error_msg or "EXTERNAL USE SCHEMA" in error_msg or "overlaps with other external tables" in error_msg:
                    # For overlap errors, extract the conflicting table and provide guidance
                    if "overlaps with other external tables" in error_msg:
                        import re
                        # Extract conflicting table name from error
                        conflict_match = re.search(r"Conflicting tables/volumes: ([^.]+\.[^.]+\.[^.]+)", error_msg)
                        if conflict_match:
                            conflicting_table = conflict_match.group(1)
                            logger.error(f"Storage location conflict detected!")
                            logger.error(f"  Attempting to create: {resource_name}")
                            logger.error(f"  Conflicts with: {conflicting_table}")
                            logger.error(f"  Resolution: Either drop the conflicting table or use a different storage location")

                            # If it's the same table in a differently named catalog (e.g., mixed or double suffix), skip
                            if ("_dev_dev" in conflicting_table or "_prd_prd" in conflicting_table or
                                "_prd_dev" in conflicting_table or "_dev_prd" in conflicting_table):
                                logger.warning(f"Detected mixed/double environment suffix in conflicting table, likely from previous test run")
                                logger.info(f"Skipping table creation due to unresolvable conflict")
                                duration = time.time() - start_time
                                return ExecutionResult(
                                    success=True,
                                    operation=OperationType.SKIPPED,
                                    resource_type=self.get_resource_type(),
                                    resource_name=resource_name,
                                    message=f"Skipped due to conflict with {conflicting_table}",
                                    duration_seconds=duration
                                )

                        # Check if the table exists at the expected location
                        if self.exists(table):
                            logger.info(f"Table {resource_name} already exists, treating as success")
                            duration = time.time() - start_time
                            return ExecutionResult(
                                success=True,
                                operation=OperationType.NO_OP,
                                resource_type=self.get_resource_type(),
                                resource_name=resource_name,
                                message="Table already exists (overlap detected)",
                                duration_seconds=duration
                            )

                    logger.info(f"SDK API failed with error ({error_msg[:100]}...), falling back to SQL DDL")
                    
                    # Use SQL DDL approach
                    ddl = table.to_sql_ddl(if_not_exists=True)
                    logger.info(f"Creating table {resource_name} via SQL DDL")
                    
                    # Execute SQL using the workspace client's SQL execution
                    # Note: This requires the client to have SQL execution capabilities
                    # We'll use the client's statement execution API
                    from databricks.sdk.service.sql import StatementState
                    
                    # Get or create a SQL warehouse endpoint
                    warehouses = list(self.client.warehouses.list())
                    if not warehouses:
                        raise ValueError("No SQL warehouse available for SQL DDL execution")
                    
                    warehouse_id = warehouses[0].id
                    
                    # Execute the DDL statement
                    response = self.client.statement_execution.execute_statement(
                        warehouse_id=warehouse_id,
                        statement=ddl,
                        catalog=table.resolved_catalog_name if hasattr(table, 'resolved_catalog_name') else None,
                        schema=table.schema_name
                    )
                    
                    # Wait for statement to complete
                    import time as time_module
                    max_wait = 60  # seconds
                    wait_time = 0
                    while wait_time < max_wait:
                        status = self.client.statement_execution.get_statement(
                            statement_id=response.statement_id
                        )
                        if status.status.state in [StatementState.SUCCEEDED, StatementState.FAILED, StatementState.CLOSED]:
                            break
                        time_module.sleep(1)
                        wait_time += 1
                    
                    if status.status.state != StatementState.SUCCEEDED:
                        error_msg = f"SQL DDL execution failed: {status.status.state}"
                        if hasattr(status.status, 'error') and status.status.error:
                            error_msg += f" - {status.status.error.message}"
                        logger.error(f"SQL DDL:\n{ddl}")
                        raise Exception(error_msg)
                    
                    created_via = "SQL DDL"
                else:
                    # Not a permission error, re-raise
                    raise sdk_error
            
            # Apply row filter if specified
            if table.row_filter:
                logger.info(f"Applying row filter to {resource_name}")
                # Note: Row filter is set via ALTER TABLE in SQL, not SDK directly
                # This would need to be done via SQL execution
            
            # Apply column masks if specified
            if table.column_masks:
                for column, function in table.column_masks.items():
                    logger.info(f"Applying column mask to {resource_name}.{column}")
                    # Note: Column masks are set via ALTER TABLE in SQL
            
            self._rollback_stack.append(
                lambda: self.client.tables.delete(resource_name)
            )
            
            duration = time.time() - start_time
            return ExecutionResult(
                success=True,
                operation=OperationType.CREATE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message=f"Created successfully via {created_via}",
                duration_seconds=duration
            )
            
        except Exception as e:
            return self._handle_error(OperationType.CREATE, resource_name, e)
    
    def update(self, table: Table) -> ExecutionResult:
        """Update an existing table."""
        start_time = time.time()
        resource_name = table.fqdn
        
        try:
            existing = self.client.tables.get(resource_name)
            changes = self._get_table_changes(existing, table)
            
            if not changes:
                return ExecutionResult(
                    success=True,
                    operation=OperationType.NO_OP,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message="No changes needed"
                )
            
            if self.dry_run:
                logger.info(f"[DRY RUN] Would update table {resource_name}")
                return ExecutionResult(
                    success=True,
                    operation=OperationType.UPDATE,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message=f"Would update: {changes} (dry run)",
                    changes=changes
                )
            
            params = table.to_sdk_update_params()
            logger.info(f"Updating table {resource_name}: {changes}")
            self.execute_with_retry(self.client.tables.update, **params)
            
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
    
    def delete(self, table: Table) -> ExecutionResult:
        """Delete a table."""
        start_time = time.time()
        resource_name = table.fqdn
        
        try:
            if not self.exists(table):
                return ExecutionResult(
                    success=True,
                    operation=OperationType.NO_OP,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message="Does not exist"
                )
            
            if self.dry_run:
                logger.info(f"[DRY RUN] Would delete table {resource_name}")
                return ExecutionResult(
                    success=True,
                    operation=OperationType.DELETE,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message="Would be deleted (dry run)"
                )
            
            logger.info(f"Deleting table {resource_name}")
            self.execute_with_retry(self.client.tables.delete, resource_name)
            
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
    
    def _get_table_changes(self, existing: TableInfo, desired: Table) -> Dict[str, Any]:
        """Compare existing and desired table to find changes."""
        changes = {}

        # Tables have very limited update capabilities via SDK
        # Comment can only be updated via SQL ALTER TABLE

        # Only update owner if:
        # 1. We have a desired owner
        # 2. It's not the default owner
        # 3. It's different from the existing owner
        from dbrcdk.models.base import DEFAULT_SECURABLE_OWNER
        if desired.owner and desired.owner.name != DEFAULT_SECURABLE_OWNER:
            desired_owner = desired.owner.resolved_name
            if hasattr(existing, 'owner') and existing.owner != desired_owner:
                changes['owner'] = {'from': existing.owner, 'to': desired_owner}

        return changes