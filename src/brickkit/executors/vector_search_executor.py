"""
Vector Search executor for AI/ML governance operations.

Handles creation and management of Vector Search endpoints and indexes.
Includes endpoint provisioning wait, batch deployment, and sync monitoring.

Usage:
    from databricks.sdk import WorkspaceClient
    from brickkit.executors import VectorSearchEndpointExecutor, VectorSearchIndexExecutor
    from brickkit import VectorSearchEndpoint, VectorSearchIndex

    client = WorkspaceClient()

    # Deploy endpoint
    endpoint_executor = VectorSearchEndpointExecutor(client)
    endpoint = VectorSearchEndpoint(name="search_endpoint")
    result = endpoint_executor.create(endpoint)

    # Wait for endpoint to be online
    endpoint_executor.wait_for_endpoint(endpoint)

    # Deploy index
    index_executor = VectorSearchIndexExecutor(client)
    index = VectorSearchIndex(
        name="products",
        endpoint_name="search_endpoint",
        source_table="catalog.schema.products",
        primary_key="id",
        embedding_column="embedding"
    )
    result = index_executor.create(index)
"""

import logging
import time
from typing import Any, Dict, List, Optional, Union

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import (
    BadRequest,
    NotFound,
    PermissionDenied,
    ResourceDoesNotExist,
)

from brickkit.models.vector_search import VectorSearchEndpoint, VectorSearchIndex

from .base import BaseExecutor, ExecutionResult, OperationType

logger = logging.getLogger(__name__)

# Type alias for either endpoint or index
VectorSearchResource = Union[VectorSearchEndpoint, VectorSearchIndex]


# =============================================================================
# ENDPOINT STATUS
# =============================================================================

class EndpointStatus:
    """Status values for Vector Search endpoints."""
    ONLINE = "ONLINE"
    PROVISIONING = "PROVISIONING"
    PENDING = "PENDING"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"


# =============================================================================
# VECTOR SEARCH ENDPOINT EXECUTOR
# =============================================================================

class VectorSearchEndpointExecutor(BaseExecutor[VectorSearchEndpoint]):
    """
    Executor for Vector Search Endpoint operations.

    Features:
    - Create and delete endpoints
    - Wait for endpoint provisioning
    - Batch endpoint deployment
    - Status monitoring
    """

    def __init__(
        self,
        client: WorkspaceClient,
        dry_run: bool = False,
        max_retries: int = 3,
        continue_on_error: bool = False,
        governance_defaults: Optional[Any] = None,
        wait_timeout_seconds: int = 1800,  # 30 minutes default
        poll_interval_seconds: int = 30,
    ):
        """
        Initialize the Vector Search Endpoint executor.

        Args:
            client: Databricks SDK client
            dry_run: If True, only show what would be done
            max_retries: Maximum retry attempts for transient failures
            continue_on_error: Continue execution despite errors
            governance_defaults: Optional governance defaults for validation
            wait_timeout_seconds: Timeout for waiting on endpoint provisioning
            poll_interval_seconds: Interval between status checks
        """
        super().__init__(client, dry_run, max_retries, continue_on_error, governance_defaults)
        self.wait_timeout_seconds = wait_timeout_seconds
        self.poll_interval_seconds = poll_interval_seconds

    def get_resource_type(self) -> str:
        return "VECTOR_SEARCH_ENDPOINT"

    def exists(self, resource: VectorSearchEndpoint) -> bool:
        """Check if endpoint exists."""
        try:
            self.client.vector_search_endpoints.get_endpoint(resource.resolved_name)
            return True
        except (ResourceDoesNotExist, NotFound):
            return False
        except PermissionDenied as e:
            logger.error(f"Permission denied checking endpoint: {e}")
            raise
        except Exception as e:
            # Handle string-based error detection for older SDK versions
            if "RESOURCE_DOES_NOT_EXIST" in str(e) or "NOT_FOUND" in str(e):
                return False
            raise

    def get_endpoint_status(self, endpoint_name: str) -> str:
        """
        Get the current status of an endpoint.

        Args:
            endpoint_name: Name of the endpoint (with env suffix)

        Returns:
            Status string (ONLINE, PROVISIONING, PENDING, FAILED, UNKNOWN)
        """
        try:
            endpoint = self.client.vector_search_endpoints.get_endpoint(endpoint_name)
            # Handle different SDK response formats
            if hasattr(endpoint, 'endpoint_status'):
                status_obj = endpoint.endpoint_status
                if hasattr(status_obj, 'state'):
                    return status_obj.state
                elif isinstance(status_obj, dict):
                    return status_obj.get('state', EndpointStatus.UNKNOWN)
            return EndpointStatus.UNKNOWN
        except Exception as e:
            logger.error(f"Failed to get endpoint status: {e}")
            return EndpointStatus.UNKNOWN

    def wait_for_endpoint(
        self,
        resource: VectorSearchEndpoint,
        timeout_seconds: Optional[int] = None,
        poll_interval: Optional[int] = None,
    ) -> bool:
        """
        Wait for an endpoint to become online.

        Args:
            resource: The endpoint to wait for
            timeout_seconds: Override timeout (uses default if None)
            poll_interval: Override poll interval (uses default if None)

        Returns:
            True if endpoint is online, False if timeout or failed
        """
        timeout = timeout_seconds or self.wait_timeout_seconds
        interval = poll_interval or self.poll_interval_seconds
        endpoint_name = resource.resolved_name

        if self.dry_run:
            logger.info(f"[DRY RUN] Would wait for endpoint {endpoint_name} to be online")
            return True

        logger.info(f"Waiting for endpoint {endpoint_name} to be online...")
        start_time = time.time()

        while True:
            elapsed = time.time() - start_time
            if elapsed > timeout:
                logger.error(f"Timeout waiting for endpoint {endpoint_name} after {elapsed:.0f}s")
                return False

            status = self.get_endpoint_status(endpoint_name)
            logger.info(f"Endpoint status: {status} (elapsed: {elapsed:.0f}s)")

            if status == EndpointStatus.ONLINE:
                logger.info(f"Endpoint {endpoint_name} is ready!")
                return True
            elif status == EndpointStatus.FAILED:
                logger.error(f"Endpoint {endpoint_name} failed to provision")
                return False
            elif status in [EndpointStatus.PROVISIONING, EndpointStatus.PENDING]:
                logger.info(f"Waiting {interval} seconds...")
                time.sleep(interval)
            else:
                logger.warning(f"Unexpected status: {status}")
                time.sleep(interval)

    def create(self, resource: VectorSearchEndpoint) -> ExecutionResult:
        """Create a Vector Search endpoint."""
        start_time = time.time()
        resource_name = resource.resolved_name

        if self.dry_run:
            logger.info(f"[DRY RUN] Would create endpoint {resource_name}")
            return ExecutionResult(
                success=True,
                operation=OperationType.CREATE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message="Would be created (dry run)"
            )

        try:
            if self.exists(resource):
                return ExecutionResult(
                    success=True,
                    operation=OperationType.NO_OP,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message="Already exists"
                )

            logger.info(f"Creating Vector Search endpoint: {resource_name}")
            params = resource.to_sdk_create_params()
            self.client.vector_search_endpoints.create_endpoint(**params)

            duration = time.time() - start_time
            return ExecutionResult(
                success=True,
                operation=OperationType.CREATE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message="Created successfully (may take time to provision)",
                duration_seconds=duration
            )

        except PermissionDenied as e:
            logger.error(f"Permission denied creating endpoint: {e}")
            raise
        except BadRequest as e:
            return self._handle_error(OperationType.CREATE, resource_name, e)

    def update(self, resource: VectorSearchEndpoint) -> ExecutionResult:
        """Update is not supported for endpoints."""
        return ExecutionResult(
            success=True,
            operation=OperationType.NO_OP,
            resource_type=self.get_resource_type(),
            resource_name=resource.resolved_name,
            message="Endpoint updates not supported"
        )

    def delete(self, resource: VectorSearchEndpoint) -> ExecutionResult:
        """Delete a Vector Search endpoint."""
        start_time = time.time()
        resource_name = resource.resolved_name

        if self.dry_run:
            logger.info(f"[DRY RUN] Would delete endpoint {resource_name}")
            return ExecutionResult(
                success=True,
                operation=OperationType.DELETE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message="Would be deleted (dry run)"
            )

        try:
            if not self.exists(resource):
                return ExecutionResult(
                    success=True,
                    operation=OperationType.NO_OP,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message="Does not exist"
                )

            logger.info(f"Deleting Vector Search endpoint: {resource_name}")
            self.client.vector_search_endpoints.delete_endpoint(resource_name)

            duration = time.time() - start_time
            return ExecutionResult(
                success=True,
                operation=OperationType.DELETE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message="Deleted successfully",
                duration_seconds=duration
            )

        except PermissionDenied as e:
            logger.error(f"Permission denied deleting endpoint: {e}")
            raise
        except BadRequest as e:
            return self._handle_error(OperationType.DELETE, resource_name, e)

    # =========================================================================
    # BATCH OPERATIONS
    # =========================================================================

    def deploy_all(
        self,
        endpoints: List[VectorSearchEndpoint],
        wait_for_online: bool = True,
    ) -> Dict[str, ExecutionResult]:
        """
        Deploy multiple Vector Search endpoints.

        Args:
            endpoints: List of endpoints to deploy
            wait_for_online: If True, wait for each endpoint to be online

        Returns:
            Dict mapping endpoint names to ExecutionResults
        """
        results = {}

        for endpoint in endpoints:
            logger.info(f"Deploying endpoint: {endpoint.resolved_name}")
            try:
                result = self.create(endpoint)
                results[endpoint.resolved_name] = result
                self.results.append(result)

                if result.success and result.operation == OperationType.CREATE and wait_for_online:
                    if self.wait_for_endpoint(endpoint):
                        logger.info(f"  Endpoint {endpoint.resolved_name} is online")
                    else:
                        logger.warning(f"  Endpoint {endpoint.resolved_name} not online yet")

            except Exception as e:
                error_result = ExecutionResult(
                    success=False,
                    operation=OperationType.CREATE,
                    resource_type=self.get_resource_type(),
                    resource_name=endpoint.resolved_name,
                    message=str(e),
                    error=e
                )
                results[endpoint.resolved_name] = error_result
                self.results.append(error_result)

                if not self.continue_on_error:
                    raise

        return results


# =============================================================================
# VECTOR SEARCH INDEX EXECUTOR
# =============================================================================

class VectorSearchIndexExecutor(BaseExecutor[VectorSearchIndex]):
    """
    Executor for Vector Search Index operations.

    Features:
    - Create and delete indexes
    - Batch index deployment
    - Sync status monitoring
    """

    def get_resource_type(self) -> str:
        return "VECTOR_SEARCH_INDEX"

    def _get_full_index_name(self, resource: VectorSearchIndex) -> str:
        """Get the full index name including endpoint."""
        return f"{resource.resolved_endpoint_name}/{resource.resolved_name}"

    def exists(self, resource: VectorSearchIndex) -> bool:
        """Check if index exists."""
        try:
            self.client.vector_search_indexes.get_index(
                index_name=self._get_full_index_name(resource)
            )
            return True
        except (ResourceDoesNotExist, NotFound):
            return False
        except PermissionDenied as e:
            logger.error(f"Permission denied checking index: {e}")
            raise
        except Exception as e:
            # Handle string-based error detection for older SDK versions
            if "RESOURCE_DOES_NOT_EXIST" in str(e) or "NOT_FOUND" in str(e):
                return False
            raise

    def create(self, resource: VectorSearchIndex) -> ExecutionResult:
        """Create a Vector Search index."""
        start_time = time.time()
        resource_name = resource.resolved_name

        if self.dry_run:
            logger.info(f"[DRY RUN] Would create index {resource_name}")
            return ExecutionResult(
                success=True,
                operation=OperationType.CREATE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message="Would be created (dry run)",
                changes=self._get_index_summary(resource)
            )

        try:
            if self.exists(resource):
                return ExecutionResult(
                    success=True,
                    operation=OperationType.NO_OP,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message="Already exists"
                )

            logger.info(f"Creating Vector Search index: {resource_name}")
            params = resource.to_sdk_create_params()

            # Use delta sync or direct access based on index type
            if "delta_sync_index_spec" in params:
                self.client.vector_search_indexes.create_index(
                    name=params["name"],
                    endpoint_name=params["endpoint_name"],
                    primary_key=params["primary_key"],
                    index_type=params["index_type"],
                    delta_sync_index_spec=params["delta_sync_index_spec"]
                )
            else:
                self.client.vector_search_indexes.create_index(**params)

            duration = time.time() - start_time
            return ExecutionResult(
                success=True,
                operation=OperationType.CREATE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message="Created successfully (syncing in progress)",
                duration_seconds=duration
            )

        except PermissionDenied as e:
            logger.error(f"Permission denied creating index: {e}")
            raise
        except BadRequest as e:
            return self._handle_error(OperationType.CREATE, resource_name, e)

    def update(self, resource: VectorSearchIndex) -> ExecutionResult:
        """Update is not supported for indexes - recreate instead."""
        return ExecutionResult(
            success=True,
            operation=OperationType.NO_OP,
            resource_type=self.get_resource_type(),
            resource_name=resource.resolved_name,
            message="Index updates not supported - delete and recreate"
        )

    def delete(self, resource: VectorSearchIndex) -> ExecutionResult:
        """Delete a Vector Search index."""
        start_time = time.time()
        resource_name = resource.resolved_name

        if self.dry_run:
            logger.info(f"[DRY RUN] Would delete index {resource_name}")
            return ExecutionResult(
                success=True,
                operation=OperationType.DELETE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message="Would be deleted (dry run)"
            )

        try:
            if not self.exists(resource):
                return ExecutionResult(
                    success=True,
                    operation=OperationType.NO_OP,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message="Does not exist"
                )

            full_name = self._get_full_index_name(resource)
            logger.info(f"Deleting Vector Search index: {full_name}")
            self.client.vector_search_indexes.delete_index(index_name=full_name)

            duration = time.time() - start_time
            return ExecutionResult(
                success=True,
                operation=OperationType.DELETE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message="Deleted successfully",
                duration_seconds=duration
            )

        except PermissionDenied as e:
            logger.error(f"Permission denied deleting index: {e}")
            raise
        except BadRequest as e:
            return self._handle_error(OperationType.DELETE, resource_name, e)

    # =========================================================================
    # BATCH OPERATIONS
    # =========================================================================

    def deploy_all(
        self,
        indexes: List[VectorSearchIndex],
    ) -> Dict[str, ExecutionResult]:
        """
        Deploy multiple Vector Search indexes.

        Args:
            indexes: List of indexes to deploy

        Returns:
            Dict mapping index names to ExecutionResults
        """
        results = {}

        for index in indexes:
            logger.info(f"Deploying index: {index.resolved_name}")
            try:
                result = self.create(index)
                results[index.resolved_name] = result
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
                    resource_name=index.resolved_name,
                    message=str(e),
                    error=e
                )
                results[index.resolved_name] = error_result
                self.results.append(error_result)

                if not self.continue_on_error:
                    raise

        return results

    # =========================================================================
    # HELPERS
    # =========================================================================

    def _get_index_summary(self, index: VectorSearchIndex) -> Dict[str, Any]:
        """Get a summary of index configuration for logging."""
        return {
            "name": index.resolved_name,
            "endpoint": index.resolved_endpoint_name,
            "source_table": index.source_table,
            "primary_key": index.primary_key,
            "embedding_column": index.embedding_column,
        }
