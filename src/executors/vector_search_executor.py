"""
Vector Search executor for AI/ML governance operations.

Handles creation and management of Vector Search endpoints and indexes.
"""

import time
import logging
from typing import Union
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import (
    ResourceDoesNotExist,
    NotFound,
    PermissionDenied,
    BadRequest,
)

from ..vector_search.models import VectorSearchEndpoint, VectorSearchIndex
from .base import BaseExecutor, ExecutionResult, OperationType

logger = logging.getLogger(__name__)

# Type alias for either endpoint or index
VectorSearchResource = Union[VectorSearchEndpoint, VectorSearchIndex]


class VectorSearchEndpointExecutor(BaseExecutor[VectorSearchEndpoint]):
    """Executor for Vector Search Endpoint operations."""

    def get_resource_type(self) -> str:
        return "VECTOR_SEARCH_ENDPOINT"

    def exists(self, endpoint: VectorSearchEndpoint) -> bool:
        """Check if endpoint exists."""
        try:
            self.client.vector_search_endpoints.get_endpoint(endpoint.resolved_name)
            return True
        except (ResourceDoesNotExist, NotFound):
            return False
        except PermissionDenied as e:
            logger.error(f"Permission denied checking endpoint: {e}")
            raise

    def create(self, endpoint: VectorSearchEndpoint) -> ExecutionResult:
        """Create a Vector Search endpoint."""
        start_time = time.time()
        resource_name = endpoint.resolved_name

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
            if self.exists(endpoint):
                return ExecutionResult(
                    success=True,
                    operation=OperationType.NO_OP,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message="Already exists"
                )

            logger.info(f"Creating Vector Search endpoint: {resource_name}")
            params = endpoint.to_sdk_create_params()
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

    def update(self, endpoint: VectorSearchEndpoint) -> ExecutionResult:
        """Update is not supported for endpoints."""
        return ExecutionResult(
            success=True,
            operation=OperationType.NO_OP,
            resource_type=self.get_resource_type(),
            resource_name=endpoint.resolved_name,
            message="Endpoint updates not supported"
        )

    def delete(self, endpoint: VectorSearchEndpoint) -> ExecutionResult:
        """Delete a Vector Search endpoint."""
        start_time = time.time()
        resource_name = endpoint.resolved_name

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
            if not self.exists(endpoint):
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


class VectorSearchIndexExecutor(BaseExecutor[VectorSearchIndex]):
    """Executor for Vector Search Index operations."""

    def get_resource_type(self) -> str:
        return "VECTOR_SEARCH_INDEX"

    def exists(self, index: VectorSearchIndex) -> bool:
        """Check if index exists."""
        try:
            self.client.vector_search_indexes.get_index(
                index_name=f"{index.resolved_endpoint_name}/{index.resolved_name}"
            )
            return True
        except (ResourceDoesNotExist, NotFound):
            return False
        except PermissionDenied as e:
            logger.error(f"Permission denied checking index: {e}")
            raise

    def create(self, index: VectorSearchIndex) -> ExecutionResult:
        """Create a Vector Search index."""
        start_time = time.time()
        resource_name = index.resolved_name

        if self.dry_run:
            logger.info(f"[DRY RUN] Would create index {resource_name}")
            return ExecutionResult(
                success=True,
                operation=OperationType.CREATE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message="Would be created (dry run)"
            )

        try:
            if self.exists(index):
                return ExecutionResult(
                    success=True,
                    operation=OperationType.NO_OP,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message="Already exists"
                )

            logger.info(f"Creating Vector Search index: {resource_name}")
            params = index.to_sdk_create_params()

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

    def update(self, index: VectorSearchIndex) -> ExecutionResult:
        """Update is not supported for indexes - recreate instead."""
        return ExecutionResult(
            success=True,
            operation=OperationType.NO_OP,
            resource_type=self.get_resource_type(),
            resource_name=index.resolved_name,
            message="Index updates not supported - delete and recreate"
        )

    def delete(self, index: VectorSearchIndex) -> ExecutionResult:
        """Delete a Vector Search index."""
        start_time = time.time()
        resource_name = index.resolved_name

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
            if not self.exists(index):
                return ExecutionResult(
                    success=True,
                    operation=OperationType.NO_OP,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message="Does not exist"
                )

            full_name = f"{index.resolved_endpoint_name}/{index.resolved_name}"
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
