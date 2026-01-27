"""
Integration tests for VectorSearchEndpointExecutor and VectorSearchIndexExecutor.

Tests vector search endpoint and index operations against real Databricks.

NOTE: These tests are marked as 'slow' because Vector Search endpoints:
- Take 5-10 minutes to provision
- Cost approximately $2/hour once running
- Require manual cleanup if tests fail

Run with: uv run pytest tests/integration/test_vector_search_executor.py -m slow -v
Skip with: uv run pytest tests/integration -m "not slow"
"""

import pytest
from databricks.sdk import WorkspaceClient

from brickkit.executors.vector_search_executor import (
    VectorSearchEndpointExecutor,
    VectorSearchIndexExecutor,
)
from brickkit.models.vector_search import (
    VectorEndpointType,
    VectorSearchEndpoint,
    VectorSearchIndex,
)
from tests.integration.conftest import ResourceTracker


@pytest.mark.slow
class TestVectorSearchEndpointExecutor:
    """
    Tests for VectorSearchEndpointExecutor.

    WARNING: These tests create real Vector Search endpoints which:
    - Take 5-10 minutes to provision
    - Cost approximately $2/hour while running
    """

    def test_create_endpoint(
        self,
        workspace_client: WorkspaceClient,
        vector_search_endpoint_executor: VectorSearchEndpointExecutor,
        resource_tracker: ResourceTracker,
        test_prefix: str,
    ) -> None:
        """Create a Vector Search endpoint (slow)."""
        endpoint = VectorSearchEndpoint(
            name=f"{test_prefix}_vs_ep",
            endpoint_type=VectorEndpointType.STANDARD,
            comment="Integration test endpoint",
        )

        result = vector_search_endpoint_executor.create(endpoint)
        assert result.success, f"Failed to create endpoint: {result.message}"
        resource_tracker.add_vector_search_endpoint(endpoint.resolved_name)

    def test_exists_true_when_present(
        self,
        workspace_client: WorkspaceClient,
        vector_search_endpoint_executor: VectorSearchEndpointExecutor,
        resource_tracker: ResourceTracker,
        test_prefix: str,
    ) -> None:
        """exists() returns True when endpoint is present (slow)."""
        endpoint = VectorSearchEndpoint(
            name=f"{test_prefix}_vs_exists",
            endpoint_type=VectorEndpointType.STANDARD,
        )

        result = vector_search_endpoint_executor.create(endpoint)
        assert result.success
        resource_tracker.add_vector_search_endpoint(endpoint.resolved_name)

        assert vector_search_endpoint_executor.exists(endpoint) is True

    def test_exists_false_when_missing(
        self,
        workspace_client: WorkspaceClient,
        vector_search_endpoint_executor: VectorSearchEndpointExecutor,
        test_prefix: str,
    ) -> None:
        """exists() returns False when endpoint doesn't exist."""
        endpoint = VectorSearchEndpoint(
            name=f"{test_prefix}_vs_nonexistent",
            endpoint_type=VectorEndpointType.STANDARD,
        )

        assert vector_search_endpoint_executor.exists(endpoint) is False

    def test_delete_endpoint(
        self,
        workspace_client: WorkspaceClient,
        vector_search_endpoint_executor: VectorSearchEndpointExecutor,
        resource_tracker: ResourceTracker,
        test_prefix: str,
    ) -> None:
        """delete() removes the endpoint (slow)."""
        endpoint = VectorSearchEndpoint(
            name=f"{test_prefix}_vs_delete",
            endpoint_type=VectorEndpointType.STANDARD,
        )

        create_result = vector_search_endpoint_executor.create(endpoint)
        assert create_result.success

        # Delete immediately (don't add to tracker)
        delete_result = vector_search_endpoint_executor.delete(endpoint)
        assert delete_result.success, f"Failed to delete endpoint: {delete_result.message}"

        # Verify it's gone
        assert vector_search_endpoint_executor.exists(endpoint) is False


class TestVectorSearchEndpointExecutorDryRun:
    """Tests for VectorSearchEndpointExecutor dry run mode (fast)."""

    def test_dry_run_does_not_create(
        self,
        workspace_client: WorkspaceClient,
        test_prefix: str,
    ) -> None:
        """Dry run mode doesn't actually create the endpoint."""
        dry_run_executor = VectorSearchEndpointExecutor(workspace_client, dry_run=True)

        endpoint = VectorSearchEndpoint(
            name=f"{test_prefix}_vs_dry",
            endpoint_type=VectorEndpointType.STANDARD,
        )

        result = dry_run_executor.create(endpoint)
        assert result.success
        assert "dry run" in result.message.lower()

        # Verify endpoint was NOT actually created
        real_executor = VectorSearchEndpointExecutor(workspace_client)
        assert real_executor.exists(endpoint) is False


@pytest.mark.slow
class TestVectorSearchIndexExecutor:
    """
    Tests for VectorSearchIndexExecutor.

    WARNING: These tests require:
    - An existing Vector Search endpoint (takes 5-10 min to provision)
    - An existing Delta table with data and embedding column
    """

    @pytest.fixture
    def vector_search_endpoint_with_cleanup(
        self,
        workspace_client: WorkspaceClient,
        vector_search_endpoint_executor: VectorSearchEndpointExecutor,
        resource_tracker: ResourceTracker,
        test_prefix: str,
    ) -> VectorSearchEndpoint:
        """Create and track an endpoint for index tests."""
        endpoint = VectorSearchEndpoint(
            name=f"{test_prefix}_idx_ep",
            endpoint_type=VectorEndpointType.STANDARD,
        )
        result = vector_search_endpoint_executor.create(endpoint)
        assert result.success, f"Failed to create endpoint: {result.message}"
        resource_tracker.add_vector_search_endpoint(endpoint.resolved_name)

        # Wait for endpoint to be online
        # Note: In a real test you'd want to wait for the endpoint to be ONLINE
        # This is simplified for the test template
        return endpoint

    def test_exists_false_when_missing(
        self,
        workspace_client: WorkspaceClient,
        vector_search_index_executor: VectorSearchIndexExecutor,
        test_prefix: str,
    ) -> None:
        """exists() returns False when index doesn't exist."""
        index = VectorSearchIndex(
            name=f"{test_prefix}_nonexistent_idx",
            endpoint_name="nonexistent_endpoint",
            source_table="catalog.schema.table",
            primary_key="id",
            embedding_column="embedding",
        )

        assert vector_search_index_executor.exists(index) is False


class TestVectorSearchIndexExecutorDryRun:
    """Tests for VectorSearchIndexExecutor dry run mode (fast)."""

    def test_dry_run_does_not_create(
        self,
        workspace_client: WorkspaceClient,
        test_prefix: str,
    ) -> None:
        """Dry run mode doesn't actually create the index."""
        dry_run_executor = VectorSearchIndexExecutor(workspace_client, dry_run=True)

        index = VectorSearchIndex(
            name=f"{test_prefix}_idx_dry",
            endpoint_name="any_endpoint",
            source_table="catalog.schema.table",
            primary_key="id",
            embedding_column="embedding",
            embedding_model="databricks-bge-large-en",
        )

        result = dry_run_executor.create(index)
        assert result.success
        assert "dry run" in result.message.lower()
