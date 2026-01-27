"""
Integration test fixtures for BrickKit.

Provides workspace client, executor fixtures, and resource cleanup.
"""

import logging
import os
from dataclasses import dataclass, field
from typing import Callable, Generator, List

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, ResourceDoesNotExist

from brickkit.executors import (
    CatalogExecutor,
    GrantExecutor,
    GroupExecutor,
    SchemaExecutor,
    ServicePrincipalExecutor,
    TableExecutor,
    VolumeExecutor,
)
from brickkit.executors.vector_search_executor import (
    VectorSearchEndpointExecutor,
    VectorSearchIndexExecutor,
)
from brickkit.models import Catalog, Schema

logger = logging.getLogger(__name__)


@dataclass
class ResourceTracker:
    """
    Tracks created resources for cleanup after tests.

    Resources are cleaned up in reverse dependency order:
    1. Vector Search Indexes (depend on endpoints + tables)
    2. Vector Search Endpoints
    3. Tables (depend on schemas)
    4. Volumes (depend on schemas)
    5. Schemas (depend on catalogs)
    6. Catalogs
    7. Groups
    8. Service Principals
    """

    # Existing
    catalogs: List[str] = field(default_factory=list)
    schemas: List[str] = field(default_factory=list)  # Full names (catalog.schema)
    groups: List[str] = field(default_factory=list)
    service_principals: List[str] = field(default_factory=list)
    custom_cleanups: List[Callable[[], None]] = field(default_factory=list)

    # New
    tables: List[str] = field(default_factory=list)  # full_name: catalog.schema.table
    volumes: List[str] = field(default_factory=list)  # full_name: catalog.schema.volume
    vector_search_endpoints: List[str] = field(default_factory=list)
    vector_search_indexes: List[str] = field(default_factory=list)  # full_name: endpoint/index

    def add_catalog(self, name: str) -> None:
        """Track a catalog for cleanup."""
        if name not in self.catalogs:
            self.catalogs.append(name)

    def add_schema(self, full_name: str) -> None:
        """Track a schema for cleanup (full_name = catalog.schema)."""
        if full_name not in self.schemas:
            self.schemas.append(full_name)

    def add_group(self, name: str) -> None:
        """Track a group for cleanup."""
        if name not in self.groups:
            self.groups.append(name)

    def add_service_principal(self, name: str) -> None:
        """Track a service principal for cleanup."""
        if name not in self.service_principals:
            self.service_principals.append(name)

    def add_custom_cleanup(self, cleanup_fn: Callable[[], None]) -> None:
        """Add a custom cleanup function."""
        self.custom_cleanups.append(cleanup_fn)

    def add_table(self, full_name: str) -> None:
        """Track a table for cleanup (full_name = catalog.schema.table)."""
        if full_name not in self.tables:
            self.tables.append(full_name)

    def add_volume(self, full_name: str) -> None:
        """Track a volume for cleanup (full_name = catalog.schema.volume)."""
        if full_name not in self.volumes:
            self.volumes.append(full_name)

    def add_vector_search_endpoint(self, name: str) -> None:
        """Track a vector search endpoint for cleanup."""
        if name not in self.vector_search_endpoints:
            self.vector_search_endpoints.append(name)

    def add_vector_search_index(self, endpoint: str, index: str) -> None:
        """Track a vector search index for cleanup (full_name = endpoint/index)."""
        full_name = f"{endpoint}/{index}"
        if full_name not in self.vector_search_indexes:
            self.vector_search_indexes.append(full_name)


@pytest.fixture(scope="session")
def workspace_client() -> WorkspaceClient:
    """
    Session-scoped WorkspaceClient using SDK auto-configuration.

    Respects DATABRICKS_HOST, DATABRICKS_TOKEN, or CLI profile.
    """
    # Ensure DEV environment for integration tests
    os.environ["DATABRICKS_ENV"] = "DEV"
    client = WorkspaceClient()
    # Verify connection works
    try:
        current_user = client.current_user.me()
        logger.info(f"Connected to Databricks as {current_user.user_name}")
    except Exception as e:
        pytest.skip(f"Could not connect to Databricks: {e}")
    return client


@pytest.fixture
def resource_tracker() -> ResourceTracker:
    """Fixture that provides a resource tracker for the test."""
    return ResourceTracker()


@pytest.fixture(autouse=True)
def cleanup_resources(
    workspace_client: WorkspaceClient,
    resource_tracker: ResourceTracker,
) -> Generator[None, None, None]:
    """
    Autouse fixture that cleans up tracked resources after each test.

    Cleanup order (reverse dependency):
    1. Custom cleanups (in reverse order)
    2. Vector Search Indexes (depend on endpoints + tables)
    3. Vector Search Endpoints
    4. Tables (depend on schemas)
    5. Volumes (depend on schemas)
    6. Schemas (depend on catalogs)
    7. Catalogs
    8. Groups
    9. Service Principals

    Failed cleanups are logged but don't fail the test.
    """
    yield

    # Run custom cleanups in reverse order
    for cleanup_fn in reversed(resource_tracker.custom_cleanups):
        try:
            cleanup_fn()
        except Exception as e:
            logger.warning(f"Custom cleanup failed: {e}")

    # Delete Vector Search Indexes first (depend on endpoints + tables)
    for index_name in reversed(resource_tracker.vector_search_indexes):
        try:
            workspace_client.vector_search_indexes.delete_index(index_name=index_name)
            logger.info(f"Cleaned up vector search index: {index_name}")
        except (NotFound, ResourceDoesNotExist):
            pass  # Already gone
        except Exception as e:
            logger.warning(f"Failed to cleanup vector search index {index_name}: {e}")

    # Delete Vector Search Endpoints
    for endpoint_name in reversed(resource_tracker.vector_search_endpoints):
        try:
            workspace_client.vector_search_endpoints.delete_endpoint(endpoint_name)
            logger.info(f"Cleaned up vector search endpoint: {endpoint_name}")
        except (NotFound, ResourceDoesNotExist):
            pass  # Already gone
        except Exception as e:
            logger.warning(f"Failed to cleanup vector search endpoint {endpoint_name}: {e}")

    # Delete tables (depend on schemas)
    for table_name in reversed(resource_tracker.tables):
        try:
            workspace_client.tables.delete(table_name)
            logger.info(f"Cleaned up table: {table_name}")
        except (NotFound, ResourceDoesNotExist):
            pass  # Already gone
        except Exception as e:
            logger.warning(f"Failed to cleanup table {table_name}: {e}")

    # Delete volumes (depend on schemas)
    for volume_name in reversed(resource_tracker.volumes):
        try:
            workspace_client.volumes.delete(volume_name)
            logger.info(f"Cleaned up volume: {volume_name}")
        except (NotFound, ResourceDoesNotExist):
            pass  # Already gone
        except Exception as e:
            logger.warning(f"Failed to cleanup volume {volume_name}: {e}")

    # Delete schemas (dependency order)
    for schema_name in reversed(resource_tracker.schemas):
        try:
            workspace_client.schemas.delete(schema_name)
            logger.info(f"Cleaned up schema: {schema_name}")
        except (NotFound, ResourceDoesNotExist):
            pass  # Already gone
        except Exception as e:
            logger.warning(f"Failed to cleanup schema {schema_name}: {e}")

    # Delete catalogs
    for catalog_name in reversed(resource_tracker.catalogs):
        try:
            workspace_client.catalogs.delete(catalog_name, force=True)
            logger.info(f"Cleaned up catalog: {catalog_name}")
        except (NotFound, ResourceDoesNotExist):
            pass  # Already gone
        except Exception as e:
            logger.warning(f"Failed to cleanup catalog {catalog_name}: {e}")

    # Delete groups
    for group_name in reversed(resource_tracker.groups):
        try:
            # Find group by name
            groups = list(workspace_client.groups.list(filter=f'displayName eq "{group_name}"'))
            if groups and groups[0].id:
                workspace_client.groups.delete(groups[0].id)
                logger.info(f"Cleaned up group: {group_name}")
        except (NotFound, ResourceDoesNotExist):
            pass
        except Exception as e:
            logger.warning(f"Failed to cleanup group {group_name}: {e}")

    # Delete service principals
    for spn_name in reversed(resource_tracker.service_principals):
        try:
            spns = list(workspace_client.service_principals.list(filter=f'displayName eq "{spn_name}"'))
            if spns and spns[0].id:
                workspace_client.service_principals.delete(spns[0].id)
                logger.info(f"Cleaned up service principal: {spn_name}")
        except (NotFound, ResourceDoesNotExist):
            pass
        except Exception as e:
            logger.warning(f"Failed to cleanup service principal {spn_name}: {e}")


# =============================================================================
# EXECUTOR FIXTURES
# =============================================================================


@pytest.fixture
def catalog_executor(workspace_client: WorkspaceClient) -> CatalogExecutor:
    """Fixture that provides a CatalogExecutor."""
    return CatalogExecutor(workspace_client)


@pytest.fixture
def schema_executor(workspace_client: WorkspaceClient) -> SchemaExecutor:
    """Fixture that provides a SchemaExecutor."""
    return SchemaExecutor(workspace_client)


@pytest.fixture
def group_executor(workspace_client: WorkspaceClient) -> GroupExecutor:
    """Fixture that provides a GroupExecutor."""
    return GroupExecutor(workspace_client)


@pytest.fixture
def service_principal_executor(workspace_client: WorkspaceClient) -> ServicePrincipalExecutor:
    """Fixture that provides a ServicePrincipalExecutor."""
    return ServicePrincipalExecutor(workspace_client)


@pytest.fixture
def grant_executor(workspace_client: WorkspaceClient) -> GrantExecutor:
    """Fixture that provides a GrantExecutor."""
    return GrantExecutor(workspace_client)


@pytest.fixture
def table_executor(workspace_client: WorkspaceClient) -> TableExecutor:
    """Fixture that provides a TableExecutor."""
    return TableExecutor(workspace_client)


@pytest.fixture
def volume_executor(workspace_client: WorkspaceClient) -> VolumeExecutor:
    """Fixture that provides a VolumeExecutor."""
    return VolumeExecutor(workspace_client)


@pytest.fixture
def vector_search_endpoint_executor(workspace_client: WorkspaceClient) -> VectorSearchEndpointExecutor:
    """Fixture that provides a VectorSearchEndpointExecutor."""
    return VectorSearchEndpointExecutor(workspace_client)


@pytest.fixture
def vector_search_index_executor(workspace_client: WorkspaceClient) -> VectorSearchIndexExecutor:
    """Fixture that provides a VectorSearchIndexExecutor."""
    return VectorSearchIndexExecutor(workspace_client)


# =============================================================================
# COMMON TEST FIXTURES
# =============================================================================


@pytest.fixture
def test_catalog(
    test_prefix: str,
    catalog_executor: CatalogExecutor,
    resource_tracker: ResourceTracker,
) -> Generator[Catalog, None, None]:
    """
    Fixture that creates a test catalog and cleans it up after.

    The catalog is created with a unique name based on test_prefix.
    """
    catalog = Catalog(name=f"{test_prefix}_catalog")
    result = catalog_executor.create(catalog)

    if not result.success:
        pytest.fail(f"Failed to create test catalog: {result.message}")

    resource_tracker.add_catalog(catalog.resolved_name)
    yield catalog


@pytest.fixture
def test_schema(
    test_catalog: Catalog,
    schema_executor: SchemaExecutor,
    resource_tracker: ResourceTracker,
) -> Generator[Schema, None, None]:
    """
    Fixture that creates a test schema within test_catalog.

    The schema is created with a unique name.
    """
    schema = Schema(name="test_schema", catalog_name=test_catalog.name)
    test_catalog.add_schema(schema)
    result = schema_executor.create(schema)

    if not result.success:
        pytest.fail(f"Failed to create test schema: {result.message}")

    resource_tracker.add_schema(schema.fqdn)
    yield schema
