"""
Integration tests for TableExecutor.

Tests table creation, update, and deletion against real Databricks Unity Catalog.
"""

from databricks.sdk import WorkspaceClient

from brickkit.executors import CatalogExecutor, SchemaExecutor, TableExecutor
from brickkit.models import Catalog, Schema, Table
from brickkit.models.enums import TableType
from brickkit.models.tables import ColumnInfo
from tests.integration.conftest import ResourceTracker


class TestTableExecutorCreate:
    """Tests for TableExecutor.create()."""

    def test_create_managed_table(
        self,
        workspace_client: WorkspaceClient,
        catalog_executor: CatalogExecutor,
        schema_executor: SchemaExecutor,
        table_executor: TableExecutor,
        resource_tracker: ResourceTracker,
        test_prefix: str,
    ) -> None:
        """Create a managed table."""
        # Create catalog and schema first
        catalog = Catalog(name=f"{test_prefix}_table_create")
        catalog_result = catalog_executor.create(catalog)
        assert catalog_result.success, f"Failed to create catalog: {catalog_result.message}"
        resource_tracker.add_catalog(catalog.resolved_name)

        schema = Schema(name="test_schema", catalog_name=catalog.name)
        catalog.add_schema(schema)
        schema_result = schema_executor.create(schema)
        assert schema_result.success, f"Failed to create schema: {schema_result.message}"
        resource_tracker.add_schema(schema.fqdn)

        # Create table
        table = Table(
            name="test_table",
            catalog_name=catalog.name,
            schema_name="test_schema",
            table_type=TableType.MANAGED,
            columns=[
                ColumnInfo(name="id", type_name="BIGINT", nullable=False),
                ColumnInfo(name="name", type_name="STRING", nullable=True),
            ],
            comment="Integration test table",
        )
        schema.add_table(table)
        result = table_executor.create(table)
        assert result.success, f"Failed to create table: {result.message}"
        resource_tracker.add_table(table.fqdn)

    def test_create_idempotent(
        self,
        workspace_client: WorkspaceClient,
        catalog_executor: CatalogExecutor,
        schema_executor: SchemaExecutor,
        table_executor: TableExecutor,
        resource_tracker: ResourceTracker,
        test_prefix: str,
    ) -> None:
        """Creating same table twice should be idempotent."""
        # Create catalog and schema first
        catalog = Catalog(name=f"{test_prefix}_table_idemp")
        catalog_result = catalog_executor.create(catalog)
        assert catalog_result.success
        resource_tracker.add_catalog(catalog.resolved_name)

        schema = Schema(name="test_schema", catalog_name=catalog.name)
        catalog.add_schema(schema)
        schema_result = schema_executor.create(schema)
        assert schema_result.success
        resource_tracker.add_schema(schema.fqdn)

        # Create table first time
        table = Table(
            name="idempotent_table",
            catalog_name=catalog.name,
            schema_name="test_schema",
            table_type=TableType.MANAGED,
            columns=[ColumnInfo(name="id", type_name="BIGINT")],
        )
        schema.add_table(table)
        result1 = table_executor.create(table)
        assert result1.success
        resource_tracker.add_table(table.fqdn)

        # Create same table again (should be NO_OP or still success)
        result2 = table_executor.create(table)
        assert result2.success


class TestTableExecutorExists:
    """Tests for TableExecutor.exists()."""

    def test_exists_true_when_present(
        self,
        workspace_client: WorkspaceClient,
        catalog_executor: CatalogExecutor,
        schema_executor: SchemaExecutor,
        table_executor: TableExecutor,
        resource_tracker: ResourceTracker,
        test_prefix: str,
    ) -> None:
        """exists() returns True when table is present."""
        # Create catalog, schema, and table
        catalog = Catalog(name=f"{test_prefix}_table_exists")
        catalog_result = catalog_executor.create(catalog)
        assert catalog_result.success
        resource_tracker.add_catalog(catalog.resolved_name)

        schema = Schema(name="test_schema", catalog_name=catalog.name)
        catalog.add_schema(schema)
        schema_result = schema_executor.create(schema)
        assert schema_result.success
        resource_tracker.add_schema(schema.fqdn)

        table = Table(
            name="exists_test",
            catalog_name=catalog.name,
            schema_name="test_schema",
            table_type=TableType.MANAGED,
            columns=[ColumnInfo(name="id", type_name="BIGINT")],
        )
        schema.add_table(table)
        create_result = table_executor.create(table)
        assert create_result.success
        resource_tracker.add_table(table.fqdn)

        # Now check exists
        assert table_executor.exists(table) is True

    def test_exists_false_when_missing(
        self,
        workspace_client: WorkspaceClient,
        catalog_executor: CatalogExecutor,
        schema_executor: SchemaExecutor,
        table_executor: TableExecutor,
        resource_tracker: ResourceTracker,
        test_prefix: str,
    ) -> None:
        """exists() returns False when table doesn't exist."""
        # Create catalog and schema
        catalog = Catalog(name=f"{test_prefix}_table_missing")
        catalog_result = catalog_executor.create(catalog)
        assert catalog_result.success
        resource_tracker.add_catalog(catalog.resolved_name)

        schema = Schema(name="test_schema", catalog_name=catalog.name)
        catalog.add_schema(schema)
        schema_result = schema_executor.create(schema)
        assert schema_result.success
        resource_tracker.add_schema(schema.fqdn)

        # Check for non-existent table
        table = Table(
            name="nonexistent_table",
            catalog_name=catalog.name,
            schema_name="test_schema",
            table_type=TableType.MANAGED,
            columns=[ColumnInfo(name="id", type_name="BIGINT")],
        )
        schema.add_table(table)

        assert table_executor.exists(table) is False


class TestTableExecutorDelete:
    """Tests for TableExecutor.delete()."""

    def test_delete_removes_table(
        self,
        workspace_client: WorkspaceClient,
        catalog_executor: CatalogExecutor,
        schema_executor: SchemaExecutor,
        table_executor: TableExecutor,
        resource_tracker: ResourceTracker,
        test_prefix: str,
    ) -> None:
        """delete() removes the table."""
        # Create catalog, schema, and table
        catalog = Catalog(name=f"{test_prefix}_table_delete")
        catalog_result = catalog_executor.create(catalog)
        assert catalog_result.success
        resource_tracker.add_catalog(catalog.resolved_name)

        schema = Schema(name="test_schema", catalog_name=catalog.name)
        catalog.add_schema(schema)
        schema_result = schema_executor.create(schema)
        assert schema_result.success
        resource_tracker.add_schema(schema.fqdn)

        table = Table(
            name="delete_test",
            catalog_name=catalog.name,
            schema_name="test_schema",
            table_type=TableType.MANAGED,
            columns=[ColumnInfo(name="id", type_name="BIGINT")],
        )
        schema.add_table(table)
        create_result = table_executor.create(table)
        assert create_result.success
        # Don't add to tracker - we're deleting it

        # Delete the table
        delete_result = table_executor.delete(table)
        assert delete_result.success, f"Failed to delete table: {delete_result.message}"

        # Verify it's gone
        assert table_executor.exists(table) is False


class TestTableExecutorDryRun:
    """Tests for TableExecutor dry run mode."""

    def test_dry_run_does_not_create(
        self,
        workspace_client: WorkspaceClient,
        catalog_executor: CatalogExecutor,
        schema_executor: SchemaExecutor,
        resource_tracker: ResourceTracker,
        test_prefix: str,
    ) -> None:
        """Dry run mode doesn't actually create the table."""
        # Create catalog and schema
        catalog = Catalog(name=f"{test_prefix}_table_dry")
        catalog_result = catalog_executor.create(catalog)
        assert catalog_result.success
        resource_tracker.add_catalog(catalog.resolved_name)

        schema = Schema(name="test_schema", catalog_name=catalog.name)
        catalog.add_schema(schema)
        schema_result = schema_executor.create(schema)
        assert schema_result.success
        resource_tracker.add_schema(schema.fqdn)

        # Create dry run executor
        dry_run_executor = TableExecutor(workspace_client, dry_run=True)

        table = Table(
            name="dry_run_table",
            catalog_name=catalog.name,
            schema_name="test_schema",
            table_type=TableType.MANAGED,
            columns=[ColumnInfo(name="id", type_name="BIGINT")],
        )
        schema.add_table(table)

        # Dry run create
        result = dry_run_executor.create(table)
        assert result.success
        assert "dry run" in result.message.lower()

        # Verify table was NOT actually created
        real_executor = TableExecutor(workspace_client)
        assert real_executor.exists(table) is False
