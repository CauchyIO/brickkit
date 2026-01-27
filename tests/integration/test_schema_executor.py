"""
Integration tests for SchemaExecutor.

Tests schema CRUD operations against a real Databricks workspace.
"""

import pytest

from brickkit.executors import SchemaExecutor
from brickkit.executors.base import OperationType
from brickkit.models import Catalog, Schema


@pytest.mark.integration
class TestSchemaExecutorCreate:
    """Tests for schema creation."""

    def test_create_schema(
        self,
        test_catalog: Catalog,
        schema_executor: SchemaExecutor,
        resource_tracker,
    ) -> None:
        """SchemaExecutor can create a new schema."""
        schema = Schema(
            name="test_create_schema",
            catalog_name=test_catalog.name,
            comment="Integration test schema",
        )
        test_catalog.add_schema(schema)

        result = schema_executor.create(schema)
        resource_tracker.add_schema(schema.fqdn)

        assert result.success is True
        assert result.operation == OperationType.CREATE
        assert result.resource_type == "SCHEMA"

    def test_create_schema_idempotent(
        self,
        test_catalog: Catalog,
        schema_executor: SchemaExecutor,
        resource_tracker,
    ) -> None:
        """Creating same schema twice is idempotent."""
        schema = Schema(
            name="test_idempotent_schema",
            catalog_name=test_catalog.name,
            comment="Idempotent test",
        )
        test_catalog.add_schema(schema)

        result1 = schema_executor.create(schema)
        resource_tracker.add_schema(schema.fqdn)
        assert result1.success is True

        # Create again
        result2 = schema_executor.create(schema)
        assert result2.success is True
        assert result2.operation in (OperationType.NO_OP, OperationType.UPDATE, OperationType.SKIPPED)


@pytest.mark.integration
class TestSchemaExecutorExists:
    """Tests for schema existence checks."""

    def test_exists_true(
        self,
        test_schema: Schema,
        schema_executor: SchemaExecutor,
    ) -> None:
        """exists() returns True for existing schema."""
        assert schema_executor.exists(test_schema) is True

    def test_exists_false(
        self,
        test_catalog: Catalog,
        schema_executor: SchemaExecutor,
    ) -> None:
        """exists() returns False for non-existing schema."""
        schema = Schema(
            name="nonexistent_schema",
            catalog_name=test_catalog.name,
        )
        test_catalog.add_schema(schema)
        assert schema_executor.exists(schema) is False


@pytest.mark.integration
class TestSchemaExecutorUpdate:
    """Tests for schema updates."""

    def test_update_comment(
        self,
        test_catalog: Catalog,
        schema_executor: SchemaExecutor,
        resource_tracker,
    ) -> None:
        """Schema comment can be updated."""
        schema = Schema(
            name="test_update_schema",
            catalog_name=test_catalog.name,
            comment="Original comment",
        )
        test_catalog.add_schema(schema)
        schema_executor.create(schema)
        resource_tracker.add_schema(schema.fqdn)

        # Update the comment
        schema.comment = "Updated comment"
        result = schema_executor.update(schema)

        assert result.success is True
        # Could be UPDATE or NO_OP depending on implementation
        assert result.operation in (OperationType.UPDATE, OperationType.NO_OP)


@pytest.mark.integration
class TestSchemaExecutorDelete:
    """Tests for schema deletion."""

    def test_delete_schema(
        self,
        test_catalog: Catalog,
        schema_executor: SchemaExecutor,
    ) -> None:
        """SchemaExecutor can delete a schema."""
        schema = Schema(
            name="test_delete_schema",
            catalog_name=test_catalog.name,
        )
        test_catalog.add_schema(schema)
        schema_executor.create(schema)

        result = schema_executor.delete(schema)

        assert result.success is True
        assert result.operation == OperationType.DELETE
        assert schema_executor.exists(schema) is False

    def test_delete_nonexistent(
        self,
        test_catalog: Catalog,
        schema_executor: SchemaExecutor,
    ) -> None:
        """Deleting non-existent schema returns NO_OP."""
        schema = Schema(
            name="never_existed_schema",
            catalog_name=test_catalog.name,
        )
        test_catalog.add_schema(schema)

        result = schema_executor.delete(schema)

        assert result.success is True
        assert result.operation == OperationType.NO_OP


@pytest.mark.integration
class TestSchemaExecutorDryRun:
    """Tests for dry-run mode."""

    def test_dry_run_create(
        self,
        test_catalog: Catalog,
        workspace_client,
    ) -> None:
        """Dry run create doesn't actually create schema."""
        executor = SchemaExecutor(workspace_client, dry_run=True)
        schema = Schema(
            name="dry_run_schema",
            catalog_name=test_catalog.name,
        )
        test_catalog.add_schema(schema)

        result = executor.create(schema)

        assert result.success is True
        assert "dry run" in result.message.lower()
        # Verify schema wasn't actually created
        assert executor.exists(schema) is False
