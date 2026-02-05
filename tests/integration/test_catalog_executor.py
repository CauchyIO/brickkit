"""
Integration tests for CatalogExecutor.

Tests catalog CRUD operations against a real Databricks workspace.
"""

import pytest

from brickkit.executors import CatalogExecutor
from brickkit.executors.base import OperationType
from brickkit.models import Catalog, Tag


@pytest.mark.integration
class TestCatalogExecutorCreate:
    """Tests for catalog creation."""

    def test_create_catalog(
        self,
        test_prefix: str,
        catalog_executor: CatalogExecutor,
        resource_tracker,
    ) -> None:
        """CatalogExecutor can create a new catalog."""
        catalog = Catalog(
            name=f"{test_prefix}_create_test",
            comment="Integration test catalog",
        )

        result = catalog_executor.create(catalog)
        resource_tracker.add_catalog(catalog.resolved_name)

        assert result.success is True
        assert result.operation == OperationType.CREATE
        assert result.resource_type == "CATALOG"

    def test_create_catalog_idempotent(
        self,
        test_prefix: str,
        catalog_executor: CatalogExecutor,
        resource_tracker,
    ) -> None:
        """Creating same catalog twice is idempotent."""
        catalog = Catalog(
            name=f"{test_prefix}_idempotent",
            comment="First creation",
        )

        result1 = catalog_executor.create(catalog)
        resource_tracker.add_catalog(catalog.resolved_name)
        assert result1.success is True

        # Create again - should be idempotent (NO_OP or UPDATE)
        result2 = catalog_executor.create(catalog)
        assert result2.success is True
        assert result2.operation in (OperationType.NO_OP, OperationType.UPDATE)

    def test_create_catalog_with_tags(
        self,
        test_prefix: str,
        catalog_executor: CatalogExecutor,
        resource_tracker,
    ) -> None:
        """Catalog can be created with tags."""
        catalog = Catalog(
            name=f"{test_prefix}_tagged",
            comment="Catalog with tags",
            tags=[
                Tag(key="environment", value="test"),
                Tag(key="cost_center", value="integration_tests"),
            ],
        )

        result = catalog_executor.create(catalog)
        resource_tracker.add_catalog(catalog.resolved_name)

        assert result.success is True


@pytest.mark.integration
class TestCatalogExecutorExists:
    """Tests for catalog existence checks."""

    def test_exists_true(
        self,
        test_prefix: str,
        catalog_executor: CatalogExecutor,
        resource_tracker,
    ) -> None:
        """exists() returns True for existing catalog."""
        catalog = Catalog(name=f"{test_prefix}_exists_true")
        catalog_executor.create(catalog)
        resource_tracker.add_catalog(catalog.resolved_name)

        assert catalog_executor.exists(catalog) is True

    def test_exists_false(
        self,
        test_prefix: str,
        catalog_executor: CatalogExecutor,
    ) -> None:
        """exists() returns False for non-existing catalog."""
        catalog = Catalog(name=f"{test_prefix}_nonexistent")
        assert catalog_executor.exists(catalog) is False


@pytest.mark.integration
class TestCatalogExecutorUpdate:
    """Tests for catalog updates."""

    def test_update_comment(
        self,
        test_prefix: str,
        catalog_executor: CatalogExecutor,
        resource_tracker,
    ) -> None:
        """Catalog comment can be updated."""
        catalog = Catalog(
            name=f"{test_prefix}_update_comment",
            comment="Original comment",
        )
        catalog_executor.create(catalog)
        resource_tracker.add_catalog(catalog.resolved_name)

        # Update the comment
        catalog.comment = "Updated comment"
        result = catalog_executor.update(catalog)

        assert result.success is True
        assert result.operation == OperationType.UPDATE
        assert "comment" in result.changes

    def test_update_no_changes(
        self,
        test_prefix: str,
        catalog_executor: CatalogExecutor,
        resource_tracker,
    ) -> None:
        """Update with no changes returns NO_OP."""
        catalog = Catalog(
            name=f"{test_prefix}_no_changes",
            comment="Static comment",
        )
        catalog_executor.create(catalog)
        resource_tracker.add_catalog(catalog.resolved_name)

        # Update without changes
        result = catalog_executor.update(catalog)

        assert result.success is True
        assert result.operation == OperationType.NO_OP


@pytest.mark.integration
class TestCatalogExecutorDelete:
    """Tests for catalog deletion."""

    def test_delete_catalog(
        self,
        test_prefix: str,
        catalog_executor: CatalogExecutor,
    ) -> None:
        """CatalogExecutor can delete a catalog."""
        catalog = Catalog(name=f"{test_prefix}_delete_me")
        catalog_executor.create(catalog)

        result = catalog_executor.delete(catalog)

        assert result.success is True
        assert result.operation == OperationType.DELETE
        assert catalog_executor.exists(catalog) is False

    def test_delete_nonexistent(
        self,
        test_prefix: str,
        catalog_executor: CatalogExecutor,
    ) -> None:
        """Deleting non-existent catalog returns NO_OP."""
        catalog = Catalog(name=f"{test_prefix}_never_existed")

        result = catalog_executor.delete(catalog)

        assert result.success is True
        assert result.operation == OperationType.NO_OP


@pytest.mark.integration
class TestCatalogExecutorDryRun:
    """Tests for dry-run mode."""

    def test_dry_run_create(
        self,
        test_prefix: str,
        workspace_client,
    ) -> None:
        """Dry run create doesn't actually create catalog."""
        executor = CatalogExecutor(workspace_client, dry_run=True)
        catalog = Catalog(name=f"{test_prefix}_dry_run")

        result = executor.create(catalog)

        assert result.success is True
        assert "dry run" in result.message.lower()
        # Verify catalog wasn't actually created
        assert executor.exists(catalog) is False

    def test_dry_run_delete(
        self,
        test_prefix: str,
        catalog_executor: CatalogExecutor,
        workspace_client,
        resource_tracker,
    ) -> None:
        """Dry run delete doesn't actually delete catalog."""
        # First create a real catalog
        catalog = Catalog(name=f"{test_prefix}_dry_delete")
        catalog_executor.create(catalog)
        resource_tracker.add_catalog(catalog.resolved_name)

        # Try to delete in dry-run mode
        dry_executor = CatalogExecutor(workspace_client, dry_run=True)
        result = dry_executor.delete(catalog)

        assert result.success is True
        assert "dry run" in result.message.lower()
        # Verify catalog still exists
        assert catalog_executor.exists(catalog) is True
