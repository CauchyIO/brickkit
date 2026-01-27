"""
Integration tests for GroupExecutor.

Tests group CRUD operations against a real Databricks workspace.
"""

import pytest

from brickkit.executors import GroupExecutor
from brickkit.executors.base import OperationType
from brickkit.models import ManagedGroup


@pytest.mark.integration
class TestGroupExecutorCreate:
    """Tests for group creation."""

    def test_create_group(
        self,
        test_prefix: str,
        group_executor: GroupExecutor,
        resource_tracker,
    ) -> None:
        """GroupExecutor can create a new group."""
        group = ManagedGroup(
            name=f"{test_prefix}_grp_create",
            entitlements=["workspace-access"],
        )

        result = group_executor.create(group)
        resource_tracker.add_group(group.resolved_name)

        assert result.success is True
        assert result.operation in (OperationType.CREATE, OperationType.SKIPPED)

    def test_create_group_idempotent(
        self,
        test_prefix: str,
        group_executor: GroupExecutor,
        resource_tracker,
    ) -> None:
        """Creating same group twice is idempotent."""
        group = ManagedGroup(
            name=f"{test_prefix}_grp_idempotent",
        )

        result1 = group_executor.create(group)
        resource_tracker.add_group(group.resolved_name)
        assert result1.success is True

        # Create again
        result2 = group_executor.create(group)
        assert result2.success is True
        assert result2.operation == OperationType.SKIPPED


@pytest.mark.integration
class TestGroupExecutorDelete:
    """Tests for group deletion."""

    def test_delete_group(
        self,
        test_prefix: str,
        group_executor: GroupExecutor,
    ) -> None:
        """GroupExecutor can delete a group."""
        group = ManagedGroup(
            name=f"{test_prefix}_grp_delete",
        )
        create_result = group_executor.create(group)
        assert create_result.success is True

        result = group_executor.delete(group)

        assert result.success is True
        # Could be DELETE (freshly created) or SKIPPED (already cleaned)
        assert result.operation in (OperationType.DELETE, OperationType.SKIPPED)

    def test_delete_nonexistent(
        self,
        test_prefix: str,
        group_executor: GroupExecutor,
    ) -> None:
        """Deleting non-existent group returns SKIPPED."""
        group = ManagedGroup(
            name=f"{test_prefix}_grp_nonexistent",
        )

        result = group_executor.delete(group)

        assert result.success is True
        assert result.operation == OperationType.SKIPPED


@pytest.mark.integration
class TestGroupExecutorMembership:
    """Tests for group membership management."""

    def test_create_group_with_entitlements(
        self,
        test_prefix: str,
        group_executor: GroupExecutor,
        resource_tracker,
    ) -> None:
        """Groups can be created with entitlements."""
        group = ManagedGroup(name=f"{test_prefix}_grp_entitled")
        group.add_entitlement("workspace-access")

        result = group_executor.create(group)
        resource_tracker.add_group(group.resolved_name)

        assert result.success is True


@pytest.mark.integration
class TestGroupExecutorDryRun:
    """Tests for dry-run mode."""

    def test_dry_run_create(
        self,
        test_prefix: str,
        workspace_client,
    ) -> None:
        """Dry run create doesn't actually create group."""
        executor = GroupExecutor(workspace_client, dry_run=True)
        group = ManagedGroup(
            name=f"{test_prefix}_grp_dry_run",
        )

        result = executor.create(group)

        assert result.success is True
        assert "dry run" in result.message.lower()
