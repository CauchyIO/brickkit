"""
Integration tests for ServicePrincipalExecutor.

Tests service principal CRUD operations against a real Databricks workspace.
"""

import pytest

from brickkit.executors import ServicePrincipalExecutor
from brickkit.executors.base import OperationType
from brickkit.models import ManagedServicePrincipal


@pytest.mark.integration
class TestServicePrincipalExecutorCreate:
    """Tests for service principal creation."""

    def test_create_service_principal(
        self,
        test_prefix: str,
        service_principal_executor: ServicePrincipalExecutor,
        resource_tracker,
    ) -> None:
        """ServicePrincipalExecutor can create a new SPN."""
        spn = ManagedServicePrincipal(
            name=f"{test_prefix}_spn_create",
            entitlements=["workspace-access"],
        )

        result = service_principal_executor.create(spn)
        resource_tracker.add_service_principal(spn.resolved_name)

        assert result.success is True
        assert result.operation in (OperationType.CREATE, OperationType.SKIPPED)
        assert spn.application_id is not None

    def test_create_service_principal_idempotent(
        self,
        test_prefix: str,
        service_principal_executor: ServicePrincipalExecutor,
        resource_tracker,
    ) -> None:
        """Creating same SPN twice is idempotent."""
        spn = ManagedServicePrincipal(
            name=f"{test_prefix}_spn_idempotent",
        )

        result1 = service_principal_executor.create(spn)
        resource_tracker.add_service_principal(spn.resolved_name)
        assert result1.success is True

        # Create again
        result2 = service_principal_executor.create(spn)
        assert result2.success is True
        assert result2.operation == OperationType.SKIPPED


@pytest.mark.integration
class TestServicePrincipalExecutorEntitlements:
    """Tests for entitlement sync."""

    def test_sync_entitlements_add(
        self,
        test_prefix: str,
        service_principal_executor: ServicePrincipalExecutor,
        resource_tracker,
    ) -> None:
        """Entitlements can be added to SPN."""
        spn = ManagedServicePrincipal(
            name=f"{test_prefix}_spn_ent_add",
        )
        service_principal_executor.create(spn)
        resource_tracker.add_service_principal(spn.resolved_name)

        # Add entitlement
        spn.add_entitlement("workspace-access")
        result = service_principal_executor.sync_entitlements(spn)

        assert result.success is True
        if result.operation == OperationType.UPDATE:
            assert "entitlements_added" in result.changes

    def test_sync_entitlements_already_synced(
        self,
        test_prefix: str,
        service_principal_executor: ServicePrincipalExecutor,
        resource_tracker,
    ) -> None:
        """Sync with no changes returns SKIPPED."""
        spn = ManagedServicePrincipal(
            name=f"{test_prefix}_spn_ent_sync",
            entitlements=["workspace-access"],
        )
        service_principal_executor.create(spn)
        resource_tracker.add_service_principal(spn.resolved_name)

        # Sync again with same entitlements
        result = service_principal_executor.sync_entitlements(spn)

        assert result.success is True
        assert result.operation == OperationType.SKIPPED


@pytest.mark.integration
class TestServicePrincipalExecutorDelete:
    """Tests for service principal deletion."""

    def test_delete_service_principal(
        self,
        test_prefix: str,
        service_principal_executor: ServicePrincipalExecutor,
    ) -> None:
        """ServicePrincipalExecutor can delete an SPN."""
        spn = ManagedServicePrincipal(
            name=f"{test_prefix}_spn_delete",
        )
        service_principal_executor.create(spn)

        result = service_principal_executor.delete(spn)

        assert result.success is True
        assert result.operation == OperationType.DELETE

    def test_delete_nonexistent(
        self,
        test_prefix: str,
        service_principal_executor: ServicePrincipalExecutor,
    ) -> None:
        """Deleting non-existent SPN returns SKIPPED."""
        spn = ManagedServicePrincipal(
            name=f"{test_prefix}_spn_nonexistent",
        )

        result = service_principal_executor.delete(spn)

        assert result.success is True
        assert result.operation == OperationType.SKIPPED


@pytest.mark.integration
class TestServicePrincipalExecutorDryRun:
    """Tests for dry-run mode."""

    def test_dry_run_create(
        self,
        test_prefix: str,
        workspace_client,
    ) -> None:
        """Dry run create doesn't actually create SPN."""
        executor = ServicePrincipalExecutor(workspace_client, dry_run=True)
        spn = ManagedServicePrincipal(
            name=f"{test_prefix}_spn_dry_run",
        )

        result = executor.create(spn)

        assert result.success is True
        assert "dry run" in result.message.lower()
        # SPN shouldn't have application_id since it wasn't really created
        assert spn.application_id is None


@pytest.mark.integration
class TestServicePrincipalToPrincipal:
    """Tests for converting SPN to Principal for grants."""

    def test_to_principal_after_create(
        self,
        test_prefix: str,
        service_principal_executor: ServicePrincipalExecutor,
        resource_tracker,
    ) -> None:
        """to_principal() works after SPN is created."""
        spn = ManagedServicePrincipal(
            name=f"{test_prefix}_spn_to_principal",
        )
        service_principal_executor.create(spn)
        resource_tracker.add_service_principal(spn.resolved_name)

        principal = spn.to_principal()

        assert principal is not None
        assert principal.application_id == spn.application_id
        # Principal's resolved_name should be the application_id
        assert principal.resolved_name == spn.application_id
