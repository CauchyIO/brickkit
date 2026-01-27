"""
Integration tests for GrantExecutor.

Tests privilege granting/revoking against real Databricks Unity Catalog.
"""

from databricks.sdk import WorkspaceClient

from brickkit.executors import CatalogExecutor, GrantExecutor
from brickkit.models import AccessPolicy, Catalog, Principal, Privilege
from brickkit.models.enums import PrivilegeType, SecurableType
from tests.integration.conftest import ResourceTracker


class TestGrantExecutorBasics:
    """Tests for basic GrantExecutor functionality."""

    def test_grant_privilege_to_user(
        self,
        workspace_client: WorkspaceClient,
        grant_executor: GrantExecutor,
        catalog_executor: CatalogExecutor,
        resource_tracker: ResourceTracker,
        test_prefix: str,
    ) -> None:
        """Grant privilege to a user principal."""
        # Create test catalog
        catalog = Catalog(name=f"{test_prefix}_grant_user")
        result = catalog_executor.create(catalog)
        assert result.success, f"Failed to create catalog: {result.message}"
        resource_tracker.add_catalog(catalog.resolved_name)

        # Get current user
        current_user = workspace_client.current_user.me()
        assert current_user.user_name is not None

        # Create privilege for current user
        priv = Privilege(
            level_1=catalog.resolved_name,
            securable_type=SecurableType.CATALOG,
            principal=current_user.user_name,
            privilege_type=PrivilegeType.USE_CATALOG,
        )

        # Grant the privilege
        result = grant_executor.grant(priv)
        assert result.success, f"Failed to grant privilege: {result.message}"

    def test_grant_privilege_to_group(
        self,
        workspace_client: WorkspaceClient,
        grant_executor: GrantExecutor,
        catalog_executor: CatalogExecutor,
        resource_tracker: ResourceTracker,
        test_prefix: str,
    ) -> None:
        """Grant privilege to a group principal."""
        # Create test catalog
        catalog = Catalog(name=f"{test_prefix}_grant_group")
        result = catalog_executor.create(catalog)
        assert result.success, f"Failed to create catalog: {result.message}"
        resource_tracker.add_catalog(catalog.resolved_name)

        # Use built-in 'users' group
        priv = Privilege(
            level_1=catalog.resolved_name,
            securable_type=SecurableType.CATALOG,
            principal="users",
            privilege_type=PrivilegeType.BROWSE,
        )

        # Grant the privilege
        result = grant_executor.grant(priv)
        assert result.success, f"Failed to grant privilege: {result.message}"

    def test_revoke_privilege(
        self,
        workspace_client: WorkspaceClient,
        grant_executor: GrantExecutor,
        catalog_executor: CatalogExecutor,
        resource_tracker: ResourceTracker,
        test_prefix: str,
    ) -> None:
        """Revoke a previously granted privilege."""
        # Create test catalog
        catalog = Catalog(name=f"{test_prefix}_revoke")
        result = catalog_executor.create(catalog)
        assert result.success, f"Failed to create catalog: {result.message}"
        resource_tracker.add_catalog(catalog.resolved_name)

        # Grant privilege first
        priv = Privilege(
            level_1=catalog.resolved_name,
            securable_type=SecurableType.CATALOG,
            principal="users",
            privilege_type=PrivilegeType.BROWSE,
        )
        grant_result = grant_executor.grant(priv)
        assert grant_result.success

        # Revoke the privilege
        revoke_result = grant_executor.revoke(priv)
        assert revoke_result.success, f"Failed to revoke privilege: {revoke_result.message}"


class TestGrantExecutorDryRun:
    """Tests for GrantExecutor dry run mode."""

    def test_dry_run_logs_but_does_not_grant(
        self,
        workspace_client: WorkspaceClient,
        catalog_executor: CatalogExecutor,
        resource_tracker: ResourceTracker,
        test_prefix: str,
    ) -> None:
        """Dry run mode logs intent but doesn't actually grant."""
        # Create test catalog
        catalog = Catalog(name=f"{test_prefix}_dry_run")
        result = catalog_executor.create(catalog)
        assert result.success, f"Failed to create catalog: {result.message}"
        resource_tracker.add_catalog(catalog.resolved_name)

        # Create dry run executor
        dry_run_executor = GrantExecutor(workspace_client, dry_run=True)

        priv = Privilege(
            level_1=catalog.resolved_name,
            securable_type=SecurableType.CATALOG,
            principal="users",
            privilege_type=PrivilegeType.BROWSE,
        )

        # Dry run grant
        result = dry_run_executor.grant(priv)
        assert result.success
        assert "dry run" in result.message.lower()


class TestGrantExecutorWithPolicy:
    """Tests for granting via AccessPolicy."""

    def test_grant_reader_policy(
        self,
        workspace_client: WorkspaceClient,
        grant_executor: GrantExecutor,
        catalog_executor: CatalogExecutor,
        resource_tracker: ResourceTracker,
        test_prefix: str,
    ) -> None:
        """Grant READER policy privileges to a catalog."""
        # Create test catalog
        catalog = Catalog(name=f"{test_prefix}_reader_policy")
        result = catalog_executor.create(catalog)
        assert result.success, f"Failed to create catalog: {result.message}"
        resource_tracker.add_catalog(catalog.resolved_name)

        # Grant READER policy
        principal = Principal(name="users", add_environment_suffix=False)
        privileges = catalog.grant(principal, AccessPolicy.READER())

        # Apply privileges via executor
        for priv in privileges:
            result = grant_executor.grant(priv)
            assert result.success, f"Failed to grant {priv.privilege_type}: {result.message}"

    def test_grant_multiple_policies(
        self,
        workspace_client: WorkspaceClient,
        grant_executor: GrantExecutor,
        catalog_executor: CatalogExecutor,
        resource_tracker: ResourceTracker,
        test_prefix: str,
    ) -> None:
        """Grant different policies to different principals."""
        # Create test catalog
        catalog = Catalog(name=f"{test_prefix}_multi_policy")
        result = catalog_executor.create(catalog)
        assert result.success, f"Failed to create catalog: {result.message}"
        resource_tracker.add_catalog(catalog.resolved_name)

        # Grant READER to users
        users = Principal(name="users", add_environment_suffix=False)
        reader_privs = catalog.grant(users, AccessPolicy.READER())

        # Grant ADMIN to admins
        admins = Principal(name="admins", add_environment_suffix=False)
        admin_privs = catalog.grant(admins, AccessPolicy.ADMIN())

        # Apply all privileges
        for priv in reader_privs + admin_privs:
            result = grant_executor.grant(priv)
            assert result.success, f"Failed to grant {priv.privilege_type}: {result.message}"
