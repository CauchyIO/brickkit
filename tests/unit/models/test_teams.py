"""
Unit tests for Team and AccessManager models.

Tests team workspace management and access orchestration.
"""

import pytest

from brickkit.models import AccessPolicy, Catalog, Principal
from brickkit.models.enums import Environment, IsolationMode, SecurableType
from brickkit.models.teams import AccessManager, Team
from brickkit.models.workspace_bindings import Workspace
from tests.fixtures import make_catalog, make_principal


def make_workspace(
    workspace_id: str = "123456",
    environment: Environment = Environment.DEV,
    name: str = "test_workspace",
    hostname: str = "test.cloud.databricks.com",
) -> Workspace:
    """Create a test Workspace."""
    return Workspace(workspace_id=workspace_id, environment=environment, name=name, hostname=hostname)


class TestTeamWorkspaceManagement:
    """Tests for Team workspace management."""

    def test_add_workspace(self) -> None:
        """Workspace can be added to team."""
        team = Team(name="test_team")
        workspace = make_workspace(workspace_id="123", environment=Environment.DEV)
        team.add_workspace(workspace)

        assert Environment.DEV in team.workspaces
        assert team.workspaces[Environment.DEV].workspace_id == "123"

    def test_add_workspace_duplicate_environment_raises(self) -> None:
        """Adding two workspaces for same environment raises ValueError."""
        team = Team(name="test_team")
        ws1 = make_workspace(workspace_id="123", environment=Environment.DEV)
        ws2 = make_workspace(workspace_id="456", environment=Environment.DEV)

        team.add_workspace(ws1)
        with pytest.raises(ValueError) as exc_info:
            team.add_workspace(ws2)
        assert "already has workspace" in str(exc_info.value)

    def test_get_workspace(self) -> None:
        """get_workspace returns correct workspace for environment."""
        team = Team(name="test_team")
        ws_dev = make_workspace(workspace_id="123", environment=Environment.DEV)
        ws_prd = make_workspace(workspace_id="456", environment=Environment.PRD)

        team.add_workspace(ws_dev)
        team.add_workspace(ws_prd)

        assert team.get_workspace(Environment.DEV) == ws_dev
        assert team.get_workspace(Environment.PRD) == ws_prd
        assert team.get_workspace(Environment.ACC) is None

    def test_workspace_ids_computed_property(self) -> None:
        """workspace_ids returns dict of environment -> workspace_id."""
        team = Team(name="test_team")
        team.add_workspace(make_workspace(workspace_id="123", environment=Environment.DEV))
        team.add_workspace(make_workspace(workspace_id="456", environment=Environment.PRD))

        ids = team.workspace_ids
        assert ids["dev"] == "123"
        assert ids["prd"] == "456"


class TestTeamPrincipalManagement:
    """Tests for Team principal management."""

    def test_add_principal(self) -> None:
        """Principal can be added to team."""
        team = Team(name="test_team")
        principal = Principal(name="data_team")
        team.add_principal(principal)

        assert len(team.principals) == 1
        assert team.principals[0].name == "data_team"

    def test_add_principal_duplicate_raises(self) -> None:
        """Adding duplicate principal raises ValueError."""
        team = Team(name="test_team")
        p1 = Principal(name="data_team")
        p2 = Principal(name="data_team")

        team.add_principal(p1)
        with pytest.raises(ValueError) as exc_info:
            team.add_principal(p2)
        assert "already exists" in str(exc_info.value)


class TestTeamCatalogBinding:
    """Tests for Team catalog workspace binding."""

    def test_add_catalog_sets_workspace_ids_for_isolated(self, dev_environment: None) -> None:
        """Adding ISOLATED catalog to team sets workspace_ids."""
        team = Team(name="test_team")
        team.add_workspace(make_workspace(workspace_id="123", environment=Environment.DEV))

        catalog = Catalog(name="test_catalog", isolation_mode=IsolationMode.ISOLATED, workspace_ids=[])
        team.add_catalog(catalog)

        assert 123 in catalog.workspace_ids

    def test_add_catalog_clears_workspace_ids_for_open(self, dev_environment: None) -> None:
        """Adding OPEN catalog to team keeps workspace_ids empty."""
        team = Team(name="test_team")
        team.add_workspace(make_workspace(workspace_id="123", environment=Environment.DEV))

        # OPEN catalogs cannot have workspace_ids by validation
        catalog = Catalog(name="test_catalog", isolation_mode=IsolationMode.OPEN)
        team.add_catalog(catalog)

        # Should remain empty (OPEN catalogs don't get workspace bindings)
        assert catalog.workspace_ids == []


class TestAccessManagerGrant:
    """Tests for AccessManager grant orchestration."""

    def test_grant_creates_privileges(self, dev_environment: None) -> None:
        """grant() creates privileges via securable.grant()."""
        manager = AccessManager(team_name="test_team")
        catalog = make_catalog(name="test")
        principal = make_principal(name="test_group")
        policy = AccessPolicy.READER()

        manager.grant(principal, catalog, policy)

        assert len(manager.grants) == 1
        assert manager.grants[0]["principal"] == principal.resolved_name
        assert manager.grants[0]["policy"] == "READER"
        assert len(catalog.privileges) > 0

    def test_grant_many(self, dev_environment: None) -> None:
        """grant_many grants to multiple securables."""
        manager = AccessManager(team_name="test_team")
        catalogs = [make_catalog(name=f"catalog_{i}") for i in range(3)]
        principal = make_principal(name="test_group")

        manager.grant_many(principal, catalogs, AccessPolicy.READER())

        assert len(manager.grants) == 3
        for catalog in catalogs:
            assert len(catalog.privileges) > 0

    def test_get_grants_for_principal(self, dev_environment: None) -> None:
        """get_grants_for_principal returns grants for specific principal."""
        manager = AccessManager(team_name="test_team")
        catalog = make_catalog(name="test")
        p1 = make_principal(name="group_a")
        p2 = make_principal(name="group_b")

        manager.grant(p1, catalog, AccessPolicy.READER())
        manager.grant(p2, catalog, AccessPolicy.WRITER())

        p1_grants = manager.get_grants_for_principal(p1.resolved_name)
        assert len(p1_grants) == 1
        assert p1_grants[0]["policy"] == "READER"

        p2_grants = manager.get_grants_for_principal(p2.resolved_name)
        assert len(p2_grants) == 1
        assert p2_grants[0]["policy"] == "WRITER"


class TestAccessManagerPrivileges:
    """Tests for AccessManager privilege tracking."""

    def test_privileges_property(self, dev_environment: None) -> None:
        """privileges property returns all created privileges."""
        manager = AccessManager(team_name="test_team")
        catalog = make_catalog(name="test")
        principal = make_principal(name="test_group")

        manager.grant(principal, catalog, AccessPolicy.READER())

        privileges = manager.privileges
        assert len(privileges) > 0
        assert all(p.securable_type == SecurableType.CATALOG for p in privileges)


class TestAccessManagerValidation:
    """Tests for AccessManager input validation."""

    def test_grant_invalid_principal_raises(self, dev_environment: None) -> None:
        """grant() with invalid principal type raises TypeError."""
        manager = AccessManager(team_name="test_team")
        catalog = make_catalog(name="test")

        with pytest.raises(TypeError) as exc_info:
            manager.grant("not_a_principal", catalog, AccessPolicy.READER())  # type: ignore
        assert "resolved_name" in str(exc_info.value)

    def test_grant_invalid_policy_raises(self, dev_environment: None) -> None:
        """grant() with invalid policy type raises TypeError."""
        manager = AccessManager(team_name="test_team")
        catalog = make_catalog(name="test")
        principal = make_principal(name="test_group")

        with pytest.raises(TypeError) as exc_info:
            manager.grant(principal, catalog, "READER")  # type: ignore
        assert "AccessPolicy" in str(exc_info.value)

    def test_grant_invalid_securable_raises(self, dev_environment: None) -> None:
        """grant() with invalid securable type raises TypeError."""
        manager = AccessManager(team_name="test_team")
        principal = make_principal(name="test_group")

        with pytest.raises(TypeError) as exc_info:
            manager.grant(principal, "not_a_securable", AccessPolicy.READER())  # type: ignore
        assert "grant()" in str(exc_info.value)
