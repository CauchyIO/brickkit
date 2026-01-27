"""
Unit tests for Principal models.

Tests Principal, ManagedServicePrincipal, and ManagedGroup classes.
"""

import pytest

from brickkit.models import ManagedGroup, ManagedServicePrincipal, Principal
from brickkit.models.enums import Environment, PrincipalType, WorkspaceEntitlement
from brickkit.models.principals import MemberReference
from tests.fixtures import make_group, make_service_principal


class TestPrincipalNaming:
    """Tests for Principal naming and environment suffixes."""

    def test_principal_resolved_name_dev(self, dev_environment: None) -> None:
        """Principal resolved_name includes env suffix in DEV."""
        principal = Principal(name="data_team")
        assert principal.resolved_name == "data_team_dev"

    def test_principal_resolved_name_no_suffix(self, dev_environment: None) -> None:
        """Principal can disable environment suffix."""
        principal = Principal(name="data_team", add_environment_suffix=False)
        assert principal.resolved_name == "data_team"

    def test_principal_custom_environment_mapping(self, prd_environment: None) -> None:
        """Principal can have custom per-environment names."""
        principal = Principal(
            name="data_team",
            environment_mapping={
                Environment.DEV: "dev_data_team",
                Environment.PRD: "prod_data_team",
            },
        )
        assert principal.resolved_name == "prod_data_team"

    def test_principal_type_methods(self) -> None:
        """Principal type helper methods work correctly."""
        user = Principal(name="alice@example.com", principal_type=PrincipalType.USER)
        group = Principal(name="data_team", principal_type=PrincipalType.GROUP)
        spn = Principal(name="spn_etl", principal_type=PrincipalType.SERVICE_PRINCIPAL)

        assert user.is_user() is True
        assert user.is_group() is False
        assert user.is_service_principal() is False

        assert group.is_group() is True
        assert spn.is_service_principal() is True


class TestSpecialPrincipals:
    """Tests for special built-in principals."""

    def test_users_principal(self) -> None:
        """'users' principal never gets environment suffix."""
        principal = Principal(name="users")
        assert principal.add_environment_suffix is False
        assert principal.resolved_name == "users"

    def test_admins_principal(self) -> None:
        """'admins' principal never gets environment suffix."""
        principal = Principal(name="admins")
        assert principal.add_environment_suffix is False
        assert principal.resolved_name == "admins"

    def test_account_users_principal(self) -> None:
        """'account users' principal never gets environment suffix."""
        principal = Principal(name="account users")
        assert principal.add_environment_suffix is False
        assert principal.resolved_name == "account users"

    def test_all_workspace_users_factory(self) -> None:
        """Factory method creates correct 'users' principal."""
        principal = Principal.all_workspace_users()
        assert principal.name == "users"
        assert principal.principal_type == PrincipalType.GROUP
        assert principal.resolved_name == "users"

    def test_workspace_admins_factory(self) -> None:
        """Factory method creates correct 'admins' principal."""
        principal = Principal.workspace_admins()
        assert principal.name == "admins"
        assert principal.principal_type == PrincipalType.GROUP
        assert principal.resolved_name == "admins"

    def test_all_account_users_factory(self) -> None:
        """Factory method creates correct 'account users' principal."""
        principal = Principal.all_account_users()
        assert principal.name == "account users"
        assert principal.principal_type == PrincipalType.GROUP
        assert principal.resolved_name == "account users"


class TestServicePrincipalGrants:
    """Tests for service principal grant behavior."""

    def test_spn_with_application_id(self, dev_environment: None) -> None:
        """SPN with application_id uses it for resolved_name."""
        principal = Principal(
            name="spn_etl",
            principal_type=PrincipalType.SERVICE_PRINCIPAL,
            application_id="12345678-1234-1234-1234-123456789012",
        )
        assert principal.resolved_name == "12345678-1234-1234-1234-123456789012"

    def test_spn_display_name(self, dev_environment: None) -> None:
        """SPN display_name uses the regular naming (not application_id)."""
        principal = Principal(
            name="spn_etl",
            principal_type=PrincipalType.SERVICE_PRINCIPAL,
            application_id="12345678-1234-1234-1234-123456789012",
        )
        assert principal.display_name == "spn_etl_dev"

    def test_spn_without_application_id(self, dev_environment: None) -> None:
        """SPN without application_id uses display name for resolved_name."""
        principal = Principal(
            name="spn_etl",
            principal_type=PrincipalType.SERVICE_PRINCIPAL,
        )
        assert principal.resolved_name == "spn_etl_dev"

    def test_with_application_id(self) -> None:
        """with_application_id creates new Principal with ID."""
        original = Principal(name="spn_etl", principal_type=PrincipalType.SERVICE_PRINCIPAL)
        with_id = original.with_application_id("new-app-id")
        assert with_id.application_id == "new-app-id"
        assert original.application_id is None  # Original unchanged


class TestManagedServicePrincipal:
    """Tests for ManagedServicePrincipal model."""

    def test_resolved_name(self, dev_environment: None) -> None:
        """ManagedServicePrincipal resolved_name includes env suffix."""
        spn = make_service_principal(name="spn_etl")
        assert spn.resolved_name == "spn_etl_dev"

    def test_add_entitlement_string(self) -> None:
        """Entitlements can be added as strings."""
        spn = ManagedServicePrincipal(name="spn_test")
        spn.add_entitlement("workspace-access")
        assert "workspace-access" in spn.entitlements

    def test_add_entitlement_enum(self) -> None:
        """Entitlements can be added as enums."""
        spn = ManagedServicePrincipal(name="spn_test")
        spn.add_entitlement(WorkspaceEntitlement.WORKSPACE_ACCESS)
        assert "workspace-access" in spn.entitlements

    def test_add_entitlement_invalid_raises(self) -> None:
        """Invalid entitlement string raises ValueError."""
        spn = ManagedServicePrincipal(name="spn_test")
        with pytest.raises(ValueError) as exc_info:
            spn.add_entitlement("invalid-entitlement")
        assert "Unknown entitlement" in str(exc_info.value)

    def test_add_entitlement_deduplicates(self) -> None:
        """Adding same entitlement twice doesn't duplicate."""
        spn = ManagedServicePrincipal(name="spn_test")
        spn.add_entitlement("workspace-access")
        spn.add_entitlement("workspace-access")
        assert spn.entitlements.count("workspace-access") == 1

    def test_to_sdk_service_principal(self, dev_environment: None) -> None:
        """to_sdk_service_principal returns correct SDK object."""
        spn = ManagedServicePrincipal(
            name="spn_test",
            application_id="app-id-123",
            entitlements=["workspace-access"],
            active=True,
        )
        sdk_sp = spn.to_sdk_service_principal()
        assert sdk_sp.display_name == "spn_test_dev"
        assert sdk_sp.application_id == "app-id-123"
        assert sdk_sp.active is True
        assert sdk_sp.entitlements is not None
        assert len(sdk_sp.entitlements) == 1

    def test_to_principal(self, dev_environment: None) -> None:
        """to_principal creates grant-ready Principal."""
        spn = ManagedServicePrincipal(
            name="spn_test",
            application_id="app-id-123",
        )
        principal = spn.to_principal()
        assert principal.application_id == "app-id-123"
        assert principal.principal_type == PrincipalType.SERVICE_PRINCIPAL

    def test_to_principal_without_app_id_raises(self) -> None:
        """to_principal raises if application_id not set."""
        spn = ManagedServicePrincipal(name="spn_test")
        with pytest.raises(ValueError) as exc_info:
            spn.to_principal()
        assert "application_id not set" in str(exc_info.value)


class TestManagedGroup:
    """Tests for ManagedGroup model."""

    def test_resolved_name(self, dev_environment: None) -> None:
        """ManagedGroup resolved_name includes env suffix."""
        group = make_group(name="grp_test")
        assert group.resolved_name == "grp_test_dev"

    def test_add_user(self) -> None:
        """add_user adds user member correctly."""
        group = ManagedGroup(name="grp_test")
        result = group.add_user("alice@example.com")
        assert len(group.members) == 1
        assert group.members[0].name == "alice@example.com"
        assert group.members[0].principal_type == PrincipalType.USER
        assert group.members[0].add_environment_suffix is False
        assert result is group  # Chaining

    def test_add_service_principal(self, dev_environment: None) -> None:
        """add_service_principal adds SPN member correctly."""
        group = ManagedGroup(name="grp_test")
        group.add_service_principal("spn_etl")
        assert len(group.members) == 1
        assert group.members[0].name == "spn_etl"
        assert group.members[0].principal_type == PrincipalType.SERVICE_PRINCIPAL
        assert group.members[0].resolved_name == "spn_etl_dev"

    def test_add_nested_group(self, dev_environment: None) -> None:
        """add_nested_group adds group member correctly."""
        group = ManagedGroup(name="grp_parent")
        group.add_nested_group("grp_child")
        assert len(group.members) == 1
        assert group.members[0].name == "grp_child"
        assert group.members[0].principal_type == PrincipalType.GROUP

    def test_fluent_api(self) -> None:
        """Methods return self for chaining."""
        group = (
            ManagedGroup(name="grp_test")
            .add_user("alice@example.com")
            .add_service_principal("spn_etl")
            .add_entitlement(WorkspaceEntitlement.WORKSPACE_ACCESS)
        )
        assert len(group.members) == 2
        assert len(group.entitlements) == 1

    def test_to_sdk_group(self, dev_environment: None) -> None:
        """to_sdk_group returns correct SDK object."""
        group = (
            ManagedGroup(name="grp_test", display_name="Test Group")
            .add_user("alice@example.com")
            .add_entitlement("workspace-access")
        )
        sdk_group = group.to_sdk_group()
        assert sdk_group.display_name == "Test Group"
        assert sdk_group.members is not None
        assert len(sdk_group.members) == 1
        assert sdk_group.entitlements is not None
        assert len(sdk_group.entitlements) == 1


class TestMemberReference:
    """Tests for MemberReference model."""

    def test_user_never_gets_suffix(self, dev_environment: None) -> None:
        """User members never get environment suffix."""
        member = MemberReference(
            name="alice@example.com",
            principal_type=PrincipalType.USER,
            add_environment_suffix=True,  # Should be ignored for users
        )
        assert member.resolved_name == "alice@example.com"

    def test_group_gets_suffix(self, dev_environment: None) -> None:
        """Group members get environment suffix by default."""
        member = MemberReference(
            name="grp_child",
            principal_type=PrincipalType.GROUP,
            add_environment_suffix=True,
        )
        assert member.resolved_name == "grp_child_dev"

    def test_spn_gets_suffix(self, dev_environment: None) -> None:
        """SPN members get environment suffix by default."""
        member = MemberReference(
            name="spn_etl",
            principal_type=PrincipalType.SERVICE_PRINCIPAL,
            add_environment_suffix=True,
        )
        assert member.resolved_name == "spn_etl_dev"

    def test_to_complex_value(self, dev_environment: None) -> None:
        """to_complex_value returns SDK ComplexValue."""
        member = MemberReference(
            name="grp_child",
            principal_type=PrincipalType.GROUP,
        )
        cv = member.to_complex_value()
        assert cv.value == "grp_child_dev"
