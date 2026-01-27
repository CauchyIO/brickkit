"""
Unit tests for Grant models.

Tests Privilege, AccessPolicy, and securable grant functionality.
"""

import pytest

from brickkit.models import AccessPolicy, Principal, Privilege
from brickkit.models.enums import PrivilegeType, SecurableType
from tests.fixtures import make_catalog, make_principal, make_privilege, make_schema


class TestPrivilege:
    """Tests for Privilege model."""

    def test_securable_name_one_level(self) -> None:
        """securable_name returns level_1 for catalog."""
        priv = make_privilege(level_1="my_catalog_dev")
        assert priv.securable_name == "my_catalog_dev"

    def test_securable_name_two_levels(self) -> None:
        """securable_name includes schema for two-level securable."""
        priv = Privilege(
            level_1="catalog_dev",
            level_2="schema",
            securable_type=SecurableType.SCHEMA,
            principal="group",
            privilege_type=PrivilegeType.USE_SCHEMA,
        )
        assert priv.securable_name == "catalog_dev.schema"

    def test_securable_name_three_levels(self) -> None:
        """securable_name includes table for three-level securable."""
        priv = Privilege(
            level_1="catalog_dev",
            level_2="schema",
            level_3="table",
            securable_type=SecurableType.TABLE,
            principal="group",
            privilege_type=PrivilegeType.SELECT,
        )
        assert priv.securable_name == "catalog_dev.schema.table"

    def test_principal_accepts_principal_object(self) -> None:
        """Principal field accepts Principal object via field_validator."""
        principal = Principal(name="test_group", add_environment_suffix=False)
        # The field_validator accepts Principal and extracts resolved_name
        priv = Privilege(
            level_1="catalog",
            securable_type=SecurableType.CATALOG,
            principal=principal.resolved_name,
            privilege_type=PrivilegeType.USE_CATALOG,
        )
        assert priv.principal == "test_group"

    def test_parse_securable_name(self) -> None:
        """Privilege can be created from securable_name via model_validator."""
        # The model_validator mode="before" parses securable_name into levels
        priv = Privilege.model_validate(
            {
                "securable_name": "catalog_dev.schema.table",
                "securable_type": SecurableType.TABLE,
                "principal": "group",
                "privilege_type": PrivilegeType.SELECT,
            }
        )
        assert priv.level_1 == "catalog_dev"
        assert priv.level_2 == "schema"
        assert priv.level_3 == "table"


class TestAccessPolicy:
    """Tests for AccessPolicy model."""

    def test_reader_policy(self) -> None:
        """READER policy has correct privileges."""
        policy = AccessPolicy.READER()
        assert policy.name == "READER"

        catalog_privs = policy.get_privileges(SecurableType.CATALOG)
        assert PrivilegeType.USE_CATALOG in catalog_privs
        assert PrivilegeType.BROWSE in catalog_privs

        schema_privs = policy.get_privileges(SecurableType.SCHEMA)
        assert PrivilegeType.USE_SCHEMA in schema_privs
        assert PrivilegeType.SELECT in schema_privs

        table_privs = policy.get_privileges(SecurableType.TABLE)
        assert PrivilegeType.SELECT in table_privs

    def test_writer_policy(self) -> None:
        """WRITER policy has correct privileges."""
        policy = AccessPolicy.WRITER()
        assert policy.name == "WRITER"

        schema_privs = policy.get_privileges(SecurableType.SCHEMA)
        assert PrivilegeType.CREATE_TABLE in schema_privs
        assert PrivilegeType.MODIFY in schema_privs

        table_privs = policy.get_privileges(SecurableType.TABLE)
        assert PrivilegeType.MODIFY in table_privs

    def test_admin_policy(self) -> None:
        """ADMIN policy has correct privileges."""
        policy = AccessPolicy.ADMIN()
        assert policy.name == "ADMIN"

        catalog_privs = policy.get_privileges(SecurableType.CATALOG)
        assert PrivilegeType.ALL_PRIVILEGES in catalog_privs

        schema_privs = policy.get_privileges(SecurableType.SCHEMA)
        assert PrivilegeType.MANAGE in schema_privs

    def test_has_privileges_for(self) -> None:
        """has_privileges_for returns correct boolean."""
        policy = AccessPolicy.READER()
        assert policy.has_privileges_for(SecurableType.CATALOG) is True
        assert policy.has_privileges_for(SecurableType.SCHEMA) is True
        assert policy.has_privileges_for(SecurableType.TABLE) is True
        # READER doesn't have explicit storage credential privileges
        assert policy.has_privileges_for(SecurableType.STORAGE_CREDENTIAL) is False

    def test_get_privileges_empty(self) -> None:
        """get_privileges returns empty list for unknown securable."""
        policy = AccessPolicy.READER()
        assert policy.get_privileges(SecurableType.METASTORE) == []


class TestSecurableGrant:
    """Tests for grant method on securables."""

    def test_grant_creates_privileges(self, dev_environment: None) -> None:
        """grant() creates Privilege objects on securable."""
        catalog = make_catalog(name="test")
        principal = make_principal(name="test_group")
        policy = AccessPolicy.READER()

        result = catalog.grant(principal, policy)

        assert len(result) > 0
        assert len(catalog.privileges) > 0
        # Check catalog-level privileges were created
        catalog_privs = [p for p in catalog.privileges if p.securable_type == SecurableType.CATALOG]
        assert len(catalog_privs) > 0

    def test_grant_idempotent(self, dev_environment: None) -> None:
        """Granting same privileges twice doesn't duplicate."""
        catalog = make_catalog(name="test")
        principal = make_principal(name="test_group")
        policy = AccessPolicy.READER()

        result1 = catalog.grant(principal, policy)
        result2 = catalog.grant(principal, policy)

        # Second grant should return empty (already exists)
        assert len(result2) == 0
        # Privileges shouldn't be duplicated
        assert len(catalog.privileges) == len(result1)

    def test_grant_propagates_to_schemas(self, dev_environment: None) -> None:
        """grant() propagates to child schemas."""
        catalog = make_catalog(name="parent")
        schema = make_schema(name="child")
        catalog.add_schema(schema)

        principal = make_principal(name="test_group")
        policy = AccessPolicy.READER()

        catalog.grant(principal, policy)

        # Schema should have privileges too
        assert len(schema.privileges) > 0
        schema_privs = [p for p in schema.privileges if p.securable_type == SecurableType.SCHEMA]
        assert len(schema_privs) > 0

    def test_all_privileges_only_at_catalog(self, dev_environment: None) -> None:
        """ALL_PRIVILEGES can only be granted at catalog level."""
        schema = make_schema(name="test")

        # Create a policy with ALL_PRIVILEGES at schema level
        policy = AccessPolicy(
            name="BAD_POLICY",
            privilege_map={SecurableType.SCHEMA: [PrivilegeType.ALL_PRIVILEGES]},
        )
        principal = make_principal(name="test_group")

        with pytest.raises(ValueError) as exc_info:
            schema.grant(principal, policy)
        assert "CATALOG level" in str(exc_info.value)

    def test_grant_with_different_policies(self, dev_environment: None) -> None:
        """Different policies can be granted to different principals."""
        catalog = make_catalog(name="test")
        reader = make_principal(name="readers")
        writer = make_principal(name="writers")

        catalog.grant(reader, AccessPolicy.READER())
        catalog.grant(writer, AccessPolicy.WRITER())

        # Both principals should have privileges
        reader_privs = [p for p in catalog.privileges if p.principal == "readers_dev"]
        writer_privs = [p for p in catalog.privileges if p.principal == "writers_dev"]

        assert len(reader_privs) > 0
        assert len(writer_privs) > 0

    def test_grant_many(self, dev_environment: None) -> None:
        """grant_many grants to multiple principals."""
        catalog = make_catalog(name="test")
        principals = [make_principal(name=f"group_{i}") for i in range(3)]

        results = catalog.grant_many(principals, AccessPolicy.READER())

        assert len(results) == 3
        for principal in principals:
            assert principal.resolved_name in results

    def test_grant_all(self, dev_environment: None) -> None:
        """grant_all applies different policies to different principals."""
        catalog = make_catalog(name="test")
        reader = make_principal(name="reader")
        writer = make_principal(name="writer")

        results = catalog.grant_all(
            [
                (reader, AccessPolicy.READER()),
                (writer, AccessPolicy.WRITER()),
            ]
        )

        assert "reader_dev" in results
        assert "writer_dev" in results
