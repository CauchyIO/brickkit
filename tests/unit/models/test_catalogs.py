"""
Unit tests for Catalog model.

Tests catalog creation, environment-aware naming, validation, and schema management.
"""


import pytest
from pydantic import ValidationError

from brickkit.models import Catalog, Schema
from brickkit.models.enums import Environment, IsolationMode
from tests.fixtures import make_catalog, make_schema, make_tag


class TestCatalogNaming:
    """Tests for environment-aware catalog naming."""

    def test_resolved_name_dev(self, dev_environment: None) -> None:
        """Catalog resolved_name includes _dev suffix in DEV environment."""
        catalog = Catalog(name="analytics")
        assert catalog.resolved_name == "analytics_dev"

    def test_resolved_name_acc(self, acc_environment: None) -> None:
        """Catalog resolved_name includes _acc suffix in ACC environment."""
        catalog = Catalog(name="analytics")
        assert catalog.resolved_name == "analytics_acc"

    def test_resolved_name_prd(self, prd_environment: None) -> None:
        """Catalog resolved_name includes _prd suffix in PRD environment."""
        catalog = Catalog(name="analytics")
        assert catalog.resolved_name == "analytics_prd"

    def test_resolved_name_with_environment_override(self, dev_environment: None) -> None:
        """Catalog can override environment via field."""
        catalog = Catalog(name="analytics", environment=Environment.PRD)
        assert catalog.resolved_name == "analytics_prd"

    def test_environment_name_alias(self, dev_environment: None) -> None:
        """environment_name is an alias for resolved_name."""
        catalog = Catalog(name="analytics")
        assert catalog.environment_name == catalog.resolved_name
        assert catalog.environment_name == "analytics_dev"


class TestCatalogValidation:
    """Tests for custom catalog validation rules (not Pydantic builtins)."""

    def test_workspace_ids_require_isolated_mode(self) -> None:
        """Workspace IDs can only be set with ISOLATED mode."""
        with pytest.raises(ValidationError) as exc_info:
            Catalog(name="test", workspace_ids=[123], isolation_mode=IsolationMode.OPEN)
        assert "ISOLATED" in str(exc_info.value)


class TestCatalogHierarchy:
    """Tests for managing schemas within a catalog."""

    def test_add_schema(self) -> None:
        """Schemas can be added to a catalog."""
        catalog = make_catalog(name="parent")
        schema = make_schema(name="child")
        catalog.add_schema(schema)
        assert len(catalog.schemas) == 1
        assert catalog.schemas[0].name == "child"

    def test_add_schema_sets_catalog_name(self) -> None:
        """Adding schema sets the catalog_name reference."""
        catalog = Catalog(name="parent")
        schema = Schema(name="child")
        catalog.add_schema(schema)
        assert schema.catalog_name == "parent"

    def test_add_schema_duplicate_raises(self) -> None:
        """Adding duplicate schema raises ValueError."""
        catalog = make_catalog()
        schema1 = make_schema(name="duplicate")
        schema2 = make_schema(name="duplicate")
        catalog.add_schema(schema1)
        with pytest.raises(ValueError) as exc_info:
            catalog.add_schema(schema2)
        assert "already exists" in str(exc_info.value)

    def test_schema_inherits_owner(self) -> None:
        """Schema inherits owner from catalog if not set."""
        from brickkit.models import Principal

        owner = Principal(name="catalog_owner", add_environment_suffix=False)
        catalog = Catalog(name="parent", owner=owner)
        schema = Schema(name="child")
        catalog.add_schema(schema)
        assert schema.owner is not None
        assert schema.owner.name == "catalog_owner"


class TestCatalogSdkConversion:
    """Tests for SDK parameter conversion."""

    def test_to_sdk_create_params(self, dev_environment: None) -> None:
        """to_sdk_create_params returns correct structure."""
        catalog = Catalog(name="analytics", comment="Test catalog")
        params = catalog.to_sdk_create_params()
        assert params["name"] == "analytics_dev"
        assert params["comment"] == "Test catalog"
        assert "storage_root" not in params or params["storage_root"] is None

    def test_to_sdk_create_params_with_storage(self, dev_environment: None) -> None:
        """Storage root is included when set via managed_location."""
        catalog = Catalog(
            name="analytics",
            managed_location="abfss://container@account.dfs.core.windows.net/path",
        )
        params = catalog.to_sdk_create_params()
        assert params["storage_root"] == "abfss://container@account.dfs.core.windows.net/path"

    def test_to_sdk_update_params(self, dev_environment: None) -> None:
        """to_sdk_update_params returns correct structure."""
        catalog = Catalog(name="analytics", comment="Updated comment")
        params = catalog.to_sdk_update_params()
        assert params["name"] == "analytics_dev"
        assert params["comment"] == "Updated comment"
        assert params["isolation_mode"] == "OPEN"

    def test_securable_type(self) -> None:
        """Catalog has CATALOG securable type."""
        from brickkit.models.enums import SecurableType

        catalog = Catalog(name="test")
        assert catalog.securable_type == SecurableType.CATALOG

    def test_get_level_1_name(self, dev_environment: None) -> None:
        """get_level_1_name returns resolved catalog name."""
        catalog = Catalog(name="test")
        assert catalog.get_level_1_name() == "test_dev"
