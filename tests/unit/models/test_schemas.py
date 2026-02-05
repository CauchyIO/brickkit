"""
Unit tests for Schema model.

Tests schema creation, parent catalog association, and child management.
"""

import pytest

from brickkit.models import Catalog, Principal, Schema, Table
from brickkit.models.enums import SecurableType, TableType
from tests.fixtures import make_schema


class TestSchemaNaming:
    """Tests for environment-aware catalog name resolution and FQDN."""

    def test_resolved_catalog_name_from_parent(self, dev_environment: None) -> None:
        """resolved_catalog_name uses parent catalog's resolved_name."""
        catalog = Catalog(name="data_lake")
        schema = Schema(name="raw")
        catalog.add_schema(schema)
        assert schema.resolved_catalog_name == "data_lake_dev"

    def test_resolved_catalog_name_from_catalog_name(self, dev_environment: None) -> None:
        """resolved_catalog_name works without parent reference."""
        schema = Schema(name="raw", catalog_name="data_lake")
        assert schema.resolved_catalog_name == "data_lake_dev"

    def test_resolved_catalog_name_no_catalog_raises(self) -> None:
        """resolved_catalog_name raises if no catalog association."""
        schema = Schema(name="orphan")
        with pytest.raises(ValueError) as exc_info:
            _ = schema.resolved_catalog_name
        assert "not associated with a catalog" in str(exc_info.value)

    def test_fqdn(self, dev_environment: None) -> None:
        """fqdn returns catalog.schema format."""
        schema = Schema(name="raw", catalog_name="data_lake")
        assert schema.fqdn == "data_lake_dev.raw"

    def test_full_name_alias(self, dev_environment: None) -> None:
        """full_name is an alias for fqdn."""
        schema = Schema(name="raw", catalog_name="data_lake")
        assert schema.full_name == schema.fqdn

    def test_fqdn_no_catalog_raises(self) -> None:
        """fqdn raises if no catalog association."""
        schema = Schema(name="orphan")
        with pytest.raises(ValueError):
            _ = schema.fqdn


class TestSchemaHierarchy:
    """Tests for managing tables within a schema."""

    def test_add_table(self) -> None:
        """Tables can be added to a schema."""
        schema = make_schema(name="raw", catalog_name="data_lake")
        table = Table(name="events", table_type=TableType.MANAGED)
        schema.add_table(table)
        assert len(schema.tables) == 1
        assert schema.tables[0].name == "events"

    def test_add_table_sets_names(self) -> None:
        """Adding table sets catalog_name and schema_name."""
        schema = Schema(name="raw", catalog_name="data_lake")
        table = Table(name="events", table_type=TableType.MANAGED)
        schema.add_table(table)
        assert table.catalog_name == "data_lake"
        assert table.schema_name == "raw"

    def test_add_table_duplicate_raises(self) -> None:
        """Adding duplicate table raises ValueError."""
        schema = make_schema()
        table1 = Table(name="duplicate", table_type=TableType.MANAGED)
        table2 = Table(name="duplicate", table_type=TableType.MANAGED)
        schema.add_table(table1)
        with pytest.raises(ValueError) as exc_info:
            schema.add_table(table2)
        assert "already exists" in str(exc_info.value)

    def test_table_inherits_owner(self) -> None:
        """Table inherits owner from schema if not set."""
        owner = Principal(name="schema_owner", add_environment_suffix=False)
        schema = Schema(name="raw", catalog_name="data_lake", owner=owner)
        table = Table(name="events", table_type=TableType.MANAGED)
        schema.add_table(table)
        assert table.owner is not None
        assert table.owner.name == "schema_owner"

    def test_schema_added_to_catalog(self, dev_environment: None) -> None:
        """Schema works correctly when added via catalog.add_schema()."""
        catalog = Catalog(name="data_lake")
        schema = Schema(name="raw")
        catalog.add_schema(schema)

        assert schema.catalog_name == "data_lake"
        assert schema.resolved_catalog_name == "data_lake_dev"
        assert schema.fqdn == "data_lake_dev.raw"

    def test_table_added_through_catalog(self, dev_environment: None) -> None:
        """Tables added to schema via catalog work correctly."""
        catalog = Catalog(name="data_lake")
        schema = Schema(name="raw")
        catalog.add_schema(schema)

        table = Table(name="events", table_type=TableType.MANAGED)
        schema.add_table(table)

        assert table.catalog_name == "data_lake"
        assert table.schema_name == "raw"


class TestSchemaSdkConversion:
    """Tests for SDK parameter conversion."""

    def test_to_sdk_create_params(self, dev_environment: None) -> None:
        """to_sdk_create_params returns correct structure."""
        schema = Schema(name="raw", catalog_name="data_lake", comment="Raw data")
        params = schema.to_sdk_create_params()
        assert params["name"] == "raw"
        assert params["catalog_name"] == "data_lake_dev"
        assert params["comment"] == "Raw data"

    def test_to_sdk_update_params(self, dev_environment: None) -> None:
        """to_sdk_update_params returns correct structure."""
        schema = Schema(name="raw", catalog_name="data_lake", comment="Updated")
        params = schema.to_sdk_update_params()
        assert params["full_name"] == "data_lake_dev.raw"
        assert params["comment"] == "Updated"

    def test_securable_type(self) -> None:
        """Schema has SCHEMA securable type."""
        schema = Schema(name="test", catalog_name="catalog")
        assert schema.securable_type == SecurableType.SCHEMA

    def test_get_level_1_name(self, dev_environment: None) -> None:
        """get_level_1_name returns resolved catalog name."""
        schema = Schema(name="test", catalog_name="catalog")
        assert schema.get_level_1_name() == "catalog_dev"

    def test_get_level_2_name(self) -> None:
        """get_level_2_name returns schema name."""
        schema = Schema(name="test", catalog_name="catalog")
        assert schema.get_level_2_name() == "test"
