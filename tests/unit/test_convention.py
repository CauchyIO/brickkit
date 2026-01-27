"""
Unit tests for Convention model.

Tests convention application, validation, and tag propagation.
"""


from brickkit.convention import Convention
from brickkit.defaults import NamingConvention, RequiredTag, TagDefault
from brickkit.models.base import Tag
from brickkit.models.enums import Environment, SecurableType
from tests.fixtures import make_catalog


class TestConventionApplyTo:
    """Tests for Convention.apply_to() method."""

    def test_apply_adds_default_tags(self, dev_environment: None) -> None:
        """apply_to() adds default tags to securable."""
        convention = Convention(
            name="test_convention",
            default_tags=[
                TagDefault(key="managed_by", value="brickkit"),
                TagDefault(key="environment", value="development"),
            ],
        )
        catalog = make_catalog(name="test", tags=[])

        convention.apply_to(catalog, Environment.DEV)

        assert len(catalog.tags) == 2
        tag_dict = {t.key: t.value for t in catalog.tags}
        assert tag_dict["managed_by"] == "brickkit"
        assert tag_dict["environment"] == "development"

    def test_apply_does_not_overwrite_existing_tags(self, dev_environment: None) -> None:
        """apply_to() does not overwrite existing tags."""
        convention = Convention(
            name="test_convention",
            default_tags=[
                TagDefault(key="managed_by", value="brickkit"),
            ],
        )
        catalog = make_catalog(name="test", tags=[Tag(key="managed_by", value="terraform")])

        convention.apply_to(catalog, Environment.DEV)

        # Existing tag should be preserved
        assert len(catalog.tags) == 1
        assert catalog.tags[0].value == "terraform"

    def test_apply_with_environment_specific_values(self, prd_environment: None) -> None:
        """apply_to() uses environment-specific tag values."""
        convention = Convention(
            name="test_convention",
            default_tags=[
                TagDefault(
                    key="environment",
                    value="development",
                    environment_values={"PRD": "production"},
                ),
            ],
        )
        catalog = make_catalog(name="test", tags=[])

        convention.apply_to(catalog, Environment.PRD)

        assert catalog.tags[0].value == "production"

    def test_apply_with_securable_type_filter(self, dev_environment: None) -> None:
        """apply_to() respects applies_to filter."""
        convention = Convention(
            name="test_convention",
            default_tags=[
                TagDefault(key="catalog_only", value="yes", applies_to={"CATALOG"}),
                TagDefault(key="schema_only", value="yes", applies_to={"SCHEMA"}),
            ],
        )
        catalog = make_catalog(name="test", tags=[])

        convention.apply_to(catalog, Environment.DEV)

        # Only catalog_only tag should be applied
        assert len(catalog.tags) == 1
        assert catalog.tags[0].key == "catalog_only"


class TestConventionValidation:
    """Tests for Convention.validate_* methods."""

    def test_validate_tags_missing_required(self) -> None:
        """validate_tags() returns error for missing required tag."""
        convention = Convention(
            name="test_convention",
            required_tags=[
                RequiredTag(key="cost_center", error_message="Cost center is required"),
            ],
        )

        errors = convention.validate_tags(SecurableType.CATALOG, {})
        assert len(errors) == 1
        assert "cost_center" in errors[0].lower() or "Cost center" in errors[0]

    def test_validate_tags_present_required(self) -> None:
        """validate_tags() returns no error when required tag is present."""
        convention = Convention(
            name="test_convention",
            required_tags=[
                RequiredTag(key="cost_center"),
            ],
        )

        errors = convention.validate_tags(SecurableType.CATALOG, {"cost_center": "engineering"})
        assert len(errors) == 0

    def test_validate_tags_invalid_value(self) -> None:
        """validate_tags() returns error for invalid allowed value."""
        convention = Convention(
            name="test_convention",
            required_tags=[
                RequiredTag(key="pii", allowed_values={"true", "false"}),
            ],
        )

        errors = convention.validate_tags(SecurableType.TABLE, {"pii": "maybe"})
        assert len(errors) == 1
        assert "invalid value" in errors[0].lower()

    def test_validate_tags_respects_applies_to(self) -> None:
        """validate_tags() respects applies_to filter."""
        convention = Convention(
            name="test_convention",
            required_tags=[
                RequiredTag(key="table_only", applies_to={"TABLE"}),
            ],
        )

        # CATALOG should not require this tag
        catalog_errors = convention.validate_tags(SecurableType.CATALOG, {})
        assert len(catalog_errors) == 0

        # TABLE should require this tag
        table_errors = convention.validate_tags(SecurableType.TABLE, {})
        assert len(table_errors) == 1


class TestConventionNaming:
    """Tests for Convention naming validation."""

    def test_validate_naming_with_pattern(self) -> None:
        """validate_naming() validates against pattern."""
        convention = Convention(
            name="test_convention",
            naming_conventions=[
                NamingConvention(
                    pattern=r"^[a-z][a-z0-9_]+$",
                    error_message="Name must be lowercase with underscores",
                ),
            ],
        )

        # Valid name
        valid_errors = convention.validate_naming(SecurableType.CATALOG, "valid_name_123")
        assert len(valid_errors) == 0

        # Invalid name
        invalid_errors = convention.validate_naming(SecurableType.CATALOG, "InvalidName")
        assert len(invalid_errors) == 1
        assert "lowercase" in invalid_errors[0].lower()

    def test_validate_naming_respects_applies_to(self) -> None:
        """validate_naming() respects applies_to filter."""
        convention = Convention(
            name="test_convention",
            naming_conventions=[
                NamingConvention(
                    pattern=r"^tbl_",
                    applies_to={"TABLE"},
                    error_message="Table names must start with tbl_",
                ),
            ],
        )

        # CATALOG should not be validated
        catalog_errors = convention.validate_naming(SecurableType.CATALOG, "my_catalog")
        assert len(catalog_errors) == 0

        # TABLE should be validated
        table_errors = convention.validate_naming(SecurableType.TABLE, "my_table")
        assert len(table_errors) == 1


class TestConventionValidateSecurable:
    """Tests for Convention.validate_securable() method."""

    def test_validate_securable_combines_all_checks(self, dev_environment: None) -> None:
        """validate_securable() runs both tag and naming validation."""
        convention = Convention(
            name="test_convention",
            required_tags=[RequiredTag(key="cost_center")],
            naming_conventions=[
                NamingConvention(
                    pattern=r"^cat_",
                    applies_to={"CATALOG"},
                    error_message="Catalog names must start with cat_",
                ),
            ],
        )
        catalog = make_catalog(name="my_catalog", tags=[])

        errors = convention.validate_securable(catalog)

        assert len(errors) == 2  # Missing tag + bad naming


class TestConventionToGovernanceDefaults:
    """Tests for Convention interoperability with GovernanceDefaults."""

    def test_to_governance_defaults(self) -> None:
        """to_governance_defaults() returns compatible object."""
        convention = Convention(
            name="test_convention",
            default_tags=[TagDefault(key="managed_by", value="brickkit")],
            required_tags=[RequiredTag(key="cost_center")],
            default_owner="platform_team",
        )

        defaults = convention.to_governance_defaults()

        assert len(defaults.default_tags) == 1
        assert len(defaults.required_tags) == 1
        assert defaults.default_owner == "platform_team"

    def test_from_governance_defaults(self) -> None:
        """from_governance_defaults() creates Convention from defaults."""
        from brickkit.defaults import GovernanceDefaults

        class MyDefaults(GovernanceDefaults):
            @property
            def default_tags(self):
                return [TagDefault(key="managed_by", value="brickkit")]

            @property
            def default_owner(self):
                return "my_team"

        convention = Convention.from_governance_defaults("converted", MyDefaults())

        assert convention.name == "converted"
        assert len(convention.default_tags) == 1
        assert convention.default_owner == "my_team"
