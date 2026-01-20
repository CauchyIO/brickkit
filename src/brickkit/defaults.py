"""
Governance defaults mechanism for centralized tag and naming policies.

Governance teams subclass GovernanceDefaults to define organization-wide
policies for tags, naming conventions, and required metadata.

Usage:
    class MyOrgDefaults(GovernanceDefaults):
        @property
        def default_tags(self) -> List[TagDefault]:
            return [
                TagDefault(key="cost_center", value="shared"),
                TagDefault(key="environment", value="dev",
                           environment_values={Environment.PRD: "prod"})
            ]

        @property
        def required_tags(self) -> List[RequiredTag]:
            return [
                RequiredTag(key="data_owner",
                           applies_to={SecurableType.CATALOG, SecurableType.SCHEMA}),
                RequiredTag(key="pii", allowed_values={"true", "false"},
                           applies_to={SecurableType.TABLE})
            ]

    # Apply to securables
    catalog = Catalog(name="analytics").with_defaults(MyOrgDefaults())
"""

from __future__ import annotations

from abc import ABC
from typing import TYPE_CHECKING, Dict, List, Optional, Set, TypeVar

from pydantic import BaseModel, Field, field_validator

from brickkit.models.enums import get_valid_securable_types

if TYPE_CHECKING:
    from brickkit.models.base import BaseSecurable
    from brickkit.models.enums import Environment, SecurableType

T = TypeVar('T', bound='BaseSecurable')


class TagDefault(BaseModel):
    """
    Definition of a default tag with optional environment-specific values.

    Attributes:
        key: Tag key (e.g., 'environment', 'cost_center')
        value: Default value when no environment-specific value exists
        environment_values: Optional mapping of Environment to specific values
        applies_to: Set of SecurableTypes this default applies to (empty = all)
    """
    key: str
    value: str
    environment_values: Dict[str, str] = Field(default_factory=dict)
    applies_to: Set[str] = Field(default_factory=set)

    @field_validator("applies_to")
    @classmethod
    def validate_applies_to(cls, v: Set[str]) -> Set[str]:
        """Validate that applies_to contains valid SecurableType values."""
        if not v:  # Empty set means "all types"
            return v
        valid_types = get_valid_securable_types()
        invalid = v - valid_types
        if invalid:
            raise ValueError(
                f"Invalid securable type(s): {sorted(invalid)}. "
                f"Valid types: {sorted(valid_types)}"
            )
        return v

    def get_value(self, env: 'Environment') -> str:
        """Get tag value for specific environment."""
        return self.environment_values.get(env.value, self.value)


class RequiredTag(BaseModel):
    """
    Definition of a required tag that must be present on securables.

    Attributes:
        key: Tag key that must be present
        allowed_values: Optional set of allowed values (None = any value)
        applies_to: Set of SecurableTypes this requirement applies to (empty = all)
        error_message: Custom error message for validation failures
    """
    key: str
    allowed_values: Optional[Set[str]] = None
    applies_to: Set[str] = Field(default_factory=set)
    error_message: Optional[str] = None

    @field_validator("applies_to")
    @classmethod
    def validate_applies_to(cls, v: Set[str]) -> Set[str]:
        """Validate that applies_to contains valid SecurableType values."""
        if not v:  # Empty set means "all types"
            return v
        valid_types = get_valid_securable_types()
        invalid = v - valid_types
        if invalid:
            raise ValueError(
                f"Invalid securable type(s): {sorted(invalid)}. "
                f"Valid types: {sorted(valid_types)}"
            )
        return v


class NamingConvention(BaseModel):
    """
    Naming convention rule for securables.

    Attributes:
        pattern: Regex pattern that names must match
        applies_to: Set of SecurableTypes this convention applies to (empty = all)
        error_message: Error message shown when validation fails
    """
    pattern: str
    applies_to: Set[str] = Field(default_factory=set)
    error_message: str = "Name does not match required pattern"

    @field_validator("applies_to")
    @classmethod
    def validate_applies_to(cls, v: Set[str]) -> Set[str]:
        """Validate that applies_to contains valid SecurableType values."""
        if not v:  # Empty set means "all types"
            return v
        valid_types = get_valid_securable_types()
        invalid = v - valid_types
        if invalid:
            raise ValueError(
                f"Invalid securable type(s): {sorted(invalid)}. "
                f"Valid types: {sorted(valid_types)}"
            )
        return v


class GovernanceDefaults(ABC):
    """
    Base class for governance defaults that teams subclass.

    Governance teams define organization-wide policies by subclassing
    this and overriding the property methods to return their defaults.

    Example:
        class MyOrgDefaults(GovernanceDefaults):
            @property
            def default_tags(self) -> List[TagDefault]:
                return [
                    TagDefault(key="managed_by", value="brickkit"),
                ]

            @property
            def required_tags(self) -> List[RequiredTag]:
                return [
                    RequiredTag(
                        key="cost_center",
                        applies_to={"CATALOG"},
                        error_message="Catalogs must have a cost_center tag"
                    ),
                ]

        # Apply to securables
        catalog = Catalog(name="analytics").with_defaults(MyOrgDefaults())
    """

    @property
    def default_tags(self) -> List[TagDefault]:
        """Default tags to apply to securables."""
        return []

    @property
    def required_tags(self) -> List[RequiredTag]:
        """Required tags that must be present."""
        return []

    @property
    def naming_conventions(self) -> List[NamingConvention]:
        """Naming convention rules."""
        return []

    @property
    def default_owner(self) -> Optional[str]:
        """Default owner principal name for securables without explicit owner."""
        return None

    def get_default_tags_for(
        self,
        securable_type: 'SecurableType',
        environment: 'Environment'
    ) -> Dict[str, str]:
        """
        Get default tags for a specific securable type and environment.

        Args:
            securable_type: The type of securable
            environment: Current deployment environment

        Returns:
            Dict mapping tag keys to values
        """
        result = {}
        for tag_default in self.default_tags:
            # Check if this default applies to this securable type
            if not tag_default.applies_to or securable_type.value in tag_default.applies_to:
                result[tag_default.key] = tag_default.get_value(environment)
        return result

    def validate_tags(
        self,
        securable_type: 'SecurableType',
        tags: Dict[str, str]
    ) -> List[str]:
        """
        Validate tags against required tag rules.

        Args:
            securable_type: The type of securable being validated
            tags: Dict of tag key -> value currently on the securable

        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []
        for required in self.required_tags:
            # Check if this requirement applies to this securable type
            if required.applies_to and securable_type.value not in required.applies_to:
                continue

            if required.key not in tags:
                msg = required.error_message or f"Missing required tag: {required.key}"
                errors.append(msg)
            elif required.allowed_values and tags[required.key] not in required.allowed_values:
                errors.append(
                    f"Tag '{required.key}' has invalid value '{tags[required.key]}'. "
                    f"Allowed: {required.allowed_values}"
                )
        return errors

    def apply_to(self, securable: T, environment: 'Environment') -> T:
        """
        Apply defaults to a securable.

        Mutates the securable in place and returns it for method chaining.
        Does not overwrite existing tags - only adds missing defaults.

        Args:
            securable: The securable to apply defaults to
            environment: Current deployment environment

        Returns:
            The modified securable (for method chaining)
        """
        # Import here to avoid circular dependency
        from brickkit.models.base import Tag

        # Get default tags for this securable type
        default_tags = self.get_default_tags_for(securable.securable_type, environment)

        # Get existing tag keys
        existing_tag_keys = {t.key for t in getattr(securable, 'tags', [])}

        # Add missing default tags
        for key, value in default_tags.items():
            if key not in existing_tag_keys:
                securable.tags.append(Tag(key=key, value=value))

        return securable


class EmptyDefaults(GovernanceDefaults):
    """No defaults applied. Use when governance is not needed."""
    pass


class StandardDefaults(GovernanceDefaults):
    """
    Standard governance defaults for typical enterprise use.

    Provides sensible defaults that most organizations can use as-is
    or extend by subclassing.
    """

    @property
    def default_tags(self) -> List[TagDefault]:
        return [
            TagDefault(
                key="managed_by",
                value="brickkit"
            ),
        ]

    @property
    def required_tags(self) -> List[RequiredTag]:
        return [
            RequiredTag(
                key="cost_center",
                applies_to={"CATALOG"},
                error_message="Catalogs must have a cost_center tag for chargeback"
            ),
        ]


__all__ = [
    "GovernanceDefaults",
    "TagDefault",
    "RequiredTag",
    "NamingConvention",
    "EmptyDefaults",
    "StandardDefaults",
]
