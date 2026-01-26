"""
Convention mechanism for hierarchical governance propagation.

A Convention encapsulates governance rules (tags, naming, ownership) that
automatically propagate through the Unity Catalog hierarchy when applied
at any level.

Usage:
    from brickkit import Convention, Metastore, Catalog, Tag
    from brickkit.defaults import TagDefault, RequiredTag

    # Define a convention
    convention = Convention(
        name="finance_standards",
        default_tags=[
            TagDefault(key="compliance", value="sox"),
            TagDefault(key="managed_by", value="brickkit"),
        ],
        required_tags=[
            RequiredTag(key="cost_center", applies_to={"CATALOG", "SCHEMA"}),
        ],
    )

    # Apply at any level - propagates to all descendants
    m = Metastore(name="main")
    c = Catalog(name="finance")
    m.add_catalog(c)

    m.with_convention(convention)  # Propagates to catalog and all children

    # New children automatically inherit the convention
    s = Schema(name="reports")
    c.add_schema(s)  # Convention auto-applied to schema
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from pydantic import BaseModel, Field

from .defaults import (
    GovernanceDefaults,
    NamingConvention,
    RequiredTag,
    TagDefault,
)

if TYPE_CHECKING:
    from brickkit.models.enums import Environment, SecurableType

logger = logging.getLogger(__name__)


class Convention(BaseModel):
    """
    A Convention defines organizational standards that propagate through the hierarchy.

    Conventions bundle governance rules (default tags, required tags, naming conventions,
    ownership) and provide automatic propagation when applied to container objects like
    Metastore, Catalog, or Schema.

    Key features:
    - Automatic propagation: Calling `with_convention()` on a container applies
      the convention to all existing descendants
    - Auto-inheritance: New children added via `add_*` methods automatically
      inherit the parent's convention
    - Non-destructive: Existing tags are preserved; only missing defaults are added

    Attributes:
        name: Identifier for this convention (for logging and debugging)
        default_tags: Tags automatically applied to securables
        required_tags: Tags that must be present (validated on demand)
        naming_conventions: Regex patterns for naming validation
        default_owner: Default owner principal name for securables

    Example:
        # Architect team defines convention
        finance_convention = Convention(
            name="finance_standards",
            default_tags=[
                TagDefault(key="compliance", value="sox"),
                TagDefault(key="business_unit", value="finance"),
            ],
            required_tags=[
                RequiredTag(key="data_owner", applies_to={"TABLE"}),
            ],
            default_owner="finance_platform_team",
        )

        # Apply to metastore - propagates everywhere
        metastore.with_convention(finance_convention)
    """

    name: str = Field(..., description="Convention identifier for logging")
    default_tags: List[TagDefault] = Field(default_factory=list, description="Default tags to apply to securables")
    required_tags: List[RequiredTag] = Field(
        default_factory=list, description="Tags that must be present on securables"
    )
    naming_conventions: List[NamingConvention] = Field(default_factory=list, description="Naming convention rules")
    default_owner: Optional[str] = Field(None, description="Default owner principal name")

    class Config:
        arbitrary_types_allowed = True

    def get_default_tags_for(self, securable_type: "SecurableType", environment: "Environment") -> Dict[str, str]:
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

    def validate_tags(self, securable_type: "SecurableType", tags: Dict[str, str]) -> List[str]:
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
                    f"Tag '{required.key}' has invalid value '{tags[required.key]}'. Allowed: {required.allowed_values}"
                )
        return errors

    def validate_naming(self, securable_type: "SecurableType", name: str) -> List[str]:
        """
        Validate a name against naming conventions.

        Args:
            securable_type: The type of securable being validated
            name: The name to validate

        Returns:
            List of validation error messages (empty if valid)
        """
        import re

        errors = []
        for convention in self.naming_conventions:
            # Check if this convention applies to this securable type
            if convention.applies_to and securable_type.value not in convention.applies_to:
                continue

            if not re.match(convention.pattern, name):
                errors.append(convention.error_message)

        return errors

    def apply_to(self, securable: Any, environment: "Environment") -> Any:
        """
        Apply convention defaults to a single securable.

        Mutates the securable in place and returns it for method chaining.
        Does not overwrite existing tags - only adds missing defaults.

        Args:
            securable: The securable to apply defaults to
            environment: Current deployment environment

        Returns:
            The modified securable (for method chaining)
        """
        from brickkit.models.base import Tag

        logger.debug(
            f"Applying convention '{self.name}' to {securable.securable_type.value} '{getattr(securable, 'name', 'unknown')}'"
        )

        # Get default tags for this securable type
        default_tags = self.get_default_tags_for(securable.securable_type, environment)

        # Get existing tag keys
        existing_tag_keys = {t.key for t in getattr(securable, "tags", [])}

        # Add missing default tags
        for key, value in default_tags.items():
            if key not in existing_tag_keys:
                securable.tags.append(Tag(key=key, value=value))
                logger.debug(f"  Added tag: {key}={value}")

        return securable

    def validate_securable(self, securable: Any) -> List[str]:
        """
        Validate a securable against this convention's rules.

        Checks required tags and naming conventions.

        Args:
            securable: The securable to validate

        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []

        # Validate tags
        tag_dict = {t.key: t.value for t in getattr(securable, "tags", [])}
        errors.extend(self.validate_tags(securable.securable_type, tag_dict))

        # Validate naming
        name = getattr(securable, "name", None)
        if name:
            errors.extend(self.validate_naming(securable.securable_type, name))

        return errors

    def to_governance_defaults(self) -> "ConventionAsDefaults":
        """
        Convert this Convention to a GovernanceDefaults instance.

        Useful for interoperability with code expecting GovernanceDefaults.

        Returns:
            GovernanceDefaults-compatible object wrapping this convention
        """
        return ConventionAsDefaults(self)

    @classmethod
    def from_governance_defaults(cls, name: str, defaults: GovernanceDefaults) -> "Convention":
        """
        Create a Convention from an existing GovernanceDefaults instance.

        Args:
            name: Name for the new convention
            defaults: GovernanceDefaults to convert

        Returns:
            New Convention instance
        """
        return cls(
            name=name,
            default_tags=defaults.default_tags,
            required_tags=defaults.required_tags,
            naming_conventions=defaults.naming_conventions,
            default_owner=defaults.default_owner,
        )


class ConventionAsDefaults(GovernanceDefaults):
    """
    Adapter that wraps a Convention to provide GovernanceDefaults interface.

    This allows Conventions to be used anywhere GovernanceDefaults is expected,
    maintaining backward compatibility with existing code.
    """

    def __init__(self, convention: Convention) -> None:
        self._convention = convention

    @property
    def default_tags(self) -> List[TagDefault]:
        return self._convention.default_tags

    @property
    def required_tags(self) -> List[RequiredTag]:
        return self._convention.required_tags

    @property
    def naming_conventions(self) -> List[NamingConvention]:
        return self._convention.naming_conventions

    @property
    def default_owner(self) -> Optional[str]:
        return self._convention.default_owner


__all__ = [
    "Convention",
    "ConventionAsDefaults",
]
