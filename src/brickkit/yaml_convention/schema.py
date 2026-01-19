"""
Pydantic models for YAML convention schema validation.

This module defines the data models that represent the structure of
YAML convention files, enabling validation and type-safe access.
"""

from __future__ import annotations

from enum import Enum
from typing import Dict, List, Optional, Set

from pydantic import BaseModel, Field, field_validator


class RuleMode(str, Enum):
    """Execution mode for governance rules."""
    ENFORCED = "enforced"  # Rule violations raise errors
    ADVISORY = "advisory"  # Rule violations generate warnings


class OwnershipSpec(BaseModel):
    """
    Specification for ownership configuration.

    Attributes:
        type: Principal type (USER, GROUP, SERVICE_PRINCIPAL)
        name: Principal base name
        add_environment_suffix: Whether to append env suffix to name
    """
    type: str = Field(..., description="Principal type: USER, GROUP, or SERVICE_PRINCIPAL")
    name: str = Field(..., description="Principal base name")
    add_environment_suffix: bool = Field(
        default=True,
        description="Whether to add environment suffix to principal name"
    )

    @field_validator("type")
    @classmethod
    def validate_type(cls, v: str) -> str:
        """Validate principal type."""
        allowed = {"USER", "GROUP", "SERVICE_PRINCIPAL"}
        upper = v.upper()
        if upper not in allowed:
            raise ValueError(f"type must be one of {allowed}, got '{v}'")
        return upper


class NamingSpec(BaseModel):
    """
    Specification for naming conventions.

    Supports pattern-based name generation with placeholders:
    - {env}: Current environment (dev, acc, prd)
    - {team}: Team name from config
    - {product}: Product name from config
    - {acronym}: Securable type acronym (cat, sch, tbl, etc.)
    - {name}: Base name provided at creation time

    Attributes:
        pattern: Name pattern with placeholders
        team: Default team name for {team} placeholder
        product: Default product name for {product} placeholder
        separator: Separator between pattern parts (default: "_")
    """
    pattern: str = Field(
        default="{env}_{team}_{product}",
        description="Name pattern with placeholders"
    )
    team: Optional[str] = Field(None, description="Team name for {team} placeholder")
    product: Optional[str] = Field(None, description="Product name for {product} placeholder")
    separator: str = Field(default="_", description="Separator between name parts")


class RuleSpec(BaseModel):
    """
    Specification for a governance rule.

    Rules define validation constraints that are checked against securables.
    Each rule has a name that maps to a registered rule implementation.

    Attributes:
        rule: Rule name (must be registered in RulesRegistry)
        mode: Execution mode (enforced or advisory)
        tags: Tag names required (for require_tags rule)
        pattern: Regex pattern (for naming_pattern rule)
        applies_to: Set of securable types this rule applies to
    """
    rule: str = Field(..., description="Rule name from registry")
    mode: RuleMode = Field(
        default=RuleMode.ENFORCED,
        description="Rule execution mode"
    )
    # Parameters for specific rules
    tags: Optional[List[str]] = Field(None, description="Required tag names")
    pattern: Optional[str] = Field(None, description="Regex pattern for naming")
    applies_to: Optional[Set[str]] = Field(
        None,
        description="Securable types this rule applies to"
    )


class YamlConventionSchema(BaseModel):
    """
    Root schema for YAML convention files.

    This is the main model that represents a complete convention
    definition loaded from a YAML file.

    Example YAML:
        version: "1.0"
        convention: financial_services

        naming:
          pattern: "{env}_{team}_{product}"
          team: quant
          product: risk_analytics

        ownership:
          catalog: { type: SERVICE_PRINCIPAL, name: spn_trading_platform }
          default: { type: GROUP, name: grp_quant_team }

        rules:
          - rule: catalog_must_have_sp_owner
            mode: enforced
          - rule: require_tags
            tags: [cost_center, team]
            mode: advisory

        tags:
          cost_center: CC-TRD-4521
          team: quant

        tag_overrides:
          prd:
            environment: production
    """
    version: str = Field(default="1.0", description="Schema version")
    convention: str = Field(..., description="Convention name/identifier")

    naming: Optional[NamingSpec] = Field(None, description="Naming configuration")
    ownership: Optional[Dict[str, OwnershipSpec]] = Field(
        None,
        description="Ownership by securable type (catalog, schema, default, etc.)"
    )
    rules: List[RuleSpec] = Field(
        default_factory=list,
        description="Governance rules to apply"
    )
    tags: Dict[str, str] = Field(
        default_factory=dict,
        description="Default tags to apply to all securables"
    )
    tag_overrides: Dict[str, Dict[str, str]] = Field(
        default_factory=dict,
        description="Environment-specific tag overrides"
    )

    def get_tags_for_environment(self, environment: str) -> Dict[str, str]:
        """
        Get merged tags for a specific environment.

        Base tags are merged with environment-specific overrides.

        Args:
            environment: Environment name (dev, acc, prd)

        Returns:
            Dict of tag key -> value
        """
        result = dict(self.tags)
        env_lower = environment.lower()
        if env_lower in self.tag_overrides:
            result.update(self.tag_overrides[env_lower])
        return result

    def get_owner_for_type(self, securable_type: str) -> Optional[OwnershipSpec]:
        """
        Get ownership spec for a securable type.

        Falls back to 'default' if no specific owner is defined.

        Args:
            securable_type: Securable type name (catalog, schema, etc.)

        Returns:
            OwnershipSpec or None if no owner defined
        """
        if not self.ownership:
            return None

        type_lower = securable_type.lower()
        if type_lower in self.ownership:
            return self.ownership[type_lower]
        return self.ownership.get("default")


__all__ = [
    "RuleMode",
    "OwnershipSpec",
    "NamingSpec",
    "RuleSpec",
    "YamlConventionSchema",
]
