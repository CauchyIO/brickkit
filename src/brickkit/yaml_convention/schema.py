"""
Pydantic models for YAML convention schema validation.

This module defines the data models that represent the structure of
YAML convention files, enabling validation and type-safe access.
"""

from __future__ import annotations

from enum import Enum
from typing import Dict, List, Optional, Set

from pydantic import BaseModel, Field, field_validator

from brickkit.models.enums import Environment, get_valid_securable_types


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


class RequestForAccessSpec(BaseModel):
    """
    Specification for Request for Access (RFA) configuration.

    RFA allows users without access to request it from data owners.
    Supports inheritance: Table inherits from Schema, Schema from Catalog.

    Attributes:
        destination: Email address where access requests are sent
        instructions: Instructions shown to users requesting access
        inherit: Whether to inherit RFA from parent (default: True for schema/table)
    """
    destination: Optional[str] = Field(
        None,
        description="Email address for access requests"
    )
    instructions: Optional[str] = Field(
        None,
        description="Instructions shown to users requesting access"
    )
    inherit: bool = Field(
        default=True,
        description="Whether to inherit RFA from parent securable"
    )

    @field_validator("destination")
    @classmethod
    def validate_destination(cls, v: Optional[str]) -> Optional[str]:
        """Validate destination is a valid email format if provided."""
        if v is None:
            return v
        # Basic email validation
        import re
        email_pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        if not re.match(email_pattern, v):
            raise ValueError(f"Invalid email format: '{v}'")
        return v


# Valid placeholders for naming patterns
VALID_NAMING_PLACEHOLDERS = {"{env}", "{team}", "{product}", "{acronym}", "{name}"}


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

    @field_validator("pattern")
    @classmethod
    def validate_pattern(cls, v: str) -> str:
        """Validate that pattern contains only valid placeholders."""
        import re
        # Find all {placeholder} patterns in the string
        found_placeholders = set(re.findall(r"\{[^}]+\}", v))
        invalid = found_placeholders - VALID_NAMING_PLACEHOLDERS
        if invalid:
            raise ValueError(
                f"Invalid placeholder(s) in pattern: {sorted(invalid)}. "
                f"Valid placeholders: {sorted(VALID_NAMING_PLACEHOLDERS)}"
            )
        return v


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

    @field_validator("applies_to")
    @classmethod
    def validate_applies_to(cls, v: Optional[Set[str]]) -> Optional[Set[str]]:
        """Validate that applies_to contains valid SecurableType values."""
        if v is None:
            return v
        valid_types = get_valid_securable_types()
        invalid = v - valid_types
        if invalid:
            raise ValueError(
                f"Invalid securable type(s): {sorted(invalid)}. "
                f"Valid types: {sorted(valid_types)}"
            )
        return v


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
    request_for_access: Optional[Dict[str, RequestForAccessSpec]] = Field(
        None,
        description="Request for Access configuration by securable type (catalog, schema, table, default)"
    )

    @field_validator("ownership")
    @classmethod
    def validate_ownership_keys(cls, v: Optional[Dict[str, OwnershipSpec]]) -> Optional[Dict[str, OwnershipSpec]]:
        """Validate that ownership keys are valid securable types or 'default'."""
        if v is None:
            return v
        # Get valid securable types in lowercase
        valid_types = {st.lower() for st in get_valid_securable_types()}
        valid_types.add("default")  # 'default' is a valid fallback key
        invalid = set(v.keys()) - valid_types
        if invalid:
            raise ValueError(
                f"Invalid ownership key(s): {sorted(invalid)}. "
                f"Valid keys: {sorted(valid_types)}"
            )
        return v

    @field_validator("tag_overrides")
    @classmethod
    def validate_tag_override_keys(cls, v: Dict[str, Dict[str, str]]) -> Dict[str, Dict[str, str]]:
        """Validate that tag_overrides keys are valid environment names."""
        if not v:
            return v
        valid_envs = {e.value.lower() for e in Environment}
        invalid = set(v.keys()) - valid_envs
        if invalid:
            raise ValueError(
                f"Invalid environment(s) in tag_overrides: {sorted(invalid)}. "
                f"Valid environments: {sorted(valid_envs)}"
            )
        return v

    @field_validator("request_for_access")
    @classmethod
    def validate_rfa_keys(cls, v: Optional[Dict[str, RequestForAccessSpec]]) -> Optional[Dict[str, RequestForAccessSpec]]:
        """Validate that request_for_access keys are valid securable types or 'default'."""
        if v is None:
            return v
        # RFA applies to securables that support it in Unity Catalog
        valid_rfa_types = {"catalog", "schema", "table", "volume", "function", "model", "default"}
        invalid = set(v.keys()) - valid_rfa_types
        if invalid:
            raise ValueError(
                f"Invalid request_for_access key(s): {sorted(invalid)}. "
                f"Valid keys: {sorted(valid_rfa_types)}"
            )
        return v

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

    def get_rfa_for_type(self, securable_type: str) -> Optional[RequestForAccessSpec]:
        """
        Get Request for Access spec for a securable type with inheritance.

        Inheritance chain:
        - Table → Schema → Catalog → default
        - Volume/Function/Model → Schema → Catalog → default
        - Schema → Catalog → default
        - Catalog → default

        Args:
            securable_type: Securable type name (catalog, schema, table, etc.)

        Returns:
            RequestForAccessSpec or None if no RFA configured
        """
        if not self.request_for_access:
            return None

        type_lower = securable_type.lower()

        # Define inheritance chain for each type
        inheritance_chains = {
            "table": ["table", "schema", "catalog", "default"],
            "volume": ["volume", "schema", "catalog", "default"],
            "function": ["function", "schema", "catalog", "default"],
            "model": ["model", "schema", "catalog", "default"],
            "schema": ["schema", "catalog", "default"],
            "catalog": ["catalog", "default"],
        }

        chain = inheritance_chains.get(type_lower, [type_lower, "default"])

        for level in chain:
            if level in self.request_for_access:
                spec = self.request_for_access[level]
                # If this level has explicit destination, return it
                if spec.destination:
                    return spec
                # If inherit is False and no destination, stop here (no RFA)
                if not spec.inherit:
                    return None
                # Otherwise continue up the chain

        return None


__all__ = [
    "RuleMode",
    "OwnershipSpec",
    "RequestForAccessSpec",
    "NamingSpec",
    "RuleSpec",
    "YamlConventionSchema",
]
