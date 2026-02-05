"""
Rules registry and built-in governance rules for YAML conventions.

This module provides:
- RuleDefinition: Dataclass describing a rule
- RulesRegistry: Central registry for rule implementations
- Built-in rules for common governance patterns
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set

from brickkit.models.enums import PrincipalType

logger = logging.getLogger(__name__)


@dataclass
class RuleValidationResult:
    """Result of a rule validation check."""

    passed: bool
    message: Optional[str] = None
    rule_name: str = ""
    mode: Optional[str] = None  # "enforced" or "advisory" - set by YamlConvention.validate()


@dataclass
class RuleDefinition:
    """
    Definition of a governance rule.

    Attributes:
        name: Unique rule identifier
        description: Human-readable description
        validator_factory: Factory function that creates a validator
            The factory receives rule parameters (from YAML) and returns
            a validator function that takes (securable, context) and returns
            RuleValidationResult.
        default_applies_to: Default securable types if not specified
    """

    name: str
    description: str
    validator_factory: Callable[..., Callable[[Any, Dict[str, Any]], RuleValidationResult]]
    default_applies_to: Set[str] = field(default_factory=set)


class RulesRegistry:
    """
    Central registry for governance rule definitions.

    Rules are registered by name and can be retrieved to create validators
    based on YAML configuration.

    Usage:
        registry = RulesRegistry()
        registry.register(RuleDefinition(
            name="my_rule",
            description="Custom rule",
            validator_factory=my_validator_factory
        ))

        rule_def = registry.get("my_rule")
        validator = rule_def.validator_factory(param1="value")
        result = validator(securable, context)
    """

    def __init__(self) -> None:
        self._rules: Dict[str, RuleDefinition] = {}

    def register(self, rule: RuleDefinition) -> None:
        """
        Register a rule definition.

        Args:
            rule: RuleDefinition to register

        Raises:
            ValueError: If rule name is already registered
        """
        if rule.name in self._rules:
            raise ValueError(f"Rule '{rule.name}' is already registered")
        self._rules[rule.name] = rule
        logger.debug(f"Registered rule: {rule.name}")

    def get(self, name: str) -> RuleDefinition:
        """
        Get a rule definition by name.

        Args:
            name: Rule name

        Returns:
            RuleDefinition

        Raises:
            KeyError: If rule is not registered
        """
        if name not in self._rules:
            available = ", ".join(sorted(self._rules.keys()))
            raise KeyError(f"Rule '{name}' not found. Available rules: {available}")
        return self._rules[name]

    def list_rules(self) -> List[str]:
        """
        List all registered rule names.

        Returns:
            Sorted list of rule names
        """
        return sorted(self._rules.keys())

    def has_rule(self, name: str) -> bool:
        """Check if a rule is registered."""
        return name in self._rules


# =============================================================================
# BUILT-IN RULE VALIDATORS
# =============================================================================


def _catalog_must_have_sp_owner_factory(**kwargs: Any) -> Callable[[Any, Dict[str, Any]], RuleValidationResult]:
    """Factory for catalog_must_have_sp_owner rule."""

    def validator(securable: Any, context: Dict[str, Any]) -> RuleValidationResult:
        # Only applies to catalogs
        securable_type = getattr(securable, "securable_type", None)
        if securable_type is None:
            return RuleValidationResult(passed=True, rule_name="catalog_must_have_sp_owner")

        if securable_type.value != "CATALOG":
            return RuleValidationResult(passed=True, rule_name="catalog_must_have_sp_owner")

        owner = getattr(securable, "owner", None)
        if owner is None:
            return RuleValidationResult(
                passed=False, message="Catalog must have an owner", rule_name="catalog_must_have_sp_owner"
            )

        # Check if owner is a Principal with service_principal type
        if hasattr(owner, "principal_type"):
            if owner.principal_type == PrincipalType.SERVICE_PRINCIPAL:
                return RuleValidationResult(passed=True, rule_name="catalog_must_have_sp_owner")
            return RuleValidationResult(
                passed=False,
                message=f"Catalog owner must be a SERVICE_PRINCIPAL, got {owner.principal_type}",
                rule_name="catalog_must_have_sp_owner",
            )

        # If owner is a string, check context for owner info
        owner_info = context.get("owner_info", {})
        owner_type = owner_info.get("type", "").upper()
        if owner_type == "SERVICE_PRINCIPAL":
            return RuleValidationResult(passed=True, rule_name="catalog_must_have_sp_owner")

        return RuleValidationResult(
            passed=False, message="Catalog owner must be a SERVICE_PRINCIPAL", rule_name="catalog_must_have_sp_owner"
        )

    return validator


def _owner_must_be_sp_or_group_factory(**kwargs: Any) -> Callable[[Any, Dict[str, Any]], RuleValidationResult]:
    """Factory for owner_must_be_sp_or_group rule."""

    def validator(securable: Any, context: Dict[str, Any]) -> RuleValidationResult:
        owner = getattr(securable, "owner", None)
        if owner is None:
            return RuleValidationResult(passed=True, rule_name="owner_must_be_sp_or_group")

        # Check if owner is a Principal with valid type
        if hasattr(owner, "principal_type"):
            allowed = {PrincipalType.SERVICE_PRINCIPAL, PrincipalType.GROUP}
            if owner.principal_type in allowed:
                return RuleValidationResult(passed=True, rule_name="owner_must_be_sp_or_group")
            return RuleValidationResult(
                passed=False,
                message=f"Owner must be SERVICE_PRINCIPAL or GROUP, got {owner.principal_type}",
                rule_name="owner_must_be_sp_or_group",
            )

        # If owner is a string, check context for owner info
        owner_info = context.get("owner_info", {})
        owner_type = owner_info.get("type", "").upper()
        if owner_type in {"SERVICE_PRINCIPAL", "GROUP"}:
            return RuleValidationResult(passed=True, rule_name="owner_must_be_sp_or_group")

        if owner_type == "USER":
            return RuleValidationResult(
                passed=False,
                message="Owner cannot be an individual USER, must be SERVICE_PRINCIPAL or GROUP",
                rule_name="owner_must_be_sp_or_group",
            )

        # Unknown type, pass (no info to validate against)
        return RuleValidationResult(passed=True, rule_name="owner_must_be_sp_or_group")

    return validator


def _require_tags_factory(
    tags: Optional[List[str]] = None, **kwargs: Any
) -> Callable[[Any, Dict[str, Any]], RuleValidationResult]:
    """Factory for require_tags rule."""
    required_tags = tags or []

    def validator(securable: Any, context: Dict[str, Any]) -> RuleValidationResult:
        if not required_tags:
            return RuleValidationResult(passed=True, rule_name="require_tags")

        securable_tags = getattr(securable, "tags", [])
        tag_keys = {t.key for t in securable_tags}

        missing = [t for t in required_tags if t not in tag_keys]
        if missing:
            return RuleValidationResult(
                passed=False, message=f"Missing required tags: {', '.join(missing)}", rule_name="require_tags"
            )

        return RuleValidationResult(passed=True, rule_name="require_tags")

    return validator


def _naming_pattern_factory(
    pattern: Optional[str] = None, **kwargs: Any
) -> Callable[[Any, Dict[str, Any]], RuleValidationResult]:
    """Factory for naming_pattern rule."""
    regex_pattern = pattern

    def validator(securable: Any, context: Dict[str, Any]) -> RuleValidationResult:
        if not regex_pattern:
            return RuleValidationResult(passed=True, rule_name="naming_pattern")

        name = getattr(securable, "name", None)
        if not name:
            return RuleValidationResult(passed=True, rule_name="naming_pattern")

        if re.match(regex_pattern, name):
            return RuleValidationResult(passed=True, rule_name="naming_pattern")

        return RuleValidationResult(
            passed=False, message=f"Name '{name}' does not match pattern '{regex_pattern}'", rule_name="naming_pattern"
        )

    return validator


def _require_rfa_factory(**kwargs: Any) -> Callable[[Any, Dict[str, Any]], RuleValidationResult]:
    """
    Factory for require_rfa rule.

    Validates that securables have Request for Access (RFA) configured.
    RFA must have a destination email set.
    """

    def validator(securable: Any, context: Dict[str, Any]) -> RuleValidationResult:
        rfa = getattr(securable, "request_for_access", None)

        if rfa is None:
            securable_name = getattr(securable, "name", "unknown")
            securable_type = getattr(securable, "securable_type", None)
            type_str = securable_type.value if securable_type else "securable"
            return RuleValidationResult(
                passed=False,
                message=f"{type_str} '{securable_name}' must have Request for Access (RFA) configured",
                rule_name="require_rfa",
            )

        # Check that destination is set
        if not rfa.destination:
            securable_name = getattr(securable, "name", "unknown")
            return RuleValidationResult(
                passed=False,
                message=f"Request for Access on '{securable_name}' must have a destination email",
                rule_name="require_rfa",
            )

        return RuleValidationResult(passed=True, rule_name="require_rfa")

    return validator


# =============================================================================
# DEFAULT REGISTRY WITH BUILT-IN RULES
# =============================================================================


def create_default_registry() -> RulesRegistry:
    """
    Create a registry with all built-in rules registered.

    Returns:
        RulesRegistry with built-in rules
    """
    registry = RulesRegistry()

    registry.register(
        RuleDefinition(
            name="catalog_must_have_sp_owner",
            description="Catalogs must be owned by a service principal",
            validator_factory=_catalog_must_have_sp_owner_factory,
            default_applies_to={"CATALOG"},
        )
    )

    registry.register(
        RuleDefinition(
            name="owner_must_be_sp_or_group",
            description="Owners must be service principals or groups (no individual users)",
            validator_factory=_owner_must_be_sp_or_group_factory,
        )
    )

    registry.register(
        RuleDefinition(
            name="require_tags",
            description="Require specific tags to be present",
            validator_factory=_require_tags_factory,
        )
    )

    registry.register(
        RuleDefinition(
            name="naming_pattern",
            description="Names must match a regex pattern",
            validator_factory=_naming_pattern_factory,
        )
    )

    registry.register(
        RuleDefinition(
            name="require_rfa",
            description="Securables must have Request for Access (RFA) configured",
            validator_factory=_require_rfa_factory,
        )
    )

    return registry


# Global default registry instance
_default_registry: Optional[RulesRegistry] = None


def get_default_registry() -> RulesRegistry:
    """
    Get the default rules registry (singleton).

    Returns:
        The default RulesRegistry with built-in rules
    """
    global _default_registry
    if _default_registry is None:
        _default_registry = create_default_registry()
    return _default_registry


__all__ = [
    "RuleDefinition",
    "RuleValidationResult",
    "RulesRegistry",
    "create_default_registry",
    "get_default_registry",
]
