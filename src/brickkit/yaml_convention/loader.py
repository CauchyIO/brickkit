"""
YAML convention loader and runtime wrapper.

This module provides functions to load convention files and a wrapper
class that provides runtime functionality for applying conventions.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from brickkit.models.base import RequestForAccess, Tag, get_current_environment
from brickkit.models.enums import Environment, PrincipalType, SecurableType
from brickkit.models.grants import Principal

from .naming import NameGenerator
from .rules import RulesRegistry, RuleValidationResult, get_default_registry
from .schema import RuleMode, YamlConventionSchema

logger = logging.getLogger(__name__)


class YamlConvention:
    """
    Runtime wrapper for a loaded YAML convention.

    Provides methods to:
    - Generate names following the convention's pattern
    - Get ownership specifications
    - Apply tags to securables
    - Validate securables against rules

    Usage:
        convention = load_convention("path/to/convention.yml")

        # Generate name for a catalog
        name = convention.generate_name(SecurableType.CATALOG)

        # Get owner for catalog
        owner = convention.get_catalog_owner()

        # Apply convention to a securable
        convention.apply_to(catalog)

        # Validate securable against rules
        errors = convention.validate(catalog)
    """

    def __init__(
        self,
        schema: YamlConventionSchema,
        registry: Optional[RulesRegistry] = None,
    ) -> None:
        """
        Initialize the convention wrapper.

        Args:
            schema: Validated YAML convention schema
            registry: Optional rules registry (uses default if not provided)
        """
        self._schema = schema
        self._registry = registry or get_default_registry()
        self._name_generator: Optional[NameGenerator] = None

        if schema.naming:
            self._name_generator = NameGenerator(schema.naming)

    @property
    def name(self) -> str:
        """Convention name/identifier."""
        return self._schema.convention

    @property
    def version(self) -> str:
        """Schema version."""
        return self._schema.version

    @property
    def schema(self) -> YamlConventionSchema:
        """Access to the underlying schema."""
        return self._schema

    def generate_name(
        self,
        securable_type: SecurableType,
        environment: Optional[Environment] = None,
        name: Optional[str] = None,
        **kwargs: str,
    ) -> str:
        """
        Generate a name following the convention's pattern.

        Args:
            securable_type: Type of securable to name
            environment: Environment (uses current if not specified)
            name: Base name for {name} placeholder
            **kwargs: Additional placeholder values (team, product)

        Returns:
            Generated name string

        Raises:
            ValueError: If naming spec not configured or required values missing
        """
        if not self._name_generator:
            raise ValueError(f"Convention '{self.name}' has no naming configuration")

        env = environment or get_current_environment()
        return self._name_generator.generate(
            securable_type=securable_type,
            environment=env,
            name=name,
            team=kwargs.get("team"),
            product=kwargs.get("product"),
        )

    def validate_name(self, name: str) -> bool:
        """
        Validate a name against the convention's pattern.

        Args:
            name: Name to validate

        Returns:
            True if name matches pattern (or no pattern configured)
        """
        if not self._name_generator:
            return True
        return self._name_generator.validate(name)

    def get_owner(
        self,
        securable_type: SecurableType,
        environment: Optional[Environment] = None,
    ) -> Optional[Principal]:
        """
        Get the owner Principal for a securable type.

        Args:
            securable_type: Type of securable
            environment: Environment (uses current if not specified)

        Returns:
            Principal or None if no owner configured
        """
        owner_spec = self._schema.get_owner_for_type(securable_type.value)
        if not owner_spec:
            return None

        env = environment or get_current_environment()

        # Map type string to PrincipalType enum
        type_map = {
            "USER": PrincipalType.USER,
            "GROUP": PrincipalType.GROUP,
            "SERVICE_PRINCIPAL": PrincipalType.SERVICE_PRINCIPAL,
        }
        principal_type = type_map.get(owner_spec.type.upper())

        return Principal(
            name=owner_spec.name,
            principal_type=principal_type,
            add_environment_suffix=owner_spec.add_environment_suffix,
            environment=env if not owner_spec.add_environment_suffix else None,
        )

    def get_catalog_owner(
        self,
        environment: Optional[Environment] = None,
    ) -> Optional[Principal]:
        """
        Get the owner Principal for catalogs.

        Convenience method for get_owner(SecurableType.CATALOG).

        Args:
            environment: Environment (uses current if not specified)

        Returns:
            Principal or None if no catalog owner configured
        """
        return self.get_owner(SecurableType.CATALOG, environment)

    def get_tags(
        self,
        environment: Optional[Environment] = None,
    ) -> List[Tag]:
        """
        Get default tags for the current environment.

        Args:
            environment: Environment (uses current if not specified)

        Returns:
            List of Tag objects
        """
        env = environment or get_current_environment()
        tag_dict = self._schema.get_tags_for_environment(env.value)
        return [Tag(key=k, value=v) for k, v in tag_dict.items()]

    def get_rfa(
        self,
        securable_type: SecurableType,
    ) -> Optional[RequestForAccess]:
        """
        Get Request for Access configuration for a securable type.

        Uses inheritance: Table → Schema → Catalog → default.

        Args:
            securable_type: Type of securable

        Returns:
            RequestForAccess or None if no RFA configured
        """
        rfa_spec = self._schema.get_rfa_for_type(securable_type.value)
        if not rfa_spec or not rfa_spec.destination:
            return None

        return RequestForAccess(
            destination=rfa_spec.destination,
            instructions=rfa_spec.instructions,
        )

    def apply_to(
        self,
        securable: Any,
        environment: Optional[Environment] = None,
    ) -> Any:
        """
        Apply convention to a securable (tags and RFA).

        Adds default tags without overwriting existing ones.
        Applies Request for Access if configured and not already set.
        Returns the securable for method chaining.

        Args:
            securable: Securable to apply convention to
            environment: Environment (uses current if not specified)

        Returns:
            The modified securable
        """
        env = environment or get_current_environment()
        tags = self.get_tags(env)

        existing_keys = {t.key for t in getattr(securable, "tags", [])}

        for tag in tags:
            if tag.key not in existing_keys:
                securable.tags.append(tag)
                logger.debug(f"Applied tag {tag.key}={tag.value} to {getattr(securable, 'name', 'unknown')}")

        # Apply Request for Access if not already set
        securable_type = getattr(securable, "securable_type", None)
        if securable_type and not getattr(securable, "request_for_access", None):
            rfa = self.get_rfa(securable_type)
            if rfa:
                securable.request_for_access = rfa
                logger.debug(f"Applied RFA (destination={rfa.destination}) to {getattr(securable, 'name', 'unknown')}")

        return securable

    def validate(
        self,
        securable: Any,
        context: Optional[Dict[str, Any]] = None,
    ) -> List[RuleValidationResult]:
        """
        Validate a securable against the convention's rules.

        Args:
            securable: Securable to validate
            context: Optional context dict passed to validators

        Returns:
            List of validation results (including failures)
        """
        results: List[RuleValidationResult] = []
        ctx = context or {}

        # Add owner info to context if available
        owner_spec = self._schema.get_owner_for_type(
            getattr(securable, "securable_type", SecurableType.CATALOG).value
        )
        if owner_spec:
            ctx["owner_info"] = {"type": owner_spec.type, "name": owner_spec.name}

        securable_type_value = getattr(securable, "securable_type", None)
        securable_type_str = securable_type_value.value if securable_type_value else None

        for rule_spec in self._schema.rules:
            # Check if rule applies to this securable type
            if rule_spec.applies_to and securable_type_str:
                if securable_type_str not in rule_spec.applies_to:
                    continue

            try:
                rule_def = self._registry.get(rule_spec.rule)
            except KeyError as e:
                logger.warning(f"Rule not found: {e}")
                continue

            # Check default applies_to from rule definition
            if rule_def.default_applies_to and securable_type_str:
                if securable_type_str not in rule_def.default_applies_to:
                    continue

            # Create validator with rule parameters
            params = {}
            if rule_spec.tags:
                params["tags"] = rule_spec.tags
            if rule_spec.pattern:
                params["pattern"] = rule_spec.pattern

            validator = rule_def.validator_factory(**params)
            result = validator(securable, ctx)

            # Store the mode on the result for later use
            result.mode = rule_spec.mode.value

            # For advisory mode, don't treat failures as blocking
            if not result.passed and rule_spec.mode == RuleMode.ADVISORY:
                logger.warning(f"[ADVISORY] {result.message}")

            results.append(result)

        return results

    def get_validation_errors(
        self,
        securable: Any,
        context: Optional[Dict[str, Any]] = None,
        include_advisory: bool = False,
    ) -> List[str]:
        """
        Get validation error messages for a securable.

        Args:
            securable: Securable to validate
            context: Optional context dict passed to validators
            include_advisory: Include advisory rule failures

        Returns:
            List of error messages (empty if valid)
        """
        results = self.validate(securable, context)
        errors = []

        for result in results:
            if result.passed:
                continue

            # Use the mode stored on the result (set during validate())
            is_advisory = result.mode == RuleMode.ADVISORY.value
            if is_advisory:
                if include_advisory:
                    errors.append(f"[ADVISORY] {result.message}")
            else:
                errors.append(result.message or f"Rule {result.rule_name} failed")

        return errors


def load_convention(
    path: str | Path,
    registry: Optional[RulesRegistry] = None,
) -> YamlConvention:
    """
    Load a convention from a YAML file.

    Args:
        path: Path to YAML file
        registry: Optional rules registry (uses default if not provided)

    Returns:
        YamlConvention instance

    Raises:
        FileNotFoundError: If file doesn't exist
        yaml.YAMLError: If YAML parsing fails
        ValidationError: If schema validation fails
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Convention file not found: {path}")

    with open(path, "r") as f:
        data = yaml.safe_load(f)

    schema = YamlConventionSchema.model_validate(data)
    logger.info(f"Loaded convention '{schema.convention}' from {path}")

    return YamlConvention(schema, registry)


def load_conventions_dir(
    directory: str | Path,
    registry: Optional[RulesRegistry] = None,
) -> Dict[str, YamlConvention]:
    """
    Load all conventions from a directory.

    Loads all .yml and .yaml files from the directory.

    Args:
        directory: Directory path
        registry: Optional rules registry (uses default if not provided)

    Returns:
        Dict mapping convention names to YamlConvention instances

    Raises:
        FileNotFoundError: If directory doesn't exist
    """
    directory = Path(directory)
    if not directory.exists():
        raise FileNotFoundError(f"Convention directory not found: {directory}")

    if not directory.is_dir():
        raise ValueError(f"Path is not a directory: {directory}")

    conventions: Dict[str, YamlConvention] = {}

    for ext in ("*.yml", "*.yaml"):
        for path in directory.glob(ext):
            try:
                convention = load_convention(path, registry)
                conventions[convention.name] = convention
                logger.info(f"Loaded convention: {convention.name}")
            except Exception as e:
                logger.error(f"Failed to load {path}: {e}")
                raise

    return conventions


__all__ = [
    "YamlConvention",
    "load_convention",
    "load_conventions_dir",
]
