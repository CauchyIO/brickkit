"""
Name generation utilities for YAML conventions.

This module provides pattern-based name generation that supports
placeholders like {env}, {team}, {product}, and {acronym}.
"""

from __future__ import annotations

import re
from typing import Optional

from brickkit.models.enums import Environment, SecurableAcronym, SecurableType

from .schema import NamingSpec


class NameGenerator:
    """
    Pattern-based name generator for securables.

    Supports placeholders:
    - {env}: Current environment (dev, acc, prd)
    - {team}: Team name from naming spec
    - {product}: Product name from naming spec
    - {acronym}: Securable type acronym (cat, sch, tbl, etc.)
    - {name}: Base name provided at generation time

    Example:
        spec = NamingSpec(pattern="{env}_{team}_{product}_{acronym}", team="quant", product="risk")
        gen = NameGenerator(spec)
        name = gen.generate(SecurableType.CATALOG, Environment.DEV)
        # Returns: "dev_quant_risk_cat"
    """

    def __init__(self, spec: NamingSpec) -> None:
        """
        Initialize the name generator.

        Args:
            spec: NamingSpec with pattern and default values
        """
        self.spec = spec
        self._pattern_regex = self._compile_validation_regex()

    def _compile_validation_regex(self) -> Optional[re.Pattern[str]]:
        """
        Compile a regex for validating names against the pattern.

        Converts placeholders to regex groups for validation.
        """
        pattern = self.spec.pattern

        # Escape special regex characters except our placeholders
        escaped = re.escape(pattern)

        # Replace escaped placeholders with regex patterns
        # Keys are the escaped form (what re.escape produces from "{env}")
        # Values are the regex capture groups to replace them with
        placeholder_regex = {
            r"\{env\}": r"(?P<env>dev|acc|prd)",
            r"\{team\}": r"(?P<team>[a-z0-9_]+)",
            r"\{product\}": r"(?P<product>[a-z0-9_]+)",
            r"\{acronym\}": r"(?P<acronym>[a-z]+)",
            r"\{name\}": r"(?P<name>[a-z0-9_]+)",
        }

        for escaped_placeholder, regex in placeholder_regex.items():
            escaped = escaped.replace(escaped_placeholder, regex)

        try:
            return re.compile(f"^{escaped}$", re.IGNORECASE)
        except re.error:
            return None

    def generate(
        self,
        securable_type: SecurableType,
        environment: Environment,
        name: Optional[str] = None,
        team: Optional[str] = None,
        product: Optional[str] = None,
    ) -> str:
        """
        Generate a name based on the pattern.

        Args:
            securable_type: Type of securable to generate name for
            environment: Current environment
            name: Optional base name for {name} placeholder
            team: Override team name (uses spec default if not provided)
            product: Override product name (uses spec default if not provided)

        Returns:
            Generated name string

        Raises:
            ValueError: If required placeholder values are missing
        """
        pattern = self.spec.pattern

        # Get acronym for securable type
        try:
            acronym = SecurableAcronym.from_securable_type(securable_type).value
        except ValueError:
            acronym = securable_type.value.lower()[:3]

        # Build replacement values
        replacements = {
            "{env}": environment.value.lower(),
            "{acronym}": acronym,
        }

        # Add team if in pattern
        if "{team}" in pattern:
            team_value = team or self.spec.team
            if not team_value:
                raise ValueError("Team name required for pattern but not provided")
            replacements["{team}"] = team_value

        # Add product if in pattern
        if "{product}" in pattern:
            product_value = product or self.spec.product
            if not product_value:
                raise ValueError("Product name required for pattern but not provided")
            replacements["{product}"] = product_value

        # Add name if in pattern
        if "{name}" in pattern:
            if not name:
                raise ValueError("Name required for pattern but not provided")
            replacements["{name}"] = name

        # Apply replacements
        result = pattern
        for placeholder, value in replacements.items():
            result = result.replace(placeholder, value)

        return result

    def validate(self, name: str) -> bool:
        """
        Validate a name against the pattern.

        Args:
            name: Name to validate

        Returns:
            True if name matches the pattern
        """
        if not self._pattern_regex:
            return True  # No pattern to validate against
        return bool(self._pattern_regex.match(name))

    def parse(self, name: str) -> Optional[dict[str, str]]:
        """
        Parse a name to extract placeholder values.

        Args:
            name: Name to parse

        Returns:
            Dict of placeholder name -> value, or None if no match
        """
        if not self._pattern_regex:
            return None

        match = self._pattern_regex.match(name)
        if not match:
            return None

        return match.groupdict()


__all__ = [
    "NameGenerator",
]
