"""
YAML-first convention system for declarative governance rules.

This package allows architects to define governance conventions in YAML
files instead of writing Python code.

Quick Start:
    from brickkit import load_convention, Catalog, SecurableType

    # Load convention from YAML file
    convention = load_convention("conventions/financial_services.yml")

    # Generate a compliant name
    name = convention.generate_name(SecurableType.CATALOG)

    # Get the catalog owner
    owner = convention.get_catalog_owner()

    # Create catalog and apply convention
    catalog = Catalog(name=name, owner=owner.resolved_name)
    convention.apply_to(catalog)  # Adds tags

    # Validate against rules
    errors = convention.get_validation_errors(catalog)

Example YAML Convention:
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

Built-in Rules:
    - catalog_must_have_sp_owner: Catalogs must be owned by service principals
    - owner_must_be_sp_or_group: No individual user owners allowed
    - require_tags: Specified tags must be present
    - naming_pattern: Names must match a regex pattern
"""

from .loader import YamlConvention, load_convention, load_conventions_dir
from .naming import NameGenerator
from .rules import (
    RuleDefinition,
    RulesRegistry,
    RuleValidationResult,
    create_default_registry,
    get_default_registry,
)
from .schema import (
    NamingSpec,
    OwnershipSpec,
    RuleMode,
    RuleSpec,
    YamlConventionSchema,
)

__all__ = [
    # Main loader functions
    "load_convention",
    "load_conventions_dir",
    # Convention wrapper
    "YamlConvention",
    # Rules
    "RulesRegistry",
    "RuleDefinition",
    "RuleValidationResult",
    "create_default_registry",
    "get_default_registry",
    # Schema models
    "YamlConventionSchema",
    "NamingSpec",
    "OwnershipSpec",
    "RuleSpec",
    "RuleMode",
    # Name generation
    "NameGenerator",
]
