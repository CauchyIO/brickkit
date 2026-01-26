"""
Financial Services Governance Defaults

Strict regulatory compliance for banking/finance.
Implements GDPR, SOX, Basel III requirements.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from typing import List, Optional
from brickkit.defaults import GovernanceDefaults, TagDefault, RequiredTag, NamingConvention
from models.securables import Catalog, Schema
from models.base import Tag
from models.enums import Environment, SecurableType


class FinancialServicesDefaults(GovernanceDefaults):
    """
    Strict governance for financial services.

    Requirements:
    - All data must have classification
    - PII/PCI data must be explicitly tagged
    - Retention periods must be defined
    - Audit trail mandatory
    """

    @property
    def default_tags(self) -> List[TagDefault]:
        return [
            TagDefault(key="managed_by", value="brickkit"),
            TagDefault(key="compliance_framework", value="sox,gdpr"),
            TagDefault(key="audit_enabled", value="true"),
            # Environment-aware retention
            TagDefault(
                key="retention_days",
                value="90",  # Dev: 90 days
                environment_values={
                    "ACC": "180",  # Acceptance: 6 months
                    "PRD": "2555",  # Production: 7 years (SOX)
                },
            ),
        ]

    @property
    def required_tags(self) -> List[RequiredTag]:
        return [
            # Data classification is mandatory on all securables
            RequiredTag(
                key="data_classification",
                allowed_values={"public", "internal", "confidential", "restricted"},
                error_message="All assets must have data_classification (public/internal/confidential/restricted)",
            ),
            # Catalogs need owner and cost center
            RequiredTag(
                key="data_owner",
                applies_to={"CATALOG", "SCHEMA"},
                error_message="Data owner is mandatory for cost allocation and accountability",
            ),
            RequiredTag(
                key="cost_center", applies_to={"CATALOG"}, error_message="Cost center required for financial reporting"
            ),
            # Tables need PII/PCI declaration
            RequiredTag(
                key="contains_pii",
                allowed_values={"true", "false"},
                applies_to={"TABLE"},
                error_message="Tables must declare contains_pii status",
            ),
            RequiredTag(
                key="contains_pci",
                allowed_values={"true", "false"},
                applies_to={"TABLE"},
                error_message="Tables must declare contains_pci status (payment card data)",
            ),
            # Data lineage requirement
            RequiredTag(
                key="source_system", applies_to={"TABLE"}, error_message="Tables must declare source_system for lineage"
            ),
        ]

    @property
    def naming_conventions(self) -> List[NamingConvention]:
        return [
            # Strict naming: domain_function format
            NamingConvention(
                pattern=r"^[a-z]+_[a-z][a-z0-9_]*$",
                applies_to={"CATALOG"},
                error_message="Catalog names must follow domain_function format (e.g., finance_reporting)",
            ),
            # Schemas: zone_subject format
            NamingConvention(
                pattern=r"^(bronze|silver|gold|raw|staging|curated)_[a-z][a-z0-9_]*$",
                applies_to={"SCHEMA"},
                error_message="Schema names must follow zone_subject format (e.g., gold_customers)",
            ),
        ]

    @property
    def default_owner(self) -> str:
        return "data-governance-office"


# Usage example
defaults = FinancialServicesDefaults()

# Create compliant catalog
catalog = Catalog(
    name="finance_reporting",  # Follows domain_function format
    tags=[
        Tag(key="data_owner", value="cfo-office"),
        Tag(key="cost_center", value="CC-FIN-001"),
        Tag(key="data_classification", value="confidential"),
    ],
)

# Apply defaults
catalog = defaults.apply_to(catalog, Environment.PRD)

print("Financial Services Catalog Tags (PRD):")
for tag in sorted(catalog.tags, key=lambda t: t.key):
    print(f"  {tag.key}: {tag.value}")

# Validate
tag_dict = {t.key: t.value for t in catalog.tags}
errors = defaults.validate_tags(catalog.securable_type, tag_dict)
print(f"\nValidation: {'PASSED' if not errors else 'FAILED'}")
for err in errors:
    print(f"  - {err}")

# Show what a non-compliant table would look like
print("\n--- Non-compliant table example ---")
non_compliant_tags = {"data_classification": "internal"}  # Missing PII, PCI, source_system
errors = defaults.validate_tags(SecurableType.TABLE, non_compliant_tags)
print("Missing required tags:")
for err in errors:
    print(f"  - {err}")

# Output:
# Financial Services Catalog Tags (PRD):
#   audit_enabled: true
#   compliance_framework: sox,gdpr
#   cost_center: CC-FIN-001
#   data_classification: confidential
#   data_owner: cfo-office
#   managed_by: brickkit
#   retention_days: 2555
#
# Validation: PASSED
#
# --- Non-compliant table example ---
# Missing required tags:
#   - Tables must declare contains_pii status
#   - Tables must declare contains_pci status (payment card data)
#   - Tables must declare source_system for lineage
