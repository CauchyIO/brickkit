"""
Enterprise Governance Defaults

Standard organization-wide governance policies.
Suitable for most enterprise deployments.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from typing import List
from brickkit.defaults import GovernanceDefaults, TagDefault, RequiredTag, NamingConvention
from models.securables import Catalog, Schema
from models.base import Tag
from models.enums import Environment


class EnterpriseDefaults(GovernanceDefaults):
    """Standard enterprise governance defaults."""

    @property
    def default_tags(self) -> List[TagDefault]:
        return [
            # Managed-by tag for tracking
            TagDefault(key="managed_by", value="brickkit"),
            # Environment tag with env-specific values
            TagDefault(
                key="environment",
                value="development",
                environment_values={
                    "DEV": "development",
                    "ACC": "acceptance",
                    "PRD": "production",
                },
            ),
            # Default cost center
            TagDefault(key="cost_center", value="shared-platform"),
        ]

    @property
    def required_tags(self) -> List[RequiredTag]:
        return [
            # Catalogs must have a data owner
            RequiredTag(
                key="data_owner",
                applies_to={"CATALOG"},
                error_message="Catalogs must have a data_owner tag for accountability",
            ),
            # Tables must declare PII status
            RequiredTag(
                key="pii",
                allowed_values={"true", "false"},
                applies_to={"TABLE"},
                error_message="Tables must declare pii=true or pii=false",
            ),
        ]

    @property
    def naming_conventions(self) -> List[NamingConvention]:
        return [
            # Catalogs: lowercase with underscores
            NamingConvention(
                pattern=r"^[a-z][a-z0-9_]*$",
                applies_to={"CATALOG"},
                error_message="Catalog names must be lowercase with underscores",
            ),
        ]

    @property
    def default_owner(self) -> str:
        return "platform-team"


# Usage
defaults = EnterpriseDefaults()

# Create catalog with defaults applied
catalog = Catalog(
    name="sales",
    tags=[Tag(key="data_owner", value="sales-team")],  # Required tag
)

# Apply defaults (adds managed_by, environment, cost_center)
catalog = defaults.apply_to(catalog, Environment.DEV)

print("Tags after applying defaults:")
for tag in catalog.tags:
    print(f"  {tag.key}: {tag.value}")

# Validate
tag_dict = {t.key: t.value for t in catalog.tags}
errors = defaults.validate_tags(catalog.securable_type, tag_dict)
print(f"\nValidation errors: {errors or 'None'}")

# Output:
# Tags after applying defaults:
#   data_owner: sales-team
#   managed_by: brickkit
#   environment: development
#   cost_center: shared-platform
#
# Validation errors: None
