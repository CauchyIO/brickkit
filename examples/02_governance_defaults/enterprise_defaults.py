# Databricks notebook source
"""
Enterprise Governance Defaults

Standard organization-wide governance policies.
Suitable for most enterprise deployments.
"""

from typing import List

from loguru import logger

from brickkit.defaults import GovernanceDefaults, NamingConvention, RequiredTag, TagDefault
from brickkit.models.base import Tag, init_environment
from brickkit.models.catalogs import Catalog
from brickkit.models.enums import Environment

# COMMAND ----------

env = init_environment()


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


# COMMAND ----------

# Usage
defaults = EnterpriseDefaults()

# Create catalog with defaults applied
catalog = Catalog(
    name="sales",
    tags=[Tag(key="data_owner", value="sales-team")],  # Required tag
)

# Apply defaults (adds managed_by, environment, cost_center)
catalog = defaults.apply_to(catalog, Environment.DEV)

logger.info("Tags after applying defaults:")
for tag in catalog.tags:
    logger.info(f"  {tag.key}: {tag.value}")

# Validate
tag_dict = {t.key: t.value for t in catalog.tags}
errors = defaults.validate_tags(catalog.securable_type, tag_dict)
logger.info(f"\nValidation errors: {errors or 'None'}")

# Output:
# Tags after applying defaults:
#   data_owner: sales-team
#   managed_by: brickkit
#   environment: development
#   cost_center: shared-platform
#
# Validation errors: None
