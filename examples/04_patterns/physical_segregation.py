# Databricks notebook source
"""
Physical Segregation Pattern

Separate data by classification tier into distinct catalogs.
Each tier has different security controls and access policies.

Tiers:
- PUBLIC: No restrictions, can be shared externally
- INTERNAL: Organization-wide access
- CONFIDENTIAL: Restricted to specific groups
- RESTRICTED: PII/PCI, need-to-know only
"""

from typing import Dict

from loguru import logger

from brickkit.models.base import Tag, init_environment
from brickkit.models.catalogs import Catalog
from brickkit.models.enums import IsolationMode
from brickkit.models.grants import AccessPolicy, Principal
from brickkit.models.schemas import Schema

# COMMAND ----------

env = init_environment()


def create_tiered_catalogs() -> Dict[str, Catalog]:
    """Create catalogs for each data classification tier."""

    tiers = {
        "public": {
            "isolation": IsolationMode.OPEN,
            "tags": [
                Tag(key="data_classification", value="public"),
                Tag(key="encryption", value="optional"),
            ],
            "access": [
                (Principal.all_workspace_users(), AccessPolicy.READER()),
            ],
        },
        "internal": {
            "isolation": IsolationMode.OPEN,
            "tags": [
                Tag(key="data_classification", value="internal"),
                Tag(key="encryption", value="at_rest"),
            ],
            "access": [
                (Principal(name="employees"), AccessPolicy.READER()),
                (Principal(name="data_engineers"), AccessPolicy.WRITER()),
            ],
        },
        "confidential": {
            "isolation": IsolationMode.ISOLATED,
            "tags": [
                Tag(key="data_classification", value="confidential"),
                Tag(key="encryption", value="at_rest_and_transit"),
                Tag(key="audit_level", value="enhanced"),
            ],
            "access": [
                (Principal(name="authorized_analysts"), AccessPolicy.READER()),
            ],
        },
        "restricted": {
            "isolation": IsolationMode.ISOLATED,
            "tags": [
                Tag(key="data_classification", value="restricted"),
                Tag(key="encryption", value="hsm"),
                Tag(key="audit_level", value="detailed"),
                Tag(key="compliance", value="gdpr,pci"),
            ],
            "access": [
                (Principal(name="privacy_officers"), AccessPolicy.READER()),
            ],
        },
    }

    catalogs = {}

    for tier_name, config in tiers.items():
        catalog = Catalog(
            name=f"tier_{tier_name}",
            comment=f"{tier_name.title()} classified data",
            isolation_mode=config["isolation"],
            tags=config["tags"],
        )

        # Apply access policies
        for principal, policy in config["access"]:
            catalog.grant(principal, policy)

        catalogs[tier_name] = catalog

    return catalogs


# COMMAND ----------

# Create the tiered structure
catalogs = create_tiered_catalogs()

# Display the structure
for tier_name, catalog in catalogs.items():
    logger.info(f"\n=== {tier_name.upper()} TIER ===")
    logger.info(f"Catalog: {catalog.resolved_name}")
    logger.info(f"Isolation: {catalog.isolation_mode.value}")
    logger.info("Tags:")
    for tag in catalog.tags:
        logger.info(f"  {tag.key}: {tag.value}")
    logger.info("Access:")
    for priv in catalog.privileges:
        logger.info(f"  {priv.principal}: {priv.privilege.value}")


# COMMAND ----------

# Example: Adding data to the right tier
logger.info("\n\n=== Example Usage ===")
logger.info("Customer emails → RESTRICTED tier (PII)")
logger.info("Sales aggregates → INTERNAL tier (no PII)")
logger.info("Public reports → PUBLIC tier")

# Adding schema to restricted tier
pii_catalog = catalogs["restricted"]
pii_catalog.add_schema(
    Schema(
        name="customer_pii",
        comment="Raw customer PII - access restricted",
        tags=[Tag(key="contains_pii", value="true")],
    )
)

logger.info(f"\nAdded 'customer_pii' schema to {pii_catalog.resolved_name}")

# Output:
# === PUBLIC TIER ===
# Catalog: tier_public_dev
# Isolation: OPEN
# Tags:
#   data_classification: public
#   encryption: optional
# Access:
#   users: USE_CATALOG
#   users: BROWSE
# ...
