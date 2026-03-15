# Databricks notebook source
"""
Basic Catalog Example

Creates a simple catalog with environment-aware naming and tags.
The catalog name automatically gets a suffix based on DATABRICKS_ENV.
"""

from loguru import logger

from brickkit.models.base import Tag, get_current_environment, init_environment
from brickkit.models.catalogs import Catalog
from brickkit.models.enums import IsolationMode
from brickkit.models.schemas import Schema

# COMMAND ----------

env = init_environment()

# Create a basic catalog
# Name will resolve to "analytics_dev", "analytics_acc", or "analytics_prd"
# based on DATABRICKS_ENV environment variable
catalog = Catalog(
    name="analytics",
    comment="Analytics domain data products",
    isolation_mode=IsolationMode.OPEN,
    tags=[
        Tag(key="domain", value="analytics"),
        Tag(key="cost_center", value="data-platform"),
    ],
)

# Check resolved name (includes environment suffix)
logger.info(f"Environment: {get_current_environment()}")
logger.info(f"Base name: {catalog.name}")
logger.info(f"Resolved name: {catalog.resolved_name}")
logger.info(f"Tags: {[(t.key, t.value) for t in catalog.tags]}")

# Add schemas to the catalog
bronze = Schema(name="bronze", comment="Raw landing zone")
silver = Schema(name="silver", comment="Cleansed data")
gold = Schema(name="gold", comment="Business-ready aggregates")

catalog.add_schema(bronze)
catalog.add_schema(silver)
catalog.add_schema(gold)

logger.info(f"\nSchemas: {[s.name for s in catalog.schemas]}")

# Output example (when DATABRICKS_ENV=dev):
# Environment: Environment.DEV
# Base name: analytics
# Resolved name: analytics_dev
# Tags: [('domain', 'analytics'), ('cost_center', 'data-platform')]
# Schemas: ['bronze', 'silver', 'gold']
