# Databricks notebook source
"""
Example: Using a Project Manifest for governance configuration.

This example shows how teams can define governance rules in a JSON manifest
file and load it at runtime, instead of writing Python classes.
"""

from pathlib import Path

from loguru import logger

from brickkit import (
    Catalog,
    Tag,
    load_project_manifest,
)
from brickkit.models.base import get_current_environment, init_environment

# COMMAND ----------

env = init_environment()

# Load the manifest from JSON
manifest_path = Path(__file__).parent / "project.manifest.json"
defaults = load_project_manifest(manifest_path)

# Access manifest metadata
logger.info(f"Organization: {defaults.organization}")
logger.info(f"Default owner: {defaults.default_owner}")
logger.info(f"Default tags: {[t.key for t in defaults.default_tags]}")
logger.info(f"Required tags: {[t.key for t in defaults.required_tags]}")

# COMMAND ----------

# Create a catalog with required tags
catalog = Catalog(
    name="sales_analytics",
    tags=[
        Tag(key="cost_center", value="cc_engineering_002"),
        Tag(key="data_classification", value="internal"),
        Tag(key="data_owner", value="sales_team"),
    ],
)

# COMMAND ----------

# Apply defaults - adds managed_by, business_unit, environment
catalog = defaults.apply_to(catalog, defaults.manifest.version)

# Validate against governance rules
env = get_current_environment()
errors = defaults.validate_tags(catalog.securable_type, {t.key: t.value for t in catalog.tags})

if errors:
    logger.info(f"Validation errors: {errors}")
else:
    logger.info("Catalog passes governance validation")

# Print resulting tags
logger.info("\nCatalog tags after applying defaults:")
for tag in catalog.tags:
    logger.info(f"  {tag.key}: {tag.value}")
