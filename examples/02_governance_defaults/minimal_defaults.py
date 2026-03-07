# Databricks notebook source
"""
Minimal Governance Defaults

Lightweight governance for startups or small teams.
Only the essentials, no bureaucracy.
"""

from typing import List

from loguru import logger

from brickkit.defaults import EmptyDefaults, GovernanceDefaults, RequiredTag, TagDefault
from brickkit.models.base import init_environment
from brickkit.models.catalogs import Catalog
from brickkit.models.enums import Environment

# COMMAND ----------

env = init_environment()


class MinimalDefaults(GovernanceDefaults):
    """
    Minimal governance for small teams.

    Just enough structure to:
    - Track who manages what
    - Know the environment
    """

    @property
    def default_tags(self) -> List[TagDefault]:
        return [
            TagDefault(key="managed_by", value="brickkit"),
        ]

    @property
    def required_tags(self) -> List[RequiredTag]:
        # No required tags - trust the team
        return []


# COMMAND ----------

# Compare: EmptyDefaults vs MinimalDefaults

logger.info("=== EmptyDefaults (no governance) ===")
empty = EmptyDefaults()
catalog1 = Catalog(name="analytics")
catalog1 = empty.apply_to(catalog1, Environment.DEV)
logger.info(f"Tags: {[(t.key, t.value) for t in catalog1.tags]}")

logger.info("\n=== MinimalDefaults (just tracking) ===")
minimal = MinimalDefaults()
catalog2 = Catalog(name="analytics")
catalog2 = minimal.apply_to(catalog2, Environment.DEV)
logger.info(f"Tags: {[(t.key, t.value) for t in catalog2.tags]}")

# Output:
# === EmptyDefaults (no governance) ===
# Tags: []
#
# === MinimalDefaults (just tracking) ===
# Tags: [('managed_by', 'brickkit')]
