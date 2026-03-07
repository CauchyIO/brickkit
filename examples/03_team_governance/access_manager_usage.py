# Databricks notebook source
"""
Access Manager Example

AccessManager provides team-level orchestration for grants:
- Centralized grant tracking for audit
- Bulk operations for common patterns
- Team-specific access organization
"""

from loguru import logger

from brickkit.models.base import init_environment
from brickkit.models.catalogs import Catalog
from brickkit.models.enums import IsolationMode
from brickkit.models.grants import AccessPolicy, Principal
from brickkit.models.schemas import Schema
from brickkit.models.teams import AccessManager

# COMMAND ----------

env = init_environment()

# Create catalog with schemas
catalog = Catalog(name="sales", isolation_mode=IsolationMode.OPEN)
bronze = Schema(name="bronze")
silver = Schema(name="silver")
gold = Schema(name="gold")

catalog.add_schema(bronze)
catalog.add_schema(silver)
catalog.add_schema(gold)

# COMMAND ----------

# Create AccessManager for the team
manager = AccessManager(team_name="sales_team")

# Define principals
data_engineers = Principal(name="data_engineers")
analysts = Principal(name="analysts")
executives = Principal(name="executives")

# COMMAND ----------

# Grant access through the manager (tracks for audit)

# Data engineers: full write access
manager.grant(data_engineers, catalog, AccessPolicy.WRITER())

# Analysts: read access to silver and gold only
manager.grant(analysts, silver, AccessPolicy.READER())
manager.grant(analysts, gold, AccessPolicy.READER())

# Executives: read access to gold only
manager.grant(executives, gold, AccessPolicy.READER())

# COMMAND ----------

# Review grants
logger.info(f"=== Grants by {manager.team_name} ===\n")
for grant in manager.grants:
    logger.info(f"Principal: {grant['principal']}")
    logger.info(f"  Securable: {grant['securable_type']} '{grant['securable_name']}'")
    logger.info(f"  Policy: {grant['policy']}")
    logger.info()

# COMMAND ----------

# Query grants by principal
logger.info("=== Grants for 'analysts' ===")
analyst_grants = manager.get_grants_for_principal("analysts_dev")
for g in analyst_grants:
    logger.info(f"  {g['securable_type']} '{g['securable_name']}': {g['policy']}")

# Query grants by securable
logger.info("\n=== Grants on 'gold' schema ===")
gold_grants = manager.get_grants_for_securable("gold")
for g in gold_grants:
    logger.info(f"  {g['principal']}: {g['policy']}")

# COMMAND ----------

# Bulk grant to all schemas
logger.info("\n=== Bulk grant to all schemas ===")
auditors = Principal(name="auditors")
manager.grant_to_all_schemas(auditors, catalog, AccessPolicy.BROWSE_ONLY())
logger.info("Granted BROWSE_ONLY to auditors on catalog and all schemas")
logger.info(f"Total grants recorded: {len(manager.grants)}")

# Output:
# === Grants by sales_team ===
#
# Principal: data_engineers_dev
#   Securable: CATALOG 'sales'
#   Policy: WRITER
#
# Principal: analysts_dev
#   Securable: SCHEMA 'silver'
#   Policy: READER
# ...
