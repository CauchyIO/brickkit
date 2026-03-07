# Databricks notebook source
"""
Simple Grants Example

Shows how to grant access using Principal and AccessPolicy.
Demonstrates the three standard policies: READER, WRITER, ADMIN.
"""

from loguru import logger

from brickkit.models.base import init_environment
from brickkit.models.catalogs import Catalog
from brickkit.models.enums import IsolationMode
from brickkit.models.grants import AccessPolicy, Principal
from brickkit.models.schemas import Schema

# COMMAND ----------

env = init_environment()

# Create catalog and schema
catalog = Catalog(
    name="analytics",
    isolation_mode=IsolationMode.OPEN,
)
schema = Schema(name="reports")
catalog.add_schema(schema)

# Define principals (groups/users)
# Principals get environment suffix by default: analysts_dev, analysts_acc, etc.
analysts = Principal(name="analysts")
data_engineers = Principal(name="data_engineers")

# Special principals (no suffix)
all_users = Principal.all_workspace_users()  # Built-in 'users' group

logger.info(f"analysts resolved: {analysts.resolved_name}")
logger.info(f"data_engineers resolved: {data_engineers.resolved_name}")
logger.info(f"all_users resolved: {all_users.resolved_name}")

# Grant access using predefined policies
# READER: SELECT, BROWSE, USE_CATALOG, USE_SCHEMA
catalog.grant(analysts, AccessPolicy.READER())

# WRITER: READER + CREATE_TABLE, MODIFY, CREATE_SCHEMA
catalog.grant(data_engineers, AccessPolicy.WRITER())

# Collect privileges for review
logger.info(f"\nPrivileges on catalog '{catalog.resolved_name}':")
for priv in catalog.privileges:
    logger.info(f"  {priv.principal}: {priv.privilege.value}")

# Output (when DATABRICKS_ENV=dev):
# analysts resolved: analysts_dev
# data_engineers resolved: data_engineers_dev
# all_users resolved: users
#
# Privileges on catalog 'analytics_dev':
#   analysts_dev: USE_CATALOG
#   analysts_dev: BROWSE
#   data_engineers_dev: USE_CATALOG
#   data_engineers_dev: CREATE_SCHEMA
