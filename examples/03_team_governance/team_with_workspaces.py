# Databricks notebook source
"""
Team with Workspaces Example

Shows how to define a team with workspace assignments per environment.
Teams are the organizational unit that connects workspaces, principals, and catalogs.
"""

from loguru import logger

from brickkit.models.base import init_environment
from brickkit.models.catalogs import Catalog
from brickkit.models.enums import Environment, IsolationMode
from brickkit.models.grants import Principal
from brickkit.models.teams import Team
from brickkit.models.workspace_bindings import WorkspaceBindingPattern, WorkspaceRegistry

# COMMAND ----------

env = init_environment()

# Clear registry for clean example
WorkspaceRegistry().clear()

# Define workspaces (these must pre-exist in Databricks)
registry = WorkspaceRegistry()

ws_dev = registry.get_or_create(
    workspace_id="1234567890",
    name="analytics-dev",
    hostname="analytics-dev.cloud.databricks.com",
    environment=Environment.DEV,
)

ws_acc = registry.get_or_create(
    workspace_id="1234567891",
    name="analytics-acc",
    hostname="analytics-acc.cloud.databricks.com",
    environment=Environment.ACC,
)

ws_prd = registry.get_or_create(
    workspace_id="1234567892",
    name="analytics-prd",
    hostname="analytics-prd.cloud.databricks.com",
    environment=Environment.PRD,
)

# COMMAND ----------

# Create team with workspace assignments
analytics_team = Team(
    name="analytics",
    binding_pattern=WorkspaceBindingPattern.STANDARD_HIERARCHY(),
)

# Add workspaces to team
analytics_team.add_workspace(ws_dev)
analytics_team.add_workspace(ws_acc)
analytics_team.add_workspace(ws_prd)

# Add team members
analytics_team.add_principal(Principal(name="alice", add_environment_suffix=False))
analytics_team.add_principal(Principal(name="bob", add_environment_suffix=False))
analytics_team.add_principal(Principal(name="analytics_service_principal"))

logger.info(f"Team: {analytics_team.name}")
logger.info(f"Binding pattern: {analytics_team.binding_pattern.name}")
logger.info("\nWorkspaces:")
for env, ws in analytics_team.workspaces.items():
    logger.info(f"  {env.value}: {ws.name} ({ws.workspace_id})")

logger.info("\nPrincipals:")
for p in analytics_team.principals:
    logger.info(f"  {p.name} -> {p.resolved_name}")

# COMMAND ----------

# Create an ISOLATED catalog and bind to team's workspaces
catalog = Catalog(
    name="analytics_data",
    isolation_mode=IsolationMode.ISOLATED,
)

# Team.add_catalog automatically sets workspace_ids based on binding pattern
analytics_team.add_catalog(catalog)

logger.info(f"\nCatalog: {catalog.resolved_name}")
logger.info(f"Isolation mode: {catalog.isolation_mode.value}")
logger.info(f"Bound to workspace IDs: {catalog.workspace_ids}")

# Output:
# Team: analytics
# Binding pattern: STANDARD_HIERARCHY
#
# Workspaces:
#   DEV: analytics-dev (1234567890)
#   ACC: analytics-acc (1234567891)
#   PRD: analytics-prd (1234567892)
#
# Principals:
#   alice -> alice
#   bob -> bob
#   analytics_service_principal -> analytics_service_principal_dev
#
# Catalog: analytics_data_dev
# Isolation mode: ISOLATED
# Bound to workspace IDs: [1234567890, 1234567891, 1234567892]
