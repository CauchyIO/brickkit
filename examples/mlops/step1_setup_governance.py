# Databricks notebook source
"""
Step 1: Setup Governance Foundation

Creates the shared governance infrastructure:
  - Budget policy for serverless cost attribution
  - Databricks group that will own all AI/ML workspace objects

Results are written to governance_config.yml in this directory.
Run this once per environment before running step2_apply_governance.py.
"""

import logging
from pathlib import Path

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.billing import CreateBudgetPolicyRequest

from brickkit.executors import GroupExecutor
from brickkit.models.principals import ManagedGroup

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# COMMAND ----------

# =============================================================================
# CONFIGURATION
# =============================================================================

GROUP_NAME = "grp_ai_platform"
BUDGET_POLICY_NAME = "ai-platform-serverless"
CONFIG_PATH = Path(__file__).parent / "governance_config.yml"

DRY_RUN = True  # Set to False to apply changes

# COMMAND ----------

# =============================================================================
# CLIENT
# =============================================================================

client = WorkspaceClient()

# COMMAND ----------

# =============================================================================
# BUDGET POLICY
# =============================================================================

logger.info("--- Budget Policy ---")

if DRY_RUN:
    usage_policy_id = "dry-run-policy-id"
    logger.info(f"[DRY RUN] Would create budget policy: {BUDGET_POLICY_NAME}")
else:
    existing = next(
        (p for p in client.budget_policies.list() if p.policy_name == BUDGET_POLICY_NAME),
        None,
    )
    if existing:
        usage_policy_id = existing.policy_id
        logger.info(f"Already exists: {BUDGET_POLICY_NAME} (id={usage_policy_id})")
    else:
        policy = client.budget_policies.create(
            CreateBudgetPolicyRequest(policy_name=BUDGET_POLICY_NAME)
        )
        usage_policy_id = policy.policy_id
        logger.info(f"Created: {BUDGET_POLICY_NAME} (id={usage_policy_id})")

# COMMAND ----------

# =============================================================================
# GROUP
# =============================================================================

logger.info("--- Group ---")

group = ManagedGroup(name=GROUP_NAME, add_environment_suffix=False)
# group.add_user("alice@company.com")
# group.add_entitlement("workspace-access")

result = GroupExecutor(client, dry_run=DRY_RUN).create(group)
logger.info(f"Group '{group.resolved_name}': {result.operation.value} - {result.message}")

# COMMAND ----------

# =============================================================================
# WRITE CONFIG
# =============================================================================

config = {
    "usage_policy_id": usage_policy_id,
    "group_id": GROUP_NAME,
    "policy_name": BUDGET_POLICY_NAME,
}

if DRY_RUN:
    logger.info(f"[DRY RUN] Would write config to {CONFIG_PATH}:\n{yaml.dump(config)}")
else:
    CONFIG_PATH.write_text(yaml.dump(config, default_flow_style=False))
    logger.info(f"Config written to {CONFIG_PATH}")
