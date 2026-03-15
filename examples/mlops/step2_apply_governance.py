# Databricks notebook source
"""
Step 2: Apply Governance to AI/ML Workspace Objects

Reads governance_config.yml (written by step1_setup_governance.py) and applies:
  - MLflow experiment: ensure the group is owner (create if missing, update if found)
  - Model serving endpoint: attach budget policy + group ownership
  - Vector search endpoint: attach budget policy + group ownership
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import PermissionLevel

from brickkit.executors import (
    MlflowExperimentExecutor,
    ModelServingEndpointExecutor,
    VectorSearchEndpointExecutor,
)
from brickkit.models.experiments import MlflowExperiment
from brickkit.models.grants import Principal
from brickkit.models.ml_models import ModelServingEndpoint
from brickkit.models.vector_search import VectorSearchEndpoint

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# COMMAND ----------

# =============================================================================
# LOAD CONFIG (written by step1_setup_governance.py)
# =============================================================================

CONFIG_PATH = Path(__file__).parent / "governance_config.yml"
config = yaml.safe_load(CONFIG_PATH.read_text())

USAGE_POLICY_ID: str = config["usage_policy_id"]
GROUP_NAME: str = config["group_id"]

DRY_RUN = True  # Set to False to apply changes

client = WorkspaceClient()

# COMMAND ----------

# =============================================================================
# HELPERS
# =============================================================================


def ensure_experiment_owner(name: str, group_name: str) -> None:
    """Ensure the group has CAN_MANAGE on an MLflow experiment.

    Creates the experiment if it does not exist; updates the owner ACL if it does.
    Budget policy is not applicable to MLflow experiments.
    """
    experiment = MlflowExperiment(
        name=name,
        owner=Principal(name=group_name, add_environment_suffix=False),
    )
    executor = MlflowExperimentExecutor(client, dry_run=DRY_RUN)
    result = executor.update(experiment) if executor.exists(experiment) else executor.create(experiment)
    logger.info(f"Experiment '{experiment.resolved_name}': {result.operation.value} - {result.message}")


def ensure_serving_endpoint(
    name: str,
    group_name: str,
    usage_policy_id: str,
    served_models: Optional[list] = None,
) -> None:
    """Create or update a model serving endpoint with budget policy + group ownership."""
    endpoint = ModelServingEndpoint(
        name=name,
        budget_policy_id=usage_policy_id,
        owner=Principal(name=group_name, add_environment_suffix=False),
        config={"served_models": served_models or []},
    )
    executor = ModelServingEndpointExecutor(client, dry_run=DRY_RUN)
    result = executor.update(endpoint) if executor.exists(endpoint) else executor.create(endpoint)
    logger.info(f"Serving endpoint '{endpoint.resolved_name}': {result.operation.value} - {result.message}")


def ensure_vector_search_endpoint(name: str, group_name: str, usage_policy_id: str) -> None:
    """Create or update a vector search endpoint with budget policy + group ownership."""
    endpoint = VectorSearchEndpoint(name=name, budget_policy_id=usage_policy_id)
    executor = VectorSearchEndpointExecutor(client, dry_run=DRY_RUN)
    result = executor.update(endpoint) if executor.exists(endpoint) else executor.create(endpoint)
    logger.info(f"Vector search endpoint '{endpoint.resolved_name}': {result.operation.value} - {result.message}")

    if not DRY_RUN:
        client.permissions.set(
            request_object_type="vector-search-endpoints",
            request_object_id=endpoint.resolved_name,
            access_control_list=[
                {"group_name": group_name, "permission_level": PermissionLevel.CAN_MANAGE.value}
            ],
        )
        logger.info(f"Set CAN_MANAGE on '{endpoint.resolved_name}' for group '{group_name}'")
    else:
        logger.info(f"[DRY RUN] Would set CAN_MANAGE on '{endpoint.resolved_name}' for group '{group_name}'")


# COMMAND ----------

# =============================================================================
# APPLY GOVERNANCE
# =============================================================================

logger.info(f"Applying governance — group={GROUP_NAME}, policy={USAGE_POLICY_ID}")

ensure_experiment_owner(
    name="/experiments/fraud_model",
    group_name=GROUP_NAME,
)

ensure_serving_endpoint(
    name="fraud_detector",
    group_name=GROUP_NAME,
    usage_policy_id=USAGE_POLICY_ID,
    served_models=[
        # {
        #     "model_name": "catalog.schema.fraud_model",
        #     "model_version": "1",
        #     "workload_size": "Small",
        #     "scale_to_zero_enabled": True,
        # }
    ],
)

ensure_vector_search_endpoint(
    name="ai_platform_search",
    group_name=GROUP_NAME,
    usage_policy_id=USAGE_POLICY_ID,
)
