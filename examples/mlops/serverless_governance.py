# Databricks notebook source
"""
Serverless Workspace Governance

Demonstrates end-to-end governance for serverless AI/ML workspace objects:

1. Create a budget policy (used to tag all serverless resources for cost control)
2. Create a Databricks group (will be the owner of all AI/ML workspace objects)
3. Govern an MLflow experiment - ensure the group is owner; create or update
4. Govern a model serving endpoint - attach budget policy + set group ownership
5. Govern a vector search endpoint - attach budget policy + set group ownership

Prerequisites:
  - Databricks SDK configured (via ~/.databrickscfg or environment variables)
  - Appropriate permissions: budget-policy admin, group admin, workspace admin
"""

import logging

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.billing import CreateBudgetPolicyRequest
from databricks.sdk.service.iam import PermissionLevel

from brickkit.executors import (
    GroupExecutor,
    MlflowExperimentExecutor,
    ModelServingEndpointExecutor,
    VectorSearchEndpointExecutor,
)
from brickkit.models.experiments import MlflowExperiment
from brickkit.models.grants import Principal
from brickkit.models.ml_models import ModelServingEndpoint
from brickkit.models.principals import ManagedGroup
from brickkit.models.vector_search import VectorSearchEndpoint

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# COMMAND ----------

# =============================================================================
# CONFIGURATION
# =============================================================================

# Names (BrickKit appends environment suffix automatically, e.g. _dev)
GROUP_NAME = "grp_ai_platform"
EXPERIMENT_NAME = "/experiments/fraud_model"
MODEL_SERVING_ENDPOINT_NAME = "fraud_detector"
VECTOR_SEARCH_ENDPOINT_NAME = "ai_platform_search"

# Budget policy label shown in Databricks cost dashboards
BUDGET_POLICY_NAME = "ai-platform-serverless"

DRY_RUN = True  # Set to False to apply changes

# COMMAND ----------

# =============================================================================
# CLIENT
# =============================================================================

client = WorkspaceClient()

# COMMAND ----------

# =============================================================================
# STEP 1: CREATE BUDGET POLICY
# =============================================================================
# A budget policy tags all serverless compute (model serving, vector search, etc.)
# so costs are attributed to this team/project in the Databricks billing console.

logger.info("--- Step 1: Budget Policy ---")

if DRY_RUN:
    logger.info(f"[DRY RUN] Would create budget policy: {BUDGET_POLICY_NAME}")
    usage_policy_id = "dry-run-policy-id"
else:
    existing_policies = list(client.budget_policies.list())
    existing = next((p for p in existing_policies if p.policy_name == BUDGET_POLICY_NAME), None)

    if existing:
        usage_policy_id = existing.policy_id
        logger.info(f"Budget policy already exists: {BUDGET_POLICY_NAME} (id={usage_policy_id})")
    else:
        policy = client.budget_policies.create(
            CreateBudgetPolicyRequest(policy_name=BUDGET_POLICY_NAME)
        )
        usage_policy_id = policy.policy_id
        logger.info(f"Created budget policy: {BUDGET_POLICY_NAME} (id={usage_policy_id})")

logger.info(f"Usage policy ID: {usage_policy_id}")

# COMMAND ----------

# =============================================================================
# STEP 2: CREATE GROUP
# =============================================================================
# The group owns all AI/ML workspace objects. Members of this group get
# CAN_MANAGE on experiments, model serving endpoints, and vector search endpoints.

logger.info("--- Step 2: Group ---")

group = ManagedGroup(name=GROUP_NAME, add_environment_suffix=False)
# Add members as needed:
# group.add_user("alice@company.com")
# group.add_entitlement("workspace-access")

group_executor = GroupExecutor(client, dry_run=DRY_RUN)
result = group_executor.create(group)
logger.info(f"Group '{group.resolved_name}': {result.operation.value} - {result.message}")

# COMMAND ----------

# =============================================================================
# STEP 3: MLflow EXPERIMENT - ensure group is owner
# =============================================================================
# Budget policy is NOT applicable to MLflow experiments.
# We only ensure the group holds CAN_MANAGE on the experiment.
# If the experiment already exists we update its owner; if not, we create it.

logger.info("--- Step 3: MLflow Experiment ---")

experiment = MlflowExperiment(
    name=EXPERIMENT_NAME,
    description="Fraud detection model training experiments",
    owner=Principal(name=GROUP_NAME, add_environment_suffix=False),
)

experiment_executor = MlflowExperimentExecutor(client, dry_run=DRY_RUN)

if experiment_executor.exists(experiment):
    result = experiment_executor.update(experiment)
else:
    result = experiment_executor.create(experiment)

logger.info(f"Experiment '{experiment.resolved_name}': {result.operation.value} - {result.message}")

# COMMAND ----------

# =============================================================================
# STEP 4: MODEL SERVING ENDPOINT - attach budget policy + set group ownership
# =============================================================================

logger.info("--- Step 4: Model Serving Endpoint ---")

serving_endpoint = ModelServingEndpoint(
    name=MODEL_SERVING_ENDPOINT_NAME,
    budget_policy_id=usage_policy_id,
    owner=Principal(name=GROUP_NAME, add_environment_suffix=False),
    # config holds the served models - add yours here:
    config={
        "served_models": [
            # {
            #     "model_name": "catalog.schema.fraud_model",
            #     "model_version": "1",
            #     "workload_size": "Small",
            #     "scale_to_zero_enabled": True,
            # }
        ]
    },
)

serving_executor = ModelServingEndpointExecutor(client, dry_run=DRY_RUN)

if serving_executor.exists(serving_endpoint):
    result = serving_executor.update(serving_endpoint)
else:
    result = serving_executor.create(serving_endpoint)

logger.info(f"Serving endpoint '{serving_endpoint.resolved_name}': {result.operation.value} - {result.message}")

# COMMAND ----------

# =============================================================================
# STEP 5: VECTOR SEARCH ENDPOINT - attach budget policy + set group ownership
# =============================================================================
# Note: Vector Search Endpoints do not support custom tags, but they do
# support budget_policy_id for cost attribution and ACL-based ownership.

logger.info("--- Step 5: Vector Search Endpoint ---")

vs_endpoint = VectorSearchEndpoint(
    name=VECTOR_SEARCH_ENDPOINT_NAME,
    budget_policy_id=usage_policy_id,
)

vs_executor = VectorSearchEndpointExecutor(client, dry_run=DRY_RUN)

if vs_executor.exists(vs_endpoint):
    result = vs_executor.update(vs_endpoint)
else:
    result = vs_executor.create(vs_endpoint)

logger.info(f"Vector search endpoint '{vs_endpoint.resolved_name}': {result.operation.value} - {result.message}")

# Apply group ownership via the Databricks permissions API.
# Vector search endpoints use the "vector-search-endpoints" ACL object type.
if not DRY_RUN:
    client.permissions.set(
        request_object_type="vector-search-endpoints",
        request_object_id=vs_endpoint.resolved_name,
        access_control_list=[
            {
                "group_name": GROUP_NAME,
                "permission_level": PermissionLevel.CAN_MANAGE.value,
            }
        ],
    )
    logger.info(f"Set CAN_MANAGE on '{vs_endpoint.resolved_name}' for group '{GROUP_NAME}'")
else:
    logger.info(
        f"[DRY RUN] Would set CAN_MANAGE on '{vs_endpoint.resolved_name}' for group '{GROUP_NAME}'"
    )

# COMMAND ----------

# =============================================================================
# SUMMARY
# =============================================================================

logger.info("--- Summary ---")
logger.info(f"Budget policy : {BUDGET_POLICY_NAME} (id={usage_policy_id})")
logger.info(f"Group         : {GROUP_NAME}")
logger.info(f"Experiment    : {experiment.resolved_name}  (owner={GROUP_NAME})")
logger.info(f"Serving EP    : {serving_endpoint.resolved_name}  (budget_policy={usage_policy_id}, owner={GROUP_NAME})")
logger.info(f"VS endpoint   : {vs_endpoint.resolved_name}  (budget_policy={usage_policy_id}, owner={GROUP_NAME})")
