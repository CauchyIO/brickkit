"""
MLflow Experiment executor for AI/ML governance operations.

Handles creation, update, and deletion of MLflow Experiments, including
owner ACL assignment via AclExecutor.

Usage:
    from databricks.sdk import WorkspaceClient
    from brickkit.executors import MlflowExperimentExecutor
    from brickkit.models.experiments import MlflowExperiment
    from brickkit.models.grants import Principal

    client = WorkspaceClient()
    executor = MlflowExperimentExecutor(client)

    experiment = MlflowExperiment(
        name="/experiments/fraud_model",
        owner=Principal(name="grp_ml_team"),
        budget_policy_id="bp-abc123",
    )
    result = executor.create(experiment)
"""

import logging
import time

from databricks.sdk.errors import BadRequest, NotFound, PermissionDenied, ResourceDoesNotExist
from databricks.sdk.service.iam import PermissionLevel
from databricks.sdk.service.ml import ExperimentTag

from brickkit.executors.acl_executor import AclExecutor
from brickkit.executors.base import BaseExecutor, ExecutionResult, OperationType
from brickkit.models.acls import AclBinding
from brickkit.models.experiments import MlflowExperiment
from brickkit.models.grants import Principal

logger = logging.getLogger(__name__)


class MlflowExperimentExecutor(BaseExecutor[MlflowExperiment]):
    """
    Executor for MLflow Experiment lifecycle operations.

    Creates, updates, and deletes MLflow Experiments and optionally applies
    a CAN_MANAGE ACL for the configured owner principal.
    """

    def get_resource_type(self) -> str:
        return "MLFLOW_EXPERIMENT"

    def _get_experiment(self, name: str):
        """Return the experiment object or None if not found."""
        try:
            resp = self.client.experiments.get_by_name(experiment_name=name)
            return getattr(resp, "experiment", None)
        except (ResourceDoesNotExist, NotFound):
            return None
        except Exception as e:
            if "RESOURCE_DOES_NOT_EXIST" in str(e) or "NOT_FOUND" in str(e):
                return None
            raise

    def exists(self, resource: MlflowExperiment) -> bool:
        """Check if the experiment exists by its resolved name."""
        return self._get_experiment(resource.resolved_name) is not None

    def _apply_owner_acl(self, experiment_id: str, owner: Principal) -> None:
        """
        Grant CAN_MANAGE to the owner principal on the experiment.

        Args:
            experiment_id: The experiment ID (string) used as object_id.
            owner: Principal to receive CAN_MANAGE.
        """
        binding = AclBinding.for_experiment(experiment_id)
        if owner.is_service_principal():
            binding.grant_service_principal(owner.resolved_name, PermissionLevel.CAN_MANAGE, add_env_suffix=False)
        else:
            binding.grant_group(owner.resolved_name, PermissionLevel.CAN_MANAGE, add_env_suffix=False)
        AclExecutor(self.client, self.dry_run).set_permissions(binding)

    def create(self, resource: MlflowExperiment) -> ExecutionResult:
        """Create an MLflow Experiment and apply owner ACL if set."""
        start_time = time.time()
        resource_name = resource.resolved_name

        if self.dry_run:
            logger.info(f"[DRY RUN] Would create MLflow experiment {resource_name}")
            return ExecutionResult(
                success=True,
                operation=OperationType.CREATE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message="Would be created (dry run)",
            )

        try:
            existing = self._get_experiment(resource_name)
            if existing is not None:
                return ExecutionResult(
                    success=True,
                    operation=OperationType.NO_OP,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message="Already exists",
                )

            logger.info(f"Creating MLflow experiment: {resource_name}")

            mlflow_tags = [ExperimentTag(key=k, value=v) for k, v in resource.tags.items()] if resource.tags else None
            response = self.client.experiments.create_experiment(
                name=resource_name,
                artifact_location=resource.artifact_location,
                tags=mlflow_tags,
            )
            experiment_id = str(response.experiment_id)

            if resource.owner:
                self._apply_owner_acl(experiment_id, resource.owner)

            duration = time.time() - start_time
            return ExecutionResult(
                success=True,
                operation=OperationType.CREATE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message="Created successfully",
                duration_seconds=duration,
                changes={"experiment_id": experiment_id},
            )

        except PermissionDenied as e:
            logger.error(f"Permission denied creating experiment: {e}")
            raise
        except BadRequest as e:
            return self._handle_error(OperationType.CREATE, resource_name, e)

    def update(self, resource: MlflowExperiment) -> ExecutionResult:
        """Update the experiment name and re-apply owner ACL if set."""
        start_time = time.time()
        resource_name = resource.resolved_name

        if self.dry_run:
            logger.info(f"[DRY RUN] Would update MLflow experiment {resource_name}")
            return ExecutionResult(
                success=True,
                operation=OperationType.UPDATE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message="Would be updated (dry run)",
            )

        try:
            existing = self._get_experiment(resource_name)
            if existing is None:
                return ExecutionResult(
                    success=False,
                    operation=OperationType.UPDATE,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message="Experiment does not exist",
                )

            experiment_id = str(existing.experiment_id)
            logger.info(f"Updating MLflow experiment: {resource_name} (id={experiment_id})")
            self.client.experiments.update_experiment(
                experiment_id=experiment_id,
                new_name=resource_name,
            )

            if resource.owner:
                self._apply_owner_acl(experiment_id, resource.owner)

            duration = time.time() - start_time
            return ExecutionResult(
                success=True,
                operation=OperationType.UPDATE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message="Updated successfully",
                duration_seconds=duration,
            )

        except PermissionDenied as e:
            logger.error(f"Permission denied updating experiment: {e}")
            raise
        except BadRequest as e:
            return self._handle_error(OperationType.UPDATE, resource_name, e)

    def delete(self, resource: MlflowExperiment) -> ExecutionResult:
        """Delete an MLflow Experiment."""
        start_time = time.time()
        resource_name = resource.resolved_name

        if self.dry_run:
            logger.info(f"[DRY RUN] Would delete MLflow experiment {resource_name}")
            return ExecutionResult(
                success=True,
                operation=OperationType.DELETE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message="Would be deleted (dry run)",
            )

        try:
            existing = self._get_experiment(resource_name)
            if existing is None:
                return ExecutionResult(
                    success=True,
                    operation=OperationType.NO_OP,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message="Does not exist",
                )

            experiment_id = str(existing.experiment_id)
            logger.info(f"Deleting MLflow experiment: {resource_name} (id={experiment_id})")
            self.client.experiments.delete_experiment(
                experiment_id=experiment_id,
            )

            duration = time.time() - start_time
            return ExecutionResult(
                success=True,
                operation=OperationType.DELETE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message="Deleted successfully",
                duration_seconds=duration,
            )

        except PermissionDenied as e:
            logger.error(f"Permission denied deleting experiment: {e}")
            raise
        except BadRequest as e:
            return self._handle_error(OperationType.DELETE, resource_name, e)
