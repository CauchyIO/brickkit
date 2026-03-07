"""
Model Serving Endpoint executor for AI/ML governance operations.

Handles creation, update, and deletion of Databricks Model Serving Endpoints,
including owner ACL assignment via AclExecutor.

Usage:
    from databricks.sdk import WorkspaceClient
    from brickkit.executors import ModelServingEndpointExecutor
    from brickkit.models import ModelServingEndpoint
    from brickkit.models.grants import Principal

    client = WorkspaceClient()
    executor = ModelServingEndpointExecutor(client)

    endpoint = ModelServingEndpoint(
        name="fraud_detector",
        owner=Principal(name="grp_ml_team"),
        budget_policy_id="bp-abc123",
    )
    result = executor.create(endpoint)
"""

import logging
import time

from databricks.sdk.errors import BadRequest, NotFound, PermissionDenied, ResourceDoesNotExist
from databricks.sdk.service.iam import PermissionLevel

from brickkit.executors.acl_executor import AclExecutor
from brickkit.executors.base import BaseExecutor, ExecutionResult, OperationType
from brickkit.models.acls import AclBinding
from brickkit.models.grants import Principal
from brickkit.models.ml_models import ModelServingEndpoint

logger = logging.getLogger(__name__)


class ModelServingEndpointExecutor(BaseExecutor[ModelServingEndpoint]):
    """
    Executor for Model Serving Endpoint lifecycle operations.

    Creates, updates, and deletes Databricks Model Serving Endpoints and
    optionally applies a CAN_MANAGE ACL for the configured owner principal.
    """

    def get_resource_type(self) -> str:
        return "MODEL_SERVING_ENDPOINT"

    def exists(self, resource: ModelServingEndpoint) -> bool:
        """Check if the endpoint exists."""
        try:
            self.client.serving_endpoints.get(resource.resolved_name)
            return True
        except (ResourceDoesNotExist, NotFound):
            return False
        except PermissionDenied as e:
            logger.error(f"Permission denied checking serving endpoint: {e}")
            raise
        except Exception as e:
            if "RESOURCE_DOES_NOT_EXIST" in str(e) or "NOT_FOUND" in str(e):
                return False
            raise

    def _apply_owner_acl(self, endpoint_name: str, owner: Principal) -> None:
        """
        Grant CAN_MANAGE to the owner principal on the endpoint.

        Args:
            endpoint_name: The resolved (env-suffixed) endpoint name used as object_id.
            owner: Principal to receive CAN_MANAGE.
        """
        binding = AclBinding.for_serving_endpoint(endpoint_name)
        if owner.is_service_principal():
            binding.grant_service_principal(owner.resolved_name, PermissionLevel.CAN_MANAGE, add_env_suffix=False)
        else:
            binding.grant_group(owner.resolved_name, PermissionLevel.CAN_MANAGE, add_env_suffix=False)
        AclExecutor(self.client, self.dry_run).set_permissions(binding)

    def create(self, resource: ModelServingEndpoint) -> ExecutionResult:
        """Create a Model Serving Endpoint and apply owner ACL if set."""
        start_time = time.time()
        resource_name = resource.resolved_name

        if self.dry_run:
            logger.info(f"[DRY RUN] Would create serving endpoint {resource_name}")
            return ExecutionResult(
                success=True,
                operation=OperationType.CREATE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message="Would be created (dry run)",
            )

        try:
            if self.exists(resource):
                return ExecutionResult(
                    success=True,
                    operation=OperationType.NO_OP,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message="Already exists",
                )

            logger.info(f"Creating Model Serving Endpoint: {resource_name}")
            params = resource.to_sdk_create_params()
            endpoint = self.client.serving_endpoints.create(**params)

            if resource.owner:
                # Use the endpoint name as object ID for the permissions API
                endpoint_id = getattr(endpoint, "name", resource_name)
                self._apply_owner_acl(endpoint_id, resource.owner)

            duration = time.time() - start_time
            return ExecutionResult(
                success=True,
                operation=OperationType.CREATE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message="Created successfully",
                duration_seconds=duration,
            )

        except PermissionDenied as e:
            logger.error(f"Permission denied creating serving endpoint: {e}")
            raise
        except BadRequest as e:
            return self._handle_error(OperationType.CREATE, resource_name, e)

    def update(self, resource: ModelServingEndpoint) -> ExecutionResult:
        """Update the endpoint config and re-apply owner ACL if set."""
        start_time = time.time()
        resource_name = resource.resolved_name

        if self.dry_run:
            logger.info(f"[DRY RUN] Would update serving endpoint {resource_name}")
            return ExecutionResult(
                success=True,
                operation=OperationType.UPDATE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message="Would be updated (dry run)",
            )

        try:
            if not self.exists(resource):
                return ExecutionResult(
                    success=False,
                    operation=OperationType.UPDATE,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message="Endpoint does not exist",
                )

            logger.info(f"Updating Model Serving Endpoint config: {resource_name}")
            if resource.config:
                self.client.serving_endpoints.update_config(name=resource_name, **resource.config)

            if resource.owner:
                self._apply_owner_acl(resource_name, resource.owner)

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
            logger.error(f"Permission denied updating serving endpoint: {e}")
            raise
        except BadRequest as e:
            return self._handle_error(OperationType.UPDATE, resource_name, e)

    def delete(self, resource: ModelServingEndpoint) -> ExecutionResult:
        """Delete a Model Serving Endpoint."""
        start_time = time.time()
        resource_name = resource.resolved_name

        if self.dry_run:
            logger.info(f"[DRY RUN] Would delete serving endpoint {resource_name}")
            return ExecutionResult(
                success=True,
                operation=OperationType.DELETE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message="Would be deleted (dry run)",
            )

        try:
            if not self.exists(resource):
                return ExecutionResult(
                    success=True,
                    operation=OperationType.NO_OP,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message="Does not exist",
                )

            logger.info(f"Deleting Model Serving Endpoint: {resource_name}")
            self.client.serving_endpoints.delete(resource_name)

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
            logger.error(f"Permission denied deleting serving endpoint: {e}")
            raise
        except BadRequest as e:
            return self._handle_error(OperationType.DELETE, resource_name, e)
