"""
ACL models for managing object-level permissions in Databricks.

This module provides governance wrappers around the Databricks SDK Permissions API,
adding environment-aware principal resolution and declarative permission management.
"""

from __future__ import annotations

import logging
from typing import List

from databricks.sdk.service.iam import AccessControlRequest, PermissionLevel
from pydantic import Field, computed_field

from .base import BaseGovernanceModel, get_current_environment
from .enums import AclObjectType, PrincipalType

logger = logging.getLogger(__name__)


class AclEntry(BaseGovernanceModel):
    """
    Single ACL entry mapping a principal to a permission.

    Example:
        ```python
        entry = AclEntry(
            principal_name="grp_data_engineering",
            principal_type=PrincipalType.GROUP,
            permission=PermissionLevel.CAN_RESTART,
        )
        ```
    """

    principal_name: str = Field(..., description="Principal name (group, user, or SPN)")
    principal_type: PrincipalType = Field(..., description="Type of principal")
    permission: PermissionLevel = Field(..., description="Permission level to grant")
    add_environment_suffix: bool = Field(
        default=True, description="Whether to add environment suffix to principal name"
    )

    @computed_field
    @property
    def resolved_principal_name(self) -> str:
        """Get environment-aware principal name."""
        # Users never get environment suffixes
        if self.principal_type == PrincipalType.USER:
            return self.principal_name
        if not self.add_environment_suffix:
            return self.principal_name
        env = get_current_environment()
        return f"{self.principal_name}_{env.value.lower()}"

    def to_access_control_request(self) -> AccessControlRequest:
        """Convert to SDK AccessControlRequest."""
        req = AccessControlRequest(permission_level=self.permission)
        if self.principal_type == PrincipalType.GROUP:
            req.group_name = self.resolved_principal_name
        elif self.principal_type == PrincipalType.SERVICE_PRINCIPAL:
            req.service_principal_name = self.resolved_principal_name
        else:
            req.user_name = self.resolved_principal_name
        return req


class AclBinding(BaseGovernanceModel):
    """
    Declarative ACL binding for a Databricks object.

    Maps principal references to permissions on a specific object
    (cluster, job, notebook, etc.).

    Example:
        ```python
        binding = AclBinding(
            object_type=AclObjectType.CLUSTERS,
            object_id="0123-456789-abcdef",
        )
        binding.grant_group("grp_data_engineering", PermissionLevel.CAN_RESTART)
        binding.grant_service_principal("spn_etl", PermissionLevel.CAN_MANAGE)
        ```
    """

    object_type: AclObjectType = Field(..., description="Type of object")
    object_id: str = Field(..., description="Object identifier (cluster ID, job ID, notebook path, etc.)")

    # ACL entries (desired state)
    permissions: List[AclEntry] = Field(default_factory=list, description="Permission entries to apply")

    # Sync mode
    replace_all: bool = Field(
        default=False, description="If True, replace all permissions; if False, merge with existing"
    )

    def to_access_control_requests(self) -> List[AccessControlRequest]:
        """Convert all entries to SDK AccessControlRequest list."""
        return [entry.to_access_control_request() for entry in self.permissions]

    # Convenience methods for building permissions
    def grant_user(self, email: str, permission: PermissionLevel) -> "AclBinding":
        """
        Grant permission to a user.

        Args:
            email: User's email address
            permission: Permission level to grant

        Returns:
            Self for chaining
        """
        self.permissions.append(
            AclEntry(
                principal_name=email,
                principal_type=PrincipalType.USER,
                permission=permission,
                add_environment_suffix=False,
            )
        )
        return self

    def grant_group(self, name: str, permission: PermissionLevel, add_env_suffix: bool = True) -> "AclBinding":
        """
        Grant permission to a group.

        Args:
            name: Group name
            permission: Permission level to grant
            add_env_suffix: Whether to add environment suffix

        Returns:
            Self for chaining
        """
        self.permissions.append(
            AclEntry(
                principal_name=name,
                principal_type=PrincipalType.GROUP,
                permission=permission,
                add_environment_suffix=add_env_suffix,
            )
        )
        return self

    def grant_service_principal(
        self, name: str, permission: PermissionLevel, add_env_suffix: bool = True
    ) -> "AclBinding":
        """
        Grant permission to a service principal.

        Args:
            name: Service principal name
            permission: Permission level to grant
            add_env_suffix: Whether to add environment suffix

        Returns:
            Self for chaining
        """
        self.permissions.append(
            AclEntry(
                principal_name=name,
                principal_type=PrincipalType.SERVICE_PRINCIPAL,
                permission=permission,
                add_environment_suffix=add_env_suffix,
            )
        )
        return self

    def revoke(self, principal_name: str) -> "AclBinding":
        """
        Remove a principal's permissions.

        Args:
            principal_name: Principal to remove

        Returns:
            Self for chaining
        """
        self.permissions = [
            p
            for p in self.permissions
            if p.principal_name != principal_name and p.resolved_principal_name != principal_name
        ]
        return self

    @classmethod
    def for_cluster(cls, cluster_id: str) -> "AclBinding":
        """Create an ACL binding for a cluster."""
        return cls(object_type=AclObjectType.CLUSTERS, object_id=cluster_id)

    @classmethod
    def for_job(cls, job_id: str) -> "AclBinding":
        """Create an ACL binding for a job."""
        return cls(object_type=AclObjectType.JOBS, object_id=job_id)

    @classmethod
    def for_notebook(cls, notebook_path: str) -> "AclBinding":
        """Create an ACL binding for a notebook."""
        return cls(object_type=AclObjectType.NOTEBOOKS, object_id=notebook_path)

    @classmethod
    def for_directory(cls, directory_path: str) -> "AclBinding":
        """Create an ACL binding for a directory."""
        return cls(object_type=AclObjectType.DIRECTORIES, object_id=directory_path)

    @classmethod
    def for_warehouse(cls, warehouse_id: str) -> "AclBinding":
        """Create an ACL binding for a SQL warehouse."""
        return cls(object_type=AclObjectType.WAREHOUSES, object_id=warehouse_id)

    @classmethod
    def for_pipeline(cls, pipeline_id: str) -> "AclBinding":
        """Create an ACL binding for a Delta Live Tables pipeline."""
        return cls(object_type=AclObjectType.PIPELINES, object_id=pipeline_id)

    @classmethod
    def for_serving_endpoint(cls, endpoint_id: str) -> "AclBinding":
        """Create an ACL binding for a model serving endpoint."""
        return cls(object_type=AclObjectType.SERVING_ENDPOINTS, object_id=endpoint_id)

    @classmethod
    def for_experiment(cls, experiment_id: str) -> "AclBinding":
        """Create an ACL binding for an MLflow experiment."""
        return cls(object_type=AclObjectType.EXPERIMENTS, object_id=experiment_id)

    @classmethod
    def for_registered_model(cls, model_name: str) -> "AclBinding":
        """Create an ACL binding for a registered model."""
        return cls(object_type=AclObjectType.REGISTERED_MODELS, object_id=model_name)
