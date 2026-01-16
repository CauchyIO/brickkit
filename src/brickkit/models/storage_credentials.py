"""
Storage credential models for Unity Catalog.

This module contains models for cloud storage authentication:
- Cloud provider credential configurations (AWS, Azure, GCP)
- StorageCredential securable

Mirrors the Databricks SDK StorageCredentialsAPI pattern.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from pydantic import (
    BaseModel,
    Field,
    computed_field,
    model_validator,
)
from typing_extensions import Self

from .base import DEFAULT_SECURABLE_OWNER, BaseSecurable, get_current_environment
from .enums import SecurableType
from .grants import Principal

logger = logging.getLogger(__name__)


# =============================================================================
# CLOUD CREDENTIAL MODELS
# =============================================================================

class AwsIamRole(BaseModel):
    """AWS IAM role configuration for storage access."""
    role_arn: str = Field(
        ...,
        pattern=r'^arn:aws:iam::\d{12}:role/[\w+=,.@/-]+$',
        description="AWS IAM role ARN"
    )
    external_id: Optional[str] = Field(None, description="External ID for assume role")
    unity_catalog_iam_arn: Optional[str] = Field(None, description="Unity Catalog IAM ARN for trust policy")


class AzureServicePrincipal(BaseModel):
    """Azure service principal configuration for storage access."""
    directory_id: str = Field(
        ...,
        pattern=r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
        description="Azure Active Directory tenant ID"
    )
    application_id: str = Field(
        ...,
        pattern=r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
        description="Azure service principal application ID"
    )
    client_secret: Optional[str] = Field(None, description="Client secret (write-only)")


class AzureManagedIdentity(BaseModel):
    """Azure managed identity configuration for storage access."""
    access_connector_id: str = Field(
        ...,
        pattern=r'^/subscriptions/[^/]+/resourceGroups/[^/]+/providers/Microsoft\.Databricks/accessConnectors/[^/]+$',
        description="Azure access connector resource ID"
    )
    managed_identity_id: Optional[str] = Field(None, description="Specific managed identity ID if using user-assigned")


class GcpServiceAccountKey(BaseModel):
    """GCP service account key configuration for storage access."""
    email: str = Field(
        ...,
        pattern=r'^[a-z0-9-]+@[a-z0-9-]+\.iam\.gserviceaccount\.com$',
        description="Service account email"
    )
    private_key_id: str = Field(..., description="Private key ID")
    private_key: str = Field(..., description="Private key (PEM format, write-only)")


# =============================================================================
# STORAGE CREDENTIAL
# =============================================================================

class StorageCredential(BaseSecurable):
    """
    Defines authentication for cloud storage access.

    StorageCredential is a level-1 object that manages cloud provider
    authentication (AWS IAM roles, Azure service principals, etc.).
    ExternalLocations reference these credentials to access storage.
    """
    name: str = Field(
        ...,
        pattern=r'^[a-zA-Z0-9][a-zA-Z0-9_-]*$',
        min_length=1,
        max_length=255,
        description="Storage credential name (base name without environment suffix)"
    )

    # Cloud provider credentials (exactly one must be set)
    aws_iam_role: Optional[AwsIamRole] = Field(None, description="AWS IAM role configuration")
    azure_service_principal: Optional[AzureServicePrincipal] = Field(None, description="Azure service principal")
    azure_managed_identity: Optional[AzureManagedIdentity] = Field(None, description="Azure managed identity")
    gcp_service_account_key: Optional[GcpServiceAccountKey] = Field(None, description="GCP service account key")

    # Common fields
    owner: Optional[Principal] = Field(
        default_factory=lambda: Principal(name=DEFAULT_SECURABLE_OWNER, add_environment_suffix=False),
        description="Owner principal"
    )
    comment: Optional[str] = Field(None, max_length=1024, description="Description")
    read_only: bool = Field(False, description="Whether credential is read-only")
    is_existing: bool = Field(
        default=False,
        description="If True, credential already exists and name won't get environment suffix"
    )

    # Workspace isolation (optional)
    workspace_ids: List[int] = Field(
        default_factory=list,
        description="List of workspace IDs that can access this storage credential (empty = all workspaces)"
    )

    @model_validator(mode='after')
    def validate_exactly_one_credential(self) -> Self:
        """Ensure exactly one credential type is specified."""
        creds = [
            self.aws_iam_role,
            self.azure_service_principal,
            self.azure_managed_identity,
            self.gcp_service_account_key
        ]
        if sum(c is not None for c in creds) != 1:
            raise ValueError("Exactly one credential type must be specified")
        return self

    @computed_field
    @property
    def resolved_name(self) -> str:
        """Name with environment suffix (unless is_existing=True)."""
        if self.is_existing:
            # Existing credentials don't get environment suffix
            return self.name
        env = get_current_environment()
        return f"{self.name}_{env.value.lower()}"

    @property
    def securable_type(self) -> SecurableType:
        """Return the securable type."""
        return SecurableType.STORAGE_CREDENTIAL

    def get_level_1_name(self) -> str:
        """Return the level-1 name (resolved name for Privilege storage)."""
        return self.resolved_name

    def to_sdk_create_params(self) -> Dict[str, Any]:
        """Convert to SDK create parameters."""
        params = {
            "name": self.resolved_name,
            "comment": self.comment
        }

        # Add the appropriate credential configuration
        if self.aws_iam_role:
            params["aws_iam_role"] = {
                "role_arn": self.aws_iam_role.role_arn,
                "external_id": self.aws_iam_role.external_id,
                "unity_catalog_iam_arn": self.aws_iam_role.unity_catalog_iam_arn
            }
        elif self.azure_service_principal:
            params["azure_service_principal"] = {
                "directory_id": self.azure_service_principal.directory_id,
                "application_id": self.azure_service_principal.application_id,
                "client_secret": self.azure_service_principal.client_secret
            }
        elif self.azure_managed_identity:
            params["azure_managed_identity"] = {
                "access_connector_id": self.azure_managed_identity.access_connector_id,
                "managed_identity_id": self.azure_managed_identity.managed_identity_id
            }
        elif self.gcp_service_account_key:
            params["gcp_service_account_key"] = {
                "email": self.gcp_service_account_key.email,
                "private_key_id": self.gcp_service_account_key.private_key_id,
                "private_key": self.gcp_service_account_key.private_key
            }

        return params

    def to_sdk_update_params(self) -> Dict[str, Any]:
        """Convert to SDK update parameters."""
        params = {
            "name": self.resolved_name,
            "comment": self.comment
        }

        # Credential updates require full replacement
        if self.aws_iam_role:
            params["aws_iam_role"] = {
                "role_arn": self.aws_iam_role.role_arn
            }
        # Similar for other credential types...

        return params
