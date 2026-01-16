"""
External location models for Unity Catalog.

This module contains the ExternalLocation securable which references
a storage path with associated credentials.

Mirrors the Databricks SDK ExternalLocationsAPI pattern.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from pydantic import (
    Field,
    computed_field,
    field_validator,
)

from .base import BaseSecurable, DEFAULT_SECURABLE_OWNER, get_current_environment
from .enums import SecurableType
from .grants import Principal, AccessPolicy
from .storage_credentials import StorageCredential

logger = logging.getLogger(__name__)


class ExternalLocation(BaseSecurable):
    """
    References a storage path with associated credentials.

    ExternalLocation is a level-1 object that combines a storage path (URL) with
    the credentials needed to access it. It's used by catalogs, schemas, tables,
    and volumes to define where their data is stored.
    """
    name: str = Field(
        ...,
        pattern=r'^[a-zA-Z0-9][a-zA-Z0-9_-]*$',
        min_length=1,
        max_length=255,
        description="External location name (base name without environment suffix)"
    )
    url: str = Field(
        ...,
        min_length=10,
        max_length=1024,
        description="Storage URL (s3://, abfss://, gs://)"
    )
    storage_credential: StorageCredential = Field(
        ...,
        description="Associated storage credential for authentication"
    )
    owner: Optional[Principal] = Field(
        default_factory=lambda: Principal(name=DEFAULT_SECURABLE_OWNER, add_environment_suffix=False),
        description="Owner principal"
    )
    comment: Optional[str] = Field(None, max_length=1024, description="Description of the external location")
    skip_validation: bool = Field(False, description="Skip path validation during creation")
    access_point: Optional[str] = Field(None, description="S3 access point ARN if applicable")

    # Workspace isolation (optional)
    workspace_ids: List[int] = Field(
        default_factory=list,
        description="List of workspace IDs that can access this external location (empty = all workspaces)"
    )

    @field_validator('url')
    @classmethod
    def validate_storage_url(cls, v: str) -> str:
        """Validate and secure storage URL format."""
        # Check for valid prefixes
        valid_prefixes = ['s3://', 'abfss://', 'gs://']
        if not any(v.startswith(prefix) for prefix in valid_prefixes):
            raise ValueError(f"Invalid storage URL. Must start with one of: {valid_prefixes}")

        # Security: Check for path traversal attempts
        if '..' in v or '//' in v[8:]:  # Skip protocol://
            raise ValueError("Invalid characters in storage URL - potential security risk")

        # Validate URL structure
        try:
            parsed = urlparse(v)
            if not parsed.netloc:
                raise ValueError("Storage URL must include a bucket/container name")

            # Validate based on storage type
            if v.startswith('s3://'):
                # S3 bucket naming rules (simplified)
                bucket = parsed.netloc.split('.')[0] if '.' in parsed.netloc else parsed.netloc
                if not re.match(r'^[a-z0-9][a-z0-9.-]*[a-z0-9]$', bucket):
                    raise ValueError(f"Invalid S3 bucket name: {bucket}")
            elif v.startswith('abfss://'):
                # Azure ADLS Gen2 validation
                if '@' not in parsed.netloc or not parsed.netloc.endswith('.dfs.core.windows.net'):
                    raise ValueError("Invalid Azure storage format. Expected: container@account.dfs.core.windows.net")
            elif v.startswith('gs://'):
                # GCS bucket naming rules
                bucket = parsed.netloc
                if not re.match(r'^[a-z0-9][a-z0-9._-]*[a-z0-9]$', bucket):
                    raise ValueError(f"Invalid GCS bucket name: {bucket}")
        except ValueError:
            raise

        return v.rstrip('/')  # Normalize by removing trailing slash

    @computed_field
    @property
    def storage_root(self) -> str:
        """Returns self.url for consistency with SDK expectations."""
        return self.url

    @computed_field
    @property
    def resolved_name(self) -> str:
        """Name with environment suffix."""
        env = get_current_environment()
        return f"{self.name}_{env.value.lower()}"

    @property
    def securable_type(self) -> SecurableType:
        """Return the securable type."""
        return SecurableType.EXTERNAL_LOCATION

    def get_level_1_name(self) -> str:
        """Return the level-1 name (resolved name for Privilege storage)."""
        return self.resolved_name

    def to_sdk_create_params(self) -> Dict[str, Any]:
        """Convert to SDK create parameters."""
        params = {
            "name": self.resolved_name,
            "url": self.url,
            "credential_name": self.storage_credential.resolved_name,
            "comment": self.comment,
            "skip_validation": self.skip_validation
        }
        # Only include access_point if it's set (SDK might not support it)
        if self.access_point:
            params["access_point"] = self.access_point
        return params

    def to_sdk_update_params(self) -> Dict[str, Any]:
        """Convert to SDK update parameters."""
        return {
            "name": self.resolved_name,
            "url": self.url,
            "comment": self.comment
        }

    def _propagate_grants(self, principal: Principal, policy: AccessPolicy) -> List[Any]:
        """
        Propagate grants to storage credential if present.

        Args:
            principal: The principal to grant to
            policy: The access policy to apply

        Returns:
            List of propagated privileges
        """
        result = []

        # Propagate to storage credential if it exists
        if self.storage_credential:
            logger.debug(f"Propagating grants from external location {self.name} to storage credential")
            result.extend(self.storage_credential.grant(principal, policy, _skip_validation=True))

        return result
