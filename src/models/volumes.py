"""
Volume models for Unity Catalog.

This module contains the Volume securable for unstructured data storage.

Mirrors the Databricks SDK VolumesAPI pattern.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, TYPE_CHECKING
from typing_extensions import Self

from pydantic import (
    Field,
    PrivateAttr,
    computed_field,
    field_validator,
    model_validator,
)

from .base import BaseSecurable, Tag, DEFAULT_SECURABLE_OWNER, get_current_environment
from .enums import SecurableType, PrivilegeType, VolumeType, ALL_PRIVILEGES_EXPANSION
from .grants import Principal, AccessPolicy
from .external_locations import ExternalLocation

if TYPE_CHECKING:
    from .schemas import Schema
    from .catalogs import Catalog

logger = logging.getLogger(__name__)


class Volume(BaseSecurable):
    """
    Third-level object for unstructured data storage.

    Volumes provide a way to store and manage unstructured data like files,
    ML models, images, and documents within Unity Catalog. They can be either
    managed (storage handled by Databricks) or external (references external storage).
    """
    name: str = Field(
        ...,
        pattern=r'^[a-zA-Z0-9][a-zA-Z0-9_-]*$',
        description="Volume name (no environment suffix - parent catalog has it)"
    )
    volume_type: VolumeType = Field(
        VolumeType.MANAGED,
        description="MANAGED or EXTERNAL"
    )
    owner: Optional[Principal] = Field(
        default_factory=lambda: Principal(name=DEFAULT_SECURABLE_OWNER, add_environment_suffix=False),
        description="Owner principal (defaults to parent schema owner)"
    )
    storage_location: Optional[str] = Field(
        None,
        description="For external volumes - the storage path"
    )
    external_location: Optional[ExternalLocation] = Field(
        None,
        description="External location object (inherited from schema if not set)"
    )
    comment: Optional[str] = Field(None, max_length=1024, description="Description of the volume")

    # Metadata
    tags: List[Tag] = Field(default_factory=list, description="Metadata tags for ABAC")

    # Private parent reference (not serialized)
    _parent_schema: Optional[Schema] = PrivateAttr(default=None)
    _parent_catalog: Optional[Catalog] = PrivateAttr(default=None)

    catalog_name: Optional[str] = Field(
        None,
        description="Parent catalog name (set by add_volume)"
    )
    schema_name: Optional[str] = Field(
        None,
        description="Parent schema name (set by add_volume)"
    )

    @computed_field
    @property
    def resolved_catalog_name(self) -> str:
        """Get resolved catalog name with environment suffix from parent."""
        if self._parent_catalog:
            return self._parent_catalog.resolved_name
        elif self._parent_schema and self._parent_schema._parent_catalog:
            return self._parent_schema._parent_catalog.resolved_name
        if not self.catalog_name:
            raise ValueError(f"Volume '{self.name}' is not associated with a catalog")
        # Fallback to string field with current env if no parent
        env = get_current_environment()
        return f"{self.catalog_name}_{env.value.lower()}"

    @field_validator('volume_type', mode='before')
    @classmethod
    def convert_volume_type(cls, v: Any) -> VolumeType:
        """Convert string to VolumeType enum if needed."""
        if isinstance(v, str):
            return VolumeType(v.upper())
        return v

    @model_validator(mode='after')
    def validate_external_volume(self) -> Self:
        """Ensure external volumes have either storage_location or external_location."""
        if self.volume_type == VolumeType.EXTERNAL:
            if not self.storage_location and not self.external_location:
                raise ValueError("External volumes require either storage_location or external_location")
        return self

    @computed_field  # type: ignore[prop-decorator]
    @property
    def fqdn(self) -> str:
        """Fully qualified domain name (catalog.schema.volume format)."""
        if not self.catalog_name or not self.schema_name:
            raise ValueError(f"Volume '{self.name}' is not associated with a catalog and schema")
        return f"{self.resolved_catalog_name}.{self.schema_name}.{self.name}"

    def _propagate_grants(self, principal: Principal, policy: AccessPolicy) -> List[Any]:
        """
        Propagate grants to external location if present.

        Args:
            principal: The principal to grant to
            policy: The access policy

        Returns:
            List of propagated privileges
        """
        result = []

        # Propagate to external location if policy has external location privileges
        if self.external_location and policy.has_privileges_for(SecurableType.EXTERNAL_LOCATION):
            result.extend(self.external_location.grant(principal, policy, _skip_validation=True))

        return result

    def get_effective_privileges(self, principal: Principal) -> List[PrivilegeType]:
        """
        Get all privileges for a principal including inherited from schema.

        Args:
            principal: The principal to check

        Returns:
            List of effective privilege types
        """
        privileges = []

        # Collect direct privileges on this securable
        for priv in self.privileges:
            if priv.principal == principal.resolved_name:
                privileges.append(priv.privilege)

        # Naively try to get parent privileges (will just continue if no parent)
        if hasattr(self, '_parent_schema') and self._parent_schema:
            # Just add all parent privileges, let the natural structure handle filtering
            privileges.extend(self._parent_schema.get_effective_privileges(principal))

        # Handle ALL_PRIVILEGES expansion
        if PrivilegeType.ALL_PRIVILEGES in privileges:
            # Get expansion for this securable type
            expanded = ALL_PRIVILEGES_EXPANSION.get(self.securable_type, []).copy()
            # Preserve MANAGE if explicitly granted
            if PrivilegeType.MANAGE in privileges:
                expanded.append(PrivilegeType.MANAGE)
            return list(set(expanded))

        # Return unique privileges
        return list(set(privileges))

    @property
    def securable_type(self) -> SecurableType:
        """Return the securable type."""
        return SecurableType.VOLUME

    def get_level_1_name(self) -> str:
        """Return the level-1 name (resolved catalog name for Privilege storage)."""
        return self.resolved_catalog_name

    def get_level_2_name(self) -> str:
        """Return the level-2 name (schema name)."""
        return self.schema_name

    def get_level_3_name(self) -> str:
        """Return the level-3 name (volume name)."""
        return self.name

    def to_sdk_create_params(self) -> Dict[str, Any]:
        """Convert to SDK create parameters."""
        params = {
            "name": self.name,
            "catalog_name": self.catalog_name,
            "schema_name": self.schema_name,
            "volume_type": self.volume_type.value,
            "comment": self.comment
        }
        if self.storage_location:
            params["storage_location"] = self.storage_location
        elif self.external_location:
            params["storage_location"] = self.external_location.url
        return params

    def to_sdk_update_params(self) -> Dict[str, Any]:
        """Convert to SDK update parameters."""
        return {
            "full_name": self.fqdn,
            "comment": self.comment
        }
