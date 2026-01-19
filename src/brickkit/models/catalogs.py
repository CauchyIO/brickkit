"""
Catalog models for Unity Catalog.

This module contains the Catalog securable - the first layer of Unity Catalog's namespace.

Mirrors the Databricks SDK CatalogsAPI pattern.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from pydantic import (
    Field,
    computed_field,
    model_validator,
)
from typing_extensions import Self

from .base import DEFAULT_SECURABLE_OWNER, BaseSecurable, Tag, get_current_environment
from .enums import ALL_PRIVILEGES_EXPANSION, Environment, IsolationMode, PrivilegeType, SecurableType
from .external_locations import ExternalLocation
from .grants import AccessPolicy, Principal
from .schemas import Schema

if TYPE_CHECKING:
    from .references import FunctionReference, ModelReference, TableReference, VolumeReference

logger = logging.getLogger(__name__)


class Catalog(BaseSecurable):
    """
    First layer of Unity Catalog's three-level namespace.

    Catalogs are the top-level containers for schemas and provide a way to organize
    data assets. They support isolation modes and workspace bindings for access control.
    """
    name: str = Field(
        ...,
        pattern=r'^[a-zA-Z0-9][a-zA-Z0-9_-]*$',
        description="Catalog name (base name without environment suffix)"
    )
    comment: Optional[str] = Field(None, max_length=1024, description="Description of the catalog")
    owner: Optional[Principal] = Field(
        default_factory=lambda: Principal(name=DEFAULT_SECURABLE_OWNER, add_environment_suffix=False),
        description="Owner principal"
    )
    external_location: Optional[ExternalLocation] = Field(
        None,
        description="Optional external storage location"
    )
    isolation_mode: IsolationMode = Field(
        IsolationMode.OPEN,
        description="OPEN or ISOLATED workspace access mode"
    )

    # Workspace bindings for ISOLATED catalogs
    workspace_ids: List[int] = Field(
        default_factory=list,
        description="List of workspace IDs that can access this catalog (only for ISOLATED mode)"
    )

    # Child containers
    schemas: List[Schema] = Field(default_factory=list, description="Child schemas")

    # References to discovered objects (for governance of DABs/MLflow created resources)
    table_refs: List['TableReference'] = Field(default_factory=list, description="References to discovered tables")
    model_refs: List['ModelReference'] = Field(default_factory=list, description="References to discovered models")
    volume_refs: List['VolumeReference'] = Field(default_factory=list, description="References to discovered volumes")
    function_refs: List['FunctionReference'] = Field(default_factory=list, description="References to discovered functions")

    # Metadata
    tags: List[Tag] = Field(default_factory=list, description="Metadata tags for ABAC")

    # Optional environment override (for workspace bindings)
    environment: Optional[Environment] = Field(
        None,
        description="Optional environment override (mainly for workspace bindings)"
    )

    @model_validator(mode='after')
    def validate_workspace_bindings(self) -> Self:
        """Validate workspace bindings are only used with ISOLATED mode."""
        if self.workspace_ids and self.isolation_mode != IsolationMode.ISOLATED:
            raise ValueError(
                f"Catalog '{self.name}' has workspace_ids but isolation_mode is {self.isolation_mode}. "
                "Workspace bindings are only valid for ISOLATED catalogs."
            )
        return self

    @computed_field
    @property
    def storage_root(self) -> Optional[str]:
        """Returns external_location.url for SDK export."""
        return self.external_location.url if self.external_location else None

    @computed_field
    @property
    def resolved_name(self) -> str:
        """Name with environment suffix."""
        env = self.environment or get_current_environment()
        return f"{self.name}_{env.value.lower()}"

    @property
    def environment_name(self) -> str:
        """Alias for resolved_name for backward compatibility."""
        return self.resolved_name

    def add_schema(self, schema: Schema) -> None:
        """Add schema with duplicate check."""
        if any(s.name == schema.name for s in self.schemas):
            raise ValueError(f"Schema '{schema.name}' already exists in catalog '{self.name}'")
        self.schemas.append(schema)

        schema.catalog_name = self.name
        schema._parent_catalog = self

        # Update catalog_name for all children
        for table in schema.tables:
            table.catalog_name = self.name
            table._parent_catalog = self
        for volume in schema.volumes:
            volume.catalog_name = self.name
            volume._parent_catalog = self
        for function in schema.functions:
            function.catalog_name = self.name
            function._parent_catalog = self
        for model in schema.models:
            model.catalog_name = self.name
            model._parent_catalog = self

        # Inherit external location if schema doesn't have one
        if not schema.external_location and self.external_location:
            schema.external_location = self.external_location

        # Inherit owner if schema has default owner
        if not schema.owner or (schema.owner and schema.owner.name == DEFAULT_SECURABLE_OWNER):
            schema.owner = self.owner

    def add_table_reference(self, table_ref: 'TableReference') -> None:
        """Add a reference to a table discovered or created by DABs."""
        table_ref.catalog_name = self.name
        self.table_refs.append(table_ref)

    def add_model_reference(self, model_ref: 'ModelReference') -> None:
        """Add a reference to a model discovered from MLflow."""
        model_ref.catalog_name = self.name
        self.model_refs.append(model_ref)

    def add_volume_reference(self, volume_ref: 'VolumeReference') -> None:
        """Add a reference to a discovered volume."""
        volume_ref.catalog_name = self.name
        self.volume_refs.append(volume_ref)

    def add_function_reference(self, function_ref: 'FunctionReference') -> None:
        """Add a reference to a discovered function."""
        function_ref.catalog_name = self.name
        self.function_refs.append(function_ref)

    def _propagate_grants(self, principal: Principal, policy: AccessPolicy) -> List[Any]:
        """Propagate grants to child schemas and external location."""
        result = []

        if self.external_location and policy.has_privileges_for(SecurableType.EXTERNAL_LOCATION):
            result.extend(self.external_location.grant(principal, policy, _skip_validation=True))

        if policy.has_privileges_for(SecurableType.SCHEMA):
            for schema in self.schemas:
                result.extend(schema.grant(principal, policy, _skip_validation=True))

        return result

    def get_effective_privileges(self, principal: Principal) -> List[PrivilegeType]:
        """Get all privileges for a principal including expanded ALL_PRIVILEGES."""
        privileges = []

        for priv in self.privileges:
            if priv.principal == principal.resolved_name:
                privileges.append(priv.privilege)

        if PrivilegeType.ALL_PRIVILEGES in privileges:
            expanded = ALL_PRIVILEGES_EXPANSION.get(self.securable_type, []).copy()
            if PrivilegeType.MANAGE in privileges:
                expanded.append(PrivilegeType.MANAGE)
            return list(set(expanded))

        return list(set(privileges))

    def get_effective_tags(self) -> List[Tag]:
        """Returns tags on this catalog only."""
        return self.tags.copy()

    def to_sdk_create_params(self) -> Dict[str, Any]:
        """Convert to SDK create parameters."""
        params = {"name": self.resolved_name}

        if self.comment:
            params["comment"] = self.comment
        if self.storage_root:
            params["storage_root"] = self.storage_root
        if self.tags:
            params["properties"] = {tag.key: tag.value for tag in self.tags}

        return params

    def to_sdk_update_params(self) -> Dict[str, Any]:
        """Convert to SDK update parameters."""
        params = {
            "name": self.resolved_name,
            "comment": self.comment,
            "isolation_mode": self.isolation_mode.value
        }

        if self.owner:
            params["owner"] = self.owner.resolved_name
        if self.tags:
            params["properties"] = {tag.key: tag.value for tag in self.tags}

        return params

    def _propagate_convention(self, convention, env) -> None:
        """Propagate convention to all schemas."""
        for schema in self.schemas:
            convention.apply_to(schema, env)
            schema._propagate_convention(convention, env)

    @property
    def securable_type(self) -> SecurableType:
        return SecurableType.CATALOG

    def get_level_1_name(self) -> str:
        return self.resolved_name
