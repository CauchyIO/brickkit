"""
Schema models for Unity Catalog.

This module contains the Schema securable - the second layer of Unity Catalog's namespace.

Mirrors the Databricks SDK SchemasAPI pattern.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from pydantic import (
    Field,
    PrivateAttr,
    computed_field,
)

from .base import DEFAULT_SECURABLE_OWNER, BaseSecurable, get_current_environment
from .enums import ALL_PRIVILEGES_EXPANSION, PrivilegeType, SecurableType, TableType
from .external_locations import ExternalLocation
from .functions import Function
from .grants import AccessPolicy, Principal
from .tables import Table
from .volumes import Volume

if TYPE_CHECKING:
    from .catalogs import Catalog
    from .references import FunctionReference, ModelReference, TableReference, VolumeReference

# Try importing FlexibleFieldMixin
try:
    from ..mixins.flexible_fields import FlexibleFieldMixin
except (ImportError, ValueError):
    class FlexibleFieldMixin:
        pass

logger = logging.getLogger(__name__)


class Schema(FlexibleFieldMixin, BaseSecurable):
    """
    Second layer of Unity Catalog's namespace.

    Schemas (also known as databases) organize tables, volumes, and functions
    within a catalog. They inherit storage locations and owner from their parent
    catalog if not explicitly set.
    """
    name: str = Field(
        ...,
        pattern=r'^[a-zA-Z0-9][a-zA-Z0-9_-]*$',
        description="Schema name (no environment suffix - parent catalog has it)"
    )
    comment: Optional[str] = Field(None, max_length=1024, description="Description of the schema")
    owner: Optional[Principal] = Field(
        default_factory=lambda: Principal(name=DEFAULT_SECURABLE_OWNER, add_environment_suffix=False),
        description="Owner principal"
    )
    external_location: Optional[ExternalLocation] = Field(
        None,
        description="Optional external storage (inherited from catalog if not set)"
    )

    # Child containers
    tables: List[Table] = Field(default_factory=list, description="Child tables")
    volumes: List[Volume] = Field(default_factory=list, description="Child volumes")
    functions: List[Function] = Field(default_factory=list, description="Child functions")
    models: List[Any] = Field(default_factory=list, description="Child ML models")

    # Reference collections for lightweight governance (objects created by DABs/MLflow)
    table_refs: List['TableReference'] = Field(default_factory=list, description="References to discovered tables")
    model_refs: List['ModelReference'] = Field(default_factory=list, description="References to discovered models")
    volume_refs: List['VolumeReference'] = Field(default_factory=list, description="References to discovered volumes")
    function_refs: List['FunctionReference'] = Field(default_factory=list, description="References to discovered functions")

    # Private parent reference (not serialized)
    _parent_catalog: Optional[Catalog] = PrivateAttr(default=None)

    catalog_name: Optional[str] = Field(
        None,
        description="Parent catalog name (set by add_schema)"
    )

    @computed_field
    @property
    def storage_root(self) -> Optional[str]:
        """Returns external_location.url for SDK export."""
        return self.external_location.url if self.external_location else None

    @computed_field
    @property
    def resolved_catalog_name(self) -> str:
        """Get catalog name with environment suffix for runtime resolution."""
        if self._parent_catalog:
            return self._parent_catalog.resolved_name
        if not self.catalog_name:
            raise ValueError(f"Schema '{self.name}' is not associated with a catalog")
        env = get_current_environment()
        return f"{self.catalog_name}_{env.value.lower()}"

    @computed_field
    @property
    def fqdn(self) -> str:
        """Fully qualified domain name (catalog.schema format)."""
        if not self.catalog_name:
            raise ValueError(f"Schema '{self.name}' is not associated with a catalog")
        return f"{self.resolved_catalog_name}.{self.name}"

    @property
    def full_name(self) -> str:
        """Alias for fqdn for consistency with reference models."""
        return self.fqdn

    def add_table(self, table: Table) -> None:
        """Add table with duplicate check."""
        if any(t.name == table.name for t in self.tables):
            raise ValueError(f"Table '{table.name}' already exists in schema '{self.name}'")
        self.tables.append(table)

        if self._parent_catalog:
            table.catalog_name = self._parent_catalog.name
        elif self.catalog_name:
            table.catalog_name = self.catalog_name
        table.schema_name = self.name
        table._parent_schema = self
        table._parent_catalog = self._parent_catalog

        if table.table_type == TableType.EXTERNAL:
            if not table.external_location and self.external_location:
                table.external_location = self.external_location
            elif not table.external_location:
                raise ValueError(f"External table '{table.name}' requires an external_location")

        if not table.owner or (table.owner and table.owner.name == DEFAULT_SECURABLE_OWNER):
            table.owner = self.owner

    def add_volume(self, volume: Volume) -> None:
        """Add volume with duplicate check."""
        if any(v.name == volume.name for v in self.volumes):
            raise ValueError(f"Volume '{volume.name}' already exists in schema '{self.name}'")
        self.volumes.append(volume)

        if self._parent_catalog:
            volume.catalog_name = self._parent_catalog.name
        elif self.catalog_name:
            volume.catalog_name = self.catalog_name
        volume.schema_name = self.name
        volume._parent_schema = self
        volume._parent_catalog = self._parent_catalog

        if not volume.external_location and self.external_location:
            volume.external_location = self.external_location
        if not volume.owner or (volume.owner and volume.owner.name == DEFAULT_SECURABLE_OWNER):
            volume.owner = self.owner

    def add_function(self, function: Function) -> None:
        """Add function with duplicate check."""
        if any(f.name == function.name for f in self.functions):
            raise ValueError(f"Function '{function.name}' already exists in schema '{self.name}'")
        self.functions.append(function)

        if self._parent_catalog:
            function.catalog_name = self._parent_catalog.name
        elif self.catalog_name:
            function.catalog_name = self.catalog_name
        function.schema_name = self.name
        function._parent_schema = self
        function._parent_catalog = self._parent_catalog

        if not function.owner or (function.owner and function.owner.name == DEFAULT_SECURABLE_OWNER):
            function.owner = self.owner

    def add_model(self, model: Any) -> None:
        """Add ML model with duplicate check."""
        if any(m.name == model.name for m in self.models):
            raise ValueError(f"Model '{model.name}' already exists in schema '{self.name}'")
        self.models.append(model)

        if self._parent_catalog:
            model.catalog_name = self._parent_catalog.name
        elif self.catalog_name:
            model.catalog_name = self.catalog_name
        model.schema_name = self.name
        model._parent_schema = self
        model._parent_catalog = self._parent_catalog

        if not model.owner or (model.owner and model.owner.name == DEFAULT_SECURABLE_OWNER):
            model.owner = self.owner

    def __repr__(self) -> str:
        return (
            f"Schema(name='{self.name}', "
            f"catalog='{self.catalog_name}', "
            f"tables={len(self.tables)}, "
            f"volumes={len(self.volumes)}, "
            f"functions={len(self.functions)}, "
            f"models={len(self.models)})"
        )

    def __str__(self) -> str:
        return f"Schema '{self.fqdn}' ({len(self.tables)} tables, {len(self.volumes)} volumes, {len(self.models)} models)"

    def _propagate_grants(self, principal: Principal, policy: AccessPolicy) -> List[Any]:
        """Propagate grants to child objects."""
        result = []

        if self.external_location and policy.has_privileges_for(SecurableType.EXTERNAL_LOCATION):
            result.extend(self.external_location.grant(principal, policy, _skip_validation=True))

        if policy.has_privileges_for(SecurableType.TABLE):
            for table in self.tables:
                result.extend(table.grant(principal, policy, _skip_validation=True))

        if policy.has_privileges_for(SecurableType.VOLUME):
            for volume in self.volumes:
                result.extend(volume.grant(principal, policy, _skip_validation=True))

        if policy.has_privileges_for(SecurableType.FUNCTION):
            for function in self.functions:
                result.extend(function.grant(principal, policy, _skip_validation=True))

        if policy.has_privileges_for(SecurableType.MODEL):
            for model in self.models:
                result.extend(model.grant(principal, policy, _skip_validation=True))

        # Propagate to references
        if policy.has_privileges_for(SecurableType.TABLE):
            for table_ref in self.table_refs:
                result.extend(table_ref.grant(principal, policy, _skip_validation=True))

        if policy.has_privileges_for(SecurableType.MODEL):
            for model_ref in self.model_refs:
                result.extend(model_ref.grant(principal, policy, _skip_validation=True))

        if policy.has_privileges_for(SecurableType.VOLUME):
            for volume_ref in self.volume_refs:
                result.extend(volume_ref.grant(principal, policy, _skip_validation=True))

        if policy.has_privileges_for(SecurableType.FUNCTION):
            for function_ref in self.function_refs:
                result.extend(function_ref.grant(principal, policy, _skip_validation=True))

        return result

    def get_effective_privileges(self, principal: Principal) -> List[PrivilegeType]:
        """Get all privileges for a principal including inherited from catalog."""
        privileges = []

        for priv in self.privileges:
            if priv.principal == principal.resolved_name:
                privileges.append(priv.privilege)

        if hasattr(self, '_parent_catalog') and self._parent_catalog:
            privileges.extend(self._parent_catalog.get_effective_privileges(principal))

        if PrivilegeType.ALL_PRIVILEGES in privileges:
            expanded = ALL_PRIVILEGES_EXPANSION.get(self.securable_type, []).copy()
            if PrivilegeType.MANAGE in privileges:
                expanded.append(PrivilegeType.MANAGE)
            return list(set(expanded))

        return list(set(privileges))

    def _propagate_convention(self, convention, env) -> None:
        """Propagate convention to all tables, volumes, functions, and models."""
        for table in self.tables:
            convention.apply_to(table, env)
        for volume in self.volumes:
            convention.apply_to(volume, env)
        for function in self.functions:
            convention.apply_to(function, env)
        for model in self.models:
            convention.apply_to(model, env)

    @property
    def securable_type(self) -> SecurableType:
        return SecurableType.SCHEMA

    def get_level_1_name(self) -> str:
        return self.resolved_catalog_name

    def get_level_2_name(self) -> str:
        return self.name

    # Reference management methods
    def add_table_reference(self, table_ref: 'TableReference') -> None:
        """Add a reference to a table discovered or created by DABs."""
        if not self.catalog_name:
            raise ValueError(f"Schema '{self.name}' must have a catalog_name before adding references")
        table_ref.catalog_name = self.catalog_name
        table_ref.schema_name = self.name
        self.table_refs.append(table_ref)

    def add_model_reference(self, model_ref: 'ModelReference') -> None:
        """Add a reference to a model discovered or registered by MLflow."""
        if not self.catalog_name:
            raise ValueError(f"Schema '{self.name}' must have a catalog_name before adding references")
        model_ref.catalog_name = self.catalog_name
        model_ref.schema_name = self.name
        self.model_refs.append(model_ref)

    def add_volume_reference(self, volume_ref: 'VolumeReference') -> None:
        """Add a reference to a volume discovered or created by DABs."""
        if not self.catalog_name:
            raise ValueError(f"Schema '{self.name}' must have a catalog_name before adding references")
        volume_ref.catalog_name = self.catalog_name
        volume_ref.schema_name = self.name
        self.volume_refs.append(volume_ref)

    def add_function_reference(self, function_ref: 'FunctionReference') -> None:
        """Add a reference to a function discovered or created by DABs."""
        if not self.catalog_name:
            raise ValueError(f"Schema '{self.name}' must have a catalog_name before adding references")
        function_ref.catalog_name = self.catalog_name
        function_ref.schema_name = self.name
        self.function_refs.append(function_ref)

    def to_sdk_create_params(self) -> Dict[str, Any]:
        """Convert to SDK create parameters."""
        params = {
            "name": self.name,
            "catalog_name": self.resolved_catalog_name,
            "comment": self.comment
        }
        if self.storage_root:
            params["storage_root"] = self.storage_root
        return params

    def to_sdk_update_params(self) -> Dict[str, Any]:
        """Convert to SDK update parameters."""
        return {
            "name": self.name,
            "catalog_name": self.resolved_catalog_name,
            "full_name": f"{self.resolved_catalog_name}.{self.name}",
            "comment": self.comment
        }
