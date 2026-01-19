"""
Function models for Unity Catalog.

This module contains the Function securable for user-defined functions.

Mirrors the Databricks SDK FunctionsAPI pattern.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from pydantic import (
    Field,
    PrivateAttr,
    computed_field,
    field_validator,
)

from .base import DEFAULT_SECURABLE_OWNER, BaseSecurable, Tag, get_current_environment
from .enums import ALL_PRIVILEGES_EXPANSION, FunctionType, PrivilegeType, SecurableType
from .grants import Principal

if TYPE_CHECKING:
    from .catalogs import Catalog
    from .schemas import Schema
    from .tables import Table

logger = logging.getLogger(__name__)


class Function(BaseSecurable):
    """
    Third-level object storing user-defined functions.

    Functions can be SQL or Python-based and can return scalar values or tables.
    When used for row filters or column masks, they execute transparently with
    definer's rights, meaning users only need SELECT on the table.
    """
    name: str = Field(
        ...,
        pattern=r'^[a-zA-Z0-9][a-zA-Z0-9_-]*$',
        description="Function name (no environment suffix - parent catalog has it)"
    )
    function_type: FunctionType = Field(
        FunctionType.SQL,
        description="SQL, PYTHON, SCALAR, or TABLE"
    )
    owner: Optional[Principal] = Field(
        default_factory=lambda: Principal(name=DEFAULT_SECURABLE_OWNER, add_environment_suffix=False),
        description="Owner principal (defaults to parent schema owner)"
    )
    input_params: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Parameter definitions with name, type, and optional default"
    )
    return_type: Optional[str] = Field(
        None,
        description="Return type specification (e.g., 'STRING', 'INT', 'TABLE')"
    )
    definition: Optional[str] = Field(
        None,
        description="Function body/implementation (generic)"
    )
    sql_definition: Optional[str] = Field(
        None,
        description="SQL function definition"
    )
    routine_definition: Optional[str] = Field(
        None,
        description="Python function definition"
    )
    routine_dependencies: Optional[List[str]] = Field(
        None,
        description="Python function dependencies"
    )
    referenced_tables: List[Any] = Field(
        default_factory=list,
        description="Tables this function reads from - for dependency tracking only"
    )
    is_deterministic: bool = Field(
        True,
        description="Whether function always returns same result for same input"
    )
    is_row_filter: bool = Field(
        False,
        description="Whether this function is used as a row filter for row-level security"
    )
    is_column_mask: bool = Field(
        False,
        description="Whether this function is used as a column mask for data masking"
    )
    referencing_tables: List[Any] = Field(
        default_factory=list,
        description="Tables that use this function as a row filter or column mask"
    )
    comment: Optional[str] = Field(None, max_length=1024, description="Description of the function")

    # Metadata
    tags: List[Tag] = Field(default_factory=list, description="Metadata tags for ABAC")

    # Private parent reference (not serialized)
    _parent_schema: Optional[Schema] = PrivateAttr(default=None)
    _parent_catalog: Optional[Catalog] = PrivateAttr(default=None)

    catalog_name: Optional[str] = Field(
        None,
        description="Parent catalog name (set by add_function)"
    )
    schema_name: Optional[str] = Field(
        None,
        description="Parent schema name (set by add_function)"
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
            raise ValueError(f"Function '{self.name}' is not associated with a catalog")
        # Fallback to string field with current env if no parent
        env = get_current_environment()
        return f"{self.catalog_name}_{env.value.lower()}"

    @field_validator('function_type', mode='before')
    @classmethod
    def convert_function_type(cls, v: Any) -> FunctionType:
        """Convert string to FunctionType enum if needed."""
        if isinstance(v, str):
            return FunctionType(v.upper())
        return v

    @computed_field
    @property
    def fqdn(self) -> str:
        """Fully qualified domain name (catalog.schema.function format)."""
        if not self.catalog_name or not self.schema_name:
            raise ValueError(f"Function '{self.name}' is not associated with a catalog and schema")
        return f"{self.resolved_catalog_name}.{self.schema_name}.{self.name}"

    def add_referenced_table(self, table: 'Table') -> None:
        """
        Add table reference for dependency tracking.

        Note: This is for documentation and impact analysis only.
        Functions execute with definer's rights, so users don't need
        SELECT on referenced tables.

        Args:
            table: The table this function reads from
        """
        if table not in self.referenced_tables:
            self.referenced_tables.append(table)
            # Also add this function to the table's referencing functions
            if self not in table.referencing_functions:
                table.referencing_functions.append(self)

    def get_effective_privileges(self, principal: Principal) -> List[PrivilegeType]:
        """
        Get all privileges for a principal including inherited from schema.

        Note: When used as a row filter or column mask, this function
        executes transparently with definer's rights, so users don't need
        EXECUTE privilege.

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
        return SecurableType.FUNCTION

    def get_level_1_name(self) -> str:
        """Return the level-1 name (resolved catalog name for Privilege storage)."""
        return self.resolved_catalog_name

    def get_level_2_name(self) -> Optional[str]:
        """Return the level-2 name (schema name)."""
        return self.schema_name

    def get_level_3_name(self) -> Optional[str]:
        """Return the level-3 name (function name)."""
        return self.name

    def to_sdk_create_params(self) -> Dict[str, Any]:
        """Convert to SDK create parameters."""
        params = {
            "name": self.name,
            "function_type": self.function_type.value
        }

        # Add catalog and schema if available
        try:
            params["catalog_name"] = self.resolved_catalog_name
        except ValueError:
            if self.catalog_name:
                params["catalog_name"] = self.catalog_name

        if self.schema_name:
            params["schema_name"] = self.schema_name

        # Add function definition based on type
        if self.function_type == FunctionType.SQL:
            if self.sql_definition:
                params["sql_definition"] = self.sql_definition
            elif self.definition:
                params["sql_definition"] = self.definition
        elif self.function_type == FunctionType.PYTHON:
            if self.routine_definition:
                params["routine_definition"] = self.routine_definition
            elif self.definition:
                params["routine_definition"] = self.definition
            if self.routine_dependencies:
                params["routine_dependencies"] = self.routine_dependencies
        else:
            # Generic function type
            if self.definition:
                params["definition"] = self.definition

        # Add other optional parameters
        if self.input_params:
            params["input_params"] = self.input_params
        if self.return_type:
            params["return_type"] = self.return_type
        if self.comment:
            params["comment"] = self.comment
        if self.is_deterministic is not None:
            params["is_deterministic"] = self.is_deterministic

        return params

    def to_sdk_update_params(self) -> Dict[str, Any]:
        """
        Convert to SDK update parameters.

        Note: Functions are typically replaced rather than updated in Unity Catalog.
        """
        return {
            "full_name": self.fqdn,
            "comment": self.comment
        }
