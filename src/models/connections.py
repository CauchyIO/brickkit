"""
Connection models for Unity Catalog.

This module contains the Connection securable for external data systems.

Mirrors the Databricks SDK ConnectionsAPI pattern.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from pydantic import (
    Field,
    computed_field,
    field_validator,
)

from .base import BaseSecurable, DEFAULT_SECURABLE_OWNER, get_current_environment
from .enums import SecurableType, ConnectionType
from .grants import Principal

logger = logging.getLogger(__name__)


class Connection(BaseSecurable):
    """
    Defines connection to external data systems.

    Connection is a level-1 object that manages connections to external
    systems like MySQL, PostgreSQL, SQL Server, etc. It stores connection
    details and credentials securely.
    """
    name: str = Field(
        ...,
        pattern=r'^[a-zA-Z0-9][a-zA-Z0-9_-]*$',
        min_length=1,
        max_length=255,
        description="Connection name (base name without environment suffix)"
    )
    connection_type: ConnectionType = Field(
        ...,
        description="Type of external connection"
    )

    # Connection options - specific to each connection type
    options: Dict[str, str] = Field(
        default_factory=dict,
        description="Connection-specific options (host, port, database, etc.)"
    )

    # Common fields
    owner: Optional[Principal] = Field(
        default_factory=lambda: Principal(name=DEFAULT_SECURABLE_OWNER, add_environment_suffix=False),
        description="Owner principal"
    )
    comment: Optional[str] = Field(None, max_length=1024, description="Description")
    read_only: bool = Field(False, description="Whether connection is read-only")

    # Properties map for additional metadata
    properties: Dict[str, str] = Field(
        default_factory=dict,
        description="Additional properties as key-value pairs"
    )

    @field_validator('options')
    @classmethod
    def validate_connection_options(cls, v: Dict[str, str], info) -> Dict[str, str]:
        """Validate required options based on connection type."""
        conn_type = info.data.get('connection_type')

        if conn_type in [ConnectionType.MYSQL, ConnectionType.POSTGRESQL, ConnectionType.SQLSERVER]:
            required = {'host', 'port', 'database'}
            missing = required - set(v.keys())
            if missing:
                raise ValueError(f"Missing required options for {conn_type}: {missing}")

        # Security: Validate no plaintext passwords
        if 'password' in v:
            raise ValueError("Passwords must be stored as secrets, not in options")

        return v

    @computed_field  # type: ignore[prop-decorator]
    @property
    def resolved_name(self) -> str:
        """Name with environment suffix."""
        env = get_current_environment()
        return f"{self.name}_{env.value.lower()}"

    @property
    def securable_type(self) -> SecurableType:
        """Return the securable type."""
        return SecurableType.CONNECTION

    def get_level_1_name(self) -> str:
        """Return the level-1 name (resolved name for Privilege storage)."""
        return self.resolved_name

    def to_sdk_create_params(self) -> Dict[str, Any]:
        """Convert to SDK create parameters."""
        params = {
            "name": self.resolved_name,
            "connection_type": self.connection_type.value,
            "comment": self.comment
        }

        # Add connection options
        if self.options:
            params["options"] = self.options

        # Add properties
        if self.properties:
            params["properties"] = self.properties

        return params

    def to_sdk_update_params(self) -> Dict[str, Any]:
        """Convert to SDK update parameters."""
        params = {
            "name": self.resolved_name,
            "comment": self.comment
        }

        # Options can be updated
        if self.options:
            params["options"] = self.options

        return params
