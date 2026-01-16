"""
Metastore models for Unity Catalog.

This module contains the Metastore - the top-level container for all Unity Catalog objects.

Mirrors the Databricks SDK MetastoresAPI pattern.
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional

from pydantic import (
    Field,
    PrivateAttr,
)

from .base import BaseGovernanceModel, DEFAULT_SECURABLE_OWNER, get_current_environment
from .enums import SecurableType
from .grants import Principal
from .catalogs import Catalog
from .storage_credentials import StorageCredential
from .external_locations import ExternalLocation
from .connections import Connection
from .workspace_bindings import Workspace

logger = logging.getLogger(__name__)


class Metastore(BaseGovernanceModel):
    """
    Top-level container for all Unity Catalog objects.

    The Metastore is a reference-only object - it must already exist in Databricks.
    This model serves as the root container for organizing all governance objects.
    """
    name: str = Field(..., description="User-specified metastore name")
    metastore_id: Optional[str] = Field(None, description="Unique metastore identifier")
    region: Optional[str] = Field(None, description="Cloud region (e.g., us-west-2)")
    storage_root: Optional[str] = Field(None, description="Default storage location")
    owner: Optional[Principal] = Field(
        default_factory=lambda: Principal(name=DEFAULT_SECURABLE_OWNER, add_environment_suffix=False),
        description="Owner principal"
    )

    # Child containers
    catalogs: List[Catalog] = Field(default_factory=list, description="Child catalogs")
    storage_credentials: List[StorageCredential] = Field(default_factory=list, description="Storage credentials")
    external_locations: List[ExternalLocation] = Field(default_factory=list, description="External locations")
    connections: List[Connection] = Field(default_factory=list, description="External connections")
    teams: List = Field(default_factory=list, description="Teams with workspace access")

    # Workspace registry (private, not serialized)
    _workspace_registry: Dict[str, Workspace] = PrivateAttr(default_factory=dict)

    def add_catalog(self, catalog: Catalog) -> None:
        """Add catalog with duplicate check."""
        if any(c.name == catalog.name for c in self.catalogs):
            raise ValueError(f"Catalog '{catalog.name}' already exists in metastore")
        self.catalogs.append(catalog)

    def add_storage_credential(self, sc: StorageCredential) -> None:
        """Add storage credential with duplicate check."""
        if any(s.name == sc.name for s in self.storage_credentials):
            raise ValueError(f"Storage credential '{sc.name}' already exists in metastore")
        self.storage_credentials.append(sc)

    def add_external_location(self, el: ExternalLocation) -> None:
        """Add external location with duplicate check."""
        if any(e.name == el.name for e in self.external_locations):
            raise ValueError(f"External location '{el.name}' already exists in metastore")
        self.external_locations.append(el)

    def add_connection(self, conn: Connection) -> None:
        """Add connection with duplicate check."""
        if any(c.name == conn.name for c in self.connections):
            raise ValueError(f"Connection '{conn.name}' already exists in metastore")
        self.connections.append(conn)

    def add_team(self, team) -> None:
        """Add team to metastore and register its workspaces."""
        if any(t.name == team.name for t in self.teams):
            raise ValueError(f"Team '{team.name}' already exists in metastore")
        self.teams.append(team)

    def with_convention(self, convention) -> "Metastore":
        """
        Apply a Convention to this metastore and propagate to all children.

        Args:
            convention: Convention instance with governance rules

        Returns:
            Self for method chaining
        """
        env = get_current_environment()
        # Metastore itself doesn't have tags, but propagate to children
        self._propagate_convention(convention, env)
        return self

    def _propagate_convention(self, convention, env) -> None:
        """Propagate convention to all catalogs."""
        for catalog in self.catalogs:
            convention.apply_to(catalog, env)
            catalog._propagate_convention(convention, env)

    @property
    def securable_type(self) -> SecurableType:
        return SecurableType.METASTORE
