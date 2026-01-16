"""
Workspace binding models for Unity Catalog.

This module contains models for workspace management and bindings:
- WorkspaceRegistry: Singleton registry for workspace management
- Workspace: Represents a Databricks workspace
- WorkspaceBinding: Tracks resource accessibility from workspaces
- WorkspaceBindingPattern: Defines cross-environment access patterns

Mirrors the Databricks SDK WorkspaceBindingsAPI pattern.
"""

from __future__ import annotations

import logging
from typing import ClassVar, Dict, List, Optional, Set

from pydantic import (
    Field,
    PrivateAttr,
)

from .base import BaseGovernanceModel
from .enums import Environment, BindingType

logger = logging.getLogger(__name__)


class WorkspaceRegistry:
    """
    Singleton registry for workspace management.

    Ensures workspace uniqueness across the entire system by maintaining
    a global registry of all workspace instances. Workspaces are shared
    across teams to enable proper cross-team collaboration.
    """
    _instance: ClassVar[Optional['WorkspaceRegistry']] = None
    _workspaces: Dict[str, 'Workspace']

    def __new__(cls) -> 'WorkspaceRegistry':
        """Singleton pattern implementation."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._workspaces = {}
        return cls._instance

    def get_or_create(self, workspace_id: str, name: str, hostname: str, environment: Environment) -> 'Workspace':
        """
        Get existing workspace or create new one.

        Args:
            workspace_id: Unique numeric identifier
            name: Human-friendly workspace name
            hostname: Workspace URL
            environment: The environment this workspace belongs to

        Returns:
            Workspace instance (existing or newly created)
        """
        if workspace_id not in self._workspaces:
            workspace = Workspace(
                workspace_id=workspace_id,
                name=name,
                hostname=hostname,
                environment=environment
            )
            self._workspaces[workspace_id] = workspace
        return self._workspaces[workspace_id]

    def get_all(self) -> List['Workspace']:
        """Get all registered workspaces."""
        return list(self._workspaces.values())

    def clear(self) -> None:
        """Clear the registry (useful for testing)."""
        self._workspaces.clear()


class Workspace(BaseGovernanceModel):
    """
    Represents a Databricks workspace.

    IMPORTANT: Workspaces are reference-only objects that must pre-exist in the
    environment. They are managed through a singleton registry to ensure uniqueness
    and enable cross-team workspace sharing.

    Workspaces track which teams reference them and maintain bindings to
    Unity Catalog resources.
    """
    workspace_id: str = Field(
        ...,
        description="Unique numeric workspace identifier"
    )
    name: str = Field(
        ...,
        description="Human-friendly workspace name"
    )
    hostname: str = Field(
        ...,
        description="Workspace URL (e.g., myworkspace.cloud.databricks.com)"
    )
    environment: Environment = Field(
        ...,
        description="Environment this workspace belongs to"
    )
    workspace_bindings: List['WorkspaceBinding'] = Field(
        default_factory=list,
        description="Resources accessible from this workspace"
    )

    # Private tracking
    _referencing_teams: Set[str] = PrivateAttr(default_factory=set)

    def add_binding(self, binding: 'WorkspaceBinding') -> None:
        """Add a workspace binding."""
        for existing in self.workspace_bindings:
            if (existing.securable_type == binding.securable_type and
                existing.securable_name == binding.securable_name):
                raise ValueError(
                    f"Binding for {binding.securable_type} '{binding.securable_name}' "
                    f"already exists in workspace {self.name}"
                )
        self.workspace_bindings.append(binding)

    def _add_referencing_team(self, team_name: str) -> None:
        """Track team that references this workspace."""
        self._referencing_teams.add(team_name)

    def get_referencing_teams(self) -> Set[str]:
        """Get set of team names that reference this workspace."""
        return self._referencing_teams.copy()


class WorkspaceBinding(BaseGovernanceModel):
    """
    Tracks which resources are accessible from which workspaces.

    Workspace bindings define the relationship between a workspace and
    Unity Catalog resources (catalogs, storage credentials, etc.) and
    specify the access level (read-write or read-only).
    """
    securable_type: str = Field(
        ...,
        description="Type of securable (catalog, storage_credential, etc.)"
    )
    securable_name: str = Field(
        ...,
        description="Name of the securable resource"
    )
    binding_type: BindingType = Field(
        ...,
        description="Access level (READ_WRITE or READ_ONLY)"
    )


class WorkspaceBindingPattern(BaseGovernanceModel):
    """
    Defines cross-environment access patterns for workspaces.

    Workspace binding patterns provide predefined templates for how
    workspaces in different environments can access resources from
    other environments. This enables consistent access patterns across teams.
    """
    name: str = Field(
        ...,
        description="Pattern name for identification"
    )
    access_matrix: Dict[Environment, Dict[str, BindingType]] = Field(
        default_factory=dict,
        description="Access matrix: source_env -> {target_env: binding_type}"
    )

    @classmethod
    def STANDARD_HIERARCHY(cls) -> 'WorkspaceBindingPattern':
        """
        Standard hierarchical access pattern.

        - DEV can read from ACC and PRD (for testing with prod data)
        - ACC can read from PRD (for validation)
        - PRD is isolated (no cross-environment access)
        """
        return cls(
            name="STANDARD_HIERARCHY",
            access_matrix={
                Environment.DEV: {
                    "dev": BindingType.BINDING_TYPE_READ_WRITE,
                    "acc": BindingType.BINDING_TYPE_READ_ONLY,
                    "prd": BindingType.BINDING_TYPE_READ_ONLY
                },
                Environment.ACC: {
                    "acc": BindingType.BINDING_TYPE_READ_WRITE,
                    "prd": BindingType.BINDING_TYPE_READ_ONLY
                },
                Environment.PRD: {
                    "prd": BindingType.BINDING_TYPE_READ_WRITE
                }
            }
        )

    @classmethod
    def ISOLATED(cls) -> 'WorkspaceBindingPattern':
        """
        Completely isolated environments.

        Each environment can only access its own resources.
        No cross-environment access is allowed.
        """
        return cls(
            name="ISOLATED",
            access_matrix={
                Environment.DEV: {
                    "dev": BindingType.BINDING_TYPE_READ_WRITE
                },
                Environment.ACC: {
                    "acc": BindingType.BINDING_TYPE_READ_WRITE
                },
                Environment.PRD: {
                    "prd": BindingType.BINDING_TYPE_READ_WRITE
                }
            }
        )

    @classmethod
    def PRODUCTION_ISOLATED(cls) -> 'WorkspaceBindingPattern':
        """
        Production isolation with lower environment sharing.

        - PRD is completely isolated
        - DEV and ACC can share resources with each other
        """
        return cls(
            name="PRODUCTION_ISOLATED",
            access_matrix={
                Environment.DEV: {
                    "dev": BindingType.BINDING_TYPE_READ_WRITE,
                    "acc": BindingType.BINDING_TYPE_READ_ONLY
                },
                Environment.ACC: {
                    "dev": BindingType.BINDING_TYPE_READ_ONLY,
                    "acc": BindingType.BINDING_TYPE_READ_WRITE
                },
                Environment.PRD: {
                    "prd": BindingType.BINDING_TYPE_READ_WRITE
                }
            }
        )
