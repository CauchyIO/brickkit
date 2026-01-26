"""
Team and AccessManager models for Unity Catalog governance.

This module contains models for team-level orchestration:
- Team: Represents a team with workspace assignments and access patterns
- AccessManager: Team-level orchestrator for access management

These are brickkit-specific orchestration models (not direct SDK mirrors).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from pydantic import (
    Field,
    PrivateAttr,
    computed_field,
    model_validator,
)
from typing_extensions import Self

from .base import BaseGovernanceModel, BaseSecurable, get_current_environment
from .enums import Environment, IsolationMode
from .grants import AccessPolicy, Principal, Privilege
from .workspace_bindings import Workspace, WorkspaceBindingPattern

if TYPE_CHECKING:
    from .catalogs import Catalog
    from .external_locations import ExternalLocation
    from .storage_credentials import StorageCredential

logger = logging.getLogger(__name__)


class Team(BaseGovernanceModel):
    """
    Represents a team with workspace assignments and access patterns.

    Teams are the organizational unit that brings together:
    - Workspace assignments per environment
    - Cross-environment access patterns via binding patterns
    - Principal memberships for access control

    Teams reference existing Workspace objects from the global registry
    to ensure workspace uniqueness across the system.
    """

    name: str = Field(..., pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_-]*$", description="Team identifier")
    workspaces: Dict[Environment, Workspace] = Field(
        default_factory=dict, description="Workspace assignments per environment"
    )
    binding_pattern: Optional[WorkspaceBindingPattern] = Field(
        default=None, description="Cross-environment access pattern"
    )
    principals: List[Principal] = Field(default_factory=list, description="Team members and service principals")

    # Private workspace references for serialization
    _workspace_refs: Dict[Environment, str] = PrivateAttr(default_factory=dict)

    @model_validator(mode="after")
    def track_workspace_references(self) -> Self:
        """Track workspace IDs for serialization."""
        for env, workspace in self.workspaces.items():
            self._workspace_refs[env] = workspace.workspace_id
            workspace._add_referencing_team(self.name)
        return self

    def add_workspace(self, workspace: Workspace) -> None:
        """Add workspace assignment, deriving environment from the workspace."""
        environment = workspace.environment

        if environment in self.workspaces:
            raise ValueError(f"Team '{self.name}' already has workspace for {environment.value}")

        self.workspaces[environment] = workspace
        self._workspace_refs[environment] = workspace.workspace_id
        workspace._add_referencing_team(self.name)

    def add_principal(self, principal: Principal) -> None:
        """Add a principal to the team."""
        if any(p.name == principal.name for p in self.principals):
            raise ValueError(f"Principal '{principal.name}' already exists in team '{self.name}'")
        self.principals.append(principal)

    def get_workspace(self, environment: Environment) -> Optional[Workspace]:
        """Get workspace for a specific environment."""
        return self.workspaces.get(environment)

    def add_catalog(self, catalog: "Catalog") -> None:
        """
        Add a catalog to the team and configure its workspace bindings.

        For ISOLATED catalogs, sets workspace_ids from team's workspaces.
        For OPEN catalogs, clears workspace_ids.
        """
        if catalog.isolation_mode == IsolationMode.ISOLATED:
            catalog_env = None

            if hasattr(catalog, "environment") and catalog.environment:
                catalog_env = catalog.environment
            else:
                resolved_name = catalog.resolved_name.lower()
                if resolved_name.endswith("_dev"):
                    catalog_env = Environment.DEV
                elif resolved_name.endswith("_acc"):
                    catalog_env = Environment.ACC
                elif resolved_name.endswith("_prd"):
                    catalog_env = Environment.PRD
                else:
                    catalog_env = get_current_environment()

            workspace_ids_to_bind = []
            if catalog_env in self.workspaces:
                workspace_ids_to_bind.append(int(self.workspaces[catalog_env].workspace_id))

            if self.binding_pattern:
                access_matrix = self.binding_pattern.access_matrix.get(catalog_env, {})
                for target_env_str, binding_type in access_matrix.items():
                    try:
                        target_env = Environment[target_env_str.upper()]
                        if target_env in self.workspaces and target_env != catalog_env:
                            workspace_ids_to_bind.append(int(self.workspaces[target_env].workspace_id))
                    except (KeyError, ValueError):
                        continue

            catalog.workspace_ids = list(set(workspace_ids_to_bind))
            logger.info(
                f"Catalog {catalog.resolved_name} (env={catalog_env}) bound to workspace IDs: {catalog.workspace_ids}"
            )

        elif catalog.isolation_mode == IsolationMode.OPEN:
            catalog.workspace_ids = []

    def add_storage_credential(self, storage_credential: "StorageCredential") -> None:
        """Add a storage credential to the team and configure its workspace bindings."""
        workspace_ids_to_bind = []
        for workspace in self.workspaces.values():
            workspace_ids_to_bind.append(int(workspace.workspace_id))
        storage_credential.workspace_ids = list(set(workspace_ids_to_bind))

    def add_external_location(self, external_location: "ExternalLocation") -> None:
        """Add an external location to the team and configure its workspace bindings."""
        workspace_ids_to_bind = []
        for workspace in self.workspaces.values():
            workspace_ids_to_bind.append(int(workspace.workspace_id))
        external_location.workspace_ids = list(set(workspace_ids_to_bind))

    def get_catalogs_for_workspace(self, workspace_id: str) -> List["Catalog"]:
        """Get all catalogs that should be accessible from a specific workspace."""
        return []  # Placeholder for future implementation

    @computed_field
    @property
    def workspace_ids(self) -> Dict[str, str]:
        """Get workspace IDs per environment for serialization."""
        return {env.value.lower(): workspace.workspace_id for env, workspace in self.workspaces.items()}


class AccessManager(BaseGovernanceModel):
    """
    Team-level orchestrator for access management.

    AccessManager provides a higher-level API for teams to organize and track
    their access declarations. It's an optional layer on top of the core
    grant() API that provides:

    1. Centralized grant tracking for audit and review
    2. Bulk operations for common patterns
    3. Team-specific access organization
    """

    team_name: str = Field(default="default", description="Team owning these access declarations")
    grants: List[Dict[str, Any]] = Field(default_factory=list, description="Audit trail of all grants made")

    @property
    def privileges(self) -> List[Privilege]:
        """Return list of all privileges created through this manager."""
        result = []
        for grant in self.grants:
            if "privileges" in grant:
                result.extend(grant["privileges"])
        return result

    def grant(self, principal: Principal, securable: BaseSecurable, policy: AccessPolicy) -> None:
        """
        Orchestrate a grant and record it for audit.

        Args:
            principal: The principal to grant to
            securable: The securable object
            policy: The access policy to apply
        """
        if not hasattr(principal, "resolved_name"):
            raise TypeError(f"principal must have a resolved_name attribute, got {type(principal)}")
        if not isinstance(policy, AccessPolicy):
            raise TypeError(f"policy must be an AccessPolicy object, got {type(policy)}")
        if not hasattr(securable, "grant"):
            raise TypeError(f"securable must have a grant() method, got {type(securable)}")

        # Delegate to the securable's grant method
        privileges = securable.grant(principal, policy)

        # Record the grant for audit
        self.grants.append(
            {
                "team": self.team_name,
                "principal": principal.resolved_name,
                "securable_type": securable.securable_type.value,
                "securable_name": getattr(securable, "name", str(securable)),
                "policy": policy.name,
                "privileges": privileges,
                "timestamp": None,
            }
        )

    def grant_many(self, principal: Principal, securables: List[Any], policy: AccessPolicy) -> None:
        """Bulk grant to multiple securables."""
        for securable in securables:
            self.grant(principal, securable, policy)

    def grant_to_all_schemas(self, principal: Principal, catalog: Any, policy: AccessPolicy) -> None:
        """Pattern-based grant to all schemas in a catalog."""
        self.grant(principal, catalog, policy)
        for schema in catalog.schemas:
            self.grant(principal, schema, policy)

    def get_grants_for_principal(self, principal_name: str) -> List[Dict[str, Any]]:
        """Get all grants for a specific principal."""
        return [g for g in self.grants if g["principal"] == principal_name]

    def get_grants_for_securable(self, securable_name: str) -> List[Dict[str, Any]]:
        """Get all grants on a specific securable."""
        return [g for g in self.grants if g["securable_name"] == securable_name]
