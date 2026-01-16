"""
Grant models for Unity Catalog governance.

This module contains models for managing principals, privileges, and access policies.
Mirrors the Databricks SDK GrantsAPI pattern.
"""

from __future__ import annotations

import logging
from typing import Any, ClassVar, Dict, List, Optional, Set
from typing_extensions import Self

from pydantic import (
    ConfigDict,
    Field,
    PrivateAttr,
    computed_field,
    field_validator,
    model_validator,
)

from databricks.sdk.errors import ResourceDoesNotExist, NotFound, PermissionDenied

from .base import BaseGovernanceModel, get_current_environment
from .enums import Environment, SecurableType, PrivilegeType

logger = logging.getLogger(__name__)


# =============================================================================
# PRINCIPAL MODEL
# =============================================================================

class Principal(BaseGovernanceModel):
    """
    Represents a grantee (user, group, or service principal) with flexible environment-specific name resolution.

    Key Concept: Unlike Catalogs and other metastore objects that get automatic environment suffixes,
    Principals are user-defined with flexible resolution logic. Teams control exactly how each
    Principal's name resolves in each environment - through custom mappings, auto-suffixes, or fixed names.

    Special Built-in Principals:
    - 'users': All workspace users
    - 'admins': Workspace admins
    - 'account users': All account users
    These special principals never receive environment suffixes.
    """
    # Define special built-in principals that should never get suffixes
    SPECIAL_PRINCIPALS: ClassVar[Set[str]] = {
        'users',           # All workspace users
        'admins',          # Workspace admins
        'account users'    # All account users
    }

    name: str = Field(..., description="Base name as defined by the team")
    add_environment_suffix: bool = Field(True, description="Whether to auto-add environment suffix")
    environment_mapping: Dict[Environment, str] = Field(
        default_factory=dict,
        description="Custom per-environment names"
    )
    environment: Optional[Environment] = Field(
        None,
        description="Optional environment override (for special cases)"
    )

    @model_validator(mode='after')
    def handle_special_principals(self) -> Self:
        """Automatically disable suffix for special built-in principals."""
        if self.name in self.SPECIAL_PRINCIPALS:
            self.add_environment_suffix = False
            logger.debug(f"Auto-disabled suffix for special principal: {self.name}")
        return self

    @computed_field  # type: ignore[prop-decorator]
    @property
    def resolved_name(self) -> str:
        """
        Environment-specific name resolution with priority order:
        1. Custom mapping (highest priority)
        2. No suffix if add_environment_suffix=False
        3. Auto suffix (default): Append _{env} to base name
        """
        # Use explicit override if set, otherwise current environment
        env = self.environment or get_current_environment()

        # Priority 1: Custom mapping
        if env in self.environment_mapping:
            return self.environment_mapping[env]

        # Priority 2: No suffix
        if not self.add_environment_suffix:
            return self.name

        # Priority 3: Auto suffix
        return f"{self.name}_{env.value.lower()}"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def environment_name(self) -> str:
        """Alias for resolved_name for backward compatibility."""
        return self.resolved_name

    def exists_in_databricks(self, workspace_client: Any) -> bool:
        """
        Check if principal exists in Databricks.

        Args:
            workspace_client: Databricks WorkspaceClient instance

        Returns:
            True if principal exists, False otherwise

        Raises:
            PermissionDenied: If caller lacks permission to check principals
        """
        resolved = self.resolved_name

        # Try as user
        try:
            workspace_client.users.get(resolved)
            return True
        except (ResourceDoesNotExist, NotFound):
            pass
        except PermissionDenied:
            raise

        # Try as group
        try:
            workspace_client.groups.get(resolved)
            return True
        except (ResourceDoesNotExist, NotFound):
            pass
        except PermissionDenied:
            raise

        # Try as service principal
        try:
            workspace_client.service_principals.get(resolved)
            return True
        except (ResourceDoesNotExist, NotFound):
            pass
        except PermissionDenied:
            raise

        return False

    # Convenience factory methods for special principals
    @classmethod
    def all_workspace_users(cls) -> 'Principal':
        """
        Create principal for all workspace users.

        This represents the built-in 'users' group in Databricks that includes
        all users who have access to the workspace.
        """
        return cls(name='users', add_environment_suffix=False)

    @classmethod
    def workspace_admins(cls) -> 'Principal':
        """
        Create principal for workspace admins.

        This represents the built-in 'admins' group in Databricks that includes
        all workspace administrators.
        """
        return cls(name='admins', add_environment_suffix=False)

    @classmethod
    def all_account_users(cls) -> 'Principal':
        """
        Create principal for all account users.

        This represents the built-in 'account users' group in Databricks that
        includes all users in the Databricks account across all workspaces.
        """
        return cls(name='account users', add_environment_suffix=False)


# =============================================================================
# PRIVILEGE MODEL
# =============================================================================

class Privilege(BaseGovernanceModel):
    """
    Runtime grant object representing specific access permissions.

    IMPORTANT: Stores strings (not object references) for clean SDK export.
    This is an internal model - users interact via AccessManager and grant methods.
    """
    level_1: str = Field(default="", description="Catalog/StorageCredential/ExternalLocation name")
    level_2: Optional[str] = Field(None, description="Schema name (if applicable)")
    level_3: Optional[str] = Field(None, description="Table/Volume/Function name (if applicable)")
    securable_type: SecurableType = Field(..., description="Type of securable")
    principal: str = Field(..., description="Resolved principal name (with environment)")
    privilege: PrivilegeType = Field(..., alias="privilege_type", description="Specific privilege granted")

    # Allow alternative initialization with securable_name (private to avoid conflict)
    _securable_name_input: Optional[str] = PrivateAttr(None)

    model_config = ConfigDict(
        populate_by_name=True,  # Allow both field name and alias
    )

    @field_validator('principal', mode='before')
    @classmethod
    def resolve_principal(cls, v):
        """Accept Principal object or string."""
        if hasattr(v, 'resolved_name'):
            return v.resolved_name
        return v

    @model_validator(mode='before')
    @classmethod
    def parse_securable_name(cls, values):
        """Parse securable_name into level_1/2/3."""
        if isinstance(values, dict) and 'securable_name' in values:
            name = values.pop('securable_name')  # Remove from dict to avoid field assignment
            if name:
                parts = name.split('.')
                if len(parts) >= 1:
                    values['level_1'] = parts[0]
                if len(parts) >= 2:
                    values['level_2'] = parts[1]
                if len(parts) >= 3:
                    values['level_3'] = parts[2]
        return values

    @computed_field
    @property
    def securable_name(self) -> str:
        """Construct full securable name from levels."""
        parts = [self.level_1]
        if self.level_2:
            parts.append(self.level_2)
        if self.level_3:
            parts.append(self.level_3)
        return '.'.join(parts)


# =============================================================================
# ACCESS POLICY MODEL
# =============================================================================

class AccessPolicy(BaseGovernanceModel):
    """
    Template defining privilege sets for common access patterns.

    AccessPolicies are predefined sets of privileges that can be granted as a unit.
    They propagate through the hierarchy - when granted at catalog level, the same
    AccessPolicy object flows to schemas and tables, each extracting relevant privileges.
    """
    name: str = Field(..., description="Policy name (e.g., READER, WRITER)")
    privilege_map: Dict[SecurableType, List[PrivilegeType]] = Field(
        default_factory=dict,
        description="Privileges per securable type"
    )

    def get_privileges(self, securable_type: SecurableType) -> List[PrivilegeType]:
        """
        Get privileges for a specific securable type.

        Args:
            securable_type: The type of securable

        Returns:
            List of privileges for that type, or empty list if none defined
        """
        return self.privilege_map.get(securable_type, [])

    def has_privileges_for(self, securable_type: SecurableType) -> bool:
        """
        Check if policy has privileges for this securable type.

        Args:
            securable_type: The type of securable

        Returns:
            True if policy defines privileges for this type
        """
        return securable_type in self.privilege_map and len(self.privilege_map[securable_type]) > 0

    @classmethod
    def READER(cls) -> 'AccessPolicy':
        """Reader access policy - SELECT and READ privileges."""
        return cls(
            name='READER',
            privilege_map={
                SecurableType.CATALOG: [PrivilegeType.USE_CATALOG, PrivilegeType.BROWSE],
                SecurableType.SCHEMA: [PrivilegeType.USE_SCHEMA, PrivilegeType.SELECT, PrivilegeType.READ_VOLUME],
                SecurableType.TABLE: [PrivilegeType.SELECT],
                SecurableType.VOLUME: [PrivilegeType.READ_VOLUME],
                SecurableType.FUNCTION: [PrivilegeType.EXECUTE],
                SecurableType.MODEL: [PrivilegeType.EXECUTE],  # Read/execute models
                SecurableType.SERVICE_CREDENTIAL: [PrivilegeType.ACCESS],  # Use service credentials
                # AI/ML Assets
                SecurableType.GENIE_SPACE: [PrivilegeType.ACCESS],  # Access Genie Space
                SecurableType.VECTOR_SEARCH_ENDPOINT: [PrivilegeType.ACCESS],  # Access endpoint
                SecurableType.VECTOR_SEARCH_INDEX: [PrivilegeType.ACCESS],  # Query index
            }
        )

    @classmethod
    def WRITER(cls) -> 'AccessPolicy':
        """Writer access policy - READ + WRITE privileges."""
        return cls(
            name='WRITER',
            privilege_map={
                SecurableType.CATALOG: [PrivilegeType.USE_CATALOG, PrivilegeType.CREATE_SCHEMA],
                SecurableType.SCHEMA: [
                    PrivilegeType.USE_SCHEMA, PrivilegeType.SELECT,
                    PrivilegeType.CREATE_TABLE, PrivilegeType.CREATE_VOLUME,
                    PrivilegeType.CREATE_FUNCTION, PrivilegeType.CREATE_MODEL,
                    PrivilegeType.MODIFY
                ],
                SecurableType.TABLE: [PrivilegeType.SELECT, PrivilegeType.MODIFY],
                SecurableType.VOLUME: [PrivilegeType.READ_VOLUME, PrivilegeType.WRITE_VOLUME],
                SecurableType.FUNCTION: [PrivilegeType.EXECUTE],
                SecurableType.MODEL: [PrivilegeType.EXECUTE, PrivilegeType.APPLY_TAG],  # Can use and update models
                SecurableType.SERVICE_CREDENTIAL: [PrivilegeType.ACCESS],  # Use service credentials
                # AI/ML Assets - writers can access but not manage
                SecurableType.GENIE_SPACE: [PrivilegeType.ACCESS],
                SecurableType.VECTOR_SEARCH_ENDPOINT: [PrivilegeType.ACCESS],
                SecurableType.VECTOR_SEARCH_INDEX: [PrivilegeType.ACCESS],
            }
        )

    @classmethod
    def ADMIN(cls) -> 'AccessPolicy':
        """Admin access policy - full management privileges."""
        return cls(
            name='ADMIN',
            privilege_map={
                SecurableType.CATALOG: [
                    PrivilegeType.ALL_PRIVILEGES  # ALL_PRIVILEGES only at catalog level
                ],
                SecurableType.SCHEMA: [
                    PrivilegeType.USE_SCHEMA, PrivilegeType.SELECT,
                    PrivilegeType.CREATE_TABLE, PrivilegeType.CREATE_VOLUME,
                    PrivilegeType.CREATE_FUNCTION, PrivilegeType.CREATE_MODEL,
                    PrivilegeType.MODIFY, PrivilegeType.MANAGE
                ],
                SecurableType.TABLE: [PrivilegeType.SELECT, PrivilegeType.MODIFY, PrivilegeType.MANAGE],
                SecurableType.VOLUME: [PrivilegeType.READ_VOLUME, PrivilegeType.WRITE_VOLUME, PrivilegeType.MANAGE],
                SecurableType.FUNCTION: [PrivilegeType.EXECUTE, PrivilegeType.MANAGE],
                SecurableType.MODEL: [PrivilegeType.EXECUTE, PrivilegeType.APPLY_TAG, PrivilegeType.MANAGE],
                SecurableType.SERVICE_CREDENTIAL: [PrivilegeType.ACCESS, PrivilegeType.MANAGE],  # Full access to service credentials
                # AI/ML Assets - full management
                SecurableType.GENIE_SPACE: [PrivilegeType.ACCESS, PrivilegeType.MANAGE],
                SecurableType.VECTOR_SEARCH_ENDPOINT: [PrivilegeType.ACCESS, PrivilegeType.MANAGE],
                SecurableType.VECTOR_SEARCH_INDEX: [PrivilegeType.ACCESS, PrivilegeType.MANAGE],
            }
        )

    @classmethod
    def ALL_PRIVILEGES(cls) -> 'AccessPolicy':
        """All privileges access policy."""
        return cls(
            name='ALL_PRIVILEGES',
            privilege_map={
                SecurableType.CATALOG: [PrivilegeType.ALL_PRIVILEGES],
            }
        )

    @classmethod
    def ALL_PRIVILEGES_CATALOG(cls) -> 'AccessPolicy':
        """All privileges for catalog only (no propagation)."""
        return cls(
            name='ALL_PRIVILEGES_CATALOG',
            privilege_map={
                SecurableType.CATALOG: [PrivilegeType.ALL_PRIVILEGES],
            }
        )

    @classmethod
    def OWNER_ADMIN(cls) -> 'AccessPolicy':
        """Owner admin privileges."""
        return cls(
            name='OWNER_ADMIN',
            privilege_map={
                SecurableType.CATALOG: [PrivilegeType.ALL_PRIVILEGES, PrivilegeType.MANAGE],
                SecurableType.SCHEMA: [
                    PrivilegeType.USE_SCHEMA, PrivilegeType.SELECT,
                    PrivilegeType.CREATE_TABLE, PrivilegeType.CREATE_VOLUME,
                    PrivilegeType.CREATE_FUNCTION, PrivilegeType.MODIFY,
                    PrivilegeType.MANAGE
                ],
                SecurableType.TABLE: [
                    PrivilegeType.SELECT, PrivilegeType.MODIFY,
                    PrivilegeType.MANAGE
                ],
                SecurableType.VOLUME: [
                    PrivilegeType.READ_VOLUME, PrivilegeType.WRITE_VOLUME,
                    PrivilegeType.MANAGE
                ],
                SecurableType.FUNCTION: [
                    PrivilegeType.EXECUTE, PrivilegeType.MANAGE
                ],
                SecurableType.STORAGE_CREDENTIAL: [
                    PrivilegeType.CREATE_EXTERNAL_LOCATION, PrivilegeType.CREATE_EXTERNAL_TABLE,
                    PrivilegeType.CREATE_EXTERNAL_VOLUME, PrivilegeType.MANAGE
                ],
                SecurableType.EXTERNAL_LOCATION: [
                    PrivilegeType.CREATE_EXTERNAL_TABLE, PrivilegeType.CREATE_EXTERNAL_VOLUME,
                    PrivilegeType.MANAGE
                ],
                SecurableType.CONNECTION: [
                    PrivilegeType.USE_CONNECTION, PrivilegeType.CREATE_FOREIGN_CATALOG,
                    PrivilegeType.MANAGE
                ]
            }
        )

    @classmethod
    def BROWSE_ONLY(cls) -> 'AccessPolicy':
        """Browse-only access policy for metadata discovery."""
        return cls(
            name='BROWSE_ONLY',
            privilege_map={
                SecurableType.CATALOG: [PrivilegeType.BROWSE],
                SecurableType.SCHEMA: [PrivilegeType.BROWSE],
                SecurableType.TABLE: [PrivilegeType.BROWSE],
                SecurableType.VOLUME: [PrivilegeType.BROWSE],
            }
        )

    @classmethod
    def DISCOVERER(cls) -> 'AccessPolicy':
        """Discoverer access policy - USE + BROWSE."""
        return cls(
            name='DISCOVERER',
            privilege_map={
                SecurableType.CATALOG: [PrivilegeType.USE_CATALOG, PrivilegeType.BROWSE],
                SecurableType.SCHEMA: [PrivilegeType.USE_SCHEMA, PrivilegeType.BROWSE],
                SecurableType.TABLE: [PrivilegeType.BROWSE],
                SecurableType.VOLUME: [PrivilegeType.BROWSE],
            }
        )
