"""
Principal models for managing Databricks Groups and Service Principals.

This module provides governance wrappers around the Databricks SDK IAM types,
adding environment-aware naming, declarative membership, and external principal support.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Dict, List, Optional

if TYPE_CHECKING:
    from brickkit.models.grants import Principal

from databricks.sdk.service.iam import (
    ComplexValue,
)
from databricks.sdk.service.iam import (
    Group as SdkGroup,
)
from databricks.sdk.service.iam import (
    ServicePrincipal as SdkServicePrincipal,
)
from pydantic import Field, computed_field

from .base import BaseGovernanceModel, get_current_environment
from .enums import Environment, PrincipalSource, PrincipalType, WorkspaceEntitlement

logger = logging.getLogger(__name__)


class MemberReference(BaseGovernanceModel):
    """
    Reference to a principal that should be a group member.

    Separates the 'what we want' (member definition) from
    'what exists' (SDK ComplexValue).
    """

    name: str = Field(..., description="Principal name or email")
    principal_type: PrincipalType = Field(..., description="Type of principal")
    add_environment_suffix: bool = Field(default=True, description="Whether to add environment suffix to the name")

    @computed_field
    @property
    def resolved_name(self) -> str:
        """Get environment-aware name."""
        # Users never get environment suffixes
        if self.principal_type == PrincipalType.USER:
            return self.name
        if not self.add_environment_suffix:
            return self.name
        env = get_current_environment()
        return f"{self.name}_{env.value.lower()}"

    def to_complex_value(self) -> ComplexValue:
        """Convert to SDK ComplexValue for API calls."""
        return ComplexValue(value=self.resolved_name)


class ManagedGroup(BaseGovernanceModel):
    """
    Governance wrapper for Databricks Group.

    Adds:
    - Environment-aware naming
    - Declarative membership (desired state)
    - External principal detection
    - Convention validation

    Example:
        ```python
        group = ManagedGroup(name="grp_data_engineering")
        group.add_user("alice@company.com")
        group.add_service_principal("spn_etl_pipeline")
        group.add_entitlement("workspace-access")  # optional
        ```
    """

    name: str = Field(..., description="Base group name")
    display_name: Optional[str] = Field(default=None, description="Human-readable display name")

    # Source - determines create vs validate behavior
    source: PrincipalSource = Field(default=PrincipalSource.DATABRICKS, description="Origin of the principal")
    external_id: Optional[str] = Field(
        default=None, description="External ID (e.g., Entra Object ID) for external groups"
    )

    # Environment configuration
    add_environment_suffix: bool = Field(default=True, description="Whether to add environment suffix to the name")
    environment_mapping: Dict[Environment, str] = Field(
        default_factory=dict, description="Custom per-environment names"
    )

    # Desired membership (declarative)
    members: List[MemberReference] = Field(default_factory=list, description="Desired group members")

    # Desired entitlements (e.g., "workspace-access", "databricks-sql-access")
    entitlements: List[str] = Field(default_factory=list, description="Workspace entitlements to assign")

    # Roles (AWS instance profiles, etc.)
    roles: List[str] = Field(default_factory=list, description="Roles to assign (e.g., AWS instance profile ARNs)")

    # Internal: SDK group ID after creation/lookup
    _sdk_id: Optional[str] = None

    @computed_field
    @property
    def resolved_name(self) -> str:
        """Environment-aware name resolution."""
        env = get_current_environment()

        # Priority 1: Custom mapping
        if env in self.environment_mapping:
            return self.environment_mapping[env]

        # Priority 2: No suffix
        if not self.add_environment_suffix:
            return self.name

        # Priority 3: Auto suffix
        return f"{self.name}_{env.value.lower()}"

    def to_sdk_group(self) -> SdkGroup:
        """Convert to SDK Group for API calls."""
        return SdkGroup(
            display_name=self.display_name or self.resolved_name,
            external_id=self.external_id,
            members=[m.to_complex_value() for m in self.members],
            entitlements=[ComplexValue(value=e) for e in self.entitlements],
            roles=[ComplexValue(value=r) for r in self.roles],
        )

    @classmethod
    def from_sdk_group(
        cls, sdk_group: SdkGroup, source: PrincipalSource = PrincipalSource.DATABRICKS
    ) -> "ManagedGroup":
        """
        Create from SDK Group (e.g., when importing).

        Args:
            sdk_group: The SDK Group object
            source: Whether this is a native or external group

        Returns:
            ManagedGroup instance
        """
        # Detect external source from external_id
        if sdk_group.external_id:
            source = PrincipalSource.EXTERNAL

        # Parse members back to MemberReference
        members = []
        if sdk_group.members:
            for m in sdk_group.members:
                # Determine type from $ref or assume user
                member_type = PrincipalType.USER
                if m.ref and "Groups" in m.ref:
                    member_type = PrincipalType.GROUP
                elif m.ref and "ServicePrincipals" in m.ref:
                    member_type = PrincipalType.SERVICE_PRINCIPAL
                members.append(
                    MemberReference(
                        name=m.value or m.display or "",
                        principal_type=member_type,
                        add_environment_suffix=False,  # Already resolved
                    )
                )

        # Parse entitlements
        entitlements = []
        if sdk_group.entitlements:
            for e in sdk_group.entitlements:
                if e.value:
                    entitlements.append(e.value)

        # Parse roles
        roles = [r.value for r in (sdk_group.roles or []) if r.value]

        group = cls(
            name=sdk_group.display_name or "",
            display_name=sdk_group.display_name,
            source=source,
            external_id=sdk_group.external_id,
            add_environment_suffix=False,  # Already resolved
            members=members,
            entitlements=entitlements,
            roles=roles,
        )
        group._sdk_id = sdk_group.id
        return group

    # Convenience methods for building membership
    def add_user(self, email: str) -> "ManagedGroup":
        """
        Add a user member by email.

        Args:
            email: User's email address

        Returns:
            Self for chaining
        """
        self.members.append(
            MemberReference(
                name=email,
                principal_type=PrincipalType.USER,
                add_environment_suffix=False,  # Users don't get suffixes
            )
        )
        return self

    def add_service_principal(self, name: str, add_env_suffix: bool = True) -> "ManagedGroup":
        """
        Add a service principal member.

        Args:
            name: Service principal name
            add_env_suffix: Whether to add environment suffix

        Returns:
            Self for chaining
        """
        self.members.append(
            MemberReference(
                name=name, principal_type=PrincipalType.SERVICE_PRINCIPAL, add_environment_suffix=add_env_suffix
            )
        )
        return self

    def add_nested_group(self, name: str, add_env_suffix: bool = True) -> "ManagedGroup":
        """
        Add a nested group member.

        Args:
            name: Group name
            add_env_suffix: Whether to add environment suffix

        Returns:
            Self for chaining
        """
        self.members.append(
            MemberReference(name=name, principal_type=PrincipalType.GROUP, add_environment_suffix=add_env_suffix)
        )
        return self

    def add_entitlement(self, entitlement: str | WorkspaceEntitlement) -> "ManagedGroup":
        """
        Add an entitlement.

        Args:
            entitlement: The entitlement to add (enum or string like "workspace-access")

        Returns:
            Self for chaining

        Raises:
            ValueError: If string doesn't match a known entitlement
        """
        if isinstance(entitlement, WorkspaceEntitlement):
            value = entitlement.value
        else:
            # Validate string against known entitlements
            valid_values = {e.value for e in WorkspaceEntitlement}
            if entitlement not in valid_values:
                raise ValueError(
                    f"Unknown entitlement '{entitlement}'. Valid values: {', '.join(sorted(valid_values))}"
                )
            value = entitlement

        if value not in self.entitlements:
            self.entitlements.append(value)
        return self


class ManagedServicePrincipal(BaseGovernanceModel):
    """
    Governance wrapper for Databricks ServicePrincipal.

    Adds:
    - Environment-aware naming
    - Declarative entitlements
    - External principal detection (Entra app registrations)

    Example:
        ```python
        spn = ManagedServicePrincipal(name="spn_etl_pipeline")
        spn.add_entitlement("workspace-access")  # optional
        ```
    """

    name: str = Field(..., description="Base service principal name")
    display_name: Optional[str] = Field(default=None, description="Human-readable display name")
    application_id: Optional[str] = Field(
        default=None, description="Application/Client ID (for external SPNs from Entra)"
    )

    # Source
    source: PrincipalSource = Field(default=PrincipalSource.DATABRICKS, description="Origin of the principal")
    external_id: Optional[str] = Field(default=None, description="External ID (e.g., Entra Object ID)")

    # Environment configuration
    add_environment_suffix: bool = Field(default=True, description="Whether to add environment suffix to the name")
    environment_mapping: Dict[Environment, str] = Field(
        default_factory=dict, description="Custom per-environment names"
    )

    # Desired entitlements (e.g., "workspace-access", "databricks-sql-access")
    entitlements: List[str] = Field(default_factory=list, description="Workspace entitlements to assign")

    # Groups this SPN should belong to (for reference, not managed here)
    group_memberships: List[str] = Field(default_factory=list, description="Group names this SPN should be a member of")

    # Active status
    active: bool = Field(default=True, description="Whether the SPN is active")

    # Internal: SDK ID after creation/lookup
    _sdk_id: Optional[str] = None

    @computed_field
    @property
    def resolved_name(self) -> str:
        """Environment-aware name resolution."""
        env = get_current_environment()

        # Priority 1: Custom mapping
        if env in self.environment_mapping:
            return self.environment_mapping[env]

        # Priority 2: No suffix
        if not self.add_environment_suffix:
            return self.name

        # Priority 3: Auto suffix
        return f"{self.name}_{env.value.lower()}"

    def to_sdk_service_principal(self) -> SdkServicePrincipal:
        """Convert to SDK ServicePrincipal for API calls."""
        return SdkServicePrincipal(
            display_name=self.display_name or self.resolved_name,
            application_id=self.application_id,
            external_id=self.external_id,
            entitlements=[ComplexValue(value=e) for e in self.entitlements],
            active=self.active,
        )

    @classmethod
    def from_sdk_service_principal(
        cls, sdk_sp: SdkServicePrincipal, source: PrincipalSource = PrincipalSource.DATABRICKS
    ) -> "ManagedServicePrincipal":
        """
        Create from SDK ServicePrincipal (e.g., when importing).

        Args:
            sdk_sp: The SDK ServicePrincipal object
            source: Whether this is a native or external SPN

        Returns:
            ManagedServicePrincipal instance
        """
        # Detect external source from external_id
        if sdk_sp.external_id:
            source = PrincipalSource.EXTERNAL

        # Parse entitlements
        entitlements = []
        if sdk_sp.entitlements:
            for e in sdk_sp.entitlements:
                if e.value:
                    entitlements.append(e.value)

        # Parse group memberships
        group_memberships = []
        if sdk_sp.groups:
            group_memberships = [g.display or g.value or "" for g in sdk_sp.groups if g.display or g.value]

        spn = cls(
            name=sdk_sp.display_name or "",
            display_name=sdk_sp.display_name,
            application_id=sdk_sp.application_id,
            source=source,
            external_id=sdk_sp.external_id,
            add_environment_suffix=False,  # Already resolved
            entitlements=entitlements,
            group_memberships=group_memberships,
            active=sdk_sp.active if sdk_sp.active is not None else True,
        )
        spn._sdk_id = sdk_sp.id
        return spn

    def add_entitlement(self, entitlement: str | WorkspaceEntitlement) -> "ManagedServicePrincipal":
        """
        Add an entitlement.

        Args:
            entitlement: The entitlement to add (enum or string like "workspace-access")

        Returns:
            Self for chaining

        Raises:
            ValueError: If string doesn't match a known entitlement
        """
        if isinstance(entitlement, WorkspaceEntitlement):
            value = entitlement.value
        else:
            # Validate string against known entitlements
            valid_values = {e.value for e in WorkspaceEntitlement}
            if entitlement not in valid_values:
                raise ValueError(
                    f"Unknown entitlement '{entitlement}'. Valid values: {', '.join(sorted(valid_values))}"
                )
            value = entitlement

        if value not in self.entitlements:
            self.entitlements.append(value)
        return self

    def to_principal(self) -> "Principal":
        """
        Create a Principal for use in grants.

        Returns a Principal configured with the application_id if available,
        which is required for grants to service principals in Databricks.

        Returns:
            Principal instance suitable for grant operations

        Raises:
            ValueError: If application_id is not set (SPN not yet created)

        Example:
            ```python
            spn = ManagedServicePrincipal(name="spn_admin")
            result, credentials = executor.create_with_secret(spn)
            # After creation, application_id is set
            principal = spn.to_principal()
            catalog.grant(principal, AccessPolicy.ADMIN())
            ```
        """
        # Import here to avoid circular imports
        from brickkit.models.grants import Principal

        if not self.application_id:
            raise ValueError(
                f"Cannot create Principal for grants: application_id not set for {self.resolved_name}. "
                "Create the service principal first, then call to_principal()."
            )

        return Principal(
            name=self.name,
            principal_type=PrincipalType.SERVICE_PRINCIPAL,
            application_id=self.application_id,
            add_environment_suffix=self.add_environment_suffix,
            environment_mapping=self.environment_mapping,
        )
