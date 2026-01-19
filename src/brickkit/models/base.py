"""
Base classes and utilities for Unity Catalog governance models.

This module contains the foundational classes, environment management,
and common functionality used across all governance models.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional, Tuple

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
)

from .enums import Environment, PrivilegeType, SecurableType, validate_privilege_dependencies

# Configure logging
logger = logging.getLogger(__name__)

# Default owner for all securables - set at project level
DEFAULT_SECURABLE_OWNER = os.getenv('DEFAULT_SECURABLE_OWNER', 'platform_automation_spn')

# =============================================================================
# ENVIRONMENT MANAGEMENT
# =============================================================================

def get_current_environment() -> Environment:
    """
    Get the current environment from DATABRICKS_ENV variable.

    Returns Environment.DEV if not set or invalid.
    """
    env_str = os.getenv('DATABRICKS_ENV', 'dev').lower()
    try:
        return Environment(env_str.upper())
    except ValueError:
        logger.warning(f"Invalid DATABRICKS_ENV='{env_str}', defaulting to DEV")
        return Environment.DEV

# =============================================================================
# BASE CONFIGURATION
# =============================================================================

class BaseGovernanceModel(BaseModel):
    """
    Base model for all governance objects with common configuration.

    This provides standard Pydantic v2 configuration and common patterns
    used across all Unity Catalog governance models.
    """

    model_config = ConfigDict(
        arbitrary_types_allowed=True,  # Allow Databricks SDK types
        validate_assignment=False,  # Disabled for performance
        validate_default=True,  # Validate defaults once
        populate_by_name=True,  # Allow field population by name
        use_enum_values=False,  # Keep enums as enum objects
        str_strip_whitespace=True,  # Strip whitespace from strings
        json_schema_extra={
            "title": "Unity Catalog Governance Model",
            "description": "Base model for Unity Catalog governance objects"
        }
    )



# =============================================================================
# BASE SECURABLE CLASS
# =============================================================================

class BaseSecurable(BaseGovernanceModel):
    """
    Base class for all Unity Catalog securables with common grant logic.

    This class provides:
    - A standard grant() method that works with AccessPolicy objects
    - Abstract securable_type property that each subclass must implement
    - Common privilege management logic
    - Governance support via tags and defaults

    Governance features:
    - tags: List of Tag objects for metadata and ABAC
    - with_defaults(): Apply GovernanceDefaults to this securable
    - validate_governance(): Validate against governance rules
    """

    # Each securable must track its privileges - imported later to avoid circular imports
    privileges: List[Any] = Field(default_factory=list, description="Granted privileges")

    # Governance tags for metadata and attribute-based access control
    tags: List["Tag"] = Field(default_factory=list, description="Governance tags")

    @property
    def securable_type(self) -> SecurableType:
        """
        Return the type of this securable.

        Must be implemented by each securable class.
        """
        raise NotImplementedError(f"{self.__class__.__name__} must implement securable_type property")

    def get_level_1_name(self) -> str:
        """Get level-1 name. Override in subclasses."""
        raise NotImplementedError(f"{self.__class__.__name__} must implement get_level_1_name")

    def get_level_2_name(self) -> Optional[str]:
        """Get level-2 name. Override in subclasses if applicable."""
        return None

    def get_level_3_name(self) -> Optional[str]:
        """Get level-3 name. Override in subclasses if applicable."""
        return None

    def grant(self, principal: Any, policy: Any, _skip_validation: bool = False) -> List[Any]:
        """
        Grant privileges to a principal based on an access policy.

        The securable automatically extracts the appropriate privileges from the policy
        based on its own securable_type.

        Args:
            principal: The principal to grant to
            policy: The access policy defining privileges
            _skip_validation: Internal flag to skip dependency validation during propagation

        Returns:
            List of created Privilege objects (including propagated ones)

        Raises:
            ValueError: If privilege dependencies are not satisfied
        """
        logger.info(f"Granting {policy.name} policy to {principal.name} on {self.securable_type} '{getattr(self, 'name', 'unknown')}'")

        # Get privileges for this securable type from the policy
        privileges = policy.get_privileges(self.securable_type)
        logger.debug(f"Privileges from policy for {self.securable_type}: {privileges}")

        # Validate ALL_PRIVILEGES is only used at CATALOG level
        if PrivilegeType.ALL_PRIVILEGES in privileges and self.securable_type != SecurableType.CATALOG:
            raise ValueError(
                f"ALL_PRIVILEGES can only be granted at the CATALOG level, "
                f"not at {self.securable_type.value} level"
            )

        # Get existing privileges for this principal
        existing_privs_for_principal = {
            p.privilege for p in self.privileges
            if p.principal == principal.resolved_name
        }

        # Validate privilege dependencies (skip during propagation)
        if not _skip_validation:
            validation_errors = validate_privilege_dependencies(
                set(privileges),
                existing_privs_for_principal
            )
            if validation_errors:
                import warnings
                warnings.warn(
                    f"Privilege dependency validation failed for {getattr(self, 'name', 'unknown')}:\n" +
                    "\n".join(f"  - {error}" for error in validation_errors),
                    UserWarning,
                    stacklevel=2
                )

        result = []

        # Create set of existing grants for O(1) lookups
        existing_grants = {
            (p.principal, p.privilege, p.level_1, p.level_2, p.level_3)
            for p in self.privileges
        }

        # Grant privileges at this level
        for priv_type in privileges:
            # Build the privilege key for duplicate checking
            priv_key = self._build_privilege_key(principal, priv_type)

            if priv_key not in existing_grants:
                privilege = self._create_privilege(principal, priv_type)
                self.privileges.append(privilege)
                result.append(privilege)
                logger.debug(f"Created privilege: {priv_type} for {principal.resolved_name} on {getattr(self, 'name', 'unknown')}")

        # Handle propagation to children (implemented by subclasses)
        result.extend(self._propagate_grants(principal, policy))

        return result

    def _build_privilege_key(self, principal: Any, priv_type: PrivilegeType) -> tuple:
        """
        Build a unique key for privilege duplicate checking.

        Args:
            principal: The principal
            priv_type: The privilege type

        Returns:
            Tuple key for the privilege
        """
        return (
            principal.resolved_name,
            priv_type,
            self.get_level_1_name(),
            self.get_level_2_name() if hasattr(self, 'get_level_2_name') else None,
            self.get_level_3_name() if hasattr(self, 'get_level_3_name') else None
        )

    def _create_privilege(self, principal: Any, priv_type: PrivilegeType) -> Any:
        """
        Create a Privilege object for this securable.

        Args:
            principal: The principal to grant to
            priv_type: The privilege type

        Returns:
            Created Privilege object
        """
        # Import here to avoid circular dependency
        from .grants import Privilege
        return Privilege(
            level_1=self.get_level_1_name(),
            level_2=self.get_level_2_name() if hasattr(self, 'get_level_2_name') else None,
            level_3=self.get_level_3_name() if hasattr(self, 'get_level_3_name') else None,
            securable_type=self.securable_type,
            principal=principal.resolved_name,
            privilege=priv_type
        )

    def _propagate_grants(self, principal: Any, policy: Any) -> List[Any]:
        """
        Propagate grants to child securables.

        Default implementation does nothing. Override in container securables
        (Catalog, Schema) to propagate to children.

        Args:
            principal: The principal to grant to
            policy: The access policy

        Returns:
            List of propagated privileges
        """
        return []

    def with_defaults(self, defaults: Any) -> "BaseSecurable":
        """
        Apply governance defaults to this securable.

        Applies default tags from the GovernanceDefaults instance without
        overwriting existing tags. Returns self for method chaining.

        Args:
            defaults: GovernanceDefaults instance with org-wide policies

        Returns:
            Self for method chaining

        Example:
            catalog = Catalog(name="analytics").with_defaults(MyOrgDefaults())
        """
        defaults.apply_to(self, get_current_environment())
        return self

    def with_convention(self, convention: Any) -> "BaseSecurable":
        """
        Apply a Convention to this securable and propagate to children.

        Applies convention defaults (tags, etc.) to this securable and
        recursively to all children. Returns self for method chaining.

        Args:
            convention: Convention instance with governance rules

        Returns:
            Self for method chaining

        Example:
            catalog = Catalog(name="analytics").with_convention(my_convention)
        """
        env = get_current_environment()
        convention.apply_to(self, env)
        self._propagate_convention(convention, env)
        return self

    def _propagate_convention(self, convention: Any, env: "Environment") -> None:
        """
        Propagate convention to child securables.

        Override in container securables (Catalog, Schema) to propagate
        to children.

        Args:
            convention: The convention to propagate
            env: Current environment
        """
        pass  # Default: no children to propagate to

    def validate_governance(self, defaults: Any = None) -> List[str]:
        """
        Validate this securable against governance rules.

        Checks that all required tags are present and have valid values.

        Args:
            defaults: Optional GovernanceDefaults to validate against.
                     If not provided, returns empty list.

        Returns:
            List of validation error messages (empty if valid)

        Example:
            errors = catalog.validate_governance(MyOrgDefaults())
            if errors:
                raise ValueError(f"Governance validation failed: {errors}")
        """
        if not defaults:
            return []

        # Convert tags list to dict for validation
        tag_dict = {t.key: t.value for t in self.tags}
        return defaults.validate_tags(self.securable_type, tag_dict)

    def grant_many(
        self,
        principals: List[Any],
        policy: Any
    ) -> Dict[str, List[Any]]:
        """
        Grant privileges to multiple principals at once.

        This is a convenience method that calls grant() for each principal
        and returns a dictionary mapping principal names to their granted privileges.

        Args:
            principals: List of principals to grant to
            policy: The access policy defining privileges

        Returns:
            Dict mapping principal resolved names to granted privileges

        Example:
            # Grant reader access to multiple users
            results = catalog.grant_many(
                [alice, bob, carol],
                AccessPolicy.READER()
            )
            # Returns: {'alice_dev': [...], 'bob_dev': [...], 'carol_dev': [...]}
        """
        results = {}
        for principal in principals:
            granted_privileges = self.grant(principal, policy)
            results[principal.resolved_name] = granted_privileges
        return results

    def grant_all(
        self,
        grants: List[Tuple[Any, Any]]
    ) -> Dict[str, List[Any]]:
        """
        Apply multiple grant combinations at once.

        This allows granting different policies to different principals in
        a single batch operation.

        Args:
            grants: List of (principal, policy) tuples

        Returns:
            Dict mapping principal resolved names to granted privileges

        Example:
            # Different access levels for different users
            results = catalog.grant_all([
                (alice, AccessPolicy.OWNER_ADMIN()),
                (bob, AccessPolicy.WRITER()),
                (carol, AccessPolicy.READER())
            ])
        """
        results = {}
        for principal, policy in grants:
            granted_privileges = self.grant(principal, policy)
            results[principal.resolved_name] = granted_privileges
        return results


# =============================================================================
# HELPER CLASSES
# =============================================================================

class Tag(BaseGovernanceModel):
    """
    Key-value pairs for metadata and attribute-based access control (ABAC).

    Tags serve dual purpose:
    - Metadata annotation for discovery and governance
    - Security control mechanism for conditional access

    This class provides conversion methods to Databricks SDK tag objects
    for proper integration with the entity_tag_assignments API.
    """
    key: str = Field(..., description="Tag key (e.g., 'environment', 'cost_center', 'pii')")
    value: str = Field(..., description="Tag value (e.g., 'production', 'analytics', 'true')")

    def __hash__(self) -> int:
        """Make Tag hashable for use in sets."""
        return hash((self.key, self.value))

    def __eq__(self, other: object) -> bool:
        """Compare tags by key and value."""
        if not isinstance(other, Tag):
            return False
        return self.key == other.key and self.value == other.value

    def to_sdk_tag_key_value(self) -> Any:
        """
        Convert to SDK TagKeyValue object.

        Returns:
            databricks.sdk.service.catalog.TagKeyValue
        """
        from databricks.sdk.service.catalog import TagKeyValue
        return TagKeyValue(key=self.key, value=self.value)

    def to_entity_assignment(self, entity_name: str, entity_type: str) -> Any:
        """
        Create an EntityTagAssignment for this tag.

        Args:
            entity_name: Full name of the entity (e.g., 'catalog.schema.table')
            entity_type: Type of entity ('catalog', 'schema', 'table', 'volume')

        Returns:
            databricks.sdk.service.catalog.EntityTagAssignment
        """
        from databricks.sdk.service.catalog import EntityTagAssignment
        return EntityTagAssignment(
            entity_name=entity_name,
            entity_type=entity_type,
            tag_key=self.key,
            tag_value=self.value
        )

    @classmethod
    def from_sdk_assignment(cls, assignment: Any) -> 'Tag':
        """
        Create Tag from SDK EntityTagAssignment.

        Args:
            assignment: databricks.sdk.service.catalog.EntityTagAssignment

        Returns:
            Tag instance
        """
        return cls(
            key=assignment.tag_key,
            value=assignment.tag_value or ""
        )


