"""
Validator for external principals (Entra ID / SCIM-synced).

Provides utilities to detect and validate externally-managed principals
that are synced via SCIM from identity providers like Entra ID (Azure AD).
"""

import logging
from dataclasses import dataclass
from typing import Optional, Set

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, PermissionDenied, ResourceDoesNotExist

from brickkit.models.enums import PrincipalType

logger = logging.getLogger(__name__)


@dataclass
class ExternalPrincipalInfo:
    """Information about an external principal."""

    name: str
    principal_type: PrincipalType
    external_id: Optional[str]
    is_external: bool
    exists: bool


class ExternalPrincipalValidator:
    """
    Validates external principals against workspace SCIM sync.

    External principals (from Entra ID) are read-only in Databricks:
    - Cannot be created/deleted via API
    - Can have entitlements/ACLs set
    - Must exist (synced) before referencing

    Example:
        ```python
        validator = ExternalPrincipalValidator(workspace_client)

        # Check if SCIM sync is enabled
        if validator.is_scim_sync_enabled():
            print("SCIM sync is active")

        # Validate an external group exists
        if validator.validate_external_exists("Cloud-Databricks-Admins", PrincipalType.GROUP):
            print("External group is synced")

        # Check if a specific group is externally managed
        if validator.is_external_group("Cloud-Databricks-Admins"):
            print("Group is managed by IdP")
        ```
    """

    def __init__(self, client: WorkspaceClient):
        """
        Initialize the validator.

        Args:
            client: Databricks WorkspaceClient
        """
        self.client = client
        self._scim_sync_detected: Optional[bool] = None
        self._external_groups_cache: Optional[Set[str]] = None

    def is_scim_sync_enabled(self, force_refresh: bool = False) -> bool:
        """
        Detect if SCIM sync is enabled for this workspace.

        Uses a heuristic: checks if any principals have external_id set,
        which indicates they were synced from an external IdP.

        Args:
            force_refresh: If True, bypass cache and re-check

        Returns:
            True if SCIM sync appears to be enabled
        """
        if self._scim_sync_detected is not None and not force_refresh:
            return self._scim_sync_detected

        self._scim_sync_detected = False

        try:
            # Check groups for external_id
            for group in self.client.groups.list(count=20):
                if group.external_id:
                    self._scim_sync_detected = True
                    logger.info("SCIM sync detected via external group")
                    return True

            # Check service principals for external_id
            for sp in self.client.service_principals.list(count=20):
                if sp.external_id:
                    self._scim_sync_detected = True
                    logger.info("SCIM sync detected via external service principal")
                    return True

        except PermissionDenied:
            logger.warning("Permission denied checking for SCIM sync - assuming not enabled")
            return self._scim_sync_detected
        except (NotFound, ResourceDoesNotExist) as e:
            logger.debug(f"Resource not found while checking SCIM sync: {e}")
            return self._scim_sync_detected

    def is_external_group(self, group_name: str) -> bool:
        """
        Check if a group is externally synced (has external_id).

        Args:
            group_name: The group's display name

        Returns:
            True if the group has an external_id set
        """
        try:
            groups = list(self.client.groups.list(filter=f'displayName eq "{group_name}"'))
            if groups and groups[0].external_id:
                return True
            return False
        except (NotFound, ResourceDoesNotExist):
            logger.debug(f"Group {group_name} not found when checking if external")
            return False
        except PermissionDenied:
            raise

    def is_external_service_principal(self, spn_name: str) -> bool:
        """
        Check if a service principal is externally synced (has external_id).

        Args:
            spn_name: The service principal's display name

        Returns:
            True if the SPN has an external_id set
        """
        try:
            sps = list(self.client.service_principals.list(filter=f'displayName eq "{spn_name}"'))
            if sps and sps[0].external_id:
                return True
            return False
        except (NotFound, ResourceDoesNotExist):
            logger.debug(f"Service principal {spn_name} not found when checking if external")
            return False
        except PermissionDenied:
            raise

    def validate_external_exists(self, name: str, principal_type: PrincipalType) -> bool:
        """
        Validate that an external principal exists in the workspace.

        Args:
            name: Principal name (display name)
            principal_type: Type of principal

        Returns:
            True if principal exists, False otherwise
        """
        try:
            if principal_type == PrincipalType.GROUP:
                groups = list(self.client.groups.list(filter=f'displayName eq "{name}"'))
                return len(groups) > 0
            elif principal_type == PrincipalType.SERVICE_PRINCIPAL:
                sps = list(self.client.service_principals.list(filter=f'displayName eq "{name}"'))
                return len(sps) > 0
            elif principal_type == PrincipalType.USER:
                users = list(self.client.users.list(filter=f'userName eq "{name}"'))
                return len(users) > 0
            return False
        except (NotFound, ResourceDoesNotExist):
            logger.debug(f"Principal {name} ({principal_type}) not found")
            return False
        except PermissionDenied:
            raise

    def get_principal_info(self, name: str, principal_type: PrincipalType) -> ExternalPrincipalInfo:
        """
        Get detailed information about a principal.

        Args:
            name: Principal name
            principal_type: Type of principal

        Returns:
            ExternalPrincipalInfo with details about the principal
        """
        external_id = None
        exists = False
        is_external = False

        try:
            if principal_type == PrincipalType.GROUP:
                groups = list(self.client.groups.list(filter=f'displayName eq "{name}"'))
                if groups:
                    exists = True
                    external_id = groups[0].external_id
                    is_external = external_id is not None
            elif principal_type == PrincipalType.SERVICE_PRINCIPAL:
                sps = list(self.client.service_principals.list(filter=f'displayName eq "{name}"'))
                if sps:
                    exists = True
                    external_id = sps[0].external_id
                    is_external = external_id is not None
            elif principal_type == PrincipalType.USER:
                users = list(self.client.users.list(filter=f'userName eq "{name}"'))
                if users:
                    exists = True
                    external_id = users[0].external_id if hasattr(users[0], "external_id") else None
                    is_external = external_id is not None
        except (NotFound, ResourceDoesNotExist):
            logger.debug(f"Principal {name} ({principal_type}) not found when getting info")
        except PermissionDenied:
            raise

        return ExternalPrincipalInfo(
            name=name, principal_type=principal_type, external_id=external_id, is_external=is_external, exists=exists
        )

    def list_external_groups(self, force_refresh: bool = False) -> Set[str]:
        """
        List all externally-synced groups in the workspace.

        Args:
            force_refresh: If True, bypass cache and re-fetch

        Returns:
            Set of group display names that are externally synced
        """
        if self._external_groups_cache is not None and not force_refresh:
            return self._external_groups_cache

        external_groups: Set[str] = set()

        try:
            for group in self.client.groups.list():
                if group.external_id and group.display_name:
                    external_groups.add(group.display_name)
        except PermissionDenied:
            logger.warning("Permission denied listing groups")
        except (NotFound, ResourceDoesNotExist) as e:
            logger.debug(f"Resource not found listing external groups: {e}")

        self._external_groups_cache = external_groups
        return external_groups

    def clear_cache(self) -> None:
        """Clear all cached data."""
        self._scim_sync_detected = None
        self._external_groups_cache = None
