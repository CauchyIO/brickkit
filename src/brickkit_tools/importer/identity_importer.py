"""
Identity resource importers.

Imports users, groups, and service principals from the workspace.
"""

import logging
import time
from dataclasses import dataclass, field
from typing import Any, List, Optional

from databricks.sdk.errors import NotFound, PermissionDenied

from .base import ImportOptions, ImportResult, ResourceImporter

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Identity Models (simple dataclasses for imported resources)
# These could be promoted to brickkit proper if needed
# -----------------------------------------------------------------------------


@dataclass
class User:
    """Imported user from Databricks workspace."""

    id: str
    user_name: str  # Email address
    display_name: Optional[str] = None
    active: bool = True
    groups: List[str] = field(default_factory=list)

    # Additional metadata
    external_id: Optional[str] = None

    def matches_pattern(self, pattern: str) -> bool:
        """Check if user_name matches a naming pattern."""
        import re

        return bool(re.match(pattern, self.user_name))


@dataclass
class Group:
    """Imported group from Databricks workspace."""

    id: str
    display_name: str
    members: List[str] = field(default_factory=list)  # User IDs or nested group IDs
    roles: List[str] = field(default_factory=list)
    entitlements: List[str] = field(default_factory=list)

    # Metadata
    external_id: Optional[str] = None
    meta: Optional[dict] = None

    def matches_pattern(self, pattern: str) -> bool:
        """Check if display_name matches a naming pattern."""
        import re

        return bool(re.match(pattern, self.display_name))


@dataclass
class ServicePrincipal:
    """Imported service principal from Databricks workspace."""

    id: str
    application_id: str
    display_name: str
    active: bool = True
    groups: List[str] = field(default_factory=list)
    roles: List[str] = field(default_factory=list)
    entitlements: List[str] = field(default_factory=list)

    # Metadata
    external_id: Optional[str] = None

    def matches_pattern(self, pattern: str) -> bool:
        """Check if display_name matches a naming pattern."""
        import re

        return bool(re.match(pattern, self.display_name))


# -----------------------------------------------------------------------------
# Importers
# -----------------------------------------------------------------------------


class UserImporter(ResourceImporter[User]):
    """
    Import users from the workspace.

    Note: This can be slow for large workspaces with many users.
    Consider using exclude patterns or include patterns to filter.
    """

    @property
    def resource_type(self) -> str:
        return "users"

    def pull_all(self) -> ImportResult:
        start_time = time.time()
        users: List[User] = []
        errors: List[str] = []
        skipped = 0

        try:
            for user_info in self.client.users.list():
                user_name = user_info.user_name or ""

                if not self._should_include(user_name):
                    skipped += 1
                    continue

                try:
                    user = self._from_info(user_info)
                    users.append(user)
                except Exception as e:
                    errors.append(f"Failed to import user '{user_name}': {e}")
                    if not self.options.skip_on_error:
                        raise

        except PermissionDenied as e:
            errors.append(f"Permission denied listing users: {e}")

        return ImportResult(
            resource_type=self.resource_type,
            count=len(users),
            resources=users,
            errors=errors,
            skipped=skipped,
            duration_seconds=time.time() - start_time,
        )

    def pull_one(self, identifier: str, **kwargs: Any) -> Optional[User]:
        """Pull a single user by ID."""
        try:
            user_info = self.client.users.get(identifier)
            return self._from_info(user_info)
        except NotFound:
            return None

    def _from_info(self, info: Any) -> User:
        """Convert SDK User to our User dataclass."""
        # Extract group memberships
        groups = []
        if hasattr(info, "groups") and info.groups:
            groups = [g.display for g in info.groups if hasattr(g, "display")]

        return User(
            id=info.id,
            user_name=info.user_name or "",
            display_name=info.display_name,
            active=info.active if hasattr(info, "active") else True,
            groups=groups,
            external_id=info.external_id if hasattr(info, "external_id") else None,
        )


class GroupImporter(ResourceImporter[Group]):
    """Import groups from the workspace."""

    @property
    def resource_type(self) -> str:
        return "groups"

    def pull_all(self) -> ImportResult:
        start_time = time.time()
        groups: List[Group] = []
        errors: List[str] = []
        skipped = 0

        try:
            for group_info in self.client.groups.list():
                display_name = group_info.display_name or ""

                if not self._should_include(display_name):
                    skipped += 1
                    continue

                try:
                    group = self._from_info(group_info)
                    groups.append(group)
                except Exception as e:
                    errors.append(f"Failed to import group '{display_name}': {e}")
                    if not self.options.skip_on_error:
                        raise

        except PermissionDenied as e:
            errors.append(f"Permission denied listing groups: {e}")

        return ImportResult(
            resource_type=self.resource_type,
            count=len(groups),
            resources=groups,
            errors=errors,
            skipped=skipped,
            duration_seconds=time.time() - start_time,
        )

    def pull_one(self, identifier: str, **kwargs: Any) -> Optional[Group]:
        """Pull a single group by ID."""
        try:
            group_info = self.client.groups.get(identifier)
            return self._from_info(group_info)
        except NotFound:
            return None

    def _from_info(self, info: Any) -> Group:
        """Convert SDK Group to our Group dataclass."""
        # Extract members
        members = []
        if hasattr(info, "members") and info.members:
            members = [m.value for m in info.members if hasattr(m, "value")]

        # Extract roles
        roles = []
        if hasattr(info, "roles") and info.roles:
            roles = [r.value for r in info.roles if hasattr(r, "value")]

        # Extract entitlements
        entitlements = []
        if hasattr(info, "entitlements") and info.entitlements:
            entitlements = [e.value for e in info.entitlements if hasattr(e, "value")]

        return Group(
            id=info.id,
            display_name=info.display_name or "",
            members=members,
            roles=roles,
            entitlements=entitlements,
            external_id=info.external_id if hasattr(info, "external_id") else None,
        )


class ServicePrincipalImporter(ResourceImporter[ServicePrincipal]):
    """
    Import service principals from the workspace.

    Service principals are often subject to naming conventions like:
    - sp-{team}-{purpose}
    - svc-{application}-{environment}
    """

    @property
    def resource_type(self) -> str:
        return "service_principals"

    def pull_all(self) -> ImportResult:
        start_time = time.time()
        principals: List[ServicePrincipal] = []
        errors: List[str] = []
        skipped = 0

        try:
            for sp_info in self.client.service_principals.list():
                display_name = sp_info.display_name or ""

                if not self._should_include(display_name):
                    skipped += 1
                    continue

                try:
                    sp = self._from_info(sp_info)
                    principals.append(sp)
                except Exception as e:
                    errors.append(f"Failed to import service principal '{display_name}': {e}")
                    if not self.options.skip_on_error:
                        raise

        except PermissionDenied as e:
            errors.append(f"Permission denied listing service principals: {e}")

        return ImportResult(
            resource_type=self.resource_type,
            count=len(principals),
            resources=principals,
            errors=errors,
            skipped=skipped,
            duration_seconds=time.time() - start_time,
        )

    def pull_one(self, identifier: str, **kwargs: Any) -> Optional[ServicePrincipal]:
        """Pull a single service principal by ID."""
        try:
            sp_info = self.client.service_principals.get(identifier)
            return self._from_info(sp_info)
        except NotFound:
            return None

    def _from_info(self, info: Any) -> ServicePrincipal:
        """Convert SDK ServicePrincipal to our ServicePrincipal dataclass."""
        # Extract groups
        groups = []
        if hasattr(info, "groups") and info.groups:
            groups = [g.display for g in info.groups if hasattr(g, "display")]

        # Extract roles
        roles = []
        if hasattr(info, "roles") and info.roles:
            roles = [r.value for r in info.roles if hasattr(r, "value")]

        # Extract entitlements
        entitlements = []
        if hasattr(info, "entitlements") and info.entitlements:
            entitlements = [e.value for e in info.entitlements if hasattr(e, "value")]

        return ServicePrincipal(
            id=info.id,
            application_id=info.application_id or "",
            display_name=info.display_name or "",
            active=info.active if hasattr(info, "active") else True,
            groups=groups,
            roles=roles,
            entitlements=entitlements,
            external_id=info.external_id if hasattr(info, "external_id") else None,
        )
