"""
Grant executor for Unity Catalog privilege management.

Handles granting and revoking privileges via the Databricks SDK.
"""

import logging
import time
from collections import defaultdict
from typing import Dict, List, Optional, Set, Tuple

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import (
    BadRequest,
    NotFound,
    PermissionDenied,
    ResourceDoesNotExist,
)
from databricks.sdk.service.catalog import PermissionsChange

from brickkit.models import Privilege, SecurableType

from .base import BaseExecutor, ExecutionResult, OperationType

logger = logging.getLogger(__name__)


class PrincipalNotFoundError(Exception):
    """Raised when a principal does not exist in Databricks."""

    def __init__(self, principal_name: str):
        self.principal_name = principal_name
        super().__init__(f"Principal '{principal_name}' does not exist in Databricks")


class SecurableNotFoundError(Exception):
    """Raised when a securable does not exist in Databricks."""

    def __init__(self, securable_type: str, securable_name: str):
        self.securable_type = securable_type
        self.securable_name = securable_name
        super().__init__(f"{securable_type} '{securable_name}' does not exist in Databricks")


def _get_enum_value(val) -> str:
    """Safely get value from enum or return string as-is."""
    return val.value if hasattr(val, "value") else val


class GrantExecutor(BaseExecutor[Privilege]):
    """Executor for privilege grant operations."""

    def __init__(
        self,
        client: WorkspaceClient,
        dry_run: bool = False,
        force: bool = False,
        validate_principals: bool = True,
        strict_mode: bool = True,
        max_retries: int = 3,
        continue_on_error: bool = False,
    ):
        """
        Initialize the grant executor.

        Args:
            client: Databricks workspace client
            dry_run: If True, only log actions without executing
            force: If True, force operations even if they appear unchanged
            validate_principals: If True, validate principal existence before grants
            strict_mode: If True (default), raise exceptions when principals or securables
                        don't exist. If False, return failed ExecutionResults instead.
            max_retries: Maximum retry attempts for transient failures
            continue_on_error: Continue execution despite errors
        """
        super().__init__(client, dry_run, max_retries, continue_on_error)
        self.force = force  # Store force separately since parent doesn't have it
        self.validate_principals = validate_principals
        self.strict_mode = strict_mode
        self._principal_cache: Dict[str, bool] = {}  # Cache for principal validation

    def get_resource_type(self) -> str:
        """Get the resource type."""
        return "PRIVILEGE"

    def exists(self, resource: Privilege) -> bool:
        """
        Check if a privilege grant exists.

        Args:
            resource: The privilege to check

        Returns:
            True if the grant exists
        """
        try:
            full_name = self._get_full_name(resource)
            grants = self.client.grants.get(securable_type=_get_enum_value(resource.securable_type), full_name=full_name)

            # Check if principal has this specific privilege
            for assignment in grants.privilege_assignments or []:
                if assignment.principal == resource.principal:
                    if _get_enum_value(resource.privilege) in (assignment.privileges or []):
                        return True

            return False

        except ResourceDoesNotExist:
            return False
        except (NotFound, PermissionDenied, BadRequest) as e:
            logger.warning(f"Error checking privilege existence for {resource.principal}: {e}")
            raise

    def create(self, resource: Privilege) -> ExecutionResult:
        """
        Grant a privilege.

        Args:
            resource: The privilege to grant

        Returns:
            ExecutionResult indicating success or failure
        """
        return self.grant_privilege(resource)

    def update(self, resource: Privilege) -> ExecutionResult:
        """
        Update is not applicable for privileges - use grant/revoke.

        Args:
            resource: The privilege

        Returns:
            ExecutionResult with NO_OP
        """
        return ExecutionResult(
            success=True,
            operation=OperationType.NO_OP,
            resource_type=self.get_resource_type(),
            resource_name=self._get_privilege_description(resource),
            message="Privileges are granted or revoked, not updated",
        )

    def delete(self, resource: Privilege) -> ExecutionResult:
        """
        Revoke a privilege.

        Args:
            resource: The privilege to revoke

        Returns:
            ExecutionResult indicating success or failure
        """
        return self.revoke_privilege(resource)

    def grant_privilege(self, privilege: Privilege) -> ExecutionResult:
        """
        Grant a single privilege.

        Args:
            privilege: The privilege to grant

        Returns:
            ExecutionResult indicating success or failure
        """
        start_time = time.time()
        description = self._get_privilege_description(privilege)

        try:
            # Validate principal exists if validation is enabled
            if self.validate_principals:
                if not self._validate_principal_exists(privilege.principal):
                    if self.strict_mode:
                        raise PrincipalNotFoundError(privilege.principal)
                    return ExecutionResult(
                        success=False,
                        operation=OperationType.GRANT,
                        resource_type=self.get_resource_type(),
                        resource_name=description,
                        message=f"Principal '{privilege.principal}' does not exist in Databricks",
                        duration_seconds=time.time() - start_time,
                    )

            # Check if already granted
            if self.exists(privilege):
                return ExecutionResult(
                    success=True,
                    operation=OperationType.NO_OP,
                    resource_type=self.get_resource_type(),
                    resource_name=description,
                    message="Already granted",
                )

            if self.dry_run:
                logger.info(f"[DRY RUN] Would grant {description}")
                return ExecutionResult(
                    success=True,
                    operation=OperationType.GRANT,
                    resource_type=self.get_resource_type(),
                    resource_name=description,
                    message="Would be granted (dry run)",
                )

            full_name = self._get_full_name(privilege)
            logger.info(f"Granting {description}")

            # Create the change request
            # SDK expects Privilege enum objects, not strings
            from databricks.sdk.service.catalog import Privilege as SDKPrivilege

            sdk_privilege = SDKPrivilege(_get_enum_value(privilege.privilege))
            changes = [PermissionsChange(principal=privilege.principal, add=[sdk_privilege])]

            self.execute_with_retry(
                self.client.grants.update,
                securable_type=_get_enum_value(privilege.securable_type),
                full_name=full_name,
                changes=changes,
            )

            # Add rollback operation
            self._rollback_stack.append(lambda: self.revoke_privilege(privilege))

            duration = time.time() - start_time
            return ExecutionResult(
                success=True,
                operation=OperationType.GRANT,
                resource_type=self.get_resource_type(),
                resource_name=description,
                message="Granted successfully",
                duration_seconds=duration,
            )

        except (PrincipalNotFoundError, SecurableNotFoundError):
            # Always propagate these custom exceptions
            raise
        except Exception as e:
            # Check if the resource doesn't exist
            error_msg = str(e).lower()
            is_not_found = "does not exist" in error_msg or "not found" in error_msg or "could not find" in error_msg
            if is_not_found:
                full_name = self._get_full_name(privilege)
                # Determine if it's a principal or securable not found
                if "principal" in error_msg:
                    if self.strict_mode:
                        raise PrincipalNotFoundError(privilege.principal) from e
                else:
                    if self.strict_mode:
                        raise SecurableNotFoundError(
                            _get_enum_value(privilege.securable_type), full_name
                        ) from e
                duration = time.time() - start_time
                return ExecutionResult(
                    success=False,
                    operation=OperationType.GRANT,
                    resource_type="PRIVILEGE",
                    resource_name=description,
                    message=f"Resource not found: {str(e)}",
                    duration_seconds=duration,
                )
            return self._handle_error(OperationType.GRANT, description, e)

    def revoke_privilege(self, privilege: Privilege) -> ExecutionResult:
        """
        Revoke a single privilege.

        Args:
            privilege: The privilege to revoke

        Returns:
            ExecutionResult indicating success or failure
        """
        start_time = time.time()
        description = self._get_privilege_description(privilege)

        try:
            # Check if not granted
            if not self.exists(privilege):
                return ExecutionResult(
                    success=True,
                    operation=OperationType.NO_OP,
                    resource_type=self.get_resource_type(),
                    resource_name=description,
                    message="Not granted",
                )

            if self.dry_run:
                logger.info(f"[DRY RUN] Would revoke {description}")
                return ExecutionResult(
                    success=True,
                    operation=OperationType.REVOKE,
                    resource_type=self.get_resource_type(),
                    resource_name=description,
                    message="Would be revoked (dry run)",
                )

            full_name = self._get_full_name(privilege)
            logger.info(f"Revoking {description}")

            # Create the change request
            # SDK expects Privilege enum objects, not strings
            from databricks.sdk.service.catalog import Privilege as SDKPrivilege

            sdk_privilege = SDKPrivilege(_get_enum_value(privilege.privilege))
            changes = [PermissionsChange(principal=privilege.principal, remove=[sdk_privilege])]

            self.execute_with_retry(
                self.client.grants.update,
                securable_type=_get_enum_value(privilege.securable_type),
                full_name=full_name,
                changes=changes,
            )

            duration = time.time() - start_time
            return ExecutionResult(
                success=True,
                operation=OperationType.REVOKE,
                resource_type=self.get_resource_type(),
                resource_name=description,
                message="Revoked successfully",
                duration_seconds=duration,
            )

        except Exception as e:
            return self._handle_error(OperationType.REVOKE, description, e)

    def apply_privileges(self, privileges: List[Privilege]) -> List[ExecutionResult]:
        """
        Apply multiple privileges efficiently.

        Groups privileges by securable for efficient batching.

        Args:
            privileges: List of privileges to apply

        Returns:
            List of ExecutionResults
        """
        results = []

        # Group privileges by securable for efficient batching
        by_securable = self._group_privileges_by_securable(privileges)

        for securable_key, privs in by_securable.items():
            result = self._apply_securable_privileges(securable_key, privs)
            results.append(result)

        return results

    def _group_privileges_by_securable(self, privileges: List[Privilege]) -> Dict[Tuple, List[Privilege]]:
        """
        Group privileges by securable for batching.

        Args:
            privileges: List of privileges

        Returns:
            Dictionary mapping securable key to list of privileges
        """
        by_securable = defaultdict(list)

        for priv in privileges:
            key = (priv.securable_type, priv.level_1, priv.level_2, priv.level_3)
            by_securable[key].append(priv)

        return by_securable

    def _apply_securable_privileges(self, securable_key: Tuple, privileges: List[Privilege]) -> ExecutionResult:
        """
        Apply all privileges for a single securable.

        Args:
            securable_key: Tuple identifying the securable
            privileges: List of privileges for this securable

        Returns:
            ExecutionResult for the batch operation

        Raises:
            PrincipalNotFoundError: If strict_mode=True and a principal doesn't exist
            SecurableNotFoundError: If strict_mode=True and the securable doesn't exist
        """
        securable_type, l1, l2, l3 = securable_key
        full_name = self._build_full_name(l1, l2, l3)

        try:
            # Validate principals if enabled
            if self.validate_principals:
                for priv in privileges:
                    if not self._validate_principal_exists(priv.principal):
                        if self.strict_mode:
                            raise PrincipalNotFoundError(priv.principal)
                        return ExecutionResult(
                            success=False,
                            operation=OperationType.GRANT,
                            resource_type="PRIVILEGE_BATCH",
                            resource_name=full_name,
                            message=f"Principal '{priv.principal}' does not exist in Databricks",
                        )

            # Get current grants
            current_grants = self._get_current_grants(securable_type, full_name)

            # Calculate changes needed
            changes = self._calculate_privilege_changes(privileges, current_grants)

            if not changes:
                return ExecutionResult(
                    success=True,
                    operation=OperationType.NO_OP,
                    resource_type="PRIVILEGE_BATCH",
                    resource_name=full_name,
                    message="No changes needed",
                )

            if self.dry_run:
                logger.info(f"[DRY RUN] Would update privileges on {full_name}")
                return ExecutionResult(
                    success=True,
                    operation=OperationType.GRANT,
                    resource_type="PRIVILEGE_BATCH",
                    resource_name=full_name,
                    message=f"Would apply {len(changes)} changes (dry run)",
                    changes={"changes": len(changes)},
                )

            # Apply changes
            logger.info(f"Applying {len(changes)} privilege changes to {full_name}")
            self.execute_with_retry(
                self.client.grants.update, securable_type=_get_enum_value(securable_type), full_name=full_name, changes=changes
            )

            return ExecutionResult(
                success=True,
                operation=OperationType.GRANT,
                resource_type="PRIVILEGE_BATCH",
                resource_name=full_name,
                message=f"Applied {len(changes)} privilege changes",
                changes={"changes": len(changes)},
            )

        except (PrincipalNotFoundError, SecurableNotFoundError):
            # Always propagate these custom exceptions
            raise
        except Exception as e:
            error_msg = str(e).lower()
            is_not_found = "does not exist" in error_msg or "not found" in error_msg or "could not find" in error_msg
            if is_not_found and self.strict_mode:
                if "principal" in error_msg:
                    # Try to extract principal name from error
                    raise PrincipalNotFoundError(f"unknown (from error: {str(e)})") from e
                else:
                    raise SecurableNotFoundError(_get_enum_value(securable_type), full_name) from e
            return ExecutionResult(
                success=False,
                operation=OperationType.GRANT,
                resource_type="PRIVILEGE_BATCH",
                resource_name=full_name,
                message=str(e),
                error=e,
            )

    def _get_current_grants(self, securable_type: SecurableType, full_name: str) -> Dict[str, Set[str]]:
        """
        Get current grants for a securable.

        Args:
            securable_type: Type of securable
            full_name: Full name of the securable

        Returns:
            Dictionary mapping principal to set of privileges

        Raises:
            SecurableNotFoundError: If strict_mode=True and the securable doesn't exist
        """
        current = defaultdict(set)

        try:
            grants = self.client.grants.get(securable_type=_get_enum_value(securable_type), full_name=full_name)

            for assignment in grants.privilege_assignments or []:
                principal = assignment.principal
                for priv in assignment.privileges or []:
                    current[principal].add(priv)

        except ResourceDoesNotExist:
            if self.strict_mode:
                raise SecurableNotFoundError(_get_enum_value(securable_type), full_name)
            # Securable doesn't exist yet - no current grants
            logger.debug(f"Securable {full_name} does not exist yet, returning empty grants")
        except (NotFound, PermissionDenied, BadRequest) as e:
            logger.error(f"Error getting current grants for {full_name}: {e}")
            raise

        return current

    def _calculate_privilege_changes(
        self, desired: List[Privilege], current: Dict[str, Set[str]]
    ) -> List[PermissionsChange]:
        """
        Calculate privilege changes needed.

        Args:
            desired: List of desired privileges
            current: Current privilege state

        Returns:
            List of PermissionsChange objects
        """
        from databricks.sdk.service.catalog import Privilege as SDKPrivilege

        # Group desired privileges by principal
        desired_by_principal = defaultdict(set)
        for priv in desired:
            desired_by_principal[priv.principal].add(_get_enum_value(priv.privilege))

        changes = []

        # Calculate additions for each principal
        for principal, desired_privs in desired_by_principal.items():
            current_privs = current.get(principal, set())
            to_add = desired_privs - current_privs

            if to_add:
                # Convert string privileges to SDK enum objects
                sdk_privileges = [SDKPrivilege(p) for p in to_add]
                changes.append(PermissionsChange(principal=principal, add=sdk_privileges))

        return changes

    def _get_full_name(self, privilege: Privilege) -> str:
        """
        Get the full name of the securable from a privilege.

        Args:
            privilege: The privilege

        Returns:
            Full name of the securable
        """
        return self._build_full_name(privilege.level_1, privilege.level_2, privilege.level_3)

    def _build_full_name(self, l1: str, l2: Optional[str] = None, l3: Optional[str] = None) -> str:
        """
        Build the full name of a securable.

        Args:
            l1: Level 1 name (catalog)
            l2: Level 2 name (schema)
            l3: Level 3 name (table/volume/function)

        Returns:
            Full name in dot notation
        """
        if l3:
            return f"{l1}.{l2}.{l3}"
        elif l2:
            return f"{l1}.{l2}"
        else:
            return l1

    def _get_privilege_description(self, privilege: Privilege) -> str:
        """
        Get a human-readable description of a privilege.

        Args:
            privilege: The privilege

        Returns:
            Description string
        """
        full_name = self._get_full_name(privilege)
        return f"{_get_enum_value(privilege.privilege)} on {_get_enum_value(privilege.securable_type)} {full_name} to {privilege.principal}"

    def _validate_principal_exists(self, principal_name: str) -> bool:
        """
        Validate that a principal exists in Databricks.

        Checks users, groups, and service principals in sequence.
        Returns False if principal is not found in any category.
        Raises on permission errors to avoid masking access issues.

        Args:
            principal_name: The name of the principal to validate

        Returns:
            True if principal exists, False otherwise

        Raises:
            PermissionDenied: If caller lacks permission to list principals
        """
        # Check cache first
        if principal_name in self._principal_cache:
            return self._principal_cache[principal_name]

        # Try as a user
        try:
            users = self.client.users.list(filter=f"userName eq '{principal_name}'")
            if any(u.user_name == principal_name for u in users):
                self._principal_cache[principal_name] = True
                return True
        except (NotFound, ResourceDoesNotExist):
            # User not found - continue checking other principal types
            pass
        except PermissionDenied:
            # Caller lacks permission - propagate error, don't mask it
            raise
        except BadRequest as e:
            # Malformed request - log and continue
            logger.debug(f"User lookup failed for '{principal_name}': {e}")

        # Try as a group
        try:
            groups = self.client.groups.list(filter=f"displayName eq '{principal_name}'")
            if any(g.display_name == principal_name for g in groups):
                self._principal_cache[principal_name] = True
                return True
        except (NotFound, ResourceDoesNotExist):
            pass
        except PermissionDenied:
            raise
        except BadRequest as e:
            logger.debug(f"Group lookup failed for '{principal_name}': {e}")

        # Try as a service principal
        # List all service principals and check against both display_name and application_id
        try:
            for sp in self.client.service_principals.list():
                if sp.display_name == principal_name or sp.application_id == principal_name:
                    self._principal_cache[principal_name] = True
                    return True
        except (NotFound, ResourceDoesNotExist):
            pass
        except PermissionDenied:
            raise
        except BadRequest as e:
            logger.debug(f"Service principal lookup failed for '{principal_name}': {e}")

        # Principal not found in any category
        logger.debug(f"Principal '{principal_name}' not found as user, group, or service principal")
        self._principal_cache[principal_name] = False
        return False
