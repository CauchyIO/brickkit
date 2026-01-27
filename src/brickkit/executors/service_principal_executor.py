"""
Executor for managing Databricks Service Principals.

Handles creating, updating, and syncing service principals including
entitlements, with support for external (Entra-synced) SPNs.

Also provides OAuth secret generation and secure credential storage
for creating privileged WorkspaceClients.
"""

import logging
from dataclasses import dataclass
from typing import Any, Optional, Set

from databricks.sdk.errors import NotFound, ResourceDoesNotExist
from databricks.sdk.service.iam import (
    Patch,
    PatchOp,
    PatchSchema,
)
from databricks.sdk.service.iam import (
    ServicePrincipal as SdkServicePrincipal,
)

from brickkit.models.enums import PrincipalSource
from brickkit.models.principals import ManagedServicePrincipal

from .base import BaseExecutor, ExecutionResult, OperationType

logger = logging.getLogger(__name__)


@dataclass
class ServicePrincipalCredentials:
    """OAuth credentials for a service principal."""

    application_id: str
    client_secret: str
    display_name: str
    spn_id: str


class ServicePrincipalExecutor(BaseExecutor[ManagedServicePrincipal]):
    """
    Executor for managing Databricks service principals.

    Handles:
    - Create (DATABRICKS source only)
    - Update entitlements
    - Validate (EXTERNAL source - check exists)
    - Sync (full reconciliation)

    Example:
        ```python
        executor = ServicePrincipalExecutor(workspace_client)

        spn = ManagedServicePrincipal(name="spn_etl_pipeline")
        spn.add_entitlement("workspace-access")

        result = executor.create(spn)
        result = executor.sync_entitlements(spn)
        ```
    """

    def get_resource_type(self) -> str:
        """Get the resource type name."""
        return "ServicePrincipal"

    def exists(self, resource: ManagedServicePrincipal) -> bool:
        """Check if the service principal exists."""
        return self._get_by_name(resource.resolved_name) is not None

    def create(self, resource: ManagedServicePrincipal) -> ExecutionResult:
        """
        Create or validate a service principal.

        For DATABRICKS source: Creates the SPN if it doesn't exist
        For EXTERNAL source: Validates the SPN exists (synced via SCIM)
        """
        start_time = self._start_timer()
        resource_name = resource.resolved_name

        if self.dry_run:
            return ExecutionResult(
                success=True,
                operation=OperationType.NO_OP,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message=f"[DRY RUN] Would create service principal {resource_name}",
                duration_seconds=self._elapsed(start_time),
            )

        # For external SPNs, just validate existence
        if resource.source == PrincipalSource.EXTERNAL:
            return self._validate_external(resource, start_time)

        # Check if already exists
        existing = self._get_by_name(resource_name)
        if existing:
            resource._sdk_id = existing.id
            resource.application_id = existing.application_id
            return ExecutionResult(
                success=True,
                operation=OperationType.SKIPPED,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message=f"Service principal {resource_name} already exists",
                duration_seconds=self._elapsed(start_time),
                changes={"application_id": existing.application_id},
            )

        # Create via SDK
        sdk_sp = resource.to_sdk_service_principal()
        try:
            result = self.client.service_principals.create(
                display_name=sdk_sp.display_name,
                entitlements=sdk_sp.entitlements,
                active=sdk_sp.active,
            )
            resource._sdk_id = result.id
            resource.application_id = result.application_id

            return ExecutionResult(
                success=True,
                operation=OperationType.CREATE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message=f"Created service principal {resource_name} (app_id: {result.application_id})",
                duration_seconds=self._elapsed(start_time),
                changes={"application_id": result.application_id},
            )
        except Exception as e:
            return self._handle_error(OperationType.CREATE, resource_name, e)

    def update(self, resource: ManagedServicePrincipal) -> ExecutionResult:
        """Update a service principal (sync entitlements and active status)."""
        return self.sync_entitlements(resource)

    def delete(self, resource: ManagedServicePrincipal) -> ExecutionResult:
        """Delete a service principal."""
        start_time = self._start_timer()
        resource_name = resource.resolved_name

        if self.dry_run:
            return ExecutionResult(
                success=True,
                operation=OperationType.NO_OP,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message=f"[DRY RUN] Would delete service principal {resource_name}",
                duration_seconds=self._elapsed(start_time),
            )

        # Cannot delete external SPNs
        if resource.source == PrincipalSource.EXTERNAL:
            return ExecutionResult(
                success=False,
                operation=OperationType.DELETE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message=f"Cannot delete external service principal {resource_name} - managed by IdP",
                duration_seconds=self._elapsed(start_time),
            )

        existing = self._get_by_name(resource_name)
        if not existing:
            return ExecutionResult(
                success=True,
                operation=OperationType.SKIPPED,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message=f"Service principal {resource_name} does not exist",
                duration_seconds=self._elapsed(start_time),
            )

        try:
            if not existing.id:
                raise ValueError(f"Service principal {resource_name} has no ID")
            self.client.service_principals.delete(existing.id)
            return ExecutionResult(
                success=True,
                operation=OperationType.DELETE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message=f"Deleted service principal {resource_name}",
                duration_seconds=self._elapsed(start_time),
            )
        except Exception as e:
            return self._handle_error(OperationType.DELETE, resource_name, e)

    def sync_entitlements(self, resource: ManagedServicePrincipal) -> ExecutionResult:
        """
        Sync service principal entitlements to desired state.

        Adds missing entitlements and removes entitlements not in the desired state.
        """
        start_time = self._start_timer()
        resource_name = resource.resolved_name

        if self.dry_run:
            return ExecutionResult(
                success=True,
                operation=OperationType.NO_OP,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message=f"[DRY RUN] Would sync entitlements for {resource_name}",
                duration_seconds=self._elapsed(start_time),
            )

        existing = self._get_by_name(resource_name)
        if not existing:
            return ExecutionResult(
                success=False,
                operation=OperationType.UPDATE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message=f"Service principal {resource_name} does not exist",
                duration_seconds=self._elapsed(start_time),
            )

        # Calculate diff
        desired_entitlements: Set[str] = set(resource.entitlements)
        current_entitlements: Set[str] = {e.value for e in (existing.entitlements or []) if e.value}

        to_add = desired_entitlements - current_entitlements
        to_remove = current_entitlements - desired_entitlements

        # Also check active status
        active_changed = existing.active != resource.active

        if not to_add and not to_remove and not active_changed:
            return ExecutionResult(
                success=True,
                operation=OperationType.SKIPPED,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message="Entitlements and status already in sync",
                duration_seconds=self._elapsed(start_time),
            )

        if not existing.id:
            raise ValueError(f"Service principal {resource_name} has no ID")

        # Use PATCH to update
        try:
            operations = []

            if to_add:
                operations.append(Patch(op=PatchOp.ADD, path="entitlements", value=[{"value": e} for e in to_add]))
            if to_remove:
                for e in to_remove:
                    operations.append(Patch(op=PatchOp.REMOVE, path=f'entitlements[value eq "{e}"]'))
            if active_changed:
                operations.append(Patch(op=PatchOp.REPLACE, path="active", value=resource.active))

            self.client.service_principals.patch(
                id=existing.id,
                operations=operations,
                schemas=[PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
            )

            changes = {}
            if to_add:
                changes["entitlements_added"] = list(to_add)
            if to_remove:
                changes["entitlements_removed"] = list(to_remove)
            if active_changed:
                changes["active"] = resource.active

            return ExecutionResult(
                success=True,
                operation=OperationType.UPDATE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message=f"Updated service principal {resource_name}",
                duration_seconds=self._elapsed(start_time),
                changes=changes,
            )
        except Exception as e:
            return self._handle_error(OperationType.UPDATE, resource_name, e)

    def set_active(self, resource: ManagedServicePrincipal, active: bool) -> ExecutionResult:
        """
        Enable or disable a service principal.

        Args:
            resource: The service principal
            active: Whether to activate (True) or deactivate (False)
        """
        start_time = self._start_timer()
        resource_name = resource.resolved_name

        if self.dry_run:
            action = "activate" if active else "deactivate"
            return ExecutionResult(
                success=True,
                operation=OperationType.NO_OP,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message=f"[DRY RUN] Would {action} service principal {resource_name}",
                duration_seconds=self._elapsed(start_time),
            )

        existing = self._get_by_name(resource_name)
        if not existing:
            return ExecutionResult(
                success=False,
                operation=OperationType.UPDATE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message=f"Service principal {resource_name} does not exist",
                duration_seconds=self._elapsed(start_time),
            )

        if existing.active == active:
            state = "active" if active else "inactive"
            return ExecutionResult(
                success=True,
                operation=OperationType.SKIPPED,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message=f"Service principal {resource_name} is already {state}",
                duration_seconds=self._elapsed(start_time),
            )

        try:
            if not existing.id:
                raise ValueError(f"Service principal {resource_name} has no ID")
            self.client.service_principals.patch(
                id=existing.id,
                operations=[Patch(op=PatchOp.REPLACE, path="active", value=active)],
                schemas=[PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
            )

            action = "Activated" if active else "Deactivated"
            return ExecutionResult(
                success=True,
                operation=OperationType.UPDATE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message=f"{action} service principal {resource_name}",
                duration_seconds=self._elapsed(start_time),
                changes={"active": active},
            )
        except Exception as e:
            return self._handle_error(OperationType.UPDATE, resource_name, e)

    def sync(self, resource: ManagedServicePrincipal) -> ExecutionResult:
        """
        Full sync: create if needed, then sync entitlements.
        """
        # First ensure SPN exists
        create_result = self.create(resource)
        if not create_result.success and create_result.operation != OperationType.SKIPPED:
            return create_result

        # Sync entitlements
        entitlement_result = self.sync_entitlements(resource)

        if not entitlement_result.success:
            return entitlement_result

        return ExecutionResult(
            success=True,
            operation=OperationType.UPDATE,
            resource_type=self.get_resource_type(),
            resource_name=resource.resolved_name,
            message=f"Synced service principal {resource.resolved_name}",
            changes={"create": create_result.message, "entitlements": entitlement_result.message},
        )

    def create_with_secret(self, resource: ManagedServicePrincipal) -> tuple[ExecutionResult, Optional[ServicePrincipalCredentials]]:
        """
        Create a service principal and generate an OAuth secret.

        This is useful for bootstrap scenarios where you need a privileged SPN
        to perform operations that notebook tokens cannot.

        Args:
            resource: The service principal to create

        Returns:
            Tuple of (ExecutionResult, ServicePrincipalCredentials or None)
            Credentials are only returned on successful creation with secret.

        Example:
            ```python
            admin_spn = ManagedServicePrincipal(name="spn_brickkit_admin")
            admin_spn.add_entitlement("workspace-access")

            result, credentials = executor.create_with_secret(admin_spn)
            if credentials:
                # Store credentials securely
                executor.store_credentials(credentials, scope="brickkit")
            ```
        """
        resource_name = resource.resolved_name
        start_time = self._start_timer()

        if self.dry_run:
            return (
                ExecutionResult(
                    success=True,
                    operation=OperationType.NO_OP,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message=f"[DRY RUN] Would create service principal {resource_name} with OAuth secret",
                    duration_seconds=self._elapsed(start_time),
                ),
                None,
            )

        # Check if SPN already exists
        existing = self._get_by_name(resource_name)
        if existing:
            resource._sdk_id = existing.id
            resource.application_id = existing.application_id
            # Generate new secret for existing SPN
            credentials = self.generate_secret(resource)
            return (
                ExecutionResult(
                    success=True,
                    operation=OperationType.UPDATE,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message=f"Service principal {resource_name} exists (app_id: {existing.application_id}), generated new secret",
                    duration_seconds=self._elapsed(start_time),
                    changes={"application_id": existing.application_id},
                ),
                credentials,
            )

        # Create new SPN
        sdk_sp = resource.to_sdk_service_principal()
        try:
            result = self.client.service_principals.create(
                display_name=sdk_sp.display_name,
                entitlements=sdk_sp.entitlements,
                active=sdk_sp.active,
            )
            resource._sdk_id = result.id
            resource.application_id = result.application_id

            # Generate OAuth secret
            if not result.id:
                raise ValueError(f"Created SPN {resource_name} has no ID")

            secret_response = self.client.service_principal_secrets_proxy.create(service_principal_id=result.id)

            credentials = ServicePrincipalCredentials(
                application_id=result.application_id or "",
                client_secret=secret_response.secret or "",
                display_name=resource_name,
                spn_id=result.id,
            )

            return (
                ExecutionResult(
                    success=True,
                    operation=OperationType.CREATE,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message=f"Created service principal {resource_name} (app_id: {result.application_id}) with OAuth secret",
                    duration_seconds=self._elapsed(start_time),
                    changes={"application_id": result.application_id},
                ),
                credentials,
            )

        except Exception as e:
            return (self._handle_error(OperationType.CREATE, resource_name, e), None)

    def generate_secret(self, resource: ManagedServicePrincipal) -> Optional[ServicePrincipalCredentials]:
        """
        Generate a new OAuth secret for an existing service principal.

        Note: The secret is only returned once at creation time.
        Store it securely immediately.

        Args:
            resource: The service principal (must have _sdk_id set or exist by name)

        Returns:
            ServicePrincipalCredentials or None if SPN not found
        """
        resource_name = resource.resolved_name

        # Get SPN ID if not already set
        if not resource._sdk_id:
            existing = self._get_by_name(resource_name)
            if not existing or not existing.id:
                logger.error(f"Service principal {resource_name} not found")
                return None
            resource._sdk_id = existing.id
            application_id = existing.application_id or ""
        else:
            # Look up application_id
            existing = self._get_by_name(resource_name)
            application_id = existing.application_id if existing else ""

        try:
            secret_response = self.client.service_principal_secrets_proxy.create(service_principal_id=resource._sdk_id)

            return ServicePrincipalCredentials(
                application_id=application_id or "",
                client_secret=secret_response.secret or "",
                display_name=resource_name,
                spn_id=resource._sdk_id,
            )
        except Exception as e:
            logger.error(f"Failed to generate secret for {resource_name}: {e}")
            raise

    def store_credentials(
        self,
        credentials: ServicePrincipalCredentials,
        scope: str,
        client_id_key: str = "admin-spn-client-id",
        client_secret_key: str = "admin-spn-client-secret",
    ) -> None:
        """
        Store service principal credentials in Databricks Secrets.

        Creates the scope if it doesn't exist.

        Args:
            credentials: The credentials to store
            scope: Secret scope name (e.g., "brickkit")
            client_id_key: Key for the application/client ID
            client_secret_key: Key for the client secret

        Example:
            ```python
            executor.store_credentials(
                credentials,
                scope="brickkit",
                client_id_key="admin-spn-client-id",
                client_secret_key="admin-spn-client-secret",
            )
            ```
        """
        # Create scope if it doesn't exist
        try:
            self.client.secrets.create_scope(scope=scope)
            logger.info(f"Created secret scope: {scope}")
        except Exception as e:
            if "already exists" in str(e).lower():
                logger.debug(f"Secret scope {scope} already exists")
            else:
                raise

        # Store credentials
        self.client.secrets.put_secret(scope=scope, key=client_id_key, string_value=credentials.application_id)
        self.client.secrets.put_secret(scope=scope, key=client_secret_key, string_value=credentials.client_secret)

        logger.info(f"Stored credentials for {credentials.display_name} in scope '{scope}'")

    def _get_by_name(self, name: str) -> Optional[SdkServicePrincipal]:
        """Find service principal by display name."""
        try:
            sps = list(self.client.service_principals.list(filter=f'displayName eq "{name}"'))
            return sps[0] if sps else None
        except (NotFound, ResourceDoesNotExist):
            return None
        except Exception as e:
            logger.warning(f"Error looking up service principal {name}: {e}")
            return None

    def _validate_external(self, resource: ManagedServicePrincipal, start_time: float) -> ExecutionResult:
        """Validate external service principal exists."""
        resource_name = resource.resolved_name
        existing = self._get_by_name(resource_name)

        if existing:
            resource._sdk_id = existing.id
            if existing.external_id:
                return ExecutionResult(
                    success=True,
                    operation=OperationType.SKIPPED,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message=f"External service principal {resource_name} exists (external_id: {existing.external_id})",
                    duration_seconds=self._elapsed(start_time),
                )
            else:
                return ExecutionResult(
                    success=True,
                    operation=OperationType.SKIPPED,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message=f"Service principal {resource_name} exists (not externally synced)",
                    duration_seconds=self._elapsed(start_time),
                )

        return ExecutionResult(
            success=False,
            operation=OperationType.SKIPPED,
            resource_type=self.get_resource_type(),
            resource_name=resource_name,
            message=f"External service principal {resource_name} not found - check SCIM sync configuration",
            duration_seconds=self._elapsed(start_time),
        )

    def _start_timer(self) -> float:
        """Start a timer for duration tracking."""
        import time

        return time.time()

    def _elapsed(self, start_time: float) -> float:
        """Get elapsed time since start."""
        import time

        return time.time() - start_time


def get_privileged_client(
    host: str,
    *,
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None,
    scope: str = "brickkit",
    client_id_key: str = "admin-spn-client-id",
    client_secret_key: str = "admin-spn-client-secret",
    dbutils: Any = None,
):
    """
    Create a WorkspaceClient authenticated with SPN credentials.

    Credentials are resolved in order of priority:
    1. Direct parameters (client_id, client_secret)
    2. Environment variables (DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET)
    3. Databricks Secrets via dbutils (notebook context only)

    This is useful for operations that require elevated permissions beyond what
    notebook tokens provide (e.g., catalog updates, isolation mode changes).

    Args:
        host: Workspace hostname (e.g., "https://dbc-xxxxx.cloud.databricks.com")
        client_id: OAuth client/application ID (optional, takes priority)
        client_secret: OAuth client secret (optional, takes priority)
        scope: Secret scope containing credentials (for dbutils fallback)
        client_id_key: Key for the application/client ID in the scope
        client_secret_key: Key for the client secret in the scope
        dbutils: Databricks utilities object (for notebook context)

    Returns:
        WorkspaceClient authenticated with the SPN credentials

    Example:
        ```python
        # Option 1: Direct credentials (works anywhere)
        privileged_client = get_privileged_client(
            host="https://dbc-xxxxx.cloud.databricks.com",
            client_id="your-app-id",
            client_secret="your-secret",
        )

        # Option 2: Environment variables (works anywhere)
        # Set DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET
        privileged_client = get_privileged_client(host="https://...")

        # Option 3: Databricks notebook with dbutils
        privileged_client = get_privileged_client(host=WORKSPACE_HOSTNAME, dbutils=dbutils)
        ```
    """
    import os

    from databricks.sdk import WorkspaceClient

    resolved_client_id = client_id
    resolved_client_secret = client_secret

    # Priority 1: Direct parameters (already set above)

    # Priority 2: Environment variables
    if resolved_client_id is None:
        resolved_client_id = os.environ.get("DATABRICKS_CLIENT_ID")
    if resolved_client_secret is None:
        resolved_client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET")

    # Priority 3: dbutils secrets (notebook context)
    if (resolved_client_id is None or resolved_client_secret is None) and dbutils is not None:
        resolved_client_id = resolved_client_id or dbutils.secrets.get(scope=scope, key=client_id_key)
        resolved_client_secret = resolved_client_secret or dbutils.secrets.get(scope=scope, key=client_secret_key)

    # Validate we have credentials
    if resolved_client_id is None or resolved_client_secret is None:
        raise ValueError(
            "No credentials found. Provide client_id/client_secret directly, "
            "set DATABRICKS_CLIENT_ID/DATABRICKS_CLIENT_SECRET environment variables, "
            "or pass dbutils to read from Databricks Secrets."
        )

    # Ensure host has https://
    if not host.startswith("https://"):
        host = f"https://{host}"

    return WorkspaceClient(
        host=host,
        client_id=resolved_client_id,
        client_secret=resolved_client_secret,
    )
