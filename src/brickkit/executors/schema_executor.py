"""
Schema executor for Unity Catalog operations.

Handles creation, update, and deletion of schemas via the Databricks SDK.
"""

import logging
import time
from typing import Any, Dict

from databricks.sdk.errors import NotFound, PermissionDenied, ResourceDoesNotExist
from databricks.sdk.service.catalog import SchemaInfo

from brickkit.models import Schema
from brickkit.models.enums import PrincipalType
from brickkit.models.grants import Principal

from .base import BaseExecutor, ExecutionResult, OperationType
from .tag_executor import TagExecutor

logger = logging.getLogger(__name__)


class SchemaExecutor(BaseExecutor[Schema]):
    """Executor for schema operations."""

    def _get_tag_executor(self) -> TagExecutor:
        """Get or create the TagExecutor instance."""
        if not hasattr(self, "_tag_executor"):
            self._tag_executor = TagExecutor(self.client)
        return self._tag_executor

    def _apply_tags(self, resource: Schema) -> None:
        """Apply tags to a schema using the entity_tag_assignments API."""
        if not resource.tags:
            return

        if self.dry_run:
            logger.info(f"[DRY RUN] Would apply {len(resource.tags)} tags to schema {resource.fqdn}")
            return

        tag_executor = self._get_tag_executor()
        tag_executor.apply_tags(
            entity_name=resource.fqdn,
            entity_type="schemas",
            tags=resource.tags,
            update_existing=True,
        )

    def _sync_tags(self, resource: Schema) -> Dict[str, Any]:
        """Sync tags on a schema to match the desired state."""
        if self.dry_run:
            logger.info(f"[DRY RUN] Would sync tags on schema {resource.fqdn}")
            return {}

        tag_executor = self._get_tag_executor()
        return tag_executor.sync_tags(
            entity_name=resource.fqdn,
            entity_type="schemas",
            desired_tags=resource.tags,
        )

    def get_resource_type(self) -> str:
        """Get the resource type."""
        return "SCHEMA"

    def _resolve_owner_for_sdk(self, owner: Principal) -> str:
        """
        Resolve owner to the correct identifier for SDK calls.

        For service principals, Databricks expects the application_id (UUID).
        For users and groups, it expects the display_name/email.
        """
        if owner.principal_type == PrincipalType.SERVICE_PRINCIPAL:
            resolved_name = owner.resolved_name
            try:
                spns = list(self.client.service_principals.list(filter=f'displayName eq "{resolved_name}"'))
                if spns and spns[0].application_id:
                    logger.debug(f"Resolved SPN {resolved_name} to application_id {spns[0].application_id}")
                    return spns[0].application_id
                else:
                    logger.warning(f"Could not find application_id for SPN {resolved_name}, using display_name")
                    return resolved_name
            except Exception as e:
                logger.warning(f"Error looking up SPN {resolved_name}: {e}, using display_name")
                return resolved_name
        else:
            return owner.resolved_name

    def exists(self, resource: Schema) -> bool:
        """Check if a schema exists."""
        try:
            self.client.schemas.get(resource.fqdn)
            return True
        except (ResourceDoesNotExist, NotFound):
            return False
        except PermissionDenied as e:
            logger.error(f"Permission denied checking schema existence: {e}")
            raise

    def create(self, resource: Schema) -> ExecutionResult:
        """Create a new resource."""
        start_time = time.time()
        resource_name = resource.fqdn

        try:
            # Check if schema already exists first
            if self.exists(resource):
                logger.info(f"Schema {resource_name} already exists, checking for updates")
                return self.update(resource)

            if self.dry_run:
                logger.info(f"[DRY RUN] Would create schema {resource_name}")
                return ExecutionResult(
                    success=True,
                    operation=OperationType.CREATE,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message="Would be created (dry run)",
                )

            params = resource.to_sdk_create_params()
            logger.info(f"Creating schema {resource_name}")
            self.execute_with_retry(self.client.schemas.create, **params)

            self._rollback_stack.append(lambda: self.client.schemas.delete(resource_name, force=True))

            # Apply tags via entity_tag_assignments API
            self._apply_tags(resource)

            duration = time.time() - start_time
            return ExecutionResult(
                success=True,
                operation=OperationType.CREATE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message="Created successfully",
                duration_seconds=duration,
            )

        except Exception as e:
            # Handle "already exists" error gracefully
            error_msg = str(e)
            if "already exists" in error_msg.lower():
                logger.info(f"Schema {resource_name} already exists, checking for updates")
                return self.update(resource)
            return self._handle_error(OperationType.CREATE, resource_name, e)

    def update(self, resource: Schema) -> ExecutionResult:
        """Update an existing resource."""
        start_time = time.time()
        resource_name = resource.fqdn

        try:
            existing = self.client.schemas.get(resource_name)
            changes = self._get_schema_changes(existing, resource)

            # Check if tags need syncing
            tags_need_sync = False
            if resource.tags:
                tag_executor = self._get_tag_executor()
                current_tags = tag_executor.list_tags(resource_name, "schemas")
                current_tag_dict = {t.key: t.value for t in current_tags}
                desired_tag_dict = {t.key: t.value for t in resource.tags}
                if current_tag_dict != desired_tag_dict:
                    tags_need_sync = True
                    changes["tags"] = {"from": current_tag_dict, "to": desired_tag_dict}
            elif not resource.tags:
                # Check if there are existing tags that need to be removed
                tag_executor = self._get_tag_executor()
                current_tags = tag_executor.list_tags(resource_name, "schemas")
                if current_tags:
                    tags_need_sync = True
                    changes["tags"] = {"from": {t.key: t.value for t in current_tags}, "to": {}}

            if not changes and not tags_need_sync:
                return ExecutionResult(
                    success=True,
                    operation=OperationType.NO_OP,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message="No changes needed",
                )

            if self.dry_run:
                logger.info(f"[DRY RUN] Would update schema {resource_name}")
                return ExecutionResult(
                    success=True,
                    operation=OperationType.UPDATE,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message=f"Would update: {changes} (dry run)",
                    changes=changes,
                )

            # Only update schema properties if there are non-tag changes
            if any(k != "tags" for k in changes.keys()):
                params = resource.to_sdk_update_params()

                # Resolve owner to application_id for service principals
                if "owner" in params and resource.owner:
                    params["owner"] = self._resolve_owner_for_sdk(resource.owner)

                logger.info(f"Updating schema {resource_name}: {changes}")
                self.execute_with_retry(self.client.schemas.update, **params)

            # Sync tags via entity_tag_assignments API
            if tags_need_sync:
                self._sync_tags(resource)

            duration = time.time() - start_time
            return ExecutionResult(
                success=True,
                operation=OperationType.UPDATE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message=f"Updated: {changes}",
                duration_seconds=duration,
                changes=changes,
            )

        except Exception as e:
            return self._handle_error(OperationType.UPDATE, resource_name, e)

    def delete(self, resource: Schema) -> ExecutionResult:
        """Delete a resource."""
        start_time = time.time()
        resource_name = resource.fqdn

        try:
            if not self.exists(resource):
                return ExecutionResult(
                    success=True,
                    operation=OperationType.NO_OP,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message="Does not exist",
                )

            if self.dry_run:
                logger.info(f"[DRY RUN] Would delete schema {resource_name}")
                return ExecutionResult(
                    success=True,
                    operation=OperationType.DELETE,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message="Would be deleted (dry run)",
                )

            logger.info(f"Deleting schema {resource_name}")
            self.execute_with_retry(self.client.schemas.delete, resource_name, force=True)

            duration = time.time() - start_time
            return ExecutionResult(
                success=True,
                operation=OperationType.DELETE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message="Deleted successfully",
                duration_seconds=duration,
            )

        except Exception as e:
            return self._handle_error(OperationType.DELETE, resource_name, e)

    def _get_schema_changes(self, existing: SchemaInfo, desired: Schema) -> Dict[str, Any]:
        """Compare existing and desired schema to find changes."""
        changes = {}

        if hasattr(existing, "comment") and existing.comment != desired.comment:
            changes["comment"] = {"from": existing.comment, "to": desired.comment}

        if desired.owner:
            desired_owner = desired.owner.resolved_name
            if hasattr(existing, "owner") and existing.owner != desired_owner:
                changes["owner"] = {"from": existing.owner, "to": desired_owner}

        return changes
