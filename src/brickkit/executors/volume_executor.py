"""
Volume executor for Unity Catalog operations.

Handles creation, update, and deletion of volumes (unstructured storage) via the Databricks SDK.
"""

import logging
import time
from typing import Any, Dict

from databricks.sdk.errors import NotFound, PermissionDenied, ResourceDoesNotExist
from databricks.sdk.service.catalog import VolumeInfo

from brickkit.models import Volume, VolumeType

from .base import BaseExecutor, ExecutionResult, OperationType
from .tag_executor import TagExecutor

logger = logging.getLogger(__name__)


class VolumeExecutor(BaseExecutor[Volume]):
    """Executor for volume operations."""

    def _get_tag_executor(self) -> TagExecutor:
        """Get or create the TagExecutor instance."""
        if not hasattr(self, "_tag_executor"):
            self._tag_executor = TagExecutor(self.client)
        return self._tag_executor

    def _apply_tags(self, resource: Volume) -> None:
        """Apply tags to a volume using the entity_tag_assignments API."""
        if not resource.tags:
            return

        if self.dry_run:
            logger.info(f"[DRY RUN] Would apply {len(resource.tags)} tags to volume {resource.fqdn}")
            return

        tag_executor = self._get_tag_executor()
        tag_executor.apply_tags(
            entity_name=resource.fqdn,
            entity_type="volume",
            tags=resource.tags,
            update_existing=True,
        )

    def _sync_tags(self, resource: Volume) -> Dict[str, Any]:
        """Sync tags on a volume to match the desired state."""
        if self.dry_run:
            logger.info(f"[DRY RUN] Would sync tags on volume {resource.fqdn}")
            return {}

        tag_executor = self._get_tag_executor()
        return tag_executor.sync_tags(
            entity_name=resource.fqdn,
            entity_type="volume",
            desired_tags=resource.tags,
        )

    def get_resource_type(self) -> str:
        """Get the resource type."""
        return "VOLUME"

    def exists(self, resource: Volume) -> bool:
        """Check if a volume exists."""
        try:
            self.client.volumes.read(resource.fqdn)
            return True
        except (ResourceDoesNotExist, NotFound):
            return False
        except PermissionDenied as e:
            logger.error(f"Permission denied checking volume existence: {e}")
            raise

    def create(self, resource: Volume) -> ExecutionResult:
        """Create a new resource."""
        start_time = time.time()
        resource_name = resource.fqdn

        try:
            if self.dry_run:
                logger.info(f"[DRY RUN] Would create volume {resource_name}")
                return ExecutionResult(
                    success=True,
                    operation=OperationType.CREATE,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message="Would be created (dry run)",
                )

            params = resource.to_sdk_create_params()
            logger.info(f"Creating volume {resource_name} (type: {resource.volume_type.value})")

            # Validate external volumes have required location
            if resource.volume_type == VolumeType.EXTERNAL:
                if not resource.storage_location and not resource.external_location:
                    raise ValueError(f"External volume {resource_name} requires storage_location or external_location")

            self.execute_with_retry(self.client.volumes.create, **params)

            self._rollback_stack.append(lambda: self.client.volumes.delete(resource_name))

            # Apply tags via entity_tag_assignments API
            self._apply_tags(resource)

            duration = time.time() - start_time
            return ExecutionResult(
                success=True,
                operation=OperationType.CREATE,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message=f"Created {resource.volume_type.value} volume successfully",
                duration_seconds=duration,
            )

        except Exception as e:
            return self._handle_error(OperationType.CREATE, resource_name, e)

    def update(self, resource: Volume) -> ExecutionResult:
        """Update an existing resource."""
        start_time = time.time()
        resource_name = resource.fqdn

        try:
            existing = self.client.volumes.read(resource_name)
            changes = self._get_volume_changes(existing, resource)

            # Check if tags need syncing
            tags_need_sync = False
            if resource.tags:
                tag_executor = self._get_tag_executor()
                current_tags = tag_executor.list_tags(resource_name, "volume")
                current_tag_dict = {t.key: t.value for t in current_tags}
                desired_tag_dict = {t.key: t.value for t in resource.tags}
                if current_tag_dict != desired_tag_dict:
                    tags_need_sync = True
                    changes["tags"] = {"from": current_tag_dict, "to": desired_tag_dict}
            elif not resource.tags:
                # Check if there are existing tags that need to be removed
                tag_executor = self._get_tag_executor()
                current_tags = tag_executor.list_tags(resource_name, "volume")
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
                logger.info(f"[DRY RUN] Would update volume {resource_name}")
                return ExecutionResult(
                    success=True,
                    operation=OperationType.UPDATE,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message=f"Would update: {changes} (dry run)",
                    changes=changes,
                )

            # Only update volume properties if there are non-tag changes
            if any(k != "tags" for k in changes.keys()):
                params = resource.to_sdk_update_params()
                logger.info(f"Updating volume {resource_name}: {changes}")
                self.execute_with_retry(self.client.volumes.update, **params)

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

    def delete(self, resource: Volume) -> ExecutionResult:
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
                logger.info(f"[DRY RUN] Would delete volume {resource_name}")
                return ExecutionResult(
                    success=True,
                    operation=OperationType.DELETE,
                    resource_type=self.get_resource_type(),
                    resource_name=resource_name,
                    message="Would be deleted (dry run)",
                )

            logger.info(f"Deleting volume {resource_name}")
            self.execute_with_retry(self.client.volumes.delete, resource_name)

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

    def _get_volume_changes(self, existing: VolumeInfo, desired: Volume) -> Dict[str, Any]:
        """Compare existing and desired volume to find changes."""
        changes = {}

        # Comment is typically updatable
        if hasattr(existing, "comment") and existing.comment != desired.comment:
            changes["comment"] = {"from": existing.comment, "to": desired.comment}

        # Owner can be changed
        if desired.owner:
            desired_owner = desired.owner.resolved_name
            if hasattr(existing, "owner") and existing.owner != desired_owner:
                changes["owner"] = {"from": existing.owner, "to": desired_owner}

        # Note: Volume type and storage location cannot be changed after creation

        return changes
