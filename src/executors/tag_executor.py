"""
Tag executor for managing Unity Catalog entity tags.

This module handles the application and removal of tags on Unity Catalog
objects using the Databricks SDK's entity_tag_assignments API.
"""

import logging
from typing import List, Optional, Dict, Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import EntityTagAssignment
from databricks.sdk.errors import (
    ResourceDoesNotExist,
    ResourceAlreadyExists,
    NotFound,
    PermissionDenied,
    BadRequest,
)

from ..models.base import Tag

logger = logging.getLogger(__name__)


class TagExecutor:
    """
    Executor for managing Unity Catalog tags via entity_tag_assignments API.

    This executor handles:
    - Applying tags to catalogs, schemas, tables, and volumes
    - Removing tags from entities
    - Updating existing tags
    - Bulk tag operations
    """

    def __init__(self, client: WorkspaceClient):
        """
        Initialize the tag executor.

        Args:
            client: Databricks WorkspaceClient instance
        """
        self.client = client
        self.tag_api = client.entity_tag_assignments

    def apply_tags(
        self,
        entity_name: str,
        entity_type: str,
        tags: List[Tag],
        update_existing: bool = True
    ) -> List[EntityTagAssignment]:
        """
        Apply tags to a Unity Catalog entity.

        Args:
            entity_name: Full name of the entity (e.g., 'catalog.schema.table')
            entity_type: Type of entity ('catalog', 'schema', 'table', 'volume')
            tags: List of Tag objects to apply
            update_existing: If True, update existing tags; if False, skip existing

        Returns:
            List of created/updated EntityTagAssignment objects
        """
        if not tags:
            logger.debug(f"No tags to apply to {entity_type} {entity_name}")
            return []

        applied = []
        for tag in tags:
            try:
                assignment = tag.to_entity_assignment(entity_name, entity_type)

                if update_existing:
                    # Try to update first, create if doesn't exist
                    try:
                        self.tag_api.update(
                            entity_type=entity_type,
                            entity_name=entity_name,
                            tag_key=tag.key,
                            tag_value=tag.value
                        )
                        logger.debug(f"Updated tag {tag.key}={tag.value} on {entity_type} {entity_name}")
                    except (NotFound, ResourceDoesNotExist):
                        # Tag doesn't exist, create it
                        self.tag_api.create(assignment)
                        logger.debug(f"Created tag {tag.key}={tag.value} on {entity_type} {entity_name}")
                else:
                    # Just create, will fail if exists
                    self.tag_api.create(assignment)
                    logger.debug(f"Created tag {tag.key}={tag.value} on {entity_type} {entity_name}")

                applied.append(assignment)

            except ResourceAlreadyExists:
                logger.debug(f"Tag {tag.key} already exists on {entity_type} {entity_name}")
                applied.append(assignment)
            except PermissionDenied as e:
                logger.error(f"Permission denied applying tag {tag.key} to {entity_type} {entity_name}: {e}")
                raise
            except BadRequest as e:
                logger.warning(f"Invalid tag request for {tag.key} on {entity_type} {entity_name}: {e}")

        logger.info(f"Applied {len(applied)} tags to {entity_type} {entity_name}")
        return applied

    def remove_tags(
        self,
        entity_name: str,
        entity_type: str,
        tag_keys: Optional[List[str]] = None
    ) -> int:
        """
        Remove tags from a Unity Catalog entity.

        Args:
            entity_name: Full name of the entity
            entity_type: Type of entity
            tag_keys: Specific tag keys to remove; if None, remove all tags

        Returns:
            Number of tags removed
        """
        removed = 0

        if tag_keys is None:
            # Get all existing tags
            existing = self.list_tags(entity_name, entity_type)
            tag_keys = [tag.key for tag in existing]

        for key in tag_keys:
            try:
                self.tag_api.delete(
                    entity_type=entity_type,
                    entity_name=entity_name,
                    tag_key=key
                )
                removed += 1
                logger.debug(f"Removed tag {key} from {entity_type} {entity_name}")
            except (NotFound, ResourceDoesNotExist):
                # Tag already doesn't exist
                logger.debug(f"Tag {key} not found on {entity_type} {entity_name}")
            except PermissionDenied as e:
                logger.error(f"Permission denied removing tag {key}: {e}")
                raise
            except BadRequest as e:
                logger.warning(f"Invalid request removing tag {key}: {e}")

        logger.info(f"Removed {removed} tags from {entity_type} {entity_name}")
        return removed

    def list_tags(self, entity_name: str, entity_type: str) -> List[Tag]:
        """
        List all tags for an entity.

        Args:
            entity_name: Full name of the entity
            entity_type: Type of entity

        Returns:
            List of Tag objects
        """
        try:
            assignments = list(self.tag_api.list(
                entity_type=entity_type,
                entity_name=entity_name
            ))

            tags = [
                Tag.from_sdk_assignment(assignment)
                for assignment in assignments
            ]

            logger.debug(f"Found {len(tags)} tags on {entity_type} {entity_name}")
            return tags

        except (NotFound, ResourceDoesNotExist):
            # Entity has no tags or doesn't exist
            logger.debug(f"No tags found for {entity_type} {entity_name}")
            return []
        except PermissionDenied as e:
            logger.error(f"Permission denied listing tags for {entity_type} {entity_name}: {e}")
            raise

    def sync_tags(
        self,
        entity_name: str,
        entity_type: str,
        desired_tags: List[Tag]
    ) -> Dict[str, Any]:
        """
        Synchronize tags to match desired state.

        This method ensures the entity has exactly the specified tags:
        - Adds missing tags
        - Updates existing tags with different values
        - Removes tags not in the desired list

        Args:
            entity_name: Full name of the entity
            entity_type: Type of entity
            desired_tags: Desired list of tags

        Returns:
            Dictionary with sync results:
            - added: List of added tags
            - updated: List of updated tags
            - removed: List of removed tag keys
        """
        # Get current tags
        current_tags = self.list_tags(entity_name, entity_type)
        current_dict = {tag.key: tag.value for tag in current_tags}
        desired_dict = {tag.key: tag.value for tag in desired_tags}

        results = {
            "added": [],
            "updated": [],
            "removed": []
        }

        # Add or update tags
        for tag in desired_tags:
            if tag.key not in current_dict:
                # Add new tag
                try:
                    assignment = tag.to_entity_assignment(entity_name, entity_type)
                    self.tag_api.create(assignment)
                    results["added"].append(tag)
                    logger.debug(f"Added tag {tag.key}={tag.value}")
                except ResourceAlreadyExists:
                    # Tag was added concurrently
                    results["added"].append(tag)
                except PermissionDenied as e:
                    logger.error(f"Permission denied adding tag {tag.key}: {e}")
                    raise
                except BadRequest as e:
                    logger.warning(f"Invalid request adding tag {tag.key}: {e}")

            elif current_dict[tag.key] != tag.value:
                # Update existing tag
                try:
                    self.tag_api.update(
                        entity_type=entity_type,
                        entity_name=entity_name,
                        tag_key=tag.key,
                        tag_value=tag.value
                    )
                    results["updated"].append(tag)
                    logger.debug(f"Updated tag {tag.key} from {current_dict[tag.key]} to {tag.value}")
                except (NotFound, ResourceDoesNotExist):
                    # Tag was deleted concurrently, try to create instead
                    try:
                        assignment = tag.to_entity_assignment(entity_name, entity_type)
                        self.tag_api.create(assignment)
                        results["added"].append(tag)
                    except PermissionDenied:
                        raise
                except PermissionDenied as e:
                    logger.error(f"Permission denied updating tag {tag.key}: {e}")
                    raise
                except BadRequest as e:
                    logger.warning(f"Invalid request updating tag {tag.key}: {e}")

        # Remove unwanted tags
        for key in current_dict:
            if key not in desired_dict:
                try:
                    self.tag_api.delete(
                        entity_type=entity_type,
                        entity_name=entity_name,
                        tag_key=key
                    )
                    results["removed"].append(key)
                    logger.debug(f"Removed tag {key}")
                except (NotFound, ResourceDoesNotExist):
                    # Already removed
                    results["removed"].append(key)
                except PermissionDenied as e:
                    logger.error(f"Permission denied removing tag {key}: {e}")
                    raise
                except BadRequest as e:
                    logger.warning(f"Failed to remove tag {key}: {e}")

        logger.info(f"Tag sync for {entity_type} {entity_name}: +{len(results['added'])} ~{len(results['updated'])} -{len(results['removed'])}")
        return results

    def copy_tags(
        self,
        source_entity_name: str,
        source_entity_type: str,
        target_entity_name: str,
        target_entity_type: str
    ) -> List[Tag]:
        """
        Copy all tags from one entity to another.

        Args:
            source_entity_name: Source entity full name
            source_entity_type: Source entity type
            target_entity_name: Target entity full name
            target_entity_type: Target entity type

        Returns:
            List of tags copied to target
        """
        source_tags = self.list_tags(source_entity_name, source_entity_type)

        if source_tags:
            self.apply_tags(target_entity_name, target_entity_type, source_tags)
            logger.info(f"Copied {len(source_tags)} tags from {source_entity_name} to {target_entity_name}")

        return source_tags