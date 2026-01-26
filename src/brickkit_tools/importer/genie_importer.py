"""
Genie Space importer.

Imports Genie Spaces from the workspace using the existing brickkit models.
"""

import logging
import time
from typing import Any, List, Optional

from databricks.sdk.errors import NotFound, PermissionDenied

from brickkit.models.genie import GenieSpace

from .base import ImportOptions, ImportResult, ResourceImporter

logger = logging.getLogger(__name__)


class GenieSpaceImporter(ResourceImporter[GenieSpace]):
    """
    Import Genie Spaces from the workspace.

    Uses the existing GenieSpace.from_sdk() method to convert SDK responses
    to brickkit models.
    """

    @property
    def resource_type(self) -> str:
        return "genie_spaces"

    def is_available(self) -> bool:
        """Check if Genie API is available."""
        try:
            self.client.genie.list_spaces()
            return True
        except (PermissionDenied, NotFound, AttributeError) as e:
            logger.debug(f"Genie API not available: {e}")
            return False

    def pull_all(self) -> ImportResult:
        """Pull all Genie Spaces from the workspace."""
        start_time = time.time()
        spaces: List[GenieSpace] = []
        errors: List[str] = []
        skipped = 0

        try:
            response = self.client.genie.list_spaces()
            sdk_spaces = response.spaces if response and response.spaces else []

            for sdk_space in sdk_spaces:
                title = sdk_space.title or ""

                if not self._should_include(title):
                    skipped += 1
                    continue

                try:
                    # Get full space details
                    full_space = self.client.genie.get_space(sdk_space.space_id)

                    # Convert using existing from_sdk method
                    space = GenieSpace.from_sdk(
                        full_space,
                        source_workspace=self.client.config.host,
                    )
                    spaces.append(space)

                except Exception as e:
                    error_msg = f"Failed to import Genie Space '{title}': {e}"
                    logger.warning(error_msg)
                    errors.append(error_msg)

                    if not self.options.skip_on_error:
                        raise

        except PermissionDenied as e:
            errors.append(f"Permission denied listing Genie Spaces: {e}")

        return ImportResult(
            resource_type=self.resource_type,
            count=len(spaces),
            resources=spaces,
            errors=errors,
            skipped=skipped,
            duration_seconds=time.time() - start_time,
        )

    def pull_one(self, identifier: str, **kwargs: Any) -> Optional[GenieSpace]:
        """
        Pull a single Genie Space by ID.

        Args:
            identifier: The space_id
        """
        try:
            sdk_space = self.client.genie.get_space(identifier)
            return GenieSpace.from_sdk(
                sdk_space,
                source_workspace=self.client.config.host,
            )
        except NotFound:
            logger.warning(f"Genie Space not found: {identifier}")
            return None
        except PermissionDenied as e:
            logger.error(f"Permission denied accessing Genie Space '{identifier}': {e}")
            raise

    def pull_by_title(self, title: str) -> Optional[GenieSpace]:
        """
        Pull a Genie Space by title.

        Args:
            title: The space title to search for
        """
        try:
            response = self.client.genie.list_spaces()
            sdk_spaces = response.spaces if response and response.spaces else []

            for sdk_space in sdk_spaces:
                if sdk_space.title == title:
                    return self.pull_one(sdk_space.space_id)

            return None
        except PermissionDenied as e:
            logger.error(f"Permission denied: {e}")
            raise
