"""
Base classes for workspace importers.

Provides the foundation for pulling Databricks resources and converting them
to brickkit models.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Generic, List, Optional, TypeVar

from databricks.sdk import WorkspaceClient

# Type variable for resource types
T = TypeVar("T")


@dataclass
class ImportResult:
    """Result of importing a resource type."""

    resource_type: str
    count: int
    resources: List[Any]
    errors: List[str] = field(default_factory=list)
    skipped: int = 0
    duration_seconds: float = 0.0

    @property
    def success(self) -> bool:
        """Check if import was successful (has resources, no errors)."""
        return len(self.errors) == 0

    @property
    def has_errors(self) -> bool:
        """Check if any errors occurred."""
        return len(self.errors) > 0

    def __repr__(self) -> str:
        status = "OK" if self.success else f"ERRORS({len(self.errors)})"
        return f"ImportResult({self.resource_type}: {self.count} resources, {status})"


@dataclass
class ImportOptions:
    """Options for controlling import behavior."""

    # Depth control
    include_descendants: bool = True  # Pull children (schemas, tables, etc.)
    max_depth: int = -1  # -1 = unlimited

    # Filtering
    name_pattern: Optional[str] = None  # Regex to filter by name
    exclude_patterns: List[str] = field(default_factory=list)

    # Performance
    parallel: bool = True
    batch_size: int = 100

    # Error handling
    skip_on_error: bool = True  # Continue on individual resource errors
    max_errors: int = 100  # Stop after this many errors

    # What to include
    include_tags: bool = True
    include_grants: bool = True
    include_properties: bool = True


class ResourceImporter(ABC, Generic[T]):
    """
    Base class for resource-specific importers.

    Each importer handles one type of Databricks resource (catalogs, jobs, etc.)
    and converts SDK responses to brickkit models.

    Subclasses must implement:
        - resource_type: Name of the resource type
        - pull_all(): Pull all resources of this type
        - pull_one(): Pull a single resource by identifier
    """

    def __init__(self, client: WorkspaceClient, options: Optional[ImportOptions] = None):
        self.client = client
        self.options = options or ImportOptions()
        self._errors: List[str] = []

    @property
    @abstractmethod
    def resource_type(self) -> str:
        """Return the resource type name (e.g., 'catalogs', 'jobs')."""
        pass

    @abstractmethod
    def pull_all(self) -> ImportResult:
        """
        Pull all resources of this type from the workspace.

        Returns:
            ImportResult containing all pulled resources
        """
        pass

    @abstractmethod
    def pull_one(self, identifier: str, **kwargs: Any) -> Optional[T]:
        """
        Pull a single resource by its identifier.

        Args:
            identifier: Resource name or ID
            **kwargs: Additional options (e.g., depth for catalogs)

        Returns:
            The brickkit model or None if not found
        """
        pass

    def is_available(self) -> bool:
        """
        Check if this resource type is available in the workspace.

        Override for resource types that may not be enabled (e.g., Unity Catalog).
        """
        return True

    def _record_error(self, message: str) -> None:
        """Record an error during import."""
        self._errors.append(message)
        if len(self._errors) >= self.options.max_errors:
            raise ImportError(f"Max errors ({self.options.max_errors}) exceeded")

    def _should_include(self, name: str) -> bool:
        """Check if a resource should be included based on filters."""
        import re

        # Check exclude patterns
        for pattern in self.options.exclude_patterns:
            if re.match(pattern, name):
                return False

        # Check include pattern
        if self.options.name_pattern:
            if not re.match(self.options.name_pattern, name):
                return False

        return True


class CompositeImporter(ResourceImporter[Any]):
    """
    An importer that combines multiple child importers.

    Useful for grouping related resource types (e.g., all UC resources).
    """

    def __init__(
        self,
        client: WorkspaceClient,
        importers: List[ResourceImporter[Any]],
        options: Optional[ImportOptions] = None,
    ):
        super().__init__(client, options)
        self._importers = importers

    @property
    def resource_type(self) -> str:
        return "composite"

    def pull_all(self) -> ImportResult:
        """Pull all resources from all child importers."""
        all_resources: List[Any] = []
        all_errors: List[str] = []
        total_skipped = 0

        for importer in self._importers:
            if not importer.is_available():
                continue

            result = importer.pull_all()
            all_resources.extend(result.resources)
            all_errors.extend(result.errors)
            total_skipped += result.skipped

        return ImportResult(
            resource_type=self.resource_type,
            count=len(all_resources),
            resources=all_resources,
            errors=all_errors,
            skipped=total_skipped,
        )

    def pull_one(self, identifier: str, **kwargs: Any) -> Optional[Any]:
        """Not supported for composite importers."""
        raise NotImplementedError("Use individual importers for pull_one()")


# Type aliases for common patterns
ResourceList = List[Any]
ErrorList = List[str]
TagDict = Dict[str, str]
