"""
WorkspaceImporter - Pull entire Databricks workspace as brickkit models.

This module provides the main entry point for importing workspace resources.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set

from databricks.sdk import WorkspaceClient

from .base import ImportOptions, ImportResult, ResourceImporter

if TYPE_CHECKING:
    from brickkit import Convention
    from brickkit.models.enums import Environment

logger = logging.getLogger(__name__)


@dataclass
class WorkspaceSnapshot:
    """
    Complete snapshot of a Databricks workspace.

    Contains all imported resources organized by type. Can be used to:
    - Inspect existing resources
    - Apply conventions to add governance
    - Validate against organizational standards
    - Re-apply to Databricks to fix drift

    Usage:
        importer = WorkspaceImporter(client)
        snapshot = importer.pull_all()

        # Apply convention
        snapshot.apply_convention(my_convention, Environment.DEV)

        # Validate
        errors = snapshot.validate(my_convention)

        # Get all resources
        for resource in snapshot.all_resources():
            print(resource)
    """

    workspace_url: str
    imported_at: datetime

    # Unity Catalog
    catalogs: List[Any] = field(default_factory=list)
    schemas: List[Any] = field(default_factory=list)  # Flat list when pulled separately
    tables: List[Any] = field(default_factory=list)
    volumes: List[Any] = field(default_factory=list)
    functions: List[Any] = field(default_factory=list)
    storage_credentials: List[Any] = field(default_factory=list)
    external_locations: List[Any] = field(default_factory=list)
    connections: List[Any] = field(default_factory=list)

    # Identity
    users: List[Any] = field(default_factory=list)
    groups: List[Any] = field(default_factory=list)
    service_principals: List[Any] = field(default_factory=list)

    # Compute
    clusters: List[Any] = field(default_factory=list)
    warehouses: List[Any] = field(default_factory=list)
    instance_pools: List[Any] = field(default_factory=list)

    # Workflows
    jobs: List[Any] = field(default_factory=list)
    pipelines: List[Any] = field(default_factory=list)

    # AI/ML
    experiments: List[Any] = field(default_factory=list)
    registered_models: List[Any] = field(default_factory=list)
    serving_endpoints: List[Any] = field(default_factory=list)
    vector_search_indexes: List[Any] = field(default_factory=list)
    vector_search_endpoints: List[Any] = field(default_factory=list)
    genie_spaces: List[Any] = field(default_factory=list)

    # Apps
    apps: List[Any] = field(default_factory=list)

    # Dashboards
    dashboards: List[Any] = field(default_factory=list)

    # Import metadata
    import_errors: List[str] = field(default_factory=list)
    import_duration_seconds: float = 0.0

    def all_resources(self) -> List[Any]:
        """Return all resources as a flat list."""
        resources: List[Any] = []
        for field_name in self._resource_fields():
            field_value = getattr(self, field_name)
            if isinstance(field_value, list):
                resources.extend(field_value)
        return resources

    def _resource_fields(self) -> List[str]:
        """Get names of all resource list fields."""
        return [
            "catalogs",
            "schemas",
            "tables",
            "volumes",
            "functions",
            "storage_credentials",
            "external_locations",
            "connections",
            "users",
            "groups",
            "service_principals",
            "clusters",
            "warehouses",
            "instance_pools",
            "jobs",
            "pipelines",
            "experiments",
            "registered_models",
            "serving_endpoints",
            "vector_search_indexes",
            "vector_search_endpoints",
            "genie_spaces",
            "apps",
            "dashboards",
        ]

    def resource_counts(self) -> Dict[str, int]:
        """Get count of each resource type."""
        return {field_name: len(getattr(self, field_name)) for field_name in self._resource_fields()}

    def apply_convention(self, convention: "Convention", environment: "Environment") -> None:
        """
        Apply a convention to all resources that support it.

        This adds default tags and other governance attributes defined
        in the convention. Does not overwrite existing values.

        Args:
            convention: The convention to apply
            environment: Current deployment environment
        """
        for resource in self.all_resources():
            if hasattr(resource, "securable_type"):
                convention.apply_to(resource, environment)

    def validate(self, convention: "Convention") -> List[str]:
        """
        Validate all resources against a convention.

        Returns:
            List of validation error messages
        """
        errors: List[str] = []
        for resource in self.all_resources():
            if hasattr(resource, "securable_type"):
                resource_errors = convention.validate_securable(resource)
                for error in resource_errors:
                    name = getattr(resource, "name", "unknown")
                    errors.append(f"{name}: {error}")
        return errors

    def summary(self) -> str:
        """Generate a summary of the snapshot."""
        lines = [
            f"Workspace Snapshot: {self.workspace_url}",
            f"Imported at: {self.imported_at}",
            f"Duration: {self.import_duration_seconds:.2f}s",
            "",
            "Resource counts:",
        ]
        for field_name, count in self.resource_counts().items():
            if count > 0:
                lines.append(f"  {field_name}: {count}")

        if self.import_errors:
            lines.append("")
            lines.append(f"Errors: {len(self.import_errors)}")

        return "\n".join(lines)


# Mapping from resource type name to snapshot field name
RESOURCE_TYPE_TO_FIELD: Dict[str, str] = {
    "catalogs": "catalogs",
    "schemas": "schemas",
    "tables": "tables",
    "volumes": "volumes",
    "functions": "functions",
    "storage_credentials": "storage_credentials",
    "external_locations": "external_locations",
    "connections": "connections",
    "users": "users",
    "groups": "groups",
    "service_principals": "service_principals",
    "clusters": "clusters",
    "warehouses": "warehouses",
    "instance_pools": "instance_pools",
    "jobs": "jobs",
    "pipelines": "pipelines",
    "experiments": "experiments",
    "registered_models": "registered_models",
    "serving_endpoints": "serving_endpoints",
    "vector_search_indexes": "vector_search_indexes",
    "vector_search_endpoints": "vector_search_endpoints",
    "genie_spaces": "genie_spaces",
    "apps": "apps",
    "dashboards": "dashboards",
}


class WorkspaceImporter:
    """
    Import entire Databricks workspace as brickkit models.

    This is the main entry point for pulling workspace resources. It coordinates
    multiple resource-specific importers and aggregates results into a
    WorkspaceSnapshot.

    Usage:
        from databricks.sdk import WorkspaceClient
        from brickkit_tools.importer import WorkspaceImporter

        client = WorkspaceClient()
        importer = WorkspaceImporter(client)

        # Pull everything
        snapshot = importer.pull_all()

        # Pull specific resource types only
        snapshot = importer.pull(include=["catalogs", "jobs"])

        # Pull single catalog with full hierarchy
        catalog = importer.pull_catalog("my_catalog", depth="full")

        # Apply conventions and re-apply
        snapshot.apply_convention(my_convention, Environment.DEV)

    The importer is designed to be:
    - Resilient: Continues on individual errors
    - Efficient: Pulls in parallel where possible
    - Extensible: New resource types can be added via register_importer()
    """

    def __init__(
        self,
        client: WorkspaceClient,
        options: Optional[ImportOptions] = None,
    ):
        self.client = client
        self.options = options or ImportOptions()
        self._importers: Dict[str, ResourceImporter[Any]] = {}
        self._register_default_importers()

    def _register_default_importers(self) -> None:
        """Register all built-in importers."""
        # Import here to avoid circular imports
        from .catalog_importer import (
            CatalogImporter,
            ConnectionImporter,
            ExternalLocationImporter,
            StorageCredentialImporter,
        )
        from .genie_importer import GenieSpaceImporter
        from .identity_importer import (
            GroupImporter,
            ServicePrincipalImporter,
            UserImporter,
        )
        from .job_importer import JobImporter, PipelineImporter

        # Unity Catalog
        self._register(CatalogImporter(self.client, self.options))
        self._register(StorageCredentialImporter(self.client, self.options))
        self._register(ExternalLocationImporter(self.client, self.options))
        self._register(ConnectionImporter(self.client, self.options))

        # Identity
        self._register(UserImporter(self.client, self.options))
        self._register(GroupImporter(self.client, self.options))
        self._register(ServicePrincipalImporter(self.client, self.options))

        # Workflows
        self._register(JobImporter(self.client, self.options))
        self._register(PipelineImporter(self.client, self.options))

        # AI/ML
        self._register(GenieSpaceImporter(self.client, self.options))

        # TODO: Add more importers as they're implemented
        # - ClusterImporter
        # - WarehouseImporter
        # - ExperimentImporter
        # - RegisteredModelImporter
        # - ServingEndpointImporter
        # - VectorSearchImporter
        # - GenieSpaceImporter
        # - AppImporter
        # - DashboardImporter

    def _register(self, importer: ResourceImporter[Any]) -> None:
        """Register an importer if available."""
        if importer.is_available():
            self._importers[importer.resource_type] = importer
            logger.debug(f"Registered importer: {importer.resource_type}")
        else:
            logger.debug(f"Importer not available: {importer.resource_type}")

    def register_importer(self, importer: ResourceImporter[Any]) -> None:
        """
        Register a custom importer.

        Use this to add support for custom resource types or override
        built-in importers.

        Args:
            importer: The importer to register
        """
        self._register(importer)

    def available_types(self) -> List[str]:
        """List available resource types that can be imported."""
        return list(self._importers.keys())

    def pull_all(
        self,
        include: Optional[List[str]] = None,
        exclude: Optional[List[str]] = None,
    ) -> WorkspaceSnapshot:
        """
        Pull all resources from the workspace.

        Args:
            include: Only pull these resource types (default: all available)
            exclude: Skip these resource types

        Returns:
            WorkspaceSnapshot containing all imported resources
        """
        import time

        start_time = time.time()

        snapshot = WorkspaceSnapshot(
            workspace_url=self.client.config.host or "unknown",
            imported_at=datetime.now(),
        )

        # Determine which types to pull
        types_to_pull = self._filter_types(include, exclude)

        logger.info(f"Pulling resource types: {types_to_pull}")

        # Pull each resource type
        for resource_type in types_to_pull:
            importer = self._importers.get(resource_type)
            if not importer:
                logger.warning(f"No importer for type: {resource_type}")
                continue

            logger.info(f"Pulling {resource_type}...")
            try:
                result = importer.pull_all()

                # Map result to snapshot field
                field_name = RESOURCE_TYPE_TO_FIELD.get(resource_type)
                if field_name and hasattr(snapshot, field_name):
                    setattr(snapshot, field_name, result.resources)

                # Collect errors
                snapshot.import_errors.extend(result.errors)

                logger.info(
                    f"  Pulled {result.count} {resource_type}"
                    + (f" ({len(result.errors)} errors)" if result.errors else "")
                )

            except Exception as e:
                error_msg = f"Failed to pull {resource_type}: {e}"
                logger.error(error_msg)
                snapshot.import_errors.append(error_msg)

                if not self.options.skip_on_error:
                    raise

        snapshot.import_duration_seconds = time.time() - start_time
        logger.info(f"Import complete in {snapshot.import_duration_seconds:.2f}s")

        return snapshot

    def pull(
        self,
        include: Optional[List[str]] = None,
        exclude: Optional[List[str]] = None,
    ) -> WorkspaceSnapshot:
        """Alias for pull_all()."""
        return self.pull_all(include=include, exclude=exclude)

    def pull_catalog(
        self,
        name: str,
        depth: str = "full",  # "catalog_only", "schemas", "full"
    ) -> Any:
        """
        Pull a specific catalog with optional descendants.

        Args:
            name: Catalog name
            depth: How deep to pull
                - "catalog_only": Just the catalog
                - "schemas": Catalog + schemas
                - "full": Catalog + schemas + tables/volumes/functions

        Returns:
            brickkit Catalog model with descendants
        """
        importer = self._importers.get("catalogs")
        if not importer:
            raise ValueError("Catalog importer not available")

        return importer.pull_one(name, depth=depth)

    def _filter_types(
        self,
        include: Optional[List[str]],
        exclude: Optional[List[str]],
    ) -> Set[str]:
        """Filter resource types based on include/exclude lists."""
        types = set(self._importers.keys())

        if include:
            types &= set(include)

        if exclude:
            types -= set(exclude)

        return types
