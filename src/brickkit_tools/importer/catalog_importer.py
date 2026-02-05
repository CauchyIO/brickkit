"""
Unity Catalog resource importers.

Imports catalogs, schemas, tables, volumes, functions, storage credentials,
external locations, and connections.
"""

import logging
import time
from typing import Any, List, Optional

from databricks.sdk.errors import NotFound, PermissionDenied
from databricks.sdk.service.catalog import (
    CatalogInfo,
    ConnectionInfo,
    ExternalLocationInfo,
    FunctionInfo,
    SchemaInfo,
    StorageCredentialInfo,
    TableInfo,
    VolumeInfo,
)

from brickkit import (
    Catalog,
    Connection,
    ExternalLocation,
    Schema,
    StorageCredential,
    Tag,
)
from brickkit.models.enums import IsolationMode
from brickkit.models.grants import Principal
from brickkit.models.references import (
    FunctionReference,
    TableReference,
    VolumeReference,
)

from .base import ImportOptions, ImportResult, ResourceImporter

logger = logging.getLogger(__name__)


class CatalogImporter(ResourceImporter[Catalog]):
    """
    Import Unity Catalog catalogs and their descendants.

    Can pull:
    - Catalog metadata, tags, owner
    - Child schemas
    - Tables, volumes, functions within schemas
    """

    @property
    def resource_type(self) -> str:
        return "catalogs"

    def is_available(self) -> bool:
        """Check if Unity Catalog is available."""
        try:
            # Try to list catalogs - will fail if UC not enabled
            list(self.client.catalogs.list())
            return True
        except (PermissionDenied, NotFound) as e:
            logger.debug(f"Unity Catalog not available: {e}")
            return False

    def pull_all(self) -> ImportResult:
        """Pull all catalogs (without descendants for performance)."""
        start_time = time.time()
        catalogs: List[Catalog] = []
        errors: List[str] = []
        skipped = 0

        try:
            for info in self.client.catalogs.list():
                if not self._should_include(info.name):
                    skipped += 1
                    continue

                try:
                    catalog = self._from_info(info)
                    catalogs.append(catalog)
                except Exception as e:
                    error_msg = f"Failed to import catalog '{info.name}': {e}"
                    logger.warning(error_msg)
                    errors.append(error_msg)

                    if not self.options.skip_on_error:
                        raise

        except PermissionDenied as e:
            errors.append(f"Permission denied listing catalogs: {e}")

        return ImportResult(
            resource_type=self.resource_type,
            count=len(catalogs),
            resources=catalogs,
            errors=errors,
            skipped=skipped,
            duration_seconds=time.time() - start_time,
        )

    def pull_one(
        self,
        identifier: str,
        depth: str = "full",
        **kwargs: Any,
    ) -> Optional[Catalog]:
        """
        Pull a single catalog with optional descendants.

        Args:
            identifier: Catalog name
            depth: How deep to pull
                - "catalog_only": Just the catalog metadata
                - "schemas": Catalog + schemas
                - "full": Catalog + schemas + tables/volumes/functions
        """
        try:
            info = self.client.catalogs.get(identifier)
            catalog = self._from_info(info)

            if depth in ("schemas", "full"):
                self._pull_schemas(catalog, include_children=(depth == "full"))

            return catalog

        except NotFound:
            logger.warning(f"Catalog not found: {identifier}")
            return None
        except PermissionDenied as e:
            logger.error(f"Permission denied accessing catalog '{identifier}': {e}")
            raise

    def _from_info(self, info: CatalogInfo) -> Catalog:
        """Convert SDK CatalogInfo to brickkit Catalog."""
        # Pull tags (stored as properties in SDK)
        tags = []
        if info.properties:
            for key, value in info.properties.items():
                # Skip internal properties
                if not key.startswith("__"):
                    tags.append(Tag(key=key, value=str(value)))

        # Determine isolation mode
        isolation_mode = IsolationMode.OPEN
        if hasattr(info, "isolation_mode") and info.isolation_mode:
            try:
                isolation_mode = IsolationMode(info.isolation_mode.value)
            except (ValueError, AttributeError):
                pass

        # Create catalog - use base name without environment suffix
        # The SDK returns the full name, we need to strip any suffix
        base_name = self._strip_env_suffix(info.name)

        catalog = Catalog(
            name=base_name,
            comment=info.comment,
            owner=Principal(name=info.owner, add_environment_suffix=False) if info.owner else None,
            isolation_mode=isolation_mode,
            tags=tags,
        )

        # Store original full name for reference
        catalog._imported_full_name = info.name  # type: ignore

        return catalog

    def _pull_schemas(self, catalog: Catalog, include_children: bool) -> None:
        """Pull all schemas for a catalog."""
        try:
            # Use the original imported name if available
            catalog_name = getattr(catalog, "_imported_full_name", catalog.resolved_name)

            for schema_info in self.client.schemas.list(catalog_name=catalog_name):
                # Skip system schemas
                if schema_info.name in ("information_schema", "default"):
                    continue

                if not self._should_include(schema_info.name):
                    continue

                try:
                    schema = self._schema_from_info(schema_info)
                    catalog.add_schema(schema)

                    if include_children:
                        self._pull_tables(schema, catalog_name)
                        self._pull_volumes(schema, catalog_name)
                        self._pull_functions(schema, catalog_name)

                except Exception as e:
                    logger.warning(f"Failed to import schema '{schema_info.name}': {e}")
                    if not self.options.skip_on_error:
                        raise

        except PermissionDenied as e:
            logger.warning(f"Permission denied listing schemas in '{catalog.name}': {e}")

    def _schema_from_info(self, info: SchemaInfo) -> Schema:
        """Convert SDK SchemaInfo to brickkit Schema."""
        tags = []
        if info.properties:
            for key, value in info.properties.items():
                if not key.startswith("__"):
                    tags.append(Tag(key=key, value=str(value)))

        return Schema(
            name=info.name,
            comment=info.comment,
            owner=Principal(name=info.owner, add_environment_suffix=False) if info.owner else None,
            tags=tags,
        )

    def _pull_tables(self, schema: Schema, catalog_name: str) -> None:
        """Pull all tables in a schema as TableReferences."""
        try:
            for table_info in self.client.tables.list(
                catalog_name=catalog_name,
                schema_name=schema.name,
            ):
                try:
                    table_ref = self._table_ref_from_info(table_info)
                    schema.add_table_reference(table_ref)
                except Exception as e:
                    logger.warning(f"Failed to import table '{table_info.name}': {e}")
                    if not self.options.skip_on_error:
                        raise

        except PermissionDenied as e:
            logger.warning(f"Permission denied listing tables in '{schema.name}': {e}")

    def _table_ref_from_info(self, info: TableInfo) -> TableReference:
        """Convert SDK TableInfo to brickkit TableReference."""
        tags = {}
        if info.properties:
            for key, value in info.properties.items():
                if not key.startswith("__"):
                    tags[key] = str(value)

        # Strip environment suffix from catalog name
        base_catalog = self._strip_env_suffix(info.catalog_name)

        return TableReference(
            name=info.name,
            catalog_name=base_catalog,
            schema_name=info.schema_name,
            tags=tags,
            owner=info.owner,
        )

    def _pull_volumes(self, schema: Schema, catalog_name: str) -> None:
        """Pull all volumes in a schema as VolumeReferences."""
        try:
            for volume_info in self.client.volumes.list(
                catalog_name=catalog_name,
                schema_name=schema.name,
            ):
                try:
                    volume_ref = self._volume_ref_from_info(volume_info)
                    schema.add_volume_reference(volume_ref)
                except Exception as e:
                    logger.warning(f"Failed to import volume '{volume_info.name}': {e}")
                    if not self.options.skip_on_error:
                        raise

        except PermissionDenied as e:
            logger.warning(f"Permission denied listing volumes in '{schema.name}': {e}")

    def _volume_ref_from_info(self, info: VolumeInfo) -> VolumeReference:
        """Convert SDK VolumeInfo to brickkit VolumeReference."""
        base_catalog = self._strip_env_suffix(info.catalog_name)

        return VolumeReference(
            name=info.name,
            catalog_name=base_catalog,
            schema_name=info.schema_name,
            volume_type=info.volume_type.value if info.volume_type else "MANAGED",
        )

    def _pull_functions(self, schema: Schema, catalog_name: str) -> None:
        """Pull all functions in a schema as FunctionReferences."""
        try:
            for func_info in self.client.functions.list(
                catalog_name=catalog_name,
                schema_name=schema.name,
            ):
                try:
                    func_ref = self._function_ref_from_info(func_info)
                    schema.add_function_reference(func_ref)
                except Exception as e:
                    logger.warning(f"Failed to import function '{func_info.name}': {e}")
                    if not self.options.skip_on_error:
                        raise

        except PermissionDenied as e:
            logger.warning(f"Permission denied listing functions in '{schema.name}': {e}")

    def _function_ref_from_info(self, info: FunctionInfo) -> FunctionReference:
        """Convert SDK FunctionInfo to brickkit FunctionReference."""
        base_catalog = self._strip_env_suffix(info.catalog_name)

        return FunctionReference(
            name=info.name,
            catalog_name=base_catalog,
            schema_name=info.schema_name,
        )

    def _strip_env_suffix(self, name: str) -> str:
        """Strip environment suffix from a name if present."""
        for suffix in ("_dev", "_acc", "_prd", "_prod", "_test", "_staging"):
            if name.lower().endswith(suffix):
                return name[: -len(suffix)]
        return name


class StorageCredentialImporter(ResourceImporter[StorageCredential]):
    """Import storage credentials."""

    @property
    def resource_type(self) -> str:
        return "storage_credentials"

    def pull_all(self) -> ImportResult:
        start_time = time.time()
        credentials: List[StorageCredential] = []
        errors: List[str] = []

        try:
            for info in self.client.storage_credentials.list():
                if not self._should_include(info.name):
                    continue

                try:
                    cred = self._from_info(info)
                    credentials.append(cred)
                except Exception as e:
                    errors.append(f"Failed to import storage credential '{info.name}': {e}")
                    if not self.options.skip_on_error:
                        raise

        except PermissionDenied as e:
            errors.append(f"Permission denied listing storage credentials: {e}")

        return ImportResult(
            resource_type=self.resource_type,
            count=len(credentials),
            resources=credentials,
            errors=errors,
            duration_seconds=time.time() - start_time,
        )

    def pull_one(self, identifier: str, **kwargs: Any) -> Optional[StorageCredential]:
        try:
            info = self.client.storage_credentials.get(identifier)
            return self._from_info(info)
        except NotFound:
            return None

    def _from_info(self, info: StorageCredentialInfo) -> StorageCredential:
        """Convert SDK StorageCredentialInfo to brickkit StorageCredential."""
        # Note: We can't import the actual credential config (secrets)
        # Just import the metadata
        return StorageCredential(
            name=info.name,
            comment=info.comment,
            owner=Principal(name=info.owner, add_environment_suffix=False) if info.owner else None,
            read_only=info.read_only or False,
        )


class ExternalLocationImporter(ResourceImporter[ExternalLocation]):
    """Import external locations."""

    @property
    def resource_type(self) -> str:
        return "external_locations"

    def pull_all(self) -> ImportResult:
        start_time = time.time()
        locations: List[ExternalLocation] = []
        errors: List[str] = []

        try:
            for info in self.client.external_locations.list():
                if not self._should_include(info.name):
                    continue

                try:
                    loc = self._from_info(info)
                    locations.append(loc)
                except Exception as e:
                    errors.append(f"Failed to import external location '{info.name}': {e}")
                    if not self.options.skip_on_error:
                        raise

        except PermissionDenied as e:
            errors.append(f"Permission denied listing external locations: {e}")

        return ImportResult(
            resource_type=self.resource_type,
            count=len(locations),
            resources=locations,
            errors=errors,
            duration_seconds=time.time() - start_time,
        )

    def pull_one(self, identifier: str, **kwargs: Any) -> Optional[ExternalLocation]:
        try:
            info = self.client.external_locations.get(identifier)
            return self._from_info(info)
        except NotFound:
            return None

    def _from_info(self, info: ExternalLocationInfo) -> ExternalLocation:
        """Convert SDK ExternalLocationInfo to brickkit ExternalLocation."""
        return ExternalLocation(
            name=info.name,
            url=info.url,
            credential_name=info.credential_name,
            comment=info.comment,
            owner=Principal(name=info.owner, add_environment_suffix=False) if info.owner else None,
            read_only=info.read_only or False,
        )


class ConnectionImporter(ResourceImporter[Connection]):
    """Import connections."""

    @property
    def resource_type(self) -> str:
        return "connections"

    def pull_all(self) -> ImportResult:
        start_time = time.time()
        connections: List[Connection] = []
        errors: List[str] = []

        try:
            for info in self.client.connections.list():
                if not self._should_include(info.name):
                    continue

                try:
                    conn = self._from_info(info)
                    connections.append(conn)
                except Exception as e:
                    errors.append(f"Failed to import connection '{info.name}': {e}")
                    if not self.options.skip_on_error:
                        raise

        except PermissionDenied as e:
            errors.append(f"Permission denied listing connections: {e}")

        return ImportResult(
            resource_type=self.resource_type,
            count=len(connections),
            resources=connections,
            errors=errors,
            duration_seconds=time.time() - start_time,
        )

    def pull_one(self, identifier: str, **kwargs: Any) -> Optional[Connection]:
        try:
            info = self.client.connections.get(identifier)
            return self._from_info(info)
        except NotFound:
            return None

    def _from_info(self, info: ConnectionInfo) -> Connection:
        """Convert SDK ConnectionInfo to brickkit Connection."""
        from brickkit.models.enums import ConnectionType

        conn_type = ConnectionType.DATABRICKS  # Default
        if info.connection_type:
            try:
                conn_type = ConnectionType(info.connection_type.value)
            except ValueError:
                pass

        return Connection(
            name=info.name,
            connection_type=conn_type,
            comment=info.comment,
            owner=Principal(name=info.owner, add_environment_suffix=False) if info.owner else None,
            # Note: options/credentials not imported for security
        )
