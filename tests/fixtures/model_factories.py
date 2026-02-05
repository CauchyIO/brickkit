"""
Factory functions for creating test models.

These factories create BrickKit models with sensible defaults for testing.
All factories accept overrides for any field.
"""

from typing import Any, Dict, List, Literal, Optional

from brickkit.models import (
    Catalog,
    ManagedGroup,
    ManagedServicePrincipal,
    Principal,
    Privilege,
    Schema,
    Table,
    Tag,
    Volume,
)
from brickkit.models.enums import (
    Environment,
    PrincipalType,
    PrivilegeType,
    SecurableType,
    TableType,
    VolumeType,
)
from brickkit.models.vector_search import (
    VectorEndpointType,
    VectorIndexType,
    VectorSearchEndpoint,
    VectorSearchIndex,
)


def make_tag(key: str = "test_key", value: str = "test_value") -> Tag:
    """Create a Tag for testing."""
    return Tag(key=key, value=value)


def make_principal(
    name: str = "test_principal",
    principal_type: Optional[PrincipalType] = None,
    application_id: Optional[str] = None,
    add_environment_suffix: bool = True,
    environment_mapping: Optional[Dict[Environment, str]] = None,
) -> Principal:
    """
    Create a Principal for testing.

    Args:
        name: Base principal name
        principal_type: Type of principal (USER, GROUP, SERVICE_PRINCIPAL)
        application_id: Application ID for service principals
        add_environment_suffix: Whether to add env suffix to name
        environment_mapping: Custom per-environment names

    Returns:
        Principal instance
    """
    return Principal(
        name=name,
        principal_type=principal_type,
        application_id=application_id,
        add_environment_suffix=add_environment_suffix,
        environment_mapping=environment_mapping or {},
    )


def make_service_principal(
    name: str = "spn_test",
    display_name: Optional[str] = None,
    application_id: Optional[str] = None,
    add_environment_suffix: bool = True,
    entitlements: Optional[List[str]] = None,
    active: bool = True,
) -> ManagedServicePrincipal:
    """
    Create a ManagedServicePrincipal for testing.

    Args:
        name: Base SPN name
        display_name: Human-readable display name
        application_id: Application/Client ID
        add_environment_suffix: Whether to add env suffix
        entitlements: List of entitlements
        active: Whether SPN is active

    Returns:
        ManagedServicePrincipal instance
    """
    return ManagedServicePrincipal(
        name=name,
        display_name=display_name,
        application_id=application_id,
        add_environment_suffix=add_environment_suffix,
        entitlements=entitlements or [],
        active=active,
    )


def make_group(
    name: str = "grp_test",
    display_name: Optional[str] = None,
    add_environment_suffix: bool = True,
    entitlements: Optional[List[str]] = None,
) -> ManagedGroup:
    """
    Create a ManagedGroup for testing.

    Args:
        name: Base group name
        display_name: Human-readable display name
        add_environment_suffix: Whether to add env suffix
        entitlements: List of entitlements

    Returns:
        ManagedGroup instance
    """
    return ManagedGroup(
        name=name,
        display_name=display_name,
        add_environment_suffix=add_environment_suffix,
        entitlements=entitlements or [],
    )


def make_catalog(
    name: str = "test_catalog",
    comment: Optional[str] = "Test catalog for BrickKit",
    tags: Optional[List[Tag]] = None,
    **kwargs: Any,
) -> Catalog:
    """
    Create a Catalog for testing.

    Args:
        name: Catalog base name (without env suffix)
        comment: Catalog description
        tags: List of tags to apply
        **kwargs: Additional fields to override

    Returns:
        Catalog instance
    """
    return Catalog(
        name=name,
        comment=comment,
        tags=tags or [],
        **kwargs,
    )


def make_schema(
    name: str = "test_schema",
    catalog_name: Optional[str] = "test_catalog",
    comment: Optional[str] = "Test schema for BrickKit",
    **kwargs: Any,
) -> Schema:
    """
    Create a Schema for testing.

    Args:
        name: Schema name
        catalog_name: Parent catalog name
        comment: Schema description
        **kwargs: Additional fields to override

    Returns:
        Schema instance
    """
    return Schema(
        name=name,
        catalog_name=catalog_name,
        comment=comment,
        **kwargs,
    )


def make_table(
    name: str = "test_table",
    catalog_name: Optional[str] = "test_catalog",
    schema_name: Optional[str] = "test_schema",
    table_type: TableType = TableType.MANAGED,
    comment: Optional[str] = "Test table for BrickKit",
    **kwargs: Any,
) -> Table:
    """
    Create a Table for testing.

    Args:
        name: Table name
        catalog_name: Parent catalog name
        schema_name: Parent schema name
        table_type: Type of table (MANAGED, EXTERNAL, VIEW)
        comment: Table description
        **kwargs: Additional fields to override

    Returns:
        Table instance
    """
    return Table(
        name=name,
        catalog_name=catalog_name,
        schema_name=schema_name,
        table_type=table_type,
        comment=comment,
        **kwargs,
    )


def make_volume(
    name: str = "test_volume",
    catalog_name: Optional[str] = "test_catalog",
    schema_name: Optional[str] = "test_schema",
    volume_type: VolumeType = VolumeType.MANAGED,
    comment: Optional[str] = "Test volume for BrickKit",
    storage_location: Optional[str] = None,
    **kwargs: Any,
) -> Volume:
    """
    Create a Volume for testing.

    Args:
        name: Volume name
        catalog_name: Parent catalog name
        schema_name: Parent schema name
        volume_type: Type of volume (MANAGED, EXTERNAL)
        comment: Volume description
        storage_location: External storage location (for EXTERNAL volumes)
        **kwargs: Additional fields to override

    Returns:
        Volume instance
    """
    return Volume(
        name=name,
        catalog_name=catalog_name,
        schema_name=schema_name,
        volume_type=volume_type,
        comment=comment,
        storage_location=storage_location,
        **kwargs,
    )


def make_privilege(
    level_1: str = "test_catalog_dev",
    level_2: Optional[str] = None,
    level_3: Optional[str] = None,
    securable_type: SecurableType = SecurableType.CATALOG,
    principal: str = "test_principal_dev",
    privilege: PrivilegeType = PrivilegeType.USE_CATALOG,
) -> Privilege:
    """
    Create a Privilege for testing.

    Args:
        level_1: First level name (catalog/credential/etc)
        level_2: Second level name (schema)
        level_3: Third level name (table/volume/etc)
        securable_type: Type of securable
        principal: Resolved principal name
        privilege: The privilege type

    Returns:
        Privilege instance
    """
    return Privilege(
        level_1=level_1,
        level_2=level_2,
        level_3=level_3,
        securable_type=securable_type,
        principal=principal,
        privilege_type=privilege,
    )


# =============================================================================
# VECTOR SEARCH FACTORIES
# =============================================================================


def make_vector_search_endpoint(
    name: str = "test_endpoint",
    endpoint_type: VectorEndpointType = VectorEndpointType.STANDARD,
    comment: Optional[str] = "Test endpoint for BrickKit",
    tags: Optional[List[Tag]] = None,
    **kwargs: Any,
) -> VectorSearchEndpoint:
    """
    Create a VectorSearchEndpoint for testing.

    Args:
        name: Endpoint name (base, without env suffix)
        endpoint_type: Type of endpoint (STANDARD)
        comment: Endpoint description
        tags: Governance tags
        **kwargs: Additional fields to override

    Returns:
        VectorSearchEndpoint instance
    """
    return VectorSearchEndpoint(
        name=name,
        endpoint_type=endpoint_type,
        comment=comment,
        tags=tags or [],
        **kwargs,
    )


def make_vector_search_index(
    name: str = "test_index",
    endpoint_name: str = "test_endpoint",
    source_table: str = "test_catalog_dev.test_schema.test_table",
    primary_key: str = "id",
    embedding_column: str = "embedding_text",
    embedding_model: Optional[str] = "databricks-bge-large-en",
    index_type: VectorIndexType = VectorIndexType.DELTA_SYNC,
    pipeline_type: Literal["TRIGGERED", "CONTINUOUS"] = "TRIGGERED",
    sync_columns: Optional[List[str]] = None,
    tags: Optional[List[Tag]] = None,
    **kwargs: Any,
) -> VectorSearchIndex:
    """
    Create a VectorSearchIndex for testing.

    Args:
        name: Index name (base, without env suffix)
        endpoint_name: Parent endpoint name (base, without env suffix)
        source_table: Full table name (catalog.schema.table) - must be 3-part
        primary_key: Primary key column
        embedding_column: Column containing embeddings
        embedding_model: Model endpoint for computing embeddings
        index_type: Type of index (DELTA_SYNC or DIRECT_ACCESS)
        pipeline_type: TRIGGERED or CONTINUOUS
        sync_columns: Additional columns to sync
        tags: Governance tags
        **kwargs: Additional fields to override

    Returns:
        VectorSearchIndex instance
    """
    return VectorSearchIndex(
        name=name,
        endpoint_name=endpoint_name,
        source_table=source_table,
        primary_key=primary_key,
        embedding_column=embedding_column,
        embedding_model=embedding_model,
        index_type=index_type,
        pipeline_type=pipeline_type,
        sync_columns=sync_columns or [],
        tags=tags or [],
        **kwargs,
    )
