"""
Unity Catalog governance models.

This package provides all Pydantic models for the Unity Catalog governance system.
The models are organized into logical modules mirroring the Databricks SDK pattern.

Module organization:
- enums: All enumerations (SecurableType, PrivilegeType, Environment, etc.)
- base: Base classes (BaseGovernanceModel, BaseSecurable, Tag)
- grants: Access control (Principal, Privilege, AccessPolicy)
- catalogs: Catalog securable
- schemas: Schema securable
- tables: Table and ColumnInfo
- volumes: Volume securable
- functions: Function securable
- storage_credentials: StorageCredential and cloud credential models
- external_locations: ExternalLocation securable
- connections: Connection securable
- metastores: Metastore container
- workspace_bindings: Workspace, WorkspaceBinding, WorkspaceBindingPattern
- teams: Team, AccessManager
- references: Lightweight reference models (TableReference, etc.)
- sharing: Delta Sharing models
- ml_models: ML-specific models
- genie: Genie Space models
- vector_search: Vector Search models
"""

# Import typing for model_rebuild() forward reference resolution
from typing import List, Dict, Optional, Any  # noqa: F401

# Import all enums
from .enums import (
    SecurableType,
    PrivilegeType,
    Environment,
    BindingType,
    IsolationMode,
    TableType,
    VolumeType,
    FunctionType,
    ConnectionType,
    validate_privilege_dependencies,
    ALL_PRIVILEGES_EXPANSION,
    PRIVILEGE_DEPENDENCIES,
)

# Import base classes and utilities
from .base import (
    BaseGovernanceModel,
    BaseSecurable,
    Tag,
    DEFAULT_SECURABLE_OWNER,
    get_current_environment,
)

# Import grants/access control models
from .grants import (
    Principal,
    Privilege,
    AccessPolicy,
)

# Import workspace binding models
from .workspace_bindings import (
    Workspace,
    WorkspaceBinding,
    WorkspaceBindingPattern,
    WorkspaceRegistry,
)

# Import team models
from .teams import (
    Team,
    AccessManager,
)

# Import storage credentials
from .storage_credentials import (
    StorageCredential,
    AwsIamRole,
    AzureServicePrincipal,
    AzureManagedIdentity,
    GcpServiceAccountKey,
)

# Import external locations
from .external_locations import (
    ExternalLocation,
)

# Import connections
from .connections import (
    Connection,
)

# Import tables
from .tables import (
    Table,
    ColumnInfo,
)

# Import volumes
from .volumes import (
    Volume,
)

# Import functions
from .functions import (
    Function,
)

# Import schemas
from .schemas import (
    Schema,
)

# Import catalogs
from .catalogs import (
    Catalog,
)

# Import metastores
from .metastores import (
    Metastore,
)

# Import lightweight reference models
from .references import (
    TableReference,
    ModelReference,
    VolumeReference,
    FunctionReference,
)


# Import ML models
from .ml_models import (
    RegisteredModel,
    ModelVersion,
    ServiceCredential,
    ModelServingEndpoint,
    ModelVersionStatus,
)

# Import Vector Search models
from .vector_search import (
    VectorSearchEndpoint,
    VectorSearchIndex,
    VectorIndexType,
    VectorSimilarityMetric,
    VectorEndpointType,
)

# Import Genie models
from .genie import (
    GenieSpace,
    GenieSpaceConfig,
    SerializedSpace,
    DataSources,
    TableDataSource,
    ColumnConfig,
    Instructions,
    TextInstruction,
    SqlFunction,
    JoinSpec,
    quick_table,
    quick_function,
)

# Import sharing models
from .sharing import (
    Provider,
    Recipient,
    Share,
    SharedObject,
    AuthenticationType,
    SharingStatus,
)

# Import governance-aware table models (Column with tags, GoverningTable)
from .tables import (
    Column,
    GoverningTable,
    SCD2_COLUMNS,
    BaseColumn,
    BaseTable,
)

# Re-export everything
__all__ = [
    # Enums
    'SecurableType',
    'PrivilegeType',
    'Environment',
    'BindingType',
    'IsolationMode',
    'TableType',
    'VolumeType',
    'FunctionType',
    'ConnectionType',

    # Base classes
    'BaseGovernanceModel',
    'BaseSecurable',
    'Tag',

    # Access control
    'Principal',
    'Privilege',
    'AccessPolicy',

    # Workspace bindings
    'Workspace',
    'WorkspaceBinding',
    'WorkspaceBindingPattern',
    'WorkspaceRegistry',

    # Teams
    'Team',
    'AccessManager',

    # Storage credentials
    'StorageCredential',
    'AwsIamRole',
    'AzureServicePrincipal',
    'AzureManagedIdentity',
    'GcpServiceAccountKey',

    # External locations
    'ExternalLocation',

    # Connections
    'Connection',

    # Tables
    'Table',
    'ColumnInfo',

    # Volumes
    'Volume',

    # Functions
    'Function',

    # Schemas
    'Schema',

    # Catalogs
    'Catalog',

    # Metastores
    'Metastore',

    # Reference models
    'TableReference',
    'ModelReference',
    'VolumeReference',
    'FunctionReference',

    # ML Models
    'RegisteredModel',
    'ModelVersion',
    'ServiceCredential',
    'ModelServingEndpoint',
    'ModelVersionStatus',

    # Vector Search
    'VectorSearchEndpoint',
    'VectorSearchIndex',
    'VectorIndexType',
    'VectorSimilarityMetric',
    'VectorEndpointType',

    # Genie Space
    'GenieSpace',
    'GenieSpaceConfig',
    'SerializedSpace',
    'DataSources',
    'TableDataSource',
    'ColumnConfig',
    'Instructions',
    'TextInstruction',
    'SqlFunction',
    'JoinSpec',
    'quick_table',
    'quick_function',

    # Sharing
    'Provider',
    'Recipient',
    'Share',
    'SharedObject',
    'AuthenticationType',
    'SharingStatus',

    # Utilities
    'get_current_environment',
    'DEFAULT_SECURABLE_OWNER',
    'validate_privilege_dependencies',
    'ALL_PRIVILEGES_EXPANSION',
    'PRIVILEGE_DEPENDENCIES',

    # Table models with tag support (backward compatibility)
    'Column',
    'GoverningTable',
    'SCD2_COLUMNS',
    'BaseColumn',
    'BaseTable',
]

# Rebuild models to resolve forward references
# Order matters: rebuild references first, then models that use them
TableReference.model_rebuild()
VolumeReference.model_rebuild()
ModelReference.model_rebuild()
FunctionReference.model_rebuild()
Function.model_rebuild()
Table.model_rebuild()
Volume.model_rebuild()
Schema.model_rebuild()
Catalog.model_rebuild()
Metastore.model_rebuild()
Column.model_rebuild()
GoverningTable.model_rebuild()
