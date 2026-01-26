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
from typing import Any, Dict, List, Optional  # noqa: F401

# Import base classes and utilities
from .base import (
    DEFAULT_SECURABLE_OWNER,
    BaseGovernanceModel,
    BaseSecurable,
    Tag,
    get_current_environment,
)

# Import catalogs
from .catalogs import (
    Catalog,
)

# Import connections
from .connections import (
    Connection,
)

# Import all enums
from .enums import (
    ALL_PRIVILEGES_EXPANSION,
    PRIVILEGE_DEPENDENCIES,
    AclObjectType,
    BindingType,
    ConnectionType,
    Environment,
    FunctionType,
    IsolationMode,
    PrincipalSource,
    PrincipalType,
    PrivilegeType,
    SecurableType,
    TableType,
    VolumeType,
    WorkspaceEntitlement,
    validate_privilege_dependencies,
)

# Import external locations
from .external_locations import (
    ExternalLocation,
)

# Import functions
from .functions import (
    Function,
)

# Import Genie models
from .genie import (
    ColumnConfig,
    DataSources,
    GenieSpace,
    GenieSpaceConfig,
    Instructions,
    JoinSpec,
    SerializedSpace,
    SqlFunction,
    TableDataSource,
    TextInstruction,
    quick_function,
    quick_table,
)

# Import grants/access control models
from .grants import (
    AccessPolicy,
    Principal,
    Privilege,
)

# Import metastores
from .metastores import (
    Metastore,
)

# Import ML models
from .ml_models import (
    ModelServingEndpoint,
    ModelVersion,
    ModelVersionStatus,
    RegisteredModel,
    ServiceCredential,
)

# Import lightweight reference models
from .references import (
    FunctionReference,
    ModelReference,
    TableReference,
    VolumeReference,
)

# Import schemas
from .schemas import (
    Schema,
)

# Import sharing models
from .sharing import (
    AuthenticationType,
    Provider,
    Recipient,
    Share,
    SharedObject,
    SharingStatus,
)

# Import storage credentials
from .storage_credentials import (
    AwsIamRole,
    AzureManagedIdentity,
    AzureServicePrincipal,
    GcpServiceAccountKey,
    StorageCredential,
)

# Import tables
# Import governance-aware table models (Column with tags, GoverningTable)
from .tables import (
    SCD2_COLUMNS,
    BaseColumn,
    BaseTable,
    Column,
    ColumnInfo,
    GoverningTable,
    Table,
)

# Import team models
from .teams import (
    AccessManager,
    Team,
)

# Import Vector Search models
from .vector_search import (
    VectorEndpointType,
    VectorIndexType,
    VectorSearchEndpoint,
    VectorSearchIndex,
    VectorSimilarityMetric,
)

# Import volumes
from .volumes import (
    Volume,
)

# Import workspace binding models
from .workspace_bindings import (
    Workspace,
    WorkspaceBinding,
    WorkspaceBindingPattern,
    WorkspaceRegistry,
)

# Import principal management models
from .principals import (
    ManagedGroup,
    ManagedServicePrincipal,
    MemberReference,
)

# Import ACL models
from .acls import (
    AclBinding,
    AclEntry,
)

# Re-export everything
__all__ = [
    # Enums
    "SecurableType",
    "PrivilegeType",
    "Environment",
    "BindingType",
    "IsolationMode",
    "TableType",
    "VolumeType",
    "FunctionType",
    "ConnectionType",
    "PrincipalType",
    "PrincipalSource",
    "WorkspaceEntitlement",
    "AclObjectType",
    # Base classes
    "BaseGovernanceModel",
    "BaseSecurable",
    "Tag",
    # Access control
    "Principal",
    "Privilege",
    "AccessPolicy",
    # Workspace bindings
    "Workspace",
    "WorkspaceBinding",
    "WorkspaceBindingPattern",
    "WorkspaceRegistry",
    # Teams
    "Team",
    "AccessManager",
    # Storage credentials
    "StorageCredential",
    "AwsIamRole",
    "AzureServicePrincipal",
    "AzureManagedIdentity",
    "GcpServiceAccountKey",
    # External locations
    "ExternalLocation",
    # Connections
    "Connection",
    # Tables
    "Table",
    "ColumnInfo",
    # Volumes
    "Volume",
    # Functions
    "Function",
    # Schemas
    "Schema",
    # Catalogs
    "Catalog",
    # Metastores
    "Metastore",
    # Reference models
    "TableReference",
    "ModelReference",
    "VolumeReference",
    "FunctionReference",
    # ML Models
    "RegisteredModel",
    "ModelVersion",
    "ServiceCredential",
    "ModelServingEndpoint",
    "ModelVersionStatus",
    # Vector Search
    "VectorSearchEndpoint",
    "VectorSearchIndex",
    "VectorIndexType",
    "VectorSimilarityMetric",
    "VectorEndpointType",
    # Genie Space
    "GenieSpace",
    "GenieSpaceConfig",
    "SerializedSpace",
    "DataSources",
    "TableDataSource",
    "ColumnConfig",
    "Instructions",
    "TextInstruction",
    "SqlFunction",
    "JoinSpec",
    "quick_table",
    "quick_function",
    # Sharing
    "Provider",
    "Recipient",
    "Share",
    "SharedObject",
    "AuthenticationType",
    "SharingStatus",
    # Utilities
    "get_current_environment",
    "DEFAULT_SECURABLE_OWNER",
    "validate_privilege_dependencies",
    "ALL_PRIVILEGES_EXPANSION",
    "PRIVILEGE_DEPENDENCIES",
    # Table models with tag support (backward compatibility)
    "Column",
    "GoverningTable",
    "SCD2_COLUMNS",
    "BaseColumn",
    "BaseTable",
    # Principal management
    "ManagedGroup",
    "ManagedServicePrincipal",
    "MemberReference",
    # ACL models
    "AclBinding",
    "AclEntry",
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
