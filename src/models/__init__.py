"""
Unity Catalog governance models.

This package provides all Pydantic models for the Unity Catalog governance system.
The models are organized into logical modules but are all exported from this
single interface for convenience.

IMPORTANT: Some models are deprecated. Use references.py for lightweight governance.
"""

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

# Import access control models
from .access import (
    AccessPolicy,
    Team,
    AccessManager,
    Workspace,
    WorkspaceBinding,
    WorkspaceBindingPattern,
    WorkspaceRegistry,
)

# Import NEW lightweight reference models (preferred)
from .references import (
    TableReference,
    ModelReference,
    VolumeReference,
    FunctionReference,
)

# Import securables from securables module (including Catalog/Schema)
from .securables import (
    Catalog,  # Use the full-featured version from securables
    Schema,   # Use the full-featured version from securables
    ColumnInfo,
    StorageCredential,
    ExternalLocation,
    Connection,
    Volume,
    Function,
    Metastore,
)

# Import DEPRECATED models for backward compatibility
# These import with warnings
from .deprecated import (
    Table,  # Use TableReference instead
    RegisteredModel,  # Use ModelReference instead
)

# Import ML models (some deprecated)
from .ml_models import (
    ModelVersion,  # Will be deprecated
    ServiceCredential,
    ModelServingEndpoint,
    VectorSearchIndex,
    ModelVersionStatus,
    VectorIndexType,
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

# Principal and Privilege are in access module
from .access import Principal, Privilege

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
    'Team',
    'AccessManager',
    
    # NEW Reference models (preferred)
    'TableReference',
    'ModelReference',
    'VolumeReference',
    'FunctionReference',
    
    # Governance models
    'Metastore',
    'Catalog',
    'Schema',
    
    # Deprecated models (backward compatibility)
    'Table',  # DEPRECATED - use TableReference
    'RegisteredModel',  # DEPRECATED - use ModelReference
    
    # Other securables
    'ColumnInfo',
    'Volume',
    'Function',
    'StorageCredential',
    'ExternalLocation',
    'Connection',
    
    # ML Models
    'ModelVersion',
    'ServiceCredential',
    'ModelServingEndpoint',
    'VectorSearchIndex',
    'ModelVersionStatus',
    'VectorIndexType',
    
    # Sharing
    'Provider',
    'Recipient',
    'Share',
    'SharedObject',
    'AuthenticationType',
    'SharingStatus',
    
    # Workspace
    'Workspace',
    'WorkspaceBinding',
    'WorkspaceBindingPattern',
    'WorkspaceRegistry',
    
    # Utilities
    'get_current_environment',
    'DEFAULT_SECURABLE_OWNER',
    'validate_privilege_dependencies',
    'ALL_PRIVILEGES_EXPANSION',
    'PRIVILEGE_DEPENDENCIES',
]

# Rebuild models to resolve forward references
# This is needed because Catalog references TableReference, etc.
Catalog.model_rebuild()
Schema.model_rebuild()