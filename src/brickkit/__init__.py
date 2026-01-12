"""
Brickkit - Building blocks for Databricks governance.

This library provides a unified set of governed securables for Databricks,
including Unity Catalog objects, Genie Spaces, and Vector Search assets.

Key Features:
- Unified governance through BaseSecurable inheritance
- Tag-based access control (ABAC) support
- GovernanceDefaults for organization-wide policies
- Environment-aware naming (DEV/ACC/PRD suffixes)
- Tight integration with databricks-sdk

Quick Start:
    from brickkit import (
        Catalog, Schema, GenieSpace, VectorSearchEndpoint,
        GovernanceDefaults, TagDefault, RequiredTag,
        Tag, Principal, AccessPolicy, SecurableType
    )

    # Define organization defaults
    class MyOrgDefaults(GovernanceDefaults):
        @property
        def default_tags(self):
            return [TagDefault(key="managed_by", value="brickkit")]

        @property
        def required_tags(self):
            return [RequiredTag(key="cost_center", applies_to={"CATALOG"})]

    # Create governed securables
    defaults = MyOrgDefaults()

    catalog = Catalog(name="analytics").with_defaults(defaults)
    genie = GenieSpace(
        name="sales_space",
        title="Sales Analytics",
        tags=[Tag(key="domain", value="sales")]
    ).with_defaults(defaults)

    # Grant access
    genie.grant(Principal(name="analysts"), AccessPolicy.READER())
"""

# =============================================================================
# Core Governance
# =============================================================================

from brickkit.defaults import (
    GovernanceDefaults,
    TagDefault,
    RequiredTag,
    NamingConvention,
    EmptyDefaults,
    StandardDefaults,
)

# =============================================================================
# Base Classes and Utilities
# =============================================================================

from models.base import (
    BaseGovernanceModel,
    BaseSecurable,
    Tag,
    get_current_environment,
)

from models.enums import (
    SecurableType,
    PrivilegeType,
    Environment,
    BindingType,
    IsolationMode,
    TableType,
    VolumeType,
    FunctionType,
    ConnectionType,
)

# =============================================================================
# Access Control
# =============================================================================

from models.access import (
    Principal,
    Privilege,
    AccessPolicy,
)

# =============================================================================
# Unity Catalog Securables
# =============================================================================

from models.securables import (
    Catalog,
    Schema,
    StorageCredential,
    ExternalLocation,
    Connection,
)

from models.references import (
    TableReference,
    VolumeReference,
    FunctionReference,
    ModelReference,
)

# =============================================================================
# AI/ML Securables
# =============================================================================

from genie.models import (
    GenieSpace,
    GenieSpaceConfig,  # Backward compatibility
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

from vector_search.models import (
    VectorSearchEndpoint,
    VectorSearchIndex,
    VectorIndexType,
    VectorSimilarityMetric,
    VectorEndpointType,
    VectorSearchConfig,  # Backward compatibility
    VectorSearchIndexConfig,  # Backward compatibility
)

# =============================================================================
# ML Models (if available)
# =============================================================================

try:
    from models.ml_models import (
        RegisteredModel,
        ModelVersion,
        ServiceCredential,
    )
except ImportError:
    # ML models not available
    pass

# =============================================================================
# Delta Sharing (if available)
# =============================================================================

try:
    from models.sharing import (
        Provider,
        Recipient,
        Share,
    )
except ImportError:
    # Sharing models not available
    pass


__all__ = [
    # Governance Defaults
    "GovernanceDefaults",
    "TagDefault",
    "RequiredTag",
    "NamingConvention",
    "EmptyDefaults",
    "StandardDefaults",
    # Base Classes
    "BaseGovernanceModel",
    "BaseSecurable",
    "Tag",
    "get_current_environment",
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
    # Access Control
    "Principal",
    "Privilege",
    "AccessPolicy",
    # Unity Catalog Securables
    "Catalog",
    "Schema",
    "StorageCredential",
    "ExternalLocation",
    "Connection",
    # References
    "TableReference",
    "VolumeReference",
    "FunctionReference",
    "ModelReference",
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
    # Vector Search
    "VectorSearchEndpoint",
    "VectorSearchIndex",
    "VectorIndexType",
    "VectorSimilarityMetric",
    "VectorEndpointType",
    "VectorSearchConfig",
    "VectorSearchIndexConfig",
]
