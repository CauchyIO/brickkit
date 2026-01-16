"""
Brickkit - Building blocks for Databricks governance.

This library provides a unified set of governed securables for Databricks,
including Unity Catalog objects, Genie Spaces, and Vector Search assets.

Key Features:
- Unified governance through BaseSecurable inheritance
- Tag-based access control (ABAC) support
- GovernanceDefaults for organization-wide policies
- Convention pattern for hierarchical governance propagation
- Environment-aware naming (DEV/ACC/PRD suffixes)
- Tight integration with databricks-sdk

Quick Start:
    from brickkit import (
        Metastore, Catalog, Schema,
        Convention, TagDefault, RequiredTag,
        Tag, Principal, AccessPolicy
    )

    # Define a convention (propagates through hierarchy)
    convention = Convention(
        name="org_standards",
        default_tags=[
            TagDefault(key="managed_by", value="brickkit"),
            TagDefault(key="compliance", value="sox"),
        ],
        required_tags=[
            RequiredTag(key="cost_center", applies_to={"CATALOG"}),
        ],
    )

    # Build hierarchy
    m = Metastore(name="main")
    c = Catalog(name="finance")
    s = Schema(name="reports")

    m.add_catalog(c)
    c.add_schema(s)

    # Apply convention at top level - propagates to all descendants
    m.with_convention(convention)

    # New children automatically inherit the convention
    s2 = Schema(name="analytics")
    c.add_schema(s2)  # Convention auto-applied

    # Grant access (also propagates)
    c.grant(Principal(name="analysts"), AccessPolicy.READER())
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

from brickkit.manifest import (
    ProjectManifest,
    ManifestBasedDefaults,
    load_project_manifest,
)

from brickkit.convention import (
    Convention,
    ConventionAsDefaults,
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

from models.grants import (
    Principal,
    Privilege,
    AccessPolicy,
)

# =============================================================================
# Unity Catalog Securables
# =============================================================================

from models.metastores import Metastore
from models.catalogs import Catalog
from models.schemas import Schema
from models.storage_credentials import StorageCredential
from models.external_locations import ExternalLocation
from models.connections import Connection

from models.references import (
    TableReference,
    VolumeReference,
    FunctionReference,
    ModelReference,
)

# =============================================================================
# AI/ML Securables
# =============================================================================

from models.genie import (
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

from models.vector_search import (
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
    from models.ml_models import (  # noqa: F401
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
    from models.sharing import (  # noqa: F401
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
    # Convention
    "Convention",
    "ConventionAsDefaults",
    # Project Manifest
    "ProjectManifest",
    "ManifestBasedDefaults",
    "load_project_manifest",
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
    "Metastore",
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
