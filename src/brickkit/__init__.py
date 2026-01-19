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

__version__ = "0.1.0"

# =============================================================================
# Core Governance
# =============================================================================

from brickkit.convention import (
    Convention,
    ConventionAsDefaults,
)
from brickkit.defaults import (
    EmptyDefaults,
    GovernanceDefaults,
    NamingConvention,
    RequiredTag,
    StandardDefaults,
    TagDefault,
)
from brickkit.manifest import (
    ManifestBasedDefaults,
    ProjectManifest,
    load_project_manifest,
)

# =============================================================================
# Base Classes and Utilities
# =============================================================================
from brickkit.models.base import (
    BaseGovernanceModel,
    BaseSecurable,
    Tag,
    get_current_environment,
)
from brickkit.models.catalogs import Catalog
from brickkit.models.connections import Connection
from brickkit.models.enums import (
    BindingType,
    ConnectionType,
    Environment,
    FunctionType,
    IsolationMode,
    PrivilegeType,
    SecurableType,
    TableType,
    VolumeType,
)
from brickkit.models.external_locations import ExternalLocation

# =============================================================================
# AI/ML Securables
# =============================================================================
from brickkit.models.genie import (
    ColumnConfig,
    DataSources,
    GenieSpace,
    GenieSpaceConfig,  # Backward compatibility
    Instructions,
    JoinSpec,
    SerializedSpace,
    SqlFunction,
    TableDataSource,
    TextInstruction,
    quick_function,
    quick_table,
)

# =============================================================================
# Access Control
# =============================================================================
from brickkit.models.grants import (
    AccessPolicy,
    Principal,
    Privilege,
)

# =============================================================================
# Unity Catalog Securables
# =============================================================================
from brickkit.models.metastores import Metastore
from brickkit.models.references import (
    FunctionReference,
    ModelReference,
    TableReference,
    VolumeReference,
)
from brickkit.models.schemas import Schema
from brickkit.models.storage_credentials import StorageCredential
from brickkit.models.vector_search import (
    VectorEndpointType,
    VectorIndexType,
    VectorSearchConfig,  # Backward compatibility
    VectorSearchEndpoint,
    VectorSearchIndex,
    VectorSearchIndexConfig,  # Backward compatibility
    VectorSimilarityMetric,
)

# =============================================================================
# ML Models (if available)
# =============================================================================

try:
    from brickkit.models.ml_models import (  # noqa: F401
        ModelVersion,
        RegisteredModel,
        ServiceCredential,
    )
except ImportError:
    # ML models not available
    pass

# =============================================================================
# Delta Sharing (if available)
# =============================================================================

try:
    from brickkit.models.sharing import (  # noqa: F401
        Provider,
        Recipient,
        Share,
    )
except ImportError:
    # Sharing models not available
    pass

# =============================================================================
# ML Governance (requires mlflow)
# =============================================================================

try:
    from brickkit.ml_governance import (  # noqa: F401
        DataClassification,
        GovernanceError,
        GovernanceMLflowClient,
        GovernanceMonitor,
        GovernancePolicy,
        GovernanceValidator,
        GovernedMLTemplate,
        ModelTier,
        ValidationResult,
        data_lineage_tracking,
        generate_governance_report,
        governed_experiment,
        governed_training,
        requires_approval,
    )
except ImportError:
    # MLflow not available
    pass


__all__ = [
    # Version
    "__version__",
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
    # ML Governance
    "ModelTier",
    "DataClassification",
    "GovernancePolicy",
    "ValidationResult",
    "GovernanceValidator",
    "GovernedMLTemplate",
    "GovernanceMLflowClient",
    "GovernanceMonitor",
    "GovernanceError",
    "governed_training",
    "requires_approval",
    "governed_experiment",
    "data_lineage_tracking",
    "generate_governance_report",
]
