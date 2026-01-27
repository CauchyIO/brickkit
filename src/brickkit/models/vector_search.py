"""
Vector Search models as governed securables.

Vector Search endpoints and indexes are AI infrastructure assets
that support governance through tags, owners, and access policies.

This module provides both:
- Governed securable classes (VectorSearchEndpoint, VectorSearchIndex)
- Configuration classes (VectorSearchConfig, VectorSearchIndexConfig) for backward compat

Usage:
    from brickkit import VectorSearchEndpoint, VectorSearchIndex, Tag, Principal, AccessPolicy

    # Create governed endpoint
    endpoint = VectorSearchEndpoint(
        name="product_search",
        tags=[Tag(key="team", value="ml")],
    )

    # Create governed index
    index = VectorSearchIndex(
        name="product_embeddings",
        endpoint_name="product_search",
        source_table="catalog.schema.products",
        primary_key="product_id",
        embedding_column="embedding",
        tags=[Tag(key="domain", value="products")],
    )

    # Grant access
    endpoint.grant(Principal(name="ml_team"), AccessPolicy.ADMIN())
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Literal, Optional

from pydantic import Field, computed_field, field_validator

# Import governance base classes
from .base import BaseGovernanceModel, BaseSecurable, Tag, get_current_environment
from .enums import SecurableType

# =============================================================================
# ENUMS
# =============================================================================


class VectorIndexType(str, Enum):
    """Type of vector index."""

    DELTA_SYNC = "DELTA_SYNC"
    DIRECT_ACCESS = "DIRECT_ACCESS"


class VectorSimilarityMetric(str, Enum):
    """Similarity metric for vector search."""

    COSINE = "COSINE"
    DOT_PRODUCT = "DOT_PRODUCT"
    EUCLIDEAN = "EUCLIDEAN"


class VectorEndpointType(str, Enum):
    """Type of vector search endpoint."""

    STANDARD = "STANDARD"


# =============================================================================
# VECTOR SEARCH ENDPOINT - GOVERNED SECURABLE
# =============================================================================


class VectorSearchEndpoint(BaseSecurable):
    """
    Vector Search Endpoint as a governed securable.

    Endpoints host vector search indexes and provide compute resources
    for similarity search operations.

    Attributes:
        name: Endpoint name (base, without env suffix)
        endpoint_type: Type of endpoint (STANDARD)
        comment: Optional description
        tags: Governance tags (inherited from BaseSecurable)

    Example:
        endpoint = VectorSearchEndpoint(
            name="search_endpoint",
            endpoint_type=VectorEndpointType.STANDARD,
            tags=[Tag(key="team", value="ml")],
        )

        # Grant access
        endpoint.grant(Principal(name="ml_team"), AccessPolicy.ADMIN())
    """

    name: str = Field(..., description="Endpoint name (base, without env suffix)")
    endpoint_type: VectorEndpointType = Field(VectorEndpointType.STANDARD, description="Type of endpoint")
    comment: Optional[str] = Field(None, description="Description")

    @computed_field
    @property
    def resolved_name(self) -> str:
        """Name with environment suffix."""
        env = get_current_environment()
        return f"{self.name}_{env.value.lower()}"

    @property
    def securable_type(self) -> SecurableType:
        return SecurableType.VECTOR_SEARCH_ENDPOINT

    def get_level_1_name(self) -> str:
        return self.resolved_name

    def get_level_2_name(self) -> Optional[str]:
        return None

    def get_level_3_name(self) -> Optional[str]:
        return None

    def to_sdk_create_params(self) -> Dict[str, Any]:
        """Convert to SDK parameters for endpoint creation."""
        # Import SDK's EndpointType enum - the SDK expects this, not a string
        from databricks.sdk.service.vectorsearch import EndpointType as SdkEndpointType

        # Map our enum to SDK enum
        endpoint_type_map = {
            VectorEndpointType.STANDARD: SdkEndpointType.STANDARD,
            "STANDARD": SdkEndpointType.STANDARD,
        }
        sdk_endpoint_type = endpoint_type_map.get(self.endpoint_type, SdkEndpointType.STANDARD)

        return {
            "name": self.resolved_name,
            "endpoint_type": sdk_endpoint_type,
        }


# =============================================================================
# VECTOR SEARCH INDEX - GOVERNED SECURABLE
# =============================================================================


class VectorSearchIndex(BaseSecurable):
    """
    Vector Search Index as a governed securable.

    Indexes enable similarity search over embeddings stored in Delta tables
    or directly managed vectors.

    Attributes:
        name: Index name (base, without env suffix)
        endpoint_name: Parent endpoint name (base, without env suffix)
        index_type: Type of index (DELTA_SYNC or DIRECT_ACCESS)
        source_table: Full table name (catalog.schema.table)
        primary_key: Primary key column
        embedding_column: Column containing embeddings
        embedding_model: Model endpoint for computing embeddings
        sync_columns: Additional columns to sync
        pipeline_type: TRIGGERED or CONTINUOUS
        tags: Governance tags (inherited from BaseSecurable)

    Example:
        index = VectorSearchIndex(
            name="product_embeddings",
            endpoint_name="search_endpoint",
            source_table="catalog.schema.products",
            primary_key="product_id",
            embedding_column="embedding",
            tags=[Tag(key="domain", value="products")],
        )
    """

    name: str = Field(..., description="Index name (base, without env suffix)")
    endpoint_name: str = Field(..., description="Parent endpoint name (base, without env suffix)")

    # Index configuration
    index_type: VectorIndexType = Field(VectorIndexType.DELTA_SYNC)
    source_table: str = Field(..., description="Full table name (catalog.schema.table)")
    primary_key: str = Field("id", description="Primary key column")
    embedding_column: str = Field(..., description="Column containing embeddings")
    embedding_model: Optional[str] = Field(None, description="Model endpoint for computing embeddings")
    embedding_dimension: Optional[int] = Field(None, description="Embedding vector dimension")
    sync_columns: List[str] = Field(default_factory=list, description="Additional columns to sync")
    pipeline_type: Literal["TRIGGERED", "CONTINUOUS"] = Field("TRIGGERED")
    similarity_metric: VectorSimilarityMetric = Field(VectorSimilarityMetric.COSINE)

    comment: Optional[str] = None

    @computed_field
    @property
    def resolved_name(self) -> str:
        """Full index name with environment suffix."""
        env = get_current_environment()
        return f"{self.name}_{env.value.lower()}"

    @computed_field
    @property
    def resolved_endpoint_name(self) -> str:
        """Endpoint name with environment suffix."""
        env = get_current_environment()
        return f"{self.endpoint_name}_{env.value.lower()}"

    @property
    def securable_type(self) -> SecurableType:
        return SecurableType.VECTOR_SEARCH_INDEX

    def get_level_1_name(self) -> str:
        return self.resolved_endpoint_name

    def get_level_2_name(self) -> Optional[str]:
        return self.resolved_name

    def get_level_3_name(self) -> Optional[str]:
        return None

    @field_validator("source_table")
    @classmethod
    def validate_source_table(cls, v: str) -> str:
        """Validate source table is fully qualified."""
        parts = v.split(".")
        if len(parts) != 3:
            raise ValueError(f"source_table must be fully qualified (catalog.schema.table), got: {v}")
        return v

    def to_sdk_create_params(self) -> Dict[str, Any]:
        """Convert to SDK parameters for index creation."""
        # Import SDK enums - the SDK expects these, not strings
        from databricks.sdk.service.vectorsearch import PipelineType as SdkPipelineType
        from databricks.sdk.service.vectorsearch import VectorIndexType as SdkVectorIndexType

        # Map our enums to SDK enums
        index_type_map = {
            VectorIndexType.DELTA_SYNC: SdkVectorIndexType.DELTA_SYNC,
            VectorIndexType.DIRECT_ACCESS: SdkVectorIndexType.DIRECT_ACCESS,
            "DELTA_SYNC": SdkVectorIndexType.DELTA_SYNC,
            "DIRECT_ACCESS": SdkVectorIndexType.DIRECT_ACCESS,
        }
        pipeline_type_map = {
            "TRIGGERED": SdkPipelineType.TRIGGERED,
            "CONTINUOUS": SdkPipelineType.CONTINUOUS,
        }

        sdk_index_type = index_type_map.get(self.index_type, SdkVectorIndexType.DELTA_SYNC)
        sdk_pipeline_type = pipeline_type_map.get(self.pipeline_type, SdkPipelineType.TRIGGERED)

        params = {
            "name": self.resolved_name,
            "endpoint_name": self.resolved_endpoint_name,
            "index_type": sdk_index_type,
            "primary_key": self.primary_key,
        }

        # Check for DELTA_SYNC
        is_delta_sync = sdk_index_type == SdkVectorIndexType.DELTA_SYNC
        if is_delta_sync:
            delta_spec = {
                "source_table": self.source_table,
                "embedding_source_column": self.embedding_column,
                "pipeline_type": sdk_pipeline_type,
            }
            if self.embedding_model:
                delta_spec["embedding_model_endpoint_name"] = self.embedding_model
            if self.sync_columns:
                delta_spec["columns_to_sync"] = self.sync_columns
            params["delta_sync_index_spec"] = delta_spec

        return params


# =============================================================================
# BACKWARD COMPATIBILITY - Configuration Classes
# =============================================================================


class VectorSearchIndexConfig(BaseGovernanceModel):
    """
    Configuration for a single Vector Search Index.

    Backward compatible with existing usage patterns.
    For new code, prefer using VectorSearchIndex directly.
    """

    source_table: str
    primary_key: str = "indicator_id"
    embedding_column: str = "embedding_text"
    embedding_model: str = "databricks-bge-large-en"
    pipeline_type: Literal["TRIGGERED", "CONTINUOUS"] = "TRIGGERED"
    sql_function_name: str

    @computed_field
    @property
    def index_name(self) -> str:
        return f"{self.source_table}_index"


class VectorSearchConfig(BaseGovernanceModel):
    """
    Top-level configuration for all vector search resources.

    Backward compatible with existing usage patterns.
    For new code, prefer using VectorSearchEndpoint and VectorSearchIndex directly.
    """

    catalog: str = "main_catalog"
    schema_name: str = "dev"
    endpoint_name: str = "dev_vector_search"
    indices: List[VectorSearchIndexConfig] = Field(default_factory=list)

    # Governance additions
    tags: List[Tag] = Field(default_factory=list)

    def get_full_table_name(self, table: str) -> str:
        return f"{self.catalog}.{self.schema_name}.{table}"

    def get_full_index_name(self, index_config: VectorSearchIndexConfig) -> str:
        return f"{self.catalog}.{self.schema_name}.{index_config.index_name}"

    def get_full_function_name(self, index_config: VectorSearchIndexConfig) -> str:
        return f"{self.catalog}.{self.schema_name}.{index_config.sql_function_name}"

    def to_governed_securables(self) -> tuple[VectorSearchEndpoint, List[VectorSearchIndex]]:
        """
        Convert configuration to governed securables.

        Returns:
            Tuple of (VectorSearchEndpoint, List[VectorSearchIndex])

        Example:
            config = VectorSearchConfig(...)
            endpoint, indexes = config.to_governed_securables()

            # Now you can use governance features
            endpoint.grant(Principal(name="ml_team"), AccessPolicy.ADMIN())
        """
        endpoint = VectorSearchEndpoint(
            name=self.endpoint_name,
            tags=self.tags.copy(),
        )

        indexes = []
        for idx_config in self.indices:
            full_table = self.get_full_table_name(idx_config.source_table)
            index = VectorSearchIndex(
                name=idx_config.index_name,
                endpoint_name=self.endpoint_name,
                source_table=full_table,
                primary_key=idx_config.primary_key,
                embedding_column=idx_config.embedding_column,
                embedding_model=idx_config.embedding_model,
                pipeline_type=idx_config.pipeline_type,
                tags=self.tags.copy(),
            )
            indexes.append(index)

        return endpoint, indexes


# =============================================================================
# EXPORTS
# =============================================================================

__all__ = [
    # Governed securables
    "VectorSearchEndpoint",
    "VectorSearchIndex",
    # Enums
    "VectorIndexType",
    "VectorSimilarityMetric",
    "VectorEndpointType",
    # Backward compatibility
    "VectorSearchConfig",
    "VectorSearchIndexConfig",
    # Re-exported governance types
    "Tag",
]
