"""
Unit tests for Vector Search models.

Tests VectorSearchEndpoint and VectorSearchIndex models.
"""

import pytest
from pydantic import ValidationError

from brickkit.models.enums import SecurableType
from brickkit.models.vector_search import (
    VectorIndexType,
    VectorSearchEndpoint,
    VectorSearchIndex,
)
from tests.fixtures import make_tag, make_vector_search_endpoint, make_vector_search_index


class TestVectorSearchEndpointNaming:
    """Tests for VectorSearchEndpoint environment-aware naming."""

    def test_resolved_name_with_environment_suffix_dev(self, dev_environment: None) -> None:
        """Endpoint resolved_name includes _dev suffix in DEV environment."""
        endpoint = VectorSearchEndpoint(name="search_endpoint")
        assert endpoint.resolved_name == "search_endpoint_dev"

    def test_resolved_name_with_environment_suffix_acc(self, acc_environment: None) -> None:
        """Endpoint resolved_name includes _acc suffix in ACC environment."""
        endpoint = VectorSearchEndpoint(name="search_endpoint")
        assert endpoint.resolved_name == "search_endpoint_acc"

    def test_resolved_name_with_environment_suffix_prd(self, prd_environment: None) -> None:
        """Endpoint resolved_name includes _prd suffix in PRD environment."""
        endpoint = VectorSearchEndpoint(name="search_endpoint")
        assert endpoint.resolved_name == "search_endpoint_prd"


class TestVectorSearchEndpointSdkConversion:
    """Tests for VectorSearchEndpoint SDK parameter conversion."""

    def test_to_sdk_create_params(self, dev_environment: None) -> None:
        """to_sdk_create_params returns correct structure."""
        endpoint = make_vector_search_endpoint(name="my_endpoint")
        params = endpoint.to_sdk_create_params()
        assert params["name"] == "my_endpoint_dev"
        assert "endpoint_type" in params

    def test_securable_type_property(self) -> None:
        """Endpoint has VECTOR_SEARCH_ENDPOINT securable type."""
        endpoint = VectorSearchEndpoint(name="test")
        assert endpoint.securable_type == SecurableType.VECTOR_SEARCH_ENDPOINT

    def test_get_level_1_name(self, dev_environment: None) -> None:
        """get_level_1_name returns resolved endpoint name."""
        endpoint = VectorSearchEndpoint(name="test")
        assert endpoint.get_level_1_name() == "test_dev"

    def test_get_level_2_name_is_none(self) -> None:
        """get_level_2_name returns None for endpoints."""
        endpoint = VectorSearchEndpoint(name="test")
        assert endpoint.get_level_2_name() is None


class TestVectorSearchIndexNaming:
    """Tests for VectorSearchIndex environment-aware naming."""

    def test_resolved_name_with_environment_suffix(self, dev_environment: None) -> None:
        """Index resolved_name includes _dev suffix in DEV environment."""
        index = make_vector_search_index(name="my_index")
        assert index.resolved_name == "my_index_dev"

    def test_resolved_endpoint_name(self, dev_environment: None) -> None:
        """Index resolved_endpoint_name includes _dev suffix."""
        index = make_vector_search_index(name="my_index", endpoint_name="my_endpoint")
        assert index.resolved_endpoint_name == "my_endpoint_dev"


class TestVectorSearchIndexValidation:
    """Tests for VectorSearchIndex validation."""

    def test_validate_source_table_format_valid(self) -> None:
        """Valid 3-part source table names are accepted."""
        index = make_vector_search_index(source_table="catalog.schema.table")
        assert index.source_table == "catalog.schema.table"

    def test_validate_source_table_format_invalid_two_parts(self) -> None:
        """Two-part source table names raise validation error."""
        with pytest.raises(ValidationError) as exc_info:
            VectorSearchIndex(
                name="test_index",
                endpoint_name="endpoint",
                source_table="schema.table",
                primary_key="id",
                embedding_column="embedding",
            )
        assert "fully qualified" in str(exc_info.value).lower()

    def test_validate_source_table_format_invalid_one_part(self) -> None:
        """One-part source table names raise validation error."""
        with pytest.raises(ValidationError) as exc_info:
            VectorSearchIndex(
                name="test_index",
                endpoint_name="endpoint",
                source_table="table_only",
                primary_key="id",
                embedding_column="embedding",
            )
        assert "fully qualified" in str(exc_info.value).lower()


class TestVectorSearchIndexSdkConversion:
    """Tests for VectorSearchIndex SDK parameter conversion."""

    def test_to_sdk_create_params_delta_sync(self, dev_environment: None) -> None:
        """to_sdk_create_params returns correct structure for DELTA_SYNC index."""
        index = make_vector_search_index(
            name="my_index",
            endpoint_name="my_endpoint",
            index_type=VectorIndexType.DELTA_SYNC,
            source_table="catalog.schema.table",
            primary_key="id",
            embedding_column="embedding",
            embedding_model="databricks-bge-large-en",
        )
        params = index.to_sdk_create_params()
        assert params["name"] == "my_index_dev"
        assert params["endpoint_name"] == "my_endpoint_dev"
        assert params["primary_key"] == "id"
        assert "delta_sync_index_spec" in params
        assert params["delta_sync_index_spec"]["source_table"] == "catalog.schema.table"
        assert params["delta_sync_index_spec"]["embedding_source_column"] == "embedding"
        assert params["delta_sync_index_spec"]["embedding_model_endpoint_name"] == "databricks-bge-large-en"

    def test_to_sdk_create_params_with_sync_columns(self, dev_environment: None) -> None:
        """to_sdk_create_params includes sync columns when specified."""
        index = make_vector_search_index(
            name="my_index",
            sync_columns=["col1", "col2"],
        )
        params = index.to_sdk_create_params()
        assert "delta_sync_index_spec" in params
        assert params["delta_sync_index_spec"]["columns_to_sync"] == ["col1", "col2"]

    def test_securable_type_property(self) -> None:
        """Index has VECTOR_SEARCH_INDEX securable type."""
        index = make_vector_search_index()
        assert index.securable_type == SecurableType.VECTOR_SEARCH_INDEX

    def test_get_level_1_name(self, dev_environment: None) -> None:
        """get_level_1_name returns resolved endpoint name."""
        index = make_vector_search_index(endpoint_name="my_endpoint")
        assert index.get_level_1_name() == "my_endpoint_dev"

    def test_get_level_2_name(self, dev_environment: None) -> None:
        """get_level_2_name returns resolved index name."""
        index = make_vector_search_index(name="my_index")
        assert index.get_level_2_name() == "my_index_dev"


class TestVectorSearchIndexTags:
    """Tests for VectorSearchIndex governance features."""

    def test_index_with_tags(self, dev_environment: None) -> None:
        """Index can have governance tags."""
        tags = [make_tag("team", "ml"), make_tag("domain", "products")]
        index = make_vector_search_index(tags=tags)
        assert len(index.tags) == 2
        assert index.tags[0].key == "team"
        assert index.tags[0].value == "ml"
