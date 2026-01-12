import time
from typing import Literal

from databricks.vector_search.client import VectorSearchClient
from pydantic import BaseModel, computed_field


class VectorSearchIndexConfig(BaseModel):
    """Configuration for a single Vector Search Index."""

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


class VectorSearchConfig(BaseModel):
    """Top-level configuration for all vector search resources."""

    catalog: str = "main_catalog"
    schema_name: str = "dev"
    endpoint_name: str = "dev_vector_search"
    indices: list[VectorSearchIndexConfig]

    def get_full_table_name(self, table: str) -> str:
        return f"{self.catalog}.{self.schema_name}.{table}"

    def get_full_index_name(self, index_config: VectorSearchIndexConfig) -> str:
        return f"{self.catalog}.{self.schema_name}.{index_config.index_name}"

    def get_full_function_name(self, index_config: VectorSearchIndexConfig) -> str:
        return f"{self.catalog}.{self.schema_name}.{index_config.sql_function_name}"