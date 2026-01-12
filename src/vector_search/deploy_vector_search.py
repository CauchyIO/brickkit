import time
from typing import Literal

from databricks.vector_search.client import VectorSearchClient
from pydantic import BaseModel, computed_field


def create_endpoint_if_not_exists(client: VectorSearchClient, endpoint_name: str) -> None:
    """Create the vector search endpoint if it doesn't exist."""
    try:
        endpoint = client.get_endpoint(endpoint_name)
        print(f"Endpoint '{endpoint_name}' already exists")
    except Exception as e:
        if "RESOURCE_DOES_NOT_EXIST" in str(e) or "NOT_FOUND" in str(e):
            print(f"Creating endpoint '{endpoint_name}'...")
            client.create_endpoint(name=endpoint_name, endpoint_type="STANDARD")
            print(f"Endpoint '{endpoint_name}' created.")
        else:
            raise e


def wait_for_endpoint(client: VectorSearchClient, endpoint_name: str) -> None:
    """Wait for endpoint to be online."""
    while True:
        endpoint = client.get_endpoint(endpoint_name)
        status = endpoint.get("endpoint_status", {}).get("state", "UNKNOWN")
        print(f"Endpoint status: {status}")
        if status == "ONLINE":
            print("Endpoint is ready!")
            break
        elif status in ["PROVISIONING", "PENDING"]:
            print("Waiting 30 seconds...")
            time.sleep(30)
        else:
            print(f"Unexpected status: {status}")
            break

def create_index_if_not_exists(
    client: VectorSearchClient,
    endpoint_name: str,
    index_name: str,
    source_table: str,
    primary_key: str,
    embedding_column: str,
    embedding_model: str,
    pipeline_type: str,
) -> None:
    """Create a vector search index if it doesn't exist."""
    try:
        index = client.get_index(endpoint_name=endpoint_name, index_name=index_name)
        print(f"Index '{index_name}' already exists")
    except Exception as e:
        if "RESOURCE_DOES_NOT_EXIST" in str(e) or "NOT_FOUND" in str(e):
            print(f"Creating managed embedding index '{index_name}'...")
            client.create_delta_sync_index(
                endpoint_name=endpoint_name,
                index_name=index_name,
                source_table_name=source_table,
                primary_key=primary_key,
                embedding_source_column=embedding_column,
                embedding_model_endpoint_name=embedding_model,
                pipeline_type=pipeline_type,
            )
            print(f"Index '{index_name}' created and syncing...")
        else:
            raise e


for index_config in config.indices:
    create_index_if_not_exists(
        client=client,
        endpoint_name=config.endpoint_name,
        index_name=config.get_full_index_name(index_config),
        source_table=config.get_full_table_name(index_config.source_table),
        primary_key=index_config.primary_key,
        embedding_column=index_config.embedding_column,
        embedding_model=index_config.embedding_model,
        pipeline_type=index_config.pipeline_type,
    )
    print()