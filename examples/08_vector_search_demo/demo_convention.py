#!/usr/bin/env python
"""
CLI tool to test YAML convention across all resource types.

This is a quick dry-run test - no actual deployment. Use the notebook for real deployment.

Run with:
    PYTHONPATH=src uv run python examples/08_vector_search_demo/demo_convention.py
    PYTHONPATH=src uv run python examples/08_vector_search_demo/demo_convention.py --env prd
"""

import argparse
from pathlib import Path

from brickkit import (
    Catalog,
    Schema,
    SecurableType,
    Tag,
    VectorSearchEndpoint,
    VectorSearchIndex,
    load_convention,
)
from brickkit.models.base import set_current_environment
from brickkit.models.enums import Environment
from brickkit.models.tables import ColumnInfo, Table

CONVENTION_PATH = Path(__file__).parent / "conventions" / "financial_services.yml"


def create_governed_resources(convention, env: Environment) -> dict:
    """Create all governed resources with convention applied."""
    resources = {}

    # Catalog
    catalog_name = convention.generate_name(SecurableType.CATALOG, env)
    catalog_owner = convention.get_catalog_owner(env)
    catalog = Catalog(
        name=catalog_name,
        owner=catalog_owner,
        comment="Risk Analytics catalog",
    )
    convention.apply_to(catalog, env)
    resources["catalog"] = catalog

    # Schema
    schema_owner = convention.get_owner(SecurableType.SCHEMA, env)
    schema = Schema(
        name="indicators",
        catalog_name=catalog.name,
        owner=schema_owner,
        comment="World Bank indicators",
    )
    convention.apply_to(schema, env)
    resources["schema"] = schema

    # Table
    table = Table(
        name="worldbank_indicators",
        catalog_name=catalog.name,
        schema_name=schema.name,
        owner=schema_owner,
        comment="World Bank indicator metadata with embeddings",
        columns=[
            ColumnInfo(name="indicator_id", type="STRING", nullable=False),
            ColumnInfo(name="indicator_name", type="STRING", nullable=True),
            ColumnInfo(name="description", type="STRING", nullable=True),
            ColumnInfo(name="topic", type="STRING", nullable=True),
            ColumnInfo(name="embedding_text", type="STRING", nullable=True),
        ],
        tags=[
            Tag(key="data_source", value="worldbank_api"),
            Tag(key="contains_pii", value="false"),
        ],
    )
    convention.apply_to(table, env)
    resources["table"] = table

    # Vector Search Endpoint
    endpoint = VectorSearchEndpoint(
        name="quant_risk_search",
        comment="Semantic search endpoint",
        tags=[Tag(key="purpose", value="semantic_search")],
    )
    convention.apply_to(endpoint, env)
    resources["endpoint"] = endpoint

    # Vector Search Index
    index = VectorSearchIndex(
        name="worldbank_indicators_index",
        endpoint_name="quant_risk_search",
        source_table=table.fqdn,  # Reference the governed Table
        primary_key="indicator_id",
        embedding_column="embedding_text",
        embedding_model="databricks-bge-large-en",
        tags=[Tag(key="index_type", value="managed_embedding")],
    )
    convention.apply_to(index, env)
    resources["index"] = index

    return resources


def display_resource(name: str, resource, convention) -> bool:
    """Display resource details and validation status. Returns True if valid."""
    print(f"\n  {name.upper()}")
    print("  " + "-" * 40)

    # Name
    if hasattr(resource, "resolved_name"):
        print(f"    Name: {resource.resolved_name}")
    elif hasattr(resource, "fqdn"):
        try:
            print(f"    Name: {resource.fqdn}")
        except ValueError:
            print(f"    Name: {resource.name}")
    else:
        print(f"    Name: {resource.name}")

    # Owner
    if hasattr(resource, "owner") and resource.owner:
        owner = resource.owner
        print(f"    Owner: {owner.resolved_name} ({owner.principal_type.value})")

    # Request for Access (RFA)
    if hasattr(resource, "request_for_access") and resource.request_for_access:
        rfa = resource.request_for_access
        print(f"    RFA Destination: {rfa.destination}")
        if rfa.instructions:
            print(f"    RFA Instructions: {rfa.instructions}")

    # Tags
    if hasattr(resource, "tags") and resource.tags:
        print(f"    Tags: {len(resource.tags)}")
        for tag in sorted(resource.tags, key=lambda t: t.key)[:5]:
            print(f"      - {tag.key}: {tag.value}")
        if len(resource.tags) > 5:
            print(f"      ... and {len(resource.tags) - 5} more")

    # Validation
    errors = convention.get_validation_errors(resource)
    if errors:
        print(f"    Validation: FAILED - {errors}")
        return False
    print("    Validation: PASSED")
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Test YAML convention (dry-run)")
    parser.add_argument(
        "--env",
        choices=["dev", "acc", "prd"],
        default="dev",
        help="Environment to test (default: dev)",
    )
    args = parser.parse_args()

    env_map = {"dev": Environment.DEV, "acc": Environment.ACC, "prd": Environment.PRD}
    env = env_map[args.env]
    set_current_environment(env)

    print("=" * 60)
    print("CONVENTION TEST (DRY RUN)")
    print("=" * 60)

    # Load convention
    convention = load_convention(CONVENTION_PATH)
    print(f"Convention: {convention.name} (v{convention.version})")
    print(f"Environment: {env.value}")

    # Show rules
    print("\nRules:")
    for rule in convention.schema.rules:
        mode = "ENFORCED" if rule.mode.value == "enforced" else "ADVISORY"
        print(f"  [{mode}] {rule.rule}")

    # Create and validate all resources
    print("\n" + "-" * 60)
    print("RESOURCES")
    print("-" * 60)

    resources = create_governed_resources(convention, env)
    all_valid = True
    for name, resource in resources.items():
        if not display_resource(name, resource, convention):
            all_valid = False

    # Summary
    print("\n" + "=" * 60)
    if all_valid:
        print("ALL VALIDATIONS PASSED")
    else:
        print("SOME VALIDATIONS FAILED")
    print("=" * 60)
    print("\nThis was a dry-run. Use vector_search_demo.ipynb for actual deployment.")


if __name__ == "__main__":
    main()
