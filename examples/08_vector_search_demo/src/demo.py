"""
BrickKit Vector Search Demo

This demo demonstrates:
1. Loading governance conventions from a YAML file
2. Creating governed securables (Catalog, Schema, Table, VectorSearch)
3. Applying naming conventions and tags from YAML
4. Validating ownership rules (SP for catalogs, SP/Group for others)
5. Deploying resources with proper tags

Prerequisites:
- Databricks workspace with Unity Catalog enabled
- Service Principal configured
- Vector Search enabled on workspace

Usage:
    # Dry run (no deployment)
    python demo.py --dry-run

    # Deploy to dev environment
    python demo.py --environment dev

    # Deploy to prod
    python demo.py --environment prd
"""

import argparse
import logging
import sys
from pathlib import Path

# Add brickkit to path if running from examples
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "src"))

from databricks.sdk import WorkspaceClient

from brickkit import (
    Catalog,
    RuleMode,
    Schema,
    SecurableType,
    Tag,
    VectorSearchEndpoint,
    VectorSearchIndex,
    load_convention,
)
from brickkit.executors import (
    CatalogExecutor,
    SchemaExecutor,
    VectorSearchEndpointExecutor,
)
from brickkit.models.base import set_current_environment
from brickkit.models.enums import Environment
from brickkit.models.tables import Table

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def create_governed_resources(convention, environment: Environment) -> dict:
    """
    Create governed resources following the convention.

    Returns dict of securables organized by type.
    """
    resources = {}

    # Generate catalog name from convention
    catalog_name = convention.generate_name(SecurableType.CATALOG, environment)
    logger.info(f"Catalog name (from convention): {catalog_name}")

    # Get owners from convention
    catalog_owner = convention.get_catalog_owner(environment)
    schema_owner = convention.get_owner(SecurableType.SCHEMA, environment)

    logger.info(f"Catalog owner: {catalog_owner.resolved_name} ({catalog_owner.principal_type.value})")
    logger.info(f"Schema owner: {schema_owner.resolved_name} ({schema_owner.principal_type.value})")

    # 1. Create Catalog with SP owner
    catalog = Catalog(
        name=catalog_name,
        owner=catalog_owner,
        comment="Risk Analytics catalog for trading quantitative analytics",
    )

    # Apply convention (adds tags)
    convention.apply_to(catalog, environment)

    # Validate
    errors = convention.get_validation_errors(catalog)
    if errors:
        logger.error(f"[FAIL] Catalog validation failed: {errors}")
        raise ValueError(f"Catalog validation failed: {errors}")
    logger.info(f"[PASS] Catalog '{catalog.name}' passed convention validation")

    resources["catalog"] = catalog

    # 2. Create Schema with default owner (SP or Group)
    schema = Schema(
        name="indicators",
        catalog_name=catalog.name,
        owner=schema_owner,
        comment="World Bank indicator metadata for vector search",
    )

    convention.apply_to(schema, environment)
    errors = convention.get_validation_errors(schema)
    if errors:
        logger.error(f"[FAIL] Schema validation failed: {errors}")
        raise ValueError(f"Schema validation failed: {errors}")
    logger.info(f"[PASS] Schema '{schema.fqdn}' passed convention validation")

    resources["schema"] = schema

    # 3. Create Table definition (source table for vector search)
    table = Table(
        name="worldbank_indicators",
        catalog_name=catalog.name,
        schema_name=schema.name,
        owner=schema_owner,
        comment="World Bank indicator metadata with embeddings for semantic search",
        tags=[
            Tag(key="data_source", value="worldbank_api"),
            Tag(key="refresh_frequency", value="weekly"),
        ],
    )

    convention.apply_to(table, environment)
    errors = convention.get_validation_errors(table)
    if errors:
        logger.error(f"[FAIL] Table validation failed: {errors}")
        raise ValueError(f"Table validation failed: {errors}")
    logger.info(f"[PASS] Table '{table.fqdn}' passed convention validation")

    resources["table"] = table

    # 4. Create Vector Search Endpoint
    endpoint_name = convention.generate_name(SecurableType.VECTOR_SEARCH_ENDPOINT, environment)
    vs_endpoint = VectorSearchEndpoint(
        name=endpoint_name,
        comment="Vector search endpoint for risk analytics indicators",
        tags=[
            Tag(key="purpose", value="semantic_search"),
            Tag(key="model", value="databricks-bge-large-en"),
        ],
    )

    convention.apply_to(vs_endpoint, environment)
    errors = convention.get_validation_errors(vs_endpoint)
    if errors:
        logger.error(f"[FAIL] VectorSearchEndpoint validation failed: {errors}")
        raise ValueError(f"VectorSearchEndpoint validation failed: {errors}")
    logger.info(f"[PASS] VectorSearchEndpoint '{vs_endpoint.name}' passed validation")

    resources["vs_endpoint"] = vs_endpoint

    # 5. Create Vector Search Index
    vs_index = VectorSearchIndex(
        name="worldbank_indicators",
        endpoint_name=endpoint_name,
        source_table=f"{catalog.name}.{schema.name}.{table.name}",
        primary_key="indicator_id",
        embedding_column="embedding_text",
        embedding_model="databricks-bge-large-en",
        tags=[
            Tag(key="index_type", value="managed_embedding"),
        ],
    )

    convention.apply_to(vs_index, environment)
    errors = convention.get_validation_errors(vs_index)
    if errors:
        logger.error(f"[FAIL] VectorSearchIndex validation failed: {errors}")
        raise ValueError(f"VectorSearchIndex validation failed: {errors}")
    logger.info(f"[PASS] VectorSearchIndex '{vs_index.name}' passed validation")

    resources["vs_index"] = vs_index

    return resources


def get_resource_name(resource) -> str:
    """Get the best display name for a resource."""
    if hasattr(resource, "fqdn"):
        try:
            return resource.fqdn
        except ValueError:
            pass
    if hasattr(resource, "resolved_name"):
        return resource.resolved_name
    return getattr(resource, "name", "N/A")


def display_resource_summary(resources: dict, convention):
    """Display summary of all resources and their governance status."""
    print("\n" + "=" * 70)
    print("RESOURCE SUMMARY")
    print("=" * 70)

    for resource_type, resource in resources.items():
        print(f"\n{resource_type.upper()}")
        print("-" * 40)

        name = get_resource_name(resource)
        print(f"  Name: {name}")

        owner = getattr(resource, "owner", None)
        if owner:
            owner_type = owner.principal_type.value if owner.principal_type else "UNKNOWN"
            print(f"  Owner: {owner.resolved_name} ({owner_type})")

        tags = getattr(resource, "tags", [])
        if tags:
            print(f"  Tags ({len(tags)}):")
            for tag in tags[:5]:
                print(f"    - {tag.key}: {tag.value}")
            if len(tags) > 5:
                print(f"    ... and {len(tags) - 5} more")

    print("\n" + "=" * 70)
    print("CONVENTION RULES")
    print("=" * 70)
    for rule_spec in convention.schema.rules:
        mode_str = "[ENFORCED]" if rule_spec.mode == RuleMode.ENFORCED else "[ADVISORY]"
        print(f"  {mode_str} {rule_spec.rule}")


def deploy_resources(resources: dict, client: WorkspaceClient, dry_run: bool = True):
    """Deploy resources to Databricks."""
    print("\n" + "=" * 70)
    print("DEPLOYMENT" + (" (DRY RUN)" if dry_run else ""))
    print("=" * 70)

    # Deploy Catalog
    catalog = resources.get("catalog")
    if catalog:
        executor = CatalogExecutor(client, dry_run=dry_run)
        result = executor.create(catalog)
        status = "[OK]" if result.success else "[FAIL]"
        print(f"  {status} Catalog: {result.message}")

    # Deploy Schema
    schema = resources.get("schema")
    if schema:
        executor = SchemaExecutor(client, dry_run=dry_run)
        result = executor.create(schema)
        status = "[OK]" if result.success else "[FAIL]"
        print(f"  {status} Schema: {result.message}")

    # Note: Table is created via ETL notebook, not here
    print("  [SKIP] Table: Will be created via ETL notebook")

    # Deploy Vector Search Endpoint
    vs_endpoint = resources.get("vs_endpoint")
    if vs_endpoint:
        executor = VectorSearchEndpointExecutor(client, dry_run=dry_run)
        result = executor.create(vs_endpoint)
        status = "[OK]" if result.success else "[FAIL]"
        print(f"  {status} VectorSearchEndpoint: {result.message}")

    # Note: Index requires source table to exist first
    print("  [SKIP] VectorSearchIndex: Will be created after ETL completes")


def main():
    parser = argparse.ArgumentParser(description="BrickKit Vector Search Demo")
    parser.add_argument(
        "--environment",
        "-e",
        choices=["dev", "acc", "prd"],
        default="dev",
        help="Deployment environment (default: dev)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes",
    )
    parser.add_argument(
        "--convention",
        default="conventions/financial_services.yml",
        help="Path to convention YAML file",
    )
    args = parser.parse_args()

    # Map string to Environment enum
    env_map = {"dev": Environment.DEV, "acc": Environment.ACC, "prd": Environment.PRD}
    environment = env_map[args.environment]

    # Set the current environment
    set_current_environment(environment)

    print("=" * 70)
    print("BRICKKIT VECTOR SEARCH GOVERNANCE DEMO")
    print("=" * 70)
    print(f"Environment: {environment.value}")
    print(f"Convention: {args.convention}")
    print(f"Mode: {'DRY RUN' if args.dry_run else 'DEPLOYMENT'}")
    print("=" * 70)

    # Resolve convention path relative to demo directory
    demo_dir = Path(__file__).parent.parent
    convention_path = demo_dir / args.convention

    # 1. Load convention from YAML
    logger.info(f"Loading convention from {convention_path}")
    convention = load_convention(str(convention_path))
    logger.info(f"Loaded convention: {convention.name} (v{convention.version})")
    logger.info(f"  - {len(convention.schema.rules)} rules defined")

    # 2. Create governed resources
    resources = create_governed_resources(convention, environment)

    # 3. Display summary
    display_resource_summary(resources, convention)

    # 4. Deploy (if not dry-run or if --dry-run for simulation)
    if not args.dry_run:
        client = WorkspaceClient()
        deploy_resources(resources, client, dry_run=False)
    else:
        # Even in dry-run, show what would happen
        print("\n[DRY RUN] Would deploy the following resources:")
        deploy_resources(resources, None, dry_run=True)


if __name__ == "__main__":
    main()
