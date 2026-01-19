#!/usr/bin/env python
"""
Demo script showing YAML convention usage for catalog deployment.

Run with:
    PYTHONPATH=src uv run python examples/08_vector_search_demo/demo_convention.py
"""

from pathlib import Path

from brickkit import Catalog, SecurableType, load_convention
from brickkit.models.base import set_current_environment
from brickkit.models.enums import Environment

# Path to convention file
CONVENTION_PATH = Path(__file__).parent / "conventions" / "financial_services.yml"


def main() -> None:
    print("=" * 60)
    print("YAML Convention Demo - Catalog Deployment")
    print("=" * 60)

    # 1. Load convention
    print("\n[1] Loading convention...")
    convention = load_convention(CONVENTION_PATH)
    print(f"    Loaded: {convention.name} (v{convention.version})")

    # 2. Deploy to each environment
    for env in [Environment.DEV, Environment.ACC, Environment.PRD]:
        # Set the current environment (simulates DATABRICKS_ENV)
        set_current_environment(env)
        print(f"\n[2] Deploying to {env.value}...")

        # Generate compliant name
        catalog_name = convention.generate_name(SecurableType.CATALOG, env)
        print(f"    Catalog name: {catalog_name}")

        # Get configured owner
        owner = convention.get_catalog_owner(env)
        print(f"    Owner: {owner.resolved_name} ({owner.principal_type.value})")

        # Create catalog
        catalog = Catalog(name=catalog_name, owner=owner)

        # Apply convention tags
        convention.apply_to(catalog, env)
        print(f"    Tags applied: {len(catalog.tags)}")
        for tag in catalog.tags:
            print(f"      - {tag.key}: {tag.value}")

        # Validate
        errors = convention.get_validation_errors(catalog)
        if errors:
            print(f"    Validation FAILED: {errors}")
        else:
            print("    Validation: PASSED")

        # Show what would be deployed (dry-run)
        print("\n    Would deploy:")
        print(f"      catalog: {catalog.name}")
        print(f"      owner: {catalog.owner.resolved_name}")
        print(f"      isolation_mode: {catalog.isolation_mode.value}")

    print("\n" + "=" * 60)
    print("Demo complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
