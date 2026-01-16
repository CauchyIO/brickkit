"""
Example: Using a Project Manifest for governance configuration.

This example shows how teams can define governance rules in a JSON manifest
file and load it at runtime, instead of writing Python classes.
"""

from pathlib import Path

from brickkit import (
    Catalog,
    Schema,
    Tag,
    load_project_manifest,
)

# Load the manifest from JSON
manifest_path = Path(__file__).parent / "project.manifest.json"
defaults = load_project_manifest(manifest_path)

# Access manifest metadata
print(f"Organization: {defaults.organization}")
print(f"Default owner: {defaults.default_owner}")
print(f"Default tags: {[t.key for t in defaults.default_tags]}")
print(f"Required tags: {[t.key for t in defaults.required_tags]}")

# Create a catalog with required tags
catalog = Catalog(
    name="sales_analytics",
    tags=[
        Tag(key="cost_center", value="cc_engineering_002"),
        Tag(key="data_classification", value="internal"),
        Tag(key="data_owner", value="sales_team"),
    ],
)

# Apply defaults - adds managed_by, business_unit, environment
catalog = defaults.apply_to(catalog, defaults.manifest.version)

# Validate against governance rules
from models.enums import Environment, get_current_environment

env = get_current_environment()
errors = defaults.validate_tags(catalog.securable_type, {t.key: t.value for t in catalog.tags})

if errors:
    print(f"Validation errors: {errors}")
else:
    print("Catalog passes governance validation")

# Print resulting tags
print("\nCatalog tags after applying defaults:")
for tag in catalog.tags:
    print(f"  {tag.key}: {tag.value}")
