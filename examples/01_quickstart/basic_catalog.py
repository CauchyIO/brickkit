"""
Basic Catalog Example

Creates a simple catalog with environment-aware naming and tags.
The catalog name automatically gets a suffix based on DATABRICKS_ENV.
"""

import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from models.base import Tag, get_current_environment
from models.securables import Catalog, Schema
from models.enums import IsolationMode

# Create a basic catalog
# Name will resolve to "analytics_dev", "analytics_acc", or "analytics_prd"
# based on DATABRICKS_ENV environment variable
catalog = Catalog(
    name="analytics",
    comment="Analytics domain data products",
    isolation_mode=IsolationMode.OPEN,
    tags=[
        Tag(key="domain", value="analytics"),
        Tag(key="cost_center", value="data-platform"),
    ],
)

# Check resolved name (includes environment suffix)
print(f"Environment: {get_current_environment()}")
print(f"Base name: {catalog.name}")
print(f"Resolved name: {catalog.resolved_name}")
print(f"Tags: {[(t.key, t.value) for t in catalog.tags]}")

# Add schemas to the catalog
bronze = Schema(name="bronze", comment="Raw landing zone")
silver = Schema(name="silver", comment="Cleansed data")
gold = Schema(name="gold", comment="Business-ready aggregates")

catalog.add_schema(bronze)
catalog.add_schema(silver)
catalog.add_schema(gold)

print(f"\nSchemas: {[s.name for s in catalog.schemas]}")

# Output example (when DATABRICKS_ENV=dev):
# Environment: Environment.DEV
# Base name: analytics
# Resolved name: analytics_dev
# Tags: [('domain', 'analytics'), ('cost_center', 'data-platform')]
# Schemas: ['bronze', 'silver', 'gold']
