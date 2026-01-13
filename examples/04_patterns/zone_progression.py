"""
Zone Progression Pattern (Medallion Architecture)

Data flows through quality stages: Bronze → Silver → Gold

Bronze (Raw):
- Preserve raw data exactly as received
- No quality guarantees
- Access: Data engineers only

Silver (Cleansed):
- Validated, deduplicated, typed
- Quality enforced
- Access: Analysts, data scientists

Gold (Business-Ready):
- Aggregates, features, metrics
- SLA guaranteed
- Access: Business users, applications
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from models.securables import Catalog, Schema
from models.access import Principal, AccessPolicy
from models.references import TableReference
from models.base import Tag
from models.enums import IsolationMode


def create_medallion_catalog(domain: str) -> Catalog:
    """
    Create a catalog with Bronze/Silver/Gold schema structure.

    Args:
        domain: Business domain name (e.g., 'sales', 'marketing')
    """
    catalog = Catalog(
        name=domain,
        comment=f"{domain.title()} domain data",
        isolation_mode=IsolationMode.OPEN,
        tags=[Tag(key="architecture", value="medallion")],
    )

    # Bronze: Raw landing zone
    bronze = Schema(
        name="bronze",
        comment="Raw data exactly as received from source systems",
        tags=[
            Tag(key="zone", value="bronze"),
            Tag(key="quality", value="none"),
            Tag(key="retention_days", value="90"),
        ],
    )

    # Silver: Cleansed and validated
    silver = Schema(
        name="silver",
        comment="Cleansed, validated, deduplicated data",
        tags=[
            Tag(key="zone", value="silver"),
            Tag(key="quality", value="enforced"),
            Tag(key="retention_days", value="365"),
        ],
    )

    # Gold: Business-ready aggregates
    gold = Schema(
        name="gold",
        comment="Business aggregates, features, and metrics",
        tags=[
            Tag(key="zone", value="gold"),
            Tag(key="quality", value="sla_guaranteed"),
            Tag(key="retention_days", value="2555"),  # 7 years
        ],
    )

    catalog.add_schema(bronze)
    catalog.add_schema(silver)
    catalog.add_schema(gold)

    # Access control by zone
    data_engineers = Principal(name="data_engineers")
    analysts = Principal(name="analysts")
    business_users = Principal(name="business_users")

    # Data engineers: full access to all zones
    catalog.grant(data_engineers, AccessPolicy.WRITER())

    # Analysts: read silver and gold
    silver.grant(analysts, AccessPolicy.READER())
    gold.grant(analysts, AccessPolicy.READER())

    # Business users: read gold only
    gold.grant(business_users, AccessPolicy.READER())

    return catalog


# Create a sales domain with medallion architecture
sales = create_medallion_catalog("sales")

# Add table references to show the flow
bronze = sales.schemas[0]  # bronze
silver = sales.schemas[1]  # silver
gold = sales.schemas[2]    # gold

# Bronze: raw events from source
bronze.add_table_reference(TableReference(
    name="orders_raw",
    catalog_name="sales",
    schema_name="bronze",
))
bronze.add_table_reference(TableReference(
    name="customers_raw",
    catalog_name="sales",
    schema_name="bronze",
))

# Silver: cleansed entities
silver.add_table_reference(TableReference(
    name="orders_cleansed",
    catalog_name="sales",
    schema_name="silver",
))
silver.add_table_reference(TableReference(
    name="customers_cleansed",
    catalog_name="sales",
    schema_name="silver",
))

# Gold: business metrics
gold.add_table_reference(TableReference(
    name="daily_sales",
    catalog_name="sales",
    schema_name="gold",
))
gold.add_table_reference(TableReference(
    name="customer_lifetime_value",
    catalog_name="sales",
    schema_name="gold",
))

# Display the structure
print(f"=== {sales.resolved_name} (Medallion Architecture) ===\n")

for schema in sales.schemas:
    zone_tag = next((t for t in schema.tags if t.key == "zone"), None)
    quality_tag = next((t for t in schema.tags if t.key == "quality"), None)

    print(f"{schema.name.upper()}")
    print(f"  Quality: {quality_tag.value if quality_tag else 'N/A'}")
    print(f"  Tables:")
    for table_ref in schema.table_refs:
        print(f"    - {table_ref.name}")
    print(f"  Access:")
    for priv in schema.privileges:
        print(f"    - {priv.principal}: {priv.privilege.value}")
    print()

# Data flow visualization
print("=== Data Flow ===")
print("orders_raw (bronze) → orders_cleansed (silver) → daily_sales (gold)")
print("customers_raw (bronze) → customers_cleansed (silver) → customer_lifetime_value (gold)")

# Output:
# === sales_dev (Medallion Architecture) ===
#
# BRONZE
#   Quality: none
#   Tables:
#     - orders_raw: Raw order events from POS system
#     - customers_raw: Raw customer data from CRM
#   Access:
#     - data_engineers_dev: USE_SCHEMA
#     - data_engineers_dev: SELECT
#     ...
#
# SILVER
#   Quality: enforced
#   ...
