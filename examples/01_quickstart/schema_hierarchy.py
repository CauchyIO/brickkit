"""
Schema Hierarchy Example

Shows how to build a catalog with schemas and table references.
Demonstrates the three-level hierarchy: Catalog → Schema → References.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from models.base import Tag
from models.securables import Catalog, Schema
from models.references import TableReference, VolumeReference
from models.enums import IsolationMode

# Create catalog
sales_catalog = Catalog(
    name="sales",
    comment="Sales domain data products",
    isolation_mode=IsolationMode.OPEN,
)

# Create schema with table references
orders_schema = Schema(
    name="orders",
    comment="Order transaction data",
    tags=[Tag(key="pii", value="false")],
)

# Add lightweight table references (pointers, not full definitions)
# Tables are created by DABs/DLT, brickkit just manages permissions
orders_schema.add_table_reference(
    TableReference(name="orders_raw", catalog_name="sales", schema_name="orders")
)
orders_schema.add_table_reference(
    TableReference(name="orders_enriched", catalog_name="sales", schema_name="orders")
)
orders_schema.add_volume_reference(
    VolumeReference(name="order_files", catalog_name="sales", schema_name="orders")
)

sales_catalog.add_schema(orders_schema)

# Print hierarchy
print(f"Catalog: {sales_catalog.resolved_name}")
for schema in sales_catalog.schemas:
    print(f"  Schema: {schema.name}")
    for table_ref in schema.table_refs:
        print(f"    Table: {table_ref.name}")
    for volume_ref in schema.volume_refs:
        print(f"    Volume: {volume_ref.name}")

# Output:
# Catalog: sales_dev
#   Schema: orders
#     Table: orders_raw
#     Table: orders_enriched
#     Volume: order_files
