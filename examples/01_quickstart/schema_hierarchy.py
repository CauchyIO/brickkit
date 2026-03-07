# Databricks notebook source
"""
Schema Hierarchy Example

Shows how to build a catalog with schemas and table references.
Demonstrates the three-level hierarchy: Catalog → Schema → References.
"""

from loguru import logger

from brickkit.models.base import Tag, init_environment
from brickkit.models.catalogs import Catalog
from brickkit.models.enums import IsolationMode
from brickkit.models.references import TableReference, VolumeReference
from brickkit.models.schemas import Schema

# COMMAND ----------

env = init_environment()

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
orders_schema.add_table_reference(TableReference(name="orders_raw", catalog_name="sales", schema_name="orders"))
orders_schema.add_table_reference(TableReference(name="orders_enriched", catalog_name="sales", schema_name="orders"))
orders_schema.add_volume_reference(VolumeReference(name="order_files", catalog_name="sales", schema_name="orders"))

sales_catalog.add_schema(orders_schema)

# Print hierarchy
logger.info(f"Catalog: {sales_catalog.resolved_name}")
for schema in sales_catalog.schemas:
    logger.info(f"  Schema: {schema.name}")
    for table_ref in schema.table_refs:
        logger.info(f"    Table: {table_ref.name}")
    for volume_ref in schema.volume_refs:
        logger.info(f"    Volume: {volume_ref.name}")

# Output:
# Catalog: sales_dev
#   Schema: orders
#     Table: orders_raw
#     Table: orders_enriched
#     Volume: order_files
