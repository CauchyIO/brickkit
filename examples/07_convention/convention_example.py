# Databricks notebook source
"""
Convention Pattern Example

This example demonstrates how to use the Convention pattern for
hierarchical governance propagation in Brickkit.

Key concepts:
1. Define a Convention with org-wide standards
2. Apply it at any level (Metastore, Catalog, or Schema)
3. Governance rules automatically propagate to all descendants
4. New children automatically inherit the convention

Run with: python convention_example.py
"""

from loguru import logger

from brickkit.convention import Convention
from brickkit.defaults import NamingConvention, RequiredTag, TagDefault
from brickkit.models.base import Tag, init_environment
from brickkit.models.catalogs import Catalog
from brickkit.models.enums import TableType
from brickkit.models.metastores import Metastore
from brickkit.models.schemas import Schema
from brickkit.models.tables import ColumnInfo, Table

# COMMAND ----------

env = init_environment()


def main():
    # =========================================================================
    # STEP 1: Define a Convention
    # =========================================================================
    # An Architect team defines organizational standards that will be
    # consistently applied across all securables.

    finance_convention = Convention(
        name="finance_standards",
        default_tags=[
            # These tags are automatically applied to all securables
            TagDefault(key="managed_by", value="brickkit"),
            TagDefault(key="compliance", value="sox"),
            TagDefault(
                key="environment",
                value="dev",
                # Environment-specific overrides
                environment_values={"PRD": "prod", "ACC": "acc"},
            ),
            # Tag only applies to specific securable types
            TagDefault(key="data_classification", value="internal", applies_to={"TABLE", "VOLUME"}),
        ],
        required_tags=[
            # These tags must be present (validated on demand)
            RequiredTag(
                key="cost_center", applies_to={"CATALOG", "SCHEMA"}, error_message="Cost center required for chargeback"
            ),
            RequiredTag(
                key="data_owner", applies_to={"TABLE"}, allowed_values={"finance_team", "audit_team", "shared"}
            ),
        ],
        naming_conventions=[
            # Naming rules (validated on demand)
            NamingConvention(
                pattern=r"^fin_[a-z][a-z0-9_]*$",
                applies_to={"CATALOG"},
                error_message="Finance catalogs must start with 'fin_'",
            ),
        ],
        default_owner="finance_platform_team",
    )

    logger.info("=" * 60)
    logger.info("Convention Pattern Example")
    logger.info("=" * 60)

    # =========================================================================
    # STEP 2: Build the Hierarchy
    # =========================================================================

    # Create metastore (top-level container)
    m = Metastore(name="main_metastore")

    # Create catalog with required tag
    catalog = Catalog(name="fin_analytics", tags=[Tag(key="cost_center", value="finance-001")])

    # Create schema with required tag
    schema = Schema(name="reports", tags=[Tag(key="cost_center", value="finance-001")])

    # Create table with required tag
    table = Table(
        name="quarterly_revenue",
        table_type=TableType.MANAGED,
        columns=[
            ColumnInfo(name="quarter", type_name="STRING"),
            ColumnInfo(name="revenue", type_name="DECIMAL(18,2)"),
            ColumnInfo(name="region", type_name="STRING"),
        ],
        tags=[Tag(key="data_owner", value="finance_team")],
    )

    # Build hierarchy
    m.add_catalog(catalog)
    catalog.add_schema(schema)
    schema.add_table(table)

    logger.info("\n1. Built hierarchy:")
    logger.info(f"   Metastore: {m.name}")
    logger.info(f"   +-- Catalog: {catalog.name}")
    logger.info(f"       +-- Schema: {schema.name}")
    logger.info(f"           +-- Table: {table.name}")

    # =========================================================================
    # STEP 3: Apply Convention at Top Level
    # =========================================================================
    # This propagates governance to ALL descendants automatically!

    logger.info("\n2. Applying convention at metastore level...")
    m.with_convention(finance_convention)

    # Check that tags were applied
    logger.info("\n3. Tags after convention applied:")
    logger.info(f"   Catalog tags: {[f'{t.key}={t.value}' for t in catalog.tags]}")
    logger.info(f"   Schema tags:  {[f'{t.key}={t.value}' for t in schema.tags]}")
    logger.info(f"   Table tags:   {[f'{t.key}={t.value}' for t in table.tags]}")

    # =========================================================================
    # STEP 4: New Children Automatically Inherit Convention
    # =========================================================================

    logger.info("\n4. Adding new schema (auto-inherits convention)...")

    new_schema = Schema(name="audit_reports", tags=[Tag(key="cost_center", value="finance-002")])
    catalog.add_schema(new_schema)

    logger.info(f"   New schema tags: {[f'{t.key}={t.value}' for t in new_schema.tags]}")

    # Add a table to the new schema
    audit_table = Table(
        name="audit_log",
        table_type=TableType.MANAGED,
        columns=[
            ColumnInfo(name="timestamp", type_name="TIMESTAMP"),
            ColumnInfo(name="action", type_name="STRING"),
        ],
        tags=[Tag(key="data_owner", value="audit_team")],
    )
    new_schema.add_table(audit_table)

    logger.info(f"   New table tags: {[f'{t.key}={t.value}' for t in audit_table.tags]}")

    # =========================================================================
    # STEP 5: Validate Against Convention Rules
    # =========================================================================

    logger.info("\n5. Validating against convention rules...")

    # Validate catalog
    catalog_errors = finance_convention.validate(catalog)
    logger.info(f"   Catalog validation: {'PASS' if not catalog_errors else 'FAIL: ' + str(catalog_errors)}")

    # Validate table
    table_errors = finance_convention.validate(table)
    logger.info(f"   Table validation: {'PASS' if not table_errors else 'FAIL: ' + str(table_errors)}")

    # Create a table missing required tag to show validation failure
    bad_table = Table(
        name="missing_owner",
        table_type=TableType.MANAGED,
        columns=[ColumnInfo(name="id", type_name="INT")],
        # Missing data_owner tag!
    )
    bad_table_errors = finance_convention.validate(bad_table)
    logger.info(f"   Bad table validation: {'PASS' if not bad_table_errors else 'FAIL: ' + str(bad_table_errors)}")

    # =========================================================================
    # STEP 6: Convention Can Be Applied at Any Level
    # =========================================================================

    logger.info("\n6. Convention can be applied at any level...")

    # Create a standalone schema and apply convention directly
    standalone_schema = Schema(name="standalone", tags=[Tag(key="cost_center", value="finance-003")])
    standalone_table = Table(
        name="data",
        table_type=TableType.MANAGED,
        columns=[ColumnInfo(name="value", type_name="STRING")],
        tags=[Tag(key="data_owner", value="shared")],
    )
    standalone_schema.add_table(standalone_table)

    # Apply convention at schema level
    standalone_schema.with_convention(finance_convention)

    logger.info(f"   Standalone schema tags: {[f'{t.key}={t.value}' for t in standalone_schema.tags]}")
    logger.info(f"   Standalone table tags: {[f'{t.key}={t.value}' for t in standalone_table.tags]}")

    # =========================================================================
    # STEP 7: Interoperability with GovernanceDefaults
    # =========================================================================

    logger.info("\n7. Convention is interoperable with GovernanceDefaults...")

    # Convert to GovernanceDefaults for backward compatibility
    as_defaults = finance_convention.to_governance_defaults()
    logger.info(f"   Convention as GovernanceDefaults: {type(as_defaults).__name__}")
    logger.info(f"   Default tags count: {len(as_defaults.default_tags)}")

    logger.info("\n" + "=" * 60)
    logger.info("Convention pattern successfully demonstrated!")
    logger.info("=" * 60)


# COMMAND ----------

if __name__ == "__main__":
    main()
