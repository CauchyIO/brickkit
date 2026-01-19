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

import sys
from pathlib import Path

# Add src to path for local development
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from brickkit.convention import Convention, ConventionAsDefaults
from brickkit.defaults import TagDefault, RequiredTag, NamingConvention
from models.securables import Metastore, Catalog, Schema, Table, ColumnInfo
from models.base import Tag
from models.enums import TableType
from models.access import Principal, AccessPolicy


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
                environment_values={"PRD": "prod", "ACC": "acc"}
            ),
            # Tag only applies to specific securable types
            TagDefault(
                key="data_classification",
                value="internal",
                applies_to={"TABLE", "VOLUME"}
            ),
        ],
        required_tags=[
            # These tags must be present (validated on demand)
            RequiredTag(
                key="cost_center",
                applies_to={"CATALOG", "SCHEMA"},
                error_message="Cost center required for chargeback"
            ),
            RequiredTag(
                key="data_owner",
                applies_to={"TABLE"},
                allowed_values={"finance_team", "audit_team", "shared"}
            ),
        ],
        naming_conventions=[
            # Naming rules (validated on demand)
            NamingConvention(
                pattern=r"^fin_[a-z][a-z0-9_]*$",
                applies_to={"CATALOG"},
                error_message="Finance catalogs must start with 'fin_'"
            ),
        ],
        default_owner="finance_platform_team",
    )

    print("=" * 60)
    print("Convention Pattern Example")
    print("=" * 60)

    # =========================================================================
    # STEP 2: Build the Hierarchy
    # =========================================================================

    # Create metastore (top-level container)
    m = Metastore(name="main_metastore")

    # Create catalog with required tag
    catalog = Catalog(
        name="fin_analytics",
        tags=[Tag(key="cost_center", value="finance-001")]
    )

    # Create schema with required tag
    schema = Schema(
        name="reports",
        tags=[Tag(key="cost_center", value="finance-001")]
    )

    # Create table with required tag
    table = Table(
        name="quarterly_revenue",
        table_type=TableType.MANAGED,
        columns=[
            ColumnInfo(name="quarter", type_name="STRING"),
            ColumnInfo(name="revenue", type_name="DECIMAL(18,2)"),
            ColumnInfo(name="region", type_name="STRING"),
        ],
        tags=[Tag(key="data_owner", value="finance_team")]
    )

    # Build hierarchy
    m.add_catalog(catalog)
    catalog.add_schema(schema)
    schema.add_table(table)

    print("\n1. Built hierarchy:")
    print(f"   Metastore: {m.name}")
    print(f"   +-- Catalog: {catalog.name}")
    print(f"       +-- Schema: {schema.name}")
    print(f"           +-- Table: {table.name}")

    # =========================================================================
    # STEP 3: Apply Convention at Top Level
    # =========================================================================
    # This propagates governance to ALL descendants automatically!

    print("\n2. Applying convention at metastore level...")
    m.with_convention(finance_convention)

    # Check that tags were applied
    print("\n3. Tags after convention applied:")
    print(f"   Catalog tags: {[f'{t.key}={t.value}' for t in catalog.tags]}")
    print(f"   Schema tags:  {[f'{t.key}={t.value}' for t in schema.tags]}")
    print(f"   Table tags:   {[f'{t.key}={t.value}' for t in table.tags]}")

    # =========================================================================
    # STEP 4: New Children Automatically Inherit Convention
    # =========================================================================

    print("\n4. Adding new schema (auto-inherits convention)...")

    new_schema = Schema(
        name="audit_reports",
        tags=[Tag(key="cost_center", value="finance-002")]
    )
    catalog.add_schema(new_schema)

    print(f"   New schema tags: {[f'{t.key}={t.value}' for t in new_schema.tags]}")

    # Add a table to the new schema
    audit_table = Table(
        name="audit_log",
        table_type=TableType.MANAGED,
        columns=[
            ColumnInfo(name="timestamp", type_name="TIMESTAMP"),
            ColumnInfo(name="action", type_name="STRING"),
        ],
        tags=[Tag(key="data_owner", value="audit_team")]
    )
    new_schema.add_table(audit_table)

    print(f"   New table tags: {[f'{t.key}={t.value}' for t in audit_table.tags]}")

    # =========================================================================
    # STEP 5: Validate Against Convention Rules
    # =========================================================================

    print("\n5. Validating against convention rules...")

    # Validate catalog
    catalog_errors = finance_convention.validate(catalog)
    print(f"   Catalog validation: {'PASS' if not catalog_errors else 'FAIL: ' + str(catalog_errors)}")

    # Validate table
    table_errors = finance_convention.validate(table)
    print(f"   Table validation: {'PASS' if not table_errors else 'FAIL: ' + str(table_errors)}")

    # Create a table missing required tag to show validation failure
    bad_table = Table(
        name="missing_owner",
        table_type=TableType.MANAGED,
        columns=[ColumnInfo(name="id", type_name="INT")],
        # Missing data_owner tag!
    )
    bad_table_errors = finance_convention.validate(bad_table)
    print(f"   Bad table validation: {'PASS' if not bad_table_errors else 'FAIL: ' + str(bad_table_errors)}")

    # =========================================================================
    # STEP 6: Convention Can Be Applied at Any Level
    # =========================================================================

    print("\n6. Convention can be applied at any level...")

    # Create a standalone schema and apply convention directly
    standalone_schema = Schema(
        name="standalone",
        tags=[Tag(key="cost_center", value="finance-003")]
    )
    standalone_table = Table(
        name="data",
        table_type=TableType.MANAGED,
        columns=[ColumnInfo(name="value", type_name="STRING")],
        tags=[Tag(key="data_owner", value="shared")]
    )
    standalone_schema.add_table(standalone_table)

    # Apply convention at schema level
    standalone_schema.with_convention(finance_convention)

    print(f"   Standalone schema tags: {[f'{t.key}={t.value}' for t in standalone_schema.tags]}")
    print(f"   Standalone table tags: {[f'{t.key}={t.value}' for t in standalone_table.tags]}")

    # =========================================================================
    # STEP 7: Interoperability with GovernanceDefaults
    # =========================================================================

    print("\n7. Convention is interoperable with GovernanceDefaults...")

    # Convert to GovernanceDefaults for backward compatibility
    as_defaults = finance_convention.to_governance_defaults()
    print(f"   Convention as GovernanceDefaults: {type(as_defaults).__name__}")
    print(f"   Default tags count: {len(as_defaults.default_tags)}")

    print("\n" + "=" * 60)
    print("Convention pattern successfully demonstrated!")
    print("=" * 60)


if __name__ == "__main__":
    main()
