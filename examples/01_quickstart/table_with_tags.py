# Databricks notebook source
"""
Table with Tags Example

Shows how to define tables and columns with governance tags.
Demonstrates:
- Column-level PII tagging
- Table-level governance tags
- SCD2 column support
- SQL generation with tags
- GovernanceDefaults integration
"""

from loguru import logger

from brickkit.defaults import GovernanceDefaults, RequiredTag, TagDefault
from brickkit.models.base import Tag, get_current_environment, init_environment
from brickkit.models.tables import Column, Table

# COMMAND ----------

env = init_environment()

# =============================================================================
# Define governance defaults
# =============================================================================


class DataGovernanceDefaults(GovernanceDefaults):
    """Organization-wide data governance policies."""

    @property
    def default_tags(self):
        return [
            TagDefault(key="managed_by", value="brickkit"),
            TagDefault(key="environment", value="dev", environment_values={"PRD": "prod", "ACC": "acc"}),
        ]

    @property
    def required_tags(self):
        return [
            # All tables must declare PII status
            RequiredTag(
                key="pii",
                allowed_values={"true", "false"},
                applies_to={"TABLE"},
                error_message="Tables must declare pii=true or pii=false",
            ),
            # Tables must have a data owner
            RequiredTag(key="data_owner", applies_to={"TABLE"}, error_message="Tables must have a data_owner tag"),
        ]


# COMMAND ----------

# =============================================================================
# Define table with columns
# =============================================================================

# Define columns with tags
columns = [
    Column(
        name="customer_id",
        data_type="BIGINT",
        nullable=False,
        is_primary_key=True,
        description="Unique customer identifier",
        tags=[
            Tag(key="pii", value="false"),
            Tag(key="source_system", value="crm"),
        ],
    ),
    Column(
        name="email",
        data_type="STRING",
        nullable=True,
        description="Customer email address",
        tags=[
            Tag(key="pii", value="true"),
            Tag(key="gdpr_sensitive", value="true"),
        ],
    ),
    Column(
        name="phone",
        data_type="STRING",
        nullable=True,
        description="Customer phone number",
        tags=[
            Tag(key="pii", value="true"),
        ],
    ),
    Column(
        name="segment",
        data_type="STRING",
        nullable=True,
        description="Customer segment",
        tags=[
            Tag(key="pii", value="false"),
        ],
    ),
]

# Create table with SCD2 support
customers_table = Table(
    name="customers",
    catalog_name="crm",
    schema_name="silver",
    description="Customer master data with history tracking",
    columns=columns,
    enable_scd2=True,  # Adds SCD2 tracking columns
    tags=[
        Tag(key="data_owner", value="crm-team"),
        Tag(key="pii", value="true"),  # Table contains PII
        Tag(key="retention_days", value="2555"),
    ],
)

# Apply governance defaults
defaults = DataGovernanceDefaults()
customers_table.with_defaults(defaults)

# COMMAND ----------

# =============================================================================
# Display table info
# =============================================================================

logger.info(f"Environment: {get_current_environment()}")
logger.info(f"Table FQDN: {customers_table.fqdn}")
logger.info(f"Primary key: {customers_table.primary_key_column}")

logger.info("\n--- Table Tags ---")
for tag in customers_table.tags:
    logger.info(f"  {tag.key}: {tag.value}")

logger.info("\n--- PII Columns ---")
for col in customers_table.get_pii_columns():
    logger.info(f"  {col.name}: {col.description}")

logger.info("\n--- All Columns (including SCD2) ---")
for col in customers_table.all_columns:
    pii_tag = col.get_tag("pii") or "N/A"
    logger.info(f"  {col.name}: {col.data_type} (PII: {pii_tag})")

# COMMAND ----------

# =============================================================================
# Generate SQL statements
# =============================================================================

logger.info("\n--- CREATE TABLE Statement ---")
logger.info(customers_table.create_table_statement())

logger.info("\n--- ALTER TABLE SET TAGS Statements ---")
for stmt in customers_table.alter_tag_statements():
    logger.info(stmt)

# COMMAND ----------

# =============================================================================
# Validate governance
# =============================================================================

logger.info("\n--- Governance Validation ---")
errors = customers_table.validate_governance(defaults)
logger.info(f"Validation: {'PASSED' if not errors else 'FAILED'}")
for err in errors:
    logger.info(f"  - {err}")

# Output (when DATABRICKS_ENV=dev):
# Environment: DEV
# Table FQDN: crm_dev.silver.customers
# Primary key: customer_id
#
# --- Table Tags ---
#   data_owner: crm-team
#   pii: true
#   retention_days: 2555
#   managed_by: brickkit
#   environment: dev
#
# --- PII Columns ---
#   email: Customer email address
#   phone: Customer phone number
# ...
