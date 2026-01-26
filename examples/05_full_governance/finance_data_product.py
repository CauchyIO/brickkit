# Databricks notebook source
# MAGIC %md
# MAGIC # Finance Data Product: End-to-End Governance Example
# MAGIC
# MAGIC This notebook demonstrates a complete governance implementation for a Finance domain
# MAGIC data product team using brickkit. It implements patterns from the governance documentation:
# MAGIC
# MAGIC **Patterns Demonstrated:**
# MAGIC - **Physical Segregation** (DATA_GOVERNANCE_PRINCIPLES.md §6.1): Data classified by sensitivity
# MAGIC - **Zone Progression** (OPERATIONAL_GOVERNANCE_PATTERNS.md §1.2): Bronze → Silver → Gold
# MAGIC - **Mixed Sensitivity Layering** (OPERATIONAL_GOVERNANCE_PATTERNS.md §1.3): PII masking through zones
# MAGIC - **Producer-Consumer Model** (OPERATIONAL_GOVERNANCE_PATTERNS.md §2.1): Team ownership boundaries
# MAGIC - **Interface Contract** (OPERATIONAL_GOVERNANCE_PATTERNS.md §2.2): SLA definitions
# MAGIC
# MAGIC **Assets Created:**
# MAGIC - Finance catalog with Bronze/Silver/Gold schemas
# MAGIC - Tables with PII propagation pattern
# MAGIC - ML Model (fraud detection)
# MAGIC - Vector Search Index (document search)
# MAGIC - Genie Space (finance analytics assistant)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Imports and Path Configuration

# COMMAND ----------

import sys
from pathlib import Path

# Add brickkit src to path (adjust based on your installation)
# In production, you would: pip install brickkit
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()  # noqa: F821
repo_root = "/Workspace" + "/".join(notebook_path.split("/")[:-3])
sys.path.insert(0, f"{repo_root}/src")

# COMMAND ----------

from typing import List, Optional

from models.base import Tag, get_current_environment
from models.enums import SecurableType, Environment
from models.table_models import Column, Table
from models.securables import Catalog, Schema
from models.ml_models import RegisteredModel
from models.access import Principal, AccessPolicy
from brickkit.defaults import GovernanceDefaults, TagDefault, RequiredTag, NamingConvention

# Genie and Vector Search
from genie.models import GenieSpace, SerializedSpace, DataSources, TableDataSource, ColumnConfig, Instructions
from vector_search.models import VectorSearchEndpoint, VectorSearchIndex, VectorIndexType

print(f"Current Environment: {get_current_environment()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. Governance Policy Definition
# MAGIC
# MAGIC Based on **DATA_GOVERNANCE_PRINCIPLES.md**:
# MAGIC - §6.1: Sensitivity Classification (Internal/Confidential/Restricted)
# MAGIC - §6.2: Regulatory Classifications (PII, SOX)
# MAGIC - §6.3: Business Criticality Tiers
# MAGIC - §6.4: Temporal Properties (retention)
# MAGIC
# MAGIC This policy enforces SOX compliance for financial data.

# COMMAND ----------


class FinanceGovernancePolicy(GovernanceDefaults):
    """
    Finance domain governance policy.

    Implements SOX and GDPR compliance requirements:
    - All assets must declare data classification
    - Tables must declare PII status
    - Retention periods must be defined
    - Full audit trail via tags

    Reference: DATA_GOVERNANCE_PRINCIPLES.md §6, §8.3
    """

    @property
    def default_tags(self) -> List[TagDefault]:
        """Tags automatically applied to all securables."""
        return [
            # Infrastructure tracking
            TagDefault(key="managed_by", value="brickkit"),
            TagDefault(key="domain", value="finance"),
            # Compliance framework
            TagDefault(key="compliance_framework", value="sox"),
            # Environment-aware tags
            TagDefault(
                key="environment",
                value="development",
                environment_values={
                    "DEV": "development",
                    "ACC": "acceptance",
                    "PRD": "production",
                },
            ),
            # Audit requirement (SOX)
            TagDefault(key="audit_enabled", value="true"),
            # Default retention (overridden per zone)
            TagDefault(
                key="retention_days",
                value="90",  # DEV: 90 days
                environment_values={
                    "ACC": "180",  # ACC: 6 months
                    "PRD": "2555",  # PRD: 7 years (SOX requirement)
                },
            ),
        ]

    @property
    def required_tags(self) -> List[RequiredTag]:
        """Tags that MUST be present on securables."""
        return [
            # All securables must have data classification
            # Reference: DATA_GOVERNANCE_PRINCIPLES.md §6.1
            RequiredTag(
                key="data_classification",
                allowed_values={"public", "internal", "confidential", "restricted"},
                error_message="All assets must declare data_classification (public/internal/confidential/restricted)",
            ),
            # Catalogs and schemas need owner for accountability
            # Reference: DATA_GOVERNANCE_PRINCIPLES.md §3.1 (Data Owner role)
            RequiredTag(
                key="data_owner",
                applies_to={"CATALOG", "SCHEMA"},
                error_message="Catalogs and schemas must have a data_owner for accountability",
            ),
            # Tables must declare PII status
            # Reference: DATA_GOVERNANCE_PRINCIPLES.md §6.2
            RequiredTag(
                key="pii",
                allowed_values={"true", "false"},
                applies_to={"TABLE"},
                error_message="Tables must declare pii=true or pii=false",
            ),
            # Tables must declare retention
            # Reference: DATA_GOVERNANCE_PRINCIPLES.md §6.4
            RequiredTag(
                key="retention_days",
                applies_to={"TABLE"},
                error_message="Tables must declare retention_days for lifecycle management",
            ),
            # Cost center for chargeback (SOX)
            RequiredTag(
                key="cost_center",
                applies_to={"CATALOG"},
                error_message="Catalogs must have cost_center for financial reporting",
            ),
        ]

    @property
    def naming_conventions(self) -> List[NamingConvention]:
        """Naming rules for securables."""
        return [
            # Catalogs: domain_function format
            NamingConvention(
                pattern=r"^[a-z]+(_[a-z][a-z0-9_]*)?$",
                applies_to={"CATALOG"},
                error_message="Catalog names must be lowercase (e.g., 'finance', 'finance_reporting')",
            ),
            # Schemas: zone or function name
            NamingConvention(
                pattern=r"^[a-z][a-z0-9_]*$",
                applies_to={"SCHEMA"},
                error_message="Schema names must be lowercase with underscores",
            ),
        ]

    @property
    def default_owner(self) -> Optional[str]:
        """Default owner for securables without explicit owner."""
        return "finance-data-team"


# Instantiate the policy
governance_policy = FinanceGovernancePolicy()
print("Finance Governance Policy loaded")
print(f"  Required tags: {[r.key for r in governance_policy.required_tags]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Catalog and Schema Definition
# MAGIC
# MAGIC Based on **OPERATIONAL_GOVERNANCE_PATTERNS.md §1.2**: Zone Progression Pattern
# MAGIC
# MAGIC ```
# MAGIC finance_catalog
# MAGIC ├── bronze/   (Raw - Data Engineers only)
# MAGIC ├── silver/   (Cleansed - Analysts + Engineers)
# MAGIC └── gold/     (Business-Ready - All finance users)
# MAGIC ```

# COMMAND ----------

# Create the Finance catalog
finance_catalog = Catalog(
    name="finance",
    comment="Finance domain data products - SOX compliant",
    tags=[
        Tag(key="data_owner", value="cfo-office"),
        Tag(key="cost_center", value="CC-FIN-001"),
        Tag(key="data_classification", value="confidential"),
        Tag(key="business_criticality", value="tier_1"),
    ],
)

# Apply governance defaults
finance_catalog.with_defaults(governance_policy)

print(f"Catalog: {finance_catalog.resolved_name}")
print("Tags:")
for tag in sorted(finance_catalog.tags, key=lambda t: t.key):
    print(f"  {tag.key}: {tag.value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Zone Schemas
# MAGIC
# MAGIC Each zone has different:
# MAGIC - **Quality guarantees** (none → enforced → SLA)
# MAGIC - **Access policies** (engineers → analysts → business)
# MAGIC - **Retention periods** (30 days → 1 year → 7 years)
# MAGIC
# MAGIC Reference: OPERATIONAL_GOVERNANCE_PATTERNS.md §1.2

# COMMAND ----------

# Bronze Schema: Raw data landing zone
# Access: Data Engineers only
# Quality: No guarantees, may have errors
bronze_schema = Schema(
    name="bronze",
    comment="Raw data exactly as received from source systems",
    tags=[
        Tag(key="data_owner", value="finance-data-engineers"),
        Tag(key="data_classification", value="confidential"),
        Tag(key="zone", value="bronze"),
        Tag(key="quality_level", value="none"),
        Tag(key="retention_days", value="90"),
    ],
)

# Silver Schema: Cleansed and validated
# Access: Analysts + Engineers
# Quality: Enforced constraints, deduplicated
silver_schema = Schema(
    name="silver",
    comment="Cleansed, validated, and deduplicated data",
    tags=[
        Tag(key="data_owner", value="finance-data-engineers"),
        Tag(key="data_classification", value="confidential"),
        Tag(key="zone", value="silver"),
        Tag(key="quality_level", value="enforced"),
        Tag(key="retention_days", value="365"),
    ],
)

# Gold Schema: Business-ready aggregates
# Access: All finance users
# Quality: SLA guaranteed, monitored
gold_schema = Schema(
    name="gold",
    comment="Business-ready aggregates, metrics, and features",
    tags=[
        Tag(key="data_owner", value="finance-analytics-team"),
        Tag(key="data_classification", value="internal"),  # Aggregates are less sensitive
        Tag(key="zone", value="gold"),
        Tag(key="quality_level", value="sla_guaranteed"),
        Tag(key="sla_freshness", value="1_hour"),
        Tag(key="retention_days", value="2555"),  # 7 years for SOX
    ],
)

# Add schemas to catalog
finance_catalog.add_schema(bronze_schema)
finance_catalog.add_schema(silver_schema)
finance_catalog.add_schema(gold_schema)

print("Schemas created:")
for schema in finance_catalog.schemas:
    zone_tag = next((t.value for t in schema.tags if t.key == "zone"), "N/A")
    quality = next((t.value for t in schema.tags if t.key == "quality_level"), "N/A")
    print(f"  {schema.name}: zone={zone_tag}, quality={quality}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Table Definitions with PII Propagation
# MAGIC
# MAGIC Based on **OPERATIONAL_GOVERNANCE_PATTERNS.md §1.3**: Mixed Sensitivity Layering Pattern
# MAGIC
# MAGIC ```
# MAGIC Bronze (Full PII) → Silver (Masked PII) → Gold (No PII)
# MAGIC ```
# MAGIC
# MAGIC This pattern ensures:
# MAGIC - Raw PII preserved for compliance/audit in Bronze
# MAGIC - Masked/tokenized versions available in Silver
# MAGIC - Only aggregates (no individual records) in Gold

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Bronze: Raw Transactions (Contains PII)

# COMMAND ----------

# Bronze: Raw transaction data with full PII
# This is the "base table" in the Mixed Sensitivity Layering Pattern
transactions_raw = Table(
    name="transactions_raw",
    catalog_name="finance",
    schema_name="bronze",
    description="Raw transaction data from payment processing system. Contains PII.",
    enable_scd2=False,  # Raw data, no history tracking yet
    columns=[
        # Primary key
        Column(
            name="transaction_id",
            data_type="STRING",
            nullable=False,
            is_primary_key=True,
            description="Unique transaction identifier",
            tags=[Tag(key="pii", value="false")],
        ),
        # PII Columns - Must be tagged for compliance
        Column(
            name="customer_id",
            data_type="STRING",
            nullable=False,
            description="Customer identifier - links to customer PII",
            tags=[
                Tag(key="pii", value="true"),
                Tag(key="pii_type", value="indirect_identifier"),
            ],
        ),
        Column(
            name="account_number",
            data_type="STRING",
            nullable=False,
            description="Bank account number",
            tags=[
                Tag(key="pii", value="true"),
                Tag(key="pii_type", value="financial"),
                Tag(key="gdpr_sensitive", value="true"),
            ],
        ),
        Column(
            name="customer_email",
            data_type="STRING",
            nullable=True,
            description="Customer email address",
            tags=[
                Tag(key="pii", value="true"),
                Tag(key="pii_type", value="contact"),
                Tag(key="gdpr_sensitive", value="true"),
            ],
        ),
        # Non-PII transaction data
        Column(
            name="amount",
            data_type="DECIMAL(18,2)",
            nullable=False,
            description="Transaction amount",
            tags=[Tag(key="pii", value="false")],
        ),
        Column(
            name="currency",
            data_type="STRING",
            nullable=False,
            description="ISO currency code",
            tags=[Tag(key="pii", value="false")],
        ),
        Column(
            name="transaction_type",
            data_type="STRING",
            nullable=False,
            description="Type: CREDIT, DEBIT, TRANSFER",
            tags=[Tag(key="pii", value="false")],
        ),
        Column(
            name="merchant_name",
            data_type="STRING",
            nullable=True,
            description="Merchant/payee name",
            tags=[Tag(key="pii", value="false")],
        ),
        Column(
            name="transaction_timestamp",
            data_type="TIMESTAMP",
            nullable=False,
            description="When transaction occurred",
            tags=[Tag(key="pii", value="false")],
        ),
        Column(
            name="raw_payload",
            data_type="STRING",
            nullable=True,
            description="Original JSON payload from source",
            tags=[
                Tag(key="pii", value="true"),
                Tag(key="pii_type", value="unknown"),  # May contain PII
            ],
        ),
    ],
    tags=[
        Tag(key="data_classification", value="restricted"),  # Contains PII
        Tag(key="pii", value="true"),
        Tag(key="source_system", value="payment_gateway"),
        Tag(key="ingestion_method", value="streaming"),
        Tag(key="retention_days", value="90"),
    ],
)

print(f"Bronze Table: {transactions_raw.fqdn}")
print(f"PII Columns: {[c.name for c in transactions_raw.get_pii_columns()]}")
print(f"Total Columns: {len(transactions_raw.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Silver: Validated Transactions (Masked PII)

# COMMAND ----------

# Silver: Cleansed transactions with masked PII
# This is the "masked view" in the Mixed Sensitivity Layering Pattern
transactions_validated = Table(
    name="transactions_validated",
    catalog_name="finance",
    schema_name="silver",
    description="Validated and cleansed transactions with masked PII for analyst access.",
    enable_scd2=True,  # Track changes for audit
    columns=[
        # Primary key (same)
        Column(
            name="transaction_id",
            data_type="STRING",
            nullable=False,
            is_primary_key=True,
            description="Unique transaction identifier",
            tags=[Tag(key="pii", value="false")],
        ),
        # Masked PII - Derived from bronze
        Column(
            name="customer_id_hash",
            data_type="STRING",
            nullable=False,
            description="SHA-256 hash of customer_id for joining without exposing PII",
            tags=[
                Tag(key="pii", value="false"),  # Hash is not PII
                Tag(key="derived_from", value="customer_id"),
                Tag(key="masking_method", value="sha256"),
            ],
        ),
        Column(
            name="account_number_masked",
            data_type="STRING",
            nullable=False,
            description="Masked account: ****1234 (last 4 digits only)",
            tags=[
                Tag(key="pii", value="false"),  # Masked is not PII
                Tag(key="derived_from", value="account_number"),
                Tag(key="masking_method", value="last_4_digits"),
            ],
        ),
        Column(
            name="email_domain",
            data_type="STRING",
            nullable=True,
            description="Email domain only (e.g., gmail.com) - no username",
            tags=[
                Tag(key="pii", value="false"),
                Tag(key="derived_from", value="customer_email"),
                Tag(key="masking_method", value="domain_only"),
            ],
        ),
        # Non-PII transaction data (validated)
        Column(
            name="amount",
            data_type="DECIMAL(18,2)",
            nullable=False,
            description="Validated transaction amount",
            tags=[Tag(key="pii", value="false")],
        ),
        Column(
            name="currency",
            data_type="STRING",
            nullable=False,
            description="Validated ISO currency code",
            tags=[Tag(key="pii", value="false")],
        ),
        Column(
            name="transaction_type",
            data_type="STRING",
            nullable=False,
            description="Validated type: CREDIT, DEBIT, TRANSFER",
            tags=[Tag(key="pii", value="false")],
        ),
        Column(
            name="merchant_category",
            data_type="STRING",
            nullable=True,
            description="Standardized merchant category code",
            tags=[Tag(key="pii", value="false")],
        ),
        Column(
            name="transaction_date",
            data_type="DATE",
            nullable=False,
            description="Transaction date (derived from timestamp)",
            tags=[Tag(key="pii", value="false")],
        ),
        Column(
            name="is_valid",
            data_type="BOOLEAN",
            nullable=False,
            description="Passed all validation rules",
            tags=[Tag(key="pii", value="false")],
        ),
        Column(
            name="validation_errors",
            data_type="ARRAY<STRING>",
            nullable=True,
            description="List of validation errors if any",
            tags=[Tag(key="pii", value="false")],
        ),
    ],
    tags=[
        Tag(key="data_classification", value="confidential"),  # Masked, not restricted
        Tag(key="pii", value="false"),  # No PII after masking
        Tag(key="source_table", value="finance.bronze.transactions_raw"),
        Tag(key="retention_days", value="365"),
    ],
)

print(f"Silver Table: {transactions_validated.fqdn}")
print(f"PII Columns: {[c.name for c in transactions_validated.get_pii_columns()]}")  # Should be empty
print(f"Masked Columns: {[c.name for c in transactions_validated.columns if c.get_tag('derived_from')]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Gold: Revenue Metrics (No PII - Aggregates Only)

# COMMAND ----------

# Gold: Business-ready aggregates
# This is the "aggregated view" in the Mixed Sensitivity Layering Pattern
# No individual records, no PII - only metrics
daily_revenue_metrics = Table(
    name="daily_revenue_metrics",
    catalog_name="finance",
    schema_name="gold",
    description="Daily revenue aggregates by category. No PII, safe for broad access.",
    enable_scd2=False,  # Aggregates are recalculated, not tracked
    columns=[
        # Grain: Date + Category
        Column(
            name="metric_date",
            data_type="DATE",
            nullable=False,
            is_primary_key=True,
            description="Date of the metrics",
            tags=[Tag(key="pii", value="false")],
        ),
        Column(
            name="merchant_category",
            data_type="STRING",
            nullable=False,
            description="Merchant category for aggregation",
            tags=[Tag(key="pii", value="false")],
        ),
        Column(
            name="currency",
            data_type="STRING",
            nullable=False,
            description="Currency of the metrics",
            tags=[Tag(key="pii", value="false")],
        ),
        # Aggregate metrics - no individual data
        Column(
            name="transaction_count",
            data_type="BIGINT",
            nullable=False,
            description="Number of transactions",
            tags=[Tag(key="pii", value="false")],
        ),
        Column(
            name="total_revenue",
            data_type="DECIMAL(18,2)",
            nullable=False,
            description="Sum of credit transactions",
            tags=[Tag(key="pii", value="false")],
        ),
        Column(
            name="total_expenses",
            data_type="DECIMAL(18,2)",
            nullable=False,
            description="Sum of debit transactions",
            tags=[Tag(key="pii", value="false")],
        ),
        Column(
            name="net_amount",
            data_type="DECIMAL(18,2)",
            nullable=False,
            description="Revenue minus expenses",
            tags=[Tag(key="pii", value="false")],
        ),
        Column(
            name="avg_transaction_amount",
            data_type="DECIMAL(18,2)",
            nullable=False,
            description="Average transaction size",
            tags=[Tag(key="pii", value="false")],
        ),
        Column(
            name="unique_customers",
            data_type="BIGINT",
            nullable=False,
            description="Count of unique customer hashes (not identifiable)",
            tags=[Tag(key="pii", value="false")],
        ),
        # Quality metadata
        Column(
            name="data_quality_score",
            data_type="DECIMAL(5,2)",
            nullable=False,
            description="Percentage of valid source records",
            tags=[Tag(key="pii", value="false")],
        ),
        Column(
            name="last_updated",
            data_type="TIMESTAMP",
            nullable=False,
            description="When metrics were last calculated",
            tags=[Tag(key="pii", value="false")],
        ),
    ],
    tags=[
        Tag(key="data_classification", value="internal"),  # Safe for broad access
        Tag(key="pii", value="false"),
        Tag(key="aggregation_grain", value="daily,category,currency"),
        Tag(key="source_table", value="finance.silver.transactions_validated"),
        Tag(key="retention_days", value="2555"),  # 7 years SOX
        Tag(key="sla_freshness", value="1_hour"),
    ],
)

print(f"Gold Table: {daily_revenue_metrics.fqdn}")
print(f"PII Columns: {[c.name for c in daily_revenue_metrics.get_pii_columns()]}")  # Should be empty
print(f"Metric Columns: {len(daily_revenue_metrics.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### PII Propagation Summary

# COMMAND ----------

# Visualize the PII propagation pattern
print("=" * 60)
print("PII PROPAGATION THROUGH ZONES")
print("Reference: OPERATIONAL_GOVERNANCE_PATTERNS.md §1.3")
print("=" * 60)

tables = [
    ("BRONZE (Raw)", transactions_raw),
    ("SILVER (Masked)", transactions_validated),
    ("GOLD (Aggregates)", daily_revenue_metrics),
]

for zone_name, table in tables:
    pii_cols = table.get_pii_columns()
    classification = table.get_tag("data_classification")
    print(f"\n{zone_name}: {table.name}")
    print(f"  Classification: {classification}")
    print(f"  Contains PII: {table.get_tag('pii')}")
    print(f"  PII Columns: {[c.name for c in pii_cols] if pii_cols else 'None'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. ML Model: Fraud Detection
# MAGIC
# MAGIC Based on **OPERATIONAL_GOVERNANCE_PATTERNS.md §5.1**: Experiment to Production Pattern
# MAGIC
# MAGIC ML models require governance for:
# MAGIC - Lineage tracking (what data was used)
# MAGIC - Version management
# MAGIC - Production deployment approval

# COMMAND ----------

# ML Model: Fraud Detection
# Lives in the gold schema (production models)
fraud_detection_model = RegisteredModel(
    name="fraud_detection",
    catalog_name="finance",
    schema_name="gold",
    comment="Real-time fraud detection model for transaction scoring",
    description="""
    XGBoost-based fraud detection model trained on historical transaction patterns.

    Features:
    - Transaction amount, frequency, merchant category
    - Customer behavior patterns (from silver layer)
    - Time-based features (hour, day of week)

    Performance:
    - Precision: 0.94
    - Recall: 0.87
    - F1: 0.90

    Training Data: finance.silver.transactions_validated (2023-01-01 to 2024-01-01)
    """,
    aliases={
        "champion": 5,  # Current production model
        "challenger": 6,  # A/B testing candidate
    },
    tags=[
        Tag(key="data_classification", value="confidential"),
        Tag(key="model_type", value="classification"),
        Tag(key="framework", value="xgboost"),
        Tag(key="training_data", value="finance.silver.transactions_validated"),
        Tag(key="pii_in_features", value="false"),  # Uses masked data only
        Tag(key="bias_tested", value="true"),
        Tag(key="explainability", value="shap"),
        Tag(key="tier", value="production"),  # Reference: ML Lifecycle stages
    ],
)

print(f"ML Model: {fraud_detection_model.fqdn}")
print(f"Aliases: {fraud_detection_model.aliases}")
print(f"Tags: {[(t.key, t.value) for t in fraud_detection_model.tags[:5]]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Vector Search Index: Document Search
# MAGIC
# MAGIC Vector search enables semantic search over financial documents
# MAGIC (policies, contracts, compliance documents).

# COMMAND ----------

# Vector Search Endpoint
finance_search_endpoint = VectorSearchEndpoint(
    name="finance_search",
    comment="Vector search endpoint for finance document retrieval",
    tags=[
        Tag(key="data_classification", value="confidential"),
        Tag(key="domain", value="finance"),
        Tag(key="use_case", value="document_search"),
    ],
)

# Vector Search Index: Compliance Documents
compliance_docs_index = VectorSearchIndex(
    name="compliance_documents",
    endpoint_name="finance_search",
    source_table="finance.gold.compliance_documents",  # Source table with embeddings
    primary_key="document_id",
    embedding_column="embedding",
    index_type=VectorIndexType.DELTA_SYNC,
    comment="Semantic search over SOX compliance documents",
    tags=[
        Tag(key="data_classification", value="confidential"),
        Tag(key="embedding_model", value="text-embedding-ada-002"),
        Tag(key="embedding_dimensions", value="1536"),
        Tag(key="use_case", value="compliance_search"),
        Tag(key="pii", value="false"),
    ],
)

print(f"Vector Search Endpoint: {finance_search_endpoint.name}")
print(f"Vector Search Index: {compliance_docs_index.name}")
print(f"  Source: {compliance_docs_index.source_table}")
print(f"  Type: {compliance_docs_index.index_type.value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 6. Genie Space: Finance Analytics Assistant
# MAGIC
# MAGIC AI-powered natural language interface for finance analytics.
# MAGIC Provides governed access to gold-layer metrics.

# COMMAND ----------

# Genie Space: Finance Analytics
finance_genie_space = GenieSpace(
    name="finance_analytics",
    title="Finance Analytics Assistant",
    description="""
    AI-powered analytics assistant for the Finance team.

    Capabilities:
    - Query daily revenue metrics
    - Analyze transaction patterns
    - Generate financial reports

    Data Access: Gold layer only (no PII)
    """,
    warehouse_id="abc123def456",  # Replace with actual warehouse ID
    tags=[
        Tag(key="data_classification", value="internal"),
        Tag(key="domain", value="finance"),
        Tag(key="pii_access", value="false"),  # Only accesses gold layer
    ],
    serialized_space=SerializedSpace(
        data_sources=DataSources(
            tables=[
                TableDataSource(
                    identifier="finance.gold.daily_revenue_metrics",
                    column_configs=[
                        ColumnConfig(column_name="metric_date", get_example_values=True),
                        ColumnConfig(column_name="merchant_category", build_value_dictionary=True),
                        ColumnConfig(column_name="currency", build_value_dictionary=True),
                        ColumnConfig(column_name="total_revenue", get_example_values=False),
                        ColumnConfig(column_name="transaction_count", get_example_values=False),
                    ],
                ),
            ]
        ),
        instructions=Instructions(
            general_instructions="""
            You are a Finance Analytics Assistant. Help users analyze revenue metrics
            and transaction patterns. Always specify the date range and currency when
            answering questions about financial metrics.

            Important:
            - This data is aggregated and contains no PII
            - Revenue figures are in the original currency
            - Data freshness SLA is 1 hour
            """,
        ),
    ),
)

print(f"Genie Space: {finance_genie_space.title}")
print(f"  Tables: {[t.identifier for t in finance_genie_space.serialized_space.data_sources.tables]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 7. Access Control: Team-Based Policies
# MAGIC
# MAGIC Based on **OPERATIONAL_GOVERNANCE_PATTERNS.md §2.1**: Producer-Consumer Model
# MAGIC
# MAGIC | Zone | Data Engineers | Analysts | Business Users |
# MAGIC |------|---------------|----------|----------------|
# MAGIC | Bronze | WRITER | - | - |
# MAGIC | Silver | WRITER | READER | - |
# MAGIC | Gold | READER | READER | READER |

# COMMAND ----------

# Define principals (teams/groups)
finance_data_engineers = Principal(name="finance_data_engineers")
finance_analysts = Principal(name="finance_analysts")
finance_business_users = Principal(name="finance_business_users")
ml_engineers = Principal(name="ml_engineers")

# Access policies by zone
# Reference: OPERATIONAL_GOVERNANCE_PATTERNS.md §2.1

# Bronze: Data Engineers only (producers)
bronze_schema.grant(finance_data_engineers, AccessPolicy.WRITER())

# Silver: Engineers write, Analysts read
silver_schema.grant(finance_data_engineers, AccessPolicy.WRITER())
silver_schema.grant(finance_analysts, AccessPolicy.READER())

# Gold: Everyone reads (consumers)
gold_schema.grant(finance_data_engineers, AccessPolicy.READER())
gold_schema.grant(finance_analysts, AccessPolicy.READER())
gold_schema.grant(finance_business_users, AccessPolicy.READER())

# ML Model: ML Engineers can manage, others can use
fraud_detection_model.grant(ml_engineers, AccessPolicy.WRITER())
fraud_detection_model.grant(finance_analysts, AccessPolicy.READER())

print("Access Policies Applied:")
print("\nBronze Schema:")
for priv in bronze_schema.privileges[:3]:
    print(f"  {priv.principal}: {priv.privilege.value}")

print("\nSilver Schema:")
for priv in silver_schema.privileges[:5]:
    print(f"  {priv.principal}: {priv.privilege.value}")

print("\nGold Schema:")
for priv in gold_schema.privileges[:5]:
    print(f"  {priv.principal}: {priv.privilege.value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 8. Governance Validation
# MAGIC
# MAGIC Validate all assets against the governance policy.
# MAGIC Reference: DATA_GOVERNANCE_PRINCIPLES.md §12 (Metrics and KPIs)

# COMMAND ----------


def validate_asset(asset, asset_type: str, policy: GovernanceDefaults):
    """Validate an asset against governance policy."""
    name = getattr(asset, "name", str(asset))

    # Get tags as dict
    tags_dict = {}
    if hasattr(asset, "tags"):
        tags_dict = {t.key: t.value for t in asset.tags}

    # Get securable type
    securable_type = None
    if hasattr(asset, "securable_type"):
        securable_type = asset.securable_type
    else:
        # Map asset_type string to SecurableType
        type_map = {
            "CATALOG": SecurableType.CATALOG,
            "SCHEMA": SecurableType.SCHEMA,
            "TABLE": SecurableType.TABLE,
            "MODEL": SecurableType.MODEL,
        }
        securable_type = type_map.get(asset_type.upper())

    if securable_type:
        errors = policy.validate_tags(securable_type, tags_dict)
    else:
        errors = []

    return {
        "asset": name,
        "type": asset_type,
        "valid": len(errors) == 0,
        "errors": errors,
    }


# COMMAND ----------

# Validate all assets
all_assets = [
    (finance_catalog, "CATALOG"),
    (bronze_schema, "SCHEMA"),
    (silver_schema, "SCHEMA"),
    (gold_schema, "SCHEMA"),
    (transactions_raw, "TABLE"),
    (transactions_validated, "TABLE"),
    (daily_revenue_metrics, "TABLE"),
    (fraud_detection_model, "MODEL"),
]

print("=" * 60)
print("GOVERNANCE VALIDATION REPORT")
print("=" * 60)

all_valid = True
for asset, asset_type in all_assets:
    result = validate_asset(asset, asset_type, governance_policy)
    status = "✓ PASS" if result["valid"] else "✗ FAIL"
    print(f"\n{status} {result['type']}: {result['asset']}")
    if result["errors"]:
        all_valid = False
        for err in result["errors"]:
            print(f"    - {err}")

print("\n" + "=" * 60)
print(f"OVERALL: {'ALL ASSETS COMPLIANT' if all_valid else 'VALIDATION FAILURES DETECTED'}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 9. SQL Generation Preview
# MAGIC
# MAGIC Preview the SQL statements that would be generated for governance.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table Creation SQL

# COMMAND ----------

print("=" * 60)
print("CREATE TABLE STATEMENTS")
print("=" * 60)

for table in [transactions_raw, transactions_validated, daily_revenue_metrics]:
    print(f"\n-- {table.fqdn}")
    print(table.create_table_statement())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tag Application SQL

# COMMAND ----------

print("=" * 60)
print("ALTER TABLE SET TAGS STATEMENTS")
print("=" * 60)

for table in [transactions_raw, transactions_validated, daily_revenue_metrics]:
    print(f"\n-- Tags for {table.fqdn}")
    for stmt in table.alter_tag_statements():
        print(stmt)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Summary
# MAGIC
# MAGIC This notebook demonstrated a complete governance implementation:
# MAGIC
# MAGIC | Component | Pattern | Reference |
# MAGIC |-----------|---------|-----------|
# MAGIC | `FinanceGovernancePolicy` | Required tags, naming conventions | DATA_GOVERNANCE_PRINCIPLES.md §6 |
# MAGIC | Zone schemas | Zone Progression (Bronze/Silver/Gold) | OPERATIONAL_GOVERNANCE_PATTERNS.md §1.2 |
# MAGIC | PII handling | Mixed Sensitivity Layering | OPERATIONAL_GOVERNANCE_PATTERNS.md §1.3 |
# MAGIC | Team access | Producer-Consumer Model | OPERATIONAL_GOVERNANCE_PATTERNS.md §2.1 |
# MAGIC | ML Model | Experiment to Production | OPERATIONAL_GOVERNANCE_PATTERNS.md §5.1 |
# MAGIC
# MAGIC **Key Takeaways:**
# MAGIC 1. Governance policies are code - version controlled, testable, auditable
# MAGIC 2. PII should be progressively masked through data zones
# MAGIC 3. Access control follows the producer-consumer model
# MAGIC 4. All assets can be validated against governance rules before deployment
