# Databricks notebook source
# MAGIC %md
# MAGIC # Vector Search with BrickKit
# MAGIC
# MAGIC This notebook demonstrates **end-to-end usage of BrickKit** for deploying a governed Vector Search solution.
# MAGIC
# MAGIC ## What BrickKit Does
# MAGIC
# MAGIC BrickKit automates governance for Databricks resources:
# MAGIC - **Team management** - Define teams with workspaces and principals, auto-configure catalog bindings
# MAGIC - **Principal management** - Define service principals for ownership (Unity Catalog compatible)
# MAGIC - **Naming conventions** - Environment-aware names (dev/acc/prd suffixes)
# MAGIC - **Tagging** - Automatic cost center, team, compliance tags
# MAGIC - **Ownership rules** - Enforce service principals for all securables
# MAGIC - **Permission grants** - Ensure teams retain access after ownership changes
# MAGIC - **Request for Access (RFA)** - Configure access request destinations with inheritance
# MAGIC - **Validation** - Catch governance violations before deployment
# MAGIC
# MAGIC ## BrickKit vs DAB (Databricks Asset Bundles)
# MAGIC
# MAGIC | Resource | DAB | BrickKit | Notes |
# MAGIC |----------|-----|----------|-------|
# MAGIC | **Teams** | Not supported | Defines & manages | BrickKit organizes workspace + principals |
# MAGIC | **SPNs** | Not supported | Defines & manages | BrickKit defines principals declaratively |
# MAGIC | **Catalog** | References only | Creates & governs | DAB passes variables, BrickKit deploys |
# MAGIC | **Schema** | References only | Creates & governs | Same |
# MAGIC | **Table** | References only | Creates & governs | BrickKit defines structure + tags |
# MAGIC | **VS Endpoint** | Not supported | Creates & governs | DAB can't create these |
# MAGIC | **VS Index** | Not supported | Creates & governs | DAB can't create these |
# MAGIC | **Jobs/Workflows** | Defines | N/A | DAB's strength |
# MAGIC | **Notebook sync** | Deploys | N/A | DAB syncs to workspace |
# MAGIC
# MAGIC **Key insight:** DAB deploys *code assets* (notebooks, jobs). BrickKit deploys *data assets* (catalogs, tables, VS) and *principals* (teams, SPNs).
# MAGIC
# MAGIC ## What This Demo Shows
# MAGIC
# MAGIC 1. Load a governance convention from YAML
# MAGIC 2. **Define Team** with workspace and principals
# MAGIC 3. Define governed resources (Catalog, Schema, Table, VS Endpoint, VS Index)
# MAGIC 4. **Auto-configure workspace bindings** via `team.add_catalog()`
# MAGIC 5. Deploy using BrickKit executors
# MAGIC 6. **Grant permissions** to team groups after ownership change
# MAGIC 7. Test vector search
# MAGIC 8. See what governance BrickKit applied automatically

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. Configuration
# MAGIC
# MAGIC Edit the constants below to customize the deployment. This notebook runs on both Databricks and locally:
# MAGIC
# MAGIC - **Databricks**: Full deployment including table writes and vector search
# MAGIC - **Local**: Model definitions, validation, and executor calls (table writes skipped)

# COMMAND ----------

# MAGIC %pip install --upgrade databricks-vectorsearch databricks-sdk pydantic pyyaml --quiet

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# === CONFIGURATION ===
CATALOG_BASE = "quant_risk"
SCHEMA_NAME = "indicators"
ENDPOINT_NAME = "worldbank_vector_search"
MANAGED_LOCATION = None  # Set if using Default Storage workspaces
ENVIRONMENT = "dev"  # "dev", "acc", or "prd"
DRY_RUN = False

# Workspace config (for workspace bindings)
WORKSPACE_ID = "4188055811360976"  # Your workspace ID
WORKSPACE_NAME = "free-edition-workspace"
WORKSPACE_HOSTNAME = "https://dbc-930eaa5c-35a0.cloud.databricks.com"

# Derived names
TABLE_NAME = "worldbank_indicators"
INDEX_NAME = f"{TABLE_NAME}_index"

print(f"Environment: {ENVIRONMENT}")
print(f"Dry Run: {DRY_RUN}")
print(f"Catalog (base): {CATALOG_BASE}")
print(f"Schema: {SCHEMA_NAME}")
print(f"Endpoint: {ENDPOINT_NAME}")
print(f"Workspace: {WORKSPACE_NAME} (ID: {WORKSPACE_ID})")
if MANAGED_LOCATION:
    print(f"Managed Location: {MANAGED_LOCATION}")

# COMMAND ----------

# === IMPORTS ===
import logging
import sys
import os
from pathlib import Path
from pyspark.sql.types import StructType, StructField, StringType

# Detect environment
IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ

# Add brickkit to path (not yet published to PyPI)
if IS_DATABRICKS:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    notebook_dir = os.path.dirname(notebook_path)
    workspace_notebook_dir = f"/Workspace{notebook_dir}"
    src_path = os.path.abspath(os.path.join(workspace_notebook_dir, "..", "..", "src"))
else:
    # Local: use file path relative to this notebook
    notebook_dir = Path(__file__).parent if "__file__" in dir() else Path.cwd()
    src_path = str(notebook_dir.parent.parent / "src")
    workspace_notebook_dir = str(notebook_dir)  # For convention path

if src_path not in sys.path:
    sys.path.insert(0, src_path)
print(f"Added to sys.path: {src_path}")

# Create SparkSession - different approach for local vs Databricks
if IS_DATABRICKS:
    # On Databricks, spark is already available
    print(f"Using Databricks SparkSession")
else:
    # Local: use databricks-connect with serverless
    # Uses VS Code extension's connection or default profile from ~/.databrickscfg
    from databricks.connect import DatabricksSession
    spark = DatabricksSession.builder.serverless(True).getOrCreate()
    print(f"Connected to Databricks via databricks-connect (serverless)")

from databricks.sdk import WorkspaceClient
from databricks.vector_search.client import VectorSearchClient

# BrickKit imports
from brickkit import (
    Catalog,
    Schema,
    Tag,
    SecurableType,
    VectorSearchEndpoint,
    VectorSearchIndex,
    load_convention,
)
from brickkit.models.tables import Table, ColumnInfo
from brickkit.models.grants import Principal, AccessPolicy
from brickkit.models.principals import ManagedGroup, ManagedServicePrincipal
from brickkit.models.enums import PrincipalType, IsolationMode
from brickkit.models.workspace_bindings import Workspace, WorkspaceRegistry
from brickkit.models.teams import Team
from brickkit.executors import (
    CatalogExecutor,
    SchemaExecutor,
    GrantExecutor,
    VectorSearchEndpointExecutor,
    VectorSearchIndexExecutor,
    ServicePrincipalExecutor,
    get_privileged_client,
)
from brickkit.models.base import set_current_environment
from brickkit.models.enums import Environment

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# Set BrickKit environment
ENV_MAP = {"dev": Environment.DEV, "acc": Environment.ACC, "prd": Environment.PRD}
set_current_environment(ENV_MAP[ENVIRONMENT])

print(f"BrickKit environment set to: {ENVIRONMENT}")

# COMMAND ----------

# === LOAD GOVERNANCE CONVENTION ===
# The convention defines naming patterns, required tags, and ownership rules

if IS_DATABRICKS:
    CONVENTION_PATH = os.path.join(workspace_notebook_dir, "conventions", "financial_services.yml")
else:
    CONVENTION_PATH = str(notebook_dir / "conventions" / "financial_services.yml")

convention = load_convention(CONVENTION_PATH)

print(f"Loaded convention: {convention.name} (v{convention.version})")
print(f"Rules: {len(convention.schema.rules)}")
print(f"Default tags: {len(convention.schema.tags)}")

# Show what the convention enforces
for rule in convention.schema.rules:
    mode = "ENFORCED" if rule.mode.value == "enforced" else "ADVISORY"
    print(f"  [{mode}] {rule.rule}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bootstrap: Admin Service Principal
# MAGIC
# MAGIC Notebook tokens have limited permissions (e.g., cannot update catalog isolation mode).
# MAGIC We create an "admin" service principal with OAuth credentials stored in Databricks Secrets.
# MAGIC
# MAGIC **Important: Grants to Service Principals require `application_id`**
# MAGIC
# MAGIC Databricks grants API requires the `application_id` (UUID) for service principals, not the display name.
# MAGIC BrickKit handles this automatically:
# MAGIC 1. After creating an SPN, the `application_id` is stored on the `ManagedServicePrincipal`
# MAGIC 2. Use `spn.to_principal()` to get a `Principal` with the `application_id` set
# MAGIC 3. The `Principal.resolved_name` returns the `application_id` for grants
# MAGIC
# MAGIC **Bootstrap flow:**
# MAGIC 1. Create admin SPN using your user token
# MAGIC 2. Store SPN credentials in Databricks Secrets
# MAGIC 3. Get Principal with application_id via `admin_spn.to_principal()`
# MAGIC 4. Grant the admin SPN `MANAGE` + `USE_CATALOG` on the catalog
# MAGIC 5. Create privileged client from stored credentials
# MAGIC 6. Use privileged client for catalog operations

# COMMAND ----------

# === BOOTSTRAP: ADMIN SERVICE PRINCIPAL ===
# Creates an admin SPN with OAuth credentials for privileged operations.
# IMPORTANT: For grants to service principals, Databricks requires the application_id (UUID),
# not the display name. After creating the SPN, we use to_principal() to get a Principal
# with the application_id set.

ADMIN_SPN_NAME = "spn_brickkit_admin"
SECRET_SCOPE = "brickkit"

admin_spn = ManagedServicePrincipal(name=ADMIN_SPN_NAME)
admin_spn.add_entitlement("workspace-access")
admin_spn.add_entitlement("databricks-sql-access")

# WorkspaceClient uses VS Code extension's connection or default profile
bootstrap_client = WorkspaceClient()

spn_executor = ServicePrincipalExecutor(bootstrap_client, dry_run=DRY_RUN)

# Check if SPN + credentials already exist
spn_exists = spn_executor.exists(admin_spn)

# Get secrets - different approach for local vs Databricks
def get_secret(scope: str, key: str) -> str | None:
    """Get secret from Databricks or environment variable for local."""
    if IS_DATABRICKS:
        try:
            return dbutils.secrets.get(scope=scope, key=key)
        except Exception:
            return None
    else:
        # Local: try environment variables first, then fall back to SDK
        env_key = f"{scope.upper()}_{key.upper().replace('-', '_')}"
        if env_key in os.environ:
            return os.environ[env_key]
        # Try using the SDK to list secrets (won't get values, but can check existence)
        return None

ADMIN_SPN_APP_ID = get_secret(SECRET_SCOPE, "admin-spn-client-id")

# Create SPN if needed, or just look up existing one
if not spn_exists and not DRY_RUN:
    # SPN doesn't exist - create it with a secret
    print(f"Creating admin SPN: {admin_spn.resolved_name}")
    result, credentials = spn_executor.create_with_secret(admin_spn)
    print(f"  {result.operation.value}: {result.message}")

    if credentials:
        if IS_DATABRICKS:
            spn_executor.store_credentials(credentials, scope=SECRET_SCOPE)
            print(f"  Stored credentials in Databricks secrets")
        else:
            print(f"  NOTE: For SPN-based auth locally, set these env vars:")
            print(f"    export BRICKKIT_ADMIN_SPN_CLIENT_ID={credentials.application_id}")
            print(f"    export BRICKKIT_ADMIN_SPN_CLIENT_SECRET=<secret>")
        ADMIN_SPN_APP_ID = credentials.application_id
        print(f"  Application ID: {ADMIN_SPN_APP_ID}")
else:
    # SPN exists - just look up the application_id (don't create new secret)
    if ADMIN_SPN_APP_ID:
        admin_spn.application_id = ADMIN_SPN_APP_ID
    else:
        # Look it up from Databricks (create() is idempotent, won't create new secret)
        result = spn_executor.create(admin_spn)
        ADMIN_SPN_APP_ID = admin_spn.application_id
    print(f"Admin SPN ready: {admin_spn.resolved_name} (app_id: {ADMIN_SPN_APP_ID})")

# Create Principal with application_id for use in grants
# Grants to service principals require the application_id (UUID), not the display name
admin_spn_principal = admin_spn.to_principal()
print(f"  Admin principal for grants: {admin_spn_principal.resolved_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define Team, Workspace & Principals
# MAGIC
# MAGIC BrickKit uses `Team` to bring together:
# MAGIC - **Workspace** - The Databricks workspace(s) per environment
# MAGIC - **Service Principals** - For ownership and access (account-level compatible)
# MAGIC - **Catalog bindings** - Automatically configured via `team.add_catalog()`
# MAGIC
# MAGIC **Why SPNs only?** Unity Catalog requires account-level principals for both ownership AND grants. Service Principals work at both workspace and account level. Workspace groups (created via SCIM) are workspace-local and cannot be used with Unity Catalog. In production with IdP integration, account-level groups synced via SCIM would also work.

# COMMAND ----------

# === DEFINE WORKSPACE ===

registry = WorkspaceRegistry()
dev_workspace = registry.get_or_create(
    workspace_id=WORKSPACE_ID,
    name=WORKSPACE_NAME,
    hostname=WORKSPACE_HOSTNAME,
    environment=Environment.DEV,
)

print(f"Workspace: {dev_workspace.name} (ID: {dev_workspace.workspace_id})")
print(f"  Hostname: {WORKSPACE_HOSTNAME}")

# COMMAND ----------

# === DEFINE PRINCIPALS ===
# Service principal for resource ownership.
# Environment-aware: names automatically get _dev/_acc/_prd suffixes.
#
# NOTE: Unity Catalog requires account-level principals for ownership AND grants.
# Service Principals work at both workspace and account level, making them
# the most reliable choice for automated deployments.

# Service Principal for catalog AND schema ownership (per convention)
trading_platform_spn = ManagedServicePrincipal(name="spn_trading_platform")
trading_platform_spn.add_entitlement("workspace-access")
trading_platform_spn.add_entitlement("databricks-sql-access")

print(f"Service Principal (owner): {trading_platform_spn.resolved_name}")

# COMMAND ----------

# === DEPLOY TEAM PRINCIPALS ===
# Create the service principal in Databricks before using it as owner.
# The executor is idempotent - returns SKIPPED if principal already exists.

# Deploy trading platform SPN (will be catalog AND schema owner)
print(f"Deploying service principal: {trading_platform_spn.resolved_name}")
result = spn_executor.create(trading_platform_spn)
print(f"  {result.operation.value}: {result.message}")

# NOTE: We skip creating the workspace group since Unity Catalog
# requires account-level principals for both ownership AND grants.
# In production, use account-level groups synced via IdP SCIM.

# COMMAND ----------

# === DEFINE TEAM ===
# Team brings together workspace, principals, and manages catalog bindings.

# After SPN deployment (cell 11), the application_id is set on trading_platform_spn.
# Use to_principal() to get a Principal with the application_id for ownership/grants.
# Both catalog AND schema are owned by the same SPN (per convention: owner_must_be_sp)

catalog_owner = trading_platform_spn.to_principal()
schema_owner = trading_platform_spn.to_principal()  # Same SPN owns schemas too

# Create team and add workspace + principals
quant_team = Team(name="quant_trading")
quant_team.add_workspace(dev_workspace)
quant_team.add_principal(catalog_owner)

print(f"Team: {quant_team.name}")
print(f"  Workspaces: {list(quant_team.workspaces.keys())}")
print(f"  Catalog/Schema owner: {catalog_owner.display_name} (app_id: {catalog_owner.application_id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Sample Data
# MAGIC
# MAGIC We'll use a small inline dataset of World Bank indicators. This lets you run the full demo quickly without external API calls.

# COMMAND ----------

# === SAMPLE DATA ===
# 20 World Bank indicators with embedding text for vector search

SAMPLE_INDICATORS = [
    ("SP.POP.TOTL", "Population, total", "Total population counts all residents regardless of legal status or citizenship.", "Demographics"),
    ("NY.GDP.MKTP.CD", "GDP (current US$)", "GDP at purchaser's prices is the sum of gross value added by all resident producers.", "Economy"),
    ("NY.GDP.PCAP.CD", "GDP per capita (current US$)", "GDP per capita is gross domestic product divided by midyear population.", "Economy"),
    ("SI.POV.DDAY", "Poverty headcount ratio at $2.15 a day", "Poverty headcount ratio at $2.15 a day is the percentage of the population living on less than $2.15 a day.", "Poverty"),
    ("SI.POV.GINI", "Gini index", "Gini index measures the extent to which the distribution of income among individuals deviates from a perfectly equal distribution.", "Inequality"),
    ("SL.UEM.TOTL.ZS", "Unemployment, total (% of labor force)", "Unemployment refers to the share of the labor force that is without work but available and seeking employment.", "Labor"),
    ("FP.CPI.TOTL.ZG", "Inflation, consumer prices (annual %)", "Inflation as measured by the consumer price index reflects the annual percentage change in the cost of goods and services.", "Economy"),
    ("SP.DYN.LE00.IN", "Life expectancy at birth, total (years)", "Life expectancy at birth indicates the number of years a newborn infant would live if patterns of mortality at birth were to stay the same.", "Health"),
    ("SH.DYN.MORT", "Mortality rate, under-5 (per 1,000 live births)", "Under-five mortality rate is the probability per 1,000 that a newborn baby will die before reaching age five.", "Health"),
    ("SE.ADT.LITR.ZS", "Literacy rate, adult total (% of people ages 15 and above)", "Adult literacy rate is the percentage of people ages 15 and above who can read and write a short simple statement.", "Education"),
    ("SE.PRM.ENRR", "School enrollment, primary (% gross)", "Gross enrollment ratio is the ratio of total enrollment to the population of the age group that officially corresponds to the level of education.", "Education"),
    ("EG.USE.ELEC.KH.PC", "Electric power consumption (kWh per capita)", "Electric power consumption measures the production of power plants and combined heat and power plants less transmission losses.", "Energy"),
    ("EN.ATM.CO2E.PC", "CO2 emissions (metric tons per capita)", "Carbon dioxide emissions are those stemming from the burning of fossil fuels and the manufacture of cement.", "Environment"),
    ("AG.LND.FRST.ZS", "Forest area (% of land area)", "Forest area is land under natural or planted stands of trees of at least 5 meters in situ.", "Environment"),
    ("SH.XPD.CHEX.PC.CD", "Current health expenditure per capita (current US$)", "Current expenditures on health per capita in current US dollars.", "Health"),
    ("IT.NET.USER.ZS", "Individuals using the Internet (% of population)", "Internet users are individuals who have used the Internet in the last 3 months.", "Technology"),
    ("BX.KLT.DINV.CD.WD", "Foreign direct investment, net inflows (BoP, current US$)", "Foreign direct investment are the net inflows of investment to acquire a lasting management interest.", "Economy"),
    ("GC.DOD.TOTL.GD.ZS", "Central government debt, total (% of GDP)", "Debt is the entire stock of direct government fixed-term contractual obligations to others outstanding.", "Economy"),
    ("NE.EXP.GNFS.ZS", "Exports of goods and services (% of GDP)", "Exports of goods and services represent the value of all goods and other market services provided to the rest of the world.", "Trade"),
    ("NE.IMP.GNFS.ZS", "Imports of goods and services (% of GDP)", "Imports of goods and services represent the value of all goods and other market services received from the rest of the world.", "Trade"),
]

INDICATORS_SCHEMA = StructType([
    StructField("indicator_id", StringType(), False),
    StructField("indicator_name", StringType(), True),
    StructField("description", StringType(), True),
    StructField("topic", StringType(), True),
    StructField("embedding_text", StringType(), True),
])

def create_sample_dataframe(spark_session):
    """Create DataFrame from sample indicators with embedding text."""
    rows = [(ind_id, name, desc, topic, f"{name}. {desc}") for ind_id, name, desc, topic in SAMPLE_INDICATORS]
    return spark_session.createDataFrame(rows, INDICATORS_SCHEMA)

print(f"Sample data defined: {len(SAMPLE_INDICATORS)} indicators")

# COMMAND ----------

# MAGIC %md
# MAGIC ### (Optional) Fetch Real Data from World Bank API
# MAGIC
# MAGIC Uncomment and run the cell below to fetch real indicator metadata. This takes several minutes.

# COMMAND ----------

# === OPTIONAL: FETCH FROM WORLD BANK API ===
# Uncomment this cell to fetch real data (takes several minutes)

# %pip install wbgapi requests tqdm --quiet

# import wbgapi as wb
# import requests
# from requests.exceptions import RequestException, Timeout
# from tqdm import tqdm

# def fetch_worldbank_indicators(spark: SparkSession, limit: int = 100):
#     """Fetch indicator metadata from World Bank API."""
#     series_list = wb.series.info()
#     series_ids = [s.get("id") for s in series_list.items][:limit]
#
#     rows = []
#     for series_id in tqdm(series_ids, desc="Fetching"):
#         try:
#             url = f"https://api.worldbank.org/v2/indicator/{series_id}?format=json"
#             resp = requests.get(url, timeout=30)
#             resp.raise_for_status()
#             data = resp.json()
#             if len(data) >= 2 and data[1]:
#                 meta = data[1][0]
#                 name = meta.get("name", "") or ""
#                 desc = meta.get("sourceNote", "") or ""
#                 topics = meta.get("topics", []) or []
#                 topic = topics[0].get("value", "") if topics else ""
#                 embedding_text = f"{name}. {desc}".strip()
#                 rows.append((series_id, name, desc, topic, embedding_text))
#         except (RequestException, Timeout, ValueError) as e:
#             print(f"Skipping {series_id}: {e}")
#
#     return spark.createDataFrame(rows, INDICATORS_SCHEMA)

# # Fetch real data (uncomment to use)
# sample_df = fetch_worldbank_indicators(spark, limit=500)
# print(f"Fetched {sample_df.count()} indicators from World Bank API")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Define Governed Resources
# MAGIC
# MAGIC Now we define our resources using BrickKit models. The convention automatically applies:
# MAGIC - Environment-specific naming (e.g., `quant_risk_dev`)
# MAGIC - Required governance tags
# MAGIC - Ownership rules validation

# COMMAND ----------

# === ENVIRONMENT SETUP ===
# catalog_owner and schema_owner are already defined in the Team cell above

environment = ENV_MAP[ENVIRONMENT]

print(f"Catalog owner: {catalog_owner.resolved_name} ({catalog_owner.principal_type.value})")
print(f"Schema owner: {schema_owner.resolved_name} ({schema_owner.principal_type.value})")

# COMMAND ----------

# === CATALOG ===
# NOTE: In Free Edition workspaces with "Default Storage", catalogs cannot be created via SDK.
# Create the catalog manually via UI first (with "Use default storage" checked), then reference it here.

catalog_name = convention.generate_name(SecurableType.CATALOG, environment)

catalog = Catalog(
    name=catalog_name,
    owner=catalog_owner,
    comment="Risk Analytics catalog for quantitative trading",
    isolation_mode=IsolationMode.ISOLATED,  # Default Storage catalogs are workspace-isolated
)

# Use Team to automatically configure workspace bindings
# This sets catalog.workspace_ids based on the team's workspace for this environment
quant_team.add_catalog(catalog)

# Apply convention (adds tags, validates rules)
convention.apply_to(catalog, environment)
errors = convention.get_validation_errors(catalog)
if errors:
    raise ValueError(f"Catalog validation failed: {errors}")

print(f"Catalog: {catalog.name}")
print(f"  Isolation Mode: {catalog.isolation_mode.value}")
print(f"  Workspace IDs: {catalog.workspace_ids} (auto-configured by Team)")
print(f"  Tags: {len(catalog.tags)}")

# COMMAND ----------

# === SCHEMA ===
schema = Schema(
    name=SCHEMA_NAME,
    catalog_name=catalog.name,
    owner=schema_owner,
    comment="World Bank indicator metadata for vector search",
)

convention.apply_to(schema, environment)
errors = convention.get_validation_errors(schema)
if errors:
    raise ValueError(f"Schema validation failed: {errors}")

print(f"Schema: {schema.fqdn}")
print(f"  Tags: {len(schema.tags)}")

# COMMAND ----------

# === TABLE ===
# Define the table structure with BrickKit (not just raw PySpark write)

table = Table(
    name=TABLE_NAME,
    catalog_name=catalog.name,
    schema_name=schema.name,
    owner=schema_owner,
    comment="World Bank indicator metadata with embeddings for semantic search",
    columns=[
        ColumnInfo(name="indicator_id", type="STRING", nullable=False, comment="World Bank indicator code"),
        ColumnInfo(name="indicator_name", type="STRING", nullable=True, comment="Human-readable indicator name"),
        ColumnInfo(name="description", type="STRING", nullable=True, comment="Full description of the indicator"),
        ColumnInfo(name="topic", type="STRING", nullable=True, comment="Category/topic of the indicator"),
        ColumnInfo(name="embedding_text", type="STRING", nullable=True, comment="Text used for embedding generation"),
    ],
    tags=[
        Tag(key="data_source", value="worldbank_api"),
        Tag(key="refresh_frequency", value="weekly"),
        Tag(key="contains_pii", value="false"),
    ],
)

convention.apply_to(table, environment)
errors = convention.get_validation_errors(table)
if errors:
    raise ValueError(f"Table validation failed: {errors}")

print(f"Table: {table.fqdn}")
print(f"  Columns: {len(table.columns)}")
print(f"  Tags: {len(table.tags)}")

# COMMAND ----------

# === VECTOR SEARCH ENDPOINT ===
vs_endpoint = VectorSearchEndpoint(
    name=ENDPOINT_NAME,
    comment="Semantic search endpoint for risk analytics indicators",
    # NOTE: Endpoints don't support custom tags
)

convention.apply_to(vs_endpoint, environment)
errors = convention.get_validation_errors(vs_endpoint)
if errors:
    raise ValueError(f"Endpoint validation failed: {errors}")

print(f"Endpoint: {vs_endpoint.resolved_name}")

# COMMAND ----------

# === VECTOR SEARCH INDEX ===
# Use table.fqdn to reference the governed table

vs_index = VectorSearchIndex(
    name=INDEX_NAME,
    endpoint_name=ENDPOINT_NAME,
    source_table=table.fqdn,  # Reference the governed Table model
    primary_key="indicator_id",
    embedding_column="embedding_text",
    embedding_model="databricks-bge-large-en",
    pipeline_type="TRIGGERED",
    # NOTE: Skipping tags for now - SDK support TBD
)

convention.apply_to(vs_index, environment)
errors = convention.get_validation_errors(vs_index)
if errors:
    raise ValueError(f"Index validation failed: {errors}")

print(f"Index: {vs_index.resolved_name}")
print(f"  Source: {vs_index.source_table}")
print(f"  Endpoint: {vs_index.resolved_endpoint_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Deploy with BrickKit Executors
# MAGIC
# MAGIC BrickKit executors handle:
# MAGIC - Idempotent create (skip if exists)
# MAGIC - Wait for provisioning
# MAGIC - Tag application
# MAGIC - **Permission grants** - Ensure teams have access after ownership change
# MAGIC - Error handling

# COMMAND ----------

# === INITIALIZE CLIENTS AND EXECUTORS ===

# WorkspaceClient uses VS Code extension's connection or default profile
ws_client = WorkspaceClient()

# VectorSearchClient doesn't support OAuth directly, so we extract the token
# from WorkspaceClient's auth and pass it explicitly
if IS_DATABRICKS:
    vs_client = VectorSearchClient()
else:
    auth_header = ws_client.config.authenticate()
    token = auth_header.get("Authorization", "").replace("Bearer ", "")
    vs_client = VectorSearchClient(
        workspace_url=ws_client.config.host,
        personal_access_token=token,
    )

# Create privileged client from stored SPN credentials
# On Databricks: uses dbutils to get secrets
# Locally: uses the same profile (user auth) - may have limited permissions
if IS_DATABRICKS:
    privileged_client = get_privileged_client(
        host=WORKSPACE_HOSTNAME,
        scope=SECRET_SCOPE,
        dbutils=dbutils,
    )
else:
    # Local: use the same client (your user has permissions)
    # For full SPN-based auth locally, set BRICKKIT_ADMIN_SPN_CLIENT_ID and _SECRET env vars
    client_id = os.environ.get("BRICKKIT_ADMIN_SPN_CLIENT_ID")
    client_secret = os.environ.get("BRICKKIT_ADMIN_SPN_CLIENT_SECRET")
    if client_id and client_secret:
        privileged_client = WorkspaceClient(
            host=f"https://{WORKSPACE_HOSTNAME}",
            client_id=client_id,
            client_secret=client_secret,
        )
        print(f"Using SPN-based privileged client")
    else:
        privileged_client = ws_client
        print(f"Using user-based client (no SPN credentials in env)")

# Grant OWNER_ADMIN (ALL_PRIVILEGES + MANAGE) to the admin SPN on the catalog
print(f"Granting admin SPN access to catalog {catalog.resolved_name}...")
catalog.grant(admin_spn_principal, AccessPolicy.OWNER_ADMIN())

bootstrap_grant_executor = GrantExecutor(ws_client, dry_run=False)
for result in bootstrap_grant_executor.apply_privileges(catalog.privileges):
    print(f"  {result.operation.value}: {result.message}")

# Initialize executors
catalog_executor = CatalogExecutor(privileged_client, dry_run=DRY_RUN)
schema_executor = SchemaExecutor(privileged_client, dry_run=DRY_RUN)
grant_executor = GrantExecutor(ws_client, dry_run=DRY_RUN)
endpoint_executor = VectorSearchEndpointExecutor(ws_client, dry_run=DRY_RUN)
index_executor = VectorSearchIndexExecutor(ws_client, dry_run=DRY_RUN)

print(f"Executors initialized (dry_run={DRY_RUN})")

# COMMAND ----------

# === DEPLOY CATALOG ===
# In Free Edition with "Default Storage", create the catalog manually via UI first.
# The executor handles existing catalogs gracefully (returns NO_OP if already exists).
result = catalog_executor.create(catalog)
print(f"Catalog: {result.operation.value} - {result.message}")

# COMMAND ----------

# === DEPLOY SCHEMA ===
result = schema_executor.create(schema)
print(f"Schema: {result.operation.value} - {result.message}")

# COMMAND ----------

# === VERIFY GRANTS ===
# The admin SPN already has OWNER_ADMIN access (granted in cell 25).
# That's sufficient for this demo.
#
# NOTE: Unity Catalog requires account-level principals for grants.
# Workspace groups (created via SCIM) cannot receive UC grants.
# In production, you would:
# 1. Use account-level groups (synced from IdP via SCIM at account level)
# 2. Or grant to individual users
# 3. Or use additional service principals

# Show what privileges are configured on the catalog
print(f"Privileges on catalog {catalog.resolved_name}:")
for priv in catalog.privileges:
    print(f"  - {priv.privilege.value} to {priv.principal}")

print(f"\n✓ Admin SPN has full access to manage resources")

# COMMAND ----------

# === WRITE DATA TO TABLE ===
sample_df = create_sample_dataframe(spark)

sample_df.write.format("delta") \
    .option("delta.enableChangeDataFeed", "true") \
    .mode("overwrite") \
    .saveAsTable(table.fqdn)

print(f"✓ Table {table.fqdn}: {spark.table(table.fqdn).count()} rows")

# COMMAND ----------

# === DEPLOY VECTOR SEARCH ENDPOINT ===
result = endpoint_executor.create(vs_endpoint)
print(f"Endpoint: {result.operation.value} - {result.message}")

# Wait for endpoint to be online (uses executor's built-in wait logic)
if not DRY_RUN and result.operation.value == "CREATE":
    print("Waiting for endpoint to be online...")
    if endpoint_executor.wait_for_endpoint(vs_endpoint):
        print(f"Endpoint {vs_endpoint.resolved_name} is ONLINE")
    else:
        raise RuntimeError(f"Endpoint {vs_endpoint.resolved_name} failed to provision")

# COMMAND ----------

# === DEPLOY VECTOR SEARCH INDEX ===
result = index_executor.create(vs_index)
print(f"Index: {result.operation.value} - {result.message}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Test Vector Search
# MAGIC
# MAGIC The index syncs asynchronously. Once ready, we can run similarity searches.

# COMMAND ----------

# === CHECK INDEX STATUS ===
FULL_INDEX_NAME = vs_index.fqdn  # Use the model's fully qualified name

index = vs_client.get_index(
    endpoint_name=vs_endpoint.resolved_name,
    index_name=FULL_INDEX_NAME,
)
status = index.describe().get("status", {})
print(f"Index status: ready={status.get('ready', 'UNKNOWN')}, message={status.get('message', 'N/A')}")

# COMMAND ----------

# === RUN SIMILARITY SEARCH ===
TEST_QUERY = "poverty and inequality measures"

results = index.similarity_search(
    query_text=TEST_QUERY,
    columns=["indicator_id", "indicator_name", "description", "topic"],
    num_results=5,
)

print(f"Search: '{TEST_QUERY}'")
print("=" * 60)
for i, row in enumerate(results.get("result", {}).get("data_array", []), 1):
    print(f"{i}. [{row[3]}] {row[1]}")
    print(f"   {row[2][:80]}...")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 6. What BrickKit Added (Governance Value)
# MAGIC
# MAGIC Let's see what governance BrickKit applied automatically.

# COMMAND ----------

# === GOVERNANCE SUMMARY ===

def print_tags(resource):
    if resource.tags:
        print(f"  Tags: {', '.join(f'{t.key}={t.value}' for t in resource.tags)}")

print("=" * 60)
print("TEAM")
print("=" * 60)
print(f"  Name: {quant_team.name}")
print(f"  Workspaces: {[(e.value, w.name) for e, w in quant_team.workspaces.items()]}")
print(f"  Principals: {[p.resolved_name for p in quant_team.principals]}")

print("\n" + "=" * 60)
print("CATALOG")
print("=" * 60)
print(f"  Name: {catalog.resolved_name}")
print(f"  Owner: {catalog.owner.resolved_name}")
print(f"  Isolation: {catalog.isolation_mode.value}")
print(f"  Workspaces: {catalog.workspace_ids}")
print_tags(catalog)

print("\n" + "=" * 60)
print("SCHEMA")
print("=" * 60)
print(f"  Name: {schema.fqdn}")
print(f"  Owner: {schema.owner.resolved_name}")
print_tags(schema)

print("\n" + "=" * 60)
print("TABLE")
print("=" * 60)
print(f"  Name: {table.fqdn}")
print(f"  Columns: {len(table.columns)}")
print_tags(table)

print("\n" + "=" * 60)
print("VECTOR SEARCH")
print("=" * 60)
print(f"  Endpoint: {vs_endpoint.resolved_name}")
print(f"  Index: {vs_index.resolved_name}")
print(f"  Source: {vs_index.source_table}")

# COMMAND ----------

# === CONVENTION RULES APPLIED ===

print("\n" + "=" * 60)
print("CONVENTION RULES")
print("=" * 60)
print(f"Convention: {convention.name} (v{convention.version})")
print()

for rule in convention.schema.rules:
    mode = "ENFORCED" if rule.mode.value == "enforced" else "ADVISORY"
    print(f"[{mode}] {rule.rule}")

print()
print("What this means:")
print("- All securables MUST be owned by service principals (not users or groups)")
print("- This ensures compatibility with Unity Catalog's account-level principal requirements")
print("- Resources SHOULD have cost_center and team tags")
print("- BrickKit validated all these rules before deployment")

# COMMAND ----------

# === WHAT YOU DIDN'T HAVE TO DO ===

print("\n" + "=" * 60)
print("WHAT BRICKKIT DID FOR YOU")
print("=" * 60)

benefits = [
    ("Team definition", f"Team '{quant_team.name}' brings together workspace + principals"),
    ("Principal definitions", f"Defined SPN with application_id for ownership"),
    ("Workspace binding", f"team.add_catalog() auto-configured workspace IDs: {catalog.workspace_ids}"),
    ("Environment suffixes", f"All names automatically suffixed with '_{ENVIRONMENT}'"),
    ("Governance tags", f"{len(catalog.tags)} tags auto-applied from convention"),
    ("Ownership validation", "Verified all securables have SPN owners (Unity Catalog compatible)"),
    ("Permission grants", f"Admin SPN has OWNER_ADMIN for full access"),
    ("Request for Access", "RFA configured with inheritance (table inherits from schema)"),
    ("Idempotent deployment", "Executors skip if resource exists, sync tags if needed"),
    ("Wait logic", "Built-in endpoint provisioning wait with timeout/retry"),
    ("Consistent patterns", "Same governance across Catalog, Schema, Endpoint, Index"),
]

for benefit, detail in benefits:
    print(f"\n{benefit}:")
    print(f"  {detail}")

print("\n" + "=" * 60)
print("Without BrickKit, you would manually:")
print("  - Define SPNs with raw SDK calls and track application_ids")
print("  - Track which workspace IDs to bind to each catalog")
print("  - Add environment suffixes to every resource name")
print("  - Remember which tags to apply (and apply them consistently)")
print("  - Validate ownership rules before deployment")
print("  - Grant permissions after changing ownership")
print("  - Configure RFA on each securable individually")
print("  - Write wait/retry logic for endpoint provisioning")
print("  - Handle idempotency (check exists, update tags, etc.)")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Summary
# MAGIC
# MAGIC This demo showed:
# MAGIC
# MAGIC 1. **Convention Loading** - Governance rules from YAML
# MAGIC 2. **Team Definition** - `Team` with `Workspace` and `Principal` members
# MAGIC 3. **Principal Definition** - `ManagedServicePrincipal`, `ManagedGroup` with members
# MAGIC 4. **Catalog Binding** - `team.add_catalog()` auto-configures workspace IDs
# MAGIC 5. **Governed Models** - `Catalog`, `Schema`, `Table`, `VectorSearchEndpoint`, `VectorSearchIndex`
# MAGIC 6. **Executors** - Idempotent deployment with built-in wait logic
# MAGIC 7. **Permission Grants** - Ensure team access after ownership changes
# MAGIC 8. **Automatic Governance** - Tags, naming, ownership validation
# MAGIC
# MAGIC ### BrickKit vs DAB Recap
# MAGIC
# MAGIC - **DAB** handles: notebook sync, job definitions, workflow orchestration
# MAGIC - **BrickKit** handles: teams, principals, catalog/schema/table creation, workspace bindings, VS endpoint/index, grants, tags, validation
# MAGIC - **Together**: DAB runs this notebook as a job, BrickKit deploys the governed resources
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC - Modify `conventions/financial_services.yml` to change governance rules
# MAGIC - Set `dry_run=false` to deploy for real
# MAGIC - Try different environments (`dev`, `acc`, `prd`) to see naming changes
# MAGIC - Add more workspaces to the team for multi-environment deployments
# MAGIC - Add your own data source instead of sample indicators