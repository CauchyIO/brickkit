"""
Table models for Unity Catalog.

This module contains:
- ColumnInfo: Column definition for tables (SDK-aligned)
- Column: Governance-aware column with tags
- Table: Third-level object storing structured data (SDK-aligned)
- GoverningTable: Table with governance support (SQL DDL generation, SCD2)

Mirrors the Databricks SDK TablesAPI pattern with governance extensions.
"""

from __future__ import annotations

import json
import keyword
import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PrivateAttr,
    computed_field,
    field_validator,
)
from typing_extensions import Self

from .base import DEFAULT_SECURABLE_OWNER, BaseGovernanceModel, BaseSecurable, Tag, get_current_environment
from .enums import ALL_PRIVILEGES_EXPANSION, PrivilegeType, SecurableType, TableType
from .external_locations import ExternalLocation
from .grants import AccessPolicy, Principal

if TYPE_CHECKING:
    from .catalogs import Catalog
    from .functions import Function
    from .schemas import Schema

logger = logging.getLogger(__name__)


# =============================================================================
# COLUMN INFO
# =============================================================================

class ColumnInfo(BaseModel):
    """
    Column definition for tables, matching Databricks SDK format.

    This model matches the SDK's ColumnInfo structure for table creation.
    """
    name: str = Field(
        ...,
        pattern=r'^[a-zA-Z_][a-zA-Z0-9_]*$',
        description="Column name (SQL identifier rules)"
    )
    type_name: str = Field(
        ...,
        alias='type',  # Allow 'type' as alias for convenience
        description="SQL data type (e.g., STRING, INT, BIGINT, DECIMAL(10,2), TIMESTAMP)"
    )
    nullable: bool = Field(True, description="Whether column allows NULL values")
    comment: Optional[str] = Field(None, max_length=1024, description="Column description")
    position: Optional[int] = Field(None, description="Column position in table")
    default_value: Optional[str] = Field(None, description="Default value expression")
    partition_index: Optional[int] = Field(None, description="Partition column index")

    model_config = ConfigDict(
        populate_by_name=True,  # Allow both field name and alias
    )

    @field_validator('type_name', mode='before')
    @classmethod
    def normalize_type_name(cls, v: Any) -> str:
        """Normalize data type to uppercase."""
        if isinstance(v, str):
            return v.upper()
        return v

    @field_validator('name')
    @classmethod
    def validate_column_name(cls, v: str) -> str:
        """Validate column name follows SQL identifier rules."""
        # Check for Python/SQL reserved words
        sql_reserved = {'SELECT', 'FROM', 'WHERE', 'ORDER', 'GROUP', 'TABLE', 'INDEX'}
        if v.upper() in sql_reserved or keyword.iskeyword(v):
            logger.warning(f"Column name '{v}' is a reserved word - consider renaming")
        return v


# =============================================================================
# TABLE
# =============================================================================

class Table(BaseSecurable):
    """
    Third-level object storing structured data.

    Tables represent structured data within a schema and support various types
    including managed, external, views, and streaming tables. They can have
    row-level security filters and column masking functions applied.
    """
    name: str = Field(
        ...,
        pattern=r'^[a-zA-Z0-9][a-zA-Z0-9_-]*$',
        description="Table name (no environment suffix - parent catalog has it)"
    )
    table_type: TableType = Field(
        TableType.MANAGED,
        description="MANAGED, EXTERNAL, VIEW, MATERIALIZED_VIEW, or STREAMING_TABLE"
    )
    owner: Optional[Principal] = Field(
        default_factory=lambda: Principal(name=DEFAULT_SECURABLE_OWNER, add_environment_suffix=False),
        description="Owner principal (defaults to parent schema owner)"
    )
    columns: List[ColumnInfo] = Field(
        default_factory=list,
        description="Strongly typed column definitions"
    )
    external_location: Optional[ExternalLocation] = Field(
        None,
        description="For external tables (inherited from schema if not set)"
    )
    row_filter: Optional[Any] = Field(
        None,
        description="Row-level security function (executes with definer's rights)"
    )
    column_masks: Dict[str, Any] = Field(
        default_factory=dict,
        description="Column masking functions by column name (execute with definer's rights)"
    )
    comment: Optional[str] = Field(None, max_length=1024, description="Description of the table")

    # Metadata
    tags: List[Tag] = Field(default_factory=list, description="Metadata tags for ABAC")

    # Dependencies
    referencing_functions: List[Any] = Field(
        default_factory=list,
        description="Functions that reference this table for dependency tracking"
    )

    # Private parent reference (not serialized)
    _parent_schema: Optional[Schema] = PrivateAttr(default=None)
    _parent_catalog: Optional[Catalog] = PrivateAttr(default=None)

    catalog_name: Optional[str] = Field(
        None,
        description="Parent catalog name (set by add_table)"
    )
    schema_name: Optional[str] = Field(
        None,
        description="Parent schema name (set by add_table)"
    )

    @computed_field
    @property
    def resolved_catalog_name(self) -> str:
        """Get resolved catalog name with environment suffix from parent."""
        if self._parent_catalog:
            return self._parent_catalog.resolved_name
        elif self._parent_schema and self._parent_schema._parent_catalog:
            return self._parent_schema._parent_catalog.resolved_name
        if not self.catalog_name:
            raise ValueError(f"Table '{self.name}' is not associated with a catalog")
        # Fallback to string field with current env if no parent
        env = get_current_environment()
        return f"{self.catalog_name}_{env.value.lower()}"

    @field_validator('table_type', mode='before')
    @classmethod
    def convert_table_type(cls, v: Any) -> TableType:
        """Convert string to TableType enum if needed."""
        if isinstance(v, str):
            return TableType(v.upper())
        return v

    @computed_field
    @property
    def fqdn(self) -> str:
        """Fully qualified domain name (catalog.schema.table format)."""
        if not self.catalog_name or not self.schema_name:
            raise ValueError(f"Table '{self.name}' is not associated with a catalog and schema")
        return f"{self.resolved_catalog_name}.{self.schema_name}.{self.name}"

    def set_row_filter(self, function: 'Function') -> None:
        """
        Set row-level security filter function.

        The function will execute transparently with definer's rights.
        Users only need SELECT on the table, not EXECUTE on the function.

        Args:
            function: The filter function to apply
        """
        self.row_filter = function
        # Add this table to the function's referenced tables for tracking
        if self not in function.referenced_tables:
            function.referenced_tables.append(self)

    def add_column_mask(self, column: str, function: 'Function') -> None:
        """
        Add column masking function.

        The function will execute transparently with definer's rights.
        Users only need SELECT on the table, not EXECUTE on the function.

        Args:
            column: The column name to mask
            function: The masking function to apply

        Raises:
            ValueError: If column doesn't exist in table
        """
        # Validate column exists
        column_names = [col.name for col in self.columns]
        if column_names and column not in column_names:
            raise ValueError(f"Column '{column}' not found in table columns")

        self.column_masks[column] = function
        # Add this table to the function's referenced tables for tracking
        if self not in function.referenced_tables:
            function.referenced_tables.append(self)

    def __repr__(self) -> str:
        """Developer-friendly representation."""
        return (
            f"Table(name='{self.name}', "
            f"type={self.table_type.value if self.table_type else 'MANAGED'}, "
            f"catalog='{self.catalog_name}', "
            f"schema='{self.schema_name}')"
        )

    def __str__(self) -> str:
        """User-friendly representation."""
        return f"Table '{self.fqdn}' ({self.table_type.value if self.table_type else 'MANAGED'})"

    def _propagate_grants(self, principal: Principal, policy: AccessPolicy) -> List[Any]:
        """
        Propagate grants to external location if present.

        Args:
            principal: The principal to grant to
            policy: The access policy

        Returns:
            List of propagated privileges
        """
        result = []

        # Propagate to external location if policy has external location privileges
        if self.external_location and policy.has_privileges_for(SecurableType.EXTERNAL_LOCATION):
            result.extend(self.external_location.grant(principal, policy, _skip_validation=True))

        return result

    def get_effective_privileges(self, principal: Principal) -> List[PrivilegeType]:
        """
        Get all privileges for a principal including inherited from schema.

        Note: Row filters and column masks execute transparently, so no
        EXECUTE privileges are needed on those functions.

        Args:
            principal: The principal to check

        Returns:
            List of effective privilege types
        """
        privileges = []

        # Collect direct privileges on this securable
        for priv in self.privileges:
            if priv.principal == principal.resolved_name:
                privileges.append(priv.privilege)

        # Naively try to get parent privileges (will just continue if no parent)
        if hasattr(self, '_parent_schema') and self._parent_schema:
            privileges.extend(self._parent_schema.get_effective_privileges(principal))

        # Handle ALL_PRIVILEGES expansion
        if PrivilegeType.ALL_PRIVILEGES in privileges:
            expanded = ALL_PRIVILEGES_EXPANSION.get(self.securable_type, []).copy()
            if PrivilegeType.MANAGE in privileges:
                expanded.append(PrivilegeType.MANAGE)
            return list(set(expanded))

        return list(set(privileges))

    @property
    def securable_type(self) -> SecurableType:
        """Return the securable type."""
        return SecurableType.TABLE

    def get_level_1_name(self) -> str:
        """Return the level-1 name (resolved catalog name for Privilege storage)."""
        return self.resolved_catalog_name

    def get_level_2_name(self) -> Optional[str]:
        """Return the level-2 name (schema name)."""
        return self.schema_name

    def get_level_3_name(self) -> Optional[str]:
        """Return the level-3 name (table name)."""
        return self.name

    def to_sdk_create_params(self) -> Dict[str, Any]:
        """Convert to SDK create parameters."""
        from databricks.sdk.service.catalog import ColumnInfo as SDKColumnInfo
        from databricks.sdk.service.catalog import ColumnTypeName, DataSourceFormat

        # Map SQL types to SDK ColumnTypeName enum values
        TYPE_MAPPING = {
            "BIGINT": "LONG",
            "INT": "INT",
            "INTEGER": "INT",
            "SMALLINT": "SHORT",
            "TINYINT": "BYTE",
            "DOUBLE": "DOUBLE",
            "FLOAT": "FLOAT",
            "DECIMAL": "DECIMAL",
            "STRING": "STRING",
            "VARCHAR": "STRING",
            "CHAR": "CHAR",
            "BOOLEAN": "BOOLEAN",
            "BOOL": "BOOLEAN",
            "DATE": "DATE",
            "TIMESTAMP": "TIMESTAMP",
            "BINARY": "BINARY",
            "ARRAY": "ARRAY",
            "MAP": "MAP",
            "STRUCT": "STRUCT",
        }

        sdk_columns = []
        for i, col in enumerate(self.columns):
            if isinstance(col, ColumnInfo):
                col_name = col.name
                col_type = col.type_name
                col_nullable = col.nullable
                col_comment = col.comment
            elif isinstance(col, dict):
                col_name = col.get("name")
                col_type = col.get("type", "STRING")
                col_nullable = col.get("nullable", True)
                col_comment = col.get("comment")
            else:
                continue

            base_type = col_type.upper().split('(')[0].strip()
            enum_name = TYPE_MAPPING.get(base_type, "STRING")
            type_json = json.dumps({"type": col_type})

            sdk_column = SDKColumnInfo(
                name=col_name,
                type_name=ColumnTypeName[enum_name],
                type_text=col_type,
                type_json=type_json,
                nullable=col_nullable,
                comment=col_comment,
                position=i
            )
            sdk_columns.append(sdk_column)

        params = {
            "name": self.name,
            "table_type": self.table_type,
            "columns": sdk_columns,
            "data_source_format": DataSourceFormat.DELTA,
        }

        if self.catalog_name:
            params["catalog_name"] = self.resolved_catalog_name
        if self.schema_name:
            params["schema_name"] = self.schema_name

        if self.external_location:
            base_url = self.external_location.url.rstrip('/')
            catalog_part = self.resolved_catalog_name if self.catalog_name else ""
            table_path = f"{catalog_part}/{self.schema_name}/{self.name}" if catalog_part else f"{self.schema_name}/{self.name}"
            params["storage_location"] = f"{base_url}/{table_path}"
        elif self.table_type == TableType.EXTERNAL:
            params["storage_location"] = None
        else:
            params["storage_location"] = ""

        return params

    def to_sdk_update_params(self) -> Dict[str, Any]:
        """Convert to SDK update parameters (limited update capability)."""
        params = {"full_name": self.fqdn}
        if self.owner:
            params["owner"] = self.owner.resolved_name
        return params

    def to_sql_ddl(self, if_not_exists: bool = True) -> str:
        """
        Generate SQL DDL CREATE TABLE statement.

        Args:
            if_not_exists: Whether to include IF NOT EXISTS clause

        Returns:
            SQL DDL CREATE TABLE statement
        """
        ddl_parts = []

        create_clause = "CREATE"
        if self.table_type == TableType.EXTERNAL:
            create_clause += " EXTERNAL"
        create_clause += " TABLE"
        if if_not_exists:
            create_clause += " IF NOT EXISTS"

        ddl_parts.append(f"{create_clause} {self.fqdn}")

        if self.columns:
            col_defs = []
            for col in self.columns:
                col_def = f"  {col.name} {col.type_name}"
                if not col.nullable:
                    col_def += " NOT NULL"
                if col.comment:
                    col_def += f" COMMENT '{col.comment}'"
                col_defs.append(col_def)

            ddl_parts.append("(")
            ddl_parts.append(",\n".join(col_defs))
            ddl_parts.append(")")

        ddl_parts.append("USING DELTA")

        if hasattr(self, 'partition_cols') and self.partition_cols:
            ddl_parts.append(f"PARTITIONED BY ({', '.join(self.partition_cols)})")

        if self.table_type == TableType.EXTERNAL and self.external_location:
            base_url = self.external_location.url.rstrip('/')
            catalog_part = self.resolved_catalog_name if self.catalog_name else ""
            table_path = f"{catalog_part}/{self.schema_name}/{self.name}" if catalog_part else f"{self.schema_name}/{self.name}"
            ddl_parts.append(f"LOCATION '{base_url}/{table_path}'")

        if self.comment:
            ddl_parts.append(f"COMMENT '{self.comment}'")

        return "\n".join(ddl_parts)

    def to_sql_alter_owner(self) -> Optional[str]:
        """Generate SQL ALTER TABLE ... SET OWNER statement."""
        if self.owner:
            return f"ALTER TABLE {self.fqdn} SET OWNER TO `{self.owner.resolved_name}`"
        return None

    # =============================================================================
    # ETL HELPER METHODS
    # =============================================================================

    def create_empty(self, spark) -> None:
        """Create empty table with schema - useful for placeholders."""
        spark.sql(self.to_sql_ddl())

    def create_from_dataframe(
        self,
        df,
        mode: str = "error",
        partition_by: Optional[List[str]] = None,
        cluster_by: Optional[List[str]] = None
    ) -> None:
        """
        Create or populate table from DataFrame.

        Args:
            df: PySpark DataFrame to write
            mode: Write mode - "overwrite", "append", "error", "ignore"
            partition_by: Optional partition columns
            cluster_by: Optional clustering columns for optimization
        """
        if not self.validate_dataframe(df):
            raise ValueError(f"DataFrame schema doesn't match table {self.name} definition")

        writer = df.write.mode(mode)
        partitions = partition_by or getattr(self, 'partition_cols', None)
        if partitions:
            writer = writer.partitionBy(*partitions)

        writer.saveAsTable(self.fqdn)

        if cluster_by:
            df.sparkSession.sql(f"OPTIMIZE {self.fqdn} ZORDER BY ({', '.join(cluster_by)})")

    def upsert_scd2(
        self,
        df,
        merge_keys: List[str],
        effective_date_col: str = "effective_date",
        end_date_col: str = "end_date",
        current_flag_col: str = "is_current"
    ) -> None:
        """
        Standard SCD Type 2 pattern - handle slowly changing dimensions.

        Args:
            df: DataFrame with new/updated records
            merge_keys: List of business keys to match records
            effective_date_col: Column name for record start date
            end_date_col: Column name for record end date
            current_flag_col: Column name for current record flag
        """
        from delta.tables import DeltaTable
        from pyspark.sql.functions import col, current_date, lit

        spark = df.sparkSession

        if not spark.catalog.tableExists(self.fqdn):
            df_with_scd = df.withColumn(effective_date_col, current_date()) \
                           .withColumn(end_date_col, lit(None).cast("date")) \
                           .withColumn(current_flag_col, lit(True))
            self.create_from_dataframe(df_with_scd)
            return

        merge_conditions = [f"target.{key} = source.{key}" for key in merge_keys]
        merge_conditions.append(f"target.{current_flag_col} = true")
        merge_condition = " AND ".join(merge_conditions)

        all_cols = set(df.columns)
        key_cols = set(merge_keys)
        compare_cols = all_cols - key_cols - {effective_date_col, end_date_col, current_flag_col}

        change_conditions = [f"target.{col_name} != source.{col_name}" for col_name in compare_cols]
        change_condition = " OR ".join(change_conditions) if change_conditions else "1=1"

        delta_table = DeltaTable.forName(spark, self.fqdn)

        source_df = df
        if effective_date_col not in df.columns:
            source_df = source_df.withColumn(effective_date_col, current_date())
        if end_date_col not in df.columns:
            source_df = source_df.withColumn(end_date_col, lit(None).cast("date"))
        if current_flag_col not in df.columns:
            source_df = source_df.withColumn(current_flag_col, lit(True))

        delta_table.alias("target").merge(
            source_df.alias("source"),
            merge_condition
        ).whenMatchedUpdate(
            condition=change_condition,
            set={
                end_date_col: current_date(),
                current_flag_col: lit(False)
            }
        ).whenNotMatchedInsertAll().execute()

        changed_records = source_df.alias("source").join(
            delta_table.toDF().alias("target"),
            on=[col(f"source.{k}") == col(f"target.{k}") for k in merge_keys],
            how="inner"
        ).where(f"target.{current_flag_col} = false AND target.{end_date_col} = current_date()")

        if changed_records.count() > 0:
            changed_records.select("source.*").write.mode("append").saveAsTable(self.fqdn)

    def optimize(
        self,
        spark,
        zorder_by: Optional[List[str]] = None,
        vacuum: bool = False,
        vacuum_hours: int = 168
    ) -> None:
        """
        Optimize table performance with OPTIMIZE and optional VACUUM.

        Args:
            spark: SparkSession instance
            zorder_by: Columns to Z-order by for query optimization
            vacuum: Whether to run VACUUM to clean old files
            vacuum_hours: Hours of history to retain (default 7 days)
        """
        optimize_sql = f"OPTIMIZE {self.fqdn}"
        if zorder_by:
            optimize_sql += f" ZORDER BY ({', '.join(zorder_by)})"
        spark.sql(optimize_sql)

        if vacuum:
            spark.sql(f"VACUUM {self.fqdn} RETAIN {vacuum_hours} HOURS")

    def validate_dataframe(self, df) -> bool:
        """Check if DataFrame schema matches table definition."""
        expected_cols = {col.name.lower() for col in self.columns}
        actual_cols = {field.name.lower() for field in df.schema.fields}

        for col_name in expected_cols:
            if col_name not in actual_cols:
                return False
        return True

    def to_spark_schema(self):
        """Convert table columns to Spark StructType."""
        from pyspark.sql.types import (
            BooleanType,
            DateType,
            DecimalType,
            DoubleType,
            IntegerType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        type_map = {
            "STRING": StringType(),
            "INT": IntegerType(),
            "INTEGER": IntegerType(),
            "BIGINT": IntegerType(),
            "DOUBLE": DoubleType(),
            "FLOAT": DoubleType(),
            "BOOLEAN": BooleanType(),
            "DATE": DateType(),
            "TIMESTAMP": TimestampType(),
            "DECIMAL": DecimalType(10, 2),
        }

        fields = []
        for col in self.columns:
            spark_type = type_map.get(col.type_name.upper(), StringType())
            field = StructField(col.name, spark_type, col.nullable)
            fields.append(field)

        return StructType(fields)


# =============================================================================
# GOVERNANCE-AWARE COLUMN MODEL
# =============================================================================

class Column(BaseGovernanceModel):
    """
    Column definition with governance tags support.

    Supports both simple column definitions and advanced features like
    primary/foreign keys, data types, and governance metadata via tags.

    This is separate from ColumnInfo which is SDK-aligned. Column is used
    for governance purposes with tag support at column level.

    Example:
        col = Column(
            name="customer_id",
            data_type="BIGINT",
            is_primary_key=True,
            tags=[
                Tag(key="pii", value="false"),
                Tag(key="source_system", value="crm"),
            ]
        )
    """
    # Core column properties
    name: str = Field(..., description="Column name (output name)")
    data_type: str = Field(..., description="SQL data type (STRING, BIGINT, DATE, etc.)")
    nullable: bool = Field(True, description="Whether column allows NULL values")
    description: Optional[str] = Field(None, description="Column description/comment")

    # Optional input mapping (for ETL scenarios)
    input_name: Optional[str] = Field(None, description="Source column name if different from output")

    # Key constraints
    is_primary_key: bool = Field(False, description="Primary key column")
    is_foreign_key: bool = Field(False, description="Foreign key column")
    foreign_key_table: Optional[str] = Field(None, description="Referenced table for FK")
    foreign_key_column: Optional[str] = Field(None, description="Referenced column for FK")

    # Governance tags
    tags: List[Tag] = Field(default_factory=list, description="Column-level governance tags")

    # Optional PySpark type for schema generation
    spark_type: Optional[Any] = Field(None, description="PySpark DataType for schema generation")

    model_config = {"arbitrary_types_allowed": True}

    @property
    def comment(self) -> str:
        """Get sanitized comment for SQL."""
        if self.description:
            return self.description.replace("'", "").replace('"', "")
        return ""

    @property
    def input_col(self) -> str:
        """Get input column name (for backward compatibility)."""
        return self.input_name or self.name

    @property
    def output_col(self) -> str:
        """Get output column name (for backward compatibility)."""
        return self.name

    def add_tag(self, key: str, value: str) -> Self:
        """Add a governance tag to this column."""
        self.tags.append(Tag(key=key, value=value))
        return self

    def get_tag(self, key: str) -> Optional[str]:
        """Get tag value by key."""
        for tag in self.tags:
            if tag.key == key:
                return tag.value
        return None

    def to_sql_definition(self, include_constraints: bool = True) -> str:
        """
        Generate SQL column definition.

        Args:
            include_constraints: Include PRIMARY KEY constraint inline

        Returns:
            SQL column definition string
        """
        parts = [self.name, self.data_type]

        if not self.nullable:
            parts.append("NOT NULL")

        if include_constraints and self.is_primary_key:
            parts.append("PRIMARY KEY")

        if self.description:
            parts.append(f"COMMENT '{self.comment}'")

        return " ".join(parts)


# =============================================================================
# SCD2 COLUMNS (Slowly Changing Dimension Type 2)
# =============================================================================

SCD2_COLUMNS = [
    Column(name="__valid_from", data_type="TIMESTAMP", nullable=False,
           description="SCD2: Timestamp when this record version became valid"),
    Column(name="__valid_to", data_type="TIMESTAMP", nullable=True,
           description="SCD2: Timestamp when this record version became invalid"),
    Column(name="__is_current", data_type="BOOLEAN", nullable=False,
           description="SCD2: Boolean flag indicating if this is the current version"),
    Column(name="__operation", data_type="STRING", nullable=False,
           description="SCD2: Type of operation (INSERT, UPDATE, DELETE)"),
    Column(name="__processed_time", data_type="TIMESTAMP", nullable=False,
           description="SCD2: Timestamp when this record was processed"),
    Column(name="__row_hash", data_type="STRING", nullable=False,
           description="SCD2: MD5 hash of tracked columns for change detection"),
    Column(name="__version", data_type="INTEGER", nullable=False,
           description="SCD2: Version number of this record"),
]


# =============================================================================
# GOVERNING TABLE MODEL (with full governance support)
# =============================================================================

class GoverningTable(BaseSecurable):
    """
    Table definition with full governance support.

    Provides:
    - Tag support at table and column level
    - Column definitions with governance tags
    - SQL DDL generation
    - SCD2 column support
    - Foreign key management
    - Environment-aware naming
    - Integration with GovernanceDefaults

    This is separate from Table which is SDK-aligned. GoverningTable uses
    the Column class (with tags) and provides SQL DDL generation.

    Example:
        table = GoverningTable(
            name="customers",
            catalog_name="sales",
            schema_name="bronze",
            description="Customer master data",
            columns=[
                Column(name="id", data_type="BIGINT", is_primary_key=True),
                Column(name="email", data_type="STRING",
                       tags=[Tag(key="pii", value="true")]),
            ],
            tags=[
                Tag(key="data_owner", value="crm-team"),
                Tag(key="retention_days", value="365"),
            ],
            enable_scd2=True,
        )
    """
    # Core table properties
    name: str = Field(..., description="Table name")
    description: Optional[str] = Field(None, description="Table description")
    columns: List[Column] = Field(default_factory=list, description="Column definitions with governance tags")

    # Location in Unity Catalog
    catalog_name: str = Field(..., description="Catalog name (without env suffix)")
    schema_name: str = Field(..., description="Schema name")

    # SCD2 support
    enable_scd2: bool = Field(False, description="Add SCD2 tracking columns")

    # Governance tags (inherited from BaseSecurable, but explicitly defined)
    tags: List[Tag] = Field(default_factory=list, description="Table-level governance tags")

    # Optional metadata
    source_url: Optional[str] = Field(None, description="Source system URL")
    source_database: Optional[str] = Field(None, description="Source database name")

    @property
    def securable_type(self) -> SecurableType:
        """Return TABLE securable type."""
        return SecurableType.TABLE

    @computed_field
    @property
    def resolved_catalog_name(self) -> str:
        """Catalog name with environment suffix."""
        env = get_current_environment()
        return f"{self.catalog_name}_{env.value.lower()}"

    @computed_field
    @property
    def fqdn(self) -> str:
        """Fully qualified table name with environment suffix."""
        return f"{self.resolved_catalog_name}.{self.schema_name}.{self.name}"

    @property
    def primary_key_column(self) -> Optional[str]:
        """Get primary key column name, or None if not defined."""
        for col in self.columns:
            if col.is_primary_key:
                return col.name
        return None

    @property
    def all_columns(self) -> List[Column]:
        """Get all columns including SCD2 if enabled."""
        if self.enable_scd2:
            return self.columns + SCD2_COLUMNS
        return self.columns

    def get_level_1_name(self) -> str:
        """Get catalog name for privilege tracking."""
        return self.resolved_catalog_name

    def get_level_2_name(self) -> Optional[str]:
        """Get schema name for privilege tracking."""
        return self.schema_name

    def get_level_3_name(self) -> Optional[str]:
        """Get table name for privilege tracking."""
        return self.name

    # -------------------------------------------------------------------------
    # Tag Management
    # -------------------------------------------------------------------------

    def add_tag(self, key: str, value: str) -> Self:
        """Add a governance tag to this table."""
        self.tags.append(Tag(key=key, value=value))
        return self

    def get_tag(self, key: str) -> Optional[str]:
        """Get tag value by key."""
        for tag in self.tags:
            if tag.key == key:
                return tag.value
        return None

    def get_column_tags(self, column_name: str) -> List[Tag]:
        """Get all tags for a specific column."""
        for col in self.columns:
            if col.name == column_name:
                return col.tags
        return []

    def get_pii_columns(self) -> List[Column]:
        """Get all columns tagged as PII."""
        return [col for col in self.columns if col.get_tag("pii") == "true"]

    # -------------------------------------------------------------------------
    # SQL Generation
    # -------------------------------------------------------------------------

    def create_table_statement(
        self,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        include_foreign_keys: bool = True,
    ) -> str:
        """
        Generate CREATE TABLE SQL statement.

        Args:
            catalog: Override catalog name (uses resolved_catalog_name if None)
            schema: Override schema name (uses self.schema_name if None)
            include_foreign_keys: Include FK constraints

        Returns:
            SQL CREATE TABLE statement
        """
        cat = catalog or self.resolved_catalog_name
        sch = schema or self.schema_name

        fk_statements = []
        create_stmt = f"CREATE TABLE IF NOT EXISTS {cat}.{sch}.{self.name} (\n"

        for col in self.all_columns:
            nullable = "" if col.nullable else "NOT NULL"
            fk_ref_col = col.foreign_key_column or col.name

            if col.is_primary_key:
                create_stmt += f"\t{col.name} {col.data_type} {nullable} PRIMARY KEY,\n"
            elif col.is_foreign_key and col.foreign_key_table:
                create_stmt += f"\t{col.name} {col.data_type} {nullable},\n"
                fk_statements.append(
                    f"\tFOREIGN KEY ({col.name}) REFERENCES "
                    f"{cat}.{sch}.{col.foreign_key_table}({fk_ref_col}),\n"
                )
            else:
                create_stmt += f"\t{col.name} {col.data_type} {nullable},\n"

        if include_foreign_keys:
            create_stmt += "".join(fk_statements)

        # Remove trailing comma
        create_stmt = create_stmt.rstrip(",\n") + "\n"
        create_stmt += ") USING DELTA;"

        return create_stmt

    def alter_comment_statements(
        self,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> List[str]:
        """
        Generate ALTER TABLE statements for comments.

        Returns list because Spark SQL can only process one ALTER at a time.

        Args:
            catalog: Override catalog name
            schema: Override schema name

        Returns:
            List of ALTER TABLE statements
        """
        cat = catalog or self.resolved_catalog_name
        sch = schema or self.schema_name
        fqdn = f"{cat}.{sch}.{self.name}"

        statements = []

        # Table comment
        if self.description:
            table_comment = self.description.replace("'", "").replace('"', "")
            statements.append(
                f'ALTER TABLE {fqdn} SET TBLPROPERTIES ("comment" = "{table_comment}");'
            )

        # Column comments
        for col in self.all_columns:
            if col.comment:
                statements.append(
                    f'ALTER TABLE {fqdn} ALTER COLUMN {col.name} COMMENT "{col.comment}";'
                )

        return statements

    def alter_tag_statements(
        self,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> List[str]:
        """
        Generate ALTER TABLE statements for tags.

        Args:
            catalog: Override catalog name
            schema: Override schema name

        Returns:
            List of ALTER TABLE SET TAGS statements
        """
        cat = catalog or self.resolved_catalog_name
        sch = schema or self.schema_name
        fqdn = f"{cat}.{sch}.{self.name}"

        statements = []

        # Table-level tags
        if self.tags:
            tag_pairs = ", ".join(f"'{t.key}' = '{t.value}'" for t in self.tags)
            statements.append(f"ALTER TABLE {fqdn} SET TAGS ({tag_pairs});")

        # Column-level tags
        for col in self.columns:
            if col.tags:
                tag_pairs = ", ".join(f"'{t.key}' = '{t.value}'" for t in col.tags)
                statements.append(
                    f"ALTER TABLE {fqdn} ALTER COLUMN {col.name} SET TAGS ({tag_pairs});"
                )

        return statements

    def alter_fk_statements(
        self,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> List[Dict[str, str]]:
        """
        Generate ALTER TABLE ADD CONSTRAINT statements for foreign keys.

        Returns:
            List of dicts with 'constraint_name' and 'statement' keys
        """
        cat = catalog or self.resolved_catalog_name
        sch = schema or self.schema_name
        fqdn = f"{cat}.{sch}.{self.name}"

        statements = []
        for col in self.columns:
            if col.is_foreign_key and col.foreign_key_table:
                fk_ref_col = col.foreign_key_column or col.name
                constraint_name = f"{self.name}_{col.name}_fk_to_{col.foreign_key_table}"

                statements.append({
                    "constraint_name": constraint_name,
                    "statement": (
                        f"ALTER TABLE {fqdn} ADD CONSTRAINT {constraint_name} "
                        f"FOREIGN KEY ({col.name}) REFERENCES "
                        f"{cat}.{sch}.{col.foreign_key_table}({fk_ref_col});"
                    )
                })

        return statements

    def check_fk_exists_query(
        self,
        constraint_name: str,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> str:
        """Generate query to check if FK constraint exists."""
        cat = catalog or self.resolved_catalog_name
        sch = schema or self.schema_name

        return f"""
            SELECT constraint_name
            FROM {cat}.information_schema.table_constraints
            WHERE table_catalog = '{cat}'
              AND table_schema = '{sch}'
              AND table_name = '{self.name}'
              AND constraint_name = '{constraint_name}'
              AND constraint_type = 'FOREIGN KEY'
        """


# =============================================================================
# CONVENIENCE ALIASES (backward compatibility)
# =============================================================================

# Aliases for backward compatibility
BaseColumn = Column
BaseTable = GoverningTable
