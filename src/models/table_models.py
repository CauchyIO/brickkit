"""
Table and Column models for Unity Catalog governance.

This module provides Pydantic models for defining tables and columns with:
- Tag support at column and table level
- Environment-aware naming
- SQL DDL generation
- SCD2 column support
- Foreign key relationships
- Integration with GovernanceDefaults

PySpark types are optional - if not available, schema_definition will not work.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, TYPE_CHECKING

from pydantic import Field, computed_field, model_validator
from typing_extensions import Self

from .base import BaseGovernanceModel, BaseSecurable, Tag, get_current_environment
from .enums import SecurableType, Environment

# Optional PySpark import for schema generation
try:
    from pyspark.sql.types import StructType, StructField, DataType
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    StructType = Any
    StructField = Any
    DataType = Any

if TYPE_CHECKING:
    from .access import Principal, AccessPolicy


# =============================================================================
# COLUMN MODEL
# =============================================================================

class Column(BaseGovernanceModel):
    """
    Column definition with governance tags support.

    Supports both simple column definitions and advanced features like
    primary/foreign keys, data types, and governance metadata via tags.

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
# TABLE MODEL
# =============================================================================

class Table(BaseSecurable):
    """
    Table definition with governance support.

    Provides:
    - Tag support at table level
    - Column definitions with tags
    - SQL DDL generation
    - SCD2 column support
    - Foreign key management
    - Environment-aware naming
    - Integration with GovernanceDefaults

    Example:
        table = Table(
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
    columns: List[Column] = Field(default_factory=list, description="Column definitions")

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

    # -------------------------------------------------------------------------
    # PySpark Schema Generation (optional)
    # -------------------------------------------------------------------------

    @computed_field
    @property
    def schema_definition(self) -> Optional[Any]:
        """
        Generate PySpark StructType schema.

        Returns None if PySpark is not available.
        """
        if not PYSPARK_AVAILABLE:
            return None

        fields = []
        for col in self.all_columns:
            if col.spark_type is not None:
                fields.append(StructField(col.name, col.spark_type, col.nullable))

        return StructType(fields) if fields else None


# =============================================================================
# CONVENIENCE ALIASES (backward compatibility)
# =============================================================================

# Aliases for backward compatibility with the original file
BaseColumn = Column
BaseTable = Table
