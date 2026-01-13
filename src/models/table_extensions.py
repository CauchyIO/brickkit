"""
DEPRECATED: Extensions to Table model for ETL-friendly operations.

This module is deprecated. Use Databricks Asset Bundles (DABs) for table creation
and management. DBRCDK now focuses on governance, not object creation.

Migration guide:
1. For table creation: Use DABs
2. For governance: Use TableReference from dbrcdk.models.references
"""

from __future__ import annotations
from typing import Dict, Any, Optional, List
from enum import Enum
import logging
import warnings

from .deprecated import Table  # Using deprecated model for backward compatibility
from .securables import ColumnInfo
from .enums import TableType

logger = logging.getLogger(__name__)

# Show deprecation warning when module is imported
warnings.warn(
    "table_extensions module is deprecated. Use Databricks Asset Bundles (DABs) "
    "for table creation and TableReference for governance.",
    DeprecationWarning,
    stacklevel=2
)


class TableCreationMode(Enum):
    """Defines how a table should be created."""
    SDK_EXTERNAL = "sdk_external"  # SDK API for external tables (from orchestrator)
    SQL_DDL = "sql_ddl"  # SQL DDL for any table type (in notebooks/jobs)
    AUTO = "auto"  # Automatically determine based on context


class ETLTable(Table):
    """
    Extended Table class that supports both SDK and SQL DDL creation patterns.
    
    This class extends the base Table model to provide:
    1. SQL DDL generation for MANAGED tables in notebooks
    2. SDK creation for EXTERNAL tables from orchestrator
    3. Automatic mode detection based on runtime context
    """
    
    # Additional fields for ETL workflows
    partition_cols: List[str] = []
    cluster_cols: List[str] = []
    table_properties: Dict[str, str] = {}
    tbl_properties: Dict[str, str] = {}  # TBLPROPERTIES for SQL DDL
    location: Optional[str] = None  # Explicit location for EXTERNAL tables
    using: str = "DELTA"  # File format (DELTA, PARQUET, CSV, JSON)
    
    def detect_creation_mode(self) -> TableCreationMode:
        """
        Detect the appropriate creation mode based on runtime context.
        
        Returns:
            TableCreationMode based on environment detection
        """
        try:
            # Try to detect if we're in a Databricks notebook/job
            import IPython
            get_ipython = IPython.get_ipython()
            if get_ipython and 'dbutils' in dir(get_ipython.user_ns):
                # We're in a Databricks notebook
                return TableCreationMode.SQL_DDL
        except (ImportError, AttributeError):
            pass
        
        # Check if we're in a Databricks job (spark session available)
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.getActiveSession()
            if spark:
                return TableCreationMode.SQL_DDL
        except (ImportError, AttributeError):
            pass
        
        # Default to SDK for external orchestration
        if self.table_type == TableType.EXTERNAL:
            return TableCreationMode.SDK_EXTERNAL
        else:
            # MANAGED tables must use SQL DDL
            logger.warning(f"Table {self.name} is MANAGED but running outside Databricks - will generate SQL DDL")
            return TableCreationMode.SQL_DDL
    
    def to_sql_ddl(self, if_not_exists: bool = True) -> str:
        """
        Generate SQL DDL CREATE TABLE statement.
        
        This method generates standard SQL DDL that can be executed in:
        - Databricks notebooks
        - Databricks jobs
        - SQL warehouses
        - Any Spark SQL context
        
        Args:
            if_not_exists: Whether to include IF NOT EXISTS clause
            
        Returns:
            SQL DDL CREATE TABLE statement
        """
        ddl_parts = []
        
        # CREATE TABLE statement
        create_clause = "CREATE TABLE"
        if if_not_exists:
            create_clause += " IF NOT EXISTS"
        
        if self.table_type == TableType.EXTERNAL:
            create_clause += " EXTERNAL"
        
        ddl_parts.append(f"{create_clause} {self.fqdn}")
        
        # Column definitions
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
        
        # USING clause (file format)
        ddl_parts.append(f"USING {self.using}")
        
        # PARTITIONED BY clause
        if self.partition_cols:
            ddl_parts.append(f"PARTITIONED BY ({', '.join(self.partition_cols)})")
        
        # CLUSTERED BY clause (Z-ordering)
        if self.cluster_cols:
            ddl_parts.append(f"CLUSTERED BY ({', '.join(self.cluster_cols)})")
            ddl_parts.append("INTO 256 BUCKETS")  # Default bucket count
        
        # LOCATION clause for external tables
        if self.table_type == TableType.EXTERNAL:
            if self.location:
                ddl_parts.append(f"LOCATION '{self.location}'")
            elif self.external_location:
                # Use external location's URL
                location_url = f"{self.external_location.url}/{self.name}"
                ddl_parts.append(f"LOCATION '{location_url}'")
        
        # COMMENT clause
        if self.comment:
            ddl_parts.append(f"COMMENT '{self.comment}'")
        
        # TBLPROPERTIES clause
        if self.tbl_properties:
            props = [f"'{k}' = '{v}'" for k, v in self.tbl_properties.items()]
            ddl_parts.append(f"TBLPROPERTIES ({', '.join(props)})")
        
        return "\n".join(ddl_parts)
    
    def to_sql_alter_owner(self) -> Optional[str]:
        """
        Generate SQL ALTER TABLE SET OWNER statement.
        
        Returns:
            SQL ALTER TABLE statement or None if no owner specified
        """
        if self.owner:
            return f"ALTER TABLE {self.fqdn} SET OWNER TO `{self.owner.resolved_name}`"
        return None
    
    def to_sql_grants(self) -> List[str]:
        """
        Generate SQL GRANT statements for table privileges.
        
        Returns:
            List of SQL GRANT statements
        """
        grants = []
        for privilege in self.privileges:
            priv_str = privilege.privilege.value
            principal = privilege.principal
            
            grants.append(f"GRANT {priv_str} ON TABLE {self.fqdn} TO `{principal}`")
        
        return grants
    
    def to_creation_script(self, mode: Optional[TableCreationMode] = None) -> str:
        """
        Generate complete creation script based on mode.
        
        Args:
            mode: Creation mode (auto-detected if None)
            
        Returns:
            Python code or SQL script for table creation
        """
        if mode is None:
            mode = self.detect_creation_mode()
        
        if mode == TableCreationMode.SQL_DDL:
            # Generate SQL script
            script_parts = []
            
            # Add header comment
            script_parts.append(f"-- Create table: {self.fqdn}")
            script_parts.append("-- Generated from ETLTable model")
            script_parts.append("")
            
            # Add DDL
            script_parts.append(self.to_sql_ddl())
            script_parts.append("")
            
            # Add owner change if needed
            if owner_sql := self.to_sql_alter_owner():
                script_parts.append(owner_sql)
                script_parts.append("")
            
            # Add grants
            if grants := self.to_sql_grants():
                script_parts.append("-- Apply grants")
                script_parts.extend(grants)
            
            return "\n".join(script_parts)
        
        elif mode == TableCreationMode.SDK_EXTERNAL:
            # Generate Python SDK code
            script_parts = []
            script_parts.append(f"# Create external table: {self.fqdn}")
            script_parts.append("from databricks.sdk import WorkspaceClient")
            script_parts.append("client = WorkspaceClient()")
            script_parts.append("")
            script_parts.append("# Create table via SDK")
            script_parts.append(f"params = {self.to_sdk_create_params()}")
            script_parts.append("client.tables.create(**params)")
            
            return "\n".join(script_parts)
        
        else:
            # AUTO mode - return both options
            return f"""# Auto-detected mode: Choose based on your context

# Option 1: SQL DDL (for notebooks/jobs)
# Run this in a notebook cell with %sql magic command:
{self.to_sql_ddl()}

# Option 2: SDK Creation (for external orchestration)
# Run this Python code:
from databricks.sdk import WorkspaceClient
client = WorkspaceClient()
params = {self.to_sdk_create_params()}
client.tables.create(**params)
"""
    
    def validate_for_etl(self) -> List[str]:
        """
        Validate table configuration for ETL workflows.
        
        Returns:
            List of validation warnings/errors
        """
        issues = []
        
        # Check MANAGED table creation context
        if self.table_type == TableType.MANAGED:
            mode = self.detect_creation_mode()
            if mode == TableCreationMode.SDK_EXTERNAL:
                issues.append(
                    f"MANAGED table '{self.name}' cannot be created via SDK from outside Databricks. "
                    "Use SQL DDL in a notebook/job or change to EXTERNAL table."
                )
        
        # Validate external table has location
        if self.table_type == TableType.EXTERNAL:
            if not self.location and not self.external_location:
                issues.append(
                    f"EXTERNAL table '{self.name}' requires either 'location' or 'external_location' to be set"
                )
        
        # Check for partition columns exist in schema
        if self.partition_cols:
            col_names = {col.name for col in self.columns}
            missing = set(self.partition_cols) - col_names
            if missing:
                issues.append(f"Partition columns not in schema: {missing}")
        
        # Validate cluster columns exist
        if self.cluster_cols:
            col_names = {col.name for col in self.columns}
            missing = set(self.cluster_cols) - col_names  
            if missing:
                issues.append(f"Cluster columns not in schema: {missing}")
        
        return issues


class TableFactory:
    """
    Factory for creating tables with appropriate settings based on context.
    """
    
    @staticmethod
    def create_managed_table(
        name: str,
        schema_name: str,
        catalog_name: str,
        columns: List[ColumnInfo],
        **kwargs
    ) -> ETLTable:
        """
        Create a MANAGED table optimized for ETL.
        
        Args:
            name: Table name
            schema_name: Schema name
            catalog_name: Catalog name  
            columns: Column definitions
            **kwargs: Additional table properties
            
        Returns:
            ETLTable configured as MANAGED
        """
        return ETLTable(
            name=name,
            schema_name=schema_name,
            catalog_name=catalog_name,
            table_type=TableType.MANAGED,
            columns=columns,
            **kwargs
        )
    
    @staticmethod
    def create_external_table(
        name: str,
        schema_name: str,
        catalog_name: str,
        columns: List[ColumnInfo],
        location: Optional[str] = None,
        external_location: Optional[Any] = None,
        **kwargs
    ) -> ETLTable:
        """
        Create an EXTERNAL table that can be created from anywhere.
        
        Args:
            name: Table name
            schema_name: Schema name
            catalog_name: Catalog name
            columns: Column definitions
            location: Explicit storage location
            external_location: ExternalLocation object
            **kwargs: Additional table properties
            
        Returns:
            ETLTable configured as EXTERNAL
        """
        return ETLTable(
            name=name,
            schema_name=schema_name,
            catalog_name=catalog_name,
            table_type=TableType.EXTERNAL,
            columns=columns,
            location=location,
            external_location=external_location,
            **kwargs
        )
    
    @staticmethod
    def from_base_table(table: Table) -> ETLTable:
        """
        Convert a base Table to an ETLTable.
        
        Args:
            table: Base Table object
            
        Returns:
            ETLTable with same configuration
        """
        # Copy all base table attributes
        etl_table = ETLTable(
            name=table.name,
            table_type=table.table_type,
            owner=table.owner,
            columns=table.columns,
            external_location=table.external_location,
            row_filter=table.row_filter,
            column_masks=table.column_masks,
            comment=table.comment,
            catalog_name=table.catalog_name,
            schema_name=table.schema_name,
        )
        
        # Copy private attributes if they exist
        if hasattr(table, '_parent_schema'):
            etl_table._parent_schema = table._parent_schema
        if hasattr(table, '_parent_catalog'):
            etl_table._parent_catalog = table._parent_catalog
        
        # Copy privileges
        etl_table.privileges = table.privileges.copy()
        
        return etl_table