"""
Enum definitions for Unity Catalog governance models.

This module contains all enumeration types used throughout the governance system.
"""

from enum import Enum
from typing import Dict, List, Set

# FlexibleFieldMixin not needed in enums module


class SecurableType(str, Enum):
    """Identifies the type of Unity Catalog object for privilege management."""
    METASTORE = "METASTORE"
    CATALOG = "CATALOG"
    SCHEMA = "SCHEMA"
    TABLE = "TABLE"
    VOLUME = "VOLUME"
    FUNCTION = "FUNCTION"
    MODEL = "MODEL"  # ML models in Unity Catalog
    SERVICE_CREDENTIAL = "SERVICE_CREDENTIAL"  # AI/ML service credentials
    STORAGE_CREDENTIAL = "STORAGE_CREDENTIAL"
    EXTERNAL_LOCATION = "EXTERNAL_LOCATION"
    CONNECTION = "CONNECTION"
    SHARE = "SHARE"  # Delta Sharing shares
    RECIPIENT = "RECIPIENT"  # Delta Sharing recipients
    PROVIDER = "PROVIDER"  # Delta Sharing providers
    PIPELINE = "PIPELINE"  # Delta Live Tables pipelines

    # AI/ML Assets
    GENIE_SPACE = "GENIE_SPACE"  # Databricks Genie Spaces
    VECTOR_SEARCH_ENDPOINT = "VECTOR_SEARCH_ENDPOINT"  # Vector Search endpoints
    VECTOR_SEARCH_INDEX = "VECTOR_SEARCH_INDEX"  # Vector Search indexes


class PrivilegeType(str, Enum):
    """
    Complete list of Unity Catalog privileges from Databricks SDK.

    IMPORTANT:
    - ALL_PRIVILEGES expands at runtime to all applicable privileges (except MANAGE)
    - Privileges are ALWAYS ADDITIVE - multiple grants accumulate
    - To remove privileges, you must explicitly REVOKE
    - BROWSE is a metadata-only privilege for discovery without data access
    """
    # General privileges
    ACCESS = "ACCESS"
    ALL_PRIVILEGES = "ALL_PRIVILEGES"  # Expands to all applicable privileges
    APPLY_TAG = "APPLY_TAG"
    BROWSE = "BROWSE"  # Metadata discovery privilege
    CREATE = "CREATE"
    MANAGE = "MANAGE"  # Full control including grant management
    MANAGE_ALLOWLIST = "MANAGE_ALLOWLIST"
    USAGE = "USAGE"

    # Catalog privileges
    USE_CATALOG = "USE_CATALOG"
    CREATE_CATALOG = "CREATE_CATALOG"
    CREATE_SCHEMA = "CREATE_SCHEMA"
    CREATE_FOREIGN_CATALOG = "CREATE_FOREIGN_CATALOG"

    # Schema privileges
    USE_SCHEMA = "USE_SCHEMA"
    CREATE_TABLE = "CREATE_TABLE"
    CREATE_VOLUME = "CREATE_VOLUME"
    CREATE_FUNCTION = "CREATE_FUNCTION"
    CREATE_MODEL = "CREATE_MODEL"
    CREATE_VIEW = "CREATE_VIEW"
    CREATE_MATERIALIZED_VIEW = "CREATE_MATERIALIZED_VIEW"
    CREATE_FOREIGN_SECURABLE = "CREATE_FOREIGN_SECURABLE"

    # Table/View privileges
    SELECT = "SELECT"
    MODIFY = "MODIFY"
    REFRESH = "REFRESH"  # For materialized views and streaming tables

    # Volume privileges
    READ_VOLUME = "READ_VOLUME"
    WRITE_VOLUME = "WRITE_VOLUME"

    # Function privileges
    EXECUTE = "EXECUTE"

    # Storage/External Location privileges
    CREATE_EXTERNAL_TABLE = "CREATE_EXTERNAL_TABLE"
    CREATE_EXTERNAL_VOLUME = "CREATE_EXTERNAL_VOLUME"
    CREATE_MANAGED_STORAGE = "CREATE_MANAGED_STORAGE"
    CREATE_STORAGE_CREDENTIAL = "CREATE_STORAGE_CREDENTIAL"
    CREATE_EXTERNAL_LOCATION = "CREATE_EXTERNAL_LOCATION"
    READ_FILES = "READ_FILES"
    WRITE_FILES = "WRITE_FILES"
    READ_PRIVATE_FILES = "READ_PRIVATE_FILES"
    WRITE_PRIVATE_FILES = "WRITE_PRIVATE_FILES"

    # Connection privileges
    USE_CONNECTION = "USE_CONNECTION"
    CREATE_CONNECTION = "CREATE_CONNECTION"

    # Service credential privileges
    CREATE_SERVICE_CREDENTIAL = "CREATE_SERVICE_CREDENTIAL"

    # Delta Sharing privileges
    CREATE_PROVIDER = "CREATE_PROVIDER"
    USE_PROVIDER = "USE_PROVIDER"
    CREATE_RECIPIENT = "CREATE_RECIPIENT"
    USE_RECIPIENT = "USE_RECIPIENT"
    CREATE_SHARE = "CREATE_SHARE"
    USE_SHARE = "USE_SHARE"
    SET_SHARE_PERMISSION = "SET_SHARE_PERMISSION"

    # Clean Room privileges
    CREATE_CLEAN_ROOM = "CREATE_CLEAN_ROOM"
    MODIFY_CLEAN_ROOM = "MODIFY_CLEAN_ROOM"
    EXECUTE_CLEAN_ROOM_TASK = "EXECUTE_CLEAN_ROOM_TASK"

    # Marketplace privileges
    USE_MARKETPLACE_ASSETS = "USE_MARKETPLACE_ASSETS"


class Environment(str, Enum):
    """
    Deployment environments with standard suffixes.

    IMPORTANT: Enum values are UPPERCASE but suffixes use lowercase.
    Example: Environment.DEV -> suffix "_dev"
    """
    DEV = "DEV"
    ACC = "ACC"  # Acceptance environment
    PRD = "PRD"  # Production


class BindingType(str, Enum):
    """Workspace binding access levels for catalogs and other securables."""
    BINDING_TYPE_READ_WRITE = "BINDING_TYPE_READ_WRITE"
    BINDING_TYPE_READ_ONLY = "BINDING_TYPE_READ_ONLY"


class IsolationMode(str, Enum):
    """Catalog isolation configuration for workspace access."""
    OPEN = "OPEN"  # Accessible from all bound workspaces
    ISOLATED = "ISOLATED"  # Restricted to specific workspaces


class TableType(str, Enum):
    """Types of tables in Unity Catalog."""
    MANAGED = "MANAGED"
    EXTERNAL = "EXTERNAL"
    VIEW = "VIEW"
    MATERIALIZED_VIEW = "MATERIALIZED_VIEW"
    STREAMING_TABLE = "STREAMING_TABLE"


class VolumeType(str, Enum):
    """Types of volumes in Unity Catalog."""
    MANAGED = "MANAGED"
    EXTERNAL = "EXTERNAL"


class FunctionType(str, Enum):
    """Types of functions in Unity Catalog."""
    SQL = "SQL"
    PYTHON = "PYTHON"
    SCALAR = "SCALAR"
    TABLE = "TABLE"


class ConnectionType(str, Enum):
    """Types of external connections."""
    MYSQL = "MYSQL"
    POSTGRESQL = "POSTGRESQL"
    SNOWFLAKE = "SNOWFLAKE"
    REDSHIFT = "REDSHIFT"
    SQLDW = "SQLDW"  # Azure Synapse
    SQLSERVER = "SQLSERVER"
    DATABRICKS = "DATABRICKS"  # Cross-workspace


# =============================================================================
# PRIVILEGE DEPENDENCIES
# =============================================================================

# Define privilege dependencies - what privileges require other privileges
PRIVILEGE_DEPENDENCIES: Dict[PrivilegeType, Set[PrivilegeType]] = {
    # Table operations require schema and catalog access
    PrivilegeType.CREATE_TABLE: {PrivilegeType.USE_SCHEMA},
    PrivilegeType.SELECT: {PrivilegeType.USE_SCHEMA, PrivilegeType.USE_CATALOG},
    PrivilegeType.MODIFY: {PrivilegeType.USE_SCHEMA, PrivilegeType.USE_CATALOG},

    # Schema operations require catalog access
    PrivilegeType.CREATE_SCHEMA: {PrivilegeType.USE_CATALOG},
    PrivilegeType.CREATE_VIEW: {PrivilegeType.USE_SCHEMA, PrivilegeType.USE_CATALOG},
    PrivilegeType.CREATE_MATERIALIZED_VIEW: {PrivilegeType.USE_SCHEMA, PrivilegeType.USE_CATALOG},
    PrivilegeType.CREATE_FUNCTION: {PrivilegeType.USE_SCHEMA, PrivilegeType.USE_CATALOG},
    PrivilegeType.CREATE_VOLUME: {PrivilegeType.USE_SCHEMA, PrivilegeType.USE_CATALOG},

    # Volume operations require schema access
    PrivilegeType.READ_VOLUME: {PrivilegeType.USE_SCHEMA, PrivilegeType.USE_CATALOG},
    PrivilegeType.WRITE_VOLUME: {PrivilegeType.USE_SCHEMA, PrivilegeType.USE_CATALOG, PrivilegeType.READ_VOLUME},

    # External location operations
    PrivilegeType.CREATE_EXTERNAL_LOCATION: {PrivilegeType.CREATE_STORAGE_CREDENTIAL},

    # Function operations
    PrivilegeType.EXECUTE: {PrivilegeType.USE_SCHEMA, PrivilegeType.USE_CATALOG},
}


def validate_privilege_dependencies(
    privileges: Set[PrivilegeType],
    existing_privileges: Set[PrivilegeType]
) -> List[str]:
    """
    Validate that all privilege dependencies are satisfied.

    Args:
        privileges: Set of privileges to grant
        existing_privileges: Set of privileges already granted

    Returns:
        List of validation error messages (empty if valid)
    """
    errors = []
    all_privileges = privileges | existing_privileges

    for priv in privileges:
        if priv in PRIVILEGE_DEPENDENCIES:
            required = PRIVILEGE_DEPENDENCIES[priv]
            missing = required - all_privileges
            if missing:
                errors.append(
                    f"Privilege {priv.value} requires: {', '.join(p.value for p in missing)}"
                )

    return errors


# ALL_PRIVILEGES expansion for each securable type
# Based on Unity Catalog documentation
ALL_PRIVILEGES_EXPANSION = {
    SecurableType.CATALOG: [
        PrivilegeType.USE_CATALOG,
        PrivilegeType.CREATE_SCHEMA,
        PrivilegeType.CREATE_TABLE,
        PrivilegeType.CREATE_VOLUME,
        PrivilegeType.CREATE_FUNCTION,
        PrivilegeType.CREATE_MODEL,
        PrivilegeType.SELECT,
        PrivilegeType.MODIFY,
        PrivilegeType.REFRESH,
        PrivilegeType.EXECUTE,
        PrivilegeType.READ_VOLUME,
        PrivilegeType.WRITE_VOLUME,
    ],
    SecurableType.SCHEMA: [
        PrivilegeType.USE_SCHEMA,
        PrivilegeType.CREATE_TABLE,
        PrivilegeType.CREATE_VOLUME,
        PrivilegeType.CREATE_FUNCTION,
        PrivilegeType.CREATE_MODEL,
        PrivilegeType.SELECT,
        PrivilegeType.MODIFY,
        PrivilegeType.REFRESH,
        PrivilegeType.EXECUTE,
        PrivilegeType.READ_VOLUME,
        PrivilegeType.WRITE_VOLUME,
    ],
    SecurableType.TABLE: [
        PrivilegeType.SELECT,
        PrivilegeType.MODIFY,
        PrivilegeType.REFRESH,
    ],
    SecurableType.VOLUME: [
        PrivilegeType.READ_VOLUME,
        PrivilegeType.WRITE_VOLUME,
    ],
    SecurableType.FUNCTION: [
        PrivilegeType.EXECUTE,
    ],
    SecurableType.STORAGE_CREDENTIAL: [
        PrivilegeType.CREATE_EXTERNAL_LOCATION,
        PrivilegeType.CREATE_EXTERNAL_TABLE,
        PrivilegeType.CREATE_EXTERNAL_VOLUME,
        PrivilegeType.READ_FILES,
        PrivilegeType.WRITE_FILES,
    ],
    SecurableType.EXTERNAL_LOCATION: [
        PrivilegeType.CREATE_EXTERNAL_TABLE,
        PrivilegeType.CREATE_EXTERNAL_VOLUME,
        PrivilegeType.READ_FILES,
        PrivilegeType.WRITE_FILES,
    ],
    SecurableType.CONNECTION: [
        PrivilegeType.USE_CONNECTION,
        PrivilegeType.CREATE_FOREIGN_CATALOG,
    ],
    SecurableType.MODEL: [
        PrivilegeType.EXECUTE,
        PrivilegeType.APPLY_TAG,
        PrivilegeType.MANAGE,  # Full management of model
    ],
    SecurableType.SERVICE_CREDENTIAL: [
        PrivilegeType.ACCESS,  # Access to use the service credential
        PrivilegeType.MANAGE,  # Full management of service credential
    ],
    SecurableType.SHARE: [
        PrivilegeType.SELECT,  # Can view share contents
    ],
    SecurableType.RECIPIENT: [
        PrivilegeType.USE_RECIPIENT,  # Can activate recipient
    ],
    SecurableType.PROVIDER: [
        PrivilegeType.USE_PROVIDER,  # Can use provider
    ],
    # AI/ML Assets
    SecurableType.GENIE_SPACE: [
        PrivilegeType.ACCESS,  # Can access the Genie Space
        PrivilegeType.MANAGE,  # Full management of Genie Space
    ],
    SecurableType.VECTOR_SEARCH_ENDPOINT: [
        PrivilegeType.ACCESS,  # Can access the endpoint
        PrivilegeType.MANAGE,  # Full management of endpoint
    ],
    SecurableType.VECTOR_SEARCH_INDEX: [
        PrivilegeType.ACCESS,  # Can query the index
        PrivilegeType.MANAGE,  # Full management of index
    ],
}
