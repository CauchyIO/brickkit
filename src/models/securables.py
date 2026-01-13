"""
Unity Catalog securable objects.

This module contains all securable objects in the Unity Catalog hierarchy:
- Level 0: Metastore
- Level 1: Catalog, StorageCredential, ExternalLocation, Connection
- Level 2: Schema  
- Level 3: Table, Volume, Function, RegisteredModel

It also includes supporting models like ColumnInfo.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, TYPE_CHECKING
from typing_extensions import Self

if TYPE_CHECKING:
    from .references import TableReference, ModelReference, VolumeReference, FunctionReference

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PrivateAttr,
    computed_field,
    field_validator,
    model_validator,
)

from .base import (
    BaseGovernanceModel, BaseSecurable, Tag,
    DEFAULT_SECURABLE_OWNER, get_current_environment
)
from .enums import (
    Environment, SecurableType, PrivilegeType,
    TableType, VolumeType, FunctionType, ConnectionType,
    IsolationMode, ALL_PRIVILEGES_EXPANSION
)
from .access import Principal, Privilege, AccessPolicy, Team, Workspace

# Try importing FlexibleFieldMixin
try:
    from ..mixins.flexible_fields import FlexibleFieldMixin
except ImportError:
    # If mixin doesn't exist, create a simple placeholder
    class FlexibleFieldMixin:
        pass

# Configure logging
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
        import keyword
        # Check for Python/SQL reserved words
        sql_reserved = {'SELECT', 'FROM', 'WHERE', 'ORDER', 'GROUP', 'TABLE', 'INDEX'}
        if v.upper() in sql_reserved or keyword.iskeyword(v):
            logger.warning(f"Column name '{v}' is a reserved word - consider renaming")
        return v


# =============================================================================
# TYPED CREDENTIAL MODELS
# =============================================================================

class AWSIamRole(BaseModel):
    """AWS IAM role configuration for storage access."""
    role_arn: str = Field(
        ...,
        pattern=r'^arn:aws:iam::\d{12}:role/[\w+=,.@/-]+$',
        description="AWS IAM role ARN"
    )
    external_id: Optional[str] = Field(None, description="External ID for assume role")
    unity_catalog_iam_arn: Optional[str] = Field(None, description="Unity Catalog IAM ARN for trust policy")


class AzureServicePrincipal(BaseModel):
    """Azure service principal configuration for storage access."""
    directory_id: str = Field(
        ...,
        pattern=r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
        description="Azure Active Directory tenant ID"
    )
    application_id: str = Field(
        ...,
        pattern=r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
        description="Azure service principal application ID"  
    )
    client_secret: Optional[str] = Field(None, description="Client secret (write-only)")


class AzureManagedIdentity(BaseModel):
    """Azure managed identity configuration for storage access."""
    access_connector_id: str = Field(
        ...,
        pattern=r'^/subscriptions/[^/]+/resourceGroups/[^/]+/providers/Microsoft\.Databricks/accessConnectors/[^/]+$',
        description="Azure access connector resource ID"
    )
    managed_identity_id: Optional[str] = Field(None, description="Specific managed identity ID if using user-assigned")


class GCPServiceAccountKey(BaseModel):
    """GCP service account key configuration for storage access."""
    email: str = Field(
        ...,
        pattern=r'^[a-z0-9-]+@[a-z0-9-]+\.iam\.gserviceaccount\.com$',
        description="Service account email"
    )
    private_key_id: str = Field(..., description="Private key ID")
    private_key: str = Field(..., description="Private key (PEM format, write-only)")


# =============================================================================
# STORAGE CREDENTIAL  
# =============================================================================

class StorageCredential(BaseSecurable):
    """
    Defines authentication for cloud storage access.
    
    StorageCredential is a level-1 object that manages cloud provider
    authentication (AWS IAM roles, Azure service principals, etc.).
    ExternalLocations reference these credentials to access storage.
    """
    name: str = Field(
        ...,
        pattern=r'^[a-zA-Z0-9][a-zA-Z0-9_-]*$',
        min_length=1,
        max_length=255,
        description="Storage credential name (base name without environment suffix)"
    )
    
    # Cloud provider credentials (exactly one must be set)
    aws_iam_role: Optional[AWSIamRole] = Field(None, description="AWS IAM role configuration")
    azure_service_principal: Optional[AzureServicePrincipal] = Field(None, description="Azure service principal")
    azure_managed_identity: Optional[AzureManagedIdentity] = Field(None, description="Azure managed identity")
    gcp_service_account_key: Optional[GCPServiceAccountKey] = Field(None, description="GCP service account key")
    
    # Common fields
    owner: Optional[Principal] = Field(
        default_factory=lambda: Principal(name=DEFAULT_SECURABLE_OWNER, add_environment_suffix=False),
        description="Owner principal"
    )
    comment: Optional[str] = Field(None, max_length=1024, description="Description")
    read_only: bool = Field(False, description="Whether credential is read-only")
    is_existing: bool = Field(
        default=False,
        description="If True, credential already exists and name won't get environment suffix"
    )

    # Workspace isolation (optional)
    workspace_ids: List[int] = Field(
        default_factory=list,
        description="List of workspace IDs that can access this storage credential (empty = all workspaces)"
    )

    @model_validator(mode='after')
    def validate_exactly_one_credential(self) -> Self:
        """Ensure exactly one credential type is specified."""
        creds = [
            self.aws_iam_role,
            self.azure_service_principal,
            self.azure_managed_identity,
            self.gcp_service_account_key
        ]
        if sum(c is not None for c in creds) != 1:
            raise ValueError("Exactly one credential type must be specified")
        return self
    
    @computed_field  # type: ignore[prop-decorator]
    @property
    def resolved_name(self) -> str:
        """Name with environment suffix (unless is_existing=True)."""
        if self.is_existing:
            # Existing credentials don't get environment suffix
            return self.name
        env = get_current_environment()
        return f"{self.name}_{env.value.lower()}"
    
    @property
    def securable_type(self) -> SecurableType:
        """Return the securable type."""
        return SecurableType.STORAGE_CREDENTIAL
    
    def to_sdk_create_params(self) -> Dict[str, Any]:
        """Convert to SDK create parameters."""
        params = {
            "name": self.resolved_name,
            "comment": self.comment
        }
        
        # Add the appropriate credential configuration
        if self.aws_iam_role:
            params["aws_iam_role"] = {
                "role_arn": self.aws_iam_role.role_arn,
                "external_id": self.aws_iam_role.external_id,
                "unity_catalog_iam_arn": self.aws_iam_role.unity_catalog_iam_arn
            }
        elif self.azure_service_principal:
            params["azure_service_principal"] = {
                "directory_id": self.azure_service_principal.directory_id,
                "application_id": self.azure_service_principal.application_id,
                "client_secret": self.azure_service_principal.client_secret
            }
        elif self.azure_managed_identity:
            params["azure_managed_identity"] = {
                "access_connector_id": self.azure_managed_identity.access_connector_id,
                "managed_identity_id": self.azure_managed_identity.managed_identity_id
            }
        elif self.gcp_service_account_key:
            params["gcp_service_account_key"] = {
                "email": self.gcp_service_account_key.email,
                "private_key_id": self.gcp_service_account_key.private_key_id,
                "private_key": self.gcp_service_account_key.private_key
            }
        
        return params
    
    def to_sdk_update_params(self) -> Dict[str, Any]:
        """Convert to SDK update parameters."""
        params = {
            "name": self.resolved_name,
            "comment": self.comment
        }
        
        # Credential updates require full replacement
        if self.aws_iam_role:
            params["aws_iam_role"] = {
                "role_arn": self.aws_iam_role.role_arn
            }
        # Similar for other credential types...
        
        return params


# =============================================================================
# EXTERNAL LOCATION
# =============================================================================

class ExternalLocation(BaseSecurable):
    """
    References a storage path with associated credentials.
    
    ExternalLocation is a level-1 object that combines a storage path (URL) with
    the credentials needed to access it. It's used by catalogs, schemas, tables,
    and volumes to define where their data is stored.
    """
    name: str = Field(
        ...,
        pattern=r'^[a-zA-Z0-9][a-zA-Z0-9_-]*$',
        min_length=1,
        max_length=255,
        description="External location name (base name without environment suffix)"
    )
    url: str = Field(
        ...,
        min_length=10,
        max_length=1024,
        description="Storage URL (s3://, abfss://, gs://)"
    )
    storage_credential: StorageCredential = Field(
        ...,
        description="Associated storage credential for authentication"
    )
    owner: Optional[Principal] = Field(
        default_factory=lambda: Principal(name=DEFAULT_SECURABLE_OWNER, add_environment_suffix=False),
        description="Owner principal"
    )
    comment: Optional[str] = Field(None, max_length=1024, description="Description of the external location")
    skip_validation: bool = Field(False, description="Skip path validation during creation")
    access_point: Optional[str] = Field(None, description="S3 access point ARN if applicable")

    # Workspace isolation (optional)
    workspace_ids: List[int] = Field(
        default_factory=list,
        description="List of workspace IDs that can access this external location (empty = all workspaces)"
    )

    @field_validator('url')
    @classmethod
    def validate_storage_url(cls, v: str) -> str:
        """Validate and secure storage URL format."""
        import re
        from urllib.parse import urlparse
        
        # Check for valid prefixes
        valid_prefixes = ['s3://', 'abfss://', 'gs://']
        if not any(v.startswith(prefix) for prefix in valid_prefixes):
            raise ValueError(f"Invalid storage URL. Must start with one of: {valid_prefixes}")
        
        # Security: Check for path traversal attempts
        if '..' in v or '//' in v[8:]:  # Skip protocol://
            raise ValueError("Invalid characters in storage URL - potential security risk")
        
        # Validate URL structure
        try:
            parsed = urlparse(v)
            if not parsed.netloc:
                raise ValueError("Storage URL must include a bucket/container name")
            
            # Validate based on storage type
            if v.startswith('s3://'):
                # S3 bucket naming rules (simplified)
                bucket = parsed.netloc.split('.')[0] if '.' in parsed.netloc else parsed.netloc
                if not re.match(r'^[a-z0-9][a-z0-9.-]*[a-z0-9]$', bucket):
                    raise ValueError(f"Invalid S3 bucket name: {bucket}")
            elif v.startswith('abfss://'):
                # Azure ADLS Gen2 validation
                if '@' not in parsed.netloc or not parsed.netloc.endswith('.dfs.core.windows.net'):
                    raise ValueError("Invalid Azure storage format. Expected: container@account.dfs.core.windows.net")
            elif v.startswith('gs://'):
                # GCS bucket naming rules
                bucket = parsed.netloc
                if not re.match(r'^[a-z0-9][a-z0-9._-]*[a-z0-9]$', bucket):
                    raise ValueError(f"Invalid GCS bucket name: {bucket}")
        except ValueError:
            raise
        except Exception as e:
            raise ValueError(f"Invalid storage URL format: {e}")
        
        return v.rstrip('/')  # Normalize by removing trailing slash
    
    @computed_field  # type: ignore[prop-decorator]
    @property
    def storage_root(self) -> str:
        """Returns self.url for consistency with SDK expectations."""
        return self.url
    
    @computed_field  # type: ignore[prop-decorator]
    @property
    def resolved_name(self) -> str:
        """Name with environment suffix."""
        env = get_current_environment()
        return f"{self.name}_{env.value.lower()}"
    
    @property
    def securable_type(self) -> SecurableType:
        """Return the securable type."""
        return SecurableType.EXTERNAL_LOCATION
    
    def to_sdk_create_params(self) -> Dict[str, Any]:
        """Convert to SDK create parameters."""
        params = {
            "name": self.resolved_name,
            "url": self.url,
            "credential_name": self.storage_credential.resolved_name,
            "comment": self.comment,
            "skip_validation": self.skip_validation
        }
        # Only include access_point if it's set (SDK might not support it)
        if self.access_point:
            params["access_point"] = self.access_point
        return params
    
    def to_sdk_update_params(self) -> Dict[str, Any]:
        """Convert to SDK update parameters."""
        return {
            "name": self.resolved_name,
            "url": self.url,
            "comment": self.comment
        }
    
    def _propagate_grants(self, principal: Principal, policy: AccessPolicy) -> List[Privilege]:
        """
        Propagate grants to storage credential if present.
        
        Args:
            principal: The principal to grant to
            policy: The access policy to apply
            
        Returns:
            List of propagated privileges
        """
        result = []
        
        # Propagate to storage credential if it exists
        if self.storage_credential:
            logger.debug(f"Propagating grants from external location {self.name} to storage credential")
            result.extend(self.storage_credential.grant(principal, policy, _skip_validation=True))
        
        return result


# =============================================================================
# CONNECTION
# =============================================================================

class Connection(BaseSecurable):
    """
    Defines connection to external data systems.
    
    Connection is a level-1 object that manages connections to external
    systems like MySQL, PostgreSQL, SQL Server, etc. It stores connection
    details and credentials securely.
    """
    name: str = Field(
        ...,
        pattern=r'^[a-zA-Z0-9][a-zA-Z0-9_-]*$',
        min_length=1,
        max_length=255,
        description="Connection name (base name without environment suffix)"
    )
    connection_type: ConnectionType = Field(
        ...,
        description="Type of external connection"
    )
    
    # Connection options - specific to each connection type
    options: Dict[str, str] = Field(
        default_factory=dict,
        description="Connection-specific options (host, port, database, etc.)"
    )
    
    # Common fields
    owner: Optional[Principal] = Field(
        default_factory=lambda: Principal(name=DEFAULT_SECURABLE_OWNER, add_environment_suffix=False),
        description="Owner principal"
    )
    comment: Optional[str] = Field(None, max_length=1024, description="Description")
    read_only: bool = Field(False, description="Whether connection is read-only")
    
    # Properties map for additional metadata
    properties: Dict[str, str] = Field(
        default_factory=dict,
        description="Additional properties as key-value pairs"
    )
    
    @field_validator('options')
    @classmethod
    def validate_connection_options(cls, v: Dict[str, str], info) -> Dict[str, str]:
        """Validate required options based on connection type."""
        conn_type = info.data.get('connection_type')
        
        if conn_type in [ConnectionType.MYSQL, ConnectionType.POSTGRESQL, ConnectionType.SQLSERVER]:
            required = {'host', 'port', 'database'}
            missing = required - set(v.keys())
            if missing:
                raise ValueError(f"Missing required options for {conn_type}: {missing}")
        
        # Security: Validate no plaintext passwords
        if 'password' in v:
            raise ValueError("Passwords must be stored as secrets, not in options")
        
        return v
    
    @computed_field  # type: ignore[prop-decorator]
    @property
    def resolved_name(self) -> str:
        """Name with environment suffix."""
        env = get_current_environment()
        return f"{self.name}_{env.value.lower()}"
    
    @property
    def securable_type(self) -> SecurableType:
        """Return the securable type."""
        return SecurableType.CONNECTION
    
    def to_sdk_create_params(self) -> Dict[str, Any]:
        """Convert to SDK create parameters."""
        params = {
            "name": self.resolved_name,
            "connection_type": self.connection_type.value,
            "comment": self.comment
        }
        
        # Add connection options
        if self.options:
            params["options"] = self.options
        
        # Add properties
        if self.properties:
            params["properties"] = self.properties
        
        return params
    
    def to_sdk_update_params(self) -> Dict[str, Any]:
        """Convert to SDK update parameters."""
        params = {
            "name": self.resolved_name,
            "comment": self.comment
        }
        
        # Options can be updated
        if self.options:
            params["options"] = self.options
        
        return params



# =============================================================================
# SCHEMA
# =============================================================================

class Schema(FlexibleFieldMixin, BaseSecurable):
    """
    Second layer of Unity Catalog's namespace.
    
    Schemas (also known as databases) organize tables, volumes, and functions
    within a catalog. They inherit storage locations and owner from their parent
    catalog if not explicitly set.
    """
    name: str = Field(
        ...,
        pattern=r'^[a-zA-Z0-9][a-zA-Z0-9_-]*$',
        description="Schema name (no environment suffix - parent catalog has it)"
    )
    comment: Optional[str] = Field(None, max_length=1024, description="Description of the schema")
    owner: Optional[Principal] = Field(
        default_factory=lambda: Principal(name=DEFAULT_SECURABLE_OWNER, add_environment_suffix=False),
        description="Owner principal"
    )
    external_location: Optional[ExternalLocation] = Field(
        None,
        description="Optional external storage (inherited from catalog if not set)"
    )
    
    # Child containers
    tables: List[Table] = Field(default_factory=list, description="Child tables")
    volumes: List[Volume] = Field(default_factory=list, description="Child volumes")
    functions: List[Function] = Field(default_factory=list, description="Child functions")
    models: List[Any] = Field(default_factory=list, description="Child ML models")  # RegisteredModel type
    
    # Reference collections for lightweight governance (objects created by DABs/MLflow)
    table_refs: List['TableReference'] = Field(default_factory=list, description="References to discovered tables")
    model_refs: List['ModelReference'] = Field(default_factory=list, description="References to discovered models")
    volume_refs: List['VolumeReference'] = Field(default_factory=list, description="References to discovered volumes")
    function_refs: List['FunctionReference'] = Field(default_factory=list, description="References to discovered functions")
    
    # Grant tracking
    # Private parent reference (not serialized)
    _parent_catalog: Optional[Catalog] = PrivateAttr(default=None)
    
    
    catalog_name: Optional[str] = Field(
        None,
        description="Parent catalog name (set by add_schema)"
    )
    
    @computed_field  # type: ignore[prop-decorator]
    @property
    def storage_root(self) -> Optional[str]:
        """
        Returns external_location.url for SDK export.
        
        This is a computed field that derives its value from external_location.
        It's included in serialization but cannot be set directly.
        """
        return self.external_location.url if self.external_location else None
    
    @computed_field  # type: ignore[prop-decorator]
    @property
    def resolved_catalog_name(self) -> str:
        """Get catalog name with environment suffix for runtime resolution."""
        if self._parent_catalog:
            return self._parent_catalog.resolved_name
        if not self.catalog_name:
            raise ValueError(f"Schema '{self.name}' is not associated with a catalog")
        # Fallback to string field with current env if no parent
        env = get_current_environment()
        return f"{self.catalog_name}_{env.value.lower()}"
    
    @computed_field  # type: ignore[prop-decorator]
    @property
    def fqdn(self) -> str:
        """Fully qualified domain name (catalog.schema format)."""
        if not self.catalog_name:
            raise ValueError(f"Schema '{self.name}' is not associated with a catalog")
        # Use resolved catalog name for FQDN
        return f"{self.resolved_catalog_name}.{self.name}"
    
    @property
    def full_name(self) -> str:
        """Alias for fqdn for consistency with reference models."""
        return self.fqdn
    
    def add_table(self, table: Table) -> None:
        """
        Add table with duplicate check.
        
        Args:
            table: The table to add
            
        Raises:
            ValueError: If table with same name already exists
        """
        if any(t.name == table.name for t in self.tables):
            raise ValueError(f"Table '{table.name}' already exists in schema '{self.name}'")
        self.tables.append(table)
        # Set parent references and names
        # Only set catalog_name if we have a catalog association
        if self._parent_catalog:
            table.catalog_name = self._parent_catalog.name  # Use base name from parent
        elif self.catalog_name:
            table.catalog_name = self.catalog_name  # Use the catalog_name field (base name)
        table.schema_name = self.name  # Set schema name
        table._parent_schema = self  # Set private parent reference
        table._parent_catalog = self._parent_catalog  # Pass catalog reference
        # Inherit external location if table doesn't have one
        if table.table_type == TableType.EXTERNAL:
            if not table.external_location and self.external_location:
                table.external_location = self.external_location
            elif not table.external_location:
                raise ValueError(f"External table '{table.name}' requires an external_location and schema '{self.name}' doesn't have one to inherit")
        # Inherit owner if table has default owner or None
        if not table.owner or (table.owner and table.owner.name == DEFAULT_SECURABLE_OWNER):
            table.owner = self.owner
        
        # Inheritance is handled inline above
    
    def add_volume(self, volume: Volume) -> None:
        """
        Add volume with duplicate check.
        
        Args:
            volume: The volume to add
            
        Raises:
            ValueError: If volume with same name already exists
        """
        if any(v.name == volume.name for v in self.volumes):
            raise ValueError(f"Volume '{volume.name}' already exists in schema '{self.name}'")
        self.volumes.append(volume)
        # Set parent references and names
        # Only set catalog_name if we have a catalog association
        if self._parent_catalog:
            volume.catalog_name = self._parent_catalog.name  # Use base name from parent
        elif self.catalog_name:
            volume.catalog_name = self.catalog_name  # Use the catalog_name field (base name)
        volume.schema_name = self.name  # Set schema name
        volume._parent_schema = self  # Set private parent reference
        volume._parent_catalog = self._parent_catalog  # Pass catalog reference
        # Inherit external location if volume doesn't have one
        if not volume.external_location and self.external_location:
            volume.external_location = self.external_location
        # Inherit owner if volume has default owner or None
        if not volume.owner or (volume.owner and volume.owner.name == DEFAULT_SECURABLE_OWNER):
            volume.owner = self.owner
        
        # Inheritance is handled inline above
    
    def add_function(self, function: Function) -> None:
        """
        Add function with duplicate check.
        
        Args:
            function: The function to add
            
        Raises:
            ValueError: If function with same name already exists
        """
        if any(f.name == function.name for f in self.functions):
            raise ValueError(f"Function '{function.name}' already exists in schema '{self.name}'")
        self.functions.append(function)
        # Set parent references and names
        # Only set catalog_name if we have a catalog association
        if self._parent_catalog:
            function.catalog_name = self._parent_catalog.name  # Use base name from parent
        elif self.catalog_name:
            function.catalog_name = self.catalog_name  # Use the catalog_name field (base name)
        function.schema_name = self.name  # Set schema name
        function._parent_schema = self  # Set private parent reference
        function._parent_catalog = self._parent_catalog  # Pass catalog reference
        # Inherit owner if function has default owner or None
        if not function.owner or (function.owner and function.owner.name == DEFAULT_SECURABLE_OWNER):
            function.owner = self.owner
        
        # Inheritance is handled inline above
    
    def add_model(self, model: Any) -> None:  # RegisteredModel type
        """
        Add ML model with duplicate check.
        
        Args:
            model: The RegisteredModel to add
            
        Raises:
            ValueError: If model with same name already exists
        """
        if any(m.name == model.name for m in self.models):
            raise ValueError(f"Model '{model.name}' already exists in schema '{self.name}'")
        self.models.append(model)
        # Set parent references and names
        # Only set catalog_name if we have a catalog association
        if self._parent_catalog:
            model.catalog_name = self._parent_catalog.name  # Use base name from parent
        elif self.catalog_name:
            model.catalog_name = self.catalog_name  # Use the catalog_name field (base name)
        model.schema_name = self.name  # Set schema name
        model._parent_schema = self  # Set private parent reference
        model._parent_catalog = self._parent_catalog  # Pass catalog reference
        # Inherit owner if model has default owner or None
        if not model.owner or (model.owner and model.owner.name == DEFAULT_SECURABLE_OWNER):
            model.owner = self.owner
        
        # Inheritance is handled inline above
    
    def __repr__(self) -> str:
        """Developer-friendly representation."""
        return (
            f"Schema(name='{self.name}', "
            f"catalog='{self.catalog_name}', "
            f"tables={len(self.tables)}, "
            f"volumes={len(self.volumes)}, "
            f"functions={len(self.functions)}, "
            f"models={len(self.models)})"
        )
    
    def __str__(self) -> str:
        """User-friendly representation."""
        return f"Schema '{self.fqdn}' ({len(self.tables)} tables, {len(self.volumes)} volumes, {len(self.models)} models)"
    
    # grant() method inherited from BaseSecurable

    def _propagate_grants(self, principal: Principal, policy: AccessPolicy) -> List[Privilege]:
        """
        Propagate grants to child tables, volumes, functions, models, and external location.
        
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
        
        # Propagate to tables if policy has table privileges
        if policy.has_privileges_for(SecurableType.TABLE):
            for table in self.tables:
                result.extend(table.grant(principal, policy, _skip_validation=True))
        
        # Propagate to volumes if policy has volume privileges
        if policy.has_privileges_for(SecurableType.VOLUME):
            for volume in self.volumes:
                result.extend(volume.grant(principal, policy, _skip_validation=True))
        
        # Propagate to functions if policy has function privileges
        if policy.has_privileges_for(SecurableType.FUNCTION):
            for function in self.functions:
                result.extend(function.grant(principal, policy, _skip_validation=True))
        
        # Propagate to models if policy has model privileges
        if policy.has_privileges_for(SecurableType.MODEL):
            for model in self.models:
                result.extend(model.grant(principal, policy, _skip_validation=True))
        
        # Propagate to table references if policy has table privileges
        if policy.has_privileges_for(SecurableType.TABLE):
            for table_ref in self.table_refs:
                result.extend(table_ref.grant(principal, policy, _skip_validation=True))
        
        # Propagate to model references if policy has model privileges
        if policy.has_privileges_for(SecurableType.MODEL):
            for model_ref in self.model_refs:
                result.extend(model_ref.grant(principal, policy, _skip_validation=True))
        
        # Propagate to volume references if policy has volume privileges
        if policy.has_privileges_for(SecurableType.VOLUME):
            for volume_ref in self.volume_refs:
                result.extend(volume_ref.grant(principal, policy, _skip_validation=True))
        
        # Propagate to function references if policy has function privileges
        if policy.has_privileges_for(SecurableType.FUNCTION):
            for function_ref in self.function_refs:
                result.extend(function_ref.grant(principal, policy, _skip_validation=True))
        
        return result
    def get_effective_privileges(self, principal: Principal) -> List[PrivilegeType]:
        """
        Get all privileges for a principal including inherited from catalog.
        
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
        if hasattr(self, '_parent_catalog') and self._parent_catalog:
            # Just add all parent privileges, let the natural structure handle filtering
            privileges.extend(self._parent_catalog.get_effective_privileges(principal))
        
        # Handle ALL_PRIVILEGES expansion
        if PrivilegeType.ALL_PRIVILEGES in privileges:
            # Get expansion for this securable type
            expanded = ALL_PRIVILEGES_EXPANSION.get(self.securable_type, []).copy()
            # Preserve MANAGE if explicitly granted
            if PrivilegeType.MANAGE in privileges:
                expanded.append(PrivilegeType.MANAGE)
            return list(set(expanded))
        
        # Return unique privileges
        return list(set(privileges))
    
    @property
    def securable_type(self) -> SecurableType:
        """Return the securable type."""
        return SecurableType.SCHEMA
    
    def get_level_1_name(self) -> str:
        """Return the level-1 name (resolved catalog name for Privilege storage)."""
        return self.resolved_catalog_name
    
    def get_level_2_name(self) -> str:
        """Return the level-2 name (schema name)."""
        return self.name
    
    # Reference management methods for lightweight governance
    def add_table_reference(self, table_ref: 'TableReference') -> None:
        """
        Add a reference to a table discovered or created by DABs.
        
        Args:
            table_ref: The table reference to add
        """
        table_ref.catalog_name = self.catalog_name
        table_ref.schema_name = self.name
        self.table_refs.append(table_ref)
    
    def add_model_reference(self, model_ref: 'ModelReference') -> None:
        """
        Add a reference to a model discovered or registered by MLflow.
        
        Args:
            model_ref: The model reference to add
        """
        model_ref.catalog_name = self.catalog_name
        model_ref.schema_name = self.name
        self.model_refs.append(model_ref)
    
    def add_volume_reference(self, volume_ref: 'VolumeReference') -> None:
        """
        Add a reference to a volume discovered or created by DABs.
        
        Args:
            volume_ref: The volume reference to add
        """
        volume_ref.catalog_name = self.catalog_name
        volume_ref.schema_name = self.name
        self.volume_refs.append(volume_ref)
    
    def add_function_reference(self, function_ref: 'FunctionReference') -> None:
        """
        Add a reference to a function discovered or created by DABs.
        
        Args:
            function_ref: The function reference to add
        """
        function_ref.catalog_name = self.catalog_name
        function_ref.schema_name = self.name
        self.function_refs.append(function_ref)
    
    def to_sdk_create_params(self) -> Dict[str, Any]:
        """Convert to SDK create parameters."""
        # Use resolved catalog name (with environment suffix)
        params = {
            "name": self.name,
            "catalog_name": self.resolved_catalog_name,
            "comment": self.comment
        }
        if self.storage_root:
            params["storage_root"] = self.storage_root
        return params

    def to_sdk_update_params(self) -> Dict[str, Any]:
        """Convert to SDK update parameters."""
        # SDK's update method needs both name and catalog_name for identification
        params = {
            "name": self.name,  # Include the name for identification
            "catalog_name": self.resolved_catalog_name,  # Include catalog name
            "full_name": f"{self.resolved_catalog_name}.{self.name}",  # Also include full_name for SDK
            "comment": self.comment
        }
        # Note: Schema doesn't have a properties field in this implementation
        return params

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
    row_filter: Optional[Function] = Field(
        None,
        description="Row-level security function (executes with definer's rights)"
    )
    column_masks: Dict[str, Function] = Field(
        default_factory=dict,
        description="Column masking functions by column name (execute with definer's rights)"
    )
    comment: Optional[str] = Field(None, max_length=1024, description="Description of the table")
    
    # Grant tracking
    # Metadata
    tags: List[Tag] = Field(default_factory=list, description="Metadata tags for ABAC")
    
    # Dependencies
    referencing_functions: List[Function] = Field(
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
    
    # Note: External table validation removed - handled in Schema.add_table()
    # External tables can be created without a location and inherit from schema
    
    @computed_field  # type: ignore[prop-decorator]
    @property
    def fqdn(self) -> str:
        """Fully qualified domain name (catalog.schema.table format)."""
        if not self.catalog_name or not self.schema_name:
            raise ValueError(f"Table '{self.name}' is not associated with a catalog and schema")
        return f"{self.resolved_catalog_name}.{self.schema_name}.{self.name}"
    
    def set_row_filter(self, function: Function) -> None:
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
    
    def add_column_mask(self, column: str, function: Function) -> None:
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
        column_names = [col.get('name') for col in self.columns if 'name' in col]
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
    
    # grant() method inherited from BaseSecurable
    
    def _propagate_grants(self, principal: Principal, policy: AccessPolicy) -> List[Privilege]:
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
            # Just add all parent privileges, let the natural structure handle filtering
            privileges.extend(self._parent_schema.get_effective_privileges(principal))
        
        # Handle ALL_PRIVILEGES expansion
        if PrivilegeType.ALL_PRIVILEGES in privileges:
            # Get expansion for this securable type
            expanded = ALL_PRIVILEGES_EXPANSION.get(self.securable_type, []).copy()
            # Preserve MANAGE if explicitly granted
            if PrivilegeType.MANAGE in privileges:
                expanded.append(PrivilegeType.MANAGE)
            return list(set(expanded))
        
        # Return unique privileges
        return list(set(privileges))
    
    @property
    def securable_type(self) -> SecurableType:
        """Return the securable type."""
        return SecurableType.TABLE
    
    def get_level_1_name(self) -> str:
        """Return the level-1 name (resolved catalog name for Privilege storage)."""
        return self.resolved_catalog_name
    
    def get_level_2_name(self) -> str:
        """Return the level-2 name (schema name)."""
        return self.schema_name
    
    def get_level_3_name(self) -> str:
        """Return the level-3 name (table name)."""
        return self.name
    
    def to_sdk_create_params(self) -> Dict[str, Any]:
        """Convert to SDK create parameters."""
        # Convert columns to SDK ColumnInfo objects with proper enum values
        from databricks.sdk.service.catalog import ColumnInfo as SDKColumnInfo, ColumnTypeName

        # Map SQL types to SDK ColumnTypeName enum values
        TYPE_MAPPING = {
            "BIGINT": "LONG",  # BIGINT maps to LONG in the enum
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
            if isinstance(col, ColumnInfo) or isinstance(col, dict):
                # Get column properties
                if isinstance(col, ColumnInfo):
                    col_name = col.name
                    col_type = col.type_name
                    col_nullable = col.nullable if hasattr(col, 'nullable') else True
                    col_comment = col.comment if hasattr(col, 'comment') else None
                else:  # dict
                    col_name = col.get("name")
                    col_type = col.get("type", "STRING")
                    col_nullable = col.get("nullable", True)
                    col_comment = col.get("comment")

                # Map SQL type to enum value
                base_type = col_type.upper().split('(')[0].strip()
                enum_name = TYPE_MAPPING.get(base_type, "STRING")

                # Create SDK ColumnInfo with all type fields
                # The API seems to want all three: type_name (enum), type_text (string), type_json (JSON)
                import json
                type_json = json.dumps({"type": col_type})

                sdk_column = SDKColumnInfo(
                    name=col_name,
                    type_name=ColumnTypeName[enum_name],  # Enum value
                    type_text=col_type,  # SQL type string
                    type_json=type_json,  # JSON representation
                    nullable=col_nullable,
                    comment=col_comment,
                    position=i  # Column position in table
                )
                sdk_columns.append(sdk_column)
            else:
                # Try to convert other types
                if hasattr(col, 'model_dump'):
                    col_dict = col.model_dump(exclude_none=True, by_alias=False)
                    # Ensure required fields
                    if "type_name" not in col_dict and "type" in col_dict:
                        col_dict["type_name"] = col_dict["type"]
                    if "type_text" not in col_dict and "type" in col_dict:
                        col_dict["type_text"] = col_dict["type"]
                    # Convert dict back to SDK ColumnInfo
                    from databricks.sdk.service.catalog import ColumnInfo as SDKColumnInfo
                    sdk_columns.append(SDKColumnInfo.from_dict(col_dict))
                else:
                    # Last resort - pass as is
                    sdk_columns.append(col)
        
        from databricks.sdk.service.catalog import DataSourceFormat

        params = {
            "name": self.name,
            "table_type": self.table_type,  # Keep as our enum - SDK will handle conversion
            "columns": sdk_columns,
            "data_source_format": DataSourceFormat.DELTA,  # Default to DELTA format (enum, not string)
        }
        
        # Add catalog and schema with resolved names (with environment suffix)
        if self.catalog_name:
            params["catalog_name"] = self.resolved_catalog_name

        if self.schema_name:
            params["schema_name"] = self.schema_name

        # NOTE: comment is NOT accepted by create API
        # It must be set via update after creation
            
        if self.external_location:
            # For EXTERNAL tables, append catalog/schema/table path to avoid overlap
            base_url = self.external_location.url.rstrip('/')
            # Include resolved catalog name (with env suffix) to ensure uniqueness
            catalog_part = self.resolved_catalog_name if self.catalog_name else ""
            table_path = f"{catalog_part}/{self.schema_name}/{self.name}" if catalog_part else f"{self.schema_name}/{self.name}"
            params["storage_location"] = f"{base_url}/{table_path}"
        elif self.table_type == TableType.EXTERNAL:
            # External tables need a storage location, but don't validate here
            # Let the SDK handle validation
            params["storage_location"] = None
        else:
            # For managed tables, storage location is derived from catalog/schema
            # Provide empty string to let Databricks handle it
            params["storage_location"] = ""
            
        return params
    
    def to_sdk_update_params(self) -> Dict[str, Any]:
        """Convert to SDK update parameters (limited update capability)."""
        params = {
            "full_name": self.fqdn
        }
        # Only owner can be updated via SDK API
        if self.owner:
            params["owner"] = self.owner.resolved_name
        return params
    
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
        create_clause = "CREATE"
        if self.table_type == TableType.EXTERNAL:
            create_clause += " EXTERNAL"
        create_clause += " TABLE"
        if if_not_exists:
            create_clause += " IF NOT EXISTS"
        
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
        
        # USING clause (file format) - default to DELTA
        using_format = "DELTA"
        ddl_parts.append(f"USING {using_format}")
        
        # PARTITIONED BY clause (if attribute exists)
        if hasattr(self, 'partition_cols') and self.partition_cols:
            ddl_parts.append(f"PARTITIONED BY ({', '.join(self.partition_cols)})")
        
        # CLUSTERED BY clause (Z-ordering) (if attribute exists)
        if hasattr(self, 'cluster_cols') and self.cluster_cols:
            ddl_parts.append(f"CLUSTERED BY ({', '.join(self.cluster_cols)})")
            ddl_parts.append("INTO 256 BUCKETS")  # Default bucket count
        
        # LOCATION clause for external tables
        if self.table_type == TableType.EXTERNAL:
            if self.external_location:
                # Use external location's URL with catalog/schema/table path to avoid conflicts
                base_url = self.external_location.url.rstrip('/')
                # Include resolved catalog name (with env suffix) to ensure uniqueness across environments
                catalog_part = self.resolved_catalog_name if self.catalog_name else ""
                table_path = f"{catalog_part}/{self.schema_name}/{self.name}" if catalog_part else f"{self.schema_name}/{self.name}"
                location_url = f"{base_url}/{table_path}"
                ddl_parts.append(f"LOCATION '{location_url}'")
        
        # COMMENT clause
        if self.comment:
            ddl_parts.append(f"COMMENT '{self.comment}'")
        
        # TBLPROPERTIES clause (if attribute exists)
        if hasattr(self, 'properties') and self.properties:
            props = [f"'{k}' = '{v}'" for k, v in self.properties.items()]
            ddl_parts.append(f"TBLPROPERTIES ({', '.join(props)})")
        
        return "\n".join(ddl_parts)
    
    def to_sql_alter_owner(self) -> Optional[str]:
        """
        Generate SQL ALTER TABLE ... SET OWNER statement.
        
        Returns:
            SQL ALTER statement or None if no owner specified
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
            principals = ", ".join([f"`{p.resolved_name}`" for p in privilege.principals])
            grants.append(f"GRANT {privilege.privilege_type.value} ON TABLE {self.fqdn} TO {principals}")
        return grants
    
    # =============================================================================
    # ETL HELPER METHODS - Optional smart helpers for common patterns
    # =============================================================================
    
    def create_empty(self, spark) -> None:
        """
        Create empty table with schema - useful for placeholders.
        
        Args:
            spark: SparkSession instance
        """
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
            partition_by: Optional partition columns (overrides model definition)
            cluster_by: Optional clustering columns for optimization
        """
        # Validate schema compatibility
        if not self.validate_dataframe(df):
            raise ValueError(f"DataFrame schema doesn't match table {self.name} definition")
        
        # Start building the write operation
        writer = df.write.mode(mode)
        
        # Add partitioning if specified
        partitions = partition_by or getattr(self, 'partition_cols', None)
        if partitions:
            writer = writer.partitionBy(*partitions)
        
        # Save as Delta table
        writer.saveAsTable(self.fqdn)
        
        # Apply clustering if specified (requires separate OPTIMIZE)
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
        from pyspark.sql.functions import current_date, lit, col
        from delta.tables import DeltaTable
        
        spark = df.sparkSession
        
        # Check if table exists
        if not spark.catalog.tableExists(self.fqdn):
            # First load - all records are current
            df_with_scd = df.withColumn(effective_date_col, current_date()) \
                           .withColumn(end_date_col, lit(None).cast("date")) \
                           .withColumn(current_flag_col, lit(True))
            self.create_from_dataframe(df_with_scd)
            return
        
        # Build merge condition
        merge_conditions = []
        for key in merge_keys:
            merge_conditions.append(f"target.{key} = source.{key}")
        merge_conditions.append(f"target.{current_flag_col} = true")
        merge_condition = " AND ".join(merge_conditions)
        
        # Build change detection condition (all non-key columns)
        all_cols = set(df.columns)
        key_cols = set(merge_keys)
        compare_cols = all_cols - key_cols - {effective_date_col, end_date_col, current_flag_col}
        
        change_conditions = []
        for col_name in compare_cols:
            change_conditions.append(f"target.{col_name} != source.{col_name}")
        change_condition = " OR ".join(change_conditions) if change_conditions else "1=1"
        
        # Perform SCD2 merge
        delta_table = DeltaTable.forName(spark, self.fqdn)
        
        # Add SCD columns to source if not present
        source_df = df
        if effective_date_col not in df.columns:
            source_df = source_df.withColumn(effective_date_col, current_date())
        if end_date_col not in df.columns:
            source_df = source_df.withColumn(end_date_col, lit(None).cast("date"))
        if current_flag_col not in df.columns:
            source_df = source_df.withColumn(current_flag_col, lit(True))
        
        # Execute merge
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
        
        # Insert new versions for changed records
        changed_records = source_df.alias("source").join(
            delta_table.toDF().alias("target"),
            on=[col(f"source.{k}") == col(f"target.{k}") for k in merge_keys],
            how="inner"
        ).where(f"target.{current_flag_col} = false AND target.{end_date_col} = current_date()")
        
        if changed_records.count() > 0:
            changed_records.select("source.*").write.mode("append").saveAsTable(self.fqdn)
    
    def create_streaming_table(
        self,
        source_df,
        checkpoint_location: str,
        trigger: str = "10 seconds",
        output_mode: str = "append"
    ) -> Any:
        """
        Create auto-refreshing streaming table.
        
        Args:
            source_df: Streaming DataFrame source
            checkpoint_location: Path for checkpointing
            trigger: Processing trigger interval
            output_mode: Output mode - "append", "complete", "update"
            
        Returns:
            StreamingQuery object
        """
        return source_df.writeStream \
            .format("delta") \
            .outputMode(output_mode) \
            .option("checkpointLocation", checkpoint_location) \
            .trigger(processingTime=trigger) \
            .table(self.fqdn)
    
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
        # Run OPTIMIZE with optional Z-ordering
        optimize_sql = f"OPTIMIZE {self.fqdn}"
        if zorder_by:
            optimize_sql += f" ZORDER BY ({', '.join(zorder_by)})"
        spark.sql(optimize_sql)
        
        # Run VACUUM if requested
        if vacuum:
            spark.sql(f"VACUUM {self.fqdn} RETAIN {vacuum_hours} HOURS")
    
    def apply_quality_checks(
        self,
        spark,
        expectations: Dict[str, str],
        on_violation: str = "fail"
    ) -> None:
        """
        Apply Delta Lake quality constraints.
        
        Args:
            spark: SparkSession instance
            expectations: Dict of constraint name -> SQL condition
            on_violation: Action on violation - "fail" or "drop"
        """
        for name, condition in expectations.items():
            # Clean constraint name
            clean_name = name.replace(" ", "_").replace("-", "_")
            
            if on_violation == "fail":
                # Add as CHECK constraint
                spark.sql(f"""
                    ALTER TABLE {self.fqdn}
                    ADD CONSTRAINT {clean_name} CHECK ({condition})
                """)
            else:
                # Note: For drop mode, this would be better in DLT
                # Here we just validate and warn
                violations = spark.sql(f"""
                    SELECT COUNT(*) as violations
                    FROM {self.fqdn}
                    WHERE NOT ({condition})
                """).collect()[0].violations
                
                if violations > 0:
                    print(f"WARNING: {violations} rows violate constraint {name}: {condition}")
    
    def clone(
        self,
        spark,
        target_name: str,
        shallow: bool = True
    ) -> 'Table':
        """
        Clone table for testing/development.
        
        Args:
            spark: SparkSession instance
            target_name: Full name of target table
            shallow: If True, creates shallow clone (metadata only)
            
        Returns:
            New Table object for the clone
        """
        clone_type = "SHALLOW" if shallow else "DEEP"
        spark.sql(f"CREATE TABLE {target_name} {clone_type} CLONE {self.fqdn}")
        
        # Create new Table object for the clone
        parts = target_name.split(".")
        cloned = self.model_copy()
        cloned.name = parts[-1] if len(parts) > 0 else target_name
        if len(parts) >= 2:
            cloned.schema_name = parts[-2]
        if len(parts) >= 3:
            cloned.catalog_name = parts[-3]
        
        return cloned
    
    def validate_dataframe(self, df) -> bool:
        """
        Check if DataFrame schema matches table definition.
        
        Args:
            df: PySpark DataFrame to validate
            
        Returns:
            True if schemas are compatible
        """
        # Get expected columns
        expected_cols = {col.name.lower(): col for col in self.columns}
        
        # Get actual columns from DataFrame
        actual_cols = {field.name.lower(): field for field in df.schema.fields}
        
        # Check if all expected columns exist
        for col_name, col_info in expected_cols.items():
            if col_name not in actual_cols:
                return False
            
            # Could add type checking here if needed
            # actual_type = str(actual_cols[col_name].dataType)
            # if not self._types_compatible(col_info.data_type, actual_type):
            #     return False
        
        return True
    
    def to_spark_schema(self):
        """
        Convert table columns to Spark StructType.
        
        Returns:
            Spark StructType matching table definition
        """
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, \
            DoubleType, BooleanType, DateType, TimestampType, DecimalType
        
        # Type mapping from SQL to Spark
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
            "DECIMAL": DecimalType(10, 2),  # Default precision
            # Add more mappings as needed
        }
        
        fields = []
        for col in self.columns:
            spark_type = type_map.get(col.data_type.upper(), StringType())
            field = StructField(col.name, spark_type, col.nullable)
            fields.append(field)
        
        return StructType(fields)

# =============================================================================
# VOLUME
# =============================================================================

class Volume(BaseSecurable):
    """
    Third-level object for unstructured data storage.
    
    Volumes provide a way to store and manage unstructured data like files,
    ML models, images, and documents within Unity Catalog. They can be either
    managed (storage handled by Databricks) or external (references external storage).
    """
    name: str = Field(
        ...,
        pattern=r'^[a-zA-Z0-9][a-zA-Z0-9_-]*$',
        description="Volume name (no environment suffix - parent catalog has it)"
    )
    volume_type: VolumeType = Field(
        VolumeType.MANAGED,
        description="MANAGED or EXTERNAL"
    )
    owner: Optional[Principal] = Field(
        default_factory=lambda: Principal(name=DEFAULT_SECURABLE_OWNER, add_environment_suffix=False),
        description="Owner principal (defaults to parent schema owner)"
    )
    storage_location: Optional[str] = Field(
        None,
        description="For external volumes - the storage path"
    )
    external_location: Optional[ExternalLocation] = Field(
        None,
        description="External location object (inherited from schema if not set)"
    )
    comment: Optional[str] = Field(None, max_length=1024, description="Description of the volume")
    
    # Grant tracking
    # Metadata
    tags: List[Tag] = Field(default_factory=list, description="Metadata tags for ABAC")
    
    # Private parent reference (not serialized)
    _parent_schema: Optional[Schema] = PrivateAttr(default=None)
    _parent_catalog: Optional[Catalog] = PrivateAttr(default=None)
    
    catalog_name: Optional[str] = Field(
        None,
        description="Parent catalog name (set by add_volume)"
    )
    schema_name: Optional[str] = Field(
        None,
        description="Parent schema name (set by add_volume)"
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
            raise ValueError(f"Volume '{self.name}' is not associated with a catalog")
        # Fallback to string field with current env if no parent
        env = get_current_environment()
        return f"{self.catalog_name}_{env.value.lower()}"
    
    
    @field_validator('volume_type', mode='before')
    @classmethod
    def convert_volume_type(cls, v: Any) -> VolumeType:
        """Convert string to VolumeType enum if needed."""
        if isinstance(v, str):
            return VolumeType(v.upper())
        return v
    
    @model_validator(mode='after')
    def validate_external_volume(self) -> Self:
        """Ensure external volumes have either storage_location or external_location."""
        if self.volume_type == VolumeType.EXTERNAL:
            if not self.storage_location and not self.external_location:
                raise ValueError("External volumes require either storage_location or external_location")
        return self
    
    @computed_field  # type: ignore[prop-decorator]
    @property
    def fqdn(self) -> str:
        """Fully qualified domain name (catalog.schema.volume format)."""
        if not self.catalog_name or not self.schema_name:
            raise ValueError(f"Volume '{self.name}' is not associated with a catalog and schema")
        return f"{self.resolved_catalog_name}.{self.schema_name}.{self.name}"
    
    # grant() method inherited from BaseSecurable
    
    def _propagate_grants(self, principal: Principal, policy: AccessPolicy) -> List[Privilege]:
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
            # Just add all parent privileges, let the natural structure handle filtering
            privileges.extend(self._parent_schema.get_effective_privileges(principal))
        
        # Handle ALL_PRIVILEGES expansion
        if PrivilegeType.ALL_PRIVILEGES in privileges:
            # Get expansion for this securable type
            expanded = ALL_PRIVILEGES_EXPANSION.get(self.securable_type, []).copy()
            # Preserve MANAGE if explicitly granted
            if PrivilegeType.MANAGE in privileges:
                expanded.append(PrivilegeType.MANAGE)
            return list(set(expanded))
        
        # Return unique privileges
        return list(set(privileges))
    
    @property
    def securable_type(self) -> SecurableType:
        """Return the securable type."""
        return SecurableType.VOLUME
    
    def get_level_1_name(self) -> str:
        """Return the level-1 name (resolved catalog name for Privilege storage)."""
        return self.resolved_catalog_name
    
    def get_level_2_name(self) -> str:
        """Return the level-2 name (schema name)."""
        return self.schema_name
    
    def get_level_3_name(self) -> str:
        """Return the level-3 name (volume name)."""
        return self.name
    
    def to_sdk_create_params(self) -> Dict[str, Any]:
        """Convert to SDK create parameters."""
        params = {
            "name": self.name,
            "catalog_name": self.catalog_name,
            "schema_name": self.schema_name,
            "volume_type": self.volume_type.value,
            "comment": self.comment
        }
        if self.storage_location:
            params["storage_location"] = self.storage_location
        elif self.external_location:
            params["storage_location"] = self.external_location.url
        return params
    
    def to_sdk_update_params(self) -> Dict[str, Any]:
        """Convert to SDK update parameters."""
        return {
            "full_name": self.fqdn,
            "comment": self.comment
        }

# =============================================================================
# FUNCTION
# =============================================================================

class Function(BaseSecurable):
    """
    Third-level object storing user-defined functions.
    
    Functions can be SQL or Python-based and can return scalar values or tables.
    When used for row filters or column masks, they execute transparently with
    definer's rights, meaning users only need SELECT on the table.
    """
    name: str = Field(
        ...,
        pattern=r'^[a-zA-Z0-9][a-zA-Z0-9_-]*$',
        description="Function name (no environment suffix - parent catalog has it)"
    )
    function_type: FunctionType = Field(
        FunctionType.SQL,
        description="SQL, PYTHON, SCALAR, or TABLE"
    )
    owner: Optional[Principal] = Field(
        default_factory=lambda: Principal(name=DEFAULT_SECURABLE_OWNER, add_environment_suffix=False),
        description="Owner principal (defaults to parent schema owner)"
    )
    input_params: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Parameter definitions with name, type, and optional default"
    )
    return_type: Optional[str] = Field(
        None,
        description="Return type specification (e.g., 'STRING', 'INT', 'TABLE')"
    )
    definition: Optional[str] = Field(
        None,
        description="Function body/implementation (generic)"
    )
    sql_definition: Optional[str] = Field(
        None,
        description="SQL function definition"
    )
    routine_definition: Optional[str] = Field(
        None,
        description="Python function definition"
    )
    routine_dependencies: Optional[List[str]] = Field(
        None,
        description="Python function dependencies"
    )
    referenced_tables: List[Table] = Field(
        default_factory=list,
        description="Tables this function reads from - for dependency tracking only"
    )
    is_deterministic: bool = Field(
        True,
        description="Whether function always returns same result for same input"
    )
    comment: Optional[str] = Field(None, max_length=1024, description="Description of the function")
    
    # Grant tracking
    # Metadata
    tags: List[Tag] = Field(default_factory=list, description="Metadata tags for ABAC")
    
    # Private parent reference (not serialized)
    _parent_schema: Optional[Schema] = PrivateAttr(default=None)
    _parent_catalog: Optional[Catalog] = PrivateAttr(default=None)
    
    catalog_name: Optional[str] = Field(
        None,
        description="Parent catalog name (set by add_function)"
    )
    schema_name: Optional[str] = Field(
        None,
        description="Parent schema name (set by add_function)"
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
            raise ValueError(f"Function '{self.name}' is not associated with a catalog")
        # Fallback to string field with current env if no parent
        env = get_current_environment()
        return f"{self.catalog_name}_{env.value.lower()}"
    
    
    @field_validator('function_type', mode='before')
    @classmethod
    def convert_function_type(cls, v: Any) -> FunctionType:
        """Convert string to FunctionType enum if needed."""
        if isinstance(v, str):
            return FunctionType(v.upper())
        return v
    
    @computed_field  # type: ignore[prop-decorator]
    @property
    def fqdn(self) -> str:
        """Fully qualified domain name (catalog.schema.function format)."""
        if not self.catalog_name or not self.schema_name:
            raise ValueError(f"Function '{self.name}' is not associated with a catalog and schema")
        return f"{self.resolved_catalog_name}.{self.schema_name}.{self.name}"
    
    def add_referenced_table(self, table: Table) -> None:
        """
        Add table reference for dependency tracking.
        
        Note: This is for documentation and impact analysis only.
        Functions execute with definer's rights, so users don't need
        SELECT on referenced tables.
        
        Args:
            table: The table this function reads from
        """
        if table not in self.referenced_tables:
            self.referenced_tables.append(table)
            # Also add this function to the table's referencing functions
            if self not in table.referencing_functions:
                table.referencing_functions.append(self)
    
    # grant() method inherited from BaseSecurable
    def get_effective_privileges(self, principal: Principal) -> List[PrivilegeType]:
        """
        Get all privileges for a principal including inherited from schema.
        
        Note: When used as a row filter or column mask, this function
        executes transparently with definer's rights, so users don't need
        EXECUTE privilege.
        
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
            # Just add all parent privileges, let the natural structure handle filtering
            privileges.extend(self._parent_schema.get_effective_privileges(principal))
        
        # Handle ALL_PRIVILEGES expansion
        if PrivilegeType.ALL_PRIVILEGES in privileges:
            # Get expansion for this securable type
            expanded = ALL_PRIVILEGES_EXPANSION.get(self.securable_type, []).copy()
            # Preserve MANAGE if explicitly granted
            if PrivilegeType.MANAGE in privileges:
                expanded.append(PrivilegeType.MANAGE)
            return list(set(expanded))
        
        # Return unique privileges
        return list(set(privileges))
    
    @property
    def securable_type(self) -> SecurableType:
        """Return the securable type."""
        return SecurableType.FUNCTION
    
    def get_level_1_name(self) -> str:
        """Return the level-1 name (resolved catalog name for Privilege storage)."""
        return self.resolved_catalog_name
    
    def get_level_2_name(self) -> str:
        """Return the level-2 name (schema name)."""
        return self.schema_name
    
    def get_level_3_name(self) -> str:
        """Return the level-3 name (function name)."""
        return self.name
    
    def to_sdk_create_params(self) -> Dict[str, Any]:
        """Convert to SDK create parameters."""
        params = {
            "name": self.name,
            "function_type": self.function_type.value
        }
        
        # Add catalog and schema if available
        try:
            params["catalog_name"] = self.resolved_catalog_name
        except ValueError:
            if self.catalog_name:
                params["catalog_name"] = self.catalog_name
                
        if self.schema_name:
            params["schema_name"] = self.schema_name
            
        # Add function definition based on type
        if self.function_type == FunctionType.SQL:
            if self.sql_definition:
                params["sql_definition"] = self.sql_definition
            elif self.definition:
                params["sql_definition"] = self.definition
        elif self.function_type == FunctionType.PYTHON:
            if self.routine_definition:
                params["routine_definition"] = self.routine_definition
            elif self.definition:
                params["routine_definition"] = self.definition
            if self.routine_dependencies:
                params["routine_dependencies"] = self.routine_dependencies
        else:
            # Generic function type
            if self.definition:
                params["definition"] = self.definition
                
        # Add other optional parameters
        if self.input_params:
            params["input_params"] = self.input_params
        if self.return_type:
            params["return_type"] = self.return_type
        if self.comment:
            params["comment"] = self.comment
        if self.is_deterministic is not None:
            params["is_deterministic"] = self.is_deterministic
            
        return params
    
    def to_sdk_update_params(self) -> Dict[str, Any]:
        """
        Convert to SDK update parameters.
        
        Note: Functions are typically replaced rather than updated in Unity Catalog.
        """
        return {
            "full_name": self.fqdn,
            "comment": self.comment
        }


# =============================================================================
# TYPED DATA MODELS  
# =============================================================================

# =============================================================================
# CATALOG
# =============================================================================

class Catalog(BaseSecurable):
    """
    First layer of Unity Catalog's three-level namespace.
    
    Catalogs are the top-level containers for schemas and provide a way to organize
    data assets. They support isolation modes and workspace bindings for access control.
    """
    name: str = Field(
        ...,
        pattern=r'^[a-zA-Z0-9][a-zA-Z0-9_-]*$',
        description="Catalog name (base name without environment suffix)"
    )
    comment: Optional[str] = Field(None, max_length=1024, description="Description of the catalog")
    owner: Optional[Principal] = Field(
        default_factory=lambda: Principal(name=DEFAULT_SECURABLE_OWNER, add_environment_suffix=False),
        description="Owner principal"
    )
    external_location: Optional[ExternalLocation] = Field(
        None, 
        description="Optional external storage location"
    )
    isolation_mode: IsolationMode = Field(
        IsolationMode.OPEN,
        description="OPEN or ISOLATED workspace access mode"
    )
    
    # Workspace bindings for ISOLATED catalogs
    workspace_ids: List[int] = Field(
        default_factory=list,
        description="List of workspace IDs that can access this catalog (only for ISOLATED mode)"
    )
    
    # Child containers
    schemas: List[Schema] = Field(default_factory=list, description="Child schemas")
    
    # References to discovered objects (for governance of DABs/MLflow created resources)
    # These are lightweight references, not full objects
    table_refs: List['TableReference'] = Field(default_factory=list, description="References to discovered tables")
    model_refs: List['ModelReference'] = Field(default_factory=list, description="References to discovered models")
    volume_refs: List['VolumeReference'] = Field(default_factory=list, description="References to discovered volumes")
    function_refs: List['FunctionReference'] = Field(default_factory=list, description="References to discovered functions")
    
    # Grant tracking
    # Metadata
    tags: List[Tag] = Field(default_factory=list, description="Metadata tags for ABAC")
    
    # Optional environment override (for workspace bindings)
    environment: Optional[Environment] = Field(
        None,
        description="Optional environment override (mainly for workspace bindings)"
    )
    
    @model_validator(mode='after')
    def validate_workspace_bindings(self) -> Self:
        """Validate workspace bindings are only used with ISOLATED mode."""
        if self.workspace_ids and self.isolation_mode != IsolationMode.ISOLATED:
            raise ValueError(
                f"Catalog '{self.name}' has workspace_ids but isolation_mode is {self.isolation_mode}. "
                "Workspace bindings are only valid for ISOLATED catalogs."
            )
        if self.isolation_mode == IsolationMode.ISOLATED and not self.workspace_ids:
            # This is a warning, not an error - bindings can be added later
            pass  # Could log a warning here
        return self
    
    @computed_field  # type: ignore[prop-decorator]
    @property
    def storage_root(self) -> Optional[str]:
        """
        Returns external_location.url for SDK export.
        
        This is a computed field that derives its value from external_location.
        It's included in serialization but cannot be set directly.
        """
        return self.external_location.url if self.external_location else None
    
    @computed_field  # type: ignore[prop-decorator]
    @property
    def resolved_name(self) -> str:
        """Name with environment suffix."""
        # Use explicit override if set, otherwise current environment
        env = self.environment or get_current_environment()
        return f"{self.name}_{env.value.lower()}"
    
    @property
    def environment_name(self) -> str:
        """Alias for resolved_name for backward compatibility."""
        return self.resolved_name
    
    def add_schema(self, schema: Schema) -> None:
        """
        Add schema with duplicate check.
        
        Args:
            schema: The schema to add
            
        Raises:
            ValueError: If schema with same name already exists
        """
        if any(s.name == schema.name for s in self.schemas):
            raise ValueError(f"Schema '{schema.name}' already exists in catalog '{self.name}'")
        self.schemas.append(schema)
        # Set parent reference and catalog_name
        # Store base name, environment suffix is applied at runtime via resolved_catalog_name
        schema.catalog_name = self.name  # Store base name without suffix
        schema._parent_catalog = self  # Set private parent reference
        
        # Update catalog_name for all children that were added before the schema was added to catalog
        # Store base name, runtime resolution handles suffix
        for table in schema.tables:
            table.catalog_name = self.name
            table._parent_catalog = self
        for volume in schema.volumes:
            volume.catalog_name = self.name
            volume._parent_catalog = self
        for function in schema.functions:
            function.catalog_name = self.name
            function._parent_catalog = self
        for model in schema.models:
            model.catalog_name = self.name
            model._parent_catalog = self
        # Inherit external location if schema doesn't have one
        if not schema.external_location and self.external_location:
            schema.external_location = self.external_location
        # Inherit owner if schema has default owner or None
        if not schema.owner or (schema.owner and schema.owner.name == DEFAULT_SECURABLE_OWNER):
            schema.owner = self.owner
        
        # Inheritance is handled inline above
    
    def add_table_reference(self, table_ref: 'TableReference') -> None:
        """
        Add a reference to a table discovered or created by DABs.
        Used for governance of externally-created tables.
        """
        table_ref.catalog_name = self.name
        self.table_refs.append(table_ref)
    
    def add_model_reference(self, model_ref: 'ModelReference') -> None:
        """
        Add a reference to a model discovered from MLflow.
        Used for governance of ML models.
        """
        model_ref.catalog_name = self.name
        self.model_refs.append(model_ref)
    
    def add_volume_reference(self, volume_ref: 'VolumeReference') -> None:
        """
        Add a reference to a discovered volume.
        Used for governance of externally-created volumes.
        """
        volume_ref.catalog_name = self.name
        self.volume_refs.append(volume_ref)
    
    def add_function_reference(self, function_ref: 'FunctionReference') -> None:
        """
        Add a reference to a discovered function.
        Used for governance of UDFs.
        """
        function_ref.catalog_name = self.name
        self.function_refs.append(function_ref)
    
    # grant() method inherited from BaseSecurable

    def _propagate_grants(self, principal: Principal, policy: AccessPolicy) -> List[Privilege]:
        """
        Propagate grants to child schemas and external location.
        
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
        
        # Propagate to schemas if policy has schema privileges
        if policy.has_privileges_for(SecurableType.SCHEMA):
            for schema in self.schemas:
                result.extend(schema.grant(principal, policy, _skip_validation=True))
        
        return result
    def get_effective_privileges(self, principal: Principal) -> List[PrivilegeType]:
        """
        Get all privileges for a principal including expanded ALL_PRIVILEGES.
        
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
        
        # Handle ALL_PRIVILEGES expansion
        if PrivilegeType.ALL_PRIVILEGES in privileges:
            # Get expansion for this securable type
            expanded = ALL_PRIVILEGES_EXPANSION.get(self.securable_type, []).copy()
            # Preserve MANAGE if explicitly granted
            if PrivilegeType.MANAGE in privileges:
                expanded.append(PrivilegeType.MANAGE)
            return list(set(expanded))
        
        # Return unique privileges
        return list(set(privileges))
    
    def get_effective_tags(self) -> List[Tag]:
        """Returns tags on this catalog only."""
        return self.tags.copy()

    def to_sdk_create_params(self) -> Dict[str, Any]:
        """Convert to SDK create parameters."""
        params = {
            "name": self.resolved_name
        }

        # Comment is accepted by create API
        if self.comment:
            params["comment"] = self.comment

        # NOTE: owner is NOT accepted by create API - defaults to caller
        # It must be changed via update after creation
        # NOTE: isolation_mode is NOT accepted by create API
        # It must be set via update after creation

        # Add storage root if set
        if self.storage_root:
            params["storage_root"] = self.storage_root

        # Add properties (tags as properties)
        if self.tags:
            params["properties"] = {tag.key: tag.value for tag in self.tags}

        return params

    def to_sdk_update_params(self) -> Dict[str, Any]:
        """Convert to SDK update parameters."""
        params = {
            "name": self.resolved_name,  # Name needed for identification
            "comment": self.comment
        }

        # Owner can be updated
        if self.owner:
            params["owner"] = self.owner.resolved_name

        # Isolation mode can be changed
        params["isolation_mode"] = self.isolation_mode.value

        # Properties can be updated
        if self.tags:
            params["properties"] = {tag.key: tag.value for tag in self.tags}

        return params
    
    @property
    def securable_type(self) -> SecurableType:
        """Return the securable type."""
        return SecurableType.CATALOG
    
    def get_level_1_name(self) -> str:
        """Return the level-1 name (resolved)."""
        return self.resolved_name


# =============================================================================
# METASTORE
# =============================================================================

class Metastore(BaseGovernanceModel):
    """
    Top-level container for all Unity Catalog objects.
    
    The Metastore is a reference-only object - it must already exist in Databricks.
    This model serves as the root container for organizing all governance objects.
    """
    name: str = Field(..., description="User-specified metastore name")
    metastore_id: Optional[str] = Field(None, description="Unique metastore identifier")
    region: Optional[str] = Field(None, description="Cloud region (e.g., us-west-2)")
    storage_root: Optional[str] = Field(None, description="Default storage location")
    owner: Optional[Principal] = Field(
        default_factory=lambda: Principal(name=DEFAULT_SECURABLE_OWNER, add_environment_suffix=False),
        description="Owner principal"
    )
    
    # Child containers
    catalogs: List[Catalog] = Field(default_factory=list, description="Child catalogs")
    storage_credentials: List[StorageCredential] = Field(default_factory=list, description="Storage credentials")
    external_locations: List[ExternalLocation] = Field(default_factory=list, description="External locations")
    connections: List[Connection] = Field(default_factory=list, description="External connections")
    teams: List[Team] = Field(default_factory=list, description="Teams with workspace access")
    
    # Workspace registry (private, not serialized)
    _workspace_registry: Dict[str, Workspace] = PrivateAttr(default_factory=dict)
    
    
    def add_catalog(self, catalog: Catalog) -> None:
        """
        Add catalog with duplicate check.
        
        Args:
            catalog: The catalog to add
            
        Raises:
            ValueError: If catalog with same name already exists
        """
        if any(c.name == catalog.name for c in self.catalogs):
            raise ValueError(f"Catalog '{catalog.name}' already exists in metastore")
        self.catalogs.append(catalog)
        # Catalog doesn't need parent reference since metastore is root
    
    def add_storage_credential(self, sc: StorageCredential) -> None:
        """
        Add storage credential with duplicate check.
        
        Args:
            sc: The storage credential to add
            
        Raises:
            ValueError: If credential with same name already exists
        """
        if any(s.name == sc.name for s in self.storage_credentials):
            raise ValueError(f"Storage credential '{sc.name}' already exists in metastore")
        self.storage_credentials.append(sc)
    
    def add_external_location(self, el: ExternalLocation) -> None:
        """
        Add external location with duplicate check.
        
        Args:
            el: The external location to add
            
        Raises:
            ValueError: If location with same name already exists
        """
        if any(e.name == el.name for e in self.external_locations):
            raise ValueError(f"External location '{el.name}' already exists in metastore")
        self.external_locations.append(el)
    
    def add_connection(self, conn: Connection) -> None:
        """
        Add connection with duplicate check.
        
        Args:
            conn: The connection to add
            
        Raises:
            ValueError: If connection with same name already exists
        """
        if any(c.name == conn.name for c in self.connections):
            raise ValueError(f"Connection '{conn.name}' already exists in metastore")
        self.connections.append(conn)
    
    def add_team(self, team: Team) -> None:
        """
        Add team to metastore and register its workspaces.
        
        Args:
            team: The team to add
            
        Raises:
            ValueError: If team with same name already exists
        """
        if any(t.name == team.name for t in self.teams):
            raise ValueError(f"Team '{team.name}' already exists in metastore")
        self.teams.append(team)
        
        # Register all team's workspaces (deduplication handled by registry)
        # Note: This will be fully implemented when Team model is complete
    
    @property
    def securable_type(self) -> SecurableType:
        """Return the securable type."""
        return SecurableType.METASTORE
