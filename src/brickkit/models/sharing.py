"""
Delta Sharing models for Unity Catalog.

This module implements the Delta Sharing governance models for cross-organization
data sharing in Unity Catalog.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from pydantic import Field, computed_field, field_validator, model_validator

from .base import DEFAULT_SECURABLE_OWNER, BaseModel, BaseSecurable, get_current_environment
from .enums import SecurableType
from .grants import Principal
from .references import ModelReference, TableReference, VolumeReference
from .schemas import Schema


class AuthenticationType(str, Enum):
    """Authentication type for sharing."""
    TOKEN = "TOKEN"
    DATABRICKS = "DATABRICKS"


class SharingStatus(str, Enum):
    """Status of a sharing relationship."""
    PENDING = "PENDING"
    ACTIVE = "ACTIVE"
    SUSPENDED = "SUSPENDED"
    REVOKED = "REVOKED"


class Provider(BaseSecurable):
    """
    Organization that shares data with external recipients.
    Level 1 object (Metastore level).
    """

    name: str = Field(
        ...,
        description="Provider name (gets environment suffix at runtime)"
    )

    comment: Optional[str] = Field(
        None,
        max_length=1024,
        description="Description of the provider"
    )

    owner: Optional[Principal] = Field(
        default_factory=lambda: Principal(name=DEFAULT_SECURABLE_OWNER, add_environment_suffix=False),
        description="Owner principal"
    )

    status: SharingStatus = Field(
        default=SharingStatus.ACTIVE,
        description="Provider status"
    )

    authentication_type: AuthenticationType = Field(
        default=AuthenticationType.TOKEN,
        description="Authentication method for recipients"
    )

    cloud: Optional[str] = Field(
        None,
        description="Cloud provider (AWS, Azure, GCP)"
    )

    region: Optional[str] = Field(
        None,
        description="Cloud region"
    )

    recipient_profile: Optional[Dict[str, Any]] = Field(
        None,
        description="Recipient profile configuration"
    )

    @computed_field
    @property
    def resolved_name(self) -> str:
        """Name with environment suffix."""
        env = get_current_environment()
        return f"{self.name}_{env.value.lower()}"

    @property
    def name_with_env(self) -> str:
        """Alias for resolved_name for consistency."""
        return self.resolved_name

    @property
    def fqdn(self) -> str:
        """Provider uses just name with environment suffix."""
        return self.resolved_name

    @property
    def securable_type(self) -> SecurableType:
        """Type of this securable."""
        return SecurableType.PROVIDER

    def to_sdk_create_params(self) -> Dict[str, Any]:
        """Convert to SDK provider create parameters."""
        params = {
            "name": self.resolved_name,
            "authentication_type": self.authentication_type.value
        }

        if self.comment:
            params["comment"] = self.comment
        if self.recipient_profile:
            params["recipient_profile"] = self.recipient_profile

        return params

    def to_sdk_update_params(self) -> Dict[str, Any]:
        """Convert to SDK provider update parameters."""
        params = {"name": self.resolved_name}

        if self.comment:
            params["comment"] = self.comment
        if self.owner:
            params["owner"] = self.owner.resolved_name

        return params


class Recipient(BaseSecurable):
    """
    External organization that receives shared data.
    Level 1 object (Metastore level).
    """

    name: str = Field(
        ...,
        description="Recipient name (gets environment suffix at runtime)"
    )

    comment: Optional[str] = Field(
        None,
        max_length=1024,
        description="Description of the recipient"
    )

    owner: Optional[Principal] = Field(
        default_factory=lambda: Principal(name=DEFAULT_SECURABLE_OWNER, add_environment_suffix=False),
        description="Owner principal"
    )

    authentication_type: AuthenticationType = Field(
        default=AuthenticationType.TOKEN,
        description="Authentication method"
    )

    sharing_code: Optional[str] = Field(
        None,
        description="Sharing code for DATABRICKS authentication"
    )

    ip_access_list: List[str] = Field(
        default_factory=list,
        description="IP addresses/ranges allowed to access shares"
    )

    status: SharingStatus = Field(
        default=SharingStatus.PENDING,
        description="Current status of the recipient"
    )

    activation_url: Optional[str] = Field(
        None,
        description="URL for recipient activation"
    )

    activated_at: Optional[datetime] = Field(
        None,
        description="When the recipient was activated"
    )

    metastore_id: Optional[str] = Field(
        None,
        description="Metastore ID for Databricks-to-Databricks sharing"
    )

    cloud: Optional[str] = Field(
        None,
        description="Recipient's cloud provider"
    )

    region: Optional[str] = Field(
        None,
        description="Recipient's cloud region"
    )

    @field_validator('ip_access_list')
    def validate_ip_list(cls, v):
        """Validate IP addresses/CIDR ranges."""
        # Basic validation - could be enhanced with ipaddress module
        for ip in v:
            if not ip:
                raise ValueError("Empty IP address in access list")
        return v

    @model_validator(mode='after')
    def validate_databricks_auth(self):
        """Validate Databricks authentication requirements."""
        if self.authentication_type == AuthenticationType.DATABRICKS:
            if not self.sharing_code and not self.metastore_id:
                raise ValueError(
                    "DATABRICKS authentication requires sharing_code or metastore_id"
                )
        return self

    @computed_field
    @property
    def resolved_name(self) -> str:
        """Name with environment suffix."""
        env = get_current_environment()
        return f"{self.name}_{env.value.lower()}"

    @property
    def name_with_env(self) -> str:
        """Alias for resolved_name for consistency."""
        return self.resolved_name

    @property
    def fqdn(self) -> str:
        """Recipient uses just name with environment suffix."""
        return self.resolved_name

    @property
    def securable_type(self) -> SecurableType:
        """Type of this securable."""
        return SecurableType.RECIPIENT

    def to_sdk_create_params(self) -> Dict[str, Any]:
        """Convert to SDK recipient create parameters."""
        params = {
            "name": self.resolved_name,
            "authentication_type": self.authentication_type.value
        }

        if self.comment:
            params["comment"] = self.comment
        if self.sharing_code:
            params["sharing_code"] = self.sharing_code
        if self.ip_access_list:
            params["ip_access_list"] = {
                "allowed_ip_addresses": self.ip_access_list
            }
        if self.metastore_id:
            params["data_recipient_global_metastore_id"] = self.metastore_id

        return params

    def to_sdk_update_params(self) -> Dict[str, Any]:
        """Convert to SDK recipient update parameters."""
        params = {"name": self.resolved_name}

        if self.comment:
            params["comment"] = self.comment
        if self.owner:
            params["owner"] = self.owner.resolved_name
        if self.ip_access_list:
            params["ip_access_list"] = {
                "allowed_ip_addresses": self.ip_access_list
            }

        return params


class SharedObject(BaseModel):
    """Base class for objects shared through Delta Sharing."""

    name: str = Field(
        ...,
        description="Name exposed to recipients (can differ from source)"
    )

    comment: Optional[str] = Field(
        None,
        description="Description of the shared object"
    )

    data_object_type: Optional[str] = Field(
        None,
        description="Type of shared object (TABLE, VOLUME, SCHEMA, MODEL)"
    )

    fqdn: Optional[str] = Field(
        None,
        description="Fully qualified name of the source object"
    )

    shared_columns: Optional[List[str]] = Field(
        None,
        description="Columns shared (for tables)"
    )

    added_at: Optional[datetime] = Field(
        None,
        description="When object was added to share"
    )

    added_by: Optional[str] = Field(
        None,
        description="Principal who added the object"
    )

    cdf_enabled: bool = Field(
        default=False,
        description="Enable Change Data Feed for incremental updates"
    )

    history_data_sharing_status: Optional[str] = Field(
        None,
        description="Status of historical data sharing"
    )

    partitions: List[Dict[str, str]] = Field(
        default_factory=list,
        description="Partition filters for the shared object"
    )


class SharedTable(SharedObject):
    """Table shared through Delta Sharing."""

    table: Union[TableReference, str] = Field(
        ...,
        description="Table reference or FQDN to share"
    )

    columns: Optional[List[str]] = Field(
        None,
        description="Specific columns to share (None = all columns)"
    )

    def get_table_fqdn(self) -> str:
        """Get the fully qualified table name."""
        if isinstance(self.table, TableReference):
            return self.table.full_name
        return self.table

    def to_sdk_params(self) -> Dict[str, Any]:
        """Convert to SDK share table parameters."""
        params = {
            "name": self.name,
            "schema_name": self.get_table_fqdn().rsplit('.', 1)[0],
            "cdf_enabled": self.cdf_enabled
        }

        if self.comment:
            params["comment"] = self.comment
        if self.partitions:
            params["partitions"] = self.partitions

        return params


class SharedVolume(SharedObject):
    """Volume shared through Delta Sharing."""

    volume: Union[VolumeReference, str] = Field(
        ...,
        description="Volume reference or FQDN to share"
    )

    def get_volume_fqdn(self) -> str:
        """Get the fully qualified volume name."""
        if isinstance(self.volume, VolumeReference):
            return self.volume.full_name
        return self.volume

    def to_sdk_params(self) -> Dict[str, Any]:
        """Convert to SDK share volume parameters."""
        params = {
            "name": self.name,
            "volume_name": self.get_volume_fqdn()
        }

        if self.comment:
            params["comment"] = self.comment

        return params


class SharedSchema(SharedObject):
    """Schema shared through Delta Sharing."""

    schema: Union[Schema, str] = Field(
        ...,
        description="Schema object or FQDN to share"
    )

    def get_schema_fqdn(self) -> str:
        """Get the fully qualified schema name."""
        if isinstance(self.schema, Schema):
            return self.schema.fqdn
        return self.schema

    def to_sdk_params(self) -> Dict[str, Any]:
        """Convert to SDK share schema parameters."""
        params = {
            "name": self.name,
            "schema_name": self.get_schema_fqdn()
        }

        if self.comment:
            params["comment"] = self.comment

        return params


class SharedModel(SharedObject):
    """ML Model shared through Delta Sharing."""

    model: Union[ModelReference, str] = Field(
        ...,
        description="Model reference or FQDN to share"
    )

    model_version: Optional[int] = Field(
        None,
        description="Specific version to share (None = latest)"
    )

    def get_model_fqdn(self) -> str:
        """Get the fully qualified model name."""
        if isinstance(self.model, ModelReference):
            return self.model.full_name
        return self.model

    def to_sdk_params(self) -> Dict[str, Any]:
        """Convert to SDK share model parameters."""
        params = {
            "name": self.name,
            "model_name": self.get_model_fqdn()
        }

        if self.comment:
            params["comment"] = self.comment
        if self.model_version:
            params["model_version"] = self.model_version

        return params


class Share(BaseSecurable):
    """
    Collection of objects shared with recipients.
    Level 1 object (Metastore level).
    """

    name: str = Field(
        ...,
        description="Share name (gets environment suffix at runtime)"
    )

    comment: Optional[str] = Field(
        None,
        max_length=1024,
        description="Description of the share"
    )

    owner: Optional[Principal] = Field(
        default_factory=lambda: Principal(name=DEFAULT_SECURABLE_OWNER, add_environment_suffix=False),
        description="Owner principal"
    )

    status: SharingStatus = Field(
        default=SharingStatus.ACTIVE,
        description="Share status"
    )

    # Objects in the share - unified list for all object types
    objects: List[SharedObject] = Field(
        default_factory=list,
        description="All objects shared in this share"
    )

    # Deprecated: keeping for backward compatibility
    tables: List[SharedTable] = Field(
        default_factory=list,
        description="Tables shared in this share (deprecated, use objects)"
    )
    volumes: List[SharedVolume] = Field(
        default_factory=list,
        description="Volumes shared in this share (deprecated, use objects)"
    )
    schemas: List[SharedSchema] = Field(
        default_factory=list,
        description="Schemas shared in this share (deprecated, use objects)"
    )
    models: List[SharedModel] = Field(
        default_factory=list,
        description="Models shared in this share (deprecated, use objects)"
    )

    # Recipients with access
    recipients: List[Union[Recipient, str]] = Field(
        default_factory=list,
        description="Recipients with access to this share"
    )

    @computed_field
    @property
    def resolved_name(self) -> str:
        """Name with environment suffix."""
        env = get_current_environment()
        return f"{self.name}_{env.value.lower()}"

    @property
    def name_with_env(self) -> str:
        """Alias for resolved_name for consistency."""
        return self.resolved_name

    @property
    def fqdn(self) -> str:
        """Share uses just name with environment suffix."""
        return self.resolved_name

    @property
    def securable_type(self) -> SecurableType:
        """Type of this securable."""
        return SecurableType.SHARE

    def add_table(
        self,
        table: Union[TableReference, str],
        name: Optional[str] = None,
        columns: Optional[List[str]] = None,
        cdf_enabled: bool = False,
        partitions: Optional[List[Dict[str, str]]] = None,
        comment: Optional[str] = None
    ) -> None:
        """Add a table to the share."""
        if name is None:
            name = table.name if isinstance(table, TableReference) else table.split('.')[-1]

        # Get FQDN
        fqdn = table.full_name if isinstance(table, TableReference) else table

        # Create SharedObject with all attributes
        shared_obj = SharedObject(
            name=name,
            data_object_type="TABLE",
            fqdn=fqdn,
            shared_columns=columns,
            cdf_enabled=cdf_enabled,
            partitions=partitions or [],
            comment=comment
        )
        self.objects.append(shared_obj)

        # Also create SharedTable for backward compatibility
        shared_table = SharedTable(
            name=name,
            table=table,
            columns=columns,
            cdf_enabled=cdf_enabled,
            partitions=partitions or [],
            comment=comment
        )
        self.tables.append(shared_table)

    def add_volume(
        self,
        volume: Union[VolumeReference, str],
        name: Optional[str] = None,
        comment: Optional[str] = None
    ) -> None:
        """Add a volume to the share."""
        if name is None:
            name = volume.name if isinstance(volume, VolumeReference) else volume.split('.')[-1]

        # Get FQDN
        fqdn = volume.full_name if isinstance(volume, VolumeReference) else volume

        # Create SharedObject with all attributes
        shared_obj = SharedObject(
            name=name,
            data_object_type="VOLUME",
            fqdn=fqdn,
            comment=comment
        )
        self.objects.append(shared_obj)

        # Also create SharedVolume for backward compatibility
        shared_volume = SharedVolume(
            name=name,
            volume=volume,
            comment=comment
        )
        self.volumes.append(shared_volume)

    def add_schema(
        self,
        schema: Union[Schema, str],
        name: Optional[str] = None,
        comment: Optional[str] = None
    ) -> None:
        """Add an entire schema to the share."""
        if name is None:
            name = schema.name if isinstance(schema, Schema) else schema.split('.')[-1]

        shared_schema = SharedSchema(
            name=name,
            schema=schema,
            comment=comment
        )
        self.schemas.append(shared_schema)

    def add_model(
        self,
        model: Union[ModelReference, str],
        name: Optional[str] = None,
        version: Optional[int] = None,
        comment: Optional[str] = None
    ) -> None:
        """Add an ML model to the share."""
        if name is None:
            name = model.name if isinstance(model, ModelReference) else model.split('.')[-1]

        # Get FQDN
        fqdn = model.full_name if isinstance(model, ModelReference) else model

        # Create SharedObject with all attributes
        shared_obj = SharedObject(
            name=name,
            data_object_type="MODEL",
            fqdn=fqdn,
            comment=comment
        )
        self.objects.append(shared_obj)

        # Also create SharedModel for backward compatibility
        shared_model = SharedModel(
            name=name,
            model=model,
            model_version=version,
            comment=comment
        )
        self.models.append(shared_model)

    def grant_to_recipient(self, recipient: Union[Recipient, str]) -> None:
        """Grant access to a recipient."""
        # Get the resolved name for comparison
        recipient_name = recipient.resolved_name if isinstance(recipient, Recipient) else recipient

        # Check if not already granted
        existing_names = [
            r.resolved_name if isinstance(r, Recipient) else r
            for r in self.recipients
        ]
        if recipient_name not in existing_names:
            self.recipients.append(recipient_name)

    def revoke_from_recipient(self, recipient: Union[Recipient, str]) -> None:
        """Revoke access from a recipient."""
        # Get the resolved name for comparison
        recipient_name = recipient.resolved_name if isinstance(recipient, Recipient) else recipient

        # Remove by name
        self.recipients = [
            r for r in self.recipients
            if (r.resolved_name if isinstance(r, Recipient) else r) != recipient_name
        ]

    def get_recipients(self) -> List[str]:
        """Get list of recipient names with access."""
        recipients = []
        for r in self.recipients:
            if isinstance(r, Recipient):
                recipients.append(r.name_with_env)
            else:
                recipients.append(r)
        return recipients

    def to_sdk_create_params(self) -> Dict[str, Any]:
        """Convert to SDK share create parameters."""
        params = {
            "name": self.name_with_env
        }

        if self.comment:
            params["comment"] = self.comment

        return params

    def to_sdk_update_params(self) -> Dict[str, Any]:
        """Convert to SDK share update parameters."""
        params = {"name": self.resolved_name}

        if self.comment:
            params["comment"] = self.comment
        if self.owner:
            params["owner"] = self.owner.resolved_name

        # Include object updates
        updates = []

        for table in self.tables:
            update = {
                "action": "ADD",
                "data_object": {
                    "type": "TABLE",
                    **table.to_sdk_params()
                }
            }
            updates.append(update)

        for volume in self.volumes:
            update = {
                "action": "ADD",
                "data_object": {
                    "type": "VOLUME",
                    **volume.to_sdk_params()
                }
            }
            updates.append(update)

        for schema in self.schemas:
            update = {
                "action": "ADD",
                "data_object": {
                    "type": "SCHEMA",
                    **schema.to_sdk_params()
                }
            }
            updates.append(update)

        for model in self.models:
            update = {
                "action": "ADD",
                "data_object": {
                    "type": "MODEL",
                    **model.to_sdk_params()
                }
            }
            updates.append(update)

        if updates:
            params["updates"] = updates

        return params

    def to_sdk_add_object_params(self, obj: SharedObject) -> Dict[str, Any]:
        """
        Convert a shared object to SDK parameters for adding to share.
        
        Args:
            obj: The SharedObject to add
            
        Returns:
            SDK parameters for the object itself (not the share update)
        """
        # Return the object parameters, not the share update parameters
        params = {
            "name": obj.name,
            "data_object_type": obj.data_object_type,
            "full_name": obj.fqdn
        }

        # Add optional fields if present
        if obj.shared_columns:
            params["shared_columns"] = obj.shared_columns
        if obj.cdf_enabled:
            params["cdf_enabled"] = obj.cdf_enabled
        if obj.comment:
            params["comment"] = obj.comment
        if obj.partitions:
            params["partitions"] = obj.partitions

        return params

    def __str__(self) -> str:
        """String representation showing share contents."""
        counts = []
        if self.tables:
            counts.append(f"{len(self.tables)} tables")
        if self.volumes:
            counts.append(f"{len(self.volumes)} volumes")
        if self.schemas:
            counts.append(f"{len(self.schemas)} schemas")
        if self.models:
            counts.append(f"{len(self.models)} models")
        if self.recipients:
            counts.append(f"{len(self.recipients)} recipients")

        content = ", ".join(counts) if counts else "empty"
        return f"Share '{self.name_with_env}' ({content})"


class OnlineTable(BaseSecurable):
    """
    Online feature store table for real-time serving.
    Level 3 object (under Schema).
    """

    name: str = Field(
        ...,
        description="Online table name (gets environment suffix at runtime)"
    )

    comment: Optional[str] = Field(
        None,
        max_length=1024,
        description="Description of the online table"
    )

    owner: Optional[Principal] = Field(
        default_factory=lambda: Principal(name=DEFAULT_SECURABLE_OWNER, add_environment_suffix=False),
        description="Owner principal"
    )

    catalog_name: Optional[str] = Field(
        None,
        description="Name of parent catalog"
    )

    schema_name: Optional[str] = Field(
        None,
        description="Name of parent schema"
    )

    source_table_fqdn: str = Field(
        ...,
        description="Fully qualified name of source Delta table"
    )

    primary_key_columns: List[str] = Field(
        ...,
        description="Columns that form the primary key"
    )

    timeseries_key_column: Optional[str] = Field(
        None,
        description="Column for time-based lookups"
    )

    snapshot_trigger: Optional[Dict[str, Any]] = Field(
        None,
        description="Trigger configuration for snapshot updates"
    )

    refresh_schedule: Optional[str] = Field(
        None,
        description="Cron schedule for refreshing from source"
    )

    @computed_field
    @property
    def resolved_name(self) -> str:
        """Name with environment suffix."""
        env = get_current_environment()
        return f"{self.name}_{env.value.lower()}"

    @property
    def name_with_env(self) -> str:
        """Alias for resolved_name for consistency."""
        return self.resolved_name

    @computed_field
    @property
    def fqdn(self) -> str:
        """Build fully qualified name."""
        parts = []
        if self.catalog_name:
            parts.append(self.catalog_name)
        if self.schema_name:
            parts.append(self.schema_name)
        parts.append(self.resolved_name)
        return ".".join(parts)

    @property
    def securable_type(self) -> SecurableType:
        """Type of this securable."""
        # OnlineTable doesn't have a specific SecurableType in Unity Catalog
        # It's a special type of table, so we use TABLE
        return SecurableType.TABLE

    def to_sdk_create_params(self) -> Dict[str, Any]:
        """Convert to SDK online table create parameters."""
        spec: Dict[str, Any] = {
            "source_table_full_name": self.source_table_fqdn,
            "primary_key_columns": self.primary_key_columns
        }

        if self.timeseries_key_column:
            spec["timeseries_key"] = self.timeseries_key_column
        if self.snapshot_trigger:
            spec["snapshot_trigger"] = self.snapshot_trigger

        params: Dict[str, Any] = {
            "name": self.resolved_name,
            "spec": spec
        }

        if self.catalog_name:
            params["catalog_name"] = self.catalog_name
        if self.schema_name:
            params["schema_name"] = self.schema_name

        return params

    def to_sdk_update_params(self) -> Dict[str, Any]:
        """Convert to SDK online table update parameters."""
        params = {
            "name": self.fqdn
        }

        if self.refresh_schedule:
            params["spec"] = {
                "refresh_schedule": self.refresh_schedule
            }

        return params
