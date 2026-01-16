"""
Unity Catalog ML/AI model definitions.

This module contains Pydantic models for machine learning assets in Unity Catalog:
- RegisteredModel: Container for ML model versions (Level 3 - under Schema)
- ModelVersion: Specific version of a model (Level 4 - under RegisteredModel)
- ServiceCredential: API credentials for AI services (Level 1)
- ModelServingEndpoint: Production model deployment endpoints

Note: VectorSearchIndex and VectorSearchEndpoint have been moved to models.vector_search.
"""

from __future__ import annotations
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
from enum import Enum
from pydantic import Field, field_validator, computed_field

from .base import BaseGovernanceModel, BaseSecurable, DEFAULT_SECURABLE_OWNER, get_current_environment
from .enums import SecurableType
from .grants import Privilege, Principal


class ModelVersionStatus(str, Enum):
    """Status of a model version."""
    PENDING_REGISTRATION = "PENDING_REGISTRATION"
    READY = "READY"
    FAILED_REGISTRATION = "FAILED_REGISTRATION"
    PENDING_DELETION = "PENDING_DELETION"


class ModelVersionStage(str, Enum):
    """Stage of a model version (deprecated in favor of aliases)."""
    NONE = "None"
    ARCHIVED = "Archived"
    STAGING = "Staging"
    PRODUCTION = "Production"


class RegisteredModel(BaseSecurable):
    """
    Represents a registered ML model in Unity Catalog.
    
    A RegisteredModel is a container for model versions, similar to how a
    schema contains tables. It lives at Level 3 in the hierarchy under a schema.
    
    Hierarchy: Catalog → Schema → RegisteredModel → ModelVersion
    """
    
    # Model identification
    name: str = Field(
        ...,
        description="Model name (gets environment suffix at runtime)"
    )
    
    comment: Optional[str] = Field(
        None,
        max_length=1024,
        description="Description of the model"
    )
    
    owner: Optional[Principal] = Field(
        default_factory=lambda: Principal(name=DEFAULT_SECURABLE_OWNER, add_environment_suffix=False),
        description="Owner principal (defaults to parent schema owner)"
    )
    
    catalog_name: Optional[str] = Field(
        None,
        description="Name of parent catalog (inherited from schema if not set)"
    )
    schema_name: Optional[str] = Field(
        None,
        description="Name of parent schema"
    )
    
    # Model metadata
    description: Optional[str] = Field(
        None,
        description="Description of the model"
    )
    
    # Model aliases (replaces stages)
    aliases: Dict[str, int] = Field(
        default_factory=dict,
        description="Aliases mapping to version numbers (e.g., 'champion': 5)"
    )
    
    # MLflow integration
    storage_location: Optional[str] = Field(
        None,
        description="Storage location for model artifacts"
    )
    
    # Tracking
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    created_by: Optional[str] = None
    updated_by: Optional[str] = None
    
    # Model versions (child objects)
    versions: List[ModelVersion] = Field(
        default_factory=list,
        description="Model versions in this registered model"
    )
    
    # Parent reference
    _parent_schema: Optional[Any] = None
    _parent_catalog: Optional[Any] = None
    
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
    def resolved_catalog_name(self) -> str:
        """Get catalog name with environment suffix for runtime resolution."""
        if self._parent_catalog:
            return self._parent_catalog.resolved_name
        if not self.catalog_name:
            raise ValueError(f"Model '{self.name}' is not associated with a catalog")
        # Fallback to string field with current env if no parent
        env = get_current_environment()
        return f"{self.catalog_name}_{env.value.lower()}"
    
    @computed_field
    @property
    def fqdn(self) -> str:
        """Fully qualified domain name."""
        if self.catalog_name and self.schema_name:
            # ML models don't get env suffix (catalog already has it)
            return f"{self.resolved_catalog_name}.{self.schema_name}.{self.name}"
        return self.name
    
    @computed_field
    @property
    def securable_type(self) -> SecurableType:
        """Type of this securable."""
        return SecurableType.MODEL
    
    def add_version(self, version: ModelVersion) -> None:
        """
        Add a model version to this registered model.
        
        Args:
            version: The ModelVersion to add
            
        Raises:
            ValueError: If a version with the same number already exists
        """
        version._parent_model = self
        version.model_name = self.name
        version.catalog_name = self.catalog_name
        version.schema_name = self.schema_name
        
        # Auto-increment version number if not set
        if version.version is None:
            version.version = len(self.versions) + 1
        
        # Check for duplicate version number
        if any(v.version == version.version for v in self.versions):
            raise ValueError(f"Version {version.version} already exists in model '{self.name}'")
        
        self.versions.append(version)
        
        # Update aliases mapping
        for alias in version.aliases:
            self.aliases[alias] = version.version
    
    def get_version(self, version: Union[int, str]) -> Optional[ModelVersion]:
        """
        Get a specific version by number or alias.
        
        Args:
            version: Version number or alias name
            
        Returns:
            ModelVersion if found, None otherwise
        """
        if isinstance(version, str):
            # Look up by alias
            if version in self.aliases:
                version = self.aliases[version]
            else:
                return None
        
        for v in self.versions:
            if v.version == version:
                return v
        return None
    
    def get_latest_version(self) -> Optional[ModelVersion]:
        """
        Get the latest (highest version number) model version.
        
        Returns:
            Latest ModelVersion if any exist, None otherwise
        """
        if not self.versions:
            return None
        return max(self.versions, key=lambda v: v.version)
    
    @computed_field
    @property
    def latest_version_number(self) -> Optional[int]:
        """
        Get the latest (highest) version number.
        
        Returns:
            Latest version number if any versions exist, None otherwise
        """
        latest = self.get_latest_version()
        return latest.version if latest else None
    
    def get_version_by_alias(self, alias: str) -> Optional[ModelVersion]:
        """
        Get a model version by its alias.
        
        Args:
            alias: Alias name (e.g., 'champion', 'production')
            
        Returns:
            ModelVersion if found, None otherwise
        """
        if alias in self.aliases:
            return self.get_version(self.aliases[alias])
        return None
    
    def set_alias(self, alias: str, version: int) -> None:
        """
        Set an alias for a model version.
        
        Common aliases: 'champion', 'challenger', 'baseline'
        
        Args:
            alias: Alias name
            version: Version number
        """
        self.aliases[alias] = version
        # Also update the version's alias list
        v = self.get_version(version)
        if v and alias not in v.aliases:
            v.aliases.append(alias)
    
    def get_level_1_name(self) -> str:
        """Get the level-1 (catalog) name for this model."""
        if self._parent_catalog:
            return self._parent_catalog.resolved_name
        elif self.catalog_name:
            env = get_current_environment()
            return f"{self.catalog_name}_{env.value.lower()}"
        return ""
    
    def grant(self, principal: Any, policy: Any, _skip_validation: bool = False) -> List[Privilege]:
        """
        Grant privileges on this model.

        Privileges propagate to all versions.

        Args:
            principal: Principal to grant to
            policy: AccessPolicy defining privileges
            _skip_validation: Internal flag to skip dependency validation during propagation

        Returns:
            List of Privilege objects created
        """
        privileges = super().grant(principal, policy, _skip_validation=_skip_validation)
        
        # Propagate to versions
        for version in self.versions:
            child_privs = version.grant(principal, policy)
            privileges.extend(child_privs)
        
        return privileges
    
    def to_sdk_create_params(self) -> Dict[str, Any]:
        """Convert to SDK parameters for model creation."""
        params = {
            "name": self.name,  # ML models don't get env suffix (catalog/schema already have it)
            "catalog_name": self.catalog_name,
            "schema_name": self.schema_name,
        }
        
        if self.comment:
            params["comment"] = self.comment
        
        if self.description:
            params["description"] = self.description
        
        if self.storage_location:
            params["storage_location"] = self.storage_location
        
        return params
    
    def to_sdk_update_params(self) -> Dict[str, Any]:
        """Convert to SDK parameters for model update."""
        params = {
            "full_name": self.fqdn,
        }
        
        if self.comment:
            params["comment"] = self.comment
        
        if self.owner:
            params["owner"] = self.owner.resolved_name
        
        return params


class ModelVersion(BaseGovernanceModel):
    """
    Represents a specific version of a registered model.
    
    ModelVersions are immutable snapshots of a model at a point in time.
    They are Level 4 objects under RegisteredModel.
    """
    
    # Version identification
    version: Optional[int] = Field(
        None,
        description="Version number (auto-incremented if not set)"
    )
    model_name: Optional[str] = Field(
        None,
        description="Name of parent registered model"
    )
    catalog_name: Optional[str] = Field(
        None,
        description="Name of parent catalog"
    )
    schema_name: Optional[str] = Field(
        None,
        description="Name of parent schema"
    )
    
    # Version metadata
    description: Optional[str] = Field(
        None,
        description="Description of this version"
    )
    comment: Optional[str] = Field(
        None,
        description="Comment for this version"
    )
    
    # MLflow integration
    run_id: Optional[str] = Field(
        None,
        description="MLflow run ID that created this version"
    )
    run_link: Optional[str] = Field(
        None,
        description="Link to MLflow run"
    )
    source: Optional[str] = Field(
        None,
        description="Source path of model artifacts"
    )
    
    # Status tracking
    status: ModelVersionStatus = Field(
        ModelVersionStatus.PENDING_REGISTRATION,
        description="Current status of the version"
    )
    status_message: Optional[str] = None
    
    # Model metadata
    tags: Dict[str, str] = Field(
        default_factory=dict,
        description="Key-value tags for the version"
    )
    
    # Aliases for this version (e.g., "champion", "production")
    aliases: List[str] = Field(
        default_factory=list,
        description="Aliases for this version"
    )
    
    # Tracking
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    created_by: Optional[str] = None
    updated_by: Optional[str] = None
    
    # Parent reference
    _parent_model: Optional[RegisteredModel] = None
    
    @computed_field
    @property
    def fqdn(self) -> str:
        """Fully qualified name including version."""
        if self.catalog_name and self.schema_name and self.model_name:
            return f"{self.catalog_name}.{self.schema_name}.{self.model_name}.v{self.version}"
        return f"{self.model_name}.v{self.version}" if self.model_name else str(self.version)
    
    def grant(self, principal: Any, policy: Any) -> List[Privilege]:
        """
        Grant privileges on this model version.
        
        Args:
            principal: Principal to grant to
            policy: AccessPolicy defining privileges
            
        Returns:
            List of Privilege objects created
        """
        privileges = []
        
        for privilege_type in policy.get_privileges(SecurableType.MODEL):
            priv = Privilege(
                principal=principal.resolved_name,
                privilege=privilege_type,
                securable_type=SecurableType.MODEL,
                securable_name=self.fqdn
            )
            privileges.append(priv)
        
        return privileges
    
    def to_sdk_create_params(self) -> Dict[str, Any]:
        """Convert to SDK parameters for version creation."""
        params = {
            "name": self.model_name,
            "source": self.source,
        }
        
        if self.run_id:
            params["run_id"] = self.run_id
        
        if self.description:
            params["description"] = self.description
        
        if self.tags:
            params["tags"] = [{"key": k, "value": v} for k, v in self.tags.items()]
        
        return params


class ServiceCredentialPurpose(str, Enum):
    """Purpose of service credential."""
    AI_GATEWAY = "AI_GATEWAY"
    MODEL_SERVING = "MODEL_SERVING"
    VECTOR_SEARCH = "VECTOR_SEARCH"
    CUSTOM = "CUSTOM"


class ServiceCredential(BaseSecurable):
    """
    Represents service credentials for AI/ML services.
    
    ServiceCredentials are Level 1 objects (like StorageCredential) that
    manage authentication to external AI services like OpenAI, Anthropic, etc.
    """
    
    # Credential identification
    name: str = Field(
        ...,
        description="Credential name (gets environment suffix at runtime)"
    )
    
    comment: Optional[str] = Field(
        None,
        max_length=1024,
        description="Description of the credential"
    )
    
    owner: Optional[Principal] = Field(
        default_factory=lambda: Principal(name=DEFAULT_SECURABLE_OWNER, add_environment_suffix=False),
        description="Owner principal"
    )
    
    # Credential metadata
    service_type: Optional[str] = Field(
        None,
        description="Type of service (OPENAI, ANTHROPIC, AZURE_OPENAI, etc.)"
    )
    
    endpoint_url: Optional[str] = Field(
        None,
        description="Service endpoint URL"
    )
    
    purpose: Optional[str] = Field(
        None,
        description="Purpose or use case for this credential"
    )
    
    # Provider-specific options (only one should be set)
    openai_api_key: Optional[str] = Field(
        None,
        description="OpenAI API key (stored securely)"
    )
    
    anthropic_api_key: Optional[str] = Field(
        None,
        description="Anthropic API key (stored securely)"
    )
    
    azure_openai_config: Optional[Dict[str, str]] = Field(
        None,
        description="Azure OpenAI configuration"
    )
    
    custom_config: Optional[Dict[str, Any]] = Field(
        None,
        description="Custom service configuration"
    )
    
    # Access control
    allowed_endpoints: List[str] = Field(
        default_factory=list,
        description="List of allowed model serving endpoints"
    )
    
    # Tracking
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    created_by: Optional[str] = None
    
    @field_validator('openai_api_key', 'anthropic_api_key')
    def validate_api_key(cls, v: Optional[str]) -> Optional[str]:
        """Validate API key format."""
        if v and not v.startswith(("sk-", "anthropic-")):
            # In production, keys should be references to secret scope
            if not v.startswith("{{secrets/"):
                raise ValueError("API keys should reference secret scope: {{secrets/scope/key}}")
        return v
    
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
        """FQDN is just resolved name for Level 1 objects."""
        return self.resolved_name
    
    @computed_field
    @property
    def securable_type(self) -> SecurableType:
        """Type of this securable."""
        return SecurableType.SERVICE_CREDENTIAL

    def get_level_1_name(self) -> str:
        """Get level-1 name for this service credential."""
        return self.resolved_name

    def to_sdk_create_params(self) -> Dict[str, Any]:
        """Convert to SDK parameters for credential creation."""
        params = {
            "name": self.resolved_name,
        }
        
        # Add purpose if it's a valid enum value
        if self.purpose and self.purpose.upper() in ['AI_GATEWAY', 'MODEL_SERVING', 'VECTOR_SEARCH', 'CUSTOM']:
            params["purpose"] = self.purpose.upper()
        elif self.purpose:
            # Custom purpose, use CUSTOM enum with comment
            params["purpose"] = "CUSTOM"
        
        if self.comment:
            params["comment"] = self.comment
        
        # Add provider-specific configuration
        if self.openai_api_key:
            params["openai_api_key"] = self.openai_api_key
        elif self.anthropic_api_key:
            params["anthropic_api_key"] = self.anthropic_api_key
        elif self.azure_openai_config:
            params["azure_openai_config"] = self.azure_openai_config
        elif self.custom_config:
            params["custom_config"] = self.custom_config
        
        return params


class ModelServingEndpoint(BaseGovernanceModel):
    """
    Represents a model serving endpoint for production deployment.
    
    Endpoints serve registered models and can handle multiple model versions
    with traffic splitting, A/B testing, and auto-scaling.
    """
    
    name: str = Field(..., description="Endpoint name")
    
    comment: Optional[str] = Field(
        None,
        description="Description of the endpoint"
    )
    
    @computed_field
    @property
    def resolved_name(self) -> str:
        """Name with environment suffix."""
        env = get_current_environment()
        return f"{self.name}_{env.value.lower()}"
    
    # Served models list - direct attribute for easier access
    served_models: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="List of models served by this endpoint"
    )
    
    # External service credential (for external models like OpenAI)
    service_credential: Optional[ServiceCredential] = Field(
        None,
        description="Credential for external AI services"
    )
    
    # Served models configuration
    config: Dict[str, Any] = Field(
        default_factory=dict,
        description="Endpoint configuration including served models"
    )
    
    # Example config structure:
    # {
    #   "served_models": [
    #     {
    #       "model_name": "catalog.schema.model",
    #       "model_version": "1",
    #       "workload_size": "Small",
    #       "scale_to_zero_enabled": true
    #     }
    #   ],
    #   "traffic_config": {
    #     "routes": [
    #       {"served_model_name": "model-v1", "traffic_percentage": 100}
    #     ]
    #   }
    # }
    
    # Optimization
    route_optimized: bool = Field(
        False,
        description="Whether to optimize routing"
    )
    
    # State
    state: str = Field(
        "NOT_READY",
        description="Current state (NOT_READY, READY, CREATING, UPDATING, FAILED)"
    )
    
    # Tracking
    creation_timestamp: Optional[datetime] = None
    last_updated_timestamp: Optional[datetime] = None
    creator: Optional[str] = None
    
    def add_served_model(
        self,
        model: RegisteredModel,
        version: Union[int, str] = "latest",
        workload_size: str = "Small",
        scale_to_zero: bool = True
    ) -> None:
        """
        Add a model to serve on this endpoint.
        
        Args:
            model: RegisteredModel to serve
            version: Version number or alias
            workload_size: Size of compute (Small, Medium, Large)
            scale_to_zero: Whether to scale to zero when idle
        """
        served_config = {
            "model_name": model.fqdn,
            "model_version": str(version),  # Convert to string for consistency
            "workload_size": workload_size,
            "scale_to_zero_enabled": scale_to_zero
        }
        
        # Add to both served_models list and config
        self.served_models.append(served_config)
        
        # Also maintain in config for SDK compatibility
        if "served_models" not in self.config:
            self.config["served_models"] = []
        self.config["served_models"].append(served_config)
    
    def to_sdk_create_params(self) -> Dict[str, Any]:
        """Convert to SDK parameters for endpoint creation."""
        return {
            "name": self.resolved_name,
            "config": self.config,
            "route_optimized": self.route_optimized
        }


