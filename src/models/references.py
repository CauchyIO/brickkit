"""
Lightweight references to Unity Catalog objects for governance.

These are NOT for creating objects, only for managing permissions.
Objects are assumed to be created by other tools (DABs, MLflow, direct SDK).
"""

from typing import Dict, Optional
from pydantic import Field
from .base import BaseSecurable, get_current_environment
from .enums import SecurableType


class TableReference(BaseSecurable):
    """
    Reference to a table managed by DABs or created directly.
    Used only for permission management and governance.
    
    Example:
        # Reference an existing table for governance
        table_ref = TableReference(
            name="customer_metrics",
            catalog_name="analytics",
            schema_name="bronze",
            tags={"pii": "true", "retention": "7years"}
        )
        
        # Grant permissions
        table_ref.grant(data_engineers, AccessPolicy.WRITER())
    """
    name: str = Field(..., description="Table name")
    catalog_name: str = Field(..., description="Catalog containing the table")
    schema_name: str = Field(..., description="Schema containing the table")
    
    # Governance metadata (optional)
    tags: Dict[str, str] = Field(default_factory=dict, description="Governance tags")
    owner: Optional[str] = Field(None, description="Table owner for governance")
    data_classification: Optional[str] = Field(None, description="PII/CONFIDENTIAL/PUBLIC")
    
    @property
    def full_name(self) -> str:
        """Full qualified name for Unity Catalog."""
        env = get_current_environment()
        catalog = f"{self.catalog_name}_{env.value.lower()}"
        return f"{catalog}.{self.schema_name}.{self.name}"
    
    @property
    def securable_type(self) -> SecurableType:
        return SecurableType.TABLE
    
    def get_level_1_name(self) -> str:
        """Get catalog name with environment suffix."""
        env = get_current_environment()
        return f"{self.catalog_name}_{env.value.lower()}"
    
    def get_level_2_name(self) -> Optional[str]:
        """Get schema name."""
        return self.schema_name
    
    def get_level_3_name(self) -> Optional[str]:
        """Get table name."""
        return self.name


class ModelReference(BaseSecurable):
    """
    Reference to an MLflow model for governance.
    The model itself is managed by MLflow.
    
    Example:
        # Reference a model for governance
        model_ref = ModelReference(
            name="fraud_detector",
            catalog_name="ml",
            schema_name="models",
            model_tier="PRODUCTION",
            requires_approval=True
        )
        
        # Grant permissions
        model_ref.grant(ml_engineers, AccessPolicy.EXECUTE())
    """
    name: str = Field(..., description="Model name")
    catalog_name: str = Field(..., description="Catalog for models")
    schema_name: str = Field(default="models", description="Schema for models")
    
    # Governance metadata
    model_tier: Optional[str] = Field(None, description="EXPERIMENTAL/DEVELOPMENT/PRODUCTION")
    data_classification: Optional[str] = Field(None, description="Data sensitivity level")
    requires_approval: bool = Field(False, description="Needs approval for production")
    approved_by: Optional[str] = Field(None, description="Who approved for production")
    
    @property
    def full_name(self) -> str:
        """Full MLflow model name."""
        env = get_current_environment()
        catalog = f"{self.catalog_name}_{env.value.lower()}"
        return f"{catalog}.{self.schema_name}.{self.name}"
    
    @property
    def securable_type(self) -> SecurableType:
        return SecurableType.MODEL
    
    def get_level_1_name(self) -> str:
        """Get catalog name with environment suffix."""
        env = get_current_environment()
        return f"{self.catalog_name}_{env.value.lower()}"
    
    def get_level_2_name(self) -> Optional[str]:
        """Get schema name."""
        return self.schema_name
    
    def get_level_3_name(self) -> Optional[str]:
        """Get model name."""
        return self.name


class VolumeReference(BaseSecurable):
    """
    Reference to a volume for governance.
    Volumes can be created by various means.
    
    Example:
        # Reference a volume for governance
        volume_ref = VolumeReference(
            name="raw_data",
            catalog_name="analytics",
            schema_name="bronze",
            volume_type="EXTERNAL"
        )
        
        # Grant permissions
        volume_ref.grant(data_readers, AccessPolicy.READ_VOLUME())
    """
    name: str = Field(..., description="Volume name")
    catalog_name: str = Field(..., description="Catalog containing the volume")
    schema_name: str = Field(..., description="Schema containing the volume")
    volume_type: str = Field("MANAGED", description="MANAGED or EXTERNAL")
    
    @property
    def full_name(self) -> str:
        env = get_current_environment()
        catalog = f"{self.catalog_name}_{env.value.lower()}"
        return f"{catalog}.{self.schema_name}.{self.name}"
    
    @property
    def securable_type(self) -> SecurableType:
        return SecurableType.VOLUME
    
    def get_level_1_name(self) -> str:
        """Get catalog name with environment suffix."""
        env = get_current_environment()
        return f"{self.catalog_name}_{env.value.lower()}"
    
    def get_level_2_name(self) -> Optional[str]:
        """Get schema name."""
        return self.schema_name
    
    def get_level_3_name(self) -> Optional[str]:
        """Get volume name."""
        return self.name


class FunctionReference(BaseSecurable):
    """
    Reference to a UDF for governance.
    
    Example:
        # Reference a function for governance
        func_ref = FunctionReference(
            name="calculate_risk_score",
            catalog_name="analytics",
            schema_name="functions"
        )
        
        # Grant permissions
        func_ref.grant(analysts, AccessPolicy.EXECUTE())
    """
    name: str = Field(..., description="Function name")
    catalog_name: str = Field(..., description="Catalog containing the function")
    schema_name: str = Field(..., description="Schema containing the function")
    
    @property
    def full_name(self) -> str:
        env = get_current_environment()
        catalog = f"{self.catalog_name}_{env.value.lower()}"
        return f"{catalog}.{self.schema_name}.{self.name}"
    
    @property
    def securable_type(self) -> SecurableType:
        return SecurableType.FUNCTION
    
    def get_level_1_name(self) -> str:
        """Get catalog name with environment suffix."""
        env = get_current_environment()
        return f"{self.catalog_name}_{env.value.lower()}"
    
    def get_level_2_name(self) -> Optional[str]:
        """Get schema name."""
        return self.schema_name
    
    def get_level_3_name(self) -> Optional[str]:
        """Get function name."""
        return self.name