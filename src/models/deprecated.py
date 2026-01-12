"""
Deprecated models for backward compatibility.

These will be removed in v2.0.
"""

import warnings
from typing import List, Optional, Any, Dict
from .references import TableReference, ModelReference
from .enums import TableType


class Table(TableReference):
    """
    DEPRECATED: Use TableReference or manage tables via DABs.
    This class exists only for backward compatibility.
    
    Migration path:
    1. For governance: Use TableReference
    2. For table creation: Use Databricks Asset Bundles (DABs)
    """
    
    # Old detailed properties (deprecated)
    columns: Optional[List[Any]] = None
    constraints: Optional[List[Any]] = None
    table_type: Optional[TableType] = None
    data_source_format: Optional[str] = None
    storage_location: Optional[str] = None
    properties: Optional[Dict[str, str]] = None
    
    def __init__(self, **data):
        warnings.warn(
            "Table class is deprecated. Use TableReference for governance "
            "or Databricks Asset Bundles (DABs) for table management.\n"
            "See migration guide: https://github.com/dbrcdk/migration-guide",
            DeprecationWarning,
            stacklevel=2
        )
        
        # Extract only what TableReference needs
        ref_data = {
            'name': data.get('name'),
            'catalog_name': data.get('catalog_name'),
            'schema_name': data.get('schema_name'),
            'tags': data.get('tags', {}),
            'owner': data.get('owner'),
            'data_classification': data.get('data_classification')
        }
        
        # Remove None values
        ref_data = {k: v for k, v in ref_data.items() if v is not None}
        
        super().__init__(**ref_data)
        
        # Store deprecated fields for compatibility
        self.columns = data.get('columns')
        self.constraints = data.get('constraints')
        self.table_type = data.get('table_type')
        self.data_source_format = data.get('data_source_format')
        self.storage_location = data.get('storage_location')
        self.properties = data.get('properties')
    
    def to_dabs_yaml(self) -> str:
        """
        Convert to DABs bundle format for migration.
        
        Returns:
            YAML string for DABs bundle configuration
        """
        return f"""# Migrate this table definition to DABs
resources:
  schemas:
    {self.schema_name}:
      name: {self.schema_name}
      catalog_name: {self.catalog_name}_${{var.environment}}
      comment: "Schema for {self.schema_name}"
      
  tables:
    {self.name}:
      name: {self.name}
      catalog_name: {self.catalog_name}_${{var.environment}}
      schema_name: {self.schema_name}
      table_type: {self.table_type.value if self.table_type else 'MANAGED'}
      comment: "Migrated from DBRCDK"
      {f'storage_location: {self.storage_location}' if self.storage_location else ''}
      {f'data_source_format: {self.data_source_format}' if self.data_source_format else ''}
"""
    
    def to_table_reference(self) -> TableReference:
        """
        Convert to the new TableReference model.
        
        Returns:
            TableReference object for governance
        """
        return TableReference(
            name=self.name,
            catalog_name=self.catalog_name,
            schema_name=self.schema_name,
            tags=self.tags,
            owner=self.owner,
            data_classification=self.data_classification
        )


class RegisteredModel(ModelReference):
    """
    DEPRECATED: Use ModelReference or manage models via MLflow.
    This class exists only for backward compatibility.
    
    Migration path:
    1. For governance: Use ModelReference
    2. For model management: Use MLflow v3 directly
    """
    
    # Old detailed properties (deprecated)
    description: Optional[str] = None
    versions: Optional[List[Any]] = None
    latest_version: Optional[int] = None
    
    def __init__(self, **data):
        warnings.warn(
            "RegisteredModel class is deprecated. Use ModelReference for governance "
            "or MLflow for model management.\n"
            "See migration guide: https://github.com/dbrcdk/migration-guide",
            DeprecationWarning,
            stacklevel=2
        )
        
        # Extract only what ModelReference needs
        ref_data = {
            'name': data.get('name'),
            'catalog_name': data.get('catalog_name'),
            'schema_name': data.get('schema_name', 'models'),
            'model_tier': data.get('model_tier'),
            'data_classification': data.get('data_classification'),
            'requires_approval': data.get('requires_approval', False),
            'approved_by': data.get('approved_by')
        }
        
        # Remove None values
        ref_data = {k: v for k, v in ref_data.items() if v is not None}
        
        super().__init__(**ref_data)
        
        # Store deprecated fields for compatibility
        self.description = data.get('description')
        self.versions = data.get('versions')
        self.latest_version = data.get('latest_version')
    
    def to_mlflow_code(self) -> str:
        """
        Generate MLflow code for migration.
        
        Returns:
            Python code for MLflow model management
        """
        return f"""# Migrate this model to MLflow
import mlflow
from mlflow.tracking import MlflowClient

client = MlflowClient()

# Register model in Unity Catalog
model_name = "{self.catalog_name}_{{}}.{self.schema_name}.{self.name}"  # {{}} will be environment

# Create or update model
try:
    client.create_registered_model(
        name=model_name,
        tags={{
            "tier": "{self.model_tier or 'EXPERIMENTAL'}",
            "classification": "{self.data_classification or 'INTERNAL'}",
            "managed_by": "mlflow"
        }},
        description="{self.description or 'Migrated from DBRCDK'}"
    )
except Exception:
    # Model already exists
    pass

# For governance, use ModelReference:
from dbrcdk.models.references import ModelReference

model_ref = ModelReference(
    name="{self.name}",
    catalog_name="{self.catalog_name}",
    schema_name="{self.schema_name}",
    model_tier="{self.model_tier or 'EXPERIMENTAL'}"
)
"""
    
    def to_model_reference(self) -> ModelReference:
        """
        Convert to the new ModelReference model.
        
        Returns:
            ModelReference object for governance
        """
        return ModelReference(
            name=self.name,
            catalog_name=self.catalog_name,
            schema_name=self.schema_name,
            model_tier=self.model_tier,
            data_classification=self.data_classification,
            requires_approval=self.requires_approval,
            approved_by=self.approved_by
        )


class ModelVersion:
    """
    DEPRECATED: Model versions are managed entirely by MLflow.
    This class is removed - use MLflow client directly.
    """
    
    def __init__(self, **data):
        raise NotImplementedError(
            "ModelVersion is no longer supported. Use MLflow client directly:\n"
            "from mlflow.tracking import MlflowClient\n"
            "client = MlflowClient()\n"
            "version = client.create_model_version(...)"
        )


class Column:
    """
    DEPRECATED: Column definitions belong in DABs table definitions.
    This class is removed - define columns in your DABs bundle.
    """
    
    def __init__(self, **data):
        raise NotImplementedError(
            "Column is no longer supported. Define columns in your DABs bundle:\n"
            "resources:\n"
            "  tables:\n"
            "    my_table:\n"
            "      columns:\n"
            "        - name: id\n"
            "          type: INT\n"
            "          nullable: false"
        )


class Constraint:
    """
    DEPRECATED: Constraints belong in DABs table definitions.
    This class is removed - define constraints in your DABs bundle.
    """
    
    def __init__(self, **data):
        raise NotImplementedError(
            "Constraint is no longer supported. Define constraints in your DABs bundle:\n"
            "resources:\n"
            "  tables:\n"
            "    my_table:\n"
            "      primary_keys: [id]\n"
            "      foreign_keys:\n"
            "        - columns: [customer_id]\n"
            "          parent_table: customers\n"
            "          parent_columns: [id]"
        )