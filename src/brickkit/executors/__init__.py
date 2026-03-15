"""
Executor modules for applying Unity Catalog configurations via SDK.
"""

from brickkit.executors.acl_executor import AclExecutor
from brickkit.executors.base import BaseExecutor, ExecutionPlan, ExecutionResult
from brickkit.executors.catalog_executor import CatalogExecutor
from brickkit.executors.connection_executor import ConnectionExecutor
from brickkit.executors.experiment_executor import MlflowExperimentExecutor
from brickkit.executors.external_location_executor import ExternalLocationExecutor
from brickkit.executors.function_executor import FunctionExecutor
from brickkit.executors.genie_executor import GenieSpaceExecutor, GenieSpacePermission, ServicePrincipal
from brickkit.executors.grant_executor import GrantExecutor, PrincipalNotFoundError, SecurableNotFoundError

# Principal management executors
from brickkit.executors.group_executor import GroupExecutor
from brickkit.executors.metastore_assignment_executor import MetastoreAssignmentExecutor
from brickkit.executors.model_serving_executor import ModelServingEndpointExecutor
from brickkit.executors.schema_executor import SchemaExecutor
from brickkit.executors.service_principal_executor import (
    ServicePrincipalCredentials,
    ServicePrincipalExecutor,
    get_privileged_client,
)
from brickkit.executors.storage_credential_executor import StorageCredentialExecutor
from brickkit.executors.table_executor import TableExecutor
from brickkit.executors.vector_search_executor import (
    EndpointStatus,
    VectorSearchEndpointExecutor,
    VectorSearchIndexExecutor,
)
from brickkit.executors.volume_executor import VolumeExecutor
from brickkit.executors.workspace_binding_executor import WorkspaceBindingExecutor

__all__ = [
    # Base classes
    "BaseExecutor",
    "ExecutionResult",
    "ExecutionPlan",
    # Hierarchy executors (Level 1-3)
    "CatalogExecutor",
    "SchemaExecutor",
    "TableExecutor",
    "VolumeExecutor",
    "FunctionExecutor",
    # Infrastructure executors
    "StorageCredentialExecutor",
    "ExternalLocationExecutor",
    "ConnectionExecutor",
    # Permission executor
    "GrantExecutor",
    "PrincipalNotFoundError",
    "SecurableNotFoundError",
    # Metastore executor
    "MetastoreAssignmentExecutor",
    # Workspace binding executor
    "WorkspaceBindingExecutor",
    # AI/ML executors
    "ModelServingEndpointExecutor",
    "MlflowExperimentExecutor",
    "GenieSpaceExecutor",
    "ServicePrincipal",
    "GenieSpacePermission",
    "VectorSearchEndpointExecutor",
    "VectorSearchIndexExecutor",
    "EndpointStatus",
    # Principal management executors
    "GroupExecutor",
    "ServicePrincipalExecutor",
    "ServicePrincipalCredentials",
    "get_privileged_client",
    "AclExecutor",
]
