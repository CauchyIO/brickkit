"""
Executor modules for applying Unity Catalog configurations via SDK.
"""

from .acl_executor import AclExecutor
from .base import BaseExecutor, ExecutionPlan, ExecutionResult
from .catalog_executor import CatalogExecutor
from .connection_executor import ConnectionExecutor
from .external_location_executor import ExternalLocationExecutor
from .function_executor import FunctionExecutor
from .genie_executor import GenieSpaceExecutor, GenieSpacePermission, ServicePrincipal
from .grant_executor import GrantExecutor, PrincipalNotFoundError, SecurableNotFoundError

# Principal management executors
from .group_executor import GroupExecutor
from .metastore_assignment_executor import MetastoreAssignmentExecutor
from .schema_executor import SchemaExecutor
from .service_principal_executor import ServicePrincipalCredentials, ServicePrincipalExecutor, get_privileged_client
from .storage_credential_executor import StorageCredentialExecutor
from .table_executor import TableExecutor
from .vector_search_executor import EndpointStatus, VectorSearchEndpointExecutor, VectorSearchIndexExecutor
from .volume_executor import VolumeExecutor
from .workspace_binding_executor import WorkspaceBindingExecutor

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
