"""
Workspace Importer - Pull Databricks workspace resources as brickkit models.

This module provides tools to import existing Databricks workspace resources
and convert them to brickkit models for governance, validation, and drift detection.

Usage:
    from databricks.sdk import WorkspaceClient
    from brickkit_tools.importer import WorkspaceImporter, WorkspaceSnapshot

    client = WorkspaceClient()
    importer = WorkspaceImporter(client)

    # Pull everything
    snapshot = importer.pull_all()
    print(snapshot.summary())

    # Pull specific resource types
    snapshot = importer.pull(include=["catalogs", "jobs", "service_principals"])

    # Pull single catalog with full hierarchy
    catalog = importer.pull_catalog("my_catalog", depth="full")

    # Apply conventions
    from brickkit import Convention, Environment
    snapshot.apply_convention(my_convention, Environment.DEV)

    # Validate
    errors = snapshot.validate(my_convention)

Key Classes:
    - WorkspaceImporter: Main entry point for importing resources
    - WorkspaceSnapshot: Container for all imported resources
    - ImportResult: Result of importing a single resource type
    - ImportOptions: Configuration for import behavior

Resource Importers:
    - CatalogImporter: Unity Catalog (catalogs, schemas, tables, volumes, functions)
    - StorageCredentialImporter: Storage credentials
    - ExternalLocationImporter: External locations
    - ConnectionImporter: Lakehouse Federation connections
    - UserImporter: Workspace users
    - GroupImporter: Workspace groups
    - ServicePrincipalImporter: Service principals
    - JobImporter: Databricks jobs
    - PipelineImporter: DLT pipelines

Example - Governance Audit:
    from brickkit_tools.importer import WorkspaceImporter
    from brickkit import Convention, RequiredTag

    # Define what you expect
    convention = Convention(
        name="audit",
        required_tags=[
            RequiredTag(key="cost_center", applies_to={"CATALOG", "SCHEMA"}),
            RequiredTag(key="team", applies_to={"JOB"}),
        ],
    )

    # Pull and validate
    importer = WorkspaceImporter(client)
    snapshot = importer.pull_all()

    errors = snapshot.validate(convention)
    for error in errors:
        print(f"VIOLATION: {error}")

Example - Service Principal Naming:
    from brickkit_tools.importer import WorkspaceImporter
    import re

    importer = WorkspaceImporter(client)
    snapshot = importer.pull(include=["service_principals"])

    # Check naming convention: sp-{team}-{purpose}
    pattern = r"^sp-[a-z]+-[a-z0-9-]+$"
    for sp in snapshot.service_principals:
        if not re.match(pattern, sp.display_name):
            print(f"BAD NAME: {sp.display_name}")
"""

# Base classes
from .base import (
    CompositeImporter,
    ImportOptions,
    ImportResult,
    ResourceImporter,
)

# Main importer
from .workspace import (
    WorkspaceImporter,
    WorkspaceSnapshot,
)

# Unity Catalog importers
from .catalog_importer import (
    CatalogImporter,
    ConnectionImporter,
    ExternalLocationImporter,
    StorageCredentialImporter,
)

# AI/ML importers
from .genie_importer import GenieSpaceImporter

# Identity importers
from .identity_importer import (
    Group,
    GroupImporter,
    ServicePrincipal,
    ServicePrincipalImporter,
    User,
    UserImporter,
)

# Workflow importers
from .job_importer import (
    Job,
    JobImporter,
    Pipeline,
    PipelineImporter,
)

__all__ = [
    # Main classes
    "WorkspaceImporter",
    "WorkspaceSnapshot",
    # Base classes
    "ResourceImporter",
    "CompositeImporter",
    "ImportResult",
    "ImportOptions",
    # Unity Catalog
    "CatalogImporter",
    "StorageCredentialImporter",
    "ExternalLocationImporter",
    "ConnectionImporter",
    # AI/ML
    "GenieSpaceImporter",
    # Identity
    "UserImporter",
    "GroupImporter",
    "ServicePrincipalImporter",
    "User",
    "Group",
    "ServicePrincipal",
    # Workflows
    "JobImporter",
    "PipelineImporter",
    "Job",
    "Pipeline",
]
