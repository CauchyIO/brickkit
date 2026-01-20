"""
Brickkit Tools - Optional utilities for Databricks workspace management.

This package provides tools that complement the core brickkit library but are
kept separate to avoid bloating the main package. Import explicitly when needed.

Available modules:
    - importer: Pull existing workspace resources as brickkit models

Usage:
    from brickkit_tools.importer import WorkspaceImporter, WorkspaceSnapshot

    importer = WorkspaceImporter(client)
    snapshot = importer.pull_all()
"""

__version__ = "0.1.0"
