"""
Project manifest for declarative governance configuration.

Teams can define their governance rules (tags, naming conventions, ownership)
in a JSON manifest file and load it at runtime.

Usage:
    from brickkit import load_project_manifest, Catalog

    # Load manifest from JSON file
    defaults = load_project_manifest("./project.manifest.json")

    # Use like any GovernanceDefaults
    catalog = Catalog(name="analytics").with_defaults(defaults)
    errors = catalog.validate_governance(defaults)

Example manifest (project.manifest.json):
    {
        "version": "1.0",
        "organization": "acme_corp",
        "default_owner": "data-governance-team",
        "default_tags": [
            {"key": "business_unit", "value": "engineering"}
        ],
        "required_tags": [
            {"key": "cost_center", "allowed_values": ["fin-001", "eng-002"]}
        ],
        "naming_conventions": [
            {"pattern": "^[a-z][a-z0-9_]*$", "applies_to": ["CATALOG"]}
        ]
    }
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Union

from pydantic import BaseModel, Field, field_validator, model_validator

from brickkit.defaults import (
    GovernanceDefaults,
    NamingConvention,
    RequiredTag,
    TagDefault,
)


class ManifestTagDefault(BaseModel):
    """Tag default definition in manifest format."""

    key: str = Field(..., min_length=1)
    value: str = Field(..., min_length=1)
    environment_values: Dict[str, str] = Field(default_factory=dict)
    applies_to: List[str] = Field(default_factory=list)

    @field_validator("key")
    @classmethod
    def validate_key(cls, v: str) -> str:
        if not re.match(r"^[a-zA-Z][a-zA-Z0-9_]*$", v):
            raise ValueError(
                f"Tag key '{v}' must start with a letter and contain only "
                "alphanumeric characters and underscores"
            )
        return v

    @field_validator("environment_values")
    @classmethod
    def validate_environment_values(cls, v: Dict[str, str]) -> Dict[str, str]:
        valid_envs = {"dev", "acc", "prd"}
        for env in v.keys():
            if env not in valid_envs:
                raise ValueError(
                    f"Invalid environment '{env}'. Must be one of: {valid_envs}"
                )
        return v

    def to_tag_default(self) -> TagDefault:
        """Convert to internal TagDefault model."""
        return TagDefault(
            key=self.key,
            value=self.value,
            environment_values=self.environment_values,
            applies_to=set(self.applies_to),
        )


class ManifestRequiredTag(BaseModel):
    """Required tag definition in manifest format."""

    key: str = Field(..., min_length=1)
    allowed_values: Optional[List[str]] = None
    applies_to: List[str] = Field(default_factory=list)
    error_message: Optional[str] = None

    @field_validator("key")
    @classmethod
    def validate_key(cls, v: str) -> str:
        if not re.match(r"^[a-zA-Z][a-zA-Z0-9_]*$", v):
            raise ValueError(
                f"Tag key '{v}' must start with a letter and contain only "
                "alphanumeric characters and underscores"
            )
        return v

    def to_required_tag(self) -> RequiredTag:
        """Convert to internal RequiredTag model."""
        return RequiredTag(
            key=self.key,
            allowed_values=set(self.allowed_values) if self.allowed_values else None,
            applies_to=set(self.applies_to),
            error_message=self.error_message,
        )


class ManifestNamingConvention(BaseModel):
    """Naming convention definition in manifest format."""

    pattern: str = Field(..., min_length=1)
    applies_to: List[str] = Field(default_factory=list)
    error_message: str = "Name does not match required pattern"

    @field_validator("pattern")
    @classmethod
    def validate_pattern(cls, v: str) -> str:
        try:
            re.compile(v)
        except re.error as e:
            raise ValueError(f"Invalid regex pattern '{v}': {e}") from e
        return v

    def to_naming_convention(self) -> NamingConvention:
        """Convert to internal NamingConvention model."""
        return NamingConvention(
            pattern=self.pattern,
            applies_to=set(self.applies_to),
            error_message=self.error_message,
        )


class ProjectManifest(BaseModel):
    """
    Project manifest for governance configuration.

    Defines the governance rules for a project including default tags,
    required tags, naming conventions, and default ownership.

    Attributes:
        version: Manifest schema version (currently "1.0")
        organization: Organization identifier
        default_owner: Default owner principal for securables
        default_tags: Tags automatically applied to securables
        required_tags: Tags that must be present on securables
        naming_conventions: Regex patterns for naming validation
    """

    version: str = Field(default="1.0", pattern=r"^\d+\.\d+$")
    organization: Optional[str] = None
    default_owner: Optional[str] = None
    default_tags: List[ManifestTagDefault] = Field(default_factory=list)
    required_tags: List[ManifestRequiredTag] = Field(default_factory=list)
    naming_conventions: List[ManifestNamingConvention] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_no_duplicate_tag_keys(self) -> "ProjectManifest":
        """Ensure no duplicate keys in default_tags or required_tags."""
        default_keys = [t.key for t in self.default_tags]
        if len(default_keys) != len(set(default_keys)):
            duplicates = [k for k in default_keys if default_keys.count(k) > 1]
            raise ValueError(f"Duplicate keys in default_tags: {set(duplicates)}")

        required_keys = [t.key for t in self.required_tags]
        if len(required_keys) != len(set(required_keys)):
            duplicates = [k for k in required_keys if required_keys.count(k) > 1]
            raise ValueError(f"Duplicate keys in required_tags: {set(duplicates)}")

        return self


class ManifestBasedDefaults(GovernanceDefaults):
    """
    GovernanceDefaults implementation backed by a ProjectManifest.

    This class wraps a ProjectManifest and exposes it through the
    GovernanceDefaults interface, allowing manifest-based configuration
    to be used anywhere GovernanceDefaults is expected.
    """

    def __init__(self, manifest: ProjectManifest) -> None:
        self._manifest = manifest
        self._default_tags = [t.to_tag_default() for t in manifest.default_tags]
        self._required_tags = [t.to_required_tag() for t in manifest.required_tags]
        self._naming_conventions = [
            n.to_naming_convention() for n in manifest.naming_conventions
        ]

    @property
    def manifest(self) -> ProjectManifest:
        """Access the underlying manifest."""
        return self._manifest

    @property
    def organization(self) -> Optional[str]:
        """Organization identifier from manifest."""
        return self._manifest.organization

    @property
    def default_tags(self) -> List[TagDefault]:
        """Default tags from manifest."""
        return self._default_tags

    @property
    def required_tags(self) -> List[RequiredTag]:
        """Required tags from manifest."""
        return self._required_tags

    @property
    def naming_conventions(self) -> List[NamingConvention]:
        """Naming conventions from manifest."""
        return self._naming_conventions

    @property
    def default_owner(self) -> Optional[str]:
        """Default owner from manifest."""
        return self._manifest.default_owner


def load_project_manifest(path: Union[str, Path]) -> ManifestBasedDefaults:
    """
    Load a project manifest from a JSON file.

    Fails fast on any validation error - invalid JSON structure,
    missing required fields, invalid regex patterns, etc.

    Args:
        path: Path to the manifest JSON file

    Returns:
        ManifestBasedDefaults instance ready for use

    Raises:
        FileNotFoundError: If the manifest file doesn't exist
        json.JSONDecodeError: If the file contains invalid JSON
        pydantic.ValidationError: If the manifest structure is invalid
    """
    path = Path(path)

    if not path.exists():
        raise FileNotFoundError(f"Manifest file not found: {path}")

    if not path.is_file():
        raise ValueError(f"Manifest path is not a file: {path}")

    content = path.read_text(encoding="utf-8")
    data = json.loads(content)
    manifest = ProjectManifest.model_validate(data)

    return ManifestBasedDefaults(manifest)


__all__ = [
    "ProjectManifest",
    "ManifestBasedDefaults",
    "ManifestTagDefault",
    "ManifestRequiredTag",
    "ManifestNamingConvention",
    "load_project_manifest",
]
