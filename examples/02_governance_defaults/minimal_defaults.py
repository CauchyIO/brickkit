"""
Minimal Governance Defaults

Lightweight governance for startups or small teams.
Only the essentials, no bureaucracy.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from typing import List
from brickkit.defaults import GovernanceDefaults, TagDefault, RequiredTag, EmptyDefaults
from models.securables import Catalog
from models.base import Tag
from models.enums import Environment


class MinimalDefaults(GovernanceDefaults):
    """
    Minimal governance for small teams.

    Just enough structure to:
    - Track who manages what
    - Know the environment
    """

    @property
    def default_tags(self) -> List[TagDefault]:
        return [
            TagDefault(key="managed_by", value="brickkit"),
        ]

    @property
    def required_tags(self) -> List[RequiredTag]:
        # No required tags - trust the team
        return []


# Compare: EmptyDefaults vs MinimalDefaults

print("=== EmptyDefaults (no governance) ===")
empty = EmptyDefaults()
catalog1 = Catalog(name="analytics")
catalog1 = empty.apply_to(catalog1, Environment.DEV)
print(f"Tags: {[(t.key, t.value) for t in catalog1.tags]}")

print("\n=== MinimalDefaults (just tracking) ===")
minimal = MinimalDefaults()
catalog2 = Catalog(name="analytics")
catalog2 = minimal.apply_to(catalog2, Environment.DEV)
print(f"Tags: {[(t.key, t.value) for t in catalog2.tags]}")

# Output:
# === EmptyDefaults (no governance) ===
# Tags: []
#
# === MinimalDefaults (just tracking) ===
# Tags: [('managed_by', 'brickkit')]
