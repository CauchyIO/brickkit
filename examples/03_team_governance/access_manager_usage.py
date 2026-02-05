"""
Access Manager Example

AccessManager provides team-level orchestration for grants:
- Centralized grant tracking for audit
- Bulk operations for common patterns
- Team-specific access organization
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from models.securables import Catalog, Schema
from models.access import Principal, AccessPolicy, AccessManager
from models.enums import IsolationMode

# Create catalog with schemas
catalog = Catalog(name="sales", isolation_mode=IsolationMode.OPEN)
bronze = Schema(name="bronze")
silver = Schema(name="silver")
gold = Schema(name="gold")

catalog.add_schema(bronze)
catalog.add_schema(silver)
catalog.add_schema(gold)

# Create AccessManager for the team
manager = AccessManager(team_name="sales_team")

# Define principals
data_engineers = Principal(name="data_engineers")
analysts = Principal(name="analysts")
executives = Principal(name="executives")

# Grant access through the manager (tracks for audit)

# Data engineers: full write access
manager.grant(data_engineers, catalog, AccessPolicy.WRITER())

# Analysts: read access to silver and gold only
manager.grant(analysts, silver, AccessPolicy.READER())
manager.grant(analysts, gold, AccessPolicy.READER())

# Executives: read access to gold only
manager.grant(executives, gold, AccessPolicy.READER())

# Review grants
print(f"=== Grants by {manager.team_name} ===\n")
for grant in manager.grants:
    print(f"Principal: {grant['principal']}")
    print(f"  Securable: {grant['securable_type']} '{grant['securable_name']}'")
    print(f"  Policy: {grant['policy']}")
    print()

# Query grants by principal
print("=== Grants for 'analysts' ===")
analyst_grants = manager.get_grants_for_principal("analysts_dev")
for g in analyst_grants:
    print(f"  {g['securable_type']} '{g['securable_name']}': {g['policy']}")

# Query grants by securable
print("\n=== Grants on 'gold' schema ===")
gold_grants = manager.get_grants_for_securable("gold")
for g in gold_grants:
    print(f"  {g['principal']}: {g['policy']}")

# Bulk grant to all schemas
print("\n=== Bulk grant to all schemas ===")
auditors = Principal(name="auditors")
manager.grant_to_all_schemas(auditors, catalog, AccessPolicy.BROWSE_ONLY())
print(f"Granted BROWSE_ONLY to auditors on catalog and all schemas")
print(f"Total grants recorded: {len(manager.grants)}")

# Output:
# === Grants by sales_team ===
#
# Principal: data_engineers_dev
#   Securable: CATALOG 'sales'
#   Policy: WRITER
#
# Principal: analysts_dev
#   Securable: SCHEMA 'silver'
#   Policy: READER
# ...
