"""
Simple Grants Example

Shows how to grant access using Principal and AccessPolicy.
Demonstrates the three standard policies: READER, WRITER, ADMIN.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from models.securables import Catalog, Schema
from models.access import Principal, AccessPolicy
from models.enums import IsolationMode

# Create catalog and schema
catalog = Catalog(
    name="analytics",
    isolation_mode=IsolationMode.OPEN,
)
schema = Schema(name="reports")
catalog.add_schema(schema)

# Define principals (groups/users)
# Principals get environment suffix by default: analysts_dev, analysts_acc, etc.
analysts = Principal(name="analysts")
data_engineers = Principal(name="data_engineers")

# Special principals (no suffix)
all_users = Principal.all_workspace_users()  # Built-in 'users' group

print(f"analysts resolved: {analysts.resolved_name}")
print(f"data_engineers resolved: {data_engineers.resolved_name}")
print(f"all_users resolved: {all_users.resolved_name}")

# Grant access using predefined policies
# READER: SELECT, BROWSE, USE_CATALOG, USE_SCHEMA
catalog.grant(analysts, AccessPolicy.READER())

# WRITER: READER + CREATE_TABLE, MODIFY, CREATE_SCHEMA
catalog.grant(data_engineers, AccessPolicy.WRITER())

# Collect privileges for review
print(f"\nPrivileges on catalog '{catalog.resolved_name}':")
for priv in catalog.privileges:
    print(f"  {priv.principal}: {priv.privilege.value}")

# Output (when DATABRICKS_ENV=dev):
# analysts resolved: analysts_dev
# data_engineers resolved: data_engineers_dev
# all_users resolved: users
#
# Privileges on catalog 'analytics_dev':
#   analysts_dev: USE_CATALOG
#   analysts_dev: BROWSE
#   data_engineers_dev: USE_CATALOG
#   data_engineers_dev: CREATE_SCHEMA
