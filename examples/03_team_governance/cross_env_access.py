"""
Cross-Environment Access Example

Demonstrates workspace binding patterns that control which environments
can access which data. Common patterns:
- STANDARD_HIERARCHY: DEV can read ACC/PRD, ACC can read PRD
- ISOLATED: Each environment can only access its own data
- PRODUCTION_ISOLATED: PRD isolated, DEV/ACC can share
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from models.access import WorkspaceBindingPattern
from models.enums import Environment, BindingType

# Show the three standard patterns

print("=== STANDARD_HIERARCHY ===")
print("DEV can read ACC and PRD data (for testing with prod data)")
print("ACC can read PRD data (for validation)")
print("PRD is isolated (no cross-environment access)\n")

standard = WorkspaceBindingPattern.STANDARD_HIERARCHY()
for source_env, targets in standard.access_matrix.items():
    print(f"{source_env.value} can access:")
    for target, binding_type in targets.items():
        access = "READ_WRITE" if binding_type == BindingType.BINDING_TYPE_READ_WRITE else "READ_ONLY"
        print(f"  -> {target.upper()}: {access}")
    print()


print("=== ISOLATED ===")
print("Each environment can only access its own data.")
print("Maximum security, no cross-environment exposure.\n")

isolated = WorkspaceBindingPattern.ISOLATED()
for source_env, targets in isolated.access_matrix.items():
    print(f"{source_env.value} can access:")
    for target, binding_type in targets.items():
        print(f"  -> {target.upper()}: READ_WRITE")
    print()


print("=== PRODUCTION_ISOLATED ===")
print("PRD is completely isolated.")
print("DEV and ACC can share data with each other.\n")

prod_isolated = WorkspaceBindingPattern.PRODUCTION_ISOLATED()
for source_env, targets in prod_isolated.access_matrix.items():
    print(f"{source_env.value} can access:")
    for target, binding_type in targets.items():
        access = "READ_WRITE" if binding_type == BindingType.BINDING_TYPE_READ_WRITE else "READ_ONLY"
        print(f"  -> {target.upper()}: {access}")
    print()


# Custom pattern example
print("=== Custom Pattern ===")
print("Create your own cross-environment access rules:\n")

custom_pattern = WorkspaceBindingPattern(
    name="STRICT_SEPARATION",
    access_matrix={
        Environment.DEV: {"dev": BindingType.BINDING_TYPE_READ_WRITE},
        Environment.ACC: {"acc": BindingType.BINDING_TYPE_READ_WRITE},
        Environment.PRD: {"prd": BindingType.BINDING_TYPE_READ_WRITE},
    }
)
print(f"Pattern name: {custom_pattern.name}")
print("Each environment completely isolated (same as ISOLATED pattern)")

# Output:
# === STANDARD_HIERARCHY ===
# DEV can read ACC and PRD data (for testing with prod data)
# ACC can read PRD data (for validation)
# PRD is isolated (no cross-environment access)
#
# DEV can access:
#   -> DEV: READ_WRITE
#   -> ACC: READ_ONLY
#   -> PRD: READ_ONLY
#
# ACC can access:
#   -> ACC: READ_WRITE
#   -> PRD: READ_ONLY
#
# PRD can access:
#   -> PRD: READ_WRITE
# ...
