# Databricks notebook source
"""
Cross-Environment Access Example

Demonstrates workspace binding patterns that control which environments
can access which data. Common patterns:
- STANDARD_HIERARCHY: DEV can read ACC/PRD, ACC can read PRD
- ISOLATED: Each environment can only access its own data
- PRODUCTION_ISOLATED: PRD isolated, DEV/ACC can share
"""

from loguru import logger

from brickkit.models.base import init_environment
from brickkit.models.enums import BindingType, Environment
from brickkit.models.workspace_bindings import WorkspaceBindingPattern

# COMMAND ----------

env = init_environment()

# Show the three standard patterns

logger.info("=== STANDARD_HIERARCHY ===")
logger.info("DEV can read ACC and PRD data (for testing with prod data)")
logger.info("ACC can read PRD data (for validation)")
logger.info("PRD is isolated (no cross-environment access)\n")

standard = WorkspaceBindingPattern.STANDARD_HIERARCHY()
for source_env, targets in standard.access_matrix.items():
    logger.info(f"{source_env.value} can access:")
    for target, binding_type in targets.items():
        access = "READ_WRITE" if binding_type == BindingType.BINDING_TYPE_READ_WRITE else "READ_ONLY"
        logger.info(f"  -> {target.upper()}: {access}")
    logger.info()


logger.info("=== ISOLATED ===")
logger.info("Each environment can only access its own data.")
logger.info("Maximum security, no cross-environment exposure.\n")

isolated = WorkspaceBindingPattern.ISOLATED()
for source_env, targets in isolated.access_matrix.items():
    logger.info(f"{source_env.value} can access:")
    for target, binding_type in targets.items():
        logger.info(f"  -> {target.upper()}: READ_WRITE")
    logger.info()


logger.info("=== PRODUCTION_ISOLATED ===")
logger.info("PRD is completely isolated.")
logger.info("DEV and ACC can share data with each other.\n")

prod_isolated = WorkspaceBindingPattern.PRODUCTION_ISOLATED()
for source_env, targets in prod_isolated.access_matrix.items():
    logger.info(f"{source_env.value} can access:")
    for target, binding_type in targets.items():
        access = "READ_WRITE" if binding_type == BindingType.BINDING_TYPE_READ_WRITE else "READ_ONLY"
        logger.info(f"  -> {target.upper()}: {access}")
    logger.info()


# COMMAND ----------

# Custom pattern example
logger.info("=== Custom Pattern ===")
logger.info("Create your own cross-environment access rules:\n")

custom_pattern = WorkspaceBindingPattern(
    name="STRICT_SEPARATION",
    access_matrix={
        Environment.DEV: {"dev": BindingType.BINDING_TYPE_READ_WRITE},
        Environment.ACC: {"acc": BindingType.BINDING_TYPE_READ_WRITE},
        Environment.PRD: {"prd": BindingType.BINDING_TYPE_READ_WRITE},
    },
)
logger.info(f"Pattern name: {custom_pattern.name}")
logger.info("Each environment completely isolated (same as ISOLATED pattern)")

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
