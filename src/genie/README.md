# Genie Space Management

This module provides documentation for defining and deploying Databricks Genie Spaces as code.

## Overview

Genie Spaces are now managed through the unified executor system:

- `models/genie.py` - Pydantic models for type-safe Genie Space configuration
- `executors/genie_executor.py` - Executor for deploying spaces to Databricks

## Quick Start

```python
from databricks.sdk import WorkspaceClient
from executors import GenieSpaceExecutor
from models.genie import GenieSpace, SerializedSpace, DataSources, TableDataSource

client = WorkspaceClient(profile='my-profile')
executor = GenieSpaceExecutor(client)

# Create a space
space = GenieSpace(
    name="analytics",
    title="Analytics Space",
    serialized_space=SerializedSpace(
        data_sources=DataSources(
            tables=[TableDataSource(identifier="catalog.schema.table")]
        )
    )
)

# Deploy (creates or updates)
result = executor.create_or_update(space)

# Dry run
executor_dry = GenieSpaceExecutor(client, dry_run=True)
result = executor_dry.create_or_update(space)

# Deploy multiple spaces
results = executor.deploy_all([space1, space2])

# Export to JSON
executor.export_to_json([space], Path("./exports"))
```

## Defining a Genie Space

```python
from models.genie import (
    GenieSpace,
    SerializedSpace,
    DataSources,
    TableDataSource,
    ColumnConfig,
    Instructions,
    TextInstruction,
    SqlFunction,
)

MY_SPACE = GenieSpace(
    name="my_analytics",  # Internal name (used for environment suffixing)
    title="My Analytics Space",  # Display title
    description="Description shown to users",
    serialized_space=SerializedSpace(
        data_sources=DataSources(
            tables=[
                TableDataSource(
                    identifier="catalog.schema.table",
                    column_configs=[
                        # Sort alphabetically by column_name
                        ColumnConfig(column_name="col1", get_example_values=True),
                        ColumnConfig(column_name="col2", build_value_dictionary=True),
                    ],
                ),
            ]
        ),
        instructions=Instructions(
            text_instructions=[
                TextInstruction(content="Instructions for the AI assistant..."),
            ],
            sql_functions=[
                # Sort alphabetically by identifier
                SqlFunction(identifier="catalog.schema.my_function"),
            ],
        ),
    ),
)
```

## Genie API Requirements

The Databricks Genie API has specific (undocumented) requirements:

### sql_functions
- **MUST include an `id` field** - 32-character hex string
- **MUST be sorted by `(id, identifier)` tuple**
- Without proper IDs and sorting, the API returns "Internal Error"

The models handle this automatically:
- `SqlFunction` generates deterministic IDs using MD5 hash of the identifier
- `Instructions.to_dict()` sorts functions by `(id, identifier)`

### text_instructions
- Should **NOT** include an `id` field - the API generates it
- Content can be a string or list of strings

### column_configs
- Should be sorted alphabetically by `column_name`
- The API may reject unsorted columns

### Fetching Existing Space Config

To see how an existing space is configured:

```python
from databricks.sdk import WorkspaceClient

client = WorkspaceClient(profile='my-profile')
space = client.genie.get_space(
    space_id='your-space-id',
    include_serialized_space=True  # Required to get the config!
)
print(space.serialized_space)
```

## Executor Features

The `GenieSpaceExecutor` provides:

- **Batch deployment**: `deploy_all([space1, space2])`
- **Dry-run mode**: Preview what would be deployed
- **JSON export**: Export spaces for version control
- **Permission management**: Grant service principal access
- **Automatic warehouse resolution**: Uses first available if not specified

```python
from executors import GenieSpaceExecutor
from executors.genie_executor import ServicePrincipal, GenieSpacePermission

executor = GenieSpaceExecutor(client)

# Deploy and grant access
result = executor.create_or_update(space)
if result.success:
    spn = ServicePrincipal(application_id="xxx", name="my_spn")
    executor.grant_access(space.space_id, spn, GenieSpacePermission.CAN_EDIT)
```

## Troubleshooting

### "Internal Error" on deploy
- Check that sql_functions have proper IDs (32-char hex)
- Ensure sql_functions are sorted by (id, identifier)
- Try deploying with a single function first to isolate the issue

### "sql_functions must be sorted by (id, identifier)"
- The models should handle this automatically
- If you see this error, check that you're using the latest models

### Space updates fail but creates work
- Existing space may be in a corrupted state
- Try creating a new space with a different title
- Delete the old space via UI if needed
