# Local Notebook Development with Databricks-Connect

This guide explains how to develop Databricks notebooks locally while executing Spark operations on remote serverless compute.

## How It Works

| Code Type | Runs Where |
|-----------|------------|
| Python logic, SDK calls | **Local** machine |
| Spark operations (`df.select()`, `df.write()`) | **Remote** serverless compute |
| `spark.sql()` queries | **Remote** serverless compute |
| Results from `df.collect()`, `df.toPandas()` | Returned to local |

## Prerequisites

### 1. Install Dependencies

Add to `pyproject.toml`:

```toml
dependencies = [
    "databricks-sdk[notebook]>=0.20.0",
    "databricks-connect==17.3.*",
]
```

Sync dependencies:

```bash
uv sync
```

### 2. Configure Databricks Authentication

Set up CLI authentication (OAuth-based, recommended over PAT tokens):

```bash
databricks auth login --host https://your-workspace.cloud.databricks.com
```

Configure `~/.databrickscfg` to use `databricks-cli` auth:

```ini
[DEFAULT]
host      = https://your-workspace.cloud.databricks.com
auth_type = databricks-cli
```

**Important**: Avoid hardcoded PAT tokens - they expire and cause cryptic auth errors.

## Notebook Code Patterns

### Detect Environment

```python
import os

IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ
```

### Create SparkSession

```python
if IS_DATABRICKS:
    # spark variable is pre-defined on Databricks
    pass
else:
    # Local: use databricks-connect with serverless
    from databricks.connect import DatabricksSession
    spark = DatabricksSession.builder.serverless(True).getOrCreate()
```

### WorkspaceClient (SDK Calls)

```python
from databricks.sdk import WorkspaceClient

# Uses default profile from ~/.databrickscfg
ws_client = WorkspaceClient()
```

### Handle dbutils

`dbutils` is not available locally. Use conditional logic:

```python
if IS_DATABRICKS:
    hostname = dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()
    secret_value = dbutils.secrets.get(scope="my-scope", key="my-key")
else:
    hostname = ws_client.config.host.replace("https://", "")
    secret_value = os.environ.get("MY_SECRET_KEY")  # Use env vars locally
```

### Path Handling

```python
from pathlib import Path

if IS_DATABRICKS:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    notebook_dir = f"/Workspace{os.path.dirname(notebook_path)}"
else:
    notebook_dir = Path(__file__).parent if "__file__" in dir() else Path.cwd()
```

## Running Locally

```bash
# From project root
uv run path/to/your_notebook.py
```

## Common Issues

### "Invalid access token"

Your profile has an expired PAT token. Fix by using OAuth:

```ini
[DEFAULT]
host      = https://your-workspace.cloud.databricks.com
auth_type = databricks-cli
```

Then re-authenticate:

```bash
databricks auth login --host https://your-workspace.cloud.databricks.com
```

### "Cluster id or serverless are required"

Add `.serverless(True)` to the DatabricksSession builder:

```python
spark = DatabricksSession.builder.serverless(True).getOrCreate()
```

### "Multiple profiles matched"

Multiple profiles in `~/.databrickscfg` have the same host. Either:

1. Specify profile explicitly:
   ```python
   spark = DatabricksSession.builder.profile("my-profile").serverless(True).getOrCreate()
   ```

2. Or remove duplicate profiles from `~/.databrickscfg`

### Local Libraries Not Available on Remote

Libraries installed locally (via `uv`) are not available on serverless compute. This is fine for most cases because:

- SDK calls run locally
- Spark DataFrames don't need your local libraries
- Only UDFs would need libraries on the remote side

If you need local libraries in UDFs, use `spark.sparkContext.addPyFile()` to upload them.

## VS Code Integration

If using the Databricks VS Code extension:

1. Configure the extension to connect to your workspace
2. The extension manages authentication automatically
3. When no profile is specified, `DatabricksSession` and `WorkspaceClient` will use the extension's connection

## Example Notebook Structure

```python
import os
import sys
from pathlib import Path

# === ENVIRONMENT DETECTION ===
IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ

# === SPARK SESSION ===
if IS_DATABRICKS:
    print("Running on Databricks")
else:
    from databricks.connect import DatabricksSession
    spark = DatabricksSession.builder.serverless(True).getOrCreate()
    print("Connected via databricks-connect (serverless)")

# === SDK CLIENT ===
from databricks.sdk import WorkspaceClient
ws_client = WorkspaceClient()
print(f"Workspace: {ws_client.config.host}")

# === YOUR CODE ===
# Spark operations run on remote serverless compute
df = spark.sql("SELECT current_user() as user")
print(df.collect())
```
