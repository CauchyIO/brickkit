"""
Workflow resource importers.

Imports jobs and DLT pipelines from the workspace.
"""

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from databricks.sdk.errors import NotFound, PermissionDenied

from .base import ImportOptions, ImportResult, ResourceImporter

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Workflow Models (simple dataclasses for imported resources)
# -----------------------------------------------------------------------------


@dataclass
class Job:
    """
    Imported job from Databricks workspace.

    Jobs support tags which can be used for governance and cost tracking.
    """

    job_id: int
    name: str
    creator_user_name: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)

    # Run configuration
    schedule: Optional[str] = None  # Cron expression if scheduled
    max_concurrent_runs: int = 1

    # Compute
    cluster_spec: Optional[Dict[str, Any]] = None

    # Tasks summary
    task_count: int = 0
    task_types: List[str] = field(default_factory=list)  # ["notebook", "spark_jar", etc.]

    # Metadata
    created_time: Optional[int] = None
    run_as_user_name: Optional[str] = None

    def matches_pattern(self, pattern: str) -> bool:
        """Check if job name matches a naming pattern."""
        import re

        return bool(re.match(pattern, self.name))

    def has_tag(self, key: str) -> bool:
        """Check if job has a specific tag."""
        return key in self.tags

    def get_tag(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """Get a tag value."""
        return self.tags.get(key, default)


@dataclass
class Pipeline:
    """
    Imported DLT pipeline from Databricks workspace.

    Pipelines (Delta Live Tables) are data engineering workflows.
    """

    pipeline_id: str
    name: str
    creator_user_name: Optional[str] = None

    # Configuration
    continuous: bool = False
    development: bool = False
    photon: bool = False
    serverless: bool = False

    # Storage
    storage: Optional[str] = None
    target: Optional[str] = None  # Target schema

    # Catalog (Unity Catalog integration)
    catalog: Optional[str] = None

    # Compute
    cluster_spec: Optional[Dict[str, Any]] = None

    # State
    state: Optional[str] = None  # IDLE, RUNNING, etc.

    # Metadata
    created_at: Optional[str] = None

    def matches_pattern(self, pattern: str) -> bool:
        """Check if pipeline name matches a naming pattern."""
        import re

        return bool(re.match(pattern, self.name))


# -----------------------------------------------------------------------------
# Importers
# -----------------------------------------------------------------------------


class JobImporter(ResourceImporter[Job]):
    """
    Import jobs from the workspace.

    Jobs are a key governance target because they:
    - Execute code with specific permissions
    - Consume compute resources (cost)
    - Can be tagged for tracking

    Common governance patterns:
    - Naming: [TEAM] job_name or team-job_name
    - Required tags: team, cost_center, owner
    """

    @property
    def resource_type(self) -> str:
        return "jobs"

    def pull_all(self) -> ImportResult:
        start_time = time.time()
        jobs: List[Job] = []
        errors: List[str] = []
        skipped = 0

        try:
            for job_info in self.client.jobs.list():
                job_name = ""
                if hasattr(job_info, "settings") and job_info.settings:
                    job_name = job_info.settings.name or ""

                if not self._should_include(job_name):
                    skipped += 1
                    continue

                try:
                    job = self._from_info(job_info)
                    jobs.append(job)
                except Exception as e:
                    errors.append(f"Failed to import job '{job_name}' (ID: {job_info.job_id}): {e}")
                    if not self.options.skip_on_error:
                        raise

        except PermissionDenied as e:
            errors.append(f"Permission denied listing jobs: {e}")

        return ImportResult(
            resource_type=self.resource_type,
            count=len(jobs),
            resources=jobs,
            errors=errors,
            skipped=skipped,
            duration_seconds=time.time() - start_time,
        )

    def pull_one(self, identifier: str, **kwargs: Any) -> Optional[Job]:
        """Pull a single job by ID."""
        try:
            job_info = self.client.jobs.get(int(identifier))
            return self._from_full_info(job_info)
        except NotFound:
            return None
        except ValueError:
            logger.error(f"Invalid job ID: {identifier}")
            return None

    def _from_info(self, info: Any) -> Job:
        """Convert SDK BaseJob (from list) to our Job dataclass."""
        settings = info.settings if hasattr(info, "settings") else None

        name = ""
        tags: Dict[str, str] = {}
        schedule = None
        max_concurrent_runs = 1
        task_count = 0
        task_types: List[str] = []

        if settings:
            name = settings.name or ""
            tags = dict(settings.tags) if settings.tags else {}
            max_concurrent_runs = settings.max_concurrent_runs or 1

            if settings.schedule:
                schedule = settings.schedule.quartz_cron_expression

            if settings.tasks:
                task_count = len(settings.tasks)
                for task in settings.tasks:
                    if task.notebook_task:
                        task_types.append("notebook")
                    elif task.spark_jar_task:
                        task_types.append("spark_jar")
                    elif task.spark_python_task:
                        task_types.append("spark_python")
                    elif task.spark_submit_task:
                        task_types.append("spark_submit")
                    elif task.pipeline_task:
                        task_types.append("pipeline")
                    elif task.python_wheel_task:
                        task_types.append("python_wheel")
                    elif task.sql_task:
                        task_types.append("sql")
                    elif task.dbt_task:
                        task_types.append("dbt")
                    else:
                        task_types.append("other")

        return Job(
            job_id=info.job_id,
            name=name,
            creator_user_name=info.creator_user_name if hasattr(info, "creator_user_name") else None,
            tags=tags,
            schedule=schedule,
            max_concurrent_runs=max_concurrent_runs,
            task_count=task_count,
            task_types=list(set(task_types)),
            created_time=info.created_time if hasattr(info, "created_time") else None,
        )

    def _from_full_info(self, info: Any) -> Job:
        """Convert SDK Job (from get) to our Job dataclass - has more detail."""
        # The full job info has the same structure but with more fields populated
        return self._from_info(info)


class PipelineImporter(ResourceImporter[Pipeline]):
    """
    Import DLT pipelines from the workspace.

    Pipelines are data engineering workflows that:
    - Process data through defined transformations
    - Integrate with Unity Catalog
    - Have their own compute configuration
    """

    @property
    def resource_type(self) -> str:
        return "pipelines"

    def pull_all(self) -> ImportResult:
        start_time = time.time()
        pipelines: List[Pipeline] = []
        errors: List[str] = []
        skipped = 0

        try:
            for pipeline_info in self.client.pipelines.list_pipelines():
                pipeline_name = pipeline_info.name or ""

                if not self._should_include(pipeline_name):
                    skipped += 1
                    continue

                try:
                    pipeline = self._from_info(pipeline_info)
                    pipelines.append(pipeline)
                except Exception as e:
                    errors.append(f"Failed to import pipeline '{pipeline_name}' (ID: {pipeline_info.pipeline_id}): {e}")
                    if not self.options.skip_on_error:
                        raise

        except PermissionDenied as e:
            errors.append(f"Permission denied listing pipelines: {e}")

        return ImportResult(
            resource_type=self.resource_type,
            count=len(pipelines),
            resources=pipelines,
            errors=errors,
            skipped=skipped,
            duration_seconds=time.time() - start_time,
        )

    def pull_one(self, identifier: str, **kwargs: Any) -> Optional[Pipeline]:
        """Pull a single pipeline by ID."""
        try:
            pipeline_info = self.client.pipelines.get(identifier)
            return self._from_full_info(pipeline_info)
        except NotFound:
            return None

    def _from_info(self, info: Any) -> Pipeline:
        """Convert SDK PipelineStateInfo (from list) to our Pipeline dataclass."""
        return Pipeline(
            pipeline_id=info.pipeline_id,
            name=info.name or "",
            creator_user_name=info.creator_user_name if hasattr(info, "creator_user_name") else None,
            state=info.state.value if info.state else None,
        )

    def _from_full_info(self, info: Any) -> Pipeline:
        """Convert SDK GetPipelineResponse to our Pipeline dataclass."""
        spec = info.spec if hasattr(info, "spec") else None

        name = info.name or ""
        continuous = False
        development = False
        photon = False
        serverless = False
        storage = None
        target = None
        catalog = None

        if spec:
            name = spec.name or name
            continuous = spec.continuous or False
            development = spec.development or False
            photon = spec.photon or False
            serverless = spec.serverless or False
            storage = spec.storage
            target = spec.target
            catalog = spec.catalog

        return Pipeline(
            pipeline_id=info.pipeline_id,
            name=name,
            creator_user_name=info.creator_user_name if hasattr(info, "creator_user_name") else None,
            continuous=continuous,
            development=development,
            photon=photon,
            serverless=serverless,
            storage=storage,
            target=target,
            catalog=catalog,
            state=info.state.value if hasattr(info, "state") and info.state else None,
        )
