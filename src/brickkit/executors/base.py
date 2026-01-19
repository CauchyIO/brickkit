"""
Base executor class for Unity Catalog operations.

Provides common functionality for all executors including error handling,
idempotency, dry-run support, rollback capabilities, and governance validation.
"""

import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, Callable, Dict, Generic, List, Optional, TypeVar

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import (
    AlreadyExists,
    BadRequest,
    InternalError,
    InvalidParameterValue,
    NotFound,
    NotImplemented,
    PermissionDenied,
    ResourceAlreadyExists,
    ResourceDoesNotExist,
    ResourceExhausted,
    TemporarilyUnavailable,
    Unauthenticated,
)

if TYPE_CHECKING:
    from brickkit.defaults import GovernanceDefaults

logger = logging.getLogger(__name__)

T = TypeVar('T')  # Generic type for models


class OperationType(str, Enum):
    """Types of operations that can be performed."""
    CREATE = "CREATE"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    GRANT = "GRANT"
    REVOKE = "REVOKE"
    NO_OP = "NO_OP"
    SKIPPED = "SKIPPED"


@dataclass
class ExecutionResult:
    """Result of an execution operation."""

    success: bool
    operation: OperationType
    resource_type: str
    resource_name: str
    message: str = ""
    error: Optional[Exception] = None
    duration_seconds: float = 0.0
    changes: Dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        """String representation of the result."""
        status = "✅" if self.success else "❌"
        return (
            f"{status} {self.operation.value} {self.resource_type} "
            f"{self.resource_name}: {self.message}"
        )


@dataclass
class ExecutionPlan:
    """Execution plan showing what will be done."""

    operations: List[ExecutionResult] = field(default_factory=list)
    estimated_duration_seconds: float = 0.0

    def add_operation(
        self,
        operation: OperationType,
        resource_type: str,
        resource_name: str,
        changes: Optional[Dict[str, Any]] = None
    ):
        """Add an operation to the plan."""
        self.operations.append(ExecutionResult(
            success=True,  # Plan assumes success
            operation=operation,
            resource_type=resource_type,
            resource_name=resource_name,
            message="Planned",
            changes=changes or {}
        ))

    def __str__(self) -> str:
        """String representation of the plan."""
        if not self.operations:
            return "No operations planned"

        lines = ["Execution Plan:"]
        for i, op in enumerate(self.operations, 1):
            lines.append(f"  {i}. {op.operation.value} {op.resource_type} {op.resource_name}")
            if op.changes:
                for key, value in op.changes.items():
                    lines.append(f"      {key}: {value}")

        lines.append(f"\nTotal operations: {len(self.operations)}")
        lines.append(f"Estimated duration: {self.estimated_duration_seconds:.1f}s")

        return "\n".join(lines)


class BaseExecutor(ABC, Generic[T]):
    """
    Base class for all Unity Catalog executors.

    Provides common functionality including:
    - Error handling and retries
    - Idempotency checks
    - Dry-run mode support
    - Audit logging
    - Rollback capability
    """

    def __init__(
        self,
        client: WorkspaceClient,
        dry_run: bool = False,
        max_retries: int = 3,
        continue_on_error: bool = False,
        governance_defaults: Optional['GovernanceDefaults'] = None
    ):
        """
        Initialize the executor.

        Args:
            client: Databricks SDK client
            dry_run: If True, only show what would be done
            max_retries: Maximum retry attempts for transient failures
            continue_on_error: Continue execution despite errors
            governance_defaults: Optional governance defaults for validation
        """
        self.client = client
        self.dry_run = dry_run
        self.max_retries = max_retries
        self.continue_on_error = continue_on_error
        self.governance_defaults = governance_defaults
        self.results: List[ExecutionResult] = []
        self._rollback_stack: List[Callable[[], None]] = []

    @abstractmethod
    def create(self, resource: T) -> ExecutionResult:
        """
        Create a new resource.

        Args:
            resource: The resource to create

        Returns:
            ExecutionResult indicating success or failure
        """
        pass

    @abstractmethod
    def update(self, resource: T) -> ExecutionResult:
        """
        Update an existing resource.

        Args:
            resource: The resource to update

        Returns:
            ExecutionResult indicating success or failure
        """
        pass

    @abstractmethod
    def delete(self, resource: T) -> ExecutionResult:
        """
        Delete a resource.

        Args:
            resource: The resource to delete

        Returns:
            ExecutionResult indicating success or failure
        """
        pass

    @abstractmethod
    def exists(self, resource: T) -> bool:
        """
        Check if a resource exists.

        Args:
            resource: The resource to check

        Returns:
            True if resource exists, False otherwise
        """
        pass

    @abstractmethod
    def get_resource_type(self) -> str:
        """Get the type of resource this executor handles."""
        pass

    def create_or_update(self, resource: T) -> ExecutionResult:
        """
        Create resource if it doesn't exist, update if it does.

        Args:
            resource: The resource to create or update

        Returns:
            ExecutionResult indicating what was done
        """
        try:
            if self.exists(resource):
                return self.update(resource)
            else:
                return self.create(resource)
        except Exception as e:
            return self._handle_error(
                OperationType.CREATE,
                self._get_resource_name(resource),
                e
            )

    def plan(self, resources: List[T]) -> ExecutionPlan:
        """
        Generate an execution plan for a list of resources.

        Args:
            resources: List of resources to process

        Returns:
            ExecutionPlan showing what would be done
        """
        plan = ExecutionPlan()

        for resource in resources:
            if self.exists(resource):
                # Check if update needed
                if self._needs_update(resource):
                    changes = self._get_changes(resource)
                    plan.add_operation(
                        OperationType.UPDATE,
                        self.get_resource_type(),
                        self._get_resource_name(resource),
                        changes
                    )
                else:
                    plan.add_operation(
                        OperationType.NO_OP,
                        self.get_resource_type(),
                        self._get_resource_name(resource)
                    )
            else:
                plan.add_operation(
                    OperationType.CREATE,
                    self.get_resource_type(),
                    self._get_resource_name(resource)
                )

        # Estimate duration (simple heuristic)
        plan.estimated_duration_seconds = len(plan.operations) * 2.0

        return plan

    def execute_with_retry(self, operation: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """
        Execute an operation with retry logic.

        Args:
            operation: The operation to execute
            *args: Positional arguments for the operation
            **kwargs: Keyword arguments for the operation

        Returns:
            Result of the operation

        Raises:
            Exception: If all retries fail
        """
        last_error = None

        for attempt in range(self.max_retries):
            try:
                result = operation(*args, **kwargs)
                return result
            except (ResourceDoesNotExist, ResourceAlreadyExists, PermissionDenied,
                    InvalidParameterValue, NotFound, AlreadyExists, BadRequest,
                    Unauthenticated, NotImplemented):
                # These are not transient errors - don't retry
                raise
            except (TemporarilyUnavailable, InternalError, ResourceExhausted) as e:
                # These are transient errors - retry with backoff
                last_error = e
                if attempt < self.max_retries - 1:
                    wait_time = 2 ** attempt  # Exponential backoff
                    logger.warning(
                        f"Attempt {attempt + 1} failed: {e}. "
                        f"Retrying in {wait_time} seconds..."
                    )
                    time.sleep(wait_time)
                else:
                    logger.error(f"All {self.max_retries} attempts failed")
            except Exception:
                # Other exceptions - don't retry but capture for re-raising
                raise

        if last_error:
            raise last_error

    def rollback(self):
        """
        Rollback all operations performed by this executor.

        Executes rollback operations in reverse order.
        """
        if not self._rollback_stack:
            logger.info("No operations to rollback")
            return

        logger.info(f"Rolling back {len(self._rollback_stack)} operations")

        while self._rollback_stack:
            rollback_op = self._rollback_stack.pop()
            try:
                rollback_op()
            except Exception as e:
                logger.error(f"Rollback failed: {e}")
                if not self.continue_on_error:
                    raise

    def _handle_error(
        self,
        operation: OperationType,
        resource_name: str,
        error: Exception
    ) -> ExecutionResult:
        """
        Handle an error during execution.

        Args:
            operation: The operation that failed
            resource_name: Name of the resource
            error: The exception that occurred

        Returns:
            ExecutionResult with error details
        """
        # Provide specific error messages for SDK exceptions
        if isinstance(error, PermissionDenied):
            message = f"Permission denied: {str(error)}. Check that the service principal or user has required Unity Catalog privileges."
        elif isinstance(error, (ResourceDoesNotExist, NotFound)):
            message = f"Resource not found: {str(error)}"
        elif isinstance(error, (ResourceAlreadyExists, AlreadyExists)):
            message = f"Resource already exists: {str(error)}"
        elif isinstance(error, (InvalidParameterValue, BadRequest)):
            message = f"Invalid parameter: {str(error)}. Check input values and naming conventions."
        elif isinstance(error, Unauthenticated):
            message = f"Authentication failed: {str(error)}. Check credentials and workspace URL."
        elif isinstance(error, TemporarilyUnavailable):
            message = f"Service temporarily unavailable: {str(error)}. Try again later."
        elif isinstance(error, ResourceExhausted):
            message = f"Resource limit exceeded: {str(error)}. Check quotas and limits."
        elif isinstance(error, NotImplemented):
            message = f"Feature not implemented: {str(error)}. This feature may not be available in your workspace tier."
        else:
            message = str(error)

        result = ExecutionResult(
            success=False,
            operation=operation,
            resource_type=self.get_resource_type(),
            resource_name=resource_name,
            message=message,
            error=error
        )

        self.results.append(result)
        logger.error(f"Operation failed: {result}")

        if not self.continue_on_error:
            raise error

        return result

    def _get_resource_name(self, resource: T) -> str:
        """
        Get the name of a resource.

        Args:
            resource: The resource

        Returns:
            Resource name for logging
        """
        # Try common attribute names
        for attr in ['resolved_name', 'fqdn', 'name']:
            if hasattr(resource, attr):
                value = getattr(resource, attr)
                if callable(value):
                    return value()
                return str(value)
        return str(resource)

    def _needs_update(self, resource: T) -> bool:
        """
        Check if a resource needs updating.

        Override in subclasses for specific logic.

        Args:
            resource: The resource to check

        Returns:
            True if update needed
        """
        return False

    def _get_changes(self, resource: T) -> Dict[str, Any]:
        """
        Get the changes that would be made to a resource.

        Override in subclasses for specific logic.

        Args:
            resource: The resource

        Returns:
            Dictionary of changes
        """
        return {}

    def get_summary(self) -> str:
        """
        Get a summary of execution results.

        Returns:
            Summary string
        """
        if not self.results:
            return "No operations performed"

        successful = sum(1 for r in self.results if r.success)
        failed = sum(1 for r in self.results if not r.success)

        lines = [
            "Execution Summary:",
            f"  Total operations: {len(self.results)}",
            f"  Successful: {successful}",
            f"  Failed: {failed}"
        ]

        if failed > 0:
            lines.append("\nFailed operations:")
            for result in self.results:
                if not result.success:
                    lines.append(f"  - {result}")

        return "\n".join(lines)

    def validate_governance(self, resource: T) -> List[str]:
        """
        Validate resource against governance defaults.

        Args:
            resource: The resource to validate

        Returns:
            List of validation error messages (empty if valid)
        """
        if not self.governance_defaults:
            return []

        # Use resource's validate_governance method if available
        validate_method = getattr(resource, 'validate_governance', None)
        if callable(validate_method):
            return validate_method(self.governance_defaults)

        # Manual validation for resources without the method
        securable_type = getattr(resource, 'securable_type', None)
        tags = getattr(resource, 'tags', None)
        if securable_type is None or tags is None:
            return []

        tag_dict = {t.key: t.value for t in tags}
        return self.governance_defaults.validate_tags(securable_type, tag_dict)

    def ensure_governance(self, resource: T, fail_on_error: bool = True) -> Optional[ExecutionResult]:
        """
        Validate governance and optionally fail if violations found.

        Args:
            resource: The resource to validate
            fail_on_error: If True, return failure result on violations

        Returns:
            ExecutionResult with SKIPPED if violations and fail_on_error, else None
        """
        errors = self.validate_governance(resource)
        if errors and fail_on_error:
            resource_name = self._get_resource_name(resource)
            error_msg = "; ".join(errors)
            logger.warning(f"Governance validation failed for {resource_name}: {error_msg}")
            return ExecutionResult(
                success=False,
                operation=OperationType.SKIPPED,
                resource_type=self.get_resource_type(),
                resource_name=resource_name,
                message=f"Governance validation failed: {error_msg}"
            )
        elif errors:
            resource_name = self._get_resource_name(resource)
            logger.warning(f"Governance warnings for {resource_name}: {errors}")
        return None
