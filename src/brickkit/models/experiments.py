"""
MLflow Experiment governance model.

This module provides a governed model for MLflow experiments, supporting
owner assignment (via CAN_MANAGE ACL), budget policy attachment, and
environment-aware naming.
"""

from __future__ import annotations

from typing import Dict, Optional

from pydantic import Field, computed_field

from brickkit.models.base import DEFAULT_SECURABLE_OWNER, BaseGovernanceModel, get_current_environment
from brickkit.models.grants import Principal


class MlflowExperiment(BaseGovernanceModel):
    """
    Represents a governed MLflow experiment.

    Environment-aware: the resolved_name appends the current environment suffix
    to the base name, so ``/experiments/fraud_model`` becomes
    ``/experiments/fraud_model_dev`` in DEV.

    Attributes:
        name: Base experiment path (e.g. ``/experiments/fraud_model``).
        artifact_location: DBFS or cloud storage path for artifacts.
        description: Human-readable description (stored as an MLflow tag).
        tags: MLflow key-value tags applied on creation.
        owner: Principal that receives CAN_MANAGE ACL on the experiment.
        budget_policy_id: Budget policy ID to attach to this experiment.
    """

    name: str = Field(..., description="Base experiment path (e.g. /experiments/my_exp)")
    artifact_location: Optional[str] = Field(None, description="DBFS or cloud storage path for artifacts")
    description: Optional[str] = Field(None, description="Human-readable description")
    tags: Dict[str, str] = Field(default_factory=dict, description="MLflow key-value tags")
    owner: Optional[Principal] = Field(
        default_factory=lambda: Principal(name=DEFAULT_SECURABLE_OWNER, add_environment_suffix=False),
        description="Owner group/SPN — will receive CAN_MANAGE ACL on the experiment",
    )
    budget_policy_id: Optional[str] = Field(None, description="Budget policy ID to attach to this experiment")

    @computed_field
    @property
    def resolved_name(self) -> str:
        """Experiment path with environment suffix appended to the final segment."""
        env = get_current_environment()
        return f"{self.name}_{env.value.lower()}"


__all__ = ["MlflowExperiment"]
