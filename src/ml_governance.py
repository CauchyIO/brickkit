"""
Proactive ML Governance for MLflow - Shift-Left Approach

This module provides decorators, hooks, and templates that embed governance
directly into the ML development workflow, catching issues during experimentation
rather than after production deployment.
"""

from __future__ import annotations
import functools
import inspect
import json
import logging
import os
import time
import warnings
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

import mlflow
from mlflow import MlflowClient
from mlflow.entities import Run, Experiment
from mlflow.models import Model, ModelSignature, infer_signature
from mlflow.tracking import MlflowClient
from pydantic import BaseModel, Field, field_validator, ValidationError

logger = logging.getLogger(__name__)


# ============================================================================
# GOVERNANCE POLICIES - Define what's allowed/required
# ============================================================================

class ModelTier(str, Enum):
    """Model tier determines governance requirements."""
    EXPERIMENTAL = "experimental"  # Low risk, minimal requirements
    DEVELOPMENT = "development"    # Medium risk, standard requirements
    PRODUCTION = "production"      # High risk, strict requirements
    CRITICAL = "critical"          # Mission-critical, maximum requirements


class DataClassification(str, Enum):
    """Data sensitivity classification."""
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"
    PII = "pii"  # Personally Identifiable Information


@dataclass
class GovernancePolicy:
    """
    Defines governance requirements for ML models.
    
    This is configured at the team/project level and enforced
    during training and registration.
    """
    # Basic requirements
    tier: ModelTier = ModelTier.DEVELOPMENT
    data_classification: DataClassification = DataClassification.INTERNAL
    
    # Required metadata
    required_tags: Set[str] = field(default_factory=lambda: {
        "team", "project", "owner", "business_unit"
    })
    required_metrics: Set[str] = field(default_factory=lambda: {
        "accuracy", "precision", "recall"
    })
    
    # Model requirements
    require_signature: bool = True
    require_input_example: bool = True
    require_code_version: bool = True
    require_data_lineage: bool = True
    require_feature_importance: bool = False
    
    # Testing requirements
    min_test_coverage: float = 0.8
    require_validation_dataset: bool = True
    require_cross_validation: bool = False
    min_cv_folds: int = 5
    
    # Performance thresholds
    min_accuracy: Optional[float] = None
    max_latency_ms: Optional[float] = None
    max_memory_mb: Optional[float] = None
    
    # Compliance requirements
    require_bias_testing: bool = False
    require_explainability: bool = False
    require_privacy_assessment: bool = False
    require_security_scan: bool = False
    
    # Approval requirements
    require_peer_review: bool = True
    min_reviewers: int = 1
    require_manager_approval: bool = False
    
    def for_tier(self, tier: ModelTier) -> 'GovernancePolicy':
        """Get policy adjusted for specific tier."""
        policy = GovernancePolicy(
            tier=tier,
            data_classification=self.data_classification,
            required_tags=self.required_tags.copy(),
            required_metrics=self.required_metrics.copy()
        )
        
        if tier == ModelTier.EXPERIMENTAL:
            # Minimal requirements for experiments
            policy.require_signature = False
            policy.require_input_example = False
            policy.min_test_coverage = 0.5
            policy.require_validation_dataset = False
            policy.require_peer_review = False
            
        elif tier == ModelTier.PRODUCTION:
            # Strict requirements for production
            policy.require_signature = True
            policy.require_input_example = True
            policy.require_code_version = True
            policy.require_data_lineage = True
            policy.min_test_coverage = 0.9
            policy.require_cross_validation = True
            policy.require_bias_testing = True
            policy.require_explainability = True
            policy.require_peer_review = True
            policy.min_reviewers = 2
            
        elif tier == ModelTier.CRITICAL:
            # Maximum requirements for critical models
            policy.require_signature = True
            policy.require_input_example = True
            policy.require_code_version = True
            policy.require_data_lineage = True
            policy.require_feature_importance = True
            policy.min_test_coverage = 0.95
            policy.require_cross_validation = True
            policy.min_cv_folds = 10
            policy.require_bias_testing = True
            policy.require_explainability = True
            policy.require_privacy_assessment = True
            policy.require_security_scan = True
            policy.require_peer_review = True
            policy.min_reviewers = 3
            policy.require_manager_approval = True
            
        return policy


# ============================================================================
# VALIDATION FRAMEWORK - Check compliance during development
# ============================================================================

class ValidationResult(BaseModel):
    """Result of a governance validation check."""
    passed: bool
    check_name: str
    message: str
    severity: str = "error"  # error, warning, info
    details: Optional[Dict[str, Any]] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class GovernanceValidator:
    """
    Validates ML artifacts against governance policies.
    
    This runs during training to provide immediate feedback.
    """
    
    def __init__(self, policy: GovernancePolicy):
        self.policy = policy
        self.validation_results: List[ValidationResult] = []
    
    def validate_experiment(self, experiment: Experiment) -> List[ValidationResult]:
        """Validate experiment setup."""
        results = []
        
        # Check experiment naming
        if not self._is_valid_name(experiment.name):
            results.append(ValidationResult(
                passed=False,
                check_name="experiment_naming",
                message=f"Experiment name '{experiment.name}' doesn't follow naming convention",
                severity="warning"
            ))
        
        # Check experiment tags
        missing_tags = self.policy.required_tags - set(experiment.tags.keys())
        if missing_tags:
            results.append(ValidationResult(
                passed=False,
                check_name="experiment_tags",
                message=f"Missing required tags: {missing_tags}",
                severity="error"
            ))
        
        return results
    
    def validate_run(self, run: Run) -> List[ValidationResult]:
        """Validate a training run."""
        results = []
        
        # Check required metrics
        logged_metrics = set(run.data.metrics.keys())
        missing_metrics = self.policy.required_metrics - logged_metrics
        if missing_metrics:
            results.append(ValidationResult(
                passed=False,
                check_name="required_metrics",
                message=f"Missing required metrics: {missing_metrics}",
                severity="error"
            ))
        
        # Check performance thresholds
        if self.policy.min_accuracy and "accuracy" in run.data.metrics:
            accuracy = run.data.metrics["accuracy"]
            if accuracy < self.policy.min_accuracy:
                results.append(ValidationResult(
                    passed=False,
                    check_name="min_accuracy",
                    message=f"Accuracy {accuracy:.3f} below minimum {self.policy.min_accuracy}",
                    severity="error"
                ))
        
        # Check tags
        missing_tags = self.policy.required_tags - set(run.data.tags.keys())
        if missing_tags:
            results.append(ValidationResult(
                passed=False,
                check_name="run_tags",
                message=f"Missing required tags: {missing_tags}",
                severity="error"
            ))
        
        # Check code version
        if self.policy.require_code_version:
            if "mlflow.source.git.commit" not in run.data.tags:
                results.append(ValidationResult(
                    passed=False,
                    check_name="code_version",
                    message="Git commit hash not logged",
                    severity="error" if self.policy.tier >= ModelTier.PRODUCTION else "warning"
                ))
        
        return results
    
    def validate_model(self, model_path: str) -> List[ValidationResult]:
        """Validate a saved model."""
        results = []
        
        try:
            model = mlflow.models.load_model(model_path)
            
            # Check signature
            if self.policy.require_signature and not model.signature:
                results.append(ValidationResult(
                    passed=False,
                    check_name="model_signature",
                    message="Model signature is required but not found",
                    severity="error"
                ))
            
            # Check input example
            if self.policy.require_input_example and not model.saved_input_example_info:
                results.append(ValidationResult(
                    passed=False,
                    check_name="input_example",
                    message="Input example is required but not found",
                    severity="error"
                ))
            
            # Check model metadata
            if model.metadata:
                # Validate model size for latency requirements
                if self.policy.max_memory_mb:
                    model_size_mb = self._get_model_size_mb(model_path)
                    if model_size_mb > self.policy.max_memory_mb:
                        results.append(ValidationResult(
                            passed=False,
                            check_name="model_size",
                            message=f"Model size {model_size_mb:.1f}MB exceeds limit {self.policy.max_memory_mb}MB",
                            severity="error"
                        ))
            
        except Exception as e:
            results.append(ValidationResult(
                passed=False,
                check_name="model_loading",
                message=f"Failed to load model: {e}",
                severity="error"
            ))
        
        return results
    
    def validate_data_lineage(self, run: Run) -> List[ValidationResult]:
        """Validate data lineage tracking."""
        results = []
        
        if self.policy.require_data_lineage:
            # Check for dataset tags
            dataset_tags = {k: v for k, v in run.data.tags.items() 
                          if k.startswith("dataset.")}
            
            if not dataset_tags:
                results.append(ValidationResult(
                    passed=False,
                    check_name="data_lineage",
                    message="No dataset information logged",
                    severity="error"
                ))
            else:
                # Check specific dataset attributes
                required_dataset_info = {"dataset.name", "dataset.version", "dataset.source"}
                missing_info = required_dataset_info - set(dataset_tags.keys())
                if missing_info:
                    results.append(ValidationResult(
                        passed=False,
                        check_name="data_lineage_details",
                        message=f"Missing dataset info: {missing_info}",
                        severity="warning"
                    ))
        
        return results
    
    def _is_valid_name(self, name: str) -> bool:
        """Check if name follows naming convention."""
        # Example: team_project_model_YYYYMMDD
        import re
        pattern = r'^[a-z]+_[a-z]+_[a-z]+_\d{8}$'
        return bool(re.match(pattern, name.lower()))
    
    def _get_model_size_mb(self, model_path: str) -> float:
        """Get model size in MB."""
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(model_path):
            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                total_size += os.path.getsize(filepath)
        return total_size / (1024 * 1024)


# ============================================================================
# DECORATORS - Embed governance into training code
# ============================================================================

def governed_training(
    policy: Optional[GovernancePolicy] = None,
    tier: ModelTier = ModelTier.DEVELOPMENT,
    auto_fix: bool = True
):
    """
    Decorator for training functions that enforces governance.
    
    Usage:
        @governed_training(tier=ModelTier.PRODUCTION)
        def train_model(X, y):
            model = RandomForestClassifier()
            model.fit(X, y)
            return model
    """
    def decorator(train_func: Callable) -> Callable:
        @functools.wraps(train_func)
        def wrapper(*args, **kwargs):
            # Get or create policy
            gov_policy = policy or GovernancePolicy().for_tier(tier)
            validator = GovernanceValidator(gov_policy)
            
            # Start MLflow run with governance context
            with mlflow.start_run() as run:
                # Auto-add required tags
                if auto_fix:
                    _auto_add_governance_tags(run, gov_policy)
                
                # Log governance metadata
                mlflow.set_tag("governance.tier", tier.value)
                mlflow.set_tag("governance.policy_version", "1.0")
                mlflow.set_tag("governance.validated", "true")
                
                # Execute training
                try:
                    result = train_func(*args, **kwargs)
                    
                    # Post-training validation
                    validation_results = validator.validate_run(run)
                    
                    # Log validation results
                    for val_result in validation_results:
                        mlflow.log_metric(f"governance.{val_result.check_name}", 
                                         1.0 if val_result.passed else 0.0)
                    
                    # Fail if critical validations failed
                    critical_failures = [r for r in validation_results 
                                        if not r.passed and r.severity == "error"]
                    if critical_failures:
                        error_msg = "\n".join([f"- {r.message}" for r in critical_failures])
                        raise GovernanceError(f"Governance validation failed:\n{error_msg}")
                    
                    return result
                    
                except Exception as e:
                    mlflow.set_tag("governance.error", str(e))
                    raise
        
        return wrapper
    return decorator


def requires_approval(
    min_reviewers: int = 1,
    approval_tags: Optional[List[str]] = None
):
    """
    Decorator that requires approval before model registration.
    
    Usage:
        @requires_approval(min_reviewers=2)
        def register_model(model_uri, name):
            return mlflow.register_model(model_uri, name)
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Check for approval tags in current run
            client = MlflowClient()
            run = mlflow.active_run()
            
            if run:
                approvals = [tag for tag in run.data.tags 
                           if tag.startswith("approval.reviewer.")]
                
                if len(approvals) < min_reviewers:
                    raise GovernanceError(
                        f"Model requires {min_reviewers} approvals, found {len(approvals)}"
                    )
                
                # Check specific approval tags if provided
                if approval_tags:
                    for required_tag in approval_tags:
                        if required_tag not in run.data.tags:
                            raise GovernanceError(f"Missing required approval: {required_tag}")
            
            return func(*args, **kwargs)
        
        return wrapper
    return decorator


# ============================================================================
# CONTEXT MANAGERS - Governance contexts for training
# ============================================================================

@contextmanager
def governed_experiment(
    name: str,
    policy: Optional[GovernancePolicy] = None,
    tier: ModelTier = ModelTier.DEVELOPMENT,
    tags: Optional[Dict[str, str]] = None
):
    """
    Context manager for governed experiments.
    
    Usage:
        with governed_experiment("my_experiment", tier=ModelTier.PRODUCTION) as exp:
            # Training code here
            mlflow.log_metric("accuracy", 0.95)
    """
    policy = policy or GovernancePolicy().for_tier(tier)
    validator = GovernanceValidator(policy)
    
    # Create or get experiment
    experiment = mlflow.set_experiment(name)
    
    # Validate experiment setup
    validation_results = validator.validate_experiment(experiment)
    for result in validation_results:
        if not result.passed and result.severity == "error":
            raise GovernanceError(f"Experiment validation failed: {result.message}")
    
    # Start governed run
    with mlflow.start_run(tags=tags) as run:
        # Add governance context
        mlflow.set_tag("governance.tier", tier.value)
        mlflow.set_tag("governance.experiment_validated", "true")
        
        # Auto-add required tags
        for tag in policy.required_tags:
            if tag not in run.data.tags:
                # Try to infer tag value
                tag_value = _infer_tag_value(tag)
                if tag_value:
                    mlflow.set_tag(tag, tag_value)
        
        try:
            yield run
            
            # Post-run validation
            validation_results = validator.validate_run(run)
            _handle_validation_results(validation_results, policy)
            
        except Exception as e:
            mlflow.set_tag("governance.error", str(e))
            raise


@contextmanager
def data_lineage_tracking(
    dataset_name: str,
    dataset_version: str,
    source: str,
    classification: DataClassification = DataClassification.INTERNAL
):
    """
    Context manager for tracking data lineage.
    
    Usage:
        with data_lineage_tracking("customer_data", "v1.2", "s3://bucket/data"):
            # Training code
            model = train_model(X, y)
    """
    with mlflow.start_run(nested=True) as run:
        # Log dataset information
        mlflow.set_tag("dataset.name", dataset_name)
        mlflow.set_tag("dataset.version", dataset_version)
        mlflow.set_tag("dataset.source", source)
        mlflow.set_tag("dataset.classification", classification.value)
        mlflow.set_tag("dataset.logged_at", datetime.utcnow().isoformat())
        
        # Log data statistics if available
        try:
            # This would integrate with your data profiling tools
            mlflow.log_metric("dataset.row_count", 0)  # Placeholder
            mlflow.log_metric("dataset.column_count", 0)  # Placeholder
        except:
            pass
        
        yield run


# ============================================================================
# MLFLOW HOOKS - Intercept MLflow operations for governance
# ============================================================================

class GovernanceMLflowClient(MlflowClient):
    """
    Extended MLflow client with governance hooks.
    
    This intercepts MLflow operations to enforce governance policies.
    """
    
    def __init__(self, policy: GovernancePolicy, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.policy = policy
        self.validator = GovernanceValidator(policy)
    
    def create_registered_model(self, name: str, tags: Optional[Dict[str, str]] = None, 
                               description: Optional[str] = None) -> Any:
        """Override model registration to enforce governance."""
        # Validate model name
        if not self._is_valid_model_name(name):
            raise GovernanceError(f"Model name '{name}' doesn't follow naming convention")
        
        # Ensure required tags
        tags = tags or {}
        for required_tag in self.policy.required_tags:
            if required_tag not in tags:
                tags[required_tag] = _infer_tag_value(required_tag)
        
        # Add governance metadata
        tags["governance.tier"] = self.policy.tier.value
        tags["governance.validated"] = "true"
        tags["governance.policy_version"] = "1.0"
        
        return super().create_registered_model(name, tags, description)
    
    def create_model_version(self, name: str, source: str, run_id: Optional[str] = None,
                            tags: Optional[Dict[str, str]] = None, 
                            run_link: Optional[str] = None,
                            description: Optional[str] = None) -> Any:
        """Override version creation to validate governance."""
        # Validate the source model
        validation_results = self.validator.validate_model(source)
        
        # Check for blocking issues
        blocking_issues = [r for r in validation_results 
                          if not r.passed and r.severity == "error"]
        if blocking_issues:
            error_msg = "\n".join([f"- {r.message}" for r in blocking_issues])
            raise GovernanceError(f"Model validation failed:\n{error_msg}")
        
        # Validate run if provided
        if run_id:
            run = self.get_run(run_id)
            run_validation = self.validator.validate_run(run)
            blocking_issues = [r for r in run_validation 
                              if not r.passed and r.severity == "error"]
            if blocking_issues:
                error_msg = "\n".join([f"- {r.message}" for r in blocking_issues])
                raise GovernanceError(f"Run validation failed:\n{error_msg}")
        
        return super().create_model_version(name, source, run_id, tags, run_link, description)
    
    def transition_model_version_stage(self, name: str, version: str, stage: str,
                                      archive_existing_versions: bool = False) -> Any:
        """Override stage transition to enforce approval requirements."""
        # Check approval requirements for production transitions
        if stage.lower() in ["production", "staging"]:
            model_version = self.get_model_version(name, version)
            
            # Check for approval tags
            approval_tags = [tag for tag in model_version.tags 
                           if tag.startswith("approval.")]
            
            if len(approval_tags) < self.policy.min_reviewers:
                raise GovernanceError(
                    f"Transition to {stage} requires {self.policy.min_reviewers} approvals, "
                    f"found {len(approval_tags)}"
                )
        
        return super().transition_model_version_stage(name, version, stage, 
                                                     archive_existing_versions)
    
    def _is_valid_model_name(self, name: str) -> bool:
        """Validate model naming convention."""
        # Example: team_project_model
        import re
        pattern = r'^[a-z]+_[a-z]+_[a-z_]+$'
        return bool(re.match(pattern, name.lower()))


# ============================================================================
# TEMPLATES - Pre-configured governance patterns
# ============================================================================

class GovernedMLTemplate:
    """
    Base template for governed ML workflows.
    
    Provides standard patterns that embed governance by default.
    """
    
    def __init__(self, 
                 experiment_name: str,
                 tier: ModelTier = ModelTier.DEVELOPMENT,
                 policy: Optional[GovernancePolicy] = None):
        self.experiment_name = experiment_name
        self.tier = tier
        self.policy = policy or GovernancePolicy().for_tier(tier)
        self.validator = GovernanceValidator(self.policy)
        self.client = GovernanceMLflowClient(self.policy)
    
    def train(self, 
              train_func: Callable,
              X_train: Any,
              y_train: Any,
              X_val: Optional[Any] = None,
              y_val: Optional[Any] = None,
              **kwargs) -> Any:
        """
        Standard training workflow with governance.
        
        Args:
            train_func: Function that trains and returns a model
            X_train, y_train: Training data
            X_val, y_val: Validation data (required for production tier)
            **kwargs: Additional arguments for train_func
        """
        # Validate inputs
        if self.tier >= ModelTier.PRODUCTION and (X_val is None or y_val is None):
            raise GovernanceError("Validation data required for production models")
        
        with governed_experiment(self.experiment_name, self.policy, self.tier) as run:
            # Log data lineage
            mlflow.set_tag("dataset.train_shape", f"{X_train.shape}")
            if X_val is not None:
                mlflow.set_tag("dataset.val_shape", f"{X_val.shape}")
            
            # Train model
            model = train_func(X_train, y_train, **kwargs)
            
            # Compute and log metrics
            self._log_metrics(model, X_train, y_train, "train")
            if X_val is not None:
                self._log_metrics(model, X_val, y_val, "val")
            
            # Log model with governance metadata
            signature = None
            input_example = None
            
            if self.policy.require_signature:
                signature = infer_signature(X_train, model.predict(X_train))
            
            if self.policy.require_input_example:
                input_example = X_train[:5] if len(X_train) > 5 else X_train
            
            mlflow.sklearn.log_model(
                model,
                "model",
                signature=signature,
                input_example=input_example,
                registered_model_name=self._generate_model_name()
            )
            
            # Run post-training validation
            validation_results = self.validator.validate_run(run)
            self._handle_validation_results(validation_results)
            
            return model
    
    def _log_metrics(self, model: Any, X: Any, y: Any, prefix: str) -> None:
        """Log standard metrics."""
        from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
        
        y_pred = model.predict(X)
        
        metrics = {
            f"{prefix}_accuracy": accuracy_score(y, y_pred),
            f"{prefix}_precision": precision_score(y, y_pred, average='weighted'),
            f"{prefix}_recall": recall_score(y, y_pred, average='weighted'),
            f"{prefix}_f1": f1_score(y, y_pred, average='weighted')
        }
        
        for name, value in metrics.items():
            mlflow.log_metric(name, value)
    
    def _generate_model_name(self) -> str:
        """Generate compliant model name."""
        import re
        # Clean experiment name to create model name
        base_name = re.sub(r'[^a-z0-9_]', '_', self.experiment_name.lower())
        return f"{base_name}_model"
    
    def _handle_validation_results(self, results: List[ValidationResult]) -> None:
        """Handle validation results based on policy."""
        errors = [r for r in results if not r.passed and r.severity == "error"]
        warnings = [r for r in results if not r.passed and r.severity == "warning"]
        
        # Log all results
        for result in results:
            mlflow.log_metric(f"validation.{result.check_name}", 
                            1.0 if result.passed else 0.0)
        
        # Handle based on tier
        if errors:
            if self.tier >= ModelTier.PRODUCTION:
                raise GovernanceError(f"Validation failed: {[e.message for e in errors]}")
            else:
                for error in errors:
                    warnings.warn(f"Governance Error: {error.message}", UserWarning)
        
        if warnings:
            for warning in warnings:
                warnings.warn(f"Governance Warning: {warning.message}", UserWarning)


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

class GovernanceError(Exception):
    """Raised when governance validation fails."""
    pass


def _auto_add_governance_tags(run: Run, policy: GovernancePolicy) -> None:
    """Automatically add required governance tags."""
    for tag in policy.required_tags:
        if tag not in run.data.tags:
            value = _infer_tag_value(tag)
            if value:
                mlflow.set_tag(tag, value)


def _infer_tag_value(tag_name: str) -> Optional[str]:
    """Try to infer tag value from environment or defaults."""
    inferences = {
        "team": os.getenv("TEAM_NAME", "data_science"),
        "project": os.getenv("PROJECT_NAME", "ml_project"),
        "owner": os.getenv("USER", "unknown"),
        "business_unit": os.getenv("BUSINESS_UNIT", "analytics"),
        "environment": os.getenv("ENVIRONMENT", "dev"),
    }
    return inferences.get(tag_name, f"missing_{tag_name}")


def _handle_validation_results(results: List[ValidationResult], 
                              policy: GovernancePolicy) -> None:
    """Handle validation results based on policy settings."""
    errors = [r for r in results if not r.passed and r.severity == "error"]
    warnings = [r for r in results if not r.passed and r.severity == "warning"]
    
    if errors and policy.tier >= ModelTier.PRODUCTION:
        error_msg = "\n".join([f"- {e.message}" for e in errors])
        raise GovernanceError(f"Governance validation failed:\n{error_msg}")
    
    for warning in warnings:
        warnings.warn(warning.message, UserWarning)


# ============================================================================
# EARLY WARNING SYSTEM - Detect issues during training
# ============================================================================

class GovernanceMonitor:
    """
    Real-time monitoring of training runs for governance issues.
    
    Provides early warnings and can auto-correct some issues.
    """
    
    def __init__(self, policy: GovernancePolicy):
        self.policy = policy
        self.validator = GovernanceValidator(policy)
        self.issues_detected = []
    
    def monitor_training(self, check_interval: int = 30):
        """
        Monitor active training runs for governance issues.
        
        Args:
            check_interval: Seconds between checks
        """
        client = MlflowClient()
        
        while True:
            active_runs = client.search_runs(
                experiment_ids=[mlflow.get_experiment_by_name(
                    mlflow.get_experiment().name).experiment_id],
                filter_string="status = 'RUNNING'"
            )
            
            for run in active_runs:
                issues = self._check_run_health(run)
                if issues:
                    self._handle_issues(run, issues)
            
            time.sleep(check_interval)
    
    def _check_run_health(self, run: Run) -> List[str]:
        """Check for governance issues in running training."""
        issues = []
        
        # Check if required metrics are being logged
        if not run.data.metrics:
            issues.append("No metrics logged yet")
        
        # Check for missing tags
        missing_tags = self.policy.required_tags - set(run.data.tags.keys())
        if missing_tags:
            issues.append(f"Missing tags: {missing_tags}")
        
        # Check for data leakage indicators
        if "val_accuracy" in run.data.metrics and "train_accuracy" in run.data.metrics:
            val_acc = run.data.metrics["val_accuracy"]
            train_acc = run.data.metrics["train_accuracy"]
            if val_acc > train_acc + 0.05:  # Suspicious if val > train
                issues.append("Potential data leakage detected")
        
        return issues
    
    def _handle_issues(self, run: Run, issues: List[str]) -> None:
        """Handle detected issues."""
        for issue in issues:
            logger.warning(f"Governance issue in run {run.info.run_id}: {issue}")
            
            # Log issue to MLflow
            with mlflow.start_run(run_id=run.info.run_id):
                mlflow.set_tag(f"governance.issue.{len(self.issues_detected)}", issue)
            
            self.issues_detected.append({
                "run_id": run.info.run_id,
                "issue": issue,
                "timestamp": datetime.utcnow()
            })


# ============================================================================
# INTEGRATION WITH CI/CD
# ============================================================================

def generate_governance_report(run_id: str, output_path: str = "governance_report.json") -> Dict:
    """
    Generate governance report for CI/CD integration.
    
    This can be used in GitHub Actions, Jenkins, etc. to gate deployments.
    """
    client = MlflowClient()
    run = client.get_run(run_id)
    
    # Extract governance metadata
    governance_tags = {k: v for k, v in run.data.tags.items() 
                      if k.startswith("governance.")}
    
    # Extract validation metrics
    validation_metrics = {k: v for k, v in run.data.metrics.items() 
                         if k.startswith("validation.") or k.startswith("governance.")}
    
    report = {
        "run_id": run_id,
        "experiment_name": run.info.experiment_id,
        "status": run.info.status,
        "governance": {
            "tier": governance_tags.get("governance.tier", "unknown"),
            "validated": governance_tags.get("governance.validated", "false") == "true",
            "policy_version": governance_tags.get("governance.policy_version", "unknown"),
            "tags": governance_tags,
            "validation_results": validation_metrics
        },
        "metrics": run.data.metrics,
        "approvals": [k for k in run.data.tags if k.startswith("approval.")],
        "passed": all(v == 1.0 for v in validation_metrics.values()),
        "generated_at": datetime.utcnow().isoformat()
    }
    
    # Save report
    with open(output_path, 'w') as f:
        json.dump(report, f, indent=2)
    
    return report