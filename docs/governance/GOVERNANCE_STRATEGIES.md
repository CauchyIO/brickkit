# Unity Catalog Governance Strategies Guide

## Executive Summary

This guide provides comprehensive governance strategies for Unity Catalog implementations using the dbrcdk framework. Each strategy addresses real-world scenarios with practical implementation patterns, leveraging the Pydantic models and grant system already in place.

**Note**: Each strategy section references the corresponding Operational Governance Pattern it implements. See OPERATIONAL_GOVERNANCE_PATTERNS.md for pattern details.

## Pattern Implementation Matrix

| Operational Pattern | Section | Maturity Level | Implementation Status |
|---------------------|---------|----------------|----------------------|
| Physical Segregation | §3.1-3.2, §11.3 | Starter (0-6mo) | ✅ Full |
| Zone Progression | §11.3, §8.2 | Starter (0-6mo) | ✅ Full |
| Producer Quality | §1.1, §1.3, §5.1 | Starter (0-6mo) | ✅ Full |
| Interface Contract | §6.3 | Intermediate (6-12mo) | ⚠️ Partial |
| Data Mesh | §6.1-6.2 | Advanced (12+mo) | ⚠️ Partial |
| Hub-and-Spoke | §5 | Intermediate (6-12mo) | ⚠️ Implicit |
| SLA Negotiation | §14.1 | Intermediate (6-12mo) | ✅ Full |
| Shared Enterprise | §14.2 | Intermediate (6-12mo) | ✅ Full |
| Mixed Sensitivity | §14.3 | Intermediate (6-12mo) | ✅ Full |
| Data Vault | §14.4 | Advanced (12+mo) | ✅ Full |
| Push vs Pull | §14.5 | Starter (0-6mo) | ✅ Full |
| ML Lifecycle | §9 | Advanced (12+mo) | ✅ Full |
| Deployment | §10 | Intermediate (6-12mo) | ✅ Full |

---

## 1. Data Producer Governance Patterns

### 1.1 Domain-Owned Data Products

**Business Scenario**: Each business domain (sales, marketing, finance) owns and governs their data products while enabling controlled cross-domain access.

**Implementation Strategy**:
```python
from dbrcdk import Catalog, Schema, AccessManager, Principal, PrivilegeType

# Domain team owns their catalog
sales_catalog = Catalog(
    name="sales",
    comment="Sales domain data products",
    owner="sales_data_team"  # Domain ownership
)

# Structured schema hierarchy for data products
sales_catalog.add_schema(Schema(name="raw", comment="Landing zone"))
sales_catalog.add_schema(Schema(name="cleansed", comment="Validated data"))
sales_catalog.add_schema(Schema(name="curated", comment="Business-ready products"))

# Domain team self-manages access
access_manager = AccessManager()

# Full control for domain team
access_manager.grant(
    sales_catalog,
    Principal("sales_data_team"),
    [PrivilegeType.MANAGE]  # Full ownership including grant management
)

# Read-only for consumers
access_manager.grant(
    sales_catalog.get_schema("curated"),
    Principal("analytics_consumers"),
    [PrivilegeType.USE_SCHEMA, PrivilegeType.SELECT]
)
```

**Best Practices**:
- Use catalog-level ownership for clear domain boundaries
- Implement three-tier schema structure (raw → cleansed → curated)
- Grant MANAGE privilege to domain teams for self-governance
- Expose only curated schemas to consumers

### 1.2 Self-Service Data Publishing

**Business Scenario**: Data producers can publish datasets without central IT intervention while maintaining governance standards.

**Implementation Strategy**:
```python
# Publishing workspace with write permissions
publishing_catalog = Catalog(
    name="data_products",
    isolation_mode=IsolationMode.OPEN,
    tags=[
        Tag(key="data_classification", value="internal"),
        Tag(key="publishing_enabled", value="true")
    ]
)

# Schema per publishing team
for team in ["marketing", "sales", "operations"]:
    team_schema = Schema(
        name=f"{team}_published",
        comment=f"Published datasets from {team}"
    )
    publishing_catalog.add_schema(team_schema)
    
    # Team can create and manage their objects
    access_manager.grant(
        team_schema,
        Principal(f"{team}_publishers"),
        [
            PrivilegeType.USE_SCHEMA,
            PrivilegeType.CREATE_TABLE,
            PrivilegeType.CREATE_VIEW,
            PrivilegeType.MODIFY
        ]
    )
```

**Challenges & Mitigations**:
- **Challenge**: Data quality control
  - **Mitigation**: Implement automated validation before publishing
- **Challenge**: Naming conflicts
  - **Mitigation**: Enforce naming conventions via validation rules

### 1.3 Ownership Delegation Models

**Business Scenario**: Central governance team delegates specific ownership responsibilities to domain teams.

**Implementation Strategy**:
```python
# Hierarchical ownership delegation
class GovernanceHierarchy:
    def setup_delegated_ownership(self):
        # Central governance owns metastore
        metastore_owner = Principal("central_governance")
        
        # Domain leads own catalogs
        catalog_owners = {
            "finance": Principal("finance_lead"),
            "marketing": Principal("marketing_lead"),
            "operations": Principal("ops_lead")
        }
        
        # Project teams own schemas
        for catalog_name, owner in catalog_owners.items():
            catalog = Catalog(name=catalog_name, owner=owner.resolved_name)
            
            # Allow catalog owner to create schemas
            access_manager.grant(
                catalog,
                owner,
                [PrivilegeType.CREATE_SCHEMA, PrivilegeType.MANAGE]
            )
            
            # Catalog owner can delegate schema ownership
            project_schema = Schema(
                name="project_alpha",
                owner="project_alpha_team"  # Delegated ownership
            )
            catalog.add_schema(project_schema)
```

---

## 2. Access Control Strategies

### 2.1 Role-Based Access Patterns (RBAC)

**Business Scenario**: Standard roles with predefined permission sets across the organization.

**Implementation Strategy**:
```python
from dbrcdk import AccessPolicy, SecurableType

# Define standard role policies
STANDARD_ROLES = {
    "DATA_READER": AccessPolicy(
        name="DATA_READER",
        privilege_map={
            SecurableType.CATALOG: [PrivilegeType.USE_CATALOG, PrivilegeType.BROWSE],
            SecurableType.SCHEMA: [PrivilegeType.USE_SCHEMA, PrivilegeType.BROWSE],
            SecurableType.TABLE: [PrivilegeType.SELECT],
            SecurableType.VIEW: [PrivilegeType.SELECT]
        }
    ),
    
    "DATA_WRITER": AccessPolicy(
        name="DATA_WRITER",
        privilege_map={
            SecurableType.CATALOG: [PrivilegeType.USE_CATALOG],
            SecurableType.SCHEMA: [PrivilegeType.USE_SCHEMA, PrivilegeType.CREATE_TABLE],
            SecurableType.TABLE: [PrivilegeType.SELECT, PrivilegeType.MODIFY],
            SecurableType.VOLUME: [PrivilegeType.READ_VOLUME, PrivilegeType.WRITE_VOLUME]
        }
    ),
    
    "DATA_OWNER": AccessPolicy(
        name="DATA_OWNER",
        privilege_map={
            SecurableType.CATALOG: [PrivilegeType.MANAGE],
            SecurableType.SCHEMA: [PrivilegeType.MANAGE],
            SecurableType.TABLE: [PrivilegeType.MANAGE]
        }
    ),
    
    "DATA_STEWARD": AccessPolicy(
        name="DATA_STEWARD",
        privilege_map={
            SecurableType.CATALOG: [PrivilegeType.USE_CATALOG, PrivilegeType.APPLY_TAG],
            SecurableType.SCHEMA: [PrivilegeType.USE_SCHEMA, PrivilegeType.APPLY_TAG],
            SecurableType.TABLE: [PrivilegeType.BROWSE, PrivilegeType.APPLY_TAG]
        }
    )
}

# Apply role-based access
def assign_role(securable, principal: Principal, role_name: str):
    role_policy = STANDARD_ROLES[role_name]
    access_manager.grant(securable, principal, role_policy)
```

### 2.2 Team-Based Access Models

**Business Scenario**: Cross-functional teams need collaborative access to shared resources.

**Implementation Strategy**:
```python
# Team-based catalog structure
class TeamAccessModel:
    def setup_team_collaboration(self):
        # Shared collaboration catalog
        collab_catalog = Catalog(name="collaboration")
        
        # Project-specific schemas
        project_schema = Schema(name="customer_360")
        collab_catalog.add_schema(project_schema)
        
        # Core team members - full access
        core_team = Principal(
            name="customer_360_team",
            add_environment_suffix=True
        )
        
        # Extended team - read and contribute
        extended_team = Principal(
            name="customer_360_extended",
            add_environment_suffix=True
        )
        
        # Stakeholders - read only
        stakeholders = Principal(
            name="customer_360_stakeholders",
            add_environment_suffix=True
        )
        
        # Layered access model
        access_manager.grant(
            project_schema,
            core_team,
            [PrivilegeType.MANAGE]  # Full control
        )
        
        access_manager.grant(
            project_schema,
            extended_team,
            [PrivilegeType.USE_SCHEMA, PrivilegeType.CREATE_TABLE, 
             PrivilegeType.SELECT, PrivilegeType.MODIFY]
        )
        
        access_manager.grant(
            project_schema,
            stakeholders,
            [PrivilegeType.USE_SCHEMA, PrivilegeType.SELECT]
        )
```

### 2.3 Temporal Access Patterns

**Business Scenario**: Time-bound access for contractors, auditors, or project-based work.

**Implementation Strategy**:
```python
from datetime import datetime, timedelta

class TemporalAccessManager:
    """Manages time-bound access grants with automatic expiration."""
    
    def grant_temporal_access(
        self,
        securable,
        principal: Principal,
        privileges: List[PrivilegeType],
        expiry_date: datetime
    ):
        # Add temporal metadata via tags
        temporal_tag = Tag(
            key="access_expiry",
            value=expiry_date.isoformat()
        )
        
        # Create temporary principal with expiry marker
        temp_principal = Principal(
            name=f"{principal.name}_temp",
            environment_mapping={
                Environment.DEV: f"{principal.name}_temp_dev",
                Environment.ACC: f"{principal.name}_temp_acc",
                Environment.PRD: f"{principal.name}_temp_prd"
            }
        )
        
        # Grant access with expiry tracking
        access_manager.grant(securable, temp_principal, privileges)
        
        # Schedule revocation job (pseudo-code)
        # scheduler.schedule_at(expiry_date, revoke_access, temp_principal)
        
        return temp_principal

# Usage example
temporal_manager = TemporalAccessManager()

# Grant 30-day access for contractor
contractor_access = temporal_manager.grant_temporal_access(
    securable=analytics_catalog.get_schema("sandbox"),
    principal=Principal("contractor_john"),
    privileges=[PrivilegeType.USE_SCHEMA, PrivilegeType.CREATE_TABLE],
    expiry_date=datetime.now() + timedelta(days=30)
)
```

### 2.4 Cross-Functional Collaboration Patterns

**Business Scenario**: Enable secure collaboration between different departments while maintaining boundaries.

**Implementation Strategy**:
```python
class CrossFunctionalCollaboration:
    def setup_collaboration_zone(self):
        # Dedicated collaboration catalog
        collab_catalog = Catalog(
            name="cross_functional",
            comment="Cross-functional collaboration zone",
            isolation_mode=IsolationMode.OPEN
        )
        
        # Department-specific contribution areas
        for dept in ["sales", "marketing", "finance", "operations"]:
            # Each department can contribute
            contrib_schema = Schema(
                name=f"{dept}_contribution",
                comment=f"Data contributed by {dept}"
            )
            collab_catalog.add_schema(contrib_schema)
            
            # Department owns their contribution area
            dept_principal = Principal(f"{dept}_team")
            access_manager.grant(
                contrib_schema,
                dept_principal,
                [PrivilegeType.MANAGE]
            )
        
        # Shared results area
        results_schema = Schema(
            name="shared_insights",
            comment="Cross-functional analysis results"
        )
        collab_catalog.add_schema(results_schema)
        
        # All departments can read results
        all_depts = Principal("all_departments_group")
        access_manager.grant(
            results_schema,
            all_depts,
            [PrivilegeType.USE_SCHEMA, PrivilegeType.SELECT]
        )
        
        # Analytics team can write results
        analytics_team = Principal("analytics_team")
        access_manager.grant(
            results_schema,
            analytics_team,
            [PrivilegeType.CREATE_TABLE, PrivilegeType.MODIFY]
        )
```

---

## 3. Data Classification-Based Strategies

### 3.1 PII/Sensitive Data Handling

**Business Scenario**: Enforce strict controls on personally identifiable information and sensitive data.

**Implementation Strategy**:
```python
class PIIGovernanceStrategy:
    def setup_pii_controls(self):
        # PII-specific catalog with enhanced security
        pii_catalog = Catalog(
            name="sensitive_data",
            comment="Contains PII and sensitive information",
            isolation_mode=IsolationMode.ISOLATED,  # Restricted access
            tags=[
                Tag(key="data_classification", value="sensitive"),
                Tag(key="compliance", value="gdpr,ccpa"),
                Tag(key="encryption", value="required")
            ]
        )
        
        # Structured PII schemas
        pii_catalog.add_schema(Schema(
            name="customer_pii",
            comment="Customer personal information",
            tags=[Tag(key="contains_pii", value="true")]
        ))
        
        pii_catalog.add_schema(Schema(
            name="employee_pii",
            comment="Employee personal information",
            tags=[Tag(key="contains_pii", value="true")]
        ))
        
        # Anonymized views for broader access
        anonymized_schema = Schema(
            name="anonymized",
            comment="Anonymized/pseudonymized data for analysis"
        )
        pii_catalog.add_schema(anonymized_schema)
        
        # Strict access controls
        # Only privacy team has direct PII access
        privacy_team = Principal(
            name="privacy_officers",
            add_environment_suffix=False  # Same team across environments
        )
        
        access_manager.grant(
            pii_catalog.get_schema("customer_pii"),
            privacy_team,
            [PrivilegeType.SELECT, PrivilegeType.MODIFY]
        )
        
        # Data scientists get anonymized access only
        data_scientists = Principal("data_science_team")
        access_manager.grant(
            anonymized_schema,
            data_scientists,
            [PrivilegeType.USE_SCHEMA, PrivilegeType.SELECT]
        )
        
        # Audit all access
        self.enable_audit_logging(pii_catalog)
    
    def enable_audit_logging(self, catalog):
        """Enable comprehensive audit logging for sensitive data."""
        catalog.tags.append(
            Tag(key="audit_level", value="detailed")
        )
        # Additional audit configuration would happen here
```

### 3.2 Tiered Data Classification

**Business Scenario**: Implement multi-tier classification system (Public, Internal, Confidential, Restricted).

**Implementation Strategy**:
```python
from enum import Enum

class DataClassification(str, Enum):
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"

class TieredAccessControl:
    def setup_classification_tiers(self):
        # Map classification to required privileges
        classification_policies = {
            DataClassification.PUBLIC: {
                "allowed_principals": [Principal("all_users")],
                "privileges": [PrivilegeType.SELECT, PrivilegeType.BROWSE]
            },
            DataClassification.INTERNAL: {
                "allowed_principals": [Principal("employees")],
                "privileges": [PrivilegeType.SELECT, PrivilegeType.USE_SCHEMA]
            },
            DataClassification.CONFIDENTIAL: {
                "allowed_principals": [Principal("authorized_users")],
                "privileges": [PrivilegeType.SELECT],
                "requires_approval": True
            },
            DataClassification.RESTRICTED: {
                "allowed_principals": [Principal("security_team")],
                "privileges": [PrivilegeType.SELECT],
                "requires_mfa": True,
                "requires_approval": True
            }
        }
        
        # Create catalogs per classification tier
        for classification in DataClassification:
            catalog = Catalog(
                name=f"tier_{classification.value}",
                comment=f"{classification.value.title()} classified data",
                tags=[
                    Tag(key="classification", value=classification.value),
                    Tag(key="min_clearance", value=classification.value)
                ]
            )
            
            # Apply tier-specific policies
            policy = classification_policies[classification]
            for principal in policy["allowed_principals"]:
                access_manager.grant(
                    catalog,
                    principal,
                    policy["privileges"]
                )
            
            # Add approval workflow tags if required
            if policy.get("requires_approval"):
                catalog.tags.append(
                    Tag(key="approval_required", value="true")
                )
            
            if policy.get("requires_mfa"):
                catalog.tags.append(
                    Tag(key="mfa_required", value="true")
                )
```

### 3.3 Compliance-Driven Access Patterns

**Business Scenario**: Implement access patterns that ensure regulatory compliance (GDPR, HIPAA, SOX).

**Implementation Strategy**:
```python
class ComplianceGovernance:
    def setup_gdpr_compliance(self):
        """GDPR-compliant data governance."""
        gdpr_catalog = Catalog(
            name="eu_customer_data",
            comment="EU customer data under GDPR",
            tags=[
                Tag(key="regulation", value="gdpr"),
                Tag(key="data_residency", value="eu-west-1"),
                Tag(key="retention_days", value="2555")  # 7 years
            ]
        )
        
        # Purpose-based schemas
        purposes = {
            "marketing": "Direct marketing activities",
            "analytics": "Business intelligence and analytics",
            "operations": "Service delivery and operations",
            "legal": "Legal basis processing"
        }
        
        for purpose, description in purposes.items():
            schema = Schema(
                name=f"purpose_{purpose}",
                comment=description,
                tags=[
                    Tag(key="legal_basis", value=purpose),
                    Tag(key="purpose_limitation", value="enforced")
                ]
            )
            gdpr_catalog.add_schema(schema)
            
            # Purpose-specific access groups
            purpose_group = Principal(f"gdpr_{purpose}_processors")
            access_manager.grant(
                schema,
                purpose_group,
                [PrivilegeType.USE_SCHEMA, PrivilegeType.SELECT]
            )
        
        # Right to be forgotten support
        deletion_schema = Schema(
            name="deletion_requests",
            comment="GDPR Article 17 deletion tracking"
        )
        gdpr_catalog.add_schema(deletion_schema)
        
        # Only DPO can process deletions
        dpo_principal = Principal(
            name="data_protection_officer",
            add_environment_suffix=False
        )
        access_manager.grant(
            deletion_schema,
            dpo_principal,
            [PrivilegeType.MANAGE]
        )
    
    def setup_hipaa_compliance(self):
        """HIPAA-compliant healthcare data governance."""
        hipaa_catalog = Catalog(
            name="protected_health_info",
            comment="PHI under HIPAA compliance",
            isolation_mode=IsolationMode.ISOLATED,
            tags=[
                Tag(key="regulation", value="hipaa"),
                Tag(key="encryption", value="aes256"),
                Tag(key="access_control", value="role_based")
            ]
        )
        
        # Minimum necessary standard
        access_levels = {
            "full_phi": "Complete PHI access for treatment",
            "limited_phi": "Limited dataset for research",
            "deidentified": "Safe harbor de-identified data"
        }
        
        for level, description in access_levels.items():
            schema = Schema(
                name=level,
                comment=description,
                tags=[Tag(key="phi_level", value=level)]
            )
            hipaa_catalog.add_schema(schema)
        
        # Healthcare role-based access
        healthcare_roles = {
            "physicians": ["full_phi"],
            "researchers": ["limited_phi", "deidentified"],
            "analysts": ["deidentified"]
        }
        
        for role, allowed_schemas in healthcare_roles.items():
            principal = Principal(f"healthcare_{role}")
            for schema_name in allowed_schemas:
                schema = hipaa_catalog.get_schema(schema_name)
                access_manager.grant(
                    schema,
                    principal,
                    [PrivilegeType.USE_SCHEMA, PrivilegeType.SELECT]
                )
```

---

## 4. Environment Progression Strategies

### 4.1 Dev → Acceptance → Production Access Patterns

**Business Scenario**: Different access levels across environments with promotion workflows.

**Implementation Strategy**:
```python
class EnvironmentProgression:
    def setup_environment_access(self):
        """Configure environment-specific access patterns."""
        
        # Environment-specific principal mappings
        principals_by_env = {
            Environment.DEV: {
                "developers": ["full_access"],
                "testers": ["read_write"],
                "analysts": ["read_only"]
            },
            Environment.ACC: {
                "developers": ["read_write"],
                "testers": ["full_access"],
                "analysts": ["read_only"],
                "validators": ["read_only"]
            },
            Environment.PRD: {
                "developers": ["read_only"],
                "operators": ["read_write"],
                "analysts": ["read_only"],
                "support": ["read_only_limited"]
            }
        }
        
        # Access policies per level
        env_policies = {
            "full_access": [PrivilegeType.MANAGE],
            "read_write": [
                PrivilegeType.USE_CATALOG,
                PrivilegeType.USE_SCHEMA,
                PrivilegeType.SELECT,
                PrivilegeType.MODIFY,
                PrivilegeType.CREATE_TABLE
            ],
            "read_only": [
                PrivilegeType.USE_CATALOG,
                PrivilegeType.USE_SCHEMA,
                PrivilegeType.SELECT
            ],
            "read_only_limited": [
                PrivilegeType.USE_CATALOG,
                PrivilegeType.BROWSE  # Metadata only
            ]
        }
        
        # Apply environment-specific access
        current_env = get_current_environment()
        catalog = Catalog(name="application_data")
        
        for role, access_levels in principals_by_env[current_env].items():
            principal = Principal(
                name=role,
                environment_mapping={
                    Environment.DEV: f"{role}_dev",
                    Environment.ACC: f"{role}_acc",
                    Environment.PRD: f"{role}_prd"
                }
            )
            
            for access_level in access_levels:
                privileges = env_policies[access_level]
                access_manager.grant(catalog, principal, privileges)
```

### 4.2 Environment-Specific Role Mappings

**Business Scenario**: Same logical role has different permissions across environments.

**Implementation Strategy**:
```python
class EnvironmentRoleMapping:
    def create_environment_aware_roles(self):
        """Roles that adapt to environment context."""
        
        # Define role with environment-specific privileges
        class EnvironmentAwareRole:
            def __init__(self, role_name: str):
                self.role_name = role_name
                self.env_privileges = {
                    Environment.DEV: {
                        "data_engineer": [
                            PrivilegeType.MANAGE  # Full control in dev
                        ],
                        "data_analyst": [
                            PrivilegeType.CREATE_TABLE,
                            PrivilegeType.MODIFY,
                            PrivilegeType.SELECT
                        ]
                    },
                    Environment.ACC: {
                        "data_engineer": [
                            PrivilegeType.CREATE_TABLE,
                            PrivilegeType.MODIFY,
                            PrivilegeType.SELECT
                        ],
                        "data_analyst": [
                            PrivilegeType.SELECT  # Read-only in ACC
                        ]
                    },
                    Environment.PRD: {
                        "data_engineer": [
                            PrivilegeType.SELECT,  # Read-only in prod
                            PrivilegeType.BROWSE
                        ],
                        "data_analyst": [
                            PrivilegeType.BROWSE  # Metadata only in prod
                        ]
                    }
                }
            
            def get_privileges(self, environment: Environment):
                return self.env_privileges[environment].get(
                    self.role_name, []
                )
        
        # Apply environment-aware roles
        engineer_role = EnvironmentAwareRole("data_engineer")
        analyst_role = EnvironmentAwareRole("data_analyst")
        
        current_env = get_current_environment()
        
        # Grant appropriate privileges based on environment
        for role in [engineer_role, analyst_role]:
            principal = Principal(role.role_name)
            privileges = role.get_privileges(current_env)
            
            if privileges:
                access_manager.grant(
                    catalog,
                    principal,
                    privileges
                )
```

### 4.3 Testing Data vs Production Data Access

**Business Scenario**: Different access patterns for test data (synthetic/masked) vs production data.

**Implementation Strategy**:
```python
class TestDataGovernance:
    def setup_test_data_access(self):
        """Configure test data access with production data protection."""
        
        # Test data catalog (synthetic/masked)
        test_catalog = Catalog(
            name="test_data",
            comment="Synthetic and masked test data",
            tags=[
                Tag(key="data_type", value="synthetic"),
                Tag(key="production_safe", value="true")
            ]
        )
        
        # Production data catalog
        prod_catalog = Catalog(
            name="production_data",
            comment="Production customer data",
            isolation_mode=IsolationMode.ISOLATED,
            tags=[
                Tag(key="data_type", value="production"),
                Tag(key="sensitive", value="true")
            ]
        )
        
        # Developers get full test access, no prod access
        developers = Principal("developers")
        access_manager.grant(
            test_catalog,
            developers,
            [PrivilegeType.MANAGE]  # Full control of test data
        )
        
        # Testers get read access to both
        testers = Principal("qa_testers")
        access_manager.grant(
            test_catalog,
            testers,
            [PrivilegeType.SELECT]
        )
        
        # Only in production environment, grant limited prod access
        if get_current_environment() == Environment.PRD:
            # Support team gets production read access
            support = Principal(
                name="support_team",
                add_environment_suffix=False  # Already in prod
            )
            access_manager.grant(
                prod_catalog,
                support,
                [PrivilegeType.USE_CATALOG, PrivilegeType.SELECT]
            )
        
        # Data masking for test environment creation
        self.setup_masking_pipeline(prod_catalog, test_catalog)
    
    def setup_masking_pipeline(self, source_catalog, target_catalog):
        """Setup automated masking pipeline from prod to test."""
        # Pipeline service account
        pipeline_account = Principal(
            name="masking_pipeline_svc",
            add_environment_suffix=False
        )
        
        # Read from production
        access_manager.grant(
            source_catalog,
            pipeline_account,
            [PrivilegeType.SELECT]
        )
        
        # Write to test
        access_manager.grant(
            target_catalog,
            pipeline_account,
            [PrivilegeType.MODIFY, PrivilegeType.CREATE_TABLE]
        )
```

---

## 5. Hierarchical Governance Models

### 5.1 Catalog-Level Strategies (Business Domain Ownership)

**Business Scenario**: Each business domain owns and governs a catalog representing their data assets.

**Implementation Strategy**:
```python
class DomainDrivenGovernance:
    def setup_domain_catalogs(self):
        """Implement domain-driven catalog ownership."""
        
        domains = {
            "sales": {
                "owner": "chief_sales_officer",
                "data_steward": "sales_data_steward",
                "schemas": ["leads", "opportunities", "accounts", "forecasts"]
            },
            "marketing": {
                "owner": "chief_marketing_officer",
                "data_steward": "marketing_data_steward",
                "schemas": ["campaigns", "segments", "attribution", "content"]
            },
            "finance": {
                "owner": "chief_financial_officer",
                "data_steward": "finance_data_steward",
                "schemas": ["gl", "ar", "ap", "budgets", "forecasts"]
            },
            "hr": {
                "owner": "chief_people_officer",
                "data_steward": "hr_data_steward",
                "schemas": ["employees", "recruiting", "performance", "compensation"]
            }
        }
        
        for domain_name, config in domains.items():
            # Create domain catalog
            domain_catalog = Catalog(
                name=domain_name,
                owner=config["owner"],
                comment=f"{domain_name.title()} domain data assets",
                tags=[
                    Tag(key="domain", value=domain_name),
                    Tag(key="data_steward", value=config["data_steward"])
                ]
            )
            
            # Add domain schemas
            for schema_name in config["schemas"]:
                schema = Schema(
                    name=schema_name,
                    comment=f"{domain_name.title()} {schema_name}",
                    owner=config["data_steward"]  # Steward owns schemas
                )
                domain_catalog.add_schema(schema)
            
            # Domain owner has full control
            owner_principal = Principal(
                name=config["owner"],
                add_environment_suffix=False  # C-level doesn't change
            )
            access_manager.grant(
                domain_catalog,
                owner_principal,
                [PrivilegeType.MANAGE]
            )
            
            # Data steward manages day-to-day
            steward_principal = Principal(config["data_steward"])
            access_manager.grant(
                domain_catalog,
                steward_principal,
                [
                    PrivilegeType.CREATE_SCHEMA,
                    PrivilegeType.CREATE_TABLE,
                    PrivilegeType.MODIFY,
                    PrivilegeType.APPLY_TAG
                ]
            )
            
            # Domain users get read access
            domain_users = Principal(f"{domain_name}_users")
            access_manager.grant(
                domain_catalog,
                domain_users,
                [PrivilegeType.USE_CATALOG, PrivilegeType.SELECT]
            )
```

### 5.2 Schema-Level Strategies (Project/Team Boundaries)

**Business Scenario**: Schemas represent project or team boundaries within a domain.

**Implementation Strategy**:
```python
class ProjectBasedSchemas:
    def setup_project_schemas(self):
        """Create project-based schema organization."""
        
        # Analytics catalog with project-based schemas
        analytics_catalog = Catalog(name="analytics")
        
        projects = [
            {
                "name": "customer_360",
                "team_lead": "john_doe",
                "team_members": ["analyst_1", "analyst_2", "engineer_1"],
                "stakeholders": ["exec_team", "sales_managers"],
                "tables": ["customer_profile", "customer_behavior", "customer_value"]
            },
            {
                "name": "revenue_optimization",
                "team_lead": "jane_smith",
                "team_members": ["analyst_3", "analyst_4", "data_scientist_1"],
                "stakeholders": ["finance_team", "exec_team"],
                "tables": ["revenue_drivers", "pricing_analysis", "churn_prediction"]
            }
        ]
        
        for project in projects:
            # Create project schema
            project_schema = Schema(
                name=project["name"],
                owner=project["team_lead"],
                comment=f"Project: {project['name'].replace('_', ' ').title()}",
                tags=[
                    Tag(key="project_type", value="analytics"),
                    Tag(key="team_size", value=str(len(project["team_members"])))
                ]
            )
            analytics_catalog.add_schema(project_schema)
            
            # Team lead gets full control
            lead_principal = Principal(project["team_lead"])
            access_manager.grant(
                project_schema,
                lead_principal,
                [PrivilegeType.MANAGE]
            )
            
            # Team members get read/write
            team_group = Principal(f"{project['name']}_team")
            access_manager.grant(
                project_schema,
                team_group,
                [
                    PrivilegeType.USE_SCHEMA,
                    PrivilegeType.CREATE_TABLE,
                    PrivilegeType.SELECT,
                    PrivilegeType.MODIFY
                ]
            )
            
            # Stakeholders get read-only
            stakeholder_group = Principal(f"{project['name']}_stakeholders")
            access_manager.grant(
                project_schema,
                stakeholder_group,
                [PrivilegeType.USE_SCHEMA, PrivilegeType.SELECT]
            )
```

### 5.3 Table-Level Strategies (Fine-Grained Access)

**Business Scenario**: Implement row-level and column-level security for sensitive tables.

**Implementation Strategy**:
```python
class FineGrainedTableAccess:
    def setup_table_level_security(self):
        """Implement fine-grained table access controls."""
        
        # Customer data with varying sensitivity
        customer_catalog = Catalog(name="customer_data")
        customer_schema = Schema(name="customers")
        customer_catalog.add_schema(customer_schema)
        
        # Different views for different access levels
        table_variants = [
            {
                "name": "customers_full",
                "description": "Complete customer data with PII",
                "access_level": "restricted",
                "allowed_roles": ["privacy_team", "customer_service_managers"]
            },
            {
                "name": "customers_masked",
                "description": "Customer data with masked PII",
                "access_level": "internal",
                "allowed_roles": ["customer_service", "analytics_team"]
            },
            {
                "name": "customers_aggregated",
                "description": "Aggregated customer metrics",
                "access_level": "general",
                "allowed_roles": ["all_employees"]
            }
        ]
        
        for variant in table_variants:
            # Create table/view with specific access
            table = Table(
                name=variant["name"],
                table_type=TableType.VIEW if "aggregated" in variant["name"] else TableType.TABLE,
                comment=variant["description"],
                tags=[
                    Tag(key="access_level", value=variant["access_level"]),
                    Tag(key="contains_pii", value=str(variant["access_level"] == "restricted"))
                ]
            )
            customer_schema.add_table(table)
            
            # Grant specific access per table
            for role in variant["allowed_roles"]:
                principal = Principal(role)
                
                # More restrictive tables get fewer privileges
                if variant["access_level"] == "restricted":
                    privileges = [PrivilegeType.SELECT]  # Read only
                elif variant["access_level"] == "internal":
                    privileges = [PrivilegeType.SELECT, PrivilegeType.MODIFY]
                else:  # general
                    privileges = [PrivilegeType.SELECT]
                
                # Apply grants at table level
                access_manager.grant(table, principal, privileges)
        
        # Row-level security via dynamic views
        self.create_row_level_security_views(customer_schema)
    
    def create_row_level_security_views(self, schema):
        """Create views with row-level security."""
        
        # Regional data access - users only see their region
        regional_view = Table(
            name="customers_by_region",
            table_type=TableType.VIEW,
            comment="Customers filtered by user's region",
            tags=[
                Tag(key="row_filter", value="current_user_region()"),
                Tag(key="security_type", value="row_level")
            ]
        )
        schema.add_table(regional_view)
        
        # Time-based access - recent data only for most users
        recent_data_view = Table(
            name="customers_recent",
            table_type=TableType.VIEW,
            comment="Last 90 days of customer data",
            tags=[
                Tag(key="time_filter", value="last_90_days"),
                Tag(key="security_type", value="temporal")
            ]
        )
        schema.add_table(recent_data_view)
```

---

## 6. Data Mesh Principles

### 6.1 Domain Ownership Patterns

**Business Scenario**: Implement data mesh with decentralized domain ownership.

**Implementation Strategy**:
```python
class DataMeshGovernance:
    def implement_data_mesh(self):
        """Full data mesh implementation with federated governance."""
        
        # Central governance catalog for shared resources
        platform_catalog = Catalog(
            name="data_platform",
            comment="Shared platform capabilities",
            owner="platform_team"
        )
        
        # Add platform schemas
        platform_schemas = [
            Schema(name="quality_metrics", comment="Data quality scores"),
            Schema(name="lineage", comment="Data lineage tracking"),
            Schema(name="catalog_registry", comment="Data product registry")
        ]
        
        for schema in platform_schemas:
            platform_catalog.add_schema(schema)
        
        # Domain data products
        domains = ["sales", "marketing", "supply_chain", "customer_service"]
        
        for domain in domains:
            # Each domain gets its own catalog
            domain_catalog = Catalog(
                name=f"{domain}_products",
                comment=f"{domain.title()} data products",
                owner=f"{domain}_product_owner",
                tags=[
                    Tag(key="mesh_domain", value=domain),
                    Tag(key="self_serve", value="true")
                ]
            )
            
            # Standard data product structure
            self.create_data_product_schemas(domain_catalog, domain)
            
            # Domain team has full autonomy
            domain_team = Principal(f"{domain}_data_team")
            access_manager.grant(
                domain_catalog,
                domain_team,
                [PrivilegeType.MANAGE]
            )
            
            # Register in central catalog
            self.register_data_product(platform_catalog, domain_catalog)
    
    def create_data_product_schemas(self, catalog, domain):
        """Standard schemas for data products."""
        
        # Input ports (how data enters the domain)
        input_schema = Schema(
            name="input_ports",
            comment="Standardized data ingestion interfaces",
            tags=[Tag(key="port_type", value="input")]
        )
        catalog.add_schema(input_schema)
        
        # Core domain data
        core_schema = Schema(
            name="core",
            comment="Core domain entities and logic",
            tags=[Tag(key="domain_core", value="true")]
        )
        catalog.add_schema(core_schema)
        
        # Output ports (data products for consumption)
        output_schema = Schema(
            name="output_ports",
            comment="Published data products",
            tags=[Tag(key="port_type", value="output")]
        )
        catalog.add_schema(output_schema)
        
        # SLOs and metadata
        metadata_schema = Schema(
            name="product_metadata",
            comment="SLOs, schemas, and documentation",
            tags=[Tag(key="self_describing", value="true")]
        )
        catalog.add_schema(metadata_schema)
```

### 6.2 Federated Governance Models

**Business Scenario**: Balance domain autonomy with enterprise standards.

**Implementation Strategy**:
```python
class FederatedGovernance:
    def setup_federated_model(self):
        """Implement federated governance with central standards."""
        
        # Central governance standards
        governance_standards = {
            "naming_conventions": {
                "catalog": "^[a-z]+(_[a-z]+)*$",
                "schema": "^[a-z]+(_[a-z]+)*$",
                "table": "^(dim|fact|stg|int|rpt)_[a-z]+(_[a-z]+)*$"
            },
            "required_tags": [
                "data_owner",
                "data_steward",
                "update_frequency",
                "retention_period"
            ],
            "quality_thresholds": {
                "completeness": 0.95,
                "accuracy": 0.99,
                "timeliness_hours": 24
            }
        }
        
        # Governance committee principals
        governance_committee = [
            Principal("chief_data_officer"),
            Principal("enterprise_architect"),
            Principal("compliance_officer"),
            Principal("security_officer")
        ]
        
        # Domain governance representatives
        domain_representatives = {}
        for domain in ["sales", "marketing", "operations", "finance"]:
            domain_representatives[domain] = Principal(f"{domain}_data_steward")
        
        # Central governance catalog
        governance_catalog = Catalog(
            name="governance",
            comment="Enterprise governance and standards",
            owner="chief_data_officer"
        )
        
        # Standards enforcement schema
        standards_schema = Schema(
            name="standards",
            comment="Governance standards and policies"
        )
        governance_catalog.add_schema(standards_schema)
        
        # Domain compliance tracking
        compliance_schema = Schema(
            name="compliance_tracking",
            comment="Domain compliance metrics"
        )
        governance_catalog.add_schema(compliance_schema)
        
        # Committee has full control of standards
        for committee_member in governance_committee:
            access_manager.grant(
                standards_schema,
                committee_member,
                [PrivilegeType.MANAGE]
            )
        
        # Domain reps can read standards, update compliance
        for domain, rep in domain_representatives.items():
            # Read standards
            access_manager.grant(
                standards_schema,
                rep,
                [PrivilegeType.SELECT]
            )
            
            # Update their compliance status
            access_manager.grant(
                compliance_schema,
                rep,
                [PrivilegeType.SELECT, PrivilegeType.MODIFY]
            )
```

### 6.3 Data Product Thinking

**Business Scenario**: Treat data as products with clear interfaces, SLOs, and ownership.

**Implementation Strategy**:
```python
class DataProductGovernance:
    def create_data_product(
        self,
        product_name: str,
        domain: str,
        owner: str,
        consumers: List[str],
        slos: Dict[str, Any]
    ):
        """Create a complete data product with governance."""
        
        # Data product catalog
        product_catalog = Catalog(
            name=f"product_{product_name}",
            comment=f"Data Product: {product_name}",
            owner=owner,
            tags=[
                Tag(key="product_name", value=product_name),
                Tag(key="product_domain", value=domain),
                Tag(key="product_status", value="active"),
                Tag(key="product_version", value="1.0.0")
            ]
        )
        
        # Interface schema (contracts)
        interface_schema = Schema(
            name="interface",
            comment="Product API and contracts",
            tags=[
                Tag(key="contract_version", value="1.0.0"),
                Tag(key="breaking_changes", value="false")
            ]
        )
        product_catalog.add_schema(interface_schema)
        
        # Add interface tables
        input_contract = Table(
            name="input_contract",
            comment="Expected input format",
            table_type=TableType.VIEW
        )
        interface_schema.add_table(input_contract)
        
        output_contract = Table(
            name="output_contract",
            comment="Guaranteed output format",
            table_type=TableType.VIEW
        )
        interface_schema.add_table(output_contract)
        
        # Implementation schema (internal)
        implementation_schema = Schema(
            name="implementation",
            comment="Internal product implementation",
            tags=[Tag(key="internal", value="true")]
        )
        product_catalog.add_schema(implementation_schema)
        
        # Quality schema
        quality_schema = Schema(
            name="quality",
            comment="Data quality metrics and monitoring",
            tags=[
                Tag(key="slo_completeness", value=str(slos.get("completeness", 0.99))),
                Tag(key="slo_freshness", value=str(slos.get("freshness_hours", 1))),
                Tag(key="slo_availability", value=str(slos.get("availability", 0.999)))
            ]
        )
        product_catalog.add_schema(quality_schema)
        
        # Product owner has full control
        owner_principal = Principal(owner)
        access_manager.grant(
            product_catalog,
            owner_principal,
            [PrivilegeType.MANAGE]
        )
        
        # Consumers get interface access only
        for consumer in consumers:
            consumer_principal = Principal(consumer)
            access_manager.grant(
                interface_schema,
                consumer_principal,
                [PrivilegeType.USE_SCHEMA, PrivilegeType.SELECT]
            )
            
            # Can read quality metrics
            access_manager.grant(
                quality_schema,
                consumer_principal,
                [PrivilegeType.SELECT]
            )
        
        # Hide implementation from consumers
        # (They can't access implementation_schema)
        
        return product_catalog
```

---

## 7. Audit and Compliance Strategies

### 7.1 Access Review Patterns

**Business Scenario**: Regular access reviews to ensure least privilege and remove stale access.

**Implementation Strategy**:
```python
from datetime import datetime, timedelta
import json

class AccessReviewGovernance:
    def implement_access_reviews(self):
        """Implement periodic access review process."""
        
        # Audit catalog for governance
        audit_catalog = Catalog(
            name="governance_audit",
            comment="Access reviews and audit trails",
            owner="security_team"
        )
        
        # Access review tracking
        review_schema = Schema(
            name="access_reviews",
            comment="Periodic access review records"
        )
        audit_catalog.add_schema(review_schema)
        
        # Current access snapshot
        access_snapshot = Table(
            name="access_snapshot",
            comment=f"Access snapshot as of {datetime.now().isoformat()}",
            table_type=TableType.TABLE,
            tags=[
                Tag(key="snapshot_date", value=datetime.now().isoformat()),
                Tag(key="review_cycle", value="quarterly")
            ]
        )
        review_schema.add_table(access_snapshot)
        
        # Review decisions table
        review_decisions = Table(
            name="review_decisions",
            comment="Manager review decisions on access",
            table_type=TableType.TABLE,
            columns=[
                ColumnInfo(name="review_id", type_name="STRING"),
                ColumnInfo(name="principal", type_name="STRING"),
                ColumnInfo(name="resource", type_name="STRING"),
                ColumnInfo(name="current_privileges", type_name="STRING"),
                ColumnInfo(name="decision", type_name="STRING"),  # approve/revoke/modify
                ColumnInfo(name="reviewer", type_name="STRING"),
                ColumnInfo(name="review_date", type_name="TIMESTAMP"),
                ColumnInfo(name="justification", type_name="STRING")
            ]
        )
        review_schema.add_table(review_decisions)
        
        # Stale access detection
        stale_access = Table(
            name="stale_access_report",
            comment="Unused access rights (no activity in 90 days)",
            table_type=TableType.VIEW,
            tags=[
                Tag(key="detection_threshold_days", value="90"),
                Tag(key="auto_revoke", value="false")  # Manual review required
            ]
        )
        review_schema.add_table(stale_access)
    
    def generate_access_review(self, catalog_name: str):
        """Generate access review report for a catalog."""
        
        review_data = {
            "review_id": f"review_{datetime.now().strftime('%Y%m%d')}",
            "catalog": catalog_name,
            "review_date": datetime.now().isoformat(),
            "principals_reviewed": [],
            "access_matrix": {}
        }
        
        # Would integrate with SDK to get actual access
        # This is pseudo-code for the pattern
        
        return review_data
    
    def schedule_review_cycles(self):
        """Setup automated review scheduling."""
        
        review_schedule = {
            "sensitive_data": "monthly",
            "production_data": "quarterly",
            "development_data": "semi_annual",
            "public_data": "annual"
        }
        
        for classification, frequency in review_schedule.items():
            # Create review jobs (pseudo-code)
            pass
```

### 7.2 Privilege Escalation Workflows

**Business Scenario**: Controlled, audited privilege escalation for emergency access.

**Implementation Strategy**:
```python
class PrivilegeEscalation:
    def setup_escalation_workflow(self):
        """Implement break-glass privilege escalation."""
        
        # Emergency access catalog
        emergency_catalog = Catalog(
            name="emergency_access",
            comment="Break-glass emergency access management",
            isolation_mode=IsolationMode.ISOLATED,
            owner="security_team"
        )
        
        # Escalation request tracking
        escalation_schema = Schema(
            name="escalation_requests",
            comment="Privilege escalation audit trail"
        )
        emergency_catalog.add_schema(escalation_schema)
        
        # Request table
        escalation_requests = Table(
            name="escalation_log",
            comment="All privilege escalation requests",
            columns=[
                ColumnInfo(name="request_id", type_name="STRING"),
                ColumnInfo(name="requester", type_name="STRING"),
                ColumnInfo(name="target_resource", type_name="STRING"),
                ColumnInfo(name="requested_privileges", type_name="STRING"),
                ColumnInfo(name="justification", type_name="STRING"),
                ColumnInfo(name="incident_ticket", type_name="STRING"),
                ColumnInfo(name="approval_status", type_name="STRING"),
                ColumnInfo(name="approver", type_name="STRING"),
                ColumnInfo(name="grant_duration_hours", type_name="INT"),
                ColumnInfo(name="grant_start", type_name="TIMESTAMP"),
                ColumnInfo(name="grant_end", type_name="TIMESTAMP"),
                ColumnInfo(name="auto_revoked", type_name="BOOLEAN")
            ]
        )
        escalation_schema.add_table(escalation_requests)
        
        # Break-glass accounts with time-limited access
        self.create_break_glass_account()
    
    def create_break_glass_account(self):
        """Create break-glass emergency account."""
        
        # Emergency account with no default access
        emergency_principal = Principal(
            name="break_glass_emergency",
            add_environment_suffix=False,  # Same across environments
            environment_mapping={
                Environment.DEV: "break_glass_dev",
                Environment.ACC: "break_glass_acc",
                Environment.PRD: "break_glass_prd"
            }
        )
        
        # Activation requires multi-party approval
        activation_requirements = {
            "min_approvers": 2,
            "approver_roles": ["security_team", "operations_manager"],
            "max_duration_hours": 4,
            "require_incident": True,
            "auto_revoke": True,
            "alert_channels": ["security_slack", "ops_pager"]
        }
        
        return emergency_principal, activation_requirements
    
    def grant_emergency_access(
        self,
        requester: str,
        resource,
        duration_hours: int,
        incident_id: str
    ):
        """Grant time-limited emergency access."""
        
        # Create temporary principal
        temp_principal = Principal(
            name=f"emergency_{requester}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            add_environment_suffix=False
        )
        
        # Grant with automatic expiry
        expiry_time = datetime.now() + timedelta(hours=duration_hours)
        
        # Log the grant
        audit_entry = {
            "timestamp": datetime.now().isoformat(),
            "requester": requester,
            "principal": temp_principal.name,
            "resource": resource.fqdn if hasattr(resource, 'fqdn') else str(resource),
            "duration_hours": duration_hours,
            "incident_id": incident_id,
            "expiry": expiry_time.isoformat()
        }
        
        # Apply emergency access
        access_manager.grant(
            resource,
            temp_principal,
            [PrivilegeType.SELECT, PrivilegeType.MODIFY]  # Limited to data access
        )
        
        # Schedule automatic revocation
        # self.schedule_revocation(temp_principal, expiry_time)
        
        return temp_principal, audit_entry
```

### 7.3 Compliance Validation Approaches

**Business Scenario**: Automated compliance validation against policies and regulations.

**Implementation Strategy**:
```python
class ComplianceValidation:
    def setup_compliance_validation(self):
        """Implement automated compliance checking."""
        
        # Compliance validation catalog
        compliance_catalog = Catalog(
            name="compliance_validation",
            comment="Automated compliance checking and reporting",
            owner="compliance_officer"
        )
        
        # Validation rules schema
        rules_schema = Schema(
            name="validation_rules",
            comment="Compliance rules and policies"
        )
        compliance_catalog.add_schema(rules_schema)
        
        # Define compliance rules
        compliance_rules = {
            "gdpr": {
                "pii_encryption": "All PII must be encrypted at rest",
                "retention_limit": "Personal data retained max 7 years",
                "purpose_limitation": "Data used only for stated purpose",
                "access_logging": "All PII access must be logged"
            },
            "sox": {
                "segregation_of_duties": "No single person has full financial access",
                "audit_trail": "Complete audit trail for financial data",
                "access_certification": "Quarterly access certification required"
            },
            "hipaa": {
                "minimum_necessary": "Access limited to minimum necessary",
                "phi_encryption": "PHI encrypted in transit and at rest",
                "access_controls": "Role-based access to PHI"
            }
        }
        
        # Validation results schema
        results_schema = Schema(
            name="validation_results",
            comment="Compliance validation outcomes"
        )
        compliance_catalog.add_schema(results_schema)
        
        # Create validation result tables
        self.create_validation_tables(results_schema, compliance_rules)
    
    def create_validation_tables(self, schema, rules):
        """Create tables for tracking validation results."""
        
        # Overall compliance score
        compliance_score = Table(
            name="compliance_scorecard",
            comment="Overall compliance metrics",
            columns=[
                ColumnInfo(name="validation_date", type_name="DATE"),
                ColumnInfo(name="regulation", type_name="STRING"),
                ColumnInfo(name="catalog", type_name="STRING"),
                ColumnInfo(name="score", type_name="DECIMAL(3,2)"),
                ColumnInfo(name="passed_rules", type_name="INT"),
                ColumnInfo(name="failed_rules", type_name="INT"),
                ColumnInfo(name="warnings", type_name="INT")
            ]
        )
        schema.add_table(compliance_score)
        
        # Detailed violations
        violations = Table(
            name="compliance_violations",
            comment="Specific compliance violations found",
            columns=[
                ColumnInfo(name="violation_id", type_name="STRING"),
                ColumnInfo(name="detection_date", type_name="TIMESTAMP"),
                ColumnInfo(name="regulation", type_name="STRING"),
                ColumnInfo(name="rule_name", type_name="STRING"),
                ColumnInfo(name="resource_type", type_name="STRING"),
                ColumnInfo(name="resource_name", type_name="STRING"),
                ColumnInfo(name="violation_details", type_name="STRING"),
                ColumnInfo(name="severity", type_name="STRING"),
                ColumnInfo(name="remediation_status", type_name="STRING"),
                ColumnInfo(name="assigned_to", type_name="STRING")
            ]
        )
        schema.add_table(violations)
    
    def run_compliance_check(self, catalog, regulation: str):
        """Execute compliance validation for a catalog."""
        
        validation_results = {
            "catalog": catalog.name,
            "regulation": regulation,
            "timestamp": datetime.now().isoformat(),
            "checks": [],
            "violations": []
        }
        
        # Check for required tags
        required_tags = {
            "gdpr": ["data_classification", "retention_period", "legal_basis"],
            "sox": ["financial_impact", "audit_required", "sox_relevant"],
            "hipaa": ["phi_present", "encryption_status", "access_logged"]
        }
        
        for tag_key in required_tags.get(regulation, []):
            has_tag = any(tag.key == tag_key for tag in catalog.tags)
            validation_results["checks"].append({
                "rule": f"required_tag_{tag_key}",
                "passed": has_tag,
                "message": f"Tag '{tag_key}' {'present' if has_tag else 'missing'}"
            })
            
            if not has_tag:
                validation_results["violations"].append({
                    "rule": f"required_tag_{tag_key}",
                    "severity": "medium",
                    "resource": catalog.name,
                    "details": f"Missing required tag: {tag_key}"
                })
        
        # Check access patterns
        # (Would integrate with actual access data from SDK)
        
        return validation_results
```

---

## 8. Advanced Governance Patterns

### 8.1 Data Quality-Based Access

**Business Scenario**: Grant different access levels based on data quality scores.

**Implementation Strategy**:
```python
class DataQualityGovernance:
    def setup_quality_based_access(self):
        """Implement access control based on data quality."""
        
        # Quality tiers
        quality_tiers = {
            "gold": {"min_score": 0.95, "access": "unrestricted"},
            "silver": {"min_score": 0.80, "access": "limited"},
            "bronze": {"min_score": 0.60, "access": "restricted"},
            "quarantine": {"min_score": 0.0, "access": "blocked"}
        }
        
        # Create quality-tiered catalog
        quality_catalog = Catalog(
            name="quality_tiered_data",
            comment="Data with quality-based access control"
        )
        
        for tier, config in quality_tiers.items():
            tier_schema = Schema(
                name=f"{tier}_tier",
                comment=f"Data with {tier} quality rating",
                tags=[
                    Tag(key="quality_tier", value=tier),
                    Tag(key="min_quality_score", value=str(config["min_score"])),
                    Tag(key="access_level", value=config["access"])
                ]
            )
            quality_catalog.add_schema(tier_schema)
            
            # Grant access based on tier
            if tier == "gold":
                # Everyone can access gold tier data
                access_manager.grant(
                    tier_schema,
                    Principal("all_users"),
                    [PrivilegeType.SELECT]
                )
            elif tier == "silver":
                # Only verified users access silver
                access_manager.grant(
                    tier_schema,
                    Principal("verified_users"),
                    [PrivilegeType.SELECT]
                )
            elif tier == "bronze":
                # Only data team accesses bronze
                access_manager.grant(
                    tier_schema,
                    Principal("data_team"),
                    [PrivilegeType.SELECT]
                )
            # Quarantine tier gets no grants
```

### 8.2 Cost-Aware Governance

**Business Scenario**: Implement governance that considers compute and storage costs.

**Implementation Strategy**:
```python
class CostAwareGovernance:
    def setup_cost_governance(self):
        """Implement cost-aware access patterns."""
        
        # Cost-tiered storage
        storage_tiers = {
            "hot": {
                "storage_type": "SSD",
                "cost_per_gb": 0.30,
                "access_frequency": "hourly",
                "allowed_workloads": ["real_time", "operational"]
            },
            "warm": {
                "storage_type": "HDD",
                "cost_per_gb": 0.10,
                "access_frequency": "daily",
                "allowed_workloads": ["analytics", "reporting"]
            },
            "cold": {
                "storage_type": "ARCHIVE",
                "cost_per_gb": 0.01,
                "access_frequency": "monthly",
                "allowed_workloads": ["compliance", "backup"]
            }
        }
        
        cost_catalog = Catalog(
            name="cost_optimized",
            comment="Cost-optimized data storage"
        )
        
        for tier, config in storage_tiers.items():
            tier_schema = Schema(
                name=f"{tier}_storage",
                comment=f"{tier.title()} tier storage",
                tags=[
                    Tag(key="storage_tier", value=tier),
                    Tag(key="cost_per_gb", value=str(config["cost_per_gb"])),
                    Tag(key="sla_access_time", value=config["access_frequency"])
                ]
            )
            cost_catalog.add_schema(tier_schema)
            
            # Grant based on workload type
            for workload in config["allowed_workloads"]:
                workload_principal = Principal(f"{workload}_workload")
                
                # Hot tier gets immediate access
                if tier == "hot":
                    privileges = [PrivilegeType.SELECT, PrivilegeType.MODIFY]
                # Warm tier has read with potential delays
                elif tier == "warm":
                    privileges = [PrivilegeType.SELECT]
                # Cold tier requires request and warming
                else:
                    privileges = [PrivilegeType.BROWSE]  # Metadata only
                
                access_manager.grant(
                    tier_schema,
                    workload_principal,
                    privileges
                )
```

### 8.3 Multi-Region Governance

**Business Scenario**: Governance across multiple regions with data residency requirements.

**Implementation Strategy**:
```python
class MultiRegionGovernance:
    def setup_regional_governance(self):
        """Implement multi-region governance with data residency."""
        
        regions = {
            "us_east": {
                "location": "us-east-1",
                "regulations": ["sox", "ccpa"],
                "allowed_countries": ["US", "CA"]
            },
            "eu_west": {
                "location": "eu-west-1",
                "regulations": ["gdpr"],
                "allowed_countries": ["DE", "FR", "UK", "NL"]
            },
            "asia_pac": {
                "location": "ap-southeast-1",
                "regulations": ["pdpa"],
                "allowed_countries": ["SG", "JP", "AU"]
            }
        }
        
        for region_name, config in regions.items():
            # Regional catalog
            regional_catalog = Catalog(
                name=f"data_{region_name}",
                comment=f"Data residing in {config['location']}",
                tags=[
                    Tag(key="region", value=config["location"]),
                    Tag(key="data_residency", value="enforced"),
                    Tag(key="regulations", value=",".join(config["regulations"]))
                ]
            )
            
            # Regional access principal
            regional_users = Principal(
                name=f"users_{region_name}",
                add_environment_suffix=True
            )
            
            # Grant regional access
            access_manager.grant(
                regional_catalog,
                regional_users,
                [PrivilegeType.USE_CATALOG, PrivilegeType.SELECT]
            )
            
            # Cross-region replication schema (if allowed)
            if "gdpr" not in config["regulations"]:  # GDPR restricts transfers
                replication_schema = Schema(
                    name="cross_region_replicated",
                    comment="Data replicated from other regions",
                    tags=[Tag(key="replicated", value="true")]
                )
                regional_catalog.add_schema(replication_schema)
```

---

## Implementation Best Practices

### 1. Start with Classification
- Begin by classifying all data assets
- Use tags extensively for metadata
- Implement automated classification where possible

### 2. Layer Security Controls
- Use defense in depth approach
- Combine catalog, schema, and table-level controls
- Implement both preventive and detective controls

### 3. Automate Governance
- Use the SDK for automated provisioning
- Implement CI/CD for governance changes
- Create self-service capabilities with guardrails

### 4. Monitor and Audit
- Log all access attempts
- Regular access reviews
- Automated compliance scanning

### 5. Enable Self-Service
- Provide clear documentation
- Create request workflows
- Implement approval automation

### 6. Plan for Scale
- Design for thousands of users
- Implement efficient permission inheritance
- Use groups and roles over individual grants

### 7. Handle Exceptions
- Design break-glass procedures
- Implement time-bound access
- Create escalation workflows

### 8. Maintain Agility
- Regular review cycles
- Feedback mechanisms
- Continuous improvement process

---

---

## 9. Machine Learning Model Governance

### 9.1 Model Registry Governance with MLflow Integration

**Business Scenario**: Govern ML models throughout their lifecycle from experimentation to production deployment.

**Implementation Strategy**:
```python
from enum import Enum
from typing import Optional, List, Dict, Any
from datetime import datetime

class ModelStage(str, Enum):
    """MLflow model stages aligned with environment progression."""
    NONE = "None"  # Initial stage
    EXPERIMENTAL = "Experimental"  # DEV environment
    STAGING = "Staging"  # ACC environment  
    PRODUCTION = "Production"  # PRD environment
    ARCHIVED = "Archived"  # Decommissioned

class ModelTier(str, Enum):
    """Model criticality classification."""
    TIER_1 = "TIER_1"  # Business critical, requires approval
    TIER_2 = "TIER_2"  # Important, standard review
    TIER_3 = "TIER_3"  # Experimental, minimal governance

class MLModelGovernance:
    def setup_model_registry(self):
        """Implement comprehensive model governance."""
        
        # ML-specific catalog structure
        ml_catalog = Catalog(
            name="ml_platform",
            comment="Machine Learning platform catalog",
            isolation_mode=IsolationMode.OPEN,
            tags=[
                Tag(key="platform", value="mlflow"),
                Tag(key="version", value="3.3+")
            ]
        )
        
        # Environment-aligned schemas
        ml_schemas = {
            "experiments": "Development experiments and prototypes",
            "staging": "Models undergoing validation",
            "production": "Production-ready models",
            "archived": "Decommissioned models"
        }
        
        for schema_name, description in ml_schemas.items():
            schema = Schema(
                name=schema_name,
                comment=description,
                tags=[
                    Tag(key="ml_stage", value=schema_name),
                    Tag(key="audit_enabled", value="true")
                ]
            )
            ml_catalog.add_schema(schema)
        
        # Role-based ML access
        self.setup_ml_roles(ml_catalog)
    
    def setup_ml_roles(self, catalog):
        """Define ML-specific access roles."""
        
        # Data Scientists - experiment and develop
        ml_developers = Principal("ml_developers")
        access_manager.grant(
            catalog.get_schema("experiments"),
            ml_developers,
            [
                PrivilegeType.USE_SCHEMA,
                PrivilegeType.CREATE_MODEL,
                PrivilegeType.CREATE_TABLE,  # For feature engineering
                PrivilegeType.MODIFY
            ]
        )
        
        # ML Engineers - manage lifecycle
        ml_engineers = Principal("ml_engineers")
        access_manager.grant(
            catalog.get_schema("staging"),
            ml_engineers,
            [
                PrivilegeType.USE_SCHEMA,
                PrivilegeType.CREATE_MODEL,
                PrivilegeType.MODIFY,
                PrivilegeType.EXECUTE  # Test inference
            ]
        )
        
        # MLOps Team - production deployment
        mlops_team = Principal("mlops_team")
        access_manager.grant(
            catalog.get_schema("production"),
            mlops_team,
            [PrivilegeType.ALL_PRIVILEGES]  # Full control in production
        )
        
        # Model Consumers - inference only
        model_consumers = Principal("model_api_services")
        access_manager.grant(
            catalog.get_schema("production"),
            model_consumers,
            [PrivilegeType.EXECUTE]  # Invoke models only
        )
```

### 9.2 Model Lineage and Data Dependency Tracking

**Business Scenario**: Track which datasets were used to train models and ensure compliance.

**Implementation Strategy**:
```python
class ModelLineageGovernance:
    def track_model_lineage(self):
        """Comprehensive lineage tracking for models."""
        
        # Lineage tracking schema
        lineage_schema = Schema(
            name="model_lineage",
            comment="Model training data dependencies",
            tags=[
                Tag(key="retention_days", value="365"),
                Tag(key="compliance_required", value="true")
            ]
        )
        
        # Training dataset tracking table
        training_datasets = Table(
            name="model_training_datasets",
            comment="Datasets used for model training",
            columns=[
                ColumnInfo(name="model_name", type_name="STRING"),
                ColumnInfo(name="model_version", type_name="INT"),
                ColumnInfo(name="dataset_name", type_name="STRING"),
                ColumnInfo(name="dataset_version", type_name="STRING"),
                ColumnInfo(name="data_classification", type_name="STRING"),
                ColumnInfo(name="row_count", type_name="BIGINT"),
                ColumnInfo(name="training_date", type_name="TIMESTAMP")
            ]
        )
        lineage_schema.add_table(training_datasets)
        
        # Feature store dependencies
        feature_dependencies = Table(
            name="model_feature_dependencies",
            comment="Feature store tables used by models",
            columns=[
                ColumnInfo(name="model_name", type_name="STRING"),
                ColumnInfo(name="feature_table", type_name="STRING"),
                ColumnInfo(name="features_used", type_name="ARRAY<STRING>"),
                ColumnInfo(name="point_in_time_correct", type_name="BOOLEAN")
            ]
        )
        lineage_schema.add_table(feature_dependencies)
        
        # Audit access to lineage data
        access_manager.grant(
            lineage_schema,
            Principal("compliance_team"),
            [PrivilegeType.SELECT]  # Read-only for compliance
        )
```

### 9.3 Model Deployment Permission Strategy

**Business Scenario**: Control who can deploy models to different environments based on model tier.

**Implementation Strategy**:
```python
class ModelDeploymentGovernance:
    def setup_deployment_permissions(self):
        """Environment-aligned deployment permissions."""
        
        deployment_policies = {
            Environment.DEV: {
                ModelTier.TIER_1: ["mlops_team"],  # Even in dev, Tier 1 restricted
                ModelTier.TIER_2: ["ml_engineers", "mlops_team"],
                ModelTier.TIER_3: ["ml_developers", "ml_engineers", "mlops_team"]
            },
            Environment.ACC: {
                ModelTier.TIER_1: ["mlops_team"],  # Only MLOps
                ModelTier.TIER_2: ["ml_engineers", "mlops_team"],
                ModelTier.TIER_3: ["ml_engineers", "mlops_team"]
            },
            Environment.PRD: {
                ModelTier.TIER_1: ["mlops_team"],  # Requires approval workflow
                ModelTier.TIER_2: ["mlops_team"],
                ModelTier.TIER_3: ["mlops_team"]
            }
        }
        
        # Model serving endpoints governance
        serving_catalog = Catalog(name="model_serving")
        
        for env in Environment:
            env_schema = Schema(
                name=f"endpoints_{env.value}",
                comment=f"Model serving endpoints for {env.value}"
            )
            serving_catalog.add_schema(env_schema)
            
            # Apply environment-specific permissions
            for tier, allowed_roles in deployment_policies[env].items():
                for role in allowed_roles:
                    principal = Principal(role)
                    
                    # Grant deployment permissions
                    if env == Environment.PRD and tier == ModelTier.TIER_1:
                        # Production Tier 1 requires approval
                        privileges = [PrivilegeType.USE_SCHEMA]  # View only
                    else:
                        privileges = [
                            PrivilegeType.USE_SCHEMA,
                            PrivilegeType.CREATE_FUNCTION,  # Create endpoints
                            PrivilegeType.EXECUTE  # Invoke endpoints
                        ]
                    
                    access_manager.grant(env_schema, principal, privileges)
```

### 9.4 Model Compliance and Monitoring Access

**Business Scenario**: Implement compliance requirements for models handling sensitive data.

**Implementation Strategy**:
```python
class ModelComplianceGovernance:
    def setup_model_compliance(self):
        """Compliance and monitoring for ML models."""
        
        # Compliance tracking catalog
        compliance_catalog = Catalog(
            name="ml_compliance",
            comment="ML model compliance and monitoring",
            isolation_mode=IsolationMode.ISOLATED  # Restricted access
        )
        
        # Model audit schema
        audit_schema = Schema(
            name="model_audits",
            comment="Model access and prediction audits"
        )
        compliance_catalog.add_schema(audit_schema)
        
        # Prediction audit table (for PII models)
        prediction_audit = Table(
            name="prediction_logs",
            comment="Audit log for model predictions on PII data",
            columns=[
                ColumnInfo(name="prediction_id", type_name="STRING"),
                ColumnInfo(name="model_name", type_name="STRING"),
                ColumnInfo(name="model_version", type_name="INT"),
                ColumnInfo(name="principal", type_name="STRING"),
                ColumnInfo(name="timestamp", type_name="TIMESTAMP"),
                ColumnInfo(name="input_contains_pii", type_name="BOOLEAN"),
                ColumnInfo(name="output_contains_pii", type_name="BOOLEAN"),
                ColumnInfo(name="latency_ms", type_name="INT")
            ],
            tags=[
                Tag(key="retention_days", value="180"),  # 6 months retention
                Tag(key="encryption", value="required")
            ]
        )
        audit_schema.add_table(prediction_audit)
        
        # Performance monitoring table
        performance_monitoring = Table(
            name="model_performance",
            comment="Model performance metrics",
            columns=[
                ColumnInfo(name="model_name", type_name="STRING"),
                ColumnInfo(name="metric_name", type_name="STRING"),
                ColumnInfo(name="metric_value", type_name="DOUBLE"),
                ColumnInfo(name="timestamp", type_name="TIMESTAMP")
            ]
        )
        audit_schema.add_table(performance_monitoring)
        
        # Monitoring access patterns
        # ML developers see their own models
        access_manager.grant(
            performance_monitoring,
            Principal("ml_developers"),
            [PrivilegeType.SELECT],
            conditions=["model_owner = current_user()"]
        )
        
        # MLOps team sees everything
        access_manager.grant(
            audit_schema,
            Principal("mlops_team"),
            [PrivilegeType.SELECT, PrivilegeType.MODIFY]
        )
        
        # Compliance team has audit access
        access_manager.grant(
            audit_schema,
            Principal("compliance_team"),
            [PrivilegeType.SELECT]
        )
```

---

## 10. Databricks Asset Bundles (DAB) Integration

### 10.1 DAB-Based Governance Deployment

**Business Scenario**: Use DABs for declarative governance deployment across environments.

**Implementation Strategy**:
```yaml
# bundle.yml - Main governance bundle configuration
bundle:
  name: unified_governance
  
  # Governance metadata
  annotations:
    governance_version: "2.0"
    managed_by: "dbrcdk"
    compliance_frameworks: ["gdpr", "sox", "hipaa"]

variables:
  # Environment-specific variables
  environment:
    description: "Target environment"
    type: string
    enum: ["dev", "acc", "prd"]
  
  data_classification:
    description: "Data classification level"
    type: string
    enum: ["public", "internal", "confidential", "restricted"]

# Include team-specific governance
include:
  - teams/*/governance.yml
  - policies/${var.data_classification}_policy.yml

# Environment-specific targets
targets:
  dev:
    variables:
      environment: "dev"
      max_privileges: ["ALL_PRIVILEGES"]
    
    resources:
      catalogs:
        ${var.catalog_name}_dev:
          isolation_mode: "OPEN"
  
  prd:
    variables:
      environment: "prd"
      max_privileges: ["SELECT"]
    
    resources:
      catalogs:
        ${var.catalog_name}_prd:
          isolation_mode: "ISOLATED"
          managed_access: true
```

### 10.2 Hybrid SDK-DAB Governance Pattern

**Business Scenario**: Combine Pydantic models for logic with DABs for deployment.

**Implementation Strategy**:
```python
class HybridGovernanceDeployer:
    """Deploy governance using SDK logic and DAB infrastructure."""
    
    def deploy_team_governance(self, team: Team, environment: Environment):
        """Generate and deploy team-specific governance."""
        
        # Step 1: Use Pydantic models for governance logic
        catalog = self.create_team_catalog(team, environment)
        access_policies = self.generate_access_policies(team, catalog)
        
        # Step 2: Generate DAB configuration from models
        dab_config = self.generate_dab_config(catalog, access_policies)
        
        # Step 3: Write DAB configuration
        bundle_path = Path(f"bundles/{team.name}/bundle.yml")
        with open(bundle_path, 'w') as f:
            yaml.dump(dab_config, f)
        
        # Step 4: Deploy using DABs
        self.deploy_with_dabs(bundle_path, environment)
        
        # Step 5: Apply complex permissions with SDK
        self.apply_complex_permissions(catalog, team)
    
    def generate_dab_config(self, catalog, policies):
        """Convert Pydantic models to DAB format."""
        
        dab_resources = {
            "catalogs": {},
            "schemas": {},
            "grants": []
        }
        
        # Convert catalog
        dab_resources["catalogs"][catalog.name] = {
            "name": catalog.name,
            "comment": catalog.comment,
            "isolation_mode": catalog.isolation_mode.value
        }
        
        # Convert schemas and grants
        for schema in catalog.schemas:
            schema_key = f"{catalog.name}_{schema.name}"
            dab_resources["schemas"][schema_key] = {
                "catalog_name": catalog.name,
                "name": schema.name,
                "grants": self.convert_grants_to_dab(schema.grants)
            }
        
        return {
            "bundle": {"name": f"{catalog.name}_governance"},
            "resources": dab_resources
        }
    
    def apply_complex_permissions(self, catalog, team):
        """Apply permissions DABs can't handle."""
        
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        
        # Row-level security (DABs can't do this)
        if catalog.requires_row_level_security:
            self.create_row_filters_with_sdk(w, catalog)
        
        # Temporal access (DABs can't do this)
        if team.has_contractors:
            self.setup_temporal_access_with_sdk(w, team)
        
        # Dynamic permission resolution (DABs can't do this)
        if catalog.has_dynamic_permissions:
            self.resolve_dynamic_permissions_with_sdk(w, catalog)
```

### 10.3 CI/CD Pipeline for Governance

**Business Scenario**: Automated governance deployment with approval workflows.

**Implementation Strategy**:
```yaml
# .github/workflows/governance-deploy.yml
name: Governance Deployment Pipeline

on:
  pull_request:
    paths:
      - 'governance/**'
      - 'bundles/**'

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - name: Generate DAB configs from Pydantic
        run: |
          uv run python -m dbrcdk.governance.generate_bundles \
            --source governance/ \
            --output bundles/generated/
      
      - name: Validate DAB syntax
        run: |
          databricks bundle validate \
            --target ${{ matrix.environment }} \
            -p bundles/generated/
      
      - name: Compliance check
        run: |
          uv run python -m dbrcdk.governance.compliance_check \
            --bundle bundles/generated/ \
            --policies governance/policies/
      
      - name: Security scan
        run: |
          # Check for overly permissive grants
          uv run python -m dbrcdk.governance.security_scan \
            --check-all-privileges \
            --check-public-access
  
  deploy_dev:
    needs: validate
    if: github.ref == 'refs/heads/develop'
    runs-on: ubuntu-latest
    steps:
      - name: Deploy to DEV
        run: |
          databricks bundle deploy \
            --target dev \
            --auto-approve \
            -p bundles/generated/
  
  deploy_prd:
    needs: validate
    if: github.ref == 'refs/heads/main'
    environment: production  # Requires manual approval
    runs-on: ubuntu-latest
    steps:
      - name: Generate production configs
        run: |
          uv run python -m dbrcdk.governance.generate_bundles \
            --source governance/ \
            --output bundles/generated/ \
            --environment prd \
            --strict-mode
      
      - name: Deploy to PRODUCTION
        run: |
          databricks bundle deploy \
            --target prd \
            --compute-id ${{ secrets.PROD_COMPUTE }} \
            -p bundles/generated/
      
      - name: Post-deployment validation
        run: |
          uv run python -m dbrcdk.governance.validate_deployment \
            --environment prd \
            --expected-state governance/expected_state.json
```

### 10.4 DAB Limitations and When to Use SDK

**Business Scenario**: Understanding when DABs aren't sufficient.

**Decision Matrix**:

| Governance Task | Use DABs | Use SDK | Hybrid |
|----------------|----------|---------|--------|
| Create catalogs/schemas | ✅ | | |
| Basic grants (SELECT, MODIFY) | ✅ | | |
| Environment-specific configs | ✅ | | |
| Row-level security | | ✅ | |
| Column masking | | ✅ | |
| Temporal access | | ✅ | |
| Dynamic permissions | | ✅ | |
| Complex privilege propagation | | | ✅ |
| Bulk permission updates | | ✅ | |
| Conditional grants | | ✅ | |
| CI/CD deployment | ✅ | | |
| State management | ✅ | | |
| Rollback capability | ✅ | | |

**Implementation Example**:
```python
class GovernanceOrchestrator:
    """Orchestrate governance using appropriate tools."""
    
    def deploy_complete_governance(self, organization):
        """Deploy using the right tool for each task."""
        
        # DABs for declarative resources
        self.deploy_catalogs_with_dabs(organization.catalogs)
        self.deploy_schemas_with_dabs(organization.schemas)
        self.deploy_basic_grants_with_dabs(organization.basic_grants)
        
        # SDK for complex logic
        self.setup_row_security_with_sdk(organization.rls_policies)
        self.setup_temporal_access_with_sdk(organization.temp_access)
        self.setup_dynamic_permissions_with_sdk(organization.dynamic_rules)
        
        # Hybrid for environment progression
        for env in [Environment.DEV, Environment.ACC, Environment.PRD]:
            # Generate environment-specific DAB configs
            dab_config = self.generate_env_config(organization, env)
            
            # Deploy with DABs
            self.deploy_with_dabs(dab_config, env)
            
            # Apply environment-specific SDK logic
            self.apply_env_specific_logic(organization, env)
```

---

## 11. Unity Catalog Advanced Patterns

### 11.1 Metastore-Level Governance

**Business Scenario**: Enterprise-wide governance at the metastore level.

**Implementation Strategy**:
```python
class MetastoreGovernance:
    """Enterprise metastore governance patterns."""
    
    def setup_regional_metastore(self, region: str):
        """Best practice: One metastore per region."""
        
        metastore = Metastore(
            name=f"enterprise_{region}",
            storage_root=f"s3://company-uc-{region}/",
            region=region,
            owner="metastore_admin_group",  # Group ownership recommended
            tags=[
                Tag(key="region", value=region),
                Tag(key="compliance_zone", value=self.get_compliance_zone(region)),
                Tag(key="data_residency", value="enforced")
            ]
        )
        
        # Configure metastore defaults
        metastore.default_isolation_mode = IsolationMode.ISOLATED
        metastore.audit_log_path = f"s3://company-audit-{region}/uc-logs/"
        metastore.delta_sharing_enabled = True
        
        # Workspace bindings for isolation
        metastore.workspace_bindings = [
            WorkspaceBinding(
                workspace_id=f"prod-{region}-ws",
                binding_type="PRIMARY"
            ),
            WorkspaceBinding(
                workspace_id=f"analytics-{region}-ws",
                binding_type="SECONDARY"
            )
        ]
        
        return metastore
    
    def setup_metastore_admin_privileges(self):
        """Properly scope metastore admin privileges."""
        
        # IMPORTANT: Use groups for metastore administration
        metastore_admins = Principal("metastore_admins_group")
        
        # Grant metastore admin privileges
        access_manager.grant(
            metastore,
            metastore_admins,
            [PrivilegeType.CREATE_CATALOG, PrivilegeType.CREATE_EXTERNAL_LOCATION]
        )
        
        # DO NOT grant ALL_PRIVILEGES at metastore level
        # This is too broad and creates security risks
```

### 11.2 Delta Sharing Governance

**Business Scenario**: Securely share data with external partners.

**Implementation Strategy**:
```python
class DeltaSharingGovernance:
    """Secure external data sharing patterns."""
    
    def setup_partner_data_sharing(self):
        """Configure governed data sharing."""
        
        # Create sharing catalog
        sharing_catalog = Catalog(
            name="external_sharing",
            comment="Data shared with external partners",
            isolation_mode=IsolationMode.ISOLATED,
            tags=[
                Tag(key="sharing_enabled", value="true"),
                Tag(key="audit_level", value="detailed")
            ]
        )
        
        # Schema for shared data products
        shared_schema = Schema(
            name="partner_products",
            comment="Curated data products for partners"
        )
        sharing_catalog.add_schema(shared_schema)
        
        # Create share with governance
        share = Share(
            name="customer_insights_share",
            comment="Customer aggregate data for partners",
            owner="partnerships_team"
        )
        
        # Add filtered, aggregated data only
        share.add_table(
            table=shared_schema.get_table("customer_aggregates"),
            partition_spec="year >= 2023 AND region = 'US'",  # Filter data
            alias="us_customers_2023_onwards"
        )
        
        # Create recipient with restrictions
        recipient = Recipient(
            name="partner_analytics_firm",
            authentication_type="TOKEN",
            ip_access_list=["10.0.0.0/8"],  # Network restriction
            sharing_code="secure-token-xyz",
            expiry_time=datetime.now() + timedelta(days=90)  # Time-bound
        )
        
        # Grant share with audit
        access_manager.grant_share(
            share=share,
            recipient=recipient,
            audit_access=True  # Log all access
        )
```

### 11.3 Storage Credential and External Location Management

**Business Scenario**: Manage external storage with proper governance.

**Implementation Strategy**:
```python
class StorageGovernance:
    """External storage governance patterns."""
    
    def setup_tiered_storage_governance(self):
        """Implement storage tiers with governance."""
        
        storage_tiers = {
            "bronze": {
                "credential": StorageCredential(
                    name="bronze_storage_cred",
                    aws_iam_role={"role_arn": "arn:aws:iam::123456789:role/uc-bronze"},
                    comment="Raw data ingestion credential",
                    owner="data_engineering"
                ),
                "location": ExternalLocation(
                    name="bronze_landing",
                    url="s3://company-bronze/landing/",
                    credential_name="bronze_storage_cred",
                    read_only=False,  # Allow writes for ingestion
                    tags=[
                        Tag(key="tier", value="bronze"),
                        Tag(key="retention_days", value="30")
                    ]
                )
            },
            "silver": {
                "credential": StorageCredential(
                    name="silver_storage_cred",
                    aws_iam_role={"role_arn": "arn:aws:iam::123456789:role/uc-silver"},
                    comment="Cleansed data credential",
                    owner="data_quality_team"
                ),
                "location": ExternalLocation(
                    name="silver_cleansed",
                    url="s3://company-silver/cleansed/",
                    credential_name="silver_storage_cred",
                    read_only=False,
                    tags=[
                        Tag(key="tier", value="silver"),
                        Tag(key="retention_days", value="90")
                    ]
                )
            },
            "gold": {
                "credential": StorageCredential(
                    name="gold_storage_cred",
                    aws_iam_role={"role_arn": "arn:aws:iam::123456789:role/uc-gold"},
                    comment="Business-ready data credential",
                    owner="analytics_team"
                ),
                "location": ExternalLocation(
                    name="gold_analytics",
                    url="s3://company-gold/analytics/",
                    credential_name="gold_storage_cred",
                    read_only=True,  # Read-only for consumption
                    tags=[
                        Tag(key="tier", value="gold"),
                        Tag(key="retention_days", value="365")
                    ]
                )
            }
        }
        
        # Apply tier-specific permissions
        for tier_name, tier_config in storage_tiers.items():
            # Only data engineering can manage credentials
            access_manager.grant(
                tier_config["credential"],
                Principal("data_platform_team"),
                [PrivilegeType.CREATE_EXTERNAL_TABLE]
            )
            
            # Grant usage based on tier
            if tier_name == "bronze":
                # Data engineers write to bronze
                access_manager.grant(
                    tier_config["location"],
                    Principal("data_engineers"),
                    [PrivilegeType.CREATE_EXTERNAL_TABLE]
                )
            elif tier_name == "gold":
                # Everyone can read from gold
                access_manager.grant(
                    tier_config["location"],
                    Principal("all_users"),
                    [PrivilegeType.READ_FILES]
                )
```

### 11.4 Volume Governance

**Business Scenario**: Manage Unity Catalog volumes for unstructured data.

**Implementation Strategy**:
```python
class VolumeGovernance:
    """Governance for UC volumes (files, models, etc.)."""
    
    def setup_volume_governance(self):
        """Implement volume access patterns."""
        
        # ML model volume
        model_volume = Volume(
            name="ml_model_artifacts",
            volume_type=VolumeType.MANAGED,
            comment="ML model files and artifacts",
            tags=[
                Tag(key="content_type", value="ml_models"),
                Tag(key="scanning_enabled", value="true")  # Security scanning
            ]
        )
        
        # Document volume with access controls
        document_volume = Volume(
            name="corporate_documents",
            volume_type=VolumeType.EXTERNAL,
            storage_location="s3://company-docs/",
            comment="Corporate documentation",
            tags=[
                Tag(key="content_type", value="documents"),
                Tag(key="classification", value="internal")
            ]
        )
        
        # Grant volume permissions
        # ML team can write models
        access_manager.grant(
            model_volume,
            Principal("ml_team"),
            [PrivilegeType.READ_VOLUME, PrivilegeType.WRITE_VOLUME]
        )
        
        # Everyone can read documents
        access_manager.grant(
            document_volume,
            Principal("all_employees"),
            [PrivilegeType.READ_VOLUME]
        )
        
        # Only admins can write documents
        access_manager.grant(
            document_volume,
            Principal("content_admins"),
            [PrivilegeType.WRITE_VOLUME]
        )
```

---

## 12. Performance and Optimization Considerations

### 12.1 Permission Hierarchy Optimization

**Business Scenario**: Optimize permission checks for better query performance.

**Implementation Strategy**:
```python
class PerformanceOptimizedGovernance:
    """Performance-aware governance patterns."""
    
    def optimize_permission_structure(self):
        """Minimize permission check overhead."""
        
        # BEST PRACTICE: Use groups instead of individual grants
        # Groups reduce permission check complexity
        analytics_group = Principal("analytics_users_group")
        
        # BEST PRACTICE: Grant at appropriate level
        # Higher-level grants are more efficient than many table-level grants
        access_manager.grant(
            catalog,  # Grant at catalog level
            analytics_group,
            [PrivilegeType.USE_CATALOG, PrivilegeType.SELECT]
        )
        
        # ANTI-PATTERN: Too many individual grants
        # This creates performance overhead
        # for user in range(1000):
        #     access_manager.grant(table, Principal(f"user_{user}"), [SELECT])
        
        # BEST PRACTICE: Use privilege inheritance
        # Let permissions cascade down the hierarchy
        access_manager.grant(
            schema,  # Schema-level grant cascades to tables
            Principal("data_scientists"),
            [PrivilegeType.USE_SCHEMA, PrivilegeType.SELECT]
        )
    
    def optimize_catalog_organization(self):
        """Organize catalogs for performance."""
        
        # BEST PRACTICE: Separate hot and cold data
        hot_catalog = Catalog(
            name="real_time",
            comment="Frequently accessed data",
            properties={
                "delta.autoOptimize.optimizeWrite": "true",
                "delta.autoOptimize.autoCompact": "true"
            }
        )
        
        cold_catalog = Catalog(
            name="archive",
            comment="Infrequently accessed data",
            properties={
                "delta.deletedFileRetentionDuration": "7 days",
                "delta.logRetentionDuration": "30 days"
            }
        )
        
        # BEST PRACTICE: Use partition pruning
        partitioned_table = Table(
            name="events",
            columns=[
                ColumnInfo(name="event_date", type_name="DATE"),
                ColumnInfo(name="event_type", type_name="STRING")
            ],
            partitioning_columns=["event_date"],  # Partition by date
            clustering_columns=["event_type"]  # Cluster by type
        )
```

---

## 13. Common Pitfalls and Solutions

### Anti-Patterns to Avoid

1. **Over-granting at metastore level**
   ```python
   # ❌ WRONG: Too broad
   access_manager.grant(metastore, Principal("users"), [ALL_PRIVILEGES])
   
   # ✅ RIGHT: Grant at appropriate level
   access_manager.grant(catalog, Principal("domain_team"), [USE_CATALOG])
   ```

2. **Ignoring privilege dependencies**
   ```python
   # ❌ WRONG: Missing parent privileges
   access_manager.grant(table, user, [SELECT])
   
   # ✅ RIGHT: Include all required privileges
   access_manager.grant(catalog, user, [USE_CATALOG])
   access_manager.grant(schema, user, [USE_SCHEMA])
   access_manager.grant(table, user, [SELECT])
   ```

3. **Not using groups for scale**
   ```python
   # ❌ WRONG: Individual user grants don't scale
   for user in users:
       grant(table, user, privileges)
   
   # ✅ RIGHT: Use groups
   create_group("analysts", users)
   grant(schema, Principal("analysts"), privileges)
   ```

4. **Mixing environment permissions**
   ```python
   # ❌ WRONG: Same permissions across environments
   grant(catalog, Principal("developers"), [ALL_PRIVILEGES])
   
   # ✅ RIGHT: Environment-specific permissions
   if environment == Environment.DEV:
       grant(catalog, Principal("developers"), [ALL_PRIVILEGES])
   elif environment == Environment.PRD:
       grant(catalog, Principal("developers"), [SELECT])
   ```

5. **Forgetting audit requirements**
   ```python
   # ❌ WRONG: No audit trail
   grant(sensitive_data, user, [SELECT])
   
   # ✅ RIGHT: Enable auditing for sensitive data
   sensitive_catalog.tags.append(Tag(key="audit_enabled", value="true"))
   grant(sensitive_data, user, [SELECT])
   log_grant_event(user, sensitive_data, timestamp)
   ```

---

---

## 14. Additional Operational Pattern Implementations

### 14.1 SLA Negotiation Pattern Implementation
**[Implements: Operational Governance Pattern §4.1]**

**Business Scenario**: Producers and consumers need to agree on service levels with cost allocation.

**Implementation Strategy**:
```python
class SLATieredGovernance:
    """Implement tiered SLA offerings with Unity Catalog."""
    
    def setup_sla_tiers(self):
        """Define SLA tiers with cost allocation."""
        
        sla_configs = {
            "platinum": {
                "freshness_minutes": 5,
                "availability": 0.9995,
                "support": "24x7",
                "cost_factor": 4,
                "compute": "OPTIMIZED"
            },
            "gold": {
                "freshness_hours": 1,
                "availability": 0.999,
                "support": "business_hours",
                "cost_factor": 2,
                "compute": "STANDARD"
            },
            "silver": {
                "freshness_hours": 24,
                "availability": 0.99,
                "support": "best_effort",
                "cost_factor": 1,
                "compute": "ECONOMY"
            },
            "bronze": {
                "freshness": "best_effort",
                "availability": 0.95,
                "support": "community",
                "cost_factor": 0,
                "compute": "SPOT"
            }
        }
        
        # Tag tables with SLA commitments
        for table in self.managed_tables:
            sla_tier = self.negotiate_sla(table)
            table.tags = [
                Tag(key="sla_tier", value=sla_tier),
                Tag(key="freshness_commitment", value=str(sla_configs[sla_tier]["freshness"])),
                Tag(key="availability_target", value=str(sla_configs[sla_tier]["availability"])),
                Tag(key="cost_center", value=table.consumer_cost_center),
                Tag(key="monitoring_enabled", value="true")
            ]
            
            # Create SLA monitoring view
            self.create_sla_monitoring(table, sla_tier)
    
    def create_sla_monitoring(self, table: Table, sla_tier: str):
        """Create monitoring infrastructure for SLA tracking."""
        
        monitoring_view = View(
            name=f"{table.name}_sla_monitor",
            catalog_name=table.catalog_name,
            schema_name="monitoring",
            comment=f"SLA monitoring for {table.name} ({sla_tier} tier)",
            query=f"""
                SELECT 
                    current_timestamp() as check_time,
                    '{sla_tier}' as sla_tier,
                    max(_metadata.file_modification_time) as last_update,
                    (unix_timestamp(current_timestamp()) - 
                     unix_timestamp(max(_metadata.file_modification_time)))/60 as minutes_since_update,
                    CASE 
                        WHEN '{sla_tier}' = 'platinum' AND minutes_since_update > 5 THEN 'VIOLATION'
                        WHEN '{sla_tier}' = 'gold' AND minutes_since_update > 60 THEN 'VIOLATION'
                        ELSE 'COMPLIANT'
                    END as sla_status
                FROM {table.catalog_name}.{table.schema_name}.{table.name}
            """
        )
        
        # Grant monitoring access
        access_manager.grant(
            monitoring_view,
            Principal("sla_monitoring_team"),
            [PrivilegeType.SELECT]
        )
```

**Trade-offs**:
- ✅ Clear expectations and cost allocation
- ✅ Automated monitoring and alerting
- ❌ Requires external monitoring infrastructure
- ❌ Complex cost tracking

### 14.2 Shared Enterprise Dataset Pattern Implementation
**[Implements: Operational Governance Pattern §2.3]**

**Business Scenario**: Enterprise datasets like Customer 360 require shared ownership.

**Implementation Strategy**:
```python
class SharedDatasetGovernance:
    """Implement committee-based governance for shared datasets."""
    
    def setup_shared_governance(self, dataset_name: str = "customer_360"):
        """Create shared governance structure."""
        
        # Unity Catalog limitation: single owner
        # Workaround: Use group ownership + RACI matrix
        
        # Create committee group
        committee_group = Principal(f"{dataset_name}_committee")
        
        # Create shared catalog with group ownership
        shared_catalog = Catalog(
            name=f"enterprise_{dataset_name}",
            comment="Shared enterprise dataset with committee governance",
            owner=committee_group.resolved_name,
            isolation_mode=IsolationMode.OPEN,
            tags=[
                Tag(key="governance_model", value="committee"),
                Tag(key="dataset_type", value="enterprise_shared")
            ]
        )
        
        # Define RACI matrix via tags and documentation
        raci_matrix = {
            "responsible": "data_engineering",  # Day-to-day operations
            "accountable": "chief_data_officer",  # Ultimate accountability
            "consulted": ["sales_lead", "marketing_lead", "finance_lead"],
            "informed": ["analytics_consumers", "business_users"]
        }
        
        # Add RACI as tags
        for role, teams in raci_matrix.items():
            if isinstance(teams, list):
                shared_catalog.tags.append(
                    Tag(key=f"raci_{role}", value=",".join(teams))
                )
            else:
                shared_catalog.tags.append(
                    Tag(key=f"raci_{role}", value=teams)
                )
        
        # Grant operational privileges based on RACI
        # Responsible team gets operational control
        access_manager.grant(
            shared_catalog,
            Principal(raci_matrix["responsible"]),
            [
                PrivilegeType.CREATE_TABLE,
                PrivilegeType.CREATE_SCHEMA,
                PrivilegeType.MODIFY
            ]
        )
        
        # Consulted teams get read and suggest
        for team in raci_matrix["consulted"]:
            access_manager.grant(
                shared_catalog,
                Principal(team),
                [PrivilegeType.USE_CATALOG, PrivilegeType.SELECT]
            )
        
        # Create decision log table
        decision_log = Table(
            name="governance_decisions",
            catalog_name=shared_catalog.name,
            schema_name="governance",
            comment="Committee decisions and changes",
            columns=[
                ColumnInfo(name="decision_date", type_name="DATE"),
                ColumnInfo(name="decision_type", type_name="STRING"),
                ColumnInfo(name="description", type_name="STRING"),
                ColumnInfo(name="approved_by", type_name="STRING"),
                ColumnInfo(name="impact", type_name="STRING")
            ]
        )
```

**Trade-offs**:
- ✅ Shared expertise and balanced priorities
- ✅ Clear RACI responsibilities
- ❌ UC single-owner limitation requires workarounds
- ❌ Slower decision making

### 14.3 Mixed Sensitivity Layering Pattern Implementation
**[Implements: Operational Governance Pattern §1.3]**

**Business Scenario**: Single dataset contains both sensitive and non-sensitive information.

**Implementation Strategy**:
```python
class MixedSensitivityLayering:
    """Create layered views with progressive sensitivity reduction."""
    
    def create_sensitivity_layers(self, base_table: Table):
        """Implement three-layer sensitivity model."""
        
        # Layer 1: Base table with full PII (restricted access)
        base_table.tags = [
            Tag(key="sensitivity", value="restricted"),
            Tag(key="contains_pii", value="true"),
            Tag(key="audit_all_access", value="true")
        ]
        
        # Layer 2: Masked view (performance impact: 20-30%)
        masked_view = View(
            name=f"{base_table.name}_masked",
            catalog_name=base_table.catalog_name,
            schema_name=base_table.schema_name,
            comment="PII masked view for analysts",
            query=f"""
                SELECT 
                    -- Mask PII fields
                    CASE 
                        WHEN is_member('pii_viewers') THEN ssn 
                        ELSE 'XXX-XX-' || SUBSTR(ssn, -4) 
                    END as ssn,
                    CASE 
                        WHEN is_member('pii_viewers') THEN email 
                        ELSE REGEXP_REPLACE(email, '(.{3}).*@', '$1***@')
                    END as email,
                    -- Non-PII fields unchanged
                    customer_id,
                    transaction_amount,
                    transaction_date,
                    merchant_category
                FROM {base_table.catalog_name}.{base_table.schema_name}.{base_table.name}
            """,
            tags=[
                Tag(key="sensitivity", value="internal"),
                Tag(key="masking_applied", value="true"),
                Tag(key="performance_impact", value="20-30%")
            ]
        )
        
        # Layer 3: Aggregated materialized view (best performance)
        aggregated_view = Table(
            name=f"{base_table.name}_aggregated",
            catalog_name=base_table.catalog_name,
            schema_name=base_table.schema_name,
            comment="Aggregated metrics without PII",
            table_type=TableType.MANAGED,
            tags=[
                Tag(key="sensitivity", value="public"),
                Tag(key="contains_pii", value="false"),
                Tag(key="refresh_schedule", value="0 2 * * *"),  # 2am daily
                Tag(key="materialized", value="true")
            ]
        )
        
        # Create materialization job
        materialization_job = f"""
            CREATE OR REPLACE TABLE {aggregated_view.full_name} AS
            SELECT 
                DATE_TRUNC('day', transaction_date) as date,
                merchant_category,
                customer_segment,
                COUNT(*) as transaction_count,
                AVG(transaction_amount) as avg_amount,
                PERCENTILE(transaction_amount, 0.5) as median_amount
            FROM {masked_view.full_name}
            GROUP BY 1, 2, 3
        """
        
        # Apply access controls per layer
        # Layer 1: Restricted access
        access_manager.grant(
            base_table,
            Principal("pii_authorized_users"),
            [PrivilegeType.SELECT]
        )
        
        # Layer 2: Analyst access
        access_manager.grant(
            masked_view,
            Principal("data_analysts"),
            [PrivilegeType.SELECT]
        )
        
        # Layer 3: Broad access
        access_manager.grant(
            aggregated_view,
            Principal("all_employees"),
            [PrivilegeType.SELECT]
        )
```

**Performance Considerations**:
- Dynamic masking views: 20-30% performance impact
- Materialized aggregates: Best performance, requires refresh
- View nesting depth: Maximum 3 levels recommended

### 14.4 Data Vault Pattern Implementation
**[Implements: Operational Governance Pattern §3.3]**

**Business Scenario**: Need historical tracking with complex relationships.

**Implementation Strategy**:
```python
class DataVaultImplementation:
    """Implement Data Vault 2.0 pattern in Unity Catalog."""
    
    def create_data_vault_structure(self):
        """Create hub, link, and satellite structure."""
        
        # Create dedicated catalog for vault
        vault_catalog = Catalog(
            name="data_vault",
            comment="Data Vault 2.0 implementation",
            tags=[
                Tag(key="modeling_approach", value="data_vault_2.0"),
                Tag(key="history_tracking", value="full")
            ]
        )
        
        # Schema organization
        vault_catalog.add_schema(Schema(name="raw_vault", comment="Hubs, Links, Satellites"))
        vault_catalog.add_schema(Schema(name="business_vault", comment="Business rules applied"))
        vault_catalog.add_schema(Schema(name="information_marts", comment="Consumer views"))
    
    def create_hub(self, entity_name: str) -> Table:
        """Create hub table for business entity."""
        
        return Table(
            name=f"hub_{entity_name}",
            catalog_name="data_vault",
            schema_name="raw_vault",
            comment=f"Hub for {entity_name} business key",
            columns=[
                ColumnInfo(name=f"{entity_name}_hkey", type_name="STRING", comment="Hash key"),
                ColumnInfo(name=f"{entity_name}_bkey", type_name="STRING", comment="Business key"),
                ColumnInfo(name="load_date", type_name="TIMESTAMP"),
                ColumnInfo(name="record_source", type_name="STRING")
            ],
            tblproperties={
                "delta.enableChangeDataFeed": "true",
                "delta.columnMapping.mode": "name",
                "delta.appendOnly": "true"  # Hubs are insert-only
            },
            tags=[
                Tag(key="vault_type", value="hub"),
                Tag(key="entity", value=entity_name)
            ]
        )
    
    def create_satellite(self, hub_name: str, satellite_type: str) -> Table:
        """Create satellite for versioned attributes."""
        
        return Table(
            name=f"sat_{hub_name}_{satellite_type}",
            catalog_name="data_vault",
            schema_name="raw_vault",
            comment=f"Satellite for {hub_name} {satellite_type} attributes",
            columns=[
                ColumnInfo(name=f"{hub_name}_hkey", type_name="STRING"),
                ColumnInfo(name="load_date", type_name="TIMESTAMP"),
                ColumnInfo(name="load_end_date", type_name="TIMESTAMP"),
                ColumnInfo(name="record_source", type_name="STRING"),
                ColumnInfo(name="hash_diff", type_name="STRING", comment="Hash of attributes"),
                # Dynamic attributes based on satellite type
            ],
            tblproperties={
                "delta.enableChangeDataFeed": "true",
                "delta.dataSkippingNumIndexedCols": "5",
                "delta.autoOptimize.optimizeWrite": "true"
            },
            tags=[
                Tag(key="vault_type", value="satellite"),
                Tag(key="parent_hub", value=hub_name),
                Tag(key="satellite_type", value=satellite_type)
            ]
        )
    
    def create_link(self, hub1: str, hub2: str) -> Table:
        """Create link table for relationships."""
        
        return Table(
            name=f"link_{hub1}_{hub2}",
            catalog_name="data_vault",
            schema_name="raw_vault",
            comment=f"Link between {hub1} and {hub2}",
            columns=[
                ColumnInfo(name=f"{hub1}_{hub2}_hkey", type_name="STRING", comment="Link hash key"),
                ColumnInfo(name=f"{hub1}_hkey", type_name="STRING"),
                ColumnInfo(name=f"{hub2}_hkey", type_name="STRING"),
                ColumnInfo(name="load_date", type_name="TIMESTAMP"),
                ColumnInfo(name="record_source", type_name="STRING")
            ],
            tblproperties={
                "delta.appendOnly": "true"  # Links are insert-only
            },
            tags=[
                Tag(key="vault_type", value="link"),
                Tag(key="connects", value=f"{hub1},{hub2}")
            ]
        )
```

**Trade-offs**:
- ✅ Complete audit trail and history
- ✅ Flexible schema evolution
- ❌ High complexity and learning curve
- ❌ Significant storage overhead

### 14.5 Push vs Pull Data Flow Pattern Implementation
**[Implements: Operational Governance Pattern §4.2]**

**Business Scenario**: Choosing optimal data flow patterns based on requirements.

**Implementation Strategy**:
```python
class DataFlowPatterns:
    """Implement push and pull patterns with Unity Catalog."""
    
    def implement_push_pattern(self, source: str, target: Table):
        """Real-time push using streaming and Auto Loader."""
        
        # Create streaming table for push pattern
        streaming_table = Table(
            name=f"{target.name}_stream",
            catalog_name=target.catalog_name,
            schema_name=target.schema_name,
            comment="Streaming ingestion via push pattern",
            table_type=TableType.STREAMING,
            tblproperties={
                "pipelines.autoOptimize.managed": "true",
                "pipelines.trigger.interval": "10 seconds",
                "quality.rules": '[{"name": "not_null", "columns": ["id"]}]'
            },
            tags=[
                Tag(key="pattern", value="push"),
                Tag(key="latency", value="near_real_time"),
                Tag(key="source", value=source)
            ]
        )
        
        # DLT pipeline configuration for push
        push_pipeline = f"""
            CREATE OR REFRESH STREAMING TABLE {streaming_table.full_name}
            AS SELECT *
            FROM cloud_files(
                '{source}',
                'json',
                map('cloudFiles.inferColumnTypes', 'true')
            )
        """
        
        # Grant producer push privileges
        access_manager.grant(
            streaming_table,
            Principal("data_producers"),
            [PrivilegeType.MODIFY]
        )
        
        return streaming_table
    
    def implement_pull_pattern(self, source: Table, target: Table):
        """Batch pull using scheduled jobs."""
        
        # Create pull configuration
        pull_config = {
            "source": source.full_name,
            "target": target.full_name,
            "schedule": "0 */6 * * *",  # Every 6 hours
            "merge_keys": ["id"],
            "watermark_column": "modified_timestamp"
        }
        
        # Pull pattern SQL
        pull_query = f"""
            MERGE INTO {target.full_name} AS target
            USING (
                SELECT * FROM {source.full_name}
                WHERE modified_timestamp > (
                    SELECT COALESCE(MAX(modified_timestamp), '1900-01-01')
                    FROM {target.full_name}
                )
            ) AS source
            ON target.id = source.id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """
        
        # Tag target with pull metadata
        target.tags = [
            Tag(key="pattern", value="pull"),
            Tag(key="latency", value="batch"),
            Tag(key="schedule", value=pull_config["schedule"]),
            Tag(key="source", value=source.full_name)
        ]
        
        # Grant consumer pull privileges
        access_manager.grant(
            source,
            Principal("data_consumers"),
            [PrivilegeType.SELECT]
        )
        
        return pull_query
    
    def choose_pattern(self, requirements: dict) -> str:
        """Decision matrix for push vs pull."""
        
        decision_matrix = {
            ("low", "tight"): "push",  # Low latency, tight coupling
            ("low", "loose"): "events",  # Low latency, loose coupling
            ("high", "tight"): "pull",  # High latency OK, tight coupling
            ("high", "loose"): "batch"  # High latency OK, loose coupling
        }
        
        latency = "low" if requirements["latency_seconds"] < 60 else "high"
        coupling = requirements.get("coupling_preference", "loose")
        
        return decision_matrix.get((latency, coupling), "pull")
```

**Trade-offs**:
- **Push**: ✅ Low latency, ❌ Tight coupling
- **Pull**: ✅ Loose coupling, ❌ Higher latency
- **Events**: ✅ Best of both, ❌ Complex infrastructure

---

## 15. Anti-Patterns and Performance Considerations

### 15.1 Unity Catalog Anti-Patterns to Avoid

```python
# ❌ ANTI-PATTERN 1: Catalog Sprawl
# Creating too many catalogs instead of using schemas
bad_practice = {
    "project1_dev": Catalog(name="project1_dev"),
    "project1_test": Catalog(name="project1_test"),
    "project1_prod": Catalog(name="project1_prod")
    # Results in 100s of catalogs, impossible to manage
}

# ✅ CORRECT: Use schemas for project/environment separation
good_practice = Catalog(
    name="analytics",
    schemas=[
        Schema(name="project1_dev"),
        Schema(name="project1_test"),
        Schema(name="project1_prod")
    ]
)

# ❌ ANTI-PATTERN 2: Individual User Grants
# Granting to individual users instead of groups
access_manager.grant(
    table,
    Principal("john.doe@company.com"),  # Individual user
    [PrivilegeType.SELECT]
)

# ✅ CORRECT: Always use groups
access_manager.grant(
    table,
    Principal("data_analysts_group"),  # Group
    [PrivilegeType.SELECT]
)

# ❌ ANTI-PATTERN 3: Deep View Nesting
# Multiple levels of views for security
view1 = "CREATE VIEW v1 AS SELECT * FROM base WHERE filter1"
view2 = "CREATE VIEW v2 AS SELECT * FROM v1 WHERE filter2"
view3 = "CREATE VIEW v3 AS SELECT * FROM v2 WHERE filter3"
# Results in 70% performance degradation

# ✅ CORRECT: Single materialized view with all filters
materialized = """
    CREATE MATERIALIZED VIEW secure_view AS
    SELECT * FROM base 
    WHERE filter1 AND filter2 AND filter3
"""

# ❌ ANTI-PATTERN 4: Over-Granting at Metastore Level
access_manager.grant(
    metastore,
    Principal("all_users"),
    [PrivilegeType.CREATE_CATALOG]  # Too broad
)

# ✅ CORRECT: Grant at appropriate level
access_manager.grant(
    catalog,
    Principal("domain_team"),
    [PrivilegeType.USE_CATALOG]
)
```

### 15.2 Performance Impact Guidelines

```python
performance_guidelines = {
    "dynamic_views": {
        "impact": "20-30% query performance hit",
        "mitigation": "Use materialized views for frequent queries",
        "refresh_strategy": "Incremental where possible"
    },
    
    "cross_catalog_joins": {
        "impact": "3-5x slower than same-catalog joins",
        "mitigation": "Co-locate frequently joined data",
        "optimization": "Use Delta Cache for hot data"
    },
    
    "privilege_checks": {
        "impact": "<5ms for well-structured hierarchies",
        "anti_pattern": "1000s of individual grants = seconds of overhead",
        "best_practice": "Use groups and inheritance"
    },
    
    "view_nesting_depth": {
        "level_1": "~5% overhead",
        "level_2": "~15% overhead",
        "level_3": "~30% overhead",
        "level_4+": "Exponential degradation - avoid"
    }
}
```

---

## Conclusion

This comprehensive governance framework combines the best of Unity Catalog's native capabilities with modern engineering practices:

- **Pydantic Models** provide type-safe governance definitions
- **Unity Catalog** offers enterprise-grade security and compliance
- **MLflow Integration** enables ML governance throughout the lifecycle
- **DABs** provide declarative deployment and CI/CD capabilities
- **SDK** handles complex, dynamic governance requirements

The key to successful implementation is:

1. **Start Simple**: Begin with basic RBAC and catalog structure
2. **Layer Security**: Add advanced patterns as needs grow
3. **Automate Early**: Use CI/CD from the beginning
4. **Monitor Continuously**: Track usage and optimize performance
5. **Iterate Based on Feedback**: Governance is an ongoing process

By following these strategies and patterns, organizations can build a robust, scalable, and maintainable data governance system that enables secure data democratization while maintaining compliance and control.

### Key Implementation Guidelines

1. **Start with proven patterns**: Physical Segregation + Zone Progression
2. **Match patterns to maturity**: Don't implement Data Mesh before you're ready
3. **Use groups religiously**: Never grant to individual users
4. **Monitor performance**: Every security layer has a cost
5. **Leverage UC strengths**: Delta Sharing, Lakehouse Monitoring, System Tables
6. **Plan for limitations**: Work around single-owner model and lack of native RLS

### Success Metrics

- **Adoption**: >80% of teams using governance patterns
- **Quality**: >95% of data meeting SLA commitments
- **Security**: Zero unauthorized access incidents
- **Performance**: <10% overhead from governance controls
- **Automation**: >90% of governance tasks automated