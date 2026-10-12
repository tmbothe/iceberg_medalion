# RBAC & ABAC Governance Design — Manufacturing Lakehouse

> **Status:** Design proposal — not yet implemented.
> This document describes the target state for fine-grained access control
> (row, column, cell level) across the Manufacturing Lakehouse using
> Role-Based Access Control (RBAC) and Attribute-Based Access Control (ABAC).

---

## Table of Contents

1. [Problem Statement](#1-problem-statement)
2. [RBAC vs ABAC — Definitions and When to Use Each](#2-rbac-vs-abac--definitions-and-when-to-use-each)
3. [Architecture Overview](#3-architecture-overview)
4. [Identity Layer: Okta as the Source of Truth](#4-identity-layer-okta-as-the-source-of-truth)
5. [Authentication: Okta → AWS IAM via SAML Federation](#5-authentication-okta--aws-iam-via-saml-federation)
6. [ABAC: Lake Formation Tag-Based Access Control (LF-TBAC)](#6-abac-lake-formation-tag-based-access-control-lf-tbac)
7. [RBAC: Human Persona Roles and Permissions](#7-rbac-human-persona-roles-and-permissions)
8. [Machine Roles and DAG Identity Guardrails](#8-machine-roles-and-dag-identity-guardrails)
9. [Fine-Grained Table, Column, and Row-Level Security](#9-fine-grained-table-column-and-row-level-security)
10. [Schema-Level Namespace Separation](#10-schema-level-namespace-separation)
11. [Granular DDL / DML Permission Matrix](#11-granular-ddl--dml-permission-matrix)
12. [Agent Inheritance (User-to-Machine and Machine-to-Machine)](#12-agent-inheritance-user-to-machine-and-machine-to-machine)
13. [Vended S3 Credentials and Storage-Level Enforcement](#13-vended-s3-credentials-and-storage-level-enforcement)
14. [Data Residency and Enforcement Processing Location](#14-data-residency-and-enforcement-processing-location)
15. [SOX Audit Trail and Change Control](#15-sox-audit-trail-and-change-control)
16. [Step-by-Step: How a Query Flows Through the Stack](#16-step-by-step-how-a-query-flows-through-the-stack)
17. [Implementation Roadmap](#17-implementation-roadmap)

---

## 1. Problem Statement

### Current State (Gap)

Access to the Manufacturing Lakehouse is currently enforced at the **S3 bucket level only**:

```
Current enforcement:
  User A can read  s3://mfg-lakehouse-*/   → reads ALL tables in ALL layers
  User B cannot read the bucket at all     → blocked from everything
```

This means:
- A data analyst can accidentally (or intentionally) read raw Bronze PII-adjacent fields
- A supplier manager can see operator data they should not see
- A scheduled ETL job has the same permissions as a human admin
- There is no audit trail at the table/row/column level
- Schema-level separation between Bronze, Silver, and Gold is not enforced by policy

### Target State

```
Target enforcement:
  Supplier Manager → SELECT on gold_supplier_performance (specific columns only)
                  → DENIED on silver_operator, bronze_*, any other table
  Quality Analyst  → SELECT on gold_defect_analysis, silver_operation_log
                  → DENIED on hire_date column in silver_operator
  ETL Job (Glue)   → carries the permission of the engineer who scheduled it
                  → if engineer lacks WRITE to silver_*, the job fails
  Finance ETL      → WRITE to fdw_prod namespace only
                  → DENIED write to edw_dims even if code tries
```

### Compliance Requirements Driving This

| Requirement | Mandate |
|-------------|---------|
| Row-level access by persona | Internal data classification policy |
| Column-level PII masking | GDPR / CCPA |
| Machine role = owner's permission | SOX segregation of duties |
| Full audit trail | SOX Section 404 / internal audit |
| Data residency | GDPR data locality |
| Schema-level isolation | Finsys data separation controls |

---

## 2. RBAC vs ABAC — Definitions and When to Use Each

### Role-Based Access Control (RBAC)

A user is assigned a **role**. The role has a fixed set of permissions.

```
User "Alice" → Role "quality_analyst"
Role "quality_analyst" → PERMISSIONS:
    SELECT on mfg_gold_db.gold_defect_analysis
    SELECT on mfg_silver_db.silver_operation_log
    DENIED on mfg_bronze_db.*
    DENIED on column silver_operator.hire_date
```

**Best for:** Well-defined, stable personas where every member of the role has
identical access needs.

**Weakness:** Role explosion — in a large organization you end up with hundreds of
granular roles (e.g., `supplier_manager_us`, `supplier_manager_eu`,
`supplier_manager_us_readonly`...).

---

### Attribute-Based Access Control (ABAC)

Access is granted when **attributes of the identity** match **attributes of the resource**.
No explicit per-user permission grants needed.

```
User "Alice" carries attributes:
    department = "quality"
    clearance  = "internal"
    region     = "us-east"

Table "silver_operation_log" carries tags:
    domain         = "operations"
    classification = "internal"
    region         = "us-east"

Policy: "Allow SELECT where identity.clearance >= resource.classification
         AND identity.region = resource.region"

Result: Alice can query silver_operation_log — no explicit grant needed.
```

**Best for:** Dynamic, large-scale environments where attributes change frequently
(new employees, new tables, regional expansions).

**Strength:** Adding a new table with the right tags automatically grants access to
everyone with matching attributes. No re-granting required.

---

### This Project: RBAC + ABAC Together

We use **both**:
- **RBAC** for stable, well-defined personas (data engineer, analyst, supplier manager)
- **ABAC (LF-TBAC)** for scalable, attribute-driven access that propagates automatically
  as new tables are added to the catalog

```
RBAC layer:  "quality_analyst" role → can access resources tagged domain=quality
ABAC layer:  resources tagged domain=quality, classification=internal
             any principal with tag access:quality=true, clearance=internal → granted
```

---

## 3. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          IDENTITY PLANE                                     │
│                                                                             │
│  ┌─────────────────┐     SAML 2.0 / OIDC      ┌───────────────────────┐   │
│  │      Okta        │ ─────────────────────────► │   AWS IAM (IdP)      │   │
│  │  (User Directory)│  assertions carry:        │  SAML Federation     │   │
│  │                  │  department, clearance,   │  Session Tags from   │   │
│  │  Groups:         │  role, region, team       │  Okta attributes     │   │
│  │  - data-engineers│                           └──────────┬───────────┘   │
│  │  - analysts      │                                      │               │
│  │  - suppliers     │                                      │ AssumeRoleWith│
│  │  - finance       │                                      │ SAMLAssertion │
│  └─────────────────┘                                      ▼               │
└──────────────────────────────────────────────────────────────────────────── │
                                                                             │
┌─────────────────────────────────────────────────────────────────────────────▼─┐
│                         AUTHORIZATION PLANE                                    │
│                                                                                │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │                    AWS Lake Formation                                     │  │
│  │                                                                          │  │
│  │  LF-Tags on resources:          LF-Tag grants on principals:             │  │
│  │  layer = bronze|silver|gold     quality_analyst → domain:quality=true    │  │
│  │  domain = operations|quality|   data_engineer  → layer:*=true            │  │
│  │           supplier|finance      supplier_mgr   → domain:supplier=true    │  │
│  │  classification = public|       finance_etl    → domain:finance=true     │  │
│  │                  internal|                                                │  │
│  │                  confidential   Row filters / Column masks               │  │
│  │  pii = true|false               applied at query execution time          │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────────────────┘
                                  │
                                  │ Vended credentials (short-lived S3 tokens)
                                  ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           ENFORCEMENT PLANE                                     │
│                                                                                 │
│  Query Engine (Athena)  →  Lake Formation  →  S3 (scoped to permitted files)   │
│  ETL Engine (Glue)      →  Lake Formation  →  S3 (scoped to permitted paths)   │
│                                                                                 │
│  Each principal only touches the S3 files belonging to their permitted tables  │
└─────────────────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                             AUDIT PLANE                                         │
│                                                                                 │
│  AWS CloudTrail  →  all API calls (who called what, when, from where)          │
│  Lake Formation audit logs  →  table/column access, permission changes         │
│  S3 Access Logs  →  file-level access with identity                            │
│  Exported to:  CloudWatch Logs, S3 audit bucket, SIEM (Splunk/Datadog)        │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Identity Layer: Okta as the Source of Truth

Okta is the **single source of truth** for user identity and group membership.

### Okta Groups → AWS Roles Mapping

| Okta Group | AWS IAM Role | Access Level |
|-----------|-------------|-------------|
| `aws-mfg-data-engineers` | `mfg-role-data-engineer` | All layers, all DDL/DML |
| `aws-mfg-quality-analysts` | `mfg-role-quality-analyst` | Gold + Silver ops, no PII columns |
| `aws-mfg-supplier-managers` | `mfg-role-supplier-manager` | Gold supplier only |
| `aws-mfg-floor-managers` | `mfg-role-floor-manager` | Gold ops summary, their workstation only |
| `aws-mfg-finance` | `mfg-role-finance` | Finance namespace only |
| `aws-mfg-executives` | `mfg-role-executive` | Gold layer, SELECT only |
| `aws-mfg-auditors` | `mfg-role-auditor` | Read-only on ALL layers + audit logs |

### Okta User Attributes Passed as Session Tags

When a user authenticates via Okta and assumes an AWS IAM role, Okta embeds these
attributes in the SAML assertion as **session tags**:

```
saml:attribute:department   → "quality"
saml:attribute:clearance    → "internal"
saml:attribute:team         → "quality-team-us"
saml:attribute:workstation  → "WS-A"     ← for floor managers (row-level filter)
saml:attribute:region       → "us-east"
```

These session tags are automatically forwarded by IAM to Lake Formation as
**principal tags**, enabling ABAC decisions based on user attributes at query time.

### How Okta Passes Tags to IAM

1. Configure **AWS app** in Okta with SAML 2.0
2. Map Okta user profile attributes → SAML assertions → `https://aws.amazon.com/SAML/Attributes/PrincipalTag:{key}` namespace
3. AWS IAM reads these assertions and creates a **session** with the matching tags:
   ```
   {
     "department": "quality",
     "clearance":  "internal",
     "workstation": "WS-A"
   }
   ```
4. Lake Formation evaluates `aws:PrincipalTag/department` against LF-Tag grants

---

## 5. Authentication: Okta → AWS IAM via SAML Federation

### Step-by-Step Flow

```
1. User opens AWS Console or runs `aws sso login`
      │
      ▼
2. Browser redirected to Okta login page
      │
      ▼
3. Okta authenticates user (MFA optional)
      │
      ▼
4. Okta generates SAML assertion:
   - NameID: alice@company.com
   - Groups:  aws-mfg-quality-analysts
   - Attributes:
       PrincipalTag:department = "quality"
       PrincipalTag:clearance  = "internal"
       PrincipalTag:workstation = "WS-A"
      │
      ▼
5. AWS IAM receives SAML assertion via AssumeRoleWithSAML
   - Validates SAML signature against Okta's IdP certificate
   - Finds matching IAM role: mfg-role-quality-analyst
      │
      ▼
6. IAM creates short-lived session credentials (STS):
   - AccessKeyId, SecretAccessKey, SessionToken (valid 1–8 hours)
   - Session tags: {department=quality, clearance=internal, ...}
      │
      ▼
7. User makes API call to Athena / Glue with session credentials
      │
      ▼
8. Athena calls Lake Formation: "can this session access silver_operation_log?"
   - Lake Formation reads session tags from the calling principal
   - Evaluates LF-Tag policy: domain=operations, classification=internal
   - identity.department=quality matches LF grant for quality_analyst
   - identity.clearance=internal matches resource.classification=internal
   - GRANTED
      │
      ▼
9. Lake Formation vends short-lived S3 credentials scoped to
   silver/operation_log/ path only
      │
      ▼
10. Query executes; result returned to user
```

### IAM Role Trust Policy (allows Okta SAML federation)

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {
      "Federated": "arn:aws:iam::442042510197:saml-provider/OktaMfgLakehouse"
    },
    "Action": "sts:AssumeRoleWithSAML",
    "Condition": {
      "StringEquals": {
        "SAML:aud": "https://signin.aws.amazon.com/saml"
      }
    }
  }]
}
```

### Session Tag Condition in IAM Role (ABAC guard)

Optionally enforce that only users with a specific Okta attribute can assume the role:

```json
{
  "Condition": {
    "StringEquals": {
      "aws:RequestedRegion": "us-east-1",
      "sts:TransitiveTagKeys": ["department", "clearance", "workstation"]
    }
  }
}
```

---

## 6. ABAC: Lake Formation Tag-Based Access Control (LF-TBAC)

### LF-Tags on Resources

Every database, table, and column is tagged at creation time:

#### Database-Level Tags
| Database | `layer` | `domain` | `classification` |
|---------|---------|---------|-----------------|
| `mfg_bronze_db` | `bronze` | `operations` | `confidential` |
| `mfg_silver_db` | `silver` | `operations` | `internal` |
| `mfg_gold_db` | `gold` | `operations` | `public` |

#### Table-Level Tags
| Table | `layer` | `domain` | `classification` | `pii` |
|-------|---------|---------|-----------------|-------|
| `bronze_operator` | `bronze` | `hr` | `confidential` | `true` |
| `bronze_operation_log` | `bronze` | `operations` | `confidential` | `false` |
| `silver_operator` | `silver` | `hr` | `internal` | `true` |
| `silver_machine` | `silver` | `operations` | `internal` | `false` |
| `silver_supplier` | `silver` | `supplier` | `internal` | `false` |
| `silver_operation_log` | `silver` | `operations` | `internal` | `false` |
| `gold_operation_summary` | `gold` | `operations` | `public` | `false` |
| `gold_defect_analysis` | `gold` | `quality` | `public` | `false` |
| `gold_supplier_performance` | `gold` | `supplier` | `public` | `false` |

#### Column-Level Tags (PII columns)
| Table | Column | `pii` | `classification` |
|-------|--------|-------|-----------------|
| `silver_operator` | `hire_date` | `true` | `confidential` |
| `bronze_operator` | `operator_name` | `true` | `confidential` |
| `silver_supplier` | `contact_email` | `true` | `confidential` |

### LF-Tag Grants on Principals (ABAC Policies)

```
Policy: "data_engineer role can access any resource where layer=* (any)"
  → Grant: mfg-role-data-engineer → LF-Tag: layer IN [bronze, silver, gold]

Policy: "quality_analyst can access any internal operations/quality resource"
  → Grant: mfg-role-quality-analyst → LF-Tag: domain IN [operations, quality]
                                              AND classification IN [public, internal]

Policy: "supplier_manager can access supplier-domain resources"
  → Grant: mfg-role-supplier-manager → LF-Tag: domain IN [supplier]

Policy: "floor_manager can access operations gold only"
  → Grant: mfg-role-floor-manager → LF-Tag: layer=gold AND domain=operations

Policy: "auditor can access any layer but only SELECT"
  → Grant: mfg-role-auditor → LF-Tag: layer IN [bronze, silver, gold]
                               Permissions: SELECT, DESCRIBE only
```

### How ABAC Scales Without Re-granting

When a new table is added to `mfg_silver_db` with tags `domain=quality, classification=internal`:
- **No new LF grants are needed**
- Any principal with an LF-Tag grant for `domain=quality` automatically gains access
- This is the key advantage over RBAC: add a table, it inherits access from its tags

---

## 7. RBAC: Human Persona Roles and Permissions

### Personas and Their Access Matrix

#### Data Engineer
```
Identity:   Okta group: aws-mfg-data-engineers
IAM Role:   mfg-role-data-engineer
LF-Tags:    layer IN [bronze, silver, gold], domain IN [*]
Permissions:
  ALL databases:  CREATE_TABLE, ALTER, DROP, DESCRIBE
  ALL tables:     SELECT, INSERT, DELETE, ALTER, DROP, DESCRIBE
  ALL columns:    no restrictions
  Row filters:    none
```

#### Quality Analyst
```
Identity:   Okta group: aws-mfg-quality-analysts
IAM Role:   mfg-role-quality-analyst
LF-Tags:    domain IN [operations, quality], classification IN [public, internal]
Permissions:
  mfg_gold_db:    DESCRIBE on database
  gold_defect_analysis:     SELECT, DESCRIBE
  gold_operation_summary:   SELECT, DESCRIBE
  mfg_silver_db:  DESCRIBE on database
  silver_operation_log:     SELECT, DESCRIBE
  silver_operator:          SELECT on specific columns only (see column-level section)
  DENIED:         mfg_bronze_db.*, silver_operator.hire_date, bronze_operator.*
  Row filters:    none (sees all rows)
```

#### Supplier Manager
```
Identity:   Okta group: aws-mfg-supplier-managers
IAM Role:   mfg-role-supplier-manager
LF-Tags:    domain IN [supplier]
Permissions:
  gold_supplier_performance: SELECT, DESCRIBE
  silver_supplier:           SELECT, DESCRIBE (no contact_email column)
  DENIED:   all operations/quality/hr tables
  DENIED:   silver_supplier.contact_email column
```

#### Floor Manager
```
Identity:   Okta group: aws-mfg-floor-managers
IAM Role:   mfg-role-floor-manager
LF-Tags:    layer=gold, domain=operations
Permissions:
  gold_operation_summary: SELECT, DESCRIBE
  DENIED: silver_*, bronze_*, gold_defect_analysis, gold_supplier_performance
Row filter (ABAC-driven):
  WHERE workstation_id = aws:PrincipalTag/workstation
  → Each floor manager only sees rows for their assigned workstation
```

#### Executive
```
Identity:   Okta group: aws-mfg-executives
IAM Role:   mfg-role-executive
Permissions:
  mfg_gold_db.*: SELECT, DESCRIBE (all gold tables)
  DENIED: silver_*, bronze_*
  DENIED: any column tagged pii=true
```

#### Auditor
```
Identity:   Okta group: aws-mfg-auditors
IAM Role:   mfg-role-auditor
Permissions:
  ALL layers: SELECT, DESCRIBE (read everything)
  NO write operations (INSERT, DELETE, ALTER, DROP denied)
  CloudTrail logs: read-only
  Lake Formation audit logs: read-only
```

---

## 8. Machine Roles and DAG Identity Guardrails

### The Problem with Shared Machine Roles

Today, all Glue jobs run as `GlueServiceRole-MfgLakehouse` — a single shared role
with admin-level access. This violates SOX segregation of duties:

```
Problem:
  Engineer A (finance team)    → schedules Glue job → uses GlueServiceRole (admin)
  Engineer B (operations team) → schedules Glue job → uses GlueServiceRole (admin)
  Both can read/write any table, even across domains
```

### Target: Job Carries Owner's Permission

```
Solution:
  Engineer A (finance team)    → schedules Glue job
                                 → job assumes mfg-glue-finance-role
                                 → mfg-glue-finance-role has SAME permissions as
                                    mfg-role-finance (domain=finance only)
                                 → job CAN write to fdw_prod tables
                                 → job CANNOT write to edw_dims tables

  Engineer B (operations team) → schedules Glue job
                                 → job assumes mfg-glue-operations-role
                                 → mfg-glue-operations-role has SAME permissions as
                                    mfg-role-data-engineer for operations domain
                                 → job CANNOT write to fdw_prod tables → JOB FAILS
```

### Machine Role Design

Each machine role mirrors a human role's permissions but is an **IAM service role**
(trusted by `glue.amazonaws.com`):

```
Human Role                  Machine Role (for Glue)
mfg-role-finance       →    mfg-glue-role-finance
mfg-role-quality-analyst →  mfg-glue-role-quality
mfg-role-data-engineer  →   mfg-glue-role-data-engineer
mfg-role-supplier-manager → mfg-glue-role-supplier
```

### Assigning a Machine Role to a Glue Job (Owner Binding)

When a user creates a Glue job, they can only select machine roles they are
**authorized to delegate**:

```
IAM Permission: iam:PassRole
  Engineer A (finance) can PassRole for: mfg-glue-role-finance ONLY
  They CANNOT PassRole for: mfg-glue-role-data-engineer

Policy on mfg-role-finance IAM role:
{
  "Statement": [{
    "Effect": "Allow",
    "Action": "iam:PassRole",
    "Resource": "arn:aws:iam::*:role/mfg-glue-role-finance",
    "Condition": {
      "StringEquals": {
        "iam:PassedToService": "glue.amazonaws.com"
      }
    }
  }]
}
```

### DAG / Job Guardrail: Write to Protected Namespace Fails

```
Scenario: A data scientist tries to write to mfg_silver_db.silver_operator
          using their machine role (mfg-glue-role-quality)

  Job code:   df.writeTo("glue_catalog.mfg_silver_db.silver_operator").append()
  Job role:   mfg-glue-role-quality
  LF grants:  mfg-glue-role-quality → SELECT, DESCRIBE only (no INSERT/DELETE)

  Result at runtime:
    Lake Formation: DENIED — mfg-glue-role-quality does not have INSERT
                             on silver_operator
    Glue job fails: AccessDeniedException
    CloudTrail records: denied write attempt by mfg-glue-role-quality on silver_operator
```

This ensures that no ETL job can escalate its privileges beyond what its owner is
allowed to do — even if the code explicitly tries to.

---

## 9. Fine-Grained Table, Column, and Row-Level Security

### Table-Level Access

Implemented via **Lake Formation table grants** (RBAC part):

```sql
-- Grant quality analyst SELECT on silver_operation_log only
GRANT SELECT ON mfg_silver_db.silver_operation_log TO mfg-role-quality-analyst;
-- Deny access to all other silver tables implicitly (no grant = no access)
```

### Column-Level Access

Implemented via **Lake Formation column-level grants** using `TableWithColumns`:

```
Quality analyst: DENIED access to hire_date in silver_operator

Lake Formation column grant:
  Principal: mfg-role-quality-analyst
  Resource:  mfg_silver_db.silver_operator
  Columns:   ALL EXCEPT [hire_date]
  Permission: SELECT
```

When the analyst queries `silver_operator`:
```sql
-- This works:
SELECT operator_id, operator_name, workstation_id, shift, status
FROM mfg_silver_db.silver_operator WHERE is_current = true;

-- This fails:
SELECT operator_id, hire_date FROM mfg_silver_db.silver_operator;
-- AccessDeniedException: No SELECT on column hire_date
```

### Row-Level Access (Data Filters)

Implemented via **Lake Formation row-level security (data filters)**:

#### Floor Manager: Workstation Filter

```
Filter name: workstation_filter
Expression:  workstation_id = '${aws:PrincipalTag/workstation}'

Applied to:  silver_operation_log, gold_operation_summary
Applied to:  Principal: mfg-role-floor-manager
```

At query time, Lake Formation injects the filter automatically:
```sql
-- Floor manager Alice (workstation=WS-A) runs:
SELECT * FROM mfg_silver_db.silver_operation_log;

-- Athena actually executes:
SELECT * FROM mfg_silver_db.silver_operation_log
WHERE workstation_id = 'WS-A';   ← injected by LF data filter

-- Floor manager Bob (workstation=WS-B) sees:
WHERE workstation_id = 'WS-B';   ← different filter, same policy
```

#### Supplier Manager: Supplier Filter

```
Filter expression: supplier_id IN (
    SELECT supplier_id FROM authorized_suppliers
    WHERE manager_id = '${aws:PrincipalTag/user_id}'
)
```

This can be simplified for our project to a tag-based match:
```
Expression: supplier_id LIKE '${aws:PrincipalTag/supplier_scope}'
```

### Column Masking (PII)

For columns tagged `pii=true`, apply dynamic data masking to non-privileged users:

```
Data mask on silver_operator.operator_name for non-hr roles:
  Mask expression: CONCAT(LEFT(operator_name, 1), '***')
  → "Alice Johnson" becomes "A***"

Applied to:  any principal NOT in [mfg-role-data-engineer, mfg-role-auditor]
```

Lake Formation does not natively support dynamic masking (as of 2025); options:
1. **Column exclusion** — deny access to the column entirely (simpler)
2. **Athena UDF masking** — create a view that applies `regexp_replace()` masking
3. **AWS Macie** — detect and flag PII in S3, integrate with access policy decisions

---

## 10. Schema-Level Namespace Separation

Mapping the use case's representative namespaces to this project:

| Use Case Namespace | This Project Equivalent | Domain | Who Can Access |
|-------------------|------------------------|--------|----------------|
| `edw_dims` (inventory) | `mfg_silver_db` (dimensions) | operations | data-engineer, quality-analyst |
| `fdw_prod` (revenue) | `mfg_gold_db` (reports) | operations, supplier | analyst, executive, supplier-manager |
| `edw_stg` (staging) | `mfg_bronze_db` (raw) | operations | data-engineer, auditor only |

### Namespace Isolation: Cross-Schema Access Denial

```
Scenario: A supplier manager tries to query bronze data

  User: mfg-role-supplier-manager
  Query: SELECT * FROM mfg_bronze_db.bronze_operation_log;

  Lake Formation check:
    Resource tags: layer=bronze, domain=operations, classification=confidential
    Principal grants: domain IN [supplier] only
    domain=operations ≠ domain=supplier → DENIED

  Result: AccessDeniedException
  Audit log records the denied access attempt
```

### Schema-Level Tag Assignment at Database Level

```
Tagging mfg_bronze_db: classification=confidential → only data-engineer + auditor
Tagging mfg_silver_db: classification=internal     → data-engineer + analysts
Tagging mfg_gold_db:   classification=public       → all authenticated roles

Inheritance: Tables inherit parent database tags unless overridden at table level
```

---

## 11. Granular DDL / DML Permission Matrix

The following matrix shows what each role is **allowed** and **denied** for each operation.
`~` means the permission exists but may have conditions (e.g., only for specific tables).

| Permission | data-engineer | quality-analyst | supplier-manager | floor-manager | finance | auditor |
|-----------|:---:|:---:|:---:|:---:|:---:|:---:|
| **SELECT** | ✅ all | ✅ ops+quality | ✅ supplier | ✅ gold ops | ✅ finance | ✅ all |
| **INSERT** | ✅ all | ❌ | ❌ | ❌ | ~ finance | ❌ |
| **UPDATE** | ✅ all | ❌ | ❌ | ❌ | ~ finance | ❌ |
| **DELETE** | ✅ all | ❌ | ❌ | ❌ | ~ finance | ❌ |
| **CREATE TABLE** | ✅ | ❌ | ❌ | ❌ | ~ finance db | ❌ |
| **ALTER** | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **DROP** | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **DESCRIBE** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |

### Permissions Currently Available in Lake Formation (GA) vs Preview

| Permission | GA Status |
|-----------|-----------|
| SELECT (table-level) | ✅ GA |
| SELECT (column-level) | ✅ GA |
| Row-level data filters | ✅ GA |
| INSERT | ✅ GA |
| DELETE | ✅ GA |
| ALTER | ✅ GA |
| DROP | ✅ GA |
| CREATE_TABLE | ✅ GA |
| LF-Tag-based grants (TBAC) | ✅ GA |
| Dynamic column masking | ⚠️ Via views/UDF (not native LF) |
| Cell-level security | ⚠️ Preview (LF row filter + column exclusion combination) |

---

## 12. Agent Inheritance (User-to-Machine and Machine-to-Machine)

### Principle

> An agent (Glue job, Lambda, Step Functions) acting on behalf of a user must
> inherit that user's permission set. The IAM role may differ; the permissions must not.

### Implementation Pattern

#### User-to-Machine (human schedules a Glue job)

```
1. Engineer Alice (mfg-role-quality-analyst) creates Glue job
   She can only pass mfg-glue-role-quality (iam:PassRole restriction)

2. Glue job runs as mfg-glue-role-quality
   LF grants on mfg-glue-role-quality = SAME as mfg-role-quality-analyst
   (SELECT on ops/quality tables, no INSERT, no PII columns)

3. Job tries to INSERT into silver_operator
   → Lake Formation DENIES (mfg-glue-role-quality has no INSERT on silver_operator)
   → Job fails → CloudTrail records the attempt
```

#### Machine-to-Machine (Glue job invokes Lambda)

```
1. Glue job running as mfg-glue-role-quality invokes Lambda function
2. Lambda function has its own execution role: mfg-lambda-role-quality
3. mfg-lambda-role-quality is granted SAME LF permissions as mfg-glue-role-quality

IAM Resource-Based Policy on Lambda (prevents privilege escalation):
  Only allow invocation from roles that carry matching domain tag:
  Condition: aws:PrincipalTag/domain = lambda:FunctionTag/domain
```

#### Step Functions Workflow

```
Step Functions state machine carries:
  ExecutionRole: mfg-sfn-role-{owner-domain}

IAM condition on IAM role for Glue (called by Step Functions):
  The Glue job role that Step Functions can start is limited by:
  Condition: iam:AssociatedResourceArn must match allowed job names
```

---

## 13. Vended S3 Credentials and Storage-Level Enforcement

### How Lake Formation Vends S3 Credentials

When a principal (Athena, Glue) requests access to an Iceberg table, Lake Formation
does not give them the S3 bucket credentials directly. Instead:

```
1. Athena: "I need to read silver_operation_log for principal mfg-role-quality-analyst"

2. Lake Formation checks LF grants:
   mfg-role-quality-analyst has SELECT on silver_operation_log? → YES

3. Lake Formation calls STS to get short-lived credentials scoped to:
   Resource:  arn:aws:s3:::mfg-lakehouse-*/silver/operation_log/*
   Action:    s3:GetObject
   Duration:  15 minutes (configurable)

4. Returns vended credentials to Athena
   Athena reads ONLY the files under silver/operation_log/
   Cannot access silver/operator/ even if it tries

5. After 15 minutes, credentials expire
   Next query gets fresh vended credentials
```

### Why This Matters for Compliance

- A quality analyst's session **physically cannot** read Bronze files, even if they
  construct a raw S3 API call — the credentials are scoped to permitted paths only
- Principle of least privilege enforced at the storage layer, not just the query layer
- **Data residency**: S3 data never leaves the AWS account; vended credentials are
  account-internal, not cross-account (unless explicitly configured)

### Vended Credential Scope Examples

| Principal | Permitted LF Tables | Vended S3 Scope |
|-----------|--------------------|--------------------|
| mfg-role-quality-analyst | silver_operation_log, gold_defect_analysis | `s3://.../silver/operation_log/*` + `s3://.../gold/defect_analysis/*` |
| mfg-role-supplier-manager | gold_supplier_performance | `s3://.../gold/supplier_performance/*` |
| mfg-role-data-engineer | all tables | `s3://.../bronze/*` + `s3://.../silver/*` + `s3://.../gold/*` |

---

## 14. Data Residency and Enforcement Processing Location

### Where Access Enforcement Executes

| Component | Executes In | Data Crosses Account Boundary? |
|-----------|-------------|-------------------------------|
| Okta SAML assertion | Okta cloud → AWS | Assertion (metadata) only, no data |
| IAM role assumption (STS) | AWS us-east-1 | No — stays in AWS |
| Lake Formation grant check | AWS us-east-1 | No — policy evaluation only |
| Vended S3 credentials (STS) | AWS us-east-1 | No — short-lived token only |
| Athena query execution | AWS us-east-1 | No — data stays in S3 (same account) |
| Glue job execution | AWS us-east-1 | No — runs inside AWS VPC |
| Query result | S3 athena-results/ | No — stays in same bucket/account |

### Residency Guarantees

All data processing (query execution, ETL, storage) happens within the customer's
AWS account (`442042510197`) in `us-east-1`. No data is processed outside this boundary:

- Lake Formation does not move data out of the account to check permissions
- Athena executes queries within the account's compute environment
- Glue jobs run in managed containers within the account's region
- S3 data at rest: encrypted with SSE-S3 or SSE-KMS, stored in `us-east-1`

### GDPR Implications

For EU data (if operators or suppliers are EU-based):
- Deploy a **separate AWS account or region** (`eu-west-1`) for EU data
- Cross-region data transfer is blocked by IAM condition: `aws:RequestedRegion`
- Lake Formation resource links can provide unified catalog view without moving data

---

## 15. SOX Audit Trail and Change Control

### What Must Be Captured (SOX Section 404)

| Event | Who Changed | What Changed | When | Where Stored |
|-------|------------|-------------|------|-------------|
| User accesses a table | Principal ARN | Table name, columns queried | Timestamp | CloudTrail |
| User is DENIED access | Principal ARN | Table name, error | Timestamp | CloudTrail |
| LF permission granted | Admin ARN | Principal + resource + permission | Timestamp | CloudTrail |
| LF permission revoked | Admin ARN | Principal + resource + permission | Timestamp | CloudTrail |
| Schema change (ALTER TABLE) | Principal ARN | Table, column added/removed | Timestamp | CloudTrail + Glue catalog |
| Table created | Principal ARN | Table name, location | Timestamp | CloudTrail |
| Table dropped | Principal ARN | Table name | Timestamp | CloudTrail |
| Glue job run (ETL job) | Job ARN + IAM role | Input params, tables written | Timestamp | CloudTrail + Glue logs |
| S3 file accessed | IAM principal | S3 key, operation | Timestamp | S3 Access Logs |

### Audit Log Architecture

```
  CloudTrail (all API events)
      │
      ├── Glue API: CreateJob, StartJobRun, UpdateJob, DeleteJob
      ├── LF API:   GrantPermissions, RevokePermissions, GetDataAccess
      ├── S3 API:   GetObject, PutObject, DeleteObject
      ├── IAM API:  AssumeRole, AssumeRoleWithSAML
      ├── Athena:   StartQueryExecution, GetQueryResults
      ▼
  S3 bucket: s3://mfg-lakehouse-ACCOUNT/audit-logs/cloudtrail/
  (immutable, versioned, MFA-delete enabled, WORM via S3 Object Lock)
      │
      ├── CloudWatch Logs (real-time alerting)
      │       Alert: DENIED access attempt by any principal
      │       Alert: LF permission granted to new principal
      │       Alert: DROP TABLE executed
      │
      └── Athena (query audit records for auditors)
              SELECT principal, action, resource, timestamp
              FROM audit_logs
              WHERE action = 'DENIED' AND timestamp > ...
```

### Audit Log Retention

| Log Type | Retention | Storage Class | Who Can Access |
|----------|----------|---------------|----------------|
| CloudTrail | 7 years | S3 Glacier after 90 days | Auditor role only |
| LF audit logs | 7 years | S3 Glacier after 90 days | Auditor role only |
| S3 access logs | 1 year | S3 Standard | Auditor role only |
| CloudWatch Logs | 90 days | CloudWatch | Data engineer + auditor |

### Immutability (SOX Change Control)

Audit logs must be tamper-proof:

```
S3 bucket policy for audit-logs/:
  - MFA Delete enabled
  - Object Lock: COMPLIANCE mode, 7-year retention
  - No role (not even admin) can delete before retention expires
  - Versioning enabled: overwrites create new version, old version preserved

IAM Policy:
  No principal has s3:DeleteObject on audit-logs/ bucket
  Even GlueServiceRole-MfgLakehouse is denied:
  {
    "Effect": "Deny",
    "Action": ["s3:DeleteObject", "s3:DeleteObjectVersion"],
    "Resource": "arn:aws:s3:::mfg-audit-ACCOUNT-us-east-1/*"
  }
```

### Example: Auditor Queries the Audit Trail (Athena)

```sql
-- Who accessed silver_operator in the last 30 days?
SELECT
    userIdentity.arn     AS principal,
    eventName            AS action,
    requestParameters    AS resource,
    eventTime            AS accessed_at,
    errorCode            AS denied_reason
FROM audit_logs.cloudtrail_logs
WHERE requestParameters LIKE '%silver_operator%'
  AND eventTime > NOW() - INTERVAL '30' DAY
ORDER BY eventTime DESC;

-- Who changed Lake Formation permissions this quarter?
SELECT
    userIdentity.arn AS admin_who_changed,
    requestParameters AS permission_change,
    eventTime
FROM audit_logs.cloudtrail_logs
WHERE eventSource = 'lakeformation.amazonaws.com'
  AND eventName IN ('GrantPermissions', 'RevokePermissions', 'PutDataLakeSettings')
  AND eventTime > '2024-01-01'
ORDER BY eventTime;

-- Which jobs wrote to the finance namespace?
SELECT
    userIdentity.arn AS role_used,
    requestParameters AS table_written,
    eventTime
FROM audit_logs.cloudtrail_logs
WHERE eventSource = 'glue.amazonaws.com'
  AND eventName = 'StartJobRun'
  AND requestParameters LIKE '%fdw_prod%'
ORDER BY eventTime;
```

---

## 16. Step-by-Step: How a Query Flows Through the Stack

This section traces a **complete end-to-end request** for a Quality Analyst
querying the defect analysis table.

```
┌─────────────────────────────────────────────────────────────────────────┐
│  STEP 1: User authenticates via Okta                                    │
│                                                                         │
│  Alice opens AWS Console / Athena / data tool                          │
│  → Redirected to Okta SSO                                              │
│  → Alice enters credentials + MFA                                      │
│  → Okta validates identity                                             │
│  → Okta generates SAML assertion:                                      │
│       NameID:     alice@company.com                                    │
│       Groups:     aws-mfg-quality-analysts                             │
│       PrincipalTag/department: quality                                 │
│       PrincipalTag/clearance:  internal                                │
└─────────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  STEP 2: AWS IAM assumes role with session tags                        │
│                                                                         │
│  AWS STS.AssumeRoleWithSAML:                                           │
│    RoleArn: mfg-role-quality-analyst                                   │
│    SAMLAssertion: <Okta SAML blob>                                     │
│                                                                         │
│  IAM validates:                                                        │
│    - SAML signature valid? YES                                         │
│    - Role trust allows this IdP? YES                                   │
│                                                                         │
│  IAM creates STS session:                                              │
│    AccessKeyId: ASIA...                                                │
│    SessionToken: ...                                                   │
│    SessionTags: {department=quality, clearance=internal}               │
│    Expiry: 8 hours                                                     │
└─────────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  STEP 3: Alice runs query in Athena                                    │
│                                                                         │
│  SELECT defect_type, defect_count, defect_pct                         │
│  FROM mfg_gold_db.gold_defect_analysis                                │
│  ORDER BY defect_rank;                                                 │
│                                                                         │
│  Athena receives query with Alice's session credentials                │
└─────────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  STEP 4: Athena calls Glue Data Catalog to resolve table               │
│                                                                         │
│  Glue.GetTable(DatabaseName=mfg_gold_db, Name=gold_defect_analysis)   │
│  → Returns: table schema, Iceberg metadata_location                   │
└─────────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  STEP 5: Lake Formation access check                                   │
│                                                                         │
│  Athena calls LF.GetDataAccess:                                        │
│    Principal: mfg-role-quality-analyst (with session tags)             │
│    Resource: mfg_gold_db.gold_defect_analysis                         │
│    Permission: SELECT                                                  │
│                                                                         │
│  Lake Formation evaluates:                                             │
│    1. Does mfg-role-quality-analyst have an LF grant for this table?  │
│       LF-Tag grant: domain IN [operations, quality] → YES             │
│    2. Does gold_defect_analysis have domain=quality tag? → YES        │
│    3. Does classification=public allow clearance=internal? → YES      │
│    4. Any row-level filters? → NO (no filter on gold_defect_analysis) │
│    5. Any column exclusions? → NO (no PII columns here)              │
│                                                                         │
│  Result: GRANTED                                                       │
└─────────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  STEP 6: Lake Formation vends scoped S3 credentials                   │
│                                                                         │
│  LF calls STS to create short-lived credentials:                      │
│    AllowedS3Actions: s3:GetObject                                      │
│    AllowedS3Resources:                                                 │
│      arn:aws:s3:::mfg-lakehouse-.../gold/defect_analysis/*            │
│    Duration: 15 minutes                                                │
│                                                                         │
│  Returns vended credentials to Athena                                  │
│  NOTE: Alice's session CANNOT access any other S3 prefix with these  │
│        credentials                                                     │
└─────────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  STEP 7: Athena reads Iceberg metadata and data files                 │
│                                                                         │
│  Using vended credentials:                                             │
│    1. Read metadata_location from Glue catalog                        │
│       → s3://.../gold/defect_analysis/metadata/v3.metadata.json       │
│    2. Read manifest list → find data files for requested partition    │
│    3. Read Parquet data files                                         │
│    4. Apply any row filters (none in this case)                       │
│    5. Project columns requested (defect_type, defect_count, etc.)    │
└─────────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  STEP 8: Result returned + audit logged                                │
│                                                                         │
│  Athena writes query result to:                                        │
│    s3://mfg-lakehouse-.../athena-results/alice-session-xyz.csv        │
│    (Alice has s3:PutObject on athena-results/ only)                   │
│                                                                         │
│  CloudTrail records:                                                   │
│    Principal: arn:aws:sts::442042510197:assumed-role/                  │
│               mfg-role-quality-analyst/alice@company.com               │
│    Action:    athena:StartQueryExecution                               │
│    Resource:  mfg_gold_db.gold_defect_analysis                        │
│    Result:    SUCCESS                                                  │
│    Time:      2024-02-02T10:30:00Z                                    │
└─────────────────────────────────────────────────────────────────────────┘
```

### Same Flow for a DENIED Request

If Alice tries to query `mfg_bronze_db.bronze_operator`:
- Step 5 fails: `bronze_operator` has tags `layer=bronze, domain=hr, classification=confidential`
- mfg-role-quality-analyst has no LF grant for `layer=bronze` or `domain=hr`
- Lake Formation returns DENIED
- No S3 credentials are vended — Alice's session cannot touch Bronze files at all
- CloudTrail records the denied attempt

---

## 17. Implementation Roadmap

| Phase | Component | Effort | Dependencies |
|-------|-----------|--------|-------------|
| **Phase 1** | Define LF-Tags schema + tag all existing tables | Low | Lake Formation admin |
| **Phase 1** | Create IAM roles (human: 6 roles, machine: 4 roles) | Medium | IAM admin |
| **Phase 1** | Apply LF-Tag grants per role | Medium | Phase 1 tags done |
| **Phase 2** | Configure Okta AWS app (SAML federation) | Medium | Okta admin + IAM |
| **Phase 2** | Map Okta groups → IAM roles | Low | Phase 2 Okta app |
| **Phase 2** | Add session tag passthrough (Okta → IAM) | Medium | Phase 2 federation |
| **Phase 3** | Implement `iam:PassRole` restrictions for machine roles | Low | Phase 1 machine roles |
| **Phase 3** | Apply column-level exclusions for PII columns | Low | Phase 1 LF setup |
| **Phase 3** | Create row-level data filters (workstation, supplier) | Medium | Phase 3 column grants |
| **Phase 4** | Enable CloudTrail + S3 access logs to audit bucket | Low | S3 audit bucket (WORM) |
| **Phase 4** | Create Athena tables over audit logs | Low | Phase 4 CloudTrail |
| **Phase 4** | Set up CloudWatch alerts for DENIED events | Low | Phase 4 CloudTrail |
| **Phase 5** | End-to-end ABAC test for each persona | Medium | All phases complete |
| **Phase 5** | SOX audit walkthrough with auditor persona | Low | Phase 4 complete |
| **Phase 5** | Document enforcement processing location for compliance | Low | Phase 5 tests done |

**Total estimated effort:** 4–6 weeks for a team of 2 engineers with AWS governance experience.

---

*This is a design document only. No changes to the existing codebase or AWS resources
have been made. See [13_deployment_guide.md](13_deployment_guide.md) for the current
deployment state of the project.*
