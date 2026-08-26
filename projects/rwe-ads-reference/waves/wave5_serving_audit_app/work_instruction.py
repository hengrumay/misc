"""Work instruction utilities for the ADS review process.

Provides helpers to construct SOP markdown from the actual pipeline configuration
and guardrails.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any


def render_work_instruction(
    config_dict: dict[str, Any],
    poc_studies: list[dict[str, Any]],
    timestamp: str | None = None,
) -> str:
    """Render a validated work instruction (SOP) markdown document.

    Args:
        config_dict: The demo.config.yaml config as a dict
        poc_studies: List of PoC study metadata
        timestamp: Optional timestamp string (for reproducibility)

    Returns:
        Markdown string suitable for doc output
    """
    if timestamp is None:
        timestamp = datetime.utcnow().isoformat() + "Z"

    # Extract key config sections
    initiative = config_dict.get("initiative", "rwe-ads-automation")
    workspace = config_dict.get("workspace", {})
    schemas = config_dict.get("schemas", {})
    lakebase = config_dict.get("lakebase", {})
    gateway = config_dict.get("gateway", {})
    gxp = config_dict.get("gxp", {})

    md = f"""# Validated Work Instruction: Analysis-Ready Dataset (ADS) Generation
**Project:** {initiative}

**Document Status:** ⚠️ **DRAFT** — Requires formal Gen AI Governance Council validation sign-off before use.

**Generated:** {timestamp}

**Workspace:** {workspace.get('host', '<your-workspace>.cloud.databricks.com')}
**Compute:** {workspace.get('compute', 'serverless')} (ENFORCED)

---

## 1. Purpose & Scope

This SOP defines the process for transforming a **study protocol** into an
**analysis-ready dataset (ADS)** over synthetic real-world data (RWD), with:

- **Deterministic SQL generation** from an approved-SQL knowledge base
- **Mandatory analyst review gate** (human-in-the-loop) before serving
- **Full reproducibility** (protocol version, KB snippets, SQL, source table versions, e-signature)
- **GxP audit trail** (immutable, hash-chained event log)
- **PHI containment controls** (Unity AI Gateway masking + policy enforcement)
- **Low-latency serving** via Lakebase synced tables

### In Scope
- Protocol intake & validation
- KB-grounded SQL generation (approved snippets only)
- Synthetic RWD transformation (bronze → silver → gold medallion)
- Analyst review & e-signature gate
- Serving to Lakebase (read-only)
- Audit trail & reproducibility

### Out of Scope
- Real patient data ingestion (scope limited to synthetic for PoC)
- Custom ML model development (pre-approved model candidates only)
- External knowledge base curation (seeded at wave0)
- Production deployment (your production workspace analogues require separate charter)

---

## 2. Roles & Responsibilities

| Role | Responsibilities |
|------|------------------|
| **Analyst** | Submit study protocol; review generated ADS; approve/reject with e-signature |
| **Reviewer** (optional) | Secondary review gate (compliance, domain expert) |
| **System Admin** | Provision workspace, manage schemas, service principals, Lakebase |
| **AI Gateway Operator** | Configure gateway, manage cost attribution, monitor PHI masking |

---

## 3. Step-by-Step Procedure

### 3.1. Protocol Intake
**Actor:** Analyst

1. Upload study protocol (PDF/DOCX) to {schemas.get('raw', 'ads_raw')} volume
2. Protocol parsed by Databricks AI Functions (`ai_parse_document` → `ai_extract`; extract: cohort criteria, inclusion/exclusion, outcomes, covariates)
3. Generate protocol_version hash; store metadata in ads_kb schema

**Controls:**
- Protocol must define: study window dates, cohort inclusion/exclusion, index date logic, outcome definitions
- PHI screening: protocol checked for personally identifiable info (name, MRN, etc.)

### 3.2. Parse → KB-Grounded SQL Generation
**Actor:** ADS Builder (deterministic — approved-SQL template substitution, no model call)

1. Retrieve approved-SQL KB snippets from {schemas.get('kb', 'ads_kb')}
2. Matches protocol criteria to KB categories (cohort, inclusion, exclusion, derivation, outcome)
3. Assembles parameterized SQL from approved KB templates:
   - Substitutes {{gold}}, {{silver}}, {{cohort}} tokens with fully-qualified schema names
   - Resolves code mappings (ICD-10-CM, NDC, CPT, LOINC from protocol)
4. Validation layer checks:
   - Syntax (SELECT or CTE-final-SELECT only, no DML/DDL)
   - Egress guard (references only {schemas.get('serving', 'ads_serving')} or {schemas.get('curated', 'ads_curated')})
   - Schema existence (all tables/columns exist in gold)

**Controls:**
- SQL generation always "validates, never executes" against real patient DB
  (no connection exists; safety by construction)
- Approved KB snippets are version-controlled with content_hash
- Generated SQL is reproducible per protocol + KB snapshot

**Gateway Guardrails:**
- LLM calls route directly to the pay-per-token FM endpoint; PHI detection + masking + audit logging happen in-process (lib/pipeline/gateway.py). {gateway.get('endpoint', 'ads-ai-gateway')} is the configured guardrail/logging wrapper name, not a routing hop
- Cost attribution: {gateway.get('cost_attribution_tag', dict())}
- Rate limit: {gateway.get('rate_limit_qpm', 300)} QPM
- Safety features: {gateway.get('guardrails', dict())}

### 3.3. Build Medallion (Bronze → Silver → Gold)
**Actor:** Spark Declarative Pipeline (serverless)

1. **Bronze** ({schemas.get('raw', 'ads_raw')}): Synthetic RWD landing (deterministically seeded, reproducible)
2. **Silver** ({schemas.get('curated', 'ads_curated')}): Conformed common data model (patient, medical_claim, pharmacy_claim, lab_result, etc.)
3. **Gold** ({schemas.get('serving', 'ads_serving')}): Analytic base tables + ADS output
   - `patient_timeline`: all events (diagnoses, medications, labs, encounters)
   - `eligibility_periods`: insurance eligibility spans
   - `ads_output`: final ADS (one row per patient)
   - `cohort_summary`: cohort metrics (N, outcomes, covariates)

**Sync to Lakebase:**
- Delta tables in {schemas.get('serving', 'ads_serving')} synced (one-way, continuous) to Postgres:
  - `ads_output` → Lakebase `ads_output`
  - `cohort_summary` → Lakebase `cohort_summary`
- Analyst queries low-latency results via Lakebase (read-only)

### 3.4. Validation & Analyst Review Gate
**Actor:** Analyst + Review Queue

1. Analyst views generated ADS in the app (React UI)
2. Analyst reviews:
   - Cohort N and demographics
   - Outcome event rates
   - Covariate distributions
   - Generated SQL (logged in audit trail)
3. Analyst decision: **Approve** → e-sign, or **Reject** → rework
4. E-signature recorded in {schemas.get('audit', 'ads_audit')} (immutable)

**Controls:**
- Reproducibility manifest captured (protocol ver, KB snippet vers, SQL, source table Delta versions, model, agent ver, eval scores)
- Audit event logged (append-only, hash-chained) with actor + timestamp + decision
- No ADS is "approved" without analyst e-signature

### 3.5. Serving & Consumption
**Actor:** Analytics consumers (read-only)

1. Approved ADS served from Lakebase (ads_output, cohort_summary)
2. Consumers run analytics / epidemiologic analyses on approved dataset
3. All reads logged via Lakebase audit trail + Databricks SQL warehouse audit

---

## 4. Data & Governance

### 4.1. PHI Containment
- **Data:** Synthetic RWD (no real PHI)
- **Model calls:** Route directly to the pay-per-token FM endpoint; PHI masked in-process before egress (lib/pipeline/gateway.py)
- **Gateway policy:** {gateway.get('egress_policy', 'deny_external')} (no external tool egress)
- **UC governance:** Lineage, ABAC, grants per role

### 4.2. Audit & Reproducibility
**Audit Tables** ({schemas.get('audit', 'ads_audit')}):
- `repro_manifest`: Protocol version, KB snippet versions, generated SQL, source table Delta versions, model, agent version, eval scores, reviewer signature, decision
- `gxp_audit`: Immutable event log (ads_approval, kb_snippet_approval, review_gate_pass/fail)

**Reproducibility:**
- Every ADS generation is deterministic and reproducible
- Source tables pinned via Delta time travel (version string stored in manifest)
- KB snippets versioned with content_hash
- Model + agent versions captured
- Generated SQL (with step names) stored in manifest

**GxP Compliance** (if enabled):
- Audit retention: {gxp.get('audit_retention_years', 25)} years
- E-signature required for: {', '.join(gxp.get('esignature_required_events', []))}
- Part 11 compliance: {gxp.get('part11', False)}

### 4.3. Permissions & Access Control
- **Data analysts:** SELECT on {schemas.get('serving', 'ads_serving')} (approved ADS only)
- **Reviewers:** SELECT on audit tables (read audit trail)
- **Service principals:** INSERT (app backend can write to audit tables), SELECT (read-only on synced Lakebase tables)
- **Audit tables:** Append-only (no UPDATE/DELETE; enforced via GRANT/DENY)

---

## 5. PoC Studies

"""

    for study in poc_studies:
        md += f"""
### {study.get('id', 'Unknown')}: {study.get('title', 'Untitled')}
**Complexity:** {study.get('complexity', 'N/A')}

"""

    md += f"""
---

## 6. Records & Audit

### 6.1 Records Retained
- Protocol (PDF/DOCX in volume)
- Protocol metadata (parsed, version hash)
- KB snapshot (snippet_id, version, content_hash at generation time)
- Generated SQL (per step in repro_manifest)
- Source table Delta versions (time-travel snapshot via version string)
- Model + agent versions (inference metadata)
- Eval scores (cohort N, outcome rates, covariate distributions)
- Analyst review + e-signature (decision, actor, timestamp)
- Audit events (immutable, hash-chained)

### 6.2 Audit Trail Verification
- Hash chain verified on demand: `verify_chain()` recomputes SHA256 hashes and detects tampering
- Audit tables are append-only (no UPDATE/DELETE possible by construction)

---

## 7. Revision History

| Date | Version | Author | Changes |
|------|---------|--------|---------|
| {timestamp[:10]} | 1.0 (DRAFT) | System | Initial SOP generated from config |

---

## 8. Approval & Sign-Off

**Status:** ⚠️ **DRAFT** — This SOP requires formal validation by the Gen AI Governance Council.

**Required Before Production:**
- [ ] Governance Council review & approval
- [ ] Security & compliance review (PHI masking, audit trail, e-signature)
- [ ] Legal sign-off (GxP, reproducibility, audit retention)
- [ ] IT Operations sign-off (Lakebase provisioning, sync stability)
- [ ] Quality Assurance sign-off (test coverage, validation controls)

---

*This document describes the ADS generation workflow for {initiative} PoC.*
*For deployment to your production workspace or other production environments, a new charter and SOP approval cycle is required.*
"""

    return md
