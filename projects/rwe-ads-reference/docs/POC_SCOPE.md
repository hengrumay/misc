# RWE ADS Automation — PoC Scope & Success Criteria

> **⚠️ As-built note (current bundle).** This document describes the original PoC design. As deployed today the pipeline is **deterministic + native**: orchestration is **Databricks Workflows** (no Agent Bricks Supervisor); the ADS build **assembles approved-SQL KB templates + validates via `EXPLAIN`** (no model generates queries); serving is **Lakebase Autoscaling** (the retired Provisioned-tier references below — `w.database`, `<your-lakebase-project>`, `list-synced-tables --instance` — no longer apply; provision via `databricks postgres create-project`); and **Genie** (analyst Q&A) ships as a **disabled config placeholder** — not built or deployed in this bundle. For current deploy commands follow **`CLAUDE.md`** and **`.claude/skills/deploy-full-bundle`**.


**Project:** Analysis-Ready Dataset (ADS) Automation over Synthetic Real-World Data  
**Workspace:** <your-workspace>.cloud.databricks.com  
**Persona:** MD Epidemiology / Real-World Data Science (Epi-RWDS)  
**Status:** Active PoC  

---

## 1. Executive Summary

This PoC demonstrates an **automated, validated, reproducible** workflow for transforming a clinical study protocol into an analysis-ready dataset (ADS), with:

- **Deterministic SQL generation** from an approved-SQL knowledge base (avoiding hallucination via KB grounding)
- **Human review gate** before any dataset is "approved" for analysis (real code; the demo default `allow_auto_esign: true` auto-signs eval-passing specs under a SYSTEM actor — set `false` to require it)
- **Full reproducibility** (protocol version, KB snippets, source table versions, e-signature)
- **GxP audit trail** (immutable, hash-chained event log)
- **PHI containment controls** (gateway masking + policy enforcement)
- **Low-latency serving** from Lakebase (synced Delta → Postgres)

The PoC runs over **synthetic real-world data** (deterministically seeded, reproducible) to safely demonstrate controls and governance patterns without real PHI risk. Success enables a clear migration path to your production workspace (production environment) with minimal changes to the core logic.

---

## 2. In-Scope Features

### 2.1 Protocol Processing
- Study protocol upload (PDF/DOCX) to protocols volume
- Document Intelligence parsing (extract cohort, inclusion/exclusion, outcomes, covariates)
- Protocol versioning via content hash

### 2.2 SQL Generation & KB
- Approved-SQL knowledge base (versioned, indexed snippets)
  - Categories: cohort, inclusion, exclusion, derivation, outcome, assembly
  - Parameterized templates with {{gold}}, {{silver}}, {{cohort}} tokens
- KB-grounded SQL generation agent (Supervisor Agent + custom Agent) *(originally-intended — superseded, not built; as built the ADS Builder assembles approved-SQL KB templates with no model call, orchestrated by native Databricks Workflows. See the as-built note at the top.)*
- Validation layer:
  - Syntax check (SELECT/CTE only, no DML/DDL)
  - Egress guard (only gold/silver/raw schemas allowed, no external DBs)
  - Schema validation (table/column existence in gold)
  - Dry-run (LIMIT 0) to surface runtime errors early
  - Plausibility (cohort N, outcome rates within expected range)

### 2.3 Data Medallion (Synthetic RWD)
- **Bronze**: Synthetic landing (deterministically seeded, 50k patients, 2018–2024)
- **Silver**: Conformed common data model
  - `patient`: demographics, enrollment spans
  - `patient_timeline`: events (diagnoses, medications, labs, encounters)
  - `eligibility_periods`: insurance eligibility windows
- **Gold**: Analytic base tables
  - `patient_timeline`: union of all events
  - `eligibility_periods`: post-processed spans
  - `ads_output`: final ADS (one row per patient with all covariates/outcomes)
  - `cohort_summary`: cohort-level metrics (N, demographics, outcome rates)

### 2.4 Analyst Review Gate
- A model-based eval flags/blocks weak extractions and sorts a worst-first review queue
- The analyst reviews the extracted spec (with eval flags) and e-signs — **only an approved spec is built**; the built ADS (cohort N, demographics, outcome rates) is also reviewable in the app
- Decision: **Approve** (e-sign) or **Reject** (rework)
- E-signature recorded in the audit trail

### 2.5 Reproducibility & Audit
- **Reproducibility manifest** table: protocol version, KB snippet versions, generated SQL, source table Delta versions, model, agent version, eval scores, reviewer signature, decision
- **GxP audit event log**: immutable, hash-chained event trail
  - Events: ads_approval, kb_snippet_approval, review_gate_pass, review_gate_fail
- Both tables: append-only (no UPDATE/DELETE via GRANT/DENY)
- Hash chain verification: detect tampering

### 2.6 Serving via Lakebase
- Continuous synced tables (Delta → PostgreSQL, one-way)
  - `ads_output` → Lakebase `ads_output`
  - `cohort_summary` → Lakebase `cohort_summary`
- Low-latency read access from the app or downstream analytics
- Read-only (no app writes to synced tables)

### 2.7 PHI Containment Controls
- **Data**: Synthetic RWD (no real PHI)
- **AI Gateway**:
  - All model calls routed through `ads-ai-gateway`
  - PHI-aware masking enabled (SSN, MRN, email, phone, DOB patterns)
  - Contextual Service Policy: deny external tool egress
  - Cost attribution & rate limiting
- **Unity Catalog**: ABAC governance, lineage tracking

### 2.8 PoC Studies (Complexity Ladder)
Three studies span increasing complexity:

1. **poc_low**: Simple prevalence cohort
   - Cohort: patients with ≥1 ICD-10 code in a date window
   - Index date: first occurrence
   - Inclusion: continuous enrollment (90 days pre/post)
   - Outcomes: binary (event Y/N in follow-up window)
   - Success: cohort N, outcome rate distribution

2. **poc_med**: Drug-exposure new-user cohort w/ covariates
   - Cohort: first fill of a drug class (washout baseline)
   - Index date: first fill date
   - Inclusion: age range, continuous enrollment
   - Covariates: baseline diagnoses, comorbidities (flags)
   - Outcomes: time-to-event (Kaplan-Meier friendly)
   - Success: cohort N, covariate balance, outcome timing distribution

3. **poc_high**: Comparative outcomes w/ time-varying exposure
   - Cohort: drug-exposed patients with matched comparator
   - Index date: exposure start
   - Exposure eras: collapsing fills with grace periods (time-varying)
   - Covariates: baseline + time-varying (lab results, dose changes)
   - Outcomes: hazard ratio-friendly setup (censoring, follow-up windows)
   - Success: cohort N, exposure group balance, survival curves

---

## 3. Out-of-Scope (PoC Boundaries)

- **Real patient data**: Scope limited to synthetic (your production workspace analogue handles real data)
- **Custom ML models**: Only pre-approved model candidates (Sonnet, Opus, Llama, open-source embedding)
- **Knowledge base curation**: Seeded at wave0; new snippets require manual approval process (not in PoC)
- **Production deployment**: your production workspace requires separate charter, governance review, IT ops sign-off
- **Data quality monitoring**: Beyond reproducibility manifest + audit trail scope
- **Regulatory submission**: PoC is internal demo; submission-ready outputs require additional compliance
- **Multi-site federation**: Single workspace scope; multi-site federation is a future phase

---

## 4. Success Criteria

### 4.1 Functional Success
- [ ] **Protocol intake**: Analyst uploads protocol PDF; system extracts metadata (study window, cohort criteria, outcomes)
- [ ] **SQL assembly**: the builder composes valid, executable SQL from approved KB templates for each sample study (3 studies)
- [ ] **Validation passes**: Syntax, schema, egress, dry-run all pass; no external DB connections attempted
- [ ] **Medallion builds**: Bronze → Silver → Gold medallion executes end-to-end for each study
- [ ] **ADS output**: `ads_output` table has ≥1 row per study; cohort N in expected range
- [ ] **Analyst review gate**: App displays ADS preview; analyst can approve/reject with e-sign
- [ ] **Reproducibility**: Manifest table captures all metadata; hash chain verifies integrity
- [ ] **Serving**: Lakebase synced tables live; app can query approved ADS via Postgres

### 4.2 Governance & Compliance
- [ ] **GxP audit trail**: Immutable event log with hash chain; no tampering detected
- [ ] **PHI masking**: Gateway masks PHI patterns in all model I/O; audit log shows masked values
- [ ] **Egress control**: No external DB connections; all data stays in Databricks/Lakebase
- [ ] **Access control**: GRANT/DENY enforces append-only on audit tables; analysts SELECT-only
- [ ] **Validated SOP**: Work instruction (SOP) markdown generated; formal sign-off pending

### 4.3 Data Quality & Reproducibility
- [ ] **Deterministic seeding**: Synthetic RWD reproducible across runs (same seed → same rows)
- [ ] **SQL consistency**: Generated SQL matches protocol spec; cohort N, outcome rates plausible
- [ ] **Covariate distributions**: Baseline covariates show expected prevalence (e.g., >80% continuous enrollment)
- [ ] **Evaluation metrics**: Cohort N, demographics, outcome rates logged in manifest

### 4.4 Performance & Scalability
- [ ] **Medallion latency**: Bronze → Silver → Gold builds complete in <10 min (serverless)
- [ ] **Synced table latency**: Continuous sync from Delta to Lakebase <1 min lag
- [ ] **Query latency**: App queries on Lakebase <1 sec (50k row ADS, 10 col queries)
- [ ] **Cost**: PoC run cost <$50/month (synthetic, serverless, 50k rows)

### 4.5 User Experience
- [ ] **Protocol upload**: <30 sec end-to-end (file → parsed metadata)
- [ ] **SQL generation**: <2 min for poc_low, <5 min for poc_med/high (includes validation)
- [ ] **App navigation**: React UI responsive; analyst review flow intuitive
- [ ] **Error messages**: Clear guidance on validation failures (schema, egress, syntax)

---

## 5. Assumptions & Dependencies

### 5.1 Workspace Environment
- Workspace: **<your-workspace>.cloud.databricks.com**
- Catalog: **rwe_ads_catalog** (pre-existing)
- Schemas: Created by wave0 (ads_raw, ads_curated, ads_serving, ads_kb, ads_audit)
- Volumes: ads_raw.protocols (created by wave0)
- Compute: Serverless SQL warehouse + serverless jobs (enforced)

### 5.2 External Services
- **Unity AI Gateway**: `ads-ai-gateway` endpoint (PHI masking, rate limiting, cost tracking)
- **Lakebase**: `<your-lakebase-project>` instance (serving DB `ads_serving_pg`, app DB `ads_app`)
- **Model serving**: Databricks Marketplace models (Sonnet, Opus, Llama, embedding)

### 5.3 Knowledge Base
- Approved-SQL KB seeded at wave0 (9 snippet templates: cohort, inclusion, exclusion, derivation, outcome, assembly)
- KB index: Vector Search for snippet retrieval
- Vector embedding model: `databricks-gte-large-en` (no external API calls)

### 5.4 Security & Auth
- **Service principal**: App backend has SELECT on gold + INSERT on audit tables
- **Analyst**: Has SELECT on serving + audit (review gate, audit trail visibility)
- **Data flow**: No real PHI; synthetic only; gateway masking enforced by construction

---

## 6. Risks & Mitigations

| Risk | Likelihood | Mitigation |
|------|------------|-----------|
| KB snippets hallucinate SQL | Low | KB grounding + validation layer (egress, schema, dry-run) |
| Analyst review skipped | Medium | E-signature mandatory; system enforces (no serving without e-sign) |
| Lakebase sync latency >1min | Low | Continuous mode; test with large delta updates (future) |
| PHI accidentally logged | Low | Gateway masking + audit log masks before storage |
| Audit table tampering | Very Low | Hash chain; GRANT/DENY append-only; versioning via Delta |
| Synthetic data not representative | Medium | Use epidemiologic validation (age distribution, event rates vs. literature) |
| Governance council delays approval | High | Produce validated SOP early; pilot governance review in parallel |

---

## 7. Deployment & Rollout Plan

### Phase 1: Foundation (Wave 0)
- Schemas, volumes, KB seeding
- Synthetic RWD generator
- **Success**: Schemas created, KB table populated (9 snippets, version 1)

### Phase 2: Medallion (Waves 1–2)
- Bronze landing (synthetic ingestion)
- Silver transformation (common data model)
- **Success**: Gold tables exist with expected row counts

### Phase 3: ADS Builder & Validation (Waves 3–4)
- ADS builder (protocol → KB retrieval → approved-SQL template assembly, deterministic)
- Validation layer (syntax, egress, schema, dry-run)
- Supervisor agent for orchestration *(originally-intended — superseded, not built; orchestration is native Databricks Workflows. See the as-built note at the top.)*
- **Success**: the builder assembles valid SQL for poc_low study

### Phase 4: Audit & Serving (Wave 5)
- Reproducibility manifest + GxP audit tables
- Lakebase synced tables
- App review gate + e-signature
- **Success**: poc_low end-to-end (protocol → ADS → serving → approved)

### Phase 5: PoC Studies & Validation (Wave 6+, future)
- poc_med: covariates, baseline balancing
- poc_high: time-varying exposure, censoring
- Evaluation metrics (cohort N, outcome rates, covariate distributions)
- **Success**: All 3 studies pass evaluation; governance sign-off

---

## 8. Definition of Done (DoD)

A feature is **DONE** when:
1. Code is checked in with passing tests (`pytest tests/ -q`)
2. Idempotency verified (re-run wave without errors)
3. Config-driven (no hardcoded names; uses cfg())
4. Serverless only (no classic clusters)
5. No direct external DB connections
6. Audit trail captures all significant events
7. Documentation updated (CLAUDE.md, SOP)
8. Governance council aware (SOP draft reviewed, feedback incorporated)

---

## 9. Next Steps

1. **Wave 5 (Audit & Serving)**: Complete setup_audit.py, setup_sync.py, work_instruction generation
2. **Tests**: Run pytest locally; ensure all tests pass (config, idempotency, phi_mask, validation, audit)
3. **Documentation**: Generate validated SOP (docs/VALIDATED_WORK_INSTRUCTION.md)
4. **Integration**: Deploy via DAB (`databricks bundle deploy -t dev`); run wave0 → wave5 end-to-end
5. **PoC Studies**: Execute poc_low study; validate cohort N, outcome rates, SQL generated
6. **Governance**: Submit SOP + PoC results to Gen AI Governance Council for formal approval
7. **Production Readiness**: Plan your production workspace migration (edit demo.config.yaml, re-deploy)

---

## 10. Contact & Escalation

- **Workspace Owner**: <your-name> (<your-email>)
- **Governance**: Gen AI Governance Council (placeholder for actual org)
- **Escalation**: If SOP approval delays PoC progress, escalate to Chief Data Officer

---

*PoC Scope Document v1.0 — Last updated 2026-08-12*
