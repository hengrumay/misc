# Architecture — RWE ADS Automation

> **⚠️ As-built note (current bundle).** This document describes the original PoC design. As deployed today the pipeline is **deterministic + native**: orchestration is **Databricks Workflows** (no Agent Bricks Supervisor); the ADS build **assembles approved-SQL KB templates + validates via `EXPLAIN`** (no model generates queries); serving is **Lakebase Autoscaling** (the retired Provisioned-tier references below — `w.database`, `<your-lakebase-project>`, `list-synced-tables --instance` — no longer apply; provision via `databricks postgres create-project`); and **Genie** (analyst Q&A) ships as a **disabled config placeholder** — not built or deployed in this bundle. **As-built eval / gateway / e-sign truths (correcting the diagrams below):** the extraction eval gate's hard control is **Stage-1 deterministic validators** (`eval_ok`) — the Stage-3 model judges (via `gateway_call`) are **advisory** (review priority + flags only, degrading to Stage-1/2 when no endpoint/source text), so it is **not** a model-gated eval; the **egress-deny** shown in the gateway diagram is a **workspace policy** (`egress_policy: deny_external`), **not** enforced inside `gateway_call` (whose in-code controls are PII mask + audit log + MLflow trace); and the shipped demo sets **`allow_auto_esign: true`**, which auto-signs eval-passing specs under a SYSTEM actor, **bypassing the human analyst e-sign** shown below until set to `false`. For current deploy commands follow **`CLAUDE.md`** and **`.claude/skills/deploy-full-bundle`**.


**System Design for Protocol → Analysis-Ready Dataset with Reproducibility & Audit**

---

## 1. End-to-End Flow

```
Study Protocol (PDF/DOCX)
    ↓
Protocol Parsing (Document Intelligence)
    ↓
Protocol Metadata (cohort, inclusion, exclusion, outcomes)
    ↓
ADS Builder (deterministic) + KB Retrieval
    ↓
Generated SQL (validated against synthetic gold)
    ↓
Medallion Build (Bronze → Silver → Gold)
    ↓
ADS Output + Cohort Summary
    ↓
Analyst Review Gate (e-sign)
    ↓
Reproducibility Manifest (audit)
    ↓
Serve via Lakebase (low-latency read-only)
```

---

## 2. Component Architecture

### 2.1 Data Ingestion & Protocols

```
    ┌─────────────────────────┐
    │  Protocol Upload        │
    │  (PDF/DOCX)             │
    │  → protocols volume     │
    └────────┬────────────────┘
             ↓
    ┌─────────────────────────┐
    │  Document Intelligence  │
    │  Agent                  │
    │  → Extract metadata     │
    │  → Store in ads_kb      │
    └─────────────────────────┘
```

**Responsible:** Wave 5 app backend + Document Intelligence skill
**Output:** Protocol_version (content hash), metadata in ads_kb schema

---

### 2.2 Medallion Architecture (Synthetic RWD)

```
┌───────────────────────────────────────────────────────────────┐
│                    MEDALLION LAYERS                           │
├───────────────────────────────────────────────────────────────┤
│                                                               │
│ BRONZE (ads_raw)                                             │
│   synthetic_patients.delta          [50k rows, raw]          │
│   synthetic_claims.delta            [5M rows, raw]           │
│   → Deterministic seeding (seed=20260812, reproducible)      │
│                                                               │
│              ↓ (Wave 1: Synth Generation)                    │
│                                                               │
│ SILVER (ads_curated)                                         │
│   patient.delta                     [50k rows, PII-safe]     │
│   patient_timeline.delta            [10M rows, events]       │
│   encounter.delta                   [500k rows, visits]      │
│   eligibility_periods.delta         [100k rows, spans]       │
│   → Conformed common data model (Velox/OMOP-lite)           │
│                                                               │
│              ↓ (Wave 2: Conformation)                        │
│                                                               │
│ GOLD (ads_serving)                                           │
│   patient_timeline.delta            [10M rows, indexed]      │
│   eligibility_periods.delta         [100k rows, indexed]     │
│   ads_output.delta                  [50k rows, one-per-pat]  │
│   cohort_summary.delta              [1 row, agg metrics]     │
│   → Analytic-ready, indexed on patient_id                   │
│                                                               │
│              ↓ (Wave 3–4: ADS generation)                    │
│                                                               │
│ AUDIT (ads_audit)                                            │
│   repro_manifest.delta              [append-only]            │
│   gxp_audit.delta                   [hash-chained]           │
│   → Reproducibility + GxP compliance                         │
│                                                               │
└───────────────────────────────────────────────────────────────┘
```

**Compute:** Serverless SQL warehouse + Spark Declarative Pipelines
**Storage:** Delta (internal DBFS), Unity Catalog governance
**Lineage:** Tracked via UC lineage API (source → target)

---

### 2.3 KB & SQL Generation

```
┌─────────────────────────────────────┐
│  Approved-SQL Knowledge Base        │
│  ads_kb.approved_sql_kb            │
├─────────────────────────────────────┤
│ snippet_id       (VARCHAR)          │
│ category         (cohort|inclusion..│
│ sql_template     (with {{tokens}})  │
│ params_json      (schema)           │
│ status           (approved|draft)   │
│ version          (INT)              │
│ content_hash     (SHA256)           │
└──────────┬────────────────────────┬─┘
           │                        │
           ↓ (Vector embed)         ↓ (SQL parse)
    ┌─────────────────────┐  ┌──────────────────┐
    │ Vector Search Index │  │ KB Retrieval     │
    │ (similarity match)  │  │ (rule-based match│
    │                     │  │  + semantic rank)
    └─────────────────────┘  └──────────────────┘
           ↑                         ↑
           │                        │
    ┌──────┴─────────────┬─────────┴─────────┐
    │                    │                   │
Query (protocol criteria) Supervisor Agent   KB Ranking
                              │
                              ↓
                    ┌─────────────────────┐
                    │ ADS Builder         │
                    │ (matched snippets)  │
                    └──────┬──────────────┘
                           ↓
                    ┌─────────────────────┐
                    │ Token Substitution  │
                    │ {{gold}} → fqn      │
                    │ {{code}} → literals │
                    └──────┬──────────────┘
                           ↓
                    ┌─────────────────────┐
                    │ Validation Layer    │
                    │ • Syntax            │
                    │ • Egress            │
                    │ • Schema            │
                    │ • Dry-run (LIMIT 0) │
                    └──────┬──────────────┘
                           ↓
                    ┌─────────────────────┐
                    │ Valid SQL           │
                    │ (reproducible)      │
                    └─────────────────────┘
```

**Responsible:** Waves 3–4 (ADS Builder — deterministic template substitution, no model call). *(The "Supervisor Agent" in the diagram above was originally-intended orchestration — superseded, not built; orchestration is native Databricks Workflows. See the as-built note at the top.)*
**Gateway:** All model calls routed through `ads-ai-gateway` (PHI masking)
**Safety:** No dml/ddl; no external DBs; dry-run validation

---

### 2.4 Validation & Error Handling

```
Generated SQL
    ↓
┌─────────────────────────────────┐
│ 1. Syntax Check                 │
│    SELECT / WITH-SELECT only    │
│    → No INSERT/UPDATE/DELETE    │
└─────────┬───────────────────────┘
          ↓ (PASS)
┌─────────────────────────────────┐
│ 2. Egress Guard                 │
│    FROM/JOIN schema check       │
│    → Only gold, silver, raw     │
│    → No external hosts          │
└─────────┬───────────────────────┘
          ↓ (PASS)
┌─────────────────────────────────┐
│ 3. Schema Validation            │
│    Check table/column exists    │
│    in gold via Spark EXPLAIN    │
└─────────┬───────────────────────┘
          ↓ (PASS)
┌─────────────────────────────────┐
│ 4. Dry-Run (LIMIT 0)            │
│    Execute with LIMIT 0         │
│    → Surface syntax/schema errs │
│    → Estimate row count         │
└─────────┬───────────────────────┘
          ↓ (PASS)
┌─────────────────────────────────┐
│ 5. Plausibility Check           │
│    Cohort N in expected range   │
│    Outcome rate plausible       │
└─────────┬───────────────────────┘
          ↓ (PASS)
┌─────────────────────────────────┐
│ Valid SQL (safe to execute)     │
│ Ready for analyst review        │
└─────────────────────────────────┘

            OR (FAIL at any step)
            ↓
        ┌──────────────────┐
        │ Return errors    │
        │ + guidance       │
        │ → Analyst rework │
        └──────────────────┘
```

**Implementation:** lib/pipeline/validation.py
**Cost:** No execution against real patient DB (code paths prevent it)
**Audit:** Validation results logged in gxp_audit

---

### 2.5 Analyst Review Gate

```
                React App (Frontend)
                    │
                    ↓
    ┌───────────────────────────────┐
    │ ADS Preview Display           │
    │ • Cohort N                    │
    │ • Demographics (age, gender)  │
    │ • Outcome rates               │
    │ • Generated SQL               │
    │ • Protocol metadata           │
    └────────┬──────────────────────┘
             │
    Analyst Reviews
             │
          ┌──┴──┐
          │     │
      Approve  Reject
          │      │
          ↓      ↓
    ┌─────────────────┐    ┌──────────────┐
    │ Request Sign-In │    │ Rework Flag  │
    │ (MFA)           │    │ + Comments   │
    │ ↓               │    │              │
    │ E-Signature     │    │ → Agent      │
    │ Recording       │    │   Retry      │
    └────────┬────────┘    └──────────────┘
             │
             ↓
    ┌───────────────────┐
    │ Write to Audit:   │
    │ • event_type:     │
    │   ads_approval    │
    │ • actor: analyst  │
    │ • decision:       │
    │   approved        │
    │ • esignature:     │
    │   hash(cert)      │
    └────────┬──────────┘
             │
             ↓
    ┌───────────────────┐
    │ Approved ADS      │
    │ Ready for serving │
    └───────────────────┘
```

**Frontend:** React 19 + Vite + TailwindCSS v4 + Framer Motion
**Backend:** FastAPI (Python, serves /api/* endpoints)
**Auth:** Service Principal (workspace app) + OAuth for analysts
**E-Signature:** Recorded as hash(cert + timestamp + actor); stored in audit table

---

### 2.6 Audit & Reproducibility

```
┌──────────────────────────────────────────────────────────────┐
│                   AUDIT TABLES                               │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│ repro_manifest (append-only, one row per ADS)               │
│  ├─ manifest_id          (unique per build)                 │
│  ├─ ads_id, study_id                                        │
│  ├─ protocol_version     (hash of parsed protocol)          │
│  ├─ kb_snippet_versions  (array of snippet_id/vers/hash)    │
│  ├─ generated_sql        (map of step_name → SQL)           │
│  ├─ source_table_versions (map of table → delta_version)    │
│  ├─ model, agent_version, eval_scores                       │
│  ├─ reviewer, esignature, decision                          │
│  └─ prev_hash, row_hash  (SHA256 chain)                     │
│                                                              │
│ gxp_audit (immutable, hash-chained event log)               │
│  ├─ event_id             (unique per event)                 │
│  ├─ event_type           (ads_approval, review_gate_fail...) │
│  ├─ actor, subject_id, details                              │
│  ├─ ts                   (event timestamp)                   │
│  └─ prev_hash, row_hash  (SHA256 chain)                     │
│                                                              │
│ Permissions (GRANT/DENY):                                   │
│  ├─ INSERT: app service principal + reviewer (for esign)    │
│  ├─ SELECT: analysts (read audit trail)                     │
│  └─ UPDATE/DELETE: DENIED (idempotent pattern)              │
│                                                              │
│ Chain Verification (verify_chain):                          │
│  └─ Detect tampering by recomputing SHA256(prev + row)     │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

**Implementation:** waves/wave5_serving_audit_app/setup_audit.py
**Hash Function:** SHA256(prev_hash + canonical_row_str)
**Immutability:** Delta CDF + GRANT/DENY + no code path allows UPDATE
**Retention:** 25 years (per GxP config)

---

### 2.7 Serving via Lakebase

```
┌──────────────────────────────────────┐
│ Databricks (Delta)                   │
│ ads_serving schema                   │
│  • ads_output.delta (50k rows)       │
│  • cohort_summary.delta (1 row)      │
└────────┬─────────────────────────────┘
         │
         │ Continuous synced tables
         │ (Delta → Postgres, one-way)
         ↓
┌──────────────────────────────────────┐
│ Lakebase (PostgreSQL)                │
│ <your-lakebase-project> instance                 │
│ ads_serving_pg database              │
│  • ads_output                        │
│  • cohort_summary                    │
└────────┬─────────────────────────────┘
         │
         │ Low-latency read (Postgres)
         ↓
┌──────────────────────────────────────┐
│ React App (Frontend)                 │
│ Queries Postgres for analytics       │
│ Displays approved ADS                │
│ (read-only, no writes)               │
└──────────────────────────────────────┘
```

**Sync Mode:** CONTINUOUS (real-time)
**Direction:** Delta → Postgres (one-way only)
**Performance:** <1 min latency (50k rows per sync)
**App State:** Separate Lakebase DB (`ads_app`) with transactional tables:
  - `review_queue` (pending manifests for review)
  - `sessions` (user app sessions)
  - `sign_offs` (e-signature records)

---

### 2.8 PHI Containment & Gateway

```
┌──────────────────────────────────────────────────────────┐
│         All Model Calls Route via AI Gateway             │
├──────────────────────────────────────────────────────────┤
│                                                          │
│ Request Input                                            │
│  ├─ Endpoint: ads-ai-gateway                           │
│  ├─ Model: databricks-claude-sonnet-4                  │
│  └─ Prompt: Protocol text + KB query                    │
│              │                                           │
│              ↓                                           │
│  ┌────────────────────────────┐                         │
│  │ Gateway Guardrails         │                         │
│  │ ├─ PHI Detection           │                         │
│  │ │  (SSN, MRN, email...)    │                         │
│  │ ├─ Masking (→ ***)         │                         │
│  │ ├─ Rate limit: 300 QPM     │                         │
│  │ ├─ Cost attribution        │                         │
│  │ │  (rwe-ads-automation tag)│                         │
│  │ └─ Egress policy: deny     │                         │
│  │    external                │                         │
│  └────────────┬───────────────┘                         │
│               ↓                                          │
│  ┌────────────────────────────┐                         │
│  │ LLM Inference              │                         │
│  │ (safe, no external tools)  │                         │
│  └────────────┬───────────────┘                         │
│               ↓                                          │
│  ┌────────────────────────────┐                         │
│  │ Response Masking           │                         │
│  │ (mask PHI patterns before  │                         │
│  │  returning to client)      │                         │
│  └────────────┬───────────────┘                         │
│               ↓                                          │
│  ┌────────────────────────────┐                         │
│  │ Inference Log Table        │                         │
│  │ ads_audit.gateway_inference│                         │
│  │ (masked I/O, cost, tokens) │                         │
│  └────────────────────────────┘                         │
│                                                          │
│ Spend Cap: $2000/month (hard limit)                     │
│ Models: Sonnet, Opus, Llama (curated set)               │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

**No External Egress:** Policy blocks LLM tool calls to external APIs
**Data Residency:** All PHI patterns masked before logging
**Compliance:** Request/response logging in audit table (masked)

---

## 3. Wave-by-Wave Deployment

```
Wave 0: Foundation
  │
  ├─ Create schemas (raw, curated, serving, kb, audit)
  ├─ Create protocols volume
  ├─ Seed KB table (9 approved snippets, version 1)
  ├─ Create gateway inference table (placeholder)
  └─ Setup Lakebase DBs (serving, app-state)
  
  └─→ Output: Empty schemas, KB ready, Lakebase alive


Wave 1: Synthetic Bronze
  │
  ├─ Generate synthetic RWD (50k patients, 2018–2024, deterministic seed)
  ├─ Load to ads_raw (patient, claim, pharmacy, lab, encounter tables)
  └─ Register in Unity Catalog
  
  └─→ Output: ads_raw schema populated, 50k+ rows each table


Wave 2: Medallion (Silver → Gold)   [not a standalone job — runs as the `medallion`
                                     SDP pipeline task inside the Wave 1 job]
  │
  ├─ Conformation pipeline (raw → silver CDM)
  │   ├─ Flatten enrollments → eligibility_periods
  │   ├─ Union claims + pharmacy → patient_timeline
  │   └─ Index on patient_id
  │
  ├─ Assembly pipeline (silver → gold ADS)
  │   ├─ Generate ADS (one row per patient)
  │   ├─ Join demographics + covariates + outcomes
  │   └─ Compute cohort_summary (N, demographics, outcome rates)
  │
  └─→ Output: ads_serving schema, ready for SQL generation


Wave 3: ADS Builder (deterministic)
  │
  ├─ Implement ADS builder (KB retrieval + approved-SQL template assembly)
  ├─ Implement token substitution ({{gold}} → fqn, {{code}} → literals)
  ├─ Builder steps: retrieve KB → compose SQL → validate → log manifest
  └─ Test on poc_low (simple prevalence cohort)
  
  └─→ Output: Generated SQL for poc_low, reproducibility manifest


Wave 4: Supervisor & Orchestration   [originally-intended — superseded, not built:
                                      orchestration is native Databricks Workflows;
                                      wave 4 as built is model benchmark + cost]
  │
  ├─ Implement Supervisor Agent (orchestrates builder, validation, retry)
  ├─ Implement full validation layer (syntax, egress, schema, dry-run)
  ├─ Implement eval metrics logging (cohort N, outcome rates, covariate distrib)
  └─ Test poc_med, poc_high (more complex studies)
  
  └─→ Output: End-to-end SQL generation for 3 studies


Wave 5: Audit & Serving
  │
  ├─ Create repro_manifest + gxp_audit tables (append-only, hash-chained)
  ├─ Create Lakebase synced tables (continuous Delta → Postgres)
  ├─ Implement review gate (analyst e-sign)
  ├─ Generate validated work instruction (SOP)
  └─ Deploy React app (review ADS, approve/reject)
  
  └─→ Output: End-to-end with audit trail, serving via Lakebase


Wave 6+ (Future): Validation & Production Readiness
  └─ Formal governance sign-off
  └─ your production workspace migration (edit demo.config.yaml, re-deploy)
```

---

## 4. Technology Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| **Workflow** | Spark Declarative Pipelines | Serverless |
| **Storage** | Delta Lake | Unity Catalog |
| **Governance** | Unity Catalog ABAC | Latest |
| **AI Gateway** | Databricks AI Gateway | Latest |
| **Models** | Claude Sonnet/Opus, Llama, Embedding | Marketplace |
| **Serving** | Lakebase | PostgreSQL 16 |
| **Frontend** | React 19 + Vite 7 + Tailwind v4 | Latest |
| **Backend** | FastAPI | Latest |
| **Audit** | Delta + Hash Chain | SHA256 |
| **Language** | Python 3.10+ | Latest |
| **Testing** | pytest | Latest |

---

## 5. Idempotency & Reproducibility Guarantees

### 5.1 Idempotent Deployment
- All DDL: `CREATE TABLE IF NOT EXISTS`, `CREATE SCHEMA IF NOT EXISTS`
- Data seeding: `MERGE` for KB (on snippet_id match, no re-insert)
- Synthetic generation: deterministic seed (same seed → same rows)
- Rerunning wave0 → wave5 produces identical state (no mutations)

### 5.2 Reproducible SQL Generation
- KB snippets versioned (version, content_hash)
- Generated SQL logged in repro_manifest (per-step)
- Source table Delta versions captured (time-travel snapshot)
- Model + agent versions captured → can replay exact generation

### 5.3 Hash Chain Immutability
- Every audit event hash-chained: `hash = SHA256(prev_hash + row_data)`
- Tampering detected on verification (recompute all hashes, compare)
- Audit tables append-only via GRANT/DENY (no UPDATE/DELETE)

---

## 6. Cost Model (Synthetic PoC)

| Component | Cost/Month (est.) |
|-----------|-------------------|
| Serverless SQL warehouse (medallion + validation) | $15 |
| Spark jobs (protocol parsing, manifest writing) | $5 |
| Vector Search (KB embedding + retrieval) | $3 |
| AI Gateway (model calls, 10k/month @ $0.1/1k) | $1 |
| Lakebase synced tables + app-state | $20 |
| Storage (50k patient synthetic, 500GB max) | $5 |
| **Total** | ~$50/month |

---

## 7. High-Level Sequence Diagram

```
Actor: Analyst
Analyst → App: Upload protocol PDF
App → Doc Intelligence: Parse protocol
Doc Intelligence → ads_kb: Store metadata
App → Analyst: Show PoC studies available
Analyst → App: Select poc_low
# NOTE: "Supervisor" below = originally-intended orchestration — superseded, not built.
# As built, native Databricks Workflows sequences the ADS Builder directly (see the as-built note at top).
App → Supervisor: Generate ADS for poc_low
Workflow → ADS Builder: Retrieve KB snippets + assemble approved-SQL
ADS Builder → Validation: Check SQL (syntax, egress, schema, dry-run)
Validation → Validation: VALID ✓
Validation → Medallion: Execute ADS query on gold
Medallion → ADS Output: Generate ads_output table (50k rows)
ADS Builder → Manifest: Write repro_manifest (protocol ver, KB vers, SQL, source vers, eval)
Manifest → Audit: Write gxp_audit event (ads_generation_complete)
App → Lakebase Sync: Continuous sync ads_output → Postgres
App → Frontend: Display ADS preview (N, demographics, outcomes, SQL)
Analyst → Frontend: Review ADS (looks good)
Analyst → Frontend: Click "Approve"
Frontend → Auth: Request e-signature (MFA)
Auth → Analyst: Challenge
Analyst → Auth: Respond (e.g., TOTP)
Auth → Frontend: E-signature hash
Frontend → App Backend: POST /approve (manifest_id, esignature)
App Backend → gxp_audit: Append event (actor=analyst, decision=approved, esignature=hash)
App Backend → Lakebase: approved_flag = true for this ADS
Frontend → Analyst: ✓ ADS approved, serving live
Analyst → Lakebase: Query ads_output (read-only, 50k rows, <1 sec)
```

---

## 8. Disaster Recovery & Backup

- **Delta tables**: Versioned via time travel (30 day default retention)
- **Audit tables**: Immutable (append-only, no backups needed)
- **Manifests**: Reproducible from protocol + KB + source versions (can regenerate)
- **Lakebase**: Managed backups (AWS RDS automated snapshots)

---

## 9. Security & Compliance

- **PHI**: Synthetic data only (no real PHI risk in PoC); gateway masking enforced by construction
- **Access Control**: RBAC (analysts SELECT), ABAC (UC tags), audit tables append-only
- **Audit Trail**: Immutable, hash-chained; 25-year retention
- **Egress**: Gateway policy denies external tool use; no direct external DB connections
- **Encryption**: Delta at-rest (SSE), Lakebase TLS in-transit

---

*Architecture Document v1.0 — Last updated 2026-08-12*
