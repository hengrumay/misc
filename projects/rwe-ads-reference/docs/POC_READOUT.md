# RWE Analysis-Ready Dataset (ADS) Automation — PoC Scope, Implementation & Readout

> **⚠️ As-built note (current bundle).** This document describes the original PoC design. As deployed today the pipeline is **deterministic + native**: orchestration is **Databricks Workflows** (no Agent Bricks Supervisor); the ADS build **assembles approved-SQL KB templates + validates via `EXPLAIN`** (no model generates queries); serving is **Lakebase Autoscaling** (the retired Provisioned-tier references below — `w.database`, `<your-lakebase-project>`, `list-synced-tables --instance` — no longer apply; provision via `databricks postgres create-project`); and **Genie** (analyst Q&A) ships as a **disabled config placeholder** — not built or deployed in this bundle. For current deploy commands follow **`CLAUDE.md`** and **`.claude/skills/deploy-full-bundle`**.


**Persona:** MD Epidemiology / Real-World Data Science (Epi-RWDS)
**Data:** synthetic RWD — migrate to your production workspace by editing `demo.config.yaml`.
**Catalog:** `rwe_ads_catalog` (set to your own catalog in `demo.config.yaml`)
**Status:** Core pipeline verified on the synthetic PoCs (serverless).

---

## 1. Executive summary

This PoC turns a **study protocol** into an **analysis-ready dataset (ADS)** over synthetic
real-world data (RWD) by generating **validated SQL** composed from an **approved-SQL knowledge
base**, gated by an **analyst review + e-signature** step (see the auto-e-sign demo-default note below),
with a **hash-chained reproducibility/audit** trail and **PHI-containment** controls — served low-latency from **Lakebase**.

It is built as a **Databricks Asset Bundle (DAB)** with a single source of truth
(`demo.config.yaml`), so migrating to another workspace (your production workspace) is an edit-one-file operation.
Everything runs on **serverless** (jobs, SDP pipelines, SQL warehouse, apps) — no classic clusters.

The charter's headline risk — *hallucinated / unsafe SQL against patient data* — is addressed **by
construction**: the builder may only compose from **approved** KB snippets, every composed statement is
**EXPLAIN-validated before it can execute**, execution is possible **only against synthetic gold**, and
no ADS is "approved" without a recorded e-signature. *(Note: the extraction hard gate is a set of **Stage-1 deterministic validators** — `eval_ok` — not a model judge; the Stage-3 model judges are advisory. And the shipped demo sets `allow_auto_esign: true`, which auto-signs eval-passing specs under a **SYSTEM actor** — a non-human signature — bypassing the human analyst e-sign; set it to `false` for the human-signed path regulated use requires.)*

---

## 2. PoC scope

**In scope**
- Protocol intake → structured protocol spec (Document Intelligence, with deterministic fallback)
- Deterministic synthetic RWD (claims + EHR shape: ICD-10-CM, CPT/HCPCS, NDC, LOINC)
- Serverless medallion (bronze → silver conformed RWD CDM → gold analytic base)
- Approved-SQL knowledge base (governed, versioned) + retrieval
- ADS builder (deterministic): cohort → inclusion/exclusion → derivation → assembly, each emitting validated SQL from approved KB templates
- Human-in-the-loop review gate + e-signature
- Reproducibility manifest + append-only, hash-chained GxP audit
- Unity AI Gateway PHI-masking + egress-deny **pattern** (configuration + policy)
- Lakebase low-latency serving (synced tables) + React 19 / FastAPI app
- Model benchmarking + cost validation across candidate models

**Out of scope (by construction)**
- Any connection to, or execution against, a **real patient database** — impossible; no such connection exists
- Real PHI — all data is synthetic and PHI-safe; the controls are real and testable so the pattern transfers to your production workspace

---

## 3. Success criteria (charter definition-of-done) and status

| # | Success criterion | Status | Evidence |
|---|---|---|---|
| 1 | `databricks bundle deploy` provisions on serverless; idempotent | ✅ Met | Bundle deploys on serverless; jobs + serverless SDP pipeline created |
| 2 | Full medallion (bronze→silver→gold) via serverless SDP with passing expectations | ✅ Met | Pipeline `rwe-ads-medallion` completes; gold `patient_timeline` populated |
| 3 | ADS-builder turns a parsed protocol into validated SQL from approved KB, executes only on synthetic gold, passes review gate | ✅ Met | `poc_low` built: steps EXPLAIN-validated, T2DM cohort → `ads_output` |
| 4 | Reproducibility manifest + append-only, tamper-evident audit; byte-identical re-run | ✅ Met | `repro_manifest` written (KB + source Delta versions); hash chain verifies valid + tamper detected |
| 5 | Human-in-the-loop (analyst e-sign) available | ✅ Met (human path); ⚠️ off by default | `ads_approval` event with reviewer + e-signature in `gxp_audit`. **Demo default `allow_auto_esign: true` auto-signs eval-passing specs under a SYSTEM actor (non-human) — set `false` to require the human analyst e-sign** |
| 6 | Served low-latency from Lakebase (Delta→PG) | ✅ Met | `ads_serving_pg.ads_output` + `cohort_summary` loaded into Postgres. (Managed synced-table *pipeline* needs a metastore storage-root config; direct load achieves serving.) |
| 7 | 3 PoCs benchmarked; selection + cost report | ✅ Met | 3 PoCs × candidate pay-per-token FMs through the gateway; a candidate selected on grounding / hallucination / cost; results in `ads_kb.bench_results`, cost in `ads_audit.gateway_inference`, under the configured cap |
| 8 | PHI containment: gateway masking + egress-deny | ✅ Met | Every `gateway_call` PHI-masked + logged + MLflow-traced **in-code**; **no leaked rows** (MRN+SSN masked). **Egress-deny is a workspace policy (`egress_policy: deny_external`), not enforced inside `gateway_call`.** Native `ads-ai-gateway` endpoint created with AI-Gateway guardrails (PII/PHI MASK, inference table, rate limit); FM-routing via external_model→pay-per-token FM returns 403 (platform limit) so the app-layer wrapper is the functional path |
| 9 | New `rwe-ads-app` app live for Epi-RWDS; draft Work Instruction | ✅ Met | App deploys + RUNNING: `GET /` → **HTTP 200**; `/api/served/*` reads **live from Lakebase** (`source:"lakebase"`) with warehouse→synthetic fallback. Validated Work Instruction drafted. |
| 10 | No hardcoded names/secrets; serverless only | ✅ Met | Test suite passes incl. no-literal-catalog scan; serverless enforced; the one PAT lives in a secret scope (`ads-ai-gateway/sp_token`) |

Legend: ✅ proven live.

**Enablement update:** the three follow-ups are complete — Lakebase serving (data
loaded to Postgres), live 3-model benchmarking + cost + PHI-mask verification through the gateway, and
the `rwe-ads-app` app deployed and returning 200. Residual platform limits (managed synced-table
pipeline needs a metastore storage root; native gateway external_model can't wrap a pay-per-token FM;
app SP needs a Postgres role for live Lakebase reads) are noted with working alternatives in place.

---

## 4. The asks → how each is implemented on Databricks

| Charter ask (golden rule / requirement) | Databricks implementation | Where |
|---|---|---|
| **Serverless only** | Serverless SDP pipeline, serverless jobs (no `new_cluster`), serverless SQL warehouse, Databricks Apps | `resources/*.yml`, `databricks.yml` |
| **No `CREATE CATALOG`** | Only schemas/volumes/tables created inside the existing catalog | Wave 0 UC provisioning |
| **Single source of truth** | `demo.config.yaml` + `lib/config.py`; no catalog/schema literal elsewhere (enforced by test) | `lib/config.py`, `tests/test_config.py` |
| **Migratable via DABs** | One bundle; migration = edit `demo.config.yaml` + target host; names resolve at runtime | `databricks.yml`, `docs/RUNBOOK.md` |
| **Deterministic synthetic RWD** | Seeded generators (Faker+numpy), 7 entities, ICD-10/CPT/NDC/LOINC | `lib/synth/` |
| **Medallion CDM** | Serverless SDP: silver conforms + `@dlt.expect` quality constraints; gold longitudinal `patient_timeline`, `code_rollups`, `eligibility_periods` | `pipelines/{silver,gold}.py` |
| **Approved-SQL KB (governed, versioned)** | Delta table `approved_sql_kb` (status, version, content_hash); only `status='approved'` composable | `waves/wave0_foundation/kb_seeds.py` |
| **KB retrieval by intent** | Vector Search index over snippet descriptions (keyword fallback) | `lib/pipeline/kb_retrieval.py` |
| **Validate-don't-execute** | `EXPLAIN` + schema check + dry-run **before** any execution; execution only vs synthetic gold; egress guard rejects non-gold references | `lib/pipeline/validation.py`, `waves/wave3_ads_build/ads_build_core.py` |
| **Ordered ADS composition** | cohort → inclusion/exclusion → derivation (covariates, exposure eras, outcomes) → assembly (one row/patient) | `ads_build_core.py` |
| **Human-in-the-loop** | Review queue + analyst e-signature; no approval without recorded sign-off (**demo default `allow_auto_esign: true` auto-signs eval-passing specs under a SYSTEM actor, bypassing the human e-sign — set `false` for the human path**) | `waves/wave3_ads_build/review_gate.py` (human), `waves/wave3_ads_build/approve_protocols.py` (auto), `gxp_audit` |
| **Reproducibility manifest** | Protocol version, KB snippet versions+hashes, generated SQL per step, source Delta versions (time travel), model, eval scores, reviewer, e-sign | `waves/wave5_serving_audit_app/setup_audit.py` → `repro_manifest` |
| **Tamper-evident audit** | Append-only, SHA256 hash-chained `gxp_audit`; `verify_chain()` detects mutation | `setup_audit.py` |
| **PHI containment** | Unity AI Gateway PII/PHI **MASK** in+out, safety/prompt-injection/jailbreak, inference table, rate limit, spend cap; Contextual Service Policy = deny external egress | `waves/wave0_foundation/gateway.py`, `lib/phi.py` |
| **MLflow 3.x** | `mlflow.trace`, `mlflow.genai.evaluate` scorers, Prompt Registry | `lib/pipeline/`, `waves/wave4_model_benchmark/benchmark.py` |
| **Model benchmark** | Multi-model benchmark over candidate LLMs (SQL validity, grounding, faithfulness) through the gateway wrapper | `waves/wave4_model_benchmark/run_benchmark_live.py` |
| **Cost validation** | Gateway cost attribution per model/PoC vs hard monthly cap | `waves/wave4_model_benchmark/cost_report.py` |
| **Low-latency serving** | Lakebase synced tables (Delta→PG, one-way); app reads Lakebase, writes only session/review state | `waves/wave5_serving_audit_app/setup_sync.py`, `resources/app.yml` |
| **App bind `$DATABRICKS_APP_PORT`** | FastAPI binds the injected port; monolith serves the Vite bundle; 3-tier read fallback (Lakebase→warehouse→synthetic) never 500s | `app/backend/app.py` |
| **Validated Work Instruction (SOP)** | Auto-generated controlled procedure, flagged DRAFT pending Gen AI Governance Council sign-off | `docs/VALIDATED_WORK_INSTRUCTION.md` |

---

## 5. Step-wise implementation & live evidence

### Wave 0 — Foundation (live ✅)
Created the five `ads_*` schemas (`ads_raw`, `ads_curated`, `ads_serving`, `ads_kb`, `ads_audit`),
the `protocols` volume, the approved-SQL KB table seeded with **9 approved snippets** (cohort /
inclusion / exclusion / derivation / outcome), the gateway inference table, and the Lakebase serving
DB (`ads_serving_pg`) + app-state DB (`ads_app`) on `<your-lakebase-project>`.

### Wave 1–2 — Synthetic RWD + medallion (live ✅)
Deterministic generators produced bronze in `ads_raw` (synthetic patients, providers, encounters,
medical + pharmacy claims, labs). The serverless SDP pipeline `rwe-ads-medallion` conformed silver
(`ads_curated`, quality expectations passed) and built gold (`ads_serving`): `patient_timeline`,
`eligibility_periods`, `code_rollups`. *(Synthetic scale is configurable in `demo.config.yaml`; default 50,000 patients.)*

### Wave 3 — ADS builder + review gate (live ✅)
For **poc_low** ("Simple prevalence cohort — Type 2 Diabetes"), the builder retrieved approved KB
snippets, substituted protocol params, and **EXPLAIN-validated every step before executing**, then
materialized `ads_serving.ads_output` (one row per patient: index date, baseline hypertension
covariate, heart-failure outcome flag, time-to-event) and `cohort_summary` (outcome rate, baseline
covariate rate, avg time-to-event). The build was enqueued for analyst review — **pending sign-off**
until approved.

### Wave 4 — Model benchmark + cost (deployed code ◑)
An MLflow 3.x eval fixture (SQL validity, protocol faithfulness, KB grounding, hallucination rate,
analyst-edit distance) run across candidate models, plus gateway cost attribution vs the monthly cap,
are built and deployed; the live multi-model run activates once the AI-Gateway serving endpoint is
enabled.

### Wave 5 — Serving, audit, WI, app
- **Audit (live ✅):** wrote a reproducibility manifest (KB snippet versions+hashes, generated SQL
  per step, source Delta versions via time travel, model, eval scores, reviewer, **e-signature**,
  decision=approved) and a **hash-chained** `gxp_audit` (build → review_pending → approval).
  `verify_chain()` returns **valid**; a simulated tamper is **detected**.
- **Serving (configured ◑):** serving + app DBs created; `ads_output`/`cohort_summary` ready to sync;
  synced-table creation needs the Lakebase instance's database-catalog registration step.
- **Work Instruction (✅):** `docs/VALIDATED_WORK_INSTRUCTION.md` generated (DRAFT, pending council sign-off).
- **App (ready ◑):** `rwe-ads-app` (React 19 + Tailwind + Framer Motion + FastAPI) built with a
  "How It Works" flow page, protocol/build/review/served/audit pages, and a 3-tier never-500 read
  fallback; DAB resource ready to deploy.

---

## 6. PoC readout — what this proves

1. **Governed, KB-grounded generation works.** The ADS was composed only from approved, versioned SQL
   snippets — no free-form model SQL touched the data path.
2. **Validate-don't-execute is real.** Every step was `EXPLAIN`-validated before execution; the builder
   refuses to proceed on any validation failure (observed live when a malformed step blocked execution).
3. **Reproducible & auditable by construction.** The manifest pins protocol version, KB snippet
   versions/hashes, generated SQL, and **source Delta table versions (time travel)** — enough to re-run
   byte-identically. The audit chain is tamper-evident.
4. **Human-in-the-loop e-sign is available (off by default in the demo).** The human analyst e-sign path is real code and records a signed `ads_approval` event; the extraction hard gate that blocks e-sign is a set of **Stage-1 deterministic validators** (`eval_ok`), with the Stage-3 model judges advisory. The shipped demo sets `allow_auto_esign: true`, which auto-signs eval-passing specs under a SYSTEM actor (non-human) — set it to `false` to require the human e-signature.
5. **Serverless + DAB + single-source-of-truth** make the whole thing reproducible and portable to
   your production workspace by editing one config file.

### Honest caveats / next steps
- **All enablement is now live and committed as DAB jobs** (gateway, live benchmarking + cost, Lakebase
  serving, app deployed at HTTP 200 reading `source:"lakebase"`). Two platform limits remain with working
  alternatives: the *managed* synced-table pipeline needs a metastore storage root (direct load serves
  today), and the native gateway can't wrap a pay-per-token FM via external_model (app-layer gateway is
  the functional path).
- **PHI controls** transfer as-is; on your production workspace the same in-process `gateway_call` masking (+ audit + trace) and the egress-deny workspace policy (`egress_policy: deny_external`) apply to real data.
- **Scale:** runs at the configured synthetic scale (default 50,000 patients; serverless scales; longer runtime).
- The Lakebase version is set in `demo.config.yaml` (`lakebase.pg_version`).

---

## 7. Reference

- **Repo:** `<your-repo-url>`
- **Deploy:** `databricks bundle deploy -t dev` → `databricks bundle run wave1_synth_bronze -t dev` … (see `docs/RUNBOOK.md`)
- **Migrate to your production workspace:** edit `demo.config.yaml` (catalog, schemas, Lakebase, warehouse) and authenticate with your CLI profile; redeploy.
- **Key objects:** catalog `rwe_ads_catalog`; schemas `ads_{raw,curated,serving,kb,audit}`;
  pipeline `rwe-ads-medallion`; Lakebase DBs `ads_serving_pg`, `ads_app`.

---

## 8. DAB reproducibility & porting to another workspace

**Fully DAB-runnable.** Every step — including the enablement work (AI Gateway, Lakebase serving,
app-SP grants, benchmarking) — is committed as idempotent, config-driven scripts wired into
`resources/jobs.yml`, so a clean workspace is reproduced by `bundle deploy` + `bundle run`. Lakebase serving is **Autoscaling**: the DBs + catalog are provisioned **control-plane** by
`setup_lakebase.sh` / `setup_synced_tables.sh` (the serverless waves make no `w.postgres`/`w.database`
calls) — the retired Provisioned-tier REST approach no longer applies.

```bash
databricks bundle deploy -t dev
databricks bundle run wave0_foundation      -t dev   # schemas/volume/KB/VS index/gateway (Lakebase DBs: control-plane setup_lakebase.sh)
databricks bundle run wave1_synth_bronze     -t dev   # synth bronze + serverless SDP medallion
databricks bundle run wave3_ads_build        -t dev   # protocol eval + approve gate + poc_low ADS build (validate-don't-execute)
databricks bundle run wave4_model_benchmark -t dev   # live multi-model benchmark + cost report
databricks bundle run wave5_serving_audit    -t dev   # audit + Lakebase serving + app SP grant
# App: cd app && python build.py ; databricks apps deploy rwe-ads-app --source-code-path <workspace .../files/app>
```

### Port to another workspace (e.g. your production workspace) — single source of truth
1. **Prereqs on the target** (bundle does NOT create these): the **catalog** already exists (no `CREATE CATALOG`);
   a **serverless SQL warehouse** exists; a **Lakebase instance** exists; the candidate **foundation models**
   are pay-per-token-enabled; (for *managed continuous* sync) the metastore has a **storage root**.
2. **Edit `demo.config.yaml`** — the only place names resolve: `workspace.host`, `workspace.warehouse_id`,
   `catalog`, `lakebase.instance` / `lakebase.catalog`, and `models.candidates` (workspace-enabled FMs).
   `schemas`, `synced_tables`, `gateway`, `app.name` typically stay unchanged.
3. **Add/edit a bundle target** in `databricks.yml` (a `prod` template exists — set its `host`; update the
   `warehouse_id` variable default).
4. **Validate + deploy + run:** `databricks bundle validate/deploy -t prod` then `bundle run wave0…wave5 -t prod`.
5. **App:** `cd app && python build.py` (runs `npm install --legacy-peer-deps` against the public npm registry),
   then `databricks bundle deploy -t prod` + `databricks apps deploy`. `wave5`'s `grant_app_sp` task grants
   the app SP live Lakebase reads automatically.
6. **Verify:** `pytest tests/` (no-literal-catalog scan proves portability); app `GET /` → 200 and
   `/api/served/*` → `source:"lakebase"`.

Migration touches **one config file + one target host** — no code edits. See `docs/RUNBOOK.md` §5 for the
detailed guide and the per-workspace gotchas (KB DataFrame-MERGE seeding, REST for Lakebase, `sync.include`
vs `.gitignore`).
