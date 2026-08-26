# CLAUDE.md — RWE Analysis-Ready Dataset (ADS) Automation

Project memory for Claude Code. **These golden rules are non-negotiable.**

## What this is
A standalone PoC that turns a **study protocol** into an **analysis-ready dataset (ADS)**
over synthetic real-world data (RWD), by generating **validated SQL** from an
**approved-SQL knowledge base**, with a mandatory analyst-review gate, full
reproducibility/audit, and PHI-containment controls. Results served low-latency
from **Lakebase**. Persona: **MD Epidemiology / Real-World Data Science (Epi-RWDS)**.

- Runs over **synthetic RWD**; migrate to your production workspace by editing `demo.config.yaml`.
- Self-contained — own repo, own `demo.config.yaml`, own `ads_*` schemas, own app.

## Golden rules (enforce in every change)
1. **Serverless only.** Serverless jobs, serverless SDP pipelines, serverless SQL
   warehouse, serverless Model Serving, Databricks Apps. No all-purpose/classic clusters.
2. **No `CREATE CATALOG`.** The catalog exists; create schemas/volumes/tables/functions inside it only.
3. **No hardcoded secrets.** Secret scopes only; reference via `dbutils.secrets.get` or app env.
4. **Idempotent DDL.** Every `CREATE` is `IF NOT EXISTS` / `CREATE OR REPLACE`. Every wave re-runnable.
5. **MLflow 3.x APIs only** (`mlflow.trace`, `mlflow.genai.evaluate`, Prompt Registry, ResponsesAgent).
6. **Apps bind `$DATABRICKS_APP_PORT`.** Never a fixed port.
7. **Single source of truth: `demo.config.yaml`.** No literal catalog/schema/endpoint/instance
   names anywhere else. All names resolve through `lib/config.py`.
8. **Validate-don't-execute.** The ADS builder **assembles** SQL from approved KB templates
   (deterministic parameter substitution — no model generates the query) and validates it via
   **EXPLAIN**; it may execute **only against synthetic gold**. Executing against any real
   patient DB is impossible by construction (no such connection exists).
9. **PHI containment.** Every model call goes through one controlled path (`gateway_call`):
   **in-process PII masking** + audit logging + **egress-deny** (no external calls). Data is
   synthetic (PHI-safe); the pattern transfers to your production workspace. *(The native `ads-ai-gateway`
   external-model endpoint is a pattern placeholder — it 403s on pay-per-token FMs, so calls
   route directly to the FM. Server-side `ai_mask` + AI-Gateway guardrails on an owned endpoint
   are the production hardening.)*
10. **Reproducible by construction.** Every ADS build emits a reproducibility manifest
    (protocol version, KB snippet versions, generated SQL, source Delta table versions via
    time travel, agent/model versions, eval scores) into the audit schema.
11. **Human-in-the-loop is mandatory.** No ADS is "approved" without an analyst review + e-sign.

## Stack
Spark Declarative Pipelines (medallion, serverless) · Unity Catalog (governance, ABAC, lineage)
· in-process PII masking + egress-deny on model calls · Document Intelligence
(`ai_parse_document` / `ai_extract`) · Genie for analyst Q&A (optional — the bundle ships a Genie
config placeholder; point a Genie space at the approved `ads_output` post-deploy) · MLflow 3.x (tracing + eval) ·
**Lakebase Autoscaling** for low-latency serving via **synced tables** (Delta→Postgres, one-way) · Databricks App
(React 19 + Vite + Tailwind + FastAPI + Framer Motion).

## Gotchas (deploy without surprises)
1. **Modern reasoning models reject `temperature`** — Claude 5 / GPT-5 / Gemini 3.x return 400 on `temperature != 1`. `gateway_call` omits it + retries stripping any unsupported param. Don't add it back.
2. **Lakebase = Autoscaling, provisioned via control-plane** — run `databricks postgres create-project <project>` BEFORE `bundle deploy`. The serverless waves make **zero in-kernel `w.postgres` calls** (in-kernel `w.postgres` crashes the kernel). The Provisioned tier (`w.database`) is retired.
3. **Protocol volume auto-seeds if empty** — wave1 drops sample `poc_*` PDFs/DOCX so `ai_parse_document`→`ai_extract` runs out-of-the-box. Upload real protocols to the volume to override.
4. **MLflow tracing uses a per-user experiment** (`/Users/<you>/<initiative>`, not `/Shared`) so it persists on serverless jobs. Verify with `scripts/verify_tracing.py --profile <p>` (or `GET /api/2.0/mlflow/traces?experiment_ids=<id>`).
5. **`ai_extract` v2.1 + `mode=precision`** returns each scalar as `{value, citation_ids, confidence_score}` — read `:value`.
6. **`bundle validate` shows benign `sync.exclude` warnings** (`node_modules`/`__pycache__` match nothing) — safe to ignore; do not delete the excludes.
7. **5 vars are NOT read from `demo.config.yaml`** — `uc_catalog`, `silver_schema`, `warehouse_id`, `lakebase_project`, `notification_email` are bundle/pipeline vars; pass them as `--var` on EVERY `bundle deploy`/`run` or the medallion pipeline 404s on the placeholder `rwe_ads_catalog`. (See the Deploy block below.)
8. **Config is not fully DRY** — `demo.config.yaml`'s `volumes.protocols`, `gateway.inference_table`, `kb.table`, `synced_tables.*`, `genie.tables` **embed** the schema name; editing the `schemas:` block alone does NOT update them. Change those refs too, or upload/extract fails with `SCHEMA/CATALOG ... does not exist` (e.g. `ads_raw.protocols`).
9. **Deploy the app via `bundle deploy` + `bundle run <app_key>`, NOT `apps deploy` alone** — `bundle deploy` applies `resources/app.yml`, which binds the app's 4 resources (sql-warehouse, postgres, protocol-extract-job, ads-build-job) **by id** + grants the app SP `CAN_MANAGE_RUN`. A code-only `apps deploy` leaves `resources: null` → the Run-extraction / Build-ADS buttons 404 and Lakebase serving silently falls back to the warehouse.
10. **App name must be unique / non-colliding** — `resources/app.yml` sets `name: rwe-ads-app` (the default); apps are NOT dev-prefixed, so rename it to a unique value (e.g. `<initials>-rwe-ads-app`) to avoid colliding with an existing app. Serving reads Lakebase directly, so the app SP must be granted on the serving/app Postgres DBs (`scripts/setup_synced_tables.sh`).

## Deploy
Configure first: `cp demo.config.example.yaml demo.config.yaml` (gitignored) and fill in your workspace/catalog/warehouse/Lakebase. Then (the 5 `--var`s are mandatory — see gotcha 7):
```bash
VARS="--var=warehouse_id=<wh> --var=notification_email=<you> --var=lakebase_project=<proj> --var=uc_catalog=<cat> --var=silver_schema=<silver_schema> --profile <PROFILE>"
bash scripts/setup_lakebase.sh <PROFILE>                 # create the Autoscaling project + serving/app DBs FIRST
databricks bundle validate -t dev $VARS
databricks bundle deploy   -t dev $VARS
databricks bundle run wave0_foundation -t dev $VARS      # then wave1, wave3, (wave4/5 optional)
bash scripts/setup_synced_tables.sh <PROFILE>            # sync gold -> Lakebase + grant app SP
databricks bundle run rwds_ads_studio -t dev $VARS       # app resource key -> deploys rwe-ads-app WITH resource bindings
```

## Repo layout
- `demo.config.yaml` — single source of truth (edit this to migrate)
- `lib/config.py` — the ONLY place names resolve
- `lib/synth/` — deterministic seeded RWD generators
- `lib/pipeline/` — approved-SQL KB, retrieval, ADS builder (deterministic template substitution), validation
- `waves/wave{0..5}_*/` — per-wave provisioning + logic (mirrors the build waves)
- `pipelines/` — serverless SDP pipeline source (bronze/silver/gold)
- `app/frontend` (React 19 + Vite + Tailwind + Framer Motion), `app/backend` (FastAPI)
- `resources/*.yml` — DAB job/pipeline/app definitions
- `tests/` — idempotency + guardrail + PHI-mask + audit-write + validate-not-execute
