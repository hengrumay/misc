# Changelog

All notable changes to the RWE ADS Automation bundle.
Format follows Keep a Changelog (https://keepachangelog.com).
"Unreleased" collects the accuracy + naming cleanup applied so the bundle's
docs and identifiers match its as-built, deterministic behavior.

## [Unreleased]


### docs: generalize app-name references + constrain README diagram display size
Regenerated `docs/architecture-flow.png` (from `architecture-wired-vs-optional.mmd`) — the app node is
now name-neutral ("ADS review app / Databricks App") instead of a specific deployment name, so the
diagram reads as a template. README embeds it via an HTML `<img width="440">` so the tall diagram
renders at a readable size instead of dominating the page (full-res on click). App/Lakebase names
throughout the repo follow the `<initials>-rwe-ads-*` convention (pick your own; nothing hardcoded to
one deployment). Verified end-to-end this session: the full bundle (wave0/1/3/4/5 + the app) deploys
and runs green after the Wave-0 VS-move — app stays RUNNING with all 4 resource bindings
(sql-warehouse, postgres, protocol-extract-job, ads-build-job).

### refactor(bundle): move the Vector Search index build to Wave 0 (foundation)
`build_index.py` (the `build_kb_index` task) moved from `waves/wave3_ads_build/` to
`waves/wave0_foundation/` via `git mv` (content unchanged). Rationale: the KB index is
**foundation reference infrastructure derived from the Wave-0 approved-SQL KB**, not per-build
work — it belongs alongside the KB seed, built once (idempotent delta-sync), not re-touched on
every ADS-build run. `build_kb_index` now `depends_on: uc_foundation` in Wave 0; `databricks-vectorsearch`
added to the Wave-0 env. Wave 3's `protocol_eval` / `approve_protocols` **dropped their spurious
`depends_on: build_kb_index`** (they never used the index — the eval judges extraction, approval is the
gate; only the ADS builder queries the index). Wave 3 renamed "KB index + ADS builder + review gate"
→ "ADS builder + eval + review gate". **Zero runtime change** while `vector_search.enabled: false`
(the task is a no-op wherever it sits). `bundle validate` passes (no dangling `depends_on`); README +
deploy skill + RUNBOOK + POC_READOUT run-command comments updated to the Wave-0 placement.

### fix(app-ui): always-visible sidebar; nav actually renders the target page
Two layout bugs made the app look like a single dead landing page: (1) the sidebar used a
framer-motion `x`-transform toggle whose **inline** style overrode the Tailwind `md:translate-x-0`
desktop class, so any nav click slid the sidebar off-screen with no way back; (2) the main content
used `<AnimatePresence mode="wait">` keyed on `location.pathname`, but `location` resolved to the
global `window.location` (App is rendered *outside* `<Router>` and never called `useLocation()`),
so navigation stranded page content at exit-opacity-0 and never mounted the new route. Removed the
mobile/toggle sidebar entirely (plain always-visible static panel -- no overlay, hamburger, or
slide) and dropped the mis-wired `AnimatePresence` wrapper so `<Routes>` renders directly (pages
self-animate). `tsc -b` + `vite build` clean.

### docs(readme): architecture diagram is now the full wired-vs-optional image
Replaced the cramped inline Mermaid flow with `docs/architecture-flow.png` -- the comprehensive
top-down diagram (wired = solid, optional/not-wired = dashed) -- for readability.

### fix(app-triggers): the app's upload/extract/build actions actually fire their Jobs
The Databricks App wired its upload->extract and build buttons to Jobs by **display name** and sent
mis-shaped run-now parameters, so both silently failed after a `-t dev` deploy. Three fixes:
(1) bind the two trigger jobs (`app_protocol_extract`, `app_ads_build`) as `job` app resources in
`resources/app.yml` and resolve them by bundle **id** (immune to the `[dev <user>]` display-name
prefix), with a stable name-substring fallback for local dev; (2) the bound `job` resource
auto-grants the app service principal `CAN_MANAGE_RUN`, so `run_now` is permitted (no separate
`permissions:` block); (3) send run-now params as a `{key: str(value)}` dict per the SDK's
`run_now(job_parameters=...)`, replacing the invalid `JobParameter(name=, value=)` list.
`bundle validate` clean. (Live deploy + trigger-API verification in progress at time of writing.)

### fix(gateway): stop sending `temperature` to modern reasoning models
The modern benchmark candidates (Claude Sonnet 5, GPT-5, Gemini 3 Flash) reject a non-default
`temperature` (Claude/Gemini: "does not support the temperature parameter"; GPT-5: only the default
is supported). `gateway_call` no longer sends `temperature` and defensively retries without an
unsupported parameter, so the Wave-4 benchmark returns real per-model scores instead of erroring.

### fix(wave3): `run_ads_build` reports success correctly (no `sys.exit(0)` false-fail)
The ADS build task called `sys.exit(0)` on success, which a serverless `spark_python_task` flags as
a task FAILURE (`SystemExit`) even though the dataset materialized correctly -- causing false-red
tasks + retries. It now returns normally on success and only raises on a genuine build failure; the
same change fixes all three parallel `run_poc_*` builds.
### feat(eval): reference-free extraction eval + conditional approval gate
Adds the reference-free extraction-eval funnel (Stages 1-3) between extraction and approval. Stage 1
is deterministic validators (`lib/pipeline/spec_validate.py`) — the hard gate, no model call; Stage 2
is a per-field confidence threshold; Stage 3 is reference-free model judges (`lib/pipeline/spec_eval_judge.py`)
— citation-supports-value + completeness — every call routed through `gateway_call` (masked + logged +
traced). A new `waves/wave3_ads_build/run_protocol_eval.py` task writes `raw.protocol_eval` + the
`raw.protocol_review_queue` view. `approve_protocols.py` is now CONDITIONAL: it skips (fails closed on)
any spec where `eval_ok != True` and records a `protocol_eval_blocked` audit event — the analyst human
e-sign path is unchanged. App gains `/api/protocols/review_queue` and eval-signal columns on the list.
Config-driven via the `extraction_eval` block in `demo.config.yaml`.

### refactor(wave4): rename the Wave 4 dir + job key to `wave4_model_benchmark`; drop Supervisor framing
Wave 4 is model benchmarking + cost only; it does not run an Agent-Bricks orchestrator. Renamed the
Wave 4 directory and job/resource key to `wave4_model_benchmark` and updated every code/doc reference. Removed the orphaned
`agents.supervisor` config key and reconciled the doc narration (the deterministic native design uses
the ADS Builder + native Databricks Workflows). Design docs describing the originally-intended
orchestration are marked "originally-intended — superseded, not built" rather than rewritten.

### feat(observability): MLflow tracing on gateway calls + a wave4 tracking run
`lib/pipeline/gateway.py` now opens an MLflow span (`span_type=LLM`) around every `gateway_call`,
recording the masked input, output, `tokens_in`/`tokens_out`, latency and estimated cost. Spans go
to an experiment derived from `cfg().initiative` and are stored in Unity Catalog Delta trace tables
under the audit schema **when available** (`mlflow.set_experiment(experiment_name=...,
trace_location=UnityCatalog(catalog_name, schema_name, table_prefix))`). `waves/wave4_model_benchmark/run_benchmark_live.py`
now wraps the benchmark in `mlflow.start_run()` — `log_params` (candidate models, PoC ids, KB size)
+ per-model `log_metrics` (avg kb_grounding / hallucination_rate / faithfulness / sql_validity, total
tokens, total cost, avg latency). **Fully guarded**: if mlflow is absent, an import fails, or the
UC-binding API isn't available on the serverless runtime, tracing/tracking degrades to a no-op (or the
default experiment store) and the pipeline still deploys and runs to completion. `mask_phi` and the
`gateway_inference` log row are unchanged.

### feat(cost): de-stub `cost_report.py` — real inference-log read + system-table enrichment
`waves/wave4_model_benchmark/cost_report.py` no longer computes on mock data. It now reads
`cfg().inference_table` (the in-process `gateway_inference` Delta log) via Spark SQL and aggregates
authoritative per-model spend (sum `tokens_in`/`tokens_out`, sum `cost_usd`, call count, avg cost/call),
writes a real `cost_report.md`, and checks it against the monthly cap. Per-PoC cost is a best-effort
estimate from `kb.bench_results` token counts × each model's blended $/token (the inference log has no
`poc_id` column and its schema is intentionally unchanged). Optional, guarded enrichment reads
`system.serving.endpoint_usage` (joined to `system.serving.served_entities` on `served_entity_id`) for
authoritative token counts and `system.billing.usage` × `list_prices` for actual $. The module docstring
documents the **ingestion-delay caveat**: system tables are authoritative but lagged (a run can't read
its own rows immediately), so the inference log is the immediate/primary source and system tables are
best-effort enrichment. Cost-estimate rates import `_RATES` from `gateway.py` (single source of truth).

### docs(ai-functions): note that `ai_extract` v2.1 features are server-side + runtime-gated
Added a comment at the `ai_extract` call site in `waves/wave1_synth_bronze/protocol_extract.py`
clarifying that `mode='precision'` + `enableConfidenceScores`/`enableCitations` are SERVER-SIDE
Databricks AI Function features selected via the options map — not a client SDK / pip upgrade — and
need only a recent-enough serverless runtime (AI Functions baseline DBR 15.1+; `ai_parse_document`
needs DBR 17.3+). Corrected the same in `CLAUDE.md`.

### fix(docs): correct now-inaccurate model + FM-availability notes in `CLAUDE.md`
`CLAUDE.md` line ~86 now reads default `databricks-claude-sonnet-5` (candidate set sonnet-5 / gpt-5-5 /
gemini-3-7-flash) instead of the retired `databricks-meta-llama-3-3-70b-instruct`. The "only these three
legacy pay-per-token FMs work" limit note is replaced by: all workspace Foundation Model API endpoints
are usable, and the benchmark uses the modern cross-vendor set. Added a one-line AI-Functions
platform note (server-side, runtime-gated).

### fix(gateway): model calls route directly to the pay-per-token FM endpoint (403 fix)
The native `ads-ai-gateway` route is an `external_model` → pay-per-token-FM proxy, and Databricks
returns **403** when you query such a route (`external_model` is for external providers / custom
served models, not the workspace's own PPT Foundation Models). `lib/pipeline/gateway.py` now POSTs
**directly** to `/serving-endpoints/{model}/invocations`; PHI-masking + audit-logging stay
in-process. The `ads-ai-gateway` endpoint is **off the query path**, kept only as a documented
placeholder for the guardrail/logging config it carries. (commit `04bf887`)

### change(models): modern cross-vendor benchmark candidates; default = `claude-sonnet-5`
Replaced the dated candidate set (`databricks-meta-llama-3-3-70b-instruct`,
`databricks-gpt-oss-120b`, `databricks-llama-4-maverick`) with a current cross-vendor set —
`databricks-claude-sonnet-5`, `databricks-gpt-5-5`, `databricks-gemini-3-7-flash` — a current
cross-vendor pay-per-token set. `models.default` is now `databricks-claude-sonnet-5`. Changed in
`demo.config.yaml` + `app/demo.config.yaml`; cost-estimate rates for the three models added to
`lib/pipeline/gateway.py` `_RATES` (placeholders — refine against the live price list).

### change(wave1): `ai_extract` upgraded to v2.1 precision mode with confidence + citations
`waves/wave1_synth_bronze/protocol_extract.py` now calls `ai_extract` with
`map('version','2.1','mode','precision','enableConfidenceScores','true','enableCitations','true')`.
In this mode each `response` field returns as an object `{value, citation_ids, confidence_score}`
plus a top-level `metadata` block, so every field read migrates to `:value` (arrays unwrap element
objects defensively) and `extracted_json` is rebuilt to the bare-scalar shape the deterministic
standardizer consumes. `confidence_json` now captures the metadata (citations) + per-field
confidence and citation_ids — was an `r:metadata`-only stub — to drive the review queue. Precision
mode's < 20,000-char input cap is enforced by truncating oversized protocols (logged warning), so a
long document degrades instead of hard-failing.

### fix(docs): correct now-inaccurate "routed through the gateway" prose
Following the 403 fix, corrected wording in `waves/wave5_serving_audit_app/work_instruction.py`
and the (now-removed) Wave-4 supervisor simulation that implied model calls route *through*
`ads-ai-gateway`. They now state calls route directly to the FM endpoint, with PHI-masking + audit
logging in-process (`lib/pipeline/gateway.py`).

### fix(serving): no `CREATE_CATALOG` required — app reads Postgres directly
When the deployer lacks `CREATE CATALOG` on the workspace metastore, the old
serving path (which registered a new Lakebase UC catalog `ads_lakebase` via
`databricks postgres create-catalog`) is blocked. Re-architected so the full serving path needs no new
UC catalog and no `CREATE_CATALOG` — verified on the synthetic PoCs.

- **`scripts/setup_lakebase.sh`** — the `create-catalog` step is now **gated** behind opt-in
  `LAKEBASE_REGISTER_UC_CATALOG=1` (default OFF). It is the ONLY step that needs
  `CREATE_CATALOG`; it exists only for optional UC-governed (Lakehouse-federated) access to
  the Lakebase DB. The default run skips it.
- **`scripts/setup_synced_tables.sh`** — `create-synced-table` now targets the EXISTING owned
  catalog + a `public` UC schema (`{catalog}.public.{target}`), ensured with `CREATE SCHEMA`
  (which the deployer has), NOT a new Lakebase catalog. The UC schema maps 1:1 to the Postgres
  schema, so gold lands in serving Postgres `public.ads_output` (3090: poc_low 3023 + poc_med
  67) and `public.cohort_summary` (3) — exactly where the app reads. **Verdict:**
  `create-synced-table` does NOT require `CREATE_CATALOG` when it targets an existing catalog
  you can write to.
- **`demo.config.yaml`** — `lakebase.catalog` re-marked OPTIONAL (opt-in only); new
  `lakebase.synced_uc_schema: public` (UC schema hosting the synced online views == the
  Postgres schema the app reads). `app/demo.config.yaml` re-vendored to match.
- **Both scripts** now read `demo.config.yaml` with a stdlib-only, indentation-aware parser
  (no PyYAML dependency), so they run on machines without `pip install pyyaml`.
- **`app/backend/app.py`** unchanged — still reads Postgres directly.

### rearch(lakebase): provision infra control-plane — zero `w.postgres` in serverless waves
Any `w.postgres` SDK call executed inside a serverless job-task kernel hard-crashes the
kernel ("Python process exited unexpectedly", native, before psycopg) — verified on live
runs. The same operations succeed from the control plane via the `databricks postgres` /
`databricks psql` CLI. So all Lakebase INFRASTRUCTURE is now provisioned control-plane, and
the serverless waves make **zero `w.postgres` calls**.

- **NEW `scripts/setup_lakebase.sh`** (control-plane, pre-deploy, idempotent): `create-project`
  the Autoscaling project, `create-database` the serving (`ads_serving_pg`) + app-state
  (`ads_app`) Postgres DBs on the production branch (body
  `{"spec":{"postgres_database":…,"role":<full-role-path>}}` with `--database-id <hyphen-id>` +
  `--replace-existing`; `spec.role` derived live from the auto-created `databricks_postgres` DB;
  resource ids are DNS-valid hyphenated forms). UC-catalog registration is now an **optional**
  opt-in step (see the fix entry above), not the default. Reads all names from `demo.config.yaml`.
- **NEW `scripts/setup_synced_tables.sh`** (control-plane, post-deploy + post-gold):
  `create-synced-table` (SNAPSHOT, one-way Delta→PG) for `ads_output` (pk `row_id`) +
  `cohort_summary` (pk `study_id`), sourced from the REAL gold serving schema
  (`{catalog}.{schemas.serving}.{target}` — fixes a stale `ads_serving.*` source in the old
  in-kernel path); app-state DDL (`review_queue`/`sessions`/`sign_offs`) + app-SP grants via
  `databricks psql`.
- **`waves/wave0_foundation/provision_lakebase.py`** → pure no-op NOTE (was in-kernel
  `w.postgres` DB provisioning). Wave 0 keeps uc_foundation + gateway.
- **`waves/wave5_serving_audit_app/setup_sync.py` + `grant_app_sp.py`** → pure no-op NOTES (was
  in-kernel `w.postgres` catalog/synced-table/app-state DDL + SP grant, and the spark+psycopg
  direct-load fallback, which inherently required in-kernel `w.postgres` and is dropped —
  managed `create-synced-table` run control-plane is now the serving path). Wave 5's serverless
  work is the audit schema (`setup_audit.py`) only.
- **`resources/jobs.yml`** env dependencies: wave0 dropped `psycopg[binary]` (keeps
  `pyyaml`+`databricks-sdk` for gateway); wave5 → `pyyaml` only. Seed deps `reportlab` +
  `python-docx` moved from the wave1 seed's RUNTIME `pip install` into the `wave1_synth_bronze`
  and `app_protocol_extract` env deps (both run `protocol_extract.py`, which seeds sample
  PDFs/DOCX on an empty volume).
- **`scripts/seed_protocol_pdfs.py`** removed the runtime `subprocess pip install reportlab
  python-docx` (runtime pip into a live serverless kernel is the same crash-hazard class as
  in-kernel `w.postgres`).
- **`app/backend/app.py`** unchanged: keeps the psycopg Lakebase read + `w.postgres`
  token-mint. This runs in the app CONTAINER (not the serverless ipykernel that crashes) and
  is the documented Databricks-Apps Lakebase pattern (no password is injected — the app mints
  the OAuth token); it is already warehouse-guarded + bounded-timeout so it can never hang.
- **`README.md`** Deploy section rewritten to the hands-off order: `setup_lakebase.sh` →
  `bundle deploy` → run wave0/1/3/4 → `setup_synced_tables.sh` → run wave5 → app.

### migrate(lakebase): Provisioned -> Autoscaling
The retired Provisioned Lakebase tier (`w.database` SDK / `/api/2.0/database/*` REST,
static instances) is fully removed; the bundle now targets Lakebase Autoscaling
(project -> production branch -> primary endpoint -> databases). The full bundle
deploys and runs with a single documented prereq:
`databricks postgres create-project <project>` -> `bundle deploy` -> `bundle run wave0..wave5`.

- **`demo.config.yaml` / `app/demo.config.yaml`** `lakebase:` block reshaped: `instance` ->
  `project` (`rwe-ads-lakebase`), plus `branch: production`, `endpoint: primary`,
  `bound_database: databricks_postgres`, `storage_catalog` (for the synced-table pipeline),
  `pg_version: 17`. Serving/app DB + synced-table entries unchanged.
- **`lib/config.py` (+ vendored `app/lib/config.py`)** accessors updated to the Autoscaling
  shape: `lakebase_project`, `lakebase_branch`, `lakebase_endpoint_id`, `lakebase_bound_db`,
  `lakebase_storage_catalog`, and resource-path helpers `lakebase_branch_path` /
  `lakebase_endpoint_path` / `lakebase_database_path(db)`. `lakebase_instance` removed.
- **`resources/app.yml`** legacy `database` app-resource key (`instance_name`/`database_name`)
  -> `postgres` key (`branch` + `database` resource paths + `permission: CAN_CONNECT_AND_CREATE`),
  bound to the auto-created `databricks_postgres` DB. Parameterized via new bundle vars
  `lakebase_project` / `lakebase_branch` / `lakebase_bound_database` (`databricks.yml`). The
  resource auto-injects `PGHOST`/`PGUSER`/`LAKEBASE_ENDPOINT`; the app reaches `ads_serving_pg`
  / `ads_app` by dbname-override on the same endpoint.
- **`waves/wave0_foundation/provision_lakebase.py`** rewritten to Autoscaling: ensures
  `databricks-sdk>=0.81.0` at task start, verifies the project exists (list-projects; tolerant),
  then `CREATE DATABASE` the serving + app-state DBs on the production-branch primary endpoint
  (OAuth credential from `w.postgres.generate_database_credential`).
- **`waves/wave5_serving_audit_app/setup_sync.py`** synced tables now via
  `databricks postgres create-synced-table` (SNAPSHOT) + `create-catalog`, with the
  runtime-independent psycopg direct-load kept as the serving fallback; all connections use
  the Autoscaling endpoint credential.
- **`waves/wave5_serving_audit_app/grant_app_sp.py`** SP grant connects via the Autoscaling
  endpoint credential (was `/api/2.0/database/instances|credentials`).
- **`app/backend/app.py`** `_lakebase_query` now uses `PGHOST`/`PGUSER`/`LAKEBASE_ENDPOINT`
  + `w.postgres.generate_database_credential` with dbname-override — the real serving read
  (no longer the retired REST path).
- **Removed** dead `app/backend/lakebase.py` (unused; called the non-existent
  `w.postgres.get_credential`); dropped its orphaned `sqlalchemy` dependency from
  `app/requirements.txt` and raised the app SDK floor to `>=0.81.0`.
- **`resources/jobs.yml`** wave0 + wave5 job environments pin `databricks-sdk>=0.81.0`.
- **Docs:** README Deploy section documents the `create-project` prereq and the exact
  `create-project -> deploy -> run wave0..wave5` order; golden rules note "no CREATE PROJECT".
- **Tests:** `tests/test_config.py` Lakebase test asserts `lakebase_project` + resource paths.

### Added
- Genie Agent (analyst Q&A) placeholder: a new `genie:` block in `demo.config.yaml`
  (and the app copy), `enabled: false`. This is the one place an agent genuinely
  fits — read-only natural-language Q&A over the APPROVED ADS outputs
  (`ads_serving.ads_output`, `ads_serving.cohort_summary`), after review + e-sign.
  No Genie space, job, or endpoint is created and no code reads it yet; the build
  path stays deterministic.

### Changed — naming aligned to as-built (deterministic) behavior
- De-agented the `lib/pipeline/` build-path docstrings (`__init__.py`, `prompts.py`,
  `ads_builder.py`): the builder is deterministic (fills approved-SQL templates from
  the reviewed protocol spec), so "agent" was a misnomer in the prose. No behavior change.
- Renamed job/dir `wave3_ads_agent` -> `wave3_ads_build`. The ADS build path is
  deterministic: it fills approved-SQL templates from the reviewed protocol spec
  (no model generates the query), so "agent" was a misnomer.
- Renamed shared library `lib/agent/` -> `lib/pipeline/`. It holds KB retrieval,
  token substitution, validation, the ADS builder, and prompts — deterministic
  pipeline helpers, not autonomous agents.
- Renamed schema key `schemas.agents` -> `schemas.kb` (config + `lib/config.py`
  `agents_schema()` -> `kb_schema()` + callers). This schema holds the
  approved-SQL knowledge base, prompts, and eval datasets — not agents.
  (in progress)

### Documentation
- README Wave-3 corrected: "ADS Builder" (not "ADS Builder Agent"); approved-SQL
  KB retrieval via Vector Search with keyword fallback; noted "template
  substitution, no model call"; validation is `EXPLAIN` (validate-don't-execute).
- README naming note added: the remaining `agent`/`supervisor` identifiers reflect
  the project's original agentic framing; the core build path is deterministic.
- Added internal reference notes (kept out of this repo).

### Planned
- Rename audit column `agent_version` -> `builder_version` in the `gxp_audit` table.
  This is a DEPLOYED audit-table schema column (`setup_audit.py` DDL + dataclass +
  app SELECT + tests + docs), so it ships with a migration: add new column ->
  backfill -> repoint writers/readers -> drop old column.
- Rename schema value `ads_agents` -> `ads_kb` for visible consistency with the
  `kb` key. This renames a DEPLOYED Unity Catalog schema, so it ships with a
  migration: create `ads_kb` -> move objects (KB table, prompts, eval datasets) ->
  repoint references -> drop `ads_agents`. Standard schema-rename migration.
