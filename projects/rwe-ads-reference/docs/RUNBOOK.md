# Deployment Runbook

> **⚠️ As-built note (current bundle).** This document describes the original PoC design. As deployed today the pipeline is **deterministic + native**: orchestration is **Databricks Workflows** (no Agent Bricks Supervisor); the ADS build **assembles approved-SQL KB templates + validates via `EXPLAIN`** (no model generates queries); serving is **Lakebase Autoscaling** (the retired Provisioned-tier references below — `w.database`, `<your-lakebase-project>`, `list-synced-tables --instance` — no longer apply; provision via `databricks postgres create-project`); and **Genie** (analyst Q&A) ships as a **disabled config placeholder** — not built or deployed in this bundle. For current deploy commands follow **`CLAUDE.md`** and **`.claude/skills/deploy-full-bundle`**.


**RWE ADS Automation — Deployment & Operational Guide**

---

## 1. Prerequisites

### 1.1 Workspace Setup
- **Workspace**: <your-workspace>.cloud.databricks.com (or target environment)
- **Catalog**: rwe_ads_catalog (pre-existing, UC enabled)
- **Compute**: Serverless SQL warehouse configured
- **Lakebase**: Instance `<your-lakebase-project>` exists with PG 16

### 1.2 CLI & SDK
```bash
# Install Databricks CLI (latest)
curl -fsSL https://raw.githubusercontent.com/databricks/cli/main/install.sh | sh

# Install Databricks Bundle CLI
databricks bundle --version

# Install Python SDK
pip install databricks-sdk==0.18.0+
pip install pyyaml pyspark mlflow

# Verify
databricks workspace ls / 2>&1 | grep -q "Workspace" && echo "✓ CLI works"
```

### 1.3 Configuration
```bash
# Ensure DATABRICKS credentials are set
export DATABRICKS_HOST=https://<your-workspace>.cloud.databricks.com
export DATABRICKS_TOKEN=<your-PAT>

# Or use CLI profile
databricks auth login

# Verify
databricks workspace list-jobs | head -1
```

### 1.4 Clone Repo
```bash
cd ~/code
git clone https://github.com/<your-org>/rwe-ads-automation.git
cd Analytical_Dataset_RWE

# Create your local config from the tracked placeholder example, then fill in your
# workspace / warehouse / catalog. demo.config.yaml is gitignored, so your real
# values never get committed. (app/demo.config.yaml is a build artifact regenerated
# by build.py from the root config — keep the committed copy as placeholders; never
# commit real values into it.)
cp demo.config.example.yaml demo.config.yaml
$EDITOR demo.config.yaml

# Verify structure
ls -la waves/wave0_foundation waves/wave5_serving_audit_app

# Verify config
python3 -c "from lib.config import cfg; c = cfg(); print(f'Catalog: {c.catalog}, Compute: {c.compute}')"
```

---

## 2. Local Setup & Testing

### 2.1 Install Dependencies
```bash
pip install --quiet pyyaml pytest psycopg

# Optional: databricks-sdk for Lakebase provisioning
pip install --quiet databricks-sdk

# Verify
python3 -c "import yaml, pytest; print('✓ Local deps installed')"
```

### 2.2 Run Local Tests
```bash
cd /path/to/Analytical_Dataset_RWE

# Run all tests (no workspace required)
pytest tests/ -v

# Run specific test file
pytest tests/test_config.py -v

# Expected output:
# tests/test_config.py::TestConfigLoading::test_config_loads PASSED
# tests/test_idempotency.py::TestIdempotencyPatterns::test_create_table_has_if_not_exists PASSED
# ... (all tests should PASS)
```

### 2.3 Generate Work Instruction
```bash
python3 waves/wave5_serving_audit_app/generate_wi.py
# Output: docs/VALIDATED_WORK_INSTRUCTION.md

# Verify
cat docs/VALIDATED_WORK_INSTRUCTION.md | head -20
```

---

## 3. Deployment via Databricks Bundle

### 3.1 Validate Bundle
```bash
databricks bundle validate -t dev

# Expected output:
# ✓ Resources:
#   - jobs/wave0_foundation
#   - jobs/wave1_synth_bronze        # includes the `medallion` pipeline task — no standalone wave2 job
#   - jobs/wave3_ads_build
#   - jobs/wave4_model_benchmark
#   - jobs/wave5_serving_audit
#   - jobs/app_protocol_extract, jobs/app_ads_build   # app-triggered helper jobs
#   - pipelines/ads_medallion (the medallion pipeline), apps/rwds_ads_studio

# Check for errors; if any, review databricks.yml and resources/*.yml
```

### 3.2 Deploy Bundle

> **Required `--var` flags.** Five bundle variables are resolved at `bundle deploy`/`bundle run`
> and **cannot** read `demo.config.yaml`, so you must pass them explicitly (values must match your
> config). Skipping them is the #1 setup failure — the pipeline/app come up pointing at the
> placeholder `rwe_ads_catalog` and error. Set them once and append `$VARS` to every deploy/run:
> ```bash
> VARS="--var uc_catalog=<your catalog> \
>       --var silver_schema=<your schemas.curated> \
>       --var warehouse_id=<your workspace.warehouse_id> \
>       --var lakebase_project=<your lakebase.project> \
>       --var notification_email=<your email>"
> ```

```bash
# Deploy to dev
databricks bundle deploy -t dev $VARS

# Expected output:
# Updated N resources (jobs, pipeline, app)

# Verify deployment
databricks bundle list -t dev
```

### 3.3 Run Waves Sequentially

#### Wave 0: Foundation
```bash
databricks bundle run wave0_foundation -t dev $VARS

# Logs:
# === Wave 0 foundation for rwe-ads-automation @ rwe_ads_catalog ===
# [wave0] schemas
#   SQL> CREATE SCHEMA IF NOT EXISTS rwe_ads_catalog.ads_raw ...
# [wave0] protocols volume
#   SQL> CREATE VOLUME IF NOT EXISTS ...
# [wave0] approved-SQL KB table
#   SQL> CREATE TABLE IF NOT EXISTS ...
#   [KB seeding] 9 snippets inserted via MERGE
# [wave0] gateway inference table placeholder
# [wave0] done.

# Verify
databricks sql "SELECT COUNT(*) as snippet_count FROM rwe_ads_catalog.ads_kb.approved_sql_kb"
# Expected: 8
```

#### Wave 1: Synthetic RWD (Bronze)
```bash
databricks bundle run wave1_synth_bronze -t dev $VARS
# This job ALSO runs the medallion pipeline (its `medallion` pipeline task) — silver + gold
# are built here. There is no separate `wave2_medallion` job to run.

# Logs:
# === Wave 1 synthetic RWD @ rwe_ads_catalog ===
# [synth] Generating 50k patients, seed=20260812
# [synth] Generating claims, pharmacy, lab events
# [wave1] done. Bronze tables ready.

# Verify
databricks sql "SELECT COUNT(*) as n_patients FROM rwe_ads_catalog.ads_raw.synthetic_patients"
# Expected: 50000
```

#### Medallion (Silver → Gold) — runs INSIDE Wave 1

The medallion pipeline is the `medallion` **pipeline task inside `wave1_synth_bronze`** (a
serverless Spark Declarative Pipeline, `resources/pipelines.yml` → `ads_medallion`). There is
**no standalone `wave2_medallion` job** — running Wave 1 (above) triggers it automatically. It
conforms silver (`ads_curated`) and builds the gold analytic base in `ads_serving`
(`patient_timeline`, `eligibility_periods`, `code_rollups`). The ADS outputs `ads_output` /
`cohort_summary` are built later by the **Wave 3** ADS builder — not by the medallion pipeline.

```bash
# (No separate run — running Wave 1 above already triggered the medallion pipeline.)

# Verify the medallion gold analytic base:
databricks sql "SELECT COUNT(*) as n FROM rwe_ads_catalog.ads_serving.patient_timeline"
# Expected: > 0 (longitudinal events across the synthetic cohort)
```

#### Wave 3: ADS Builder (deterministic SQL assembly)
```bash
databricks bundle run wave3_ads_build -t dev $VARS

# Logs:
# === Wave 3 ADS builder (deterministic) ===
# NOTE: the "[agent] Supervisor agent" line below = originally-intended orchestration
#       — superseded, not built. As built, native Databricks Workflows sequences the
#       ADS Builder directly (see the as-built note at top).
# [agent] Supervisor agent initialized
# [agent] KB retrieval + SQL generation for poc_low
# [builder] Assembled approved-SQL for cohort:
#   WITH dx AS (
#     SELECT patient_id, MIN(event_date) AS index_date
#     FROM rwe_ads_catalog.ads_serving.patient_timeline
#     WHERE event_type = 'dx' AND code IN ('ICD10_CODE')
#       AND event_date BETWEEN ...
#   )
#   SELECT ...
# [agent] Validation: VALID ✓
# [agent] Manifest written (protocol_version, KB_vers, SQL, model, eval_scores)
# [wave3] done.

# Verify
databricks sql "SELECT COUNT(*) as n_manifests FROM rwe_ads_catalog.ads_audit.repro_manifest"
# Expected: >=1 (one per generated ADS)
```

#### Wave 4: Supervisor & Full Validation *(originally-intended — superseded, not built; as built this is `wave4_model_benchmark`: model benchmark + cost, orchestrated by native Databricks Workflows. See the as-built note at the top.)*
```bash
databricks bundle run wave4_model_benchmark -t dev $VARS

# Logs:
# === Wave 4 model benchmark + cost (native Databricks Workflows) ===
# NOTE: the "[supervisor]" lines below = originally-intended orchestration — superseded,
#       not built. As built, wave 4 is model benchmark + cost, sequenced by native
#       Databricks Workflows (see the as-built note at top).
# [supervisor] Orchestrate poc_low, poc_med, poc_high
# [supervisor] poc_low: Generated SQL, cohort N=...
# [supervisor] poc_med: Generated SQL with covariates
# [supervisor] poc_high: Generated SQL with time-varying exposure
# [supervisor] All validations passed
# [benchmark] Eval metrics logged to ads_kb.bench_results
# [wave4] done.

# Verify
databricks sql "SELECT COUNT(*) as n_ads FROM rwe_ads_catalog.ads_kb.bench_results"
# Expected: 3 (one per PoC study)
```

#### Wave 5: Audit & Serving
```bash
databricks bundle run wave5_serving_audit -t dev $VARS

# Logs:
# === Wave 5 audit & serving ===
# [wave5] reproducibility manifest table
#   SQL> CREATE TABLE IF NOT EXISTS rwe_ads_catalog.ads_audit.repro_manifest ...
# [wave5] GxP audit event log (immutable)
#   SQL> CREATE TABLE IF NOT EXISTS rwe_ads_catalog.ads_audit.gxp_audit ...
# [wave5-sync] Lakebase synced tables
#   [sync] rwe_ads_catalog.ads_serving.ads_output → <your-lakebase-project>/ads_serving_pg/ads_output
#   [sync] Would create continuous synced table
# [wave5-sync] Lakebase app-state DB
#   [lakebase-app] Instance: <your-lakebase-project>, DB: ads_app
#   [app-state DDL] Would create:
#     - ads_app.review_queue
#     - ads_app.sessions
#     - ads_app.sign_offs
# [wave5] done.

# Verify audit tables
databricks sql "SELECT COUNT(*) as n_manifests FROM rwe_ads_catalog.ads_audit.repro_manifest"
# Expected: >=1

databricks sql "SELECT COUNT(*) as n_events FROM rwe_ads_catalog.ads_audit.gxp_audit"
# Expected: >=1

# Verify Lakebase sync status (if live Lakebase available)
# databricks lakebase list-synced-tables --instance <your-lakebase-project>
```

### 3.4 Deploy the App (via the bundle)

The app resource (`rwds_ads_studio` in `resources/app.yml`) binds its SQL warehouse, Lakebase,
and app-triggered jobs **only through `bundle deploy`**. Do **not** hand-write `resources/app.yml`
or run a code-only `databricks apps deploy` — that leaves the resource bindings null and the app's
buttons go dead. Build the frontend, then deploy + run through the bundle:

```bash
# 1. Build the frontend bundle (vendors config + Vite build → app/static)
cd app && python build.py && cd ..

# 2. Deploy the bundle — creates the app AND binds its resources (warehouse / Lakebase / jobs)
databricks bundle deploy -t dev $VARS

# 3. Deploy the app code + start it (use the app resource KEY, not the display name)
databricks bundle run rwds_ads_studio -t dev $VARS

# Verify
databricks apps list | grep rwe-ads    # matches your app name (e.g. <initials>-rwe-ads-app)
```

---

## 4. Operational Tasks

### 4.1 Verify End-to-End Status
```bash
# Check schemas exist
databricks sql "SHOW SCHEMAS IN rwe_ads_catalog" | grep ads_

# Check tables exist
databricks sql "SELECT table_name FROM rwe_ads_catalog.ads_kb.information_schema.tables WHERE table_schema = 'approved_sql_kb'"

# Check audit trail
databricks sql "SELECT COUNT(*) FROM rwe_ads_catalog.ads_audit.gxp_audit"

# Check Lakebase sync (if connected)
# databricks lakebase list-synced-tables --instance <your-lakebase-project> --database ads_serving_pg
```

### 4.2 Verify Chain Integrity
```bash
# Run chain verification (Python script)
python3 << 'EOF'
from waves.wave5_serving_audit_app.setup_audit import verify_chain
from lib.config import cfg

c = cfg()
table_fqn = f"{c.audit}.gxp_audit"
is_valid, errors = verify_chain(table_fqn)

if is_valid:
    print(f"✓ Audit chain intact ({table_fqn})")
else:
    print(f"✗ Tampering detected:")
    for e in errors:
        print(f"  {e}")
EOF
```

### 4.3 Monitor Costs
```bash
# Query gateway inference log (if gateway enabled)
databricks sql "
SELECT
  DATE(request_ts) as date,
  model,
  COUNT(*) as n_calls,
  SUM(cost_usd) as cost_usd
FROM rwe_ads_catalog.ads_audit.gateway_inference
GROUP BY DATE(request_ts), model
ORDER BY date DESC
LIMIT 10
"

# Expected: <$2000/month (spend cap)
```

### 4.4 Restart a Wave (Idempotent Re-run)
```bash
# Any wave can be re-run; idempotent DDL prevents errors
databricks bundle run wave0_foundation -t dev $VARS   # Safe to re-run
databricks bundle run wave1_synth_bronze -t dev $VARS # Safe to re-run (re-triggers the medallion pipeline)
databricks bundle run wave5_serving_audit -t dev $VARS # Safe to re-run

# Verify: should report "Already exists" or update with no errors
```

### 4.5 Tail Job Logs
```bash
# List jobs
databricks jobs list | grep -i wave

# Tail logs for job
databricks jobs runs list --job-name wave1_synth_bronze --limit 1
RUNID=$(databricks jobs runs list --job-name wave1_synth_bronze --limit 1 | awk 'NR==2 {print $1}')
databricks jobs runs get-output --run-id $RUNID

# Follow live logs (if job running)
# databricks jobs runs tail --run-id $RUNID
```

---

## 5. Migration to your production workspace (or Another Environment)

Everything is a **Databricks Asset Bundle** driven by a single source of truth
(`demo.config.yaml`). Porting = edit one config file + one bundle target, then
`bundle deploy` / `bundle run`. All enablement (gateway, Lakebase serving, app SP
grants, benchmarking) is committed and DAB-wired, so nothing is manual/ad-hoc.

**5.0 Prerequisites on the target workspace** (these are NOT created by the bundle):
- The **catalog** already exists (golden rule: no `CREATE CATALOG`). Grant the deploy identity `CREATE SCHEMA`/`CREATE VOLUME`/`USE CATALOG`.
- A **serverless SQL warehouse** exists → put its id in config.
- A **Lakebase instance** exists (the bundle creates the *databases* inside it, not the instance).
- The candidate **foundation models** are pay-per-token-enabled there (verify with a test query; swap `models.candidates` if not).
- For the *managed continuous* synced-table pipeline, the metastore needs a **storage root** (otherwise `setup_sync.py` auto-falls back to the direct Delta→Postgres load — serving still works).

**5.1 Edit `demo.config.yaml`** (the only place names live). Change:
```yaml
workspace: {host: <target-host>, warehouse_id: <target-serverless-warehouse-id>}
catalog: <target_catalog>                 # must already exist
lakebase: {instance: <target-lakebase-instance>, catalog: <uc-db-catalog-name>}
models: {candidates: [<PPT-enabled FMs on target>], default: <one of them>}
```
`schemas`, `synced_tables`, `gateway`, `app.name` usually stay as-is. Validate locally:
```bash
python3 -c "from lib.config import cfg; c=cfg(); print(c.catalog, c.host, c.warehouse_id, c.lakebase_instance)"
python3 -m pytest tests/ -q     # no-literal-catalog scan + idempotency + PHI-mask etc.
```

**5.2 Add / edit a bundle target** in `databricks.yml` (a `prod` template is already there — set its host):
```yaml
targets:
  prod:
    mode: production
    workspace:
      host: https://<target-host>
      root_path: /Workspace/Shared/.bundle/${bundle.name}/${bundle.target}
    run_as: {user_name: ${workspace.current_user.userName}}
```
Also update the bundle var default `warehouse_id` (in `databricks.yml`) to the target's warehouse.

**5.3 Deploy + run every wave** (set `$VARS` for your prod values, same pattern as §3.2):
```bash
databricks bundle validate -t prod
databricks bundle deploy   -t prod $VARS
databricks bundle run wave0_foundation      -t prod $VARS  # schemas/volume/KB/VS index/gateway/lakebase DBs
databricks bundle run wave1_synth_bronze     -t prod $VARS  # synth bronze + SDP medallion (the `medallion` pipeline task)
databricks bundle run wave3_ads_build        -t prod $VARS  # protocol eval + approve gate + poc_low ADS build
databricks bundle run wave4_model_benchmark -t prod $VARS  # live benchmark + cost
databricks bundle run wave5_serving_audit    -t prod $VARS  # audit + Lakebase serving + app SP grant
```

**5.4 Build + deploy the app** (`<initials>-rwe-ads-app`):
```bash
cd app && python build.py                              # vendors config + builds Vite -> app/static
# npm install --legacy-peer-deps  (frontend deps resolve from the public npm registry)
databricks bundle deploy -t prod $VARS                 # creates the app + binds its resources (warehouse/Lakebase/jobs); node_modules excluded
databricks bundle run rwds_ads_studio -t prod $VARS    # app resource KEY — deploys the app code + starts it
```
`wave5`'s `grant_app_sp` task grants the app SP live Lakebase reads automatically (it resolves the
SP from the deployed app). If you deploy the app *after* wave5, re-run `bundle run wave5_serving_audit`.

**Porting gotchas (learned on the synthetic PoCs):** seed the KB via the DataFrame MERGE in `setup.py`
(never hand-write `''`-escaped SQL — it silently corrupts literals); Lakebase is **Autoscaling** — provision DBs/catalog **control-plane** via `setup_lakebase.sh`
(the serverless waves make no `w.postgres`/`w.database` calls; the retired Provisioned REST approach no longer applies); `bundle sync` honors `.gitignore` but
`sync.include` overrides it (node_modules is explicitly excluded, `app/static` is tracked).

### 5.3 Production Checklist
Before going live on your production workspace:
- [ ] Governance council formal sign-off (SOP review)
- [ ] Security review (PHI controls, audit trail, access control)
- [ ] IT Operations sign-off (Lakebase provisioning, backup, DR)
- [ ] Data Quality review (synthetic → real RWD transition plan)
- [ ] Legal sign-off (HIPAA compliance, audit retention, e-signature validity)
- [ ] Load test (scale to 10M patients, 1B events; verify latency <5 sec)
- [ ] Incident response plan (audit trail tampering, gateway failure, Lakebase outage)

---

## 6. Troubleshooting

### 6.1 Config Resolution Error
```
Error: FileNotFoundError: demo.config.yaml not found; set ADS_CONFIG_PATH

Solution:
export ADS_CONFIG_PATH=/full/path/to/demo.config.yaml
python3 waves/wave0_foundation/setup.py
```

### 6.2 Workspace Authentication Failure
```
Error: PERMISSION_DENIED: not authorized to access this endpoint

Solution:
1. Verify DATABRICKS_HOST and DATABRICKS_TOKEN are set
2. Test: databricks workspace list-jobs
3. Check token hasn't expired: databricks auth token login
```

### 6.3 Catalog / Schema Not Found
```
Error: Schema rwe_ads_catalog.ads_raw does not exist

Solution:
1. Run wave0_foundation: databricks bundle run wave0_foundation -t dev
2. Verify: databricks sql "SHOW SCHEMAS IN rwe_ads_catalog"
```

### 6.4 Lakebase Connection Failure
```
Error: Failed to connect to Lakebase instance <your-lakebase-project>

Solution:
1. Verify Lakebase instance exists: databricks lakebase list-clusters
2. Check credentials are valid (if using SDK)
3. Verify service principal has CONNECT grant on ads_serving_pg database
```

### 6.5 Chain Verification Fails
```
Error: Row N hash mismatch: tampering detected

Solution:
1. Check if table was modified outside system (should not happen)
2. If intentional (e.g., data migration), regenerate manifests and audit trail
3. Report to compliance team (audit trail integrity issue)
```

---

## 7. Rollback Procedure

### 7.1 Rollback One Wave
```bash
# Drop wave-specific tables (CAREFUL!)
databricks sql "
DROP TABLE IF EXISTS rwe_ads_catalog.ads_serving.ads_output;
DROP TABLE IF EXISTS rwe_ads_catalog.ads_serving.cohort_summary;
"

# Re-run previous wave, then the failed wave
databricks bundle run wave1_synth_bronze -t dev $VARS   # re-runs synth bronze + the medallion pipeline
```

### 7.2 Full Rollback (Nuke & Rebuild)
```bash
# WARNING: Deletes all schemas and data!
databricks sql "
DROP SCHEMA IF EXISTS rwe_ads_catalog.ads_raw CASCADE;
DROP SCHEMA IF EXISTS rwe_ads_catalog.ads_curated CASCADE;
DROP SCHEMA IF EXISTS rwe_ads_catalog.ads_serving CASCADE;
DROP SCHEMA IF EXISTS rwe_ads_catalog.ads_kb CASCADE;
DROP SCHEMA IF EXISTS rwe_ads_catalog.ads_audit CASCADE;
"

# Rebuild from scratch
for wave in wave0_foundation wave1_synth_bronze wave3_ads_build wave4_model_benchmark wave5_serving_audit; do
  databricks bundle run $wave -t dev $VARS || echo "Wave $wave failed"
done
```

---

## 8. Monitoring & Alerts

### 8.1 Metrics to Monitor
- **Wave latency**: wave0 <1 min, wave1 (synth + medallion pipeline) <3 min, wave3-4 <5 min
- **Synced table lag**: <1 min (continuous sync)
- **Query latency**: App queries <1 sec (50k rows)
- **Cost**: Monthly spend <$2000
- **Audit chain**: verify_chain() runs daily, 0 tampering detections

### 8.2 Alerting (example)
```python
# Run daily via a job
from waves.wave5_serving_audit_app.setup_audit import verify_chain
from lib.config import cfg

c = cfg()
is_valid, errors = verify_chain(f"{c.audit}.gxp_audit")

if not is_valid:
    # Alert
    import smtplib
    # ... send email to security team
    raise Exception("Audit chain tampering detected!")

print("✓ Daily audit verification passed")
```

---

## 9. Quick Start Checklist

```bash
# 1. Clone repo
git clone ... && cd Analytical_Dataset_RWE

# 2. Install deps
pip install -q pyyaml pytest

# 3. Test locally
pytest tests/ -v

# 4. Verify config
python3 -c "from lib.config import cfg; print(cfg().catalog)"

# 5. Authenticate
databricks auth login

# 6. Validate bundle
databricks bundle validate -t dev

# 7. Deploy (set $VARS as in §3.2 — the five required --var flags)
databricks bundle deploy -t dev $VARS

# 8. Run wave0
databricks bundle run wave0_foundation -t dev $VARS

# 9. Check schema
databricks sql "SHOW SCHEMAS IN rwe_ads_catalog"

# 10. Done!
echo "✓ RWE ADS Automation deployed"
```

---

## Appendix: CLI Commands Reference

```bash
# Workspace
databricks workspace list-jobs
databricks workspace get-status /path/to/notebook

# Bundle
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle list -t dev
databricks bundle run WAVE_NAME -t dev

# SQL
databricks sql "SELECT ..."
databricks sql execute-query --notebook-path /path/to/sql

# Apps
databricks apps list
databricks apps get --name <initials>-rwe-ads-app
databricks apps deploy --name <initials>-rwe-ads-app --source-code-path app/

# Lakebase (if installed)
databricks lakebase list-clusters
databricks lakebase list-synced-tables --instance <your-lakebase-project>

# Help
databricks bundle --help
databricks jobs --help
```

---

*Runbook v1.0 — Last updated 2026-08-12*
