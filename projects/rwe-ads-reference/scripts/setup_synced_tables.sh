#!/usr/bin/env bash
# =============================================================================
# setup_synced_tables.sh — CONTROL-PLANE Lakebase serving sync + grants.
# =============================================================================
# WHY control-plane: `w.postgres` SDK calls crash a serverless job kernel
# (native, verified), so all Lakebase work runs from the operator's machine via
# the `databricks postgres` / `databricks psql` CLI. The serverless waves make
# ZERO `w.postgres` calls.
#
# Run this AFTER `bundle deploy` (so the app's service principal exists) and
# AFTER the gold ADS is built (wave1 medallion + wave3 build), i.e. after
# `bundle run wave0..wave4`. It:
#   1. creates one-way Delta -> Postgres synced tables (SNAPSHOT) for each gold
#      table (create-synced-table), sourced from the REAL gold serving schema,
#   2. creates the app-state tables (review_queue, sessions, sign_offs) in the
#      app-state DB via `databricks psql`, and
#   3. grants the app service principal live Postgres access on the serving DB
#      (SELECT) and the app-state DB (SELECT/INSERT/UPDATE/DELETE + CREATE).
#
# Idempotent: create-synced-table tolerates "already exists"; the DDL is
# CREATE TABLE IF NOT EXISTS; grants are naturally repeatable.
#
# NOTE (source-schema fix): the retired in-kernel setup_sync.py built the synced
# source as {catalog}.{config-source} = "{catalog}.ads_serving.<t>", but the gold
# tables are written to the serving schema (schemas.serving = ads_serving)
# via c.table("serving", ...). This script sources from the real gold location
# {catalog}.{serving_schema}.{target}.
#
# Usage:
#   scripts/setup_synced_tables.sh <DATABRICKS_PROFILE>
# Example:
#   scripts/setup_synced_tables.sh <your-profile>
#
# Requires: databricks CLI + a local `psql` client (the psql grants).
# =============================================================================
set -uo pipefail

PROFILE="${1:-${DATABRICKS_CONFIG_PROFILE:-}}"
if [ -z "$PROFILE" ]; then
  echo "usage: scripts/setup_synced_tables.sh <DATABRICKS_PROFILE>" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CFG="$REPO_ROOT/demo.config.yaml"
if [ ! -f "$CFG" ]; then
  echo "demo.config.yaml not found at $CFG" >&2
  exit 2
fi

# `databricks psql` (steps 2-3) requires a local `psql` client on PATH. Homebrew's
# libpq is keg-only (not symlinked into PATH), so if psql isn't already found, add
# the common keg bin dirs — makes the psql-backed steps runnable without a global
# postgres install.
if ! command -v psql >/dev/null 2>&1; then
  for d in /opt/homebrew/opt/libpq/bin /usr/local/opt/libpq/bin \
           /opt/homebrew/opt/postgresql@16/bin /usr/local/opt/postgresql@16/bin; do
    [ -x "$d/psql" ] && PATH="$d:$PATH"
  done
  export PATH
fi

# --- scalars from the single source of truth (portable read; no mapfile) -----
# No PyYAML dependency: a stdlib-only, indentation-aware reader parses the fixed
# set of scalars we need. Quote characters are built via chr() because this
# heredoc runs inside a <(...) process substitution and bash 3.2 (macOS default)
# naively scans the body for matching quotes — literal ' or " would break it.
if ! {
  read -r PROJECT
  read -r BRANCH
  read -r ENDPOINT
  read -r SERVING_DB
  read -r APP_DB
  read -r SYNCED_UC_SCHEMA
  read -r STORAGE_CATALOG
  read -r DATA_CATALOG
  read -r SERVING_SCHEMA
  read -r APP_NAME
} < <(python3 - "$CFG" <<'PY'
import sys, re
DQ, SQ = chr(34), chr(39)
lines = open(sys.argv[1]).read().splitlines()
def indent(s): return len(s) - len(s.lstrip(chr(32)))
def clean(v):
    v = v.strip()
    if v[:1] not in (DQ, SQ):
        m = re.search(r"\s#", v)
        if m: v = v[:m.start()]
    v = v.strip()
    if len(v) >= 2 and v[0] in (DQ, SQ) and v[-1] == v[0]:
        v = v[1:-1]
    return v
def scalar(spec, default=''):
    parts = spec.split('.')
    if len(parts) == 1:
        for ln in lines:
            if indent(ln) == 0 and ln.lstrip().startswith(parts[0] + ':'):
                return clean(ln.split(':', 1)[1])
        return default
    sec, key = parts
    insec = False
    for ln in lines:
        if not ln.strip() or ln.lstrip().startswith('#'): continue
        if indent(ln) == 0:
            insec = ln.lstrip().startswith(sec + ':'); continue
        if insec and indent(ln) == 2 and ln.lstrip().startswith(key + ':'):
            return clean(ln.split(':', 1)[1])
    return default
print(scalar('lakebase.project'))
print(scalar('lakebase.branch', 'production'))
print(scalar('lakebase.endpoint', 'primary'))
print(scalar('lakebase.database'))                          # serving Postgres DB
print(scalar('lakebase.app_state_db'))                      # app-state Postgres DB
print(scalar('lakebase.synced_uc_schema', 'public'))        # UC schema hosting synced online views == Postgres schema the app reads
print(scalar('lakebase.storage_catalog') or scalar('catalog'))  # regular UC catalog for pipeline metadata
print(scalar('catalog'))                                    # data catalog
print(scalar('schemas.serving'))                            # REAL gold serving schema
print(scalar('app.name'))                                   # app name (for SP resolution)
PY
); then
  echo "failed to read demo.config.yaml (stdlib parser error)" >&2
  exit 2
fi

BRANCH_PATH="projects/${PROJECT}/branches/${BRANCH}"
FAILS=0
echo "=== setup_synced_tables (control-plane) — project ${PROJECT} on profile ${PROFILE} ==="

run_tolerant() {
  local desc="$1"; shift
  local out rc
  out="$("$@" 2>&1)"; rc=$?
  if [ "$rc" -eq 0 ]; then
    echo "  [ok] ${desc}"
  elif printf '%s' "$out" | grep -qiE "already exists|ALREADY_EXISTS|RESOURCE_ALREADY_EXISTS|RESOURCE_CONFLICT"; then
    echo "  [exists] ${desc} (tolerated)"
  else
    echo "  [FAIL] ${desc}"
    printf '%s\n' "$out" | sed 's/^/    /'
    FAILS=$((FAILS + 1))
  fi
}

# 1) synced tables (Delta -> Postgres, SNAPSHOT, one-way). NO CREATE_CATALOG:
#    the online view lands in the EXISTING owned data catalog under the
#    synced_uc_schema (default `public`), NOT a new Lakebase UC catalog. The UC
#    schema in SYNCED_TABLE_ID maps 1:1 to the Postgres schema, so with
#    synced_uc_schema=public the gold lands in Postgres public.<target> — exactly
#    where the app reads it (app/backend/app.py). Ensuring the UC schema needs
#    CREATE SCHEMA (which the operator has), never CREATE CATALOG.
#      source        = {catalog}.{serving_schema}.{target}  (real gold)
#      SYNCED_TABLE_ID = {data_catalog}.{synced_uc_schema}.{target}
echo "--- 1. synced tables (create-synced-table) ---"
run_tolerant "ensure UC schema ${DATA_CATALOG}.${SYNCED_UC_SCHEMA} (CREATE SCHEMA — not CREATE CATALOG)" \
  databricks schemas create "$SYNCED_UC_SCHEMA" "$DATA_CATALOG" --profile "$PROFILE"
while IFS='|' read -r TARGET PK; do
  [ -z "$TARGET" ] && continue
  SRC="${DATA_CATALOG}.${SERVING_SCHEMA}.${TARGET}"
  SYNCED_ID="${DATA_CATALOG}.${SYNCED_UC_SCHEMA}.${TARGET}"
  SPEC="{\"spec\": {\"source_table_full_name\": \"${SRC}\", \"primary_key_columns\": [\"${PK}\"], \"scheduling_policy\": \"SNAPSHOT\", \"branch\": \"${BRANCH_PATH}\", \"postgres_database\": \"${SERVING_DB}\", \"create_database_objects_if_missing\": true, \"new_pipeline_spec\": {\"storage_catalog\": \"${STORAGE_CATALOG}\", \"storage_schema\": \"default\"}}}"
  run_tolerant "create-synced-table ${SYNCED_ID} <- ${SRC} (pk ${PK})" \
    databricks postgres create-synced-table "$SYNCED_ID" --json "$SPEC" --profile "$PROFILE"
done < <(python3 - "$CFG" <<'PY'
import sys, re
# stdlib-only reader for the lakebase.synced_tables inline-list (no PyYAML).
lines = open(sys.argv[1]).read().splitlines()
def indent(s): return len(s) - len(s.lstrip(chr(32)))
insec = inst = False
for ln in lines:
    if not ln.strip() or ln.lstrip().startswith('#'): continue
    if indent(ln) == 0:
        insec = ln.lstrip().startswith('lakebase:'); inst = False; continue
    if insec and indent(ln) == 2:
        inst = ln.lstrip().startswith('synced_tables:'); continue
    if insec and inst and indent(ln) >= 4 and ln.lstrip().startswith('- '):
        t = re.search(r'target:\s*([A-Za-z0-9_]+)', ln)
        p = re.search(r'pk:\s*([A-Za-z0-9_]+)', ln)
        if t and p: print(t.group(1) + '|' + p.group(1))
PY
)

# psql helper: run one statement string against a database on the primary endpoint.
psql_exec() {
  local dbname="$1" desc="$2" sql="$3"
  local out rc
  out="$(databricks psql --project "$PROJECT" --branch "$BRANCH" --endpoint "$ENDPOINT" \
           --profile "$PROFILE" -- -d "$dbname" -v ON_ERROR_STOP=1 -c "$sql" 2>&1)"; rc=$?
  if [ "$rc" -eq 0 ]; then
    echo "  [ok] ${desc}"
  else
    echo "  [FAIL] ${desc}"
    printf '%s\n' "$out" | sed 's/^/    /'
    FAILS=$((FAILS + 1))
  fi
}

# 2) app-state tables in the app-state DB (native Postgres DDL, idempotent).
echo "--- 2. app-state tables in ${APP_DB} (databricks psql) ---"
APP_STATE_DDL="
CREATE TABLE IF NOT EXISTS public.review_queue (
  queue_id SERIAL PRIMARY KEY, manifest_id VARCHAR(255) UNIQUE NOT NULL,
  study_id VARCHAR(100), submitted_ts TIMESTAMPTZ DEFAULT now(),
  status VARCHAR(20) DEFAULT 'pending', reviewer VARCHAR(120),
  review_notes TEXT, reviewed_ts TIMESTAMPTZ, esignature VARCHAR(255));
CREATE TABLE IF NOT EXISTS public.sessions (
  session_id VARCHAR(255) PRIMARY KEY, user_id VARCHAR(120) NOT NULL,
  started_ts TIMESTAMPTZ DEFAULT now(), last_activity_ts TIMESTAMPTZ DEFAULT now(),
  state JSONB);
CREATE TABLE IF NOT EXISTS public.sign_offs (
  signoff_id SERIAL PRIMARY KEY, manifest_id VARCHAR(255) NOT NULL,
  signer VARCHAR(120) NOT NULL, signed_ts TIMESTAMPTZ DEFAULT now(),
  signature_method VARCHAR(50), signature_hash VARCHAR(512),
  decision VARCHAR(20), reason TEXT, audit_log_id VARCHAR(255));
"
psql_exec "$APP_DB" "app-state DDL (review_queue, sessions, sign_offs)" "$APP_STATE_DDL"

# 3) grant the app service principal live access on both DBs.
echo "--- 3. grant app SP (databricks psql) ---"
SP="$(databricks apps get "$APP_NAME" --profile "$PROFILE" -o json 2>/dev/null \
      | python3 -c "import json,sys
try:
    print(json.load(sys.stdin).get('service_principal_client_id',''))
except Exception:
    print('')" 2>/dev/null)"
if [ -z "$SP" ]; then
  echo "  [skip] app SP not resolved (is '${APP_NAME}' deployed? run 'bundle deploy' first)."
  echo "         Re-run this script after the app exists to apply grants."
else
  echo "  app SP: ${SP}"
  # The SP Postgres role is auto-provisioned when the app's postgres resource
  # binds; create it defensively if absent (control-plane superuser connection).
  ENSURE_ROLE="DO \$do\$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${SP}') THEN EXECUTE format('CREATE ROLE %I WITH LOGIN', '${SP}'); END IF; END \$do\$;"
  psql_exec "$SERVING_DB" "ensure role ${SP} (serving)" "$ENSURE_ROLE"
  psql_exec "$SERVING_DB" "grant serving (SELECT) to ${SP}" "
    GRANT USAGE ON SCHEMA public TO \"${SP}\";
    GRANT SELECT ON ALL TABLES IN SCHEMA public TO \"${SP}\";
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO \"${SP}\";"
  psql_exec "$APP_DB" "ensure role ${SP} (app-state)" "$ENSURE_ROLE"
  psql_exec "$APP_DB" "grant app-state (RW + CREATE) to ${SP}" "
    GRANT USAGE, CREATE ON SCHEMA public TO \"${SP}\";
    GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO \"${SP}\";
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO \"${SP}\";"
fi

if [ "$FAILS" -ne 0 ]; then
  echo "=== setup_synced_tables: ${FAILS} step(s) FAILED — see above ===" >&2
  exit 1
fi
echo "=== setup_synced_tables: done. Next: bundle run wave5_serving_audit -> app ==="
