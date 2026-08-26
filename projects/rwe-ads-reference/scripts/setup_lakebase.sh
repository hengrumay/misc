#!/usr/bin/env bash
# =============================================================================
# setup_lakebase.sh — CONTROL-PLANE Lakebase (Autoscaling) provisioning.
# =============================================================================
# WHY control-plane: any `w.postgres` SDK call executed INSIDE a serverless
# job-task kernel hard-crashes the kernel ("Python process exited unexpectedly",
# native, before psycopg). The SAME operations succeed from the control plane via
# the `databricks postgres` CLI. So all Lakebase INFRASTRUCTURE is provisioned
# here (operator's machine), NOT from a serverless wave. The serverless waves
# make ZERO `w.postgres` calls.
#
# This script is idempotent: re-running it tolerates "already exists" and uses
# --replace-existing for databases.
#
# It provisions (names read from demo.config.yaml — the single source of truth):
#   1. the Autoscaling PROJECT (auto-creates production branch + primary
#      read-write endpoint + the databricks_postgres bound DB),
#   2. the serving Postgres DB (lakebase.database, e.g. ads_serving_pg) and the
#      app-state Postgres DB (lakebase.app_state_db, e.g. ads_app) on the
#      production branch.
#
# It does NOT register a UC catalog in the default path. The app reads Lakebase
# Postgres DIRECTLY (psycopg via endpoint host + generate-database-credential),
# so serving needs NO UC catalog and NO CREATE_CATALOG. An OPTIONAL step (opt-in
# via LAKEBASE_REGISTER_UC_CATALOG=1) registers lakebase.catalog over the serving
# DB for UC-governed (Lakehouse-federated) access; that step alone requires
# CREATE_CATALOG on the metastore.
#
# App-state TABLE DDL and the app-SP GRANT are NOT here — they need the app's
# service principal (created by `bundle deploy`) and the gold ADS, so they live
# in scripts/setup_synced_tables.sh (run after deploy + waves produce gold).
#
# Usage:
#   scripts/setup_lakebase.sh <DATABRICKS_PROFILE>
# Example:
#   scripts/setup_lakebase.sh <your-profile>
#
# Hands-off run order (see README "Deploy"):
#   setup_lakebase.sh  ->  bundle deploy  ->  bundle run wave0..wave4
#     ->  setup_synced_tables.sh  ->  bundle run wave5_serving_audit  ->  app
# =============================================================================
set -uo pipefail

PROFILE="${1:-${DATABRICKS_CONFIG_PROFILE:-}}"
if [ -z "$PROFILE" ]; then
  echo "usage: scripts/setup_lakebase.sh <DATABRICKS_PROFILE>" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CFG="$REPO_ROOT/demo.config.yaml"
if [ ! -f "$CFG" ]; then
  echo "demo.config.yaml not found at $CFG" >&2
  exit 2
fi

# --- read names from the single source of truth (demo.config.yaml) -----------
# No PyYAML dependency: a stdlib-only, indentation-aware reader parses the small
# fixed set of scalars we need (works on machines without `pip install pyyaml`).
# One newline-separated emit; read portably (macOS bash 3.2 has no mapfile).
if ! {
  read -r PROJECT
  read -r BRANCH
  read -r SERVING_DB
  read -r APP_DB
  read -r LB_CATALOG
  read -r INITIATIVE
} < <(python3 - "$CFG" <<'PY'
import sys, re
# NOTE: quote characters are built via chr() on purpose. This heredoc runs inside
# a <(...) process substitution, and bash 3.2 (macOS default) naively scans the
# heredoc body for matching quotes; literal ' or " here would unbalance that scan
# and break the script with "unexpected EOF". chr(34)=" , chr(39)=' .
DQ, SQ = chr(34), chr(39)
lines = open(sys.argv[1]).read().splitlines()
def indent(s): return len(s) - len(s.lstrip(chr(32)))
def clean(v):
    v = v.strip()
    if v[:1] not in (DQ, SQ):                  # strip trailing "  # comment"
        m = re.search(r"\s#", v)
        if m: v = v[:m.start()]
    v = v.strip()
    if len(v) >= 2 and v[0] in (DQ, SQ) and v[-1] == v[0]:
        v = v[1:-1]
    return v
def scalar(spec, default=''):
    # spec is "key" (top-level) or "section.key" (one level of nesting).
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
print(scalar('lakebase.database'))          # serving Postgres DB (pg identifier, underscores ok)
print(scalar('lakebase.app_state_db'))      # app-state Postgres DB
print(scalar('lakebase.catalog'))           # UC catalog registered over the serving DB
print(scalar('initiative', 'rwe-ads-automation'))
PY
); then
  echo "failed to read demo.config.yaml (stdlib parser error)" >&2
  exit 2
fi

BRANCH_PATH="projects/${PROJECT}/branches/${BRANCH}"
# Resource IDs must be DNS-valid (RFC 1123: lowercase, digits, hyphens — no
# underscores); the Postgres database NAME (spec.postgres_database) keeps its
# underscores. Derive the id from the pg name by swapping _ -> -.
SERVING_ID="$(printf '%s' "$SERVING_DB" | tr '_' '-')"
APP_ID="$(printf '%s' "$APP_DB" | tr '_' '-')"

FAILS=0

# run a CLI command; treat "already exists" as success (idempotent), real errors fail.
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

echo "=== setup_lakebase (control-plane) — project ${PROJECT} on profile ${PROFILE} ==="
echo "  serving DB: ${SERVING_DB} (id ${SERVING_ID}) | app DB: ${APP_DB} (id ${APP_ID})"
echo "  UC catalog: ${LB_CATALOG} -> ${BRANCH_PATH}/${SERVING_DB}"

# 1) Autoscaling project (auto-provisions production branch + primary endpoint +
#    databricks_postgres DB). MUST run BEFORE role derivation: on a FRESH project
#    the databricks_postgres DB — and its default role — do not exist yet, so
#    deriving the role first would abort the bootstrap. Idempotent via tolerate-exists.
run_tolerant "create-project ${PROJECT}" \
  databricks postgres create-project "$PROJECT" \
    --json "{\"spec\": {\"display_name\": \"${INITIATIVE}\"}}" \
    --profile "$PROFILE"

# create-database requires spec.role = the FULL role resource path. Derive it from
# the databricks_postgres DB (status.role) created by create-project above, rather
# than hardcoding, so this stays correct across operators/workspaces.
ROLE="$(databricks postgres get-database "${BRANCH_PATH}/databases/databricks-postgres" \
          --profile "$PROFILE" -o json 2>/dev/null \
        | python3 -c "import json,sys
try:
    print(json.load(sys.stdin).get('status',{}).get('role',''))
except Exception:
    print('')" 2>/dev/null)"
if [ -z "$ROLE" ]; then
  echo "could not derive role from ${BRANCH_PATH}/databases/databricks-postgres (create-project ran above; the project's production branch / default role may still be provisioning — re-run in a moment)" >&2
  exit 2
fi
echo "  role:       ${ROLE}"

# 2) serving + app-state Postgres databases on the production branch.
#    Body: {"spec":{"postgres_database":"<name>","role":"<full-role-path>"}}.
#    spec.postgres_database keeps underscores; --database-id is the hyphenated
#    resource id; spec.role MUST be the full role resource path (derived above).
#    --replace-existing makes re-runs idempotent.
run_tolerant "create-database ${SERVING_DB}" \
  databricks postgres create-database "$BRANCH_PATH" \
    --database-id "$SERVING_ID" --replace-existing \
    --json "{\"spec\": {\"postgres_database\": \"${SERVING_DB}\", \"role\": \"${ROLE}\"}}" \
    --profile "$PROFILE"

run_tolerant "create-database ${APP_DB}" \
  databricks postgres create-database "$BRANCH_PATH" \
    --database-id "$APP_ID" --replace-existing \
    --json "{\"spec\": {\"postgres_database\": \"${APP_DB}\", \"role\": \"${ROLE}\"}}" \
    --profile "$PROFILE"

# 3) OPTIONAL: register the serving DB as a UC catalog. NOT in the default path.
#    The app reads Lakebase Postgres DIRECTLY (psycopg via endpoint host +
#    generate-database-credential), and gold is loaded into Postgres by
#    setup_synced_tables.sh under an existing owned catalog — so serving needs NO
#    UC catalog. This step is the ONLY thing here that requires CREATE_CATALOG on
#    the metastore; it exists solely for optional UC-governed (Lakehouse-
#    federated) access to the Lakebase DB. Enable with LAKEBASE_REGISTER_UC_CATALOG=1.
if [ "${LAKEBASE_REGISTER_UC_CATALOG:-0}" = "1" ]; then
  echo "--- OPTIONAL: register UC catalog ${LB_CATALOG} (LAKEBASE_REGISTER_UC_CATALOG=1; requires CREATE_CATALOG) ---"
  run_tolerant "create-catalog ${LB_CATALOG}" \
    databricks postgres create-catalog "$LB_CATALOG" \
      --json "{\"spec\": {\"postgres_database\": \"${SERVING_DB}\", \"branch\": \"${BRANCH_PATH}\"}}" \
      --profile "$PROFILE"
else
  echo "  [skip] create-catalog ${LB_CATALOG} — optional UC-catalog registration OFF (default)."
  echo "         App reads Postgres directly; no CREATE_CATALOG needed. Set LAKEBASE_REGISTER_UC_CATALOG=1 to enable."
fi

if [ "$FAILS" -ne 0 ]; then
  echo "=== setup_lakebase: ${FAILS} step(s) FAILED — see above ===" >&2
  exit 1
fi
echo "=== setup_lakebase: done. Next: bundle deploy -> run wave0..wave4 -> setup_synced_tables.sh ==="
