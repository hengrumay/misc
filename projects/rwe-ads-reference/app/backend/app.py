"""FastAPI backend for RWE ADS Automation app.

Serves the built React frontend from static/ as static files + SPA catch-all.
All routes are catalog/schema-agnostic; names resolve via lib/config.py.

Binds to $DATABRICKS_APP_PORT (injected by Databricks Apps runtime).
WorkspaceClient() auto-authenticates on Apps; local dev falls back to CLI profile.

3-tier data fallback (Lakebase → warehouse → synthetic) ensures reads never crash.
All responses include a 'source' field: "lakebase" | "warehouse" | "synthetic".
"""
import os
import json
import hashlib
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from contextlib import asynccontextmanager

# Resolve lib.config whether running from the repo (lib/ at repo root) or as a
# deployed Databricks App (only app/ ships; build.py vendors lib/ + config into
# the app dir). Insert both candidates; repo root wins locally, app dir wins deployed.
APP_DIR = Path(__file__).resolve().parent.parent          # .../app
REPO_ROOT = APP_DIR.parent                                 # repo root (local only)
for _p in (str(REPO_ROOT), str(APP_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from lib.config import cfg


# ============================================================================
# Lakebase / warehouse read helpers (3-tier fallback: Lakebase -> warehouse -> synthetic)
# ============================================================================
def _jsonable(v):
    """Coerce non-JSON scalar types (dates, decimals) to str for FastAPI."""
    return v if isinstance(v, (str, int, float, bool)) or v is None else str(v)


def _pg_connect_with_retry(kwargs: dict, *, attempts: int = 3, backoff: float = 1.0):
    """psycopg.connect with a BOUNDED retry. A scale-to-zero Lakebase endpoint wakes
    in ~100ms but the first connect can be refused; connect_timeout bounds each attempt,
    so the total time is bounded. Transient OperationalErrors are retried; exhausting
    them raises so the caller falls back to the warehouse instead of hanging."""
    import time
    import psycopg
    last = None
    for i in range(1, attempts + 1):
        try:
            return psycopg.connect(**kwargs)
        except psycopg.OperationalError as e:  # refused / starting up / timed out
            last = e
            print(f"[lakebase] connect attempt {i}/{attempts} failed ({repr(e)[:120]}); "
                  f"retrying in {backoff}s")
            time.sleep(backoff)
    raise RuntimeError(f"Lakebase connect failed after {attempts} attempts: {last!r}")


def _lakebase_query(dbname: str, sql: str):
    """Query a Lakebase Postgres DB as the app service principal. Returns list[dict] or None.

    Lakebase is Autoscaling: the `postgres` app resource auto-injects PGHOST, PGUSER
    (SP client id) and LAKEBASE_ENDPOINT (primary endpoint resource path). We mint a
    fresh OAuth credential from that endpoint and dbname-OVERRIDE to reach the real
    serving/app DBs (ads_serving_pg / ads_app) on the SAME endpoint — the resource
    binds the auto-created databricks_postgres DB. Local dev resolves the endpoint
    path from demo.config.yaml. (Replaces the retired Provisioned serving read.)
    """
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        endpoint = os.environ.get("LAKEBASE_ENDPOINT") or cfg().lakebase_endpoint_path
        host = os.environ.get("PGHOST") or w.postgres.get_endpoint(name=endpoint).status.hosts.host
        tok = w.postgres.generate_database_credential(endpoint=endpoint).token
        # On Databricks Apps the SP's Postgres role name is its client id.
        user = os.environ.get("PGUSER") or os.environ.get("DATABRICKS_CLIENT_ID") or w.current_user.me().user_name
        # Bounded connect_timeout + retry so a scale-to-zero wake never stalls the
        # request; on failure the outer except drops to the warehouse fallback.
        with _pg_connect_with_retry(dict(
                host=host, port=int(os.environ.get("PGPORT", "5432")), dbname=dbname,
                user=user, password=tok, sslmode="require", connect_timeout=10)) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                cols = [d[0] for d in cur.description]
                return [{c: _jsonable(v) for c, v in zip(cols, row)} for row in cur.fetchall()]
    except Exception as e:  # noqa: BLE001
        print(f"[lakebase] read failed ({dbname}): {repr(e)[:200]}")
        return None


def _warehouse_query(sql: str):
    """Fallback: query the Delta table via the serverless SQL warehouse. Returns list[dict] or None."""
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        r = w.statement_execution.execute_statement(warehouse_id=cfg().warehouse_id,
                                                    statement=sql, wait_timeout="30s")
        cols = [c.name for c in r.manifest.schema.columns]
        return [{c: _jsonable(v) for c, v in zip(cols, row)} for row in (r.result.data_array or [])]
    except Exception as e:  # noqa: BLE001
        print(f"[warehouse] read failed: {repr(e)[:200]}")
        return None


def _warehouse_exec(sql: str) -> bool:
    """Execute a write (UPDATE/INSERT) on the serverless warehouse. Returns success."""
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        w.statement_execution.execute_statement(warehouse_id=cfg().warehouse_id,
                                                statement=sql, wait_timeout="50s")
        return True
    except Exception as e:  # noqa: BLE001
        print(f"[warehouse] exec failed: {repr(e)[:200]}")
        return False


def _sq(v: Any) -> str:
    """Single-quote-escape a value for safe inline SQL (\' — never doubled '')."""
    if v is None:
        return "NULL"
    return "'" + str(v).replace("\\", "\\\\").replace("'", "\\'") + "'"


def _append_gxp_event(event_type: str, actor: str, subject_id: str, details: dict) -> str | None:
    """Append a hash-chained event to ads_audit.gxp_audit via the warehouse.

    Chain basis matches waves/wave5_serving_audit_app/setup_audit.compute_chain:
    sorted keys of {event_id,event_type,actor,subject_id}. Written with map() so
    the details JSON cannot corrupt the row.
    """
    tbl = cfg().table("audit", "gxp_audit")
    event_id = f"{event_type}_{subject_id}_{int(datetime.now(timezone.utc).timestamp() * 1000)}"
    prev = _warehouse_query(f"SELECT row_hash FROM {tbl} ORDER BY ts DESC LIMIT 1")
    prev_hash = (prev[0]["row_hash"] if prev else "") or ""
    basis = {"event_id": event_id, "event_type": event_type, "actor": actor, "subject_id": subject_id}
    row_str = "".join(f"{k}={v}" for k, v in sorted(basis.items()))
    row_hash = hashlib.sha256((prev_hash + row_str).encode()).hexdigest()
    dmap = ", ".join(f"{_sq(k)}, {_sq(v)}" for k, v in (details or {}).items()) or "'_', '_'"
    ok = _warehouse_exec(
        f"INSERT INTO {tbl} (event_id, event_type, actor, subject_id, details, ts, prev_hash, row_hash) "
        f"SELECT {_sq(event_id)}, {_sq(event_type)}, {_sq(actor)}, {_sq(subject_id)}, "
        f"map({dmap}), current_timestamp(), {_sq(prev_hash) if prev_hash else 'NULL'}, {_sq(row_hash)}")
    return event_id if ok else None


def _run_job(env_var: str, name_contains: str, params: dict | None = None) -> dict:
    """Trigger a bundle job via run_now, resolving the job id ROBUSTLY.

    Resolution order:
      1. ``env_var`` injected by the bound ``job`` app resource (resources/app.yml).
         The platform substitutes the RESOLVED job id, so it survives dev-mode name
         prefixing (``[dev <user>] [RWE-ADS] App — ...``). This is the path on Apps.
      2. Local-dev fallback: match by a STABLE display-name SUBSTRING. The dev-mode
         ``[dev <user>]`` prefix is prepended, so ``name_contains`` still matches;
         an exact-name lookup (the old behavior) would not.

    ``job_parameters`` is a plain ``{key: value}`` map — the shape run_now expects
    (databricks-sdk ``run_now(job_id, ..., job_parameters: Dict[str, str])``); a list
    of ``JobParameter(name=, value=)`` triggers ``BadRequest: Expected both 'key' and 'value'``.
    """
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    raw_id = os.environ.get(env_var)
    if raw_id:
        job_id = int(raw_id)
    else:
        match = [j for j in w.jobs.list()
                 if j.settings and j.settings.name and name_contains in j.settings.name]
        if not match:
            raise HTTPException(status_code=404,
                                detail=f"no job matching '{name_contains}' "
                                       f"(bind the job app resource or set {env_var})")
        job_id = match[0].job_id
    job_parameters = {k: str(v) for k, v in (params or {}).items()} or None
    run = w.jobs.run_now(job_id=job_id, job_parameters=job_parameters)
    host = cfg().host.rstrip("/")
    return {"run_id": run.run_id, "run_url": f"https://{host}/jobs/{job_id}/runs/{run.run_id}"}


def _upload_to_volume(filename: str, content: bytes) -> str:
    """Upload a protocol file into the ads_raw.protocols volume via the Files API."""
    from databricks.sdk import WorkspaceClient
    import io
    w = WorkspaceClient()
    safe = os.path.basename(filename).replace(" ", "_")
    dst = f"{cfg().protocols_volume_path}/{safe}"
    w.files.upload(dst, io.BytesIO(content), overwrite=True)
    return dst


# ============================================================================
# Startup / Shutdown
# ============================================================================

async def lifespan(app: FastAPI):
    """Startup: initialize connections; shutdown: clean up."""
    # On startup, ensure Lakebase connection is ready (optional early test)
    print(f"[APP] Initializing RWE ADS Automation (catalog={cfg().catalog})")
    print(f"[APP] Lakebase: {cfg().lakebase_project}/{cfg().lakebase_app_db} "
          f"(endpoint {cfg().lakebase_endpoint_path})")
    print(f"[APP] Synced tables: {[st['source'] for st in cfg().synced_tables]}")
    yield
    # On shutdown, close any lingering pools
    print("[APP] Shutting down")


app = FastAPI(
    title="RWE ADS Automation",
    version="0.1.0",
    lifespan=lifespan
)

# ============================================================================
# Routes
# ============================================================================

@app.get("/api/health")
async def health():
    """Health check. Returns source=ready if we can reach config."""
    return {
        "status": "ready",
        "source": "health",
        "catalog": cfg().catalog,
        "lakebase_project": cfg().lakebase_project,
    }


@app.get("/api/config/summary")
async def config_summary():
    """Return non-secret config summary for frontend (branding, poc_studies, etc.)."""
    try:
        return {
            "source": "config",
            "initiative": cfg().initiative,
            "catalog": cfg().catalog,
            "schemas": {k: cfg().schema(k) for k in cfg().all_schema_keys()},
            "lakebase": {
                "project": cfg().lakebase_project,
                "branch": cfg().lakebase_branch,
                "serving_db": cfg().lakebase_serving_db,
                "app_db": cfg().lakebase_app_db,
            },
            "branding": cfg().branding,
            "poc_studies": cfg().poc_studies,
            "synced_tables": [
                {"source": st["source"], "target": st["target"]}
                for st in cfg().synced_tables
            ],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Config error: {str(e)}")


@app.get("/api/protocols/list")
async def list_protocols():
    """List protocols from ads_raw.protocol_spec (warehouse). Never 500s.

    Enriched with the extraction-eval signal (eval_ok / review_priority / n_flags)
    via a LEFT JOIN on raw.protocol_eval so the reviewer sees quality at a glance.
    If protocol_eval does not exist yet (eval never ran), falls back to the plain
    spec query so the list still renders.
    """
    c = cfg()
    spec = c.table("raw", "protocol_spec")
    ev = c.table("raw", "protocol_eval")
    rows = _warehouse_query(
        f"SELECT s.study_id, s.title, s.complexity, s.review_status, s.source_protocol, "
        f"s.followup_days, e.eval_ok, e.review_priority, e.n_flags, e.n_hard_fails, "
        f"e.min_confidence "
        f"FROM {spec} s LEFT JOIN {ev} e ON s.study_id = e.study_id ORDER BY CASE lower(s.complexity) WHEN 'low' THEN 0 WHEN 'medium' THEN 1 WHEN 'high' THEN 2 ELSE 3 END, s.study_id")
    if rows is None:  # protocol_eval absent (or warehouse down) -> plain spec query
        rows = _warehouse_query(
            f"SELECT study_id, title, complexity, review_status, source_protocol, followup_days "
            f"FROM {spec} ORDER BY CASE lower(complexity) WHEN 'low' THEN 0 WHEN 'medium' THEN 1 WHEN 'high' THEN 2 ELSE 3 END, study_id")
    if rows is None:
        return {"source": "synthetic", "protocols": [
            {"study_id": "poc_low", "title": "Simple Prevalence Cohort", "complexity": "low",
             "review_status": "extracted", "source_protocol": None}]}
    return {"source": "warehouse", "protocols": rows}


@app.get("/api/protocols/review_queue")
async def protocol_review_queue():
    """Extracted protocol specs sorted worst-first for analyst review.

    Reads the raw.protocol_review_queue view (built by run_protocol_eval.py):
    highest review_priority / lowest confidence first, each row carrying its
    Stage-1 gate result + flags so the reviewer triages the riskiest specs first.
    Never 500s: returns an empty queue if the view is absent (eval not yet run).
    """
    view = cfg().table("raw", "protocol_review_queue")
    rows = _warehouse_query(
        f"SELECT study_id, title, complexity, review_status, source_protocol, eval_ok, "
        f"n_hard_fails, hard_fail_reasons, review_priority, n_flags, min_confidence, "
        f"completeness_ok, CAST(eval_ts AS STRING) eval_ts FROM {view} "
        f"ORDER BY review_priority DESC NULLS LAST, min_confidence ASC NULLS FIRST")
    if rows is None:
        return {"source": "synthetic", "queue": []}
    return {"source": "warehouse", "queue": rows}


@app.get("/api/protocols/spec")
async def get_protocol_spec(study_id: str):
    """Return the full extracted + standardized coded spec for one study."""
    tbl = cfg().table("raw", "protocol_spec")
    rows = _warehouse_query(f"SELECT * FROM {tbl} WHERE study_id = {_sq(study_id)} LIMIT 1")
    if not rows:
        raise HTTPException(status_code=404, detail=f"protocol '{study_id}' not found")
    return {"source": "warehouse", "study_id": study_id, "spec": rows[0]}


@app.post("/api/protocols/upload")
async def upload_protocol(file: UploadFile = File(...)):
    """Upload a PDF/DOCX protocol into the ads_raw.protocols volume."""
    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="empty file")
    try:
        dst = _upload_to_volume(file.filename, content)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"upload failed: {repr(e)[:200]}")
    return {"source": "volume", "path": dst, "bytes": len(content),
            "message": "uploaded; run extraction to parse it into a protocol_spec"}


@app.post("/api/protocols/extract")
async def trigger_extract():
    """Trigger the extraction job (ai_parse_document + ai_extract -> protocol_spec)."""
    return _run_job("DATABRICKS_JOB_PROTOCOL_EXTRACT", "App — Protocol extraction")


@app.post("/api/protocols/approve")
async def approve_protocol(body: dict):
    """Analyst e-sign of a protocol spec: set review_status + hash-chained audit event.

    body: {study_id, reviewer_name, reviewer_email, signature, decision?, comments?,
           corrected_fields?}
    """
    study_id = body.get("study_id")
    signature = (body.get("signature") or "").strip()
    reviewer_email = body.get("reviewer_email") or body.get("reviewer_name") or "analyst"
    decision = body.get("decision", "approve")
    if not study_id:
        raise HTTPException(status_code=400, detail="study_id required")
    if len(signature) < 2:
        raise HTTPException(status_code=400, detail="signature required")

    tbl = cfg().table("raw", "protocol_spec")
    status = {"approve": "approved", "reject": "rejected"}.get(decision, "extracted")
    sets = [f"review_status = {_sq(status)}", f"reviewed_by = {_sq(reviewer_email)}",
            "reviewed_ts = current_timestamp()", f"esignature = {_sq(signature)}"]
    # apply simple scalar corrections (arrays/maps are corrected via the extraction path)
    for k, v in (body.get("corrected_fields") or {}).items():
        if isinstance(v, (str, int, float)):
            sets.append(f"{k} = {_sq(v) if isinstance(v, str) else v}")
    ok = _warehouse_exec(f"UPDATE {tbl} SET {', '.join(sets)} WHERE study_id = {_sq(study_id)}")
    if not ok:
        raise HTTPException(status_code=502, detail="protocol_spec update failed")

    event_id = _append_gxp_event("protocol_approval", reviewer_email, study_id,
                                 {"decision": decision, "reviewer": body.get("reviewer_name", ""),
                                  "comments": (body.get("comments") or "")[:200]})
    return {"source": "warehouse", "study_id": study_id, "review_status": status,
            "event_id": event_id}


@app.post("/api/ads/build")
async def trigger_ads_build(study_id: str):
    """Trigger the parameterized ADS build job for an APPROVED protocol."""
    tbl = cfg().table("raw", "protocol_spec")
    chk = _warehouse_query(
        f"SELECT review_status FROM {tbl} WHERE study_id = {_sq(study_id)} LIMIT 1")
    if not chk:
        raise HTTPException(status_code=404, detail=f"protocol '{study_id}' not found")
    if chk[0]["review_status"] != "approved":
        raise HTTPException(status_code=409,
                            detail=f"protocol '{study_id}' is not approved (review + e-sign first)")
    return _run_job("DATABRICKS_JOB_ADS_BUILD", "App — ADS build", {"poc": study_id})


@app.get("/api/review/queue")
async def review_queue():
    """List ADS builds pending analyst review (serving.review_queue)."""
    tbl = cfg().table("serving", "review_queue")
    rows = _warehouse_query(
        f"SELECT review_id, ads_id, study_id, complexity, n_patients, kb_snippets_hash, status, "
        f"CAST(created_ts AS STRING) created_ts FROM {tbl} WHERE status='PENDING' "
        f"ORDER BY created_ts DESC")
    if rows is None:
        return {"source": "synthetic", "queue": []}
    return {"source": "warehouse", "queue": rows}


@app.get("/api/review/details")
async def review_details(review_id: str):
    """Fetch a review row + the generated SQL from its reproducibility manifest."""
    q = cfg().table("serving", "review_queue")
    rows = _warehouse_query(f"SELECT * FROM {q} WHERE review_id = {_sq(review_id)} LIMIT 1")
    if not rows:
        raise HTTPException(status_code=404, detail="review not found")
    rec = rows[0]
    man = _warehouse_query(
        f"SELECT generated_sql FROM {cfg().table('audit','repro_manifest')} "
        f"WHERE ads_id = {_sq(rec.get('ads_id'))} ORDER BY created_ts DESC LIMIT 1")
    if man:
        rec["generated_sql"] = man[0].get("generated_sql")
    return {"source": "warehouse", "review": rec}


@app.post("/api/review/approve")
async def approve_review(body: dict):
    """Analyst e-sign of a built ADS: update review_queue + hash-chained ads_approval."""
    review_id = body.get("review_id")
    ads_id = body.get("ads_id")
    signature = (body.get("signature") or "").strip()
    reviewer_email = body.get("reviewer_email") or body.get("reviewer_name") or "analyst"
    decision = body.get("decision", "approve")
    if not review_id or not ads_id:
        raise HTTPException(status_code=400, detail="review_id and ads_id required")
    if len(signature) < 2:
        raise HTTPException(status_code=400, detail="signature required")

    q = cfg().table("serving", "review_queue")
    status = {"approve": "APPROVED", "reject": "REJECTED", "request_revision": "REVISION"}.get(decision, "REVISION")
    _warehouse_exec(f"UPDATE {q} SET status = {_sq(status)} WHERE review_id = {_sq(review_id)}")
    ev_type = {"approve": "ads_approval", "reject": "ads_rejected"}.get(decision, "review_gate_fail")
    event_id = _append_gxp_event(ev_type, reviewer_email, ads_id,
                                 {"decision": decision, "reviewer": body.get("reviewer_name", ""),
                                  "study_id": body.get("study_id", "")})
    return {"source": "warehouse", "review_id": review_id, "ads_id": ads_id,
            "status": status, "event_id": event_id}


@app.get("/api/served/ads_output")
async def get_ads_output(limit: int = 100, study_id: str | None = None):
    """Read ads_output: Lakebase serving -> warehouse -> synthetic (never 500s)."""
    limit = max(1, min(int(limit), 1000))
    where = f" WHERE study_id = {_sq(study_id)}" if study_id else ""
    cols = "study_id, patient_id, index_date, outcome_flag, time_to_event, outcome_date, covariates"
    rows = _lakebase_query(cfg().lakebase_serving_db,
        f"SELECT {cols} FROM public.ads_output{where} LIMIT {limit}")
    source = "lakebase"
    if rows is None:
        rows = _warehouse_query(
            f"SELECT {cols} FROM {cfg().table('serving','ads_output')}{where} LIMIT {limit}")
        source = "warehouse"
    if rows is None:
        rows = [{"study_id": "poc_low", "patient_id": "synthetic-0001", "index_date": "2021-01-01",
                 "outcome_flag": 0, "time_to_event": 365, "outcome_date": None, "covariates": "{}"}]
        source = "synthetic"
    return {"source": source, "table": cfg().table("serving", "ads_output"),
            "rows": rows, "count": len(rows), "limit": limit}


@app.get("/api/served/cohort_summary")
async def get_cohort_summary():
    """Read cohort_summary: Lakebase serving -> warehouse -> synthetic (never 500s)."""
    cols = "study_id, ads_id, n_patients, n_outcomes, outcome_rate, avg_time_to_event_days, covariate_rates"
    rows = _lakebase_query(cfg().lakebase_serving_db, f"SELECT {cols} FROM public.cohort_summary ORDER BY CASE WHEN study_id LIKE '%low' THEN 0 WHEN study_id LIKE '%med%' THEN 1 WHEN study_id LIKE '%high' THEN 2 ELSE 3 END, study_id")
    source = "lakebase"
    if rows is None:
        rows = _warehouse_query(f"SELECT {cols} FROM {cfg().table('serving','cohort_summary')} ORDER BY CASE WHEN study_id LIKE '%low' THEN 0 WHEN study_id LIKE '%med%' THEN 1 WHEN study_id LIKE '%high' THEN 2 ELSE 3 END, study_id")
        source = "warehouse"
    if rows is None:
        rows = [{"study_id": "poc_low", "n_patients": 312, "n_outcomes": 12,
                 "outcome_rate": 0.0385, "avg_time_to_event_days": 358.7, "covariate_rates": "{}"}]
        source = "synthetic"
    return {"source": source, "table": cfg().table("serving", "cohort_summary"), "rows": rows}


@app.get("/api/audit/reproducibility")
async def get_reproducibility_manifests(limit: int = 20):
    """List reproducibility manifests (warehouse)."""
    tbl = cfg().table("audit", "repro_manifest")
    rows = _warehouse_query(
        f"SELECT manifest_id, ads_id, study_id, protocol_version, model, agent_version, "
        f"reviewer, decision, CAST(created_ts AS STRING) created_ts, "
        f"CAST(source_table_versions AS STRING) source_table_versions "
        f"FROM {tbl} ORDER BY created_ts DESC LIMIT {int(limit)}")
    if rows is None:
        return {"source": "synthetic", "manifests": []}
    return {"source": "warehouse", "manifests": rows}


@app.get("/api/audit/gxp")
async def get_gxp_audit(limit: int = 50):
    """Return the hash-chained GxP event log (warehouse)."""
    tbl = cfg().table("audit", "gxp_audit")
    rows = _warehouse_query(
        f"SELECT event_id, event_type, actor, subject_id, CAST(ts AS STRING) ts, "
        f"substr(row_hash,1,12) row_hash FROM {tbl} ORDER BY ts DESC LIMIT {int(limit)}")
    if rows is None:
        return {"source": "synthetic", "events": []}
    return {"source": "warehouse", "events": rows}


# ============================================================================
# Static SPA serving
# ============================================================================

STATIC_DIR = Path(__file__).resolve().parent.parent / "static"
if STATIC_DIR.exists():
    app.mount("/assets", StaticFiles(directory=STATIC_DIR / "assets"), name="assets")


@app.get("/{full_path:path}")
def serve_spa(full_path: str):
    """Serve SPA: real static files, then index.html fallback for client routing."""
    if full_path:
        candidate = STATIC_DIR / full_path
        if candidate.exists() and candidate.is_file():
            return FileResponse(candidate)
    # Fallback to index.html for client-side routing
    return FileResponse(STATIC_DIR / "index.html")


# ============================================================================
# Main
# ============================================================================

if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("DATABRICKS_APP_PORT", 8080))
    print(f"[APP] Listening on port {port}")
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
        log_level="info",
    )
