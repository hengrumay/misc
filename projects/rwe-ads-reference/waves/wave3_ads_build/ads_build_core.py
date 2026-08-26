"""Deterministic, KB-grounded ADS composition core (validate-don't-execute).

This is the safety-critical path. It composes an analysis-ready dataset from
ONLY approved KB snippets, substitutes the reviewed protocol spec, VALIDATES
each composed statement with EXPLAIN against synthetic gold *before* executing,
then materializes ads_output + cohort_summary in the serving schema.

The reviewed protocol spec is read from cfg().raw.protocol_spec (review_status=
'approved') — it is no longer hard-coded. POC_SPECS remains only as an offline
fallback for --code-only runs and unit tests.

Composition is driven by the study's `complexity` (low|medium|high), which
selects a recipe of approved snippets:
  low    prevalence dx cohort  -> age -> enrollment            -> covariates -> outcome
  medium new-user rx cohort    -> age -> enrollment -> exclude -> covariates -> outcome
  high   new-user rx cohort    -> age -> enrollment -> exclude -> exposure-era + covariates -> outcome

poc_high lab-value (LOINC) covariates are intentionally out of scope: no
approved KB snippet derives lab covariates yet (adding one must go through the
KB e-sign path). poc_high is built from the approved comorbidity/exposure/
outcome snippets — the ADS, not the downstream Cox/Fine-Gray model.

All names resolve via lib/config.py. Runs inside a Spark/Databricks context.
"""
from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field

from lib.config import cfg
from lib.pipeline.token_subst import substitute_tokens


# ---- Offline fallback specs (real path reads cfg().raw.protocol_spec) --------
# Coded form mirrors what protocol_standardize.py produces from the PDFs.
# Codes match the synthetic RWD code universe so offline builds are non-empty.
POC_SPECS = {
    "poc_low": {
        "study_id": "poc_low", "version": "1.0", "complexity": "low",
        "title": "Simple prevalence cohort — Type 2 Diabetes",
        "dx_codes": ["E11.9"], "ndc_codes": [], "exclude_dx": ["E03.9"],
        "outcome_codes": ["I50.9"],
        "study_start": "2018-01-01", "study_end": "2024-12-31",
        "min_age": 18, "max_age": 85, "pre_days": 90, "post_days": 90,
        "baseline_days": 365, "followup_days": 365, "washout_days": 365, "grace_days": 30,
        "covariates_coded": [{"name": "cov_i10", "codes": ["I10"]}],
    },
    "poc_med": {
        "study_id": "poc_med", "version": "1.0", "complexity": "medium",
        "title": "Drug-exposure new-user cohort — ACE inhibitors",
        "dx_codes": [], "ndc_codes": ["00093-5117-16"], "exclude_dx": ["N18.3"],
        "outcome_codes": ["I50.9"],
        "study_start": "2018-01-01", "study_end": "2024-12-31",
        "min_age": 18, "max_age": 75, "pre_days": 90, "post_days": 90,
        "baseline_days": 365, "followup_days": 365, "washout_days": 365, "grace_days": 30,
        "covariates_coded": [{"name": "cov_e11_9", "codes": ["E11.9"]},
                             {"name": "cov_i50_9", "codes": ["I50.9"]}],
    },
    "poc_high": {
        "study_id": "poc_high", "version": "1.0", "complexity": "high",
        "title": "Comparative outcomes w/ time-varying exposure — statins",
        "dx_codes": [], "ndc_codes": ["00054-0165-24"], "exclude_dx": ["N18.3", "K21.9"],
        "outcome_codes": ["I50.9"],
        "study_start": "2018-01-01", "study_end": "2024-12-31",
        "min_age": 40, "max_age": 75, "pre_days": 90, "post_days": 90,
        "baseline_days": 365, "followup_days": 730, "washout_days": 365, "grace_days": 30,
        "covariates_coded": [{"name": "cov_e11_9", "codes": ["E11.9"]},
                             {"name": "cov_i10", "codes": ["I10"]},
                             {"name": "cov_i50_9", "codes": ["I50.9"]}],
    },
}

# complexity -> composition recipe. Each narrowing step: (step_name, snippet_id).
# The cohort feeds step 1; each subsequent step narrows the previous survivor.
RECIPES = {
    "low":    {"cohort": "coh_base_prevalence",
               "narrow": [("inclusion_age", "inc_age_range"),
                          ("inclusion_enrollment", "inc_continuous_enrollment")],
               "exposure_era": False},
    "medium": {"cohort": "coh_new_user",
               "narrow": [("inclusion_age", "inc_age_range"),
                          ("inclusion_enrollment", "inc_continuous_enrollment"),
                          ("exclusion_prior", "exc_prior_condition")],
               "exposure_era": False},
    "high":   {"cohort": "coh_new_user",
               "narrow": [("inclusion_age", "inc_age_range"),
                          ("inclusion_enrollment", "inc_continuous_enrollment"),
                          ("exclusion_prior", "exc_prior_condition")],
               "exposure_era": True},
}

IDENTIFIER_KEYS = {"cov_name", "study_start", "study_end"}


@dataclass
class StepResult:
    step: str
    snippet_id: str
    snippet_hash: str
    sql: str
    validated: bool = False
    explain_ok: bool = False
    error: str = ""


@dataclass
class BuildManifest:
    ads_id: str
    study_id: str
    protocol_version: str
    model: str
    complexity: str = ""
    steps: list = field(default_factory=list)
    kb_snippets_hash: str = ""
    ads_row_count: int = 0
    duration_sec: float = 0.0
    executed: bool = False
    error: str = ""


def _fetch_approved_snippets(spark, kb_table: str) -> dict:
    """Retrieve ONLY status='approved' snippets, keyed by snippet_id."""
    rows = spark.sql(
        f"SELECT snippet_id, sql_template, content_hash FROM {kb_table} "
        f"WHERE status = 'approved'"
    ).collect()
    return {r["snippet_id"]: (r["sql_template"], r["content_hash"]) for r in rows}


def _validate_explain(spark, sql: str) -> tuple[bool, str]:
    """Validate-don't-execute: run EXPLAIN. A parse/analysis error raises."""
    try:
        spark.sql(f"EXPLAIN {sql}")
        return True, ""
    except Exception as e:  # analysis/parse failure
        return False, str(e)[:400]


def _wrap_for_explain(sql: str, inter: dict) -> str:
    """Snippets referencing an intermediate CTE ({{cohort}}) can't EXPLAIN alone;
    wrap them with a minimal stub CTE so EXPLAIN validates the shape."""
    c = cfg()
    if not inter:
        return sql
    stub = f"(SELECT patient_id, CAST(NULL AS DATE) index_date FROM {c.serving}.patient_timeline LIMIT 0)"
    ctes = ", ".join(f"{name} AS {stub}" for name in inter.values())
    return f"WITH {ctes} {sql}"


def _normalize_spec(spec: dict) -> dict:
    """Coerce a spec (from the table or offline) into the shape build_ads uses."""
    s = dict(spec)
    cc = s.get("covariates_coded")
    if isinstance(cc, str):
        try:
            cc = json.loads(cc)
        except (ValueError, TypeError):
            cc = []
    s["covariates_coded"] = cc or []
    for k in ("study_start", "study_end"):
        v = s.get(k)
        if hasattr(v, "isoformat"):
            s[k] = v.isoformat()
        elif v is not None:
            s[k] = str(v)
    for k in ("dx_codes", "ndc_codes", "exclude_dx", "outcome_codes"):
        s[k] = list(s.get(k) or [])
    return s


def load_protocol_spec(spark, study_id: str, require_approved: bool = True) -> dict | None:
    """Load the reviewed coded spec for a study from cfg().raw.protocol_spec.

    Only review_status='approved' rows are eligible unless require_approved=False
    (used by --code-only smoke runs). Returns None if not found/approved.
    """
    c = cfg()
    tbl = c.table("raw", "protocol_spec")
    where = f"study_id = '{study_id}'"
    if require_approved:
        where += " AND review_status = 'approved'"
    rows = spark.sql(f"SELECT * FROM {tbl} WHERE {where} LIMIT 1").collect()
    if not rows:
        return None
    return _normalize_spec(rows[0].asDict())


def build_ads(spark, poc_id: str | None = None, spec: dict | None = None,
              model: str | None = None) -> BuildManifest:
    """Compose + validate + (if valid) materialize an ADS for a study.

    spec: reviewed coded spec dict (from load_protocol_spec). If omitted, falls
    back to POC_SPECS[poc_id] (offline). Executes only against synthetic gold and
    only if every step passes EXPLAIN.
    """
    c = cfg()
    if spec is None:
        if poc_id not in POC_SPECS:
            raise ValueError(f"no spec provided and no offline fallback for '{poc_id}'")
        spec = POC_SPECS[poc_id]
    spec = _normalize_spec(spec)
    study_id = spec.get("study_id") or poc_id or "unknown"
    complexity = (spec.get("complexity") or "").lower()
    model = model or c.default_model
    t0 = time.time()
    ads_id = f"ADS_{study_id}_{int(t0)}"
    manifest = BuildManifest(ads_id=ads_id, study_id=study_id,
                             protocol_version=spec.get("version", "1.0"),
                             model=model, complexity=complexity)

    if complexity not in RECIPES:
        manifest.error = f"unknown complexity '{complexity}' (need low|medium|high)"
        manifest.duration_sec = time.time() - t0
        return manifest

    kb = _fetch_approved_snippets(spark, c.kb_table)
    recipe = RECIPES[complexity]

    # identifiers inserted raw (not quoted); everything else goes through _format_param
    idents_base = {k: spec[k] for k in IDENTIFIER_KEYS if k in spec and k != "cov_name"}
    spec_quoted = {k: v for k, v in spec.items() if k not in IDENTIFIER_KEYS}

    def compose(step, snippet_id, cte_refs, extra_spec=None, extra_idents=None):
        if snippet_id not in kb:
            sr = StepResult(step, snippet_id, "", "", validated=True, explain_ok=False,
                            error=f"snippet '{snippet_id}' not approved/available")
            manifest.steps.append(sr)
            return None
        tmpl, h = kb[snippet_id]
        ps = {**spec_quoted, **(extra_spec or {})}
        idents = {**idents_base, **(extra_idents or {}), **cte_refs}
        sql = substitute_tokens(tmpl, ps, intermediate_tables=idents)
        ok, err = _validate_explain(spark, _wrap_for_explain(sql, cte_refs))
        manifest.steps.append(StepResult(step, snippet_id, h, sql,
                                         validated=True, explain_ok=ok, error=err))
        return sql

    # 1) cohort (no cte input)
    cohort_sql = compose("cohort", recipe["cohort"], {})
    cte_sql = {"cohort": cohort_sql}
    survivor = "cohort"

    # 2) narrowing chain: each step references the previous survivor cte
    for step_name, snippet_id in recipe["narrow"]:
        cte_name = step_name  # unique cte alias
        sql = compose(step_name, snippet_id, {"cohort": survivor})
        cte_sql[cte_name] = sql
        survivor = cte_name

    # 3) baseline covariate flags (one der_baseline_covariate_flag per covariate)
    cov_ctes = []
    for i, cov in enumerate(spec["covariates_coded"]):
        cte_name = f"cov_{i}"
        sql = compose(f"covariate_{cov['name']}", "der_baseline_covariate_flag",
                      {"cohort": survivor},
                      extra_spec={"cov_codes": cov["codes"]},
                      extra_idents={"cov_name": cov["name"]})
        cte_sql[cte_name] = sql
        cov_ctes.append((cte_name, cov["name"]))

    # 4) exposure era (time-varying) for high complexity — patient-level
    era_cte = None
    if recipe["exposure_era"]:
        sql = compose("exposure_era", "der_exposure_era", {})
        cte_sql["era"] = sql
        era_cte = "era"

    # 5) outcome (references survivor)
    out_sql = compose("outcome", "out_first_event", {"cohort": survivor})
    cte_sql["outc"] = out_sql

    # assembly: one row per patient (stable multi-study schema; covariates as a MAP)
    cov_map = "CAST(NULL AS MAP<STRING,INT>)"
    if cov_ctes:
        pairs = ", ".join(f"'{name}', {cte}.{name}" for cte, name in cov_ctes)
        cov_map = f"map({pairs})"
    era_start = f"{era_cte}.era_start" if era_cte else "CAST(NULL AS DATE)"
    era_end = f"{era_cte}.era_end" if era_cte else "CAST(NULL AS DATE)"

    with_ctes = ",\n".join(f"{name} AS (\n{sql}\n)" for name, sql in cte_sql.items()
                           if sql is not None)
    joins = [f"LEFT JOIN outc ON outc.patient_id = s.patient_id"]
    for cte, _ in cov_ctes:
        joins.append(f"LEFT JOIN {cte} ON {cte}.patient_id = s.patient_id")
    if era_cte:
        joins.append(f"LEFT JOIN {era_cte} ON {era_cte}.patient_id = s.patient_id")

    ads_sql = f"""WITH {with_ctes}
SELECT
  concat('{study_id}', '_', s.patient_id) AS row_id,
  '{study_id}' AS study_id,
  '{ads_id}'  AS ads_id,
  s.patient_id, s.index_date,
  outc.outcome_flag, outc.time_to_event, outc.outcome_date,
  {cov_map} AS covariates,
  {era_start} AS era_start,
  {era_end} AS era_end
FROM {survivor} s
{chr(10).join(joins)}"""

    ok, err = _validate_explain(spark, ads_sql)
    manifest.steps.append(StepResult("assembly", "asm_one_row_per_patient",
                          kb.get("asm_one_row_per_patient", ("", ""))[1], ads_sql,
                          validated=True, explain_ok=ok, error=err))

    if not all(s.explain_ok for s in manifest.steps):
        manifest.error = "one or more steps failed EXPLAIN validation"
        manifest.duration_sec = time.time() - t0
        return manifest   # validation failed -> DO NOT execute; return for review

    # All steps passed EXPLAIN -> execute ONLY against synthetic gold, per study.
    _materialize(spark, study_id, ads_id, ads_sql, cov_ctes)
    out_tbl = c.table("serving", "ads_output")
    manifest.ads_row_count = spark.sql(
        f"SELECT COUNT(*) n FROM {out_tbl} WHERE study_id = '{study_id}'").collect()[0]["n"]
    manifest.executed = True
    manifest.kb_snippets_hash = hashlib.sha256(
        "".join(sorted(s.snippet_hash for s in manifest.steps if s.snippet_hash)).encode()
    ).hexdigest()[:16]
    manifest.duration_sec = time.time() - t0
    return manifest


def _has_column(spark, table_fqn: str, col: str) -> bool:
    try:
        cols = {r["col_name"].lower() for r in spark.sql(f"DESCRIBE TABLE {table_fqn}").collect()
                if r["col_name"] and not r["col_name"].startswith("#")}
        return col.lower() in cols
    except Exception:
        return True  # table doesn't exist yet -> CREATE IF NOT EXISTS handles it


def _materialize(spark, study_id: str, ads_id: str, ads_sql: str, cov_ctes: list) -> None:
    """Create the stable multi-study serving tables (idempotent) and refresh this study."""
    c = cfg()
    out_tbl = c.table("serving", "ads_output")
    summ_tbl = c.table("serving", "cohort_summary")

    # One-time migration: the legacy poc_low-only tables lack the multi-study
    # columns, so CREATE IF NOT EXISTS would be a no-op and the per-study
    # DELETE/INSERT would fail. Drop the legacy shape so the new schema is created.
    if not _has_column(spark, out_tbl, "study_id"):
        spark.sql(f"DROP TABLE IF EXISTS {out_tbl}")
    if not _has_column(spark, summ_tbl, "covariate_rates"):
        spark.sql(f"DROP TABLE IF EXISTS {summ_tbl}")

    spark.sql(f"""CREATE TABLE IF NOT EXISTS {out_tbl} (
  row_id STRING, study_id STRING, ads_id STRING,
  patient_id STRING, index_date DATE,
  outcome_flag INT, time_to_event INT, outcome_date DATE,
  covariates MAP<STRING,INT>, era_start DATE, era_end DATE
) USING DELTA
COMMENT 'ADS output (one row per patient per study); covariates as baseline-flag map'""")

    spark.sql(f"""CREATE TABLE IF NOT EXISTS {summ_tbl} (
  study_id STRING, ads_id STRING, n_patients BIGINT, n_outcomes BIGINT,
  outcome_rate DOUBLE, avg_time_to_event_days DOUBLE,
  covariate_rates MAP<STRING,DOUBLE>
) USING DELTA
COMMENT 'Per-study cohort summary for low-latency serving'""")

    # refresh only this study's rows (multi-study coexistence, idempotent)
    spark.sql(f"DELETE FROM {out_tbl} WHERE study_id = '{study_id}'")
    spark.sql(f"INSERT INTO {out_tbl} SELECT * FROM ({ads_sql})")

    cov_rate_map = "CAST(NULL AS MAP<STRING,DOUBLE>)"
    if cov_ctes:
        pairs = ", ".join(f"'{name}', ROUND(AVG(covariates['{name}']), 4)" for _, name in cov_ctes)
        cov_rate_map = f"map({pairs})"
    spark.sql(f"DELETE FROM {summ_tbl} WHERE study_id = '{study_id}'")
    spark.sql(f"""INSERT INTO {summ_tbl}
SELECT '{study_id}' AS study_id, '{ads_id}' AS ads_id,
       COUNT(*) AS n_patients, SUM(outcome_flag) AS n_outcomes,
       ROUND(AVG(outcome_flag), 4) AS outcome_rate,
       ROUND(AVG(time_to_event), 1) AS avg_time_to_event_days,
       {cov_rate_map} AS covariate_rates
FROM {out_tbl} WHERE study_id = '{study_id}'""")
