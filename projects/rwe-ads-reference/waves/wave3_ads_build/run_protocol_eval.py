"""Wave 3 — Extraction eval (Stages 1-3), runs BETWEEN extraction and approve.

The reference-free extraction-eval funnel from the extraction eval + HITL
design note. Reads every
``review_status='extracted'`` protocol_spec and attaches a real quality signal
so approval is no longer a zero-signal rubber stamp:

  * **Stage 1** deterministic validators (``lib.pipeline.spec_validate``) — the
    HARD gate: ``eval_ok=False`` blocks the unattended system e-sign
    (``approve_protocols.py``). No model call.
  * **Stage 2** confidence threshold — per-field ``confidence_score`` from
    ``ai_extract`` v2.1 (stored in ``protocol_spec.extraction_confidence``).
  * **Stage 3** reference-free judges (``lib.pipeline.spec_eval_judge``) —
    citation_supports_value + extraction_completeness, every call routed through
    ``gateway_call`` (masked + logged + traced). Needs the source protocol text,
    read from ``protocol_extraction.source_text``.

Outputs (idempotent):
  * ``raw.protocol_eval`` — one row per study (MERGE by study_id): ok, flags,
    confidence, review priority — the signal ``approve_protocols`` gates on and
    the human reviewer sees.
  * ``raw.protocol_review_queue`` VIEW — extracted specs sorted worst-first
    (highest priority / lowest confidence) for the review UI (golden rule: real
    review gate, not a rubber stamp).
  * ``audit.repro_manifest`` — one row per study carrying ``eval_scores`` (rule
    #10 reproducibility manifest includes the eval scores).
  * ``cfg().inference_table`` — the batched (masked) gateway log for the judge
    calls (cost attribution).

Config-driven (rule #7) via the ``extraction_eval`` block in demo.config.yaml.
CLI: python run_protocol_eval.py [--studies poc_low,poc_med,poc_high]
"""
from __future__ import annotations

# Serverless spark_python_task runs this file via exec() with no __file__ in
# globals; recover it from the frame so downstream Path(__file__) works.
try:
    __file__
except NameError:  # pragma: no cover
    import inspect as _inspect
    __file__ = _inspect.getfile(_inspect.currentframe())

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lib.config import cfg
from lib.pipeline.spec_validate import validate_spec
from lib.pipeline import spec_eval_judge

try:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
except Exception:  # pragma: no cover
    spark = None


# columns the eval reads from protocol_spec
_SPEC_COLS = [
    "study_id", "version", "complexity", "title", "objective",
    "dx_codes", "ndc_codes", "exclude_dx", "outcome_codes", "covariates_coded",
    "study_start", "study_end", "min_age", "max_age",
    "washout_days", "followup_days", "pre_days", "post_days", "baseline_days",
    "extraction_confidence", "source_protocol", "review_status",
]


def _as_bool(v, default: bool) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "on")


def _eval_config(c) -> dict:
    weights = c.get("extraction_eval.priority_weights", {}) or {}
    return {
        "enable_judge": _as_bool(c.get("extraction_eval.enable_judge", True), True),
        "judge_model": c.get("extraction_eval.judge_model") or c.default_model,
        "floor": float(c.get("extraction_eval.confidence_floor", 0.5)),
        "w_hard": float(weights.get("hard_fail", 10.0)),
        "w_lowconf": float(weights.get("low_confidence", 2.0)),
        "w_typeflag": float(weights.get("type_flag", 5.0)),
        "w_incomplete": float(weights.get("incomplete", 3.0)),
    }


def _row_to_spec(row: dict) -> dict:
    """Standardized spec dict for the validators/judges (dates -> iso strings)."""
    spec = dict(row)
    for dcol in ("study_start", "study_end"):
        v = spec.get(dcol)
        if v is not None and not isinstance(v, str):
            spec[dcol] = str(v)
    return spec


def _stage2_confidence(extraction_confidence: str, floor: float) -> tuple[list, float | None]:
    """Per-field confidence < floor + the minimum confidence seen.

    Reads ``field_confidence`` from the v2.1 confidence JSON. A field's value may
    be a scalar score, a per-element list of scores, or null (array fields whose
    top-level score is absent) — all handled.
    """
    low: list[dict] = []
    seen: list[float] = []
    if not extraction_confidence:
        return low, None
    try:
        blob = json.loads(extraction_confidence)
    except (TypeError, json.JSONDecodeError):
        return low, None
    fc = blob.get("field_confidence") or {}
    if not isinstance(fc, dict):
        return low, None
    for field, val in fc.items():
        scores = val if isinstance(val, list) else [val]
        for s in scores:
            if isinstance(s, (int, float)):
                seen.append(float(s))
                if float(s) < floor:
                    low.append({"field": field, "confidence": float(s)})
    return low, (min(seen) if seen else None)


def _source_texts(c, staging_tbl: str) -> dict:
    """study_id -> parsed source protocol text (for Stage-3 completeness)."""
    texts: dict = {}
    if spark is None:
        return texts
    try:
        for r in spark.sql(
                f"SELECT study_id, source_protocol, source_text FROM {staging_tbl}").collect():
            key = r["study_id"] or Path(r["source_protocol"] or "").stem
            if key and r["source_text"]:
                texts[key] = r["source_text"]
    except Exception as e:  # noqa: BLE001 - missing column/table degrades to no-text
        print(f"[eval] WARNING: source_text unavailable ({e}); Stage-3 completeness/citation "
              f"judges will SKIP and verdicts fall back to Stage-1/2 only. Ensure "
              f"{staging_tbl} has a populated source_text column (wave1 protocol_ingest "
              f"self-heals the schema; re-run extraction to populate).")
    return texts


def _eval_table_schema():
    from pyspark.sql.types import (
        StructType, StructField, StringType, BooleanType, IntegerType,
        DoubleType, TimestampType,
    )
    return StructType([
        StructField("study_id", StringType()),
        StructField("eval_ok", BooleanType()),
        StructField("n_hard_fails", IntegerType()),
        StructField("hard_fail_reasons", StringType()),
        StructField("min_confidence", DoubleType()),
        StructField("n_low_confidence", IntegerType()),
        StructField("low_confidence", StringType()),
        StructField("n_flags", IntegerType()),
        StructField("field_flags", StringType()),
        StructField("completeness_ok", StringType()),
        StructField("n_omissions", IntegerType()),
        StructField("omissions", StringType()),
        StructField("review_priority", DoubleType()),
        StructField("judge_model", StringType()),
        StructField("judge_calls", IntegerType()),
        StructField("eval_ts", TimestampType()),
    ])


def _create_eval_table(c) -> str:
    tbl = c.table("raw", "protocol_eval")
    if spark is not None:
        spark.sql(f"""CREATE TABLE IF NOT EXISTS {tbl} (
  study_id           STRING NOT NULL PRIMARY KEY,
  eval_ok            BOOLEAN,       -- Stage-1 hard gate: False blocks auto-e-sign
  n_hard_fails       INT,
  hard_fail_reasons  STRING,        -- JSON array of reasons
  min_confidence     DOUBLE,        -- lowest per-field confidence_score seen
  n_low_confidence   INT,
  low_confidence     STRING,        -- JSON [{{field, confidence}}]
  n_flags            INT,           -- Stage-3 type/support flags
  field_flags        STRING,        -- JSON {{field: {{supported, correct_type, rationale}}}}
  completeness_ok    STRING,        -- yes | no | unknown
  n_omissions        INT,
  omissions          STRING,        -- JSON [{{item, why_it_matters}}]
  review_priority    DOUBLE,        -- higher = worse = review first
  judge_model        STRING,
  judge_calls        INT,
  eval_ts            TIMESTAMP
) USING DELTA
COMMENT 'Reference-free extraction-eval verdict per protocol spec (Stages 1-3)'""")
    return tbl


def _merge_eval_rows(c, tbl: str, rows: list[tuple]) -> int:
    if spark is None or not rows:
        return 0
    df = spark.createDataFrame(rows, _eval_table_schema())
    df.createOrReplaceTempView("_protocol_eval_src")
    cols = [f.name for f in _eval_table_schema().fields]
    set_clause = ",\n  ".join(f"t.{col} = src.{col}" for col in cols if col != "study_id")
    spark.sql(f"""MERGE INTO {tbl} t
USING _protocol_eval_src src
ON t.study_id = src.study_id
WHEN MATCHED THEN UPDATE SET
  {set_clause}
WHEN NOT MATCHED THEN INSERT *""")
    return len(rows)


def _create_review_queue_view(c) -> None:
    """Worst-first review queue for the UI: extracted specs joined with their eval,
    ordered by review_priority DESC then min_confidence ASC (rule: real gate)."""
    if spark is None:
        return
    spec_tbl = c.table("raw", "protocol_spec")
    eval_tbl = c.table("raw", "protocol_eval")
    view = c.table("raw", "protocol_review_queue")
    spark.sql(f"""CREATE OR REPLACE VIEW {view} AS
SELECT s.study_id, s.title, s.complexity, s.review_status, s.source_protocol,
       e.eval_ok, e.n_hard_fails, e.hard_fail_reasons, e.review_priority,
       e.n_flags, e.min_confidence, e.completeness_ok, e.eval_ts
FROM {spec_tbl} s
LEFT JOIN {eval_tbl} e ON s.study_id = e.study_id
WHERE s.review_status = 'extracted'
ORDER BY e.review_priority DESC NULLS LAST, e.min_confidence ASC NULLS FIRST""")
    print(f"[eval] review-queue view refreshed: {view} (worst-first)")


def _write_gateway_log(c, log_rows: list) -> None:
    """Append the batched (masked) judge gateway calls to cfg().inference_table."""
    if spark is None or not log_rows:
        return
    from pyspark.sql import functions as F
    from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, DoubleType)
    schema = StructType([
        StructField("endpoint", StringType()), StructField("model", StringType()),
        StructField("input_masked", StringType()), StructField("output", StringType()),
        StructField("tokens_in", IntegerType()), StructField("tokens_out", IntegerType()),
        StructField("cost_usd", DoubleType()), StructField("initiative", StringType()),
        StructField("team", StringType())])
    (spark.createDataFrame(log_rows, schema).withColumn("request_ts", F.current_timestamp())
        .select("request_ts", "endpoint", "model", "input_masked", "output",
                "tokens_in", "tokens_out", "cost_usd", "initiative", "team")
        .write.mode("append").saveAsTable(c.inference_table))


def _write_manifest(c, study_id: str, version: str, scores: dict, judge_model: str) -> str | None:
    """Rule #10: fold the eval scores into the reproducibility manifest."""
    try:
        from waves.wave5_serving_audit_app.setup_audit import write_manifest, ManifestRecord
        rec = ManifestRecord(
            ads_id=f"EVAL_{study_id}", study_id=study_id, protocol_version=version,
            model=judge_model, agent_version="extraction_eval/1.0",
            eval_scores={k: float(v) for k, v in scores.items() if v is not None},
            reviewer=None, esignature=None, decision="extraction_eval",
        )
        return write_manifest(rec, c.table("audit", "repro_manifest"))
    except Exception as e:  # noqa: BLE001 - manifest is non-fatal to the eval
        print(f"[eval] manifest write skipped for {study_id}: {e}")
        return None


def evaluate_specs(c, wanted: list[str] | None = None) -> dict:
    ec = _eval_config(c)
    spec_tbl = c.table("raw", "protocol_spec")
    staging_tbl = c.table("raw", "protocol_extraction")

    if spark is None:
        return {"status": "error", "message": "PySpark not available"}

    cols = ", ".join(_SPEC_COLS)
    where = "review_status = 'extracted'"
    if wanted:
        ids = ", ".join("'" + s.replace("'", "") + "'" for s in wanted)
        where += f" AND study_id IN ({ids})"
    rows = [r.asDict() for r in spark.sql(f"SELECT {cols} FROM {spec_tbl} WHERE {where}").collect()]
    if not rows:
        print(f"[eval] no extracted specs to evaluate (where {where}).")
        _create_eval_table(c)
        _create_review_queue_view(c)
        return {"status": "success", "evaluated": 0, "judge_calls": 0, "results": {}}

    texts = _source_texts(c, staging_tbl)
    if ec["enable_judge"] and not texts:
        print(f"[eval] WARNING: no source_text found for any of the {len(rows)} extracted "
              f"spec(s) in {staging_tbl}; Stage-3 judges will not run this pass "
              f"(verdicts will be Stage-1/2 only).")
    _create_eval_table(c)
    eval_tbl = c.table("raw", "protocol_eval")

    w = None
    if ec["enable_judge"]:
        try:
            from databricks.sdk import WorkspaceClient
            w = WorkspaceClient()
        except Exception as e:  # noqa: BLE001 - no judge client -> Stage-1/2 only
            print(f"[eval] WorkspaceClient unavailable ({e}); Stage-3 judges disabled this run.")

    log_rows: list = []
    out_rows: list[tuple] = []
    results: dict = {}
    now = datetime.now(timezone.utc)

    for row in rows:
        sid = row["study_id"]
        spec = _row_to_spec(row)

        # Stage 1 — deterministic hard gate
        s1 = validate_spec(spec)

        # Stage 2 — confidence
        low_conf, min_conf = _stage2_confidence(row.get("extraction_confidence"), ec["floor"])

        # Stage 3 — reference-free judges (only if enabled + source text present)
        doc = texts.get(sid, "")
        if w is not None and doc:
            s3 = spec_eval_judge.run_judges(spec, doc, s1, w=w, log_rows=log_rows,
                                            model=ec["judge_model"])
        else:
            s3 = {"field_flags": {}, "completeness": {}, "n_type_flags": 0, "judge_calls": 0}

        comp = s3.get("completeness") or {}
        completeness_ok = str(comp.get("complete", "unknown")).lower() if comp.get("complete") else "unknown"
        omissions = comp.get("omissions") or []
        n_omissions = len(omissions) if isinstance(omissions, list) else 0

        priority = (ec["w_hard"] * s1["n_hard_fails"]
                    + ec["w_lowconf"] * len(low_conf)
                    + ec["w_typeflag"] * s3.get("n_type_flags", 0)
                    + ec["w_incomplete"] * (1 if completeness_ok == "no" else 0))

        out_rows.append((
            sid, bool(s1["ok"]), int(s1["n_hard_fails"]),
            json.dumps(s1["hard_fail_reasons"]),
            (float(min_conf) if min_conf is not None else None), len(low_conf),
            json.dumps(low_conf), int(s3.get("n_type_flags", 0)),
            json.dumps(s3.get("field_flags", {})), completeness_ok, n_omissions,
            json.dumps(omissions), float(priority), ec["judge_model"],
            int(s3.get("judge_calls", 0)), now,
        ))
        results[sid] = {"eval_ok": bool(s1["ok"]), "n_hard_fails": s1["n_hard_fails"],
                        "min_confidence": min_conf, "n_flags": s3.get("n_type_flags", 0),
                        "completeness_ok": completeness_ok, "review_priority": round(priority, 2),
                        "hard_fail_reasons": s1["hard_fail_reasons"]}

        _write_manifest(c, sid, str(row.get("version") or ""), {
            "eval_ok": 1.0 if s1["ok"] else 0.0, "n_hard_fails": float(s1["n_hard_fails"]),
            "min_confidence": min_conf, "n_flags": float(s3.get("n_type_flags", 0)),
            "review_priority": float(priority),
            "completeness_ok": (1.0 if completeness_ok == "yes" else
                                (0.0 if completeness_ok == "no" else None)),
            "judge_calls": float(s3.get("judge_calls", 0)),
        }, ec["judge_model"])

    n = _merge_eval_rows(c, eval_tbl, out_rows)
    _create_review_queue_view(c)
    _write_gateway_log(c, log_rows)
    judge_calls = len(log_rows)
    print(f"[eval] evaluated {n} spec(s); Stage-3 judge gateway call(s): {judge_calls}.")
    # Fail-loud on silent Stage-3 degradation: judges were meant to run but none did.
    # Do NOT hard-fail (source_text can be legitimately absent) — just make it obvious.
    if ec["enable_judge"] and judge_calls == 0:
        n_with_text = sum(1 for r in rows if texts.get(r["study_id"]))
        cause = ("WorkspaceClient unavailable" if w is None
                 else f"source_text present for {n_with_text}/{len(rows)} evaluated spec(s)")
        print(f"[eval] WARNING: Stage-3 judges were ENABLED but judge_calls=0 -> no "
              f"reference-free judging ran; every verdict is Stage-1/2 only. Cause: {cause}.")
    return {"status": "success", "evaluated": n, "judge_calls": judge_calls, "results": results}


def main():
    p = argparse.ArgumentParser(description="Reference-free extraction eval (Stages 1-3)")
    p.add_argument("--studies", default="",
                   help="comma-separated study_ids; empty = all review_status='extracted'")
    args = p.parse_args()
    c = cfg()
    print(f"=== Extraction eval for {c.initiative} @ {c.catalog} ===")
    wanted = [s.strip() for s in args.studies.split(",") if s.strip()] or None
    result = evaluate_specs(c, wanted)
    print(json.dumps(result, indent=2, default=str))
    return result


if __name__ == "__main__":
    main()
