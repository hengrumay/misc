"""Run a deterministic, KB-grounded ADS build for a PoC and enqueue for review.

CLI: python run_ads_build.py --poc {poc_low|poc_med|poc_high} [--code-only]

Workflow:
  1. Load the reviewed coded spec from cfg().raw.protocol_spec (review_status=
     'approved'). With --code-only, use the offline POC_SPECS fallback instead
     (no table / no approval needed) for smoke tests.
  2. Compose + EXPLAIN-validate + (if valid) materialize via ads_build_core.
  3. Print each step's generated SQL + EXPLAIN result (the "first SQL step").
  4. Write a reproducibility manifest (protocol version, KB snippet hashes,
     generated SQL, source Delta versions, model) to the audit schema.
  5. Enqueue the build into review_queue (PENDING analyst e-sign).

Validate-don't-execute is enforced in ads_build_core: nothing runs that did not
pass EXPLAIN, and execution only ever targets synthetic gold. All names resolve
through lib/config.py.
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
import logging
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

logger = logging.getLogger(__name__)

try:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
except Exception:  # pragma: no cover
    spark = None


def _source_table_versions(c) -> dict:
    """Capture current Delta versions of the gold sources for reproducibility."""
    versions = {}
    if spark is None:
        return versions
    for name in ("patient_timeline", "eligibility_periods"):
        fqn = c.table("serving", name)
        try:
            v = spark.sql(f"DESCRIBE HISTORY {fqn} LIMIT 1").collect()[0]["version"]
            versions[fqn] = str(v)
        except Exception:
            pass
    return versions


def _enqueue_for_review(c, manifest) -> str:
    """Insert a PENDING row into cfg().serving.review_queue (Delta)."""
    review_id = f"REV_{manifest.ads_id}"
    if spark is None:
        return review_id
    q = c.table("serving", "review_queue")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {q} (
  review_id STRING, ads_id STRING, study_id STRING, complexity STRING,
  n_patients BIGINT, kb_snippets_hash STRING, status STRING, created_ts TIMESTAMP
) USING DELTA COMMENT 'ADS builds pending analyst review + e-sign'""")
    from pyspark.sql.types import (StructType, StructField, StringType, LongType, TimestampType)
    schema = StructType([
        StructField("review_id", StringType()), StructField("ads_id", StringType()),
        StructField("study_id", StringType()), StructField("complexity", StringType()),
        StructField("n_patients", LongType()), StructField("kb_snippets_hash", StringType()),
        StructField("status", StringType()), StructField("created_ts", TimestampType()),
    ])
    spark.sql(f"DELETE FROM {q} WHERE study_id = '{manifest.study_id}' AND status = 'PENDING'")
    (spark.createDataFrame([(
        review_id, manifest.ads_id, manifest.study_id, manifest.complexity,
        int(manifest.ads_row_count), manifest.kb_snippets_hash, "PENDING", datetime.utcnow())],
        schema).write.mode("append").saveAsTable(q))
    return review_id


def main():
    parser = argparse.ArgumentParser(description="Run ADS build for a PoC study")
    parser.add_argument("--poc", choices=["poc_low", "poc_med", "poc_high"], default="poc_low")
    parser.add_argument("--code-only", action="store_true",
                        help="use offline POC_SPECS instead of the approved table spec")
    args = parser.parse_args()

    from lib.config import cfg
    from waves.wave3_ads_build.ads_build_core import build_ads, load_protocol_spec, POC_SPECS

    c = cfg()
    poc_id = args.poc
    logger.info(f"=== ADS Build: {poc_id} @ {c.catalog} (code_only={args.code_only}) ===")

    # Step 1: load the reviewed spec
    spec = None
    if args.code_only:
        spec = POC_SPECS.get(poc_id)
        logger.info("Using offline POC_SPECS fallback (--code-only)")
    else:
        spec = load_protocol_spec(spark, poc_id, require_approved=True)
        if spec is None:
            msg = (f"No APPROVED protocol_spec for study_id='{poc_id}'. "
                   f"Run protocol_extract, then approve via the review gate first.")
            logger.error(msg)
            return {"status": "error", "message": msg}
    if not spec:
        return {"status": "error", "message": f"no spec available for {poc_id}"}

    logger.info(f"Spec: study={spec.get('study_id')} complexity={spec.get('complexity')} "
                f"version={spec.get('version')}")

    # Step 2: build (validate-don't-execute inside)
    manifest = build_ads(spark, poc_id=poc_id, spec=spec, model=c.default_model)

    # Step 3: print generated SQL + EXPLAIN results
    logger.info("\n=== Generated SQL / EXPLAIN validation ===")
    for i, step in enumerate(manifest.steps):
        status = "PASS" if step.explain_ok else "FAIL"
        logger.info(f"[{i}] {step.step} ({step.snippet_id}): EXPLAIN {status}")
        if i == 0:
            logger.info("----- first SQL step -----\n" + (step.sql or "") + "\n--------------------------")
        if step.error:
            logger.warning(f"    error: {step.error}")

    if not manifest.executed:
        logger.error(f"ADS build did NOT execute: {manifest.error}")
        return {"status": "validation_failed", "poc_id": poc_id, "ads_id": manifest.ads_id,
                "error": manifest.error,
                "steps": [{"step": s.step, "explain_ok": s.explain_ok, "error": s.error}
                          for s in manifest.steps]}

    logger.info(f"ADS executed: {manifest.ads_row_count} rows -> {c.table('serving','ads_output')} "
                f"(study={manifest.study_id}) in {manifest.duration_sec:.1f}s")

    # Step 4: reproducibility manifest
    manifest_id = None
    try:
        from waves.wave5_serving_audit_app.setup_audit import write_manifest, ManifestRecord
        rec = ManifestRecord(
            ads_id=manifest.ads_id, study_id=manifest.study_id,
            protocol_version=manifest.protocol_version,
            kb_snippet_versions=[{"snippet_id": s.snippet_id, "version": 1,
                                  "content_hash": s.snippet_hash} for s in manifest.steps if s.snippet_hash],
            generated_sql={s.step: (s.sql or "") for s in manifest.steps},
            source_table_versions=_source_table_versions(c),
            model=manifest.model, agent_version="ads_build_core/2.0",
            reviewer=None, esignature=None, decision="built",
        )
        manifest_id = write_manifest(rec, c.table("audit", "repro_manifest"))
        logger.info(f"Reproducibility manifest: {manifest_id}")
    except Exception as e:
        logger.warning(f"repro manifest write skipped: {e}")

    # Step 5: enqueue for analyst review + e-sign
    review_id = _enqueue_for_review(c, manifest)
    logger.info(f"Enqueued for review: {review_id} (PENDING analyst e-sign)")

    return {
        "status": "success", "poc_id": poc_id, "ads_id": manifest.ads_id,
        "study_id": manifest.study_id, "complexity": manifest.complexity,
        "ads_row_count": manifest.ads_row_count, "kb_snippets_hash": manifest.kb_snippets_hash,
        "manifest_id": manifest_id, "review_id": review_id,
        "duration_sec": round(manifest.duration_sec, 2),
        "message": "ADS build complete; pending analyst review and e-signature",
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    result = main()
    print(json.dumps(result, indent=2, default=str))
    # NOTE: do NOT sys.exit(0) here. Under a serverless spark_python_task,
    # raising SystemExit (even with code 0) is flagged as a task FAILURE, so a
    # successful build would false-fail. Let a successful run reach end-of-file
    # for an implicit clean exit; raise only on a genuine failure so Databricks
    # marks real failures FAILED with a message.
    if result.get("status") not in ("success", "validation_failed"):
        raise RuntimeError(
            f"ADS build failed: {result.get('status')} — {result.get('message', '')}"
        )
