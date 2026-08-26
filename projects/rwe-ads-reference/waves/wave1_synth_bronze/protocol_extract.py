"""Wave 1 — Protocol PDF/DOCX extraction (serverless, MODEL stage). Idempotent.

The document-reading half of protocol intake:

    ads_raw.protocols/*.{pdf,docx}
        --ai_parse_document-->  plain text
        --ai_extract-->         coded fields + narrative  (raw, in protocol_extraction)
        --protocol_standardize->  validated/standardized coded spec
        --upsert-->             protocol_spec (review_status='extracted')

This is the *non-deterministic* stage: an LLM reads the documents. Everything
downstream (standardization, the review gate, the ADS build) is deterministic.
Extraction never approves anything — a human must review + e-sign before the
ADS Builder will consume a spec (review_gate.approve_protocol).

Runs as a serverless spark_python_task. AI Functions (ai_parse_document,
ai_extract) require a recent serverless runtime. All names resolve via
lib/config.py.
"""
from __future__ import annotations

# Serverless spark_python_task runs this file via exec() with no __file__ in
# globals; recover it from the frame so downstream Path(__file__) works.
try:
    __file__
except NameError:  # pragma: no cover
    import inspect as _inspect
    __file__ = _inspect.getfile(_inspect.currentframe())

import json
import os
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lib.config import cfg
from waves.wave1_synth_bronze.protocol_ingest import (
    create_protocol_spec_table,
    create_protocol_extraction_table,
    upsert_protocol_spec_rows,
)
from lib.pipeline.protocol_standardize import standardize_extraction

try:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
except Exception:
    spark = None


# ai_extract schema: the coded contract the ADS builder needs, plus a little
# narrative for the audit trail / app display. Codes come out as arrays.
EXTRACT_SCHEMA = {
    "study_id": {"type": "string"},
    "complexity": {"type": "string"},
    "title": {"type": "string"},
    "objective": {"type": "string"},
    "dx_codes": {"type": "array", "items": {"type": "string"}},
    "ndc_codes": {"type": "array", "items": {"type": "string"}},
    "exclude_dx": {"type": "array", "items": {"type": "string"}},
    "covariate_codes": {"type": "array", "items": {"type": "string"}},
    "outcome_codes": {"type": "array", "items": {"type": "string"}},
    "study_start": {"type": "string"},
    "study_end": {"type": "string"},
    "min_age": {"type": "integer"},
    "max_age": {"type": "integer"},
    "pre_days": {"type": "integer"},
    "post_days": {"type": "integer"},
    "washout_days": {"type": "integer"},
    "baseline_days": {"type": "integer"},
    "followup_days": {"type": "integer"},
}


def _volume_files(vol_path: str) -> list[str]:
    """Names of regular files directly in the protocols volume.

    UC volumes are FUSE-mounted at /Volumes/... so plain os access works on
    serverless (this is the same filesystem path the seed generator writes to).
    A missing volume path lists as empty rather than raising.
    """
    try:
        return sorted(
            f for f in os.listdir(vol_path)
            if os.path.isfile(os.path.join(vol_path, f))
        )
    except FileNotFoundError:
        return []


def seed_sample_protocols_if_empty(c) -> int:
    """Bootstrap: if the protocols volume has no files, generate the bundled
    sample protocol PDFs/DOCX into it so a FRESH deploy exercises the real
    ai_parse_document -> ai_extract path out-of-the-box (no manual seed step).

    Idempotent: once real redacted protocols (or a prior seed) are in the
    volume, this is a no-op — customers just drop their files in and re-run.

    The generator (scripts/seed_protocol_pdfs.py) needs reportlab + python-docx,
    provided by the job environment (resources/jobs.yml env deps for
    wave1_synth_bronze and app_protocol_extract) — NOT pip-installed at runtime
    (a runtime install into a live serverless kernel can hang/crash it). Returns
    the file count present after seeding.
    """
    vol_path = c.protocols_volume_path
    existing = _volume_files(vol_path)
    if existing:
        print(f"[wave1] protocols volume has {len(existing)} file(s); skipping seed.")
        return len(existing)

    print(f"[wave1] protocols volume {vol_path} is empty — seeding bundled sample protocols…")
    from scripts.seed_protocol_pdfs import main as _generate_and_upload_samples
    _generate_and_upload_samples()
    seeded = _volume_files(vol_path)
    print(f"[wave1] volume now holds {len(seeded)} sample protocol file(s): {seeded}")
    return len(seeded)


# --- ai_extract v2.1 output-shape helpers -----------------------------------
# ai_extract v2.1 + mode='precision' + enableConfidenceScores/enableCitations are
# SERVER-SIDE Databricks AI Function features selected via the options MAP below
# — NOT a client SDK / pip upgrade. Requirement is only a recent-enough serverless
# runtime (AI Functions baseline DBR 15.1+; ai_parse_document needs DBR 17.3+).
#
# With mode=precision + enableConfidenceScores + enableCitations, ai_extract
# returns EACH scalar field of `response` as an object {value, citation_ids,
# confidence_score} (NOT a bare scalar), and adds a top-level `metadata` block
# carrying the citation definitions. So every field read now goes through
# `:value`, and array elements are unwrapped defensively.
_SQL_SCALAR_TYPE = {"string": "STRING", "integer": "INT"}
# Precision mode caps the input document at < 20,000 characters (per Databricks
# docs). Oversized protocols (e.g. poc_high) are truncated to this length rather
# than dropped, so a long document degrades gracefully instead of hard-failing.
_PRECISION_MAX_CHARS = 20000


def _field_value_expr(field: str, spec: dict) -> str:
    """SQL that reads one ai_extract field back to its bare scalar/array value.

    Scalars read `:value` (they are now {value, ...} objects). Array elements may
    be bare scalars OR {value, ...} objects depending on the field, so coalesce
    the object form with the plain cast — robust to either returned shape.
    """
    if spec.get("type") == "array":
        return (f"transform(try_cast(r:response:{field} AS ARRAY<VARIANT>), "
                f"x -> coalesce(x:value::STRING, x::STRING))")
    sql_t = _SQL_SCALAR_TYPE.get(spec.get("type"), "STRING")
    return f"r:response:{field}:value::{sql_t}"


def extract_to_staging(c) -> int:
    """Parse every doc in the protocols volume and land ai_extract output raw.

    Overwrites protocol_extraction with one row per document. Read endpoints are
    never blocked by a single bad file: parse/extract errors are captured per row.
    A genuinely empty volume returns 0 (no crash) rather than reading nothing.
    """
    if spark is None:
        return 0
    vol = c.protocols_volume_path
    staging = c.table("raw", "protocol_extraction")
    model = c.default_model
    schema_json = json.dumps(EXTRACT_SCHEMA).replace("'", "\\'")

    files = _volume_files(vol)
    if not files:
        print(f"[wave1] protocols volume {vol} is empty; nothing to extract (0 rows).")
        return 0

    # ai_extract v2.1 returns each field as {value, citation_ids, confidence_score}.
    # Rebuild the bare-scalar response JSON the deterministic standardizer consumes
    # (extracted_json), and capture the metadata (citations) + per-field confidence
    # and citation_ids in confidence_json to drive the review queue.
    value_pairs = ",\n           ".join(
        f"'{f}', {_field_value_expr(f, s)}" for f, s in EXTRACT_SCHEMA.items())
    conf_pairs = ", ".join(f"'{f}', r:response:{f}:confidence_score" for f in EXTRACT_SCHEMA)
    cite_pairs = ", ".join(f"'{f}', r:response:{f}:citation_ids" for f in EXTRACT_SCHEMA)

    print(f"[wave1] extracting {len(files)} protocol file(s) from {vol} → {staging}")
    # `path` is a first-class column of the binaryFile format (unlike the hidden
    # `_metadata` metadata column, which does not resolve when the source read
    # is empty) — robust on both fresh and populated volumes.
    df = spark.sql(f"""
WITH parsed AS (
  SELECT
    path AS source_protocol,
    ai_parse_document(content, map('version','2.0')) AS doc
  FROM READ_FILES('{vol}/', format => 'binaryFile')
),
txt AS (
  SELECT source_protocol,
         doc:error_status::STRING AS parse_error,
         concat_ws('\\n', transform(
           try_cast(doc:document:elements AS ARRAY<VARIANT>), e -> e:content::STRING)) AS text_blocks
  FROM parsed
),
ext AS (
  SELECT source_protocol, parse_error, text_blocks, length(text_blocks) AS text_len,
         CASE WHEN parse_error IS NULL AND text_blocks IS NOT NULL
              THEN ai_extract(
                     -- precision mode requires input < 20,000 chars; truncate the
                     -- rare oversized protocol so it degrades instead of failing.
                     CASE WHEN length(text_blocks) >= {_PRECISION_MAX_CHARS}
                          THEN substr(text_blocks, 1, {_PRECISION_MAX_CHARS - 1})
                          ELSE text_blocks END,
                     '{schema_json}',
                     map('version','2.1','mode','precision','enableConfidenceScores','true','enableCitations','true'))
              ELSE NULL END AS r
  FROM txt
)
SELECT
  source_protocol,
  r:response:study_id:value::STRING            AS study_id,
  to_json(named_struct(
           {value_pairs}))                     AS extracted_json,
  to_json(named_struct(
           'metadata', r:metadata,
           'field_confidence', named_struct({conf_pairs}),
           'field_citations', named_struct({cite_pairs}))) AS confidence_json,
  -- parsed protocol text (same truncation ai_extract saw) for the eval's
  -- completeness judge; NULL when parsing failed.
  substr(text_blocks, 1, {_PRECISION_MAX_CHARS - 1})  AS source_text,
  text_len,
  '{model}'                                    AS extraction_model,
  parse_error,
  r:error_message::STRING                      AS extract_error,
  current_timestamp()                          AS ingestion_ts
FROM ext
""")
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(staging)
    n = spark.table(staging).count()
    # Flag any protocol whose text exceeded the precision-mode cap and was truncated
    # (text_len records the ORIGINAL length before truncation).
    for row in spark.sql(
            f"SELECT source_protocol, text_len FROM {staging} "
            f"WHERE text_len >= {_PRECISION_MAX_CHARS}").collect():
        print(f"[wave1] WARNING: {row['source_protocol']} text_len={row['text_len']} "
              f">= {_PRECISION_MAX_CHARS}; truncated to {_PRECISION_MAX_CHARS - 1} "
              f"chars for ai_extract precision mode.")
    print(f"[wave1] staged {n} extracted protocol(s)")
    return n


def promote_staging_to_spec(c) -> dict:
    """Standardize each staged extraction and upsert into protocol_spec.

    Returns a per-study report {study_id: {"ok": bool, "errors": [...], "warnings": [...]}}.
    Rows land at review_status='extracted' — never approved by this path.
    """
    if spark is None:
        return {}
    staging = c.table("raw", "protocol_extraction")
    report = {}
    spec_rows = []

    for row in spark.table(staging).collect():
        sid = row["study_id"] or Path(row["source_protocol"] or "unknown").stem
        if row["parse_error"] or row["extract_error"] or not row["extracted_json"]:
            report[sid] = {"ok": False, "errors": [
                f"parse_error={row['parse_error']}", f"extract_error={row['extract_error']}"]}
            continue
        try:
            extracted = json.loads(row["extracted_json"])
        except (TypeError, json.JSONDecodeError) as e:
            report[sid] = {"ok": False, "errors": [f"bad extracted_json: {e}"]}
            continue

        spec, result = standardize_extraction(extracted)
        spec["source_protocol"] = row["source_protocol"]
        spec["extraction_model"] = row["extraction_model"]
        spec["extraction_confidence"] = row["confidence_json"]
        spec["review_status"] = "extracted"
        report[spec.get("study_id", sid)] = {
            "ok": result["ok"], "errors": result["errors"], "warnings": result["warnings"]}
        spec_rows.append(spec)

    n = upsert_protocol_spec_rows(c, spec_rows)
    print(f"[wave1] upserted {n} standardized protocol spec(s) at review_status='extracted'")
    return report


def main():
    c = cfg()
    print(f"=== Wave 1 protocol extraction for {c.initiative} @ {c.catalog} ===")
    assert c.compute == "serverless", "Golden rule: serverless only"

    create_protocol_spec_table(c)
    create_protocol_extraction_table(c)
    seed_sample_protocols_if_empty(c)   # ensure real PDF/DOCX exist before we read
    n = extract_to_staging(c)
    if n == 0:
        print("[wave1] no protocol files found in the volume; nothing to extract.")
        return
    report = promote_staging_to_spec(c)
    print("[wave1] extraction report:")
    print(json.dumps(report, indent=2, default=str))
    print("[wave1] protocol extraction done. (specs pending analyst review + e-sign)")


if __name__ == "__main__":
    main()
