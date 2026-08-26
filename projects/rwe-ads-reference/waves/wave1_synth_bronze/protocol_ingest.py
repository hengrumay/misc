"""Wave 1 — Protocol Ingestion (serverless). Idempotent.

Owns the `protocol_spec` table (the reviewed, executable study specification the
ADS Builder consumes) and the `protocol_extraction` staging table (raw model
output from PDF/DOCX parsing).

Two ingestion paths land into `protocol_spec`:
  1. PDF/DOCX (PRIMARY): protocol_extract.py (ai_parse_document -> ai_extract) →
     protocol_standardize.py (fixed-rule validate/standardize) → upsert here.
  2. Bundled markdown (FALLBACK/seed): a regex parser over sample_protocols/*.md.
     Narrative-only — it does NOT populate the coded parameters, so specs from
     this path are not build-ready until an analyst supplies the coded fields.

The coded columns (dx_codes, study_start, min_age, cov/outcome codes, windows…)
are what `waves/wave3_ads_build/ads_build_core.py` reads to compose KB snippets.
All names resolve via lib/config.py.
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
import re
import sys
from datetime import datetime, date, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lib.config import cfg

try:
    from pyspark.sql import SparkSession, functions as F
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType, DateType,
        TimestampType, ArrayType,
    )
    spark = SparkSession.builder.getOrCreate()
except Exception:
    spark = None


# --- canonical protocol_spec column contract -------------------------------
# The ADS builder reads the coded columns; the app + audit read the rest.
_NARRATIVE_COLS = [
    "study_id", "title", "objective", "population", "index_event",
    "inclusion", "exclusion", "exposure", "outcomes", "covariates",
]
_CODED_COLS = [
    "version", "complexity",
    "dx_codes", "ndc_codes", "exclude_dx", "outcome_codes",
    "washout_days", "grace_days", "min_age", "max_age",
    "pre_days", "post_days", "baseline_days", "followup_days",
    "study_start", "study_end", "covariates_coded",
]
_REVIEW_COLS = [
    "extraction_model", "extraction_confidence", "review_status",
    "reviewed_by", "reviewed_ts", "esignature",
    "ingestion_ts", "source_protocol",
]
PROTOCOL_SPEC_COLS = _NARRATIVE_COLS + _CODED_COLS + _REVIEW_COLS

_ARRAY_COLS = {"dx_codes", "ndc_codes", "exclude_dx", "outcome_codes"}
_INT_COLS = {"followup_days", "washout_days", "grace_days", "min_age", "max_age",
             "pre_days", "post_days", "baseline_days"}
_DATE_COLS = {"study_start", "study_end"}
_TS_COLS = {"ingestion_ts", "reviewed_ts"}


def _spec_spark_schema() -> "StructType":
    fields = []
    for col in PROTOCOL_SPEC_COLS:
        if col in _ARRAY_COLS:
            fields.append(StructField(col, ArrayType(StringType())))
        elif col in _INT_COLS:
            fields.append(StructField(col, IntegerType()))
        elif col in _DATE_COLS:
            fields.append(StructField(col, DateType()))
        elif col in _TS_COLS:
            fields.append(StructField(col, TimestampType()))
        else:
            fields.append(StructField(col, StringType()))
    return StructType(fields)


def parse_markdown_protocol(md_text: str) -> dict:
    """Narrative-only fallback parser for bundled markdown samples.

    Extracts human-readable sections (objective, population, criteria, outcomes).
    Does NOT extract coded parameters — those come from the PDF/DOCX ai_extract
    path. Returns a dict keyed by the narrative columns only.
    """
    # Anchor to the explicit "Study ID" label (bold/colon tolerant); avoid the
    # loose `study.*id` form that also matched the "Study Identification" heading
    # and produced duplicate/garbage ids across files.
    # \b after ID prevents matching the "Study Identification" heading (no word
    # boundary between "Id" and "entification"); requires a real label + value.
    study_id_match = re.search(r"Study\s*ID\b[\s:*]*([a-z][a-z_0-9]*)", md_text, re.IGNORECASE)
    study_id = study_id_match.group(1) if study_id_match else "unknown"

    title_match = re.search(r"^#\s+(.+)$", md_text, re.MULTILINE)
    title = title_match.group(1) if title_match else "Unknown Protocol"

    objective_match = re.search(
        r"##\s+Objective\s*\n(.*?)(?=##|\Z)", md_text, re.DOTALL | re.IGNORECASE
    )
    objective = objective_match.group(1).strip()[:500] if objective_match else ""

    inclusion_match = re.search(
        r"(?:## Population|Inclusion Criteria)(.*?)(?=##|\Z)", md_text, re.DOTALL | re.IGNORECASE
    )
    inclusion_text = inclusion_match.group(1) if inclusion_match else ""
    inclusion = [s.strip() for s in re.findall(r"^-\s+(.+)$", inclusion_text, re.MULTILINE)]

    exclusion_match = re.search(
        r"Exclusion Criteria(.*?)(?=##|\Z)", md_text, re.DOTALL | re.IGNORECASE
    )
    exclusion_text = exclusion_match.group(1) if exclusion_match else ""
    exclusion = [s.strip() for s in re.findall(r"^-\s+(.+)$", exclusion_text, re.MULTILINE)]

    index_match = re.search(
        r"##\s+Index Event.*?\n(.*?)(?=##|\Z)", md_text, re.DOTALL | re.IGNORECASE
    )
    index_event = index_match.group(1).strip()[:300] if index_match else ""

    exposure_match = re.search(
        r"Exposure\s*:?\s*(.*?)(?=##|\n\n-|\Z)", md_text, re.DOTALL | re.IGNORECASE
    )
    exposure = exposure_match.group(1).strip()[:300] if exposure_match else ""

    outcomes_match = re.search(
        r"(?:## Study Outcomes|Outcomes)(.*?)(?=##|\Z)", md_text, re.DOTALL | re.IGNORECASE
    )
    outcomes_text = outcomes_match.group(1) if outcomes_match else ""
    outcomes = [s.strip() for s in re.findall(r"^-\s+(.+)$", outcomes_text, re.MULTILINE)]

    covariate_match = re.search(
        r"(?:## Covariates|Covariates.*?)\n(.*?)(?=##|\Z)", md_text, re.DOTALL | re.IGNORECASE
    )
    covariates_text = covariate_match.group(1) if covariate_match else ""
    covariates = [s.strip() for s in re.findall(r"^-\s+(.+)$", covariates_text, re.MULTILINE)]

    return {
        "study_id": study_id,
        "title": title,
        "objective": objective,
        "population": " | ".join(inclusion),
        "index_event": index_event,
        "inclusion": json.dumps(inclusion),
        "exclusion": json.dumps(exclusion),
        "exposure": exposure,
        "outcomes": json.dumps(outcomes),
        "covariates": json.dumps(covariates),
    }


def create_protocol_spec_table(c) -> None:
    """Create (or upgrade) the protocol_spec table with the full coded contract.

    Idempotent: CREATE IF NOT EXISTS for fresh installs; ALTER ... ADD COLUMNS
    IF NOT EXISTS to upgrade an existing narrative-only table in place.
    """
    if spark is None:
        return
    tbl = c.table("raw", "protocol_spec")
    print(f"[wave1] creating/upgrading protocol_spec table: {tbl}")

    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {tbl} (
  study_id         STRING NOT NULL PRIMARY KEY,
  title            STRING,
  objective        STRING,
  population       STRING,
  index_event      STRING,
  inclusion        STRING,        -- JSON array of inclusion criteria (narrative)
  exclusion        STRING,        -- JSON array of exclusion criteria (narrative)
  exposure         STRING,
  outcomes         STRING,        -- JSON array of outcome definitions (narrative)
  covariates       STRING,        -- JSON array of covariate descriptions (narrative)
  version          STRING,
  complexity       STRING,        -- low | medium | high (selects composition recipe)
  dx_codes         ARRAY<STRING>, -- qualifying diagnosis codes (cohort)
  ndc_codes        ARRAY<STRING>, -- drug codes (new-user cohort / exposure)
  exclude_dx       ARRAY<STRING>, -- exclusion diagnosis codes
  outcome_codes    ARRAY<STRING>, -- outcome event codes
  washout_days     INT,
  grace_days       INT,
  min_age          INT,
  max_age          INT,
  pre_days         INT,
  post_days        INT,
  baseline_days    INT,
  followup_days    INT,
  study_start      DATE,
  study_end        DATE,
  covariates_coded STRING,        -- JSON [{{"name":..,"codes":[..]}}] for baseline flags
  extraction_model STRING,        -- FM used by ai_extract (provenance)
  extraction_confidence STRING,   -- JSON {{field: confidence}} from ai_extract v2.1
  review_status    STRING,        -- extracted | approved | rejected
  reviewed_by      STRING,
  reviewed_ts      TIMESTAMP,
  esignature       STRING,
  ingestion_ts     TIMESTAMP,
  source_protocol  STRING         -- volume path of the uploaded PDF/DOCX or sample name
) USING DELTA
COMMENT 'Study protocol specification (narrative + coded params) consumed by the ADS Builder'"""
    )

    # Upgrade path for a pre-existing narrative-only table. Databricks has no
    # ADD COLUMNS IF NOT EXISTS, so diff against the current schema and add only
    # the missing coded/review columns.
    add_cols = {
        "version": "STRING", "complexity": "STRING",
        "dx_codes": "ARRAY<STRING>", "ndc_codes": "ARRAY<STRING>",
        "exclude_dx": "ARRAY<STRING>", "outcome_codes": "ARRAY<STRING>",
        "washout_days": "INT", "grace_days": "INT", "min_age": "INT",
        "max_age": "INT", "pre_days": "INT", "post_days": "INT",
        "baseline_days": "INT", "study_start": "DATE", "study_end": "DATE",
        "covariates_coded": "STRING", "extraction_model": "STRING",
        "extraction_confidence": "STRING", "review_status": "STRING",
        "reviewed_by": "STRING", "reviewed_ts": "TIMESTAMP", "esignature": "STRING",
    }
    existing = {r["col_name"].lower() for r in spark.sql(f"DESCRIBE TABLE {tbl}").collect()
                if r["col_name"] and not r["col_name"].startswith("#")}
    missing = {name: typ for name, typ in add_cols.items() if name.lower() not in existing}
    if missing:
        cols_spec = ", ".join(f"{name} {typ}" for name, typ in missing.items())
        print(f"[wave1] adding {len(missing)} missing columns: {list(missing)}")
        spark.sql(f"ALTER TABLE {tbl} ADD COLUMNS ({cols_spec})")


def create_protocol_extraction_table(c) -> None:
    """Create (or upgrade) the raw extraction staging table (one row per document).

    Idempotent: CREATE IF NOT EXISTS for fresh installs; ALTER ... ADD COLUMNS to
    upgrade a table that predates a later schema addition. A table created before
    ``source_text`` (and ``text_len`` / ``confidence_json``) were added keeps its
    old schema under CREATE IF NOT EXISTS, which silently disables the extraction
    eval's Stage-3 completeness judge. Databricks has no ADD COLUMNS IF NOT EXISTS,
    so diff the full column contract against the live schema and add only what's
    missing — deterministic on redeploy, independent of whether extraction re-runs.
    """
    if spark is None:
        return
    tbl = c.table("raw", "protocol_extraction")
    print(f"[wave1] creating/upgrading protocol_extraction staging table: {tbl}")
    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {tbl} (
  source_protocol  STRING,        -- volume path
  study_id         STRING,
  extracted_json   STRING,        -- ai_extract response (VARIANT serialized to JSON)
  confidence_json  STRING,        -- per-field confidence (ai_extract v2.1)
  source_text      STRING,        -- parsed protocol text (truncated) — read by the extraction eval's completeness judge
  text_len         INT,
  extraction_model STRING,
  parse_error      STRING,
  extract_error    STRING,
  ingestion_ts     TIMESTAMP
) USING DELTA
COMMENT 'Raw ai_parse_document + ai_extract output before standardization/review'"""
    )

    # Upgrade path for a pre-existing table that predates a later schema addition
    # (source_text / text_len / confidence_json). Diff the full column contract
    # against the live schema and add only the missing columns.
    add_cols = {
        "source_protocol": "STRING", "study_id": "STRING",
        "extracted_json": "STRING", "confidence_json": "STRING",
        "source_text": "STRING", "text_len": "INT",
        "extraction_model": "STRING", "parse_error": "STRING",
        "extract_error": "STRING", "ingestion_ts": "TIMESTAMP",
    }
    existing = {r["col_name"].lower() for r in spark.sql(f"DESCRIBE TABLE {tbl}").collect()
                if r["col_name"] and not r["col_name"].startswith("#")}
    missing = {name: typ for name, typ in add_cols.items() if name.lower() not in existing}
    if missing:
        cols_spec = ", ".join(f"{name} {typ}" for name, typ in missing.items())
        print(f"[wave1] adding {len(missing)} missing columns: {list(missing)}")
        spark.sql(f"ALTER TABLE {tbl} ADD COLUMNS ({cols_spec})")


def upsert_protocol_spec_rows(c, rows: list[dict]) -> int:
    """MERGE a list of protocol-spec dicts into protocol_spec by study_id.

    Missing columns default to NULL. Written via an explicit-schema DataFrame +
    MERGE (never string-built INSERT) so quotes/JSON cannot corrupt a row.
    Existing review_status is preserved on UPDATE only when the incoming row
    omits it (COALESCE), so re-ingesting an extraction never silently
    un-approves a spec an analyst already signed.
    """
    if spark is None or not rows:
        return 0
    tbl = c.table("raw", "protocol_spec")
    now = datetime.now(timezone.utc)

    # Dedupe by study_id (keep last) and drop rows with no id, so the MERGE can
    # never match one target row from multiple source rows.
    by_id = {}
    for r in rows:
        sid = r.get("study_id")
        if sid:
            by_id[sid] = r
    if not by_id:
        return 0

    norm = []
    for r in by_id.values():
        row = {col: r.get(col) for col in PROTOCOL_SPEC_COLS}
        if row.get("ingestion_ts") is None:
            row["ingestion_ts"] = now
        if row.get("review_status") is None:
            row["review_status"] = "extracted"
        # coerce date strings -> date
        for dc in _DATE_COLS:
            v = row.get(dc)
            if isinstance(v, str) and len(v) == 10:
                try:
                    row[dc] = date.fromisoformat(v)
                except ValueError:
                    row[dc] = None
        norm.append(tuple(row[col] for col in PROTOCOL_SPEC_COLS))

    df = spark.createDataFrame(norm, _spec_spark_schema())
    df.createOrReplaceTempView("_protocol_spec_src")

    # Preserve existing values when the incoming row leaves a column NULL, so
    # the extraction path never erases markdown narrative and re-runs are safe.
    # Corrections pass a full row (all columns populated), so they still apply.
    def _set_expr(col: str) -> str:
        if col == "review_status":
            # never let a re-extraction ('extracted') silently un-approve a spec
            return (f"t.review_status = CASE WHEN t.review_status = 'approved' "
                    f"THEN t.review_status ELSE COALESCE(src.review_status, t.review_status) END")
        return f"t.{col} = COALESCE(src.{col}, t.{col})"

    set_clause = ",\n  ".join(_set_expr(col) for col in PROTOCOL_SPEC_COLS if col != "study_id")
    spark.sql(
        f"""MERGE INTO {tbl} t
USING _protocol_spec_src src
ON t.study_id = src.study_id
WHEN MATCHED THEN UPDATE SET
  {set_clause}
WHEN NOT MATCHED THEN INSERT *"""
    )
    return len(norm)


def ingest_bundled_protocols(c) -> None:
    """Narrative-only fallback: parse bundled sample markdown into protocol_spec.

    Coded parameters are left NULL — these seed rows are NOT build-ready. The
    PDF/DOCX ai_extract path is the source of the coded spec.
    """
    if spark is None:
        return
    sample_dir = Path(__file__).parent / "sample_protocols"
    if not sample_dir.exists():
        print("[wave1] sample_protocols directory not found; skipping bundled ingestion")
        return

    print("[wave1] ingesting bundled sample protocols (narrative-only fallback)")
    rows = []
    for md_file in sorted(sample_dir.glob("*.md")):
        print(f"  parsing {md_file.name}")
        parsed = parse_markdown_protocol(md_file.read_text())
        parsed["source_protocol"] = md_file.name
        parsed["review_status"] = "extracted"
        rows.append(parsed)

    n = upsert_protocol_spec_rows(c, rows)
    print(f"[wave1] ingested {n} sample protocols (narrative-only)")


def main():
    c = cfg()
    print(f"=== Wave 1 protocol ingestion for {c.initiative} @ {c.catalog} (compute={c.compute}) ===")
    assert c.compute == "serverless", "Golden rule: serverless only"

    create_protocol_spec_table(c)
    create_protocol_extraction_table(c)
    ingest_bundled_protocols(c)

    print("[wave1] protocol ingestion done.")


if __name__ == "__main__":
    main()
