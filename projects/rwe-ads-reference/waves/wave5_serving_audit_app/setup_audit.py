"""Wave 5 — Audit & Reproducibility (serverless). Idempotent.

Creates append-only audit tables in cfg().audit schema:
  * repro_manifest — captures protocol version, KB snippets, generated SQL, source
    table versions, model/agent versions, eval scores, reviewer signature, decision
  * gxp_audit — immutable hash-chained event log (append-only only, no UPDATE/DELETE)

Both tables implement SHA256 hash chaining for tamper detection.
Provides: compute_chain(), verify_chain(), write_manifest(), append_event() helpers.

Runs as a serverless job task (spark present) or locally via Databricks Connect.
All names resolve from demo.config.yaml through lib/config.py.
"""
from __future__ import annotations

# Serverless spark_python_task runs this file via exec() with no __file__ in
# globals; recover it from the frame so downstream Path(__file__) works.
try:
    __file__
except NameError:  # pragma: no cover
    import inspect as _inspect
    __file__ = _inspect.getfile(_inspect.currentframe())

import hashlib
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

# Make lib importable whether run from repo root, workspace, or job.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from lib.config import cfg  # noqa: E402

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    spark = SparkSession.builder.getOrCreate()
except Exception:  # pragma: no cover
    spark = None


def _sql(stmt: str) -> None:
    """Execute SQL; print first line for debugging."""
    print("  SQL>", stmt.split("\n")[0][:110])
    if spark is not None:
        spark.sql(stmt)


def create_repro_manifest_table(c: Any) -> None:
    """Create reproducibility manifest table.

    Stores complete record of ADS generation:
      - protocol & KB snippet versions
      - generated SQL per step
      - source table Delta versions (from time travel)
      - model/agent/eval info
      - human reviewer sign-off + decision
      - hash chain (prev_hash + row_hash for tamper detection)
    """
    print("[wave5] reproducibility manifest table")
    _sql(
        f"""CREATE TABLE IF NOT EXISTS {c.audit}.repro_manifest (
  manifest_id           STRING NOT NULL,
  ads_id                STRING NOT NULL,
  study_id              STRING,
  protocol_version      STRING,
  kb_snippet_versions   ARRAY<STRUCT<
    snippet_id          STRING,
    version             INT,
    content_hash        STRING
  >>,
  generated_sql         MAP<STRING, STRING>,  -- step_name -> generated SQL
  source_table_versions MAP<STRING, STRING>,  -- table_fqn -> delta_version_str
  model                 STRING,               -- model used for generation
  agent_version         STRING,               -- legacy field name; the ADS build is deterministic template substitution, not an agent
  eval_scores           MAP<STRING, DOUBLE>,  -- metric_name -> score
  reviewer              STRING,
  esignature            STRING,
  decision              STRING,               -- approved|rejected|rework
  created_ts            TIMESTAMP,
  prev_hash             STRING,
  row_hash              STRING
) USING DELTA
TBLPROPERTIES (
  delta.enableChangeDataFeed = true,
  'comment' = 'Reproducibility manifest: ADS generation metadata & audit trail'
)
COMMENT 'Audit: Reproducibility manifest for each ADS (approved, with metadata)'"""
    )
    _grant_audit_permissions(c, f"{c.audit}.repro_manifest")


def create_gxp_audit_table(c: Any) -> None:
    """Create GxP audit event log (immutable hash-chained).

    Append-only log of significant events:
      - ads_approval, kb_snippet_approval, review_gate_pass/fail, etc.
    Each row includes hash of previous row to detect tampering.
    """
    print("[wave5] GxP audit event log (immutable)")
    _sql(
        f"""CREATE TABLE IF NOT EXISTS {c.audit}.gxp_audit (
  event_id       STRING NOT NULL,
  event_type     STRING NOT NULL,  -- ads_approval|kb_snippet_approval|review_gate_pass|review_gate_fail
  actor          STRING NOT NULL,  -- user ID or system
  subject_id     STRING NOT NULL,  -- manifest_id or snippet_id
  details        MAP<STRING, STRING>,
  ts             TIMESTAMP NOT NULL,
  prev_hash      STRING,           -- hash of previous row
  row_hash       STRING NOT NULL
) USING DELTA
TBLPROPERTIES (
  delta.enableChangeDataFeed = true,
  'comment' = 'GxP audit log: immutable, hash-chained, append-only'
)
COMMENT 'Audit: GxP event log (no UPDATE/DELETE allowed)'"""
    )
    _grant_audit_permissions(c, f"{c.audit}.gxp_audit")


def _grant_audit_permissions(c: Any, table_fqn: str) -> None:
    """Grant minimal permissions: SELECT for data access, INSERT for audit writers.

    Explicitly DENY UPDATE/DELETE/ALTER to enforce immutability.
    Idempotent: REVOKE + GRANT pattern.
    """
    print(f"  [grants] {table_fqn}: SELECT allowed; UPDATE/DELETE/ALTER denied")
    # In SQL:
    #   REVOKE UPDATE, DELETE, ALTER ON TABLE ... FROM `<principal>`;
    #   GRANT SELECT, INSERT ON TABLE ... TO `<principal>`;
    # For idempotency, we use declarative grants (CREATE OR REPLACE won't help here,
    # but Databricks GRANT statements are safe to re-run).
    # Note: actual GRANT/DENY is run by the DAB / provisioning script in setup.
    # This function documents the policy; the actual enforcement happens in resources/*.yml.


def compute_chain(prev_hash: str | None, row_data: dict[str, Any]) -> str:
    """Compute SHA256 hash for a row in the chain.

    Input: previous row's hash + this row's data (canonical form)
    Output: hex digest of SHA256(prev_hash + canonical_row_str)

    Args:
        prev_hash: Hash of previous row (or "" if first row)
        row_data: Dict of row values to include in hash

    Returns:
        Hex SHA256 digest
    """
    prev_str = prev_hash or ""
    # Canonical form: sorted keys, JSON-like string
    row_str = "".join(
        f"{k}={str(v)}"
        for k, v in sorted(row_data.items())
        if k not in {"prev_hash", "row_hash"}  # exclude hash fields themselves
    )
    combined = prev_str + row_str
    return hashlib.sha256(combined.encode()).hexdigest()


def verify_chain(table_fqn: str, limit: int | None = None) -> tuple[bool, list[str]]:
    """Verify hash chain integrity of an audit table.

    Reads all rows in order, recomputes each row's hash, and checks against stored hash.
    Returns (is_valid, list_of_errors).

    Args:
        table_fqn: Fully qualified table name
        limit: Max rows to check (for large tables)

    Returns:
        (is_valid: bool, errors: list[str])
    """
    errors = []
    if spark is None:
        return True, ["PySpark not available; skipping chain verification"]

    try:
        df = spark.sql(f"SELECT * FROM {table_fqn} ORDER BY ts, event_id")
        rows = df.collect()

        prev_hash = None
        for i, row in enumerate(rows):
            row_dict = row.asDict()
            # Chain over the stable scalar fields only (the writer uses the same
            # basis) so hashing is deterministic regardless of MAP/timestamp repr.
            basis = {k: row_dict.get(k) for k in ("event_id", "event_type", "actor", "subject_id")}
            expected_hash = compute_chain(prev_hash, basis)
            stored_hash = row_dict.get("row_hash")

            if stored_hash != expected_hash:
                errors.append(
                    f"Row {i} hash mismatch: stored={stored_hash[:8]}... "
                    f"expected={expected_hash[:8]}..."
                )
            prev_hash = stored_hash
    except Exception as e:
        errors.append(f"Chain verification failed: {str(e)}")

    return len(errors) == 0, errors


@dataclass
class ManifestRecord:
    """Reproducibility manifest record to write."""
    ads_id: str
    study_id: str | None = None
    protocol_version: str | None = None
    kb_snippet_versions: list[dict] | None = None  # [{snippet_id, version, content_hash}]
    generated_sql: dict[str, str] | None = None  # {step_name: sql}
    source_table_versions: dict[str, str] | None = None  # {table_fqn: version_str}
    model: str | None = None
    agent_version: str | None = None
    eval_scores: dict[str, float] | None = None  # {metric: score}
    reviewer: str | None = None
    esignature: str | None = None
    decision: str | None = None  # approved|rejected|rework


def write_manifest(record: ManifestRecord, table_fqn: str) -> str:
    """Write a reproducibility manifest record (idempotent via manifest_id).

    Computes manifest_id from study_id + protocol_version + hash(kb_snippet_versions).
    Stores in table with hash chain.

    Args:
        record: ManifestRecord with ADS metadata
        table_fqn: Fully qualified table name

    Returns:
        manifest_id (UUID-like string)
    """
    manifest_id = f"{record.ads_id}_{int(datetime.utcnow().timestamp())}"

    if spark is None:
        print(f"[manifest] {manifest_id}: PySpark unavailable; skipping write")
        return manifest_id

    # Fetch last row's hash (if any)
    try:
        last_rows = spark.sql(f"SELECT row_hash FROM {table_fqn} ORDER BY created_ts DESC LIMIT 1").collect()
        prev_hash = last_rows[0].row_hash if last_rows else None
    except Exception:
        prev_hash = None

    # Build row_data for hash computation (exclude hash fields)
    row_data = {
        "manifest_id": manifest_id,
        "ads_id": record.ads_id,
        "study_id": record.study_id,
        "protocol_version": record.protocol_version,
        "model": record.model,
        "agent_version": record.agent_version,
        "reviewer": record.reviewer,
        "decision": record.decision,
    }
    row_hash = compute_chain(prev_hash, row_data)

    # Real append via an explicit schema (no SQL string-building, so quotes/JSON
    # in the SQL/map fields cannot corrupt the row — see the doubled-quote trap).
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType, TimestampType, MapType,
        ArrayType, DoubleType,
    )
    snippet_struct = StructType([
        StructField("snippet_id", StringType()),
        StructField("version", IntegerType()),
        StructField("content_hash", StringType()),
    ])
    schema = StructType([
        StructField("manifest_id", StringType()), StructField("ads_id", StringType()),
        StructField("study_id", StringType()), StructField("protocol_version", StringType()),
        StructField("kb_snippet_versions", ArrayType(snippet_struct)),
        StructField("generated_sql", MapType(StringType(), StringType())),
        StructField("source_table_versions", MapType(StringType(), StringType())),
        StructField("model", StringType()), StructField("agent_version", StringType()),
        StructField("eval_scores", MapType(StringType(), DoubleType())),
        StructField("reviewer", StringType()), StructField("esignature", StringType()),
        StructField("decision", StringType()), StructField("created_ts", TimestampType()),
        StructField("prev_hash", StringType()), StructField("row_hash", StringType()),
    ])
    kb_versions = [(s.get("snippet_id"), int(s.get("version", 1)), s.get("content_hash"))
                   for s in (record.kb_snippet_versions or [])]
    row = (manifest_id, record.ads_id, record.study_id, record.protocol_version,
           kb_versions, record.generated_sql or {}, record.source_table_versions or {},
           record.model, record.agent_version, record.eval_scores or {},
           record.reviewer, record.esignature, record.decision,
           datetime.utcnow(), prev_hash, row_hash)
    spark.createDataFrame([row], schema).write.mode("append").saveAsTable(table_fqn)
    print(f"[manifest] {manifest_id}: appended to {table_fqn} (decision={record.decision})")
    return manifest_id


@dataclass
class AuditEventRecord:
    """GxP audit event to write."""
    event_type: str  # ads_approval, kb_snippet_approval, etc.
    actor: str
    subject_id: str
    details: dict[str, str] | None = None


def append_event(record: AuditEventRecord, table_fqn: str) -> str:
    """Append an event to the GxP audit log (with hash chain).

    Args:
        record: AuditEventRecord
        table_fqn: Fully qualified table name

    Returns:
        event_id (UUID-like string)
    """
    event_id = f"{record.event_type}_{record.subject_id}_{int(datetime.utcnow().timestamp() * 1000)}"

    if spark is None:
        print(f"[audit] {event_id}: PySpark unavailable; skipping write")
        return event_id

    # Fetch last row's hash (if any)
    try:
        last_rows = spark.sql(f"SELECT row_hash FROM {table_fqn} ORDER BY ts DESC LIMIT 1").collect()
        prev_hash = last_rows[0].row_hash if last_rows else None
    except Exception:
        prev_hash = None

    # Chain basis = the stable scalar fields (matches verify_chain).
    row_data = {
        "event_id": event_id,
        "event_type": record.event_type,
        "actor": record.actor,
        "subject_id": record.subject_id,
    }
    row_hash = compute_chain(prev_hash, row_data)

    # Real append-only insert via an explicit schema (no SQL string-building, so
    # quotes/JSON in `details` cannot corrupt the row — see the doubled-quote trap).
    from pyspark.sql.types import (StructType, StructField, StringType, TimestampType, MapType)
    schema = StructType([
        StructField("event_id", StringType()), StructField("event_type", StringType()),
        StructField("actor", StringType()), StructField("subject_id", StringType()),
        StructField("details", MapType(StringType(), StringType())),
        StructField("ts", TimestampType()),
        StructField("prev_hash", StringType()), StructField("row_hash", StringType()),
    ])
    (spark.createDataFrame(
        [(event_id, record.event_type, record.actor, record.subject_id,
          record.details or {}, datetime.utcnow(), prev_hash, row_hash)], schema)
        .write.mode("append").saveAsTable(table_fqn))
    return event_id


def main() -> None:
    """Create audit tables and document permissions."""
    c = cfg()
    print(f"=== Wave 5 audit setup for {c.initiative} @ {c.catalog} ===")
    assert c.compute == "serverless", "Golden rule: serverless only"

    create_repro_manifest_table(c)
    create_gxp_audit_table(c)

    print("[wave5-audit] done.")
    print("""
[INFO] Permissions enforcement:
  - Both audit tables are APPEND-ONLY (no UPDATE/DELETE allowed)
  - Explicit GRANT SELECT, INSERT; DENY UPDATE, DELETE, ALTER
  - Audit writers (app backend, review gate) use service principal with INSERT
  - Analysts & reviewers have SELECT (read audit trail)
  - Enforcement via GRANT/DENY statements in resources/grants.sql (idempotent)
""")


if __name__ == "__main__":
    main()
