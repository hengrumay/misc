"""Wave 0 — Foundation (serverless). Idempotent.

Creates the five ads_* schemas, the protocols volume, the approved-SQL KB table
(seeded), and the gateway inference table placeholder. UC groups + Lakebase DBs
are provisioned by the companion scripts (provision_lakebase.py) and via the
gateway/AI Gateway config, because those use control-plane APIs rather than SQL.

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
from pathlib import Path

# Make lib importable whether run from repo root, workspace, or job.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from lib.config import cfg  # noqa: E402

try:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
except Exception:  # pragma: no cover
    spark = None

from waves.wave0_foundation.kb_seeds import SEED_SNIPPETS  # noqa: E402


def _sql(stmt: str):
    print("  SQL>", stmt.split("\n")[0][:110])
    if spark is not None:
        spark.sql(stmt)


def create_schemas(c):
    print("[wave0] schemas")
    for key in c.all_schema_keys():
        _sql(
            f"CREATE SCHEMA IF NOT EXISTS {c.schema(key)} "
            f"COMMENT 'RWE ADS {key} layer — {c.initiative}'"
        )


def create_volume(c):
    print("[wave0] protocols volume")
    _sql(f"CREATE VOLUME IF NOT EXISTS {c.protocols_volume} COMMENT 'Uploaded study protocol PDFs/DOCX'")


def create_kb(c):
    print("[wave0] approved-SQL KB table")
    _sql(
        f"""CREATE TABLE IF NOT EXISTS {c.kb_table} (
  snippet_id    STRING NOT NULL,
  category      STRING,          -- cohort|inclusion|exclusion|derivation|outcome
  description   STRING,
  sql_template  STRING,
  params_json   STRING,
  status        STRING,          -- draft|approved
  version       INT,
  approved_by   STRING,
  approved_ts   TIMESTAMP,
  content_hash  STRING
) USING DELTA
TBLPROPERTIES (delta.enableChangeDataFeed = true)
COMMENT 'Governed, versioned approved-SQL snippet library for ADS composition'"""
    )
    # Idempotent seed via a DataFrame + MERGE (avoids SQL quote-escaping, which
    # can silently corrupt text — templates contain many single quotes/newlines).
    if spark is None:
        return
    from pyspark.sql import functions as F
    rows = []
    for s in SEED_SNIPPETS:
        h = hashlib.sha256((s["snippet_id"] + s["sql_template"]).encode()).hexdigest()[:16]
        rows.append((
            s["snippet_id"], s["category"], s["description"], s["sql_template"],
            s["params_json"], "approved", 1, "seed@epi-rwds", h,
        ))
    cols = ["snippet_id", "category", "description", "sql_template",
            "params_json", "status", "version", "approved_by", "content_hash"]
    df = spark.createDataFrame(rows, cols).withColumn("approved_ts", F.current_timestamp())
    df.createOrReplaceTempView("_kb_seed")
    _sql(
        f"""MERGE INTO {c.kb_table} t
USING _kb_seed src ON t.snippet_id = src.snippet_id
WHEN NOT MATCHED THEN INSERT *"""
    )


def create_inference_table(c):
    """Placeholder for the gateway inference table (real one written by the
    AI Gateway once configured). Create an empty typed table so audit joins work."""
    print("[wave0] gateway inference table placeholder")
    _sql(
        f"""CREATE TABLE IF NOT EXISTS {c.inference_table} (
  request_ts    TIMESTAMP,
  endpoint      STRING,
  model         STRING,
  input_masked  STRING,
  output        STRING,
  tokens_in     INT,
  tokens_out    INT,
  cost_usd      DOUBLE,
  initiative    STRING,
  team          STRING
) USING DELTA
COMMENT 'Unity AI Gateway inference log (PHI-masked). Populated by gateway when enabled.'"""
    )


def main():
    c = cfg()
    print(f"=== Wave 0 foundation for {c.initiative} @ {c.catalog} (compute={c.compute}) ===")
    assert c.compute == "serverless", "Golden rule: serverless only"
    create_schemas(c)
    create_volume(c)
    create_kb(c)
    create_inference_table(c)
    print("[wave0] done. Lakebase DBs + gateway configured by companion scripts.")


if __name__ == "__main__":
    main()
