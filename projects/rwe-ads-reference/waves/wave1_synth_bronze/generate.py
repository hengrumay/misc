"""Wave 1 — Synthetic Bronze Data Generation (serverless). Idempotent.

Job entrypoint: orchestrates synthetic RWD generation and writes to bronze schema.
Runs as a serverless job task or locally via Databricks Connect.

Output: bronze tables with deterministic synthetic data.
  - patient, enrollment_span, medical_claim, pharmacy_claim, provider, lab_result, encounter
"""
from __future__ import annotations

# Serverless spark_python_task runs this file via exec() with no __file__ in
# globals; recover it from the frame so downstream Path(__file__) works.
try:
    __file__
except NameError:  # pragma: no cover
    import inspect as _inspect
    __file__ = _inspect.getfile(_inspect.currentframe())

import sys
from pathlib import Path

# Make lib importable whether run from repo root, workspace, or job.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lib.config import cfg
from lib.synth import generate_all

try:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
except Exception:
    spark = None


def main():
    c = cfg()
    print(f"=== Wave 1 synthetic bronze for {c.initiative} @ {c.catalog} (compute={c.compute}) ===")
    assert c.compute == "serverless", "Golden rule: serverless only"

    dfs = generate_all(spark=spark)

    print("[wave1] generated synthetic RWD:")
    for table_name, df in dfs.items():
        print(f"  {table_name}: {len(df)} rows")

    print("[wave1] done.")


if __name__ == "__main__":
    main()
