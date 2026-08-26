"""Wave 5 — Lakebase serving NOTE (no-op). No in-kernel Postgres SDK calls.

Lakebase serving is provisioned CONTROL-PLANE by `scripts/setup_synced_tables.sh`
(run after `bundle deploy` + after the gold ADS is built), NOT from this
serverless job task:
  * `databricks postgres create-catalog` registers the serving DB in UC
    (done in scripts/setup_lakebase.sh, pre-deploy),
  * `databricks postgres create-synced-table` creates the one-way Delta->Postgres
    synced tables (ads_output, cohort_summary), and
  * `databricks psql` creates the app-state tables + grants the app SP.

WHY no work here: any in-kernel Postgres SDK call inside a serverless job-task
kernel hard-crashes the kernel (native, verified). The `databricks postgres` /
`databricks psql` CLI runs the same operations reliably from the control plane.
Wave 5's real serverless work is the hash-chained audit schema (setup_audit.py).
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

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from lib.config import cfg  # noqa: E402


def main() -> None:
    c = cfg()
    print(f"=== Wave 5 Lakebase serving is CONTROL-PLANE (no-op here) ===")
    print(f"[wave5] serving sync + app-state DDL + app-SP grant for project "
          f"{c.lakebase_project} are provisioned by")
    print(f"[wave5]   scripts/setup_synced_tables.sh <PROFILE>   "
          f"(run after `bundle deploy` + after gold is built).")
    print(f"[wave5] This serverless task makes ZERO in-kernel Postgres SDK calls by design.")


if __name__ == "__main__":
    main()
