"""Wave 0 — Lakebase provisioning NOTE (no-op). No in-kernel Postgres SDK calls.

Lakebase INFRASTRUCTURE (Autoscaling project, serving + app-state Postgres DBs,
UC catalog) is provisioned CONTROL-PLANE by `scripts/setup_lakebase.sh` BEFORE
`bundle deploy` — NOT from this serverless job task.

WHY: any in-kernel Postgres SDK call (the Autoscaling module on WorkspaceClient)
executed inside a serverless job-task kernel hard-crashes the kernel ("Python
process exited unexpectedly", native, before psycopg). The same operations
succeed from the control plane via the `databricks postgres` CLI. So this task
does no Lakebase work; it only prints what was (or must be) done control-plane.
Wave 0's real work is uc_foundation (schemas/volume/KB) + gateway.
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


def provision(c=None):
    c = c or cfg()
    print(f"=== Wave 0 Lakebase provisioning is CONTROL-PLANE (no-op here) ===")
    print(f"[lakebase] project {c.lakebase_project}: databases {c.lakebase_serving_db} + "
          f"{c.lakebase_app_db} and UC catalog {c.lakebase_catalog} are provisioned by")
    print(f"[lakebase]   scripts/setup_lakebase.sh <PROFILE>   (run BEFORE `bundle deploy`).")
    print(f"[lakebase] This serverless task makes ZERO in-kernel Postgres SDK calls "
          f"by design (they crash the serverless kernel).")


if __name__ == "__main__":
    provision()
