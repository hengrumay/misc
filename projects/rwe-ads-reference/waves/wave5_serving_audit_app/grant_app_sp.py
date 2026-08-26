"""Wave 5 — app-SP Lakebase grant NOTE (no-op). No in-kernel Postgres SDK calls.

Granting the app service principal live Lakebase access is done CONTROL-PLANE by
`scripts/setup_synced_tables.sh` (via `databricks psql`), NOT from this serverless
job task:
  * serving DB (cfg().lakebase_serving_db): SELECT on public
  * app-state DB (cfg().lakebase_app_db): SELECT/INSERT/UPDATE/DELETE + CREATE on public

WHY: an in-kernel Postgres SDK call crashes the serverless job kernel (native,
verified). The grant needs the app SP (created by `bundle deploy`) and runs
reliably from the control plane, so it lives in the setup script.
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
    print(f"=== Wave 5 app-SP Lakebase grant is CONTROL-PLANE (no-op here) ===")
    print(f"[grant] app SP grants on {c.lakebase_serving_db} + {c.lakebase_app_db} "
          f"(project {c.lakebase_project}) are applied by")
    print(f"[grant]   scripts/setup_synced_tables.sh <PROFILE>   (via `databricks psql`).")
    print(f"[grant] This serverless task makes ZERO in-kernel Postgres SDK calls by design.")


if __name__ == "__main__":
    main()
