#!/usr/bin/env python3
"""Acceptance gate: prove MLflow tracing + tracking PERSISTED for RWE-ADS.

Run this AFTER re-running the wave-4 job. It resolves the SAME experiment the
bundle logs to (``lib.pipeline.gateway.experiment_name`` —
``/Users/<identity>/<initiative>``, under the current identity's home) and
asserts persistence against the live Databricks tracking server:

  * ``mlflow.search_traces``  -> trace count  (the gateway LLM spans)
  * ``mlflow.search_runs``    -> run count + metric keys  (the wave-4 run)

Exit code is the gate: **NON-ZERO if zero traces persisted**, 0 otherwise.

Usage:
  python3 scripts/verify_tracing.py --profile <your-profile>
  python3 scripts/verify_tracing.py --profile <your-profile> \
      --experiment "/Users/you@example.com/rwe-ads-automation"

``--experiment`` overrides resolution (an experiment NAME, or a numeric id) —
use it if the job ran under a DIFFERENT identity than this profile (e.g. a
service principal), so the gate points at the experiment the job actually wrote.
The bundle runs jobs as ``run_as: current_user``, so with the same profile the
default resolution already matches; the override is the escape hatch.

No workspace state is mutated — this is read-only (search only).
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# Repo root on sys.path so we can import the SAME experiment resolver the bundle
# uses (no drift between what wave-4 writes and what this gate reads).
_REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO_ROOT))


def _resolve_experiment(mlflow, args):
    """Return (experiment_id, experiment_name) or (None, name) if it doesn't exist."""
    if args.experiment and args.experiment.isdigit():
        exp = mlflow.get_experiment(args.experiment)  # explicit numeric id
        return (exp.experiment_id, exp.name) if exp else (None, args.experiment)

    if args.experiment:
        name = args.experiment
    else:
        # Default: resolve the SAME path the bundle uses, under the profile's identity.
        from databricks.sdk import WorkspaceClient
        from lib.pipeline.gateway import experiment_name
        w = WorkspaceClient(profile=args.profile) if args.profile else WorkspaceClient()
        name = experiment_name(w)

    exp = mlflow.get_experiment_by_name(name)
    return (exp.experiment_id, name) if exp else (None, name)


def main() -> int:
    ap = argparse.ArgumentParser(description="Verify MLflow traces + runs persisted for RWE-ADS.")
    ap.add_argument("--profile", help="Databricks CLI profile (.databrickscfg) the job ran under.")
    ap.add_argument("--experiment", help="Override: experiment NAME or numeric id to check.")
    args = ap.parse_args()

    try:
        import mlflow
    except Exception as e:  # noqa: BLE001
        print(f"[verify] mlflow is required locally to run this gate: {e!r}", file=sys.stderr)
        print("[verify] install with: pip install 'mlflow>=3.1'", file=sys.stderr)
        return 2

    # Point mlflow at the same workspace/profile. `databricks://<profile>` selects
    # a profile from .databrickscfg; bare `databricks` uses ambient auth.
    mlflow.set_tracking_uri(f"databricks://{args.profile}" if args.profile else "databricks")

    try:
        exp_id, exp_name = _resolve_experiment(mlflow, args)
    except Exception as e:  # noqa: BLE001
        print(f"[verify] could not resolve experiment: {e!r}", file=sys.stderr)
        return 2

    print(f"[verify] experiment name : {exp_name}")
    if exp_id is None:
        print("[verify] experiment DOES NOT EXIST → 0 traces persisted. "
              "Tracing did not persist — re-check the job's [mlflow] log lines.", file=sys.stderr)
        return 1
    print(f"[verify] experiment id   : {exp_id}")

    # --- traces (gateway LLM spans) -----------------------------------------
    traces = mlflow.search_traces(experiment_ids=[exp_id])
    trace_count = len(traces)
    print(f"[verify] traces          : {trace_count}")
    if trace_count:
        try:  # confirm the first trace is a real span tree, not an empty shell
            first_id = traces.iloc[0]["trace_id"]
            spans = mlflow.get_trace(first_id).data.spans
            names = ", ".join(sorted({s.name for s in spans})) or "(none)"
            print(f"[verify] first-trace spans: {len(spans)} [{names}]")
        except Exception as e:  # noqa: BLE001 - span drill-down is informational only
            print(f"[verify] (span drill-down skipped: {e!r})")

    # --- runs (wave-4 params + metrics) -------------------------------------
    try:
        runs = mlflow.search_runs(experiment_ids=[exp_id])
        run_count = len(runs)
        metric_keys = sorted(c[len("metrics."):] for c in runs.columns if c.startswith("metrics."))
        print(f"[verify] runs            : {run_count}")
        print(f"[verify] metric keys     : {metric_keys if metric_keys else '(none)'}")
    except Exception as e:  # noqa: BLE001 - runs are secondary; traces are the hard gate
        print(f"[verify] (run search skipped: {e!r})")

    # The gate: zero traces means tracing is non-functional regardless of runs.
    if trace_count == 0:
        print("[verify] FAIL — zero traces persisted.", file=sys.stderr)
        return 1
    print("[verify] PASS — tracing + tracking persisted.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
