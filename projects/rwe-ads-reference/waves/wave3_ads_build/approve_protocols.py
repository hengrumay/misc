"""Protocol-spec approval task — human gate by DEFAULT; unattended auto-e-sign is opt-in.

The mandatory human review gate lives in the app (review_gate.approve_protocol,
driven by an analyst). By DEFAULT this task does NOT e-sign anything: it leaves
every eval-passing spec at ``review_status='extracted'`` for analyst review +
e-sign in the app (the GxP control). That is the correct behavior for any real /
regulated run.

OPT-IN (demo only): to reproduce the full pipeline end-to-end without a human —
e.g. an unattended ``bundle run`` — enable ``allow_auto_esign`` (config key
``allow_auto_esign: true`` and/or the ``--allow-auto-esign true`` CLI arg; the CLI
arg wins, an empty arg defers to config). When enabled, eval-passing specs are
auto-approved by ``_auto_approve`` under a SYSTEM actor + a NON-human e-signature
literal, emitting a DISTINCT ``protocol_auto_approved`` audit event — never the
human ``protocol_approval`` shape — so the hash-chained trail can never conflate an
unattended reproducibility signature with a real 21 CFR Part 11 human signature.

QUALITY GATE (charter rules #5/#10/#11): approval (human OR auto) only ever applies
to specs that passed Stage-1 eval. This task reads the verdict from
``raw.protocol_eval`` (written by ``run_protocol_eval.py``, which runs BEFORE this
task) and BLOCKS any spec where ``eval_ok != True`` — a deterministically invalid
spec is never approved, and the block is recorded as a ``protocol_eval_blocked``
audit event with its reasons.

CLI: python approve_protocols.py [--studies poc_low,poc_med,poc_high] [--allow-auto-esign true|false]
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
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lib.config import cfg

try:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
except Exception:
    spark = None


def _load_eval_gate(c) -> dict:
    """Read the Stage-1 verdict per study from raw.protocol_eval.

    Returns {study_id: {eval_ok, n_hard_fails, hard_fail_reasons, review_priority}}.
    A missing table/row means the eval did not run for that study — the caller
    treats that as NOT-approvable (fail closed), never as an implicit pass.
    """
    gate: dict = {}
    tbl = c.table("raw", "protocol_eval")
    try:
        rows = spark.sql(
            f"SELECT study_id, eval_ok, n_hard_fails, hard_fail_reasons, review_priority "
            f"FROM {tbl}").collect()
    except Exception as e:  # noqa: BLE001 - table absent => nobody passes the gate
        print(f"[approve] protocol_eval not readable ({e}); no spec will be auto-approved.")
        return gate
    for r in rows:
        d = r.asDict()
        try:
            reasons = json.loads(d.get("hard_fail_reasons") or "[]")
        except (TypeError, json.JSONDecodeError):
            reasons = []
        gate[d["study_id"]] = {"eval_ok": bool(d.get("eval_ok")),
                               "n_hard_fails": d.get("n_hard_fails"),
                               "hard_fail_reasons": reasons,
                               "review_priority": d.get("review_priority")}
    return gate


def _audit_block(c, study_id: str, reasons: list) -> str | None:
    """Record a blocked auto-approval as a hash-chained audit event."""
    try:
        from waves.wave5_serving_audit_app.setup_audit import append_event, AuditEventRecord
        rec = AuditEventRecord(
            event_type="protocol_eval_blocked", actor="system@rwe-ads", subject_id=study_id,
            details={"reason": "Stage-1 eval_ok=False (or eval missing)",
                     "hard_fail_reasons": "; ".join(str(x) for x in (reasons or []))[:900]})
        return append_event(rec, c.table("audit", "gxp_audit"))
    except Exception as e:  # noqa: BLE001 - audit write is non-fatal to the block itself
        print(f"[approve] block-audit write skipped for {study_id}: {e}")
        return None


# Non-human e-signature literal for the opt-in unattended path. Deliberately loud
# so no downstream reader mistakes it for a real 21 CFR Part 11 human signature.
_AUTO_ESIGN = "SYSTEM-AUTO-ESIGN (reproducibility — NOT a human 21 CFR Part 11 signature)"


def _resolve_auto(cli_val: str, c) -> bool:
    """Effective auto-e-sign switch: CLI arg wins; empty/unrecognized defers to config.

    The bundle passes ``--allow-auto-esign ${var.allow_auto_esign}`` (empty by
    default), so an unset bundle var falls through to ``demo.config.yaml``'s
    ``allow_auto_esign`` (which config.py treats as False when absent).
    """
    v = (cli_val or "").strip().lower()
    if v in ("1", "true", "yes", "on"):
        return True
    if v in ("0", "false", "no", "off"):
        return False
    return c.allow_auto_esign


def _auto_approve(c, study_id: str, g: dict) -> dict:
    """DEMO-ONLY unattended e-sign of an eval-passing spec (opt-in path).

    Deliberately does NOT route through review_gate.approve_protocol (the human
    analyst gate, which emits a ``protocol_approval`` event). Instead it sets
    ``review_status='approved'`` under a SYSTEM actor + a NON-human e-signature
    literal and emits a DISTINCT ``protocol_auto_approved`` audit event, so the
    hash-chained trail never conflates this reproducibility signature with a real
    21 CFR Part 11 human signature. Only reached when ``allow_auto_esign`` is enabled.
    """
    tbl = c.table("raw", "protocol_spec")
    rows = spark.sql(f"SELECT * FROM {tbl} WHERE study_id = '{study_id}' LIMIT 1").collect()
    if not rows:
        return {"status": "error", "message": f"no protocol_spec for study_id='{study_id}'"}
    row = rows[0].asDict()
    row["review_status"] = "approved"
    row["reviewed_by"] = _AUTO_ESIGN
    row["reviewed_ts"] = datetime.utcnow()
    row["esignature"] = _AUTO_ESIGN

    from waves.wave1_synth_bronze.protocol_ingest import upsert_protocol_spec_rows
    upsert_protocol_spec_rows(c, [row])

    event_id = None
    try:
        from waves.wave5_serving_audit_app.setup_audit import append_event, AuditEventRecord
        rec = AuditEventRecord(
            event_type="protocol_auto_approved", actor=_AUTO_ESIGN, subject_id=study_id,
            details={"esignature": _AUTO_ESIGN,
                     "basis": (f"eval_ok=True, priority={g.get('review_priority')}, "
                               f"hard_fails={g.get('n_hard_fails')}"),
                     "note": ("unattended reproducibility auto-e-sign; "
                              "NOT a human 21 CFR Part 11 signature")})
        event_id = append_event(rec, c.table("audit", "gxp_audit"))
    except Exception as e:  # noqa: BLE001 - audit write is non-fatal to the approval itself
        print(f"[approve] auto-approve audit write skipped for {study_id}: {e}")

    print(f"[approve] AUTO-E-SIGNED {study_id} (eval_ok=True) — event {event_id}")
    return {"status": "success", "study_id": study_id, "review_status": "approved",
            "auto_esign": True, "esignature": _AUTO_ESIGN, "event_id": event_id}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--studies", default="poc_low,poc_med,poc_high",
                   help="comma-separated study_ids to consider for approval")
    p.add_argument("--allow-auto-esign", dest="allow_auto_esign", default="",
                   help="'true'/'false' forces unattended auto-e-sign on/off; "
                        "empty (default) defers to config allow_auto_esign")
    args = p.parse_args()
    c = cfg()
    auto = _resolve_auto(args.allow_auto_esign, c)
    tbl = c.table("raw", "protocol_spec")

    wanted = [s.strip() for s in args.studies.split(",") if s.strip()]
    have = {r["study_id"] for r in spark.sql(
        f"SELECT study_id FROM {tbl} WHERE review_status='extracted'").collect()}
    gate = _load_eval_gate(c)
    results = {}
    awaiting: list[str] = []   # eval-passing specs left for the human gate (auto off)
    for sid in wanted:
        if sid not in have:
            results[sid] = {"skipped": "no extracted spec (already approved or not ingested)"}
            continue
        g = gate.get(sid)
        # QUALITY GATE: approval (human OR auto) only applies to specs that passed
        # Stage-1 (eval_ok=True). A missing eval row (eval never ran) fails closed.
        if g is None or not g.get("eval_ok"):
            reasons = (g or {}).get("hard_fail_reasons") or ["no protocol_eval row (eval did not run)"]
            event_id = _audit_block(c, sid, reasons)
            results[sid] = {"skipped": "BLOCKED by extraction eval (eval_ok!=True); not e-signed",
                            "eval_ok": bool((g or {}).get("eval_ok")),
                            "n_hard_fails": (g or {}).get("n_hard_fails"),
                            "hard_fail_reasons": reasons,
                            "review_priority": (g or {}).get("review_priority"),
                            "audit_event_id": event_id,
                            "next": "route to analyst human review + e-sign (app)"}
            print(f"[approve] BLOCKED {sid}: eval_ok!=True — {reasons}")
            continue
        # Eval passed. DEFAULT = human gate: leave the spec 'extracted' for analyst
        # review + e-sign in the app. Auto-e-sign is opt-in (demo / unattended only).
        if not auto:
            awaiting.append(sid)
            results[sid] = {"skipped": "eval passed; LEFT for human review + e-sign in the app "
                                       "(allow_auto_esign not enabled)",
                            "review_status": "extracted",
                            "review_priority": g.get("review_priority"),
                            "next": "analyst review + e-sign (app)"}
            continue
        results[sid] = _auto_approve(c, sid, g)
    if awaiting:
        print(f"[approve] {len(awaiting)} specs passed eval and await human review + e-sign "
              f"in the app (set allow_auto_esign=true for unattended demo runs).")
    print(json.dumps(results, indent=2, default=str))


if __name__ == "__main__":
    main()
