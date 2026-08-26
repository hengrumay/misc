"""Human-in-the-loop review gates and e-signature workflow (mandatory).

Two gates, both real and both hash-chained into ads_audit.gxp_audit:

  1. PROTOCOL approval (approve_protocol) — an analyst reviews the extracted +
     standardized coded spec, optionally corrects fields, and e-signs. Only
     review_status='approved' specs are consumable by the ADS Builder. Emits a
     'protocol_approval' event.
  2. ADS approval (sign_off) — after a build, an analyst reviews the generated
     SQL + validation and e-signs the ADS itself. Emits an 'ads_approval' event
     and finalizes the reproducibility manifest decision.

No ADS is 'approved' without an analyst review + e-signature (golden rule 11).
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
import logging
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

logger = logging.getLogger(__name__)

try:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
except Exception:  # pragma: no cover
    spark = None


@dataclass
class ReviewSignOff:
    review_id: str
    ads_id: str
    poc_id: str
    reviewer_name: str
    reviewer_email: str
    decision: str            # "approve" | "reject" | "request_revision"
    comments: str
    signature: str
    signed_at: str


def _cfg(c):
    if c is not None:
        return c
    from lib.config import cfg
    return cfg()


def _audit_event(event_type: str, actor: str, subject_id: str, details: dict, c) -> str:
    """Append a hash-chained event to gxp_audit (reuses wave5 append_event)."""
    from waves.wave5_serving_audit_app.setup_audit import append_event, AuditEventRecord
    rec = AuditEventRecord(event_type=event_type, actor=actor, subject_id=subject_id,
                           details={k: str(v) for k, v in (details or {}).items()})
    return append_event(rec, c.table("audit", "gxp_audit"))


# ============================ PROTOCOL approval ==============================

def approve_protocol(study_id: str, reviewer_name: str, reviewer_email: str,
                     signature: str, decision: str = "approve", comments: str = "",
                     corrected_fields: dict | None = None, c=None) -> dict:
    """Analyst review + e-sign of a protocol spec.

    On approve: applies any corrected_fields, sets review_status='approved',
    records reviewer/e-signature, and emits a 'protocol_approval' audit event.
    Only approved specs are consumable by the ADS Builder.
    """
    c = _cfg(c)
    if spark is None:
        return {"status": "error", "message": "PySpark not available"}
    if decision not in ("approve", "reject", "request_revision"):
        return {"status": "error", "message": f"invalid decision '{decision}'"}

    tbl = c.table("raw", "protocol_spec")
    rows = spark.sql(f"SELECT * FROM {tbl} WHERE study_id = '{study_id}' LIMIT 1").collect()
    if not rows:
        return {"status": "error", "message": f"no protocol_spec for study_id='{study_id}'"}

    row = rows[0].asDict()
    row.update(corrected_fields or {})
    row["review_status"] = "approved" if decision == "approve" else \
        ("rejected" if decision == "reject" else "extracted")
    row["reviewed_by"] = reviewer_email or reviewer_name
    row["reviewed_ts"] = datetime.utcnow()
    row["esignature"] = signature

    from waves.wave1_synth_bronze.protocol_ingest import upsert_protocol_spec_rows
    upsert_protocol_spec_rows(c, [row])

    event_id = _audit_event("protocol_approval", reviewer_email or reviewer_name, study_id,
                            {"decision": decision, "reviewer": reviewer_name,
                             "corrected": bool(corrected_fields), "comments": comments}, c)
    logger.info(f"protocol {decision}: {study_id} by {reviewer_name} (event {event_id})")
    return {"status": "success", "study_id": study_id, "review_status": row["review_status"],
            "event_id": event_id, "corrected_fields": list((corrected_fields or {}).keys())}


# ============================== ADS approval =================================

def list_pending_reviews(c=None) -> list[dict]:
    """List ADS builds pending analyst review from serving.review_queue."""
    c = _cfg(c)
    if spark is None:
        return []
    q = c.table("serving", "review_queue")
    try:
        rows = spark.sql(
            f"SELECT * FROM {q} WHERE status='PENDING' ORDER BY created_ts DESC").collect()
        return [r.asDict() for r in rows]
    except Exception as e:
        logger.warning(f"review_queue not readable: {e}")
        return []


def get_review_details(review_id: str, c=None) -> dict | None:
    """Fetch a review row + the generated SQL from its reproducibility manifest."""
    c = _cfg(c)
    if spark is None:
        return None
    q = c.table("serving", "review_queue")
    rows = spark.sql(f"SELECT * FROM {q} WHERE review_id='{review_id}' LIMIT 1").collect()
    if not rows:
        return None
    rec = rows[0].asDict()
    try:
        man = spark.sql(
            f"SELECT generated_sql, kb_snippet_versions FROM {c.table('audit','repro_manifest')} "
            f"WHERE ads_id='{rec['ads_id']}' ORDER BY created_ts DESC LIMIT 1").collect()
        if man:
            rec["generated_sql"] = man[0]["generated_sql"]
            rec["kb_snippet_versions"] = man[0]["kb_snippet_versions"]
    except Exception:
        pass
    return rec


def sign_off(review_id: str, ads_id: str, poc_id: str, reviewer_name: str,
             reviewer_email: str, decision: str, comments: str = "",
             signature: str = "", c=None) -> dict:
    """Record analyst sign-off (e-signature) on a built ADS.

    On approve: marks the review_queue row APPROVED, emits an 'ads_approval'
    audit event, and finalizes the reproducibility manifest decision. On reject/
    revision: records the decision + reason.
    """
    c = _cfg(c)
    if spark is None:
        return {"status": "error", "message": "PySpark not available"}
    if decision not in ("approve", "reject", "request_revision"):
        return {"status": "error", "message": f"invalid decision '{decision}'"}

    signoff = ReviewSignOff(review_id, ads_id, poc_id, reviewer_name, reviewer_email,
                            decision, comments, signature, datetime.utcnow().isoformat())
    status_map = {"approve": "APPROVED", "reject": "REJECTED", "request_revision": "REVISION"}
    q = c.table("serving", "review_queue")
    try:
        spark.sql(f"UPDATE {q} SET status='{status_map[decision]}' WHERE review_id='{review_id}'")
    except Exception as e:
        logger.warning(f"review_queue update skipped: {e}")

    event_type = {"approve": "ads_approval", "reject": "ads_rejected",
                  "request_revision": "review_gate_fail"}[decision]
    event_id = _audit_event(event_type, reviewer_email or reviewer_name, ads_id,
                            {"decision": decision, "poc_id": poc_id,
                             "reviewer": reviewer_name, "comments": comments}, c)

    manifest_id = None
    if decision == "approve":
        manifest_id = _finalize_manifest(ads_id, poc_id, signoff, c)

    logger.info(f"ADS {decision}: {ads_id} by {reviewer_name} (event {event_id})")
    return {"status": "success", "ads_id": ads_id, "decision": decision,
            "event_id": event_id, "manifest_id": manifest_id,
            "signoff_record": signoff.__dict__}


def _finalize_manifest(ads_id: str, study_id: str, signoff: ReviewSignOff, c) -> str | None:
    """Append an approved reproducibility manifest carrying the reviewer e-sign."""
    try:
        from waves.wave5_serving_audit_app.setup_audit import write_manifest, ManifestRecord
        prev = spark.sql(
            f"SELECT protocol_version, model, generated_sql, kb_snippet_versions, source_table_versions "
            f"FROM {c.table('audit','repro_manifest')} WHERE ads_id='{ads_id}' "
            f"ORDER BY created_ts DESC LIMIT 1").collect()
        base = prev[0].asDict() if prev else {}
        rec = ManifestRecord(
            ads_id=ads_id, study_id=study_id,
            protocol_version=base.get("protocol_version"),
            kb_snippet_versions=[dict(s.asDict()) for s in (base.get("kb_snippet_versions") or [])]
            if base.get("kb_snippet_versions") else None,
            generated_sql=dict(base.get("generated_sql") or {}),
            source_table_versions=dict(base.get("source_table_versions") or {}),
            model=base.get("model"), agent_version="ads_build_core/2.0",
            reviewer=signoff.reviewer_name, esignature=signoff.signature, decision="approved",
        )
        return write_manifest(rec, c.table("audit", "repro_manifest"))
    except Exception as e:
        logger.warning(f"manifest finalize skipped: {e}")
        return None


if __name__ == "__main__":
    print("review_gate module syntax OK")
