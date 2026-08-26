"""Deterministic standardization + validation of an extracted protocol (FIXED RULES).

This is the rules half of protocol intake — no model calls. It turns the raw
``ai_extract`` output (see waves/wave1_synth_bronze/protocol_extract.py) into the
coded spec the ADS Builder consumes, applying:

  * required-field enforcement (per complexity),
  * code normalization (uppercase, trim, drop wildcards, dedupe),
  * date -> ISO 'YYYY-MM-DD', int coercion + bounds,
  * covariate_codes -> covariates_coded [{name, codes}] baseline-flag specs,
  * sensible defaults for omitted windows (with warnings),
  * complexity -> low|medium|high (selects the composition recipe).

Pure Python (no Spark / no network) so it is unit-testable offline. Returns
``(spec, result)`` where ``result = {ok, errors, warnings}``. ``ok`` is False
only on hard errors (missing identity or cohort/outcome codes); everything else
degrades to a defaulted value plus a warning so a run never crashes on a
sloppy document — the human review gate is where judgement is applied.
"""
from __future__ import annotations

import json
import re
from datetime import date
from typing import Any

VALID_COMPLEXITY = {"low", "medium", "high"}

# NOTE on pre_days/post_days (continuous-enrollment window): defaulted to 90.
# The synthetic RWD's eligibility spans are short (max ~1094 days, randomly
# placed), so a 12-24 month single-span continuous-enrollment requirement
# excludes almost everyone. 90 days keeps demo cohorts non-empty; the OUTCOME
# follow-up window (followup_days) stays faithful to the protocol (365/730).
DEFAULTS = {
    "pre_days": 90, "post_days": 90, "baseline_days": 365,
    "followup_days": 365, "washout_days": 365, "grace_days": 30,
    "min_age": 0, "max_age": 120, "version": "1.0",
}
_AGE_BOUNDS = (0, 120)
_DAYS_MAX = 3650  # 10y sanity cap


def _norm_code(code: Any) -> str | None:
    if code is None:
        return None
    s = str(code).strip().upper()
    # drop trailing wildcard forms like "E10.*" / "I21*"
    s = re.sub(r"\.?\*+$", "", s)
    return s or None


def _norm_codes(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        # tolerate a JSON-ish or comma string
        try:
            parsed = json.loads(value)
            value = parsed if isinstance(parsed, list) else [value]
        except (ValueError, TypeError):
            value = [p for p in re.split(r"[,;]", value) if p.strip()]
    out, seen = [], set()
    for c in value:
        nc = _norm_code(c)
        if nc and nc not in seen:
            seen.add(nc)
            out.append(nc)
    return out


def _to_int(value: Any, lo: int = 0, hi: int = _DAYS_MAX) -> int | None:
    if value is None or value == "":
        return None
    try:
        n = int(float(str(value)))
    except (ValueError, TypeError):
        return None
    return max(lo, min(hi, n))


def _norm_date(value: Any) -> str | None:
    if not value:
        return None
    s = str(value).strip()[:10]
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", s)
    if not m:
        # try YYYY/MM/DD
        s2 = s.replace("/", "-")
        m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", s2)
        if not m:
            return None
        s = s2
    try:
        date.fromisoformat(s)
    except ValueError:
        return None
    return s


def _cov_name(code: str) -> str:
    ident = re.sub(r"[^0-9A-Za-z]", "_", code).lower()
    return f"cov_{ident}"


def standardize_extraction(extracted: dict) -> tuple[dict, dict]:
    """Standardize a raw ai_extract dict into a coded protocol spec.

    Returns (spec, result). ``spec`` uses protocol_spec column keys; ``result``
    is {"ok": bool, "errors": [...], "warnings": [...]}.
    """
    errors: list[str] = []
    warnings: list[str] = []
    e = extracted or {}

    study_id = (str(e.get("study_id") or "").strip() or None)
    if not study_id:
        errors.append("missing study_id")

    complexity = str(e.get("complexity") or "").strip().lower()
    if complexity in ("med", "medium"):
        complexity = "medium"
    if complexity not in VALID_COMPLEXITY:
        errors.append(f"invalid/absent complexity '{e.get('complexity')}' (need low|medium|high)")

    dx_codes = _norm_codes(e.get("dx_codes"))
    ndc_codes = _norm_codes(e.get("ndc_codes"))
    exclude_dx = _norm_codes(e.get("exclude_dx"))
    outcome_codes = _norm_codes(e.get("outcome_codes"))
    covariate_codes = _norm_codes(e.get("covariate_codes"))

    # cohort codes required by complexity
    if complexity == "low" and not dx_codes:
        errors.append("low-complexity prevalence cohort requires dx_codes")
    if complexity in ("medium", "high") and not ndc_codes:
        errors.append(f"{complexity}-complexity new-user cohort requires ndc_codes")
    if not outcome_codes:
        errors.append("outcome_codes is required")

    # dates
    study_start = _norm_date(e.get("study_start"))
    study_end = _norm_date(e.get("study_end"))
    if not study_start or not study_end:
        errors.append("study_start and study_end (YYYY-MM-DD) are required")

    # ints with defaults + warnings
    def _int_or_default(key: str, lo: int = 0, hi: int = _DAYS_MAX) -> int:
        v = _to_int(e.get(key), lo, hi)
        if v is None:
            v = DEFAULTS[key]
            warnings.append(f"{key} not extracted; defaulted to {v}")
        return v

    min_age = _to_int(e.get("min_age"), *_AGE_BOUNDS)
    max_age = _to_int(e.get("max_age"), *_AGE_BOUNDS)
    if min_age is None:
        min_age = DEFAULTS["min_age"]; warnings.append(f"min_age defaulted to {min_age}")
    if max_age is None:
        max_age = DEFAULTS["max_age"]; warnings.append(f"max_age defaulted to {max_age}")
    if min_age > max_age:
        warnings.append(f"min_age {min_age} > max_age {max_age}; swapped")
        min_age, max_age = max_age, min_age

    pre_days = _int_or_default("pre_days")
    post_days = _int_or_default("post_days")
    baseline_days = _int_or_default("baseline_days")
    followup_days = _int_or_default("followup_days")
    washout_days = _int_or_default("washout_days") if complexity in ("medium", "high") else _to_int(e.get("washout_days")) or DEFAULTS["washout_days"]
    grace_days = _to_int(e.get("grace_days")) or DEFAULTS["grace_days"]

    # covariate baseline-flag specs (one flag per code), deterministic names
    covariates_coded = [{"name": _cov_name(c), "codes": [c]} for c in covariate_codes]

    spec = {
        "study_id": study_id,
        "version": str(e.get("version") or DEFAULTS["version"]),
        "title": (str(e.get("title") or "").strip() or None),
        "objective": (str(e.get("objective") or "").strip() or None),
        "complexity": complexity if complexity in VALID_COMPLEXITY else None,
        "dx_codes": dx_codes,
        "ndc_codes": ndc_codes,
        "exclude_dx": exclude_dx,
        "outcome_codes": outcome_codes,
        "washout_days": washout_days,
        "grace_days": grace_days,
        "min_age": min_age,
        "max_age": max_age,
        "pre_days": pre_days,
        "post_days": post_days,
        "baseline_days": baseline_days,
        "followup_days": followup_days,
        "study_start": study_start,
        "study_end": study_end,
        "covariates_coded": json.dumps(covariates_coded),
    }
    result = {"ok": len(errors) == 0, "errors": errors, "warnings": warnings}
    return spec, result


if __name__ == "__main__":
    demo = {
        "study_id": "poc_high", "complexity": "High", "dx_codes": ["I21.9"],
        "ndc_codes": ["00071-0155"], "exclude_dx": ["K74.60", "N18.4"],
        "covariate_codes": ["E11.9", "I10", "I50.9"], "outcome_codes": ["I21.9"],
        "study_start": "2018-01-01", "study_end": "2024-12-31",
        "min_age": 40, "max_age": 75, "washout_days": 365, "followup_days": 730,
    }
    s, r = standardize_extraction(demo)
    print(json.dumps({"spec": s, "result": r}, indent=2))
