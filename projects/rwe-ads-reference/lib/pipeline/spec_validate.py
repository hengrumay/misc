"""Stage-1 deterministic validators for an extracted+standardized protocol spec.

The reference-free extraction-eval funnel (see the extraction eval + HITL
design note) has three stages;
this is **Stage 1**: pure regex + shape rules, **no model call**. It is the
hard gate — a spec that fails here is malformed by construction and must NOT be
auto-e-signed (``waves/wave3_ads_build/approve_protocols.py`` reads ``ok``).

What it checks (all HARD fails — an invalid code should never reach a GxP
e-signature unattended):

  * **required fields present** — study_id, complexity, cohort codes (dx for
    low; ndc for medium/high), outcome_codes, study_start/study_end.
  * **code well-formedness** — ICD-10-CM / NDC / LOINC format regexes; reject
    unexpanded ranges (``K74.3-K74.6``), wildcards (``I21*``), and free text /
    drug names sitting in a code field.
  * **value-in-wrong-field** — a value that is well-formed for a *different*
    vocabulary than the field contractually holds (e.g. an ICD code in
    ``ndc_codes``), which regex-per-field also surfaces but is reported with a
    clearer reason.

Ported from the proven extraction-realism prototype (``validate_code`` +
``FIELD_VOCAB``), adapted to read the
*standardized* protocol_spec (bare uppercased code arrays) instead of the raw
``ai_extract`` ``{value, ...}`` objects. Robust to either shape.

Pure Python (no Spark / no network) so it is unit-testable offline
(``tests/test_spec_validate.py``). ``validate_spec`` returns
``{ok, hard_fail_reasons, defects, n_hard_fails}``.
"""
from __future__ import annotations

import json
import re
from typing import Any

# ---- code-format regexes (ported verbatim from eval_prototype.py) ----------
ICD10 = re.compile(r"^[A-TV-Z][0-9][0-9AB](\.[0-9A-TV-Z]{1,4})?$")
NDC11 = re.compile(r"^\d{4,5}-\d{3,4}-\d{1,2}$")
LOINC = re.compile(r"^\d{1,5}-\d$")
_RANGE = re.compile(r"^[A-Z][0-9][0-9AB](\.[0-9]+)?\s*-\s*[A-Z][0-9][0-9AB](\.[0-9]+)?$")

MATCHERS = {"ICD10": ICD10, "NDC": NDC11, "LOINC": LOINC}

# which vocabulary each protocol_spec code field is contractually supposed to
# hold. ``covariates_coded`` is a JSON [{name, codes}] blob; its codes may be
# ICD-10 or LOINC (baseline covariate flags).
FIELD_VOCAB: dict[str, tuple[str, ...]] = {
    "dx_codes": ("ICD10",),
    "exclude_dx": ("ICD10",),
    "outcome_codes": ("ICD10",),
    "ndc_codes": ("NDC",),
    "covariates_coded": ("ICD10", "LOINC"),
}


def validate_code(value: Any, vocabs: tuple[str, ...]) -> str | None:
    """Return None if ``value`` is a well-formed code for one of ``vocabs``,
    else a string naming the defect. These are checks that should be
    deterministic rather than delegated to a model. Ported from eval_prototype.
    """
    if not isinstance(value, str):
        return f"not a string: {value!r}"
    v = value.strip()
    if not v:
        return "empty code"

    if "*" in v or "?" in v:
        return "wildcard — not an enumerable code"
    # a range such as K74.3-K74.6 : two code-shaped tokens joined by a hyphen
    if _RANGE.match(v):
        return "unexpanded range — matches zero rows as a literal"
    if " " in v and not NDC11.match(v):
        return "free text in a code field"
    if v.islower() or v.istitle():
        if not any(MATCHERS[k].match(v) for k in vocabs):
            return "prose/name, not a code literal"

    for k in vocabs:
        if MATCHERS[k].match(v):
            return None
    return f"malformed for {'/'.join(vocabs)}"


def _matches_any_vocab(value: str) -> str | None:
    """Name the vocabulary a well-formed value belongs to, or None."""
    for name, rx in MATCHERS.items():
        if rx.match(value.strip()):
            return name
    return None


def _iter_field_codes(spec: dict, field: str) -> list[str]:
    """Pull the code literals for one field from a standardized spec.

    Handles bare-string arrays (protocol_spec), raw ``{value, ...}`` objects
    (defensive), and the ``covariates_coded`` JSON [{name, codes:[...]}] blob.
    """
    raw = spec.get(field)
    if raw is None:
        return []
    if field == "covariates_coded":
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except (ValueError, TypeError):
                return []
        codes: list[str] = []
        for entry in raw or []:
            for c in (entry.get("codes") or []) if isinstance(entry, dict) else []:
                codes.append(c)
        return codes
    if isinstance(raw, str):  # a JSON-ish array string
        try:
            parsed = json.loads(raw)
            raw = parsed if isinstance(parsed, list) else [raw]
        except (ValueError, TypeError):
            raw = [raw]
    out = []
    for e in raw or []:
        out.append(e.get("value") if isinstance(e, dict) else e)
    return out


def run_validators(spec: dict) -> list[dict]:
    """Per-field code-format defects. Each defect is a hard fail.

    Upgrades a bare ``malformed for X`` to an explicit *value-in-wrong-field*
    message when the value is actually well-formed for a different vocabulary.
    """
    findings: list[dict] = []
    for field, vocabs in FIELD_VOCAB.items():
        for val in _iter_field_codes(spec, field):
            defect = validate_code(val, vocabs)
            if not defect:
                continue
            # A value that is well-formed for a DIFFERENT vocabulary is
            # definitively misfiled — report that regardless of which shape
            # heuristic (malformed / prose) validate_code happened to return.
            if isinstance(val, str):
                other = _matches_any_vocab(val)
                if other and other not in vocabs:
                    defect = (f"value-in-wrong-field: well-formed {other} code in "
                              f"'{field}' (expects {'/'.join(vocabs)})")
            findings.append({"field": field, "value": val, "defect": defect})
    return findings


def _missing_required(spec: dict) -> list[str]:
    """Required-field-present hard fails, re-checked on the standardized spec."""
    reasons: list[str] = []
    if not (str(spec.get("study_id") or "").strip()):
        reasons.append("required field empty: study_id")

    complexity = str(spec.get("complexity") or "").strip().lower()
    if complexity not in {"low", "medium", "high"}:
        reasons.append(f"required field invalid: complexity={spec.get('complexity')!r} "
                       f"(need low|medium|high)")

    dx = _iter_field_codes(spec, "dx_codes")
    ndc = _iter_field_codes(spec, "ndc_codes")
    if complexity == "low" and not dx:
        reasons.append("required field empty: dx_codes (low-complexity cohort)")
    if complexity in ("medium", "high") and not ndc:
        reasons.append(f"required field empty: ndc_codes ({complexity}-complexity cohort)")

    if not _iter_field_codes(spec, "outcome_codes"):
        reasons.append("required field empty: outcome_codes")

    for dcol in ("study_start", "study_end"):
        if not (str(spec.get(dcol) or "").strip()):
            reasons.append(f"required field empty: {dcol}")
    return reasons


def validate_spec(spec: dict) -> dict:
    """Stage-1 verdict for one standardized protocol spec.

    Returns::

        {
          "ok": bool,                 # False if ANY hard fail (blocks auto-e-sign)
          "hard_fail_reasons": [str], # required-empty + code-defect summaries
          "defects": [{field, value, defect}],
          "n_hard_fails": int,
        }
    """
    missing = _missing_required(spec)
    defects = run_validators(spec)
    reasons = list(missing)
    for d in defects:
        reasons.append(f"{d['field']}={d['value']!r}: {d['defect']}")
    return {
        "ok": len(reasons) == 0,
        "hard_fail_reasons": reasons,
        "defects": defects,
        "n_hard_fails": len(reasons),
    }


if __name__ == "__main__":
    # Self-check against the known realism-test failures (standardized/uppercased
    # shapes). Mirrors the prototype's proven catches: drug names in ndc_codes,
    # prose in outcome_codes, ranges/wildcards in exclude_dx.
    bad = {
        "study_id": "poc_high", "complexity": "high",
        "ndc_codes": ["ATORVASTATIN", "LISINOPRIL"],          # drug names, not NDC
        "outcome_codes": ["HEART FAILURE HOSPITALIZATION"],   # prose, not ICD
        "exclude_dx": ["K74.3-K74.6", "I21*"],                # range + wildcard
        "dx_codes": ["I21.9"],
        "study_start": "2018-01-01", "study_end": "2024-12-31",
    }
    res = validate_spec(bad)
    print(json.dumps(res, indent=2))
    assert res["ok"] is False, "expected the malformed spec to hard-fail"

    good = {
        "study_id": "poc_high", "complexity": "high",
        "ndc_codes": ["00054-0165-24"], "outcome_codes": ["I50.9"],
        "exclude_dx": ["N18.3"], "dx_codes": ["I21.9"],
        "study_start": "2018-01-01", "study_end": "2024-12-31",
    }
    res_ok = validate_spec(good)
    print(json.dumps(res_ok, indent=2))
    assert res_ok["ok"] is True, "expected the clean spec to pass Stage-1"
    print("spec_validate self-check OK")
