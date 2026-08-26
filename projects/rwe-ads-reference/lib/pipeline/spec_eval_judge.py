"""Stage-3 reference-free model checks for an extracted protocol spec.

The judge earns its place only on the class that regex + confidence cannot
decide (see the design note's failure-class table): *is the extracted value
actually supported by the document and is it the right kind of thing?* and
*what did the protocol state that the spec omitted?* Both are **reference-free**
— they need the source document and the extracted spec, NOT SME labels — so
they run today with zero labelling.

Two checks, ported from the proven extraction-realism prototype:

  * ``citation_supports_value`` — groundedness + correct-type. NOTE: v2.1
    ``metadata.citations`` are bounding-box coordinates (page + pixel box), not
    text spans, so — exactly as the prototype does — the judge reads the FULL
    source text, not a resolved span. The citation ids are still surfaced to the
    review UI as "jump to source" anchors.
  * ``extraction_completeness`` — clinically material inclusion/exclusion/
    outcome/covariate content the protocol states but the spec omits.

**Every model call goes through ``lib.pipeline.gateway.gateway_call``** — never a
raw endpoint POST — so each judge call is PHI-masked, cost/latency logged to
``cfg().inference_table``, and traced in MLflow, same as any other model call in
the system. The prototype's direct ``databricks api post`` path is deliberately
NOT reused here.

⚠ Scorer-output gotcha (agent-evaluation skill): downstream metrics want
"yes"/"no" or numerics — never "pass"/"fail" (silently cast to None). These
judges emit "yes"/"no".
"""
from __future__ import annotations

import json
import re

from lib.pipeline.gateway import gateway_call
from lib.pipeline.spec_validate import _iter_field_codes

# Contract each code field is supposed to satisfy (drives the correct-type check).
FIELD_CONTRACT: dict[str, str] = {
    "dx_codes": "ICD-10-CM diagnosis codes identifying the qualifying cohort",
    "ndc_codes": "11-digit NDC product codes as literal digits (e.g. 00054-0165-24)",
    "exclude_dx": "ICD-10-CM diagnosis codes identifying exclusions",
    "outcome_codes": "ICD-10-CM diagnosis codes identifying the outcome",
    "covariates_coded": "ICD-10-CM or LOINC codes for baseline covariate flags",
}

_JUDGE_SYSTEM = ("You are a meticulous clinical-study-protocol extraction auditor. "
                 "Answer only with the requested JSON object — no prose, no code fences.")

GROUNDEDNESS_PROMPT = """You are auditing one field extracted from a clinical study protocol.

PROTOCOL (verbatim source):
---
{doc}
---

FIELD: {field}
FIELD CONTRACT: this field must contain {contract}.
EXTRACTED VALUES: {values}

Answer strictly as JSON with these keys:
  "supported": "yes" or "no"    - is each value actually stated or directly implied by the protocol?
  "correct_type": "yes" or "no" - does each value satisfy the FIELD CONTRACT above?
  "rationale": one sentence.

Return ONLY the JSON object."""

COMPLETENESS_PROMPT = """You are auditing the completeness of a structured extraction from a clinical study protocol.

PROTOCOL (verbatim source):
---
{doc}
---

EXTRACTED SPEC (field -> values):
{spec}

Identify clinically material content that the protocol states but the extracted spec
OMITS or MISREPRESENTS. Focus on: inclusion criteria, exclusion criteria, outcomes/endpoints,
covariates, exposure definition, and follow-up windows.

Answer strictly as JSON:
  "complete": "yes" or "no"
  "omissions": [ {{"item": "...", "why_it_matters": "..."}} ]
  "rationale": one sentence.

Return ONLY the JSON object."""


def _parse_judge_json(content: str) -> dict:
    """Extract the first JSON object from a judge reply, tolerant of stray text."""
    if not content:
        return {"error": "empty judge response"}
    m = re.search(r"\{.*\}", content, re.S)
    if not m:
        return {"error": f"no JSON object in response: {content[:200]}"}
    try:
        return json.loads(m.group(0))
    except json.JSONDecodeError as e:
        return {"error": f"bad JSON: {e}: {content[:200]}"}


def _call(model: str, prompt: str, *, w, log_rows, max_tokens: int) -> dict:
    """One judge call via the governed gateway (masked + logged + traced)."""
    try:
        res = gateway_call(model, _JUDGE_SYSTEM, prompt, w=w, log_rows=log_rows,
                           max_tokens=max_tokens)
    except Exception as e:  # noqa: BLE001 - a judge failure must never crash the eval
        return {"error": f"gateway_call failed: {repr(e)[:200]}"}
    return _parse_judge_json(res.content)


def judge_field_support(doc: str, field: str, values: list, *, w, log_rows,
                        model: str, max_tokens: int = 800) -> dict:
    """citation_supports_value: is each value supported AND the correct type?"""
    prompt = GROUNDEDNESS_PROMPT.format(
        doc=doc, field=field, contract=FIELD_CONTRACT.get(field, "the documented code type"),
        values=json.dumps(values))
    return _call(model, prompt, w=w, log_rows=log_rows, max_tokens=max_tokens)


def judge_completeness(doc: str, spec_flat: dict, *, w, log_rows,
                       model: str, max_tokens: int = 1500) -> dict:
    """extraction_completeness: what material content did the spec omit?"""
    prompt = COMPLETENESS_PROMPT.format(doc=doc, spec=json.dumps(spec_flat, indent=2))
    return _call(model, prompt, w=w, log_rows=log_rows, max_tokens=max_tokens)


def _flatten_spec(spec: dict) -> dict:
    """Compact field->values view of the spec for the completeness judge."""
    flat: dict = {}
    for field in FIELD_CONTRACT:
        vals = _iter_field_codes(spec, field)
        if vals:
            flat[field] = vals
    for scalar in ("complexity", "title", "objective", "study_start", "study_end",
                   "min_age", "max_age", "washout_days", "followup_days",
                   "pre_days", "post_days", "baseline_days"):
        v = spec.get(scalar)
        if v not in (None, ""):
            flat[scalar] = v
    return flat


def run_judges(spec: dict, doc: str, stage1: dict, *, w, log_rows,
               model: str) -> dict:
    """Run Stage-3 reference-free judges over a spec, given the source document.

    Cost-ordered funnel: only judge code fields that Stage-1 did NOT already hard-
    fail (a malformed value is settled; no judge call needed) and that have values.
    Completeness runs once over the whole spec. Requires ``doc`` (source text);
    callers pass "" when it is unavailable, in which case this returns no flags.

    Returns::

        {
          "field_flags": {field: {supported, correct_type, rationale}},  # only flagged
          "completeness": {complete, omissions, rationale} | {error},
          "n_type_flags": int,   # fields where correct_type=="no" or supported=="no"
          "judge_calls": int,
        }
    """
    out = {"field_flags": {}, "completeness": {}, "n_type_flags": 0, "judge_calls": 0}
    if not doc or not str(doc).strip():
        out["completeness"] = {"error": "source_text unavailable — completeness skipped"}
        return out

    hard_failed_fields = {d["field"] for d in (stage1.get("defects") or [])}

    for field in FIELD_CONTRACT:
        if field in hard_failed_fields:
            continue  # Stage 1 already settled this field
        values = _iter_field_codes(spec, field)
        if not values:
            continue
        verdict = judge_field_support(doc, field, values, w=w, log_rows=log_rows, model=model)
        out["judge_calls"] += 1
        if verdict.get("error"):
            out["field_flags"][field] = verdict
            continue
        flagged = (str(verdict.get("correct_type", "")).lower() == "no"
                   or str(verdict.get("supported", "")).lower() == "no")
        if flagged:
            out["field_flags"][field] = {
                "supported": verdict.get("supported"),
                "correct_type": verdict.get("correct_type"),
                "rationale": verdict.get("rationale"),
            }
            out["n_type_flags"] += 1

    comp = judge_completeness(doc, _flatten_spec(spec), w=w, log_rows=log_rows, model=model)
    out["judge_calls"] += 1
    out["completeness"] = comp
    return out


if __name__ == "__main__":
    print("spec_eval_judge module syntax OK; prompts + gateway routing wired")
