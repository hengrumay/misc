"""ADS pipeline (deterministic): generate, validate, and audit analysis-ready datasets from protocol specs.

Ordered steps:
  1. cohort definition (kb_retrieval + token_subst + validation)
  2. inclusion/exclusion criteria (same pipeline)
  3. variable derivation (baseline covariates, exposure eras, outcomes, followup)
  4. assembly (one row per patient -> ads_output)

All steps validate SQL before execution against synthetic gold only.
Mandatory analyst review gate before any ADS is marked 'approved'.
"""
from __future__ import annotations

__all__ = [
    "kb_retrieval",
    "token_subst",
    "validation",
    "ads_builder",
    "prompts",
]
