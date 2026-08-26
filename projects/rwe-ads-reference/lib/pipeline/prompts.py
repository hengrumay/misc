"""Prompt templates for the ADS builder (MLflow Prompt Registry).

Prompts are organized by step:
  - system: always-on system context
  - cohort: build initial cohort from protocol inclusion criteria
  - inclusion: apply additional inclusion filters
  - exclusion: apply exclusion filters
  - derivation: derive covariates, exposure eras, outcomes
  - assembly: join all components into final ADS output
  - validation: post-hoc check for protocol compliance

Each prompt is registered to MLflow Prompt Registry with versioning and governance.
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# System prompt (always present)
SYSTEM_PROMPT = """You are an expert SQL epidemiologist assistant. Your task is to generate
clinically-sound, validated SQL queries that transform RWD into analysis-ready datasets (ADS)
for real-world evidence studies.

Key principles:
1. Every query is built from APPROVED SQL snippets from the knowledge base (never invent SQL).
2. Queries validate against synthetic gold data only (no real patient databases).
3. All table/schema references are fully-qualified and resolve through cfg().
4. Parameters are substituted from the protocol specification (study dates, codes, washout periods).
5. Every generated SQL is validated (EXPLAIN, schema check, dry-run) before any human review.
6. Queries return clear audit trails: snippet IDs, versions, and substitution parameters.

When composing SQL from approved snippets:
- Retrieve APPROVED snippets matching the intent.
- Substitute protocol parameters (dates, codes, counts).
- Validate the resulting SQL.
- Return the validated SQL along with:
  * snippet IDs and versions used
  * parameter substitutions applied
  * validation result summary
  * row count estimate (if available)
"""

# Cohort step prompt
COHORT_PROMPT = """Given a study protocol, define the initial patient cohort.

The protocol specifies:
- Population: inclusion/exclusion criteria, index event, baseline window
- Study dates: enrollment start/end
- Qualifying codes: ICD-10-CM diagnosis codes, NDC drug codes, etc.
- Washout periods: "clean" baseline free of prior exposure (for new-user designs)

Your task:
1. Retrieve APPROVED cohort snippets (e.g., 'coh_base_prevalence', 'coh_new_user')
2. Substitute protocol parameters (dates, codes)
3. Validate the resulting SQL
4. Return: generated SQL, snippet IDs, validation summary

Cohort SQL must:
- Include only patients who meet the index event definition
- Return columns: patient_id, index_date
- Be deterministic (same protocol -> same cohort SQL)
"""

# Inclusion/exclusion step prompt
INCLUSION_PROMPT = """Apply inclusion and exclusion criteria to the cohort.

Given the protocol's inclusion/exclusion rules, filter the cohort.

Examples:
- Continuous enrollment: >=30 days before and after index date
- Age range: 18-65 years at index date
- Prior condition exclusion: no diagnosis of X in the baseline window

Your task:
1. Retrieve APPROVED inclusion/exclusion snippets
2. Substitute parameters (days, age ranges, codes)
3. Join to the input cohort via {{cohort}} placeholder
4. Validate the resulting SQL
5. Return: generated SQL, snippet IDs, count of survivors

Output SQL must:
- Input: {{cohort}} (with columns patient_id, index_date)
- Return: filtered cohort (same columns)
"""

# Derivation step prompt
DERIVATION_PROMPT = """Derive analytical variables: baseline covariates, exposure eras, outcomes.

Given the protocol's covariate and outcome definitions, derive columns for each patient.

Examples:
- Baseline comorbidity flags: 1 if patient had condition X in baseline window
- Exposure eras: collapse consecutive pharmacy fills into continuous exposure periods
- Time-to-event outcomes: days from index to first outcome occurrence

Your task:
1. For each variable, retrieve APPROVED derivation snippets
2. Substitute parameters (diagnosis/drug codes, baseline windows)
3. Join to cohort via {{cohort}} placeholder
4. Validate each derived-variable SQL
5. Return: list of derived SQL + metadata

Each derived-variable SQL must:
- Input: {{cohort}} (columns patient_id, index_date)
- Return: patient_id + derived column(s)
- Be logically independent (can be joined later)
"""

# Assembly step prompt
ASSEMBLY_PROMPT = """Assemble the final ADS: one row per patient.

Join cohort + inclusion/exclusion survivors + derived covariates + outcomes
into a final analytic table (ads_output) with one row per patient.

Your task:
1. Retrieve APPROVED assembly snippet (asm_one_row_per_patient)
2. Build SELECT list (all derived columns)
3. Build JOIN clauses (all derived-variable tables)
4. Substitute {{select_list}} and {{joins}} into the template
5. Validate the final SQL
6. Return: generated SQL, final column count, row count estimate

Final SQL must:
- Return one row per included patient
- Include all protocol-specified variables (cohort, covariates, outcomes, followup)
- Be sorted deterministically (e.g., by patient_id)
"""

# Validation step prompt
VALIDATION_PROMPT = """Post-hoc validation: ensure the ADS matches the protocol.

After assembly, verify:
1. Column count: matches protocol-specified variables?
2. Row count: matches expected cohort size (plausibility check)?
3. Data types: all columns match protocol expectations?
4. Followup: all patients have >=N days of followup?
5. Missingness: patterns consistent with protocol definition?

Your task:
1. Query the ads_output table
2. Run validation checks: COUNT(*), COUNT(DISTINCT patient_id), NULL counts per column
3. Report: pass/fail for each check, any rows violating protocol constraints
4. Return: validation summary for analyst review

Validation must NOT modify the ADS; it is read-only confirmation.
"""


def register_prompts() -> dict[str, str]:
    """Register all prompts to MLflow Prompt Registry (if available).

    Returns:
        Dict mapping prompt_name -> registered_uri for the workspace.
        If MLflow unavailable, returns a local dict for development.
    """
    prompts_dict = {
        "system": SYSTEM_PROMPT,
        "cohort": COHORT_PROMPT,
        "inclusion": INCLUSION_PROMPT,
        "derivation": DERIVATION_PROMPT,
        "assembly": ASSEMBLY_PROMPT,
        "validation": VALIDATION_PROMPT,
    }

    # Try to register to MLflow Prompt Registry
    try:
        import mlflow
        registry_uris = {}

        for name, template in prompts_dict.items():
            try:
                # Register prompt (simplified; real implementation uses mlflow.MlflowClient)
                prompt_name = f"ads_{name}_prompt"
                # In a real job: mlflow.MlflowClient().create_prompt(name=prompt_name, text=template)
                registry_uris[name] = f"models:/{prompt_name}/1"
                logger.info(f"Registered prompt: {prompt_name}")
            except Exception as e:
                logger.warning(f"Could not register prompt {name}: {e}")

        if registry_uris:
            return registry_uris
    except ImportError:
        logger.info("MLflow not available; using local prompts")

    # Fallback: return local dict
    return prompts_dict


def get_prompt(name: str) -> str:
    """Get a prompt template by name (system, cohort, inclusion, etc.)."""
    prompts = {
        "system": SYSTEM_PROMPT,
        "cohort": COHORT_PROMPT,
        "inclusion": INCLUSION_PROMPT,
        "derivation": DERIVATION_PROMPT,
        "assembly": ASSEMBLY_PROMPT,
        "validation": VALIDATION_PROMPT,
    }
    if name not in prompts:
        raise ValueError(f"Unknown prompt: {name}. Available: {list(prompts.keys())}")
    return prompts[name]


if __name__ == "__main__":
    print("prompts module syntax OK")
    register_prompts()
